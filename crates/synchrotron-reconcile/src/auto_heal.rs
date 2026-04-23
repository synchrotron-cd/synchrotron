//! Periodic auto-heal scheduler.
//!
//! Wakes every [`AutoHealConfig::interval`] and enqueues a
//! reconcile for every known app, so drift that nobody observed
//! (bus event lost, informer missed an update, human edited the
//! cluster out-of-band) still gets detected within one interval.
//!
//! # Jitter
//!
//! A naive "enqueue everything at once" scheduler would hammer
//! kube-apiserver with N simultaneous reconciles every interval,
//! especially bad right after startup. [`AutoHealConfig::jitter_fraction`]
//! defines a window (`interval × fraction`) over which one tick's
//! enqueues are spread: app *i* of *N* is enqueued at
//! `tick_start + i × (window / N)`. Deterministic uniform spread
//! is easier to reason about than randomized jitter and keeps
//! tests stable; the goal is load-spreading, not unpredictability.
//!
//! # Skipping in-flight apps
//!
//! Enqueuing goes through [`PoolHandle::enqueue_coalesce`]: if the
//! app already has a pending or in-flight reconcile (e.g. triggered
//! by a webhook), the auto-heal enqueue is skipped. A separate
//! counter tracks skips so operators can see at a glance how much
//! auto-heal work was redundant with event-driven reconciles.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use synchrotron_types::AppName;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::worker_pool::{PoolHandle, Trigger};

#[derive(Debug, Clone)]
pub struct AutoHealConfig {
    /// Time between ticks. Each tick enqueues every app once.
    pub interval: Duration,
    /// Fraction of `interval` used as the spread window for a
    /// single tick's enqueues. `0.0` disables spreading (all apps
    /// enqueued back-to-back); `1.0` spreads enqueues across the
    /// whole interval. The default `0.1` spreads across 10% of
    /// the interval, giving the pool plenty of headroom before
    /// the next tick begins.
    pub jitter_fraction: f64,
}

impl Default for AutoHealConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(180),
            jitter_fraction: 0.1,
        }
    }
}

/// Source of the current set of apps to auto-heal.
///
/// Called once per tick — implementations should be cheap and
/// reflect the current state (an operator adding a new app should
/// see it picked up within one interval).
pub trait AppLister: Send + Sync {
    fn all(&self) -> Vec<AppName>;
}

#[derive(Default)]
struct Counters {
    ticks: AtomicU64,
    enqueued: AtomicU64,
    skipped: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutoHealStats {
    /// Number of tick iterations completed.
    pub ticks: u64,
    /// Reconciles accepted by the pool (counted across every tick).
    pub enqueued: u64,
    /// Enqueues coalesced away because the app already had a
    /// pending or in-flight reconcile.
    pub skipped: u64,
}

pub struct AutoHealScheduler {
    task: JoinHandle<()>,
    counters: Arc<Counters>,
}

impl AutoHealScheduler {
    pub fn spawn(pool: PoolHandle, lister: Arc<dyn AppLister>, config: AutoHealConfig) -> Self {
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(run_loop(pool, lister, config, counters.clone()));
        Self { task, counters }
    }

    pub fn stats(&self) -> AutoHealStats {
        AutoHealStats {
            ticks: self.counters.ticks.load(Ordering::Relaxed),
            enqueued: self.counters.enqueued.load(Ordering::Relaxed),
            skipped: self.counters.skipped.load(Ordering::Relaxed),
        }
    }

    pub async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn run_loop(
    pool: PoolHandle,
    lister: Arc<dyn AppLister>,
    config: AutoHealConfig,
    counters: Arc<Counters>,
) {
    loop {
        sleep(config.interval).await;
        tick(&pool, lister.as_ref(), &config, &counters).await;
    }
}

async fn tick(
    pool: &PoolHandle,
    lister: &dyn AppLister,
    config: &AutoHealConfig,
    counters: &Counters,
) {
    let apps = lister.all();
    counters.ticks.fetch_add(1, Ordering::Relaxed);
    if apps.is_empty() {
        return;
    }

    // Spread width: fraction of the interval to spread this tick's
    // enqueues over. Clamped to a sensible range.
    let fraction = config.jitter_fraction.clamp(0.0, 1.0);
    let jitter_window = config.interval.mul_f64(fraction);
    let per_app_delay = if jitter_window.is_zero() || apps.len() <= 1 {
        Duration::ZERO
    } else {
        jitter_window / apps.len() as u32
    };

    debug!(
        apps = apps.len(),
        jitter_ms = jitter_window.as_millis() as u64,
        "auto-heal tick"
    );

    for (i, app) in apps.iter().enumerate() {
        match pool.enqueue_coalesce(app.0.clone(), Trigger::AutoHeal) {
            Ok(true) => {
                counters.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                counters.skipped.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                warn!(%app, error = %e, "auto-heal enqueue failed");
            }
        }
        // Sleep between enqueues except after the last one — no
        // point waiting at the tail of a tick.
        if per_app_delay > Duration::ZERO && i + 1 < apps.len() {
            sleep(per_app_delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::time::sleep as real_sleep;

    use crate::worker_pool::{PoolConfig, WorkerPool};

    struct StaticLister(Vec<AppName>);
    impl AppLister for StaticLister {
        fn all(&self) -> Vec<AppName> {
            self.0.clone()
        }
    }

    /// Recording handler that lets the test gate completion.
    fn pool_with_gate(
        log: Arc<Mutex<Vec<(String, Trigger)>>>,
        gate: Arc<tokio::sync::Semaphore>,
    ) -> WorkerPool {
        WorkerPool::new(PoolConfig::default(), move |ctx| {
            let log = log.clone();
            let gate = gate.clone();
            Box::pin(async move {
                log.lock().unwrap().push((ctx.app_id, ctx.trigger));
                gate.acquire().await.unwrap().forget();
            })
        })
    }

    async fn wait_for<F: Fn() -> bool>(pred: F, max_ms: u64) -> bool {
        let steps = max_ms / 5;
        for _ in 0..steps {
            if pred() {
                return true;
            }
            real_sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    #[tokio::test]
    async fn first_tick_fires_after_interval_and_enqueues_all_apps() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let lister = Arc::new(StaticLister(vec![
            AppName("a".into()),
            AppName("b".into()),
            AppName("c".into()),
        ]));
        let config = AutoHealConfig {
            interval: Duration::from_millis(80),
            jitter_fraction: 0.0,
        };
        let sched = AutoHealScheduler::spawn(pool.handle(), lister, config);

        // Before the first interval elapses, nothing should be
        // enqueued. Sample at ~half the interval.
        real_sleep(Duration::from_millis(40)).await;
        assert_eq!(pool.stats().enqueued, 0);

        // After the interval, all three apps get enqueued.
        assert!(wait_for(|| pool.stats().enqueued == 3, 500).await);
        let s = sched.stats();
        assert_eq!(s.ticks, 1);
        assert_eq!(s.enqueued, 3);
        assert_eq!(s.skipped, 0);

        // All three jobs should be tagged as AutoHeal.
        gate.add_permits(3);
        assert!(wait_for(|| pool.stats().completed == 3, 500).await);
        let recorded = log.lock().unwrap().clone();
        assert!(recorded.iter().all(|(_, t)| *t == Trigger::AutoHeal));

        sched.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn in_flight_apps_are_skipped() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());

        // Pre-enqueue app "a" manually and leave it in-flight.
        pool.enqueue("a", Trigger::Manual).unwrap();
        assert!(wait_for(|| pool.stats().in_flight == 1, 500).await);

        let lister = Arc::new(StaticLister(vec![AppName("a".into()), AppName("b".into())]));
        let config = AutoHealConfig {
            interval: Duration::from_millis(60),
            jitter_fraction: 0.0,
        };
        let sched = AutoHealScheduler::spawn(pool.handle(), lister, config);

        // After the first tick, "a" should be skipped and "b"
        // should be enqueued.
        assert!(wait_for(|| sched.stats().ticks >= 1, 500).await);
        // Give the tick's inner loop a moment to finish.
        assert!(wait_for(|| sched.stats().enqueued + sched.stats().skipped >= 2, 500).await);

        let s = sched.stats();
        assert_eq!(s.enqueued, 1, "expected only `b` enqueued");
        assert_eq!(s.skipped, 1, "expected `a` skipped as in-flight");

        // Drain: release "a" and the newly-enqueued "b".
        gate.add_permits(2);
        assert!(wait_for(|| pool.stats().completed == 2, 500).await);

        sched.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn empty_app_list_still_ticks() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log, gate);
        let lister = Arc::new(StaticLister(vec![]));
        let config = AutoHealConfig {
            interval: Duration::from_millis(40),
            jitter_fraction: 0.0,
        };
        let sched = AutoHealScheduler::spawn(pool.handle(), lister, config);

        assert!(wait_for(|| sched.stats().ticks >= 2, 500).await);
        let s = sched.stats();
        assert!(s.ticks >= 2);
        assert_eq!(s.enqueued, 0);
        assert_eq!(s.skipped, 0);

        sched.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn jitter_spreads_enqueues_across_tick_window() {
        // With a 200ms interval and 0.5 jitter fraction, the
        // jitter window is 100ms. Three apps → ~33ms spacing.
        // We assert the second enqueue arrives strictly after
        // the first by at least a detectable margin.
        let log = Arc::new(Mutex::new(Vec::<(std::time::Instant, String)>::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = {
            let log = log.clone();
            let gate = gate.clone();
            WorkerPool::new(PoolConfig::default(), move |ctx| {
                let log = log.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    log.lock()
                        .unwrap()
                        .push((std::time::Instant::now(), ctx.app_id));
                    gate.acquire().await.unwrap().forget();
                })
            })
        };
        let lister = Arc::new(StaticLister(vec![
            AppName("a".into()),
            AppName("b".into()),
            AppName("c".into()),
        ]));
        let config = AutoHealConfig {
            interval: Duration::from_millis(200),
            jitter_fraction: 0.5,
        };
        let sched = AutoHealScheduler::spawn(pool.handle(), lister, config);

        assert!(wait_for(|| log.lock().unwrap().len() >= 3, 1000).await);
        let times: Vec<_> = log.lock().unwrap().iter().map(|(t, _)| *t).collect();
        // Spacing between consecutive enqueues should be at least
        // half the nominal per-app delay (~33ms) — loose enough
        // to avoid CI flakes, tight enough to prove spreading.
        let gap = times[1].duration_since(times[0]);
        assert!(
            gap >= Duration::from_millis(10),
            "expected spaced enqueues, got gap {gap:?}"
        );

        gate.add_permits(3);
        sched.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn ticks_counter_increments_per_tick() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(Arc::new(Mutex::new(Vec::new())), gate.clone());
        let lister = Arc::new(StaticLister(vec![]));
        let config = AutoHealConfig {
            interval: Duration::from_millis(30),
            jitter_fraction: 0.0,
        };
        let sched = AutoHealScheduler::spawn(pool.handle(), lister, config);

        assert!(wait_for(|| sched.stats().ticks >= 3, 500).await);
        assert!(sched.stats().ticks >= 3);

        sched.stop().await;
        pool.shutdown().await;
    }
}
