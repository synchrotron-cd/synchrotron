//! Rapid-event debouncer for reconcile triggers.
//!
//! Wraps a [`PoolHandle`] with a trailing debounce: each
//! [`Debouncer::enqueue`] starts (or restarts) a per-app timer;
//! when no further events arrive for [`DebounceConfig::window`],
//! the timer fires a single coalesced enqueue on the pool. A burst
//! of N events for the same app within the window collapses to
//! exactly one reconcile.
//!
//! # Why debounce in addition to the pool's coalescing?
//!
//! The pool's [`PoolHandle::enqueue_coalesce`] skips enqueue-while-
//! pending, but that only dedupes events that arrive *while a
//! reconcile is pending or in-flight*. If 50 webhooks land in
//! 200ms during an idle period, coalesce alone would enqueue the
//! first and skip the next 49 — which is correct, but means the
//! reconcile runs against potentially stale desired state
//! (rendered before the later webhooks). The debouncer delays the
//! first enqueue too, so the reconcile runs against the *quiesced*
//! state at the end of the burst.
//!
//! # Trigger tag: latest-wins
//!
//! When multiple events collapse into one reconcile, the trigger
//! tag from the **last** event in the burst is the one reported.
//! Rationale: the trigger tag is diagnostic ("why did this run?"),
//! and the most recent reason is the most relevant. Operators
//! debugging "why did my webhook not trigger a reconcile?" want
//! to see `Webhook`, not the `Poll` that happened to arrive a
//! second earlier.
//!
//! # Concurrency model
//!
//! Per-app state carries a monotonic generation counter. Each
//! [`Debouncer::enqueue`] call bumps the generation and spawns a
//! fresh timer task carrying its own gen. After sleeping, the
//! timer fires only if the current pending entry still matches
//! its gen — otherwise it's been superseded and it silently
//! exits. No explicit cancellation is needed; superseded timers
//! just no-op when they wake. This trades a small amount of task
//! churn under heavy bursts for a simpler, race-free design.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::sleep;
use tracing::debug;

use crate::worker_pool::{PoolHandle, Trigger};

#[derive(Debug, Clone)]
pub struct DebounceConfig {
    /// Quiet-time after the last event required before the timer
    /// fires. 500ms is a middle-ground default: long enough to
    /// collapse typical webhook bursts (CI fan-out, batched git
    /// push), short enough that human-scale waits don't feel
    /// laggy.
    pub window: Duration,
}

impl Default for DebounceConfig {
    fn default() -> Self {
        Self {
            window: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DebounceStats {
    /// Events observed from producers.
    pub accepted: u64,
    /// Enqueues dispatched to the pool after the debounce window.
    pub fired: u64,
}

struct PendingEntry {
    trigger: Trigger,
    /// Bumped on every enqueue for this app. The timer spawned
    /// alongside the bump records this value and only fires if
    /// the stored entry still matches.
    generation: u64,
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    fired: AtomicU64,
}

pub struct Debouncer {
    pool: PoolHandle,
    window: Duration,
    pending: Arc<Mutex<HashMap<String, PendingEntry>>>,
    counters: Arc<Counters>,
}

impl Debouncer {
    pub fn new(pool: PoolHandle, config: DebounceConfig) -> Self {
        Self {
            pool,
            window: config.window,
            pending: Arc::new(Mutex::new(HashMap::new())),
            counters: Arc::new(Counters::default()),
        }
    }

    /// Record a trigger event for `app_id`. Returns immediately;
    /// the reconcile will be enqueued onto the pool after
    /// [`DebounceConfig::window`] of quiet.
    pub fn enqueue(&self, app_id: impl Into<String>, trigger: Trigger) {
        let app_id = app_id.into();
        self.counters.accepted.fetch_add(1, Ordering::Relaxed);

        let generation = {
            let mut pending = self.pending.lock().expect("debouncer mutex poisoned");
            let entry = pending.entry(app_id.clone()).or_insert(PendingEntry {
                trigger,
                generation: 0,
            });
            entry.generation += 1;
            entry.trigger = trigger;
            entry.generation
        };

        let pending = self.pending.clone();
        let pool = self.pool.clone();
        let counters = self.counters.clone();
        let window = self.window;
        tokio::spawn(async move {
            sleep(window).await;
            fire_if_current(&app_id, generation, &pending, &pool, &counters);
        });
    }

    pub fn stats(&self) -> DebounceStats {
        DebounceStats {
            accepted: self.counters.accepted.load(Ordering::Relaxed),
            fired: self.counters.fired.load(Ordering::Relaxed),
        }
    }

    /// Fire every pending debounce immediately. Use at shutdown
    /// to flush outstanding work, or in tests that want to skip
    /// the timer wait. Events arriving after this call continue
    /// to debounce normally.
    pub fn flush_now(&self) {
        let drained: Vec<(String, PendingEntry)> = {
            let mut pending = self.pending.lock().expect("debouncer mutex poisoned");
            pending.drain().collect()
        };
        for (app_id, entry) in drained {
            self.counters.fired.fetch_add(1, Ordering::Relaxed);
            let _ = self.pool.enqueue_coalesce(app_id, entry.trigger);
        }
    }
}

fn fire_if_current(
    app_id: &str,
    generation: u64,
    pending: &Arc<Mutex<HashMap<String, PendingEntry>>>,
    pool: &PoolHandle,
    counters: &Arc<Counters>,
) {
    let trigger = {
        let mut pending = pending.lock().expect("debouncer mutex poisoned");
        match pending.get(app_id) {
            Some(entry) if entry.generation == generation => {
                let trigger = entry.trigger;
                pending.remove(app_id);
                Some(trigger)
            }
            // Either the entry is gone (flushed) or a newer
            // generation has taken over — this timer is stale.
            _ => None,
        }
    };
    if let Some(trigger) = trigger {
        counters.fired.fetch_add(1, Ordering::Relaxed);
        match pool.enqueue_coalesce(app_id.to_string(), trigger) {
            Ok(true) => debug!(app = app_id, ?trigger, "debounced reconcile enqueued"),
            Ok(false) => debug!(app = app_id, "debounced reconcile coalesced at pool"),
            Err(e) => debug!(app = app_id, error = %e, "debounced enqueue rejected"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use crate::worker_pool::{PoolConfig, WorkerPool};

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
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    #[tokio::test]
    async fn single_event_fires_after_window() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            DebounceConfig {
                window: Duration::from_millis(40),
            },
        );

        deb.enqueue("a", Trigger::Webhook);
        // Before window elapses, pool hasn't seen the enqueue.
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(pool.stats().enqueued, 0);

        assert!(wait_for(|| pool.stats().enqueued == 1, 500).await);
        assert_eq!(deb.stats().accepted, 1);
        assert_eq!(deb.stats().fired, 1);

        gate.add_permits(1);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn burst_collapses_to_one_enqueue() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            DebounceConfig {
                window: Duration::from_millis(40),
            },
        );

        // 20 events over ~100µs.
        for _ in 0..20 {
            deb.enqueue("a", Trigger::Webhook);
        }
        // Then keep poking well inside the window — each poke
        // resets the timer, so nothing should fire yet.
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            deb.enqueue("a", Trigger::Webhook);
        }
        assert_eq!(pool.stats().enqueued, 0);

        // After one full window of silence, exactly one fire.
        assert!(wait_for(|| deb.stats().fired == 1, 500).await);
        assert!(wait_for(|| pool.stats().enqueued == 1, 500).await);
        assert_eq!(deb.stats().accepted, 25);

        gate.add_permits(1);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn latest_trigger_wins_in_burst() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            DebounceConfig {
                window: Duration::from_millis(40),
            },
        );

        deb.enqueue("a", Trigger::Poll);
        deb.enqueue("a", Trigger::AutoHeal);
        deb.enqueue("a", Trigger::Webhook); // latest

        assert!(wait_for(|| pool.stats().enqueued == 1, 500).await);
        // Wait until the handler has actually recorded the tuple.
        assert!(wait_for(|| !log.lock().unwrap().is_empty(), 500).await);
        let recorded = log.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].1, Trigger::Webhook);

        gate.add_permits(1);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn different_apps_debounced_independently() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            DebounceConfig {
                window: Duration::from_millis(40),
            },
        );

        for _ in 0..5 {
            deb.enqueue("a", Trigger::Webhook);
            deb.enqueue("b", Trigger::Poll);
        }

        assert!(wait_for(|| deb.stats().fired == 2, 500).await);
        assert!(wait_for(|| pool.stats().enqueued == 2, 500).await);

        gate.add_permits(2);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn flush_now_fires_pending_immediately() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            // Long enough that the natural timer would not fire
            // within the test's polling window.
            DebounceConfig {
                window: Duration::from_secs(10),
            },
        );

        deb.enqueue("a", Trigger::Manual);
        deb.enqueue("b", Trigger::Poll);
        deb.flush_now();

        assert!(wait_for(|| pool.stats().enqueued == 2, 500).await);
        assert_eq!(deb.stats().fired, 2);

        gate.add_permits(2);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn flush_does_not_double_fire_when_timer_also_runs() {
        // Enqueue, flush immediately, then wait past the window.
        // The stale timer should observe a missing / superseded
        // entry and no-op rather than double-firing.
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            DebounceConfig {
                window: Duration::from_millis(30),
            },
        );

        deb.enqueue("a", Trigger::Webhook);
        deb.flush_now();
        // Wait well past the window.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(deb.stats().fired, 1);
        assert_eq!(pool.stats().enqueued, 1);

        gate.add_permits(1);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn burst_property_fires_per_app_exactly_once() {
        // Scripted "property"-style check: many interleaved events
        // across several apps. Invariants:
        //  - fired ≤ number of distinct apps that received events
        //  - fired events cover every app that received events
        //  - accepted equals the total enqueue calls
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = pool_with_gate(log.clone(), gate.clone());
        let deb = Debouncer::new(
            pool.handle(),
            DebounceConfig {
                window: Duration::from_millis(30),
            },
        );

        let apps = ["a", "b", "c", "d"];
        let total_events = Arc::new(AtomicUsize::new(0));
        // Interleave: 50 events across 4 apps, round-robin.
        for i in 0..50 {
            let app = apps[i % apps.len()];
            let trigger = match i % 3 {
                0 => Trigger::Webhook,
                1 => Trigger::Poll,
                _ => Trigger::AutoHeal,
            };
            deb.enqueue(app, trigger);
            total_events.fetch_add(1, Ordering::SeqCst);
        }

        // One full window of silence ⇒ all apps fire once.
        assert!(wait_for(|| deb.stats().fired as usize == apps.len(), 500).await);
        assert!(wait_for(|| pool.stats().enqueued as usize == apps.len(), 500).await);

        assert_eq!(
            deb.stats().accepted as usize,
            total_events.load(Ordering::SeqCst)
        );

        gate.add_permits(apps.len());
        assert!(wait_for(|| pool.stats().completed as usize == apps.len(), 500).await);

        // Every app got exactly one reconcile.
        let recorded = log.lock().unwrap().clone();
        let mut seen: Vec<_> = recorded.iter().map(|(a, _)| a.clone()).collect();
        seen.sort();
        let expected: Vec<_> = apps.iter().map(|s| s.to_string()).collect();
        assert_eq!(seen, expected);

        pool.shutdown().await;
    }
}
