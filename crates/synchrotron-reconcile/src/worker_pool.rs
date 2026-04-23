//! Bounded worker pool with per-app FIFO serialization.
//!
//! # Invariants
//!
//! - **Per-app FIFO**: jobs for a given `app_id` complete in enqueue
//!   order. A second job for the same app sits in a per-app queue
//!   until the first finishes.
//! - **At most one in-flight per app**: even when the global
//!   concurrency bound has room, an app with a running reconcile
//!   does not get another slot. This matches the acceptance
//!   criterion and keeps reconciles deterministic.
//! - **Global bound**: no more than [`PoolConfig::max_concurrent`]
//!   handler invocations in flight simultaneously.
//! - **Per-app backpressure**: [`PoolConfig::per_app_queue_cap`] caps
//!   the pending queue for each app; further enqueues return
//!   [`EnqueueError::QueueFull`]. This prevents a single runaway
//!   trigger source from unbounded memory growth.
//!
//! # Architecture
//!
//! A single dispatcher task owns the scheduling state behind a
//! `Mutex`. It wakes on a [`tokio::sync::Notify`] whenever work
//! arrives or a handler finishes, drains every dispatchable job it
//! can, then parks again. Handler invocations run on detached
//! `tokio::spawn` tasks so a slow handler never blocks dispatch.
//!
//! On completion each spawned task re-enters the state, removes
//! itself from `in_flight`, re-queues the app if more jobs are
//! pending, and notifies the dispatcher. Shutdown flips a flag and
//! wakes the dispatcher, which stops taking new work, awaits every
//! still-running handler, and exits. Already-queued but undispatched
//! jobs are dropped — matching "drain in-flight" semantics, not
//! "drain everything" (the latter would let a stuck handler hold up
//! shutdown indefinitely).

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type HandlerFn = Arc<dyn Fn(JobCtx) -> BoxFuture + Send + Sync>;

#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Hard cap on concurrent handler invocations.
    pub max_concurrent: usize,
    /// Per-app pending queue cap. Further enqueues for an app with
    /// this many jobs waiting return [`EnqueueError::QueueFull`].
    pub per_app_queue_cap: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 16,
            per_app_queue_cap: 8,
        }
    }
}

/// What caused this reconcile request. Used for observability and
/// by downstream slices (debouncing, sync-wave scheduler).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Webhook,
    Poll,
    AutoHeal,
    Manual,
}

#[derive(Debug, Clone)]
pub struct JobCtx {
    pub app_id: String,
    pub trigger: Trigger,
    pub enqueued_at: Instant,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnqueueError {
    #[error("worker pool is shut down")]
    Shutdown,
    #[error("per-app queue full for `{app_id}` (cap {cap})")]
    QueueFull { app_id: String, cap: usize },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    pub enqueued: u64,
    pub rejected_full: u64,
    pub started: u64,
    pub completed: u64,
    pub in_flight: usize,
    pub queue_depth: usize,
    /// Sum of time each started job spent in the queue, in
    /// microseconds. Divide by `started` for average wait; export
    /// directly for a counter-style metric.
    pub total_wait_micros: u64,
}

struct State {
    /// Per-app pending queues. Absent from the map ≡ no pending and
    /// not in-flight; we garbage-collect on completion.
    apps: HashMap<String, VecDeque<JobCtx>>,
    /// Apps with queued work that are not currently in-flight, in
    /// dispatch order. Avoids scanning `apps` on every wake.
    ready: VecDeque<String>,
    /// Apps with a handler currently running. Used to enforce
    /// at-most-one-in-flight-per-app.
    in_flight: HashSet<String>,
    /// Summed queued depth across every app — cached so `stats()`
    /// doesn't have to iterate.
    queue_depth: usize,
    shutdown: bool,
}

#[derive(Default)]
struct Counters {
    enqueued: AtomicU64,
    rejected_full: AtomicU64,
    started: AtomicU64,
    completed: AtomicU64,
    total_wait_micros: AtomicU64,
}

struct Inner {
    state: Mutex<State>,
    notify: Notify,
    handler: HandlerFn,
    config: PoolConfig,
    counters: Counters,
}

pub struct WorkerPool {
    inner: Arc<Inner>,
    dispatcher: Option<JoinHandle<()>>,
}

impl WorkerPool {
    pub fn new<F, Fut>(config: PoolConfig, handler: F) -> Self
    where
        F: Fn(JobCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler: HandlerFn =
            Arc::new(move |ctx| Box::pin(handler(ctx)) as Pin<Box<dyn Future<Output = ()> + Send>>);
        let inner = Arc::new(Inner {
            state: Mutex::new(State {
                apps: HashMap::new(),
                ready: VecDeque::new(),
                in_flight: HashSet::new(),
                queue_depth: 0,
                shutdown: false,
            }),
            notify: Notify::new(),
            handler,
            config,
            counters: Counters::default(),
        });
        let dispatcher = {
            let inner = inner.clone();
            tokio::spawn(async move { dispatcher_loop(inner).await })
        };
        Self {
            inner,
            dispatcher: Some(dispatcher),
        }
    }

    /// Enqueue a reconcile for `app_id`. Returns immediately without
    /// waiting for the handler to start or finish.
    pub fn enqueue(&self, app_id: impl Into<String>, trigger: Trigger) -> Result<(), EnqueueError> {
        enqueue_impl(&self.inner, app_id.into(), trigger, false).map(|_| ())
    }

    /// Enqueue only if no job for `app_id` is currently pending or
    /// in-flight. Returns `Ok(true)` if accepted, `Ok(false)` if
    /// coalesced away. Intended for event-driven triggers that fire
    /// on every upstream change — coalescing prevents a rapid burst
    /// from filling the per-app queue with redundant reconciles
    /// that would all observe the same desired state anyway.
    pub fn enqueue_coalesce(
        &self,
        app_id: impl Into<String>,
        trigger: Trigger,
    ) -> Result<bool, EnqueueError> {
        enqueue_impl(&self.inner, app_id.into(), trigger, true)
    }

    pub fn stats(&self) -> PoolStats {
        stats_snapshot(&self.inner)
    }

    pub fn config(&self) -> &PoolConfig {
        &self.inner.config
    }

    /// Obtain a cheap, clonable producer handle. Hand these to
    /// components (triggers, schedulers) that need to enqueue but
    /// must not own the pool's lifecycle.
    pub fn handle(&self) -> PoolHandle {
        PoolHandle {
            inner: self.inner.clone(),
        }
    }

    /// Close intake and await every currently-running handler.
    /// Queued-but-undispatched jobs are dropped. Consumes the pool.
    pub async fn shutdown(mut self) {
        {
            let mut state = self.inner.state.lock().expect("worker pool mutex poisoned");
            state.shutdown = true;
        }
        // Multiple waiters may be blocked: the dispatcher on
        // notify.notified(), potentially others. notify_waiters
        // wakes everyone currently parked.
        self.inner.notify.notify_waiters();
        if let Some(h) = self.dispatcher.take() {
            if let Err(e) = h.await {
                warn!(error = %e, "worker pool dispatcher task failed during shutdown");
            }
        }
        debug!("worker pool shutdown complete");
    }
}

/// Cloneable producer handle. Enqueues jobs into the pool owned
/// by whoever still holds the [`WorkerPool`] value; shutdown
/// semantics are the owner's responsibility.
#[derive(Clone)]
pub struct PoolHandle {
    inner: Arc<Inner>,
}

impl PoolHandle {
    pub fn enqueue(&self, app_id: impl Into<String>, trigger: Trigger) -> Result<(), EnqueueError> {
        enqueue_impl(&self.inner, app_id.into(), trigger, false).map(|_| ())
    }

    pub fn enqueue_coalesce(
        &self,
        app_id: impl Into<String>,
        trigger: Trigger,
    ) -> Result<bool, EnqueueError> {
        enqueue_impl(&self.inner, app_id.into(), trigger, true)
    }

    pub fn stats(&self) -> PoolStats {
        stats_snapshot(&self.inner)
    }
}

/// Shared implementation for `enqueue` and `enqueue_coalesce`.
///
/// When `coalesce` is `true` and the app already has a pending or
/// in-flight job, returns `Ok(false)` without touching state. An
/// app is considered "already scheduled" iff it's present in the
/// `apps` map — on handler completion an idle app is explicitly
/// removed, so presence in the map is the single source of truth.
fn enqueue_impl(
    inner: &Arc<Inner>,
    app_id: String,
    trigger: Trigger,
    coalesce: bool,
) -> Result<bool, EnqueueError> {
    let mut state = inner.state.lock().expect("worker pool mutex poisoned");
    if state.shutdown {
        return Err(EnqueueError::Shutdown);
    }
    if coalesce && state.apps.contains_key(&app_id) {
        return Ok(false);
    }
    let q = state.apps.entry(app_id.clone()).or_default();
    if q.len() >= inner.config.per_app_queue_cap {
        inner.counters.rejected_full.fetch_add(1, Ordering::Relaxed);
        return Err(EnqueueError::QueueFull {
            app_id,
            cap: inner.config.per_app_queue_cap,
        });
    }
    q.push_back(JobCtx {
        app_id: app_id.clone(),
        trigger,
        enqueued_at: Instant::now(),
    });
    state.queue_depth += 1;
    inner.counters.enqueued.fetch_add(1, Ordering::Relaxed);

    let already_ready = state.ready.iter().any(|a| a == &app_id);
    if !state.in_flight.contains(&app_id) && !already_ready {
        state.ready.push_back(app_id);
    }

    drop(state);
    inner.notify.notify_one();
    Ok(true)
}

fn stats_snapshot(inner: &Arc<Inner>) -> PoolStats {
    let state = inner.state.lock().expect("worker pool mutex poisoned");
    PoolStats {
        enqueued: inner.counters.enqueued.load(Ordering::Relaxed),
        rejected_full: inner.counters.rejected_full.load(Ordering::Relaxed),
        started: inner.counters.started.load(Ordering::Relaxed),
        completed: inner.counters.completed.load(Ordering::Relaxed),
        in_flight: state.in_flight.len(),
        queue_depth: state.queue_depth,
        total_wait_micros: inner.counters.total_wait_micros.load(Ordering::Relaxed),
    }
}

async fn dispatcher_loop(inner: Arc<Inner>) {
    let mut in_flight_tasks: Vec<JoinHandle<()>> = Vec::new();
    loop {
        // Harvest completed spawn handles so the vec doesn't grow
        // unbounded over the pool's lifetime.
        in_flight_tasks.retain(|h| !h.is_finished());

        // Drain everything we can dispatch this tick.
        loop {
            let dispatch = next_dispatch(&inner);
            match dispatch {
                Some(ctx) => {
                    let wait = ctx.enqueued_at.elapsed();
                    record_started(&inner, wait);
                    let h = spawn_handler(inner.clone(), ctx);
                    in_flight_tasks.push(h);
                }
                None => break,
            }
        }

        // Check whether to exit. Hold the lock only long enough to
        // read shutdown; we await outside.
        let shutdown = inner
            .state
            .lock()
            .expect("worker pool mutex poisoned")
            .shutdown;
        if shutdown {
            trace!("dispatcher: shutdown flag set, draining in-flight");
            for h in in_flight_tasks.drain(..) {
                let _ = h.await;
            }
            return;
        }

        inner.notify.notified().await;
    }
}

fn next_dispatch(inner: &Arc<Inner>) -> Option<JobCtx> {
    let mut state = inner.state.lock().expect("worker pool mutex poisoned");
    if state.shutdown {
        return None;
    }
    if state.in_flight.len() >= inner.config.max_concurrent {
        return None;
    }
    let app_id = state.ready.pop_front()?;
    // App might have been drained and queue entry removed (defensive
    // — shouldn't happen with current logic, but cheap to handle).
    let queue = match state.apps.get_mut(&app_id) {
        Some(q) if !q.is_empty() => q,
        _ => return None,
    };
    let ctx = queue.pop_front().expect("queue non-empty checked above");
    state.queue_depth -= 1;
    state.in_flight.insert(app_id);
    Some(ctx)
}

fn record_started(inner: &Arc<Inner>, wait: Duration) {
    inner.counters.started.fetch_add(1, Ordering::Relaxed);
    inner
        .counters
        .total_wait_micros
        .fetch_add(wait.as_micros() as u64, Ordering::Relaxed);
}

fn spawn_handler(inner: Arc<Inner>, ctx: JobCtx) -> JoinHandle<()> {
    let app_id = ctx.app_id.clone();
    let handler = inner.handler.clone();
    tokio::spawn(async move {
        handler(ctx).await;

        {
            let mut state = inner.state.lock().expect("worker pool mutex poisoned");
            state.in_flight.remove(&app_id);
            let has_more = state
                .apps
                .get(&app_id)
                .map(|q| !q.is_empty())
                .unwrap_or(false);
            if has_more {
                state.ready.push_back(app_id);
            } else {
                // Empty queue — drop the entry so idle apps don't
                // leak memory forever.
                state.apps.remove(&app_id);
            }
        }

        inner.counters.completed.fetch_add(1, Ordering::Relaxed);
        inner.notify.notify_one();
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;
    use tokio::time::{sleep, Duration};

    /// Handler that appends `(app_id, sequence_number)` tuples so
    /// tests can assert per-app ordering across interleaved runs.
    fn recording_handler(
        log: Arc<StdMutex<Vec<(String, usize)>>>,
    ) -> impl Fn(JobCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + Clone + 'static
    {
        let counter = Arc::new(AtomicUsize::new(0));
        move |ctx: JobCtx| {
            let log = log.clone();
            let counter = counter.clone();
            Box::pin(async move {
                let seq = counter.fetch_add(1, Ordering::SeqCst);
                log.lock().unwrap().push((ctx.app_id, seq));
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        }
    }

    #[tokio::test]
    async fn per_app_jobs_complete_in_enqueue_order() {
        let log = Arc::new(StdMutex::new(Vec::<(String, usize)>::new()));
        let pool = WorkerPool::new(
            PoolConfig {
                max_concurrent: 16,
                per_app_queue_cap: 32,
            },
            recording_handler(log.clone()),
        );

        for _ in 0..10 {
            pool.enqueue("app-a", Trigger::Manual).unwrap();
            pool.enqueue("app-b", Trigger::Manual).unwrap();
        }

        // Wait for completion.
        for _ in 0..50 {
            if pool.stats().completed == 20 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(pool.stats().completed, 20);

        // Per-app order check: sequence numbers for "app-a" (and
        // for "app-b") must be strictly increasing.
        let (a_seqs, b_seqs): (Vec<usize>, Vec<usize>) = {
            let log = log.lock().unwrap();
            (
                log.iter()
                    .filter(|(k, _)| k == "app-a")
                    .map(|(_, s)| *s)
                    .collect(),
                log.iter()
                    .filter(|(k, _)| k == "app-b")
                    .map(|(_, s)| *s)
                    .collect(),
            )
        };
        assert_eq!(a_seqs.len(), 10);
        assert_eq!(b_seqs.len(), 10);
        assert!(
            a_seqs.windows(2).all(|w| w[0] < w[1]),
            "app-a out of order: {a_seqs:?}"
        );
        assert!(
            b_seqs.windows(2).all(|w| w[0] < w[1]),
            "app-b out of order: {b_seqs:?}"
        );

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn different_apps_run_concurrently() {
        let pool = WorkerPool::new(
            PoolConfig {
                max_concurrent: 4,
                per_app_queue_cap: 4,
            },
            |_ctx| Box::pin(async move { sleep(Duration::from_millis(80)).await }),
        );

        let start = Instant::now();
        for i in 0..4 {
            pool.enqueue(format!("app-{i}"), Trigger::Manual).unwrap();
        }
        // Wait for all to complete.
        for _ in 0..50 {
            if pool.stats().completed == 4 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let elapsed = start.elapsed();
        assert_eq!(pool.stats().completed, 4);
        // Serial would be 4×80ms = 320ms. Parallel should be close
        // to 80ms + overhead. Allow generous headroom to avoid
        // flakiness on loaded CI.
        assert!(
            elapsed < Duration::from_millis(200),
            "expected parallel run, took {elapsed:?}"
        );

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn same_app_jobs_are_serialized() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let pool = {
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            WorkerPool::new(PoolConfig::default(), move |_ctx| {
                let in_flight = in_flight.clone();
                let max_seen = max_seen.clone();
                Box::pin(async move {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);
                    sleep(Duration::from_millis(20)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                })
            })
        };

        for _ in 0..6 {
            pool.enqueue("solo", Trigger::Manual).unwrap();
        }

        for _ in 0..50 {
            if pool.stats().completed == 6 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(pool.stats().completed, 6);
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "per-app serialization violated"
        );

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn global_concurrency_is_bounded() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let pool = {
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            WorkerPool::new(
                PoolConfig {
                    max_concurrent: 2,
                    per_app_queue_cap: 4,
                },
                move |_ctx| {
                    let in_flight = in_flight.clone();
                    let max_seen = max_seen.clone();
                    Box::pin(async move {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(now, Ordering::SeqCst);
                        sleep(Duration::from_millis(30)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    })
                },
            )
        };

        for i in 0..6 {
            pool.enqueue(format!("app-{i}"), Trigger::Manual).unwrap();
        }
        for _ in 0..50 {
            if pool.stats().completed == 6 {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(pool.stats().completed, 6);
        assert!(
            max_seen.load(Ordering::SeqCst) <= 2,
            "global cap violated: {}",
            max_seen.load(Ordering::SeqCst)
        );

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn queue_full_returns_error_and_increments_counter() {
        // Handler blocks on a semaphore so we can stack the queue
        // deterministically. Semaphore (not Notify) so we can wake N
        // handlers reliably — Notify only stores one permit.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = {
            let gate = gate.clone();
            WorkerPool::new(
                PoolConfig {
                    max_concurrent: 1,
                    per_app_queue_cap: 2,
                },
                move |_ctx| {
                    let gate = gate.clone();
                    Box::pin(async move {
                        gate.acquire().await.unwrap().forget();
                    })
                },
            )
        };

        // 1st is dispatched immediately; 2nd and 3rd stack in the
        // per-app queue (cap 2). 4th must be rejected.
        pool.enqueue("solo", Trigger::Manual).unwrap();
        // Let the first actually enter in-flight state.
        for _ in 0..50 {
            if pool.stats().in_flight == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
        pool.enqueue("solo", Trigger::Manual).unwrap();
        pool.enqueue("solo", Trigger::Manual).unwrap();
        let err = pool.enqueue("solo", Trigger::Manual).unwrap_err();
        assert_eq!(
            err,
            EnqueueError::QueueFull {
                app_id: "solo".into(),
                cap: 2,
            }
        );
        assert_eq!(pool.stats().rejected_full, 1);

        // Unblock all three handlers so shutdown can complete.
        gate.add_permits(3);
        // Allow all three pending/in-flight to finish.
        for _ in 0..50 {
            if pool.stats().completed == 3 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_rejects_further_enqueues() {
        let pool = WorkerPool::new(PoolConfig::default(), |_ctx| Box::pin(async move {}));
        // Drive a quick no-op so the dispatcher has spun up before
        // shutdown — avoids racing the first notify.
        pool.enqueue("warmup", Trigger::Manual).unwrap();
        for _ in 0..20 {
            if pool.stats().completed == 1 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }

        let pool_handle = Arc::new(StdMutex::new(Some(pool)));
        let tx = pool_handle.clone();
        let join = tokio::spawn(async move {
            let pool = tx.lock().unwrap().take().unwrap();
            pool.shutdown().await;
        });
        let _ = join.await;

        // We can't enqueue after shutdown() consumed the pool.
        // Instead, exercise the rejection path via a pool whose
        // state is flipped without the consume — use a fresh pool
        // and poke state through the public API.
        let pool = WorkerPool::new(PoolConfig::default(), |_ctx| Box::pin(async move {}));
        // Force the shutdown flag.
        pool.inner.state.lock().unwrap().shutdown = true;
        let err = pool.enqueue("x", Trigger::Manual).unwrap_err();
        assert_eq!(err, EnqueueError::Shutdown);
        // Release the dispatcher task so the test doesn't leak.
        pool.inner.notify.notify_waiters();
        if let Some(h) = {
            let mut p = pool;
            p.dispatcher.take()
        } {
            let _ = h.await;
        }
    }

    #[tokio::test]
    async fn shutdown_awaits_in_flight_handlers() {
        let completed = Arc::new(AtomicUsize::new(0));
        let pool = {
            let completed = completed.clone();
            WorkerPool::new(PoolConfig::default(), move |_ctx| {
                let completed = completed.clone();
                Box::pin(async move {
                    sleep(Duration::from_millis(50)).await;
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            })
        };
        for i in 0..3 {
            pool.enqueue(format!("app-{i}"), Trigger::Manual).unwrap();
        }
        // Give the dispatcher a tick to start the handlers.
        sleep(Duration::from_millis(10)).await;
        pool.shutdown().await;
        // Every in-flight handler had to finish before shutdown
        // returned.
        assert_eq!(completed.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn stats_track_wait_time() {
        let gate = Arc::new(Notify::new());
        let pool = {
            let gate = gate.clone();
            WorkerPool::new(
                PoolConfig {
                    max_concurrent: 1,
                    per_app_queue_cap: 4,
                },
                move |_ctx| {
                    let gate = gate.clone();
                    Box::pin(async move {
                        gate.notified().await;
                    })
                },
            )
        };

        pool.enqueue("a", Trigger::Manual).unwrap();
        pool.enqueue("a", Trigger::Manual).unwrap();
        // Let the first get picked up; the second waits in-queue.
        sleep(Duration::from_millis(40)).await;
        gate.notify_one();
        gate.notify_one();

        for _ in 0..50 {
            if pool.stats().completed == 2 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        let s = pool.stats();
        assert_eq!(s.enqueued, 2);
        assert_eq!(s.started, 2);
        assert_eq!(s.completed, 2);
        // The second job waited at least the 40ms gap. Use a loose
        // lower bound to keep CI stable.
        assert!(
            s.total_wait_micros >= 20_000,
            "expected non-trivial wait, got {} µs",
            s.total_wait_micros
        );

        pool.shutdown().await;
    }

    #[tokio::test]
    async fn interleaved_bursts_preserve_per_app_order() {
        // Deterministic "property" check: a scripted interleaving of
        // enqueues across many apps must still yield per-app FIFO.
        let log = Arc::new(StdMutex::new(Vec::<(String, usize)>::new()));
        let pool = WorkerPool::new(
            PoolConfig {
                max_concurrent: 4,
                per_app_queue_cap: 32,
            },
            recording_handler(log.clone()),
        );

        // Five apps, twenty enqueues each, interleaved.
        let apps = ["a", "b", "c", "d", "e"];
        let per_app = 20;
        for i in 0..per_app {
            for a in &apps {
                pool.enqueue(*a, Trigger::Manual).unwrap();
            }
            if i % 5 == 0 {
                // Yield periodically so some complete mid-enqueue.
                tokio::task::yield_now().await;
            }
        }

        let total = apps.len() * per_app;
        for _ in 0..200 {
            if pool.stats().completed as usize == total {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(pool.stats().completed as usize, total);

        {
            let log = log.lock().unwrap();
            for a in &apps {
                let seqs: Vec<usize> = log
                    .iter()
                    .filter(|(k, _)| k == *a)
                    .map(|(_, s)| *s)
                    .collect();
                assert_eq!(seqs.len(), per_app);
                assert!(
                    seqs.windows(2).all(|w| w[0] < w[1]),
                    "{a} out of order: {seqs:?}"
                );
            }
        }

        pool.shutdown().await;
    }
}
