//! Event-driven reconcile trigger.
//!
//! Subscribes to the shared [`EventBus`](synchrotron_core::events::EventBus)
//! and translates upstream events into coalesced enqueues on a
//! [`PoolHandle`]:
//!
//! - `RepoChanged` (poller saw new HEAD) → enqueue every app that
//!   references the repo, tagged as [`Trigger::Poll`].
//! - `WebhookTriggered` (inbound webhook) → same, tagged as
//!   [`Trigger::Webhook`].
//! - Other events (`RepoUnchanged`, `RepoFetchFailed`,
//!   `SyncOutcome`) are ignored — they don't imply desired-state
//!   drift.
//!
//! An [`AppResolver`] does the repo-to-apps lookup. Keeping it a
//! trait lets the trigger stay agnostic to how apps are indexed
//! (db query, in-memory map, CRD cache), and lets tests inject a
//! fixed mapping.
//!
//! Coalescing is delegated to
//! [`PoolHandle::enqueue_coalesce`]: if the app already has a
//! pending or in-flight job, the event is dropped. This is the
//! right behavior because every pending reconcile will, when it
//! runs, observe the latest desired state anyway — enqueuing
//! duplicates just fills the per-app queue with work that would
//! noop. Rapid-event *debouncing* (wait a bit before dispatching)
//! is a separate concern landing in h48.4.5.

use std::sync::Arc;

use synchrotron_core::events::{EventBus, EventReceiver, SystemEvent};
use synchrotron_types::AppName;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::worker_pool::{PoolHandle, Trigger};

/// Maps upstream event payloads to the set of apps that should be
/// reconciled in response. Implementations must be cheap: the
/// trigger calls these on the hot path of the event loop.
pub trait AppResolver: Send + Sync {
    /// Apps that reference the given repo (by repo-id string form).
    fn apps_for_repo(&self, repo: &str) -> Vec<AppName>;
}

/// Background task that drains the event bus and enqueues reconciles.
///
/// Dropping the handle aborts the task. The task also exits on its
/// own when the bus's sender is dropped (i.e. when the final
/// `EventBus` clone goes away); the trigger logs and returns in
/// that case.
pub struct EventTrigger {
    task: JoinHandle<()>,
}

impl EventTrigger {
    /// Spawn the trigger task. The returned handle owns the task's
    /// lifecycle; drop it to cancel.
    pub fn spawn(bus: &EventBus, pool: PoolHandle, resolver: Arc<dyn AppResolver>) -> Self {
        let rx = bus.subscribe();
        let task = tokio::spawn(run_loop(rx, pool, resolver));
        Self { task }
    }

    /// Abort the trigger task and await its completion. Useful in
    /// tests; in production, dropping the handle is sufficient.
    pub async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn run_loop(mut rx: EventReceiver, pool: PoolHandle, resolver: Arc<dyn AppResolver>) {
    loop {
        match rx.recv().await {
            Ok(evt) => dispatch(&evt.event, &pool, resolver.as_ref()),
            Err(_) => {
                debug!("event bus closed; trigger exiting");
                return;
            }
        }
    }
}

fn dispatch(event: &SystemEvent, pool: &PoolHandle, resolver: &dyn AppResolver) {
    let (apps, trigger) = match event {
        SystemEvent::RepoChanged { repo, .. } => (resolver.apps_for_repo(repo), Trigger::Poll),
        SystemEvent::WebhookTriggered { repo, .. } => {
            (resolver.apps_for_repo(repo), Trigger::Webhook)
        }
        // Operator-initiated requests target a specific app and skip
        // the repo→apps resolver entirely.
        SystemEvent::ManualSyncRequested { app } => (vec![app.clone()], Trigger::Manual),
        // Events that don't imply drift. Logging at trace level
        // would be noisy; these are silently ignored.
        SystemEvent::RepoUnchanged { .. }
        | SystemEvent::RepoFetchFailed { .. }
        | SystemEvent::SyncOutcome { .. }
        | SystemEvent::AppHealthAssessed { .. }
        // AppChanged seeds the render pipeline; the render loop
        // publishes ManualSyncRequested once desired state lands,
        // which is what actually enqueues the reconcile.
        | SystemEvent::AppChanged { .. } => return,
    };

    for app in apps {
        match pool.enqueue_coalesce(app.0.clone(), trigger) {
            Ok(true) => debug!(%app, ?trigger, "reconcile enqueued"),
            Ok(false) => debug!(%app, ?trigger, "reconcile coalesced (already pending)"),
            Err(e) => warn!(%app, ?trigger, error = %e, "reconcile enqueue failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use synchrotron_core::events::{EventBus, WebhookSource};
    use tokio::time::sleep;

    use crate::worker_pool::{PoolConfig, WorkerPool};

    struct StaticResolver {
        by_repo: HashMap<String, Vec<AppName>>,
    }
    impl AppResolver for StaticResolver {
        fn apps_for_repo(&self, repo: &str) -> Vec<AppName> {
            self.by_repo.get(repo).cloned().unwrap_or_default()
        }
    }

    /// Handler that records each job's (app_id, trigger) tuple so
    /// the test can assert which apps got reconciled and why.
    fn recording_pool(
        log: Arc<Mutex<Vec<(String, Trigger)>>>,
        gate: Arc<tokio::sync::Semaphore>,
    ) -> WorkerPool {
        WorkerPool::new(PoolConfig::default(), move |ctx| {
            let log = log.clone();
            let gate = gate.clone();
            Box::pin(async move {
                log.lock().unwrap().push((ctx.app_id, ctx.trigger));
                // Block until the test releases us, so we can
                // deterministically observe the pending state.
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
            sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    #[tokio::test]
    async fn repo_change_enqueues_all_mapped_apps() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = recording_pool(log.clone(), gate.clone());
        let bus = EventBus::new(16);

        let resolver = Arc::new(StaticResolver {
            by_repo: HashMap::from([(
                "repo-1".to_string(),
                vec![AppName("app-a".into()), AppName("app-b".into())],
            )]),
        });
        let trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);

        bus.publish(SystemEvent::RepoChanged {
            repo: "repo-1".into(),
            new_head: "abc123".into(),
        });

        // Two handlers should be started (one per app). They block
        // on the gate, so in_flight reports the right number.
        assert!(wait_for(|| pool.stats().in_flight == 2, 500).await);

        gate.add_permits(2);
        assert!(wait_for(|| pool.stats().completed == 2, 500).await);

        let recorded = log.lock().unwrap().clone();
        let mut apps: Vec<_> = recorded.iter().map(|(a, _)| a.clone()).collect();
        apps.sort();
        assert_eq!(apps, vec!["app-a".to_string(), "app-b".to_string()]);
        assert!(recorded.iter().all(|(_, t)| *t == Trigger::Poll));

        trigger.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn webhook_event_tags_trigger_as_webhook() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = recording_pool(log.clone(), gate.clone());
        let bus = EventBus::new(16);

        let resolver = Arc::new(StaticResolver {
            by_repo: HashMap::from([("repo-x".to_string(), vec![AppName("app-x".into())])]),
        });
        let trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);

        bus.publish(SystemEvent::WebhookTriggered {
            repo: "repo-x".into(),
            source: WebhookSource::GitHub,
        });

        assert!(wait_for(|| pool.stats().in_flight == 1, 500).await);
        gate.add_permits(1);
        assert!(wait_for(|| pool.stats().completed == 1, 500).await);

        let recorded = log.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].1, Trigger::Webhook);

        trigger.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn rapid_bursts_are_coalesced_per_app() {
        // Handler holds a permit; only one job at a time for the
        // same app can run (the pool enforces that anyway). We
        // publish 5 RepoChanged events back-to-back — the trigger
        // should enqueue only once while the first is in flight.
        let run_count = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = {
            let run_count = run_count.clone();
            let gate = gate.clone();
            WorkerPool::new(PoolConfig::default(), move |_ctx| {
                let run_count = run_count.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    gate.acquire().await.unwrap().forget();
                })
            })
        };
        let bus = EventBus::new(32);
        let resolver = Arc::new(StaticResolver {
            by_repo: HashMap::from([("repo".into(), vec![AppName("app".into())])]),
        });
        let trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);

        for i in 0..5 {
            bus.publish(SystemEvent::RepoChanged {
                repo: "repo".into(),
                new_head: format!("hash-{i}"),
            });
        }

        // Wait for the first handler to enter in-flight, then
        // give the trigger task a window to process the remaining
        // events. Since coalesce skips while pending, they should
        // all be dropped.
        assert!(wait_for(|| pool.stats().in_flight == 1, 500).await);
        sleep(Duration::from_millis(50)).await;

        // Exactly one job is running, nothing queued, and no
        // subsequent events were enqueued while it was pending.
        let s = pool.stats();
        assert_eq!(s.in_flight, 1);
        assert_eq!(s.queue_depth, 0);
        assert_eq!(s.enqueued, 1);

        gate.add_permits(1);
        assert!(wait_for(|| pool.stats().completed == 1, 500).await);
        assert_eq!(run_count.load(Ordering::SeqCst), 1);

        trigger.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn unrelated_events_do_not_enqueue() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = recording_pool(log.clone(), gate);
        let bus = EventBus::new(16);
        let resolver = Arc::new(StaticResolver {
            by_repo: HashMap::new(),
        });
        let trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);

        bus.publish(SystemEvent::RepoUnchanged {
            repo: "r".into(),
            head: "h".into(),
        });
        bus.publish(SystemEvent::RepoFetchFailed {
            repo: "r".into(),
            error: "boom".into(),
        });
        bus.publish(SystemEvent::SyncOutcome {
            app: AppName("a".into()),
            cluster: synchrotron_types::ClusterName("c".into()),
            success: true,
            message: None,
        });

        // Give the trigger a moment to drain.
        sleep(Duration::from_millis(30)).await;
        assert_eq!(pool.stats().enqueued, 0);

        trigger.stop().await;
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn repo_with_no_mapped_apps_is_noop() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let pool = recording_pool(log.clone(), gate);
        let bus = EventBus::new(16);
        let resolver = Arc::new(StaticResolver {
            by_repo: HashMap::new(),
        });
        let trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);

        bus.publish(SystemEvent::RepoChanged {
            repo: "unknown".into(),
            new_head: "h".into(),
        });
        sleep(Duration::from_millis(30)).await;
        assert_eq!(pool.stats().enqueued, 0);

        trigger.stop().await;
        pool.shutdown().await;
    }
}
