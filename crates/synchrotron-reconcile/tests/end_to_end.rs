//! End-to-end wiring test for the reconciliation engine.
//!
//! Stitches together the per-app worker pool, the event-driven
//! trigger, the per-app reconciler, the auto-heal scheduler, and the
//! event bus. Asserts the two halves of the h48.4 acceptance
//! criterion:
//!
//! 1. **Event-driven path:** a `WebhookTriggered` event reaches the
//!    per-app worker and produces a `SyncOutcome` on the event bus.
//! 2. **Auto-heal path:** the periodic tick reconciles registered
//!    apps even with no upstream event, producing its own
//!    `SyncOutcome`.
//!
//! Production wiring substitutes the in-memory stubs here for the
//! real desired-state cache, kube live source, and DB-backed app
//! lister, but the topology is identical.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use synchrotron_core::events::{EventBus, SystemEvent, WebhookSource};
use synchrotron_plugins::Manifest;
use synchrotron_reconcile::{
    AppLister, AppResolver, AutoHealConfig, AutoHealScheduler, DesiredSource, EventTrigger,
    LiveSource, PoolConfig, Reconciler, SourceError, Trigger, WorkerPool,
};
use synchrotron_types::{AppName, ClusterName};
use tokio::time::{sleep, timeout};

fn manifest(kind: &str, name: &str, ns: &str, marker: &str) -> Manifest {
    let yaml = format!(
        "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: {ns}\ndata:\n  marker: {marker}\n",
    );
    synchrotron_plugins::manifest::parse_stream("e2e", &yaml)
        .expect("parse manifest")
        .pop()
        .expect("one manifest")
}

#[derive(Default)]
struct StaticDesired {
    by_app: Mutex<HashMap<String, Arc<[Manifest]>>>,
}
impl StaticDesired {
    fn set(&self, app: &str, m: Vec<Manifest>) {
        self.by_app.lock().unwrap().insert(app.into(), m.into());
    }
}
impl DesiredSource for StaticDesired {
    fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
        self.by_app
            .lock()
            .unwrap()
            .get(&app.0)
            .cloned()
            .ok_or(SourceError::NotFound)
    }
}

type LiveMap = HashMap<(String, String), Arc<[Manifest]>>;

#[derive(Default)]
struct StaticLive {
    by_key: Mutex<LiveMap>,
}
impl StaticLive {
    fn set(&self, app: &str, cluster: &str, m: Vec<Manifest>) {
        self.by_key
            .lock()
            .unwrap()
            .insert((app.into(), cluster.into()), m.into());
    }
}
impl LiveSource for StaticLive {
    fn live(&self, app: &AppName, cluster: &ClusterName) -> Result<Arc<[Manifest]>, SourceError> {
        self.by_key
            .lock()
            .unwrap()
            .get(&(app.0.clone(), cluster.0.clone()))
            .cloned()
            .ok_or(SourceError::NotFound)
    }
}

struct StaticResolver {
    by_repo: HashMap<String, Vec<AppName>>,
}
impl AppResolver for StaticResolver {
    fn apps_for_repo(&self, repo: &str) -> Vec<AppName> {
        self.by_repo.get(repo).cloned().unwrap_or_default()
    }
}

struct StaticLister(Vec<AppName>);
impl AppLister for StaticLister {
    fn all(&self) -> Vec<AppName> {
        self.0.clone()
    }
}

/// Wait for `pred` to become true, polling every 5ms up to `max_ms`.
async fn wait_for<F: Fn() -> bool>(pred: F, max_ms: u64) -> bool {
    for _ in 0..(max_ms / 5) {
        if pred() {
            return true;
        }
        sleep(Duration::from_millis(5)).await;
    }
    pred()
}

#[tokio::test]
async fn webhook_event_drives_reconcile_and_emits_sync_outcome() {
    let cluster = ClusterName("prod".into());
    let app = AppName("payments".into());

    let desired = Arc::new(StaticDesired::default());
    let live = Arc::new(StaticLive::default());
    let m = manifest("ConfigMap", "cfg", "payments", "v1");
    desired.set(&app.0, vec![m.clone()]);
    live.set(&app.0, &cluster.0, vec![m]);

    let bus = EventBus::new(64);
    let reconciler = Arc::new(Reconciler::new(desired.clone(), live.clone(), bus.clone()));

    // Subscribe BEFORE publishing so we don't race the trigger.
    let mut rx = bus.subscribe();

    let trigger_log = Arc::new(Mutex::new(Vec::<Trigger>::new()));
    let pool = {
        let reconciler = reconciler.clone();
        let cluster = cluster.clone();
        let trigger_log = trigger_log.clone();
        WorkerPool::new(PoolConfig::default(), move |ctx| {
            let reconciler = reconciler.clone();
            let cluster = cluster.clone();
            let trigger_log = trigger_log.clone();
            Box::pin(async move {
                trigger_log.lock().unwrap().push(ctx.trigger);
                reconciler.reconcile_app(&AppName(ctx.app_id), &cluster, ctx.trigger.as_str());
            })
        })
    };

    let resolver = Arc::new(StaticResolver {
        by_repo: HashMap::from([("repo-1".into(), vec![app.clone()])]),
    });
    let trigger = EventTrigger::spawn(&bus, pool.handle(), resolver);

    bus.publish(SystemEvent::WebhookTriggered {
        repo: "repo-1".into(),
        source: WebhookSource::GitHub,
    });

    // Drain the bus until we see the SyncOutcome for our app — there
    // will be a WebhookTriggered echo first.
    let outcome = timeout(Duration::from_secs(2), async {
        loop {
            let evt = rx.recv().await.expect("bus open");
            if let SystemEvent::SyncOutcome {
                app: a,
                cluster: c,
                success,
                ..
            } = evt.event
            {
                return (a, c, success);
            }
        }
    })
    .await
    .expect("SyncOutcome arrives within 2s");

    assert_eq!(outcome.0, app);
    assert_eq!(outcome.1, cluster);
    assert!(outcome.2, "no-drift reconcile should succeed");

    // The worker recorded the webhook trigger tag.
    assert!(wait_for(|| !trigger_log.lock().unwrap().is_empty(), 500).await);
    assert_eq!(trigger_log.lock().unwrap()[0], Trigger::Webhook);

    trigger.stop().await;
    pool.shutdown().await;
}

#[tokio::test]
async fn auto_heal_tick_reconciles_apps_with_no_upstream_event() {
    let cluster = ClusterName("prod".into());
    let app = AppName("orders".into());

    let desired = Arc::new(StaticDesired::default());
    let live = Arc::new(StaticLive::default());
    let m = manifest("ConfigMap", "cfg", "orders", "v1");
    desired.set(&app.0, vec![m.clone()]);
    live.set(&app.0, &cluster.0, vec![m]);

    let bus = EventBus::new(64);
    let reconciler = Arc::new(Reconciler::new(desired.clone(), live.clone(), bus.clone()));
    let mut rx = bus.subscribe();

    let trigger_log = Arc::new(Mutex::new(Vec::<Trigger>::new()));
    let pool = {
        let reconciler = reconciler.clone();
        let cluster = cluster.clone();
        let trigger_log = trigger_log.clone();
        WorkerPool::new(PoolConfig::default(), move |ctx| {
            let reconciler = reconciler.clone();
            let cluster = cluster.clone();
            let trigger_log = trigger_log.clone();
            Box::pin(async move {
                trigger_log.lock().unwrap().push(ctx.trigger);
                reconciler.reconcile_app(&AppName(ctx.app_id), &cluster, ctx.trigger.as_str());
            })
        })
    };

    // Short interval so the tick fires within the test window. No
    // jitter spread so the enqueue lands deterministically.
    let lister = Arc::new(StaticLister(vec![app.clone()]));
    let scheduler = AutoHealScheduler::spawn(
        pool.handle(),
        lister,
        AutoHealConfig {
            interval: Duration::from_millis(100),
            jitter_fraction: 0.0,
        },
    );

    let outcome = timeout(Duration::from_secs(2), async {
        loop {
            let evt = rx.recv().await.expect("bus open");
            if let SystemEvent::SyncOutcome {
                app: a, success, ..
            } = evt.event
            {
                return (a, success);
            }
        }
    })
    .await
    .expect("auto-heal SyncOutcome arrives within 2s");

    assert_eq!(outcome.0, app);
    assert!(outcome.1);

    assert!(wait_for(|| !trigger_log.lock().unwrap().is_empty(), 500).await);
    assert_eq!(
        trigger_log.lock().unwrap()[0],
        Trigger::AutoHeal,
        "auto-heal scheduler must tag the job as AutoHeal"
    );
    assert!(scheduler.stats().enqueued >= 1);

    scheduler.stop().await;
    pool.shutdown().await;
}
