//! Per-app reconcile function.
//!
//! Composes the pure planner (see [`crate::plan`]) with desired /
//! live state sources and the event bus. The reconcile function:
//!
//! 1. Fetches the desired manifests for the app from a
//!    [`DesiredSource`] (backed by the app cache in production).
//! 2. Fetches the live state from a [`LiveSource`] (backed by the
//!    kube informer cache in production).
//! 3. Calls [`plan`](crate::plan::plan) to compute planned actions.
//! 4. Publishes a [`SystemEvent::SyncOutcome`] on the event bus.
//!
//! Actually executing the plan (kubectl apply / delete) lands in a
//! later slice. This slice ships the end-to-end plumbing so the
//! downstream slices can focus on the apply side in isolation.
//!
//! Both `DesiredSource` and `LiveSource` are intentionally
//! synchronous: in production they read in-memory caches, and a
//! sync trait keeps the call sites simple. Wrapping a blocking I/O
//! source with a background task is the caller's problem.

use std::sync::Arc;
use std::time::Instant;

use synchrotron_core::events::{EventBus, SystemEvent};
use synchrotron_core::metrics::Metrics;
use synchrotron_core::telemetry::reconcile_span;
use synchrotron_plugins::Manifest;
use synchrotron_types::{AppName, ClusterName};
use thiserror::Error;
use tracing::{debug, warn};

use crate::plan::{plan, Plan};

/// Reason a source couldn't satisfy a lookup.
///
/// `NotFound` is deliberately distinct from `Unavailable` so the
/// reconciler can report a clear sync-outcome message: a missing app
/// is a user-facing configuration error, an unavailable source is
/// an operational one.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SourceError {
    #[error("not found")]
    NotFound,
    #[error("source unavailable: {0}")]
    Unavailable(String),
}

pub trait DesiredSource: Send + Sync {
    /// Returns the per-app manifest set as a refcounted slice. The
    /// reconciler only reads this — never mutates — so handing back
    /// an `Arc<[Manifest]>` (cheap clone, shared backing buffer)
    /// avoids deep-copying the per-app manifest set on every call.
    /// At 10k apps × 25 manifests this matters a lot for steady-state
    /// memory; see y0v.3 baseline.
    fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError>;
}

pub trait LiveSource: Send + Sync {
    /// See [`DesiredSource::desired`]; same shared-buffer rationale.
    fn live(&self, app: &AppName, cluster: &ClusterName) -> Result<Arc<[Manifest]>, SourceError>;
}

/// Outcome of a single reconcile pass.
///
/// On success `plan` is populated and `error` is `None`. On failure
/// the opposite. This struct is what the caller (e.g. the worker
/// pool handler) inspects; the `SyncOutcome` event is the
/// observability side-channel.
#[derive(Debug, Clone)]
pub struct ReconcileOutcome {
    pub app: AppName,
    pub cluster: ClusterName,
    pub plan: Option<Plan>,
    pub error: Option<ReconcileError>,
}

impl ReconcileOutcome {
    pub fn success(&self) -> bool {
        self.error.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReconcileError {
    #[error("app `{0}` not found in desired-state cache")]
    AppNotFound(AppName),
    #[error("cluster `{0}` not available")]
    ClusterNotFound(ClusterName),
    #[error("failed to fetch desired manifests for `{app}`: {source}")]
    DesiredFetch { app: AppName, source: SourceError },
    #[error("failed to fetch live state for `{app}` on `{cluster}`: {source}")]
    LiveFetch {
        app: AppName,
        cluster: ClusterName,
        source: SourceError,
    },
}

pub struct Reconciler {
    desired: Arc<dyn DesiredSource>,
    live: Arc<dyn LiveSource>,
    bus: EventBus,
    metrics: Option<Arc<Metrics>>,
}

impl Reconciler {
    pub fn new(desired: Arc<dyn DesiredSource>, live: Arc<dyn LiveSource>, bus: EventBus) -> Self {
        Self {
            desired,
            live,
            bus,
            metrics: None,
        }
    }

    /// Attach a metrics registry. Once set, every reconcile pass
    /// records `synchrotron_reconcile_total`,
    /// `synchrotron_reconcile_duration_seconds`, and (on success)
    /// `synchrotron_plan_changes`.
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Run one reconcile pass for `app` against `cluster`.
    ///
    /// Always publishes a `SyncOutcome` event, whether the pass
    /// succeeded or failed. Returns the structured outcome so the
    /// caller can decide whether to retry, back off, or update
    /// per-app status.
    pub fn reconcile_app(&self, app: &AppName, cluster: &ClusterName) -> ReconcileOutcome {
        let _enter = reconcile_span(&app.0, &cluster.0).entered();
        let started = Instant::now();
        let desired = match self.desired.desired(app) {
            Ok(d) => d,
            Err(SourceError::NotFound) => {
                return self.emit_failure(app, cluster, ReconcileError::AppNotFound(app.clone()));
            }
            Err(e) => {
                return self.emit_failure(
                    app,
                    cluster,
                    ReconcileError::DesiredFetch {
                        app: app.clone(),
                        source: e,
                    },
                );
            }
        };
        let live = match self.live.live(app, cluster) {
            Ok(l) => l,
            Err(SourceError::NotFound) => {
                return self.emit_failure(
                    app,
                    cluster,
                    ReconcileError::ClusterNotFound(cluster.clone()),
                );
            }
            Err(e) => {
                return self.emit_failure(
                    app,
                    cluster,
                    ReconcileError::LiveFetch {
                        app: app.clone(),
                        cluster: cluster.clone(),
                        source: e,
                    },
                );
            }
        };

        let plan = plan(&desired, &live);
        debug!(
            %app, %cluster,
            applies = plan.apply_count(),
            deletes = plan.delete_count(),
            noops = plan.noop_count(),
            "reconcile plan produced"
        );
        if let Some(m) = &self.metrics {
            m.record_reconcile(&app.0, &cluster.0, true, started.elapsed());
            m.record_plan_changes(&app.0, &cluster.0, plan.changes());
        }
        self.bus.publish(SystemEvent::SyncOutcome {
            app: app.clone(),
            cluster: cluster.clone(),
            success: true,
            message: None,
        });
        ReconcileOutcome {
            app: app.clone(),
            cluster: cluster.clone(),
            plan: Some(plan),
            error: None,
        }
    }

    fn emit_failure(
        &self,
        app: &AppName,
        cluster: &ClusterName,
        error: ReconcileError,
    ) -> ReconcileOutcome {
        warn!(%app, %cluster, %error, "reconcile failed");
        if let Some(m) = &self.metrics {
            // Failure path doesn't have access to `started`, so the
            // duration is recorded at the call site that owns it.
            // For now, count failures with a zero duration —
            // operators care about the rate, not the latency, of
            // failures (failures usually short-circuit before doing
            // meaningful work).
            m.record_reconcile(&app.0, &cluster.0, false, std::time::Duration::ZERO);
        }
        self.bus.publish(SystemEvent::SyncOutcome {
            app: app.clone(),
            cluster: cluster.clone(),
            success: false,
            message: Some(error.to_string()),
        });
        ReconcileOutcome {
            app: app.clone(),
            cluster: cluster.clone(),
            plan: None,
            error: Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use synchrotron_core::events::{BusEvent, EventReceiver};

    fn manifest(kind: &str, name: &str, ns: Option<&str>, marker: &str) -> Manifest {
        let yaml = format!(
            "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n{ns_line}data:\n  marker: {marker}\n",
            ns_line = ns
                .map(|n| format!("  namespace: {n}\n"))
                .unwrap_or_default(),
        );
        synchrotron_plugins::manifest::parse_stream("test", &yaml)
            .expect("parse")
            .pop()
            .expect("one manifest")
    }

    type DesiredMap = HashMap<String, Result<Arc<[Manifest]>, SourceError>>;

    #[derive(Default)]
    struct StubDesired {
        by_app: Mutex<DesiredMap>,
    }
    impl StubDesired {
        fn set(&self, app: &str, v: Result<Vec<Manifest>, SourceError>) {
            self.by_app
                .lock()
                .unwrap()
                .insert(app.to_string(), v.map(Arc::from));
        }
    }
    impl DesiredSource for StubDesired {
        fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
            self.by_app
                .lock()
                .unwrap()
                .get(&app.0)
                .cloned()
                .unwrap_or(Err(SourceError::NotFound))
        }
    }

    type LiveMap = HashMap<(String, String), Result<Arc<[Manifest]>, SourceError>>;

    #[derive(Default)]
    struct StubLive {
        by_key: Mutex<LiveMap>,
    }
    impl StubLive {
        fn set(&self, app: &str, cluster: &str, v: Result<Vec<Manifest>, SourceError>) {
            self.by_key
                .lock()
                .unwrap()
                .insert((app.to_string(), cluster.to_string()), v.map(Arc::from));
        }
    }
    impl LiveSource for StubLive {
        fn live(
            &self,
            app: &AppName,
            cluster: &ClusterName,
        ) -> Result<Arc<[Manifest]>, SourceError> {
            self.by_key
                .lock()
                .unwrap()
                .get(&(app.0.clone(), cluster.0.clone()))
                .cloned()
                .unwrap_or(Err(SourceError::NotFound))
        }
    }

    fn try_drain(rx: &mut EventReceiver) -> Vec<BusEvent> {
        let mut out = Vec::new();
        while let Some(e) = rx.try_recv() {
            out.push(e);
        }
        out
    }

    fn setup() -> (Arc<StubDesired>, Arc<StubLive>, Reconciler, EventReceiver) {
        let desired = Arc::new(StubDesired::default());
        let live = Arc::new(StubLive::default());
        let bus = EventBus::new(16);
        let rx = bus.subscribe();
        let reconciler = Reconciler::new(desired.clone(), live.clone(), bus);
        (desired, live, reconciler, rx)
    }

    #[test]
    fn reconcile_no_drift_publishes_success_and_all_noop() {
        let (desired, live, reconciler, mut rx) = setup();
        let m = manifest("ConfigMap", "cm", Some("app"), "v1");
        desired.set("app-a", Ok(vec![m.clone()]));
        live.set("app-a", "prod", Ok(vec![m]));

        let out = reconciler.reconcile_app(&AppName("app-a".into()), &ClusterName("prod".into()));
        assert!(out.success());
        let p = out.plan.expect("plan");
        assert_eq!(p.noop_count(), 1);
        assert_eq!(p.changes(), 0);

        let events = try_drain(&mut rx);
        assert_eq!(events.len(), 1);
        match &events[0].event {
            SystemEvent::SyncOutcome { success, .. } => assert!(*success),
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn reconcile_drift_produces_apply_and_delete() {
        let (desired, live, reconciler, _rx) = setup();
        let update_d = manifest("ConfigMap", "update", Some("app"), "v2");
        let update_l = manifest("ConfigMap", "update", Some("app"), "v1");
        let new = manifest("ConfigMap", "new", Some("app"), "v1");
        let orphan = manifest("ConfigMap", "orphan", Some("app"), "v1");
        desired.set("app-a", Ok(vec![update_d, new]));
        live.set("app-a", "prod", Ok(vec![update_l, orphan]));

        let out = reconciler.reconcile_app(&AppName("app-a".into()), &ClusterName("prod".into()));
        assert!(out.success());
        let p = out.plan.unwrap();
        assert_eq!(p.apply_count(), 2);
        assert_eq!(p.delete_count(), 1);
    }

    #[test]
    fn reconcile_missing_app_publishes_failure() {
        let (_desired, live, reconciler, mut rx) = setup();
        // No desired entry → NotFound.
        live.set("ghost", "prod", Ok(vec![]));

        let out = reconciler.reconcile_app(&AppName("ghost".into()), &ClusterName("prod".into()));
        assert!(!out.success());
        assert!(matches!(out.error, Some(ReconcileError::AppNotFound(_))));
        assert!(out.plan.is_none());

        let events = try_drain(&mut rx);
        assert_eq!(events.len(), 1);
        match &events[0].event {
            SystemEvent::SyncOutcome {
                success, message, ..
            } => {
                assert!(!*success);
                assert!(message.as_ref().unwrap().contains("not found"));
            }
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn reconcile_missing_cluster_publishes_failure() {
        let (desired, _live, reconciler, mut rx) = setup();
        desired.set("app-a", Ok(vec![]));
        // No live entry → NotFound.

        let out = reconciler.reconcile_app(&AppName("app-a".into()), &ClusterName("dead".into()));
        assert!(!out.success());
        assert!(matches!(
            out.error,
            Some(ReconcileError::ClusterNotFound(_))
        ));

        let events = try_drain(&mut rx);
        assert_eq!(events.len(), 1);
        match &events[0].event {
            SystemEvent::SyncOutcome { success, .. } => assert!(!success),
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn reconcile_desired_fetch_failure_propagates() {
        let (desired, live, reconciler, mut rx) = setup();
        desired.set("app-a", Err(SourceError::Unavailable("cache cold".into())));
        live.set("app-a", "prod", Ok(vec![]));

        let out = reconciler.reconcile_app(&AppName("app-a".into()), &ClusterName("prod".into()));
        assert!(!out.success());
        match out.error {
            Some(ReconcileError::DesiredFetch { source, .. }) => {
                assert_eq!(source, SourceError::Unavailable("cache cold".into()));
            }
            other => panic!("unexpected error {other:?}"),
        }
        assert_eq!(try_drain(&mut rx).len(), 1);
    }

    #[test]
    fn reconcile_live_fetch_failure_propagates() {
        let (desired, live, reconciler, mut rx) = setup();
        desired.set("app-a", Ok(vec![]));
        live.set(
            "app-a",
            "prod",
            Err(SourceError::Unavailable("apiserver 503".into())),
        );

        let out = reconciler.reconcile_app(&AppName("app-a".into()), &ClusterName("prod".into()));
        assert!(!out.success());
        assert!(matches!(out.error, Some(ReconcileError::LiveFetch { .. })));
        assert_eq!(try_drain(&mut rx).len(), 1);
    }
}
