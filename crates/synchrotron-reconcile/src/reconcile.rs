//! Per-app reconcile function.
//!
//! Two entry points share the same fetch+plan core:
//!
//! - [`Reconciler::reconcile_app`] (sync, plan-only): fetches
//!   desired + live, calls [`plan`](crate::plan::plan), publishes a
//!   `SyncOutcome` event. Used by the diff API and the criterion
//!   benches where we only want to measure the planner cost.
//! - [`Reconciler::reconcile_and_apply_app`] (async, plan + apply):
//!   does the above, then if a [`ReconcileExecutor`] is attached
//!   (via [`Reconciler::with_executor`]), groups the plan into waves
//!   and drives [`execute_waves`] against the cluster's [`Applier`]
//!   and [`HealthChecker`]. This is the production path.
//!
//! `DesiredSource` and `LiveSource` are sync because in production
//! they read in-memory caches; wrapping a blocking I/O source with a
//! background task is the caller's problem. The apply path is async
//! because the kube `Applier` does real network I/O.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use synchrotron_core::events::{EventBus, SystemEvent};
use synchrotron_core::metrics::Metrics;
use synchrotron_core::telemetry::reconcile_span;
use synchrotron_plugins::Manifest;
use synchrotron_types::{AppName, ClusterName, HealthStatusCode};
use thiserror::Error;
use tracing::{debug, warn};

use crate::plan::{plan, Plan, ResourceRef};
use crate::wave::{
    execute_waves, group_into_waves, Applier, HealthChecker, WaveExecConfig, WaveExecError,
    WaveExecReport,
};

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
///
/// `apply` is `None` for the plan-only path
/// ([`Reconciler::reconcile_app`]) or when the planner failed before
/// executing. When the apply path runs ([`Reconciler::reconcile_and_apply_app`])
/// it carries either the [`WaveExecReport`] from a successful run
/// or the [`WaveExecError`] that aborted it. `error` is *not* set
/// for an apply failure — `apply` is the source of truth there, and
/// `success()` consults both.
#[derive(Debug, Clone)]
pub struct ReconcileOutcome {
    pub app: AppName,
    pub cluster: ClusterName,
    pub plan: Option<Plan>,
    pub error: Option<ReconcileError>,
    pub apply: Option<Result<WaveExecReport, WaveExecError>>,
}

impl ReconcileOutcome {
    pub fn success(&self) -> bool {
        self.error.is_none() && !matches!(self.apply, Some(Err(_)))
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

/// Optional execution side: an [`Applier`] (e.g. kube SSA) plus a
/// [`HealthChecker`] (e.g. informer-backed status reader), with the
/// per-wave config that drives [`execute_waves`]. Reconciler holds
/// this behind an `Option` so the planner-only path stays usable for
/// benchmarks and the diff API.
pub struct ReconcileExecutor {
    pub applier: Arc<dyn Applier>,
    pub health: Arc<dyn HealthChecker>,
    pub config: WaveExecConfig,
}

/// Trivial [`HealthChecker`] that reports `Healthy` for any input.
/// Used as a default before slice 2 (oes) lands the real
/// informer-backed health reader. Safe-but-eager: every wave
/// auto-advances the moment its applies finish, so a still-rolling
/// Deployment won't block the next wave. That's fine for the
/// happy-path bring-up; production deployments should swap this
/// out.
pub struct AlwaysHealthy;

impl HealthChecker for AlwaysHealthy {
    fn health<'a>(
        &'a self,
        _resources: &'a [ResourceRef],
    ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>> {
        Box::pin(async { HealthStatusCode::Healthy })
    }
}

pub struct Reconciler {
    desired: Arc<dyn DesiredSource>,
    live: Arc<dyn LiveSource>,
    bus: EventBus,
    metrics: Option<Arc<Metrics>>,
    executor: Option<Arc<ReconcileExecutor>>,
}

impl Reconciler {
    pub fn new(desired: Arc<dyn DesiredSource>, live: Arc<dyn LiveSource>, bus: EventBus) -> Self {
        Self {
            desired,
            live,
            bus,
            metrics: None,
            executor: None,
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

    /// Attach an apply-side executor. Without this, only
    /// [`Self::reconcile_app`] (plan-only) is meaningful;
    /// [`Self::reconcile_and_apply_app`] degrades to plan-only when
    /// no executor is configured.
    pub fn with_executor(mut self, executor: ReconcileExecutor) -> Self {
        self.executor = Some(Arc::new(executor));
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
            apply: None,
        }
    }

    /// Plan + execute. Runs the same planning step as
    /// [`Self::reconcile_app`], then (if an [`Applier`] /
    /// [`HealthChecker`] are attached via [`Self::with_executor`])
    /// groups the plan into waves and drives [`execute_waves`]
    /// against the cluster.
    ///
    /// If no executor is attached this degrades to plan-only — the
    /// returned [`ReconcileOutcome`] has `apply: None` and matches
    /// what [`Self::reconcile_app`] would have produced. That keeps
    /// the call site uniform whether or not the apply path is
    /// configured (the diff API and benches both rely on this).
    ///
    /// On apply failure, the planning side still succeeds (the plan
    /// in `outcome.plan` is the one we tried to execute); the apply
    /// failure surfaces in `outcome.apply = Some(Err(_))` and
    /// `outcome.success()` returns false.
    pub async fn reconcile_and_apply_app(
        &self,
        app: &AppName,
        cluster: &ClusterName,
    ) -> ReconcileOutcome {
        let mut outcome = self.reconcile_app(app, cluster);
        let Some(plan_ref) = &outcome.plan else {
            return outcome; // planning already failed
        };
        let Some(executor) = self.executor.clone() else {
            return outcome; // plan-only mode
        };

        // Re-fetch sources to group into waves. Both reads hit the
        // in-memory caches behind `Arc<[Manifest]>`, so this is a
        // refcount bump per call, not a deep clone — see y0v.3 /
        // d2p notes for the rationale.
        let desired = match self.desired.desired(app) {
            Ok(d) => d,
            Err(e) => {
                outcome.apply = Some(Err(WaveExecError::ApplyFailed {
                    wave: 0,
                    resource: ResourceRef::default(),
                    source: crate::wave::ApplyError::new(format!(
                        "could not refetch desired manifests for wave grouping: {e}"
                    )),
                }));
                return outcome;
            }
        };
        let live = match self.live.live(app, cluster) {
            Ok(l) => l,
            Err(e) => {
                outcome.apply = Some(Err(WaveExecError::ApplyFailed {
                    wave: 0,
                    resource: ResourceRef::default(),
                    source: crate::wave::ApplyError::new(format!(
                        "could not refetch live manifests for wave grouping: {e}"
                    )),
                }));
                return outcome;
            }
        };

        let wave_plan = group_into_waves(plan_ref, &desired, &live);
        let result = execute_waves(
            &wave_plan,
            executor.applier.as_ref(),
            executor.health.as_ref(),
            &executor.config,
        )
        .await;

        if let Err(err) = &result {
            warn!(%app, %cluster, ?err, "wave execution failed");
        } else {
            debug!(
                %app, %cluster,
                waves = wave_plan.waves.len(),
                "wave execution complete"
            );
        }
        outcome.apply = Some(result);
        outcome
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
            apply: None,
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

    // ---- apply-side tests (slice 1 of d2p — wire pipeline) ----

    use crate::wave::{ApplyError, WaveExecConfig};
    use std::pin::Pin;
    use synchrotron_types::HealthStatusCode;

    /// Records every apply call so tests can assert what got applied.
    #[derive(Default)]
    struct RecordingApplier {
        calls: Mutex<Vec<crate::plan::ResourceRef>>,
        fail_on: Mutex<Option<crate::plan::ResourceRef>>,
    }
    impl RecordingApplier {
        fn fail_on(self, r: crate::plan::ResourceRef) -> Self {
            *self.fail_on.lock().unwrap() = Some(r);
            self
        }
    }
    impl crate::wave::Applier for RecordingApplier {
        fn apply<'a>(
            &'a self,
            entry: &'a crate::plan::PlanEntry,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), ApplyError>> + Send + 'a>>
        {
            let resource = entry.resource.clone();
            self.calls.lock().unwrap().push(resource.clone());
            let fail = self.fail_on.lock().unwrap().clone();
            Box::pin(async move {
                if fail.as_ref() == Some(&resource) {
                    Err(ApplyError::new("simulated"))
                } else {
                    Ok(())
                }
            })
        }
    }

    fn executor_with(applier: Arc<RecordingApplier>) -> ReconcileExecutor {
        ReconcileExecutor {
            applier,
            health: Arc::new(AlwaysHealthy),
            // Tight config so timeouts in misbehaving tests fail
            // fast instead of stalling the suite.
            config: WaveExecConfig {
                per_wave_timeout: std::time::Duration::from_secs(1),
                poll_interval: std::time::Duration::from_millis(10),
            },
        }
    }

    #[tokio::test]
    async fn reconcile_and_apply_runs_applier_for_each_change() {
        let (desired_src, live_src, mut reconciler, _rx) = setup();
        // Two desired CMs, no live → both planned as Apply.
        desired_src.set(
            "app-a",
            Ok(vec![
                manifest("ConfigMap", "cm-1", Some("default"), "v1"),
                manifest("ConfigMap", "cm-2", Some("default"), "v1"),
            ]),
        );
        live_src.set("app-a", "prod", Ok(vec![]));

        let applier = Arc::new(RecordingApplier::default());
        reconciler = reconciler.with_executor(executor_with(applier.clone()));

        let out = reconciler
            .reconcile_and_apply_app(&AppName("app-a".into()), &ClusterName("prod".into()))
            .await;

        assert!(out.success(), "outcome should succeed: {out:?}");
        assert!(matches!(out.apply, Some(Ok(_))));
        let calls = applier.calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "both applies should have run");
    }

    #[tokio::test]
    async fn reconcile_and_apply_surfaces_apply_failure() {
        let (desired_src, live_src, mut reconciler, _rx) = setup();
        desired_src.set(
            "app-a",
            Ok(vec![manifest("ConfigMap", "cm-1", Some("default"), "v1")]),
        );
        live_src.set("app-a", "prod", Ok(vec![]));

        let target = crate::plan::ResourceRef {
            gvk: synchrotron_plugins::Gvk::parse("v1", "ConfigMap"),
            namespace: Some("default".into()),
            name: "cm-1".into(),
        };
        let applier = Arc::new(RecordingApplier::default().fail_on(target));
        reconciler = reconciler.with_executor(executor_with(applier));

        let out = reconciler
            .reconcile_and_apply_app(&AppName("app-a".into()), &ClusterName("prod".into()))
            .await;

        assert!(
            !out.success(),
            "apply failure should make outcome unsuccessful"
        );
        assert!(matches!(out.apply, Some(Err(_))));
        assert!(
            out.plan.is_some(),
            "plan still produced, apply is what failed"
        );
    }

    #[tokio::test]
    async fn reconcile_and_apply_with_no_executor_is_plan_only() {
        let (desired_src, live_src, reconciler, _rx) = setup();
        desired_src.set(
            "app-a",
            Ok(vec![manifest("ConfigMap", "cm-1", Some("default"), "v1")]),
        );
        live_src.set("app-a", "prod", Ok(vec![]));

        let out = reconciler
            .reconcile_and_apply_app(&AppName("app-a".into()), &ClusterName("prod".into()))
            .await;

        assert!(out.success());
        assert!(out.apply.is_none(), "no executor → no apply run");
        assert!(out.plan.is_some());
    }

    #[tokio::test]
    async fn always_healthy_reports_healthy() {
        let h = AlwaysHealthy;
        let status = h.health(&[]).await;
        assert_eq!(status, HealthStatusCode::Healthy);
    }
}
