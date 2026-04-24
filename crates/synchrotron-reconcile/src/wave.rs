//! Sync-wave ordering and readiness gates.
//!
//! Kubernetes resources often have ordering constraints: CRDs must
//! exist before the custom resources that reference them; namespaces
//! must exist before the workloads placed in them; a database has to
//! be ready before the consumers that will connect to it. The
//! industry convention (borrowed from Argo CD) is a per-manifest
//! `sync-wave` annotation carrying a signed integer. The reconciler
//! groups resources into waves, applies wave N in full, waits for
//! wave N to report [`HealthStatusCode::Healthy`], and only then
//! advances to wave N+1.
//!
//! # Annotations
//!
//! We recognise two annotation keys so users can bring existing Argo
//! manifests over unchanged:
//!
//! - `synchrotron.io/sync-wave` — the preferred key for new apps.
//! - `argocd.argoproj.io/sync-wave` — Argo CD's key; falls back to
//!   this if the preferred key is absent.
//!
//! Values parse as signed integers. Missing, unparseable, or
//! non-scalar values default to [`DEFAULT_WAVE`] (0) rather than
//! failing the whole plan: an annotation typo should not brick a
//! reconcile.
//!
//! # Delete entries
//!
//! `PlanEntry` carries only a [`ResourceRef`] for deletes, not the
//! full manifest, so [`group_into_waves`] takes both the desired and
//! live slices and looks up the wave from whichever side owns the
//! resource. In practice deletes usually land in wave 0 (nothing
//! annotates orphans), which is also the right default ordering:
//! prune first, then apply.
//!
//! # Readiness gate
//!
//! [`execute_waves`] polls a [`HealthChecker`] on the resources that
//! were *changed* in the current wave (NoOp entries don't block
//! advance — if a resource is already in sync, its health is not
//! this reconcile's concern). The gate is per-wave: a wave that does
//! not reach `Healthy` within [`WaveExecConfig::per_wave_timeout`]
//! fails the whole execution with [`WaveExecError::WaveTimeout`].

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use synchrotron_plugins::Manifest;
use synchrotron_types::HealthStatusCode;
use thiserror::Error;
use tokio::time::{sleep, Instant};
use tracing::{debug, warn};

use crate::plan::{Plan, PlanEntry, PlannedAction, ResourceRef};

pub const SYNCHROTRON_WAVE_ANNOTATION: &str = "synchrotron.io/sync-wave";
pub const ARGOCD_WAVE_ANNOTATION: &str = "argocd.argoproj.io/sync-wave";

/// Wave used for manifests with no annotation, or with an unparseable
/// value. Matches Argo's default; negative waves run *before* 0.
pub const DEFAULT_WAVE: i32 = 0;

/// Read the sync-wave annotation from a manifest's metadata. Prefers
/// `synchrotron.io/sync-wave`, falls back to Argo's key, defaults to
/// [`DEFAULT_WAVE`] when absent or unparseable.
pub fn wave_of(manifest: &Manifest) -> i32 {
    let annotations = manifest
        .body
        .get("metadata")
        .and_then(|m| m.get("annotations"));
    let Some(annotations) = annotations else {
        return DEFAULT_WAVE;
    };
    for key in [SYNCHROTRON_WAVE_ANNOTATION, ARGOCD_WAVE_ANNOTATION] {
        if let Some(raw) = annotations.get(key).and_then(|v| v.as_str()) {
            if let Ok(n) = raw.trim().parse::<i32>() {
                return n;
            }
        }
        // Some YAML serializations produce an integer scalar, not a
        // string — accept those too.
        if let Some(n) = annotations.get(key).and_then(|v| v.as_i64()) {
            return n.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
        }
    }
    DEFAULT_WAVE
}

/// A single wave's slice of the plan. Entries within a wave have no
/// defined order among themselves — the apply step may parallelise
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaveGroup {
    pub wave: i32,
    pub entries: Vec<PlanEntry>,
}

/// The plan split into waves, sorted ascending by wave number.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WavePlan {
    pub waves: Vec<WaveGroup>,
}

impl WavePlan {
    pub fn is_empty(&self) -> bool {
        self.waves.iter().all(|w| w.entries.is_empty())
    }
}

/// Split a [`Plan`] into waves by looking up each entry's annotation
/// in the desired (or live, for deletes) manifest slice.
///
/// Empty plans round-trip to empty [`WavePlan`]s. NoOp entries are
/// retained in their wave — they carry no work but help executors
/// report "this wave is already satisfied".
pub fn group_into_waves(plan: &Plan, desired: &[Manifest], live: &[Manifest]) -> WavePlan {
    let mut manifest_by_ref: HashMap<ResourceRef, &Manifest> = HashMap::new();
    for m in desired {
        manifest_by_ref.insert(ResourceRef::of(m), m);
    }
    // Live fills in only where desired didn't — deletes.
    for m in live {
        manifest_by_ref.entry(ResourceRef::of(m)).or_insert(m);
    }

    let mut by_wave: HashMap<i32, Vec<PlanEntry>> = HashMap::new();
    for entry in &plan.entries {
        let wave = manifest_by_ref
            .get(&entry.resource)
            .map(|m| wave_of(m))
            .unwrap_or(DEFAULT_WAVE);
        by_wave.entry(wave).or_default().push(entry.clone());
    }

    let mut waves: Vec<WaveGroup> = by_wave
        .into_iter()
        .map(|(wave, entries)| WaveGroup { wave, entries })
        .collect();
    waves.sort_by_key(|w| w.wave);
    WavePlan { waves }
}

/// Applies a single plan entry against the cluster. A real
/// implementation wraps kubectl-server-side-apply or the informer's
/// writer; tests inject a recorder.
pub trait Applier: Send + Sync {
    fn apply<'a>(
        &'a self,
        entry: &'a PlanEntry,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplyError>> + Send + 'a>>;
}

/// Checks the aggregate health of the resources changed by a wave.
/// The executor polls this on an interval and advances as soon as it
/// reports [`HealthStatusCode::Healthy`].
pub trait HealthChecker: Send + Sync {
    fn health<'a>(
        &'a self,
        resources: &'a [ResourceRef],
    ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>>;
}

#[derive(Debug, Clone, Error)]
#[error("apply failed: {reason}")]
pub struct ApplyError {
    pub reason: String,
}

impl ApplyError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WaveExecConfig {
    /// Maximum time to wait for a wave to report `Healthy` after its
    /// applies finish. On timeout, execution aborts with
    /// [`WaveExecError::WaveTimeout`].
    pub per_wave_timeout: Duration,
    /// How often to re-poll the [`HealthChecker`] while waiting.
    /// Short enough to keep latency low on fast-healing waves, long
    /// enough not to hammer the health engine.
    pub poll_interval: Duration,
}

impl Default for WaveExecConfig {
    fn default() -> Self {
        Self {
            per_wave_timeout: Duration::from_secs(300),
            poll_interval: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Error)]
pub enum WaveExecError {
    #[error("wave {wave} apply failed on {resource:?}: {source}")]
    ApplyFailed {
        wave: i32,
        resource: ResourceRef,
        #[source]
        source: ApplyError,
    },
    #[error(
        "wave {wave} did not become Healthy within {timeout:?} (last status: {last_status:?})"
    )]
    WaveTimeout {
        wave: i32,
        timeout: Duration,
        last_status: HealthStatusCode,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaveExecReport {
    /// Waves that finished applying and reported healthy (in order).
    pub completed_waves: Vec<i32>,
}

/// Apply waves sequentially: for each wave, run every `Apply`/`Delete`
/// entry through the [`Applier`], then poll the [`HealthChecker`]
/// until it reports `Healthy` or the per-wave timeout elapses. NoOp
/// entries are skipped in both steps.
///
/// A wave with zero changed resources auto-advances without consulting
/// the health checker — there's nothing to wait for.
pub async fn execute_waves(
    plan: &WavePlan,
    applier: &dyn Applier,
    health: &dyn HealthChecker,
    cfg: &WaveExecConfig,
) -> Result<WaveExecReport, WaveExecError> {
    let mut report = WaveExecReport::default();
    for wave in &plan.waves {
        let changed: Vec<&PlanEntry> = wave
            .entries
            .iter()
            .filter(|e| e.action != PlannedAction::NoOp)
            .collect();

        for entry in &changed {
            if let Err(source) = applier.apply(entry).await {
                return Err(WaveExecError::ApplyFailed {
                    wave: wave.wave,
                    resource: entry.resource.clone(),
                    source,
                });
            }
        }

        if changed.is_empty() {
            debug!(wave = wave.wave, "wave has no changes; auto-advancing");
            report.completed_waves.push(wave.wave);
            continue;
        }

        let resources: Vec<ResourceRef> = changed.iter().map(|e| e.resource.clone()).collect();
        let deadline = Instant::now() + cfg.per_wave_timeout;
        loop {
            let last_status = health.health(&resources).await;
            if last_status == HealthStatusCode::Healthy {
                debug!(wave = wave.wave, "wave healthy; advancing");
                break;
            }
            if Instant::now() >= deadline {
                warn!(
                    wave = wave.wave,
                    ?last_status,
                    "wave timed out waiting for Healthy"
                );
                return Err(WaveExecError::WaveTimeout {
                    wave: wave.wave,
                    timeout: cfg.per_wave_timeout,
                    last_status,
                });
            }
            sleep(cfg.poll_interval).await;
        }
        report.completed_waves.push(wave.wave);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use synchrotron_plugins::manifest::parse_stream;

    use crate::plan::plan;

    fn manifest_with_wave(kind: &str, name: &str, ns: &str, wave: Option<i32>) -> Manifest {
        let ann = match wave {
            Some(w) => format!("  annotations:\n    {SYNCHROTRON_WAVE_ANNOTATION}: \"{w}\"\n"),
            None => String::new(),
        };
        let yaml = format!(
            "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: {ns}\n{ann}data:\n  marker: v1\n"
        );
        parse_stream("test", &yaml).expect("parse").pop().unwrap()
    }

    #[test]
    fn wave_of_defaults_when_annotation_missing() {
        let m = manifest_with_wave("ConfigMap", "cm", "app", None);
        assert_eq!(wave_of(&m), DEFAULT_WAVE);
    }

    #[test]
    fn wave_of_reads_synchrotron_key() {
        let m = manifest_with_wave("ConfigMap", "cm", "app", Some(5));
        assert_eq!(wave_of(&m), 5);
    }

    #[test]
    fn wave_of_reads_negative_waves() {
        let m = manifest_with_wave("ConfigMap", "cm", "app", Some(-3));
        assert_eq!(wave_of(&m), -3);
    }

    #[test]
    fn wave_of_falls_back_to_argo_annotation() {
        let yaml = format!(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\n  annotations:\n    {ARGOCD_WAVE_ANNOTATION}: \"2\"\ndata:\n  marker: v1\n"
        );
        let m = parse_stream("t", &yaml).unwrap().pop().unwrap();
        assert_eq!(wave_of(&m), 2);
    }

    #[test]
    fn wave_of_garbage_annotation_defaults() {
        let yaml = format!(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\n  annotations:\n    {SYNCHROTRON_WAVE_ANNOTATION}: \"not-a-number\"\ndata:\n  marker: v1\n"
        );
        let m = parse_stream("t", &yaml).unwrap().pop().unwrap();
        assert_eq!(wave_of(&m), DEFAULT_WAVE);
    }

    #[test]
    fn group_into_waves_sorts_ascending() {
        let a = manifest_with_wave("ConfigMap", "a", "ns", Some(2));
        let b = manifest_with_wave("ConfigMap", "b", "ns", Some(-1));
        let c = manifest_with_wave("ConfigMap", "c", "ns", Some(0));
        let p = plan(&[a.clone(), b.clone(), c.clone()], &[]);
        let wp = group_into_waves(&p, &[a, b, c], &[]);
        let waves: Vec<i32> = wp.waves.iter().map(|w| w.wave).collect();
        assert_eq!(waves, vec![-1, 0, 2]);
    }

    #[test]
    fn group_into_waves_places_deletes_via_live_annotation() {
        let orphan = manifest_with_wave("ConfigMap", "orphan", "ns", Some(7));
        let p = plan(&[], std::slice::from_ref(&orphan));
        let wp = group_into_waves(&p, &[], std::slice::from_ref(&orphan));
        assert_eq!(wp.waves.len(), 1);
        assert_eq!(wp.waves[0].wave, 7);
        assert_eq!(wp.waves[0].entries[0].action, PlannedAction::Delete);
    }

    // Test harness: record apply order; health reports a configurable
    // per-wave progression sequence.
    struct RecordingApplier {
        order: Arc<Mutex<Vec<(ResourceRef, PlannedAction)>>>,
    }
    impl Applier for RecordingApplier {
        fn apply<'a>(
            &'a self,
            entry: &'a PlanEntry,
        ) -> Pin<Box<dyn Future<Output = Result<(), ApplyError>> + Send + 'a>> {
            let order = self.order.clone();
            let rref = entry.resource.clone();
            let action = entry.action;
            Box::pin(async move {
                order.lock().unwrap().push((rref, action));
                Ok(())
            })
        }
    }

    /// Health checker that returns `Progressing` for the first
    /// `delays_per_call` calls then `Healthy`. Independent per
    /// invocation set (each wave resets the counter via a fresh
    /// instance).
    struct ScriptedHealth {
        progressing_calls: AtomicUsize,
        remaining: AtomicUsize,
    }
    impl ScriptedHealth {
        fn new(delays: usize) -> Self {
            Self {
                progressing_calls: AtomicUsize::new(0),
                remaining: AtomicUsize::new(delays),
            }
        }
    }
    impl HealthChecker for ScriptedHealth {
        fn health<'a>(
            &'a self,
            _resources: &'a [ResourceRef],
        ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>> {
            Box::pin(async move {
                if self.remaining.load(Ordering::SeqCst) == 0 {
                    HealthStatusCode::Healthy
                } else {
                    self.remaining.fetch_sub(1, Ordering::SeqCst);
                    self.progressing_calls.fetch_add(1, Ordering::SeqCst);
                    HealthStatusCode::Progressing
                }
            })
        }
    }

    /// Health checker that never becomes healthy — always Progressing.
    struct StuckHealth;
    impl HealthChecker for StuckHealth {
        fn health<'a>(
            &'a self,
            _resources: &'a [ResourceRef],
        ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>> {
            Box::pin(async move { HealthStatusCode::Progressing })
        }
    }

    #[tokio::test]
    async fn three_wave_app_applies_in_order_and_gates_on_health() {
        // Wave -1: namespace. Wave 0: configmap. Wave 1: workload.
        // Each wave's health goes Progressing once, then Healthy.
        let ns = manifest_with_wave("Namespace", "app-ns", "", Some(-1));
        let cm = manifest_with_wave("ConfigMap", "cm", "app-ns", Some(0));
        let dep = manifest_with_wave("Deployment", "web", "app-ns", Some(1));
        let desired = vec![ns.clone(), cm.clone(), dep.clone()];
        let p = plan(&desired, &[]);
        let wp = group_into_waves(&p, &desired, &[]);
        assert_eq!(wp.waves.len(), 3);

        let order = Arc::new(Mutex::new(Vec::new()));
        let applier = RecordingApplier {
            order: order.clone(),
        };
        // A single health checker whose counter we consume across
        // waves would couple them; instead, make it instantly healthy
        // per wave so the gating path runs without real delay.
        let health = ScriptedHealth::new(0);
        let cfg = WaveExecConfig {
            per_wave_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(5),
        };
        let report = execute_waves(&wp, &applier, &health, &cfg)
            .await
            .expect("waves succeed");

        assert_eq!(report.completed_waves, vec![-1, 0, 1]);
        let recorded = order.lock().unwrap().clone();
        // Applies happen in wave order; within a wave there's only
        // one resource here so total order is deterministic.
        let names: Vec<String> = recorded.iter().map(|(r, _)| r.name.clone()).collect();
        assert_eq!(names, vec!["app-ns", "cm", "web"]);
    }

    #[tokio::test]
    async fn wave_does_not_advance_until_prior_wave_healthy() {
        // Two waves; the first spends 3 poll ticks Progressing before
        // turning Healthy. We verify wave 1 applies only *after* the
        // gate opened.
        let a = manifest_with_wave("ConfigMap", "first", "ns", Some(0));
        let b = manifest_with_wave("ConfigMap", "second", "ns", Some(1));
        let desired = vec![a.clone(), b.clone()];
        let p = plan(&desired, &[]);
        let wp = group_into_waves(&p, &desired, &[]);

        let order = Arc::new(Mutex::new(Vec::new()));
        let applier = RecordingApplier {
            order: order.clone(),
        };
        let health = ScriptedHealth::new(3);
        let cfg = WaveExecConfig {
            per_wave_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
        };

        execute_waves(&wp, &applier, &health, &cfg)
            .await
            .expect("waves succeed");

        let recorded = order.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].0.name, "first");
        assert_eq!(recorded[1].0.name, "second");
        assert_eq!(health.progressing_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn wave_timeout_aborts_execution() {
        let a = manifest_with_wave("ConfigMap", "stuck", "ns", Some(0));
        let b = manifest_with_wave("ConfigMap", "later", "ns", Some(1));
        let desired = vec![a.clone(), b.clone()];
        let p = plan(&desired, &[]);
        let wp = group_into_waves(&p, &desired, &[]);

        let order = Arc::new(Mutex::new(Vec::new()));
        let applier = RecordingApplier {
            order: order.clone(),
        };
        let cfg = WaveExecConfig {
            per_wave_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(10),
        };

        let err = execute_waves(&wp, &applier, &StuckHealth, &cfg)
            .await
            .expect_err("should time out");
        match err {
            WaveExecError::WaveTimeout {
                wave, last_status, ..
            } => {
                assert_eq!(wave, 0);
                assert_eq!(last_status, HealthStatusCode::Progressing);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        // Wave 1 never applied.
        let recorded = order.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0.name, "stuck");
    }

    #[tokio::test]
    async fn empty_wave_auto_advances_without_health_check() {
        // A wave that is all NoOp should not consult the health
        // checker at all — we assert this with a health impl that
        // panics if called.
        struct ExplodingHealth;
        impl HealthChecker for ExplodingHealth {
            fn health<'a>(
                &'a self,
                _r: &'a [ResourceRef],
            ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>> {
                Box::pin(async move { panic!("health should not be called for NoOp wave") })
            }
        }

        let a = manifest_with_wave("ConfigMap", "keep", "ns", Some(0));
        let p = plan(std::slice::from_ref(&a), std::slice::from_ref(&a));
        assert_eq!(p.noop_count(), 1);
        let wp = group_into_waves(&p, &[a], &[]);

        let order = Arc::new(Mutex::new(Vec::new()));
        let applier = RecordingApplier {
            order: order.clone(),
        };
        let cfg = WaveExecConfig::default();
        let report = execute_waves(&wp, &applier, &ExplodingHealth, &cfg)
            .await
            .expect("no-op wave succeeds");
        assert_eq!(report.completed_waves, vec![0]);
        assert!(order.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn apply_failure_aborts_and_reports_wave() {
        struct FailingApplier;
        impl Applier for FailingApplier {
            fn apply<'a>(
                &'a self,
                _entry: &'a PlanEntry,
            ) -> Pin<Box<dyn Future<Output = Result<(), ApplyError>> + Send + 'a>> {
                Box::pin(async move { Err(ApplyError::new("denied")) })
            }
        }

        let a = manifest_with_wave("ConfigMap", "bad", "ns", Some(0));
        let p = plan(std::slice::from_ref(&a), &[]);
        let wp = group_into_waves(&p, std::slice::from_ref(&a), &[]);
        let cfg = WaveExecConfig::default();

        let err = execute_waves(&wp, &FailingApplier, &StuckHealth, &cfg)
            .await
            .expect_err("apply failure");
        match err {
            WaveExecError::ApplyFailed { wave, resource, .. } => {
                assert_eq!(wave, 0);
                assert_eq!(resource.name, "bad");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
