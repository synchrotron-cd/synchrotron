//! App-level aggregation and readiness-gate hook.
//!
//! Given a set of manifests describing an app's live state, produce
//! a single [`AppHealth`] by running [`assess`] per manifest and
//! taking the *worst* resulting status across the set. The ordering
//! comes from [`HealthStatusCode`] itself, whose derived `Ord`
//! sequences the codes from best to worst
//! (`Healthy < Progressing < Suspended < Unknown < Missing <
//! Degraded`), so `max()` picks the right one.
//!
//! # Readiness gate
//!
//! [`AppHealthChecker`] wraps a [`ManifestStore`] (an async fetcher
//! from the informer cache, test fixture, etc.) and implements
//! [`synchrotron_reconcile::HealthChecker`]. That's the seam the
//! wave executor in `synchrotron-reconcile` uses to advance waves:
//! each poll, it asks "are these resources healthy yet?" — we
//! resolve their live manifests, aggregate, and return the code.
//!
//! # Published event
//!
//! [`publish_app_health`] writes a [`SystemEvent::AppHealthAssessed`]
//! to the shared bus so status APIs and notification sinks can react
//! to health transitions without polling. Aggregation itself is pure
//! — publishing is a separate step so tests and dry-run tooling can
//! aggregate without a bus.

use std::future::Future;
use std::pin::Pin;

use synchrotron_core::events::{EventBus, SystemEvent};
use synchrotron_plugins::Manifest;
use synchrotron_reconcile::{HealthChecker, ResourceRef};
use synchrotron_types::{AppName, ClusterName, HealthStatusCode};

use crate::{assess, HealthAssessment};

/// Per-resource health entry. Retained alongside the aggregate so
/// callers (status APIs, CLI) can explain "why is this app
/// Degraded?" by pointing at the specific resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceHealth {
    pub resource: ResourceRef,
    pub assessment: HealthAssessment,
}

/// Aggregate health for an app.
///
/// `status` is the worst status across `resources`. `message` is the
/// message from whichever resource drove the aggregate (so users see
/// the specific reason, not a generic "Degraded"). An empty resource
/// list aggregates to [`HealthStatusCode::Healthy`] with no message:
/// no resources means nothing can be unhealthy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppHealth {
    pub status: HealthStatusCode,
    pub message: Option<String>,
    pub resources: Vec<ResourceHealth>,
}

/// Aggregate a slice of live manifests into an [`AppHealth`].
///
/// Deterministic: order-independent because aggregation reduces via
/// `max`, and ties broken by the first-seen worst resource only
/// affect the *message* field — the `status` is stable.
pub fn aggregate(manifests: &[Manifest]) -> AppHealth {
    let resources: Vec<ResourceHealth> = manifests
        .iter()
        .map(|m| ResourceHealth {
            resource: ResourceRef::of(m),
            assessment: assess(m),
        })
        .collect();

    let (status, message) = resources
        .iter()
        .map(|r| (r.assessment.status, r.assessment.message.clone()))
        .max_by_key(|(s, _)| *s)
        .unwrap_or((HealthStatusCode::Healthy, None));

    AppHealth {
        status,
        message,
        resources,
    }
}

/// Fetches live manifests for a set of resource refs. The concrete
/// implementation in production wraps the informer cache; tests
/// inject fixtures.
pub trait ManifestStore: Send + Sync {
    fn fetch<'a>(
        &'a self,
        resources: &'a [ResourceRef],
    ) -> Pin<Box<dyn Future<Output = Vec<Manifest>> + Send + 'a>>;
}

/// [`HealthChecker`] adapter that resolves refs → manifests, runs
/// [`aggregate`], and returns the aggregate status. Drop-in for
/// [`synchrotron_reconcile::execute_waves`].
pub struct AppHealthChecker<S: ManifestStore> {
    store: S,
}

impl<S: ManifestStore> AppHealthChecker<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Run aggregation once for the given resources. Useful outside
    /// the wave-executor flow — e.g. a status API asking for
    /// current app health on demand.
    pub async fn assess_app(&self, resources: &[ResourceRef]) -> AppHealth {
        let manifests = self.store.fetch(resources).await;
        aggregate(&manifests)
    }
}

impl<S: ManifestStore> HealthChecker for AppHealthChecker<S> {
    fn health<'a>(
        &'a self,
        resources: &'a [ResourceRef],
    ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>> {
        Box::pin(async move { self.assess_app(resources).await.status })
    }
}

/// Publish an [`AppHealth`] result to the event bus as
/// [`SystemEvent::AppHealthAssessed`]. The bus is
/// [`broadcast`](tokio::sync::broadcast)-backed so this is non-blocking;
/// a lagging consumer is its own problem.
pub fn publish_app_health(bus: &EventBus, app: AppName, cluster: ClusterName, health: &AppHealth) {
    bus.publish(SystemEvent::AppHealthAssessed {
        app,
        cluster,
        status: health.status,
        message: health.message.clone(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use synchrotron_plugins::manifest::parse_stream;

    fn parse(yaml: &str) -> Manifest {
        parse_stream("t", yaml).expect("parse").pop().unwrap()
    }

    fn healthy_deployment(name: &str) -> Manifest {
        parse(&format!(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {name}\n  generation: 1\nspec:\n  replicas: 1\nstatus:\n  observedGeneration: 1\n  replicas: 1\n  updatedReplicas: 1\n  availableReplicas: 1\n"
        ))
    }

    fn progressing_deployment(name: &str) -> Manifest {
        parse(&format!(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: {name}\n  generation: 2\nspec:\n  replicas: 3\nstatus:\n  observedGeneration: 2\n  replicas: 3\n  updatedReplicas: 1\n  availableReplicas: 1\n"
        ))
    }

    fn degraded_pod(name: &str) -> Manifest {
        parse(&format!(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\nstatus:\n  phase: Failed\n"
        ))
    }

    fn bound_pvc(name: &str) -> Manifest {
        parse(&format!(
            "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: {name}\nstatus:\n  phase: Bound\n"
        ))
    }

    fn ready_crd_instance(name: &str) -> Manifest {
        // Tier-2 kind (unknown to tier-1) reporting Ready=True.
        parse(&format!(
            "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: {name}\nstatus:\n  conditions:\n    - type: Ready\n      status: \"True\"\n"
        ))
    }

    #[test]
    fn empty_manifests_aggregate_to_healthy() {
        let h = aggregate(&[]);
        assert_eq!(h.status, HealthStatusCode::Healthy);
        assert!(h.resources.is_empty());
    }

    #[test]
    fn all_healthy_aggregates_to_healthy() {
        let manifests = vec![
            healthy_deployment("a"),
            bound_pvc("data"),
            ready_crd_instance("w"),
        ];
        let h = aggregate(&manifests);
        assert_eq!(h.status, HealthStatusCode::Healthy);
        assert_eq!(h.resources.len(), 3);
    }

    #[test]
    fn worst_status_wins_over_healthy_siblings() {
        // Healthy + Progressing + Degraded → Degraded. The message
        // must come from the degraded resource.
        let manifests = vec![
            healthy_deployment("ok"),
            progressing_deployment("rolling"),
            degraded_pod("crashed"),
        ];
        let h = aggregate(&manifests);
        assert_eq!(h.status, HealthStatusCode::Degraded);
        assert_eq!(h.message.as_deref(), Some("pod failed"));
    }

    #[test]
    fn progressing_beats_healthy_when_no_degradation() {
        let manifests = vec![healthy_deployment("a"), progressing_deployment("b")];
        let h = aggregate(&manifests);
        assert_eq!(h.status, HealthStatusCode::Progressing);
    }

    #[test]
    fn aggregation_is_order_independent() {
        let a = healthy_deployment("a");
        let b = progressing_deployment("b");
        let c = degraded_pod("c");
        let h1 = aggregate(&[a.clone(), b.clone(), c.clone()]);
        let h2 = aggregate(&[c, b, a]);
        assert_eq!(h1.status, h2.status);
        assert_eq!(h1.message, h2.message);
    }

    #[test]
    fn aggregation_spans_tier1_and_tier2_kinds() {
        // Mixing a tier-1 kind (Pod) with a tier-2 CRD. Both report
        // Healthy; aggregate is Healthy.
        let manifests = vec![
            parse("apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nstatus:\n  phase: Succeeded\n"),
            ready_crd_instance("w"),
        ];
        let h = aggregate(&manifests);
        assert_eq!(h.status, HealthStatusCode::Healthy);
    }

    // ---- HealthChecker adapter ---------------------------------

    struct FixtureStore {
        manifests: Mutex<Vec<Manifest>>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl FixtureStore {
        fn new(manifests: Vec<Manifest>) -> Self {
            Self {
                manifests: Mutex::new(manifests),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn set(&self, manifests: Vec<Manifest>) {
            *self.manifests.lock().unwrap() = manifests;
        }
    }
    impl ManifestStore for FixtureStore {
        fn fetch<'a>(
            &'a self,
            _resources: &'a [ResourceRef],
        ) -> Pin<Box<dyn Future<Output = Vec<Manifest>> + Send + 'a>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let manifests = self.manifests.lock().unwrap().clone();
            Box::pin(async move { manifests })
        }
    }

    #[tokio::test]
    async fn health_checker_returns_aggregate_status() {
        let store = FixtureStore::new(vec![healthy_deployment("a"), progressing_deployment("b")]);
        let checker = AppHealthChecker::new(store);
        let refs: Vec<ResourceRef> = Vec::new();
        let status = checker.health(&refs).await;
        assert_eq!(status, HealthStatusCode::Progressing);
    }

    #[tokio::test]
    async fn health_checker_transitions_as_store_updates() {
        // Simulates the wave executor's poll loop: first call the
        // app is rolling out, second call it's healthy. The checker
        // must return the fresh status each time.
        let store = FixtureStore::new(vec![progressing_deployment("rolling")]);
        let checker = AppHealthChecker::new(store);
        let refs: Vec<ResourceRef> = Vec::new();
        assert_eq!(checker.health(&refs).await, HealthStatusCode::Progressing);
        checker.store.set(vec![healthy_deployment("rolling")]);
        assert_eq!(checker.health(&refs).await, HealthStatusCode::Healthy);
    }

    #[tokio::test]
    async fn publish_app_health_emits_event_on_bus() {
        let bus = EventBus::new(8);
        let mut rx = bus.subscribe();
        let health = AppHealth {
            status: HealthStatusCode::Degraded,
            message: Some("pod failed".to_string()),
            resources: Vec::new(),
        };
        publish_app_health(
            &bus,
            AppName("my-app".into()),
            ClusterName("prod".into()),
            &health,
        );
        let evt = rx.recv().await.expect("receive");
        match evt.event {
            SystemEvent::AppHealthAssessed {
                app,
                cluster,
                status,
                message,
            } => {
                assert_eq!(app.0, "my-app");
                assert_eq!(cluster.0, "prod");
                assert_eq!(status, HealthStatusCode::Degraded);
                assert_eq!(message.as_deref(), Some("pod failed"));
            }
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[tokio::test]
    async fn integration_multi_kind_app_assessment() {
        // Three-kind app: a Deployment, a Service (LoadBalancer), a
        // PVC. While the LB address is still pending, the app is
        // Progressing. Once the LB gets an IP, everything's Healthy.
        let svc_pending = parse(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: api\nspec:\n  type: LoadBalancer\nstatus:\n  loadBalancer: {}\n",
        );
        let svc_ready = parse(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: api\nspec:\n  type: LoadBalancer\nstatus:\n  loadBalancer:\n    ingress:\n      - ip: 10.0.0.5\n",
        );
        let store = FixtureStore::new(vec![
            healthy_deployment("api"),
            svc_pending,
            bound_pvc("data"),
        ]);
        let checker = AppHealthChecker::new(store);
        let refs: Vec<ResourceRef> = Vec::new();
        assert_eq!(checker.health(&refs).await, HealthStatusCode::Progressing);

        checker.store.set(vec![
            healthy_deployment("api"),
            svc_ready,
            bound_pvc("data"),
        ]);
        assert_eq!(checker.health(&refs).await, HealthStatusCode::Healthy);
    }
}
