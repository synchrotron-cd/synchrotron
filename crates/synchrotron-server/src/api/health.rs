//! Liveness, readiness, and a richer health summary.
//!
//! - `/healthz` (alias `/livez`) is the kubelet liveness probe:
//!   trivially "the listener is up". If this 200s, axum is serving.
//! - `/readyz` is the kubelet readiness probe. Body is JSON listing
//!   each registered component's state; HTTP status is `200` iff
//!   every component is `up`, otherwise `503`.
//! - `/api/v1/health` is the cheap top-level ping used by the CLI
//!   and dashboards. Always `200` once axum is up; subsystem-level
//!   detail lives in `/readyz`.
//!
//! ## Registering a component
//!
//! Subsystems clone the [`HealthRegistry`] on startup and call
//! [`HealthRegistry::report`] whenever their state transitions. The
//! built-in `"startup"` component starts `Down` and is flipped to
//! `Up` by [`ReadinessGate::mark_ready`] once the binary has finished
//! bootstrapping (DB opened, listener bound). Other components
//! (`db`, `reconciler`, cluster connectors) report their own state.
//!
//! ## Component states
//!
//! `up` is healthy. `degraded` is "still serving traffic but
//! something's off" (e.g. one cluster connector down out of three) —
//! /readyz returns 503 for `degraded` so kube routes around the pod
//! while the operator investigates. `down` is "not serving" — same
//! 503, just labelled differently.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use utoipa::ToSchema;

/// Component-level state. Stringly-typed on the wire (snake_case)
/// so the CLI and dashboards can switch on stable values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case", tag = "state", content = "message")]
pub enum ComponentState {
    Up,
    Degraded(String),
    Down(String),
}

impl ComponentState {
    pub fn is_up(&self) -> bool {
        matches!(self, Self::Up)
    }
}

/// Concurrent map of component name → [`ComponentState`]. Cheap to
/// clone — clones share the same inner map. Reads (snapshot) take a
/// short lock; writes (report) are likewise short.
#[derive(Clone, Default, Debug)]
pub struct HealthRegistry {
    inner: Arc<Mutex<BTreeMap<String, ComponentState>>>,
}

impl HealthRegistry {
    pub fn new() -> Self {
        let me = Self::default();
        // The `startup` component is the one ReadinessGate flips, so
        // pre-seed it as Down. Without this `/readyz` would return
        // an empty component map at startup, which an operator might
        // misread as "no components registered, so trivially ok".
        me.report(
            "startup",
            ComponentState::Down("server still booting".into()),
        );
        me
    }

    pub fn report(&self, name: &str, state: ComponentState) {
        self.inner.lock().unwrap().insert(name.to_string(), state);
    }

    pub fn snapshot(&self) -> BTreeMap<String, ComponentState> {
        self.inner.lock().unwrap().clone()
    }

    pub fn all_up(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .values()
            .all(ComponentState::is_up)
    }
}

/// Backwards-compatible startup gate. Wraps a [`HealthRegistry`] and
/// flips the `"startup"` component when bootstrap finishes. Keep this
/// type around even though it's a thin wrapper — callers should not
/// have to know about the registry just to flip the bootstrap flag.
#[derive(Clone, Debug)]
pub struct ReadinessGate {
    registry: HealthRegistry,
}

impl Default for ReadinessGate {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadinessGate {
    pub fn new() -> Self {
        Self {
            registry: HealthRegistry::new(),
        }
    }

    pub fn from_registry(registry: HealthRegistry) -> Self {
        Self { registry }
    }

    pub fn mark_ready(&self) {
        self.registry.report("startup", ComponentState::Up);
    }

    pub fn is_ready(&self) -> bool {
        self.registry
            .snapshot()
            .get("startup")
            .map(ComponentState::is_up)
            .unwrap_or(false)
    }

    pub fn registry(&self) -> HealthRegistry {
        self.registry.clone()
    }
}

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "system",
    responses(
        (status = 200, description = "Server is up", body = HealthResponse)
    )
)]
pub async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Readiness body. Top-level `ready` is `true` iff every component is
/// `up`. `components` is sorted by name (stable for diffs).
#[derive(Serialize, ToSchema)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub components: BTreeMap<String, ComponentState>,
}

/// Plain-text liveness handler. Always 200 once axum routes.
pub async fn livez() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// JSON readiness handler. 200 iff every registered component is
/// `up`, otherwise 503.
pub async fn readyz(State(reg): State<HealthRegistry>) -> impl IntoResponse {
    let components = reg.snapshot();
    let ready = components.values().all(ComponentState::is_up);
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadinessResponse { ready, components }))
}

/// Build the probe router. Kept separate from the main API router so
/// probes never depend on application state being initialized — the
/// registry passed in here is the same one components write to.
pub fn probes_router(gate: ReadinessGate) -> Router {
    Router::new()
        .route("/healthz", get(livez))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .with_state(gate.registry())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn healthz_is_always_ok() {
        let app = probes_router(ReadinessGate::new());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 64).await.unwrap();
        assert_eq!(&body[..], b"ok");
    }

    #[tokio::test]
    async fn livez_is_alias_for_healthz() {
        let app = probes_router(ReadinessGate::new());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_starts_unready_with_startup_down() {
        let gate = ReadinessGate::new();
        let app = probes_router(gate);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let json = body_json(resp).await;
        assert_eq!(json["ready"], false);
        assert_eq!(json["components"]["startup"]["state"], "down");
    }

    #[tokio::test]
    async fn readyz_flips_to_ok_after_mark_ready() {
        let gate = ReadinessGate::new();
        gate.mark_ready();
        let app = probes_router(gate);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = body_json(resp).await;
        assert_eq!(json["ready"], true);
        assert_eq!(json["components"]["startup"]["state"], "up");
    }

    #[tokio::test]
    async fn readyz_reports_degraded_components() {
        let gate = ReadinessGate::new();
        gate.mark_ready();
        let registry = gate.registry();
        registry.report("db", ComponentState::Up);
        registry.report(
            "reconciler",
            ComponentState::Degraded("worker pool saturated".into()),
        );
        registry.report(
            "cluster:prod",
            ComponentState::Down("apiserver unreachable".into()),
        );

        let app = probes_router(gate);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let json = body_json(resp).await;
        assert_eq!(json["ready"], false);
        assert_eq!(json["components"]["db"]["state"], "up");
        assert_eq!(json["components"]["reconciler"]["state"], "degraded");
        assert_eq!(
            json["components"]["reconciler"]["message"],
            "worker pool saturated"
        );
        assert_eq!(json["components"]["cluster:prod"]["state"], "down");
    }

    #[test]
    fn mark_ready_is_idempotent() {
        let g = ReadinessGate::new();
        assert!(!g.is_ready());
        g.mark_ready();
        g.mark_ready();
        assert!(g.is_ready());
    }

    #[test]
    fn registry_all_up_requires_every_component_up() {
        let r = HealthRegistry::new();
        assert!(!r.all_up()); // startup is Down
        r.report("startup", ComponentState::Up);
        assert!(r.all_up());
        r.report("db", ComponentState::Degraded("slow".into()));
        assert!(!r.all_up());
    }
}
