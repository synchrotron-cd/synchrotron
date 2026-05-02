//! Liveness, readiness, and a richer health summary.
//!
//! - `/healthz` is the kubelet liveness probe: trivially "the
//!   listener is up". If this 200s, axum is serving.
//! - `/readyz` is the kubelet readiness probe: 200 only after the
//!   binary has finished startup (DB opened, config loaded). Until
//!   then it returns 503 so kube doesn't route traffic to a half-
//!   booted pod.
//! - `/api/v1/health` is the richer JSON summary used by the CLI
//!   and dashboards. It always returns 200 once axum is up;
//!   subsystem-level health lives in fields, not the HTTP status.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use utoipa::ToSchema;

/// Shared readiness signal. Cheap to clone; readers see a release
/// store from whichever task completed startup.
#[derive(Clone, Default, Debug)]
pub struct ReadinessGate {
    ready: Arc<AtomicBool>,
}

impl ReadinessGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Flip the gate to ready. Idempotent.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
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

/// Plain-text liveness handler. Always 200 once axum routes.
pub async fn livez() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Plain-text readiness handler. 200 once `mark_ready()` has fired
/// on the gate, 503 otherwise.
pub async fn readyz(State(gate): State<ReadinessGate>) -> impl IntoResponse {
    if gate.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// Build the probe router. Kept separate from the main API router
/// so probes never depend on application state being initialized.
pub fn probes_router(gate: ReadinessGate) -> Router {
    Router::new()
        .route("/healthz", get(livez))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .with_state(gate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn healthz_is_always_ok() {
        let gate = ReadinessGate::new();
        let app = probes_router(gate);
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
    async fn readyz_starts_unready_then_flips() {
        let gate = ReadinessGate::new();
        let app = probes_router(gate.clone());

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(resp.into_body(), 64).await.unwrap();
        assert_eq!(&body[..], b"not ready");

        gate.mark_ready();

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
        let body = to_bytes(resp.into_body(), 64).await.unwrap();
        assert_eq!(&body[..], b"ready");
    }

    #[test]
    fn mark_ready_is_idempotent() {
        let g = ReadinessGate::new();
        assert!(!g.is_ready());
        g.mark_ready();
        g.mark_ready();
        assert!(g.is_ready());
    }
}
