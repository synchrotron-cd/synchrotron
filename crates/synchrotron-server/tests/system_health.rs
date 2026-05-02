//! End-to-end tests for the system health surface (if7.5).
//!
//! Drives `/healthz` and `/readyz` together with the rest of the
//! routed surface to confirm the probe router and the API router
//! co-exist on a single tower stack.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use synchrotron_server::api::{self, ComponentState, ReadinessGate};
use tower::ServiceExt;

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn healthz_is_always_200_even_before_ready() {
    let gate = ReadinessGate::new();
    // Don't mark ready — /healthz must not depend on it.
    let app = api::router().merge(api::probes_router(gate));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn readyz_503_until_all_components_up() {
    let gate = ReadinessGate::new();
    let registry = gate.registry();
    registry.report("db", ComponentState::Up);
    registry.report(
        "cluster:prod",
        ComponentState::Down("apiserver unreachable".into()),
    );
    gate.mark_ready();

    let app = api::probes_router(gate.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(resp).await;
    assert_eq!(json["ready"], false);
    assert_eq!(json["components"]["db"]["state"], "up");
    assert_eq!(json["components"]["cluster:prod"]["state"], "down");
    assert_eq!(json["components"]["startup"]["state"], "up");

    // Recover the cluster — same gate, fresh router so we re-snapshot.
    registry.report("cluster:prod", ComponentState::Up);
    let app = api::probes_router(gate);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["ready"], true);
}

#[tokio::test]
async fn openapi_describes_readiness_schema() {
    let app = api::router();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert!(
        json["components"]["schemas"]["ReadinessResponse"].is_object(),
        "ReadinessResponse must appear in the OpenAPI doc"
    );
    assert!(json["components"]["schemas"]["ComponentState"].is_object());
}
