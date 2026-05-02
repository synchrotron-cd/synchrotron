//! Smoke test for the public REST API skeleton (if7.1).
//!
//! Drives the assembled [`api::router`] — TraceLayer, OpenAPI, and
//! health endpoint together — through axum's tower stack so any
//! middleware-level wiring breakage shows up here, not at startup.

use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use synchrotron_server::api;
use tower::ServiceExt;

#[tokio::test]
async fn health_endpoint_returns_status_and_version() {
    let app = api::router();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert!(
        json["version"].as_str().is_some_and(|v| !v.is_empty()),
        "version field should be a non-empty string"
    );
}

#[tokio::test]
async fn openapi_json_describes_health_endpoint() {
    let app = api::router();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["paths"]["/api/v1/health"]["get"].is_object());
}

#[tokio::test]
async fn unknown_route_returns_404() {
    let app = api::router();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/does-not-exist")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
