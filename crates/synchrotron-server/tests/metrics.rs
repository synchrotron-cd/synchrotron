//! End-to-end test for the `/metrics` endpoint exposing Prometheus
//! / OpenMetrics text.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

use synchrotron_core::metrics::Metrics;
use synchrotron_server::api::metrics_router;

#[tokio::test]
async fn metrics_endpoint_returns_openmetrics_text() {
    let metrics = Arc::new(Metrics::new());
    let app = metrics_router(metrics.clone());

    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    assert!(
        ct.starts_with("application/openmetrics-text"),
        "content-type: {ct}"
    );

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&bytes).to_string();
    assert!(body.contains("# HELP"), "body: {body}");
    assert!(body.trim_end().ends_with("# EOF"), "body: {body}");
}

#[tokio::test]
async fn metrics_endpoint_reflects_recorded_data() {
    let metrics = Arc::new(Metrics::new());
    metrics.record_reconcile("app-a", "prod", true, std::time::Duration::from_millis(120));
    metrics.set_worker_active(3);

    let app = metrics_router(metrics);
    let req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&bytes).to_string();

    assert!(body.contains("synchrotron_reconcile_total"), "body: {body}");
    assert!(body.contains("app-a"), "body: {body}");
    assert!(body.contains("synchrotron_worker_active"), "body: {body}");
}
