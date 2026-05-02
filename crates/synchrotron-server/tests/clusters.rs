//! Integration tests for the Clusters REST API (if7.3).
//!
//! Drives the assembled router through axum's tower stack against an
//! in-memory SQLite database. The connectivity check exercises the
//! `build_config` failure path (missing kubeconfig) without needing a
//! live apiserver — successful connect is validated separately by
//! the synchrotron-kube crate's own tests.

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use synchrotron_core::db::Database;
use synchrotron_server::api::{self, ClustersState};
use tower::ServiceExt;

fn make_state() -> ClustersState {
    let db = Database::open_in_memory().unwrap();
    ClustersState::new(Arc::new(Mutex::new(db)))
}

fn json_body(v: serde_json::Value) -> Body {
    Body::from(v.to_string())
}

async fn read_json(resp: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn create_body() -> serde_json::Value {
    serde_json::json!({
        "name": "prod",
        "auth_source": "kubeconfig",
        "kubeconfig_path": "/tmp/no-such-kubeconfig",
        "context": "prod-ctx",
        "bearer_token": "secret-token",
        "labels": {"region": "us-east"},
    })
}

#[tokio::test]
async fn create_lists_and_redacts_token() {
    let app = api::router_with_clusters(make_state());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters")
                .header("content-type", "application/json")
                .body(json_body(create_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = read_json(resp).await;
    assert_eq!(view["name"], "prod");
    assert_eq!(view["has_bearer_token"], true);
    assert!(view.get("bearer_token").is_none());
    assert_eq!(view["labels"]["region"], "us-east");

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/clusters")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list = read_json(resp).await;
    assert_eq!(list["clusters"].as_array().unwrap().len(), 1);
    let entry = &list["clusters"][0];
    assert!(entry.get("bearer_token").is_none(), "token must not leak");
}

#[tokio::test]
async fn create_validates_kubeconfig_path() {
    let app = api::router_with_clusters(make_state());

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters")
                .header("content-type", "application/json")
                .body(json_body(serde_json::json!({
                    "name": "bad",
                    "auth_source": "kubeconfig",
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json = read_json(resp).await;
    assert_eq!(json["error"]["code"], "bad_request");
}

#[tokio::test]
async fn duplicate_name_409() {
    let app = api::router_with_clusters(make_state());
    let mk = || {
        Request::builder()
            .method("POST")
            .uri("/api/v1/clusters")
            .header("content-type", "application/json")
            .body(json_body(create_body()))
            .unwrap()
    };
    assert_eq!(
        app.clone().oneshot(mk()).await.unwrap().status(),
        StatusCode::CREATED
    );
    assert_eq!(
        app.oneshot(mk()).await.unwrap().status(),
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn update_modifies_labels_and_token() {
    let app = api::router_with_clusters(make_state());

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters")
                .header("content-type", "application/json")
                .body(json_body(create_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/clusters/prod")
                .header("content-type", "application/json")
                .body(json_body(serde_json::json!({
                    "labels": {"tier": "blue"},
                    "bearer_token": "rotated",
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let view = read_json(resp).await;
    assert_eq!(view["labels"]["tier"], "blue");
    assert_eq!(view["has_bearer_token"], true);
}

#[tokio::test]
async fn delete_removes_cluster() {
    let app = api::router_with_clusters(make_state());

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters")
                .header("content-type", "application/json")
                .body(json_body(create_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/clusters/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/clusters/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn check_reports_missing_kubeconfig() {
    let app = api::router_with_clusters(make_state());

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters")
                .header("content-type", "application/json")
                .body(json_body(create_body()))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters/prod/check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = read_json(resp).await;
    assert_eq!(json["ok"], false);
    assert_eq!(json["failed_stage"], "connect");
    assert!(json["error"].as_str().is_some());
}

#[tokio::test]
async fn check_404_on_unknown_cluster() {
    let app = api::router_with_clusters(make_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/clusters/missing/check")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
