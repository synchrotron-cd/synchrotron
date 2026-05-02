//! Integration tests for the Repos REST API (if7.4).

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use synchrotron_core::db::Database;
use synchrotron_server::api::{self, ReposState};
use tower::ServiceExt;

fn make_state() -> ReposState {
    let db = Database::open_in_memory().unwrap();
    ReposState::new(Arc::new(Mutex::new(db)))
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
        "name": "manifests",
        "url": "https://github.com/org/manifests.git",
        "branch": "main",
        "credentials_secret_ref": "vault://secret/git/org",
        "password": "should-not-leak",
        "labels": {"tier": "core"},
    })
}

#[tokio::test]
async fn create_lists_and_redacts_password() {
    let app = api::router_with_repos(make_state());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/repos")
                .header("content-type", "application/json")
                .body(json_body(create_body()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = read_json(resp).await;
    assert_eq!(view["name"], "manifests");
    assert_eq!(view["has_password"], true);
    assert_eq!(view["credentials_secret_ref"], "vault://secret/git/org");
    assert!(view.get("password").is_none());

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/repos")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list = read_json(resp).await;
    assert_eq!(list["repos"].as_array().unwrap().len(), 1);
    let entry = &list["repos"][0];
    assert!(
        entry.get("password").is_none(),
        "password must not appear in list responses"
    );
    let body_str = serde_json::to_string(&list).unwrap();
    assert!(
        !body_str.contains("should-not-leak"),
        "raw password value leaked in body: {body_str}"
    );
}

#[tokio::test]
async fn rejects_garbage_url() {
    let app = api::router_with_repos(make_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/repos")
                .header("content-type", "application/json")
                .body(json_body(serde_json::json!({
                    "name": "bad",
                    "url": "not a url",
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_blank_secret_ref() {
    let app = api::router_with_repos(make_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/repos")
                .header("content-type", "application/json")
                .body(json_body(serde_json::json!({
                    "name": "bad",
                    "url": "https://example.com/r.git",
                    "credentials_secret_ref": "no-scheme-here",
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
async fn accepts_ssh_url_form() {
    let app = api::router_with_repos(make_state());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/repos")
                .header("content-type", "application/json")
                .body(json_body(serde_json::json!({
                    "name": "via-ssh",
                    "url": "git@github.com:org/repo.git",
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn duplicate_name_409() {
    let app = api::router_with_repos(make_state());
    let mk = || {
        Request::builder()
            .method("POST")
            .uri("/api/v1/repos")
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
async fn update_changes_branch_and_password() {
    let app = api::router_with_repos(make_state());

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/repos")
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
                .uri("/api/v1/repos/manifests")
                .header("content-type", "application/json")
                .body(json_body(serde_json::json!({
                    "branch": "develop",
                    "password": "rotated",
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let view = read_json(resp).await;
    assert_eq!(view["branch"], "develop");
    assert_eq!(view["has_password"], true);
    assert!(view.get("password").is_none());
}

#[tokio::test]
async fn delete_removes_repo() {
    let app = api::router_with_repos(make_state());
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/repos")
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
                .uri("/api/v1/repos/manifests")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/repos/manifests")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
