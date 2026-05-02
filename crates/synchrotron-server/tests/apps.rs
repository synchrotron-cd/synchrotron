//! Integration tests for the Apps REST API (if7.2).
//!
//! Drives the assembled router through axum's tower stack against an
//! in-memory SQLite database. Each test gets its own state so they
//! can run in parallel.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use synchrotron_core::db::{Database, SyncRecord, SyncRecordStatus, SyncRevision, SyncTrigger};
use synchrotron_core::{EventBus, SystemEvent};
use synchrotron_server::api::{self, AppsState};
use tower::ServiceExt;

fn make_state() -> (AppsState, EventBus) {
    let db = Database::open_in_memory().unwrap();
    let bus = EventBus::new(64);
    (AppsState::new(db, bus.clone()), bus)
}

fn create_body(name: &str) -> Body {
    Body::from(
        serde_json::json!({
            "name": name,
            "namespace": "argocd",
            "repo_url": "https://example.com/r.git",
            "path": "manifests",
            "target_revision": "main",
            "dest_cluster": "in-cluster",
            "dest_namespace": "default",
        })
        .to_string(),
    )
}

async fn read_json(resp: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn create_then_list_then_get() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = read_json(resp).await;
    assert_eq!(view["name"], "web");
    assert_eq!(view["namespace"], "argocd");

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list = read_json(resp).await;
    assert_eq!(list["apps"].as_array().unwrap().len(), 1);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps/web")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = read_json(resp).await;
    assert_eq!(got["name"], "web");
}

#[tokio::test]
async fn create_duplicate_returns_409() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    let mk = || {
        Request::builder()
            .method("POST")
            .uri("/api/v1/apps")
            .header("content-type", "application/json")
            .body(create_body("web"))
            .unwrap()
    };

    let r1 = app.clone().oneshot(mk()).await.unwrap();
    assert_eq!(r1.status(), StatusCode::CREATED);
    let r2 = app.oneshot(mk()).await.unwrap();
    assert_eq!(r2.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn get_missing_returns_404() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json = read_json(resp).await;
    assert_eq!(json["error"]["code"], "not_found");
}

#[tokio::test]
async fn update_modifies_fields() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/apps/web")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"namespace": "production"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let got = read_json(resp).await;
    assert_eq!(got["namespace"], "production");
}

#[tokio::test]
async fn delete_removes_and_then_404s() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/apps/web")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/apps/web")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sync_publishes_manual_event() {
    let (state, bus) = make_state();
    let mut rx = bus.subscribe();
    let app = api::router_with_apps(state);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps/web/sync")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let evt = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
        .await
        .expect("event arrived")
        .expect("not closed");
    match evt.event {
        SystemEvent::ManualSyncRequested { app } => assert_eq!(app.0, "web"),
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn diff_returns_empty_with_note() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps/web/diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = read_json(resp).await;
    assert_eq!(json["app"], "web");
    assert!(json["entries"].as_array().unwrap().is_empty());
    assert!(json["note"].as_str().is_some());
}

#[tokio::test]
async fn rollback_unknown_revision_returns_404() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps/web/rollback")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"revision_id": 9999}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rollback_succeeds_for_owned_revision() {
    let (state, bus) = make_state();
    let mut rx = bus.subscribe();
    let app = api::router_with_apps(state.clone());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();
    let view = read_json(resp).await;
    let app_id = view["id"].as_str().unwrap().to_string();

    // Insert a revision belonging to this app directly via the DB.
    let rev_id = {
        let db = state.db.lock().unwrap();
        db.record_sync_revision(&app_id, "deadbeef", &[0u8; 32], &[], 0)
            .unwrap()
    };

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps/web/rollback")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"revision_id": rev_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let json = read_json(resp).await;
    assert_eq!(json["revision_id"], rev_id);
    assert_eq!(json["commit_hash"], "deadbeef");

    let evt = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
        .await
        .expect("event arrived")
        .expect("not closed");
    matches!(evt.event, SystemEvent::ManualSyncRequested { .. });
}

#[tokio::test]
async fn history_returns_recorded_attempts() {
    let (state, _bus) = make_state();
    let app = api::router_with_apps(state.clone());

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body("web"))
                .unwrap(),
        )
        .await
        .unwrap();
    let view = read_json(resp).await;
    let app_id_str = view["id"].as_str().unwrap();
    let app_id = uuid::Uuid::parse_str(app_id_str).unwrap();

    {
        let db = state.db.lock().unwrap();
        db.insert_sync_record(&SyncRecord {
            id: uuid::Uuid::new_v4(),
            app_id,
            revision: "abc".into(),
            status: SyncRecordStatus::Succeeded,
            message: None,
            trigger: SyncTrigger::Manual,
            resources_synced: 3,
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
        })
        .unwrap();
    }

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps/web/history")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = read_json(resp).await;
    let entries = json["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["status"], "Succeeded");
    assert_eq!(entries[0]["trigger"], "manual");
}

// silence unused-import lints when individual tests are excluded
#[allow(dead_code)]
fn _unused(_: SyncRevision) {}
#[allow(dead_code)]
async fn _unused_body(b: Body) {
    let _ = b.collect().await;
}
