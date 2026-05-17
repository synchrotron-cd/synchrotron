//! Integration tests for the SSE watch endpoint (if7.6).
//!
//! Each test drives the assembled router via axum's tower stack and
//! reads the SSE response body frame-by-frame to assert on the event
//! stream the way a real EventSource client would consume it.

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use synchrotron_core::db::Database;
use synchrotron_core::{EventBus, SystemEvent};
use synchrotron_server::api::{self, AppWatcher, AppsState, WatchState};
use synchrotron_types::{AppName, ClusterName};
use tokio::time::timeout;
use tower::ServiceExt;

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

async fn create_app(router: &axum::Router, name: &str) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/apps")
                .header("content-type", "application/json")
                .body(create_body(name))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

/// Read and concatenate body frames until `pred` is satisfied or the
/// overall deadline elapses. Returns the accumulated text.
async fn read_until(
    mut body: axum::body::Body,
    pred: impl Fn(&str) -> bool,
    deadline: Duration,
) -> String {
    let mut buf = String::new();
    let _ = timeout(deadline, async {
        while let Some(frame) = body.frame().await {
            let frame = frame.unwrap();
            if let Ok(data) = frame.into_data() {
                buf.push_str(&String::from_utf8_lossy(&data));
                if pred(&buf) {
                    break;
                }
            }
        }
    })
    .await;
    buf
}

#[tokio::test]
async fn watch_unknown_app_returns_404() {
    let db = Database::open_in_memory().unwrap();
    let bus = EventBus::new(64);
    let apps_state = AppsState::new(db, bus.clone());
    let watcher = AppWatcher::start(bus);
    let watch_state = WatchState {
        apps: apps_state.clone(),
        watcher,
    };
    let app = api::router_with_watch(apps_state, watch_state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps/missing/watch")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn watch_streams_live_sync_outcome_event() {
    let db = Database::open_in_memory().unwrap();
    let bus = EventBus::new(64);
    let apps_state = AppsState::new(db, bus.clone());
    let watcher = AppWatcher::start(bus.clone());
    let watch_state = WatchState {
        apps: apps_state.clone(),
        watcher,
    };
    let app = api::router_with_watch(apps_state, watch_state);

    create_app(&app, "web").await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps/web/watch")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("text/event-stream"),
        "expected SSE content-type, got {ct}"
    );

    // Give the watcher's fan-in task and SSE subscriber a tick to attach
    // before publishing.
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(20)).await;

    bus.publish(SystemEvent::SyncOutcome {
        app: AppName("web".into()),
        cluster: ClusterName("prod".into()),
        success: true,
        message: None,
    });
    // A second app's events must not appear on the `web` stream.
    bus.publish(SystemEvent::ManualSyncRequested {
        app: AppName("other".into()),
    });

    let body = read_until(
        resp.into_body(),
        |s| s.contains("event: sync_outcome"),
        Duration::from_secs(2),
    )
    .await;

    assert!(
        body.contains("event: sync_outcome"),
        "did not see sync_outcome in body: {body:?}"
    );
    assert!(
        body.contains("\"app\":\"web\""),
        "did not see web app in body: {body:?}"
    );
    assert!(
        !body.contains("\"app\":\"other\""),
        "stream leaked another app's events: {body:?}"
    );
}

#[tokio::test]
async fn watch_replays_backlog_via_last_event_id() {
    let db = Database::open_in_memory().unwrap();
    let bus = EventBus::new(64);
    let apps_state = AppsState::new(db, bus.clone());
    let watcher = AppWatcher::start(bus.clone());
    let watch_state = WatchState {
        apps: apps_state.clone(),
        watcher: watcher.clone(),
    };
    let app = api::router_with_watch(apps_state, watch_state);

    create_app(&app, "web").await;

    // Wait for the fan-in to drain the AppChanged event published by
    // create_app (5bv), then snapshot the buffer so we're starting
    // from a known 1-event baseline.
    let mut probe = watcher.subscribe();
    timeout(Duration::from_secs(1), probe.recv())
        .await
        .expect("AppChanged arrived")
        .expect("not closed");

    // Publish three more events before any client connects.
    for _ in 0..3 {
        bus.publish(SystemEvent::ManualSyncRequested {
            app: AppName("web".into()),
        });
    }
    for _ in 0..3 {
        timeout(Duration::from_secs(1), probe.recv())
            .await
            .expect("event arrived")
            .expect("not closed");
    }
    drop(probe);

    let buffered = watcher.replay("web", None);
    assert_eq!(buffered.len(), 4);
    // Skip past AppChanged + the first ManualSync; expect 2 left.
    let first_id = buffered[1].id;

    // Reconnect with Last-Event-ID set to the first id; expect the
    // replay to deliver only the two newer events before the live tail.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/apps/web/watch")
                .header("last-event-id", first_id.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = read_until(
        resp.into_body(),
        |s| s.matches("event: sync_requested").count() >= 2,
        Duration::from_secs(2),
    )
    .await;

    let count = body.matches("event: sync_requested").count();
    assert_eq!(
        count, 2,
        "expected 2 backlog events after Last-Event-ID, got {count}; body: {body:?}"
    );
    assert!(
        !body.contains(&format!("id: {first_id}\n")),
        "reconnect must skip the event whose id == Last-Event-ID"
    );
}
