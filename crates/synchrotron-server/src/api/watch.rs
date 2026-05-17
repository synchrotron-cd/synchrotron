//! Server-Sent Events (SSE) stream for live app status (if7.6).
//!
//! `GET /api/v1/apps/{name}/watch` opens a long-lived SSE connection.
//! The server fans select [`SystemEvent`]s from the in-process
//! [`EventBus`] into per-app watch events, each tagged with a
//! monotonic `id` and a `kind` label (`sync_requested`,
//! `sync_outcome`, `health`).
//!
//! ## Backpressure
//!
//! Each SSE client gets its own [`tokio::sync::broadcast`] receiver
//! with a fixed-capacity ring buffer. A slow consumer's receiver
//! drops the oldest events when full (the broadcast channel's
//! semantics: receivers see `Lagged(n)` and we silently advance —
//! event drops are logged via tracing). Producers are never blocked.
//!
//! ## Reconnect with `Last-Event-ID`
//!
//! On reconnect the EventSource client re-sends the last `id` it saw
//! via the `Last-Event-ID` header. The watcher keeps a small
//! per-app ring buffer of recent events; on reconnect we replay
//! every buffered event with `id > last_event_id` before switching
//! to live broadcast. If the gap is larger than the buffer the
//! client just resumes from "now" — losing intermediate events is
//! the price of in-memory state, and the on-reconnect snapshot
//! rebuilds the full picture from the apps API.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::Router;
use futures_core::Stream;
use serde::Serialize;
use synchrotron_core::events::{EventBus, SystemEvent};
use tokio::sync::broadcast;
use tracing::{debug, warn};

use crate::api::apps::AppsState;
use crate::api::errors::ApiError;

/// Default per-app replay window. ~1 minute of activity at the
/// busiest cadence we'd expect from any one app.
const DEFAULT_PER_APP_BUFFER: usize = 64;

/// Default broadcast capacity. Sized so a streaming consumer can
/// tolerate a one-second hiccup at the global event rate without
/// dropping.
const DEFAULT_BROADCAST_CAPACITY: usize = 1024;

/// One event delivered to SSE subscribers. The `id` is monotonic
/// across the whole watcher (not per-app), so clients can use it as
/// `Last-Event-ID` regardless of which app they're watching.
#[derive(Debug, Clone, Serialize)]
pub struct AppWatchEvent {
    pub id: u64,
    pub app: String,
    pub kind: &'static str,
    pub payload: serde_json::Value,
}

#[derive(Clone)]
pub struct AppWatcher {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    log: Mutex<HashMap<String, VecDeque<AppWatchEvent>>>,
    per_app_capacity: usize,
    tx: broadcast::Sender<AppWatchEvent>,
}

impl AppWatcher {
    /// Spawn the fan-in task that converts bus events into watch
    /// events. The returned [`AppWatcher`] is cheap to clone.
    pub fn start(bus: EventBus) -> Self {
        Self::with_capacity(bus, DEFAULT_PER_APP_BUFFER, DEFAULT_BROADCAST_CAPACITY)
    }

    pub fn with_capacity(
        bus: EventBus,
        per_app_capacity: usize,
        broadcast_capacity: usize,
    ) -> Self {
        let (tx, _rx) = broadcast::channel(broadcast_capacity);
        let inner = Arc::new(Inner {
            next_id: AtomicU64::new(1),
            log: Mutex::new(HashMap::new()),
            per_app_capacity,
            tx,
        });

        let mut rx = bus.subscribe();
        let task_inner = inner.clone();
        tokio::spawn(async move {
            while let Ok(bus_event) = rx.recv().await {
                if let Some(evt) = convert_event(&bus_event.event, &task_inner.next_id) {
                    task_inner.append(&evt);
                    // send returns Err only when there are zero
                    // subscribers; that's fine and not worth logging.
                    let _ = task_inner.tx.send(evt);
                }
            }
            debug!("AppWatcher fan-in task exiting (bus closed)");
        });

        Self { inner }
    }

    /// Replay every buffered event for `app` with `id > since`.
    /// Returns events in arrival order.
    pub fn replay(&self, app: &str, since: Option<u64>) -> Vec<AppWatchEvent> {
        let log = self.inner.log.lock().unwrap();
        let Some(buf) = log.get(app) else {
            return Vec::new();
        };
        let lower = since.unwrap_or(0);
        buf.iter().filter(|e| e.id > lower).cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppWatchEvent> {
        self.inner.tx.subscribe()
    }
}

impl Inner {
    fn append(&self, evt: &AppWatchEvent) {
        let mut log = self.log.lock().unwrap();
        let buf = log.entry(evt.app.clone()).or_default();
        buf.push_back(evt.clone());
        // Drop-oldest on overflow.
        while buf.len() > self.per_app_capacity {
            buf.pop_front();
        }
    }
}

fn convert_event(sys: &SystemEvent, counter: &AtomicU64) -> Option<AppWatchEvent> {
    let (app, kind, payload) = match sys {
        SystemEvent::ManualSyncRequested { app } => (
            app.0.clone(),
            "sync_requested",
            serde_json::json!({"app": app.0}),
        ),
        SystemEvent::AppChanged { app, repo } => (
            app.0.clone(),
            "app_changed",
            serde_json::json!({"app": app.0, "repo": repo}),
        ),
        SystemEvent::SyncOutcome {
            app,
            cluster,
            success,
            message,
            ..
        } => (
            app.0.clone(),
            "sync_outcome",
            serde_json::json!({
                "app": app.0,
                "cluster": cluster.0,
                "success": success,
                "message": message,
            }),
        ),
        SystemEvent::AppHealthAssessed {
            app,
            cluster,
            status,
            message,
        } => (
            app.0.clone(),
            "health",
            serde_json::json!({
                "app": app.0,
                "cluster": cluster.0,
                "status": status.as_str(),
                "message": message,
            }),
        ),
        // Repo-scoped and webhook-scoped events don't fit the per-app
        // stream — they're surfaced via separate endpoints.
        SystemEvent::RepoChanged { .. }
        | SystemEvent::RepoUnchanged { .. }
        | SystemEvent::RepoFetchFailed { .. }
        | SystemEvent::WebhookTriggered { .. } => return None,
    };
    Some(AppWatchEvent {
        id: counter.fetch_add(1, Ordering::Relaxed),
        app,
        kind,
        payload,
    })
}

/// Combined state for the SSE watch handler: it needs the apps state
/// to verify the app exists, and the watcher to subscribe to.
#[derive(Clone)]
pub struct WatchState {
    pub apps: AppsState,
    pub watcher: AppWatcher,
}

pub fn router(state: WatchState) -> Router {
    Router::new()
        .route("/api/v1/apps/{name}/watch", get(watch_app))
        .with_state(state)
}

#[utoipa::path(
    get,
    path = "/api/v1/apps/{name}/watch",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses(
        (status = 200, description = "SSE stream of app events"),
        (status = 404, description = "App not found"),
    )
)]
pub async fn watch_app(
    State(state): State<WatchState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Validate the app exists so the SSE handshake fails fast on a
    // typo rather than streaming an empty channel forever.
    {
        let db = state.apps.db.lock().unwrap();
        let exists = db
            .get_application(&name)
            .map_err(|e| ApiError::internal(format!("database error: {e}")))?
            .is_some();
        if !exists {
            return Err(ApiError::not_found(format!("app `{name}` not found")));
        }
    }

    let since = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    let backlog = state.watcher.replay(&name, since);
    let mut live_rx = state.watcher.subscribe();
    let target = name;

    let stream = async_stream::stream! {
        for evt in backlog {
            yield Ok(to_sse(&evt));
        }
        loop {
            match live_rx.recv().await {
                Ok(evt) if evt.app == target => yield Ok(to_sse(&evt)),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(lagged = n, "SSE watcher lagged; dropped {n} events");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

fn to_sse(evt: &AppWatchEvent) -> Event {
    // `json_data` only fails if serialization fails; AppWatchEvent's
    // payload is built from owned serde_json values so it can't.
    Event::default()
        .id(evt.id.to_string())
        .event(evt.kind)
        .json_data(&evt.payload)
        .expect("AppWatchEvent payload is always JSON-serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchrotron_types::{AppName, ClusterName, HealthStatusCode};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn fans_in_and_replays_events() {
        let bus = EventBus::new(64);
        let watcher = AppWatcher::start(bus.clone());

        // Give the spawned task a moment to subscribe before we
        // publish — otherwise the events publish to zero receivers
        // and the watcher's task starts after the queue has drained.
        tokio::task::yield_now().await;

        bus.publish(SystemEvent::ManualSyncRequested {
            app: AppName("web".into()),
        });
        bus.publish(SystemEvent::SyncOutcome {
            app: AppName("web".into()),
            cluster: ClusterName("prod".into()),
            success: true,
            message: None,
            trigger: "manual".into(),
            revision: None,
            resources_synced: 0,
        });
        bus.publish(SystemEvent::AppHealthAssessed {
            app: AppName("api".into()),
            cluster: ClusterName("prod".into()),
            status: HealthStatusCode::Healthy,
            message: None,
        });

        // Drain via a live subscriber to know the fan-in has caught
        // up before snapshotting the per-app buffer.
        let mut rx = watcher.subscribe();
        for _ in 0..3 {
            timeout(Duration::from_millis(500), rx.recv())
                .await
                .expect("event arrived")
                .expect("not closed");
        }

        let web = watcher.replay("web", None);
        assert_eq!(web.len(), 2);
        assert_eq!(web[0].kind, "sync_requested");
        assert_eq!(web[1].kind, "sync_outcome");
        assert!(web[0].id < web[1].id);

        let api = watcher.replay("api", None);
        assert_eq!(api.len(), 1);
        assert_eq!(api[0].kind, "health");
    }

    #[tokio::test]
    async fn replay_filters_by_since_id() {
        let bus = EventBus::new(64);
        let watcher = AppWatcher::start(bus.clone());
        tokio::task::yield_now().await;

        bus.publish(SystemEvent::ManualSyncRequested {
            app: AppName("web".into()),
        });
        bus.publish(SystemEvent::ManualSyncRequested {
            app: AppName("web".into()),
        });

        let mut rx = watcher.subscribe();
        for _ in 0..2 {
            timeout(Duration::from_millis(500), rx.recv())
                .await
                .unwrap()
                .unwrap();
        }

        let all = watcher.replay("web", None);
        assert_eq!(all.len(), 2);

        let after_first = watcher.replay("web", Some(all[0].id));
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].id, all[1].id);
    }

    #[tokio::test]
    async fn per_app_buffer_drops_oldest() {
        let bus = EventBus::new(64);
        let watcher = AppWatcher::with_capacity(bus.clone(), 3, 64);
        tokio::task::yield_now().await;

        for _ in 0..5 {
            bus.publish(SystemEvent::ManualSyncRequested {
                app: AppName("web".into()),
            });
        }
        let mut rx = watcher.subscribe();
        for _ in 0..5 {
            timeout(Duration::from_millis(500), rx.recv())
                .await
                .unwrap()
                .unwrap();
        }

        let buf = watcher.replay("web", None);
        assert_eq!(buf.len(), 3, "ring buffer should cap at capacity");
        // After dropping the oldest two, the smallest surviving id is 3.
        assert_eq!(buf[0].id, 3);
    }

    #[tokio::test]
    async fn repo_events_are_filtered_out() {
        let bus = EventBus::new(64);
        let watcher = AppWatcher::start(bus.clone());
        tokio::task::yield_now().await;

        bus.publish(SystemEvent::RepoChanged {
            repo: "github.com/org/r".into(),
            new_head: "abc".into(),
        });
        bus.publish(SystemEvent::ManualSyncRequested {
            app: AppName("web".into()),
        });
        let mut rx = watcher.subscribe();
        // Only one event should fan in — wait for it.
        timeout(Duration::from_millis(500), rx.recv())
            .await
            .unwrap()
            .unwrap();
        // Brief pause to confirm no second event arrives.
        let next = timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(
            next.is_err(),
            "repo events must not appear in the app watcher"
        );
    }
}
