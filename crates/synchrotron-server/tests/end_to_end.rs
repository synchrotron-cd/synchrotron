//! Umbrella end-to-end test for h48.6 Webhook & Event System.
//!
//! Exercises the full chain across the webhook router, the in-process
//! event bus, a stub reconciler, and the outbound notifier:
//!
//! ```text
//!   POST /api/v1/webhooks/github            (signed)
//!     └─► webhook handler verifies + parses
//!         └─► Orchestrator::trigger fires the registered fetch
//!             └─► WebhookTriggered → bus
//!                 └─► (stub reconciler) emits SyncOutcome → bus
//!                     └─► Notifier POSTs JSON to receiver URL
//! ```
//!
//! The real reconciler crate has its own end-to-end coverage; here
//! we use a minimal bus-to-bus shim so the chain under test is
//! "webhook → trigger event → outcome notification" without dragging
//! in DesiredSource / LiveSource fixtures.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use hmac::{digest::KeyInit, Hmac, Mac};
use http_body_util::BodyExt;
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use synchrotron_core::{EventBus, SystemEvent};
use synchrotron_git::poller::FetchFn;
use synchrotron_git::{
    Credentials, FetchResult, Orchestrator, OrchestratorConfig, PollerConfig, Repo, RepoId, Sha,
};
use synchrotron_notifier::{ConfigSource, NotificationConfig, Notifier, StaticConfigSource};
use synchrotron_server::api::{router_with_webhooks, WebhookSecrets, WebhookState};
use synchrotron_types::{AppName, ClusterName, RepoUrl};

type Outcomes = Arc<Mutex<VecDeque<Result<FetchResult, String>>>>;

fn ok_fetch() -> FetchFn {
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([Ok(FetchResult {
        previous_head: None,
        current_head: Sha("a".repeat(40)),
        changed: true,
    })])));
    Arc::new(move || {
        let outcomes = outcomes.clone();
        Box::pin(async move {
            let mut g = outcomes.lock().await;
            g.pop_front()
                .unwrap_or_else(|| Err("test: outcomes exhausted".into()))
        })
    })
}

fn long_cfg() -> OrchestratorConfig {
    OrchestratorConfig {
        max_concurrent_fetches: 4,
        poller: PollerConfig {
            interval: Duration::from_secs(3600),
            jitter_ratio: 0.0,
        },
    }
}

fn gh_sig(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

const REPO_URL: &str = "https://github.com/acme/app.git";
const APP_NAME: &str = "billing";
const CLUSTER_NAME: &str = "prod";
const GH_PUSH: &str =
    r#"{"ref":"refs/heads/main","repository":{"clone_url":"https://github.com/acme/app.git"}}"#;

#[derive(Default, Clone)]
struct Recorder {
    inner: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl Recorder {
    fn last(&self) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().last().cloned()
    }
    fn count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

async fn recorder_handler(State(rec): State<Recorder>, body: axum::body::Bytes) -> StatusCode {
    rec.inner.lock().unwrap().push(body.to_vec());
    StatusCode::OK
}

async fn spawn_recorder() -> (String, Recorder) {
    let rec = Recorder::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/notify", post(recorder_handler))
        .with_state(rec.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/notify"), rec)
}

async fn make_webhook_state(bus: EventBus, secret: &[u8]) -> (WebhookState, RepoId) {
    let orch = Arc::new(Orchestrator::new(long_cfg()));
    let repo = Repo::new(RepoUrl(REPO_URL.into()), "main", Credentials::None);
    let id = repo.id.clone();
    orch.register(repo, ok_fetch()).await.unwrap();
    let state = WebhookState {
        secrets: Arc::new(WebhookSecrets {
            github: Some(secret.to_vec()),
            gitlab: None,
            bitbucket: None,
        }),
        orchestrator: orch,
        bus,
    };
    (state, id)
}

/// Minimal stand-in for the real reconciler: subscribes to the bus,
/// translates a `WebhookTriggered` (or `RepoChanged`) into a
/// `SyncOutcome` for the test app. Real reconciler logic lives in
/// `synchrotron-reconcile`; this fixture exists so the umbrella test
/// can prove the wiring without spinning up cluster fixtures.
fn spawn_stub_reconciler(bus: EventBus, app: AppName, cluster: ClusterName) {
    let mut rx = bus.subscribe();
    tokio::spawn(async move {
        loop {
            let evt = match rx.recv().await {
                Ok(e) => e,
                Err(_) => return,
            };
            match evt.event {
                SystemEvent::WebhookTriggered { .. } | SystemEvent::RepoChanged { .. } => {
                    bus.publish(SystemEvent::SyncOutcome {
                        app: app.clone(),
                        cluster: cluster.clone(),
                        success: true,
                        message: None,
                    });
                }
                _ => {}
            }
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_event_triggers_reconcile_and_fires_notification() {
    let (notify_url, rec) = spawn_recorder().await;

    let bus = EventBus::new(64);
    let secret = b"webhook-secret";
    let (state, _repo_id) = make_webhook_state(bus.clone(), secret).await;

    // Stub reconciler: WebhookTriggered → SyncOutcome.
    spawn_stub_reconciler(
        bus.clone(),
        AppName(APP_NAME.into()),
        ClusterName(CLUSTER_NAME.into()),
    );

    // Notifier: SyncOutcome → POST notify_url.
    let mut src = StaticConfigSource::new();
    src.insert(
        APP_NAME,
        NotificationConfig::new(notify_url.clone()).with_max_attempts(2),
    );
    let notifier = Notifier::new(bus.clone(), Arc::new(src) as Arc<dyn ConfigSource>);
    let metrics = notifier.metrics();
    let _h = notifier.spawn();

    // Drive the chain by posting a signed GitHub push.
    let app = router_with_webhooks(state);
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/github")
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", gh_sig(secret, GH_PUSH.as_bytes()))
        .header("Content-Type", "application/json")
        .body(Body::from(GH_PUSH))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let _ = resp.into_body().collect().await.unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);

    // Wait up to a couple seconds for the notification to land.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while metrics.sent() == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(metrics.sent(), 1, "notifier never POSTed");
    assert_eq!(rec.count(), 1, "recorder did not see the POST");

    let body: serde_json::Value = serde_json::from_slice(&rec.last().unwrap()).unwrap();
    assert_eq!(body["kind"], "sync_outcome");
    assert_eq!(body["app"], APP_NAME);
    assert_eq!(body["cluster"], CLUSTER_NAME);
    assert_eq!(body["success"], true);
}
