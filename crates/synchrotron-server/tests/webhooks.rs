//! End-to-end tests for the webhook routes, driven through the
//! axum router via `tower::ServiceExt::oneshot`. A stub Orchestrator
//! is pre-registered with a fixture repo so trigger routing can be
//! asserted without real git operations.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{digest::KeyInit, Hmac, Mac};
use http_body_util::BodyExt;
use sha2::Sha256;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use synchrotron_core::{EventBus, SystemEvent};
use synchrotron_git::poller::FetchFn;
use synchrotron_git::{
    Credentials, FetchResult, Orchestrator, OrchestratorConfig, PollerConfig, Repo, RepoId, Sha,
};
use synchrotron_server::api::{router_with_webhooks, WebhookSecrets, WebhookState};
use synchrotron_types::RepoUrl;

type Outcomes = Arc<Mutex<VecDeque<Result<FetchResult, String>>>>;

fn ok_result() -> Result<FetchResult, String> {
    Ok(FetchResult {
        previous_head: None,
        current_head: Sha("a".repeat(40)),
        changed: true,
    })
}

fn never_fetch() -> FetchFn {
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([ok_result()])));
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

async fn make_state(secret: &[u8], registered_url: Option<&str>) -> (WebhookState, Option<RepoId>) {
    let orch = Arc::new(Orchestrator::new(long_cfg()));
    let registered_id = if let Some(url) = registered_url {
        let repo = Repo::new(RepoUrl(url.into()), "main", Credentials::None);
        let id = repo.id.clone();
        orch.register(repo, never_fetch()).await.unwrap();
        Some(id)
    } else {
        None
    };
    let state = WebhookState {
        secrets: Arc::new(WebhookSecrets {
            github: Some(secret.to_vec()),
            gitlab: None,
            bitbucket: None,
        }),
        orchestrator: orch,
        bus: EventBus::new(32),
    };
    (state, registered_id)
}

const GH_PUSH: &str =
    r#"{"ref":"refs/heads/main","repository":{"clone_url":"https://github.com/acme/app.git"}}"#;

async fn body_string(resp: axum::response::Response) -> (StatusCode, String) {
    let (parts, body) = resp.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    (parts.status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test]
async fn github_push_triggers_registered_repo() {
    let secret = b"s3cret";
    let (state, id) = make_state(secret, Some("https://github.com/acme/app.git")).await;
    let id = id.unwrap();
    let mut rx = state.bus.subscribe();
    let app = router_with_webhooks(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/github")
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", gh_sig(secret, GH_PUSH.as_bytes()))
        .header("Content-Type", "application/json")
        .body(Body::from(GH_PUSH))
        .unwrap();

    let (status, body) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("trigger"), "body was: {body}");

    let evt = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("bus recv timed out")
        .unwrap();
    match evt.event {
        SystemEvent::WebhookTriggered { repo, .. } => {
            assert_eq!(repo, id.as_str());
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[tokio::test]
async fn github_bad_signature_401() {
    let (state, _) = make_state(b"s3cret", Some("https://github.com/acme/app.git")).await;
    let app = router_with_webhooks(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/github")
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", "sha256=deadbeef")
        .header("Content-Type", "application/json")
        .body(Body::from(GH_PUSH))
        .unwrap();

    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn github_ping_is_accepted_without_trigger() {
    let secret = b"s3cret";
    let (state, _) = make_state(secret, Some("https://github.com/acme/app.git")).await;
    let app = router_with_webhooks(state);

    let ping_body =
        r#"{"zen":"Keep it simple","repository":{"clone_url":"https://github.com/acme/app.git"}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/github")
        .header("X-GitHub-Event", "ping")
        .header("X-Hub-Signature-256", gh_sig(secret, ping_body.as_bytes()))
        .header("Content-Type", "application/json")
        .body(Body::from(ping_body))
        .unwrap();

    let (status, body) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("non-push"), "body was: {body}");
}

#[tokio::test]
async fn github_unregistered_repo_returns_202_no_retry() {
    let secret = b"s3cret";
    // Register a different repo than the webhook targets.
    let (state, _) = make_state(secret, Some("https://github.com/other/repo.git")).await;
    let app = router_with_webhooks(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/github")
        .header("X-GitHub-Event", "push")
        .header("X-Hub-Signature-256", gh_sig(secret, GH_PUSH.as_bytes()))
        .header("Content-Type", "application/json")
        .body(Body::from(GH_PUSH))
        .unwrap();

    let (status, body) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("no matching repo"), "body was: {body}");
}

#[tokio::test]
async fn gitlab_without_secret_returns_503() {
    // Default make_state only configures github; gitlab route should
    // fail closed.
    let (state, _) = make_state(b"x", None).await;
    let app = router_with_webhooks(state);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/webhooks/gitlab")
        .header("X-Gitlab-Event", "Push Hook")
        .header("X-Gitlab-Token", "whatever")
        .body(Body::from(r#"{}"#))
        .unwrap();

    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
