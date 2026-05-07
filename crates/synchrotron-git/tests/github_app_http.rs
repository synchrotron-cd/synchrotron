//! Integration test for `github_app_http`: drives the production
//! `reqwest`-based fetcher against an in-process axum mock that
//! mimics GitHub's installation-token endpoint.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;

use synchrotron_git::github_app::GitHubAppConfig;
use synchrotron_git::github_app_http::{
    default_token_fetcher, reqwest_token_fetcher, ReqwestFetcherConfig,
};

/// Recorded server-side state.
#[derive(Default)]
struct Recorder {
    calls: Vec<RecordedCall>,
    responses: Vec<MockResponse>,
}

struct RecordedCall {
    installation_id: u64,
    auth: Option<String>,
    accept: Option<String>,
    api_version: Option<String>,
    user_agent: Option<String>,
}

#[derive(Clone)]
enum MockResponse {
    Ok { token: String, expires_at: String },
    Status(u16),
}

#[derive(Clone, Default)]
struct AppState {
    inner: Arc<Mutex<Recorder>>,
}

async fn token_handler(
    State(state): State<AppState>,
    Path(installation_id): Path<u64>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let mut g = state.inner.lock().unwrap();
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let api_version = headers
        .get("x-github-api-version")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    g.calls.push(RecordedCall {
        installation_id,
        auth,
        accept,
        api_version,
        user_agent,
    });
    let resp = if g.responses.is_empty() {
        MockResponse::Ok {
            token: "ghs_default".into(),
            expires_at: "2099-01-01T00:00:00Z".into(),
        }
    } else {
        g.responses.remove(0)
    };
    drop(g);
    match resp {
        MockResponse::Ok { token, expires_at } => (
            StatusCode::CREATED,
            Json(json!({
                "token": token,
                "expires_at": expires_at,
                "permissions": {"contents": "read"},
                "repository_selection": "selected"
            })),
        )
            .into_response(),
        MockResponse::Status(code) => (
            StatusCode::from_u16(code).unwrap(),
            format!("mock error body for {code}"),
        )
            .into_response(),
    }
}

async fn spawn_mock(state: AppState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route(
            "/app/installations/{installation_id}/access_tokens",
            post(token_handler),
        )
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn fast_config() -> ReqwestFetcherConfig {
    ReqwestFetcherConfig {
        request_timeout: Duration::from_secs(5),
        max_attempts: 3,
        backoff_base: Duration::from_millis(1),
        user_agent: "synchrotron-cd-test".into(),
    }
}

/// Direct test of the fetcher closure: no JWT signing needed because
/// the mock doesn't verify signatures.
async fn invoke_fetcher(
    fetcher: &synchrotron_git::github_app::TokenFetcher,
    api_base: &str,
    installation_id: u64,
) -> synchrotron_git::Result<synchrotron_git::github_app::InstallationToken> {
    let url = format!(
        "{}/app/installations/{}/access_tokens",
        api_base.trim_end_matches('/'),
        installation_id
    );
    fetcher("test.jwt.value".into(), url).await
}

#[tokio::test]
async fn happy_path_posts_jwt_and_parses_token() {
    let state = AppState::default();
    state
        .inner
        .lock()
        .unwrap()
        .responses
        .push(MockResponse::Ok {
            token: "ghs_abc123".into(),
            expires_at: "2030-01-02T03:04:05Z".into(),
        });
    let base = spawn_mock(state.clone()).await;

    let fetcher = default_token_fetcher(fast_config()).unwrap();
    let token = invoke_fetcher(&fetcher, &base, 42).await.unwrap();

    assert_eq!(token.token, "ghs_abc123");
    let expected: SystemTime = chrono::DateTime::parse_from_rfc3339("2030-01-02T03:04:05Z")
        .unwrap()
        .into();
    assert_eq!(token.expires_at, expected);

    let g = state.inner.lock().unwrap();
    assert_eq!(g.calls.len(), 1);
    let call = &g.calls[0];
    assert_eq!(call.installation_id, 42);
    assert_eq!(call.auth.as_deref(), Some("Bearer test.jwt.value"));
    assert_eq!(call.accept.as_deref(), Some("application/vnd.github+json"));
    assert_eq!(call.api_version.as_deref(), Some("2022-11-28"));
    assert_eq!(call.user_agent.as_deref(), Some("synchrotron-cd-test"));
}

#[tokio::test]
async fn retries_on_5xx_then_succeeds() {
    let state = AppState::default();
    {
        let mut g = state.inner.lock().unwrap();
        g.responses.push(MockResponse::Status(503));
        g.responses.push(MockResponse::Status(502));
        g.responses.push(MockResponse::Ok {
            token: "ghs_after_retry".into(),
            expires_at: "2030-01-01T00:00:00Z".into(),
        });
    }
    let base = spawn_mock(state.clone()).await;
    let fetcher = default_token_fetcher(fast_config()).unwrap();

    let token = invoke_fetcher(&fetcher, &base, 7).await.unwrap();
    assert_eq!(token.token, "ghs_after_retry");
    assert_eq!(state.inner.lock().unwrap().calls.len(), 3);
}

#[tokio::test]
async fn retries_on_429() {
    let state = AppState::default();
    {
        let mut g = state.inner.lock().unwrap();
        g.responses.push(MockResponse::Status(429));
        g.responses.push(MockResponse::Ok {
            token: "ghs_after_429".into(),
            expires_at: "2030-01-01T00:00:00Z".into(),
        });
    }
    let base = spawn_mock(state.clone()).await;
    let fetcher = default_token_fetcher(fast_config()).unwrap();

    let token = invoke_fetcher(&fetcher, &base, 7).await.unwrap();
    assert_eq!(token.token, "ghs_after_429");
    assert_eq!(state.inner.lock().unwrap().calls.len(), 2);
}

#[tokio::test]
async fn does_not_retry_on_401() {
    let state = AppState::default();
    state
        .inner
        .lock()
        .unwrap()
        .responses
        .push(MockResponse::Status(401));
    let base = spawn_mock(state.clone()).await;
    let fetcher = default_token_fetcher(fast_config()).unwrap();

    let err = invoke_fetcher(&fetcher, &base, 7).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("401"), "want 401 in error, got: {msg}");
    assert!(
        msg.contains("mock error body"),
        "want body snippet, got: {msg}"
    );
    assert_eq!(state.inner.lock().unwrap().calls.len(), 1);
}

#[tokio::test]
async fn gives_up_after_max_attempts_on_persistent_5xx() {
    let state = AppState::default();
    for _ in 0..5 {
        state
            .inner
            .lock()
            .unwrap()
            .responses
            .push(MockResponse::Status(500));
    }
    let base = spawn_mock(state.clone()).await;
    let fetcher = default_token_fetcher(fast_config()).unwrap();

    let err = invoke_fetcher(&fetcher, &base, 7).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("gave up after 3 attempts"), "got: {msg}");
    assert_eq!(state.inner.lock().unwrap().calls.len(), 3);
}

#[tokio::test]
async fn shared_client_works_across_calls() {
    let state = AppState::default();
    let base = spawn_mock(state.clone()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let fetcher = reqwest_token_fetcher(client, fast_config());

    invoke_fetcher(&fetcher, &base, 1).await.unwrap();
    invoke_fetcher(&fetcher, &base, 2).await.unwrap();

    let g = state.inner.lock().unwrap();
    assert_eq!(g.calls.len(), 2);
    assert_eq!(g.calls[0].installation_id, 1);
    assert_eq!(g.calls[1].installation_id, 2);
}

#[tokio::test]
async fn fetcher_yields_token_compatible_with_cache_freshness_check() {
    // The TokenCache freshness path is unit-tested in github_app.rs.
    // Here we confirm the production fetcher produces an
    // InstallationToken whose expires_at is in the future and is
    // recognised as fresh, so a TokenCache built around this fetcher
    // would actually serve cached tokens between refresh windows.
    let state = AppState::default();
    let now = SystemTime::now();
    let exp_str: chrono::DateTime<chrono::Utc> = (now + Duration::from_secs(3600)).into();
    state
        .inner
        .lock()
        .unwrap()
        .responses
        .push(MockResponse::Ok {
            token: "ghs_cached".into(),
            expires_at: exp_str.to_rfc3339(),
        });
    let base = spawn_mock(state.clone()).await;

    // Sanity-check the token URL the cache would feed the fetcher.
    let cfg = GitHubAppConfig::new(123, 456, b"unused".to_vec()).with_api_base(&base);
    assert_eq!(
        cfg.token_url(),
        format!("{base}/app/installations/456/access_tokens")
    );

    let fetcher = default_token_fetcher(fast_config()).unwrap();
    let token = invoke_fetcher(&fetcher, &base, 456).await.unwrap();
    assert_eq!(token.token, "ghs_cached");
    assert!(token.is_fresh(now, Duration::from_secs(60)));
}
