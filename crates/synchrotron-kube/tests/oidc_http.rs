//! Integration test for `oidc_http`: drives the production
//! `reqwest`-based refresher against an in-process axum mock that
//! mimics OIDC discovery + the token endpoint.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use serde_json::json;
use tokio::net::TcpListener;

use synchrotron_kube::oidc::{OidcConfig, OidcToken, OidcTokenCache};
use synchrotron_kube::oidc_http::{
    default_refresher, reqwest_refresher, ReqwestRefresherConfig,
};

#[derive(Default)]
struct Recorder {
    discovery_calls: u32,
    token_calls: Vec<RecordedTokenCall>,
    token_responses: Vec<MockTokenResponse>,
}

struct RecordedTokenCall {
    form: HashMap<String, String>,
    user_agent: Option<String>,
    accept: Option<String>,
}

#[derive(Clone)]
enum MockTokenResponse {
    Ok {
        id_token: String,
        refresh_token: Option<String>,
        expires_in: Option<u64>,
    },
    Status(u16, &'static str),
}

#[derive(Clone, Default)]
struct AppState {
    inner: Arc<Mutex<Recorder>>,
    base_url: Arc<Mutex<String>>,
}

async fn discovery_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.inner.lock().unwrap().discovery_calls += 1;
    let base = state.base_url.lock().unwrap().clone();
    Json(json!({
        "issuer": base,
        "token_endpoint": format!("{base}/token"),
        "authorization_endpoint": format!("{base}/auth"),
    }))
}

async fn token_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> impl IntoResponse {
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let mut g = state.inner.lock().unwrap();
    g.token_calls.push(RecordedTokenCall {
        form,
        user_agent,
        accept,
    });
    let resp = if g.token_responses.is_empty() {
        MockTokenResponse::Ok {
            id_token: "id_default".into(),
            refresh_token: Some("refresh_default".into()),
            expires_in: Some(3600),
        }
    } else {
        g.token_responses.remove(0)
    };
    drop(g);
    match resp {
        MockTokenResponse::Ok {
            id_token,
            refresh_token,
            expires_in,
        } => {
            let mut body = json!({
                "id_token": id_token,
                "token_type": "Bearer",
            });
            if let Some(rt) = refresh_token {
                body["refresh_token"] = json!(rt);
            }
            if let Some(exp) = expires_in {
                body["expires_in"] = json!(exp);
            }
            (StatusCode::OK, Json(body)).into_response()
        }
        MockTokenResponse::Status(code, body) => {
            (StatusCode::from_u16(code).unwrap(), body).into_response()
        }
    }
}

async fn spawn_mock(state: AppState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    *state.base_url.lock().unwrap() = base.clone();
    let app = Router::new()
        .route("/.well-known/openid-configuration", get(discovery_handler))
        .route("/token", post(token_handler))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    base
}

fn fast_config() -> ReqwestRefresherConfig {
    ReqwestRefresherConfig {
        request_timeout: Duration::from_secs(5),
        max_attempts: 3,
        backoff_base: Duration::from_millis(1),
        user_agent: "synchrotron-cd-test".into(),
    }
}

fn cfg_for(base: &str) -> OidcConfig {
    OidcConfig {
        issuer_url: base.to_string(),
        client_id: "test-client".into(),
        client_secret: Some("test-secret".into()),
    }
}

async fn invoke(
    refresher: &synchrotron_kube::oidc::Refresher,
    cfg: OidcConfig,
    refresh_token: &str,
) -> synchrotron_kube::Result<OidcToken> {
    refresher(cfg, refresh_token.to_string()).await
}

#[tokio::test]
async fn happy_path_discovers_and_exchanges() {
    let state = AppState::default();
    state
        .inner
        .lock()
        .unwrap()
        .token_responses
        .push(MockTokenResponse::Ok {
            id_token: "id_v1".into(),
            refresh_token: Some("refresh_v2".into()),
            expires_in: Some(3600),
        });
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let now = SystemTime::now();
    let token = invoke(&refresher, cfg_for(&base), "refresh_v1").await.unwrap();
    assert_eq!(token.id_token, "id_v1");
    assert_eq!(token.refresh_token, "refresh_v2");
    assert!(token.expires_at >= now + Duration::from_secs(3500));

    let g = state.inner.lock().unwrap();
    assert_eq!(g.discovery_calls, 1);
    assert_eq!(g.token_calls.len(), 1);
    let call = &g.token_calls[0];
    assert_eq!(call.form.get("grant_type").map(String::as_str), Some("refresh_token"));
    assert_eq!(call.form.get("refresh_token").map(String::as_str), Some("refresh_v1"));
    assert_eq!(call.form.get("client_id").map(String::as_str), Some("test-client"));
    assert_eq!(call.form.get("client_secret").map(String::as_str), Some("test-secret"));
    assert_eq!(call.user_agent.as_deref(), Some("synchrotron-cd-test"));
    assert_eq!(call.accept.as_deref(), Some("application/json"));
}

#[tokio::test]
async fn omits_client_secret_when_unset() {
    let state = AppState::default();
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let mut cfg = cfg_for(&base);
    cfg.client_secret = None;
    invoke(&refresher, cfg, "rt").await.unwrap();

    let g = state.inner.lock().unwrap();
    assert!(!g.token_calls[0].form.contains_key("client_secret"));
}

#[tokio::test]
async fn retries_on_5xx_then_succeeds() {
    let state = AppState::default();
    {
        let mut g = state.inner.lock().unwrap();
        g.token_responses.push(MockTokenResponse::Status(503, "busy"));
        g.token_responses.push(MockTokenResponse::Status(502, "gw"));
        g.token_responses.push(MockTokenResponse::Ok {
            id_token: "id_after_retry".into(),
            refresh_token: Some("rt2".into()),
            expires_in: Some(60),
        });
    }
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let token = invoke(&refresher, cfg_for(&base), "rt1").await.unwrap();
    assert_eq!(token.id_token, "id_after_retry");
    assert_eq!(state.inner.lock().unwrap().token_calls.len(), 3);
}

#[tokio::test]
async fn retries_on_429() {
    let state = AppState::default();
    {
        let mut g = state.inner.lock().unwrap();
        g.token_responses.push(MockTokenResponse::Status(429, "slow down"));
        g.token_responses.push(MockTokenResponse::Ok {
            id_token: "id_after_429".into(),
            refresh_token: Some("rt2".into()),
            expires_in: Some(60),
        });
    }
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let token = invoke(&refresher, cfg_for(&base), "rt1").await.unwrap();
    assert_eq!(token.id_token, "id_after_429");
    assert_eq!(state.inner.lock().unwrap().token_calls.len(), 2);
}

#[tokio::test]
async fn does_not_retry_on_invalid_grant() {
    let state = AppState::default();
    state
        .inner
        .lock()
        .unwrap()
        .token_responses
        .push(MockTokenResponse::Status(400, "invalid_grant"));
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let err = invoke(&refresher, cfg_for(&base), "rt1").await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("400"), "want 400 in error, got: {msg}");
    assert!(msg.contains("invalid_grant"), "want body snippet, got: {msg}");
    assert_eq!(state.inner.lock().unwrap().token_calls.len(), 1);
}

#[tokio::test]
async fn gives_up_after_max_attempts_on_persistent_5xx() {
    let state = AppState::default();
    for _ in 0..5 {
        state
            .inner
            .lock()
            .unwrap()
            .token_responses
            .push(MockTokenResponse::Status(500, "boom"));
    }
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let err = invoke(&refresher, cfg_for(&base), "rt1").await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("gave up after 3 attempts"), "got: {msg}");
    assert_eq!(state.inner.lock().unwrap().token_calls.len(), 3);
}

#[tokio::test]
async fn preserves_refresh_token_when_response_omits_it() {
    let state = AppState::default();
    state
        .inner
        .lock()
        .unwrap()
        .token_responses
        .push(MockTokenResponse::Ok {
            id_token: "id_v1".into(),
            refresh_token: None,
            expires_in: Some(3600),
        });
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let token = invoke(&refresher, cfg_for(&base), "original_rt").await.unwrap();
    assert_eq!(
        token.refresh_token, "original_rt",
        "RFC 6749 §6: preserve current refresh token when response omits it"
    );
}

#[tokio::test]
async fn discovery_is_cached_across_refreshes() {
    let state = AppState::default();
    let base = spawn_mock(state.clone()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let refresher = reqwest_refresher(client, fast_config());

    invoke(&refresher, cfg_for(&base), "rt1").await.unwrap();
    invoke(&refresher, cfg_for(&base), "rt2").await.unwrap();
    invoke(&refresher, cfg_for(&base), "rt3").await.unwrap();

    let g = state.inner.lock().unwrap();
    assert_eq!(
        g.discovery_calls, 1,
        "discovery should be cached for the lifetime of the refresher"
    );
    assert_eq!(g.token_calls.len(), 3);
}

#[tokio::test]
async fn integrates_with_oidc_token_cache() {
    let state = AppState::default();
    state
        .inner
        .lock()
        .unwrap()
        .token_responses
        .push(MockTokenResponse::Ok {
            id_token: "id_refreshed".into(),
            refresh_token: Some("rt_refreshed".into()),
            expires_in: Some(3600),
        });
    let base = spawn_mock(state.clone()).await;
    let refresher = default_refresher(fast_config()).unwrap();

    let seed = OidcToken {
        id_token: "id_seed".into(),
        refresh_token: "rt_seed".into(),
        expires_at: SystemTime::now() - Duration::from_secs(60), // already stale
    };
    let cache = OidcTokenCache::new(
        cfg_for(&base),
        seed,
        refresher,
        Duration::from_secs(30),
    );

    let token = cache.get().await.unwrap();
    assert_eq!(token.id_token, "id_refreshed");
    assert_eq!(token.refresh_token, "rt_refreshed");

    // Second call should hit cache (token is fresh now).
    let token2 = cache.get().await.unwrap();
    assert_eq!(token2.id_token, "id_refreshed");
    assert_eq!(state.inner.lock().unwrap().token_calls.len(), 1);
}
