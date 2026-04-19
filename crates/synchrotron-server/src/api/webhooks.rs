//! Inbound webhook routes: POST /api/v1/webhooks/{github,gitlab,bitbucket}.
//!
//! Thin glue layer over [`synchrotron_git::webhooks`] — this module
//! owns the HTTP contract (status codes, state extraction) while the
//! git crate owns parse + HMAC verify. Separation keeps the
//! cryptographic path testable without spinning up axum.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use tracing::{info, warn};

use synchrotron_core::{EventBus, SystemEvent, WebhookSource};
use synchrotron_git::webhooks::{
    parse_bitbucket, parse_github, parse_gitlab, verify_bitbucket, verify_github, verify_gitlab,
    ParsedWebhook,
};
use synchrotron_git::{Orchestrator, RepoId};
use synchrotron_types::RepoUrl;

/// Per-provider shared secrets. Missing entries cause the
/// corresponding route to return 503 — we never accept an unsigned
/// webhook.
#[derive(Clone, Default)]
pub struct WebhookSecrets {
    pub github: Option<Vec<u8>>,
    pub gitlab: Option<Vec<u8>>,
    pub bitbucket: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct WebhookState {
    pub secrets: Arc<WebhookSecrets>,
    pub orchestrator: Arc<Orchestrator>,
    pub bus: EventBus,
}

pub fn router(state: WebhookState) -> Router {
    Router::new()
        .route("/api/v1/webhooks/github", post(github_handler))
        .route("/api/v1/webhooks/gitlab", post(gitlab_handler))
        .route("/api/v1/webhooks/bitbucket", post(bitbucket_handler))
        .with_state(state)
}

fn headers_pairs(headers: &HeaderMap) -> Vec<(&str, &str)> {
    headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str(), v)))
        .collect()
}

async fn github_handler(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let Some(secret) = state.secrets.github.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "github webhook secret not configured",
        );
    };
    let hdrs = headers_pairs(&headers);
    if let Err(e) = verify_github(secret, &hdrs, &body) {
        warn!(error = %e, "github webhook verification failed");
        return (StatusCode::UNAUTHORIZED, "signature verification failed");
    }
    match parse_github(&hdrs, &body) {
        Ok(parsed) => dispatch(&state, parsed, WebhookSource::GitHub).await,
        Err(e) => {
            warn!(error = %e, "github webhook parse failed");
            (StatusCode::BAD_REQUEST, "malformed payload")
        }
    }
}

async fn gitlab_handler(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let Some(secret) = state.secrets.gitlab.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "gitlab webhook secret not configured",
        );
    };
    let hdrs = headers_pairs(&headers);
    if let Err(e) = verify_gitlab(secret, &hdrs) {
        warn!(error = %e, "gitlab webhook verification failed");
        return (StatusCode::UNAUTHORIZED, "signature verification failed");
    }
    match parse_gitlab(&hdrs, &body) {
        Ok(parsed) => dispatch(&state, parsed, WebhookSource::GitLab).await,
        Err(e) => {
            warn!(error = %e, "gitlab webhook parse failed");
            (StatusCode::BAD_REQUEST, "malformed payload")
        }
    }
}

async fn bitbucket_handler(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let Some(secret) = state.secrets.bitbucket.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "bitbucket webhook secret not configured",
        );
    };
    let hdrs = headers_pairs(&headers);
    if let Err(e) = verify_bitbucket(secret, &hdrs, &body) {
        warn!(error = %e, "bitbucket webhook verification failed");
        return (StatusCode::UNAUTHORIZED, "signature verification failed");
    }
    match parse_bitbucket(&hdrs, &body) {
        Ok(parsed) => dispatch(&state, parsed, WebhookSource::Bitbucket).await,
        Err(e) => {
            warn!(error = %e, "bitbucket webhook parse failed");
            (StatusCode::BAD_REQUEST, "malformed payload")
        }
    }
}

async fn dispatch(
    state: &WebhookState,
    parsed: ParsedWebhook,
    source: WebhookSource,
) -> (StatusCode, &'static str) {
    if !parsed.is_push() {
        // Non-push (ping, tag_push without refs, etc.) is fine —
        // acknowledge so the forge stops retrying, but don't trigger.
        return (StatusCode::ACCEPTED, "ignored non-push event");
    }
    let repo_id = RepoId::from_url(&RepoUrl(parsed.repo_url.0.clone()));
    let triggered = state.orchestrator.trigger(&repo_id).await;
    if !triggered {
        // Signature verified but the repo isn't registered with us —
        // either the caller misconfigured the forge or Synchrotron
        // isn't watching this repo. 202 (not 404) because the forge
        // shouldn't retry.
        info!(repo = %parsed.repo_url.0, "webhook for unregistered repo; ignoring");
        state.bus.publish(SystemEvent::WebhookTriggered {
            repo: repo_id.as_str().to_string(),
            source,
        });
        return (StatusCode::ACCEPTED, "no matching repo; ignored");
    }
    state.bus.publish(SystemEvent::WebhookTriggered {
        repo: repo_id.as_str().to_string(),
        source,
    });
    (StatusCode::ACCEPTED, "trigger enqueued")
}
