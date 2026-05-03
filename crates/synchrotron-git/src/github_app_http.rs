//! Production `reqwest`-based [`TokenFetcher`] for GitHub App
//! installation tokens.
//!
//! [`github_app::TokenCache`] holds the cache and refresh logic; this
//! module ships the HTTP exchange that turns a signed JWT into a
//! GitHub installation token.
//!
//! Wire format (GitHub REST v3):
//!
//! ```text
//! POST {api_base}/app/installations/{installation_id}/access_tokens
//! Authorization: Bearer <jwt>
//! Accept: application/vnd.github+json
//! User-Agent: synchrotron-cd
//! X-GitHub-Api-Version: 2022-11-28
//!
//! 201 Created
//! { "token": "ghs_…", "expires_at": "2026-05-03T12:34:56Z", … }
//! ```
//!
//! # Retry policy
//!
//! Bounded exponential backoff on transient errors (`5xx`, network
//! failure). Auth (`401`/`403`) and `404` short-circuit immediately —
//! retrying a clock-skewed JWT or a wrong installation ID won't help
//! and just delays the actionable error to the caller.
//!
//! # Errors
//!
//! Failures collapse to [`GitError::InvalidState`] with a message that
//! includes the HTTP status (where applicable) and any GitHub-supplied
//! error body, so log lines tell the operator which knob to turn
//! (rotate the private key, fix the installation ID, restore network).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use tracing::{debug, warn};

use crate::error::GitError;
use crate::github_app::{InstallationToken, TokenFetcher};
use crate::Result;

/// Tunables for the production fetcher.
#[derive(Debug, Clone)]
pub struct ReqwestFetcherConfig {
    /// Per-request HTTP timeout.
    pub request_timeout: Duration,
    /// Total attempts (including the first try). `1` disables retry.
    pub max_attempts: u32,
    /// Initial sleep between retries; doubles each subsequent attempt.
    pub backoff_base: Duration,
    /// Value of the `User-Agent` header. GitHub requires a non-empty UA.
    pub user_agent: String,
}

impl Default for ReqwestFetcherConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            max_attempts: 3,
            backoff_base: Duration::from_millis(200),
            user_agent: "synchrotron-cd".to_string(),
        }
    }
}

/// Build a [`TokenFetcher`] backed by an existing `reqwest::Client`.
/// Sharing a client across fetchers (and across the rest of the
/// process) is the right move — the client owns a connection pool.
pub fn reqwest_token_fetcher(
    client: reqwest::Client,
    config: ReqwestFetcherConfig,
) -> TokenFetcher {
    let cfg = Arc::new(config);
    Arc::new(move |jwt: String, url: String| {
        let client = client.clone();
        let cfg = Arc::clone(&cfg);
        Box::pin(async move { exchange(&client, &cfg, &jwt, &url).await })
    })
}

/// Convenience wrapper that builds a default `reqwest::Client` with
/// the configured timeout. Use [`reqwest_token_fetcher`] if you want
/// to share a client across subsystems.
pub fn default_token_fetcher(config: ReqwestFetcherConfig) -> Result<TokenFetcher> {
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()
        .map_err(|e| GitError::InvalidState(format!("reqwest client build failed: {e}")))?;
    Ok(reqwest_token_fetcher(client, config))
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    token: String,
    expires_at: String,
}

async fn exchange(
    client: &reqwest::Client,
    cfg: &ReqwestFetcherConfig,
    jwt: &str,
    url: &str,
) -> Result<InstallationToken> {
    let mut delay = cfg.backoff_base;
    let mut last_err: Option<String> = None;
    for attempt in 1..=cfg.max_attempts {
        let resp = client
            .post(url)
            .bearer_auth(jwt)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", &cfg.user_agent)
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    let body: TokenResponse = r.json().await.map_err(|e| {
                        GitError::InvalidState(format!(
                            "failed to parse GitHub token response: {e}"
                        ))
                    })?;
                    let expires_at = parse_rfc3339(&body.expires_at)?;
                    debug!(attempt, "github installation token acquired");
                    return Ok(InstallationToken {
                        token: body.token,
                        expires_at,
                    });
                }
                // Read the body for context, but cap it so a malicious
                // or runaway server can't OOM us.
                let body = r.text().await.unwrap_or_default();
                let snippet = body.chars().take(512).collect::<String>();
                if !is_retryable_status(status) {
                    return Err(GitError::InvalidState(format!(
                        "github installation-token request failed: HTTP {status}: {snippet}"
                    )));
                }
                last_err = Some(format!("HTTP {status}: {snippet}"));
                warn!(attempt, %status, "github token endpoint returned 5xx; will retry");
            }
            Err(e) => {
                last_err = Some(e.to_string());
                warn!(attempt, error = %e, "github token request failed; will retry");
            }
        }
        if attempt == cfg.max_attempts {
            break;
        }
        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2);
    }
    Err(GitError::InvalidState(format!(
        "github installation-token request gave up after {} attempts: {}",
        cfg.max_attempts,
        last_err.unwrap_or_else(|| "no error captured".into())
    )))
}

fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

fn parse_rfc3339(s: &str) -> Result<SystemTime> {
    let dt = chrono::DateTime::parse_from_rfc3339(s).map_err(|e| {
        GitError::InvalidState(format!(
            "github expires_at not RFC3339 (`{s}`): {e}"
        ))
    })?;
    Ok(SystemTime::from(dt.with_timezone(&chrono::Utc)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rfc3339_round_trips_utc() {
        let t = parse_rfc3339("2026-05-03T12:34:56Z").unwrap();
        let back: chrono::DateTime<chrono::Utc> = t.into();
        assert_eq!(back.to_rfc3339(), "2026-05-03T12:34:56+00:00");
    }

    #[test]
    fn parse_rfc3339_rejects_garbage() {
        let err = parse_rfc3339("not a timestamp").unwrap_err();
        assert!(matches!(err, GitError::InvalidState(_)));
    }

    #[test]
    fn retryable_status_classification() {
        assert!(is_retryable_status(reqwest::StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(reqwest::StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_retryable_status(reqwest::StatusCode::UNAUTHORIZED));
        assert!(!is_retryable_status(reqwest::StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(reqwest::StatusCode::OK));
    }
}
