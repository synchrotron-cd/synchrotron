//! Production `reqwest`-based [`Refresher`] for OIDC token exchange.
//!
//! [`oidc::OidcTokenCache`] holds the cache and refresh-window logic;
//! this module ships the HTTPS exchange that turns a refresh token
//! into a fresh `id_token` + (possibly rotated) refresh token.
//!
//! # Token endpoint discovery
//!
//! The endpoint is discovered via OpenID Connect Discovery 1.0:
//!
//! ```text
//! GET {issuer}/.well-known/openid-configuration
//! → { "token_endpoint": "...", … }
//! ```
//!
//! Discovery is cached per-issuer for the lifetime of the
//! [`Refresher`] — issuers don't reshuffle their endpoints at
//! runtime, and re-fetching on every refresh would double network
//! load and add a failure mode for transient discovery glitches.
//!
//! # Refresh request
//!
//! ```text
//! POST {token_endpoint}
//! Content-Type: application/x-www-form-urlencoded
//!
//! grant_type=refresh_token
//! &refresh_token=<current>
//! &client_id=<client_id>
//! &client_secret=<optional>
//! ```
//!
//! Per RFC 6749 §6, the response **may** rotate the refresh token —
//! when present the new one is returned to [`OidcTokenCache`] so the
//! next exchange uses the rotated value. When absent, the existing
//! refresh token is preserved.
//!
//! # Retry policy
//!
//! Bounded exponential backoff on `5xx` and network failures.
//! `4xx` short-circuits — `invalid_grant` (revoked/expired refresh
//! token) won't fix itself; the operator needs to re-bootstrap the
//! kubeconfig stanza.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::error::KubeError;
use crate::oidc::{OidcConfig, OidcToken, Refresher};
use crate::Result;

/// Tunables for the production refresher.
#[derive(Debug, Clone)]
pub struct ReqwestRefresherConfig {
    pub request_timeout: Duration,
    /// Total attempts (including the first try). `1` disables retry.
    pub max_attempts: u32,
    /// Initial sleep between retries; doubles each subsequent attempt.
    pub backoff_base: Duration,
    pub user_agent: String,
}

impl Default for ReqwestRefresherConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
            max_attempts: 3,
            backoff_base: Duration::from_millis(200),
            user_agent: "synchrotron-cd".to_string(),
        }
    }
}

/// Build a [`Refresher`] backed by an existing `reqwest::Client`.
/// Sharing a client across refreshers (and across other HTTP
/// subsystems) is encouraged — the client owns a connection pool.
///
/// Discovery results are cached internally per issuer URL.
pub fn reqwest_refresher(
    client: reqwest::Client,
    config: ReqwestRefresherConfig,
) -> Refresher {
    let inner = Arc::new(Inner {
        client,
        config,
        discovery: RwLock::new(HashMap::new()),
    });
    Arc::new(move |cfg, refresh_token| {
        let inner = Arc::clone(&inner);
        Box::pin(async move { inner.refresh(cfg, refresh_token).await })
    })
}

/// Convenience wrapper that builds a default `reqwest::Client` with
/// the configured timeout. Use [`reqwest_refresher`] when you want to
/// share a client across subsystems.
pub fn default_refresher(config: ReqwestRefresherConfig) -> Result<Refresher> {
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()
        .map_err(|e| KubeError::OidcRefresh(format!("reqwest client build failed: {e}")))?;
    Ok(reqwest_refresher(client, config))
}

struct Inner {
    client: reqwest::Client,
    config: ReqwestRefresherConfig,
    discovery: RwLock<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryDoc {
    token_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    /// If present per RFC 6749 §6, replaces the cached refresh token.
    refresh_token: Option<String>,
    /// Lifetime in seconds. OIDC providers commonly omit this for
    /// rotated refresh tokens; the id_token's own `exp` claim is
    /// authoritative, but parsing JWTs here would force another dep.
    /// We treat it as required for the refresh-window math; if absent
    /// we conservatively assume 5 minutes so the next refresh fires
    /// soon (better stale than wedged).
    expires_in: Option<u64>,
}

impl Inner {
    async fn refresh(&self, cfg: OidcConfig, current_refresh: String) -> Result<OidcToken> {
        let endpoint = self.token_endpoint(&cfg.issuer_url).await?;
        let now = SystemTime::now();
        let resp = self
            .post_with_retry(&endpoint, &cfg, &current_refresh)
            .await?;
        let id_token = resp
            .id_token
            .or(resp.access_token)
            .ok_or_else(|| {
                KubeError::OidcRefresh(
                    "OIDC token response missing both id_token and access_token".into(),
                )
            })?;
        let lifetime = resp.expires_in.unwrap_or(300);
        let expires_at = now + Duration::from_secs(lifetime);
        let refresh_token = resp.refresh_token.unwrap_or(current_refresh);
        Ok(OidcToken {
            id_token,
            refresh_token,
            expires_at,
        })
    }

    async fn token_endpoint(&self, issuer: &str) -> Result<String> {
        if let Some(cached) = self.discovery.read().await.get(issuer).cloned() {
            return Ok(cached);
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );
        let resp = self
            .client
            .get(&url)
            .header("User-Agent", &self.config.user_agent)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| {
                KubeError::OidcRefresh(format!("OIDC discovery GET {url} failed: {e}"))
            })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(KubeError::OidcRefresh(format!(
                "OIDC discovery returned HTTP {status}: {}",
                body.chars().take(256).collect::<String>()
            )));
        }
        let doc: DiscoveryDoc = resp.json().await.map_err(|e| {
            KubeError::OidcRefresh(format!("OIDC discovery body not JSON/missing token_endpoint: {e}"))
        })?;
        debug!(issuer, token_endpoint = %doc.token_endpoint, "OIDC discovery cached");
        self.discovery
            .write()
            .await
            .insert(issuer.to_string(), doc.token_endpoint.clone());
        Ok(doc.token_endpoint)
    }

    async fn post_with_retry(
        &self,
        endpoint: &str,
        cfg: &OidcConfig,
        current_refresh: &str,
    ) -> Result<TokenResponse> {
        let mut delay = self.config.backoff_base;
        let mut last_err: Option<String> = None;
        for attempt in 1..=self.config.max_attempts {
            let mut form: Vec<(&str, &str)> = vec![
                ("grant_type", "refresh_token"),
                ("refresh_token", current_refresh),
                ("client_id", &cfg.client_id),
            ];
            if let Some(secret) = cfg.client_secret.as_deref() {
                form.push(("client_secret", secret));
            }
            let result = self
                .client
                .post(endpoint)
                .header("User-Agent", &self.config.user_agent)
                .header("Accept", "application/json")
                .form(&form)
                .send()
                .await;
            match result {
                Ok(r) => {
                    let status = r.status();
                    if status.is_success() {
                        let body: TokenResponse = r.json().await.map_err(|e| {
                            KubeError::OidcRefresh(format!(
                                "OIDC token response not JSON: {e}"
                            ))
                        })?;
                        return Ok(body);
                    }
                    let body = r.text().await.unwrap_or_default();
                    let snippet = body.chars().take(512).collect::<String>();
                    if !is_retryable_status(status) {
                        return Err(KubeError::OidcRefresh(format!(
                            "OIDC token endpoint returned HTTP {status}: {snippet}"
                        )));
                    }
                    last_err = Some(format!("HTTP {status}: {snippet}"));
                    warn!(attempt, %status, "OIDC token endpoint 5xx; will retry");
                }
                Err(e) => {
                    last_err = Some(e.to_string());
                    warn!(attempt, error = %e, "OIDC token request failed; will retry");
                }
            }
            if attempt == self.config.max_attempts {
                break;
            }
            tokio::time::sleep(delay).await;
            delay = delay.saturating_mul(2);
        }
        Err(KubeError::OidcRefresh(format!(
            "OIDC token endpoint gave up after {} attempts: {}",
            self.config.max_attempts,
            last_err.unwrap_or_else(|| "no error captured".into())
        )))
    }
}

fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_status_classification() {
        assert!(is_retryable_status(reqwest::StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(reqwest::StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_retryable_status(reqwest::StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(reqwest::StatusCode::UNAUTHORIZED));
        assert!(!is_retryable_status(reqwest::StatusCode::OK));
    }
}
