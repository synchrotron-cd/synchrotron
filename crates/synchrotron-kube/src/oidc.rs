//! OIDC token caching with refresh-token-driven renewal.
//!
//! Mirrors the kubeconfig OIDC `auth-provider` pattern: keep an
//! `id_token` + `refresh_token`; when the id_token is within a margin
//! of `expires_at`, exchange the refresh token at the issuer's token
//! endpoint for a fresh pair.
//!
//! The actual HTTPS exchange is hidden behind a [`Refresher`] hook so
//! this crate doesn't pull in an HTTP client. A production
//! reqwest-backed adapter is filed as a follow-up bead — until then,
//! integrators provide their own. Tests use a stub refresher to drive
//! cache state deterministically.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::Mutex;

use crate::error::KubeError;
use crate::Result;

/// Static configuration for an OIDC identity used to authenticate to
/// the API server.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// OIDC issuer URL (e.g. `https://accounts.google.com`). Used by
    /// production refreshers to discover the token endpoint via
    /// `.well-known/openid-configuration` — this crate does not perform
    /// that discovery itself.
    pub issuer_url: String,
    pub client_id: String,
    /// Optional client secret (public clients omit this).
    pub client_secret: Option<String>,
}

/// A single OIDC token pair. `expires_at` is wall-clock; treat the
/// token as stale a `refresh_before` margin earlier.
#[derive(Debug, Clone)]
pub struct OidcToken {
    pub id_token: String,
    pub refresh_token: String,
    pub expires_at: SystemTime,
}

impl OidcToken {
    pub fn is_fresh(&self, now: SystemTime, refresh_before: Duration) -> bool {
        match self.expires_at.checked_sub(refresh_before) {
            Some(deadline) => now < deadline,
            None => false,
        }
    }
}

/// Async exchange: given the OIDC config and the current refresh
/// token, return a fresh [`OidcToken`] (which generally includes a
/// rotated refresh token). Implementations POST to the issuer's
/// token endpoint with `grant_type=refresh_token`.
pub type Refresher = Arc<
    dyn Fn(
            OidcConfig,
            String, // current refresh token
        ) -> Pin<Box<dyn Future<Output = Result<OidcToken>> + Send>>
        + Send
        + Sync,
>;

/// Caches an [`OidcToken`] and refreshes it before expiry.
///
/// Concurrent `get` calls serialize on an internal mutex; only one
/// refresh fires per stale window. After a refresh the rotated
/// refresh token (per RFC 6749 §6 — issuers may rotate) is persisted
/// for the next exchange.
pub struct OidcTokenCache {
    cfg: OidcConfig,
    refresher: Refresher,
    refresh_before: Duration,
    current: Mutex<OidcToken>,
}

impl OidcTokenCache {
    /// Seed the cache with a known-good token (typically loaded from
    /// the kubeconfig auth-provider stanza on first start).
    pub fn new(
        cfg: OidcConfig,
        seed: OidcToken,
        refresher: Refresher,
        refresh_before: Duration,
    ) -> Self {
        Self {
            cfg,
            refresher,
            refresh_before,
            current: Mutex::new(seed),
        }
    }

    pub async fn get(&self) -> Result<OidcToken> {
        self.get_at(SystemTime::now()).await
    }

    /// Same as [`Self::get`] but with a caller-supplied `now` so tests
    /// don't need to control the wall clock.
    pub async fn get_at(&self, now: SystemTime) -> Result<OidcToken> {
        let mut guard = self.current.lock().await;
        if guard.is_fresh(now, self.refresh_before) {
            return Ok(guard.clone());
        }
        let refreshed = (self.refresher)(self.cfg.clone(), guard.refresh_token.clone()).await?;
        if refreshed.refresh_token.is_empty() {
            return Err(KubeError::OidcRefresh(
                "refresher returned empty refresh_token".into(),
            ));
        }
        *guard = refreshed.clone();
        Ok(refreshed)
    }
}
