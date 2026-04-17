//! GitHub App installation-token authentication.
//!
//! Two-step flow:
//!   1. Sign a short-lived JWT with the App's private key.
//!   2. POST it to GitHub to exchange for an installation token
//!      (typically valid for ~1 hour).
//!
//! We split the HTTP exchange behind a [`TokenFetcher`] hook so this
//! crate doesn't pull in a specific HTTP client. A production
//! adapter (reqwest- or hyper-based) lives at the integration layer.
//! Tests use a stub fetcher to drive cache behaviour deterministically.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::GitError;
use crate::Result;

/// Static configuration for a GitHub App installation.
#[derive(Debug, Clone)]
pub struct GitHubAppConfig {
    pub app_id: u64,
    pub installation_id: u64,
    /// PEM-encoded RSA private key bytes for the App.
    pub private_key_pem: Vec<u8>,
    /// Override the installation-token endpoint base. Defaults to
    /// `https://api.github.com` (set for GitHub Enterprise Server).
    pub api_base: String,
}

impl GitHubAppConfig {
    pub fn new(app_id: u64, installation_id: u64, private_key_pem: Vec<u8>) -> Self {
        Self {
            app_id,
            installation_id,
            private_key_pem,
            api_base: "https://api.github.com".to_string(),
        }
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into();
        self
    }

    pub fn from_pem_file(app_id: u64, installation_id: u64, path: &Path) -> Result<Self> {
        let pem = std::fs::read(path).map_err(|e| GitError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        Ok(Self::new(app_id, installation_id, pem))
    }

    /// URL of the installation-token endpoint for this App+installation.
    pub fn token_url(&self) -> String {
        format!(
            "{}/app/installations/{}/access_tokens",
            self.api_base.trim_end_matches('/'),
            self.installation_id
        )
    }
}

/// Installation token returned by GitHub.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallationToken {
    pub token: String,
    /// Expiry as wall-clock time. Caller should treat the token as
    /// invalid a margin (`refresh_before`) ahead of this instant.
    pub expires_at: SystemTime,
}

impl InstallationToken {
    pub fn is_fresh(&self, now: SystemTime, refresh_before: Duration) -> bool {
        match self.expires_at.checked_sub(refresh_before) {
            Some(deadline) => now < deadline,
            None => false,
        }
    }
}

/// Async HTTP exchange: given a JWT, return a fresh installation
/// token. Implementations call `POST {token_url}` with
/// `Authorization: Bearer {jwt}` and parse the response.
pub type TokenFetcher = Arc<
    dyn Fn(
            String, // JWT
            String, // token URL
        ) -> Pin<Box<dyn Future<Output = Result<InstallationToken>> + Send>>
        + Send
        + Sync,
>;

/// Sign a JWT for the GitHub App. `iat` is set to `now - 60s` to
/// tolerate small clock skew between us and GitHub; `exp` is `now +
/// 9 minutes` (under GitHub's 10-minute hard cap).
pub fn mint_jwt(cfg: &GitHubAppConfig, now: SystemTime) -> Result<String> {
    let now_secs = now
        .duration_since(UNIX_EPOCH)
        .map_err(|e| GitError::InvalidState(format!("system clock before UNIX epoch: {e}")))?
        .as_secs() as i64;

    #[derive(Serialize)]
    struct Claims {
        iat: i64,
        exp: i64,
        iss: String,
    }
    let claims = Claims {
        iat: now_secs - 60,
        exp: now_secs + 9 * 60,
        iss: cfg.app_id.to_string(),
    };

    let key = EncodingKey::from_rsa_pem(&cfg.private_key_pem)
        .map_err(|e| GitError::InvalidState(format!("invalid GitHub App private key: {e}")))?;
    jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| GitError::InvalidState(format!("JWT signing failed: {e}")))
}

/// Caches an installation token and refreshes it before expiry.
///
/// `refresh_before` is the margin ahead of `expires_at` at which a
/// cached token is considered stale (typical: 60s).
pub struct TokenCache {
    cfg: GitHubAppConfig,
    fetcher: TokenFetcher,
    refresh_before: Duration,
    current: Mutex<Option<InstallationToken>>,
}

impl TokenCache {
    pub fn new(cfg: GitHubAppConfig, fetcher: TokenFetcher, refresh_before: Duration) -> Self {
        Self {
            cfg,
            fetcher,
            refresh_before,
            current: Mutex::new(None),
        }
    }

    /// Returns a fresh installation token, refreshing via the
    /// [`TokenFetcher`] if no cached token exists or the cached one is
    /// within `refresh_before` of expiry.
    pub async fn get(&self) -> Result<InstallationToken> {
        self.get_at(SystemTime::now()).await
    }

    /// Same as [`Self::get`] but with a caller-supplied `now`, so
    /// tests don't need to control wall-clock time.
    pub async fn get_at(&self, now: SystemTime) -> Result<InstallationToken> {
        let mut guard = self.current.lock().await;
        if let Some(tok) = guard.as_ref() {
            if tok.is_fresh(now, self.refresh_before) {
                return Ok(tok.clone());
            }
        }
        let jwt = mint_jwt(&self.cfg, now)?;
        let token = (self.fetcher)(jwt, self.cfg.token_url()).await?;
        *guard = Some(token.clone());
        Ok(token)
    }
}
