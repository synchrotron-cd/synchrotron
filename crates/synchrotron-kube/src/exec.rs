//! Kubeconfig `exec` credential plugins (aws-iam-authenticator,
//! gke-gcloud-auth-plugin, etc.).
//!
//! The plugin is invoked as a subprocess per the
//! `client.authentication.k8s.io/v1` ExecCredential spec; its stdout
//! is parsed into an [`ExecCredential`] and the contained token is
//! cached until its `expirationTimestamp`.
//!
//! The subprocess invocation is hidden behind a [`Runner`] hook so
//! the cache can be unit-tested without forking real binaries. A
//! production runner backed by `tokio::process::Command` is provided
//! as [`tokio_command_runner`].

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::error::KubeError;
use crate::Result;

/// Subset of the kubeconfig ExecConfig fields we need to invoke the
/// plugin. `install_hint`, `provide_cluster_info`, and
/// `interactive_mode` are intentionally omitted — Synchrotron always
/// runs non-interactively and never forwards cluster info.
#[derive(Debug, Clone)]
pub struct ExecPluginConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    /// API version the plugin is invoked with (passed via the
    /// `KUBERNETES_EXEC_INFO` env var per the spec). Defaults to
    /// `client.authentication.k8s.io/v1`.
    pub api_version: String,
}

impl ExecPluginConfig {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            api_version: "client.authentication.k8s.io/v1".into(),
        }
    }
}

/// Parsed plugin response. We only model the `status` fields the
/// client actually consumes — token + expiry. Client-cert auth via
/// exec plugins is not in scope for this slice (filed as a follow-up
/// if needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecCredential {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub status: ExecCredentialStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecCredentialStatus {
    pub token: String,
    /// RFC3339 timestamp. Absent means the plugin doesn't manage
    /// expiry and the token should be treated as never-cached (call
    /// the plugin every time).
    #[serde(rename = "expirationTimestamp", default)]
    pub expiration_timestamp: Option<DateTime<Utc>>,
}

impl ExecCredentialStatus {
    pub fn expires_at(&self) -> Option<SystemTime> {
        self.expiration_timestamp.map(SystemTime::from)
    }

    pub fn is_fresh(&self, now: SystemTime, refresh_before: Duration) -> bool {
        match self.expires_at() {
            Some(exp) => match exp.checked_sub(refresh_before) {
                Some(deadline) => now < deadline,
                None => false,
            },
            // No expiry → never cache.
            None => false,
        }
    }
}

/// Async subprocess runner. Implementations spawn `cfg.command` with
/// `cfg.args` and `cfg.env`, wait for it to exit, and return stdout
/// as a UTF-8 string (or an error if the plugin failed).
pub type Runner = Arc<
    dyn Fn(ExecPluginConfig) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync,
>;

/// Production runner: invokes the plugin via `tokio::process::Command`,
/// passing through `args` and `env` and capturing stdout. Non-zero
/// exit codes return a [`KubeError::ExecPlugin`] including a
/// truncated stderr for diagnostics.
pub fn tokio_command_runner() -> Runner {
    Arc::new(|cfg: ExecPluginConfig| {
        Box::pin(async move {
            let output = Command::new(&cfg.command)
                .args(&cfg.args)
                .envs(&cfg.env)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .map_err(|e| {
                    KubeError::ExecPlugin(format!("failed to spawn {:?}: {e}", cfg.command))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let snippet: String = stderr.chars().take(512).collect();
                return Err(KubeError::ExecPlugin(format!(
                    "{} exited with {}: {snippet}",
                    cfg.command, output.status,
                )));
            }
            String::from_utf8(output.stdout).map_err(|e| {
                KubeError::ExecPlugin(format!(
                    "plugin {} produced non-UTF-8 stdout: {e}",
                    cfg.command
                ))
            })
        })
    })
}

/// Caches the token returned by an exec plugin and re-invokes it when
/// the cached token is within `refresh_before` of expiry (or absent).
pub struct ExecCredentialCache {
    cfg: ExecPluginConfig,
    runner: Runner,
    refresh_before: Duration,
    current: Mutex<Option<ExecCredentialStatus>>,
}

impl ExecCredentialCache {
    pub fn new(cfg: ExecPluginConfig, runner: Runner, refresh_before: Duration) -> Self {
        Self {
            cfg,
            runner,
            refresh_before,
            current: Mutex::new(None),
        }
    }

    pub async fn token(&self) -> Result<String> {
        self.token_at(SystemTime::now()).await
    }

    /// Same as [`Self::token`] with an injected `now` for tests.
    pub async fn token_at(&self, now: SystemTime) -> Result<String> {
        let mut guard = self.current.lock().await;
        if let Some(status) = guard.as_ref() {
            if status.is_fresh(now, self.refresh_before) {
                return Ok(status.token.clone());
            }
        }
        let stdout = (self.runner)(self.cfg.clone()).await?;
        let cred: ExecCredential = serde_json::from_str(&stdout).map_err(|e| {
            KubeError::ExecPlugin(format!(
                "plugin {} returned malformed ExecCredential JSON: {e}",
                self.cfg.command
            ))
        })?;
        if cred.kind != "ExecCredential" {
            return Err(KubeError::ExecPlugin(format!(
                "expected kind=ExecCredential, got {:?}",
                cred.kind
            )));
        }
        if cred.status.token.is_empty() {
            return Err(KubeError::ExecPlugin("plugin returned empty token".into()));
        }
        let token = cred.status.token.clone();
        *guard = Some(cred.status);
        Ok(token)
    }
}
