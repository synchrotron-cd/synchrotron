//! Loading, validating, and hot-reloading config.
//!
//! [`ConfigHandle`] holds an `Arc<RwLock<Arc<Config>>>`. Readers
//! call [`ConfigHandle::current`] to grab a cheap snapshot
//! (`Arc<Config>`) that they can hold across awaits without
//! blocking writers. SIGHUP swaps the inner `Arc` atomically — a
//! reader either sees the old config in full or the new one in
//! full, never a torn mix.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::RwLock;

use super::schema::Config;
use super::validate::{validate, ValidationError};

#[derive(Debug, Error)]
pub enum ReloadError {
    #[error("read config {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parse config: {0}")]
    Parse(#[from] serde_yaml_ng::Error),
    #[error("invalid config: {0:?}")]
    Validation(Vec<ValidationError>),
}

/// Read `path`, parse YAML, run validation. The single entry point
/// any startup or reload codepath should use.
pub fn load_and_validate(path: &Path) -> Result<Config, ReloadError> {
    let text = std::fs::read_to_string(path).map_err(|source| ReloadError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let cfg: Config = serde_yaml_ng::from_str(&text)?;
    validate(&cfg).map_err(ReloadError::Validation)?;
    Ok(cfg)
}

/// Cheap-to-clone handle to the current config snapshot.
#[derive(Debug, Clone)]
pub struct ConfigHandle {
    inner: Arc<RwLock<Arc<Config>>>,
    path: Arc<PathBuf>,
}

impl ConfigHandle {
    /// Load `path`, validate, and wrap in a handle.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, ReloadError> {
        let path = path.into();
        let cfg = load_and_validate(&path)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(cfg))),
            path: Arc::new(path),
        })
    }

    /// Build a handle from an in-memory `Config`. Useful for tests
    /// and for binaries that synthesize a default config when no
    /// file is given.
    pub fn from_config(cfg: Config, path: impl Into<PathBuf>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(cfg))),
            path: Arc::new(path.into()),
        }
    }

    /// Snapshot of the active config. Holding the returned `Arc`
    /// across reloads is safe — it pins the old version.
    pub async fn current(&self) -> Arc<Config> {
        self.inner.read().await.clone()
    }

    /// Re-read the file at the original path and atomically swap
    /// in the new config. On failure the old config stays active.
    pub async fn reload(&self) -> Result<(), ReloadError> {
        let cfg = load_and_validate(&self.path)?;
        let mut guard = self.inner.write().await;
        *guard = Arc::new(cfg);
        Ok(())
    }
}

/// Spawn a task that reloads the handle on every SIGHUP. Errors
/// during reload are logged via `tracing` but do not terminate the
/// listener — the operator gets a chance to fix the file and
/// SIGHUP again.
#[cfg(unix)]
pub fn spawn_sighup_reloader(handle: ConfigHandle) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sig = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGHUP handler");
                return;
            }
        };
        while sig.recv().await.is_some() {
            match handle.reload().await {
                Ok(()) => tracing::info!("config reloaded on SIGHUP"),
                Err(e) => tracing::error!(error = %e, "config reload failed; keeping previous"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_cfg(text: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(text.as_bytes()).expect("write");
        f
    }

    #[tokio::test]
    async fn load_and_swap() {
        let f = write_cfg("server:\n  listen_addr: 127.0.0.1:9000\n");
        let h = ConfigHandle::load(f.path()).expect("load");
        assert_eq!(h.current().await.server.listen_addr, "127.0.0.1:9000");

        std::fs::write(f.path(), "server:\n  listen_addr: 127.0.0.1:9001\n").expect("rewrite");
        h.reload().await.expect("reload");
        assert_eq!(h.current().await.server.listen_addr, "127.0.0.1:9001");
    }

    #[tokio::test]
    async fn reload_failure_preserves_old_config() {
        let f = write_cfg("server:\n  listen_addr: 127.0.0.1:9000\n");
        let h = ConfigHandle::load(f.path()).expect("load");

        std::fs::write(f.path(), "server:\n  listen_addr: \"\"\n").expect("rewrite");
        let err = h.reload().await.unwrap_err();
        assert!(matches!(err, ReloadError::Validation(_)));
        assert_eq!(h.current().await.server.listen_addr, "127.0.0.1:9000");
    }

    #[test]
    fn parse_error_surfaces() {
        let f = write_cfg("server: : :\n");
        let err = ConfigHandle::load(f.path()).unwrap_err();
        assert!(matches!(err, ReloadError::Parse(_)));
    }

    #[test]
    fn missing_file_is_io_error() {
        let err = ConfigHandle::load("/nonexistent/synchrotron/cfg.yaml").unwrap_err();
        assert!(matches!(err, ReloadError::Io { .. }));
    }

    #[tokio::test]
    async fn snapshot_is_pinned_across_reload() {
        let f = write_cfg("server:\n  listen_addr: 127.0.0.1:9000\n");
        let h = ConfigHandle::load(f.path()).expect("load");
        let snap = h.current().await;

        std::fs::write(f.path(), "server:\n  listen_addr: 127.0.0.1:9001\n").expect("rewrite");
        h.reload().await.expect("reload");

        assert_eq!(snap.server.listen_addr, "127.0.0.1:9000");
        assert_eq!(h.current().await.server.listen_addr, "127.0.0.1:9001");
    }
}
