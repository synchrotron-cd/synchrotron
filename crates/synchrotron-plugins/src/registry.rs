//! Plugin registry — loads the declarative plugin config, validates
//! it, and dispatches render requests to the right runtime.
//!
//! # Config schema
//!
//! The on-disk format is YAML (same family as the Application CRD).
//! A minimal example:
//!
//! ```yaml
//! plugins:
//!   - name: raw
//!     kind: raw
//!
//!   - name: helm
//!     kind: local
//!     command: /usr/local/bin/synchrotron-helm-plugin
//!     args: []
//!     env: {}
//!     timeout_secs: 30
//!
//!   - name: jsonnet
//!     kind: sidecar
//!     endpoint: "http://jsonnet-sidecar:8080"
//! ```
//!
//! Names must be unique across the file. Each `kind` constrains the
//! required fields: `raw` takes no further config; `local` requires
//! `command` (absolute or on `$PATH`); `sidecar` requires `endpoint`.
//! Unknown kinds, missing required fields, or duplicate names are
//! rejected with a message naming the offending plugin so operators
//! can fix the config without grepping.
//!
//! # Hot reload
//!
//! [`Registry::reload_from_path`] re-parses the file and atomically
//! swaps the name → config map. In-flight render calls see the old
//! config; subsequent calls see the new one. No process churn yet —
//! the local runtime currently spawns per render, so there is
//! nothing to restart.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;
use tracing::info;

use crate::local::{Plugin, PluginError, PluginSpec, RenderRequest};
use crate::manifest::{parse_stream, Manifest, ManifestParseError};
use crate::raw;

/// Default per-request timeout for local plugins when the config
/// omits `timeout_secs`. Generous enough for Helm's slowest charts
/// without being so long that a stuck plugin wedges reconciliation.
const DEFAULT_LOCAL_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq)]
pub enum PluginKind {
    /// Built-in raw YAML directory source — no subprocess.
    Raw,
    /// Subprocess speaking JSON-RPC over stdio.
    Local {
        command: PathBuf,
        args: Vec<String>,
        env: Vec<(String, String)>,
        timeout: Duration,
    },
    /// Sidecar container reached over gRPC. Runtime is h48.3.3;
    /// dispatch currently returns [`DispatchError::Unimplemented`].
    Sidecar { endpoint: String },
}

#[derive(Debug, Clone)]
pub struct PluginConfig {
    pub name: String,
    pub kind: PluginKind,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("i/o error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("yaml parse error: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    #[error("duplicate plugin name `{name}`")]
    DuplicateName { name: String },
    #[error("plugin `{name}`: {message}")]
    Invalid { name: String, message: String },
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error("unknown plugin `{name}`")]
    Unknown { name: String },
    #[error("plugin `{name}`: sidecar runtime is not yet implemented")]
    Unimplemented { name: String },
    #[error("raw source: {0}")]
    Raw(#[from] raw::RawLoadError),
    #[error("local plugin: {0}")]
    Local(#[from] PluginError),
    #[error(transparent)]
    Parse(#[from] ManifestParseError),
}

#[derive(Clone, Debug)]
pub struct Registry {
    inner: Arc<RwLock<HashMap<String, PluginConfig>>>,
}

impl Registry {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Build from a YAML document in memory. Separated from
    /// `from_path` so tests don't need a temp file.
    pub fn from_yaml(text: &str) -> Result<Self, ConfigError> {
        let map = parse_config(text)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(map)),
        })
    }

    pub fn from_path(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_yaml(&text)
    }

    /// Replace the in-memory config with the contents of `path`.
    /// On parse or validation failure the existing config is
    /// preserved — a bad reload never leaves the registry empty.
    pub fn reload_from_path(&self, path: &Path) -> Result<(), ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let new_map = parse_config(&text)?;
        info!(path = %path.display(), plugins = new_map.len(), "plugin registry reloaded");
        let mut guard = self.inner.write().expect("registry rwlock poisoned");
        *guard = new_map;
        Ok(())
    }

    pub fn names(&self) -> Vec<String> {
        let guard = self.inner.read().expect("registry rwlock poisoned");
        let mut names: Vec<String> = guard.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn get(&self, name: &str) -> Option<PluginConfig> {
        let guard = self.inner.read().expect("registry rwlock poisoned");
        guard.get(name).cloned()
    }

    /// Render an application's manifests through the named plugin.
    /// Each runtime returns [`Manifest`]s; the caller doesn't need to
    /// know which runtime served the request.
    pub async fn render(
        &self,
        plugin_name: &str,
        source_path: &Path,
        params: serde_json::Value,
    ) -> Result<Vec<Manifest>, DispatchError> {
        let config = self
            .get(plugin_name)
            .ok_or_else(|| DispatchError::Unknown {
                name: plugin_name.to_string(),
            })?;
        match config.kind {
            PluginKind::Raw => Ok(raw::load_dir(source_path)?),
            PluginKind::Local {
                command,
                args,
                env,
                timeout,
            } => {
                let spec = PluginSpec {
                    name: config.name.clone(),
                    command,
                    args,
                    env,
                    workdir: None,
                    timeout,
                };
                let mut plugin = Plugin::spawn(spec).await?;
                let req = RenderRequest {
                    source_path: source_path.to_path_buf(),
                    params,
                };
                let resp = plugin.render(&req).await?;
                let _ = plugin.shutdown().await;
                let mut out = Vec::new();
                for (idx, doc) in resp.manifests.iter().enumerate() {
                    let label = format!("{}#{idx}", config.name);
                    out.extend(parse_stream(&label, doc)?);
                }
                Ok(out)
            }
            PluginKind::Sidecar { .. } => Err(DispatchError::Unimplemented {
                name: config.name.clone(),
            }),
        }
    }
}

// --- Config deserialization --------------------------------------------

#[derive(Debug, Deserialize)]
struct RootDoc {
    #[serde(default)]
    plugins: Vec<RawPlugin>,
}

#[derive(Debug, Deserialize)]
struct RawPlugin {
    name: String,
    kind: String,
    #[serde(default)]
    command: Option<PathBuf>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    endpoint: Option<String>,
}

fn parse_config(text: &str) -> Result<HashMap<String, PluginConfig>, ConfigError> {
    let doc: RootDoc = serde_yaml_ng::from_str(text)?;
    let mut map = HashMap::new();
    let mut seen: HashSet<String> = HashSet::new();
    for raw in doc.plugins {
        if !seen.insert(raw.name.clone()) {
            return Err(ConfigError::DuplicateName {
                name: raw.name.clone(),
            });
        }
        map.insert(raw.name.clone(), raw.validate()?);
    }
    Ok(map)
}

impl RawPlugin {
    fn validate(self) -> Result<PluginConfig, ConfigError> {
        let name = self.name.clone();
        let kind = match self.kind.as_str() {
            "raw" => {
                reject_field(&name, "command", self.command.is_some())?;
                reject_field(&name, "endpoint", self.endpoint.is_some())?;
                PluginKind::Raw
            }
            "local" => {
                let command = self.command.ok_or_else(|| ConfigError::Invalid {
                    name: name.clone(),
                    message: "kind: local requires `command`".into(),
                })?;
                reject_field(&name, "endpoint", self.endpoint.is_some())?;
                let timeout = self
                    .timeout_secs
                    .map(Duration::from_secs)
                    .unwrap_or(DEFAULT_LOCAL_TIMEOUT);
                PluginKind::Local {
                    command,
                    args: self.args,
                    env: self.env.into_iter().collect(),
                    timeout,
                }
            }
            "sidecar" => {
                let endpoint = self.endpoint.ok_or_else(|| ConfigError::Invalid {
                    name: name.clone(),
                    message: "kind: sidecar requires `endpoint`".into(),
                })?;
                reject_field(&name, "command", self.command.is_some())?;
                PluginKind::Sidecar { endpoint }
            }
            other => {
                return Err(ConfigError::Invalid {
                    name,
                    message: format!("unknown kind `{other}` (expected raw, local, or sidecar)"),
                });
            }
        };
        Ok(PluginConfig { name, kind })
    }
}

fn reject_field(name: &str, field: &str, present: bool) -> Result<(), ConfigError> {
    if present {
        Err(ConfigError::Invalid {
            name: name.to_string(),
            message: format!("field `{field}` is not allowed for this kind"),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_raw_plugin() {
        let reg = Registry::from_yaml("plugins:\n  - name: raw\n    kind: raw\n").unwrap();
        assert_eq!(reg.names(), vec!["raw"]);
        assert!(matches!(reg.get("raw").unwrap().kind, PluginKind::Raw));
    }

    #[test]
    fn parses_local_plugin_with_defaults() {
        let reg = Registry::from_yaml(
            "plugins:\n  - name: helm\n    kind: local\n    command: /bin/echo\n",
        )
        .unwrap();
        match reg.get("helm").unwrap().kind {
            PluginKind::Local {
                command,
                timeout,
                args,
                ..
            } => {
                assert_eq!(command, PathBuf::from("/bin/echo"));
                assert_eq!(timeout, DEFAULT_LOCAL_TIMEOUT);
                assert!(args.is_empty());
            }
            _ => panic!("expected Local"),
        }
    }

    #[test]
    fn parses_sidecar_plugin() {
        let reg = Registry::from_yaml(
            "plugins:\n  - name: js\n    kind: sidecar\n    endpoint: http://x:1\n",
        )
        .unwrap();
        assert!(matches!(
            reg.get("js").unwrap().kind,
            PluginKind::Sidecar { .. }
        ));
    }

    #[test]
    fn rejects_unknown_kind() {
        let err = Registry::from_yaml("plugins:\n  - name: x\n    kind: wat\n").unwrap_err();
        match err {
            ConfigError::Invalid { name, message } => {
                assert_eq!(name, "x");
                assert!(message.contains("unknown kind"), "{message}");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn rejects_local_without_command() {
        let err = Registry::from_yaml("plugins:\n  - name: helm\n    kind: local\n").unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }));
    }

    #[test]
    fn rejects_sidecar_without_endpoint() {
        let err = Registry::from_yaml("plugins:\n  - name: x\n    kind: sidecar\n").unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }));
    }

    #[test]
    fn rejects_disallowed_field_for_kind() {
        let err =
            Registry::from_yaml("plugins:\n  - name: x\n    kind: raw\n    command: /bin/true\n")
                .unwrap_err();
        match err {
            ConfigError::Invalid { message, .. } => {
                assert!(message.contains("command"), "{message}")
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn rejects_duplicate_name() {
        let err = Registry::from_yaml(
            "plugins:\n  - name: x\n    kind: raw\n  - name: x\n    kind: raw\n",
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateName { .. }));
    }

    #[test]
    fn hot_reload_replaces_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugins.yaml");
        std::fs::write(&path, "plugins:\n  - name: a\n    kind: raw\n").unwrap();
        let reg = Registry::from_path(&path).unwrap();
        assert_eq!(reg.names(), vec!["a"]);

        std::fs::write(&path, "plugins:\n  - name: b\n    kind: raw\n").unwrap();
        reg.reload_from_path(&path).unwrap();
        assert_eq!(reg.names(), vec!["b"]);
    }

    #[test]
    fn failed_reload_preserves_previous_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plugins.yaml");
        std::fs::write(&path, "plugins:\n  - name: a\n    kind: raw\n").unwrap();
        let reg = Registry::from_path(&path).unwrap();

        std::fs::write(&path, "plugins:\n  - name: bad\n    kind: nope\n").unwrap();
        let err = reg.reload_from_path(&path).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid { .. }));
        // Old config still present.
        assert_eq!(reg.names(), vec!["a"]);
    }

    #[tokio::test]
    async fn dispatch_unknown_plugin_errors() {
        let reg = Registry::empty();
        let err = reg
            .render("missing", Path::new("/"), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, DispatchError::Unknown { .. }));
    }

    #[tokio::test]
    async fn dispatch_sidecar_is_unimplemented() {
        let reg = Registry::from_yaml(
            "plugins:\n  - name: s\n    kind: sidecar\n    endpoint: http://x\n",
        )
        .unwrap();
        let err = reg
            .render("s", Path::new("/"), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, DispatchError::Unimplemented { .. }));
    }

    #[tokio::test]
    async fn dispatch_raw_loads_from_source_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("cm.yaml"),
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: from-raw\n",
        )
        .unwrap();
        let reg = Registry::from_yaml("plugins:\n  - name: raw\n    kind: raw\n").unwrap();
        let out = reg
            .render("raw", dir.path(), serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "from-raw");
    }
}
