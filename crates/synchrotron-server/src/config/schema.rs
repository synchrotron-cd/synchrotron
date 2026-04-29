//! Typed config tree.
//!
//! Everything is `serde`-deserializable from YAML. Defaults are
//! applied at the field level via `#[serde(default = "...")]`, so
//! omitting an entire section yields a sensible runtime instead of
//! a parse error. Validation (in [`super::validate`]) is the line
//! that rejects nonsense values; deserialization just shapes them.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub clusters: Vec<ClusterCfg>,
    #[serde(default)]
    pub repos: Vec<RepoCfg>,
    #[serde(default)]
    pub plugins: Vec<PluginCfg>,
    #[serde(default)]
    pub polling: Polling,
    #[serde(default)]
    pub timeouts: Timeouts,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            db_path: default_db_path(),
        }
    }
}

fn default_listen_addr() -> String {
    "0.0.0.0:8484".into()
}

fn default_db_path() -> PathBuf {
    PathBuf::from("synchrotron.db")
}

/// One Kubernetes cluster the controller manages.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterCfg {
    pub name: String,
    /// Path to a kubeconfig file. Mutually exclusive with
    /// `in_cluster: true`.
    #[serde(default)]
    pub kubeconfig: Option<PathBuf>,
    /// Context within the kubeconfig to use. If unset and the
    /// kubeconfig has a `current-context`, that one is used.
    #[serde(default)]
    pub context: Option<String>,
    /// Use the in-cluster ServiceAccount projected token. Mutually
    /// exclusive with `kubeconfig`.
    #[serde(default)]
    pub in_cluster: bool,
}

/// One git repo the controller polls and renders manifests from.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RepoCfg {
    /// Stable identifier referenced by apps. Must be unique within
    /// the config.
    pub id: String,
    /// Clone URL. Both HTTPS and SSH forms are supported.
    pub url: String,
    /// Branch to track. `None` means use the repo's default branch.
    #[serde(default)]
    pub branch: Option<String>,
    /// Name of an external secret store entry holding credentials.
    /// Resolution is the secret-store layer's job; this field is a
    /// pointer.
    #[serde(default)]
    pub credentials_secret: Option<String>,
}

/// A manifest-rendering plugin. The `kind` field selects the
/// runtime; `config` holds runtime-specific options preserved
/// verbatim for the runtime to interpret.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginCfg {
    pub name: String,
    pub kind: PluginKind,
    #[serde(default)]
    pub config: serde_yaml_ng::Value,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PluginKind {
    Raw,
    Helm,
    Kustomize,
    Local,
    Sidecar,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Polling {
    /// How often the git poller checks each repo for new commits.
    #[serde(default = "default_repo_interval")]
    pub repo_interval_seconds: u64,
    /// How often auto-heal scans for apps that haven't reconciled
    /// recently.
    #[serde(default = "default_auto_heal_interval")]
    pub auto_heal_interval_seconds: u64,
}

impl Default for Polling {
    fn default() -> Self {
        Self {
            repo_interval_seconds: default_repo_interval(),
            auto_heal_interval_seconds: default_auto_heal_interval(),
        }
    }
}

fn default_repo_interval() -> u64 {
    180
}

fn default_auto_heal_interval() -> u64 {
    600
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Timeouts {
    /// Hard upper bound on a single reconcile run.
    #[serde(default = "default_reconcile_timeout")]
    pub reconcile_seconds: u64,
    /// Hard upper bound on a single git fetch.
    #[serde(default = "default_git_fetch_timeout")]
    pub git_fetch_seconds: u64,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            reconcile_seconds: default_reconcile_timeout(),
            git_fetch_seconds: default_git_fetch_timeout(),
        }
    }
}

fn default_reconcile_timeout() -> u64 {
    300
}

fn default_git_fetch_timeout() -> u64 {
    120
}
