use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Operator-facing name for a cluster within Synchrotron, distinct from
/// the kubeconfig context name it may map to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterName(pub String);

impl fmt::Display for ClusterName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How to construct a Kubernetes client for a given cluster.
///
/// This foundation slice only supports kubeconfig-based auth. Sibling
/// sub-issues add in-cluster ServiceAccount (h48.8.2), OIDC refresh
/// (h48.8.3), and exec credential plugins (h48.8.4) as additional variants
/// or fields here.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub name: ClusterName,
    /// Explicit kubeconfig path. If `None`, uses the default search
    /// (`$KUBECONFIG` env var or `~/.kube/config`).
    pub kubeconfig: Option<PathBuf>,
    /// Context to select within the kubeconfig. If `None`, the
    /// kubeconfig's `current-context` is used.
    pub context: Option<String>,
}

impl ClusterConfig {
    pub fn from_kubeconfig(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: ClusterName(name.into()),
            kubeconfig: Some(path.into()),
            context: None,
        }
    }

    pub fn default_discovery(name: impl Into<String>) -> Self {
        Self {
            name: ClusterName(name.into()),
            kubeconfig: None,
            context: None,
        }
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }

    pub(crate) fn kubeconfig_path(&self) -> Option<&Path> {
        self.kubeconfig.as_deref()
    }
}
