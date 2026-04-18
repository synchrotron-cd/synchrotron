use std::fmt;
use std::path::PathBuf;

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
#[derive(Debug, Clone)]
pub enum AuthSource {
    /// Explicit kubeconfig file at `path` with optional context override.
    Kubeconfig {
        path: PathBuf,
        context: Option<String>,
    },
    /// In-cluster ServiceAccount auth via the projected token at
    /// `/var/run/secrets/kubernetes.io/serviceaccount/token`. kube-rs
    /// re-reads the token file on each request so projected-token
    /// rotation (kubelet refreshes well before expiry) is handled
    /// transparently — no manual rotation logic required.
    InCluster,
    /// Ambient discovery: try in-cluster first, then fall back to the
    /// default kubeconfig search (`$KUBECONFIG` or `~/.kube/config`).
    /// Useful for binaries that may run either inside the cluster or
    /// against a kubeconfig in dev.
    Default { context: Option<String> },
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub name: ClusterName,
    pub source: AuthSource,
}

impl ClusterConfig {
    pub fn from_kubeconfig(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: ClusterName(name.into()),
            source: AuthSource::Kubeconfig {
                path: path.into(),
                context: None,
            },
        }
    }

    /// Explicit in-cluster ServiceAccount auth. Use this when the
    /// process is known to be running inside Kubernetes and you want
    /// to fail fast (rather than silently falling back to a stray
    /// kubeconfig) if the projected SA token is missing.
    pub fn in_cluster(name: impl Into<String>) -> Self {
        Self {
            name: ClusterName(name.into()),
            source: AuthSource::InCluster,
        }
    }

    pub fn default_discovery(name: impl Into<String>) -> Self {
        Self {
            name: ClusterName(name.into()),
            source: AuthSource::Default { context: None },
        }
    }

    /// Set the kubeconfig context to use. No-op for [`AuthSource::InCluster`].
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        let context = context.into();
        match &mut self.source {
            AuthSource::Kubeconfig { context: c, .. } | AuthSource::Default { context: c } => {
                *c = Some(context);
            }
            AuthSource::InCluster => {}
        }
        self
    }
}
