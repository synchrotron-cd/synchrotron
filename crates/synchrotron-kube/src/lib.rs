//! Kubernetes client primitives for Synchrotron-CD.
//!
//! Foundation slice (synchrotron-cd-h48.8.1): kubeconfig-based auth and a
//! typed client handle that downstream sub-issues plug into (in-cluster
//! ServiceAccount, OIDC refresh, exec credential plugins, health probes,
//! informers, multi-cluster registry).

mod client;
mod config;
mod error;

pub use client::KubeClient;
pub use config::{ClusterConfig, ClusterName};
pub use error::KubeError;

pub type Result<T> = std::result::Result<T, KubeError>;
