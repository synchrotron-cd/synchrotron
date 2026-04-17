//! Kubernetes client primitives for Synchrotron-CD.
//!
//! Foundation slice (synchrotron-cd-h48.8.1): kubeconfig-based auth and a
//! typed client handle that downstream sub-issues plug into (in-cluster
//! ServiceAccount, OIDC refresh, exec credential plugins, health probes,
//! informers, multi-cluster registry).

mod client;
mod config;
mod error;
pub mod health;
pub mod informer;

pub use client::KubeClient;
pub use config::{ClusterConfig, ClusterName};
pub use error::KubeError;
pub use health::{HealthConfig, HealthMonitor, HealthState};
pub use informer::{Informer, InformerConfig, InformerEvent};

pub type Result<T> = std::result::Result<T, KubeError>;
