//! Kubernetes client primitives for Synchrotron-CD.
//!
//! Foundation slice (synchrotron-cd-h48.8.1): kubeconfig-based auth and a
//! typed client handle that downstream sub-issues plug into (in-cluster
//! ServiceAccount, OIDC refresh, exec credential plugins, health probes,
//! informers, multi-cluster registry).

mod client;
mod config;
mod error;
pub mod exec;
pub mod health;
pub mod informer;
pub mod oidc;
mod registry;

pub use client::KubeClient;
pub use config::{AuthSource, ClusterConfig, ClusterName};
pub use error::KubeError;
pub use exec::{
    tokio_command_runner, ExecCredential, ExecCredentialCache, ExecCredentialStatus,
    ExecPluginConfig, Runner,
};
pub use health::{HealthConfig, HealthMonitor, HealthState};
pub use informer::{Informer, InformerConfig, InformerEvent};
pub use oidc::{OidcConfig, OidcToken, OidcTokenCache, Refresher};
pub use registry::ClusterRegistry;

pub type Result<T> = std::result::Result<T, KubeError>;
