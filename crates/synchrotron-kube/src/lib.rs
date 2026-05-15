//! Kubernetes client primitives for Synchrotron-CD.
//!
//! Foundation slice (synchrotron-cd-h48.8.1): kubeconfig-based auth and a
//! typed client handle that downstream sub-issues plug into (in-cluster
//! ServiceAccount, OIDC refresh, exec credential plugins, health probes,
//! informers, multi-cluster registry).

pub mod applier_adapter;
pub mod apply;
mod client;
mod config;
pub mod dry_run;
mod error;
pub mod exec;
pub mod health;
pub mod hook_runner;
pub mod informer;
pub mod live_source;
pub mod oidc;
pub mod oidc_http;
pub mod prune;
mod registry;
pub mod scaler_discovery;

pub use applier_adapter::KubeApplierAdapter;
pub use apply::{AppliedObject, ApplyError, ApplyOptions, KubeSsaApplier};
pub use client::KubeClient;
pub use config::{AuthSource, ClusterConfig, ClusterName};
pub use dry_run::KubeDryRunApplier;
pub use error::KubeError;
pub use exec::{
    tokio_command_runner, ExecCredential, ExecCredentialCache, ExecCredentialStatus,
    ExecPluginConfig, Runner,
};
pub use health::{HealthConfig, HealthMonitor, HealthState};
pub use hook_runner::{run_hook, DeletePolicy, HookError, HookOutcome, HookRunOptions};
pub use informer::{Informer, InformerConfig, InformerEvent};
pub use live_source::{LiveStore, LiveStoreUpdater, StoreLiveSource, DEFAULT_APP_LABEL};
pub use oidc::{OidcConfig, OidcToken, OidcTokenCache, Refresher};
pub use prune::{
    compute_prune_set, is_prune_disabled, ARGOCD_SYNC_OPTIONS_ANNOTATION,
    SYNCHROTRON_PRUNE_ANNOTATION,
};
pub use registry::ClusterRegistry;
pub use scaler_discovery::ScalerDiscovery;

pub type Result<T> = std::result::Result<T, KubeError>;
