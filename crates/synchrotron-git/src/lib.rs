//! Git synchronization primitives for Synchrotron-CD.
//!
//! Provides a [`GitClient`] that maintains bare-repo caches on disk, performs
//! shallow fetches, exposes HEAD comparison, and materializes commits into
//! target directories for downstream rendering.
//!
//! This is the foundation slice (synchrotron-cd-h48.1.1): supports HTTP basic
//! auth and unauthenticated repos. SSH and GitHub App credential variants are
//! added by sibling sub-issues.

mod client;
mod credentials;
mod error;
pub mod github_app;
pub mod github_app_http;
pub mod known_hosts;
mod orchestrator;
pub mod poller;
mod repo;
mod triggers;
pub mod webhooks;
mod workspace;

pub use client::{FetchResult, GitClient, Sha};
pub use credentials::Credentials;
pub use error::GitError;
pub use known_hosts::{HostKeyMode, HostVerifier, KnownHostsFile, VerifyStatus};
pub use orchestrator::{AggregateStatus, Orchestrator, OrchestratorConfig, RepoStatus};
pub use poller::{PollEvent, Poller, PollerConfig};
pub use repo::{Repo, RepoId};
pub use triggers::RepoTriggers;
pub use workspace::Workspace;

pub type Result<T> = std::result::Result<T, GitError>;
