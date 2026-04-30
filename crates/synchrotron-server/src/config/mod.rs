//! Declarative configuration for synchrotron-server (58v.3).
//!
//! Synchrotron's runtime state is driven from a single YAML file:
//! clusters, repos, plugins, polling cadence, timeouts. The binary
//! loads and validates the file at startup, holds it behind a
//! [`ConfigHandle`] that the rest of the process reads through, and
//! hot-reloads on SIGHUP — atomically swapping the in-memory
//! `Arc<Config>` so concurrent readers either see the old config in
//! full or the new one in full, never a partial mix.
//!
//! # Layout
//!
//! - [`schema`] — the typed config tree. `serde` (de)serializers are
//!   the only shape that loads YAML into memory; everything else
//!   speaks `Config`.
//! - [`validate`] — post-deserialize structural and semantic checks.
//!   Each error carries a dotted path so misconfiguration messages
//!   can point operators at the exact field.
//! - [`reload`] — `ConfigHandle` plus the SIGHUP listener that
//!   triggers reloads in long-running binaries.
//! - [`schema_doc`] — emits an annotated YAML template for the docs.

pub mod reload;
pub mod schema;
pub mod schema_doc;
pub mod validate;

pub use reload::{ConfigHandle, ReloadError};
pub use schema::{
    ClusterCfg, Config, GitSection, PluginCfg, Polling, RepoCfg, ServerSection, SshHostKeyMode,
    SshSection, Timeouts,
};
pub use schema_doc::config_template;
pub use validate::{validate, ValidationError};

/// Convenience: load a config from `path`, validate it, and return
/// it. The binary's startup path uses this; integration tests use
/// it directly.
pub fn load(path: impl AsRef<std::path::Path>) -> Result<Config, ReloadError> {
    reload::load_and_validate(path.as_ref())
}
