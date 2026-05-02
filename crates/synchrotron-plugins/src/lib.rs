//! Plugin system for manifest rendering.
//!
//! The plugin model (see DESIGN.md §4) has three runtimes:
//! - **Built-in raw YAML** (this crate, [`raw`]) — a directory of
//!   `.yaml`/`.yml` files read and parsed natively, no subprocess.
//! - **Local plugin**: JSON-RPC over stdio subprocess (future).
//! - **Sidecar plugin**: gRPC to container sidecars (future).
//!
//! All three produce the same [`Manifest`] stream; downstream code
//! (cache, reconciler) is agnostic to which runtime produced it.

pub mod app_cache;
pub mod cache;
pub mod local;
pub mod manifest;
pub mod raw;
pub mod registry;
pub mod sidecar;

pub use app_cache::{AppCache, AppCacheKey, AppCacheStats};
pub use cache::{Cache, CacheKey, CacheStats};
pub use manifest::{Gvk, Manifest, OwnedResource};
pub use registry::{DispatchError, PluginConfig, PluginKind, Registry};
pub use sidecar::{RetryConfig, Sidecar, SidecarError};
