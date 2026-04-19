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

pub mod manifest;
pub mod raw;

pub use manifest::{Gvk, Manifest};
