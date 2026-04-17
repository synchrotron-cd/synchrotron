//! Informers: scoped watch streams over Kubernetes resources.
//!
//! An [`Informer`] follows a single resource type filtered by a label
//! selector (and optionally namespace), so we only watch what
//! Synchrotron has applied rather than the full cluster. It restarts
//! the watch on every Down→Up transition emitted by a [`HealthMonitor`]
//! (sibling sub-issue h48.8.5), which is the correct behaviour after a
//! kube client rebuild — old watch tokens become stale.

mod runner;
mod types;

pub use runner::{kube_watch_factory, ClientProvider, Informer, WatchFactory};
pub use types::{InformerConfig, InformerEvent};
