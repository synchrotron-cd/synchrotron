//! In-memory desired-state store + [`DesiredSource`] impl.
//!
//! Sister to `synchrotron_kube::live_source::LiveStore` (slice 2 of
//! the wire-pipeline epic — `oes`) but for the desired side. Where
//! `LiveStore` is keyed by `(ClusterName, AppName)` because live
//! state is per-cluster, this store is keyed by `AppName` alone:
//! desired manifests come out of the AppCache and are cluster-
//! agnostic (an app targeting multiple clusters is modeled as
//! multiple Application records, each with its own slot).
//!
//! # Slice scope (c4c)
//!
//! What this module ships:
//!   - [`DesiredStore`] — thread-safe `RwLock<HashMap>` with
//!     [`SourceError::NotFound`] for unknown apps, `Arc<[Manifest]>`
//!     reads on the hot path.
//!   - [`StoreDesiredSource`] — implements [`DesiredSource`] reading
//!     the store.
//!
//! What slice 4 (server bring-up, `70x`) does:
//!   - Drives a render pipeline (synchrotron-git poller →
//!     `synchrotron_plugins::AppRenderer` → `DesiredStore::put`).
//!   - Hooks RepoChanged events into the pipeline.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use synchrotron_plugins::Manifest;
use synchrotron_types::AppName;

use crate::reconcile::{DesiredSource, SourceError};

/// Thread-safe in-memory snapshot of "the latest rendered desired
/// manifests for each app". Writes (from the render pipeline) take
/// an exclusive lock for the duration of one swap; reads take a
/// shared lock and return an `Arc<[Manifest]>` refcount bump.
///
/// Absence of an app key means "unknown app, render hasn't run
/// yet" → [`SourceError::NotFound`], which the reconciler maps onto
/// [`crate::ReconcileError::AppNotFound`].
#[derive(Debug, Default)]
pub struct DesiredStore {
    inner: RwLock<HashMap<AppName, Arc<[Manifest]>>>,
}

impl DesiredStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the slot for `app` with `manifests`. Idempotent.
    /// Typically called by the render pipeline once a new commit
    /// has been rendered through the plugin.
    pub fn put(&self, app: AppName, manifests: Arc<[Manifest]>) {
        self.inner.write().unwrap().insert(app, manifests);
    }

    /// Convenience wrapper that takes an owned `Vec<Manifest>`.
    pub fn put_vec(&self, app: AppName, manifests: Vec<Manifest>) {
        self.put(app, Arc::from(manifests));
    }

    /// Drop the slot for `app`. Subsequent reads return
    /// [`SourceError::NotFound`]. Use when an app is deleted from
    /// the config (vs. a transient render failure — leave stale
    /// data warm in that case).
    pub fn forget(&self, app: &AppName) {
        self.inner.write().unwrap().remove(app);
    }

    /// How many apps currently have entries. Useful for metrics.
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().is_empty()
    }

    fn get(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
        self.inner
            .read()
            .unwrap()
            .get(app)
            .cloned()
            .ok_or(SourceError::NotFound)
    }
}

/// [`DesiredSource`] impl backed by a shared [`DesiredStore`].
pub struct StoreDesiredSource(pub Arc<DesiredStore>);

impl DesiredSource for StoreDesiredSource {
    fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
        self.0.get(app)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchrotron_plugins::{Gvk, ManifestBody};

    fn cm(name: &str) -> Manifest {
        Manifest {
            gvk: Gvk::parse("v1", "ConfigMap"),
            name: name.into(),
            namespace: Some("default".into()),
            body: ManifestBody::from_value(serde_yaml_ng::Value::Null),
        }
    }

    #[test]
    fn unknown_app_returns_not_found() {
        let store = Arc::new(DesiredStore::new());
        let src = StoreDesiredSource(store);
        assert!(matches!(
            src.desired(&AppName("web".into())),
            Err(SourceError::NotFound)
        ));
    }

    #[test]
    fn put_then_get_round_trips() {
        let store = Arc::new(DesiredStore::new());
        let src = StoreDesiredSource(store.clone());

        store.put_vec(AppName("web".into()), vec![cm("cm-1"), cm("cm-2")]);
        let manifests = src.desired(&AppName("web".into())).unwrap();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].name, "cm-1");
    }

    #[test]
    fn put_replaces_previous_entry() {
        let store = Arc::new(DesiredStore::new());
        let src = StoreDesiredSource(store.clone());

        store.put_vec(AppName("web".into()), vec![cm("v1")]);
        store.put_vec(AppName("web".into()), vec![cm("v2")]);
        let manifests = src.desired(&AppName("web".into())).unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].name, "v2");
    }

    #[test]
    fn forget_removes_entry() {
        let store = Arc::new(DesiredStore::new());
        let src = StoreDesiredSource(store.clone());

        store.put_vec(AppName("web".into()), vec![cm("cm-1")]);
        store.forget(&AppName("web".into()));
        assert!(matches!(
            src.desired(&AppName("web".into())),
            Err(SourceError::NotFound)
        ));
    }

    #[test]
    fn store_shares_arc_across_reads() {
        let store = Arc::new(DesiredStore::new());
        let src = StoreDesiredSource(store.clone());

        store.put_vec(AppName("web".into()), vec![cm("cm-1")]);
        let r1 = src.desired(&AppName("web".into())).unwrap();
        let r2 = src.desired(&AppName("web".into())).unwrap();
        // Same backing storage → both reads point at the same allocation.
        assert!(Arc::ptr_eq(&r1, &r2));
    }
}
