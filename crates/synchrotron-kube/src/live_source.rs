//! In-memory live-state store + [`LiveSource`] impl, plus an
//! [`Informer`]-driven bridge that keeps the store in sync.
//!
//! # Why a store rather than on-demand API queries
//!
//! `synchrotron_reconcile::LiveSource::live` is **sync**: in
//! production the reconcile hot path expects to read live state out
//! of an in-memory cache without crossing an `await` boundary or
//! holding up a worker thread on the kube API. The informer
//! infrastructure already exists (see [`crate::informer::Informer`]);
//! this module wires its event stream into a `RwLock<HashMap>` that
//! the [`StoreLiveSource`] can read synchronously.
//!
//! # Identification model
//!
//! Each managed Kubernetes resource is expected to carry a label
//! identifying which Synchrotron app owns it (default
//! `synchrotron.io/app=<name>` — see [`LiveStoreUpdater::with_label`]).
//! Objects without that label are silently ignored (they're not ours
//! to track). The store is keyed by `(ClusterName, AppName)`; the
//! per-cluster, per-app slot holds an `Arc<[Manifest]>` so reads are
//! cheap refcount bumps, matching the y0v.3 / d2p memory work.
//!
//! # Cluster lifecycle
//!
//! [`LiveStore::register_cluster`] declares a cluster known but
//! possibly empty. `live(app, cluster)` for an unregistered cluster
//! returns [`SourceError::NotFound`] (mapped to
//! `ReconcileError::ClusterNotFound` upstream). A registered
//! cluster with no known apps still returns `Ok(empty)` — that's
//! the "app deployed nothing yet" case, distinct from "cluster is
//! offline".
//!
//! # Slice scope (oes)
//!
//! What this module ships:
//!   - [`LiveStore`] — the storage primitive
//!   - [`StoreLiveSource`] — the trait impl reading the store
//!   - [`LiveStoreUpdater`] — translates an [`InformerEvent<DynamicObject>`]
//!     stream into store mutations
//!
//! What slice 4 (server wiring) does:
//!   - Decides which GVKs to spawn informers for (per cluster)
//!   - Connects the spawned informer's broadcast receiver to a
//!     `LiveStoreUpdater` background task
//!
//! Keeping the slice this size means we can land + test the
//! storage + bridging logic in isolation; the GVK-discovery and
//! per-cluster orchestration is a server-config concern, not a
//! library primitive.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use kube::api::DynamicObject;
use synchrotron_plugins::{Gvk, Manifest, ManifestBody};
use synchrotron_reconcile::{LiveSource, SourceError};
use synchrotron_types::{AppName, ClusterName};
use tracing::{debug, warn};

use crate::informer::InformerEvent;

/// Default label key consulted to attribute a live resource to a
/// Synchrotron app. Override via [`LiveStoreUpdater::with_label`].
pub const DEFAULT_APP_LABEL: &str = "synchrotron.io/app";

/// Thread-safe in-memory snapshot of "what's live in each cluster,
/// per app". Reads (via [`StoreLiveSource`]) are sync `RwLock`
/// shared-locks; writes (via [`LiveStoreUpdater`]) take exclusive
/// locks for the duration of one update.
#[derive(Debug, Default)]
pub struct LiveStore {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Per-cluster app→manifest-set. Presence of a cluster key
    /// means the cluster is registered; absence means
    /// [`SourceError::NotFound`].
    clusters: HashMap<ClusterName, HashMap<AppName, Arc<[Manifest]>>>,
}

impl LiveStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a cluster known. Idempotent. Without this, calls to
    /// [`StoreLiveSource::live`] for the cluster return
    /// [`SourceError::NotFound`].
    pub fn register_cluster(&self, cluster: ClusterName) {
        self.inner
            .write()
            .unwrap()
            .clusters
            .entry(cluster)
            .or_default();
    }

    /// Remove a cluster entirely. Subsequent reads return
    /// [`SourceError::NotFound`]. Use when a cluster connection is
    /// torn down for good (vs. a transient outage, which keeps the
    /// stale data warm).
    pub fn forget_cluster(&self, cluster: &ClusterName) {
        self.inner.write().unwrap().clusters.remove(cluster);
    }

    /// Replace one app's manifest set on `cluster`. Inserts the
    /// cluster as a side effect if it wasn't registered — handy in
    /// tests; production paths should still call
    /// [`Self::register_cluster`] explicitly to make intent clear.
    pub fn replace(&self, cluster: &ClusterName, app: &AppName, manifests: Vec<Manifest>) {
        let mut inner = self.inner.write().unwrap();
        let apps = inner.clusters.entry(cluster.clone()).or_default();
        apps.insert(app.clone(), Arc::from(manifests));
    }

    /// Remove one app's entry on `cluster`. Subsequent reads return
    /// `Ok(empty)` (cluster is still registered) — equivalent to
    /// "this app has no live resources here".
    pub fn forget_app(&self, cluster: &ClusterName, app: &AppName) {
        let mut inner = self.inner.write().unwrap();
        if let Some(apps) = inner.clusters.get_mut(cluster) {
            apps.remove(app);
        }
    }

    /// Read-side accessor used by [`StoreLiveSource`].
    fn get(&self, cluster: &ClusterName, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
        let inner = self.inner.read().unwrap();
        match inner.clusters.get(cluster) {
            None => Err(SourceError::NotFound),
            Some(apps) => Ok(apps.get(app).cloned().unwrap_or_else(|| Arc::from(vec![]))),
        }
    }
}

/// [`LiveSource`] impl that reads from a shared [`LiveStore`].
pub struct StoreLiveSource(pub Arc<LiveStore>);

impl LiveSource for StoreLiveSource {
    fn live(&self, app: &AppName, cluster: &ClusterName) -> Result<Arc<[Manifest]>, SourceError> {
        self.0.get(cluster, app)
    }
}

/// Translates `Informer<DynamicObject>` events into [`LiveStore`]
/// mutations, scoped to one (cluster, GVK) tuple.
///
/// One updater = one informer subscription. Spawn one per GVK per
/// cluster; the store key collisions are naturally resolved by the
/// `ResourceRef`-shaped identity inside each `Manifest`.
pub struct LiveStoreUpdater {
    store: Arc<LiveStore>,
    cluster: ClusterName,
    label: String,
}

impl LiveStoreUpdater {
    pub fn new(store: Arc<LiveStore>, cluster: ClusterName) -> Self {
        Self {
            store,
            cluster,
            label: DEFAULT_APP_LABEL.to_string(),
        }
    }

    /// Use a different label key to attribute objects to an app.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    /// Apply a single informer event. Returns the [`AppName`] that
    /// was affected, if any (objects without the app label are
    /// silently skipped).
    ///
    /// Caller invokes this per event drained from the informer's
    /// broadcast receiver; usually inside a `loop { rx.recv()... }`
    /// task spawned alongside the informer.
    pub fn handle_event(&self, event: InformerEvent<DynamicObject>) -> Option<AppName> {
        match event {
            InformerEvent::Applied(obj) => {
                let (app, manifest) = self.parse(obj)?;
                self.upsert_one(&app, manifest);
                Some(app)
            }
            InformerEvent::Deleted(obj) => {
                let (app, manifest) = self.parse(obj)?;
                self.remove_one(&app, &manifest);
                Some(app)
            }
            InformerEvent::Restarted(list) => {
                // A restart is the informer's "here is the world".
                // Re-bucket every observed object by app and replace
                // each app's slot atomically; apps that were tracked
                // before but absent from this list get cleared.
                let mut by_app: HashMap<AppName, Vec<Manifest>> = HashMap::new();
                for obj in list {
                    if let Some((app, manifest)) = self.parse(obj) {
                        by_app.entry(app).or_default().push(manifest);
                    }
                }
                self.replace_all(by_app);
                None
            }
        }
    }

    /// Extract `(AppName, Manifest)` from a `DynamicObject`,
    /// returning `None` if the object isn't ours (no app label) or
    /// if its body can't be re-serialized into JSON for our
    /// internal `Manifest` representation.
    fn parse(&self, obj: DynamicObject) -> Option<(AppName, Manifest)> {
        let app = obj.metadata.labels.as_ref()?.get(&self.label)?.clone();
        let app = AppName(app);

        // DynamicObject's `types` carries apiVersion + kind only
        // when the object was fetched/watched through a typed Api.
        // For our informer they're always populated; fall back to
        // empty Gvk if not (the manifest is still useful for the
        // store, just less precisely keyed).
        let gvk = obj
            .types
            .as_ref()
            .map(|t| Gvk::parse(&t.api_version, &t.kind))
            .unwrap_or_default();

        let name = obj.metadata.name.clone().unwrap_or_default();
        let namespace = obj.metadata.namespace.clone();

        // Round-trip the kube object through JSON → serde_yaml_ng::Value
        // to fit our existing Manifest shape. Same pattern as
        // crate::scaler_discovery.
        let value: serde_yaml_ng::Value = serde_json::to_value(&obj)
            .and_then(serde_json::from_value)
            .ok()?;

        Some((
            app,
            Manifest {
                gvk,
                name,
                namespace,
                body: ManifestBody::from_value(value),
            },
        ))
    }

    fn upsert_one(&self, app: &AppName, manifest: Manifest) {
        let mut inner = self.store.inner.write().unwrap();
        let apps = inner.clusters.entry(self.cluster.clone()).or_default();
        let existing = apps.entry(app.clone()).or_insert_with(|| Arc::from(vec![]));

        let mut next: Vec<Manifest> = existing
            .iter()
            .filter(|m| !same_resource(m, &manifest))
            .cloned()
            .collect();
        next.push(manifest);
        *existing = Arc::from(next);
        debug!(
            cluster = %self.cluster,
            %app,
            "live-store upsert: now {} manifests",
            existing.len()
        );
    }

    fn remove_one(&self, app: &AppName, manifest: &Manifest) {
        let mut inner = self.store.inner.write().unwrap();
        let Some(apps) = inner.clusters.get_mut(&self.cluster) else {
            warn!(cluster = %self.cluster, "delete event for un-registered cluster; ignoring");
            return;
        };
        let Some(existing) = apps.get_mut(app) else {
            return;
        };
        let next: Vec<Manifest> = existing
            .iter()
            .filter(|m| !same_resource(m, manifest))
            .cloned()
            .collect();
        *existing = Arc::from(next);
    }

    fn replace_all(&self, by_app: HashMap<AppName, Vec<Manifest>>) {
        let mut inner = self.store.inner.write().unwrap();
        let apps = inner.clusters.entry(self.cluster.clone()).or_default();
        // Clear the cluster's apps and rewrite from the resync list.
        apps.clear();
        for (app, manifests) in by_app {
            apps.insert(app, Arc::from(manifests));
        }
    }
}

/// Manifest identity within an app's slot: same GVK + same
/// (namespace, name). Body equality is irrelevant for store
/// dedup — the *latest* version replaces the older one.
fn same_resource(a: &Manifest, b: &Manifest) -> bool {
    a.gvk == b.gvk && a.namespace == b.namespace && a.name == b.name
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::api::TypeMeta;

    fn obj(name: &str, app: Option<&str>, kind: &str) -> DynamicObject {
        let mut labels = std::collections::BTreeMap::new();
        if let Some(a) = app {
            labels.insert(DEFAULT_APP_LABEL.to_string(), a.to_string());
        }
        DynamicObject {
            types: Some(TypeMeta {
                api_version: "v1".into(),
                kind: kind.into(),
            }),
            metadata: ObjectMeta {
                name: Some(name.into()),
                namespace: Some("default".into()),
                labels: Some(labels),
                ..Default::default()
            },
            data: serde_json::json!({}),
        }
    }

    #[test]
    fn unregistered_cluster_returns_not_found() {
        let store = Arc::new(LiveStore::new());
        let src = StoreLiveSource(store.clone());
        let result = src.live(&AppName("web".into()), &ClusterName("nope".into()));
        assert!(matches!(result, Err(SourceError::NotFound)));
    }

    #[test]
    fn registered_cluster_unknown_app_returns_empty() {
        let store = Arc::new(LiveStore::new());
        store.register_cluster(ClusterName("prod".into()));
        let src = StoreLiveSource(store);
        let result = src
            .live(&AppName("web".into()), &ClusterName("prod".into()))
            .unwrap();
        assert!(result.is_empty(), "expected empty slice for unknown app");
    }

    #[test]
    fn replace_then_live_returns_manifests() {
        let store = Arc::new(LiveStore::new());
        store.register_cluster(ClusterName("prod".into()));
        let src = StoreLiveSource(store.clone());

        let m = Manifest {
            gvk: Gvk::parse("v1", "ConfigMap"),
            name: "cm-1".into(),
            namespace: Some("default".into()),
            body: ManifestBody::from_value(serde_yaml_ng::Value::Null),
        };
        store.replace(&ClusterName("prod".into()), &AppName("web".into()), vec![m]);

        let live = src
            .live(&AppName("web".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].name, "cm-1");
    }

    #[test]
    fn updater_applies_event_for_labelled_object() {
        let store = Arc::new(LiveStore::new());
        let updater = LiveStoreUpdater::new(store.clone(), ClusterName("prod".into()));
        let app = updater.handle_event(InformerEvent::Applied(obj(
            "cm-1",
            Some("web"),
            "ConfigMap",
        )));
        assert_eq!(app, Some(AppName("web".into())));

        let src = StoreLiveSource(store);
        let live = src
            .live(&AppName("web".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].name, "cm-1");
    }

    #[test]
    fn updater_ignores_unlabelled_object() {
        let store = Arc::new(LiveStore::new());
        let updater = LiveStoreUpdater::new(store.clone(), ClusterName("prod".into()));
        let app = updater.handle_event(InformerEvent::Applied(obj("rogue", None, "ConfigMap")));
        assert_eq!(app, None);
        // Cluster shouldn't even get registered by ignored events.
        let src = StoreLiveSource(store);
        let result = src.live(&AppName("web".into()), &ClusterName("prod".into()));
        assert!(matches!(result, Err(SourceError::NotFound)));
    }

    #[test]
    fn updater_deletes_object_from_app_slot() {
        let store = Arc::new(LiveStore::new());
        let updater = LiveStoreUpdater::new(store.clone(), ClusterName("prod".into()));
        updater.handle_event(InformerEvent::Applied(obj(
            "cm-1",
            Some("web"),
            "ConfigMap",
        )));
        updater.handle_event(InformerEvent::Applied(obj(
            "cm-2",
            Some("web"),
            "ConfigMap",
        )));
        updater.handle_event(InformerEvent::Deleted(obj(
            "cm-1",
            Some("web"),
            "ConfigMap",
        )));

        let src = StoreLiveSource(store);
        let live = src
            .live(&AppName("web".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].name, "cm-2");
    }

    #[test]
    fn updater_restart_replaces_apps_atomically() {
        let store = Arc::new(LiveStore::new());
        let updater = LiveStoreUpdater::new(store.clone(), ClusterName("prod".into()));
        // Pre-populate the store with stale data.
        updater.handle_event(InformerEvent::Applied(obj(
            "stale",
            Some("web"),
            "ConfigMap",
        )));

        // Restart: resync says only `fresh` exists, no `stale`.
        updater.handle_event(InformerEvent::Restarted(vec![
            obj("fresh", Some("web"), "ConfigMap"),
            obj("fresh-other-app", Some("api"), "ConfigMap"),
        ]));

        let src = StoreLiveSource(store);
        let web = src
            .live(&AppName("web".into()), &ClusterName("prod".into()))
            .unwrap();
        let api = src
            .live(&AppName("api".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(web.len(), 1);
        assert_eq!(web[0].name, "fresh");
        assert_eq!(api.len(), 1);
        assert_eq!(api[0].name, "fresh-other-app");
    }

    #[test]
    fn forget_cluster_unregisters() {
        let store = Arc::new(LiveStore::new());
        store.register_cluster(ClusterName("prod".into()));
        store.forget_cluster(&ClusterName("prod".into()));

        let src = StoreLiveSource(store);
        assert!(matches!(
            src.live(&AppName("web".into()), &ClusterName("prod".into())),
            Err(SourceError::NotFound)
        ));
    }
}
