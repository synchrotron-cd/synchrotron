//! Server-side diff engine for `POST /api/v1/apps/{name}/diff`.
//!
//! The endpoint speaks two layers of "diff":
//!
//! 1. **Resource-level plan**: which resources would be created,
//!    updated, or deleted to bring the cluster into line with git.
//!    Provided by [`synchrotron_reconcile::plan`].
//! 2. **Field-level structural diff**: for each resource present on
//!    both sides, which leaf fields differ. Provided by
//!    [`synchrotron_diff::diff`].
//!
//! Both layers come out of pure data — the engine is just a small
//! shim that wires them together against the reconciler's existing
//! [`DesiredSource`] / [`LiveSource`] traits, so the API endpoint
//! stays decoupled from the underlying cache implementations and can
//! be tested with stubs.
//!
//! The endpoint surfaces two failure modes:
//!
//! - The engine isn't configured at all (the server was started
//!   without a reconciler). In that case [`AppsState`] holds `None`
//!   and the handler returns the stable empty-with-note shape.
//! - The engine is configured but a per-app lookup fails (cold cache,
//!   missing cluster). [`DiffEngineError`] carries the cause and the
//!   handler maps it onto an [`ApiError`].

use std::sync::Arc;

use serde::Serialize;
use synchrotron_diff::{diff, Change, ListMapKeys};
use synchrotron_plugins::Manifest;
use synchrotron_reconcile::{
    plan, DesiredSource, LiveSource, PlannedAction, ResourceRef, SourceError,
};
use synchrotron_types::{AppName, ClusterName};
use thiserror::Error;
use utoipa::ToSchema;

/// Per-resource diff entry rendered into [`DiffResponse`].
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DiffEntry {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    /// `"apply"` (create or update), `"delete"`, or `"noop"`.
    pub action: String,
    /// Field-level changes. Empty for `delete` and `noop` entries; for
    /// `apply` entries that are creates (no live counterpart) this
    /// stays empty too — the desired manifest itself is the change.
    pub changes: Vec<FieldChange>,
}

/// One leaf-level change inside a resource body.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct FieldChange {
    /// `"added"`, `"removed"`, or `"modified"`.
    pub op: String,
    /// Dotted path into the manifest body (see
    /// [`synchrotron_diff::ValuePath`]'s `Display`).
    pub path: String,
    pub desired: Option<serde_json::Value>,
    pub live: Option<serde_json::Value>,
}

#[derive(Debug, Error)]
pub enum DiffEngineError {
    #[error("app `{0}` not in desired-state cache")]
    AppNotInCache(AppName),
    #[error("cluster `{0}` not available")]
    ClusterNotAvailable(ClusterName),
    #[error("desired-state lookup failed: {0}")]
    DesiredFetch(String),
    #[error("live-state lookup failed: {0}")]
    LiveFetch(String),
}

/// Compute the per-app diff. Implementors are expected to be cheap
/// to call (in production: in-memory cache reads); the API handler
/// invokes this synchronously while holding no other locks.
pub trait DiffEngine: Send + Sync {
    fn compute(
        &self,
        app: &AppName,
        cluster: &ClusterName,
    ) -> Result<Vec<DiffEntry>, DiffEngineError>;
}

/// Default engine: composes the reconciler's planner with the
/// structural differ.
pub struct PlannerDiffEngine {
    desired: Arc<dyn DesiredSource>,
    live: Arc<dyn LiveSource>,
    list_maps: ListMapKeys,
}

impl PlannerDiffEngine {
    pub fn new(desired: Arc<dyn DesiredSource>, live: Arc<dyn LiveSource>) -> Self {
        Self {
            desired,
            live,
            list_maps: ListMapKeys::defaults(),
        }
    }

    pub fn with_list_maps(mut self, list_maps: ListMapKeys) -> Self {
        self.list_maps = list_maps;
        self
    }
}

impl DiffEngine for PlannerDiffEngine {
    fn compute(
        &self,
        app: &AppName,
        cluster: &ClusterName,
    ) -> Result<Vec<DiffEntry>, DiffEngineError> {
        let desired = self.desired.desired(app).map_err(|e| match e {
            SourceError::NotFound => DiffEngineError::AppNotInCache(app.clone()),
            SourceError::Unavailable(m) => DiffEngineError::DesiredFetch(m),
        })?;
        let live = self.live.live(app, cluster).map_err(|e| match e {
            SourceError::NotFound => DiffEngineError::ClusterNotAvailable(cluster.clone()),
            SourceError::Unavailable(m) => DiffEngineError::LiveFetch(m),
        })?;

        let p = plan(&desired, &live);
        let mut entries = Vec::with_capacity(p.entries.len());
        for entry in p.entries {
            let changes = match entry.action {
                PlannedAction::Apply => {
                    // Pull the matching pair (if any) for the field-
                    // level diff. A pure create has no live side, so
                    // the only meaningful "change" would be the whole
                    // body — we leave the field list empty there and
                    // let the action="apply" speak for itself.
                    match (
                        find_manifest(&desired, &entry.resource),
                        find_manifest(&live, &entry.resource),
                    ) {
                        (Some(d), Some(l)) => render_changes(&diff(d, l, &self.list_maps).changes),
                        _ => Vec::new(),
                    }
                }
                PlannedAction::Delete | PlannedAction::NoOp => Vec::new(),
            };
            entries.push(DiffEntry {
                group: entry.resource.gvk.group.clone(),
                version: entry.resource.gvk.version.clone(),
                kind: entry.resource.gvk.kind.clone(),
                namespace: entry.resource.namespace.clone(),
                name: entry.resource.name.clone(),
                action: action_str(entry.action).into(),
                changes,
            });
        }
        Ok(entries)
    }
}

fn find_manifest<'a>(set: &'a [Manifest], r: &ResourceRef) -> Option<&'a Manifest> {
    set.iter()
        .find(|m| m.gvk == r.gvk && m.namespace == r.namespace && m.name == r.name)
}

fn action_str(a: PlannedAction) -> &'static str {
    match a {
        PlannedAction::Apply => "apply",
        PlannedAction::Delete => "delete",
        PlannedAction::NoOp => "noop",
    }
}

fn render_changes(changes: &[Change]) -> Vec<FieldChange> {
    changes
        .iter()
        .map(|c| match c {
            Change::Added { path, desired } => FieldChange {
                op: "added".into(),
                path: path.to_string(),
                desired: yaml_to_json(desired),
                live: None,
            },
            Change::Removed { path, live } => FieldChange {
                op: "removed".into(),
                path: path.to_string(),
                desired: None,
                live: yaml_to_json(live),
            },
            Change::Modified {
                path,
                desired,
                live,
            } => FieldChange {
                op: "modified".into(),
                path: path.to_string(),
                desired: yaml_to_json(desired),
                live: yaml_to_json(live),
            },
        })
        .collect()
}

fn yaml_to_json(v: &serde_yaml_ng::Value) -> Option<serde_json::Value> {
    // serde_yaml_ng::Value implements Serialize, so we can transcode
    // through serde_json. None on the unlikely error so the API never
    // 500s on a value that round-tripped through the differ.
    serde_json::to_value(v).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn manifest(kind: &str, name: &str, ns: Option<&str>, marker: &str) -> Manifest {
        let yaml = format!(
            "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n{ns_line}data:\n  marker: {marker}\n",
            ns_line = ns
                .map(|n| format!("  namespace: {n}\n"))
                .unwrap_or_default(),
        );
        synchrotron_plugins::manifest::parse_stream("test", &yaml)
            .expect("parse")
            .pop()
            .expect("one manifest")
    }

    #[derive(Default)]
    struct StubDesired(Mutex<HashMap<String, Arc<[Manifest]>>>);
    impl DesiredSource for StubDesired {
        fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
            self.0
                .lock()
                .unwrap()
                .get(&app.0)
                .cloned()
                .ok_or(SourceError::NotFound)
        }
    }

    type LiveMap = HashMap<(String, String), Arc<[Manifest]>>;

    #[derive(Default)]
    struct StubLive(Mutex<LiveMap>);
    impl LiveSource for StubLive {
        fn live(
            &self,
            app: &AppName,
            cluster: &ClusterName,
        ) -> Result<Arc<[Manifest]>, SourceError> {
            self.0
                .lock()
                .unwrap()
                .get(&(app.0.clone(), cluster.0.clone()))
                .cloned()
                .ok_or(SourceError::NotFound)
        }
    }

    fn engine(d: Vec<Manifest>, l: Vec<Manifest>) -> PlannerDiffEngine {
        let desired = Arc::new(StubDesired::default());
        desired.0.lock().unwrap().insert("app".into(), d.into());
        let live = Arc::new(StubLive::default());
        live.0
            .lock()
            .unwrap()
            .insert(("app".into(), "prod".into()), l.into());
        PlannerDiffEngine::new(desired, live)
    }

    #[test]
    fn no_drift_yields_only_noop_entries() {
        let m = manifest("ConfigMap", "cm", Some("app"), "v1");
        let eng = engine(vec![m.clone()], vec![m]);
        let entries = eng
            .compute(&AppName("app".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "noop");
        assert!(entries[0].changes.is_empty());
    }

    #[test]
    fn modified_resource_emits_field_change() {
        let d = manifest("ConfigMap", "cm", Some("app"), "v2");
        let l = manifest("ConfigMap", "cm", Some("app"), "v1");
        let eng = engine(vec![d], vec![l]);
        let entries = eng
            .compute(&AppName("app".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.action, "apply");
        assert_eq!(e.kind, "ConfigMap");
        let changes = &e.changes;
        assert!(
            changes
                .iter()
                .any(|c| c.op == "modified" && c.path.contains("marker")),
            "expected a modified change on data.marker; got {changes:?}"
        );
    }

    #[test]
    fn desired_only_yields_apply_with_no_field_changes() {
        let d = manifest("ConfigMap", "new", Some("app"), "v1");
        let eng = engine(vec![d], vec![]);
        let entries = eng
            .compute(&AppName("app".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "apply");
        assert!(entries[0].changes.is_empty());
    }

    #[test]
    fn live_only_yields_delete() {
        let l = manifest("ConfigMap", "orphan", Some("app"), "v1");
        let eng = engine(vec![], vec![l]);
        let entries = eng
            .compute(&AppName("app".into()), &ClusterName("prod".into()))
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "delete");
        assert_eq!(entries[0].name, "orphan");
        assert!(entries[0].changes.is_empty());
    }

    #[test]
    fn missing_app_maps_to_app_not_in_cache() {
        let eng = engine(vec![], vec![]);
        let err = eng
            .compute(&AppName("ghost".into()), &ClusterName("prod".into()))
            .unwrap_err();
        assert!(matches!(err, DiffEngineError::AppNotInCache(_)));
    }

    #[test]
    fn missing_cluster_maps_to_cluster_not_available() {
        let m = manifest("ConfigMap", "cm", Some("app"), "v1");
        let eng = engine(vec![m], vec![]);
        let err = eng
            .compute(&AppName("app".into()), &ClusterName("dead".into()))
            .unwrap_err();
        assert!(matches!(err, DiffEngineError::ClusterNotAvailable(_)));
    }
}
