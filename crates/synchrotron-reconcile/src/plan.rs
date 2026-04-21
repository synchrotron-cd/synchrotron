//! Pure reconcile planner.
//!
//! Given a set of *desired* manifests (from the app cache) and the
//! *live* state of those resources in the target cluster (from the
//! informer cache), produce a [`Plan`] enumerating what would need
//! to change to bring the cluster in line with the desired state.
//!
//! The planner is deliberately side-effect free. It does not talk to
//! kube-apiserver, does not mutate any cache, and does not publish
//! events — callers (see [`crate::reconcile`]) handle that. This lets
//! the planner be property-tested and reused by dry-run/diff tooling.

use std::collections::HashMap;

use synchrotron_plugins::{Gvk, Manifest};

/// Stable identity of a Kubernetes resource: the GVK plus namespace
/// and name. Cluster-scoped resources use `namespace = None`.
///
/// The *name* of an app's manifest is the unit of identity for the
/// diff — if a manifest in git moves from one namespace to another,
/// the planner sees that as a `Delete` of the old one plus an
/// `Apply` of the new one, which is what a naive operator would
/// expect.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceRef {
    pub gvk: Gvk,
    pub namespace: Option<String>,
    pub name: String,
}

impl ResourceRef {
    pub fn of(m: &Manifest) -> Self {
        Self {
            gvk: m.gvk.clone(),
            namespace: m.namespace.clone(),
            name: m.name.clone(),
        }
    }

    /// Sort key for deterministic plan ordering. `Gvk` itself does
    /// not derive `Ord`, so we project it to its string fields.
    fn sort_key(&self) -> (&str, &str, &str, Option<&str>, &str) {
        (
            self.gvk.group.as_str(),
            self.gvk.version.as_str(),
            self.gvk.kind.as_str(),
            self.namespace.as_deref(),
            self.name.as_str(),
        )
    }
}

/// What the planner would do with a given resource.
///
/// `Apply` covers both create (desired-only) and update (body
/// differs) since kubectl apply semantics unify those. A future
/// slice may split them for richer reporting, but today every
/// "bring the cluster into line" action is an apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannedAction {
    Apply,
    Delete,
    NoOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEntry {
    pub resource: ResourceRef,
    pub action: PlannedAction,
}

/// The planner's output. Entries are sorted by `ResourceRef` so
/// plans for the same inputs compare equal regardless of input
/// order — important for deterministic diffs in tests and for any
/// content-addressed plan cache a later slice might add.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub entries: Vec<PlanEntry>,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Count of entries with a non-noop action. `noop` entries are
    /// reported so callers know which resources were considered and
    /// found in sync; they don't contribute to this count.
    pub fn changes(&self) -> usize {
        self.entries
            .iter()
            .filter(|e| e.action != PlannedAction::NoOp)
            .count()
    }

    pub fn apply_count(&self) -> usize {
        self.count(PlannedAction::Apply)
    }

    pub fn delete_count(&self) -> usize {
        self.count(PlannedAction::Delete)
    }

    pub fn noop_count(&self) -> usize {
        self.count(PlannedAction::NoOp)
    }

    fn count(&self, action: PlannedAction) -> usize {
        self.entries.iter().filter(|e| e.action == action).count()
    }
}

/// Produce a [`Plan`] from the desired and live manifest sets.
///
/// - Resource present in both and bodies equal → `NoOp`.
/// - Resource present in both and bodies differ → `Apply`.
/// - Resource present only in desired → `Apply` (create).
/// - Resource present only in live → `Delete`.
///
/// Ownership/prune semantics — i.e. "only delete live resources
/// that Synchrotron created" — are the caller's concern. The
/// planner assumes `live` has already been filtered to the app's
/// managed set (the informer watches a label-selected scope).
pub fn plan(desired: &[Manifest], live: &[Manifest]) -> Plan {
    // HashMap keeps iteration deterministic and sorted, so the
    // resulting Plan is order-independent w.r.t. inputs.
    let mut desired_by_ref: HashMap<ResourceRef, &Manifest> = HashMap::new();
    for m in desired {
        desired_by_ref.insert(ResourceRef::of(m), m);
    }
    let mut live_by_ref: HashMap<ResourceRef, &Manifest> = HashMap::new();
    for m in live {
        live_by_ref.insert(ResourceRef::of(m), m);
    }

    let mut entries = Vec::with_capacity(desired_by_ref.len() + live_by_ref.len());

    for (rref, desired_m) in &desired_by_ref {
        let action = match live_by_ref.get(rref) {
            Some(live_m) if manifest_bodies_equal(desired_m, live_m) => PlannedAction::NoOp,
            Some(_) => PlannedAction::Apply,
            None => PlannedAction::Apply,
        };
        entries.push(PlanEntry {
            resource: rref.clone(),
            action,
        });
    }

    for rref in live_by_ref.keys() {
        if !desired_by_ref.contains_key(rref) {
            entries.push(PlanEntry {
                resource: rref.clone(),
                action: PlannedAction::Delete,
            });
        }
    }

    // Keep the final plan ordered by resource identity so callers
    // can compare plans structurally.
    entries.sort_by(|a, b| a.resource.sort_key().cmp(&b.resource.sort_key()));
    Plan { entries }
}

/// Manifest equality used by the planner.
///
/// Today this is a direct YAML value compare. A future slice will
/// replace this with a smarter diff that strips server-side fields
/// (`metadata.resourceVersion`, managed-fields, runtime-added
/// annotations) before comparing, since those always differ between
/// desired and live and would otherwise flag every resource as
/// drift. The pure-function contract holds either way.
fn manifest_bodies_equal(a: &Manifest, b: &Manifest) -> bool {
    a.body == b.body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(kind: &str, name: &str, ns: Option<&str>, marker: &str) -> Manifest {
        let yaml = format!(
            "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n{ns_line}data:\n  marker: {marker}\n",
            ns_line = ns
                .map(|n| format!("  namespace: {n}\n"))
                .unwrap_or_default(),
        );
        let mut parsed = synchrotron_plugins::manifest::parse_stream("test", &yaml).expect("parse");
        parsed.pop().expect("one manifest")
    }

    #[test]
    fn empty_desired_and_live_yields_empty_plan() {
        let p = plan(&[], &[]);
        assert!(p.is_empty());
        assert_eq!(p.changes(), 0);
    }

    #[test]
    fn no_drift_yields_all_noop() {
        let a = manifest("ConfigMap", "cm", Some("app"), "v1");
        let p = plan(std::slice::from_ref(&a), std::slice::from_ref(&a));
        assert_eq!(p.noop_count(), 1);
        assert_eq!(p.changes(), 0);
    }

    #[test]
    fn body_difference_yields_apply() {
        let desired = manifest("ConfigMap", "cm", Some("app"), "v2");
        let live = manifest("ConfigMap", "cm", Some("app"), "v1");
        let p = plan(&[desired], &[live]);
        assert_eq!(p.apply_count(), 1);
        assert_eq!(p.changes(), 1);
    }

    #[test]
    fn desired_only_yields_apply() {
        let desired = manifest("ConfigMap", "new-cm", Some("app"), "v1");
        let p = plan(&[desired], &[]);
        assert_eq!(p.apply_count(), 1);
        assert_eq!(p.delete_count(), 0);
    }

    #[test]
    fn live_only_yields_delete() {
        let live = manifest("ConfigMap", "orphan", Some("app"), "v1");
        let p = plan(&[], &[live]);
        assert_eq!(p.delete_count(), 1);
        assert_eq!(p.apply_count(), 0);
    }

    #[test]
    fn mixed_drift_partitions_correctly() {
        let keep = manifest("ConfigMap", "keep", Some("app"), "v1");
        let update_d = manifest("ConfigMap", "update", Some("app"), "v2");
        let update_l = manifest("ConfigMap", "update", Some("app"), "v1");
        let create = manifest("ConfigMap", "create", Some("app"), "v1");
        let delete = manifest("ConfigMap", "delete", Some("app"), "v1");

        let p = plan(&[keep.clone(), update_d, create], &[keep, update_l, delete]);
        assert_eq!(p.noop_count(), 1);
        assert_eq!(p.apply_count(), 2);
        assert_eq!(p.delete_count(), 1);
        assert_eq!(p.changes(), 3);
    }

    #[test]
    fn namespace_is_part_of_identity() {
        // Same name+kind in different namespaces are distinct
        // resources: the planner should treat them independently.
        let in_a = manifest("ConfigMap", "cm", Some("a"), "v1");
        let in_b = manifest("ConfigMap", "cm", Some("b"), "v1");
        let p = plan(&[in_a], &[in_b]);
        assert_eq!(p.apply_count(), 1);
        assert_eq!(p.delete_count(), 1);
    }

    #[test]
    fn plan_is_input_order_independent() {
        let a = manifest("ConfigMap", "a", Some("x"), "v1");
        let b = manifest("ConfigMap", "b", Some("x"), "v1");
        let p1 = plan(&[a.clone(), b.clone()], &[]);
        let p2 = plan(&[b, a], &[]);
        assert_eq!(p1, p2);
    }
}
