//! SSA field-ownership filter (xje.4).
//!
//! Server-Side Apply tracks, for each field of a Kubernetes object,
//! which *manager* last wrote it. When synchrotron-cd applies a
//! manifest, the kube-apiserver records us as the manager of every
//! field in our patch. Anything we *didn't* write — defaulted by the
//! API server, set by a controller (HPA, VPA, KEDA), patched by a
//! human via `kubectl edit`, or owned by another GitOps tool — is
//! attributed to a *different* manager.
//!
//! For drift detection, this attribution is decisive. A change to a
//! field we don't own is, by definition, not drift from *our*
//! desired state — we never claimed it. Reporting it as drift would
//! cause us to fight with whoever does own it (the canonical example
//! being a fight with the HPA over `spec.replicas`).
//!
//! [`FieldOwnershipFilter::apply`] partitions a [`Diff`] into:
//!
//! - **drift**: changes whose path the live manifest attributes to
//!   our manager, *or* changes for which no manager is recorded.
//!   The latter case covers `Added` changes (we want a field that
//!   doesn't exist yet) and any field that has somehow escaped
//!   managedFields tracking. If we'd be the one to write it, it's
//!   ours to drift on.
//! - **informational**: changes attributed exclusively to a
//!   different manager. These are surfaced for visibility (logs,
//!   the UI) but do not gate sync.
//!
//! This filter runs *after* the structural diff and *after* the
//! ignore rules — by the time we get here, only changes the operator
//! still cares about are present, and we're answering the final
//! question: "is this our fight?"

use synchrotron_plugins::Manifest;

use crate::compare::Diff;
use crate::managed_fields::owner_of;

/// Configures field-ownership filtering by SSA fieldManager.
#[derive(Debug, Clone)]
pub struct FieldOwnershipFilter {
    /// The fieldManager string synchrotron-cd uses when applying
    /// manifests. Must match the value passed to the kube-apiserver
    /// in `?fieldManager=…` on dry-run and live applies.
    pub our_manager: String,
}

impl FieldOwnershipFilter {
    pub fn new(our_manager: impl Into<String>) -> Self {
        Self {
            our_manager: our_manager.into(),
        }
    }

    /// Partition `diff` into drift (we own it or it's unowned) and
    /// informational (some other manager owns it).
    pub fn apply(&self, diff: Diff, live: &Manifest) -> FilteredDiff {
        let mut drift = Vec::new();
        let mut informational = Vec::new();
        for change in diff.changes {
            match owner_of(change.path(), live.body.value()) {
                Some(m) if m == self.our_manager => drift.push(change),
                Some(_) => informational.push(change),
                None => drift.push(change),
            }
        }
        FilteredDiff {
            drift: Diff { changes: drift },
            informational: Diff {
                changes: informational,
            },
        }
    }
}

/// Partitioned diff produced by [`FieldOwnershipFilter::apply`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilteredDiff {
    /// Changes that should drive sync decisions.
    pub drift: Diff,
    /// Changes attributed to other managers; surfaced for visibility
    /// only.
    pub informational: Diff,
}

impl FilteredDiff {
    pub fn has_drift(&self) -> bool {
        !self.drift.is_empty()
    }

    pub fn total_changes(&self) -> usize {
        self.drift.len() + self.informational.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::{from_str, Value};
    use synchrotron_plugins::Gvk;

    use crate::compare::Change;
    use crate::path::ValuePath;

    fn manifest(yaml: &str) -> Manifest {
        let body: Value = from_str(yaml).unwrap();
        Manifest {
            gvk: Gvk {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: None,
            name: "x".into(),
            body: body.into(),
        }
    }

    fn modified(path: ValuePath) -> Change {
        Change::Modified {
            path,
            desired: Value::String("d".into()),
            live: Value::String("l".into()),
        }
    }

    fn added(path: ValuePath) -> Change {
        Change::Added {
            path,
            desired: Value::String("d".into()),
        }
    }

    /// managedFields fixture with two managers:
    /// - synchrotron-cd owns spec.template subtree
    /// - kube-controller-manager owns spec.replicas
    fn fixture() -> Manifest {
        manifest(
            r#"
metadata:
  managedFields:
    - manager: synchrotron-cd
      operation: Apply
      fieldsV1:
        f:spec:
          f:template: {}
    - manager: kube-controller-manager
      operation: Update
      fieldsV1:
        f:spec:
          f:replicas: {}
"#,
        )
    }

    #[test]
    fn changes_to_our_fields_are_drift() {
        let live = fixture();
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root()
                    .field("spec")
                    .field("template")
                    .field("metadata")
                    .field("labels"),
            )],
        };
        let out = filter.apply(diff, &live);
        assert_eq!(out.drift.len(), 1);
        assert!(out.informational.is_empty());
    }

    #[test]
    fn changes_to_other_manager_fields_are_informational() {
        let live = fixture();
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let diff = Diff {
            changes: vec![modified(ValuePath::root().field("spec").field("replicas"))],
        };
        let out = filter.apply(diff, &live);
        assert!(out.drift.is_empty());
        assert_eq!(out.informational.len(), 1);
        assert!(!out.has_drift());
    }

    #[test]
    fn unowned_paths_count_as_drift() {
        // No manager owns spec.paused — we'd be the one to write it,
        // so it counts as our drift.
        let live = fixture();
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let diff = Diff {
            changes: vec![modified(ValuePath::root().field("spec").field("paused"))],
        };
        let out = filter.apply(diff, &live);
        assert_eq!(out.drift.len(), 1);
        assert!(out.informational.is_empty());
    }

    #[test]
    fn added_changes_count_as_drift_even_when_path_unmapped() {
        // Added means desired wants a field that isn't in live yet,
        // so by definition managedFields can't list it. We'd be its
        // creator → drift.
        let live = fixture();
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let diff = Diff {
            changes: vec![added(ValuePath::root().field("spec").field("strategy"))],
        };
        let out = filter.apply(diff, &live);
        assert_eq!(out.drift.len(), 1);
    }

    #[test]
    fn manifest_without_managedfields_treats_all_as_drift() {
        // Older clusters or objects created before SSA may have no
        // managedFields. Be conservative: own everything we change.
        let live = manifest("metadata: {}");
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let diff = Diff {
            changes: vec![
                modified(ValuePath::root().field("spec").field("replicas")),
                modified(ValuePath::root().field("metadata").field("annotations")),
            ],
        };
        let out = filter.apply(diff, &live);
        assert_eq!(out.drift.len(), 2);
        assert!(out.informational.is_empty());
    }

    #[test]
    fn partition_is_total_and_disjoint() {
        let live = fixture();
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let our_path = ValuePath::root()
            .field("spec")
            .field("template")
            .field("metadata");
        let theirs_path = ValuePath::root().field("spec").field("replicas");
        let unowned_path = ValuePath::root().field("spec").field("paused");
        let diff = Diff {
            changes: vec![
                modified(our_path.clone()),
                modified(theirs_path.clone()),
                modified(unowned_path.clone()),
            ],
        };
        let out = filter.apply(diff, &live);
        assert_eq!(out.total_changes(), 3);
        assert_eq!(out.drift.len(), 2); // ours + unowned
        assert_eq!(out.informational.len(), 1);
    }

    #[test]
    fn keyed_subtree_ownership_is_attributed_correctly() {
        // VPA owns spec.template.spec.containers[name=app].resources
        let live = manifest(
            r#"
metadata:
  managedFields:
    - manager: vpa-recommender
      operation: Update
      fieldsV1:
        f:spec:
          f:template:
            f:spec:
              f:containers:
                "k:{\"name\":\"app\"}":
                  f:resources: {}
"#,
        );
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root()
                    .field("spec")
                    .field("template")
                    .field("spec")
                    .field("containers")
                    .keyed(vec![("name".into(), "app".into())])
                    .field("resources"),
            )],
        };
        let out = filter.apply(diff, &live);
        assert!(out.drift.is_empty());
        assert_eq!(out.informational.len(), 1);
    }

    #[test]
    fn empty_diff_yields_empty_partition() {
        let live = fixture();
        let filter = FieldOwnershipFilter::new("synchrotron-cd");
        let out = filter.apply(Diff::default(), &live);
        assert!(out.drift.is_empty());
        assert!(out.informational.is_empty());
        assert_eq!(out.total_changes(), 0);
    }
}
