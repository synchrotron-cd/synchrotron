//! User-defined ignore rules for the smart diff engine (xje.3).
//!
//! Operators can declare drift that should be treated as noise on a
//! per-app or global basis. Rules are applied *after* the structural
//! diff and after auto-ignore (xje.2): if the planner is still
//! reporting a [`Change`] that the operator considers expected, an
//! ignore rule prunes it before the reconciler decides whether to
//! sync.
//!
//! # Rule types
//!
//! - [`IgnoreRule::JsonPointer`] — RFC-6901-style pointer
//!   (`/spec/replicas`). Matches the change's path exactly. Stable,
//!   precise, the right tool for a single named field.
//! - [`IgnoreRule::PathGlob`] — dotted path with `*` segments
//!   (`spec.containers.*.image`). Reuses the same syntax the
//!   list-map registry uses internally, so operators don't have to
//!   learn two notations.
//! - [`IgnoreRule::ManagerAllowlist`] — list of SSA field-manager
//!   names. A change is ignored if the **live** manifest's
//!   `metadata.managedFields` says the field is owned by any of the
//!   listed managers. This is how operators say "the HPA owns
//!   replicas; don't drift on it."
//!
//! Field-ownership filtering by *our* fieldManager (the inverse —
//! "only show drift on fields synchrotron-cd actually wrote") is the
//! job of xje.4 and lives separately.

use synchrotron_plugins::Manifest;

use crate::compare::{Change, Diff};
use crate::managed_fields::path_owned_by_any;
use crate::path::{PathSegment, ValuePath};

/// Single ignore rule. Multiple rules combine disjunctively: a
/// change is ignored if *any* rule matches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IgnoreRule {
    /// RFC-6901 JSON pointer (`/spec/replicas`,
    /// `/spec/template/spec/containers/0/image`). Only the exact
    /// path matches; descendants do not.
    JsonPointer(String),
    /// Dotted glob with `*` matching any list element. Only the
    /// exact path (after segment count) matches; descendants do not.
    /// Example: `spec.containers.*.image`.
    PathGlob(String),
    /// Ignore changes whose path is owned by any listed SSA field
    /// manager in the live manifest's `metadata.managedFields`.
    ManagerAllowlist(Vec<String>),
}

/// A bundle of rules. Apply with [`IgnoreRules::apply`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IgnoreRules {
    pub rules: Vec<IgnoreRule>,
}

impl IgnoreRules {
    pub fn new(rules: Vec<IgnoreRule>) -> Self {
        Self { rules }
    }

    pub fn push(&mut self, rule: IgnoreRule) {
        self.rules.push(rule);
    }

    pub fn extend(&mut self, other: IgnoreRules) {
        self.rules.extend(other.rules);
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Filter `diff` by these rules. The `live` manifest is needed
    /// only for [`IgnoreRule::ManagerAllowlist`] — it's the one that
    /// carries authoritative `managedFields`.
    pub fn apply(&self, diff: Diff, live: &Manifest) -> Diff {
        if self.is_empty() {
            return diff;
        }
        let kept = diff
            .changes
            .into_iter()
            .filter(|c| !self.matches(c, live))
            .collect();
        Diff { changes: kept }
    }

    fn matches(&self, change: &Change, live: &Manifest) -> bool {
        let path = change.path();
        for rule in &self.rules {
            match rule {
                IgnoreRule::JsonPointer(p) => {
                    if path_to_json_pointer(path) == *p {
                        return true;
                    }
                }
                IgnoreRule::PathGlob(g) => {
                    if path_glob_matches(g, path) {
                        return true;
                    }
                }
                IgnoreRule::ManagerAllowlist(managers) => {
                    if path_owned_by_any(path, live.body.value(), managers) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// Render a [`ValuePath`] as RFC-6901. Field names with `/` or `~`
/// are escaped per the spec. List indices render as their decimal;
/// list-map keyed segments render as the value of the *first* key
/// (matching how kube tooling displays them).
pub fn path_to_json_pointer(path: &ValuePath) -> String {
    if path.is_root() {
        return String::new();
    }
    let mut out = String::new();
    for seg in &path.segments {
        out.push('/');
        match seg {
            PathSegment::Field(name) => out.push_str(&escape_pointer(name)),
            PathSegment::Index(i) => out.push_str(&i.to_string()),
            PathSegment::Keyed(keys) => {
                let v = keys.first().map(|(_, v)| v.as_str()).unwrap_or("");
                out.push_str(&escape_pointer(v));
            }
        }
    }
    out
}

fn escape_pointer(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

/// Match a path-glob like `spec.containers.*.image` against a
/// concrete [`ValuePath`]. `*` matches any single list element
/// (positional or list-map keyed). Field segments must match
/// exactly. Segment counts must match.
fn path_glob_matches(glob: &str, path: &ValuePath) -> bool {
    let pat: Vec<&str> = if glob.is_empty() {
        Vec::new()
    } else {
        glob.split('.').collect()
    };
    if pat.len() != path.segments.len() {
        return false;
    }
    for (p, seg) in pat.iter().zip(path.segments.iter()) {
        match seg {
            PathSegment::Field(name) => {
                if *p != "*" && *p != name {
                    return false;
                }
            }
            PathSegment::Index(_) | PathSegment::Keyed(_) => {
                if *p != "*" {
                    return false;
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::{from_str, Value};
    use synchrotron_plugins::Gvk;

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

    fn modified(path: ValuePath, d: &str, l: &str) -> Change {
        Change::Modified {
            path,
            desired: Value::String(d.into()),
            live: Value::String(l.into()),
        }
    }

    #[test]
    fn json_pointer_renders_root_as_empty() {
        assert_eq!(path_to_json_pointer(&ValuePath::root()), "");
    }

    #[test]
    fn json_pointer_renders_field_chain() {
        let p = ValuePath::root().field("spec").field("replicas");
        assert_eq!(path_to_json_pointer(&p), "/spec/replicas");
    }

    #[test]
    fn json_pointer_renders_indexed_segments() {
        let p = ValuePath::root().field("spec").field("args").index(2);
        assert_eq!(path_to_json_pointer(&p), "/spec/args/2");
    }

    #[test]
    fn json_pointer_renders_keyed_segments_using_first_value() {
        let p = ValuePath::root()
            .field("spec")
            .field("containers")
            .keyed(vec![("name".into(), "app".into())])
            .field("image");
        assert_eq!(path_to_json_pointer(&p), "/spec/containers/app/image");
    }

    #[test]
    fn json_pointer_escapes_tilde_and_slash_in_field_names() {
        let p = ValuePath::root().field("a/b").field("c~d");
        assert_eq!(path_to_json_pointer(&p), "/a~1b/c~0d");
    }

    #[test]
    fn json_pointer_rule_drops_exact_match_only() {
        let live = manifest("metadata: {}");
        let rules = IgnoreRules::new(vec![IgnoreRule::JsonPointer("/spec/replicas".into())]);
        let diff = Diff {
            changes: vec![
                modified(ValuePath::root().field("spec").field("replicas"), "3", "5"),
                modified(
                    ValuePath::root().field("spec").field("paused"),
                    "false",
                    "true",
                ),
            ],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
        match &out.changes[0] {
            Change::Modified { path, .. } => assert_eq!(path.to_string(), "spec.paused"),
            _ => panic!("unexpected change variant"),
        }
    }

    #[test]
    fn path_glob_matches_wildcard_list_element() {
        let live = manifest("metadata: {}");
        let rules = IgnoreRules::new(vec![IgnoreRule::PathGlob("spec.containers.*.image".into())]);
        let diff = Diff {
            changes: vec![
                modified(
                    ValuePath::root()
                        .field("spec")
                        .field("containers")
                        .keyed(vec![("name".into(), "app".into())])
                        .field("image"),
                    "v1",
                    "v2",
                ),
                modified(
                    ValuePath::root()
                        .field("spec")
                        .field("containers")
                        .keyed(vec![("name".into(), "app".into())])
                        .field("imagePullPolicy"),
                    "Always",
                    "IfNotPresent",
                ),
            ],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
        assert!(out.changes[0]
            .path()
            .to_string()
            .ends_with(".imagePullPolicy"));
    }

    #[test]
    fn path_glob_requires_segment_count_match() {
        // Glob with 3 segments must not match a 4-segment path.
        let live = manifest("metadata: {}");
        let rules = IgnoreRules::new(vec![IgnoreRule::PathGlob("spec.containers.*".into())]);
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root()
                    .field("spec")
                    .field("containers")
                    .keyed(vec![("name".into(), "app".into())])
                    .field("image"),
                "v1",
                "v2",
            )],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
    }

    #[test]
    fn manager_allowlist_drops_changes_owned_by_listed_manager() {
        // managedFields says kube-controller-manager owns
        // /spec/replicas. Operator allows it.
        let live = manifest(
            r#"
metadata:
  managedFields:
    - manager: kube-controller-manager
      operation: Update
      fieldsV1:
        f:spec:
          f:replicas: {}
    - manager: synchrotron-cd
      operation: Apply
      fieldsV1:
        f:spec:
          f:template: {}
"#,
        );
        let rules = IgnoreRules::new(vec![IgnoreRule::ManagerAllowlist(vec![
            "kube-controller-manager".into(),
        ])]);
        let diff = Diff {
            changes: vec![
                modified(ValuePath::root().field("spec").field("replicas"), "3", "5"),
                modified(
                    ValuePath::root().field("spec").field("paused"),
                    "false",
                    "true",
                ),
            ],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
        match &out.changes[0] {
            Change::Modified { path, .. } => assert_eq!(path.to_string(), "spec.paused"),
            _ => panic!("unexpected change variant"),
        }
    }

    #[test]
    fn manager_allowlist_keeps_changes_when_manager_not_listed() {
        let live = manifest(
            r#"
metadata:
  managedFields:
    - manager: kube-controller-manager
      operation: Update
      fieldsV1:
        f:spec:
          f:replicas: {}
"#,
        );
        let rules = IgnoreRules::new(vec![IgnoreRule::ManagerAllowlist(vec![
            "hpa-controller".into()
        ])]);
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root().field("spec").field("replicas"),
                "3",
                "5",
            )],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
    }

    #[test]
    fn manager_allowlist_subtree_ownership_covers_descendants() {
        // Manager owns the whole spec.template subtree (empty child
        // map). Any descendant change should be ignored.
        let live = manifest(
            r#"
metadata:
  managedFields:
    - manager: synchrotron-cd
      operation: Apply
      fieldsV1:
        f:spec:
          f:template: {}
"#,
        );
        let rules = IgnoreRules::new(vec![IgnoreRule::ManagerAllowlist(vec![
            "synchrotron-cd".into()
        ])]);
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root()
                    .field("spec")
                    .field("template")
                    .field("metadata")
                    .field("labels"),
                "{}",
                "{a: b}",
            )],
        };
        let out = rules.apply(diff, &live);
        assert!(out.is_empty());
    }

    #[test]
    fn manager_allowlist_descends_into_keyed_list_elements() {
        let live = manifest(
            r#"
metadata:
  managedFields:
    - manager: hpa-controller
      operation: Update
      fieldsV1:
        f:spec:
          f:containers:
            "k:{\"name\":\"app\"}":
              f:resources: {}
"#,
        );
        let rules = IgnoreRules::new(vec![IgnoreRule::ManagerAllowlist(vec![
            "hpa-controller".into()
        ])]);
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root()
                    .field("spec")
                    .field("containers")
                    .keyed(vec![("name".into(), "app".into())])
                    .field("resources"),
                "old",
                "new",
            )],
        };
        let out = rules.apply(diff, &live);
        assert!(out.is_empty());
    }

    #[test]
    fn empty_ruleset_is_passthrough() {
        let live = manifest("metadata: {}");
        let rules = IgnoreRules::default();
        let diff = Diff {
            changes: vec![modified(
                ValuePath::root().field("spec").field("replicas"),
                "3",
                "5",
            )],
        };
        let out = rules.apply(diff.clone(), &live);
        assert_eq!(out, diff);
    }

    #[test]
    fn rules_combine_disjunctively() {
        let live = manifest("metadata: {}");
        let rules = IgnoreRules::new(vec![
            IgnoreRule::JsonPointer("/spec/replicas".into()),
            IgnoreRule::PathGlob("spec.containers.*.image".into()),
        ]);
        let diff = Diff {
            changes: vec![
                modified(ValuePath::root().field("spec").field("replicas"), "3", "5"),
                modified(
                    ValuePath::root()
                        .field("spec")
                        .field("containers")
                        .keyed(vec![("name".into(), "app".into())])
                        .field("image"),
                    "v1",
                    "v2",
                ),
                modified(
                    ValuePath::root().field("spec").field("paused"),
                    "false",
                    "true",
                ),
            ],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
        match &out.changes[0] {
            Change::Modified { path, .. } => assert_eq!(path.to_string(), "spec.paused"),
            _ => panic!("unexpected change variant"),
        }
    }
}
