//! List-map key registry.
//!
//! Many Kubernetes API fields are typed as lists in JSON/YAML but
//! semantically behave as keyed maps: reordering elements is a
//! no-op, and the *identity* of an element is determined by one or
//! more key fields rather than its position. A few examples
//! [from the published OpenAPI schemas]:
//!
//! - `spec.template.spec.containers` — keyed by `name`
//! - `spec.template.spec.containers[*].env` — keyed by `name`
//! - `spec.template.spec.containers[*].ports` — keyed by
//!   `containerPort` + `protocol`
//! - `spec.template.spec.volumes` — keyed by `name`
//!
//! Without this knowledge, the differ would report a giant change
//! whenever a controller defaulted a list to a different order. With
//! it, the differ matches elements by their key fields and recurses
//! into matched pairs.
//!
//! This crate ships a built-in default registry covering the
//! workload-related kinds the reconciler sees most often. Plugins or
//! configuration can extend it; xje.4 (SSA field-ownership filter)
//! will consume the same registry so its idea of "field" matches.

use std::collections::HashMap;

use synchrotron_plugins::Gvk;

use crate::path::ValuePath;

/// How to identify elements of a list-map. Path is relative to the
/// manifest body root and uses `*` to mean "any list element."
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListMapKind {
    /// Dotted path with `*` denoting list-map descents. For example,
    /// `spec.template.spec.containers.*.env` matches the env list of
    /// every container in a Pod template.
    pub path_pattern: String,
    /// Field names on each list element that together identify it.
    /// Order is preserved when rendering the path segment so paths
    /// are stable.
    pub keys: Vec<String>,
}

/// Registry of list-map descriptors keyed by GVK.
///
/// `Gvk` is the outer key — registry lookups are by the manifest's
/// kind. The inner `Vec<ListMapKind>` is checked path-by-path during
/// the diff descent.
#[derive(Debug, Clone, Default)]
pub struct ListMapKeys {
    by_kind: HashMap<Gvk, Vec<ListMapKind>>,
    /// Patterns that apply to every kind (e.g. `metadata.ownerReferences`
    /// keyed by `uid`).
    universal: Vec<ListMapKind>,
}

impl ListMapKeys {
    pub fn new() -> Self {
        Self::default()
    }

    /// Built-in defaults for common workload kinds. Mirrors the
    /// `x-kubernetes-list-map-keys` annotations from the upstream
    /// OpenAPI schemas for the relevant fields. Not exhaustive — it
    /// covers what the reconcile path sees most often, and callers
    /// can extend via [`ListMapKeys::register`].
    pub fn defaults() -> Self {
        let mut me = Self::new();

        // Universal: ownerReferences are keyed by uid.
        me.universal.push(ListMapKind {
            path_pattern: "metadata.ownerReferences".into(),
            keys: vec!["uid".into()],
        });
        me.universal.push(ListMapKind {
            path_pattern: "metadata.managedFields".into(),
            keys: vec!["manager".into(), "operation".into()],
        });

        // Workload-shaped kinds (Pod, Deployment, StatefulSet,
        // DaemonSet, Job, CronJob) all expose the same nested
        // PodSpec; we register the patterns under whichever container
        // type is shipped.
        let pod_template_patterns = [
            // Inside a PodSpec — `spec.containers` for raw Pods,
            // `spec.template.spec.containers` for workload kinds.
            ("spec.containers", vec!["name"]),
            ("spec.initContainers", vec!["name"]),
            ("spec.ephemeralContainers", vec!["name"]),
            ("spec.containers.*.env", vec!["name"]),
            ("spec.containers.*.envFrom", vec!["name"]),
            ("spec.containers.*.ports", vec!["containerPort", "protocol"]),
            ("spec.containers.*.volumeMounts", vec!["mountPath"]),
            ("spec.initContainers.*.env", vec!["name"]),
            ("spec.initContainers.*.volumeMounts", vec!["mountPath"]),
            ("spec.volumes", vec!["name"]),
            ("spec.imagePullSecrets", vec!["name"]),
            ("spec.tolerations", vec!["key", "operator", "effect"]),
            // Workload kinds wrap PodSpec in `spec.template.spec`.
            ("spec.template.spec.containers", vec!["name"]),
            ("spec.template.spec.initContainers", vec!["name"]),
            ("spec.template.spec.containers.*.env", vec!["name"]),
            ("spec.template.spec.containers.*.envFrom", vec!["name"]),
            (
                "spec.template.spec.containers.*.ports",
                vec!["containerPort", "protocol"],
            ),
            (
                "spec.template.spec.containers.*.volumeMounts",
                vec!["mountPath"],
            ),
            ("spec.template.spec.initContainers.*.env", vec!["name"]),
            (
                "spec.template.spec.initContainers.*.volumeMounts",
                vec!["mountPath"],
            ),
            ("spec.template.spec.volumes", vec!["name"]),
            ("spec.template.spec.imagePullSecrets", vec!["name"]),
            (
                "spec.template.spec.tolerations",
                vec!["key", "operator", "effect"],
            ),
        ];

        let workload_kinds = [
            ("", "Pod"),
            ("apps", "Deployment"),
            ("apps", "StatefulSet"),
            ("apps", "DaemonSet"),
            ("apps", "ReplicaSet"),
            ("batch", "Job"),
            ("batch", "CronJob"),
        ];
        for (group, kind) in workload_kinds {
            for (pat, keys) in &pod_template_patterns {
                me.register(
                    Gvk {
                        group: group.into(),
                        version: "*".into(),
                        kind: kind.into(),
                    },
                    ListMapKind {
                        path_pattern: (*pat).into(),
                        keys: keys.iter().map(|s| (*s).into()).collect(),
                    },
                );
            }
        }

        // Service.spec.ports keyed by port + protocol.
        me.register(
            Gvk {
                group: String::new(),
                version: "*".into(),
                kind: "Service".into(),
            },
            ListMapKind {
                path_pattern: "spec.ports".into(),
                keys: vec!["port".into(), "protocol".into()],
            },
        );

        me
    }

    /// Register a list-map descriptor for a kind. Version is
    /// matched as a wildcard if the registered Gvk uses `version =
    /// "*"`, otherwise an exact match is required.
    pub fn register(&mut self, gvk: Gvk, kind_desc: ListMapKind) {
        self.by_kind.entry(gvk).or_default().push(kind_desc);
    }

    /// Check whether `path` for this `gvk` is a registered list-map.
    /// Returns the keys to use for child identification.
    pub fn keys_for(&self, gvk: &Gvk, path: &ValuePath) -> Option<&[String]> {
        let rendered = render_for_match(path);
        for k in self.universal.iter() {
            if pattern_matches(&k.path_pattern, &rendered) {
                return Some(&k.keys);
            }
        }
        // Look up by exact gvk first, then by version wildcard.
        if let Some(rules) = self.by_kind.get(gvk) {
            for k in rules {
                if pattern_matches(&k.path_pattern, &rendered) {
                    return Some(&k.keys);
                }
            }
        }
        let wildcard = Gvk {
            group: gvk.group.clone(),
            version: "*".into(),
            kind: gvk.kind.clone(),
        };
        if let Some(rules) = self.by_kind.get(&wildcard) {
            for k in rules {
                if pattern_matches(&k.path_pattern, &rendered) {
                    return Some(&k.keys);
                }
            }
        }
        None
    }
}

/// Render a path into the dot/star form used for pattern matching.
/// Field segments keep their name; `Index` and `Keyed` segments
/// collapse to `*` (we only care that the segment is a list element,
/// not which one).
fn render_for_match(path: &ValuePath) -> String {
    let mut out = String::new();
    for (i, seg) in path.segments.iter().enumerate() {
        match seg {
            crate::path::PathSegment::Field(name) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(name);
            }
            crate::path::PathSegment::Index(_) | crate::path::PathSegment::Keyed(_) => {
                out.push_str(".*");
            }
        }
    }
    out
}

/// Match a pattern (dotted with `*` for list elements) against a
/// rendered path. Both inputs use `.` as the segment separator.
fn pattern_matches(pattern: &str, rendered: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let r: Vec<&str> = rendered.split('.').collect();
    if p.len() != r.len() {
        return false;
    }
    p.iter().zip(r.iter()).all(|(p, r)| *p == "*" || *p == *r)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployment_gvk() -> Gvk {
        Gvk {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        }
    }

    #[test]
    fn defaults_recognise_deployment_containers() {
        let lm = ListMapKeys::defaults();
        let path = ValuePath::root()
            .field("spec")
            .field("template")
            .field("spec")
            .field("containers");
        let keys = lm.keys_for(&deployment_gvk(), &path).expect("registered");
        assert_eq!(keys, &["name".to_string()]);
    }

    #[test]
    fn defaults_recognise_container_ports_with_composite_key() {
        let lm = ListMapKeys::defaults();
        let path = ValuePath::root()
            .field("spec")
            .field("template")
            .field("spec")
            .field("containers")
            .keyed(vec![("name".into(), "app".into())])
            .field("ports");
        let keys = lm.keys_for(&deployment_gvk(), &path).expect("registered");
        assert_eq!(keys, &["containerPort".to_string(), "protocol".into()]);
    }

    #[test]
    fn unknown_path_returns_none() {
        let lm = ListMapKeys::defaults();
        let path = ValuePath::root()
            .field("spec")
            .field("totally")
            .field("madeUp");
        assert!(lm.keys_for(&deployment_gvk(), &path).is_none());
    }

    #[test]
    fn universal_owner_references_match_for_any_kind() {
        let lm = ListMapKeys::defaults();
        let path = ValuePath::root().field("metadata").field("ownerReferences");
        let some = lm.keys_for(
            &Gvk {
                group: "example.com".into(),
                version: "v1".into(),
                kind: "Widget".into(),
            },
            &path,
        );
        assert_eq!(some, Some(&["uid".to_string()][..]));
    }
}
