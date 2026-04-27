//! Recursive structural compare between server-normalized desired
//! and live YAML values.
//!
//! Driven by the convention that desired is *what the API server
//! would store* (post-defaulting and mutating-webhook rewrite) and
//! live is *what's currently stored*. Differences are reported as
//! [`Change`] entries with a [`ValuePath`] pointing at the leaf and
//! the before/after values from each side.
//!
//! # List-map awareness
//!
//! Lists are by default compared positionally: differing length or
//! differing element at the same index produces a change. Lists
//! registered in [`ListMapKeys`] are matched element-by-key
//! instead — the differ pulls out each element's key fields,
//! aligns desired and live by those, and recurses. This is the
//! only structurally correct way to diff containers, env vars,
//! ports, etc., where the on-the-wire order is incidental.
//!
//! # Excluded fields
//!
//! Some live-side fields would always differ from the desired
//! normalized form, regardless of whether the user changed
//! anything:
//!
//! - `metadata.resourceVersion`, `uid`, `generation`,
//!   `creationTimestamp` — server-assigned every time.
//! - `metadata.managedFields` — managed-fields tracking; xje.4
//!   uses this for ownership filtering, but it's not user-edited.
//! - `status` — the controller's report; never originates from
//!   git.
//!
//! These are stripped before comparison. The list is intentionally
//! conservative: anything else is fair game and the higher tiers
//! filter further.

use serde_yaml_ng::Value;
use synchrotron_plugins::{Gvk, Manifest};

use crate::listmap::ListMapKeys;
use crate::path::ValuePath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// Field present in desired, absent in live.
    Added { path: ValuePath, desired: Value },
    /// Field absent in desired, present in live.
    Removed { path: ValuePath, live: Value },
    /// Field present in both with non-equal values.
    Modified {
        path: ValuePath,
        desired: Value,
        live: Value,
    },
}

impl Change {
    pub fn path(&self) -> &ValuePath {
        match self {
            Change::Added { path, .. }
            | Change::Removed { path, .. }
            | Change::Modified { path, .. } => path,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Diff {
    pub changes: Vec<Change>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }
}

/// Compare two manifests structurally. The `gvk` argument is the
/// kind to look up in the list-map registry — pass the desired
/// manifest's `gvk` (desired and live should always share it; if
/// they don't, that's drift the planner already caught).
pub fn diff(desired: &Manifest, live: &Manifest, list_maps: &ListMapKeys) -> Diff {
    let mut changes = Vec::new();
    let desired_body = strip_excluded(&desired.body);
    let live_body = strip_excluded(&live.body);
    diff_value(
        &desired_body,
        &live_body,
        &ValuePath::root(),
        &desired.gvk,
        list_maps,
        &mut changes,
    );
    Diff { changes }
}

/// Convenience: true iff [`diff`] would report no changes.
pub fn manifests_equivalent(desired: &Manifest, live: &Manifest, list_maps: &ListMapKeys) -> bool {
    diff(desired, live, list_maps).is_empty()
}

fn diff_value(
    desired: &Value,
    live: &Value,
    path: &ValuePath,
    gvk: &Gvk,
    list_maps: &ListMapKeys,
    out: &mut Vec<Change>,
) {
    match (desired, live) {
        (Value::Mapping(d), Value::Mapping(l)) => {
            // Walk desired keys: present in live → recurse;
            // missing → Added.
            for (k, dv) in d {
                let key = match k.as_str() {
                    Some(s) => s.to_string(),
                    None => format!("{k:?}"),
                };
                let child = path.field(&key);
                match l.get(k) {
                    Some(lv) => diff_value(dv, lv, &child, gvk, list_maps, out),
                    None => out.push(Change::Added {
                        path: child,
                        desired: dv.clone(),
                    }),
                }
            }
            // Walk live keys not in desired → Removed.
            for (k, lv) in l {
                if d.contains_key(k) {
                    continue;
                }
                let key = match k.as_str() {
                    Some(s) => s.to_string(),
                    None => format!("{k:?}"),
                };
                out.push(Change::Removed {
                    path: path.field(&key),
                    live: lv.clone(),
                });
            }
        }
        (Value::Sequence(d), Value::Sequence(l)) => {
            if let Some(keys) = list_maps.keys_for(gvk, path) {
                diff_listmap(d, l, keys, path, gvk, list_maps, out);
            } else {
                diff_positional(d, l, path, gvk, list_maps, out);
            }
        }
        (a, b) if a == b => {}
        (a, b) => out.push(Change::Modified {
            path: path.clone(),
            desired: a.clone(),
            live: b.clone(),
        }),
    }
}

fn diff_positional(
    desired: &[Value],
    live: &[Value],
    path: &ValuePath,
    gvk: &Gvk,
    list_maps: &ListMapKeys,
    out: &mut Vec<Change>,
) {
    let max = desired.len().max(live.len());
    for i in 0..max {
        let child = path.index(i);
        match (desired.get(i), live.get(i)) {
            (Some(d), Some(l)) => diff_value(d, l, &child, gvk, list_maps, out),
            (Some(d), None) => out.push(Change::Added {
                path: child,
                desired: d.clone(),
            }),
            (None, Some(l)) => out.push(Change::Removed {
                path: child,
                live: l.clone(),
            }),
            (None, None) => unreachable!(),
        }
    }
}

fn diff_listmap(
    desired: &[Value],
    live: &[Value],
    keys: &[String],
    path: &ValuePath,
    gvk: &Gvk,
    list_maps: &ListMapKeys,
    out: &mut Vec<Change>,
) {
    // Desired side drives recursion; live-only elements become
    // Removed entries at the end.
    let mut matched_live: Vec<bool> = vec![false; live.len()];
    for d in desired {
        let key_pairs = extract_keys(d, keys);
        let child = path.keyed(key_pairs.clone());
        let mut found = false;
        for (i, l) in live.iter().enumerate() {
            if matched_live[i] {
                continue;
            }
            if extract_keys(l, keys) == key_pairs {
                diff_value(d, l, &child, gvk, list_maps, out);
                matched_live[i] = true;
                found = true;
                break;
            }
        }
        if !found {
            out.push(Change::Added {
                path: child,
                desired: d.clone(),
            });
        }
    }
    for (i, l) in live.iter().enumerate() {
        if matched_live[i] {
            continue;
        }
        let key_pairs = extract_keys(l, keys);
        out.push(Change::Removed {
            path: path.keyed(key_pairs),
            live: l.clone(),
        });
    }
}

/// Pull each named key off the element. Missing keys get an empty
/// string so unkeyed elements still align deterministically (and a
/// fully-unkeyed element on each side will match its peer).
fn extract_keys(v: &Value, keys: &[String]) -> Vec<(String, String)> {
    keys.iter()
        .map(|k| {
            let value = v.get(k.as_str()).map(scalar_to_string).unwrap_or_default();
            (k.clone(), value)
        })
        .collect()
}

fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".into(),
        // For composite key fields (rare), fall back to the YAML
        // serialization. This keeps the path stable but isn't
        // pretty.
        other => serde_yaml_ng::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_string(),
    }
}

/// Drop server-managed fields from a manifest body before
/// comparison. See module docs for the exclusion list.
fn strip_excluded(body: &Value) -> Value {
    let mut cloned = body.clone();
    if let Value::Mapping(map) = &mut cloned {
        if let Some(Value::Mapping(meta)) = map.get_mut("metadata") {
            for k in [
                "resourceVersion",
                "uid",
                "generation",
                "creationTimestamp",
                "managedFields",
                "selfLink",
            ] {
                meta.remove(k);
            }
        }
        map.remove("status");
    }
    cloned
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchrotron_plugins::manifest::parse_stream;

    fn parse(yaml: &str) -> Manifest {
        parse_stream("t", yaml).expect("parse").pop().unwrap()
    }

    fn deployment(containers: &str, replicas: u32, extra_meta: &str) -> Manifest {
        parse(&format!(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\n{extra_meta}spec:\n  replicas: {replicas}\n  template:\n    spec:\n      containers:\n{containers}",
        ))
    }

    #[test]
    fn identical_manifests_yield_no_diff() {
        let m = deployment("        - name: app\n          image: nginx:1\n", 1, "");
        assert!(manifests_equivalent(&m, &m, &ListMapKeys::defaults()));
    }

    #[test]
    fn scalar_change_yields_modified_at_leaf() {
        let d = deployment("        - name: app\n          image: nginx:2\n", 1, "");
        let l = deployment("        - name: app\n          image: nginx:1\n", 1, "");
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        assert_eq!(diff.len(), 1);
        let path = diff.changes[0].path().to_string();
        assert_eq!(path, "spec.template.spec.containers[name=app].image");
        assert!(matches!(diff.changes[0], Change::Modified { .. }));
    }

    #[test]
    fn container_reordering_is_not_drift() {
        // Same containers in opposite order — list-map matching
        // by name should align them and report nothing.
        let d = deployment(
            "        - name: app\n          image: nginx:1\n        - name: side\n          image: side:1\n",
            1,
            "",
        );
        let l = deployment(
            "        - name: side\n          image: side:1\n        - name: app\n          image: nginx:1\n",
            1,
            "",
        );
        assert!(manifests_equivalent(&d, &l, &ListMapKeys::defaults()));
    }

    #[test]
    fn added_container_is_one_change() {
        let d = deployment(
            "        - name: app\n          image: nginx:1\n        - name: side\n          image: side:1\n",
            1,
            "",
        );
        let l = deployment("        - name: app\n          image: nginx:1\n", 1, "");
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        assert_eq!(diff.len(), 1);
        match &diff.changes[0] {
            Change::Added { path, .. } => {
                assert_eq!(path.to_string(), "spec.template.spec.containers[name=side]");
            }
            other => panic!("expected Added, got {other:?}"),
        }
    }

    #[test]
    fn removed_container_is_one_change() {
        let d = deployment("        - name: app\n          image: nginx:1\n", 1, "");
        let l = deployment(
            "        - name: app\n          image: nginx:1\n        - name: side\n          image: side:1\n",
            1,
            "",
        );
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        assert_eq!(diff.len(), 1);
        match &diff.changes[0] {
            Change::Removed { path, .. } => {
                assert_eq!(path.to_string(), "spec.template.spec.containers[name=side]");
            }
            other => panic!("expected Removed, got {other:?}"),
        }
    }

    #[test]
    fn status_field_is_stripped_before_diff() {
        let d = parse(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\ndata:\n  k: v\n",
        );
        let l = parse(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\ndata:\n  k: v\nstatus:\n  whatever: yes\n",
        );
        assert!(manifests_equivalent(&d, &l, &ListMapKeys::defaults()));
    }

    #[test]
    fn server_added_metadata_fields_are_stripped() {
        // Live carries resourceVersion / uid / managedFields;
        // desired does not. None should show up as drift.
        let d = parse(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\ndata:\n  k: v\n",
        );
        let l = parse(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\n  resourceVersion: \"42\"\n  uid: 11111111-2222-3333-4444-555555555555\n  generation: 7\n  managedFields:\n    - manager: kubectl\n      operation: Apply\ndata:\n  k: v\n",
        );
        assert!(manifests_equivalent(&d, &l, &ListMapKeys::defaults()));
    }

    #[test]
    fn user_added_annotation_in_live_is_a_removed_change() {
        // Desired has no `extra` annotation; live has one. From
        // desired's POV that's a removal (a field present in live
        // but not desired). The differ doesn't editorialise — it
        // reports the structural fact and lets higher tiers
        // (xje.4 SSA ownership filter) decide whether to ignore.
        let d = parse(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\ndata:\n  k: v\n",
        );
        let l = parse(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\n  namespace: app\n  annotations:\n    extra: yes\ndata:\n  k: v\n",
        );
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        assert_eq!(diff.len(), 1);
        assert_eq!(diff.changes[0].path().to_string(), "metadata.annotations");
    }

    #[test]
    fn ports_keyed_by_composite_align_correctly() {
        // Two ports with same containerPort but different
        // protocol — must align by both, not collide.
        let yaml = |a_protocol: &str, b_protocol: &str| {
            format!(
                "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\nspec:\n  replicas: 1\n  template:\n    spec:\n      containers:\n        - name: app\n          image: nginx\n          ports:\n            - containerPort: 80\n              protocol: {a_protocol}\n            - containerPort: 80\n              protocol: {b_protocol}\n",
            )
        };
        let d = parse(&yaml("TCP", "UDP"));
        let l = parse(&yaml("UDP", "TCP"));
        // Same set of ports in different order → no diff.
        assert!(manifests_equivalent(&d, &l, &ListMapKeys::defaults()));
    }

    #[test]
    fn positional_list_without_registry_diffs_by_index() {
        // `args` on a container is a positional list, not keyed.
        // Reordering really *is* a change.
        let d = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\nspec:\n  replicas: 1\n  template:\n    spec:\n      containers:\n        - name: app\n          image: nginx\n          args:\n            - --foo\n            - --bar\n",
        );
        let l = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\nspec:\n  replicas: 1\n  template:\n    spec:\n      containers:\n        - name: app\n          image: nginx\n          args:\n            - --bar\n            - --foo\n",
        );
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        assert_eq!(diff.len(), 2);
    }

    #[test]
    fn env_var_change_within_keyed_list_pinpoints_leaf() {
        let yaml = |val: &str| {
            format!(
                "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: app\nspec:\n  replicas: 1\n  template:\n    spec:\n      containers:\n        - name: app\n          image: nginx\n          env:\n            - name: GREETING\n              value: {val}\n            - name: REGION\n              value: us-east-1\n",
            )
        };
        let d = parse(&yaml("hello"));
        let l = parse(&yaml("hi"));
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        assert_eq!(diff.len(), 1);
        assert_eq!(
            diff.changes[0].path().to_string(),
            "spec.template.spec.containers[name=app].env[name=GREETING].value"
        );
    }

    #[test]
    fn nested_modify_and_add_produces_two_entries() {
        let d = deployment(
            "        - name: app\n          image: nginx:2\n        - name: side\n          image: side:1\n",
            3,
            "",
        );
        let l = deployment("        - name: app\n          image: nginx:1\n", 1, "");
        let diff = diff(&d, &l, &ListMapKeys::defaults());
        // replicas 1→3, image 1→2, side container added = 3 changes.
        assert_eq!(diff.len(), 3);
    }

    #[test]
    fn empty_diff_has_zero_length() {
        let m = deployment("        - name: app\n          image: nginx:1\n", 1, "");
        let d = diff(&m, &m, &ListMapKeys::defaults());
        assert!(d.is_empty());
        assert_eq!(d.len(), 0);
    }
}
