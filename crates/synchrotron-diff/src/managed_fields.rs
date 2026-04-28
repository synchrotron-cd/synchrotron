//! Shared helpers for walking `metadata.managedFields[].fieldsV1`.
//!
//! Both [`crate::ignore::IgnoreRule::ManagerAllowlist`] and
//! [`crate::ownership::FieldOwnershipFilter`] need to ask the same
//! question: "which manager owns this field path?" The fieldsV1
//! grammar isn't documented anywhere outside the SSA implementation,
//! so we keep the parser in one place to avoid drift.
//!
//! # fieldsV1 grammar (informal)
//!
//! Each level is a mapping whose keys are prefixed:
//!
//! - `f:<name>` — field of that name (object property)
//! - `k:<json>` — keyed list element. The suffix is a JSON object
//!   of the element's identifying key fields, e.g.
//!   `k:{"name":"app"}`.
//! - `i:<n>` — positional list index, where the list is *not*
//!   list-map keyed (rare in core APIs).
//! - `v:<value>` — leaf value (set semantics, even rarer).
//!
//! An empty child mapping means "this manager owns the whole subtree
//! rooted here." The walker treats that as a wildcard match for any
//! descendant path.

use serde_yaml_ng::Value;

use crate::path::{PathSegment, ValuePath};

/// Iterate `live_body.metadata.managedFields[]` and yield each
/// `(manager, fieldsV1)` pair. Entries missing either field are
/// skipped silently.
pub fn iter_managed_fields(live_body: &Value) -> impl Iterator<Item = (&str, &Value)> {
    live_body
        .get("metadata")
        .and_then(|m| m.get("managedFields"))
        .and_then(|f| f.as_sequence())
        .into_iter()
        .flat_map(|seq| seq.iter())
        .filter_map(|entry| {
            let manager = entry.get("manager").and_then(|m| m.as_str())?;
            let fields_v1 = entry.get("fieldsV1")?;
            Some((manager, fields_v1))
        })
}

/// Return the first manager (in managedFields order) that owns
/// `path`. Returns `None` if no manager has a fieldsV1 entry
/// covering this path.
pub fn owner_of(path: &ValuePath, live_body: &Value) -> Option<String> {
    for (manager, fields_v1) in iter_managed_fields(live_body) {
        if fields_v1_owns(fields_v1, &path.segments) {
            return Some(manager.to_string());
        }
    }
    None
}

/// True iff *any* manager in `allowed` owns `path` per managedFields.
pub fn path_owned_by_any(path: &ValuePath, live_body: &Value, allowed: &[String]) -> bool {
    iter_managed_fields(live_body).any(|(manager, fields_v1)| {
        allowed.iter().any(|a| a == manager) && fields_v1_owns(fields_v1, &path.segments)
    })
}

/// Recursive descent through a `fieldsV1` tree. See module docs for
/// the grammar.
pub fn fields_v1_owns(node: &Value, segments: &[PathSegment]) -> bool {
    let map = match node.as_mapping() {
        Some(m) => m,
        None => return false,
    };
    if segments.is_empty() {
        return true;
    }
    let (head, tail) = (&segments[0], &segments[1..]);
    let candidate_key = match head {
        PathSegment::Field(name) => format!("f:{name}"),
        PathSegment::Index(i) => format!("i:{i}"),
        PathSegment::Keyed(keys) => {
            let mut obj = String::from("{");
            for (j, (k, v)) in keys.iter().enumerate() {
                if j > 0 {
                    obj.push(',');
                }
                obj.push('"');
                obj.push_str(k);
                obj.push_str("\":\"");
                obj.push_str(v);
                obj.push('"');
            }
            obj.push('}');
            format!("k:{obj}")
        }
    };
    if let Some(child) = map.get(Value::String(candidate_key)) {
        let child_map = child.as_mapping();
        if child_map.map(|m| m.is_empty()).unwrap_or(true) {
            return true;
        }
        if fields_v1_owns(child, tail) {
            return true;
        }
    }
    false
}
