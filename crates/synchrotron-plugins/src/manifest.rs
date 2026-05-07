//! The common manifest type produced by every plugin runtime.

use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};
use serde_yaml_ng::Value;
use thiserror::Error;

/// Group/Version/Kind, parsed from a manifest's `apiVersion` + `kind`.
///
/// `apiVersion` in Kubernetes is `{group}/{version}` for non-core
/// resources and bare `{version}` (e.g. `v1`) for the core group.
/// We normalize the core group to the empty string so equality
/// comparisons and cache keys are unambiguous.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Gvk {
    pub group: String,
    pub version: String,
    pub kind: String,
}

impl Gvk {
    pub fn parse(api_version: &str, kind: &str) -> Self {
        let (group, version) = match api_version.split_once('/') {
            Some((g, v)) => (g.to_string(), v.to_string()),
            None => (String::new(), api_version.to_string()),
        };
        Self {
            group,
            version,
            kind: kind.to_string(),
        }
    }
}

/// One resource the controller has applied for an app.
///
/// Persisted in the state DB and consumed by the prune sweep:
/// reconcile time `previously_owned − currently_desired` ≡ delete
/// candidates. Captured at apply time so the sweep doesn't need a
/// live API lookup to honor `Prune=false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedResource {
    pub gvk: Gvk,
    /// `None` for cluster-scoped resources.
    pub namespace: Option<String>,
    pub name: String,
    /// Sync wave at last apply time. Reverse-wave order drives
    /// delete sequencing during prune.
    pub wave: i32,
    /// True if the manifest carried an opt-out annotation
    /// (`synchrotron.io/prune: "false"` or Argo's
    /// `argocd.argoproj.io/sync-options: Prune=false`). The prune
    /// sweep skips these rows.
    pub prune_disabled: bool,
}

/// A single Kubernetes manifest, carrying the full parsed body plus
/// the identifying fields extracted from it for fast indexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub gvk: Gvk,
    pub name: String,
    /// `None` for cluster-scoped resources or manifests that omit it.
    pub namespace: Option<String>,
    /// Full YAML document preserved verbatim so templating plugins
    /// and the reconciler can see fields we don't model explicitly.
    pub body: ManifestBody,
}

/// Wrapper around the verbatim manifest body.
///
/// **Storage model** (slice 2 of d2p): the source of truth is
/// `bytes` — canonical JSON bytes. The parsed `Value` is materialized
/// lazily on first [`ManifestBody::value`] access and cached in a
/// shared [`OnceLock`] so all clones see the same parse work. This
/// trades a one-time parse cost for a much more compact in-memory
/// footprint when the parsed view isn't needed.
///
/// In slice 2 alone, `PartialEq` still walks the parsed `Value`, so
/// the planner forces a parse on every reconcile and the cache stays
/// hot — meaning slice 2 *adds* the bytes overhead without yet
/// shedding the `Value` overhead. Slice 3 swaps `PartialEq` to a
/// hash compare against the canonical bytes; only at that point does
/// the lazy `Value` actually stay un-populated for steady-state
/// no-op reconciles, and the memory win realizes.
///
/// JSON (not YAML) is the canonical form because:
/// 1. It's strictly smaller — no whitespace, no quoting flexibility.
/// 2. It's the wire format we already serialize to for SSA
///    (`yaml_to_json` in synchrotron-kube), so the round-trip is
///    one we already exercise.
/// 3. Kubernetes manifests are JSON-compatible by definition (they
///    flow through the API server, which uses JSON internally).
///
/// Mutating the parsed tree in place is *not* supported (no
/// `value_mut`); callers that need to mutate should rebuild a new
/// `ManifestBody` from the modified `Value`. In practice the only
/// mutation sites are bench/test setup, where reconstruction is
/// cheap and clearer.
#[derive(Debug, Clone)]
pub struct ManifestBody {
    /// Canonical JSON bytes. Source of truth.
    bytes: Arc<[u8]>,
    /// FNV-1a 64-bit hash over [`bytes`]. Computed once at
    /// construction. Drives the planner's fast-path equality check —
    /// hash mismatch is the most common case (drift detected) and
    /// avoids parsing either body.
    hash: u64,
    /// Lazy parsed view, shared across clones so the parse cost
    /// is paid at most once per body instance, not once per clone.
    parsed: Arc<OnceLock<Value>>,
}

impl ManifestBody {
    /// Build from an in-memory parsed value. The value is serialized
    /// to canonical JSON for storage; the parsed cache is primed
    /// with the original value so the first `.value()` call is free.
    pub fn from_value(value: Value) -> Self {
        let bytes: Arc<[u8]> = serde_json::to_vec(&value)
            .expect("manifest body must be JSON-serializable")
            .into();
        let hash = fnv1a64(&bytes);
        let parsed = OnceLock::new();
        // Best-effort prime; fails only if the cell is already
        // populated, which can't happen on a fresh OnceLock.
        let _ = parsed.set(value);
        Self {
            bytes,
            hash,
            parsed: Arc::new(parsed),
        }
    }

    /// Build from raw canonical bytes without parsing. The parsed
    /// view is materialized lazily on first `.value()` call.
    pub fn from_bytes(bytes: Arc<[u8]>) -> Self {
        let hash = fnv1a64(&bytes);
        Self {
            bytes,
            hash,
            parsed: Arc::new(OnceLock::new()),
        }
    }

    /// 64-bit FNV-1a hash over the canonical bytes. Equal bodies
    /// (byte-for-byte) always have equal hashes; the converse holds
    /// with vanishing probability of collision (~2⁻⁶⁴ per pair).
    pub fn body_hash(&self) -> u64 {
        self.hash
    }

    /// Borrow the parsed tree, materializing it on first access.
    /// Subsequent calls (and calls on clones) return the cached
    /// value without re-parsing.
    pub fn value(&self) -> &Value {
        self.parsed.get_or_init(|| {
            serde_json::from_slice(&self.bytes)
                .expect("canonical bytes must round-trip through serde_yaml_ng::Value")
        })
    }

    /// Owned version of [`Self::value`]. Materializes if necessary
    /// and returns a clone of the parsed tree.
    pub fn into_value(self) -> Value {
        self.value().clone()
    }

    /// Borrow the canonical JSON bytes. Slice 3 will use this for
    /// hash-based equality.
    pub fn bytes(&self) -> &Arc<[u8]> {
        &self.bytes
    }
}

/// Tiered equality (slice 3 of d2p):
///
/// 1. **Hash compare** (constant time): mismatch ⇒ fast `false`,
///    no parse. This is the bulk of "manifest changed" cases.
/// 2. **Pointer compare** on `bytes`: same `Arc` ⇒ trivially equal.
///    Hits when the same source manifest is referenced twice.
/// 3. **Byte compare** on `bytes`: equal bytes ⇒ equal bodies. This
///    is the steady-state no-op path — both sources produced the
///    same canonical form.
/// 4. **Value compare** (fallback): same hash, different bytes.
///    Either a hash collision (~2⁻⁶⁴ per pair) or two semantically
///    equal bodies whose canonical forms differ (e.g. JSON key order
///    differing between the desired source and the live source).
///    Walks the parsed `Value`, forcing a parse on both sides.
///
/// In the synchrotron-bench steady-state scenario every same-app
/// reconcile hits the byte-compare tier; the parsed cache stays
/// un-populated, which is what unlocks the memory budget.
impl PartialEq for ManifestBody {
    fn eq(&self, other: &Self) -> bool {
        if self.hash != other.hash {
            return false;
        }
        if Arc::ptr_eq(&self.bytes, &other.bytes) {
            return true;
        }
        if self.bytes == other.bytes {
            return true;
        }
        self.value() == other.value()
    }
}

/// FNV-1a 64-bit. Picked over std's `DefaultHasher` because it's
/// deterministic across processes (no per-process seed) and
/// trivially fast — for ~200-byte canonical bodies the cost is in
/// the tens of nanoseconds, dwarfed by JSON serialization. Not
/// cryptographically secure, but body equality isn't a security
/// boundary.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

impl Serialize for ManifestBody {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Serialize through the parsed Value so callers that emit
        // YAML (e.g. our manifest persistence) keep working as
        // before. This is a no-op clone path through the existing
        // serde_yaml_ng::Value impl.
        self.value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ManifestBody {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Ok(Self::from_value(value))
    }
}

impl From<Value> for ManifestBody {
    fn from(value: Value) -> Self {
        Self::from_value(value)
    }
}

/// Errors from parsing a YAML stream into [`Manifest`]s.
///
/// The `source_label` field is a caller-provided string that
/// identifies where the YAML came from — a file path for the raw
/// source, a plugin name for local/sidecar output. It's surfaced
/// unmodified so both sites can format actionable messages.
#[derive(Debug, Error)]
pub enum ManifestParseError {
    #[error("{source_label} (document #{doc_index}): yaml parse error: {source}")]
    Yaml {
        source_label: String,
        doc_index: usize,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("{source_label} (document #{doc_index}): missing required field `{field}`")]
    MissingField {
        source_label: String,
        doc_index: usize,
        field: &'static str,
    },
}

/// Parse a multi-document YAML stream into manifests.
///
/// Null documents are skipped (trailing `---` is common). Each
/// non-null document must have `apiVersion`, `kind`, and
/// `metadata.name`; missing fields are rejected rather than silently
/// dropped because half-formed manifests indicate a bug upstream.
pub fn parse_stream(source_label: &str, text: &str) -> Result<Vec<Manifest>, ManifestParseError> {
    let mut out = Vec::new();
    for (doc_index, doc) in serde_yaml_ng::Deserializer::from_str(text).enumerate() {
        let value = Value::deserialize(doc).map_err(|source| ManifestParseError::Yaml {
            source_label: source_label.to_string(),
            doc_index,
            source,
        })?;
        if value.is_null() {
            continue;
        }
        out.push(value_to_manifest(source_label, doc_index, value)?);
    }
    Ok(out)
}

fn value_to_manifest(
    source_label: &str,
    doc_index: usize,
    value: Value,
) -> Result<Manifest, ManifestParseError> {
    let api_version = required_str(&value, "apiVersion", source_label, doc_index)?;
    let kind = required_str(&value, "kind", source_label, doc_index)?;
    let name = value
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .ok_or(ManifestParseError::MissingField {
            source_label: source_label.to_string(),
            doc_index,
            field: "metadata.name",
        })?
        .to_string();
    let namespace = value
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(Value::as_str)
        .map(|s| s.to_string());

    Ok(Manifest {
        gvk: Gvk::parse(&api_version, &kind),
        name,
        namespace,
        body: ManifestBody::from_value(value),
    })
}

fn required_str(
    value: &Value,
    field: &'static str,
    source_label: &str,
    doc_index: usize,
) -> Result<String, ManifestParseError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or(ManifestParseError::MissingField {
            source_label: source_label.to_string(),
            doc_index,
            field,
        })
}
