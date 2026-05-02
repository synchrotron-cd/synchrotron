//! The common manifest type produced by every plugin runtime.

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
    pub body: Value,
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
        body: value,
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
