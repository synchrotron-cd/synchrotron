//! The common manifest type produced by every plugin runtime.

use serde_yaml_ng::Value;

/// Group/Version/Kind, parsed from a manifest's `apiVersion` + `kind`.
///
/// `apiVersion` in Kubernetes is `{group}/{version}` for non-core
/// resources and bare `{version}` (e.g. `v1`) for the core group.
/// We normalize the core group to the empty string so equality
/// comparisons and cache keys are unambiguous.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

/// A single Kubernetes manifest, carrying the full parsed body plus
/// the identifying fields extracted from it for fast indexing.
#[derive(Debug, Clone)]
pub struct Manifest {
    pub gvk: Gvk,
    pub name: String,
    /// `None` for cluster-scoped resources or manifests that omit it.
    pub namespace: Option<String>,
    /// Full YAML document preserved verbatim so templating plugins
    /// and the reconciler can see fields we don't model explicitly.
    pub body: Value,
}
