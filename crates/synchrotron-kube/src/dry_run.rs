//! `kube`-rs implementation of [`synchrotron_diff::DryRunApplier`].
//!
//! `synchrotron-diff` defines the trait pure-Rust so the differ stays
//! transport-independent. This module is the production transport:
//! it asks the API server to perform a server-side apply with
//! `dryRun=All`, which runs the manifest through every defaulting
//! pass and mutating webhook *without* persisting anything. The
//! server's response is the manifest as it would appear in etcd —
//! the canonical "desired side" the differ needs.
//!
//! Discovery is done lazily per-GVK via [`discovery::pinned_kind`],
//! and resolved [`ApiResource`]s are cached so repeated reconciles
//! against the same kinds don't re-hit `/apis`. The cache is
//! invalidated on transport-shaped errors only — a stable cluster's
//! `ApiResource` shape is itself stable.
//!
//! The kind-cluster integration test that exercises the wire
//! interaction (mutating webhook + dry-run round-trip) is tracked
//! separately as a follow-up bead so this crate's unit tests stay
//! offline.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::core::GroupVersionKind;
use kube::discovery::{pinned_kind, ApiCapabilities, ApiResource, Scope};
use kube::Client;
use synchrotron_diff::{DryRunApplier, DryRunError};
use synchrotron_plugins::{Gvk, Manifest};

/// SSA dry-run normalizer backed by a live `kube::Client`.
///
/// One instance per cluster. Constructing one is cheap; the first
/// call against a given GVK does a discovery round-trip to learn the
/// resource's plural name and scope, then caches it.
pub struct KubeDryRunApplier {
    client: Client,
    field_manager: String,
    cache: Arc<Mutex<HashMap<Gvk, (ApiResource, ApiCapabilities)>>>,
}

impl KubeDryRunApplier {
    pub fn new(client: Client, field_manager: impl Into<String>) -> Self {
        Self {
            client,
            field_manager: field_manager.into(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn cached(&self, gvk: &Gvk) -> Option<(ApiResource, ApiCapabilities)> {
        self.cache.lock().ok()?.get(gvk).cloned()
    }

    fn store(&self, gvk: &Gvk, entry: (ApiResource, ApiCapabilities)) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(gvk.clone(), entry);
        }
    }

    async fn resolve(&self, gvk: &Gvk) -> Result<(ApiResource, ApiCapabilities), DryRunError> {
        if let Some(hit) = self.cached(gvk) {
            return Ok(hit);
        }
        let kube_gvk = GroupVersionKind::gvk(&gvk.group, &gvk.version, &gvk.kind);
        let entry = pinned_kind(&self.client, &kube_gvk)
            .await
            .map_err(map_kube_err)?;
        self.store(gvk, entry.clone());
        Ok(entry)
    }
}

impl DryRunApplier for KubeDryRunApplier {
    fn normalize<'a>(
        &'a self,
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<Manifest, DryRunError>> + Send + 'a>> {
        Box::pin(async move {
            let (resource, caps) = self.resolve(&manifest.gvk).await?;

            let api: Api<DynamicObject> = match caps.scope {
                Scope::Namespaced => {
                    let ns = manifest.namespace.as_deref().ok_or_else(|| {
                        DryRunError::Server(format!(
                            "{} is namespaced but manifest has no namespace",
                            manifest.gvk.kind
                        ))
                    })?;
                    Api::namespaced_with(self.client.clone(), ns, &resource)
                }
                Scope::Cluster => Api::all_with(self.client.clone(), &resource),
            };

            // SSA expects JSON; convert from our YAML-shaped body.
            let body_json = yaml_to_json(&manifest.body)
                .map_err(|e| DryRunError::Decode(format!("body→json: {e}")))?;

            let params = PatchParams::apply(&self.field_manager).dry_run().force();
            let patched = api
                .patch(&manifest.name, &params, &Patch::Apply(&body_json))
                .await
                .map_err(map_kube_err)?;

            let normalized_body = serde_json::to_value(&patched)
                .and_then(serde_json::from_value::<serde_yaml_ng::Value>)
                .map_err(|e| DryRunError::Decode(format!("response→yaml: {e}")))?;

            Ok(Manifest {
                gvk: manifest.gvk.clone(),
                namespace: manifest.namespace.clone(),
                name: manifest.name.clone(),
                body: normalized_body,
            })
        })
    }
}

/// Map a `kube::Error` to our `DryRunError`. Anything that signals an
/// HTTP failure response with a status object is `Server`; anything
/// at the wire/TLS layer is `Transport`; bad responses we couldn't
/// decode are `Decode`.
fn map_kube_err(e: kube::Error) -> DryRunError {
    use kube::Error as K;
    match e {
        K::Api(status) => DryRunError::Server(format!("{}: {}", status.reason, status.message)),
        K::HyperError(inner) => DryRunError::Transport(inner.to_string()),
        K::Service(inner) => DryRunError::Transport(inner.to_string()),
        K::HttpError(inner) => DryRunError::Transport(inner.to_string()),
        K::SerdeError(inner) => DryRunError::Decode(inner.to_string()),
        K::BuildRequest(inner) => DryRunError::Decode(inner.to_string()),
        K::ReadEvents(inner) => DryRunError::Transport(inner.to_string()),
        other => DryRunError::Transport(other.to_string()),
    }
}

/// Convert YAML→JSON via a serde round-trip. Both formats are
/// representationally similar but kube-rs takes JSON for the apply
/// body, so we go through a `serde_json::Value`.
fn yaml_to_json(v: &serde_yaml_ng::Value) -> Result<serde_json::Value, serde_json::Error> {
    // `serde_yaml_ng::Value` implements Serialize; serializing to a
    // JSON value handles the conversion natively.
    serde_json::to_value(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::from_str;

    /// Round-trips a manifest body through the YAML→JSON conversion
    /// helper; used to keep the conversion correct since the rest of
    /// the impl needs a live API server.
    #[test]
    fn yaml_to_json_preserves_scalar_types() {
        let v: serde_yaml_ng::Value = from_str("a: 1\nb: \"two\"\nc: true\n").unwrap();
        let j = yaml_to_json(&v).unwrap();
        assert_eq!(j["a"], serde_json::json!(1));
        assert_eq!(j["b"], serde_json::json!("two"));
        assert_eq!(j["c"], serde_json::json!(true));
    }

    #[test]
    fn yaml_to_json_preserves_nested_structure() {
        let v: serde_yaml_ng::Value =
            from_str("spec:\n  containers:\n    - name: app\n      image: img:1\n").unwrap();
        let j = yaml_to_json(&v).unwrap();
        assert_eq!(
            j,
            serde_json::json!({
                "spec": { "containers": [ { "name": "app", "image": "img:1" } ] }
            })
        );
    }

    #[test]
    fn yaml_to_json_handles_empty_mapping() {
        let v: serde_yaml_ng::Value = from_str("{}").unwrap();
        let j = yaml_to_json(&v).unwrap();
        assert_eq!(j, serde_json::json!({}));
    }

    #[test]
    fn map_kube_err_categorises_decode_errors() {
        // Force a SerdeError by feeding bad JSON through serde_json::from_str.
        let bad: serde_json::Error =
            serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let mapped = map_kube_err(kube::Error::SerdeError(bad));
        assert!(matches!(mapped, DryRunError::Decode(_)), "got {mapped:?}");
    }
}
