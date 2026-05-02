//! Server-Side Apply against a live cluster.
//!
//! This is the production write path: take a [`Manifest`] from the
//! desired-state cache and apply it via SSA with `fieldManager =
//! synchrotron-cd` (configurable). It mirrors the discovery-cache
//! shape of [`crate::dry_run::KubeDryRunApplier`] — one instance per
//! cluster, lazy `pinned_kind` lookups, cached `ApiResource`s — but
//! the request is *not* dry-run, so changes persist.
//!
//! # Force vs. report-on-conflict
//!
//! Two operating modes, picked per call via [`ApplyOptions::force`]:
//!
//! - `force = false` (default): the API server rejects the apply with
//!   HTTP 409 if any field we'd own is currently owned by a different
//!   manager. We parse the conflicting manager names out of the
//!   server's status message and surface them via
//!   [`ApplyError::Conflict`]. Operators see *which* controller is
//!   fighting them before any state change happens.
//! - `force = true`: SSA's `?force=true` query parameter takes
//!   ownership of the conflicting fields. This is what an operator
//!   opts into per app when they accept that synchrotron-cd is the
//!   authoritative source for those fields.
//!
//! Other errors (network, decode, non-409 server status) flow through
//! unchanged so callers can distinguish "needs attention" (Conflict)
//! from transient (Transport) and protocol (Server / Decode).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::{Api, DynamicObject, Patch, PatchParams};
use kube::core::GroupVersionKind;
use kube::discovery::{pinned_kind, ApiCapabilities, ApiResource, Scope};
use kube::Client;
use synchrotron_plugins::{Gvk, Manifest};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyOptions {
    /// Take ownership of fields currently owned by other managers. The
    /// SSA spec calls this "force conflicts"; in REST terms it adds
    /// `?force=true` to the patch request.
    pub force: bool,
}

/// Result of a successful apply: the manifest as the API server now
/// stores it. Callers can compare against the desired body to decide
/// whether the apply produced visible drift (e.g. for status
/// reporting), or simply discard it.
#[derive(Debug, Clone)]
pub struct AppliedObject {
    pub manifest: Manifest,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ApplyError {
    /// SSA refused the apply because another field manager owns at
    /// least one field we'd write. `managers` is parsed best-effort
    /// from the server's status message; if parsing fails it's empty
    /// and `reason` carries the raw message for the operator.
    #[error(
        "conflict applying {kind}/{name}: {reason}{}",
        managers_suffix(.managers)
    )]
    Conflict {
        kind: String,
        name: String,
        reason: String,
        managers: Vec<String>,
    },
    /// API server returned a non-conflict status response.
    #[error("server error: {0}")]
    Server(String),
    /// Network / TLS / connection-shaped failure. Retryable.
    #[error("transport error: {0}")]
    Transport(String),
    /// Couldn't encode the request body or decode the response.
    #[error("decode error: {0}")]
    Decode(String),
}

fn managers_suffix(managers: &[String]) -> String {
    if managers.is_empty() {
        String::new()
    } else {
        format!(" (owning managers: {})", managers.join(", "))
    }
}

/// SSA writer against a live `kube::Client`.
///
/// Cheap to construct; the first call against a given GVK does a
/// discovery round-trip. Cache is shared with no other component to
/// keep ownership clear — if the same process also runs a
/// `KubeDryRunApplier`, that's a separate cache, which is fine
/// because they're populated identically.
pub struct KubeSsaApplier {
    client: Client,
    field_manager: String,
    cache: Arc<Mutex<HashMap<Gvk, (ApiResource, ApiCapabilities)>>>,
}

impl KubeSsaApplier {
    pub fn new(client: Client, field_manager: impl Into<String>) -> Self {
        Self {
            client,
            field_manager: field_manager.into(),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn field_manager(&self) -> &str {
        &self.field_manager
    }

    fn cached(&self, gvk: &Gvk) -> Option<(ApiResource, ApiCapabilities)> {
        self.cache.lock().ok()?.get(gvk).cloned()
    }

    fn store(&self, gvk: &Gvk, entry: (ApiResource, ApiCapabilities)) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(gvk.clone(), entry);
        }
    }

    async fn resolve(&self, gvk: &Gvk) -> Result<(ApiResource, ApiCapabilities), ApplyError> {
        if let Some(hit) = self.cached(gvk) {
            return Ok(hit);
        }
        let kube_gvk = GroupVersionKind::gvk(&gvk.group, &gvk.version, &gvk.kind);
        let entry = pinned_kind(&self.client, &kube_gvk)
            .await
            .map_err(|e| map_kube_err(e, gvk, ""))?;
        self.store(gvk, entry.clone());
        Ok(entry)
    }

    /// Apply `manifest` against the cluster.
    ///
    /// On success the returned [`AppliedObject`] holds the manifest as
    /// the server now stores it (post-defaulting, post-admission). On
    /// 409 with `opts.force == false` returns
    /// [`ApplyError::Conflict`] without retrying — the caller decides
    /// whether to flip force on and re-apply, surface the conflict to
    /// an operator, or fail the sync.
    pub async fn apply(
        &self,
        manifest: &Manifest,
        opts: ApplyOptions,
    ) -> Result<AppliedObject, ApplyError> {
        let (resource, caps) = self.resolve(&manifest.gvk).await?;

        let api: Api<DynamicObject> = match caps.scope {
            Scope::Namespaced => {
                let ns = manifest.namespace.as_deref().ok_or_else(|| {
                    ApplyError::Server(format!(
                        "{} is namespaced but manifest {} has no namespace",
                        manifest.gvk.kind, manifest.name
                    ))
                })?;
                Api::namespaced_with(self.client.clone(), ns, &resource)
            }
            Scope::Cluster => Api::all_with(self.client.clone(), &resource),
        };

        let body_json = yaml_to_json(&manifest.body)
            .map_err(|e| ApplyError::Decode(format!("body→json: {e}")))?;

        let mut params = PatchParams::apply(&self.field_manager);
        if opts.force {
            params = params.force();
        }

        let patched = api
            .patch(&manifest.name, &params, &Patch::Apply(&body_json))
            .await
            .map_err(|e| map_kube_err(e, &manifest.gvk, &manifest.name))?;

        let normalized: serde_yaml_ng::Value = serde_json::to_value(&patched)
            .and_then(serde_json::from_value)
            .map_err(|e| ApplyError::Decode(format!("response→yaml: {e}")))?;

        Ok(AppliedObject {
            manifest: Manifest {
                gvk: manifest.gvk.clone(),
                namespace: manifest.namespace.clone(),
                name: manifest.name.clone(),
                body: normalized,
            },
        })
    }
}

/// Map a `kube::Error` to our [`ApplyError`].
///
/// The interesting case is HTTP 409, which SSA returns when a field
/// we'd own is held by another manager. We special-case that into
/// `Conflict` and parse the manager names out of the message;
/// everything else falls through to the same Server / Transport /
/// Decode shape as the dry-run path.
fn map_kube_err(e: kube::Error, gvk: &Gvk, name: &str) -> ApplyError {
    use kube::Error as K;
    match e {
        K::Api(status) if status.code == 409 => ApplyError::Conflict {
            kind: gvk.kind.clone(),
            name: name.to_string(),
            reason: status.message.clone(),
            managers: parse_conflict_managers(&status.message),
        },
        K::Api(status) => ApplyError::Server(format!("{}: {}", status.reason, status.message)),
        K::HyperError(inner) => ApplyError::Transport(inner.to_string()),
        K::Service(inner) => ApplyError::Transport(inner.to_string()),
        K::HttpError(inner) => ApplyError::Transport(inner.to_string()),
        K::ReadEvents(inner) => ApplyError::Transport(inner.to_string()),
        K::SerdeError(inner) => ApplyError::Decode(inner.to_string()),
        K::BuildRequest(inner) => ApplyError::Decode(inner.to_string()),
        other => ApplyError::Transport(other.to_string()),
    }
}

/// Best-effort parse of conflicting manager names from an SSA
/// conflict message.
///
/// The Kubernetes conflict message format is stable enough to scan
/// for: `... conflict with "other-mgr": <field path> ...`. We pull
/// every `"…"`-quoted token that follows a `conflict with` marker.
/// On parse failure (truly novel server message), an empty vec keeps
/// `Conflict.reason` carrying the raw text so operators can diagnose
/// without us throwing the error away.
fn parse_conflict_managers(message: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = message;
    while let Some(pos) = rest.find("conflict with") {
        rest = &rest[pos + "conflict with".len()..];
        // Skip leading whitespace and find the opening quote.
        let Some(open) = rest.find('"') else { break };
        let after_open = &rest[open + 1..];
        let Some(close) = after_open.find('"') else {
            break;
        };
        let manager = &after_open[..close];
        if !manager.is_empty() && !out.iter().any(|m| m == manager) {
            out.push(manager.to_string());
        }
        rest = &after_open[close + 1..];
    }
    out
}

fn yaml_to_json(v: &serde_yaml_ng::Value) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_conflict_managers_extracts_single() {
        let msg =
            r#"Apply failed with 1 conflict: conflict with "other-mgr" using v1: .spec.replicas"#;
        assert_eq!(parse_conflict_managers(msg), vec!["other-mgr".to_string()]);
    }

    #[test]
    fn parse_conflict_managers_extracts_multiple() {
        let msg = r#"Apply failed with 2 conflicts: conflict with "mgr-a" using v1: .spec.replicas, conflict with "mgr-b" using v1: .metadata.labels.foo"#;
        assert_eq!(
            parse_conflict_managers(msg),
            vec!["mgr-a".to_string(), "mgr-b".to_string()]
        );
    }

    #[test]
    fn parse_conflict_managers_dedups_repeated_manager() {
        let msg = r#"conflict with "same-mgr" using v1: .spec.x, conflict with "same-mgr" using v1: .spec.y"#;
        assert_eq!(parse_conflict_managers(msg), vec!["same-mgr".to_string()]);
    }

    #[test]
    fn parse_conflict_managers_empty_on_unrecognized_message() {
        assert!(parse_conflict_managers("something else entirely").is_empty());
        assert!(parse_conflict_managers("").is_empty());
    }

    #[test]
    fn apply_error_conflict_display_includes_managers() {
        let e = ApplyError::Conflict {
            kind: "Deployment".into(),
            name: "web".into(),
            reason: "raw".into(),
            managers: vec!["other".into()],
        };
        let s = e.to_string();
        assert!(s.contains("Deployment/web"));
        assert!(s.contains("other"));
    }

    #[test]
    fn apply_error_conflict_display_omits_suffix_when_empty() {
        let e = ApplyError::Conflict {
            kind: "Deployment".into(),
            name: "web".into(),
            reason: "raw".into(),
            managers: vec![],
        };
        let s = e.to_string();
        assert!(!s.contains("owning managers"));
    }

    #[test]
    fn apply_options_default_is_non_force() {
        assert!(!ApplyOptions::default().force);
    }
}
