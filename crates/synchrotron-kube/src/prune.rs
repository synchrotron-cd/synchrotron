//! Pruning for resources removed from the desired set.
//!
//! Pruning runs during a sync when an app is configured with
//! `automated.prune = true` (see
//! [`AutomatedPolicy`](synchrotron_types::AutomatedPolicy)). The
//! reconciler calls [`compute_prune_set`] with the persisted
//! owned-set (from the state DB) and the new desired manifests; the
//! result is the list of resources to delete.
//!
//! Two filters apply:
//!
//! 1. **Identity match.** A resource is a prune candidate iff it's
//!    in the owned set but not in the desired set, by `(gvk,
//!    namespace, name)`.
//! 2. **Prune-disabled annotation.** Resources whose
//!    [`OwnedResource::prune_disabled`] flag is set are skipped.
//!    The flag is captured at apply time from the manifest's
//!    annotations (see [`is_prune_disabled`]) so the sweep doesn't
//!    need a live API lookup.
//!
//! Output ordering is **reverse-wave**: highest wave first, lowest
//! last. A Service deleted in wave 5 should go before its supporting
//! ConfigMap in wave 0, mirroring the forward-wave apply order. This
//! reduces the window where a partially-deleted app exposes broken
//! references to surrounding workloads.
//!
//! ## Annotation keys
//!
//! Native: `synchrotron.io/prune: "false"` — the recommended form.
//! Argo-compat fallback: an `argocd.argoproj.io/sync-options`
//! annotation containing `Prune=false` (comma-separated, the same
//! format Argo CD uses). Either is honored, so existing Argo-managed
//! manifests Just Work after a controller swap.

use std::collections::HashSet;

use kube::api::{Api, DeleteParams, DynamicObject};
use kube::discovery::Scope;
use synchrotron_plugins::{Gvk, Manifest, OwnedResource};
use tracing::debug;

use crate::apply::{ApplyError, KubeSsaApplier};

/// Native annotation key for opting a resource out of pruning.
pub const SYNCHROTRON_PRUNE_ANNOTATION: &str = "synchrotron.io/prune";
/// Argo CD's annotation key for sync options. Carries comma-separated
/// flags; we look for `Prune=false`.
pub const ARGOCD_SYNC_OPTIONS_ANNOTATION: &str = "argocd.argoproj.io/sync-options";

/// Compute the resources that should be deleted on the next sync.
///
/// Pure: takes the persisted owned-set and the new desired-set,
/// returns the prune candidates in delete order. Reverse-wave
/// ordering, with `(gvk, namespace, name)` as the secondary sort key
/// for determinism within a wave.
pub fn compute_prune_set(
    previously_owned: &[OwnedResource],
    currently_desired: &[Manifest],
) -> Vec<OwnedResource> {
    let desired_keys: HashSet<(Gvk, Option<String>, String)> = currently_desired
        .iter()
        .map(|m| (m.gvk.clone(), m.namespace.clone(), m.name.clone()))
        .collect();

    let mut candidates: Vec<OwnedResource> = previously_owned
        .iter()
        .filter(|r| !r.prune_disabled)
        .filter(|r| !desired_keys.contains(&(r.gvk.clone(), r.namespace.clone(), r.name.clone())))
        .cloned()
        .collect();

    candidates.sort_by(|a, b| {
        // Highest wave first.
        b.wave.cmp(&a.wave).then_with(|| {
            (
                a.gvk.group.as_str(),
                a.gvk.version.as_str(),
                a.gvk.kind.as_str(),
                a.namespace.as_deref(),
                a.name.as_str(),
            )
                .cmp(&(
                    b.gvk.group.as_str(),
                    b.gvk.version.as_str(),
                    b.gvk.kind.as_str(),
                    b.namespace.as_deref(),
                    b.name.as_str(),
                ))
        })
    });

    candidates
}

/// Read the prune-disabled annotation from a manifest.
///
/// Honors both the native key
/// (`synchrotron.io/prune: "false"`) and the Argo CD form
/// (`argocd.argoproj.io/sync-options: Prune=false[,…]`). Trim and
/// case-fold the boolean parse on the native key so common typos
/// (`False`, `FALSE`) still register; the Argo form is matched
/// case-sensitively because that's how Argo itself parses it.
pub fn is_prune_disabled(manifest: &Manifest) -> bool {
    let annotations = manifest
        .body
        .get("metadata")
        .and_then(|m| m.get("annotations"));
    let Some(ann) = annotations else { return false };

    if let Some(raw) = ann
        .get(SYNCHROTRON_PRUNE_ANNOTATION)
        .and_then(|v| v.as_str())
    {
        if raw.trim().eq_ignore_ascii_case("false") {
            return true;
        }
    }

    if let Some(raw) = ann
        .get(ARGOCD_SYNC_OPTIONS_ANNOTATION)
        .and_then(|v| v.as_str())
    {
        if raw
            .split(',')
            .map(str::trim)
            .any(|opt| opt == "Prune=false")
        {
            return true;
        }
    }

    false
}

impl KubeSsaApplier {
    /// Delete a single owned resource.
    ///
    /// Uses background propagation — the API server returns once the
    /// deletion is recorded, and finalizers/dependent cleanup run
    /// asynchronously. That's the right default for prune: the
    /// reconciler's next pass will observe the resource gone, and we
    /// don't want a finalizer-stuck resource blocking the rest of
    /// the prune set.
    pub async fn delete(&self, target: &OwnedResource) -> Result<(), ApplyError> {
        let (resource, caps) = self.resolve_for_prune(&target.gvk).await?;

        let api: Api<DynamicObject> = match caps.scope {
            Scope::Namespaced => {
                let ns = target.namespace.as_deref().ok_or_else(|| {
                    ApplyError::Server(format!(
                        "{} is namespaced but owned-record for {} has no namespace",
                        target.gvk.kind, target.name
                    ))
                })?;
                Api::namespaced_with(self.client_clone(), ns, &resource)
            }
            Scope::Cluster => Api::all_with(self.client_clone(), &resource),
        };

        match api.delete(&target.name, &DeleteParams::background()).await {
            Ok(_) => Ok(()),
            // 404 ≡ already gone. That's a successful prune outcome:
            // someone (or a previous run) already removed it.
            Err(kube::Error::Api(s)) if s.code == 404 => {
                debug!(
                    name = %target.name,
                    namespace = ?target.namespace,
                    kind = %target.gvk.kind,
                    "prune target already absent"
                );
                Ok(())
            }
            Err(other) => Err(map_delete_err(other)),
        }
    }

    async fn resolve_for_prune(
        &self,
        gvk: &Gvk,
    ) -> Result<
        (
            kube::discovery::ApiResource,
            kube::discovery::ApiCapabilities,
        ),
        ApplyError,
    > {
        // We deliberately reuse the apply path's discovery semantics
        // so prune and apply share a cache. Routed through a public
        // helper to avoid leaking module-internal types.
        self.discover(gvk).await
    }

    fn client_clone(&self) -> kube::Client {
        self.client_handle()
    }
}

fn map_delete_err(e: kube::Error) -> ApplyError {
    use kube::Error as K;
    match e {
        K::Api(s) => ApplyError::Server(format!("{}: {}", s.reason, s.message)),
        K::HyperError(inner) => ApplyError::Transport(inner.to_string()),
        K::Service(inner) => ApplyError::Transport(inner.to_string()),
        K::HttpError(inner) => ApplyError::Transport(inner.to_string()),
        K::ReadEvents(inner) => ApplyError::Transport(inner.to_string()),
        K::SerdeError(inner) => ApplyError::Decode(inner.to_string()),
        K::BuildRequest(inner) => ApplyError::Decode(inner.to_string()),
        other => ApplyError::Transport(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::Value;

    fn gvk(kind: &str) -> Gvk {
        Gvk {
            group: String::new(),
            version: "v1".into(),
            kind: kind.into(),
        }
    }

    fn owned(kind: &str, name: &str, ns: Option<&str>, wave: i32, disabled: bool) -> OwnedResource {
        OwnedResource {
            gvk: gvk(kind),
            namespace: ns.map(str::to_string),
            name: name.into(),
            wave,
            prune_disabled: disabled,
        }
    }

    fn manifest(
        kind: &str,
        name: &str,
        ns: Option<&str>,
        annotations: &[(&str, &str)],
    ) -> Manifest {
        let mut ann_yaml = String::new();
        if !annotations.is_empty() {
            ann_yaml.push_str("  annotations:\n");
            for (k, v) in annotations {
                ann_yaml.push_str(&format!("    {k}: \"{v}\"\n"));
            }
        }
        let ns_line = ns
            .map(|n| format!("  namespace: {n}\n"))
            .unwrap_or_default();
        let yaml = format!(
            "apiVersion: v1\nkind: {kind}\nmetadata:\n  name: {name}\n{ns_line}{ann_yaml}",
        );
        let body: Value = serde_yaml_ng::from_str(&yaml).unwrap();
        Manifest {
            gvk: gvk(kind),
            namespace: ns.map(str::to_string),
            name: name.into(),
            body,
        }
    }

    #[test]
    fn prune_set_filters_to_owned_minus_desired() {
        let owned_set = vec![
            owned("ConfigMap", "still", Some("ns"), 0, false),
            owned("ConfigMap", "removed", Some("ns"), 0, false),
            owned("Service", "also-removed", Some("ns"), 1, false),
        ];
        let desired = vec![manifest("ConfigMap", "still", Some("ns"), &[])];
        let prune = compute_prune_set(&owned_set, &desired);
        let names: Vec<_> = prune.iter().map(|r| r.name.clone()).collect();
        assert_eq!(names, vec!["also-removed", "removed"]);
    }

    #[test]
    fn prune_set_skips_prune_disabled_rows() {
        let owned_set = vec![
            owned("ConfigMap", "keep-me", Some("ns"), 0, true),
            owned("ConfigMap", "go-away", Some("ns"), 0, false),
        ];
        let desired = vec![]; // nothing desired anymore
        let prune = compute_prune_set(&owned_set, &desired);
        assert_eq!(prune.len(), 1);
        assert_eq!(prune[0].name, "go-away");
    }

    #[test]
    fn prune_set_orders_by_wave_descending() {
        let owned_set = vec![
            owned("A", "wave-low", Some("ns"), 0, false),
            owned("B", "wave-high", Some("ns"), 5, false),
            owned("C", "wave-mid", Some("ns"), 2, false),
        ];
        let prune = compute_prune_set(&owned_set, &[]);
        let waves: Vec<_> = prune.iter().map(|r| r.wave).collect();
        assert_eq!(waves, vec![5, 2, 0]);
    }

    #[test]
    fn prune_set_handles_cluster_scoped_resources() {
        let owned_set = vec![owned("Namespace", "platform", None, 0, false)];
        let prune = compute_prune_set(&owned_set, &[]);
        assert_eq!(prune.len(), 1);
        assert!(prune[0].namespace.is_none());
    }

    #[test]
    fn prune_set_empty_when_owned_subset_of_desired() {
        let owned_set = vec![owned("ConfigMap", "x", Some("ns"), 0, false)];
        let desired = vec![
            manifest("ConfigMap", "x", Some("ns"), &[]),
            manifest("ConfigMap", "y", Some("ns"), &[]),
        ];
        assert!(compute_prune_set(&owned_set, &desired).is_empty());
    }

    #[test]
    fn is_prune_disabled_native_annotation() {
        let m = manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[("synchrotron.io/prune", "false")],
        );
        assert!(is_prune_disabled(&m));
    }

    #[test]
    fn is_prune_disabled_argo_compat_annotation() {
        let m = manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[(
                "argocd.argoproj.io/sync-options",
                "Prune=false,Delete=false",
            )],
        );
        assert!(is_prune_disabled(&m));
    }

    #[test]
    fn is_prune_disabled_native_takes_priority_over_truthy_other_keys() {
        let m = manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[
                ("synchrotron.io/prune", "true"),
                ("argocd.argoproj.io/sync-options", "Prune=false"),
            ],
        );
        // Either annotation alone disables prune. Both together
        // means the operator opted out via Argo-compat — honor it.
        assert!(is_prune_disabled(&m));
    }

    #[test]
    fn is_prune_disabled_native_case_insensitive() {
        let m = manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[("synchrotron.io/prune", "FALSE")],
        );
        assert!(is_prune_disabled(&m));
    }

    #[test]
    fn is_prune_disabled_returns_false_when_no_annotation() {
        assert!(!is_prune_disabled(&manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[]
        )));
        assert!(!is_prune_disabled(&manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[("unrelated", "value")],
        )));
    }

    #[test]
    fn is_prune_disabled_argo_partial_match_does_not_count() {
        // `Prune=false` must be a complete comma-separated entry.
        let m = manifest(
            "ConfigMap",
            "x",
            Some("ns"),
            &[("argocd.argoproj.io/sync-options", "DisablePrune=false")],
        );
        assert!(!is_prune_disabled(&m));
    }
}
