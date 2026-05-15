//! Bridge from [`KubeSsaApplier`] (kube-side, native types) to
//! [`synchrotron_reconcile::Applier`] (engine-side, plan-driven).
//!
//! # Why an adapter
//!
//! The engine's `Applier` trait takes a [`PlanEntry`] (a
//! `ResourceRef` + `PlannedAction`) and an `Option<Manifest>` —
//! enough to dispatch Apply (with the desired body) or Delete
//! (with the live body, used to honor `OwnedResource`-style
//! prune-disabled semantics in a future slice).
//!
//! [`KubeSsaApplier`] takes a `&Manifest` for apply and an
//! `&OwnedResource` for delete. The shapes don't line up; the
//! adapter projects between them.
//!
//! # Slice scope (79e)
//!
//! What this ships:
//!   - [`KubeApplierAdapter`] implementing
//!     [`synchrotron_reconcile::Applier`] over an inner
//!     [`KubeSsaApplier`].
//!   - Apply: forwards the [`Manifest`] the engine handed in.
//!     Returns an error if the engine forgot to populate the
//!     manifest for an Apply entry — that's a bug in the caller,
//!     not a transient kube failure.
//!   - Delete: synthesizes an [`OwnedResource`] from the entry
//!     (gvk + namespace + name) with `prune_disabled = false`.
//!     Honoring per-resource prune annotations is a follow-up;
//!     it needs the live body which the engine *also* passes via
//!     `manifest`, so the wiring is in place.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use synchrotron_plugins::{Manifest, OwnedResource};
use synchrotron_reconcile::{Applier, ApplyError, PlanEntry, PlannedAction};

use crate::apply::{ApplyOptions, KubeSsaApplier};

/// Engine-facing wrapper around a single-cluster [`KubeSsaApplier`].
pub struct KubeApplierAdapter {
    inner: Arc<KubeSsaApplier>,
}

impl KubeApplierAdapter {
    pub fn new(inner: Arc<KubeSsaApplier>) -> Self {
        Self { inner }
    }
}

impl Applier for KubeApplierAdapter {
    fn apply<'a>(
        &'a self,
        entry: &'a PlanEntry,
        manifest: Option<Manifest>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplyError>> + Send + 'a>> {
        let inner = self.inner.clone();
        let action = entry.action;
        let resource = entry.resource.clone();
        Box::pin(async move {
            match action {
                PlannedAction::Apply => {
                    let m = manifest.ok_or_else(|| {
                        ApplyError::new(format!(
                            "engine bug: Apply entry without desired manifest for {:?}",
                            resource
                        ))
                    })?;
                    inner
                        .apply(&m, ApplyOptions::default())
                        .await
                        .map(|_| ())
                        .map_err(|e| ApplyError::new(e.to_string()))
                }
                PlannedAction::Delete => {
                    // Synthesize from the entry (live body's
                    // prune-disabled annotation handling will move
                    // here in a follow-up; for now the fallback
                    // never blocks deletes).
                    let owned = OwnedResource {
                        gvk: resource.gvk.clone(),
                        namespace: resource.namespace.clone(),
                        name: resource.name.clone(),
                        wave: 0,
                        prune_disabled: false,
                    };
                    inner
                        .delete(&owned)
                        .await
                        .map_err(|e| ApplyError::new(e.to_string()))
                }
                PlannedAction::NoOp => Ok(()),
            }
        })
    }
}
