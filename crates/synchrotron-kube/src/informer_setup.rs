//! Helper for slice wba: spawn one [`Informer<DynamicObject>`] per
//! GVK against a cluster, pipe each event into a
//! [`LiveStoreUpdater`] task. Exists in the kube crate (not the
//! server) because it composes types from this crate
//! (`KubeClient`, `Informer`, `LiveStoreUpdater`) and shouldn't
//! force callers to know all of them individually.
//!
//! # The starter GVK set
//!
//! [`DEFAULT_LIVE_GVKS`] picks the kinds that 90%+ of GitOps apps
//! deploy. Apps that ship odd kinds (CRDs, kpack Builders, custom
//! controllers) won't have their live state reflected until
//! discovery is wired off the AppCache or the OwnedResources
//! ledger — filed as a follow-up alongside this slice.
//!
//! # Health channel
//!
//! [`Informer::spawn`] takes a `watch::Receiver<HealthState>` so it
//! can pause/restart on cluster up/down. Until the
//! [`crate::HealthMonitor`] is wired into `synchrotron-server::main`
//! we feed a static `HealthState::Up` channel — the supervisor
//! short-circuits on the always-up signal and the watch starts
//! immediately.

use std::sync::Arc;

use kube::api::DynamicObject;
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use synchrotron_types::ClusterName;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::client::KubeClient;
use crate::health::HealthState;
use crate::informer::{dynamic_watch_factory, ClientProvider, Informer, InformerConfig};
use crate::live_source::{LiveStore, LiveStoreUpdater, DEFAULT_APP_LABEL};

/// Default kinds to watch per cluster. Covers the typical surface
/// area of an app: workloads, networking, configuration, jobs.
/// CRDs / cluster-scoped niche kinds aren't here — extend this list
/// (or thread a configurable one) when an app deploys something we
/// don't watch yet.
pub const DEFAULT_LIVE_GVKS: &[(&str, &str, &str)] = &[
    ("", "v1", "ConfigMap"),
    ("", "v1", "Secret"),
    ("", "v1", "Service"),
    ("apps", "v1", "Deployment"),
    ("apps", "v1", "StatefulSet"),
    ("apps", "v1", "DaemonSet"),
    ("batch", "v1", "Job"),
    ("batch", "v1", "CronJob"),
    ("networking.k8s.io", "v1", "Ingress"),
];

/// Owns the per-cluster informers plus the routing tasks that drain
/// each informer's broadcast into a [`LiveStoreUpdater`]. Keep
/// alive for the process lifetime — dropping it aborts every spawn.
pub struct ClusterInformerHandles {
    pub informers: Vec<Informer<DynamicObject>>,
    pub routers: Vec<JoinHandle<()>>,
    /// Sender for the static health channel. Held so the channel
    /// stays open for the informer supervisors. Sender drop ⇒
    /// channel closes ⇒ informer exits cleanly.
    _health_tx: watch::Sender<HealthState>,
}

/// Spawn the default-GVK informer set against `cluster`, routing
/// every event into the supplied [`LiveStore`] (keyed by
/// `cluster_name`). Discovery failures are logged + skipped: a
/// missing kind in this cluster (e.g. no Ingress controller
/// installed) doesn't stop the others from coming up.
pub async fn spawn_default_informers(
    cluster_name: ClusterName,
    client: KubeClient,
    store: Arc<LiveStore>,
) -> ClusterInformerHandles {
    spawn_informers_for_gvks(cluster_name, client, store, DEFAULT_LIVE_GVKS).await
}

/// Like [`spawn_default_informers`] but with an explicit GVK list.
/// Useful for tests and for the eventual configurable variant.
pub async fn spawn_informers_for_gvks(
    cluster_name: ClusterName,
    client: KubeClient,
    store: Arc<LiveStore>,
    gvks: &[(&str, &str, &str)],
) -> ClusterInformerHandles {
    // Static health channel — see module doc.
    let (health_tx, health_rx) = watch::channel(HealthState::Up);

    let mut informers = Vec::with_capacity(gvks.len());
    let mut routers = Vec::with_capacity(gvks.len());

    for (group, version, kind) in gvks {
        let gvk = GroupVersionKind::gvk(group, version, kind);
        let api_resource = match pinned_kind(client.client(), &gvk).await {
            Ok((ar, _caps)) => ar,
            Err(e) => {
                warn!(
                    cluster = %cluster_name,
                    %group, %version, %kind,
                    error = %e,
                    "informer: GVK not present in cluster; skipping"
                );
                continue;
            }
        };

        // ClientProvider hands the supervisor the same client on
        // every restart for now — once HealthMonitor is wired the
        // provider should re-pull the freshly rebuilt client after
        // an Up→Down→Up cycle.
        let client_for_provider = client.clone();
        let provider: ClientProvider = Arc::new(move || {
            let c = client_for_provider.clone();
            Box::pin(async move { c })
        });

        let cfg = InformerConfig {
            namespace: None,
            // Only resources we own. App-name attribution happens in
            // LiveStoreUpdater off the per-object label.
            label_selector: Some(DEFAULT_APP_LABEL.to_string()),
            field_selector: None,
        };
        let factory = dynamic_watch_factory(api_resource);

        let informer: Informer<DynamicObject> =
            Informer::spawn(provider, health_rx.clone(), cfg, factory);

        let updater = LiveStoreUpdater::new(store.clone(), cluster_name.clone());
        let mut rx = informer.subscribe();
        let router_cluster = cluster_name.clone();
        let kind_label = kind.to_string();
        let router = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(evt) => {
                        let _ = updater.handle_event(evt);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        info!(cluster = %router_cluster, kind = %kind_label, "informer router exiting");
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(cluster = %router_cluster, kind = %kind_label, lagged = n, "informer router lagged");
                    }
                }
            }
        });
        routers.push(router);
        informers.push(informer);
        info!(cluster = %cluster_name, %group, %version, %kind, "informer spawned");
    }

    ClusterInformerHandles {
        informers,
        routers,
        _health_tx: health_tx,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_set_covers_typical_app_kinds() {
        // Sanity: the well-known kinds GitOps apps use are in here.
        let kinds: Vec<&str> = DEFAULT_LIVE_GVKS.iter().map(|(_, _, k)| *k).collect();
        for needed in [
            "ConfigMap",
            "Deployment",
            "Service",
            "Secret",
            "StatefulSet",
            "Ingress",
            "Job",
        ] {
            assert!(
                kinds.contains(&needed),
                "DEFAULT_LIVE_GVKS missing {needed}"
            );
        }
    }
}
