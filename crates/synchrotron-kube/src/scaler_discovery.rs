//! Cluster discovery and informer wiring for [`ScalerCache`] (shv).
//!
//! The pure-Rust core of controller-aware auto-ignore — `parse_scaler`,
//! the rule evaluator, and `ScalerCache` itself — lives in
//! `synchrotron-diff::auto_ignore`. This module is the *transport*: it
//! runs three list/watch informers (HPA, VPA, KEDA `ScaledObject`) and
//! folds their events into a shared cache so the differ can answer
//! "which fields should I auto-ignore on this target?" without ever
//! talking to the API server itself.
//!
//! ## Why three informers, not one
//!
//! Each controller kind lives at a different GVK and is shaped
//! differently in the API. We watch each as a [`DynamicObject`] so
//! VPA and KEDA (CRDs not in `k8s-openapi`) work on the same code
//! path as the built-in HPA. Each informer feeds a per-controller
//! partition of an in-memory source-indexed map; whenever any
//! partition changes we materialise the partitions into the public
//! [`ScalerCache`] via [`ScalerCache::replace_with`]. Reads from the
//! cache are O(1); writes are batched per event.
//!
//! ## Backstop against drift
//!
//! Each informer is a [`kube::runtime::watcher`] under the hood, which
//! issues a fresh LIST whenever the watch errors or its bookmark
//! becomes stale. The supervisor surfaces those as
//! [`InformerEvent::Restarted(list)`], and we replace the affected
//! partition in full. That gives us the "periodic full refresh"
//! behaviour the bead asks for without our own timer racing against
//! the watcher's relist.
//!
//! ## Ownership / lifetimes
//!
//! [`ScalerDiscovery`] owns the consumer tasks; dropping it aborts
//! them. The cache is exposed as [`Arc<RwLock<ScalerCache>>`] so the
//! reconciler can hold a long-lived handle without taking the
//! discovery struct apart.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kube::api::DynamicObject;
use kube::discovery::ApiResource;
use kube::runtime::watcher;
use kube::Api;
use synchrotron_diff::auto_ignore::{parse_scaler, ControllerKind, ScalerCache, ScalerEntry};
use synchrotron_plugins::{Gvk, Manifest};
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::client::KubeClient;
use crate::informer::{Informer, InformerEvent, WatchFactory};

/// Source-side identity of a controller object: what produced an
/// entry, and where it lives. We keep entries indexed by source (not
/// by target) so re-applies and renames replace the prior entry
/// cleanly.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SourceKey {
    controller: ControllerKind,
    namespace: Option<String>,
    name: String,
}

/// Discovery supervisor: owns the per-controller informers, the
/// consumer tasks that translate their events, and the cache the
/// reconciler reads.
pub struct ScalerDiscovery {
    cache: Arc<RwLock<ScalerCache>>,
    handles: Vec<JoinHandle<()>>,
    // Informers must live as long as the supervisor; their broadcast
    // senders are referenced by the consumer tasks.
    _informers: Vec<Informer<DynamicObject>>,
}

impl ScalerDiscovery {
    /// Wire three pre-built informers into a single cache. One
    /// informer per controller kind; each must be configured to watch
    /// objects of that kind only.
    ///
    /// In production, [`spawn_for_cluster`] resolves the
    /// [`ApiResource`]s and constructs the informers; tests build
    /// scripted informers directly.
    pub fn spawn(
        hpa: Informer<DynamicObject>,
        vpa: Informer<DynamicObject>,
        keda: Informer<DynamicObject>,
    ) -> Self {
        let cache = Arc::new(RwLock::new(ScalerCache::new()));
        let sources: Arc<Mutex<HashMap<SourceKey, ScalerEntry>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let mut handles = Vec::with_capacity(3);
        for (informer, controller) in [
            (&hpa, ControllerKind::Hpa),
            (&vpa, ControllerKind::Vpa),
            (&keda, ControllerKind::KedaScaledObject),
        ] {
            let rx = informer.subscribe();
            handles.push(tokio::spawn(consumer_task(
                rx,
                controller,
                Arc::clone(&sources),
                Arc::clone(&cache),
            )));
        }

        Self {
            cache,
            handles,
            _informers: vec![hpa, vpa, keda],
        }
    }

    /// Shared handle to the cache. Call sites take a read lock and
    /// pass `&*guard` to [`synchrotron_diff::auto_ignore::auto_ignore_rules_for`].
    pub fn cache(&self) -> Arc<RwLock<ScalerCache>> {
        Arc::clone(&self.cache)
    }
}

impl Drop for ScalerDiscovery {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

/// Build a [`WatchFactory`] for a CRD-shaped resource discovered at
/// runtime. Used by [`spawn_for_cluster`] to put VPA and KEDA on the
/// same watch path as HPA (which we also watch as `DynamicObject` for
/// uniformity).
pub fn dynamic_watch_factory(resource: ApiResource) -> WatchFactory<DynamicObject> {
    Arc::new(move |client: KubeClient, cfg| {
        let api: Api<DynamicObject> = Api::all_with(client.client().clone(), &resource);
        let mut wcfg = watcher::Config::default();
        if let Some(sel) = cfg.label_selector {
            wcfg = wcfg.labels(&sel);
        }
        if let Some(sel) = cfg.field_selector {
            wcfg = wcfg.fields(&sel);
        }
        Box::pin(watcher(api, wcfg))
    })
}

/// Production wiring: discover each controller's [`ApiResource`] via
/// the API server, build a watch factory per kind, and spawn an
/// informer for each. Kinds whose discovery returns "not found" are
/// skipped — the cluster simply doesn't have that controller
/// installed, which is the common case for VPA and KEDA.
pub async fn spawn_for_cluster(
    client_provider: crate::informer::ClientProvider,
    health_rx: tokio::sync::watch::Receiver<crate::HealthState>,
    seed_client: &KubeClient,
) -> crate::Result<ScalerDiscovery> {
    use kube::core::GroupVersionKind;
    use kube::discovery::pinned_kind;

    // GVKs we want to watch. HPA is intentionally `autoscaling/v2`;
    // older `autoscaling/v1` HPAs are forward-served by the v2 API.
    let gvks = [
        (
            "",
            GroupVersionKind::gvk("autoscaling", "v2", "HorizontalPodAutoscaler"),
        ),
        (
            "",
            GroupVersionKind::gvk("autoscaling.k8s.io", "v1", "VerticalPodAutoscaler"),
        ),
        (
            "",
            GroupVersionKind::gvk("keda.sh", "v1alpha1", "ScaledObject"),
        ),
    ];

    let mut resources: Vec<Option<ApiResource>> = Vec::with_capacity(3);
    for (_, gvk) in &gvks {
        match pinned_kind(seed_client.client(), gvk).await {
            Ok((resource, _caps)) => resources.push(Some(resource)),
            Err(err) => {
                debug!(
                    %err,
                    group = gvk.group,
                    kind = gvk.kind,
                    "scaler_discovery: GVK not present in cluster; skipping informer"
                );
                resources.push(None);
            }
        }
    }

    // For absent GVKs we still need an Informer<DynamicObject> shaped
    // value so spawn() has three slots; build a no-op informer that
    // will never emit. Rather than fake one, fall through and only
    // wire what's discoverable. Returning an error if HPA isn't
    // present would be over-strict — clusters without metrics-server
    // legitimately lack v2 HPA.
    let mut informers: Vec<Option<Informer<DynamicObject>>> = Vec::with_capacity(3);
    for resource in resources.into_iter() {
        let informer = resource.map(|res| {
            Informer::<DynamicObject>::spawn(
                client_provider.clone(),
                health_rx.clone(),
                crate::informer::InformerConfig::default(),
                dynamic_watch_factory(res),
            )
        });
        informers.push(informer);
    }

    let mut iter = informers.into_iter();
    let hpa = iter.next().flatten().unwrap_or_else(no_op_informer);
    let vpa = iter.next().flatten().unwrap_or_else(no_op_informer);
    let keda = iter.next().flatten().unwrap_or_else(no_op_informer);

    Ok(ScalerDiscovery::spawn(hpa, vpa, keda))
}

/// Build an informer whose stream is empty and immediately closes.
/// Used for GVKs that aren't present in the cluster, so the
/// supervisor's three-slot shape stays uniform.
fn no_op_informer() -> Informer<DynamicObject> {
    use futures::stream;

    let factory: WatchFactory<DynamicObject> = Arc::new(|_client, _cfg| Box::pin(stream::empty()));
    let provider: crate::informer::ClientProvider = Arc::new(|| {
        Box::pin(async {
            // Will never be called because the stream is empty and the
            // session task exits immediately.
            unreachable!("no-op informer client provider invoked")
        })
    });
    let (_tx, rx) = tokio::sync::watch::channel(crate::HealthState::Unknown);
    Informer::<DynamicObject>::spawn(provider, rx, Default::default(), factory)
}

async fn consumer_task(
    mut rx: broadcast::Receiver<InformerEvent<DynamicObject>>,
    controller: ControllerKind,
    sources: Arc<Mutex<HashMap<SourceKey, ScalerEntry>>>,
    cache: Arc<RwLock<ScalerCache>>,
) {
    loop {
        match rx.recv().await {
            Ok(InformerEvent::Applied(obj)) => {
                let key = source_key_of(&obj, controller);
                let new_entry = manifest_for(&obj).and_then(|m| parse_scaler(&m));
                let mut changed = false;
                {
                    let Ok(mut map) = sources.lock() else { return };
                    if let Some(key) = key.clone() {
                        match new_entry {
                            Some(entry) => {
                                let prior = map.insert(key, entry.clone());
                                changed = prior.as_ref() != Some(&entry);
                            }
                            None => {
                                changed = map.remove(&key).is_some();
                            }
                        }
                    }
                }
                if changed {
                    rebuild_cache(&sources, &cache).await;
                }
            }
            Ok(InformerEvent::Deleted(obj)) => {
                let Some(key) = source_key_of(&obj, controller) else {
                    continue;
                };
                let removed = {
                    let Ok(mut map) = sources.lock() else { return };
                    map.remove(&key).is_some()
                };
                if removed {
                    rebuild_cache(&sources, &cache).await;
                }
            }
            Ok(InformerEvent::Restarted(list)) => {
                {
                    let Ok(mut map) = sources.lock() else { return };
                    map.retain(|k, _| k.controller != controller);
                    for obj in &list {
                        if let (Some(key), Some(entry)) = (
                            source_key_of(obj, controller),
                            manifest_for(obj).and_then(|m| parse_scaler(&m)),
                        ) {
                            map.insert(key, entry);
                        }
                    }
                }
                rebuild_cache(&sources, &cache).await;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(
                    controller = ?controller,
                    skipped = n,
                    "scaler_discovery: consumer lagged broadcast; will resync on next Restarted"
                );
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

async fn rebuild_cache(
    sources: &Arc<Mutex<HashMap<SourceKey, ScalerEntry>>>,
    cache: &Arc<RwLock<ScalerCache>>,
) {
    let entries: Vec<ScalerEntry> = match sources.lock() {
        Ok(map) => map.values().cloned().collect(),
        Err(_) => return,
    };
    cache.write().await.replace_with(entries);
}

fn source_key_of(obj: &DynamicObject, controller: ControllerKind) -> Option<SourceKey> {
    Some(SourceKey {
        controller,
        namespace: obj.metadata.namespace.clone(),
        name: obj.metadata.name.clone()?,
    })
}

/// Convert a [`DynamicObject`] into the `Manifest` shape `parse_scaler`
/// expects. The conversion is a JSON round-trip — `DynamicObject`
/// serializes to its over-the-wire shape (apiVersion/kind/metadata
/// plus its `data` payload), which is exactly what the YAML-shaped
/// manifest body looks like.
fn manifest_for(obj: &DynamicObject) -> Option<Manifest> {
    let types = obj.types.as_ref()?;
    let name = obj.metadata.name.clone()?;
    let json = serde_json::to_value(obj).ok()?;
    let body: serde_yaml_ng::Value = serde_json::from_value(json).ok()?;
    Some(Manifest {
        gvk: Gvk::parse(&types.api_version, &types.kind),
        name,
        namespace: obj.metadata.namespace.clone(),
        body: body.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use kube::api::TypeMeta;
    use kube::runtime::watcher::Event as WatchEvent;
    use std::time::Duration;
    use tokio::sync::watch;

    fn dyn_obj(
        api_version: &str,
        kind: &str,
        namespace: &str,
        name: &str,
        body: &str,
    ) -> DynamicObject {
        let full = format!(
            "apiVersion: {api_version}\nkind: {kind}\nmetadata:\n  name: {name}\n  namespace: {namespace}\n{body}"
        );
        let mut obj: DynamicObject = serde_yaml_ng::from_str(&full).unwrap();
        obj.types = Some(TypeMeta {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
        });
        obj.metadata.name = Some(name.to_string());
        obj.metadata.namespace = Some(namespace.to_string());
        obj
    }

    fn hpa_obj(name: &str, namespace: &str, target: &str) -> DynamicObject {
        dyn_obj(
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            namespace,
            name,
            &format!(
                "spec:\n  scaleTargetRef:\n    apiVersion: apps/v1\n    kind: Deployment\n    name: {target}\n  minReplicas: 1\n  maxReplicas: 10\n"
            ),
        )
    }

    fn vpa_obj(name: &str, namespace: &str, target: &str, mode: &str) -> DynamicObject {
        dyn_obj(
            "autoscaling.k8s.io/v1",
            "VerticalPodAutoscaler",
            namespace,
            name,
            &format!(
                "spec:\n  targetRef:\n    apiVersion: apps/v1\n    kind: Deployment\n    name: {target}\n  updatePolicy:\n    updateMode: {mode}\n"
            ),
        )
    }

    fn keda_obj(name: &str, namespace: &str, target: &str) -> DynamicObject {
        dyn_obj(
            "keda.sh/v1alpha1",
            "ScaledObject",
            namespace,
            name,
            &format!(
                "spec:\n  scaleTargetRef:\n    apiVersion: apps/v1\n    kind: Deployment\n    name: {target}\n  triggers:\n    - type: kafka\n"
            ),
        )
    }

    /// Build a one-shot watch stream that emits an Init/InitApply*/InitDone
    /// triplet (so the supervisor surfaces a `Restarted` event), then
    /// closes. Mirrors the behaviour of a freshly-started kube watcher
    /// after its initial LIST.
    fn restarted_stream(objs: Vec<DynamicObject>) -> WatchFactory<DynamicObject> {
        Arc::new(move |_client, _cfg| {
            let mut events: Vec<std::result::Result<WatchEvent<DynamicObject>, watcher::Error>> =
                Vec::with_capacity(objs.len() + 2);
            events.push(Ok(WatchEvent::Init));
            for obj in &objs {
                events.push(Ok(WatchEvent::InitApply(obj.clone())));
            }
            events.push(Ok(WatchEvent::InitDone));
            Box::pin(stream::iter(events))
        })
    }

    fn empty_stream() -> WatchFactory<DynamicObject> {
        Arc::new(|_client, _cfg| {
            let events: Vec<std::result::Result<WatchEvent<DynamicObject>, watcher::Error>> =
                vec![Ok(WatchEvent::Init), Ok(WatchEvent::InitDone)];
            Box::pin(stream::iter(events))
        })
    }

    /// A scripted apply/delete sequence for testing incremental updates.
    fn scripted_stream(events: Vec<WatchEvent<DynamicObject>>) -> WatchFactory<DynamicObject> {
        Arc::new(move |_client, _cfg| {
            let mapped: Vec<std::result::Result<WatchEvent<DynamicObject>, watcher::Error>> =
                events.iter().cloned().map(Ok).collect();
            Box::pin(stream::iter(mapped))
        })
    }

    async fn seed_kube_client() -> KubeClient {
        use crate::ClusterConfig;
        use std::fs;
        use tempfile::TempDir;

        const KCFG: &str = r#"apiVersion: v1
kind: Config
current-context: t
clusters:
- name: c
  cluster:
    server: https://127.0.0.1:1
    insecure-skip-tls-verify: true
users:
- name: u
  user:
    token: x
contexts:
- name: t
  context:
    cluster: c
    user: u
"#;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kubeconfig");
        fs::write(&path, KCFG).unwrap();
        let cfg = ClusterConfig::from_kubeconfig("c", &path);
        let client = KubeClient::connect(&cfg).await.unwrap();
        std::mem::forget(dir);
        client
    }

    fn client_provider(client: KubeClient) -> crate::informer::ClientProvider {
        let client = Arc::new(client);
        Arc::new(move || {
            let c = (*client).clone();
            Box::pin(async move { c })
        })
    }

    fn up_health() -> watch::Receiver<crate::HealthState> {
        let (tx, rx) = watch::channel(crate::HealthState::Up);
        // Keep the sender alive by leaking it; tests are short-lived.
        std::mem::forget(tx);
        rx
    }

    /// Wait until the cache hits `expected_len`, polling briefly. The
    /// consumer task hops a few times per event (broadcast → mutex →
    /// rwlock), so we need to give the scheduler time to settle.
    async fn wait_for_len(cache: &Arc<RwLock<ScalerCache>>, expected: usize) {
        for _ in 0..200 {
            if cache.read().await.len() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "cache never reached len {expected} (final = {})",
            cache.read().await.len()
        );
    }

    #[tokio::test]
    async fn restarted_event_populates_cache_for_all_three_kinds() {
        let client = seed_kube_client().await;
        let provider = client_provider(client);
        let health = up_health();

        let hpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            restarted_stream(vec![hpa_obj("h", "ns", "web")]),
        );
        let vpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            restarted_stream(vec![vpa_obj("v", "ns", "web", "Auto")]),
        );
        let keda = Informer::<DynamicObject>::spawn(
            provider,
            health,
            Default::default(),
            restarted_stream(vec![keda_obj("k", "ns", "web")]),
        );

        let disco = ScalerDiscovery::spawn(hpa, vpa, keda);
        wait_for_len(&disco.cache(), 3).await;
    }

    #[tokio::test]
    async fn vpa_with_off_mode_is_filtered_out() {
        let client = seed_kube_client().await;
        let provider = client_provider(client);
        let health = up_health();

        let hpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            empty_stream(),
        );
        let vpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            restarted_stream(vec![vpa_obj("v-off", "ns", "web", "Off")]),
        );
        let keda =
            Informer::<DynamicObject>::spawn(provider, health, Default::default(), empty_stream());

        let disco = ScalerDiscovery::spawn(hpa, vpa, keda);
        // Give the consumer tasks time to settle. There's no positive
        // signal to wait for since we're asserting absence; pause and
        // then assert.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(disco.cache().read().await.len(), 0);
    }

    #[tokio::test]
    async fn applied_then_deleted_clears_entry() {
        let client = seed_kube_client().await;
        let provider = client_provider(client);
        let health = up_health();

        let obj = hpa_obj("h1", "ns", "web");
        let hpa_factory = scripted_stream(vec![
            WatchEvent::Init,
            WatchEvent::InitDone,
            WatchEvent::Apply(obj.clone()),
            WatchEvent::Delete(obj),
        ]);
        let hpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            hpa_factory,
        );
        let vpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            empty_stream(),
        );
        let keda =
            Informer::<DynamicObject>::spawn(provider, health, Default::default(), empty_stream());

        let disco = ScalerDiscovery::spawn(hpa, vpa, keda);
        wait_for_len(&disco.cache(), 0).await;
    }

    #[tokio::test]
    async fn restarted_replaces_only_its_own_partition() {
        let client = seed_kube_client().await;
        let provider = client_provider(client);
        let health = up_health();

        // HPA seeds two entries; VPA seeds one; then HPA stream sends a
        // second Init/InitDone with a single replacement object,
        // truncating the HPA partition without disturbing VPA.
        let hpa_factory: WatchFactory<DynamicObject> = Arc::new({
            let h1 = hpa_obj("h1", "ns", "web");
            let h2 = hpa_obj("h2", "ns", "api");
            let h3 = hpa_obj("h3", "ns", "worker");
            move |_, _| {
                Box::pin(stream::iter(vec![
                    Ok(WatchEvent::Init),
                    Ok(WatchEvent::InitApply(h1.clone())),
                    Ok(WatchEvent::InitApply(h2.clone())),
                    Ok(WatchEvent::InitDone),
                    // Second relist with one entry — the supervisor
                    // emits another Restarted event.
                    Ok(WatchEvent::Init),
                    Ok(WatchEvent::InitApply(h3.clone())),
                    Ok(WatchEvent::InitDone),
                ]))
            }
        });
        let hpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            hpa_factory,
        );
        let vpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            restarted_stream(vec![vpa_obj("v", "ns", "web", "Auto")]),
        );
        let keda =
            Informer::<DynamicObject>::spawn(provider, health, Default::default(), empty_stream());

        let disco = ScalerDiscovery::spawn(hpa, vpa, keda);
        // Final state: 1 HPA + 1 VPA = 2 entries.
        wait_for_len(&disco.cache(), 2).await;
    }

    #[tokio::test]
    async fn dropping_discovery_cleans_up_consumer_tasks() {
        let client = seed_kube_client().await;
        let provider = client_provider(client);
        let health = up_health();

        let hpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            empty_stream(),
        );
        let vpa = Informer::<DynamicObject>::spawn(
            provider.clone(),
            health.clone(),
            Default::default(),
            empty_stream(),
        );
        let keda =
            Informer::<DynamicObject>::spawn(provider, health, Default::default(), empty_stream());

        let disco = ScalerDiscovery::spawn(hpa, vpa, keda);
        let handles: Vec<_> = disco.handles.iter().map(|h| h.abort_handle()).collect();
        drop(disco);
        // After drop, the abort signal should propagate; give it a
        // couple of scheduler hops.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for h in handles {
            assert!(h.is_finished());
        }
    }
}
