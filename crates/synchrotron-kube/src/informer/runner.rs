use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::{Stream, StreamExt};
use kube::runtime::watcher;
use kube::{Api, Resource};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::hash::Hash;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::client::KubeClient;
use crate::health::HealthState;
use crate::informer::types::{InformerConfig, InformerEvent};

/// Factory that produces a watch stream for a given client + config.
///
/// In production this wraps [`kube::runtime::watcher`] for a typed
/// resource. In tests it returns a scripted stream so the supervisor
/// can be exercised without a live cluster.
pub type WatchFactory<K> = Arc<
    dyn Fn(
            KubeClient,
            InformerConfig,
        ) -> Pin<
            Box<dyn Stream<Item = std::result::Result<watcher::Event<K>, watcher::Error>> + Send>,
        > + Send
        + Sync,
>;

/// Async client provider. Called whenever the supervisor (re)starts a
/// watch session, so it can pick up a fresh [`KubeClient`] after a
/// reconnect (see [`crate::HealthMonitor::current_client`]).
pub type ClientProvider =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = KubeClient> + Send>> + Send + Sync>;

/// Long-lived informer for a single resource type.
///
/// Internally runs a supervisor task that:
///   1. waits for [`HealthState::Up`],
///   2. starts a watch session via the [`WatchFactory`],
///   3. forwards events to a broadcast channel,
///   4. on Down→Up transitions, tears down the session and starts a
///      fresh one (kube client may have been rebuilt).
pub struct Informer<K: Clone + Send + 'static> {
    tx: broadcast::Sender<InformerEvent<K>>,
    handle: JoinHandle<()>,
}

impl<K: Clone + Send + 'static> Informer<K> {
    /// Test/production-shared constructor. The two hooks
    /// (`client_provider` + `factory`) make the supervisor independent
    /// of any specific kube transport.
    pub fn spawn(
        client_provider: ClientProvider,
        health_rx: watch::Receiver<HealthState>,
        cfg: InformerConfig,
        factory: WatchFactory<K>,
    ) -> Self {
        let (tx, _) = broadcast::channel(256);
        let tx_for_task = tx.clone();
        let handle = tokio::spawn(supervisor(
            client_provider,
            health_rx,
            cfg,
            factory,
            tx_for_task,
        ));
        Self { tx, handle }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<InformerEvent<K>> {
        self.tx.subscribe()
    }

    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

impl<K: Clone + Send + 'static> Drop for Informer<K> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Build the production [`WatchFactory`] for a cluster-scoped watch
/// over Kubernetes resource type `K` (`Api::all`). Honors the label
/// and field selectors in [`InformerConfig`]; ignores `namespace`
/// (callers needing per-namespace scoping should provide their own
/// factory using `Api::namespaced`, which requires a
/// `NamespaceResourceScope` bound).
pub fn kube_watch_factory<K>() -> WatchFactory<K>
where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    <K as Resource>::DynamicType: Default + Clone + Debug + Eq + Hash,
{
    Arc::new(|client: KubeClient, cfg: InformerConfig| {
        let api: Api<K> = Api::all(client.client().clone());
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

/// `WatchFactory` over [`kube::api::DynamicObject`] for a discovered
/// `ApiResource`. Used by slice wba (synchrotron-cd-wba) where we
/// want to watch arbitrary GVKs (ConfigMap, Deployment, Ingress, …)
/// without spelling out a typed `K` per kind. The `ApiResource` is
/// captured by the closure so the same factory keeps watching the
/// same GVK across reconnects.
pub fn dynamic_watch_factory(
    api_resource: kube::discovery::ApiResource,
) -> WatchFactory<kube::api::DynamicObject> {
    Arc::new(move |client: KubeClient, cfg: InformerConfig| {
        let api: Api<kube::api::DynamicObject> =
            Api::all_with(client.client().clone(), &api_resource);
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

async fn supervisor<K: Clone + Send + 'static>(
    client_provider: ClientProvider,
    mut health_rx: watch::Receiver<HealthState>,
    cfg: InformerConfig,
    factory: WatchFactory<K>,
    tx: broadcast::Sender<InformerEvent<K>>,
) {
    let mut session: Option<JoinHandle<()>> = None;
    let mut last_state = HealthState::Unknown;

    loop {
        let current = health_rx.borrow_and_update().clone();
        let was_up = matches!(last_state, HealthState::Up);
        let is_up = matches!(current, HealthState::Up);

        match (was_up, is_up) {
            (false, true) => {
                debug!("informer: starting watch session (health Up)");
                session = Some(spawn_session(
                    client_provider.clone(),
                    cfg.clone(),
                    factory.clone(),
                    tx.clone(),
                ));
            }
            (true, false) => {
                if let Some(h) = session.take() {
                    debug!("informer: aborting watch session (health left Up)");
                    h.abort();
                }
            }
            (true, true) => {
                // Already running; nothing to do.
            }
            (false, false) => {
                // Still waiting for first Up.
            }
        }

        last_state = current;

        if health_rx.changed().await.is_err() {
            // Sender dropped — exit cleanly.
            if let Some(h) = session.take() {
                h.abort();
            }
            return;
        }
    }
}

fn spawn_session<K: Clone + Send + 'static>(
    client_provider: ClientProvider,
    cfg: InformerConfig,
    factory: WatchFactory<K>,
    tx: broadcast::Sender<InformerEvent<K>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = client_provider().await;
        let mut stream = factory(client, cfg);
        info!("informer: watch session started");
        // Accumulate `InitApply` items between `Init` and `InitDone`
        // so the supervisor can emit a single `Restarted(Vec<K>)`
        // rather than leaking the kube watcher's three-phase init
        // protocol to subscribers.
        let mut init_buf: Option<Vec<K>> = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(watcher::Event::Apply(obj)) => {
                    let _ = tx.send(InformerEvent::Applied(obj));
                }
                Ok(watcher::Event::Delete(obj)) => {
                    let _ = tx.send(InformerEvent::Deleted(obj));
                }
                Ok(watcher::Event::Init) => {
                    init_buf = Some(Vec::new());
                }
                Ok(watcher::Event::InitApply(obj)) => match &mut init_buf {
                    Some(buf) => buf.push(obj),
                    None => {
                        // InitApply outside of Init/InitDone: forward as Applied
                        // to avoid silently dropping objects.
                        let _ = tx.send(InformerEvent::Applied(obj));
                    }
                },
                Ok(watcher::Event::InitDone) => {
                    let buf = init_buf.take().unwrap_or_default();
                    let _ = tx.send(InformerEvent::Restarted(buf));
                }
                Err(err) => {
                    warn!(error = %err, "informer: watch error; ending session");
                    return;
                }
            }
        }
        info!("informer: watch stream ended");
    })
}
