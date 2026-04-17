use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};

use crate::client::KubeClient;
use crate::config::ClusterConfig;
use crate::error::KubeError;
use crate::health::state::{HealthConfig, HealthState, Tracker};
use crate::Result;

/// Async probe callback. Returns `Ok(())` if the cluster is reachable,
/// or `Err(description)` otherwise. Exposed so tests and custom probes
/// (e.g. `/healthz` instead of `/version`) can be plugged in.
pub type ProbeFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>> + Send + Sync,
>;

/// Async reconnect callback. Called by the monitor after
/// `reconnect_after` consecutive failures to rebuild the underlying
/// client in-place (e.g. pick up a refreshed token).
pub type ReconnectFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>> + Send + Sync,
>;

/// Background health probe for a single cluster.
///
/// Probes `/version` (lightweight API-server reachability check)
/// periodically, advances a [`Tracker`] state machine, publishes the
/// resulting [`HealthState`] on a [`watch`] channel, and rebuilds the
/// underlying [`KubeClient`] every `reconnect_after` failures.
///
/// Downstream consumers (informers — h48.8.6) subscribe for up/down
/// transitions so they can tear down and restart watch streams when the
/// client is rebuilt.
pub struct HealthMonitor {
    name: String,
    state_rx: watch::Receiver<HealthState>,
    client: Arc<RwLock<KubeClient>>,
    handle: JoinHandle<()>,
}

impl HealthMonitor {
    /// Production constructor: build a [`KubeClient`] from `cluster` and
    /// start probing. The probe uses `apiserver_version`; reconnect
    /// rebuilds the client from the same [`ClusterConfig`].
    pub async fn spawn(cluster: ClusterConfig, health: HealthConfig) -> Result<Self> {
        let kube = KubeClient::connect(&cluster).await?;
        let name = kube.name().to_string();
        let client = Arc::new(RwLock::new(kube));

        let probe = make_kube_probe(client.clone(), health.probe_timeout);
        let reconnect = make_kube_reconnect(cluster, client.clone());

        Ok(Self::spawn_inner(name, client, probe, reconnect, health))
    }

    /// Hook-level constructor used in tests: inject arbitrary probe and
    /// reconnect futures so the monitor can be exercised without a live
    /// cluster. `seed_client` is the client the monitor will hold onto;
    /// tests may never actually use it.
    pub fn spawn_with_hooks(
        name: impl Into<String>,
        seed_client: KubeClient,
        probe: ProbeFn,
        reconnect: ReconnectFn,
        health: HealthConfig,
    ) -> Self {
        let client = Arc::new(RwLock::new(seed_client));
        Self::spawn_inner(name.into(), client, probe, reconnect, health)
    }

    fn spawn_inner(
        name: String,
        client: Arc<RwLock<KubeClient>>,
        probe: ProbeFn,
        reconnect: ReconnectFn,
        health: HealthConfig,
    ) -> Self {
        let (tx, rx) = watch::channel(HealthState::Unknown);
        let task_name = name.clone();
        let handle = tokio::spawn(run_loop(task_name, probe, reconnect, tx, health));
        Self {
            name,
            state_rx: rx,
            client,
            handle,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn current_state(&self) -> HealthState {
        self.state_rx.borrow().clone()
    }

    /// Subscribe to state updates. Receivers are notified on every probe
    /// outcome (not just transitions), so callers debouncing noise should
    /// compare against their last-seen value.
    pub fn subscribe(&self) -> watch::Receiver<HealthState> {
        self.state_rx.clone()
    }

    /// Returns a clone of the *current* kube client. After a reconnect
    /// the underlying client is swapped, so long-lived callers should
    /// re-fetch after observing a Down→Up transition.
    pub async fn current_client(&self) -> KubeClient {
        self.client.read().await.clone()
    }

    /// Stop the probe loop. Safe to call multiple times.
    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn run_loop(
    name: String,
    probe: ProbeFn,
    reconnect: ReconnectFn,
    state_tx: watch::Sender<HealthState>,
    config: HealthConfig,
) {
    let mut tracker = Tracker::new(config.clone());
    loop {
        let outcome = match time::timeout(config.probe_timeout, probe()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(format!("probe timed out after {:?}", config.probe_timeout)),
        };

        match outcome {
            Ok(()) => {
                tracker.record_success();
                debug!(cluster = %name, "probe ok");
            }
            Err(err) => {
                tracker.record_failure(err.clone());
                warn!(cluster = %name, error = %err, "probe failed");
            }
        }

        // `send` only errors if there are no receivers, which is harmless
        // for a status channel — monitors may run with no subscribers.
        let _ = state_tx.send(tracker.state.clone());

        if tracker.should_reconnect() {
            info!(cluster = %name, "attempting kube client reconnect");
            match reconnect().await {
                Ok(()) => info!(cluster = %name, "reconnect succeeded"),
                Err(e) => warn!(cluster = %name, error = %e, "reconnect failed"),
            }
        }

        time::sleep(tracker.sleep_duration()).await;
    }
}

fn make_kube_probe(client: Arc<RwLock<KubeClient>>, _timeout: std::time::Duration) -> ProbeFn {
    Arc::new(move || {
        let client = client.clone();
        Box::pin(async move {
            let kube = client.read().await;
            kube.apiserver_version()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    })
}

fn make_kube_reconnect(cluster: ClusterConfig, client: Arc<RwLock<KubeClient>>) -> ReconnectFn {
    Arc::new(move || {
        let cluster = cluster.clone();
        let client = client.clone();
        Box::pin(async move {
            let new_client = KubeClient::connect(&cluster)
                .await
                .map_err(|e: KubeError| e.to_string())?;
            *client.write().await = new_client;
            Ok(())
        })
    })
}
