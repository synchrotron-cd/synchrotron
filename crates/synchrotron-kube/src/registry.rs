//! Multi-cluster registry: named [`HealthMonitor`]s plus routing.
//!
//! Each registration owns a long-lived [`HealthMonitor`] (which in
//! turn owns the [`KubeClient`] and probe loop). The registry hands
//! out cloned monitor handles via [`Arc`], so callers can subscribe
//! to health state and grab fresh clients after reconnects without
//! holding the registry lock.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::client::KubeClient;
use crate::config::{ClusterConfig, ClusterName};
use crate::error::KubeError;
use crate::health::{HealthConfig, HealthMonitor, HealthState};
use crate::Result;

/// Concurrent map of cluster name → owning [`HealthMonitor`].
///
/// Registrations are idempotent at the API surface — calling
/// [`Self::register`] twice with the same name returns
/// [`KubeError::AlreadyRegistered`] rather than silently swapping the
/// monitor (which would orphan in-flight subscribers). Use
/// [`Self::deregister`] then [`Self::register`] to rotate.
#[derive(Default, Clone)]
pub struct ClusterRegistry {
    inner: Arc<RwLock<HashMap<ClusterName, Arc<HealthMonitor>>>>,
}

impl ClusterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a [`HealthMonitor`] for `cfg` and store it under
    /// `cfg.name`. Fails fast if the kubeconfig is malformed or the
    /// name is already taken.
    pub async fn register(&self, cfg: ClusterConfig, health: HealthConfig) -> Result<()> {
        let name = cfg.name.clone();
        {
            let g = self.inner.read().await;
            if g.contains_key(&name) {
                return Err(KubeError::AlreadyRegistered(name.0.clone()));
            }
        }
        let monitor = HealthMonitor::spawn(cfg, health).await?;
        let mut g = self.inner.write().await;
        // Re-check under the write lock to close the TOCTOU window.
        if g.contains_key(&name) {
            monitor.shutdown();
            return Err(KubeError::AlreadyRegistered(name.0.clone()));
        }
        g.insert(name, Arc::new(monitor));
        Ok(())
    }

    /// Remove the named cluster. Returns the monitor handle so the
    /// caller can observe its final state if needed; dropping it
    /// aborts the probe task.
    pub async fn deregister(&self, name: &ClusterName) -> Option<Arc<HealthMonitor>> {
        let mut g = self.inner.write().await;
        g.remove(name)
    }

    pub async fn get(&self, name: &ClusterName) -> Option<Arc<HealthMonitor>> {
        self.inner.read().await.get(name).cloned()
    }

    pub async fn list(&self) -> Vec<ClusterName> {
        self.inner.read().await.keys().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    /// Convenience: returns a clone of the current [`KubeClient`] for
    /// `name`. Re-fetch after observing a Down→Up transition, since
    /// the monitor swaps the underlying client on reconnect.
    pub async fn client(&self, name: &ClusterName) -> Option<KubeClient> {
        let monitor = self.get(name).await?;
        Some(monitor.current_client().await)
    }

    pub async fn health(&self, name: &ClusterName) -> Option<HealthState> {
        Some(self.get(name).await?.current_state())
    }
}
