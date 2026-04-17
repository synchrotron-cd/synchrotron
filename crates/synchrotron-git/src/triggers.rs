//! Out-of-band fetch triggers, keyed by [`RepoId`].
//!
//! The webhook & event system (h48.6) holds a [`RepoTriggers`] handle
//! and calls [`RepoTriggers::trigger`] when a push event arrives.
//! Each registered repo gets a [`Notify`] that some [`Poller`] is
//! waiting on; rapid bursts collapse into a single wake-up because
//! `Notify` only buffers one permit.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Notify, RwLock};

use crate::poller::Poller;
use crate::repo::RepoId;

#[derive(Default, Clone)]
pub struct RepoTriggers {
    inner: Arc<RwLock<HashMap<RepoId, Arc<Notify>>>>,
}

impl RepoTriggers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a poller's trigger handle under `id`. Replaces any
    /// previous registration for that id (last-writer wins; callers
    /// rotating pollers should `deregister` the old one explicitly if
    /// they need observable cleanup).
    pub async fn register(&self, id: RepoId, poller: &Poller) {
        let handle = poller.trigger_handle();
        self.inner.write().await.insert(id, handle);
    }

    pub async fn deregister(&self, id: &RepoId) -> bool {
        self.inner.write().await.remove(id).is_some()
    }

    /// Wake the poller registered for `id`, if any. Returns whether a
    /// registration existed. A burst of rapid `trigger` calls
    /// collapses to a single fetch via `Notify`'s single-permit
    /// buffering.
    pub async fn trigger(&self, id: &RepoId) -> bool {
        let g = self.inner.read().await;
        match g.get(id) {
            Some(notify) => {
                notify.notify_one();
                true
            }
            None => false,
        }
    }

    pub async fn known(&self) -> Vec<RepoId> {
        self.inner.read().await.keys().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }
}
