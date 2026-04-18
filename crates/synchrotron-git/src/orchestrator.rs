//! Multi-repo orchestration: a registry of [`Poller`]s with
//! bounded-concurrency fetch admission and per-repo status tracking.
//!
//! Coordinates the per-repo polling loops so that:
//!   * a global `max_concurrent_fetches` cap prevents thundering-herd
//!     on the upstream forge,
//!   * the same trigger registry is shared with the webhook layer
//!     (h48.1.5) so push notifications route to the right poller,
//!   * the latest [`PollEvent`] for each repo is observable without
//!     subscribing to the broadcast channel ourselves.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;

use crate::error::GitError;
use crate::poller::{FetchFn, PollEvent, Poller, PollerConfig};
use crate::repo::{Repo, RepoId};
use crate::triggers::RepoTriggers;
use crate::Result;

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Maximum number of fetches running concurrently across all
    /// repos. Excess fetch attempts queue on the semaphore.
    pub max_concurrent_fetches: usize,
    pub poller: PollerConfig,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_fetches: 4,
            poller: PollerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RepoStatus {
    pub last_event: Option<PollEvent>,
    pub last_event_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AggregateStatus {
    pub total: usize,
    pub fetched: usize,
    pub failed: usize,
    pub never_polled: usize,
}

struct Entry {
    repo: Repo,
    _poller: Poller,
    status: Arc<RwLock<RepoStatus>>,
    _listener: JoinHandle<()>,
}

pub struct Orchestrator {
    cfg: OrchestratorConfig,
    semaphore: Arc<Semaphore>,
    triggers: RepoTriggers,
    entries: Arc<RwLock<HashMap<RepoId, Entry>>>,
}

impl Orchestrator {
    pub fn new(cfg: OrchestratorConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(cfg.max_concurrent_fetches));
        Self {
            cfg,
            semaphore,
            triggers: RepoTriggers::new(),
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Shared trigger handle. The webhook layer (h48.6) holds this
    /// to force fetches by [`RepoId`].
    pub fn triggers(&self) -> RepoTriggers {
        self.triggers.clone()
    }

    /// Register a repo: spawn its poller (with the global concurrency
    /// cap applied to its fetch fn), wire it into the trigger
    /// registry, and start tracking its status. Errors if `repo.id`
    /// is already registered.
    pub async fn register(&self, repo: Repo, fetch: FetchFn) -> Result<()> {
        {
            let g = self.entries.read().await;
            if g.contains_key(&repo.id) {
                return Err(GitError::InvalidState(format!(
                    "repo {} already registered",
                    repo.id.as_str()
                )));
            }
        }

        let bounded = wrap_with_semaphore(fetch, self.semaphore.clone());
        let poller = Poller::spawn(
            repo.id.as_str().to_string(),
            bounded,
            self.cfg.poller.clone(),
        );
        self.triggers.register(repo.id.clone(), &poller).await;

        let status = Arc::new(RwLock::new(RepoStatus::default()));
        let listener = spawn_status_listener(poller.subscribe(), status.clone());

        let mut g = self.entries.write().await;
        if g.contains_key(&repo.id) {
            // Race: another caller registered concurrently. Roll back.
            self.triggers.deregister(&repo.id).await;
            return Err(GitError::InvalidState(format!(
                "repo {} already registered",
                repo.id.as_str()
            )));
        }
        g.insert(
            repo.id.clone(),
            Entry {
                repo,
                _poller: poller,
                status,
                _listener: listener,
            },
        );
        Ok(())
    }

    pub async fn deregister(&self, id: &RepoId) -> bool {
        let removed = self.entries.write().await.remove(id).is_some();
        if removed {
            self.triggers.deregister(id).await;
        }
        removed
    }

    pub async fn trigger(&self, id: &RepoId) -> bool {
        self.triggers.trigger(id).await
    }

    pub async fn status(&self, id: &RepoId) -> Option<RepoStatus> {
        let status_arc = {
            let g = self.entries.read().await;
            g.get(id).map(|e| e.status.clone())?
        };
        let s = status_arc.read().await.clone();
        Some(s)
    }

    pub async fn list(&self) -> Vec<RepoId> {
        self.entries.read().await.keys().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.entries.read().await.is_empty()
    }

    pub async fn repo(&self, id: &RepoId) -> Option<Repo> {
        let g = self.entries.read().await;
        g.get(id).map(|e| e.repo.clone())
    }

    pub async fn aggregate(&self) -> AggregateStatus {
        let g = self.entries.read().await;
        let mut agg = AggregateStatus {
            total: g.len(),
            ..Default::default()
        };
        for entry in g.values() {
            let s = entry.status.read().await;
            match &s.last_event {
                Some(PollEvent::Fetched(_)) => agg.fetched += 1,
                Some(PollEvent::FetchFailed(_)) => agg.failed += 1,
                None => agg.never_polled += 1,
            }
        }
        agg
    }
}

fn wrap_with_semaphore(inner: FetchFn, sem: Arc<Semaphore>) -> FetchFn {
    Arc::new(move || {
        let inner = inner.clone();
        let sem = sem.clone();
        Box::pin(async move {
            // Semaphore close is treated as a poisoned-orchestrator
            // signal — propagate as a fetch error rather than panicking.
            let _permit = sem
                .acquire()
                .await
                .map_err(|e| format!("orchestrator semaphore closed: {e}"))?;
            inner().await
        })
    })
}

fn spawn_status_listener(
    mut rx: tokio::sync::broadcast::Receiver<PollEvent>,
    status: Arc<RwLock<RepoStatus>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(evt) => {
                    let mut s = status.write().await;
                    s.last_event = Some(evt);
                    s.last_event_at = Some(SystemTime::now());
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}
