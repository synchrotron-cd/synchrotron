//! Internal event bus for decoupling producers (pollers, webhook
//! handlers, reconciler) from consumers (metrics, audit log, outbound
//! notifier).
//!
//! Built on [`tokio::sync::broadcast`] so the bus is many-producer /
//! many-consumer and a single slow consumer never stalls the
//! producers — the slow receiver gets dropped events with a
//! `RecvError::Lagged` that we surface via tracing rather than
//! propagating. That trade-off (at-most-once under pressure) is fine
//! for Synchrotron's observability/notification use cases; components
//! that need at-least-once delivery handle their own retries (e.g.
//! the outbound notifier).

use std::time::SystemTime;

use tokio::sync::broadcast;
use tracing::warn;

use synchrotron_types::{AppName, ClusterName, HealthStatusCode};

/// A `RepoId` here is the string form of `synchrotron_git::RepoId`.
/// We avoid a direct dep on synchrotron-git to keep the event bus
/// above the git crate in the module graph.
pub type RepoIdStr = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemEvent {
    /// A git fetch produced a new HEAD for the named repo.
    RepoChanged { repo: RepoIdStr, new_head: String },
    /// A git fetch succeeded but the HEAD didn't move.
    RepoUnchanged { repo: RepoIdStr, head: String },
    /// A git fetch failed. The `error` is a truncated human string —
    /// the structured error stays in the poller's own channel.
    RepoFetchFailed { repo: RepoIdStr, error: String },
    /// An inbound webhook triggered a fetch for the named repo.
    WebhookTriggered {
        repo: RepoIdStr,
        source: WebhookSource,
    },
    /// An application sync attempt finished. Populated by h48.4 once
    /// the reconciler lands.
    SyncOutcome {
        app: AppName,
        cluster: ClusterName,
        success: bool,
        message: Option<String>,
    },
    /// An operator (CLI / REST API / dashboard) explicitly requested
    /// a sync for `app`. The reconciler should enqueue it with
    /// `Trigger::Manual`. Distinct from [`Self::WebhookTriggered`]
    /// (repo-scoped) because manual requests target a specific app
    /// and bypass the repo→apps resolver.
    ManualSyncRequested { app: AppName },
    /// An aggregate health re-assessment completed for an app. The
    /// `status` is the worst-of across all owned resources;
    /// `message` surfaces the reason from whichever resource drove
    /// the aggregate. Published by the health engine on each
    /// assessment; consumers include status APIs and anything that
    /// needs to react to app-level health transitions.
    AppHealthAssessed {
        app: AppName,
        cluster: ClusterName,
        status: HealthStatusCode,
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookSource {
    GitHub,
    GitLab,
    Bitbucket,
}

/// Event wrapper with a wall-clock timestamp, stamped by the bus on
/// publish. Consumers use this for metrics/audit; the actual
/// ordering guarantee is the broadcast channel's FIFO-per-receiver.
#[derive(Debug, Clone)]
pub struct BusEvent {
    pub at: SystemTime,
    pub event: SystemEvent,
}

/// Many-producer / many-consumer event bus. Clone it freely — the
/// sender half is cheap and all clones route to the same channel.
#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<BusEvent>,
}

impl EventBus {
    /// `capacity` is the per-channel ring buffer. Slow consumers that
    /// fall more than `capacity` events behind start getting
    /// `Lagged` on their next recv — the bus logs a warning with the
    /// lag size so the drops aren't silent.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Publish an event. Returns the number of active subscribers
    /// that received it; `0` is not an error (the bus is allowed to
    /// run with no subscribers, e.g. in a polling-only deployment).
    pub fn publish(&self, event: SystemEvent) -> usize {
        let wrapped = BusEvent {
            at: SystemTime::now(),
            event,
        };
        // `send` errors only when there are zero receivers — which is
        // a perfectly fine steady-state for Synchrotron. Treat it as
        // 0 delivered.
        self.tx.send(wrapped).unwrap_or(0)
    }

    /// Subscribe a new consumer. Each call yields an independent
    /// receiver with its own ring-buffer cursor.
    pub fn subscribe(&self) -> EventReceiver {
        EventReceiver {
            rx: self.tx.subscribe(),
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

pub struct EventReceiver {
    rx: broadcast::Receiver<BusEvent>,
}

#[derive(Debug)]
pub enum RecvError {
    /// The bus was dropped. Further calls will keep returning this.
    Closed,
}

impl EventReceiver {
    /// Await the next event. Lagged receivers are silently advanced
    /// to the oldest still-buffered event; the lag count is logged
    /// via tracing so ops can correlate bursts with dropped events.
    pub async fn recv(&mut self) -> Result<BusEvent, RecvError> {
        loop {
            match self.rx.recv().await {
                Ok(evt) => return Ok(evt),
                Err(broadcast::error::RecvError::Closed) => return Err(RecvError::Closed),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(lagged = n, "event bus consumer lagged; dropped {n} events");
                    continue;
                }
            }
        }
    }

    /// Non-blocking variant for drain-at-your-leisure consumers.
    /// Returns `None` if the channel is currently empty.
    pub fn try_recv(&mut self) -> Option<BusEvent> {
        match self.rx.try_recv() {
            Ok(evt) => Some(evt),
            Err(broadcast::error::TryRecvError::Empty) => None,
            Err(broadcast::error::TryRecvError::Closed) => None,
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                warn!(lagged = n, "event bus consumer lagged; dropped {n} events");
                None
            }
        }
    }
}
