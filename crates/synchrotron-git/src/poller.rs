//! Per-repo polling scheduler.
//!
//! Loops `sleep(interval ± jitter)` → fetch, publishing
//! [`PollEvent`]s on a broadcast channel. Pause/resume and an
//! out-of-band `trigger()` (used by webhook integration in
//! h48.1.5) are exposed via `tokio::sync::watch` and
//! `tokio::sync::Notify` respectively.
//!
//! The fetch is hidden behind a [`FetchFn`] hook so this scheduler is
//! independent of any specific git transport — production wires
//! [`fetch_fn_from_client`] which `spawn_blocking`s into [`GitClient`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{broadcast, watch, Notify};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};

use crate::client::{FetchResult, GitClient};
use crate::repo::Repo;

/// Polling cadence and jitter. `jitter_ratio = 0.2` means the actual
/// sleep is uniform in `[interval * 0.8, interval * 1.2]`.
#[derive(Debug, Clone)]
pub struct PollerConfig {
    pub interval: Duration,
    pub jitter_ratio: f64,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(180),
            jitter_ratio: 0.2,
        }
    }
}

/// Outcome of a single poll cycle.
#[derive(Debug, Clone)]
pub enum PollEvent {
    Fetched(FetchResult),
    FetchFailed(String),
}

/// Async fetch hook. Production = `spawn_blocking(client.fetch)`.
/// Tests = scripted closures that return preset `FetchResult`s
/// without touching libgit2.
pub type FetchFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<FetchResult, String>> + Send>>
        + Send
        + Sync,
>;

pub struct Poller {
    tx: broadcast::Sender<PollEvent>,
    pause_tx: watch::Sender<bool>,
    trigger: Arc<Notify>,
    handle: JoinHandle<()>,
}

impl Poller {
    pub fn spawn(name: impl Into<String>, fetch: FetchFn, cfg: PollerConfig) -> Self {
        let (tx, _) = broadcast::channel(64);
        let (pause_tx, pause_rx) = watch::channel(false);
        let trigger = Arc::new(Notify::new());

        let task = run_loop(
            name.into(),
            fetch,
            cfg,
            tx.clone(),
            pause_rx,
            trigger.clone(),
        );
        let handle = tokio::spawn(task);

        Self {
            tx,
            pause_tx,
            trigger,
            handle,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PollEvent> {
        self.tx.subscribe()
    }

    pub fn pause(&self) {
        let _ = self.pause_tx.send(true);
    }

    pub fn resume(&self) {
        let _ = self.pause_tx.send(false);
    }

    /// Request an immediate fetch. The current sleep (if any) wakes up
    /// and runs a fetch even if `paused`. Useful for webhook-driven
    /// poke (h48.1.5).
    pub fn trigger(&self) {
        self.trigger.notify_one();
    }

    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn run_loop(
    name: String,
    fetch: FetchFn,
    cfg: PollerConfig,
    tx: broadcast::Sender<PollEvent>,
    mut pause_rx: watch::Receiver<bool>,
    trigger: Arc<Notify>,
) {
    info!(repo = %name, interval_ms = cfg.interval.as_millis() as u64, "poller started");
    loop {
        let sleep_for = jittered(cfg.interval, cfg.jitter_ratio);
        debug!(repo = %name, sleep_ms = sleep_for.as_millis() as u64, "scheduling next poll");
        let woken_by_trigger = tokio::select! {
            _ = time::sleep(sleep_for) => false,
            _ = trigger.notified() => true,
        };

        // Skip the fetch if paused, unless an explicit trigger fired.
        if *pause_rx.borrow_and_update() && !woken_by_trigger {
            debug!(repo = %name, "poller paused; skipping fetch");
            continue;
        }

        match fetch().await {
            Ok(result) => {
                let _ = tx.send(PollEvent::Fetched(result));
            }
            Err(err) => {
                warn!(repo = %name, error = %err, "fetch failed");
                let _ = tx.send(PollEvent::FetchFailed(err));
            }
        }
    }
}

fn jittered(interval: Duration, jitter_ratio: f64) -> Duration {
    if jitter_ratio <= 0.0 || interval.is_zero() {
        return interval;
    }
    let base = interval.as_secs_f64();
    let span = base * jitter_ratio;
    let lo = (base - span).max(0.0);
    let hi = base + span;
    let pick = rand::thread_rng().gen_range(lo..=hi);
    Duration::from_secs_f64(pick)
}

/// Build a production [`FetchFn`] that wraps a blocking
/// [`GitClient::fetch`] call in `spawn_blocking`. The client and repo
/// are cloned into the closure once at construction time.
pub fn fetch_fn_from_client(client: Arc<GitClient>, repo: Repo) -> FetchFn {
    Arc::new(move || {
        let client = client.clone();
        let repo = repo.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || client.fetch(&repo))
                .await
                .map_err(|e| format!("join error: {e}"))?
                .map_err(|e| e.to_string())
        })
    })
}
