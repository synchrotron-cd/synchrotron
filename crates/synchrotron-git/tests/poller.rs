//! Scheduler-level tests using a scripted [`FetchFn`] and tokio's
//! paused virtual clock. No git transport involved.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use synchrotron_git::poller::FetchFn;
use synchrotron_git::{FetchResult, PollEvent, Poller, PollerConfig, Sha};
use tokio::sync::Mutex;
use tokio::time;

type Outcomes = Arc<Mutex<VecDeque<Result<FetchResult, String>>>>;
type Calls = Arc<Mutex<u32>>;

fn fixture_outcome(tag: &str) -> Result<FetchResult, String> {
    Ok(FetchResult {
        previous_head: None,
        current_head: Sha(format!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa{tag:0>4}")),
        changed: true,
    })
}

fn fetch_fn(outcomes: Outcomes, calls: Calls) -> FetchFn {
    Arc::new(move || {
        let outcomes = outcomes.clone();
        let calls = calls.clone();
        Box::pin(async move {
            *calls.lock().await += 1;
            let mut g = outcomes.lock().await;
            g.pop_front()
                .unwrap_or_else(|| Err("test: outcomes exhausted".into()))
        })
    })
}

fn fast_cfg() -> PollerConfig {
    PollerConfig {
        interval: Duration::from_millis(100),
        // Disable jitter so virtual time advancement is deterministic.
        jitter_ratio: 0.0,
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fires_periodic_fetches() {
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([
        fixture_outcome("0001"),
        fixture_outcome("0002"),
        fixture_outcome("0003"),
    ])));
    let calls: Calls = Arc::new(Mutex::new(0));
    let poller = Poller::spawn("test", fetch_fn(outcomes, calls.clone()), fast_cfg());
    let mut rx = poller.subscribe();

    for _ in 0..3 {
        time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        let evt = rx.recv().await.unwrap();
        assert!(matches!(evt, PollEvent::Fetched(_)));
    }
    assert_eq!(*calls.lock().await, 3);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn trigger_runs_fetch_immediately() {
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([fixture_outcome("0099")])));
    let calls: Calls = Arc::new(Mutex::new(0));
    let cfg = PollerConfig {
        interval: Duration::from_secs(3600), // long; trigger should not wait
        jitter_ratio: 0.0,
    };
    let poller = Poller::spawn("test", fetch_fn(outcomes, calls.clone()), cfg);
    let mut rx = poller.subscribe();

    // Let the loop park on its long sleep.
    tokio::task::yield_now().await;
    poller.trigger();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let evt = rx.recv().await.unwrap();
    assert!(matches!(evt, PollEvent::Fetched(_)));
    assert_eq!(*calls.lock().await, 1);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn pause_blocks_periodic_fetch_but_not_trigger() {
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([
        fixture_outcome("0001"),
        fixture_outcome("0002"),
    ])));
    let calls: Calls = Arc::new(Mutex::new(0));
    let poller = Poller::spawn("test", fetch_fn(outcomes, calls.clone()), fast_cfg());
    let mut rx = poller.subscribe();

    poller.pause();
    // Advance well past several intervals; nothing should fetch.
    for _ in 0..5 {
        time::advance(Duration::from_millis(110)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(*calls.lock().await, 0);
    assert!(rx.try_recv().is_err());

    // Trigger overrides pause.
    poller.trigger();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let evt = rx.recv().await.unwrap();
    assert!(matches!(evt, PollEvent::Fetched(_)));
    assert_eq!(*calls.lock().await, 1);

    // Resume → next interval fetches.
    poller.resume();
    time::advance(Duration::from_millis(110)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let evt = rx.recv().await.unwrap();
    assert!(matches!(evt, PollEvent::Fetched(_)));
    assert_eq!(*calls.lock().await, 2);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fetch_failure_emits_event_and_keeps_polling() {
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([
        Err("boom".into()),
        fixture_outcome("0042"),
    ])));
    let calls: Calls = Arc::new(Mutex::new(0));
    let poller = Poller::spawn("test", fetch_fn(outcomes, calls.clone()), fast_cfg());
    let mut rx = poller.subscribe();

    time::advance(Duration::from_millis(110)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let evt = rx.recv().await.unwrap();
    assert!(matches!(evt, PollEvent::FetchFailed(ref s) if s == "boom"));

    time::advance(Duration::from_millis(110)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let evt = rx.recv().await.unwrap();
    assert!(matches!(evt, PollEvent::Fetched(_)));
    assert_eq!(*calls.lock().await, 2);
}
