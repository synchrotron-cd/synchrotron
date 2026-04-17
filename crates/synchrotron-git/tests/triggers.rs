//! Tests for the [`RepoTriggers`] registry: routing, dedup, and
//! deregistration.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use synchrotron_git::poller::FetchFn;
use synchrotron_git::{FetchResult, PollEvent, Poller, PollerConfig, RepoId, RepoTriggers, Sha};
use synchrotron_types::RepoUrl;
use tokio::sync::Mutex;

type Outcomes = Arc<Mutex<VecDeque<Result<FetchResult, String>>>>;
type Calls = Arc<Mutex<u32>>;

fn fixture() -> Result<FetchResult, String> {
    Ok(FetchResult {
        previous_head: None,
        current_head: Sha("a".repeat(40)),
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

fn long_interval() -> PollerConfig {
    PollerConfig {
        interval: Duration::from_secs(3600),
        jitter_ratio: 0.0,
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn trigger_routes_to_correct_poller() {
    let alpha_id = RepoId::from_url(&RepoUrl("https://example.com/alpha.git".into()));
    let beta_id = RepoId::from_url(&RepoUrl("https://example.com/beta.git".into()));

    let alpha_calls = Arc::new(Mutex::new(0));
    let beta_calls = Arc::new(Mutex::new(0));
    let alpha_out: Outcomes = Arc::new(Mutex::new(VecDeque::from([fixture()])));
    let beta_out: Outcomes = Arc::new(Mutex::new(VecDeque::from([fixture()])));

    let alpha = Poller::spawn(
        "alpha",
        fetch_fn(alpha_out, alpha_calls.clone()),
        long_interval(),
    );
    let beta = Poller::spawn(
        "beta",
        fetch_fn(beta_out, beta_calls.clone()),
        long_interval(),
    );

    let triggers = RepoTriggers::new();
    triggers.register(alpha_id.clone(), &alpha).await;
    triggers.register(beta_id.clone(), &beta).await;
    assert_eq!(triggers.len().await, 2);

    let mut alpha_rx = alpha.subscribe();
    let mut beta_rx = beta.subscribe();

    assert!(triggers.trigger(&alpha_id).await);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let evt = alpha_rx.recv().await.unwrap();
    assert!(matches!(evt, PollEvent::Fetched(_)));
    assert_eq!(*alpha_calls.lock().await, 1);
    assert_eq!(*beta_calls.lock().await, 0);
    assert!(beta_rx.try_recv().is_err());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn trigger_burst_collapses_to_few_fetches() {
    // Notify buffers at most one permit, so a burst of N triggers
    // can produce at most 2 fetches: one for the wake that consumed
    // the first permit, and one for the residual buffered permit.
    // The point is that 100 triggers do NOT produce 100 fetches.
    let id = RepoId::from_url(&RepoUrl("https://example.com/repo.git".into()));
    let calls = Arc::new(Mutex::new(0));
    let out: Outcomes = Arc::new(Mutex::new(
        std::iter::repeat_with(fixture).take(10).collect(),
    ));

    let poller = Poller::spawn("burst", fetch_fn(out, calls.clone()), long_interval());
    let triggers = RepoTriggers::new();
    triggers.register(id.clone(), &poller).await;

    tokio::task::yield_now().await;
    for _ in 0..100 {
        assert!(triggers.trigger(&id).await);
    }
    // Drain plenty of yields to let the loop run any follow-up
    // iterations triggered by the buffered permit.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    let n = *calls.lock().await;
    assert!(
        (1..=2).contains(&n),
        "100 triggers should collapse to 1-2 fetches, got {n}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unknown_id_returns_false() {
    let triggers = RepoTriggers::new();
    let id = RepoId::from_url(&RepoUrl("https://example.com/missing.git".into()));
    assert!(!triggers.trigger(&id).await);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn deregister_removes_routing() {
    let id = RepoId::from_url(&RepoUrl("https://example.com/dropme.git".into()));
    let calls = Arc::new(Mutex::new(0));
    let out: Outcomes = Arc::new(Mutex::new(VecDeque::from([fixture()])));

    let poller = Poller::spawn("dropme", fetch_fn(out, calls.clone()), long_interval());
    let triggers = RepoTriggers::new();
    triggers.register(id.clone(), &poller).await;
    assert!(triggers.deregister(&id).await);
    assert!(triggers.is_empty().await);

    // Subsequent triggers route to nothing.
    assert!(!triggers.trigger(&id).await);

    // The poller itself can still be triggered directly (registration
    // is just a routing hop).
    poller.trigger();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(*calls.lock().await, 1);
}
