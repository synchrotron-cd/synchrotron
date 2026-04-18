//! Tests for the multi-repo Orchestrator: registration semantics,
//! per-repo status, aggregate status, and bounded concurrent fetches.
//!
//! Uses a very long polling interval and manual `trigger()` calls so
//! fetch timing is deterministic and not racing with the test's clock
//! advancement.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use synchrotron_git::poller::FetchFn;
use synchrotron_git::{
    Credentials, FetchResult, Orchestrator, OrchestratorConfig, PollEvent, PollerConfig, Repo, Sha,
};
use synchrotron_types::RepoUrl;
use tokio::sync::Mutex;
use tokio::time;

type Outcomes = Arc<Mutex<VecDeque<Result<FetchResult, String>>>>;

fn make_repo(url: &str) -> Repo {
    Repo::new(RepoUrl(url.into()), "main", Credentials::None)
}

fn ok_result() -> Result<FetchResult, String> {
    Ok(FetchResult {
        previous_head: None,
        current_head: Sha("a".repeat(40)),
        changed: true,
    })
}

fn fetch_fn(outcomes: Outcomes) -> FetchFn {
    Arc::new(move || {
        let outcomes = outcomes.clone();
        Box::pin(async move {
            let mut g = outcomes.lock().await;
            g.pop_front()
                .unwrap_or_else(|| Err("test: outcomes exhausted".into()))
        })
    })
}

fn long_cfg(max_concurrent: usize) -> OrchestratorConfig {
    OrchestratorConfig {
        max_concurrent_fetches: max_concurrent,
        poller: PollerConfig {
            interval: Duration::from_secs(3600),
            jitter_ratio: 0.0,
        },
    }
}

#[tokio::test(flavor = "current_thread")]
async fn register_and_track_status() {
    let orch = Orchestrator::new(long_cfg(4));
    let repo = make_repo("https://example.com/alpha.git");
    let id = repo.id.clone();
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([ok_result()])));
    orch.register(repo, fetch_fn(outcomes)).await.unwrap();

    assert_eq!(orch.len().await, 1);
    assert_eq!(orch.list().await, vec![id.clone()]);
    assert!(orch.repo(&id).await.is_some());
    assert!(orch.status(&id).await.unwrap().last_event.is_none());

    assert!(orch.trigger(&id).await);
    let mut got = false;
    for _ in 0..100 {
        if let Some(s) = orch.status(&id).await {
            if matches!(s.last_event, Some(PollEvent::Fetched(_))) {
                got = true;
                break;
            }
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert!(got, "status never reflected the fetch");
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_registration_errors() {
    let orch = Orchestrator::new(long_cfg(4));
    let repo = make_repo("https://example.com/dup.git");
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([ok_result()])));

    orch.register(repo.clone(), fetch_fn(outcomes.clone()))
        .await
        .unwrap();
    let err = orch
        .register(repo, fetch_fn(outcomes))
        .await
        .expect_err("duplicate should fail");
    assert!(err.to_string().contains("already registered"));
}

#[tokio::test(flavor = "current_thread")]
async fn deregister_drops_entry() {
    let orch = Orchestrator::new(long_cfg(4));
    let repo = make_repo("https://example.com/dropme.git");
    let id = repo.id.clone();
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([ok_result()])));

    orch.register(repo, fetch_fn(outcomes)).await.unwrap();
    assert!(orch.deregister(&id).await);
    assert!(orch.is_empty().await);
    assert!(!orch.deregister(&id).await);
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_counts_outcomes() {
    let orch = Orchestrator::new(long_cfg(4));

    let a = make_repo("https://example.com/a.git");
    let b = make_repo("https://example.com/b.git");
    let c = make_repo("https://example.com/c.git");
    let a_id = a.id.clone();
    let b_id = b.id.clone();

    orch.register(
        a,
        fetch_fn(Arc::new(Mutex::new(VecDeque::from([ok_result()])))),
    )
    .await
    .unwrap();
    orch.register(
        b,
        fetch_fn(Arc::new(Mutex::new(VecDeque::from([Err("nope".into())])))),
    )
    .await
    .unwrap();
    orch.register(
        c,
        fetch_fn(Arc::new(Mutex::new(VecDeque::from([ok_result()])))),
    )
    .await
    .unwrap();

    // Trigger only a and b; leave c never-polled.
    assert!(orch.trigger(&a_id).await);
    assert!(orch.trigger(&b_id).await);

    // Wait for both outcomes to land.
    let mut agg = orch.aggregate().await;
    for _ in 0..100 {
        agg = orch.aggregate().await;
        if agg.fetched == 1 && agg.failed == 1 {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(agg.total, 3);
    assert_eq!(agg.fetched, 1);
    assert_eq!(agg.failed, 1);
    assert_eq!(agg.never_polled, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn semaphore_caps_concurrent_fetches() {
    // Each fetch parks until released. With max_concurrent=2 and 5
    // simultaneous triggers, peak in-flight should be ≤ 2.
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());

    let make_fetch = || {
        let in_flight = in_flight.clone();
        let peak = peak.clone();
        let release = release.clone();
        let f: FetchFn = Arc::new(move || {
            let in_flight = in_flight.clone();
            let peak = peak.clone();
            let release = release.clone();
            Box::pin(async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                release.notified().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                ok_result()
            })
        });
        f
    };

    let orch = Orchestrator::new(long_cfg(2));
    let mut ids = Vec::new();
    for i in 0..5 {
        let repo = make_repo(&format!("https://example.com/r{i}.git"));
        ids.push(repo.id.clone());
        orch.register(repo, make_fetch()).await.unwrap();
    }
    for id in &ids {
        assert!(orch.trigger(id).await);
    }

    // Wait until at least one fetch is in-flight, then check the cap.
    for _ in 0..100 {
        if in_flight.load(Ordering::SeqCst) > 0 {
            break;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    // Give the runtime a chance to schedule more if it would.
    time::sleep(Duration::from_millis(50)).await;

    let p = peak.load(Ordering::SeqCst);
    assert!(p <= 2, "peak in-flight should be capped at 2, was {p}");
    assert!(p >= 1, "expected at least one fetch to start");

    // Release everything so test ends cleanly.
    for _ in 0..10 {
        release.notify_one();
    }
}
