//! Monitor-level tests using injected probe/reconnect hooks.
//!
//! A seed KubeClient is built from a synthetic kubeconfig (no live
//! cluster). Probe outcomes are driven from a shared VecDeque so the
//! test can script the remote's behaviour precisely.

use std::collections::VecDeque;
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use synchrotron_kube::health::{HealthMonitor, ProbeFn, ReconnectFn};
use synchrotron_kube::{ClusterConfig, HealthConfig, HealthState, KubeClient};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time;

const MINIMAL_KUBECONFIG: &str = r#"apiVersion: v1
kind: Config
current-context: test
clusters:
- name: test-cluster
  cluster:
    server: https://127.0.0.1:12345
    insecure-skip-tls-verify: true
users:
- name: test-user
  user:
    token: dummy-token
contexts:
- name: test
  context:
    cluster: test-cluster
    user: test-user
"#;

/// Build a KubeClient from a synthetic kubeconfig. The tempdir is leaked
/// so the monitor's client reference stays valid for the test lifetime.
async fn seed_client() -> KubeClient {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("kubeconfig");
    fs::write(&path, MINIMAL_KUBECONFIG).unwrap();
    let cfg = ClusterConfig::from_kubeconfig("test-cluster", &path);
    let client = KubeClient::connect(&cfg).await.unwrap();
    std::mem::forget(dir);
    client
}

fn fast_config() -> HealthConfig {
    HealthConfig {
        interval: Duration::from_millis(20),
        probe_timeout: Duration::from_millis(50),
        reconnect_after: 3,
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
    }
}

type Outcomes = Arc<Mutex<VecDeque<Result<(), String>>>>;

/// Pop the next scripted outcome; once exhausted, repeat the last value
/// (so `recovery` tests can stay Up after a single Err is consumed).
fn scripted_probe(outcomes: Outcomes) -> ProbeFn {
    let last: Arc<Mutex<Result<(), String>>> = Arc::new(Mutex::new(Ok(())));
    Arc::new(move || {
        let outcomes = outcomes.clone();
        let last = last.clone();
        let f: Pin<Box<dyn Future<Output = Result<(), String>> + Send>> = Box::pin(async move {
            let mut g = outcomes.lock().await;
            if let Some(v) = g.pop_front() {
                *last.lock().await = v.clone();
                v
            } else {
                last.lock().await.clone()
            }
        });
        f
    })
}

fn counting_reconnect(counter: Arc<Mutex<u32>>) -> ReconnectFn {
    Arc::new(move || {
        let counter = counter.clone();
        let f: Pin<Box<dyn Future<Output = Result<(), String>> + Send>> = Box::pin(async move {
            *counter.lock().await += 1;
            Ok(())
        });
        f
    })
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn reports_up_after_successful_probe() {
    let client = seed_client().await;
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([Ok(()), Ok(()), Ok(())])));
    let reconnects = Arc::new(Mutex::new(0u32));

    let monitor = HealthMonitor::spawn_with_hooks(
        "test",
        client,
        scripted_probe(outcomes),
        counting_reconnect(reconnects.clone()),
        fast_config(),
    );

    let mut rx = monitor.subscribe();
    // Yield once so the spawned task runs its first probe.
    tokio::task::yield_now().await;
    while *rx.borrow_and_update() == HealthState::Unknown {
        rx.changed().await.unwrap();
    }
    assert!(monitor.current_state().is_up());
    assert_eq!(*reconnects.lock().await, 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn triggers_reconnect_after_threshold_failures() {
    let client = seed_client().await;
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from(vec![Err("boom".into()); 8])));
    let reconnects = Arc::new(Mutex::new(0u32));

    let monitor = HealthMonitor::spawn_with_hooks(
        "test",
        client,
        scripted_probe(outcomes),
        counting_reconnect(reconnects.clone()),
        fast_config(),
    );

    // Advance virtual time through several probe+backoff cycles.
    for _ in 0..12 {
        time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
    }

    let state = monitor.current_state();
    assert!(state.is_down(), "expected Down, got {state:?}");
    let n = *reconnects.lock().await;
    assert!(
        n >= 1,
        "expected at least one reconnect after 3+ failures, got {n}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn recovers_to_up_after_transient_failure() {
    let client = seed_client().await;
    let outcomes: Outcomes = Arc::new(Mutex::new(VecDeque::from([
        Err("transient".into()),
        Ok(()),
        Ok(()),
        Ok(()),
    ])));
    let reconnects = Arc::new(Mutex::new(0u32));

    let monitor = HealthMonitor::spawn_with_hooks(
        "test",
        client,
        scripted_probe(outcomes),
        counting_reconnect(reconnects.clone()),
        fast_config(),
    );

    for _ in 0..8 {
        time::advance(Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
    }

    assert!(monitor.current_state().is_up());
    assert_eq!(*reconnects.lock().await, 0);
}
