//! Supervisor-level tests for [`Informer`] using a scripted watch
//! factory and a manually-driven health channel. No live cluster.

use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use kube::runtime::watcher;
use synchrotron_kube::informer::{ClientProvider, WatchFactory};
use synchrotron_kube::{
    ClusterConfig, HealthState, Informer, InformerConfig, InformerEvent, KubeClient,
};
use tempfile::TempDir;
use tokio::sync::{watch, Mutex};
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

async fn seed_client() -> KubeClient {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("kubeconfig");
    fs::write(&path, MINIMAL_KUBECONFIG).unwrap();
    let cfg = ClusterConfig::from_kubeconfig("test-cluster", &path);
    let client = KubeClient::connect(&cfg).await.unwrap();
    std::mem::forget(dir);
    client
}

fn provider(client: KubeClient) -> ClientProvider {
    let client = Arc::new(client);
    Arc::new(move || {
        let c = (*client).clone();
        Box::pin(async move { c })
    })
}

/// Scripted factory: each call to the factory pops one scripted batch
/// of events from the shared queue and turns it into a finite stream.
/// A `None` batch ends the session immediately (no events).
type Script = Arc<Mutex<VecDeque<Vec<watcher::Result<watcher::Event<String>>>>>>;

fn scripted_factory(script: Script) -> WatchFactory<String> {
    Arc::new(move |_client, _cfg| {
        let mut g = script.try_lock().expect("test: script not contended");
        let batch = g.pop_front().unwrap_or_default();
        Box::pin(stream::iter(batch))
    })
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn forwards_init_sequence_as_restarted() {
    let client = seed_client().await;
    let (htx, hrx) = watch::channel(HealthState::Unknown);

    let script: Script = Arc::new(Mutex::new(VecDeque::from(vec![vec![
        Ok(watcher::Event::Init),
        Ok(watcher::Event::InitApply("a".into())),
        Ok(watcher::Event::InitApply("b".into())),
        Ok(watcher::Event::InitDone),
        Ok(watcher::Event::Apply("c".into())),
    ]])));

    let informer = Informer::<String>::spawn(
        provider(client),
        hrx,
        InformerConfig::default(),
        scripted_factory(script),
    );
    let mut rx = informer.subscribe();

    htx.send(HealthState::Up).unwrap();
    time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let restarted = rx.recv().await.unwrap();
    assert!(
        matches!(restarted, InformerEvent::Restarted(ref v) if v == &vec!["a".to_string(), "b".to_string()])
    );
    let applied = rx.recv().await.unwrap();
    assert!(matches!(applied, InformerEvent::Applied(ref s) if s == "c"));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn restarts_session_on_down_then_up() {
    let client = seed_client().await;
    let (htx, hrx) = watch::channel(HealthState::Unknown);

    // Two distinct batches: one per session.
    let script: Script = Arc::new(Mutex::new(VecDeque::from(vec![
        vec![Ok(watcher::Event::Apply("first-session".into()))],
        vec![Ok(watcher::Event::Apply("second-session".into()))],
    ])));

    let informer = Informer::<String>::spawn(
        provider(client),
        hrx,
        InformerConfig::default(),
        scripted_factory(script),
    );
    let mut rx = informer.subscribe();

    htx.send(HealthState::Up).unwrap();
    time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let first = rx.recv().await.unwrap();
    assert!(matches!(first, InformerEvent::Applied(ref s) if s == "first-session"));

    htx.send(HealthState::Down {
        consecutive_failures: 5,
        last_error: "lost".into(),
    })
    .unwrap();
    time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;

    htx.send(HealthState::Up).unwrap();
    time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let second = rx.recv().await.unwrap();
    assert!(matches!(second, InformerEvent::Applied(ref s) if s == "second-session"));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn does_not_start_session_until_health_up() {
    let client = seed_client().await;
    let (htx, hrx) = watch::channel(HealthState::Unknown);

    let script: Script = Arc::new(Mutex::new(VecDeque::from(vec![vec![Ok(
        watcher::Event::Apply("only-after-up".into()),
    )]])));

    let informer = Informer::<String>::spawn(
        provider(client),
        hrx,
        InformerConfig::default(),
        scripted_factory(script),
    );
    let mut rx = informer.subscribe();

    // While Unknown, no events should appear.
    time::advance(Duration::from_millis(50)).await;
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());

    // Down -> still no session.
    htx.send(HealthState::Down {
        consecutive_failures: 1,
        last_error: "x".into(),
    })
    .unwrap();
    time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());

    // Up -> session starts.
    htx.send(HealthState::Up).unwrap();
    time::advance(Duration::from_millis(10)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let evt = rx.recv().await.unwrap();
    assert!(matches!(evt, InformerEvent::Applied(ref s) if s == "only-after-up"));
}
