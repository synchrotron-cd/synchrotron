//! Registry-level tests against synthetic kubeconfigs. The probe loop
//! will fail (no live cluster on 127.0.0.1:12345), but registration,
//! routing, and deregistration are independent of probe outcomes.

use std::fs;
use std::time::Duration;

use synchrotron_kube::{ClusterConfig, ClusterName, ClusterRegistry, HealthConfig, KubeError};
use tempfile::TempDir;

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

fn write_kubeconfig() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("kubeconfig");
    fs::write(&path, MINIMAL_KUBECONFIG).unwrap();
    (dir, path)
}

fn slow_health() -> HealthConfig {
    HealthConfig {
        interval: Duration::from_secs(60),
        probe_timeout: Duration::from_secs(1),
        reconnect_after: 100,
        initial_backoff: Duration::from_secs(60),
        max_backoff: Duration::from_secs(60),
    }
}

#[tokio::test]
async fn register_and_route_by_name() {
    let (_dir, kc) = write_kubeconfig();
    let reg = ClusterRegistry::new();
    let cfg = ClusterConfig::from_kubeconfig("alpha", &kc);

    reg.register(cfg, slow_health()).await.unwrap();
    assert_eq!(reg.len().await, 1);

    let name = ClusterName("alpha".into());
    let monitor = reg.get(&name).await.expect("registered");
    assert_eq!(monitor.name(), "alpha");

    let client = reg.client(&name).await.expect("client present");
    assert_eq!(client.name(), "alpha");

    let names = reg.list().await;
    assert_eq!(names, vec![name]);
}

#[tokio::test]
async fn duplicate_register_errors() {
    let (_dir, kc) = write_kubeconfig();
    let reg = ClusterRegistry::new();
    let cfg = ClusterConfig::from_kubeconfig("alpha", &kc);

    reg.register(cfg.clone(), slow_health()).await.unwrap();
    let err = reg
        .register(cfg, slow_health())
        .await
        .expect_err("second register should fail");
    assert!(matches!(err, KubeError::AlreadyRegistered(s) if s == "alpha"));
}

#[tokio::test]
async fn deregister_removes_entry() {
    let (_dir, kc) = write_kubeconfig();
    let reg = ClusterRegistry::new();
    let cfg = ClusterConfig::from_kubeconfig("alpha", &kc);
    reg.register(cfg, slow_health()).await.unwrap();

    let name = ClusterName("alpha".into());
    let removed = reg.deregister(&name).await.expect("was present");
    assert_eq!(removed.name(), "alpha");
    assert!(reg.is_empty().await);
    assert!(reg.get(&name).await.is_none());
}

#[tokio::test]
async fn isolates_multiple_clusters() {
    let (_dir, kc) = write_kubeconfig();
    let reg = ClusterRegistry::new();

    for n in ["alpha", "beta", "gamma"] {
        let cfg = ClusterConfig::from_kubeconfig(n, &kc);
        reg.register(cfg, slow_health()).await.unwrap();
    }

    assert_eq!(reg.len().await, 3);
    let mut names: Vec<String> = reg.list().await.into_iter().map(|n| n.0).collect();
    names.sort();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);

    // Each entry routes to its own monitor with the right name.
    for n in ["alpha", "beta", "gamma"] {
        let m = reg.get(&ClusterName(n.into())).await.unwrap();
        assert_eq!(m.name(), n);
    }
}
