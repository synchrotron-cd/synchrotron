//! Integration tests for synchrotron-kube config loading.
//!
//! These verify client construction against a synthetic kubeconfig —
//! no real cluster is contacted. Live-cluster behaviour (apiserver_version,
//! health probes) is covered by sibling sub-issues that run against a
//! kind/minikube harness.

use std::fs;

use synchrotron_kube::{AuthSource, ClusterConfig, KubeClient, KubeError};
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
    namespace: default
- name: other
  context:
    cluster: test-cluster
    user: test-user
"#;

fn write_kubeconfig(dir: &TempDir, contents: &str) -> std::path::PathBuf {
    let path = dir.path().join("kubeconfig");
    fs::write(&path, contents).unwrap();
    path
}

#[tokio::test]
async fn connects_with_explicit_kubeconfig() {
    let dir = TempDir::new().unwrap();
    let path = write_kubeconfig(&dir, MINIMAL_KUBECONFIG);

    let cfg = ClusterConfig::from_kubeconfig("prod", &path);
    let client = KubeClient::connect(&cfg).await.unwrap();

    assert_eq!(client.name(), "prod");
    assert_eq!(client.context(), "test");
}

#[tokio::test]
async fn selects_requested_context() {
    let dir = TempDir::new().unwrap();
    let path = write_kubeconfig(&dir, MINIMAL_KUBECONFIG);

    let cfg = ClusterConfig::from_kubeconfig("prod", &path).with_context("other");
    let client = KubeClient::connect(&cfg).await.unwrap();

    assert_eq!(client.context(), "other");
}

#[tokio::test]
async fn missing_kubeconfig_reports_clearly() {
    let cfg = ClusterConfig::from_kubeconfig("prod", "/does/not/exist/kubeconfig");
    let err = KubeClient::connect(&cfg).await.unwrap_err();
    assert!(
        matches!(err, KubeError::KubeconfigMissing(_)),
        "got: {err:?}"
    );
}

#[tokio::test]
async fn in_cluster_builder_sets_source() {
    let cfg = ClusterConfig::in_cluster("self");
    assert!(matches!(cfg.source, AuthSource::InCluster));
    // with_context is a no-op for in-cluster — it has no kubeconfig context.
    let cfg = cfg.with_context("ignored");
    assert!(matches!(cfg.source, AuthSource::InCluster));
}

/// Without the projected SA token / env vars, `connect` for an
/// explicit in-cluster config must fail loudly rather than silently
/// degrading. We clear the env vars to simulate a non-cluster host.
#[tokio::test]
async fn in_cluster_connect_outside_cluster_errors() {
    // SAFETY: tests in this binary run in separate tokio runtimes but
    // share the process env. The vars we touch are only read by
    // kube-rs at the top of `Config::incluster_env`, so removing them
    // for the duration of this test cannot affect the other tests
    // (which use explicit kubeconfigs and never call `incluster()`).
    let saved_host = std::env::var("KUBERNETES_SERVICE_HOST").ok();
    let saved_port = std::env::var("KUBERNETES_SERVICE_PORT").ok();
    unsafe {
        std::env::remove_var("KUBERNETES_SERVICE_HOST");
        std::env::remove_var("KUBERNETES_SERVICE_PORT");
    }

    let cfg = ClusterConfig::in_cluster("self");
    let res = KubeClient::connect(&cfg).await;

    unsafe {
        if let Some(v) = saved_host {
            std::env::set_var("KUBERNETES_SERVICE_HOST", v);
        }
        if let Some(v) = saved_port {
            std::env::set_var("KUBERNETES_SERVICE_PORT", v);
        }
    }

    let err = res.expect_err("expected in-cluster connect to fail off-cluster");
    assert!(matches!(err, KubeError::InCluster(_)), "got: {err:?}");
}

#[tokio::test]
async fn unknown_context_errors() {
    let dir = TempDir::new().unwrap();
    let path = write_kubeconfig(&dir, MINIMAL_KUBECONFIG);

    let cfg = ClusterConfig::from_kubeconfig("prod", &path).with_context("ghost");
    let err = KubeClient::connect(&cfg).await.unwrap_err();
    assert!(
        matches!(err, KubeError::ContextNotFound(ref c) if c == "ghost"),
        "got: {err:?}"
    );
}
