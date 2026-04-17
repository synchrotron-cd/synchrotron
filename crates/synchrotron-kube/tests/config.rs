//! Integration tests for synchrotron-kube config loading.
//!
//! These verify client construction against a synthetic kubeconfig —
//! no real cluster is contacted. Live-cluster behaviour (apiserver_version,
//! health probes) is covered by sibling sub-issues that run against a
//! kind/minikube harness.

use std::fs;

use synchrotron_kube::{ClusterConfig, KubeClient, KubeError};
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
