//! Kind-cluster integration test for sync hooks.
//!
//! Env-gated by `SYNCHROTRON_KIND_TEST=1`. Proves end-to-end:
//!
//! 1. **Job hook applies + waits.** A successful Job hook runs to
//!    completion and `run_hook` returns [`HookOutcome::Succeeded`].
//! 2. **HookSucceeded cleanup.** With the policy set, the Job is
//!    deleted after the success outcome is recorded.
//! 3. **HookFailed retains success.** A failed Job with only
//!    `HookSucceeded` set leaves the artifact in place for postmortem.
//! 4. **BeforeHookCreation re-run.** A second invocation of the same
//!    hook name succeeds because the prior copy is deleted first
//!    (Job creation otherwise rejects re-creation by name).

#![cfg(test)]

use std::time::Duration;

use kube::api::{Api, DeleteParams, DynamicObject, PostParams};
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use kube::Client;
use serde_json::json;
use synchrotron_kube::{run_hook, DeletePolicy, HookOutcome, HookRunOptions, KubeSsaApplier};
use synchrotron_plugins::{Gvk, Manifest};

const FIELD_MANAGER: &str = "synchrotron-cd-hook-test";

fn enabled() -> bool {
    std::env::var("SYNCHROTRON_KIND_TEST").as_deref() == Ok("1")
}

fn unique_namespace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("synchrotron-hook-{nanos}")
}

async fn ensure_namespace(client: &Client, name: &str) {
    let gvk = GroupVersionKind::gvk("", "v1", "Namespace");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover Namespace");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let mut obj: DynamicObject = serde_json::from_value(ns_body).unwrap();
    obj.metadata.name = Some(name.to_string());
    api.create(&PostParams::default(), &obj)
        .await
        .expect("create namespace");
}

async fn delete_namespace(client: &Client, name: &str) {
    let gvk = GroupVersionKind::gvk("", "v1", "Namespace");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover Namespace");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
    let _ = api.delete(name, &DeleteParams::background()).await;
}

fn job_manifest(namespace: &str, name: &str, command: &str) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: {name}
  namespace: {namespace}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: hook
          image: busybox:1.36
          command: ["sh", "-c", "{command}"]
"#
    ))
    .unwrap();
    Manifest {
        gvk: Gvk::parse("batch/v1", "Job"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body: body.into(),
    }
}

async fn job_exists(client: &Client, namespace: &str, name: &str) -> bool {
    let gvk = GroupVersionKind::gvk("batch", "v1", "Job");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover Job");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    api.get_opt(name).await.expect("get_opt job").is_some()
}

fn opts(policies: &[DeletePolicy]) -> HookRunOptions {
    HookRunOptions {
        delete_policies: policies.to_vec(),
        timeout: Duration::from_secs(120),
        poll_interval: Duration::from_millis(500),
    }
}

#[tokio::test]
async fn hook_lifecycle_against_kind() {
    if !enabled() {
        eprintln!(
            "SYNCHROTRON_KIND_TEST not set; skipping. \
             Run with `SYNCHROTRON_KIND_TEST=1` against a kind cluster."
        );
        return;
    }

    let client = Client::try_default()
        .await
        .expect("kube client from ambient KUBECONFIG");
    let namespace = unique_namespace();
    ensure_namespace(&client, &namespace).await;

    let result = run_assertions(&client, &namespace).await;
    delete_namespace(&client, &namespace).await;
    result.expect("assertions");
}

async fn run_assertions(
    client: &Client,
    namespace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let applier = KubeSsaApplier::new(client.clone(), FIELD_MANAGER);

    // 1. Successful hook with HookSucceeded cleanup → completes,
    //    artifact is removed.
    let success = job_manifest(namespace, "ok-hook", "echo hello && exit 0");
    let outcome = run_hook(&applier, &success, &opts(&[DeletePolicy::HookSucceeded])).await?;
    assert_eq!(outcome, HookOutcome::Succeeded);
    assert!(
        !job_exists(client, namespace, "ok-hook").await,
        "HookSucceeded should have deleted the Job"
    );

    // 2. Failed hook with only HookSucceeded → outcome Failed, Job
    //    artifact preserved for debugging.
    let failed = job_manifest(namespace, "fail-hook", "echo nope && exit 1");
    let outcome = run_hook(&applier, &failed, &opts(&[DeletePolicy::HookSucceeded])).await?;
    assert_eq!(outcome, HookOutcome::Failed);
    assert!(
        job_exists(client, namespace, "fail-hook").await,
        "HookSucceeded must NOT delete a Failed Job"
    );

    // 3. BeforeHookCreation lets us re-run the same hook name. Without
    //    the policy, Job creation fails by-name; with it, the prior
    //    failed Job is removed before re-apply.
    let rerun = job_manifest(namespace, "fail-hook", "echo retry && exit 0");
    let outcome = run_hook(
        &applier,
        &rerun,
        &opts(&[
            DeletePolicy::BeforeHookCreation,
            DeletePolicy::HookSucceeded,
        ]),
    )
    .await?;
    assert_eq!(outcome, HookOutcome::Succeeded);
    assert!(
        !job_exists(client, namespace, "fail-hook").await,
        "HookSucceeded should clean up the successful re-run"
    );

    Ok(())
}
