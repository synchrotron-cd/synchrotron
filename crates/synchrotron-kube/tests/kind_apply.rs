//! Kind-cluster integration test for [`KubeSsaApplier`].
//!
//! Env-gated by `SYNCHROTRON_KIND_TEST=1`; the default `cargo test`
//! path stays offline. Assumes a live cluster reachable via the
//! ambient `KUBECONFIG`.
//!
//! What this proves end-to-end:
//!
//! 1. **PATCH with fieldManager.** A first apply creates the resource
//!    and registers our manager in `metadata.managedFields`.
//! 2. **Force-apply configurable per app.** A second manager edits a
//!    field; with `force=false` our re-apply fails with
//!    [`ApplyError::Conflict`]; with `force=true` we re-apply
//!    successfully and take ownership.
//! 3. **Reports conflicts with owning manager.** The conflict error
//!    carries the other manager's name parsed from the server's
//!    status message — what an operator sees on a sync attempt.
//!
//! Setup:
//!
//! ```bash
//! kind create cluster --name synchrotron-apply
//! kubectl cluster-info --context kind-synchrotron-apply
//! SYNCHROTRON_KIND_TEST=1 cargo test -p synchrotron-kube --test kind_apply -- --nocapture
//! ```

#![cfg(test)]

use kube::api::{Api, DeleteParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use kube::Client;
use serde_json::json;
use synchrotron_kube::{ApplyError, ApplyOptions, KubeSsaApplier};
use synchrotron_plugins::{Gvk, Manifest};

const FIELD_MANAGER: &str = "synchrotron-cd-test";
const RIVAL_MANAGER: &str = "rival-controller";

fn enabled() -> bool {
    std::env::var("SYNCHROTRON_KIND_TEST").as_deref() == Ok("1")
}

fn unique_namespace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("synchrotron-apply-{nanos}")
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

fn deployment_manifest(namespace: &str, name: &str, replicas: i32) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
    spec:
      containers:
        - name: app
          image: registry.k8s.io/pause:3.9
"#
    ))
    .unwrap();
    Manifest {
        gvk: Gvk::parse("apps/v1", "Deployment"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body,
    }
}

#[tokio::test]
async fn ssa_create_then_conflict_then_force_against_kind() {
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
    let manifest = deployment_manifest(namespace, "probe", 2);

    // 1. First apply creates the resource and registers FIELD_MANAGER
    //    in managedFields.
    let applied = applier
        .apply(&manifest, ApplyOptions::default())
        .await
        .expect("first apply succeeds");

    let managers = managers_of(&applied.manifest.body);
    assert!(
        managers.contains(&FIELD_MANAGER.to_string()),
        "first apply should register `{FIELD_MANAGER}` in managedFields; saw {managers:?}"
    );

    // 2. A rival manager grabs ownership of `.spec.replicas` by doing
    //    its own SSA against the same field. This is what would
    //    happen if e.g. an HPA scaled the deployment.
    let gvk = GroupVersionKind::gvk("apps", "v1", "Deployment");
    let (resource, _caps) = pinned_kind(client, &gvk).await?;
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    let rival_patch = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "probe" },
        "spec": { "replicas": 5 },
    });
    api.patch(
        "probe",
        &PatchParams::apply(RIVAL_MANAGER).force(),
        &Patch::Apply(&rival_patch),
    )
    .await?;

    // 3. We re-apply our manifest (still wants replicas=2) without
    //    force. The server must respond 409, and our error must name
    //    the rival manager.
    let conflict_err = applier
        .apply(&manifest, ApplyOptions { force: false })
        .await
        .expect_err("re-apply without force should conflict");

    match conflict_err {
        ApplyError::Conflict {
            kind,
            name,
            ref managers,
            ref reason,
        } => {
            assert_eq!(kind, "Deployment");
            assert_eq!(name, "probe");
            assert!(
                managers.iter().any(|m| m == RIVAL_MANAGER),
                "expected rival manager `{RIVAL_MANAGER}` in conflict; \
                 got managers={managers:?}, reason={reason:?}"
            );
        }
        other => panic!("expected Conflict, got {other:?}"),
    }

    // 4. Re-apply with force=true succeeds and we reclaim
    //    `.spec.replicas`. The applied body shows our requested value.
    let forced = applier
        .apply(&manifest, ApplyOptions { force: true })
        .await
        .expect("force apply should succeed");

    let replicas = forced
        .manifest
        .body
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|r| r.as_i64());
    assert_eq!(
        replicas,
        Some(2),
        "after force-apply, .spec.replicas should match our manifest"
    );

    Ok(())
}

/// Pull the set of `manager` strings out of `metadata.managedFields`.
fn managers_of(body: &serde_yaml_ng::Value) -> Vec<String> {
    let Some(seq) = body
        .get("metadata")
        .and_then(|m| m.get("managedFields"))
        .and_then(|s| s.as_sequence())
    else {
        return Vec::new();
    };
    seq.iter()
        .filter_map(|entry| {
            entry
                .get("manager")
                .and_then(|m| m.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}
