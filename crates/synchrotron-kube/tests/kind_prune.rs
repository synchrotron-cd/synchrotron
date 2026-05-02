//! Kind-cluster integration test for the prune sweep.
//!
//! Env-gated by `SYNCHROTRON_KIND_TEST=1`. Proves end-to-end:
//!
//! 1. **Owned-set drives deletion.** A resource present in the
//!    persisted owned-set but absent from the new desired-set is
//!    deleted by [`KubeSsaApplier::delete`].
//! 2. **Prune-disabled resources survive.** An [`OwnedResource`]
//!    flagged `prune_disabled = true` is filtered out of the prune
//!    set even when missing from the desired-set.
//! 3. **Reverse-wave order.** Higher-wave resources are returned
//!    first by [`compute_prune_set`], so dependent resources go down
//!    before their dependencies.
//! 4. **Already-absent is OK.** Calling `delete` on a resource that's
//!    already gone returns Ok, so a re-run of a partially completed
//!    prune sweep is idempotent.

#![cfg(test)]

use kube::api::{Api, DeleteParams, DynamicObject, PostParams};
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use kube::Client;
use serde_json::json;
use synchrotron_kube::{compute_prune_set, ApplyOptions, KubeSsaApplier};
use synchrotron_plugins::{Gvk, Manifest, OwnedResource};

const FIELD_MANAGER: &str = "synchrotron-cd-prune-test";

fn enabled() -> bool {
    std::env::var("SYNCHROTRON_KIND_TEST").as_deref() == Ok("1")
}

fn unique_namespace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("synchrotron-prune-{nanos}")
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

fn cm(namespace: &str, name: &str, key: &str, val: &str) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {namespace}
data:
  {key}: "{val}"
"#
    ))
    .unwrap();
    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body,
    }
}

fn owned_cm(namespace: &str, name: &str, wave: i32, prune_disabled: bool) -> OwnedResource {
    OwnedResource {
        gvk: Gvk::parse("v1", "ConfigMap"),
        namespace: Some(namespace.to_string()),
        name: name.to_string(),
        wave,
        prune_disabled,
    }
}

async fn cm_exists(client: &Client, namespace: &str, name: &str) -> bool {
    let gvk = GroupVersionKind::gvk("", "v1", "ConfigMap");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover ConfigMap");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    api.get_opt(name).await.expect("get_opt cm").is_some()
}

#[tokio::test]
async fn prune_sweep_against_kind() {
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

    // Apply three ConfigMaps: keep, drop-low (wave 0), drop-high (wave 5),
    // pinned (will be marked prune_disabled in the owned record).
    let manifests = vec![
        cm(namespace, "keep", "k", "v"),
        cm(namespace, "drop-low", "k", "v"),
        cm(namespace, "drop-high", "k", "v"),
        cm(namespace, "pinned", "k", "v"),
    ];
    for m in &manifests {
        applier
            .apply(m, ApplyOptions::default())
            .await
            .unwrap_or_else(|e| panic!("apply {} failed: {e}", m.name));
    }

    let owned_set = vec![
        owned_cm(namespace, "keep", 0, false),
        owned_cm(namespace, "drop-low", 0, false),
        owned_cm(namespace, "drop-high", 5, false),
        owned_cm(namespace, "pinned", 0, true),
    ];

    // New desired-set keeps only "keep". "pinned" is gone from the
    // desired-set but flagged prune_disabled, so it should survive.
    let desired = vec![cm(namespace, "keep", "k", "v")];
    let prune = compute_prune_set(&owned_set, &desired);

    let names: Vec<_> = prune.iter().map(|r| r.name.clone()).collect();
    assert_eq!(
        names,
        vec!["drop-high".to_string(), "drop-low".to_string()],
        "prune set should be reverse-wave ordered and exclude pinned"
    );

    for target in &prune {
        applier
            .delete(target)
            .await
            .unwrap_or_else(|e| panic!("delete {} failed: {e}", target.name));
    }

    assert!(cm_exists(client, namespace, "keep").await, "keep survives");
    assert!(
        cm_exists(client, namespace, "pinned").await,
        "pinned survives despite being absent from desired"
    );
    assert!(
        !cm_exists(client, namespace, "drop-low").await,
        "drop-low pruned"
    );
    assert!(
        !cm_exists(client, namespace, "drop-high").await,
        "drop-high pruned"
    );

    // Idempotency: re-deleting a missing resource returns Ok (404 path).
    let already_gone = owned_cm(namespace, "drop-low", 0, false);
    applier
        .delete(&already_gone)
        .await
        .expect("delete on already-gone target should be Ok");

    Ok(())
}
