//! Kind-cluster integration test for [`KubeDryRunApplier`].
//!
//! Env-gated: only runs when `SYNCHROTRON_KIND_TEST=1` is set, so the
//! default `cargo test` path stays offline. The test assumes a live
//! Kubernetes cluster is reachable via the ambient `KUBECONFIG` (kind,
//! minikube, or any real cluster — kind is the documented target).
//!
//! What this proves:
//!
//! - The wire path actually exercises Server-Side Apply with `dryRun=All`.
//! - The response carries fields the input did *not* set — i.e. the
//!   request traversed the mutating-admission pipeline (built-in
//!   defaulting plus any installed mutating webhooks) and we observe
//!   the normalized object.
//! - The dry-run object's `.spec` matches what a real (non-dry)
//!   Server-Side Apply produces. This is the acceptance criterion's
//!   "matches what a real Patch would observe": same admission
//!   pipeline, same defaulting, same field manager.
//!
//! Setup expected from the operator running this test:
//!
//! ```bash
//! kind create cluster --name synchrotron-dryrun
//! kubectl cluster-info --context kind-synchrotron-dryrun
//! SYNCHROTRON_KIND_TEST=1 cargo test -p synchrotron-kube --test kind_dry_run -- --nocapture
//! ```
//!
//! If a stricter "external mutating webhook" check is wanted later
//! (e.g., Kyverno policy that adds an annotation), that's an additive
//! follow-up; the assertions below already verify the dry-run path
//! goes through the full admission chain.

#![cfg(test)]

use kube::api::{Api, DeleteParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use kube::Client;
use serde_json::json;
use synchrotron_diff::DryRunApplier;
use synchrotron_kube::KubeDryRunApplier;
use synchrotron_plugins::{Gvk, Manifest};

const FIELD_MANAGER: &str = "synchrotron-kind-test";

/// Returns true if the test should run. Off by default; flip on with
/// `SYNCHROTRON_KIND_TEST=1` after pointing KUBECONFIG at a kind cluster.
fn enabled() -> bool {
    std::env::var("SYNCHROTRON_KIND_TEST").as_deref() == Ok("1")
}

fn unique_namespace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("synchrotron-dryrun-{nanos}")
}

async fn ensure_namespace(client: &Client, name: &str) {
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let gvk = GroupVersionKind::gvk("", "v1", "Namespace");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover Namespace");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
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

fn deployment_manifest(namespace: &str, name: &str) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
spec:
  replicas: 1
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
        body: body.into(),
    }
}

/// Walk a YAML value and remove fields whose values are non-deterministic
/// across two separate apply round-trips (timestamps, resourceVersion,
/// UIDs, generation, managedFields entries). What's left is the spec
/// the differ actually consumes, plus stable metadata.
fn strip_volatile(v: &mut serde_yaml_ng::Value) {
    use serde_yaml_ng::Value;
    let Value::Mapping(map) = v else { return };
    if let Some(Value::Mapping(meta)) = map.get_mut("metadata") {
        for k in [
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "managedFields",
            "selfLink",
        ] {
            meta.remove(k);
        }
    }
    map.remove("status");
}

#[tokio::test]
async fn dry_run_matches_real_apply_against_kind() {
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
    let manifest = deployment_manifest(namespace, "probe");
    let applier = KubeDryRunApplier::new(client.clone(), FIELD_MANAGER);

    // 1. Ask our applier for the normalized form via dry-run SSA.
    let dry = applier.normalize(&manifest).await?;

    // 2. Independently apply for real, to compare against. Same field
    //    manager so SSA ownership lines up.
    let gvk = GroupVersionKind::gvk("apps", "v1", "Deployment");
    let (resource, _caps) = pinned_kind(client, &gvk).await?;
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    let body_json: serde_json::Value = serde_json::to_value(manifest.body.value())?;
    let real_obj = api
        .patch(
            &manifest.name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&body_json),
        )
        .await?;
    let mut real_yaml: serde_yaml_ng::Value =
        serde_json::from_value(serde_json::to_value(&real_obj)?)?;

    let mut dry_yaml = dry.body.value().clone();
    strip_volatile(&mut dry_yaml);
    strip_volatile(&mut real_yaml);

    // 3. Mutation evidence: a field absent from the input is set in
    //    the dry-run response. terminationGracePeriodSeconds is a
    //    classic API server default for Pod-templated workloads.
    let tgs = dry_yaml
        .get("spec")
        .and_then(|s| s.get("template"))
        .and_then(|t| t.get("spec"))
        .and_then(|s| s.get("terminationGracePeriodSeconds"));
    assert!(
        tgs.is_some(),
        "dry-run response should carry server-defaulted \
         spec.template.spec.terminationGracePeriodSeconds; got body: {dry_yaml:?}"
    );

    // 4. imagePullPolicy is set by the API server when omitted by the
    //    user; another fingerprint of the admission pipeline running.
    let ipp = dry_yaml
        .get("spec")
        .and_then(|s| s.get("template"))
        .and_then(|t| t.get("spec"))
        .and_then(|s| s.get("containers"))
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("imagePullPolicy"));
    assert!(
        ipp.is_some(),
        "dry-run response should carry server-defaulted imagePullPolicy"
    );

    // 5. Strongest claim: the dry-run spec equals the real apply's
    //    spec. Both traverse the same admission pipeline; any drift
    //    would mean our wire integration disagrees with reality.
    let dry_spec = dry_yaml.get("spec").cloned().unwrap_or_default();
    let real_spec = real_yaml.get("spec").cloned().unwrap_or_default();
    assert_eq!(
        dry_spec, real_spec,
        "dry-run .spec should match real apply .spec"
    );

    Ok(())
}
