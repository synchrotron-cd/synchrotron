//! Kind-cluster end-to-end test for the Smart Diff pipeline (yrs).
//!
//! Env-gated (`SYNCHROTRON_KIND_TEST=1`). Stitches together everything
//! the four xje tiers ship:
//!
//! - [`KubeDryRunApplier`] (xje.1, oor) — server-side normalization.
//! - [`ScalerCache`] populated from a live HPA on the cluster (xje.2,
//!   shv) — auto-ignore for `spec.replicas`.
//! - [`SmartDiffPipeline`] (xje.3 + xje.4 + this slice) — user-rule
//!   layer and ownership partition.
//!
//! What we prove on a real cluster:
//!
//! 1. Apply a Deployment under field manager `synchrotron-cd`.
//! 2. Apply an HPA targeting that Deployment, then `kubectl
//!    scale`-style update `spec.replicas` under a *different* field
//!    manager, simulating the controller. The live object now has
//!    `replicas` owned by `kube-controller-manager`-class.
//! 3. Run the pipeline with desired `replicas: 1` against the live
//!    object's `replicas: 5`. Assert: zero drift on
//!    `spec.replicas` — auto-ignore swallowed it.
//! 4. Now bump the desired image to `pause:3.10`. Assert: drift
//!    surfaces, and its path is the container image.
//!
//! This is the second xje acceptance criterion: HPA-targeted
//! Deployment + admission pipeline produces no false positive on
//! `spec.replicas` while still catching real image drift.

#![cfg(test)]

use std::time::Duration;

use kube::api::{Api, DeleteParams, DynamicObject, Patch, PatchParams, PostParams};
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use kube::Client;
use serde_json::json;
use synchrotron_diff::auto_ignore::{parse_scaler, ScalerCache};
use synchrotron_diff::SmartDiffPipeline;
use synchrotron_kube::KubeDryRunApplier;
use synchrotron_plugins::{Gvk, Manifest};

const FIELD_MANAGER: &str = "synchrotron-cd";

fn enabled() -> bool {
    std::env::var("SYNCHROTRON_KIND_TEST").as_deref() == Ok("1")
}

fn unique_namespace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("synchrotron-yrs-{nanos}")
}

async fn ensure_namespace(client: &Client, name: &str) {
    let gvk = GroupVersionKind::gvk("", "v1", "Namespace");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover Namespace");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
    let mut obj: DynamicObject = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    }))
    .unwrap();
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

fn deployment_yaml(namespace: &str, name: &str, image: &str) -> String {
    format!(
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
          image: {image}
"#
    )
}

fn deployment_manifest(namespace: &str, name: &str, image: &str) -> Manifest {
    Manifest {
        gvk: Gvk::parse("apps/v1", "Deployment"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body: serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&deployment_yaml(namespace, name, image)).unwrap().into(),
    }
}

fn hpa_manifest(namespace: &str, name: &str, target: &str) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {name}
  namespace: {namespace}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {target}
  minReplicas: 1
  maxReplicas: 10
"#
    ))
    .unwrap();
    Manifest {
        gvk: Gvk::parse("autoscaling/v2", "HorizontalPodAutoscaler"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body: body.into(),
    }
}

async fn apply(
    client: &Client,
    manifest: &Manifest,
    field_manager: &str,
) -> Result<DynamicObject, Box<dyn std::error::Error>> {
    let kube_gvk = GroupVersionKind::gvk(
        &manifest.gvk.group,
        &manifest.gvk.version,
        &manifest.gvk.kind,
    );
    let (resource, _caps) = pinned_kind(client, &kube_gvk).await?;
    let ns = manifest.namespace.as_deref().expect("namespaced");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &resource);
    let body_json: serde_json::Value = serde_json::to_value(manifest.body.value())?;
    let obj = api
        .patch(
            &manifest.name,
            &PatchParams::apply(field_manager).force(),
            &Patch::Apply(&body_json),
        )
        .await?;
    Ok(obj)
}

async fn live_deployment(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<Manifest, Box<dyn std::error::Error>> {
    let gvk = GroupVersionKind::gvk("apps", "v1", "Deployment");
    let (resource, _caps) = pinned_kind(client, &gvk).await?;
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    let obj = api.get(name).await?;
    let body: serde_yaml_ng::Value = serde_json::from_value(serde_json::to_value(&obj)?)?;
    Ok(Manifest {
        gvk: Gvk::parse("apps/v1", "Deployment"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body: body.into(),
    })
}

#[tokio::test]
async fn smart_diff_pipeline_against_kind() {
    if !enabled() {
        eprintln!(
            "SYNCHROTRON_KIND_TEST not set; skipping. \
             Run with `SYNCHROTRON_KIND_TEST=1` against a kind cluster."
        );
        return;
    }
    let client = Client::try_default().await.expect("kube client");
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
    // 1. Apply the initial Deployment as synchrotron-cd.
    let desired_v1 = deployment_manifest(namespace, "web", "registry.k8s.io/pause:3.9");
    apply(client, &desired_v1, FIELD_MANAGER).await?;

    // 2. Apply an HPA targeting it.
    let hpa = hpa_manifest(namespace, "web", "web");
    apply(client, &hpa, FIELD_MANAGER).await?;

    // 3. Simulate the autoscaler updating replicas under a *different*
    //    field manager. We don't wait for the real HPA controller to
    //    react (that depends on metrics-server, which kind doesn't
    //    install by default); instead we patch as `kube-controller-manager`
    //    so managedFields attribution matches what production sees.
    let dep_gvk = GroupVersionKind::gvk("apps", "v1", "Deployment");
    let (dep_res, _) = pinned_kind(client, &dep_gvk).await?;
    let dep_api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &dep_res);
    let scale_patch: serde_json::Value = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "web" },
        "spec": { "replicas": 5 },
    });
    dep_api
        .patch(
            "web",
            &PatchParams::apply("kube-controller-manager").force(),
            &Patch::Apply(&scale_patch),
        )
        .await?;

    // Settle: re-read live so managedFields reflects the second apply.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let live = live_deployment(client, namespace, "web").await?;
    let live_replicas = live
        .body
        .value()
        .get("spec")
        .and_then(|s| s.get("replicas"))
        .and_then(|v| v.as_u64());
    assert_eq!(
        live_replicas,
        Some(5),
        "expected live replicas=5 after autoscaler patch"
    );

    // 4. Build the ScalerCache from the live HPA object — same
    //    transformation ScalerDiscovery does, but synchronous so the
    //    test stays small.
    let mut cache = ScalerCache::new();
    if let Some(entry) = parse_scaler(&hpa) {
        cache.insert(entry);
    } else {
        panic!("HPA failed to parse into a ScalerEntry");
    }

    // 5. Wire the pipeline.
    let applier = KubeDryRunApplier::new(client.clone(), FIELD_MANAGER);
    let pipeline = SmartDiffPipeline::new(applier, FIELD_MANAGER);

    // 6. First assertion: same-image desired against live with
    //    autoscaler-bumped replicas → zero drift. The auto-ignore
    //    layer must swallow `spec.replicas`.
    let result = pipeline.evaluate(&desired_v1, &live, &cache).await?;
    assert!(
        result.drift.is_empty(),
        "expected no drift after autoscaling; got {:?}",
        result.drift
    );

    // 7. Second assertion: bumping the image surfaces real drift,
    //    even though replicas still differs and is still ignored.
    let desired_v2 = deployment_manifest(namespace, "web", "registry.k8s.io/pause:3.10");
    let result = pipeline.evaluate(&desired_v2, &live, &cache).await?;
    let image_changes: Vec<_> = result
        .drift
        .changes
        .iter()
        .filter(|c| format!("{}", c.path()).contains("image"))
        .collect();
    assert!(
        !image_changes.is_empty(),
        "expected image drift; full drift = {:?}",
        result.drift
    );
    let replicas_changes: Vec<_> = result
        .drift
        .changes
        .iter()
        .filter(|c| format!("{}", c.path()).ends_with("spec.replicas"))
        .collect();
    assert!(
        replicas_changes.is_empty(),
        "expected no replicas drift; got {:?}",
        replicas_changes
    );

    Ok(())
}
