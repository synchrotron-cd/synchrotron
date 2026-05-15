//! End-to-end kind-cluster integration test for the Deployment
//! Orchestrator (h48.5).
//!
//! Stitches together every sub-slice of the epic against a real
//! cluster:
//!
//! - **h48.5.1** SSA apply via [`KubeSsaApplier`] driven by
//!   [`execute_waves`].
//! - **h48.5.2** Prune via [`compute_prune_set`] + `applier.delete`,
//!   keyed off a per-app owned-set.
//! - **h48.5.3** Wave + within-wave kind ordering: manifests carry
//!   `synchrotron.io/sync-wave` annotations and span two waves; the
//!   per-wave kind table sequences within them.
//! - **h48.5.4** PreSync / PostSync hooks via [`run_hook`], with
//!   `BeforeHookCreation` re-run on the second sync.
//! - **h48.5.5** Rollback: persist each successful sync via
//!   [`Database::record_sync_revision`], then load a prior revision
//!   and re-drive the same apply path.
//!
//! Env-gated by `SYNCHROTRON_KIND_TEST=1`; the default `cargo test`
//! path stays offline.

#![cfg(test)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use kube::api::{Api, DeleteParams, DynamicObject, PostParams};
use kube::core::GroupVersionKind;
use kube::discovery::pinned_kind;
use kube::Client;
use serde_json::json;
use synchrotron_core::db::Database;
use synchrotron_kube::{
    compute_prune_set, run_hook, ApplyOptions, DeletePolicy as KubeDeletePolicy, HookOutcome,
    HookRunOptions, KubeSsaApplier,
};
use synchrotron_plugins::{Gvk, Manifest, OwnedResource};
use synchrotron_reconcile::{
    execute_waves, group_into_waves, hooks_for_phase, plan, split_hooks, wave_of, Applier,
    ApplyError as ReconcileApplyError, HealthChecker, HookPhase, PlanEntry, PlannedAction,
    ResourceRef, WaveExecConfig,
};
use synchrotron_types::HealthStatusCode;
use tempfile::TempDir;

const FIELD_MANAGER: &str = "synchrotron-cd-orchestrator-test";
const APP_ID: &str = "shop";

fn enabled() -> bool {
    std::env::var("SYNCHROTRON_KIND_TEST").as_deref() == Ok("1")
}

fn unique_namespace() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("synchrotron-orch-{nanos}")
}

async fn ensure_namespace(client: &Client, name: &str) {
    let gvk = GroupVersionKind::gvk("", "v1", "Namespace");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover Namespace");
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
    let body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    });
    let mut obj: DynamicObject = serde_json::from_value(body).unwrap();
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

fn cm(namespace: &str, name: &str, marker: &str, wave: i32) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {namespace}
  annotations:
    synchrotron.io/sync-wave: "{wave}"
data:
  marker: "{marker}"
"#
    ))
    .unwrap();
    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.to_string(),
        namespace: Some(namespace.to_string()),
        body: body.into(),
    }
}

fn hook_job(namespace: &str, name: &str, phase: &str, command: &str) -> Manifest {
    let body: serde_yaml_ng::Value = serde_yaml_ng::from_str(&format!(
        r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: {name}
  namespace: {namespace}
  annotations:
    synchrotron.io/hook: {phase}
    synchrotron.io/hook-delete-policy: BeforeHookCreation,HookSucceeded
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

async fn cm_marker(client: &Client, namespace: &str, name: &str) -> Option<String> {
    let gvk = GroupVersionKind::gvk("", "v1", "ConfigMap");
    let (resource, _caps) = pinned_kind(client, &gvk).await.expect("discover ConfigMap");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    let obj = api.get_opt(name).await.expect("get_opt cm")?;
    obj.data
        .get("data")?
        .get("marker")?
        .as_str()
        .map(str::to_owned)
}

async fn cm_exists(client: &Client, namespace: &str, name: &str) -> bool {
    cm_marker(client, namespace, name).await.is_some()
}

/// Adapter from kube's [`KubeSsaApplier`] to the reconcile crate's
/// [`Applier`] trait. The reconcile [`PlanEntry`] only carries a
/// [`ResourceRef`], so we look up the desired manifest body and the
/// owned-set [`OwnedResource`] for deletes from caller-supplied tables.
struct KubeApplier<'a> {
    inner: &'a KubeSsaApplier,
    desired: HashMap<ResourceRef, Manifest>,
    owned: HashMap<ResourceRef, OwnedResource>,
}

impl<'a> Applier for KubeApplier<'a> {
    fn apply<'b>(
        &'b self,
        entry: &'b PlanEntry,
        _manifest: Option<Manifest>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ReconcileApplyError>> + Send + 'b>> {
        Box::pin(async move {
            match entry.action {
                PlannedAction::Apply => {
                    let m = self.desired.get(&entry.resource).ok_or_else(|| {
                        ReconcileApplyError::new(format!(
                            "no desired manifest for {:?}",
                            entry.resource
                        ))
                    })?;
                    self.inner
                        .apply(m, ApplyOptions::default())
                        .await
                        .map(|_| ())
                        .map_err(|e| ReconcileApplyError::new(e.to_string()))
                }
                PlannedAction::Delete => {
                    let owned =
                        self.owned
                            .get(&entry.resource)
                            .cloned()
                            .unwrap_or_else(|| OwnedResource {
                                gvk: entry.resource.gvk.clone(),
                                namespace: entry.resource.namespace.clone(),
                                name: entry.resource.name.clone(),
                                wave: 0,
                                prune_disabled: false,
                            });
                    self.inner
                        .delete(&owned)
                        .await
                        .map_err(|e| ReconcileApplyError::new(e.to_string()))
                }
                PlannedAction::NoOp => Ok(()),
            }
        })
    }
}

/// Stub health checker: ConfigMaps are trivially healthy and we don't
/// want this test to pull in the production health-checking path
/// (separate epic). Returns `Healthy` on the first poll for any
/// resource set, so `execute_waves` advances immediately.
struct AlwaysHealthy;
impl HealthChecker for AlwaysHealthy {
    fn health<'a>(
        &'a self,
        _resources: &'a [ResourceRef],
    ) -> Pin<Box<dyn Future<Output = HealthStatusCode> + Send + 'a>> {
        Box::pin(async { HealthStatusCode::Healthy })
    }
}

fn owned_from(manifests: &[Manifest]) -> Vec<OwnedResource> {
    manifests
        .iter()
        .map(|m| OwnedResource {
            gvk: m.gvk.clone(),
            namespace: m.namespace.clone(),
            name: m.name.clone(),
            wave: wave_of(m),
            prune_disabled: false,
        })
        .collect()
}

fn owned_lookup(manifests: &[Manifest]) -> HashMap<ResourceRef, OwnedResource> {
    owned_from(manifests)
        .into_iter()
        .map(|o| {
            (
                ResourceRef {
                    gvk: o.gvk.clone(),
                    namespace: o.namespace.clone(),
                    name: o.name.clone(),
                },
                o,
            )
        })
        .collect()
}

fn desired_lookup(manifests: &[Manifest]) -> HashMap<ResourceRef, Manifest> {
    manifests
        .iter()
        .map(|m| (ResourceRef::of(m), m.clone()))
        .collect()
}

fn hook_opts() -> HookRunOptions {
    HookRunOptions {
        delete_policies: vec![
            KubeDeletePolicy::BeforeHookCreation,
            KubeDeletePolicy::HookSucceeded,
        ],
        timeout: Duration::from_secs(120),
        poll_interval: Duration::from_millis(500),
    }
}

fn wave_cfg() -> WaveExecConfig {
    WaveExecConfig {
        per_wave_timeout: Duration::from_secs(30),
        poll_interval: Duration::from_millis(100),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deployment_orchestrator_full_flow_against_kind() {
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

async fn run_assertions(client: &Client, ns: &str) -> Result<(), Box<dyn std::error::Error>> {
    let applier = KubeSsaApplier::new(client.clone(), FIELD_MANAGER);
    let tmp = TempDir::new()?;
    let db = Database::open(&tmp.path().join("orch.sqlite"))?;
    db.conn().execute(
        "INSERT INTO applications (id, name, namespace, repo_url, path, dest_cluster, dest_namespace) \
         VALUES (?1, ?1, 'default', 'https://example/r.git', '.', 'in-cluster', ?1)",
        rusqlite::params![APP_ID],
    )?;

    // === v1: initial sync ===
    //
    // Two ConfigMaps spanning two waves plus a PreSync hook. The hook
    // is split out via `split_hooks` so it doesn't end up in the
    // owned-set or the wave plan.
    let v1 = vec![
        cm(ns, "settings", "v1", 0),
        cm(ns, "feature-flags", "on", 1),
        hook_job(ns, "presync", "PreSync", "echo pre-v1 && exit 0"),
    ];
    let split_v1 = split_hooks(&v1);
    assert_eq!(split_v1.non_hook.len(), 2);
    assert_eq!(split_v1.hooks.len(), 1);

    // h48.5.4: PreSync hook runs before the apply path.
    for h in hooks_for_phase(&split_v1.hooks, HookPhase::PreSync) {
        let outcome = run_hook(&applier, &h.manifest, &hook_opts()).await?;
        assert_eq!(outcome, HookOutcome::Succeeded, "PreSync v1 should succeed");
    }

    // h48.5.1 + h48.5.3: SSA apply driven by wave grouping.
    let p1 = plan(&split_v1.non_hook, &[]);
    assert_eq!(p1.apply_count(), 2);
    assert_eq!(p1.delete_count(), 0);
    let wp1 = group_into_waves(&p1, &split_v1.non_hook, &[]);
    assert_eq!(wp1.waves.len(), 2, "v1 spans two waves");

    let kube_app1 = KubeApplier {
        inner: &applier,
        desired: desired_lookup(&split_v1.non_hook),
        owned: HashMap::new(),
    };
    let report1 = execute_waves(
        &wp1,
        &kube_app1,
        &AlwaysHealthy,
        &wave_cfg(),
        &split_v1.non_hook,
        &[],
    )
    .await?;
    assert_eq!(report1.completed_waves, vec![0, 1]);

    assert_eq!(
        cm_marker(client, ns, "settings").await.as_deref(),
        Some("v1")
    );
    assert!(cm_exists(client, ns, "feature-flags").await);

    // h48.5.5: persist revision so we can roll back later.
    let rev1_id =
        db.record_sync_revision(APP_ID, "v1-commit", &[1u8; 32], &split_v1.non_hook, 0)?;
    let owned_v1 = split_v1.non_hook.clone();

    // === v2: drift ===
    //
    // Re-key `settings`, drop `feature-flags`, add `audit`. The same
    // PreSync hook name re-runs — `BeforeHookCreation` deletes the
    // prior copy first.
    let v2 = vec![
        cm(ns, "settings", "v2", 0),
        cm(ns, "audit", "ok", 0),
        hook_job(ns, "presync", "PreSync", "echo pre-v2 && exit 0"),
    ];
    let split_v2 = split_hooks(&v2);

    for h in hooks_for_phase(&split_v2.hooks, HookPhase::PreSync) {
        let outcome = run_hook(&applier, &h.manifest, &hook_opts()).await?;
        assert_eq!(
            outcome,
            HookOutcome::Succeeded,
            "PreSync v2 must re-run via BeforeHookCreation"
        );
    }

    // Apply path uses `live=&[]` so the wave plan only emits
    // creates/updates; pruning is the owned-set's job (h48.5.2).
    let p2 = plan(&split_v2.non_hook, &[]);
    let wp2 = group_into_waves(&p2, &split_v2.non_hook, &[]);
    let kube_app2 = KubeApplier {
        inner: &applier,
        desired: desired_lookup(&split_v2.non_hook),
        owned: HashMap::new(),
    };
    execute_waves(
        &wp2,
        &kube_app2,
        &AlwaysHealthy,
        &wave_cfg(),
        &split_v2.non_hook,
        &[],
    )
    .await?;

    // h48.5.2: owned-set drives the prune sweep.
    let prune_v2 = compute_prune_set(&owned_from(&owned_v1), &split_v2.non_hook);
    assert_eq!(
        prune_v2.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        vec!["feature-flags"],
        "only feature-flags should be pruned on v2"
    );
    for target in &prune_v2 {
        applier.delete(target).await?;
    }

    assert_eq!(
        cm_marker(client, ns, "settings").await.as_deref(),
        Some("v2"),
        "settings updated to v2"
    );
    assert!(cm_exists(client, ns, "audit").await, "audit added on v2");
    assert!(
        !cm_exists(client, ns, "feature-flags").await,
        "feature-flags pruned on v2"
    );

    let _rev2_id =
        db.record_sync_revision(APP_ID, "v2-commit", &[2u8; 32], &split_v2.non_hook, 0)?;
    let owned_v2 = split_v2.non_hook.clone();

    // h48.5.4: PostSync hook runs after the apply+prune phases.
    let post = hook_job(ns, "postsync", "PostSync", "echo post-v2 && exit 0");
    let outcome = run_hook(&applier, &post, &hook_opts()).await?;
    assert_eq!(outcome, HookOutcome::Succeeded);

    // === Rollback to v1 ===
    //
    // Operator inspects the revision listing, picks v1, fetches its
    // manifests, and re-drives the same apply+prune path.
    let listing = db.list_sync_revisions(APP_ID, 10)?;
    assert_eq!(listing.len(), 2);
    let target = listing
        .iter()
        .find(|r| r.commit_hash == "v1-commit")
        .unwrap();
    assert_eq!(target.id, rev1_id);

    let (_rev, manifests_v1) = db.load_sync_revision(target.id)?.unwrap();
    assert_eq!(manifests_v1.len(), 2, "rollback loads v1 non-hook set");

    let p_rb = plan(&manifests_v1, &[]);
    let wp_rb = group_into_waves(&p_rb, &manifests_v1, &[]);
    let kube_app_rb = KubeApplier {
        inner: &applier,
        desired: desired_lookup(&manifests_v1),
        owned: owned_lookup(&owned_v2),
    };
    execute_waves(
        &wp_rb,
        &kube_app_rb,
        &AlwaysHealthy,
        &wave_cfg(),
        &manifests_v1,
        &[],
    )
    .await?;

    let prune_rb = compute_prune_set(&owned_from(&owned_v2), &manifests_v1);
    assert_eq!(
        prune_rb.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        vec!["audit"],
        "rollback should prune audit (introduced in v2)"
    );
    for target in &prune_rb {
        applier.delete(target).await?;
    }

    assert_eq!(
        cm_marker(client, ns, "settings").await.as_deref(),
        Some("v1"),
        "rollback restored settings=v1"
    );
    assert!(
        cm_exists(client, ns, "feature-flags").await,
        "rollback recreated feature-flags"
    );
    assert!(
        !cm_exists(client, ns, "audit").await,
        "rollback pruned audit"
    );

    Ok(())
}
