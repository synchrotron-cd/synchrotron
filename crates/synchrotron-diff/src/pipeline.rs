//! End-to-end Smart Diff pipeline (xje / yrs).
//!
//! Composes the four tiers shipped by the xje sub-issues into the
//! single entrypoint the reconciler calls per app per reconcile:
//!
//! 1. **Dry-run normalization** ([`DryRunApplier`]). Hands the desired
//!    manifest to the API server with `dryRun=All`, getting back
//!    "what would land in etcd" — defaults filled, mutating webhooks
//!    applied. Without this step every reconcile reports drift on
//!    every defaulted field.
//! 2. **Structural diff** ([`compare::diff`]). List-map aware compare
//!    of the normalized desired against the live manifest. Produces
//!    a [`Diff`] of leaf-level changes.
//! 3. **Auto-ignore** ([`auto_ignore_rules_for`]). Generates ignore
//!    rules from the controllers active in the cluster (HPA, VPA,
//!    KEDA), so we don't fight `spec.replicas` with an autoscaler.
//! 4. **User ignore** ([`IgnoreRules`]). Operator-declared rules,
//!    applied over the auto-ignore output.
//! 5. **Field-ownership filter** ([`FieldOwnershipFilter`]). Final
//!    partition into drift (changes we own) and informational
//!    (changes another field manager owns; surfaced for visibility,
//!    but they don't gate sync).
//!
//! The pipeline is generic over the applier so unit tests can plug in
//! a mock; `synchrotron-kube::KubeDryRunApplier` is the production
//! impl. The [`ScalerCache`] is taken by reference per-call (rather
//! than owned) so the caller can keep one shared cache behind a
//! `RwLock` updated by `synchrotron-kube::ScalerDiscovery` and pass a
//! locked snapshot in.

use synchrotron_plugins::Manifest;

use crate::applier::{DryRunApplier, DryRunError};
use crate::auto_ignore::{auto_ignore_rules_for, ScalerCache, TargetRef};
use crate::compare;
use crate::ignore::IgnoreRules;
use crate::listmap::ListMapKeys;
use crate::ownership::{FieldOwnershipFilter, FilteredDiff};

/// Complete Smart Diff pipeline. One per reconciler, configured at
/// startup with the operator's user-defined ignore rules and the SSA
/// field-manager string Synchrotron applies under.
pub struct SmartDiffPipeline<A: DryRunApplier> {
    applier: A,
    list_maps: ListMapKeys,
    user_rules: IgnoreRules,
    ownership: FieldOwnershipFilter,
}

impl<A: DryRunApplier> SmartDiffPipeline<A> {
    /// Construct a pipeline. `field_manager` must match the value the
    /// kube-backed applier passes in `?fieldManager=…` — that's the
    /// string the API server stores in `managedFields` and the one
    /// the ownership filter checks against.
    pub fn new(applier: A, field_manager: impl Into<String>) -> Self {
        Self {
            applier,
            list_maps: ListMapKeys::defaults(),
            user_rules: IgnoreRules::default(),
            ownership: FieldOwnershipFilter::new(field_manager),
        }
    }

    /// Override the list-map registry. Defaults to the builtins
    /// (containers, env vars, ports, etc.). Operators registering
    /// CRD-shaped lists drop replacements in here.
    pub fn with_list_maps(mut self, maps: ListMapKeys) -> Self {
        self.list_maps = maps;
        self
    }

    /// Install user-defined ignore rules. Multiple calls replace; if
    /// you want to extend, call [`IgnoreRules::extend`] on the bundle
    /// before passing it in.
    pub fn with_user_rules(mut self, rules: IgnoreRules) -> Self {
        self.user_rules = rules;
        self
    }

    /// Run the full pipeline for one (desired, live) pair.
    ///
    /// `scaler_cache` is borrowed for the duration of the call — the
    /// production caller will hold a read lock on a shared cache for
    /// this scope. The result partitions changes into drift (sync
    /// drivers) and informational (visibility only).
    pub async fn evaluate(
        &self,
        desired: &Manifest,
        live: &Manifest,
        scaler_cache: &ScalerCache,
    ) -> Result<FilteredDiff, DryRunError> {
        // Tier 1: server-side normalization.
        let normalized = self.applier.normalize(desired).await?;

        // Tier 2: structural compare.
        let raw = compare::diff(&normalized, live, &self.list_maps);

        // Tier 3: auto-ignore from cluster controllers. Targeting is
        // identity-based — a controller in the cache claims the live
        // workload via gvk+namespace+name, which equals desired's.
        let target = TargetRef::from_manifest(live);
        let auto_rules = auto_ignore_rules_for(&target, scaler_cache);
        let after_auto = auto_rules.apply(raw, live);

        // Tier 4: user-defined rules.
        let after_user = self.user_rules.apply(after_auto, live);

        // Tier 5: ownership partition.
        Ok(self.ownership.apply(after_user, live))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auto_ignore::{ControllerKind, ScalerEntry};
    use crate::ignore::IgnoreRule;
    use serde_yaml_ng::from_str;
    use std::future::Future;
    use std::pin::Pin;
    use synchrotron_plugins::Gvk;

    /// Returns a clone of the input. Useful when desired is already
    /// in normalized form (the differ then exercises tiers 2–5
    /// independently of the applier).
    struct IdentityApplier;
    impl DryRunApplier for IdentityApplier {
        fn normalize<'a>(
            &'a self,
            manifest: &'a Manifest,
        ) -> Pin<Box<dyn Future<Output = Result<Manifest, DryRunError>> + Send + 'a>> {
            let cloned = manifest.clone();
            Box::pin(async move { Ok(cloned) })
        }
    }

    /// Returns a fixed canned manifest regardless of input — models a
    /// server that defaults a field the operator omitted.
    struct CannedApplier {
        out: Manifest,
    }
    impl DryRunApplier for CannedApplier {
        fn normalize<'a>(
            &'a self,
            _manifest: &'a Manifest,
        ) -> Pin<Box<dyn Future<Output = Result<Manifest, DryRunError>> + Send + 'a>> {
            let cloned = self.out.clone();
            Box::pin(async move { Ok(cloned) })
        }
    }

    fn deployment(name: &str, namespace: &str, body_yaml: &str) -> Manifest {
        let body: serde_yaml_ng::Value = from_str(body_yaml).unwrap();
        Manifest {
            gvk: Gvk {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some(namespace.into()),
            name: name.into(),
            body,
        }
    }

    fn target_for(m: &Manifest) -> TargetRef {
        TargetRef::from_manifest(m)
    }

    /// Identical desired/live → no drift.
    #[tokio::test]
    async fn identical_yields_no_drift() {
        let body = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: app
          image: img:1
"#;
        let desired = deployment("web", "prod", body);
        let live = desired.clone();
        let pipe = SmartDiffPipeline::new(IdentityApplier, "synchrotron-cd");
        let cache = ScalerCache::new();
        let out = pipe.evaluate(&desired, &live, &cache).await.unwrap();
        assert!(out.drift.is_empty());
        assert!(out.informational.is_empty());
    }

    /// HPA targets the deployment, live has 5 replicas, desired says
    /// 1 — auto-ignore swallows the drift.
    #[tokio::test]
    async fn hpa_replicas_drift_is_swallowed_by_auto_ignore() {
        let desired = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: app
          image: img:1
"#,
        );
        let live = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  replicas: 5
  template:
    spec:
      containers:
        - name: app
          image: img:1
"#,
        );
        let mut cache = ScalerCache::new();
        cache.insert(ScalerEntry {
            controller: ControllerKind::Hpa,
            target: target_for(&live),
            ignored_paths: vec!["spec.replicas".into()],
        });
        let pipe = SmartDiffPipeline::new(IdentityApplier, "synchrotron-cd");
        let out = pipe.evaluate(&desired, &live, &cache).await.unwrap();
        assert!(out.drift.is_empty(), "drift = {:?}", out.drift);
        assert!(out.informational.is_empty());
    }

    /// HPA-managed cluster + image change → image drift surfaces;
    /// replicas does not.
    #[tokio::test]
    async fn image_drift_surfaces_even_under_hpa() {
        let desired = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: app
          image: img:2
"#,
        );
        let live = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  replicas: 5
  template:
    spec:
      containers:
        - name: app
          image: img:1
"#,
        );
        let mut cache = ScalerCache::new();
        cache.insert(ScalerEntry {
            controller: ControllerKind::Hpa,
            target: target_for(&live),
            ignored_paths: vec!["spec.replicas".into()],
        });
        let pipe = SmartDiffPipeline::new(IdentityApplier, "synchrotron-cd");
        let out = pipe.evaluate(&desired, &live, &cache).await.unwrap();
        assert_eq!(out.drift.len(), 1);
        let path_str = format!("{}", out.drift.changes[0].path());
        assert!(path_str.contains("image"), "path = {path_str}");
    }

    /// User ignore rule prunes a change auto-ignore wouldn't catch.
    #[tokio::test]
    async fn user_rule_prunes_after_auto_ignore() {
        let desired = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
  annotations:
    deploy/seq: "10"
spec: {}
"#,
        );
        let live = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
  annotations:
    deploy/seq: "11"
spec: {}
"#,
        );
        let pipe = SmartDiffPipeline::new(IdentityApplier, "synchrotron-cd").with_user_rules(
            IgnoreRules::new(vec![IgnoreRule::PathGlob(
                "metadata.annotations.deploy/seq".into(),
            )]),
        );
        let cache = ScalerCache::new();
        let out = pipe.evaluate(&desired, &live, &cache).await.unwrap();
        assert!(out.drift.is_empty(), "drift = {:?}", out.drift);
    }

    /// Change owned by another manager → informational, not drift.
    #[tokio::test]
    async fn ownership_filter_routes_other_manager_to_informational() {
        let desired = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  paused: false
"#,
        );
        let live = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
  managedFields:
    - manager: kubectl
      operation: Update
      fieldsV1:
        f:spec:
          f:paused: {}
spec:
  paused: true
"#,
        );
        let pipe = SmartDiffPipeline::new(IdentityApplier, "synchrotron-cd");
        let cache = ScalerCache::new();
        let out = pipe.evaluate(&desired, &live, &cache).await.unwrap();
        assert!(out.drift.is_empty(), "drift = {:?}", out.drift);
        assert_eq!(out.informational.len(), 1);
    }

    /// Server defaults a field the operator omitted → no drift,
    /// because tier 1 (the applier) bakes the default into desired
    /// before the structural compare runs.
    #[tokio::test]
    async fn server_defaulting_via_applier_eliminates_false_positive() {
        let desired_input = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  template:
    spec:
      containers:
        - name: app
          image: img:1
"#,
        );
        // Applier returns the same body but with imagePullPolicy
        // defaulted in — what a real API server would do for an
        // image without an explicit `:tag` policy.
        let normalized = deployment(
            "web",
            "prod",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec:
  template:
    spec:
      containers:
        - name: app
          image: img:1
          imagePullPolicy: IfNotPresent
"#,
        );
        let live = normalized.clone();
        let pipe = SmartDiffPipeline::new(CannedApplier { out: normalized }, "synchrotron-cd");
        let cache = ScalerCache::new();
        let out = pipe.evaluate(&desired_input, &live, &cache).await.unwrap();
        assert!(out.drift.is_empty());
        assert!(out.informational.is_empty());
    }

    /// Applier failure propagates as `Err` — the pipeline does not
    /// silently swallow.
    #[tokio::test]
    async fn applier_failure_propagates() {
        struct FailApplier;
        impl DryRunApplier for FailApplier {
            fn normalize<'a>(
                &'a self,
                _manifest: &'a Manifest,
            ) -> Pin<Box<dyn Future<Output = Result<Manifest, DryRunError>> + Send + 'a>>
            {
                Box::pin(async { Err(DryRunError::Server("boom".into())) })
            }
        }
        let desired = deployment("x", "ns", "apiVersion: v1\nkind: ConfigMap\nmetadata: {}\n");
        let live = desired.clone();
        let pipe = SmartDiffPipeline::new(FailApplier, "synchrotron-cd");
        let cache = ScalerCache::new();
        let err = pipe.evaluate(&desired, &live, &cache).await.unwrap_err();
        assert!(matches!(err, DryRunError::Server(_)));
    }
}
