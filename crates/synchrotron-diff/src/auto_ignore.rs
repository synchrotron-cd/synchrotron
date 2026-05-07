//! Controller-aware auto-ignore (xje.2).
//!
//! Some Kubernetes controllers legitimately mutate fields of objects
//! we manage. Reporting those mutations as drift causes the
//! reconciler to fight the controller — the canonical bad case being
//! a GitOps tool that resets `spec.replicas` while the
//! HorizontalPodAutoscaler scales it back up. The fix is to teach
//! the differ which controllers are active in the cluster, which
//! target each is pointed at, and which fields each is allowed to
//! mutate, then ignore changes on those fields for those targets.
//!
//! This module is the *pure-Rust core*: it parses the controller
//! manifests, builds an indexed cache, and answers "what fields
//! should I auto-ignore on this target?" The cluster discovery layer
//! that populates the cache (list/watch via informers, periodic
//! refresh) lives in `synchrotron-kube` and is wired in a follow-up
//! bead. This split lets the rules table be exhaustively unit-tested
//! offline against fixture YAML.
//!
//! # Supported controllers
//!
//! - `autoscaling/v2` `HorizontalPodAutoscaler` — owns
//!   `spec.replicas` on its `scaleTargetRef`.
//! - `autoscaling.k8s.io/v1` `VerticalPodAutoscaler` — owns
//!   `spec.template.spec.containers.*.resources` on its `targetRef`
//!   when `updateMode != Off`. We're conservative: we ignore for any
//!   updateMode except an explicit `Off`.
//! - `keda.sh/v1alpha1` `ScaledObject` — owns `spec.replicas` on its
//!   `scaleTargetRef`. (KEDA also generates an HPA under the hood,
//!   so this is double-coverage; harmless.)
//!
//! Other CRDs (Argo Rollouts, Karpenter, etc.) are out of scope for
//! this slice and can be added incrementally without changing the
//! cache shape.

use std::collections::HashMap;

use serde_yaml_ng::Value;
use synchrotron_plugins::{Gvk, Manifest};

use crate::ignore::{IgnoreRule, IgnoreRules};

/// Identity of a workload that a controller targets. The triple
/// (gvk, namespace, name) is the cache key. Cluster-scoped targets
/// use `namespace: None`.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TargetRef {
    pub gvk: Gvk,
    pub namespace: Option<String>,
    pub name: String,
}

impl TargetRef {
    pub fn from_manifest(m: &Manifest) -> Self {
        Self {
            gvk: m.gvk.clone(),
            namespace: m.namespace.clone(),
            name: m.name.clone(),
        }
    }
}

/// Which kind of controller produced an entry. Carried for
/// diagnostics — the cache lookup itself doesn't care.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControllerKind {
    Hpa,
    Vpa,
    KedaScaledObject,
}

/// One controller's claim on a target's fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalerEntry {
    pub controller: ControllerKind,
    pub target: TargetRef,
    /// PathGlob entries (matching `IgnoreRule::PathGlob` syntax) the
    /// controller is allowed to mutate. Multiple are OR'd together.
    pub ignored_paths: Vec<String>,
}

/// Indexed by target. A single workload may have multiple
/// controllers pointed at it (HPA + VPA is common); their
/// `ignored_paths` accumulate.
#[derive(Debug, Clone, Default)]
pub struct ScalerCache {
    by_target: HashMap<TargetRef, Vec<ScalerEntry>>,
}

impl ScalerCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, entry: ScalerEntry) {
        self.by_target
            .entry(entry.target.clone())
            .or_default()
            .push(entry);
    }

    pub fn entries_for(&self, target: &TargetRef) -> &[ScalerEntry] {
        self.by_target
            .get(target)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn len(&self) -> usize {
        self.by_target.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_target.is_empty()
    }

    /// Replace the entire cache contents. Discovery loops use this
    /// after a full refresh to swap atomically.
    pub fn replace_with(&mut self, entries: Vec<ScalerEntry>) {
        self.by_target.clear();
        for e in entries {
            self.insert(e);
        }
    }
}

/// Generate auto-ignore rules for the given target by aggregating
/// every cached controller pointed at it. Returns an empty
/// [`IgnoreRules`] when no controller targets the workload.
pub fn auto_ignore_rules_for(target: &TargetRef, cache: &ScalerCache) -> IgnoreRules {
    let entries = cache.entries_for(target);
    if entries.is_empty() {
        return IgnoreRules::default();
    }
    let mut rules = Vec::with_capacity(entries.iter().map(|e| e.ignored_paths.len()).sum());
    for entry in entries {
        for path in &entry.ignored_paths {
            rules.push(IgnoreRule::PathGlob(path.clone()));
        }
    }
    IgnoreRules::new(rules)
}

/// Try to interpret `manifest` as one of the supported controller
/// kinds and extract a [`ScalerEntry`]. Returns `None` if the GVK
/// isn't a known controller, or if the manifest's target reference
/// is missing/malformed.
pub fn parse_scaler(manifest: &Manifest) -> Option<ScalerEntry> {
    match (manifest.gvk.group.as_str(), manifest.gvk.kind.as_str()) {
        ("autoscaling", "HorizontalPodAutoscaler") => parse_hpa(manifest),
        ("autoscaling.k8s.io", "VerticalPodAutoscaler") => parse_vpa(manifest),
        ("keda.sh", "ScaledObject") => parse_keda(manifest),
        _ => None,
    }
}

fn parse_hpa(m: &Manifest) -> Option<ScalerEntry> {
    let target = parse_scale_target_ref(m.body.value(), &["spec", "scaleTargetRef"], m.namespace.clone())?;
    Some(ScalerEntry {
        controller: ControllerKind::Hpa,
        target,
        ignored_paths: vec!["spec.replicas".into()],
    })
}

fn parse_vpa(m: &Manifest) -> Option<ScalerEntry> {
    // updateMode == "Off" means the VPA only recommends; it does not
    // mutate the target. Skip those.
    let update_mode = m
        .body
        .value()
        .get("spec")
        .and_then(|s| s.get("updatePolicy"))
        .and_then(|u| u.get("updateMode"))
        .and_then(|m| m.as_str());
    if matches!(update_mode, Some("Off")) {
        return None;
    }
    let target = parse_scale_target_ref(m.body.value(), &["spec", "targetRef"], m.namespace.clone())?;
    Some(ScalerEntry {
        controller: ControllerKind::Vpa,
        target,
        ignored_paths: vec!["spec.template.spec.containers.*.resources".into()],
    })
}

fn parse_keda(m: &Manifest) -> Option<ScalerEntry> {
    let target = parse_scale_target_ref(m.body.value(), &["spec", "scaleTargetRef"], m.namespace.clone())?;
    Some(ScalerEntry {
        controller: ControllerKind::KedaScaledObject,
        target,
        ignored_paths: vec!["spec.replicas".into()],
    })
}

/// Walk `path` into `body` and read a `{ apiVersion, kind, name }`
/// triple. The optional namespace defaults to the controller's own
/// namespace — Kubernetes target refs are typically same-namespace.
fn parse_scale_target_ref(
    body: &Value,
    path: &[&str],
    default_namespace: Option<String>,
) -> Option<TargetRef> {
    let mut cur = body;
    for seg in path {
        cur = cur.get(*seg)?;
    }
    let api_version = cur.get("apiVersion").and_then(|v| v.as_str())?;
    let kind = cur.get("kind").and_then(|v| v.as_str())?;
    let name = cur.get("name").and_then(|v| v.as_str())?.to_string();
    Some(TargetRef {
        gvk: Gvk::parse(api_version, kind),
        namespace: default_namespace,
        name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::from_str;

    fn manifest(yaml: &str) -> Manifest {
        let body: Value = from_str(yaml).unwrap();
        let api_version = body
            .get("apiVersion")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let kind = body
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let name = body
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        let namespace = body
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Manifest {
            gvk: Gvk::parse(&api_version, &kind),
            namespace,
            name,
            body: body.into(),
        }
    }

    fn deployment_target(name: &str, ns: &str) -> TargetRef {
        TargetRef {
            gvk: Gvk {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some(ns.into()),
            name: name.into(),
        }
    }

    #[test]
    fn parse_hpa_extracts_target_and_replicas_path() {
        let m = manifest(
            r#"
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: web-hpa
  namespace: prod
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web
  minReplicas: 1
  maxReplicas: 10
"#,
        );
        let entry = parse_scaler(&m).unwrap();
        assert_eq!(entry.controller, ControllerKind::Hpa);
        assert_eq!(entry.target, deployment_target("web", "prod"));
        assert_eq!(entry.ignored_paths, vec!["spec.replicas"]);
    }

    #[test]
    fn parse_vpa_in_auto_mode_extracts_resources_path() {
        let m = manifest(
            r#"
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: web-vpa
  namespace: prod
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web
  updatePolicy:
    updateMode: Auto
"#,
        );
        let entry = parse_scaler(&m).unwrap();
        assert_eq!(entry.controller, ControllerKind::Vpa);
        assert_eq!(
            entry.ignored_paths,
            vec!["spec.template.spec.containers.*.resources"]
        );
    }

    #[test]
    fn parse_vpa_with_off_mode_returns_none() {
        let m = manifest(
            r#"
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: web-vpa
  namespace: prod
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web
  updatePolicy:
    updateMode: Off
"#,
        );
        assert!(parse_scaler(&m).is_none());
    }

    #[test]
    fn parse_vpa_without_update_policy_defaults_to_active() {
        // Real VPAs may omit updatePolicy entirely — server defaults
        // it to Auto. Be conservative and assume mutating.
        let m = manifest(
            r#"
apiVersion: autoscaling.k8s.io/v1
kind: VerticalPodAutoscaler
metadata:
  name: web-vpa
  namespace: prod
spec:
  targetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web
"#,
        );
        let entry = parse_scaler(&m).unwrap();
        assert_eq!(entry.controller, ControllerKind::Vpa);
    }

    #[test]
    fn parse_keda_scaledobject_extracts_replicas_path() {
        let m = manifest(
            r#"
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: web-keda
  namespace: prod
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: web
  triggers:
    - type: kafka
"#,
        );
        let entry = parse_scaler(&m).unwrap();
        assert_eq!(entry.controller, ControllerKind::KedaScaledObject);
        assert_eq!(entry.ignored_paths, vec!["spec.replicas"]);
    }

    #[test]
    fn unknown_kind_returns_none() {
        let m = manifest(
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec: {}
"#,
        );
        assert!(parse_scaler(&m).is_none());
    }

    #[test]
    fn missing_target_ref_returns_none() {
        let m = manifest(
            r#"
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: web-hpa
  namespace: prod
spec:
  minReplicas: 1
"#,
        );
        assert!(parse_scaler(&m).is_none());
    }

    #[test]
    fn cache_aggregates_multiple_controllers_per_target() {
        let mut cache = ScalerCache::new();
        let target = deployment_target("web", "prod");
        cache.insert(ScalerEntry {
            controller: ControllerKind::Hpa,
            target: target.clone(),
            ignored_paths: vec!["spec.replicas".into()],
        });
        cache.insert(ScalerEntry {
            controller: ControllerKind::Vpa,
            target: target.clone(),
            ignored_paths: vec!["spec.template.spec.containers.*.resources".into()],
        });
        let rules = auto_ignore_rules_for(&target, &cache);
        assert_eq!(rules.rules.len(), 2);
    }

    #[test]
    fn auto_ignore_rules_apply_to_diff_drops_replicas_drift() {
        use crate::compare::{Change, Diff};
        use crate::path::ValuePath;

        let mut cache = ScalerCache::new();
        let target = deployment_target("web", "prod");
        cache.insert(ScalerEntry {
            controller: ControllerKind::Hpa,
            target: target.clone(),
            ignored_paths: vec!["spec.replicas".into()],
        });
        let rules = auto_ignore_rules_for(&target, &cache);

        let live = Manifest {
            gvk: target.gvk.clone(),
            namespace: target.namespace.clone(),
            name: target.name.clone(),
            body: from_str("metadata: {}").unwrap(),
        };
        let diff = Diff {
            changes: vec![
                Change::Modified {
                    path: ValuePath::root().field("spec").field("replicas"),
                    desired: Value::String("3".into()),
                    live: Value::String("5".into()),
                },
                Change::Modified {
                    path: ValuePath::root().field("spec").field("paused"),
                    desired: Value::String("false".into()),
                    live: Value::String("true".into()),
                },
            ],
        };
        let out = rules.apply(diff, &live);
        assert_eq!(out.changes.len(), 1);
    }

    #[test]
    fn no_targeting_controller_yields_empty_rules() {
        let cache = ScalerCache::new();
        let target = deployment_target("web", "prod");
        let rules = auto_ignore_rules_for(&target, &cache);
        assert!(rules.is_empty());
    }

    #[test]
    fn replace_with_swaps_cache_atomically() {
        let mut cache = ScalerCache::new();
        let t1 = deployment_target("a", "ns");
        let t2 = deployment_target("b", "ns");
        cache.insert(ScalerEntry {
            controller: ControllerKind::Hpa,
            target: t1.clone(),
            ignored_paths: vec!["spec.replicas".into()],
        });
        assert!(!cache.entries_for(&t1).is_empty());
        cache.replace_with(vec![ScalerEntry {
            controller: ControllerKind::Hpa,
            target: t2.clone(),
            ignored_paths: vec!["spec.replicas".into()],
        }]);
        assert!(cache.entries_for(&t1).is_empty());
        assert!(!cache.entries_for(&t2).is_empty());
    }

    #[test]
    fn target_ref_from_manifest_round_trips() {
        let m = manifest(
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: prod
spec: {}
"#,
        );
        let t = TargetRef::from_manifest(&m);
        assert_eq!(t, deployment_target("web", "prod"));
    }
}
