//! Tier-1 built-in health checks for standard Kubernetes kinds.
//!
//! This is the fast path of the three-tier health engine: for a
//! known kind (Deployment, StatefulSet, Pod, …) we inspect the
//! object's `status` field directly and return one of
//! [`HealthStatusCode`]'s six states. Tiers 2 (conditions
//! convention) and 3 (CEL overrides) land as separate slices and
//! take over when the kind isn't one we recognise here.
//!
//! The mapping is deterministic: given the same manifest bytes, the
//! same status code comes out. That property is what lets the
//! readiness gate in [`synchrotron_reconcile::execute_waves`] trust
//! this crate as its [`HealthChecker`] backend — a flapping health
//! signal would thrash the wave advance loop.
//!
//! # Entry point
//!
//! [`assess`] takes a [`Manifest`] and returns a [`HealthAssessment`].
//! Dispatch order:
//!
//! 1. **Tier 1** — a hard-coded rule for the kind (see the `match`
//!    in [`assess`]). This is the authoritative path for the kinds
//!    we ship rules for.
//! 2. **Tier 2** — [`assess_by_conditions`]: for kinds without a
//!    tier-1 rule, fall back to the conventional `status.conditions`
//!    pattern. Looks for `type: Ready` first, then `type: Available`,
//!    and maps `True/False/Unknown` to
//!    `Healthy/Degraded/Progressing`. This covers most operators and
//!    CRDs with zero per-resource config.
//! 3. **Tier 3** (future) — user-authored CEL overrides for anything
//!    Tiers 1 and 2 don't cover.
//!
//! If none of the tiers produces an answer, the result is
//! [`HealthStatusCode::Unknown`] with a short explanatory message.
//!
//! # Conventions
//!
//! - A missing/empty `status` block reports `Progressing`: the
//!   object exists but the controller hasn't observed it yet.
//!   (`Missing` is reserved for "the object isn't there at all",
//!   which this crate doesn't decide — a higher-level lookup does.)
//! - `generation != status.observedGeneration` always reports
//!   `Progressing`, regardless of the other fields. An un-observed
//!   spec change means the controller's status is stale.

use serde_yaml_ng::Value;
use synchrotron_plugins::Manifest;
use synchrotron_types::HealthStatusCode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthAssessment {
    pub status: HealthStatusCode,
    pub message: Option<String>,
}

impl HealthAssessment {
    fn new(status: HealthStatusCode, message: Option<&str>) -> Self {
        Self {
            status,
            message: message.map(str::to_string),
        }
    }

    fn healthy() -> Self {
        Self::new(HealthStatusCode::Healthy, None)
    }

    fn progressing(msg: &str) -> Self {
        Self::new(HealthStatusCode::Progressing, Some(msg))
    }

    fn degraded(msg: &str) -> Self {
        Self::new(HealthStatusCode::Degraded, Some(msg))
    }

    fn suspended(msg: &str) -> Self {
        Self::new(HealthStatusCode::Suspended, Some(msg))
    }

    fn unknown(msg: &str) -> Self {
        Self::new(HealthStatusCode::Unknown, Some(msg))
    }
}

/// Assess the health of a single Kubernetes manifest.
///
/// Dispatches on (group, kind). Unknown kinds report `Unknown` with
/// a message — not a failure, just "this crate doesn't have a rule
/// for that." The caller layers tier 2/3 on top.
pub fn assess(manifest: &Manifest) -> HealthAssessment {
    let group = manifest.gvk.group.as_str();
    let kind = manifest.gvk.kind.as_str();
    let body = &manifest.body;
    match (group, kind) {
        ("apps", "Deployment") => assess_deployment(body),
        ("apps", "StatefulSet") => assess_statefulset(body),
        ("apps", "DaemonSet") => assess_daemonset(body),
        ("batch", "Job") => assess_job(body),
        ("", "Pod") => assess_pod(body),
        ("", "Service") => assess_service(body),
        ("networking.k8s.io", "Ingress") => assess_ingress(body),
        ("", "PersistentVolumeClaim") => assess_pvc(body),
        ("apiextensions.k8s.io", "CustomResourceDefinition") => assess_crd(body),
        ("autoscaling", "HorizontalPodAutoscaler") => assess_hpa(body),
        _ => assess_by_conditions(manifest)
            .unwrap_or_else(|| HealthAssessment::unknown("no tier-1 or tier-2 signal")),
    }
}

/// Tier-2 health assessment by `status.conditions` convention.
///
/// Scans the manifest's `status.conditions` list for one of the
/// community-standard readiness conditions — `Ready` preferred,
/// `Available` as a fallback — and maps its status to a
/// [`HealthAssessment`]:
///
/// - `status: "True"` → [`HealthStatusCode::Healthy`]
/// - `status: "False"` → [`HealthStatusCode::Degraded`] (reason or
///   message copied through when present)
/// - `status: "Unknown"` or any other value → [`HealthStatusCode::Progressing`]
///
/// Returns `None` when neither condition exists. That's an
/// intentional "no signal" — callers can decide whether to surface
/// it as `Unknown` or defer to a tier-3 override.
///
/// `Ready` is chosen over `Available` because `Ready` is the
/// broader convention (Pod, Node, most controller-runtime CRDs),
/// while `Available` is narrower (Deployment uses it, but we have a
/// tier-1 rule for Deployment already). Picking the more general
/// one first handles more kinds correctly.
pub fn assess_by_conditions(manifest: &Manifest) -> Option<HealthAssessment> {
    let status = manifest.body.get("status")?;
    let cond = find_condition(status, "Ready").or_else(|| find_condition(status, "Available"))?;
    Some(health_from_condition(cond))
}

fn health_from_condition(cond: &Value) -> HealthAssessment {
    let status = condition_status(cond).unwrap_or("");
    let detail = condition_reason(cond)
        .or_else(|| cond.get("message").and_then(Value::as_str))
        .unwrap_or("");
    match status {
        "True" => HealthAssessment::healthy(),
        "False" => HealthAssessment::degraded(if detail.is_empty() {
            "condition reports false"
        } else {
            detail
        }),
        // "Unknown" or anything else: the controller hasn't
        // committed to a verdict yet.
        _ => HealthAssessment::progressing(if detail.is_empty() {
            "condition status unknown"
        } else {
            detail
        }),
    }
}

// ---- Helpers -------------------------------------------------------

fn status_of(body: &Value) -> Option<&Value> {
    body.get("status")
}

fn spec_of(body: &Value) -> Option<&Value> {
    body.get("spec")
}

fn generation_drift(body: &Value) -> bool {
    let gen = body
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(Value::as_i64);
    let observed = body
        .get("status")
        .and_then(|s| s.get("observedGeneration"))
        .and_then(Value::as_i64);
    match (gen, observed) {
        (Some(g), Some(o)) => g != o,
        // If generation is set but status hasn't reported, the
        // controller hasn't seen the update yet — treat as drift.
        (Some(_), None) => true,
        _ => false,
    }
}

/// Find a condition by `type`. Returns the first match.
fn find_condition<'a>(status: &'a Value, type_: &str) -> Option<&'a Value> {
    status
        .get("conditions")?
        .as_sequence()?
        .iter()
        .find(|c| c.get("type").and_then(Value::as_str) == Some(type_))
}

fn condition_status(cond: &Value) -> Option<&str> {
    cond.get("status").and_then(Value::as_str)
}

fn condition_reason(cond: &Value) -> Option<&str> {
    cond.get("reason").and_then(Value::as_str)
}

fn i64_field(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

// ---- Per-kind checks ----------------------------------------------

fn assess_deployment(body: &Value) -> HealthAssessment {
    if spec_of(body)
        .and_then(|s| s.get("paused"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        return HealthAssessment::suspended("deployment paused");
    }
    if generation_drift(body) {
        return HealthAssessment::progressing("generation not yet observed");
    }
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("status not yet reported");
    };
    if let Some(cond) = find_condition(status, "Progressing") {
        if condition_status(cond) == Some("False")
            && condition_reason(cond) == Some("ProgressDeadlineExceeded")
        {
            return HealthAssessment::degraded("progress deadline exceeded");
        }
    }
    // Argo convention: desired = spec.replicas (default 1).
    let desired = spec_of(body)
        .and_then(|s| s.get("replicas"))
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let updated = i64_field(status, "updatedReplicas");
    let available = i64_field(status, "availableReplicas");
    let total = i64_field(status, "replicas");
    if updated < desired {
        return HealthAssessment::progressing("rollout in progress");
    }
    if total > updated {
        return HealthAssessment::progressing("old replicas pending termination");
    }
    if available < updated {
        return HealthAssessment::progressing("replicas not yet available");
    }
    HealthAssessment::healthy()
}

fn assess_statefulset(body: &Value) -> HealthAssessment {
    if generation_drift(body) {
        return HealthAssessment::progressing("generation not yet observed");
    }
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("status not yet reported");
    };
    let desired = spec_of(body)
        .and_then(|s| s.get("replicas"))
        .and_then(Value::as_i64)
        .unwrap_or(1);
    let ready = i64_field(status, "readyReplicas");
    if ready < desired {
        return HealthAssessment::progressing("replicas not yet ready");
    }
    let current = status.get("currentRevision").and_then(Value::as_str);
    let update = status.get("updateRevision").and_then(Value::as_str);
    if let (Some(c), Some(u)) = (current, update) {
        if c != u {
            return HealthAssessment::progressing("rolling to new revision");
        }
    }
    HealthAssessment::healthy()
}

fn assess_daemonset(body: &Value) -> HealthAssessment {
    if generation_drift(body) {
        return HealthAssessment::progressing("generation not yet observed");
    }
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("status not yet reported");
    };
    let desired = i64_field(status, "desiredNumberScheduled");
    let updated = i64_field(status, "updatedNumberScheduled");
    let available = i64_field(status, "numberAvailable");
    if updated < desired {
        return HealthAssessment::progressing("pods not yet updated on all nodes");
    }
    if available < desired {
        return HealthAssessment::progressing("pods not yet available on all nodes");
    }
    HealthAssessment::healthy()
}

fn assess_job(body: &Value) -> HealthAssessment {
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("job not yet started");
    };
    if let Some(cond) = find_condition(status, "Failed") {
        if condition_status(cond) == Some("True") {
            return HealthAssessment::degraded("job failed");
        }
    }
    if let Some(cond) = find_condition(status, "Complete") {
        if condition_status(cond) == Some("True") {
            return HealthAssessment::healthy();
        }
    }
    // Suspend flag ≈ spec.suspend (batch/v1 beta+ feature).
    if spec_of(body)
        .and_then(|s| s.get("suspend"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        return HealthAssessment::suspended("job suspended");
    }
    HealthAssessment::progressing("job running")
}

fn assess_pod(body: &Value) -> HealthAssessment {
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("pod pending");
    };
    let phase = status.get("phase").and_then(Value::as_str).unwrap_or("");
    match phase {
        "Succeeded" => HealthAssessment::healthy(),
        "Failed" => HealthAssessment::degraded("pod failed"),
        "Pending" => HealthAssessment::progressing("pod pending"),
        "Running" => {
            // Ready only when every container reports ready.
            let statuses = status.get("containerStatuses").and_then(Value::as_sequence);
            let all_ready = statuses
                .map(|seq| {
                    !seq.is_empty()
                        && seq
                            .iter()
                            .all(|c| c.get("ready").and_then(Value::as_bool) == Some(true))
                })
                .unwrap_or(false);
            if all_ready {
                HealthAssessment::healthy()
            } else {
                HealthAssessment::progressing("containers not all ready")
            }
        }
        other => HealthAssessment::unknown(&format!("pod phase {other}")),
    }
}

fn assess_service(body: &Value) -> HealthAssessment {
    let is_lb = spec_of(body)
        .and_then(|s| s.get("type"))
        .and_then(Value::as_str)
        == Some("LoadBalancer");
    if !is_lb {
        return HealthAssessment::healthy();
    }
    let has_ingress = status_of(body)
        .and_then(|s| s.get("loadBalancer"))
        .and_then(|l| l.get("ingress"))
        .and_then(Value::as_sequence)
        .map(|seq| !seq.is_empty())
        .unwrap_or(false);
    if has_ingress {
        HealthAssessment::healthy()
    } else {
        HealthAssessment::progressing("awaiting load balancer ingress")
    }
}

fn assess_ingress(body: &Value) -> HealthAssessment {
    let has_ingress = status_of(body)
        .and_then(|s| s.get("loadBalancer"))
        .and_then(|l| l.get("ingress"))
        .and_then(Value::as_sequence)
        .map(|seq| !seq.is_empty())
        .unwrap_or(false);
    if has_ingress {
        HealthAssessment::healthy()
    } else {
        HealthAssessment::progressing("awaiting load balancer ingress")
    }
}

fn assess_pvc(body: &Value) -> HealthAssessment {
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("pvc not yet bound");
    };
    match status.get("phase").and_then(Value::as_str).unwrap_or("") {
        "Bound" => HealthAssessment::healthy(),
        "Pending" => HealthAssessment::progressing("pvc pending"),
        "Lost" => HealthAssessment::degraded("pvc lost"),
        other => HealthAssessment::unknown(&format!("pvc phase {other}")),
    }
}

fn assess_crd(body: &Value) -> HealthAssessment {
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("crd not yet established");
    };
    if let Some(cond) = find_condition(status, "NamesAccepted") {
        if condition_status(cond) == Some("False") {
            return HealthAssessment::degraded("crd names not accepted");
        }
    }
    let established = find_condition(status, "Established")
        .map(|c| condition_status(c) == Some("True"))
        .unwrap_or(false);
    if established {
        HealthAssessment::healthy()
    } else {
        HealthAssessment::progressing("crd not yet established")
    }
}

fn assess_hpa(body: &Value) -> HealthAssessment {
    let Some(status) = status_of(body) else {
        return HealthAssessment::progressing("hpa not yet reported");
    };
    if let Some(cond) = find_condition(status, "AbleToScale") {
        if condition_status(cond) == Some("False") {
            return HealthAssessment::degraded(
                condition_reason(cond).unwrap_or("hpa unable to scale"),
            );
        }
    }
    if let Some(cond) = find_condition(status, "ScalingActive") {
        match condition_status(cond) {
            Some("True") => return HealthAssessment::healthy(),
            Some("False") => {
                return HealthAssessment::degraded(
                    condition_reason(cond).unwrap_or("hpa scaling inactive"),
                );
            }
            _ => {}
        }
    }
    HealthAssessment::progressing("hpa initialising")
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchrotron_plugins::manifest::parse_stream;

    fn parse(yaml: &str) -> Manifest {
        parse_stream("test", yaml).expect("parse").pop().unwrap()
    }

    #[test]
    fn deployment_healthy_when_replicas_match() {
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  generation: 1\nspec:\n  replicas: 3\nstatus:\n  observedGeneration: 1\n  replicas: 3\n  updatedReplicas: 3\n  availableReplicas: 3\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn deployment_progressing_when_rollout_in_flight() {
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  generation: 2\nspec:\n  replicas: 3\nstatus:\n  observedGeneration: 2\n  replicas: 3\n  updatedReplicas: 1\n  availableReplicas: 1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn deployment_progressing_when_generation_not_observed() {
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  generation: 5\nspec:\n  replicas: 1\nstatus:\n  observedGeneration: 3\n  replicas: 1\n  updatedReplicas: 1\n  availableReplicas: 1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn deployment_degraded_on_progress_deadline() {
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  generation: 1\nspec:\n  replicas: 1\nstatus:\n  observedGeneration: 1\n  conditions:\n    - type: Progressing\n      status: \"False\"\n      reason: ProgressDeadlineExceeded\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Degraded);
    }

    #[test]
    fn deployment_suspended_when_paused() {
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  paused: true\n  replicas: 1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Suspended);
    }

    #[test]
    fn statefulset_healthy_when_ready_matches_and_revisions_match() {
        let m = parse(
            "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: db\n  generation: 1\nspec:\n  replicas: 2\nstatus:\n  observedGeneration: 1\n  readyReplicas: 2\n  currentRevision: rev-1\n  updateRevision: rev-1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn statefulset_progressing_when_rolling_to_new_revision() {
        let m = parse(
            "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: db\n  generation: 2\nspec:\n  replicas: 2\nstatus:\n  observedGeneration: 2\n  readyReplicas: 2\n  currentRevision: rev-1\n  updateRevision: rev-2\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn daemonset_healthy_when_all_available() {
        let m = parse(
            "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: node-agent\n  generation: 1\nstatus:\n  observedGeneration: 1\n  desiredNumberScheduled: 3\n  updatedNumberScheduled: 3\n  numberAvailable: 3\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn daemonset_progressing_when_not_all_available() {
        let m = parse(
            "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: node-agent\n  generation: 1\nstatus:\n  observedGeneration: 1\n  desiredNumberScheduled: 3\n  updatedNumberScheduled: 3\n  numberAvailable: 1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn job_healthy_on_complete_true() {
        let m = parse(
            "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: migrate\nstatus:\n  conditions:\n    - type: Complete\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn job_degraded_on_failed_true() {
        let m = parse(
            "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: migrate\nstatus:\n  conditions:\n    - type: Failed\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Degraded);
    }

    #[test]
    fn job_suspended_when_spec_suspend_true() {
        let m = parse(
            "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: migrate\nspec:\n  suspend: true\nstatus: {}\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Suspended);
    }

    #[test]
    fn pod_healthy_when_running_and_all_ready() {
        let m = parse(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nstatus:\n  phase: Running\n  containerStatuses:\n    - name: app\n      ready: true\n    - name: side\n      ready: true\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn pod_progressing_when_running_but_not_ready() {
        let m = parse(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nstatus:\n  phase: Running\n  containerStatuses:\n    - name: app\n      ready: false\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn pod_healthy_when_succeeded() {
        let m =
            parse("apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nstatus:\n  phase: Succeeded\n");
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn pod_degraded_when_failed() {
        let m =
            parse("apiVersion: v1\nkind: Pod\nmetadata:\n  name: p\nstatus:\n  phase: Failed\n");
        assert_eq!(assess(&m).status, HealthStatusCode::Degraded);
    }

    #[test]
    fn service_clusterip_always_healthy() {
        let m = parse(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: svc\nspec:\n  type: ClusterIP\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn service_loadbalancer_progressing_until_ingress_present() {
        let m = parse(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: svc\nspec:\n  type: LoadBalancer\nstatus:\n  loadBalancer: {}\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn service_loadbalancer_healthy_once_ingress_present() {
        let m = parse(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: svc\nspec:\n  type: LoadBalancer\nstatus:\n  loadBalancer:\n    ingress:\n      - ip: 10.0.0.1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn ingress_healthy_when_lb_address_present() {
        let m = parse(
            "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: ing\nstatus:\n  loadBalancer:\n    ingress:\n      - hostname: example.com\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn ingress_progressing_when_lb_empty() {
        let m = parse(
            "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata:\n  name: ing\nstatus:\n  loadBalancer: {}\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn pvc_bound_is_healthy() {
        let m = parse(
            "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: pvc\nstatus:\n  phase: Bound\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn pvc_pending_is_progressing() {
        let m = parse(
            "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: pvc\nstatus:\n  phase: Pending\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn pvc_lost_is_degraded() {
        let m = parse(
            "apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: pvc\nstatus:\n  phase: Lost\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Degraded);
    }

    #[test]
    fn crd_healthy_when_established() {
        let m = parse(
            "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata:\n  name: foos.example.com\nstatus:\n  conditions:\n    - type: Established\n      status: \"True\"\n    - type: NamesAccepted\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn crd_degraded_when_names_rejected() {
        let m = parse(
            "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata:\n  name: foos.example.com\nstatus:\n  conditions:\n    - type: NamesAccepted\n      status: \"False\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Degraded);
    }

    #[test]
    fn crd_progressing_when_not_established() {
        let m = parse(
            "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata:\n  name: foos.example.com\nstatus: {}\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn hpa_healthy_when_scaling_active() {
        let m = parse(
            "apiVersion: autoscaling/v2\nkind: HorizontalPodAutoscaler\nmetadata:\n  name: hpa\nstatus:\n  conditions:\n    - type: AbleToScale\n      status: \"True\"\n    - type: ScalingActive\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn hpa_degraded_when_unable_to_scale() {
        let m = parse(
            "apiVersion: autoscaling/v2\nkind: HorizontalPodAutoscaler\nmetadata:\n  name: hpa\nstatus:\n  conditions:\n    - type: AbleToScale\n      status: \"False\"\n      reason: FailedGetScale\n",
        );
        let r = assess(&m);
        assert_eq!(r.status, HealthStatusCode::Degraded);
    }

    #[test]
    fn hpa_progressing_when_no_conditions_yet() {
        let m = parse(
            "apiVersion: autoscaling/v2\nkind: HorizontalPodAutoscaler\nmetadata:\n  name: hpa\nstatus: {}\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn unknown_kind_with_no_conditions_returns_unknown() {
        let m =
            parse("apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w\nstatus: {}\n");
        assert_eq!(assess(&m).status, HealthStatusCode::Unknown);
    }

    #[test]
    fn tier2_unknown_kind_with_ready_true_is_healthy() {
        let m = parse(
            "apiVersion: example.com/v1\nkind: Foo\nmetadata:\n  name: f\nstatus:\n  conditions:\n    - type: Ready\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn tier2_unknown_kind_with_ready_false_is_degraded() {
        let m = parse(
            "apiVersion: example.com/v1\nkind: Foo\nmetadata:\n  name: f\nstatus:\n  conditions:\n    - type: Ready\n      status: \"False\"\n      reason: BackendDown\n      message: upstream unreachable\n",
        );
        let r = assess(&m);
        assert_eq!(r.status, HealthStatusCode::Degraded);
        assert_eq!(r.message.as_deref(), Some("BackendDown"));
    }

    #[test]
    fn tier2_ready_unknown_status_is_progressing() {
        let m = parse(
            "apiVersion: example.com/v1\nkind: Foo\nmetadata:\n  name: f\nstatus:\n  conditions:\n    - type: Ready\n      status: Unknown\n      reason: Initializing\n",
        );
        let r = assess(&m);
        assert_eq!(r.status, HealthStatusCode::Progressing);
        assert_eq!(r.message.as_deref(), Some("Initializing"));
    }

    #[test]
    fn tier2_falls_back_to_available_when_no_ready() {
        let m = parse(
            "apiVersion: example.com/v1\nkind: Bar\nmetadata:\n  name: b\nstatus:\n  conditions:\n    - type: Available\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Healthy);
    }

    #[test]
    fn tier2_prefers_ready_over_available() {
        // Ready=False should win even though Available=True is also
        // present. Ready is the stronger signal on kinds that expose
        // both.
        let m = parse(
            "apiVersion: example.com/v1\nkind: Bar\nmetadata:\n  name: b\nstatus:\n  conditions:\n    - type: Available\n      status: \"True\"\n    - type: Ready\n      status: \"False\"\n      reason: Stalled\n",
        );
        let r = assess(&m);
        assert_eq!(r.status, HealthStatusCode::Degraded);
        assert_eq!(r.message.as_deref(), Some("Stalled"));
    }

    #[test]
    fn tier2_ignores_unrelated_condition_types() {
        let m = parse(
            "apiVersion: example.com/v1\nkind: Foo\nmetadata:\n  name: f\nstatus:\n  conditions:\n    - type: Reconciling\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Unknown);
    }

    #[test]
    fn tier1_wins_over_tier2_for_known_kinds() {
        // A Deployment whose real status says "not rolled out" must
        // follow tier-1 (Progressing), not get shortcut to Healthy
        // because some condition list happens to include Ready=True.
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  generation: 1\nspec:\n  replicas: 3\nstatus:\n  observedGeneration: 1\n  replicas: 1\n  updatedReplicas: 1\n  availableReplicas: 1\n  conditions:\n    - type: Ready\n      status: \"True\"\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }

    #[test]
    fn assess_by_conditions_returns_none_without_conditions() {
        let m = parse("apiVersion: example.com/v1\nkind: Foo\nmetadata:\n  name: f\nstatus: {}\n");
        assert!(assess_by_conditions(&m).is_none());
    }

    #[test]
    fn missing_status_reports_progressing_not_healthy() {
        // Deployment with a spec but no status yet: the controller
        // hasn't observed it. Returning Healthy here would let a
        // wave advance past a resource that isn't really up.
        let m = parse(
            "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\nspec:\n  replicas: 1\n",
        );
        assert_eq!(assess(&m).status, HealthStatusCode::Progressing);
    }
}
