//! Kind-based topological ordering within a sync wave.
//!
//! Sync-waves group resources at coarse granularity (see
//! [`crate::wave`]). Within a wave, the order entries are applied
//! still matters: a Deployment that mounts a ConfigMap should not be
//! created before that ConfigMap exists; an HPA that targets a
//! Deployment depends on the Deployment's existence; a workload that
//! runs as a custom ServiceAccount needs the SA in place first.
//!
//! Rather than ask users to spread these dependencies across waves
//! (which is what Argo CD requires by default), we apply a built-in
//! priority table keyed on `kind`. Within a wave entries are sorted
//! ascending by [`apply_priority`] for applies and **descending** for
//! deletes — bring dependencies up before dependents, tear dependents
//! down before dependencies.
//!
//! The priority list mirrors the well-known "install order" used by
//! Helm and Argo CD so existing manifests behave the same. Unknown
//! kinds (CRDs from operators, future-Kubernetes resources) sort
//! between RBAC and workloads — a reasonable default that keeps
//! application-level CRs after their CRDs / RBAC but before
//! Service/Ingress.

use crate::plan::{PlanEntry, PlannedAction};

/// Priority bucket for an unknown kind. Sits between RBAC (which has
/// dedicated entries above) and workload kinds, so a custom resource
/// generally lands between its CRD/RBAC and any Service/Ingress that
/// fronts it. Lower number = applied earlier.
pub const DEFAULT_PRIORITY: u32 = 1000;

/// Apply-time priority for a kind. Ascending: lower numbers are
/// applied first. The exact numbers don't matter outside this
/// module — only the relative order does.
///
/// The mapping is intentionally hand-rolled rather than table-loaded
/// so the ordering is reviewed in code review.
pub fn apply_priority(kind: &str) -> u32 {
    match kind {
        // 1. Cluster-scoped containers and policy. Namespaces first
        //    so anything in them has a target.
        "Namespace" => 100,
        "NetworkPolicy" => 110,
        "ResourceQuota" => 120,
        "LimitRange" => 130,
        "PodSecurityPolicy" => 140,
        "PodDisruptionBudget" => 150,

        // 2. Identity & data. Service accounts and config feed
        //    workloads, so they must exist first.
        "ServiceAccount" => 200,
        "Secret" | "SecretList" => 210,
        "ConfigMap" => 220,

        // 3. Storage primitives. PVC depends on StorageClass / PV.
        "StorageClass" => 300,
        "PersistentVolume" => 310,
        "PersistentVolumeClaim" => 320,

        // 4. CRDs before any custom resource that uses them.
        "CustomResourceDefinition" => 400,

        // 5. RBAC: roles and bindings the workloads will assume.
        "ClusterRole" | "ClusterRoleList" => 500,
        "ClusterRoleBinding" | "ClusterRoleBindingList" => 510,
        "Role" | "RoleList" => 520,
        "RoleBinding" | "RoleBindingList" => 530,

        // 6. Service before workloads so DNS / cluster IP exists by
        //    the time a Pod's readiness probe needs it.
        "Service" => 1500,

        // 7. Workloads. DaemonSet first (often platform components),
        //    then user workloads, then autoscalers/jobs.
        "DaemonSet" => 1600,
        "Pod" => 1610,
        "ReplicationController" => 1620,
        "ReplicaSet" => 1630,
        "Deployment" => 1640,
        "HorizontalPodAutoscaler" => 1650,
        "StatefulSet" => 1660,
        "Job" => 1670,
        "CronJob" => 1680,

        // 8. Routing on top — last so backing Services exist.
        "Ingress" => 1800,
        "APIService" => 1810,

        _ => DEFAULT_PRIORITY,
    }
}

/// Sort `entries` in place into the right within-wave order.
///
/// Applies and NoOps go ascending by [`apply_priority`]; deletes go
/// descending. Mixed-action waves are uncommon (a planner usually
/// produces all-applies or all-deletes per identity) but the sort is
/// stable on action so when both occur we still emit the
/// dependency-friendly order: applies first ascending, deletes second
/// descending. Ties break on `(kind, namespace, name)` for
/// determinism.
pub fn sort_within_wave(entries: &mut [PlanEntry]) {
    entries.sort_by(|a, b| {
        // Action class first: applies before deletes. NoOp travels
        // with applies — they're already-satisfied apply intents.
        let ac = action_class(a.action).cmp(&action_class(b.action));
        if ac != std::cmp::Ordering::Equal {
            return ac;
        }
        let pa = apply_priority(&a.resource.gvk.kind);
        let pb = apply_priority(&b.resource.gvk.kind);
        let prio = match a.action {
            PlannedAction::Delete => pb.cmp(&pa), // descending for deletes
            _ => pa.cmp(&pb),                     // ascending otherwise
        };
        prio.then_with(|| {
            (
                a.resource.gvk.kind.as_str(),
                a.resource.namespace.as_deref(),
                a.resource.name.as_str(),
            )
                .cmp(&(
                    b.resource.gvk.kind.as_str(),
                    b.resource.namespace.as_deref(),
                    b.resource.name.as_str(),
                ))
        })
    });
}

fn action_class(a: PlannedAction) -> u8 {
    match a {
        PlannedAction::Apply | PlannedAction::NoOp => 0,
        PlannedAction::Delete => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ResourceRef;
    use synchrotron_plugins::Gvk;

    fn entry(kind: &str, name: &str, action: PlannedAction) -> PlanEntry {
        PlanEntry {
            resource: ResourceRef {
                gvk: Gvk {
                    group: String::new(),
                    version: "v1".into(),
                    kind: kind.into(),
                },
                namespace: Some("ns".into()),
                name: name.into(),
            },
            action,
        }
    }

    #[test]
    fn namespace_before_rbac_before_workload() {
        let p_ns = apply_priority("Namespace");
        let p_sa = apply_priority("ServiceAccount");
        let p_role = apply_priority("Role");
        let p_dep = apply_priority("Deployment");
        let p_ing = apply_priority("Ingress");
        assert!(p_ns < p_sa);
        assert!(p_sa < p_role);
        assert!(p_role < p_dep);
        assert!(p_dep < p_ing);
    }

    #[test]
    fn crd_before_unknown_custom_kinds() {
        assert!(apply_priority("CustomResourceDefinition") < apply_priority("MyCustomThing"));
    }

    #[test]
    fn unknown_kinds_after_rbac_before_service() {
        let p = apply_priority("VirtualService");
        assert!(p > apply_priority("RoleBinding"));
        assert!(p < apply_priority("Service"));
    }

    #[test]
    fn service_before_deployment_before_ingress() {
        assert!(apply_priority("Service") < apply_priority("Deployment"));
        assert!(apply_priority("Deployment") < apply_priority("Ingress"));
    }

    #[test]
    fn sort_within_wave_orders_applies_ascending() {
        let mut v = vec![
            entry("Ingress", "a", PlannedAction::Apply),
            entry("Deployment", "a", PlannedAction::Apply),
            entry("ConfigMap", "a", PlannedAction::Apply),
            entry("Namespace", "a", PlannedAction::Apply),
        ];
        sort_within_wave(&mut v);
        let kinds: Vec<_> = v.iter().map(|e| e.resource.gvk.kind.clone()).collect();
        assert_eq!(
            kinds,
            vec!["Namespace", "ConfigMap", "Deployment", "Ingress"]
        );
    }

    #[test]
    fn sort_within_wave_orders_deletes_descending() {
        let mut v = vec![
            entry("Namespace", "a", PlannedAction::Delete),
            entry("Ingress", "a", PlannedAction::Delete),
            entry("Deployment", "a", PlannedAction::Delete),
            entry("ConfigMap", "a", PlannedAction::Delete),
        ];
        sort_within_wave(&mut v);
        let kinds: Vec<_> = v.iter().map(|e| e.resource.gvk.kind.clone()).collect();
        assert_eq!(
            kinds,
            vec!["Ingress", "Deployment", "ConfigMap", "Namespace"]
        );
    }

    #[test]
    fn sort_within_wave_applies_before_deletes() {
        let mut v = vec![
            entry("Deployment", "old", PlannedAction::Delete),
            entry("ConfigMap", "new", PlannedAction::Apply),
        ];
        sort_within_wave(&mut v);
        // Apply class before delete class regardless of kind priority.
        assert_eq!(v[0].action, PlannedAction::Apply);
        assert_eq!(v[1].action, PlannedAction::Delete);
    }

    #[test]
    fn sort_within_wave_breaks_ties_deterministically() {
        let mut v = vec![
            entry("ConfigMap", "z", PlannedAction::Apply),
            entry("ConfigMap", "a", PlannedAction::Apply),
            entry("ConfigMap", "m", PlannedAction::Apply),
        ];
        sort_within_wave(&mut v);
        let names: Vec<_> = v.iter().map(|e| e.resource.name.clone()).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn sort_within_wave_keeps_noop_with_applies() {
        let mut v = vec![
            entry("Deployment", "a", PlannedAction::Delete),
            entry("Namespace", "a", PlannedAction::NoOp),
        ];
        sort_within_wave(&mut v);
        assert_eq!(v[0].action, PlannedAction::NoOp);
        assert_eq!(v[1].action, PlannedAction::Delete);
    }
}
