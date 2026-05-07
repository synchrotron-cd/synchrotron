//! Sync hooks: pure annotation parsing and per-phase planning.
//!
//! Sync hooks are manifests the user wants run *around* the regular
//! sync rather than left in the live state. A typical example is a
//! `Job` that runs database migrations before the new Deployment
//! rolls out. Hooks are identified by an annotation:
//!
//! - `synchrotron.io/hook` — preferred, native key.
//! - `argocd.argoproj.io/hook` — Argo-compat fallback. We honor the
//!   exact same phase tokens Argo uses so existing manifests work.
//!
//! The annotation value is a comma-separated list of phases. A hook
//! can opt into multiple phases (e.g. `PreSync,SyncFail` for a
//! cleanup that runs both before sync and on failure).
//!
//! ## Phases
//!
//! - **PreSync** — run before the first wave applies. The reconciler
//!   blocks on hook completion before continuing.
//! - **PostSync** — run after every wave reports `Healthy`.
//! - **SyncFail** — run only if the sync errors out. Diagnostic /
//!   alerting hooks live here.
//!
//! `Sync` (Argo's default phase, manifests applied alongside the rest
//! of the wave) is intentionally *not* modeled as a hook here — it's
//! the regular flow. A manifest annotated `Sync` is treated as a
//! non-hook so we don't double-apply.
//!
//! ## Delete policies
//!
//! `synchrotron.io/hook-delete-policy` (or `argocd.argoproj.io/`)
//! takes a comma-separated list of:
//!
//! - **HookSucceeded** — delete after the hook finishes successfully.
//! - **HookFailed** — delete after the hook finishes failed.
//! - **BeforeHookCreation** — delete any prior copy before applying a
//!   new one. Argo's default if no policy is set.
//!
//! This module is pure: it classifies manifests and reports what
//! needs to happen. The live execution lives in synchrotron-kube
//! where it can talk to the API server.

use std::collections::BTreeSet;

use synchrotron_plugins::Manifest;

/// Native annotation key for hook phases.
pub const SYNCHROTRON_HOOK_ANNOTATION: &str = "synchrotron.io/hook";
/// Argo-compat fallback for hook phases.
pub const ARGOCD_HOOK_ANNOTATION: &str = "argocd.argoproj.io/hook";
/// Native annotation key for hook delete policies.
pub const SYNCHROTRON_HOOK_DELETE_POLICY_ANNOTATION: &str = "synchrotron.io/hook-delete-policy";
/// Argo-compat fallback for hook delete policies.
pub const ARGOCD_HOOK_DELETE_POLICY_ANNOTATION: &str = "argocd.argoproj.io/hook-delete-policy";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HookPhase {
    PreSync,
    PostSync,
    SyncFail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HookDeletePolicy {
    /// Delete after the hook resource finishes successfully. Pairs
    /// well with one-shot pre-sync migrations: keep the failed
    /// artifact around for debugging, clean up the success.
    HookSucceeded,
    /// Delete after the hook resource finishes failed. Useful when
    /// the failure itself is observable elsewhere (alerts, logs) and
    /// the artifact would just clutter the namespace.
    HookFailed,
    /// Delete any prior copy of the hook before applying. Required
    /// for re-runs of a `Job` since Job creation rejects re-creation
    /// of a Job with the same name.
    BeforeHookCreation,
}

/// One classified hook manifest with its phase set and delete-policy
/// set. Both are sets so a hook can run in multiple phases or have
/// multiple delete policies (e.g. `HookSucceeded,HookFailed` to
/// always clean up).
#[derive(Debug, Clone)]
pub struct Hook {
    pub manifest: Manifest,
    pub phases: BTreeSet<HookPhase>,
    pub delete_policies: BTreeSet<HookDeletePolicy>,
}

/// Result of partitioning a desired-manifest list.
///
/// `non_hook` is the slice that flows into the regular wave-based
/// apply; `hooks` is the per-phase work the executor schedules.
#[derive(Debug, Clone, Default)]
pub struct HookSplit {
    pub non_hook: Vec<Manifest>,
    pub hooks: Vec<Hook>,
}

/// Partition `manifests` into hook and non-hook lists.
///
/// Manifests with neither a recognized hook annotation nor any phase
/// tokens after parsing land in `non_hook`. Manifests annotated
/// `Sync` (Argo's default phase) are also treated as non-hook so
/// they apply through the normal wave path. Anything carrying a
/// recognized non-Sync phase set becomes a [`Hook`].
pub fn split_hooks(manifests: &[Manifest]) -> HookSplit {
    let mut split = HookSplit::default();
    for m in manifests {
        let phases = parse_hook_phases(m);
        if phases.is_empty() {
            split.non_hook.push(m.clone());
            continue;
        }
        let delete_policies = parse_hook_delete_policies(m);
        split.hooks.push(Hook {
            manifest: m.clone(),
            phases,
            delete_policies,
        });
    }
    split
}

/// Return the hooks active for a given phase, in input order.
pub fn hooks_for_phase(hooks: &[Hook], phase: HookPhase) -> Vec<&Hook> {
    hooks.iter().filter(|h| h.phases.contains(&phase)).collect()
}

/// Read the hook phases from a manifest's annotations. Returns an
/// empty set when the manifest is not a hook (no recognized
/// annotation, or only `Sync` was listed).
pub fn parse_hook_phases(manifest: &Manifest) -> BTreeSet<HookPhase> {
    let raw = annotation(manifest, SYNCHROTRON_HOOK_ANNOTATION)
        .or_else(|| annotation(manifest, ARGOCD_HOOK_ANNOTATION));
    let Some(raw) = raw else {
        return BTreeSet::new();
    };
    let mut out = BTreeSet::new();
    for token in raw.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        match token {
            "PreSync" => {
                out.insert(HookPhase::PreSync);
            }
            "PostSync" => {
                out.insert(HookPhase::PostSync);
            }
            "SyncFail" => {
                out.insert(HookPhase::SyncFail);
            }
            // `Sync` and `Skip` are recognized but do not produce a
            // hook entry: `Sync` is the default flow, `Skip` is
            // Argo's "do not apply" — we treat that as non-hook here
            // and let the planner / user remove it from the source.
            "Sync" | "Skip" => {}
            other => {
                tracing::debug!(
                    kind = %manifest.gvk.kind,
                    name = %manifest.name,
                    phase = other,
                    "ignoring unknown hook phase"
                );
            }
        }
    }
    out
}

/// Read the hook delete-policy set from a manifest's annotations.
/// Empty set means "no automatic cleanup" — the hook artifact stays.
pub fn parse_hook_delete_policies(manifest: &Manifest) -> BTreeSet<HookDeletePolicy> {
    let raw = annotation(manifest, SYNCHROTRON_HOOK_DELETE_POLICY_ANNOTATION)
        .or_else(|| annotation(manifest, ARGOCD_HOOK_DELETE_POLICY_ANNOTATION));
    let Some(raw) = raw else {
        return BTreeSet::new();
    };
    let mut out = BTreeSet::new();
    for token in raw.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        match token {
            "HookSucceeded" => {
                out.insert(HookDeletePolicy::HookSucceeded);
            }
            "HookFailed" => {
                out.insert(HookDeletePolicy::HookFailed);
            }
            "BeforeHookCreation" => {
                out.insert(HookDeletePolicy::BeforeHookCreation);
            }
            other => {
                tracing::debug!(
                    kind = %manifest.gvk.kind,
                    name = %manifest.name,
                    policy = other,
                    "ignoring unknown hook delete policy"
                );
            }
        }
    }
    out
}

fn annotation(manifest: &Manifest, key: &str) -> Option<String> {
    manifest
        .body
        .value()
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(key))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchrotron_plugins::manifest::parse_stream;

    fn m(annotations: &[(&str, &str)]) -> Manifest {
        let mut ann = String::new();
        if !annotations.is_empty() {
            ann.push_str("  annotations:\n");
            for (k, v) in annotations {
                ann.push_str(&format!("    {k}: \"{v}\"\n"));
            }
        }
        let yaml = format!(
            "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: hook\n  namespace: app\n{ann}\
             spec: {{}}\n",
        );
        parse_stream("test", &yaml).unwrap().pop().unwrap()
    }

    #[test]
    fn parse_phases_native_single() {
        let manifest = m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PreSync")]);
        let mut expected = BTreeSet::new();
        expected.insert(HookPhase::PreSync);
        assert_eq!(parse_hook_phases(&manifest), expected);
    }

    #[test]
    fn parse_phases_argo_compat() {
        let manifest = m(&[(ARGOCD_HOOK_ANNOTATION, "PostSync")]);
        let mut expected = BTreeSet::new();
        expected.insert(HookPhase::PostSync);
        assert_eq!(parse_hook_phases(&manifest), expected);
    }

    #[test]
    fn parse_phases_native_takes_priority_over_argo() {
        let manifest = m(&[
            (SYNCHROTRON_HOOK_ANNOTATION, "PreSync"),
            (ARGOCD_HOOK_ANNOTATION, "PostSync"),
        ]);
        let phases = parse_hook_phases(&manifest);
        assert!(phases.contains(&HookPhase::PreSync));
        assert!(!phases.contains(&HookPhase::PostSync));
    }

    #[test]
    fn parse_phases_multi_value() {
        let manifest = m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PreSync,SyncFail,PostSync")]);
        let phases = parse_hook_phases(&manifest);
        assert_eq!(phases.len(), 3);
        assert!(phases.contains(&HookPhase::PreSync));
        assert!(phases.contains(&HookPhase::PostSync));
        assert!(phases.contains(&HookPhase::SyncFail));
    }

    #[test]
    fn parse_phases_treats_sync_as_non_hook() {
        let manifest = m(&[(SYNCHROTRON_HOOK_ANNOTATION, "Sync")]);
        assert!(parse_hook_phases(&manifest).is_empty());
    }

    #[test]
    fn parse_phases_unknown_token_ignored() {
        let manifest = m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PreSync,Bogus")]);
        let phases = parse_hook_phases(&manifest);
        assert_eq!(phases.len(), 1);
        assert!(phases.contains(&HookPhase::PreSync));
    }

    #[test]
    fn parse_phases_no_annotation_is_empty() {
        assert!(parse_hook_phases(&m(&[])).is_empty());
    }

    #[test]
    fn parse_delete_policy_combinations() {
        let manifest = m(&[(
            SYNCHROTRON_HOOK_DELETE_POLICY_ANNOTATION,
            "HookSucceeded,BeforeHookCreation",
        )]);
        let policies = parse_hook_delete_policies(&manifest);
        assert_eq!(policies.len(), 2);
        assert!(policies.contains(&HookDeletePolicy::HookSucceeded));
        assert!(policies.contains(&HookDeletePolicy::BeforeHookCreation));
    }

    #[test]
    fn parse_delete_policy_argo_fallback() {
        let manifest = m(&[(ARGOCD_HOOK_DELETE_POLICY_ANNOTATION, "HookFailed")]);
        let policies = parse_hook_delete_policies(&manifest);
        assert!(policies.contains(&HookDeletePolicy::HookFailed));
    }

    #[test]
    fn split_hooks_partitions_correctly() {
        let plain = m(&[]);
        let pre = m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PreSync")]);
        let post = m(&[(ARGOCD_HOOK_ANNOTATION, "PostSync")]);
        let sync_only = m(&[(SYNCHROTRON_HOOK_ANNOTATION, "Sync")]);

        let split = split_hooks(&[plain.clone(), pre, post, sync_only.clone()]);
        assert_eq!(split.non_hook.len(), 2);
        assert_eq!(split.hooks.len(), 2);
        assert!(split
            .hooks
            .iter()
            .any(|h| h.phases.contains(&HookPhase::PreSync)));
        assert!(split
            .hooks
            .iter()
            .any(|h| h.phases.contains(&HookPhase::PostSync)));
    }

    #[test]
    fn hooks_for_phase_filters_by_membership() {
        let pre = Hook {
            manifest: m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PreSync")]),
            phases: [HookPhase::PreSync].into_iter().collect(),
            delete_policies: BTreeSet::new(),
        };
        let multi = Hook {
            manifest: m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PreSync,PostSync")]),
            phases: [HookPhase::PreSync, HookPhase::PostSync]
                .into_iter()
                .collect(),
            delete_policies: BTreeSet::new(),
        };
        let post = Hook {
            manifest: m(&[(SYNCHROTRON_HOOK_ANNOTATION, "PostSync")]),
            phases: [HookPhase::PostSync].into_iter().collect(),
            delete_policies: BTreeSet::new(),
        };

        let hooks = vec![pre, multi, post];
        assert_eq!(hooks_for_phase(&hooks, HookPhase::PreSync).len(), 2);
        assert_eq!(hooks_for_phase(&hooks, HookPhase::PostSync).len(), 2);
        assert_eq!(hooks_for_phase(&hooks, HookPhase::SyncFail).len(), 0);
    }
}
