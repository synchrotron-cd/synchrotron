//! Live execution of sync hooks against a cluster.
//!
//! Pairs with the pure planning in
//! [`synchrotron_reconcile::hooks`]: take the per-phase hook list,
//! apply each hook resource via SSA, poll until it reports
//! Succeeded or Failed, and honor any
//! [`HookDeletePolicy`](DeletePolicy) opt-ins for cleanup.
//!
//! # Supported kinds
//!
//! - **Job** — `.status.succeeded >= 1` ⇒ Succeeded; `.status.failed
//!   >= 1` ⇒ Failed. Anything else ⇒ Running.
//! - **Pod** — `.status.phase` of `Succeeded` / `Failed` are terminal;
//!   `Pending` / `Running` keep us polling.
//!
//! Other kinds (typically misconfiguration — a Service annotated
//! `PreSync` doesn't make sense) return [`HookError::UnsupportedKind`]
//! before we waste time polling indefinitely.
//!
//! # BeforeHookCreation
//!
//! Jobs in particular cannot be re-applied: `kubectl apply` on a Job
//! whose pod template diverges from the existing one fails because
//! the Job controller treats the spec as immutable. The
//! `BeforeHookCreation` policy deletes any prior copy *before* apply
//! so a re-run lands cleanly. If the prior copy doesn't exist, the
//! delete is a no-op.

use std::time::Duration;

use kube::api::{Api, DeleteParams, DynamicObject};
use kube::discovery::Scope;
use synchrotron_plugins::Manifest;
use thiserror::Error;
use tokio::time::{sleep, Instant};
use tracing::{debug, warn};

use crate::apply::{ApplyError, ApplyOptions, KubeSsaApplier};

/// One of the [`HookDeletePolicy`](synchrotron_reconcile::HookDeletePolicy)
/// values, restated here so this crate doesn't need to depend on
/// synchrotron-reconcile (which would create a cycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletePolicy {
    HookSucceeded,
    HookFailed,
    BeforeHookCreation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookOutcome {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone)]
pub struct HookRunOptions {
    /// Delete-policy set for this hook. Drives the BeforeHookCreation
    /// pre-delete and the post-completion cleanup.
    pub delete_policies: Vec<DeletePolicy>,
    /// Maximum time to wait for the hook to reach a terminal state.
    pub timeout: Duration,
    /// Status-poll interval. Short enough to keep latency tight on
    /// fast hooks, long enough not to hammer the API server.
    pub poll_interval: Duration,
}

impl Default for HookRunOptions {
    fn default() -> Self {
        Self {
            delete_policies: Vec::new(),
            timeout: Duration::from_secs(300),
            poll_interval: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Error)]
pub enum HookError {
    #[error("hook kind {0} is not supported (only Job and Pod)")]
    UnsupportedKind(String),
    #[error("hook is namespaced but manifest has no namespace")]
    MissingNamespace,
    #[error("hook timed out after {0:?} without reaching terminal state")]
    Timeout(Duration),
    #[error("apply failed: {0}")]
    Apply(#[from] ApplyError),
    #[error("kube error: {0}")]
    Kube(String),
}

/// Run one hook end-to-end: optional pre-delete, apply, poll, cleanup.
///
/// Returns the terminal [`HookOutcome`]. A Failed hook is *not* an
/// `Err` — it's an expected outcome the caller dispatches on (PreSync
/// failure aborts the sync; SyncFail failure is just logged). A
/// Timeout or unsupported-kind misconfiguration is an `Err`.
pub async fn run_hook(
    applier: &KubeSsaApplier,
    manifest: &Manifest,
    opts: &HookRunOptions,
) -> Result<HookOutcome, HookError> {
    let kind = manifest.gvk.kind.as_str();
    if kind != "Job" && kind != "Pod" {
        return Err(HookError::UnsupportedKind(kind.to_string()));
    }

    let api = api_for_manifest(applier, manifest).await?;

    if opts
        .delete_policies
        .contains(&DeletePolicy::BeforeHookCreation)
    {
        delete_if_exists(&api, &manifest.name).await?;
        // Wait for the prior object to be gone before re-applying —
        // Job creation collides on its name otherwise.
        wait_for_absent(&api, &manifest.name, opts).await?;
    }

    applier
        .apply(manifest, ApplyOptions::default())
        .await
        .map_err(HookError::Apply)?;

    let outcome = poll_status(&api, &manifest.name, kind, opts).await?;

    let cleanup = match outcome {
        HookOutcome::Succeeded => DeletePolicy::HookSucceeded,
        HookOutcome::Failed => DeletePolicy::HookFailed,
    };
    if opts.delete_policies.contains(&cleanup) {
        debug!(
            kind = %manifest.gvk.kind,
            name = %manifest.name,
            ?cleanup,
            "applying hook delete policy"
        );
        if let Err(e) = delete_if_exists(&api, &manifest.name).await {
            // Cleanup failure is best-effort: the hook outcome still
            // stands. Surface it as a warning so an operator can
            // garbage-collect manually if needed.
            warn!(
                kind = %manifest.gvk.kind,
                name = %manifest.name,
                error = %e,
                "hook cleanup failed; outcome unaffected"
            );
        }
    }

    Ok(outcome)
}

async fn api_for_manifest(
    applier: &KubeSsaApplier,
    manifest: &Manifest,
) -> Result<Api<DynamicObject>, HookError> {
    let (resource, caps) = applier
        .discover(&manifest.gvk)
        .await
        .map_err(HookError::Apply)?;
    let api = match caps.scope {
        Scope::Namespaced => {
            let ns = manifest
                .namespace
                .as_deref()
                .ok_or(HookError::MissingNamespace)?;
            Api::namespaced_with(applier.client_handle(), ns, &resource)
        }
        Scope::Cluster => Api::all_with(applier.client_handle(), &resource),
    };
    Ok(api)
}

async fn delete_if_exists(api: &Api<DynamicObject>, name: &str) -> Result<(), HookError> {
    match api.delete(name, &DeleteParams::background()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(s)) if s.code == 404 => Ok(()),
        Err(other) => Err(HookError::Kube(other.to_string())),
    }
}

async fn wait_for_absent(
    api: &Api<DynamicObject>,
    name: &str,
    opts: &HookRunOptions,
) -> Result<(), HookError> {
    let deadline = Instant::now() + opts.timeout;
    loop {
        match api.get_opt(name).await {
            Ok(None) => return Ok(()),
            Ok(Some(_)) => {}
            Err(other) => return Err(HookError::Kube(other.to_string())),
        }
        if Instant::now() >= deadline {
            return Err(HookError::Timeout(opts.timeout));
        }
        sleep(opts.poll_interval).await;
    }
}

async fn poll_status(
    api: &Api<DynamicObject>,
    name: &str,
    kind: &str,
    opts: &HookRunOptions,
) -> Result<HookOutcome, HookError> {
    let deadline = Instant::now() + opts.timeout;
    loop {
        let obj = api
            .get(name)
            .await
            .map_err(|e| HookError::Kube(e.to_string()))?;
        let status = obj.data.get("status").cloned().unwrap_or_default();
        if let Some(outcome) = classify(kind, &status) {
            return Ok(outcome);
        }
        if Instant::now() >= deadline {
            return Err(HookError::Timeout(opts.timeout));
        }
        sleep(opts.poll_interval).await;
    }
}

/// Read a terminal outcome from a kube `status` subobject for `kind`.
/// Returns `None` if the resource is still in progress.
pub fn classify(kind: &str, status: &serde_json::Value) -> Option<HookOutcome> {
    match kind {
        "Job" => {
            let succeeded = status
                .get("succeeded")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let failed = status.get("failed").and_then(|v| v.as_i64()).unwrap_or(0);
            // Look for terminal conditions too — some controllers
            // set `Complete` / `Failed` before incrementing the
            // counters.
            let conditions = status.get("conditions").and_then(|v| v.as_array());
            let condition_terminal = conditions.and_then(|arr| {
                arr.iter().find_map(|c| {
                    let t = c.get("type").and_then(|v| v.as_str())?;
                    let s = c.get("status").and_then(|v| v.as_str())?;
                    if s != "True" {
                        return None;
                    }
                    match t {
                        "Complete" => Some(HookOutcome::Succeeded),
                        "Failed" => Some(HookOutcome::Failed),
                        _ => None,
                    }
                })
            });
            if let Some(c) = condition_terminal {
                return Some(c);
            }
            if succeeded >= 1 {
                Some(HookOutcome::Succeeded)
            } else if failed >= 1 {
                Some(HookOutcome::Failed)
            } else {
                None
            }
        }
        "Pod" => match status.get("phase").and_then(|v| v.as_str()) {
            Some("Succeeded") => Some(HookOutcome::Succeeded),
            Some("Failed") => Some(HookOutcome::Failed),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classify_job_succeeded_via_counter() {
        let s = json!({"succeeded": 1});
        assert_eq!(classify("Job", &s), Some(HookOutcome::Succeeded));
    }

    #[test]
    fn classify_job_failed_via_counter() {
        let s = json!({"failed": 1});
        assert_eq!(classify("Job", &s), Some(HookOutcome::Failed));
    }

    #[test]
    fn classify_job_running_returns_none() {
        let s = json!({"active": 1});
        assert_eq!(classify("Job", &s), None);
    }

    #[test]
    fn classify_job_complete_condition() {
        let s = json!({
            "conditions": [{"type": "Complete", "status": "True"}],
        });
        assert_eq!(classify("Job", &s), Some(HookOutcome::Succeeded));
    }

    #[test]
    fn classify_job_failed_condition_takes_priority_over_zero_counter() {
        let s = json!({
            "conditions": [{"type": "Failed", "status": "True"}],
            "succeeded": 0,
            "failed": 0,
        });
        assert_eq!(classify("Job", &s), Some(HookOutcome::Failed));
    }

    #[test]
    fn classify_job_condition_with_status_false_is_ignored() {
        let s = json!({
            "conditions": [{"type": "Complete", "status": "False"}],
        });
        assert_eq!(classify("Job", &s), None);
    }

    #[test]
    fn classify_pod_phase() {
        assert_eq!(
            classify("Pod", &json!({"phase": "Succeeded"})),
            Some(HookOutcome::Succeeded)
        );
        assert_eq!(
            classify("Pod", &json!({"phase": "Failed"})),
            Some(HookOutcome::Failed)
        );
        assert_eq!(classify("Pod", &json!({"phase": "Running"})), None);
        assert_eq!(classify("Pod", &json!({"phase": "Pending"})), None);
    }

    #[test]
    fn classify_unsupported_kind_returns_none() {
        assert_eq!(classify("Service", &json!({})), None);
    }
}
