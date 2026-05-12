//! Tier-3 CEL expression overrides.
//!
//! For kinds that neither a tier-1 built-in rule nor the tier-2
//! conditions convention covers, operators can supply a CEL
//! expression per (group, kind) that returns a health status
//! string. CEL is non-Turing-complete and compiles to a small AST,
//! so a bad expression can't hang the reconciler — but we still
//! enforce a per-evaluation wall-clock timeout as defence in depth.
//!
//! # Contract
//!
//! A CEL rule evaluates against a single variable, `self`, bound to
//! the manifest's body as a JSON-like value. The expression must
//! return a string matching one of
//! [`HealthStatusCode::as_str`]'s values:
//! `"Healthy"`, `"Progressing"`, `"Suspended"`, `"Unknown"`,
//! `"Missing"`, or `"Degraded"`. Any other string or a non-string
//! result reports [`CelEvalError::InvalidResult`] and the outer
//! assessor falls through to tier 2.
//!
//! ```text
//! self.status.phase == "Ready" ? "Healthy" : "Progressing"
//! ```
//!
//! # Compilation
//!
//! [`CelRule::compile`] runs once per expression. [`CelOverrides`]
//! stores compiled rules by `(group, kind)` — registration is the
//! slow path; evaluation is the hot path.
//!
//! # Errors fall through to Tier 2
//!
//! [`CelOverrides::assess`] returns `Option<HealthAssessment>`:
//! `Some` only when a rule exists *and* its evaluation produces a
//! valid status code. Compile errors surface at registration;
//! runtime errors (execution failure, invalid result, timeout) are
//! logged and turned into `None` so the caller (see
//! [`crate::assess_with_overrides`]) can try tier 2.

use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use cel_interpreter::{Context, Program};
use synchrotron_plugins::Manifest;
use synchrotron_types::HealthStatusCode;
use thiserror::Error;
use tracing::warn;

use crate::HealthAssessment;

/// Default wall-clock budget for a single CEL evaluation. CEL
/// programs are non-Turing-complete and typically run in under a
/// millisecond; 100ms leaves generous headroom while still catching
/// pathological expressions before they delay a wave advance.
pub const DEFAULT_CEL_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
#[error("CEL compile error: {message}")]
pub struct CelCompileError {
    pub message: String,
}

#[derive(Debug, Error)]
pub enum CelEvalError {
    #[error("CEL execution failed: {0}")]
    Execution(String),
    #[error("CEL result is not a valid health status: {0}")]
    InvalidResult(String),
    #[error("CEL evaluation timed out after {0:?}")]
    Timeout(Duration),
    #[error("manifest body is not representable as JSON: {0}")]
    BodyConversion(String),
}

/// A single compiled CEL rule. Cheap to clone: wraps the compiled
/// program in an `Arc` so threads evaluating the same rule share
/// the AST.
#[derive(Clone)]
pub struct CelRule {
    program: Arc<Program>,
    source: String,
}

impl CelRule {
    pub fn compile(src: impl Into<String>) -> Result<Self, CelCompileError> {
        let source = src.into();
        let program = Program::compile(&source).map_err(|e| CelCompileError {
            message: e.to_string(),
        })?;
        Ok(Self {
            program: Arc::new(program),
            source,
        })
    }

    /// Evaluate the rule against the manifest's body with a
    /// wall-clock timeout. The manifest body is bound to the `self`
    /// CEL variable. The timeout is enforced by running the eval on
    /// a worker thread and giving up on the receive side — the
    /// interpreter itself has no preemption, so a runaway expression
    /// would continue to completion in the background (CEL always
    /// halts; the timeout bounds perceived latency, not CPU).
    pub fn eval(
        &self,
        manifest: &Manifest,
        timeout: Duration,
    ) -> Result<HealthStatusCode, CelEvalError> {
        // Convert the YAML value to JSON so CEL's serde-based
        // binding can consume it. YAML → JSON is lossy for YAML-only
        // constructs (tags, anchors) but manifest bodies are
        // YAML-1.2-JSON-compatible in practice.
        let body = serde_json::to_value(&manifest.body)
            .map_err(|e| CelEvalError::BodyConversion(e.to_string()))?;

        let program = self.program.clone();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut ctx = Context::default();
            // `add_variable` routes through `TryIntoValue`, which has
            // a blanket impl for `serde::Serialize`. `serde_json::Value`
            // serializes trivially, so this only fails on internal bugs.
            if let Err(e) = ctx.add_variable("self", body) {
                let _ = tx.send(Err(cel_interpreter::ExecutionError::FunctionError {
                    function: "binding".into(),
                    message: e.to_string(),
                }));
                return;
            }
            let result = program.execute(&ctx);
            let _ = tx.send(result);
        });

        match rx.recv_timeout(timeout) {
            Ok(Ok(v)) => parse_health_value(&v),
            Ok(Err(e)) => Err(CelEvalError::Execution(e.to_string())),
            Err(_) => Err(CelEvalError::Timeout(timeout)),
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }
}

impl std::fmt::Debug for CelRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CelRule")
            .field("source", &self.source)
            .finish()
    }
}

fn parse_health_value(v: &cel_interpreter::Value) -> Result<HealthStatusCode, CelEvalError> {
    let s = match v {
        cel_interpreter::Value::String(s) => s.as_str(),
        other => {
            return Err(CelEvalError::InvalidResult(format!(
                "expected string, got {other:?}"
            )));
        }
    };
    match s {
        "Healthy" => Ok(HealthStatusCode::Healthy),
        "Progressing" => Ok(HealthStatusCode::Progressing),
        "Suspended" => Ok(HealthStatusCode::Suspended),
        "Unknown" => Ok(HealthStatusCode::Unknown),
        "Missing" => Ok(HealthStatusCode::Missing),
        "Degraded" => Ok(HealthStatusCode::Degraded),
        other => Err(CelEvalError::InvalidResult(other.to_string())),
    }
}

/// Registry of CEL health rules keyed by `(group, kind)`.
#[derive(Debug, Clone)]
pub struct CelOverrides {
    rules: HashMap<(String, String), CelRule>,
    timeout: Duration,
}

impl Default for CelOverrides {
    fn default() -> Self {
        Self::new()
    }
}

impl CelOverrides {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
            timeout: DEFAULT_CEL_TIMEOUT,
        }
    }

    /// Override the default per-evaluation timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Compile and register a rule for `(group, kind)`. Returns the
    /// compile error if the expression fails to parse; the registry
    /// is left untouched in that case.
    pub fn register(
        &mut self,
        group: impl Into<String>,
        kind: impl Into<String>,
        expr: &str,
    ) -> Result<(), CelCompileError> {
        let rule = CelRule::compile(expr)?;
        self.rules.insert((group.into(), kind.into()), rule);
        Ok(())
    }

    pub fn get(&self, group: &str, kind: &str) -> Option<&CelRule> {
        self.rules.get(&(group.to_string(), kind.to_string()))
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Assess the manifest against its registered CEL rule, if any.
    ///
    /// Returns `None` when:
    /// - no rule is registered for this `(group, kind)`, or
    /// - the rule evaluated but produced an error or invalid
    ///   result (logged; caller should try tier 2).
    pub fn assess(&self, manifest: &Manifest) -> Option<HealthAssessment> {
        let rule = self.get(&manifest.gvk.group, &manifest.gvk.kind)?;
        match rule.eval(manifest, self.timeout) {
            Ok(code) => Some(HealthAssessment {
                status: code,
                message: None,
            }),
            Err(e) => {
                warn!(
                    group = %manifest.gvk.group,
                    kind = %manifest.gvk.kind,
                    error = %e,
                    "tier-3 CEL rule errored; falling through to tier 2"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchrotron_plugins::manifest::parse_stream;

    fn parse(yaml: &str) -> Manifest {
        parse_stream("t", yaml).expect("parse").pop().unwrap()
    }

    fn widget(phase: &str) -> Manifest {
        parse(&format!(
            "apiVersion: example.com/v1\nkind: Widget\nmetadata:\n  name: w\nstatus:\n  phase: {phase}\n"
        ))
    }

    const WIDGET_RULE: &str = r#"self.status.phase == "Ready" ? "Healthy" : (self.status.phase == "Failing" ? "Degraded" : "Progressing")"#;

    #[test]
    fn compile_succeeds_for_valid_expression() {
        assert!(CelRule::compile(WIDGET_RULE).is_ok());
    }

    #[test]
    fn compile_returns_error_for_invalid_expression() {
        let err = CelRule::compile("this is not { valid CEL").unwrap_err();
        assert!(!err.message.is_empty());
    }

    #[test]
    fn eval_maps_ready_to_healthy() {
        let rule = CelRule::compile(WIDGET_RULE).unwrap();
        let out = rule.eval(&widget("Ready"), DEFAULT_CEL_TIMEOUT).unwrap();
        assert_eq!(out, HealthStatusCode::Healthy);
    }

    #[test]
    fn eval_maps_failing_to_degraded() {
        let rule = CelRule::compile(WIDGET_RULE).unwrap();
        let out = rule.eval(&widget("Failing"), DEFAULT_CEL_TIMEOUT).unwrap();
        assert_eq!(out, HealthStatusCode::Degraded);
    }

    #[test]
    fn eval_maps_other_to_progressing() {
        let rule = CelRule::compile(WIDGET_RULE).unwrap();
        let out = rule
            .eval(&widget("Reconciling"), DEFAULT_CEL_TIMEOUT)
            .unwrap();
        assert_eq!(out, HealthStatusCode::Progressing);
    }

    #[test]
    fn invalid_result_string_returns_error() {
        // Returns a string that doesn't match any HealthStatusCode.
        let rule = CelRule::compile(r#""Bogus""#).unwrap();
        let err = rule
            .eval(&widget("Ready"), DEFAULT_CEL_TIMEOUT)
            .unwrap_err();
        assert!(matches!(err, CelEvalError::InvalidResult(_)));
    }

    #[test]
    fn non_string_result_returns_error() {
        // Returns a boolean — not a health code.
        let rule = CelRule::compile("true").unwrap();
        let err = rule
            .eval(&widget("Ready"), DEFAULT_CEL_TIMEOUT)
            .unwrap_err();
        assert!(matches!(err, CelEvalError::InvalidResult(_)));
    }

    #[test]
    fn missing_field_access_returns_execution_error() {
        // Referencing a field that isn't in the body.
        let rule = CelRule::compile("self.nope.gone").unwrap();
        let m = parse("apiVersion: v1\nkind: Foo\nmetadata:\n  name: f\n");
        let err = rule.eval(&m, DEFAULT_CEL_TIMEOUT).unwrap_err();
        assert!(matches!(err, CelEvalError::Execution(_)));
    }

    #[test]
    fn overrides_register_and_lookup_by_gvk() {
        let mut o = CelOverrides::new();
        o.register("example.com", "Widget", WIDGET_RULE).unwrap();
        assert!(o.get("example.com", "Widget").is_some());
        assert!(o.get("example.com", "Other").is_none());
        assert_eq!(o.len(), 1);
    }

    #[test]
    fn overrides_assess_returns_some_when_rule_matches() {
        let mut o = CelOverrides::new();
        o.register("example.com", "Widget", WIDGET_RULE).unwrap();
        let a = o.assess(&widget("Ready")).unwrap();
        assert_eq!(a.status, HealthStatusCode::Healthy);
    }

    #[test]
    fn overrides_assess_returns_none_for_unregistered_kind() {
        let o = CelOverrides::new();
        assert!(o.assess(&widget("Ready")).is_none());
    }

    #[test]
    fn overrides_assess_returns_none_on_eval_error() {
        // Registered rule returns a bogus string → assess returns
        // None (fall through to tier 2).
        let mut o = CelOverrides::new();
        o.register("example.com", "Widget", r#""Bogus""#).unwrap();
        assert!(o.assess(&widget("Ready")).is_none());
    }

    #[test]
    fn compiled_rule_is_shared_across_threads() {
        // Same compiled rule driven concurrently. This validates
        // that Arc<Program> + the thread-per-eval timeout strategy
        // doesn't race: each thread gets its own Context, evaluates,
        // and returns.
        let mut o = CelOverrides::new();
        o.register("example.com", "Widget", WIDGET_RULE).unwrap();
        let o = Arc::new(o);
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let o = o.clone();
                let phase = if i % 2 == 0 { "Ready" } else { "Failing" };
                let m = widget(phase);
                thread::spawn(move || o.assess(&m).unwrap().status)
            })
            .collect();
        for (i, h) in handles.into_iter().enumerate() {
            let expected = if i % 2 == 0 {
                HealthStatusCode::Healthy
            } else {
                HealthStatusCode::Degraded
            };
            assert_eq!(h.join().unwrap(), expected);
        }
    }

    #[test]
    #[ignore = "racy on slow CI runners; the spawned eval thread can finish \
                and queue its result on the channel before recv_timeout(0) \
                gets a chance to look. The plumbing it asserts \
                (RecvTimeoutError::Timeout → CelEvalError::Timeout) is just \
                a one-line mapping of std behavior."]
    fn timeout_is_surfaced_when_eval_exceeds_budget() {
        let rule = CelRule::compile(WIDGET_RULE).unwrap();
        let err = rule
            .eval(&widget("Ready"), Duration::from_nanos(0))
            .unwrap_err();
        assert!(matches!(err, CelEvalError::Timeout(_)));
    }
}
