//! Reconciliation engine building blocks.
//!
//! This crate hosts the runtime that drives per-app reconciliation.
//! Everything else in the engine — triggers, debouncing, auto-heal,
//! sync waves — composes on top of the primitives here.
//!
//! Current slices:
//!
//! - [`WorkerPool`] (h48.4.1): a bounded scheduler that accepts
//!   `(app_id, trigger)` jobs and dispatches them to a user-supplied
//!   async handler with at-most-one-in-flight-per-app and a global
//!   concurrency bound.
//! - [`Reconciler`] (h48.4.2): pure [`plan`](plan::plan) planner plus
//!   a thin wrapper that fetches desired / live state, runs the
//!   planner, and publishes a `SyncOutcome` event. The actual apply
//!   step lands in a later slice.

pub mod auto_heal;
pub mod debounce;
pub mod hooks;
pub mod kind_order;
pub mod plan;
pub mod reconcile;
pub mod trigger;
pub mod wave;
pub mod worker_pool;

pub use auto_heal::{AppLister, AutoHealConfig, AutoHealScheduler, AutoHealStats};
pub use debounce::{DebounceConfig, DebounceStats, Debouncer};
pub use hooks::{
    hooks_for_phase, parse_hook_delete_policies, parse_hook_phases, split_hooks, Hook,
    HookDeletePolicy, HookPhase, HookSplit, ARGOCD_HOOK_ANNOTATION,
    ARGOCD_HOOK_DELETE_POLICY_ANNOTATION, SYNCHROTRON_HOOK_ANNOTATION,
    SYNCHROTRON_HOOK_DELETE_POLICY_ANNOTATION,
};
pub use kind_order::{apply_priority, sort_within_wave, DEFAULT_PRIORITY};
pub use plan::{plan, Plan, PlanEntry, PlannedAction, ResourceRef};
pub use reconcile::{
    DesiredSource, LiveSource, ReconcileError, ReconcileOutcome, Reconciler, SourceError,
};
pub use trigger::{AppResolver, EventTrigger};
pub use wave::{
    execute_waves, group_into_waves, wave_of, Applier, ApplyError, HealthChecker, WaveExecConfig,
    WaveExecError, WaveExecReport, WaveGroup, WavePlan, ARGOCD_WAVE_ANNOTATION, DEFAULT_WAVE,
    SYNCHROTRON_WAVE_ANNOTATION,
};
pub use worker_pool::{
    EnqueueError, JobCtx, PoolConfig, PoolHandle, PoolStats, Trigger, WorkerPool,
};
