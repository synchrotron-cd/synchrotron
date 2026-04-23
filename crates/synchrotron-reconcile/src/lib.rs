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

pub mod plan;
pub mod reconcile;
pub mod trigger;
pub mod worker_pool;

pub use plan::{plan, Plan, PlanEntry, PlannedAction, ResourceRef};
pub use reconcile::{
    DesiredSource, LiveSource, ReconcileError, ReconcileOutcome, Reconciler, SourceError,
};
pub use trigger::{AppResolver, EventTrigger};
pub use worker_pool::{
    EnqueueError, JobCtx, PoolConfig, PoolHandle, PoolStats, Trigger, WorkerPool,
};
