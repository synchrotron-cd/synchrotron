//! Reconciliation engine building blocks.
//!
//! This crate hosts the runtime that drives per-app reconciliation.
//! Everything else in the engine — triggers, debouncing, auto-heal,
//! sync waves — composes on top of the primitives here.
//!
//! The first slice (h48.4.1) is the [`WorkerPool`]: a bounded
//! scheduler that accepts `(app_id, trigger)` jobs and dispatches
//! them to a user-supplied async handler with two invariants:
//!
//! - **At most one in-flight reconcile per app.** Jobs for the
//!   same app queue FIFO and run one at a time; jobs for different
//!   apps run concurrently up to the configured bound.
//! - **Bounded global concurrency.** Never more than
//!   `PoolConfig::max_concurrent` handler invocations in flight at
//!   once — the reconciler never saturates kube-apiserver or plugin
//!   sidecars beyond what the operator allowed.

pub mod worker_pool;

pub use worker_pool::{EnqueueError, JobCtx, PoolConfig, PoolStats, Trigger, WorkerPool};
