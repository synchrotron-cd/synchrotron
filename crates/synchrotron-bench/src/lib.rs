//! End-to-end load harness for Synchrotron.
//!
//! Drives the real `Reconciler` + `WorkerPool` against synthetic
//! in-memory desired/live sources, captures latency + memory
//! samples, and emits a stable JSON report.
//!
//! Sister to the criterion microbenches (`synchrotron-diff`,
//! `synchrotron-plugins`, `synchrotron-reconcile`): those measure
//! pure functions in isolation, this measures system behavior under
//! load. Required by the y0v.2–y0v.5 perf targets, which are
//! defined at the system level (10k apps, p95 latency, RSS, …).

pub mod config;
pub mod report;
pub mod runner;
pub mod sources;

pub use config::ScenarioConfig;
pub use report::Report;
pub use runner::run_scenario;
