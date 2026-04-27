//! Tracing setup and helpers.
//!
//! Synchrotron logs everything through `tracing`. This module
//! standardizes how the binary entrypoints (server, plugin
//! sidecars, CLI) install a subscriber, which fields each reconcile
//! attempt carries, and how callers throttle noisy debug events.
//!
//! # Format
//!
//! [`init`] picks pretty or JSON output based on
//! [`TelemetryConfig::format`]. Pretty is the default for terminals
//! (developer ergonomics); JSON is the default for non-tty outputs
//! (log aggregators that parse line-by-line). Override with the
//! `SYNCHROTRON_LOG_FORMAT` env var.
//!
//! # Filtering
//!
//! Per-module log levels come from `RUST_LOG` via `tracing_subscriber`'s
//! `EnvFilter`. The default level is `info`. Examples:
//!
//! - `RUST_LOG=info,synchrotron_reconcile=debug`
//! - `RUST_LOG=synchrotron_diff=trace`
//!
//! # Trace IDs on reconciles
//!
//! [`reconcile_span`] returns a span pre-populated with `trace_id`
//! (UUID v4), `app`, and `cluster` fields so every event emitted
//! within the reconcile attempt inherits them. Downstream layers
//! (auto-heal, plan, apply, health) just need to enter that span;
//! they don't have to thread the IDs by hand.
//!
//! # Sampled debug for noisy paths
//!
//! Some hot loops (informer event firehose, debounce ticks, plan
//! diff iteration) would drown a logger if they emitted at debug
//! every iteration. [`Sampler`] is a tiny atomic counter that
//! returns `true` once every N calls; gate the noisy `debug!` /
//! `trace!` behind it so the log volume stays bounded.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{info_span, Span};
use tracing_subscriber::{fmt, EnvFilter};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

impl LogFormat {
    /// Default chosen by the runtime: pretty if stderr is a TTY,
    /// JSON otherwise. Operators almost always want machine-parseable
    /// logs in production (where stderr is a pipe), and humans almost
    /// always want pretty when running locally.
    pub fn from_env() -> Self {
        if let Ok(v) = std::env::var("SYNCHROTRON_LOG_FORMAT") {
            return match v.to_ascii_lowercase().as_str() {
                "json" => Self::Json,
                "pretty" => Self::Pretty,
                _ => Self::auto(),
            };
        }
        Self::auto()
    }

    fn auto() -> Self {
        if std::io::stderr().is_terminal() {
            Self::Pretty
        } else {
            Self::Json
        }
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub format: LogFormat,
    /// Default `RUST_LOG`-style filter applied if the env var is
    /// unset. Use `info` unless a binary has reason to be noisier.
    pub default_filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::from_env(),
            default_filter: "info".into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("invalid log filter: {0}")]
    Filter(#[from] tracing_subscriber::filter::ParseError),
    #[error("a global tracing subscriber is already installed")]
    AlreadyInstalled,
}

/// Install the global subscriber. Call once at process start —
/// returns [`TelemetryError::AlreadyInstalled`] on the second call.
///
/// Reads `RUST_LOG` first; falls back to `cfg.default_filter`.
pub fn init(cfg: TelemetryConfig) -> Result<(), TelemetryError> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(f) => f,
        Err(_) => EnvFilter::try_new(&cfg.default_filter)?,
    };
    let result = match cfg.format {
        LogFormat::Json => fmt()
            .with_env_filter(filter)
            .json()
            .with_writer(std::io::stderr)
            .try_init(),
        LogFormat::Pretty => fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .try_init(),
    };
    result.map_err(|_| TelemetryError::AlreadyInstalled)
}

/// Span for a single reconcile attempt. Every event inside this
/// span automatically carries `trace_id`, `app`, and `cluster`.
///
/// Usage:
///
/// ```ignore
/// let _enter = telemetry::reconcile_span("billing", "prod-us").entered();
/// info!("planning");           // logged with trace_id/app/cluster
/// ```
pub fn reconcile_span(app: &str, cluster: &str) -> Span {
    info_span!(
        "reconcile",
        trace_id = %Uuid::new_v4(),
        app = %app,
        cluster = %cluster,
    )
}

/// Counts calls and reports `should_log() == true` once every
/// `every_n` invocations. Cheap (single relaxed atomic add) so it's
/// safe to call from hot loops; thread-safe so a shared counter can
/// gate events from many tasks.
///
/// `every_n == 0` and `every_n == 1` both mean "always log" — handy
/// for tests or when a config explicitly disables sampling.
#[derive(Debug)]
pub struct Sampler {
    counter: AtomicU64,
    every_n: u64,
}

impl Sampler {
    pub fn new(every_n: u64) -> Self {
        Self {
            counter: AtomicU64::new(0),
            every_n,
        }
    }

    pub fn should_log(&self) -> bool {
        if self.every_n <= 1 {
            return true;
        }
        // fetch_add returns the *previous* value; the first call
        // therefore sees 0 and logs, matching the "first event of
        // every N" intuition rather than "Nth event."
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        n % self.every_n == 0
    }

    /// Reset the counter. Useful in tests; rarely needed at runtime.
    pub fn reset(&self) {
        self.counter.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_env_override_to_json() {
        let prev = std::env::var("SYNCHROTRON_LOG_FORMAT").ok();
        std::env::set_var("SYNCHROTRON_LOG_FORMAT", "json");
        assert_eq!(LogFormat::from_env(), LogFormat::Json);
        match prev {
            Some(v) => std::env::set_var("SYNCHROTRON_LOG_FORMAT", v),
            None => std::env::remove_var("SYNCHROTRON_LOG_FORMAT"),
        }
    }

    #[test]
    fn log_format_env_override_to_pretty() {
        let prev = std::env::var("SYNCHROTRON_LOG_FORMAT").ok();
        std::env::set_var("SYNCHROTRON_LOG_FORMAT", "pretty");
        assert_eq!(LogFormat::from_env(), LogFormat::Pretty);
        match prev {
            Some(v) => std::env::set_var("SYNCHROTRON_LOG_FORMAT", v),
            None => std::env::remove_var("SYNCHROTRON_LOG_FORMAT"),
        }
    }

    #[test]
    fn reconcile_span_carries_app_and_cluster_fields() {
        // Span metadata is only populated when a subscriber is
        // active (otherwise the macro short-circuits to a disabled
        // span). Install a no-op subscriber for the duration of
        // the test so the span actually materializes.
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let span = reconcile_span("billing", "prod");
            let meta = span.metadata().expect("span has metadata");
            let names: Vec<&str> = meta.fields().iter().map(|f| f.name()).collect();
            assert_eq!(names, vec!["trace_id", "app", "cluster"]);
            assert_eq!(meta.name(), "reconcile");
        });
    }

    #[test]
    fn sampler_one_means_always() {
        let s = Sampler::new(1);
        for _ in 0..10 {
            assert!(s.should_log());
        }
    }

    #[test]
    fn sampler_zero_treated_as_one() {
        let s = Sampler::new(0);
        assert!(s.should_log());
        assert!(s.should_log());
    }

    #[test]
    fn sampler_one_in_n_pattern() {
        let s = Sampler::new(4);
        // First call (counter = 0) logs; next three are skipped;
        // call 5 (counter = 4) logs again.
        let pattern: Vec<bool> = (0..8).map(|_| s.should_log()).collect();
        assert_eq!(
            pattern,
            vec![true, false, false, false, true, false, false, false]
        );
    }

    #[test]
    fn sampler_reset_restores_first_call_logs() {
        let s = Sampler::new(3);
        let _ = s.should_log();
        let _ = s.should_log();
        s.reset();
        assert!(s.should_log());
    }

    #[test]
    fn sampler_is_thread_safe() {
        use std::sync::Arc;
        use std::thread;
        let s = Arc::new(Sampler::new(10));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = s.clone();
            handles.push(thread::spawn(move || {
                let mut hits = 0usize;
                for _ in 0..100 {
                    if s.should_log() {
                        hits += 1;
                    }
                }
                hits
            }));
        }
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // 800 calls / every 10 = 80 hits, regardless of interleaving.
        assert_eq!(total, 80);
    }
}
