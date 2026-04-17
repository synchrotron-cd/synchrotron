use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Observed reachability of a cluster's API server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum HealthState {
    /// Initial state, before the first probe completes.
    Unknown,
    /// Most recent probe succeeded.
    Up,
    /// Most recent probe failed.
    Down {
        /// Number of consecutive failed probes (resets on success).
        consecutive_failures: u32,
        /// Short description of the most recent failure.
        last_error: String,
    },
}

impl HealthState {
    pub fn is_up(&self) -> bool {
        matches!(self, HealthState::Up)
    }

    pub fn is_down(&self) -> bool {
        matches!(self, HealthState::Down { .. })
    }
}

/// Tuning knobs for [`super::HealthMonitor`]. Defaults aim for fast
/// detection without overloading a wobbly API server.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Probe interval when the cluster is considered Up.
    pub interval: Duration,
    /// Per-probe timeout.
    pub probe_timeout: Duration,
    /// After this many consecutive failures, try rebuilding the kube
    /// client from the original config (e.g. to pick up refreshed tokens).
    /// The rebuild is retried every `reconnect_after` failures thereafter.
    pub reconnect_after: u32,
    /// Initial backoff after the first failure.
    pub initial_backoff: Duration,
    /// Upper bound on the backoff between probes while Down.
    pub max_backoff: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            probe_timeout: Duration::from_secs(5),
            reconnect_after: 5,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }
}

/// Pure state machine driving the probe loop. Isolated from IO so the
/// transition and backoff logic is unit-testable without spawning tasks.
pub(crate) struct Tracker {
    pub(crate) state: HealthState,
    pub(crate) config: HealthConfig,
    pub(crate) backoff: Duration,
}

impl Tracker {
    pub(crate) fn new(config: HealthConfig) -> Self {
        Self {
            state: HealthState::Unknown,
            config,
            backoff: Duration::ZERO,
        }
    }

    pub(crate) fn record_success(&mut self) {
        self.backoff = Duration::ZERO;
        self.state = HealthState::Up;
    }

    pub(crate) fn record_failure(&mut self, error: String) {
        self.backoff = if self.backoff.is_zero() {
            self.config.initial_backoff
        } else {
            std::cmp::min(self.backoff.saturating_mul(2), self.config.max_backoff)
        };
        let prev_failures = match &self.state {
            HealthState::Down {
                consecutive_failures,
                ..
            } => *consecutive_failures,
            _ => 0,
        };
        self.state = HealthState::Down {
            consecutive_failures: prev_failures.saturating_add(1),
            last_error: error,
        };
    }

    /// Duration to sleep before the next probe. Uses the normal interval
    /// while Up, and the current exponential backoff while Down.
    pub(crate) fn sleep_duration(&self) -> Duration {
        match self.state {
            HealthState::Down { .. } => self.backoff,
            _ => self.config.interval,
        }
    }

    /// Whether the monitor should attempt to rebuild the kube client now.
    /// True every `reconnect_after` consecutive failures, so the monitor
    /// keeps retrying reconnect while the cluster stays Down.
    pub(crate) fn should_reconnect(&self) -> bool {
        match &self.state {
            HealthState::Down {
                consecutive_failures,
                ..
            } => {
                *consecutive_failures > 0
                    && self.config.reconnect_after > 0
                    && consecutive_failures % self.config.reconnect_after == 0
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_config() -> HealthConfig {
        HealthConfig {
            interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(50),
            reconnect_after: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(80),
        }
    }

    #[test]
    fn starts_unknown_with_normal_interval() {
        let t = Tracker::new(fast_config());
        assert_eq!(t.state, HealthState::Unknown);
        assert_eq!(t.sleep_duration(), Duration::from_millis(100));
        assert!(!t.should_reconnect());
    }

    #[test]
    fn success_transitions_to_up_and_clears_backoff() {
        let mut t = Tracker::new(fast_config());
        t.record_failure("x".into());
        assert!(t.backoff > Duration::ZERO);

        t.record_success();
        assert_eq!(t.state, HealthState::Up);
        assert_eq!(t.backoff, Duration::ZERO);
        assert_eq!(t.sleep_duration(), Duration::from_millis(100));
    }

    #[test]
    fn failure_exponential_backoff_with_cap() {
        let mut t = Tracker::new(fast_config());
        t.record_failure("e1".into());
        assert_eq!(t.backoff, Duration::from_millis(10));
        t.record_failure("e2".into());
        assert_eq!(t.backoff, Duration::from_millis(20));
        t.record_failure("e3".into());
        assert_eq!(t.backoff, Duration::from_millis(40));
        t.record_failure("e4".into());
        assert_eq!(t.backoff, Duration::from_millis(80));
        // Capped.
        t.record_failure("e5".into());
        assert_eq!(t.backoff, Duration::from_millis(80));
    }

    #[test]
    fn failure_counts_accumulate_and_expose_last_error() {
        let mut t = Tracker::new(fast_config());
        t.record_failure("first".into());
        t.record_failure("second".into());
        t.record_failure("third".into());
        match &t.state {
            HealthState::Down {
                consecutive_failures,
                last_error,
            } => {
                assert_eq!(*consecutive_failures, 3);
                assert_eq!(last_error, "third");
            }
            other => panic!("expected Down, got {other:?}"),
        }
    }

    #[test]
    fn reconnect_fires_every_threshold_failures() {
        let mut t = Tracker::new(fast_config()); // reconnect_after = 3
        t.record_failure("a".into());
        assert!(!t.should_reconnect());
        t.record_failure("b".into());
        assert!(!t.should_reconnect());
        t.record_failure("c".into());
        assert!(t.should_reconnect()); // 3
        t.record_failure("d".into());
        assert!(!t.should_reconnect());
        t.record_failure("e".into());
        assert!(!t.should_reconnect());
        t.record_failure("f".into());
        assert!(t.should_reconnect()); // 6
    }

    #[test]
    fn recovery_resets_failure_counter() {
        let mut t = Tracker::new(fast_config());
        t.record_failure("x".into());
        t.record_failure("y".into());
        t.record_success();
        t.record_failure("z".into());
        match &t.state {
            HealthState::Down {
                consecutive_failures,
                ..
            } => assert_eq!(*consecutive_failures, 1),
            other => panic!("expected Down, got {other:?}"),
        }
    }
}
