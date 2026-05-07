//! Stable report schema. Downstream tooling (CI bench publishing,
//! perf regression detection — y0v.2/3/4/5 + 8kx) reads this JSON,
//! so additive changes only.

use serde::{Deserialize, Serialize};

use crate::config::ScenarioConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub scenario: String,
    pub config: ScenarioConfig,
    /// ISO-8601 UTC start time.
    pub started_at: String,
    pub elapsed_seconds: f64,
    pub reconciles: ReconcileCounts,
    pub latency_us: LatencyStats,
    pub memory: MemoryStats,
    pub throughput_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileCounts {
    pub completed: u64,
    pub failed: u64,
    pub sweeps: u64,
}

/// Per-reconcile latency in microseconds. Microseconds (not nanos)
/// so JSON numbers stay readable; not millis because in-memory
/// reconciles often clock under 1 ms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    pub samples: u64,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
    pub mean: u64,
}

impl LatencyStats {
    /// `samples` is consumed (sorted in place) to compute
    /// percentiles. Empty input yields an all-zero struct.
    pub fn from_micros(mut samples: Vec<u64>) -> Self {
        if samples.is_empty() {
            return Self {
                samples: 0,
                min: 0,
                p50: 0,
                p95: 0,
                p99: 0,
                max: 0,
                mean: 0,
            };
        }
        samples.sort_unstable();
        let n = samples.len();
        let pick = |q: f64| {
            let idx = ((n as f64) * q).floor() as usize;
            samples[idx.min(n - 1)]
        };
        let sum: u128 = samples.iter().map(|&x| x as u128).sum();
        Self {
            samples: n as u64,
            min: samples[0],
            p50: pick(0.50),
            p95: pick(0.95),
            p99: pick(0.99),
            max: samples[n - 1],
            mean: (sum / n as u128) as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Peak resident set size in bytes seen across the run.
    pub peak_rss_bytes: u64,
    /// Final RSS sample.
    pub final_rss_bytes: u64,
    /// Sample series, one per second: (elapsed_seconds, rss_bytes).
    pub samples: Vec<MemSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemSample {
    pub t_seconds: f64,
    pub rss_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_latency_is_all_zero() {
        let s = LatencyStats::from_micros(vec![]);
        assert_eq!(s.samples, 0);
        assert_eq!(s.p50, 0);
        assert_eq!(s.max, 0);
    }

    #[test]
    fn percentiles_track_sorted_input() {
        // 100 values 1..=100 — p50 ≈ 50, p95 ≈ 95, p99 ≈ 99.
        let xs: Vec<u64> = (1..=100).collect();
        let s = LatencyStats::from_micros(xs);
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 100);
        assert_eq!(s.p50, 51);
        assert_eq!(s.p95, 96);
        assert_eq!(s.p99, 100);
    }

    #[test]
    fn single_sample() {
        let s = LatencyStats::from_micros(vec![42]);
        assert_eq!(s.min, 42);
        assert_eq!(s.max, 42);
        assert_eq!(s.p99, 42);
    }
}
