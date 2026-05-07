//! YAML scenario config.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// One reproducible bench scenario.
///
/// Field defaults are tuned for a small smoke-test run; real
/// scenarios override them in YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioConfig {
    /// Human-readable label, copied into the report.
    pub name: String,

    /// Number of synthetic apps.
    pub apps: usize,

    /// Manifests per app. The microbench uses 25 as the baseline.
    #[serde(default = "default_manifests_per_app")]
    pub manifests_per_app: usize,

    /// Number of synthetic clusters. Apps round-robin across them.
    #[serde(default = "default_clusters")]
    pub clusters: usize,

    /// Fraction `[0.0, 1.0]` of manifests where `live` differs from
    /// `desired`. 0.0 = all-noop steady state; 1.0 = full drift.
    #[serde(default)]
    pub drift_ratio: f64,

    /// How many full sweeps (one reconcile per app per sweep) to
    /// measure. Mutually exclusive with `duration_seconds`.
    #[serde(default)]
    pub iterations: Option<u32>,

    /// Time-bound run: keep sweeping until elapsed >= this.
    #[serde(default)]
    pub duration_seconds: Option<u64>,

    /// Sweeps to run before recording stats. Lets caches and the
    /// allocator warm up. Defaults to 1.
    #[serde(default = "default_warmup_sweeps")]
    pub warmup_sweeps: u32,

    /// Worker pool max in-flight reconciles.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Webhook-burst mode: fire `webhook_bursts` synthetic webhook
    /// events, each fanning out to all `apps`, and measure per-app
    /// webhook→sync latency. When set, `iterations` /
    /// `duration_seconds` are ignored.
    #[serde(default)]
    pub webhook_bursts: Option<u32>,

    /// Bursts to run before recording stats. Lets the worker pool /
    /// allocator warm up. Defaults to 1.
    #[serde(default = "default_warmup_bursts")]
    pub webhook_warmup_bursts: u32,

    /// Per-cluster artificial latency in milliseconds, applied as a
    /// blocking sleep inside `SyntheticLive::live`. Models slow
    /// informer caches / kube-API round-trips. Length must equal
    /// `clusters` when set; `None` (default) means all-zero. Used by
    /// the y0v.5 fairness scenario to show that slow clusters don't
    /// starve fast ones.
    #[serde(default)]
    pub cluster_latencies_ms: Option<Vec<u64>>,
}

fn default_manifests_per_app() -> usize {
    25
}
fn default_clusters() -> usize {
    1
}
fn default_warmup_sweeps() -> u32 {
    1
}
fn default_concurrency() -> usize {
    64
}
fn default_warmup_bursts() -> u32 {
    1
}

impl ScenarioConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let cfg: ScenarioConfig = serde_yaml_ng::from_str(&text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.apps == 0 {
            anyhow::bail!("apps must be > 0");
        }
        if self.clusters == 0 {
            anyhow::bail!("clusters must be > 0");
        }
        if !(0.0..=1.0).contains(&self.drift_ratio) {
            anyhow::bail!("drift_ratio must be in [0.0, 1.0]");
        }
        let has_sweep_budget = self.iterations.is_some() || self.duration_seconds.is_some();
        let has_webhook_budget = self.webhook_bursts.is_some();
        match (has_sweep_budget, has_webhook_budget) {
            (false, false) => anyhow::bail!("set iterations, duration_seconds, or webhook_bursts"),
            (true, true) => anyhow::bail!(
                "set only one of iterations/duration_seconds (sweep) or webhook_bursts"
            ),
            _ => {}
        }
        if self.iterations.is_some() && self.duration_seconds.is_some() {
            anyhow::bail!("set only one of iterations / duration_seconds");
        }
        if self.concurrency == 0 {
            anyhow::bail!("concurrency must be > 0");
        }
        if let Some(lats) = &self.cluster_latencies_ms {
            if lats.len() != self.clusters {
                anyhow::bail!(
                    "cluster_latencies_ms length ({}) must equal clusters ({})",
                    lats.len(),
                    self.clusters
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_cfg() -> ScenarioConfig {
        ScenarioConfig {
            name: "t".into(),
            apps: 10,
            manifests_per_app: 5,
            clusters: 1,
            drift_ratio: 0.0,
            iterations: Some(1),
            duration_seconds: None,
            warmup_sweeps: 0,
            concurrency: 4,
            webhook_bursts: None,
            webhook_warmup_bursts: 1,
            cluster_latencies_ms: None,
        }
    }

    #[test]
    fn validate_rejects_cluster_latency_length_mismatch() {
        let mut c = ok_cfg();
        c.clusters = 3;
        c.cluster_latencies_ms = Some(vec![10, 20]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_webhook_mode() {
        let mut c = ok_cfg();
        c.iterations = None;
        c.webhook_bursts = Some(5);
        c.validate().unwrap();
    }

    #[test]
    fn validate_rejects_sweep_and_webhook_together() {
        let mut c = ok_cfg();
        c.webhook_bursts = Some(5);
        // iterations is also set
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_minimal() {
        ok_cfg().validate().unwrap();
    }

    #[test]
    fn validate_rejects_zero_apps() {
        let mut c = ok_cfg();
        c.apps = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_no_budget() {
        let mut c = ok_cfg();
        c.iterations = None;
        c.duration_seconds = None;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_both_budgets() {
        let mut c = ok_cfg();
        c.duration_seconds = Some(5);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_drift_out_of_range() {
        let mut c = ok_cfg();
        c.drift_ratio = 1.5;
        assert!(c.validate().is_err());
    }

    #[test]
    fn yaml_round_trip() {
        let yaml = "name: x\napps: 5\niterations: 2\n";
        let c: ScenarioConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(c.apps, 5);
        assert_eq!(c.manifests_per_app, 25);
        assert_eq!(c.clusters, 1);
        c.validate().unwrap();
    }
}
