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
        if self.iterations.is_none() && self.duration_seconds.is_none() {
            anyhow::bail!("set either iterations or duration_seconds");
        }
        if self.iterations.is_some() && self.duration_seconds.is_some() {
            anyhow::bail!("set only one of iterations / duration_seconds");
        }
        if self.concurrency == 0 {
            anyhow::bail!("concurrency must be > 0");
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
        }
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
