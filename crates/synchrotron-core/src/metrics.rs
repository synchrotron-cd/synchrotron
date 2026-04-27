//! Prometheus metrics for the reconcile control plane.
//!
//! All counters/gauges/histograms live on a single [`Metrics`]
//! instance owned by the server process. Subsystems take an
//! `Arc<Metrics>` and call typed helpers (`record_reconcile`,
//! `record_git_fetch`, …) — the helpers exist so the metric *names*
//! and *label sets* are defined in one place. Adding a new metric
//! means adding a typed helper next to the field; ad-hoc calls
//! against the field are discouraged.
//!
//! # Catalog
//!
//! | Metric | Type | Labels | Meaning |
//! |---|---|---|---|
//! | `synchrotron_reconcile_total` | counter | `app`, `cluster`, `outcome` | reconcile attempts split by `success`/`failure` |
//! | `synchrotron_reconcile_duration_seconds` | histogram | `app`, `cluster`, `outcome` | wall-clock per attempt |
//! | `synchrotron_plan_changes` | histogram | `app`, `cluster` | non-noop entries per plan |
//! | `synchrotron_worker_queue_depth` | gauge | _none_ | jobs waiting in the worker pool |
//! | `synchrotron_worker_active` | gauge | _none_ | jobs currently executing |
//! | `synchrotron_git_fetch_total` | counter | `repo`, `outcome` | git-fetch attempts |
//! | `synchrotron_git_fetch_duration_seconds` | histogram | `repo` | wall-clock per fetch |
//! | `synchrotron_cache_hit_total` | counter | `cache` | cache hits by name |
//! | `synchrotron_cache_miss_total` | counter | `cache` | cache misses by name |
//! | `synchrotron_cluster_up` | gauge | `cluster` | 1 if the cluster connector is healthy, 0 otherwise |
//!
//! These cover RED (Rate, Errors, Duration) for reconcile and git
//! fetch — the two operations that can fail visibly — and USE
//! (Utilization, Saturation, Errors) for the worker pool. Cache
//! counters round out the cache-hit-ratio diagnostic. Cluster_up
//! is a simple binary heartbeat scrapable by alerting rules.
//!
//! # Cardinality
//!
//! `app` and `cluster` are unbounded user-named labels. In practice
//! they're bounded by deployment size (tens to low thousands), and
//! Prometheus tolerates that comfortably. We do *not* label by
//! resource GVK or by individual manifest — those would explode
//! cardinality.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use prometheus_client::encoding::{text::encode, EncodeLabelSet};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct ReconcileLabels {
    pub app: String,
    pub cluster: String,
    pub outcome: ReconcileOutcomeLabel,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct PlanLabels {
    pub app: String,
    pub cluster: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct GitFetchLabels {
    pub repo: String,
    pub outcome: ReconcileOutcomeLabel,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct GitDurationLabels {
    pub repo: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct CacheLabels {
    pub cache: String,
}

#[derive(Clone, Hash, PartialEq, Eq, Debug, EncodeLabelSet)]
pub struct ClusterLabels {
    pub cluster: String,
}

/// Outcome dimension shared between reconcile and git counters.
/// Emitted as a Prometheus label value, so the spelling here is
/// the contract scrapers see.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub enum ReconcileOutcomeLabel {
    Success,
    Failure,
}

impl prometheus_client::encoding::EncodeLabelValue for ReconcileOutcomeLabel {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelValueEncoder<'_>,
    ) -> Result<(), std::fmt::Error> {
        let s = match self {
            Self::Success => "success",
            Self::Failure => "failure",
        };
        prometheus_client::encoding::EncodeLabelValue::encode(&s, encoder)
    }
}

/// All metrics for the control plane.
pub struct Metrics {
    registry: Registry,
    reconcile_total: Family<ReconcileLabels, Counter>,
    reconcile_duration: Family<ReconcileLabels, Histogram>,
    plan_changes: Family<PlanLabels, Histogram>,
    worker_queue_depth: Gauge,
    worker_active: Gauge,
    git_fetch_total: Family<GitFetchLabels, Counter>,
    git_fetch_duration: Family<GitDurationLabels, Histogram>,
    cache_hit_total: Family<CacheLabels, Counter>,
    cache_miss_total: Family<CacheLabels, Counter>,
    cluster_up: Family<ClusterLabels, Gauge>,
    /// Number of `record_reconcile` calls across all label sets —
    /// exposed for tests to assert recording happened without
    /// re-parsing the text format.
    record_count: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("synchrotron");

        // Histogram::new takes an iterator of upper-bound buckets in
        // seconds. Constructors must be `fn` pointers (no captures),
        // so we inline the bucket arrays.
        fn duration_histogram() -> Histogram {
            Histogram::new(
                [
                    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
                ]
                .into_iter(),
            )
        }
        fn plan_changes_histogram() -> Histogram {
            Histogram::new([0.0, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0].into_iter())
        }

        let reconcile_total: Family<ReconcileLabels, Counter> = Family::default();
        let reconcile_duration: Family<ReconcileLabels, Histogram> =
            Family::new_with_constructor(duration_histogram);
        let plan_changes: Family<PlanLabels, Histogram> =
            Family::new_with_constructor(plan_changes_histogram);
        let worker_queue_depth = Gauge::default();
        let worker_active = Gauge::default();
        let git_fetch_total: Family<GitFetchLabels, Counter> = Family::default();
        let git_fetch_duration: Family<GitDurationLabels, Histogram> =
            Family::new_with_constructor(duration_histogram);
        let cache_hit_total: Family<CacheLabels, Counter> = Family::default();
        let cache_miss_total: Family<CacheLabels, Counter> = Family::default();
        let cluster_up: Family<ClusterLabels, Gauge> = Family::default();

        registry.register(
            "reconcile_total",
            "Number of reconcile attempts, by outcome.",
            reconcile_total.clone(),
        );
        registry.register(
            "reconcile_duration_seconds",
            "Wall-clock time per reconcile attempt.",
            reconcile_duration.clone(),
        );
        registry.register(
            "plan_changes",
            "Non-noop entries produced by the planner.",
            plan_changes.clone(),
        );
        registry.register(
            "worker_queue_depth",
            "Reconcile jobs waiting in the worker pool.",
            worker_queue_depth.clone(),
        );
        registry.register(
            "worker_active",
            "Reconcile jobs currently executing.",
            worker_active.clone(),
        );
        registry.register(
            "git_fetch_total",
            "Git fetch attempts, by outcome.",
            git_fetch_total.clone(),
        );
        registry.register(
            "git_fetch_duration_seconds",
            "Wall-clock time per git fetch.",
            git_fetch_duration.clone(),
        );
        registry.register(
            "cache_hit_total",
            "Cache hits by name.",
            cache_hit_total.clone(),
        );
        registry.register(
            "cache_miss_total",
            "Cache misses by name.",
            cache_miss_total.clone(),
        );
        registry.register(
            "cluster_up",
            "1 if the cluster connector is healthy, 0 otherwise.",
            cluster_up.clone(),
        );

        Self {
            registry,
            reconcile_total,
            reconcile_duration,
            plan_changes,
            worker_queue_depth,
            worker_active,
            git_fetch_total,
            git_fetch_duration,
            cache_hit_total,
            cache_miss_total,
            cluster_up,
            record_count: AtomicU64::new(0),
        }
    }

    /// Render the metrics in OpenMetrics text format. Suitable for
    /// returning from a `/metrics` HTTP handler.
    pub fn render(&self) -> String {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).expect("encode is infallible into String");
        buf
    }

    pub fn record_reconcile(&self, app: &str, cluster: &str, success: bool, duration: Duration) {
        let outcome = if success {
            ReconcileOutcomeLabel::Success
        } else {
            ReconcileOutcomeLabel::Failure
        };
        let labels = ReconcileLabels {
            app: app.to_string(),
            cluster: cluster.to_string(),
            outcome,
        };
        self.reconcile_total.get_or_create(&labels).inc();
        self.reconcile_duration
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
        self.record_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_plan_changes(&self, app: &str, cluster: &str, changes: usize) {
        self.plan_changes
            .get_or_create(&PlanLabels {
                app: app.to_string(),
                cluster: cluster.to_string(),
            })
            .observe(changes as f64);
    }

    pub fn set_worker_queue_depth(&self, depth: i64) {
        self.worker_queue_depth.set(depth);
    }

    pub fn set_worker_active(&self, active: i64) {
        self.worker_active.set(active);
    }

    pub fn record_git_fetch(&self, repo: &str, success: bool, duration: Duration) {
        let outcome = if success {
            ReconcileOutcomeLabel::Success
        } else {
            ReconcileOutcomeLabel::Failure
        };
        self.git_fetch_total
            .get_or_create(&GitFetchLabels {
                repo: repo.to_string(),
                outcome,
            })
            .inc();
        self.git_fetch_duration
            .get_or_create(&GitDurationLabels {
                repo: repo.to_string(),
            })
            .observe(duration.as_secs_f64());
    }

    pub fn record_cache_hit(&self, cache: &str) {
        self.cache_hit_total
            .get_or_create(&CacheLabels {
                cache: cache.to_string(),
            })
            .inc();
    }

    pub fn record_cache_miss(&self, cache: &str) {
        self.cache_miss_total
            .get_or_create(&CacheLabels {
                cache: cache.to_string(),
            })
            .inc();
    }

    pub fn set_cluster_up(&self, cluster: &str, up: bool) {
        self.cluster_up
            .get_or_create(&ClusterLabels {
                cluster: cluster.to_string(),
            })
            .set(if up { 1 } else { 0 });
    }

    /// Test-only: count of `record_reconcile` calls, regardless of
    /// label set. Useful to assert recording happened without
    /// scraping the text format.
    pub fn reconcile_record_count(&self) -> u64 {
        self.record_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_render_lists_all_metric_names_with_help() {
        let m = Metrics::new();
        let text = m.render();
        // Each registered metric emits a `# HELP synchrotron_<name>`
        // line even before any sample is recorded.
        for expected in [
            "# HELP synchrotron_reconcile_total",
            "# HELP synchrotron_reconcile_duration_seconds",
            "# HELP synchrotron_plan_changes",
            "# HELP synchrotron_worker_queue_depth",
            "# HELP synchrotron_worker_active",
            "# HELP synchrotron_git_fetch_total",
            "# HELP synchrotron_git_fetch_duration_seconds",
            "# HELP synchrotron_cache_hit_total",
            "# HELP synchrotron_cache_miss_total",
            "# HELP synchrotron_cluster_up",
        ] {
            assert!(
                text.contains(expected),
                "missing {expected} in output:\n{text}"
            );
        }
    }

    #[test]
    fn record_reconcile_emits_counter_and_histogram_samples() {
        let m = Metrics::new();
        m.record_reconcile("billing", "prod", true, Duration::from_millis(120));
        let text = m.render();
        assert!(text.contains(
            r#"synchrotron_reconcile_total_total{app="billing",cluster="prod",outcome="success"} 1"#
        ));
        // Histogram emits `_count` and `_sum` plus per-bucket lines.
        assert!(text.contains(
            r#"synchrotron_reconcile_duration_seconds_count{app="billing",cluster="prod",outcome="success"} 1"#
        ));
    }

    #[test]
    fn failure_outcome_increments_separate_label_set() {
        let m = Metrics::new();
        m.record_reconcile("billing", "prod", true, Duration::from_millis(10));
        m.record_reconcile("billing", "prod", false, Duration::from_millis(10));
        let text = m.render();
        assert!(text.contains(r#"outcome="success"} 1"#));
        assert!(text.contains(r#"outcome="failure"} 1"#));
    }

    #[test]
    fn plan_changes_observation_renders() {
        let m = Metrics::new();
        m.record_plan_changes("billing", "prod", 7);
        let text = m.render();
        assert!(text.contains(r#"synchrotron_plan_changes_count{app="billing",cluster="prod"} 1"#));
        assert!(text.contains(r#"synchrotron_plan_changes_sum{app="billing",cluster="prod"} 7"#));
    }

    #[test]
    fn worker_gauges_round_trip() {
        let m = Metrics::new();
        m.set_worker_queue_depth(4);
        m.set_worker_active(2);
        let text = m.render();
        assert!(text.contains("synchrotron_worker_queue_depth 4"));
        assert!(text.contains("synchrotron_worker_active 2"));
    }

    #[test]
    fn git_fetch_records_under_repo_label() {
        let m = Metrics::new();
        m.record_git_fetch("git@example.com:org/repo", true, Duration::from_millis(50));
        let text = m.render();
        assert!(text.contains(r#"repo="git@example.com:org/repo""#));
    }

    #[test]
    fn cache_hit_and_miss_have_distinct_counters() {
        let m = Metrics::new();
        m.record_cache_hit("app_manifest");
        m.record_cache_hit("app_manifest");
        m.record_cache_miss("app_manifest");
        let text = m.render();
        assert!(text.contains(r#"synchrotron_cache_hit_total_total{cache="app_manifest"} 2"#));
        assert!(text.contains(r#"synchrotron_cache_miss_total_total{cache="app_manifest"} 1"#));
    }

    #[test]
    fn cluster_up_sets_gauge_to_one_or_zero() {
        let m = Metrics::new();
        m.set_cluster_up("prod-us", true);
        m.set_cluster_up("prod-eu", false);
        let text = m.render();
        assert!(text.contains(r#"synchrotron_cluster_up{cluster="prod-us"} 1"#));
        assert!(text.contains(r#"synchrotron_cluster_up{cluster="prod-eu"} 0"#));
    }

    #[test]
    fn reconcile_record_count_increments() {
        let m = Metrics::new();
        assert_eq!(m.reconcile_record_count(), 0);
        m.record_reconcile("a", "c", true, Duration::from_millis(1));
        m.record_reconcile("a", "c", false, Duration::from_millis(1));
        assert_eq!(m.reconcile_record_count(), 2);
    }

    #[test]
    fn render_ends_with_eof_marker() {
        // OpenMetrics text format requires a trailing `# EOF` line —
        // kube-prometheus-stack accepts both Prometheus and
        // OpenMetrics, but the exposition library always produces
        // OpenMetrics so the marker must be present.
        let m = Metrics::new();
        let text = m.render();
        assert!(text.trim_end().ends_with("# EOF"), "got:\n{text}");
    }
}
