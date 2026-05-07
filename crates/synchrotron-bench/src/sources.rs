//! Synthetic in-memory `DesiredSource` / `LiveSource`.
//!
//! Generates `apps × manifests_per_app` ConfigMaps up front and
//! serves them by `AppName`. Drift is baked in at construction:
//! the first `floor(M * drift_ratio)` manifests of each app's
//! `live` view have a tweaked body, the rest are identical to
//! desired. That gives a stable mix of Apply / NoOp plan entries
//! without per-call work.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_yaml_ng::Value;
use synchrotron_plugins::{Gvk, Manifest};
use synchrotron_reconcile::{DesiredSource, LiveSource, SourceError};
use synchrotron_types::{AppName, ClusterName};

pub struct SyntheticState {
    desired: HashMap<AppName, Arc<[Manifest]>>,
    live: HashMap<AppName, Arc<[Manifest]>>,
    pub app_names: Vec<AppName>,
    pub cluster_names: Vec<ClusterName>,
    /// Per-cluster artificial latency, indexed by cluster name. Used
    /// by `SyntheticLive` to model slow-informer behavior; empty map
    /// = uniform 0 ms.
    cluster_latency: HashMap<ClusterName, Duration>,
}

impl SyntheticState {
    pub fn build(apps: usize, manifests_per_app: usize, clusters: usize, drift_ratio: f64) -> Self {
        Self::build_with_latencies(apps, manifests_per_app, clusters, drift_ratio, None)
    }

    pub fn build_with_latencies(
        apps: usize,
        manifests_per_app: usize,
        clusters: usize,
        drift_ratio: f64,
        cluster_latencies_ms: Option<&[u64]>,
    ) -> Self {
        let drift_count = ((manifests_per_app as f64) * drift_ratio).floor() as usize;

        let mut desired = HashMap::with_capacity(apps);
        let mut live = HashMap::with_capacity(apps);
        let mut app_names = Vec::with_capacity(apps);

        for a in 0..apps {
            let app = AppName(format!("app-{a:06}"));
            let d: Vec<Manifest> = (0..manifests_per_app)
                .map(|i| configmap(&format!("cm-{a:06}-{i:03}"), "v1"))
                .collect();
            let mut l = d.clone();
            for entry in l.iter_mut().take(drift_count) {
                let mut v = entry.body.value().clone();
                if let Some(map) = v.as_mapping_mut() {
                    map.insert(Value::String("data".into()), Value::String("drift".into()));
                }
                entry.body = v.into();
            }
            desired.insert(app.clone(), Arc::from(d));
            live.insert(app.clone(), Arc::from(l));
            app_names.push(app);
        }

        let cluster_names: Vec<ClusterName> = (0..clusters)
            .map(|c| ClusterName(format!("cluster-{c:03}")))
            .collect();

        let cluster_latency: HashMap<ClusterName, Duration> = match cluster_latencies_ms {
            Some(lats) => cluster_names
                .iter()
                .zip(lats.iter())
                .filter(|(_, &ms)| ms > 0)
                .map(|(name, &ms)| (name.clone(), Duration::from_millis(ms)))
                .collect(),
            None => HashMap::new(),
        };

        Self {
            desired,
            live,
            app_names,
            cluster_names,
            cluster_latency,
        }
    }

    pub fn cluster_latency(&self, cluster: &ClusterName) -> Option<Duration> {
        self.cluster_latency.get(cluster).copied()
    }
}

fn configmap(name: &str, data_value: &str) -> Manifest {
    // Build the body directly (no YAML parse) — at 10k×25 manifests
    // construction time matters.
    let mut data = serde_yaml_ng::Mapping::new();
    data.insert(
        Value::String("key".into()),
        Value::String(data_value.into()),
    );

    let mut metadata = serde_yaml_ng::Mapping::new();
    metadata.insert(Value::String("name".into()), Value::String(name.into()));
    metadata.insert(
        Value::String("namespace".into()),
        Value::String("default".into()),
    );

    let mut body = serde_yaml_ng::Mapping::new();
    body.insert(
        Value::String("apiVersion".into()),
        Value::String("v1".into()),
    );
    body.insert(
        Value::String("kind".into()),
        Value::String("ConfigMap".into()),
    );
    body.insert(Value::String("metadata".into()), Value::Mapping(metadata));
    body.insert(Value::String("data".into()), Value::Mapping(data));

    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.into(),
        namespace: Some("default".into()),
        body: Value::Mapping(body).into(),
    }
}

/// `DesiredSource` view over a `SyntheticState`.
pub struct SyntheticDesired(pub Arc<SyntheticState>);

impl DesiredSource for SyntheticDesired {
    fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
        self.0
            .desired
            .get(app)
            .cloned()
            .ok_or(SourceError::NotFound)
    }
}

/// `LiveSource` view over a `SyntheticState`. App data is global in
/// this harness, but per-cluster artificial latency is honored —
/// the configured sleep blocks the calling thread, modeling a slow
/// informer cache / kube-API round-trip. This is a synchronous
/// blocking sleep on purpose: the production `LiveSource` reads an
/// in-memory informer cache today, but its lookup cost will scale
/// with cluster size; injecting blocking time here lets the y0v.5
/// fairness scenario stress the worker pool the way real I/O would.
pub struct SyntheticLive(pub Arc<SyntheticState>);

impl LiveSource for SyntheticLive {
    fn live(&self, app: &AppName, cluster: &ClusterName) -> Result<Arc<[Manifest]>, SourceError> {
        if let Some(d) = self.0.cluster_latency(cluster) {
            std::thread::sleep(d);
        }
        self.0.live.get(app).cloned().ok_or(SourceError::NotFound)
    }
}
