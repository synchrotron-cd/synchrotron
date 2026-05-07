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

use serde_yaml_ng::Value;
use synchrotron_plugins::{Gvk, Manifest};
use synchrotron_reconcile::{DesiredSource, LiveSource, SourceError};
use synchrotron_types::{AppName, ClusterName};

pub struct SyntheticState {
    desired: HashMap<AppName, Arc<Vec<Manifest>>>,
    live: HashMap<AppName, Arc<Vec<Manifest>>>,
    pub app_names: Vec<AppName>,
    pub cluster_names: Vec<ClusterName>,
}

impl SyntheticState {
    pub fn build(apps: usize, manifests_per_app: usize, clusters: usize, drift_ratio: f64) -> Self {
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
                if let Some(map) = entry.body.as_mapping_mut() {
                    map.insert(
                        Value::String("data".into()),
                        Value::String("drift".into()),
                    );
                }
            }
            desired.insert(app.clone(), Arc::new(d));
            live.insert(app.clone(), Arc::new(l));
            app_names.push(app);
        }

        let cluster_names: Vec<ClusterName> = (0..clusters)
            .map(|c| ClusterName(format!("cluster-{c:03}")))
            .collect();

        Self {
            desired,
            live,
            app_names,
            cluster_names,
        }
    }
}

fn configmap(name: &str, data_value: &str) -> Manifest {
    // Build the body directly (no YAML parse) — at 10k×25 manifests
    // construction time matters.
    let mut data = serde_yaml_ng::Mapping::new();
    data.insert(Value::String("key".into()), Value::String(data_value.into()));

    let mut metadata = serde_yaml_ng::Mapping::new();
    metadata.insert(Value::String("name".into()), Value::String(name.into()));
    metadata.insert(
        Value::String("namespace".into()),
        Value::String("default".into()),
    );

    let mut body = serde_yaml_ng::Mapping::new();
    body.insert(Value::String("apiVersion".into()), Value::String("v1".into()));
    body.insert(Value::String("kind".into()), Value::String("ConfigMap".into()));
    body.insert(Value::String("metadata".into()), Value::Mapping(metadata));
    body.insert(Value::String("data".into()), Value::Mapping(data));

    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.into(),
        namespace: Some("default".into()),
        body: Value::Mapping(body),
    }
}

/// `DesiredSource` view over a `SyntheticState`.
pub struct SyntheticDesired(pub Arc<SyntheticState>);

impl DesiredSource for SyntheticDesired {
    fn desired(&self, app: &AppName) -> Result<Vec<Manifest>, SourceError> {
        self.0
            .desired
            .get(app)
            .map(|v| (**v).clone())
            .ok_or(SourceError::NotFound)
    }
}

/// `LiveSource` view over a `SyntheticState`. Cluster name is
/// ignored — apps are global in this harness.
pub struct SyntheticLive(pub Arc<SyntheticState>);

impl LiveSource for SyntheticLive {
    fn live(&self, app: &AppName, _cluster: &ClusterName) -> Result<Vec<Manifest>, SourceError> {
        self.0
            .live
            .get(app)
            .map(|v| (**v).clone())
            .ok_or(SourceError::NotFound)
    }
}
