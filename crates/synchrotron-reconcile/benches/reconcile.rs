//! Microbench for the per-call reconcile path.
//!
//! Sits between `plan.rs` (pure planner) and the
//! `synchrotron-bench` runtime harness. Measures:
//! source-fetch (`DesiredSource` + `LiveSource`) → `plan()` →
//! `SyncOutcome` event publish.
//!
//! This is the right level to attribute wins from source-trait
//! changes (e.g. y0v.3's `Arc<[Manifest]>` swap) and from the
//! upcoming `Manifest.body` refactor (d2p) without spinning up the
//! worker pool or tokio. Single-threaded, tight loop — matches
//! what each pool worker is doing on the hot path.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_yaml_ng::Value;
use synchrotron_core::events::EventBus;
use synchrotron_plugins::{Gvk, Manifest};
use synchrotron_reconcile::{DesiredSource, LiveSource, Reconciler, SourceError};
use synchrotron_types::{AppName, ClusterName};

struct StaticDesired {
    by_app: HashMap<AppName, Arc<[Manifest]>>,
}
impl DesiredSource for StaticDesired {
    fn desired(&self, app: &AppName) -> Result<Arc<[Manifest]>, SourceError> {
        self.by_app.get(app).cloned().ok_or(SourceError::NotFound)
    }
}

struct StaticLive {
    by_app: HashMap<AppName, Arc<[Manifest]>>,
}
impl LiveSource for StaticLive {
    fn live(&self, app: &AppName, _cluster: &ClusterName) -> Result<Arc<[Manifest]>, SourceError> {
        self.by_app.get(app).cloned().ok_or(SourceError::NotFound)
    }
}

fn configmap(name: &str, data_value: &str) -> Manifest {
    let yaml = format!(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {name}\n  namespace: default\ndata:\n  key: {data_value}\n"
    );
    let body: Value = serde_yaml_ng::from_str(&yaml).unwrap();
    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.into(),
        namespace: Some("default".into()),
        body: body.into(),
    }
}

/// Build `apps` synthetic apps, each with `manifests_per_app`
/// ConfigMaps. `live` matches `desired` exactly (steady-state
/// no-op path — what most reconciles do in production).
fn build(apps: usize, manifests_per_app: usize) -> (StaticDesired, StaticLive, Vec<AppName>) {
    let mut desired = HashMap::with_capacity(apps);
    let mut live = HashMap::with_capacity(apps);
    let mut names = Vec::with_capacity(apps);
    for a in 0..apps {
        let app = AppName(format!("app-{a:05}"));
        let manifests: Vec<Manifest> = (0..manifests_per_app)
            .map(|i| configmap(&format!("cm-{a:05}-{i:03}"), "v1"))
            .collect();
        let arc: Arc<[Manifest]> = manifests.into();
        desired.insert(app.clone(), arc.clone());
        live.insert(app.clone(), arc);
        names.push(app);
    }
    (
        StaticDesired { by_app: desired },
        StaticLive { by_app: live },
        names,
    )
}

fn bench_reconcile(c: &mut Criterion) {
    let cluster = ClusterName("cluster-0".into());

    let mut group = c.benchmark_group("reconcile_app");
    for &apps in &[1usize, 100, 1000] {
        let (desired, live, names) = build(apps, 25);
        let bus = EventBus::new(64);
        let reconciler = Reconciler::new(Arc::new(desired), Arc::new(live), bus);

        // Throughput is in reconciles, not apps — `iter` runs one
        // reconcile per iteration regardless of how many apps
        // exist. Apps just controls the lookup-set size.
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("noop_25_manifests", apps),
            &apps,
            |b, &apps| {
                let mut i: usize = 0;
                b.iter(|| {
                    let app = &names[i % apps];
                    i = i.wrapping_add(1);
                    let outcome = reconciler.reconcile_app(black_box(app), black_box(&cluster));
                    black_box(outcome);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_reconcile);
criterion_main!(benches);
