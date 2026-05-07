//! Microbench for the reconcile planner — the pure desired-vs-live
//! diff that decides Apply / Delete / NoOp before SSA touches the
//! API server.
//!
//! This is the per-app hot path on every reconcile. At 10k apps with
//! ~25 manifests each, the planner is invoked 10k times per loop and
//! each invocation walks ~50 manifests (desired + live). Regressions
//! on the all-noop path are the most damaging because that's the
//! steady state.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_yaml_ng::Value;
use synchrotron_plugins::{Gvk, Manifest};
use synchrotron_reconcile::plan;

fn configmap(name: &str, data_value: &str) -> Manifest {
    let yaml = format!(
        r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: default
data:
  key: {data_value}
"#
    );
    let body: Value = serde_yaml_ng::from_str(&yaml).unwrap();
    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.into(),
        namespace: Some("default".into()),
        body: body.into(),
    }
}

fn build_set(count: usize) -> Vec<Manifest> {
    (0..count).map(|i| configmap(&format!("cm-{i}"), "v1")).collect()
}

fn bench_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan");
    for n in [10usize, 100, 1000] {
        let desired = build_set(n);

        // All-noop: live == desired. Steady state for an in-sync app.
        let live_equal = desired.clone();

        // Half drift: half of live entries differ in body, so half
        // produce Apply, half NoOp.
        let mut live_half_drift = desired.clone();
        for (i, m) in live_half_drift.iter_mut().enumerate() {
            if i % 2 == 0 {
                m.body
                    .value_mut()
                    .as_mapping_mut()
                    .unwrap()
                    .insert(Value::String("data".into()), Value::String("drift".into()));
            }
        }

        // Disjoint: live and desired share no resources, so every
        // desired produces Apply (create) and every live produces
        // Delete. Worst-case work for a given input size.
        let live_disjoint: Vec<Manifest> = (0..n)
            .map(|i| configmap(&format!("orphan-{i}"), "v1"))
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("noop", n), &n, |b, _| {
            b.iter(|| black_box(plan(black_box(&desired), black_box(&live_equal))));
        });
        group.bench_with_input(BenchmarkId::new("half_drift", n), &n, |b, _| {
            b.iter(|| black_box(plan(black_box(&desired), black_box(&live_half_drift))));
        });
        group.bench_with_input(BenchmarkId::new("disjoint", n), &n, |b, _| {
            b.iter(|| black_box(plan(black_box(&desired), black_box(&live_disjoint))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_plan);
criterion_main!(benches);
