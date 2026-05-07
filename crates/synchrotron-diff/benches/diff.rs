//! Microbench for `synchrotron_diff::compare::diff`.
//!
//! Reconcile spends meaningful CPU on the structural compare —
//! especially with large container envs and big config maps — and
//! every reconcile loop hits this path once per managed resource.
//! The bench fixes representative shapes so regressions show up as
//! delta on the same input rather than being masked by random
//! manifest variation.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_yaml_ng::Value;
use synchrotron_diff::{diff, ListMapKeys};
use synchrotron_plugins::Manifest;

fn deployment(name: &str, replicas: u32, env_count: usize) -> Manifest {
    let env: Vec<Value> = (0..env_count)
        .map(|i| {
            serde_yaml_ng::from_str::<Value>(&format!(
                "name: VAR_{i}\nvalue: \"value-{i}\"\n"
            ))
            .unwrap()
        })
        .collect();
    let env_yaml = serde_yaml_ng::to_string(&env).unwrap();
    let yaml = format!(
        r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: default
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
    spec:
      containers:
      - name: app
        image: nginx:1.27.0
        ports:
        - name: http
          containerPort: 8080
          protocol: TCP
        env:
{env_indented}
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
"#,
        env_indented = indent(&env_yaml, 8),
    );
    let body: Value = serde_yaml_ng::from_str(&yaml).unwrap();
    Manifest {
        gvk: synchrotron_plugins::Gvk::parse("apps/v1", "Deployment"),
        name: name.into(),
        namespace: Some("default".into()),
        body: body.into(),
    }
}

fn indent(s: &str, n: usize) -> String {
    let pad = " ".repeat(n);
    s.lines().map(|l| format!("{pad}{l}")).collect::<Vec<_>>().join("\n")
}

fn mutate_replicas(m: &mut Manifest, new: u32) {
    let v = m.body.value_mut().as_mapping_mut().unwrap();
    let spec = v.get_mut("spec").unwrap().as_mapping_mut().unwrap();
    spec.insert(Value::String("replicas".into()), Value::Number(new.into()));
}

fn mutate_one_env_value(m: &mut Manifest, idx: usize) {
    let spec = m
        .body
        .value_mut()
        .as_mapping_mut()
        .unwrap()
        .get_mut("spec")
        .unwrap()
        .as_mapping_mut()
        .unwrap();
    let template = spec.get_mut("template").unwrap().as_mapping_mut().unwrap();
    let pod_spec = template.get_mut("spec").unwrap().as_mapping_mut().unwrap();
    let containers = pod_spec
        .get_mut("containers")
        .unwrap()
        .as_sequence_mut()
        .unwrap();
    let container = containers[0].as_mapping_mut().unwrap();
    let env = container.get_mut("env").unwrap().as_sequence_mut().unwrap();
    if let Some(entry) = env.get_mut(idx) {
        let map = entry.as_mapping_mut().unwrap();
        map.insert(
            Value::String("value".into()),
            Value::String("MUTATED".into()),
        );
    }
}

fn bench_diff(c: &mut Criterion) {
    let list_maps = ListMapKeys::defaults();

    let mut group = c.benchmark_group("diff/deployment");
    for env_count in [4usize, 32, 128] {
        let desired = deployment("app", 3, env_count);
        let mut live_equal = desired.clone();
        let mut live_field_change = desired.clone();
        mutate_replicas(&mut live_field_change, 5);
        let mut live_listmap_change = desired.clone();
        mutate_one_env_value(&mut live_listmap_change, env_count.saturating_sub(1));

        // No-change comparison: hot path on every reconcile of an
        // in-sync app.
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("nochange", env_count),
            &(&desired, &live_equal),
            |b, (d, l)| {
                b.iter(|| diff(black_box(d), black_box(l), black_box(&list_maps)));
            },
        );
        // Top-level scalar field change.
        group.bench_with_input(
            BenchmarkId::new("scalar_change", env_count),
            &(&desired, &live_field_change),
            |b, (d, l)| {
                b.iter(|| diff(black_box(d), black_box(l), black_box(&list_maps)));
            },
        );
        // Single env-var mutation inside the list-map-aligned env list.
        group.bench_with_input(
            BenchmarkId::new("listmap_change", env_count),
            &(&desired, &live_listmap_change),
            |b, (d, l)| {
                b.iter(|| diff(black_box(d), black_box(l), black_box(&list_maps)));
            },
        );

        // Silence unused warnings for the mutators when env_count is small.
        let _ = (&mut live_equal, &mut live_field_change, &mut live_listmap_change);
    }
    group.finish();
}

criterion_group!(benches, bench_diff);
criterion_main!(benches);
