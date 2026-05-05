//! Microbench for the plugin manifest cache.
//!
//! Hits dominate the steady state — most reconciles re-render the
//! same input from the same git SHA — so a regression on the hit path
//! shows up immediately in steady-state CPU. Eviction is rarer but
//! its cost scales with eviction rate, so we bench it independently
//! at a small cap to amplify the signal.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde_json::json;
use serde_yaml_ng::Value;
use synchrotron_plugins::{Cache, CacheKey, Gvk, Manifest};

fn manifest(name: &str, body_size: usize) -> Manifest {
    let payload = "x".repeat(body_size);
    let yaml = format!(
        r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: default
data:
  payload: |
    {payload}
"#
    );
    let body: Value = serde_yaml_ng::from_str(&yaml).unwrap();
    Manifest {
        gvk: Gvk::parse("v1", "ConfigMap"),
        name: name.into(),
        namespace: Some("default".into()),
        body,
    }
}

fn key(commit: &str, plugin_id: &str) -> CacheKey {
    let params = json!({"values": ["a", "b"]});
    CacheKey {
        plugin_id: plugin_id.into(),
        plugin_version: "1.0.0".into(),
        input_hash: CacheKey::hash_inputs(commit, &params, b""),
    }
}

fn bench_cache_hit(c: &mut Criterion) {
    let cache = Cache::new(1024, 64 * 1024 * 1024);
    let k = key("abc123", "helm");
    cache.put(k.clone(), vec![manifest("cm", 256)]);

    let mut group = c.benchmark_group("cache");
    group.throughput(Throughput::Elements(1));
    group.bench_function("hit_small", |b| {
        b.iter(|| {
            let v = cache.get(black_box(&k));
            black_box(v);
        });
    });
    group.finish();
}

fn bench_cache_miss(c: &mut Criterion) {
    let cache = Cache::new(1024, 64 * 1024 * 1024);
    // Populate with unrelated entries so the LRU has to walk past
    // them on every miss.
    for i in 0..512 {
        cache.put(
            key(&format!("commit-{i}"), "helm"),
            vec![manifest(&format!("cm-{i}"), 64)],
        );
    }
    let miss_key = key("never-inserted", "helm");

    let mut group = c.benchmark_group("cache");
    group.throughput(Throughput::Elements(1));
    group.bench_function("miss", |b| {
        b.iter(|| black_box(cache.get(black_box(&miss_key))));
    });
    group.finish();
}

fn bench_cache_put_and_evict(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache");
    for cap in [16usize, 256, 4096] {
        let cache = Cache::new(cap, 1024 * 1024 * 1024);
        // Pre-fill so each iteration evicts.
        for i in 0..cap {
            cache.put(
                key(&format!("seed-{i}"), "helm"),
                vec![manifest(&format!("cm-{i}"), 128)],
            );
        }
        let payload = vec![manifest("hot", 128)];
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("put_evict", cap),
            &cap,
            |b, _| {
                let mut counter = 0u64;
                b.iter(|| {
                    counter = counter.wrapping_add(1);
                    let k = key(&format!("hot-{counter}"), "helm");
                    cache.put(black_box(k), black_box(payload.clone()));
                });
            },
        );
    }
    group.finish();
}

fn bench_input_hash(c: &mut Criterion) {
    let params = json!({
        "values": {"replicas": 3, "image": {"repository": "nginx", "tag": "1.27"}},
        "secrets": ["a", "b", "c"],
    });
    c.bench_function("cache/input_hash", |b| {
        b.iter(|| {
            black_box(CacheKey::hash_inputs(
                black_box("0123456789abcdef0123456789abcdef01234567"),
                black_box(&params),
                black_box(b"chart-lock-digest"),
            ));
        });
    });
}

criterion_group!(
    benches,
    bench_cache_hit,
    bench_cache_miss,
    bench_cache_put_and_evict,
    bench_input_hash
);
criterion_main!(benches);
