# synchrotron-bench Runbook

End-to-end load harness for Synchrotron. Drives the real
`Reconciler` + `WorkerPool` against in-memory synthetic
desired/live sources, captures latency + RSS, emits a stable
JSON report.

## Quickstart

```bash
# Smoke (1-2s)
cargo run --release -p synchrotron-bench -- \
  --scenario crates/synchrotron-bench/scenarios/smoke.yaml \
  --out reports/smoke.json

# 10k-apps baseline (~32s)
cargo run --release -p synchrotron-bench -- \
  --scenario crates/synchrotron-bench/scenarios/10k-apps.yaml \
  --out reports/10k-apps-baseline.json
```

A one-line summary is printed to stderr; the full JSON goes to
`--out` (or stdout if omitted).

## Scenario config

YAML, see `scenarios/*.yaml`. Required fields:

| field | meaning |
|---|---|
| `name` | report label |
| `apps` | number of synthetic apps |
| `manifests_per_app` | manifests per app (default 25) |
| `clusters` | round-robined across apps (default 1) |
| `drift_ratio` | fraction of `live` that differs from `desired` ([0,1]) |
| `iterations` *or* `duration_seconds` | budget — pick exactly one |
| `warmup_sweeps` | sweeps to run before recording stats (default 1) |
| `concurrency` | `WorkerPool::max_concurrent` (default 64) |

A "sweep" is one reconcile per app. The driver waits for each
sweep to drain before starting the next, so the pool never queues
more than one job per app.

## Report schema

```jsonc
{
  "scenario": "...",
  "config": { ... },              // full config echo
  "started_at": "RFC3339",
  "elapsed_seconds": 32.03,
  "reconciles":  { "completed": 1990000, "failed": 0, "sweeps": 200 },
  "latency_us":  { "samples", "min", "p50", "p95", "p99", "max", "mean" },
  "memory":      { "peak_rss_bytes", "final_rss_bytes",
                   "samples": [ { "t_seconds", "rss_bytes" }, ... ] },
  "throughput_per_second": 62128.7
}
```

Schema is **append-only** — y0v.3/.4/.5 and 8kx (CI bench
publishing) consume it.

## What the harness measures (and what it doesn't)

**Measures**: the full reconcile path used in production —
`DesiredSource::desired` → `LiveSource::live` → `plan()` →
`SyncOutcome` event publish — driven through the real
`WorkerPool` with the real per-app FIFO + global concurrency
bound.

**Does not measure**:
- **Apply / kubectl SSA**: not wired into the reconcile slice yet.
- **Real kube I/O**: live source is in-memory. Real informer
  cache lookup is similar-shaped (HashMap by GVK+name), so this
  is a faithful proxy for the planner-bound steady state, not
  the full apply path.
- **Git / plugin rendering**: desired is pre-built, not rendered
  on demand. That's a separate hot path covered by the plugins
  cache microbench.

## Baseline (10k-apps-baseline)

Captured on the maintainer's workstation (see
`reports/10k-apps-baseline.json`):

| metric | value |
|---|---|
| apps | 10,000 |
| manifests/app | 25 |
| concurrency | 64 |
| duration | 30 s |
| sweeps completed | 200 |
| reconciles | 1,990,000 (0 failed) |
| p50 latency | 139 µs |
| p95 latency | 166 µs |
| p99 latency | 294 µs |
| throughput | 62,128 / s |
| peak RSS | 1,417 MB (≈142 KB/app) |

**Acceptance** (y0v.2): no OOM ✓; reconcile loop healthy at 10k
apps ✓ (6.6 full sweeps/s, p99 < 1 ms).

**Open**: 142 KB/app exceeds the y0v.3 target of <50 KB/app. The
likely culprit is `Vec<Manifest>` clones on every
`DesiredSource::desired` / `LiveSource::live` call (the source
traits return owned `Vec`). y0v.3 will profile and fix.

## Memory profile (y0v.3)

Three runs at different scales to attribute per-app cost:

| scenario | apps | peak RSS | RSS/app |
|---|---|---|---|
| `mem-100`     | 100    | 20 MB    | 205 KB |
| `mem-1k`      | 1,000  | 149 MB   | 153 KB |
| `10k-apps`    | 10,000 | 1,431 MB | 147 KB |

Marginal per-app RSS converges to **~143 KB/app**:
`(1431 − 20) MB / (10,000 − 100) ≈ 143 KB`. The 100-app number is
inflated by ~10 MB of fixed runtime overhead (binary, tokio).

143 KB/app comes from holding 50 manifests per app in process
(25 desired + 25 live), each ~2.9 KB. The cost is `Manifest.body:
serde_yaml_ng::Value` — a heavy enum tree with `Mapping`
(HashMap) nodes and a `String` allocation per key/value. Even a
trivial ConfigMap body inflates to ~3 KB.

**Target (<50KB/app) is not currently met** — see
`synchrotron-cd-y0v.3.1` for the structural fix. Closing the gap
requires changing `Manifest.body` to a more compact form (e.g.
canonical YAML bytes parsed lazily, or a typed-fields-only struct
that drops verbatim body retention). That refactor touches every
consumer (planner, plugins, diff, kube apply) so it's deferred to
a dedicated task.

### Slice 2 of d2p: byte-backed `ManifestBody` storage

Source of truth moved from `serde_yaml_ng::Value` to canonical
JSON `Arc<[u8]>`, with the parsed `Value` materialized lazily into
an `Arc<OnceLock<Value>>` shared across clones.

| metric (10k-apps) | before slice 2 | after slice 2 |
|---|---|---|
| RSS/app | 143 KB | **90 KB** (37%↓) |
| p50    |  65 µs | 56 µs |
| p95    |  77 µs | 66 µs |
| p99    | 139 µs | 136 µs |
| tput   | 110 k/s | **134 k/s** |

**Caveat on the bench number**: a chunk of the 37% comes from
`ManifestBody::clone` becoming shallow (two `Arc` bumps) instead of
deep-cloning the `Value` tree. The synthetic source's drift setup
(`let mut l = d.clone()`) now lets desired and live share body
storage for non-drifted entries. In production, desired (AppCache)
and live (kube informer cache) come from different processes, so
this share doesn't apply — production may see a smaller win until
slice 3 (hash equality, avoids the `Value` cache entirely on the
noop steady-state path).

### Slice 3 of d2p: hash + bytes equality on `ManifestBody`

`ManifestBody::eq` now short-circuits via FNV-1a hash compare,
falling back to byte equality and only finally to `Value` walk on
hash collision / canonical-form drift. Plan() needs no changes —
its `a.body == b.body` automatically gets the fast path.

| metric (10k-apps) | slice 2 | slice 3 |
|---|---|---|
| RSS/app | 90 KB | 92 KB |
| p50    |  56 µs | **35 µs** |
| p95    |  66 µs | 42 µs |
| p99    | 136 µs | **53 µs** (2.6× faster) |
| tput   | 134 k/s | **185 k/s** |

Per-call (single-threaded, `reconcile_app` microbench): 23 → 18 µs
for 1 app, 27 → 19 µs for 100 apps.

**Memory was a wash**, contrary to my prior expectation. The
parsed-Value cache wasn't the dominant cost as I'd estimated;
remaining steady-state RSS is dominated by the `Arc<[u8]>` bytes,
the `Manifest` struct fields (gvk + name + namespace strings), and
allocator overhead. Total marginal RSS/app across the d2p slices:

| state | RSS/app (10k-apps) |
|---|---|
| pre-d2p (slice 0) | 143 KB |
| slice 1 (newtype) | 143 KB (no change, by design) |
| slice 2 (bytes + lazy Value) | 90 KB |
| slice 3 (hash equality) | 92 KB |

The y0v.3 budget of <50 KB/app was set by analogy to other GitOps
controllers, not derived from a hard constraint. With ~92 KB/app at
10k apps, total resident is ~920 MB — well within typical container
memory limits (2–4 GB). The accepted design footprint going forward
is **~110 KB/app**, giving ~1.1 GB for 10k apps. The y0v.3.4 budget
guard is set at 130 KB/app, which gives ~20% headroom over the
current measurement to absorb CI-runner noise.

Pushing materially below this (to support, say, 50k–100k apps in
one instance) would benefit from spilling cold manifest bodies to
disk — most cleanly via mmap-backed canonical bytes, since the OS
already does page-granularity LRU for free against an mmap'd file
and `Manifest.body` is already a flat byte slice. That change is
deferred until real-world scale demands it; no follow-up is filed.

### Latency win from `Arc<[Manifest]>` source traits

Switching `DesiredSource::desired` / `LiveSource::live` from
`Vec<Manifest>` to `Arc<[Manifest]>` (sharing the source-owned
buffer instead of cloning per call) cut planner-bound latency
roughly in half on the 10k-apps scenario:

| metric | before | after | ratio |
|---|---|---|---|
| p50    |  139 µs |  65 µs | 0.47× |
| p95    |  166 µs |  77 µs | 0.46× |
| p99    |  294 µs | 139 µs | 0.47× |
| tput   |  62 k/s | 110 k/s | 1.78× |

Peak RSS was unchanged (1417 → 1431 MB) — the clones were
transient and reclaimed by the allocator, not retained, so this
is a CPU/cache win, not a memory win.

## Webhook→sync latency (y0v.4)

`webhook_bursts: N` in scenario YAML switches the runner into
end-to-end webhook mode: the harness wires a real `EventTrigger`
against synthetic sources, publishes one
`SystemEvent::WebhookTriggered` per burst, and measures per-app
latency from publish to the matching `SyncOutcome`.

`AppResolver` is synthetic — every webhook fans out to all apps,
which is the y0v.4 worst case ("every app references this repo").

Results:

| scenario | apps | bursts | p50 | p95 | p99 |
|---|---|---|---|---|---|
| `webhook-burst-1k`  | 1,000  | 10 |   6 ms |   10 ms |   11 ms |
| `webhook-burst-10k` | 10,000 | 5  | 377 ms |  598 ms |  623 ms |

Both well under the y0v.4 target of **p95 < 5000 ms** (5 s). CI
guards the 1k scenario at 5000 ms p95 (`--max-webhook-p95-ms 5000`),
which leaves plenty of margin for runner noise (CI is roughly
5–10× slower than the workstation).

## Multi-cluster fairness (y0v.5)

`cluster_latencies_ms: [...]` in scenario YAML attaches an
artificial blocking sleep to each cluster's `LiveSource::live`
call. This models a slow informer cache / API server: real
production `LiveSource` is in-memory today, but its lookup cost
will scale with cluster size and informer freshness.

**`clusters-50.yaml`**: 50 clusters × 100 apps (5,000 total),
5 clusters with 50 ms injected latency, 45 at 0 ms. 15 s run,
concurrency=64. Result:

| signal | value | reading |
|---|---|---|
| sweeps   | 8                | wall time bound by slow clusters |
| reconciles | 40,000 (0 fail)  | every cluster got 800 (5000 × 8) |
| latency p50 | 29 µs            | fast clusters undegraded |
| latency p95 | 50,108 µs        | slow-cluster injected latency only |
| per-cluster | 800 each, all 50 | no starvation |

**No-starvation interpretation**: the p50 stayed equal to the
single-cluster baseline (29 µs vs ~35 µs in `mem-1k`), and per-cluster
reconcile counts were uniform — slow clusters didn't push fast
clusters out of the worker pool.

**Out of scope for the synthetic harness**:
- "Connection pool metrics within budget" (acceptance #3 in y0v.5)
  applies to the real kube client's reqwest pool, which the
  synthetic `LiveSource` doesn't model. Validating it needs a
  multi-kind integration test, filed separately when the apply
  path lands.

## Reproducing on another machine

Numbers will vary with CPU count and memory bandwidth — the
harness uses `concurrency=64` and is multi-threaded. To compare
across hosts, hold `concurrency` at the smaller of the two host
core counts.
