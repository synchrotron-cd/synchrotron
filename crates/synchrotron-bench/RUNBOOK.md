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

## Reproducing on another machine

Numbers will vary with CPU count and memory bandwidth — the
harness uses `concurrency=64` and is multi-threaded. To compare
across hosts, hold `concurrency` at the smaller of the two host
core counts.
