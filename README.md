# Synchrotron

[![ci](https://github.com/synchrotron-cd/synchrotron/actions/workflows/ci.yml/badge.svg)](https://github.com/synchrotron-cd/synchrotron/actions/workflows/ci.yml)
[![bench](https://github.com/synchrotron-cd/synchrotron/actions/workflows/bench.yml/badge.svg)](https://github.com/synchrotron-cd/synchrotron/actions/workflows/bench.yml)
[![helm](https://github.com/synchrotron-cd/synchrotron/actions/workflows/helm.yml/badge.svg)](https://github.com/synchrotron-cd/synchrotron/actions/workflows/helm.yml)

A lightweight, performance-tuned GitOps continuous-deployment
controller for Kubernetes. Designed from first principles as a
faster, smaller alternative to Argo CD — pure Rust, statically
linked musl binaries, opinionated about scale.

> **Status**: pre-1.0, internal development. APIs and on-disk
> formats may change.

## What's measured

End-to-end load harness lives in
[`crates/synchrotron-bench`](crates/synchrotron-bench); see its
[RUNBOOK](crates/synchrotron-bench/RUNBOOK.md) for methodology and
caveats. Steady-state numbers from a single workstation:

| signal                                  | result            |
|-----------------------------------------|-------------------|
| reconcile p99 (10k apps, no-op steady)  | **53 µs**         |
| webhook→sync p95 (10k apps fan-out)     | **598 ms** (target <5 s) |
| RSS at 10k apps                         | **~920 MB**       |
| 50-cluster fairness (5 slow + 45 fast)  | no starvation; fast-cluster p50 unchanged |

CI runs the bench harness on every PR and blocks merges on the
memory and webhook-latency budget guards.

## Quickstart

```bash
# Build everything
just build

# Run the test suite (offline; kind-gated tests self-skip)
just test

# Lint + format check (matches CI)
just lint
just fmt-check

# Run a perf scenario
just bench smoke           # ~1s smoke
just bench 10k-apps        # ~30s steady-state baseline
just bench webhook-burst   # webhook→sync p95
just bench clusters-50     # multi-cluster fairness
```

The full set of recipes lives in [`justfile`](justfile).

## Layout

The codebase is a Cargo workspace; each crate has a focused
purpose:

| crate                              | role |
|------------------------------------|------|
| `synchrotron-server`               | controller daemon (HTTP API + reconcile loop) |
| `synchrotron-cli`                  | `synchrotron` operator CLI |
| `synchrotron-reconcile`            | per-app reconcile, planner, worker pool, triggers |
| `synchrotron-kube`                 | server-side apply, dry-run, prune, OIDC |
| `synchrotron-plugins`              | manifest type, plugin registry, app cache |
| `synchrotron-helm-plugin`          | Helm rendering plugin |
| `synchrotron-kustomize-plugin`     | Kustomize rendering plugin |
| `synchrotron-diff`                 | structural manifest diff (smart-diff API) |
| `synchrotron-health`               | health assessment (tier-1 typed + tier-2 conditions) |
| `synchrotron-notifier`             | outbound notifications |
| `synchrotron-git`                  | git fetch + auth (SSH, HTTPS, GitHub App) |
| `synchrotron-core`                 | event bus, metrics, telemetry, persistence |
| `synchrotron-types`                | shared types (`AppName`, `ClusterName`, …) |
| `synchrotron-bench`                | end-to-end load harness |

## Documentation

| doc                                              | what's in it |
|--------------------------------------------------|---|
| [`docs/quickstart.md`](docs/quickstart.md)       | deploy your first app, end-to-end (~10 min) |
| [`DESIGN.md`](DESIGN.md)                         | architecture, principles, scope |
| [`BUILD.md`](BUILD.md)                           | release-build matrix, musl + cross |
| [`AGENTS.md`](AGENTS.md) / [`CLAUDE.md`](CLAUDE.md) | conventions for AI-assisted contributors |
| [`crates/synchrotron-bench/RUNBOOK.md`](crates/synchrotron-bench/RUNBOOK.md) | bench harness, scenario schema, results history |
| [`deploy/charts/synchrotron`](deploy/charts/synchrotron) | Helm chart |

## Issue tracking

Issues live in `bd` (beads), a local issue database checked into
the repo at [`.beads/issues.jsonl`](.beads/issues.jsonl). After
cloning, `bd ready` shows current open work; see
[`AGENTS.md`](AGENTS.md) for the workflow.

## License

Apache-2.0. See individual crate `Cargo.toml` files; the workspace
license is set centrally in the root manifest.
