# synchrotron v0.1.0

First public release. The engine is wired end-to-end: a configured
git repo's commits are polled, rendered through a plugin, planned
against the cluster's live state, and applied via SSA. CI covers
every layer (offline tests, microbenches, scenario benches,
kube integration tests, helm chart install, multi-arch container
image build, end-to-end smoke against a real kind cluster).

## What's in here

### Runtime
- **Reconcile engine** — Reconciler, WorkerPool (per-app FIFO,
  configurable concurrency), EventTrigger, debouncer, kind/wave
  ordering, hooks.
- **Plan executor** — wave-grouped apply via the kube
  ApplierAdapter, AlwaysHealthy gate placeholder (informer-backed
  health checker is a follow-up).
- **Multi-cluster** — per-cluster kube client + executor; the
  app's `destination.cluster` picks the right one at reconcile
  time.
- **Render pipeline** — git pollers per repo, bus bridge that
  republishes `PollEvent::Fetched` as `SystemEvent::RepoChanged`,
  render loop that materializes the repo + invokes the plugin +
  writes to the DesiredStore. Caching via AppCache.
- **Live-state pipeline** — per-cluster, per-GVK informers
  (ConfigMap, Secret, Service, Deployment, StatefulSet, DaemonSet,
  Job, CronJob, Ingress) feeding a single LiveStore keyed by
  `(cluster, app)`.
- **HTTP API** — apps / clusters / repos / sync / diff / watch
  / OpenAPI / metrics / probes.
- **CLI** — operator commands for apps, clusters, repos, sync,
  watch (SSE-based).

### Plugins
- **raw** — bundled. Reads `.yaml`/`.yml` files from the app's
  source path as-is.
- **helm**, **kustomize** — packaged binaries (sidecar plugins
  via the registry; the protocol is in `synchrotron-plugins`).

### Security
- **Git auth** — None / HttpBasic / SshKey / GitHubApp credentials.
  Per-repo `HostVerifier` with strict / TOFU / off modes
  (defaults to strict). Webhook signature verification for GitHub,
  GitLab, Bitbucket.
- **Secret store** — pluggable `SecretStore` trait with
  environment-backed and file-backed implementations (k8s Secret
  mount pattern). Repo credentials resolved by name at startup.
- **Threat model** — documented in
  [`docs/security/git-auth.md`](docs/security/git-auth.md).

### Observability
- **Prometheus metrics** — RED + USE catalog
  (reconcile/git/cache rates, worker pool gauges, per-cluster up).
  Optional `ServiceMonitor`.
- **Grafana dashboard** — bundled in the chart; opt-in
  ConfigMap auto-imported by the kube-prometheus-stack grafana
  sidecar.
- **Structured logging** — JSON or human-readable; trace IDs on
  reconcile spans.

### Operations
- **Static musl binaries** — amd64 + arm64 attached to each
  release with SLSA v1 build attestation
  (`.github/workflows/release.yml`).
- **Container image** — `ghcr.io/synchrotron-cd/synchrotron-server`,
  multi-arch (amd64 + arm64), distroless base, runs as nonroot
  (`.github/workflows/docker.yml`).
- **Helm chart** — `deploy/charts/synchrotron`. Includes CRDs,
  RBAC, ServiceMonitor, optional Grafana dashboard ConfigMap.

### CI matrix
- `ci.yml` — fmt + clippy + `cargo test --workspace` (offline).
- `bench.yml` — criterion microbenches + scenario harness +
  budget guards.
- `audit.yml` — `cargo-deny` (advisories, licenses, bans,
  sources) on PRs + weekly cron.
- `kind.yml` — env-gated kube integration tests against an
  ephemeral kind cluster.
- `helm.yml` — chart lint, template, install against kind.
- `docker.yml` — multi-arch container image build + push.
- `smoke.yml` — full image + chart + git → render → reconcile
  → apply assertion against kind.

## Performance budgets (workstation, 10k apps)

| metric | value |
|---|---|
| reconcile p99 (no-op steady) | **53 µs** |
| webhook → sync p95 (10k fan-out) | **598 ms** (target <5 s) |
| RSS / app | **~92 KB** (under accepted 110 KB budget) |
| 50-cluster fairness | no starvation; fast-cluster p50 unchanged under slow-cluster load |

CI's `bench.yml` enforces a 130 KB/app memory budget and a
5 000 ms webhook-p95 budget on every PR. See
[`crates/synchrotron-bench/RUNBOOK.md`](crates/synchrotron-bench/RUNBOOK.md)
for methodology and scenario definitions.

## Known limitations (v0.1)

- **Application CRDs** — defined in the chart but the controller
  doesn't watch them yet; create Applications via the HTTP API or
  the CLI. CRD-driven creation is on the roadmap.
- **Wave health gating** — uses `AlwaysHealthy` as a placeholder
  until the informer-backed health checker lands. Waves
  auto-advance the moment applies finish.
- **Render-on-create** — Apps created after the first poll need
  another `RepoChanged` event to trigger render+reconcile. Fix
  filed.
- **Secret store backends** — Env + File only in v0.1; Vault /
  AWS Secrets Manager / GCP Secret Manager are roadmap.

## Acknowledgements

- The `kube-rs` and `git2` ecosystems
- Argo CD for the conceptual primitives we borrow (sync waves,
  resource hooks, ApplicationSet shape)
