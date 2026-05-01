# Synchrotron-CD Design Document

## Project Vision

Synchrotron-CD is a lightweight, fast, and scalable GitOps continuous deployment system. It serves as a modern alternative to Argo CD, designed from first principles to prioritize performance, simplicity, and resource efficiency.

## Guiding Principles

### 1. Lightweight
- Minimal dependencies and resource consumption
- Small binary size and memory footprint (pure-Rust dependencies preferred)
- Simple deployment model without complex infrastructure

### 2. Fast
- Quick GitOps reconciliation cycles
- Rapid deployment pipelines
- Efficient change detection and application

### 3. Scalable
- Handle thousands of applications across multiple clusters
- Horizontal scaling without operational complexity
- Efficient resource utilization at scale

### 4. Simple
- Straightforward operational model
- Clear configuration and deployment workflows
- Minimal cognitive overhead for operators

### 5. Smart by Default
- Intelligent drift detection that understands Kubernetes semantics
- Auto-ignore fields managed by controllers (HPA, VPA, operators)
- Sensible defaults that work out of the box, with full override capability

## Architectural Goals

- **Performance as a first-class concern**: Every design decision considers performance impact
- **Memory efficiency by design**: Target sub-512MB memory footprint for deployments with thousands of apps
- **Stateless components**: Enable horizontal scaling and easy failover
- **Event-driven + polling reconciliation**: Webhooks for speed, polling for reliability
- **Composable architecture**: Individual components can be used independently
- **Plugin extensibility**: Support multiple templating engines and deployment strategies without core modifications
- **Smart drift detection**: Don't fight Kubernetes controllers; cooperate with them

## System Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                      Synchrotron-CD System                            │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌───────────────┐  ┌────────────────┐  ┌──────────────────────────┐ │
│  │ Webhook Server│  │  Git Poller    │  │   CLI / gRPC API        │ │
│  │ (GitHub, etc.)│  │  (fallback +   │  │   (user interaction,    │ │
│  │               │  │   air-gapped)  │  │    sync triggers,       │ │
│  └──────┬────────┘  └──────┬─────────┘  │    app management)      │ │
│         │                  │            └───────────┬──────────────┘ │
│         └──────┬───────────┘                        │               │
│                ▼                                    │               │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │     Event Bus (internal channel)                                │ │
│  │     - De-duplicates triggers                                    │ │
│  │     - Coalesces rapid-fire events                               │ │
│  │     - Routes to reconciliation                                  │ │
│  └──────────────────────────┬──────────────────────────────────────┘ │
│                             │                                       │
│                             ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │     Reconciliation Engine                                       │ │
│  │  - Computes desired vs actual state                             │ │
│  │  - Smart drift detection (server-side dry-run diffing)          │ │
│  │  - Auto-ignores controller-managed fields (HPA, VPA, etc.)     │ │
│  │  - Periodic auto-heal (configurable, default 3 min)            │ │
│  └──┬────────┬──────────────────────┬──────────┬──────────────────┘ │
│     │        │                      │          │                    │
│  ┌──▼───┐ ┌──▼────┐           ┌────▼────┐  ┌──▼──────────┐        │
│  │ Git  │ │Plugin │           │Manifest │  │  Smart      │        │
│  │ Sync │ │System │           │ Cache   │  │  Differ     │        │
│  │      │ │       │           │ (LRU)   │  │             │        │
│  └──────┘ └───────┘           └─────────┘  └─────────────┘        │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │   Deployment Orchestrator                                       │ │
│  │   - Server-side apply (field ownership)                         │ │
│  │   - Sync waves & hooks                                          │ │
│  │   - Rollback support                                            │ │
│  └──────────────────────────┬──────────────────────────────────────┘ │
│                             │                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │   Cluster Connector & Auth                                      │ │
│  │   - Manages connections to K8s clusters                         │ │
│  │   - Handles auth (kubeconfig, SA tokens, OIDC)                  │ │
│  │   - Connection pooling & health checks                          │ │
│  │   - Resource watches (informers) for managed resources          │ │
│  └──────────────────────────┬──────────────────────────────────────┘ │
│                             │                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │   Health Assessment Engine                                      │ │
│  │   - Resource-type-specific health checks                        │ │
│  │   - Deployment rollout status, Pod readiness, Job completion    │ │
│  │   - Custom health check plugins                                 │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                     │
│       ▼ Kubernetes Clusters (prod, staging, etc.)                   │
│                                                                     │
│  [SQLite State Storage] ◄──► All components store/read state        │
└───────────────────────────────────────────────────────────────────────┘
```

## Application Model

Users define applications via Kubernetes CRDs (primary) or standalone config files (for bootstrap / non-k8s use).

### Application CRD

```yaml
apiVersion: synchrotron.io/v1alpha1
kind: Application
metadata:
  name: my-app
  namespace: synchrotron-system
spec:
  # Git source(s)
  source:
    repoURL: https://github.com/org/repo
    path: deploy/my-app
    targetRevision: main
    # Optional: plugin for rendering
    plugin:
      name: helm
      parameters:
        - name: image.tag
          value: v1.2.3

  # Target cluster + namespace
  destination:
    cluster: production  # or server URL
    namespace: my-app

  # Sync policy
  syncPolicy:
    automated:
      selfHeal: true          # auto-correct drift (default: true)
      prune: false             # auto-delete removed resources (default: false)
      selfHealInterval: 180s   # how often to check (default: 3m)

    # Smart drift detection (the good stuff)
    drift:
      # Server-side dry-run diffing (default: true)
      # Compares server-normalized manifests instead of raw text
      serverSideDiff: true

      # Auto-ignore fields managed by Kubernetes controllers
      # Each can be true/false, all default to true
      autoIgnore:
        hpaReplicas: true      # ignore spec.replicas when HPA targets this resource
        vpaResources: true     # ignore container resources when VPA targets this resource
        defaultedFields: true  # ignore fields set by defaulting (via server-side diff)
        mutatingWebhooks: true # ignore fields added by mutating admission webhooks

      # Manual ignore rules (like Argo CD's ignoreDifferences but simpler)
      ignore:
        - group: apps
          kind: Deployment
          jsonPointers:
            - /spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt
        - kind: Service
          managedFieldsManagers:
            - kube-controller-manager  # ignore fields owned by this manager

    # Sync waves (optional, for ordering)
    waves:
      - name: crds
        path: crds/
        order: -1
      - name: app
        path: app/
        order: 0
```

### ApplicationSet (multi-app generation)

Initial scope is deliberately minimal: two generators that cover the most common patterns. The ApplicationSet interface is designed as a **generator plugin system** — each generator is a trait implementation, so adding new generators later (pull request, SCM provider, matrix combinators, etc.) is purely additive with no changes to the core reconciliation or templating logic.

**v1 generators**:
- **Git directories**: Discover apps from directory structure in a git repo
- **Cluster selector**: Generate one app per cluster matching label selectors

**Future generators** (designed-for but not built yet):
- **Matrix/Merge**: Combine generators (e.g., every service x every cluster)
- **Pull Request**: Generate preview apps from open PRs
- **SCM Provider**: Discover repos from GitHub/GitLab organizations
- **List**: Explicit enumeration for simple cases

```yaml
apiVersion: synchrotron.io/v1alpha1
kind: ApplicationSet
metadata:
  name: microservices
spec:
  generators:
    - git:
        repoURL: https://github.com/org/deploy
        directories:
          - path: services/*
    - clusters:
        selector:
          matchLabels:
            env: production
  template:
    # Application template with {{path.basename}}, {{name}} etc.
```

## Core Components

### 1. SQLite State Storage

Foundation layer. All other components depend on this.

- **Schema**: Applications, sync history, resource state snapshots, health status, cache metadata
- **WAL mode**: Enables concurrent readers with single writer (ideal for reconciliation loop + API reads)
- **Migrations**: Embedded SQL migrations, versioned with the binary
- **Bounded size**: Periodic compaction, configurable retention for history (default 30 days)
- **Pure Rust**: Use `rusqlite` with bundled SQLite (no external C dependency beyond what's compiled in)

### 2. Cluster Connector & Auth

Foundation layer. Needed before any cluster interaction.

- **Auth methods**: kubeconfig files, in-cluster service account, OIDC token refresh, exec-based credential plugins
- **Connection pooling**: Shared HTTP/2 connections per cluster, bounded pool size
- **Health checks**: Periodic `/healthz` probes, automatic reconnection
- **Informers**: Watch streams for managed resources only (not full cluster state)
  - Track only resources Synchrotron has applied
  - Reduces API server load vs watching everything
  - Feed live state into reconciliation engine
- **Multi-cluster**: Named clusters with independent auth, can target in-cluster or remote

### 3. Git Synchronization Engine

Depends on: SQLite storage.

- **Clone strategy**: Shallow clones with depth=1 for initial sync, fetch for updates
- **Change detection**: Compare HEAD commit hash vs last-synced hash in SQLite
- **Multi-repo**: Support multiple git repos, each polled independently
- **Credential management**: SSH keys, HTTP basic auth, GitHub App tokens (via credential plugins)
- **Polling**: Configurable interval per repo (default 3 min), jittered to avoid thundering herd
- **Webhook acceleration**: Webhooks trigger immediate fetch, polling is the reliability backstop
- **Workspace management**: Bare repos cached on disk, worktrees for rendering, cleaned up after sync

### 4. Plugin System (Local + Sidecar)

Depends on: Git sync (provides source material).

- **Local plugins**: Subprocess with stdin/stdout JSON-RPC. Fast, simple. Ship built-in plugins for Helm and Kustomize.
- **Sidecar plugins**: gRPC to container sidecars for heavy/custom tooling
- **Plugin interface**: `render(source_path, parameters) -> Vec<Manifest>`
- **Discovery**: Plugins declared in Application spec, loaded on demand
- **Caching**: Plugin output cached by input hash (source content + parameters)
- **Built-in**: Raw manifests (directory of YAML files) handled natively, no plugin needed

### 5. Manifest Cache (LRU)

Depends on: Git sync, plugin system (producers of manifests).

- **Key**: `(app_name, git_commit_hash, plugin_params_hash)`
- **Value**: Parsed, rendered Kubernetes manifests (structured, not raw YAML)
- **Eviction**: LRU with configurable max memory (default: 350MB, staying under 512MB total)
- **Invalidation**: On new git commit for an app, old entry evicted
- **Warm-up**: On startup, populate cache for recently-active apps from SQLite
- **Metrics**: Hit/miss rate, eviction count, memory usage

### 6. Smart Diff Engine

Depends on: Cluster connector (for server-side dry-run and live state).

This is a critical differentiator from Argo CD. See D4 below for full design.

- **Server-side dry-run diffing**: Apply desired manifests with `--dry-run=server` to get the server-normalized version, then diff against live state. This automatically handles defaulted fields, mutating webhooks, and field normalization.
- **Auto-ignore HPA replicas**: Query HPAs, resolve `scaleTargetRef`, auto-ignore `spec.replicas` on matched Deployments/StatefulSets/ReplicaSets
- **Auto-ignore VPA resources**: Query VPAs, resolve target, auto-ignore `spec.containers[*].resources` on matched workloads
- **Managed fields awareness**: Use Kubernetes managed fields metadata to only diff fields owned by Synchrotron (via server-side apply field manager)
- **Semantic diffing**: Understand Kubernetes types - e.g., `ports: [{port: 80}]` and `ports: [{port: 80, protocol: TCP}]` are the same (TCP is the default)
- **User overrides**: `ignoreDifferences` for anything the auto-detection doesn't cover

### 7. Reconciliation Engine

Depends on: Git sync, manifest cache, smart differ, cluster connector.

- **Loop**: Event-driven (webhook/poll trigger) + periodic auto-heal (default 3 min)
- **Process per app**:
  1. Fetch desired manifests from cache (or render if cache miss)
  2. Fetch live state from informer cache (or API call if not watched)
  3. Run smart diff to compute actual drift
  4. If drift detected and selfHeal enabled, enqueue sync
  5. Update app status in SQLite
- **Concurrency**: Process multiple apps in parallel, bounded by configurable worker pool
- **Debouncing**: Coalesce rapid-fire events (e.g., multiple commits in quick succession)
- **Sync waves**: Apply resources in wave order, wait for health between waves

### 8. Deployment Orchestrator

Depends on: Reconciliation engine, cluster connector.

- **Apply strategy**: Server-side apply with Synchrotron as field manager (enables managed fields awareness)
- **Pruning**: Optionally delete resources removed from git (requires explicit opt-in)
- **Sync waves**: Apply CRDs before resources that use them, namespaces before namespaced resources
- **Hooks**: Pre-sync, post-sync, sync-fail hooks (Jobs or other resources)
- **Rollback**: Track last N successful syncs, allow rollback to previous git commit
- **Resource ordering**: Respect Kubernetes resource dependencies (Namespace -> RBAC -> Deployment -> Service)

### 9. Health Assessment Engine

Depends on: Cluster connector.

Health checking is built into the core as a status-condition evaluator, not a separate scripting runtime.

- **Built-in health checks**: Hardcoded Rust functions for standard Kubernetes resource types. These evaluate `status` sub-resources that Kubernetes already computes — no custom logic needed for the common cases.
- **Condition-based convention**: For any resource with `status.conditions`, Synchrotron applies a standard heuristic: look for a condition with type `Ready` or `Available` with status `True`. This covers most operators and custom resources out of the box with zero configuration.
- **Health aggregation**: App health = worst health of all managed resources
- **Custom health overrides**: Per-resource-type health rules defined in the Application CRD as CEL expressions (Common Expression Language). CEL is already used by Kubernetes for validation webhooks and gateway API, so it's familiar territory. No Lua runtime, no plugin subprocess — just a lightweight expression evaluator compiled into the binary.
- **Status reporting**: Healthy, Progressing, Degraded, Suspended, Missing, Unknown
- **Readiness gates**: Optional wait-for-healthy after sync before proceeding to next wave

### 10. Webhook & Event System

Depends on: Git sync, reconciliation engine.

- **Inbound webhooks**: GitHub, GitLab, Bitbucket push events trigger immediate git fetch + reconciliation
- **Webhook validation**: HMAC signature verification per provider
- **Event bus**: Internal async channel for decoupling event producers from consumers
- **De-duplication**: Coalesce multiple webhooks for same repo within a short window
- **Outbound notifications**: Optional webhook/callback on sync success/failure (for Slack, PagerDuty, etc.)
- **Not required**: System works perfectly fine with polling only; webhooks are an acceleration layer

### 11. CLI & REST API

Depends on: All core components (this is the user-facing layer).

- **REST API**: JSON over HTTP. Simple, curl-friendly, easy to understand and integrate. This is an admin/operator interface — clarity and debuggability matter far more than wire efficiency.
  - `GET /api/v1/applications` - list apps (filterable by cluster, namespace, health, sync status)
  - `GET /api/v1/applications/{name}` - app details, sync status, health, managed resources
  - `POST /api/v1/applications/{name}/sync` - trigger manual sync
  - `GET /api/v1/applications/{name}/diff` - smart diff output (actionable + ignored drift)
  - `POST /api/v1/applications/{name}/rollback` - rollback to previous revision
  - `GET /api/v1/applications/{name}/history` - sync history
  - `GET /api/v1/clusters` / `POST` / `DELETE` - manage clusters
  - `GET /api/v1/repos` / `POST` / `DELETE` - manage git repositories
  - `GET /api/v1/health` - system health
  - **SSE streaming**: `GET /api/v1/applications/{name}/watch` for live status updates (Server-Sent Events — works through proxies, no WebSocket complexity)
- **CLI** (`synchrotron`): Thin HTTP client wrapping the REST API
  - `synchrotron app list` - list applications
  - `synchrotron app get <name>` - show app details, sync status, health
  - `synchrotron app sync <name>` - trigger manual sync
  - `synchrotron app diff <name>` - show what would change. Output separates **actionable drift** (fields Synchrotron manages that have diverged) from **ignored drift** (fields auto-ignored or user-ignored, shown as informational with reason annotations). Full visibility without noise.
  - `synchrotron app rollback <name>` - rollback to previous revision
  - `synchrotron cluster list/add/remove` - manage clusters
  - `synchrotron repo list/add/remove` - manage git repositories
- **Web UI**: NOT in initial scope. REST API makes future UI straightforward.
- **kubectl plugin**: Optional `kubectl synchrotron` alias

## Design Areas

### Core Components
- [ ] SQLite State Storage (foundation)
- [ ] Cluster Connector & Auth (foundation)
- [ ] Git Synchronization Engine
- [ ] Plugin System - Local & Sidecar
- [ ] Manifest Cache (LRU)
- [ ] Smart Diff Engine
- [ ] Reconciliation Engine
- [ ] Deployment Orchestrator
- [ ] Health Assessment Engine
- [ ] Webhook & Event System
- [ ] CLI & REST API
- [ ] Application Model & CRDs

### Technology Decisions
- [x] Language: Rust (zero-cost abstractions, memory efficiency, minimal binary size)
- [x] State Database: SQLite via rusqlite (pure-Rust friendly, WAL mode, simple operations)
- [x] Memory Management: In-memory manifest cache with LRU eviction under 512MB total
- [x] Drift Detection: Server-side dry-run diffing with auto-ignore for controller-managed fields
- [x] Plugin architecture: Local (stdio JSON-RPC) + Sidecar (gRPC)
- [x] Apply strategy: Server-side apply for field ownership tracking
- [x] Health checks: Built-in status checks + condition convention + CEL overrides (no Lua)
- [x] API: REST (JSON/HTTP) with SSE streaming, axum framework
- [ ] CRD schema finalization

### Operational Model
- [ ] Installation and setup (single binary + CRDs, or Helm chart)
- [ ] Configuration management
- [ ] Monitoring and observability (Prometheus metrics, structured logging)
- [x] Multi-cluster deployment patterns ([docs/multi-cluster.md](docs/multi-cluster.md))

### Performance Targets
- [ ] Git reconciliation latency (target: <5s from webhook to sync start)
- [ ] Maximum managed applications (target: 10,000+ apps per instance)
- [ ] Memory usage per application (target: <50KB per app baseline)
- [ ] Cluster scaling limits (target: 50+ clusters per instance)

## Decision Log

### D1: Rust + SQLite Stack
**Date**: 2026-02-24 (revised 2026-03-01)
**Decision**: Use Rust for the main application with SQLite for state persistence.

**Rationale**:
- **Memory Efficiency**: Must stay under 512MB RAM for deployments with thousands of apps. Rust's zero-cost abstractions and lack of garbage collection provide predictable memory usage.
- **Sync Performance**: Rust's async/await with tokio provides excellent concurrency with tight memory bounds.
- **Binary Size**: Pure-Rust stack keeps binary small (~5-8MB static). SQLite adds minimal overhead compared to RocksDB's C++ dependency.
- **SQLite Choice**: WAL mode enables concurrent reads with single writer. Perfect for reconciliation loop (single writer) + API/CLI queries (concurrent readers). Well-understood, battle-tested, zero operational overhead. `rusqlite` with bundled SQLite keeps the build simple.

**Why not RocksDB**:
- Adds ~10-15MB binary size from C++ linkage
- Operational complexity (compaction tuning, write amplification)
- Overkill for our write patterns (periodic syncs, not continuous streams)
- SQLite is more than sufficient for thousands of apps with proper schema

**Trade-offs**:
- SQLite single-writer means sync operations serialize writes (acceptable given our workload)
- Less suitable for extreme write-heavy scenarios (not our case)

### D2: Memory Management Strategy
**Date**: 2026-02-24
**Decision**: Use bounded in-memory manifest cache with LRU eviction to balance sync speed and auto-heal efficiency while staying under 512MB total memory.

**Rationale**:
- **Sync Performance**: Fresh sync operations render manifests and populate cache.
- **Auto-heal Efficiency**: Auto-heal reads from in-memory cache, minimizing CPU and DB overhead.
- **Memory Bounds**: LRU eviction ensures cache respects 512MB total memory budget (SQLite ~20MB + runtime ~30MB = cache ~400MB).
- **Cache Invalidation**: New git commits invalidate cache entries for affected apps.

**Implementation**:
- LRU cache for parsed manifests keyed by `(app, git-commit-hash, params-hash)`
- Configurable max memory (default: 350MB)
- Metrics for cache hit/miss rate

### D3: Plugin Architecture for Templating
**Date**: 2026-02-24
**Decision**: Implement plugin system for manifest templating and rendering engines.

**Rationale**:
- **Flexibility**: Teams use different templating approaches (Helm, Kustomize, Jsonnet, custom tools). Plugins allow each to be opt-in.
- **Decoupling**: Core sync engine doesn't depend on specific templating tool versions.
- **Extensibility**: Users can add custom rendering without modifying core.

**Plugin Types**:
- Templating plugins (input: values + template source, output: rendered manifests)
- Pre-sync hooks (validate, lint, transform)
- Post-deploy hooks (verify, notify)

**Transport**:
- **Local plugins**: JSON-RPC over stdio (subprocess) for fast, simple plugins (Helm, Kustomize wrappers)
- **Sidecar plugins**: gRPC to sidecar containers for resource-heavy or complex plugins
- **Config-driven**: Plugin config specifies transport (local binary path vs container image + port)

### D4: Smart Drift Detection
**Date**: 2026-03-01
**Decision**: Use server-side dry-run diffing with automatic field ignore rules as the primary drift detection strategy.

**Problem Statement**:
Argo CD's biggest operational pain point is false-positive drift detection. Users are constantly marked "OutOfSync" because:
1. HPA changes `spec.replicas` and Argo sees it as drift from the manifest
2. Mutating admission webhooks inject sidecars, annotations, or labels
3. Kubernetes defaults fields that aren't in the manifest (e.g., `protocol: TCP`, `imagePullPolicy`)
4. Operators manage sub-fields of resources they own
5. VPA adjusts resource requests/limits

Users end up maintaining long, brittle `ignoreDifferences` lists that are hard to discover and easy to get wrong.

**Solution: Three-Layer Drift Detection**

**Layer 1: Server-Side Dry-Run Normalization**
Before diffing, apply the desired manifest with `kubectl apply --dry-run=server`. The API server returns what the resource *would* look like after defaulting and mutation. Diff this normalized version against live state instead of diffing raw manifests.

This automatically handles:
- Defaulted fields (protocol, imagePullPolicy, etc.)
- Mutating admission webhooks (sidecar injection, label addition)
- API version coercion and field normalization

**Layer 2: Automatic Controller-Aware Ignore Rules**
Synchrotron actively discovers which Kubernetes controllers manage which fields:

| Controller | Discovery | Auto-Ignored Fields |
|---|---|---|
| HPA | Query HPAs, resolve `scaleTargetRef` | `spec.replicas` on targeted Deployment/StatefulSet/ReplicaSet |
| VPA | Query VPAs in `Auto`/`Recreate` mode, resolve `targetRef` | `spec.containers[*].resources.{requests,limits}` on targeted workloads |
| KEDA ScaledObject | Query ScaledObjects, resolve `scaleTargetRef` | `spec.replicas` on targeted resource |
| Istio Sidecar Injection | Detect `istio-injection: enabled` label on namespace | Sidecar container, init containers, volumes on Pods in that namespace |

**How it works**:
1. On sync, query relevant controller resources (HPAs, VPAs, ScaledObjects) in the app's namespace(s)
2. Resolve their target references
3. Build an ignore-rule set for the current reconciliation
4. Cache the controller mapping (refresh every 5 min or on informer event)

**User-facing config** (per application):
```yaml
drift:
  autoIgnore:
    hpaReplicas: true      # default: true
    vpaResources: true     # default: true
    kedaReplicas: true     # default: true
    defaultedFields: true  # default: true (via server-side diff)
    mutatingWebhooks: true # default: true (via server-side diff)
```

All auto-ignore features default to ON. Users who want strict GitOps checking can disable them selectively.

**Layer 3: User-Defined Ignore Rules**
For anything the auto-detection doesn't cover, users can specify manual rules:

```yaml
drift:
  ignore:
    # Ignore specific JSON pointers
    - group: apps
      kind: Deployment
      jsonPointers:
        - /spec/template/metadata/annotations/some-operator-annotation
    # Ignore fields by their Kubernetes field manager
    - kind: ConfigMap
      managedFieldsManagers:
        - some-operator
    # Ignore with JMESPath expressions (more powerful than JSON pointers)
    - kind: Service
      jmesPathExpressions:
        - "spec.clusterIP"
        - "spec.clusterIPs"
```

**Layer 4: Server-Side Apply Field Ownership**
Synchrotron uses `kubectl apply --server-side` with `fieldManager=synchrotron-cd`. This means Kubernetes itself tracks which fields Synchrotron owns. During diff, we can optionally restrict diffing to only fields Synchrotron manages, completely ignoring fields owned by other controllers.

This is the most elegant solution but requires server-side apply adoption, which is the default apply strategy.

**Drift Visibility**:
All drift is reported, but categorized into two tiers:
- **Actionable drift**: Fields Synchrotron owns that have diverged from desired state. These trigger auto-heal (if enabled) and are the primary output of `synchrotron app diff`.
- **Ignored drift**: Fields auto-ignored (HPA replicas, VPA resources, etc.) or user-ignored. Shown separately with reason annotations (e.g., "ignored: HPA `web-hpa` manages replicas on this Deployment"). Always visible via `synchrotron app diff`, never hidden — but clearly separated from actionable drift so operators aren't buried in noise.

App sync status reflects only actionable drift: an app with only ignored drift is reported as "Synced", not "OutOfSync".

**Design Principles**:
- **Zero-config correctness**: Out of the box, most false-positive drift is eliminated
- **Transparent**: All ignored fields are visible and annotated with *why* they're ignored
- **Override-friendly**: Every auto-rule can be disabled per-app
- **Fail-open**: If controller discovery fails, fall back to standard diff (don't silently miss real drift)

**Performance**:
- Server-side dry-run adds ~10-50ms per resource per reconciliation
- Controller discovery is cached and refreshed periodically (not per-reconcile)
- **Dry-run caching**: Server-side dry-run results are cached keyed by `(resource_uid, manifest_hash)`. Auto-heal cycles reuse cached dry-run results unless the desired manifest has changed (new git commit) or the cache entry has expired (default: 10 min). This reduces dry-run API calls from 100K/3min to near-zero during steady state, with full dry-run only on actual manifest changes.

### D5: Application Model
**Date**: 2026-03-01
**Decision**: Use Kubernetes CRDs as the primary application definition, with optional standalone config for bootstrap.

**Rationale**:
- CRDs are the standard Kubernetes-native way to extend the API
- Enables management of Synchrotron apps via GitOps (self-managing)
- Familiar to Argo CD users (eases migration)
- `ApplicationSet` generator pattern proven effective for multi-app/multi-cluster

**CRDs**:
- `Application` - single application definition (source + destination + sync policy)
- `ApplicationSet` - template + generators for multi-app creation
- `AppProject` (future) - RBAC boundaries for multi-tenant environments

**Bootstrap**: For installing Synchrotron itself or in non-k8s contexts, support a `synchrotron.yaml` config file with the same schema as the CRD spec.

### D6: Health Assessment — Status Conditions + CEL
**Date**: 2026-03-01 (revised)
**Decision**: Built-in health checks for standard resources, condition-based heuristics for everything else, CEL expressions for custom overrides. No Lua runtime, no external plugin process.

**Rationale**:
- Health assessment is one of the most valuable GitOps features
- Most health checking is just reading `status` fields that Kubernetes already computes — no need for a scripting runtime
- CEL (Common Expression Language) is already the Kubernetes ecosystem standard for in-cluster expressions (ValidatingAdmissionPolicy, Gateway API route matching). Using it here means operators learn one expression language, not a Synchrotron-specific one.
- Embedding a Lua VM (rlua/mlua) adds ~2MB binary size and a C dependency. CEL can be implemented in pure Rust with a lightweight evaluator.

**Three-tier health strategy**:

**Tier 1: Built-in rules (hardcoded Rust)**
| Resource | Healthy When |
|---|---|
| Deployment | `updatedReplicas == replicas && availableReplicas == replicas` |
| StatefulSet | `updatedReplicas == replicas && readyReplicas == replicas` |
| DaemonSet | `desiredNumberScheduled == numberReady` |
| Job | `succeeded >= completions` |
| Pod | Phase=Running, all containers ready |
| Service | Has endpoints |
| Ingress | Has at least one address |
| PVC | Phase=Bound |
| CRD | Condition `Established=True` |
| HPA | `currentReplicas` within min/max |

**Tier 2: Condition convention (automatic, zero-config)**
For any resource not in Tier 1, check `status.conditions` for:
1. A condition with `type: Ready` and `status: "True"` → Healthy
2. A condition with `type: Ready` and `status: "False"` with `reason` → Degraded (show reason)
3. A condition with `type: Available` → same logic
4. No recognized conditions → Unknown

This covers the vast majority of operators and CRDs (cert-manager, Knative, Crossplane, etc.) because the Kubernetes API conventions recommend a `Ready` condition.

**Tier 3: CEL overrides (user-configured)**
For resources where Tiers 1-2 don't work, users define CEL expressions in the Application or globally:

```yaml
healthChecks:
  - group: acme.io
    kind: Widget
    # CEL expression evaluated against the resource object
    # Must return one of: "Healthy", "Progressing", "Degraded", "Unknown"
    check: |
      object.status.phase == 'Active' ? 'Healthy' :
      object.status.phase == 'Provisioning' ? 'Progressing' : 'Degraded'
  - group: databases.example.com
    kind: PostgresCluster
    check: |
      has(object.status.conditions) &&
      object.status.conditions.exists(c, c.type == 'PostgresClusterStatus' && c.status == 'True')
      ? 'Healthy' : 'Progressing'
```

**Why not Lua (like Argo CD)**:
- Adds C dependency and ~2MB binary size
- Lua is a whole programming language — overkill for "check a status field"
- CEL is purpose-built for exactly this: evaluate a predicate against a Kubernetes object
- CEL is sandboxed by design (no I/O, no loops, no side effects) — safer than Lua
- Kubernetes ecosystem is converging on CEL, not Lua

**Aggregate Status**: App health = worst status among all managed resources. Statuses: Healthy, Progressing, Degraded, Suspended, Missing, Unknown.

### D7: REST API over gRPC
**Date**: 2026-03-01
**Decision**: Use a plain JSON REST API as the primary interface, not gRPC.

**Rationale**:
- This is an admin/operator interface. Simplicity and debuggability matter far more than wire efficiency.
- REST is universally understood — every operator knows `curl`. gRPC requires special tooling (`grpcurl`, generated clients).
- JSON responses are human-readable in logs, browser dev tools, and `jq` pipelines.
- REST works through every proxy, load balancer, and firewall without special configuration. gRPC requires HTTP/2 end-to-end and has issues with many ingress controllers.
- For real-time updates (watching app status), SSE (Server-Sent Events) provides streaming over plain HTTP — no WebSocket upgrade complexity, works through proxies.

**Why not gRPC**:
- The performance difference is irrelevant at admin API scale (tens of requests/sec, not thousands)
- Proto schema management adds build complexity
- Every consumer needs a generated client or `grpcurl` — friction for quick debugging
- gRPC-web adds another layer of complexity for browser-based UIs

**Implementation**:
- `axum` for HTTP serving (already in the Rust ecosystem, async, performant)
- `serde_json` for serialization
- OpenAPI spec generated from code (for documentation and future client generation)
- SSE via `axum`'s streaming response for watch endpoints

---

**Last Updated**: 2026-03-01
**Status**: In Progress
