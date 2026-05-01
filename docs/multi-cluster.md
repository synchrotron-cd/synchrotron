# Multi-cluster deployment patterns

Synchrotron-CD's [Cluster Connector](../DESIGN.md#2-cluster-connector--auth)
treats every target cluster as a named entry in the controller's
config: a kubeconfig path with optional context, or `in_cluster: true`
when the target is the host cluster itself. That single primitive
supports two operational patterns; this doc covers when to use each,
their failure modes, and example manifests.

> **Audience:** platform operators picking a topology before
> installing Synchrotron-CD. Not relevant if you've already chosen one
> and just want config syntax — for that, see
> [`synchrotron-server config init`](../BUILD.md) and the
> [Helm chart README](../deploy/charts/synchrotron/README.md).

## Pattern 1: hub-and-spoke

One Synchrotron-CD instance deploys to many clusters. Each managed
("spoke") cluster is referenced by name from the hub's config; the
controller connects via that cluster's kubeconfig and reconciles
every `Application` whose `spec.destination.cluster` resolves to it.

```
                       ┌──────────────────────────┐
                       │  Hub cluster (or VM)     │
                       │                          │
                       │  ┌────────────────────┐  │
                       │  │ synchrotron-server │  │
  Git repos ──polls──▶ │  │  ↳ leader-elected  │  │
                       │  └─────────┬──────────┘  │
                       │            │             │
                       └────────────┼─────────────┘
                                    │ kubeconfigs
                ┌───────────────────┼────────────────────┐
                │                   │                    │
                ▼                   ▼                    ▼
        ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
        │  prod-east   │    │  prod-west   │    │   staging    │
        │   spoke      │    │    spoke     │    │    spoke     │
        └──────────────┘    └──────────────┘    └──────────────┘
```

### When to pick it

- One platform team owns reconciliation for the whole fleet.
- Spokes are short-lived or numerous (preview environments, ephemeral
  customer clusters) — one set of credentials beats `helm install`
  per cluster.
- Single source of truth for sync history, audit logs, and
  controller version. No per-cluster upgrade dance.

### Trade-offs

- **Single blast radius.** Hub outage stops reconciliation for every
  spoke. Mitigate with leader-elected HA replicas (see
  [Helm `replicaCount`](../deploy/charts/synchrotron/values.yaml))
  and well-tested backups of the hub's SQLite state.
- **Network egress.** Hub must reach each spoke's API server. If
  spokes sit behind firewalls, you'll need a tunnel (Tailscale,
  WireGuard, AWS PrivateLink, GCP PSC) or a reverse-proxy approach.
  Synchrotron-CD's HTTPS clients honor `HTTPS_PROXY`.
- **Credential management at scale.** Each spoke needs a kubeconfig
  the hub can read. Rotating one cluster's credentials should not
  require a hub redeploy — use a credential-plugin auth source or
  mount kubeconfigs from an external secret store.
- **API server load lives on the hub side.** Watch streams, dry-run
  applies, and discovery requests for every spoke ride the hub's
  network. Plan for it.

### Failure modes

| Failure | Symptom | Recovery |
|---|---|---|
| Hub pod crashes | All sync pauses; metrics-scrape silence | Leader election fails over to a healthy replica within ~15s |
| Hub PVC unavailable | Pod stays in `Pending`; readiness 503 | Cluster admin restores PV; Synchrotron resumes from last persisted state |
| Spoke kubeconfig expires | App reports `Unknown` health, reconcile errors with auth failure | Rotate credential at its source; controller picks up the new kubeconfig on SIGHUP reload |
| Spoke API unreachable | App stalls at `OutOfSync` on first reconcile after the outage | Reconcile retries with backoff; nothing else needed once spoke recovers |
| Single spoke degraded | Errors localised to that cluster's apps; other spokes unaffected | Per-cluster reconciler tasks are isolated — no cross-contamination |

### Example: hub config

The hub's `synchrotron-server` config registers each spoke and
pins the auth source. Reload via `SIGHUP` to pick up new entries
without dropping connections to existing spokes.

```yaml
# /etc/synchrotron/config.yaml on the hub.
server:
  listen_addr: "0.0.0.0:8484"
  db_path: /var/lib/synchrotron/synchrotron.db

clusters:
  - name: prod-east
    kubeconfig: /etc/synchrotron/spokes/prod-east.kubeconfig
    context: prod-east
  - name: prod-west
    kubeconfig: /etc/synchrotron/spokes/prod-west.kubeconfig
    context: prod-west
  - name: staging
    kubeconfig: /etc/synchrotron/spokes/staging.kubeconfig

repos:
  - id: platform
    url: https://github.com/example/platform.git
    branch: main
    credentials_secret: platform-git
```

### Example: hub Helm values

Mount the spoke kubeconfigs from an existing Secret (one
recommended pattern; cluster-secret-store integrations and
external-secrets work just as well):

```yaml
# values.yaml fragment for the hub install.
config:
  clusters:
    - name: prod-east
      kubeconfig: /etc/synchrotron/spokes/prod-east.kubeconfig
      context: prod-east
    - name: prod-west
      kubeconfig: /etc/synchrotron/spokes/prod-west.kubeconfig
      context: prod-west

extraVolumes:
  - name: spoke-kubeconfigs
    secret:
      secretName: synchrotron-spokes

extraVolumeMounts:
  - name: spoke-kubeconfigs
    mountPath: /etc/synchrotron/spokes
    readOnly: true
```

### Example: targeting a spoke from an Application

```yaml
apiVersion: synchrotron.io/v1alpha1
kind: Application
metadata:
  name: payments-prod-east
  namespace: synchrotron
spec:
  source:
    repoURL: https://github.com/example/platform.git
    path: services/payments
    targetRevision: main
  destination:
    cluster: prod-east   # matches `clusters[].name` in the hub config
    namespace: payments
```

## Pattern 2: per-cluster instance

One Synchrotron-CD instance per cluster, each running with
`in_cluster: true`. Apps are deployed locally; the controller never
holds credentials for any cluster other than its host.

```
   ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────┐
   │  prod-east           │   │  prod-west           │   │  staging             │
   │ ┌──────────────────┐ │   │ ┌──────────────────┐ │   │ ┌──────────────────┐ │
   │ │synchrotron-server│ │   │ │synchrotron-server│ │   │ │synchrotron-server│ │
   │ │  in_cluster: yes │ │   │ │  in_cluster: yes │ │   │ │  in_cluster: yes │ │
   │ └────────┬─────────┘ │   │ └────────┬─────────┘ │   │ └────────┬─────────┘ │
   │          │ apps      │   │          │ apps      │   │          │ apps      │
   │          ▼           │   │          ▼           │   │          ▼           │
   │       workloads      │   │       workloads      │   │       workloads      │
   └──────────▲───────────┘   └──────────▲───────────┘   └──────────▲───────────┘
              │                          │                          │
              └──────────polls───────────┼─────────polls────────────┘
                                         │
                                    Git repos
```

### When to pick it

- Strong cluster-isolation requirements (regulated workloads,
  per-tenant clusters, air-gapped environments).
- Each cluster has its own platform team owning its Synchrotron
  install — no shared blast radius is desirable.
- Network policy forbids one cluster from holding credentials for
  another.
- Cluster count is small enough that per-cluster install/upgrade
  isn't operationally painful.

### Trade-offs

- **No central rollouts.** Promoting a controller version means
  upgrading every install separately. The Helm chart helps but
  doesn't eliminate the work.
- **Sync history fragments per cluster.** No single dashboard
  showing fleet-wide drift; aggregate via metrics scraping or a
  read-only roll-up tool if you need it.
- **Same git infrastructure repeated.** Each instance polls every
  repo it cares about. At fleet scale you'll burn more git API
  quota than the hub pattern.
- **Credentials simpler.** Each install needs only its in-cluster
  ServiceAccount. No kubeconfig fan-out, no rotation choreography.

### Failure modes

| Failure | Symptom | Recovery |
|---|---|---|
| One controller down | That cluster pauses sync; others unaffected | Leader-elected HA replicas inside the cluster fail over locally |
| One cluster's PVC corrupted | Apps in that cluster stall; per-cluster blast radius only | Restore the PVC; other clusters unaffected |
| One cluster's git creds expire | Apps in that cluster show `Unknown` source; others fine | Rotate the per-cluster Secret; controller resumes |
| Synchrotron upgrade rolls out | Per-cluster Helm rollouts are independent — staggered upgrades are natural | Revert one cluster without touching the rest |

### Example: per-cluster Helm values

Default chart values already use `in_cluster: true`. The minimum
override is the per-cluster repo list:

```yaml
# values.yaml on prod-east.
config:
  clusters:
    - name: in-cluster
      in_cluster: true
  repos:
    - id: platform
      url: https://github.com/example/platform.git
      branch: main
      credentials_secret: platform-git
```

### Example: in-cluster Application

`destination.cluster` matches the local `in-cluster` entry. Most
operators leave it implicit and rely on the controller's default
target:

```yaml
apiVersion: synchrotron.io/v1alpha1
kind: Application
metadata:
  name: payments
  namespace: synchrotron
spec:
  source:
    repoURL: https://github.com/example/platform.git
    path: services/payments
    targetRevision: main
  destination:
    cluster: in-cluster
    namespace: payments
```

## Choosing between them

| If your situation is… | Pattern |
|---|---|
| One ops team, many small clusters, easy network reachability | hub-and-spoke |
| Strong isolation requirements (regulatory, multi-tenant) | per-cluster |
| Mix of long-lived and ephemeral spokes (preview envs) | hub-and-spoke |
| Air-gapped or strict-egress clusters | per-cluster |
| Fewer than ~5 clusters total, all under one team | either; per-cluster is operationally simpler if you're already comfortable with cluster-local operators |
| 10+ clusters, including some short-lived | hub-and-spoke; install overhead per spoke is the dominant cost |

The two patterns aren't mutually exclusive: a hub managing your
long-lived production spokes plus per-cluster installs in
regulated environments is a reasonable, common topology.

## Related

- [DESIGN.md §2 Cluster Connector & Auth](../DESIGN.md#2-cluster-connector--auth) — the underlying primitive
- [Helm chart README](../deploy/charts/synchrotron/README.md) — install reference
- [BUILD.md](../BUILD.md) — release artifacts and config schema
