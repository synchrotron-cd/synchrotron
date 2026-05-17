# synchrotron

GitOps continuous delivery for Kubernetes — Helm chart.

## Install

```bash
# Add this repo (until the chart is published to a Helm registry,
# install from the cloned repo).
helm install synchrotron ./deploy/charts/synchrotron \
  --namespace synchrotron-system --create-namespace
```

The chart installs:

- The `synchrotron-server` controller (single-replica Deployment with
  `Recreate` strategy + leader election lease).
- A ClusterRole/Binding wide enough to apply arbitrary user
  manifests. Disable with `rbac.create=false` if managing RBAC
  out-of-band.
- A ConfigMap containing the rendered `synchrotron` config. The
  Deployment rolls automatically when the rendered config changes
  (`checksum/config` annotation).
- A Service exposing the HTTP API + `/metrics` on port 8484.
- A PersistentVolumeClaim for state (SQLite + git working trees).
  Disable with `persistence.enabled=false` for ephemeral environments.
- Optional `ServiceMonitor` for Prometheus Operator clusters
  (`metrics.serviceMonitor.enabled=true`).

## CRDs

CRDs live under `crds/` and are installed by Helm on **first install
only**. `helm upgrade` does **not** upgrade CRDs, by design — CRDs
are cluster-scoped and silently mutating them across versions can
break running workloads. To upgrade CRDs intentionally:

```bash
kubectl apply -f deploy/charts/synchrotron/crds/
```

The shipped CRDs are at `v1alpha1`; spec schemas use
`x-kubernetes-preserve-unknown-fields` so additive changes don't
require a CRD bump.

## Values

| Key | Default | Description |
| --- | --- | --- |
| `replicaCount` | `1` | Controller replicas (leader election ensures only one reconciles). |
| `image.repository` | `ghcr.io/synchrotron-cd/synchrotron-server` | Container image. |
| `image.tag` | `""` (chart `appVersion`) | Image tag. |
| `image.pullPolicy` | `IfNotPresent` | Pull policy. |
| `imagePullSecrets` | `[]` | Pull secrets for private registries. |
| `serviceAccount.create` | `true` | Create a dedicated ServiceAccount. |
| `serviceAccount.annotations` | `{}` | SA annotations (e.g. IRSA). |
| `rbac.create` | `true` | Create cluster-wide RBAC. |
| `service.type` | `ClusterIP` | Service type. |
| `service.port` | `8484` | HTTP API/metrics port. |
| `persistence.enabled` | `true` | Use a PVC for state. |
| `persistence.size` | `10Gi` | PVC size. |
| `persistence.storageClass` | `""` | StorageClass (empty = cluster default). |
| `persistence.accessModes` | `[ReadWriteOnce]` | Access modes. |
| `resources` | requests 100m/128Mi, limits 1/512Mi | Pod resources. |
| `podSecurityContext` | non-root, restricted PSS | Pod security context. |
| `securityContext` | `readOnlyRootFilesystem`, no caps | Container security context. |
| `metrics.serviceMonitor.enabled` | `false` | Create a Prometheus ServiceMonitor. |
| `metrics.serviceMonitor.interval` | `30s` | Scrape interval. |
| `probes.liveness` | `GET /healthz` | Liveness probe (200 once axum is serving). |
| `probes.readiness` | `GET /readyz` | Readiness probe (200 once DB+config startup completes). |
| `config` | (see `values.yaml`) | Synchrotron config — rendered to a ConfigMap. |
| `extraEnv` | `[]` | Extra env vars. |
| `extraVolumes` | `[]` | Extra volumes. |
| `extraVolumeMounts` | `[]` | Extra volume mounts. |

See [`values.yaml`](./values.yaml) for the canonical, comment-annotated source.

## Validating locally

```bash
helm lint deploy/charts/synchrotron
helm template synchrotron deploy/charts/synchrotron --debug | kubectl apply --dry-run=server -f -
```

CI runs `helm lint` and a `helm template` render on every PR; see
[`.github/workflows/helm.yml`](../../../.github/workflows/helm.yml).

## Uninstall

```bash
helm uninstall synchrotron -n synchrotron-system
# CRDs are not removed by Helm. Delete explicitly if needed:
kubectl delete crd applications.synchrotron.io applicationsets.synchrotron.io
```
