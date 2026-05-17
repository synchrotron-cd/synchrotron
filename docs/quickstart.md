# Quickstart: deploy your first app

This walks through installing Synchrotron on a local cluster,
pointing it at a git repo with one Kubernetes manifest, and
watching it reconcile. End-to-end target: about ten minutes.

## Prerequisites

- A running Kubernetes cluster you can talk to. [kind] or [k3d]
  are the easiest options if you don't already have one:

  ```bash
  kind create cluster --name synchrotron-quickstart
  ```

- `kubectl` configured for that cluster.
- `helm` 3.10 or later.
- A git repo you can push to that the cluster can reach. For a
  fully self-contained walkthrough we use a tiny in-cluster
  `git daemon` (described below). For a real setup, any
  GitHub/GitLab/Bitbucket repo over HTTPS or SSH works.

[kind]: https://kind.sigs.k8s.io/
[k3d]: https://k3d.io/

## 1. Install the controller

The Helm chart isn't published to a registry yet, so clone the
repo and install from the local path:

```bash
git clone https://github.com/synchrotron-cd/synchrotron.git
cd synchrotron

helm install synchrotron deploy/charts/synchrotron \
  --namespace synchrotron-system \
  --create-namespace
```

The chart's default image (`ghcr.io/synchrotron-cd/synchrotron-server`)
is currently only pullable by org members. Until the package is
made public (synchrotron-cd-ecr), build the image locally and
side-load it into your cluster:

```bash
docker build -t synchrotron-server:dev .
kind load docker-image synchrotron-server:dev --name synchrotron-quickstart
helm upgrade synchrotron deploy/charts/synchrotron \
  --namespace synchrotron-system \
  --set image.repository=localhost/synchrotron-server \
  --set image.tag=dev \
  --set image.pullPolicy=IfNotPresent
```

(With `kind` + `podman`, loaded images appear under the
`localhost/` prefix in containerd, so the chart needs the matching
`image.repository`. With Docker proper, drop the `localhost/`.)

That gives you a running controller with no repos and no apps
configured — it's ready to be told what to do.

Check it came up:

```bash
kubectl -n synchrotron-system rollout status deploy/synchrotron --timeout=2m
kubectl -n synchrotron-system port-forward svc/synchrotron 8484:8484 &
curl -fsS http://127.0.0.1:8484/healthz   # → "ok"
curl -fsS http://127.0.0.1:8484/readyz    # → {"ready":true,...}
```

## 2. Point the controller at a repo

Synchrotron polls configured repos for HEAD movement (default
every 180 s) and serves an inbound webhook endpoint for instant
fetches. You configure repos via the Helm chart's `config.repos`
list — they're operator-managed, not API-managed.

For the quickstart, spin up an in-cluster git daemon serving an
empty repo so you have something to push to:

```bash
cat <<'YAML' | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: git-server
  namespace: synchrotron-system
spec:
  selector: { app: git-server }
  ports: [{ port: 9418, targetPort: 9418 }]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: git-server
  namespace: synchrotron-system
spec:
  replicas: 1
  selector: { matchLabels: { app: git-server } }
  template:
    metadata: { labels: { app: git-server } }
    spec:
      initContainers:
        - name: init
          image: alpine/git:latest
          command: ["sh","-c","mkdir -p /srv/git/sample.git && git init --bare /srv/git/sample.git"]
          volumeMounts: [{ name: srv, mountPath: /srv/git }]
      containers:
        - name: daemon
          image: alpine/git:latest
          command:
            - sh
            - -c
            - 'apk add --no-cache git-daemon && git daemon --reuseaddr --base-path=/srv/git --export-all --enable=upload-pack --enable=receive-pack --listen=0.0.0.0'
          ports: [{ containerPort: 9418 }]
          volumeMounts: [{ name: srv, mountPath: /srv/git }]
      volumes: [{ name: srv, emptyDir: {} }]
YAML
kubectl -n synchrotron-system rollout status deploy/git-server --timeout=60s
```

Now reconfigure the controller to know about that repo plus the
built-in `raw` plugin (which just reads `.yaml` files from a
path):

```bash
helm upgrade synchrotron deploy/charts/synchrotron \
  --namespace synchrotron-system \
  --reuse-values \
  --set-json 'config.repos=[{"id":"sample","url":"git://git-server.synchrotron-system.svc:9418/sample.git","branch":"main"}]' \
  --set-json 'config.plugins=[{"name":"raw","kind":"raw","config":null}]' \
  --set 'config.polling.repo_interval_seconds=10'

kubectl -n synchrotron-system rollout status deploy/synchrotron --timeout=60s
```

## 3. Push a manifest to the repo

Drop a ConfigMap into the repo's `manifests/` directory:

```bash
kubectl -n synchrotron-system exec deploy/git-server -c daemon -- sh -c '
  set -e
  rm -rf /tmp/work && git clone -q /srv/git/sample.git /tmp/work
  cd /tmp/work
  git config user.email quickstart@synchrotron-cd.example
  git config user.name "synchrotron quickstart"
  # Fresh bare repo has no HEAD ref yet; the clone leaves the
  # local copy on whatever init.defaultBranch is. Force "main" so
  # the first push lands on the branch synchrotron is polling.
  git checkout -b main
  mkdir -p manifests
  cat > manifests/hello.yaml <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: synchrotron-hello
  namespace: default
data:
  greeting: "hello from synchrotron"
YAML
  git add manifests/hello.yaml
  git commit -q -m "add hello configmap"
  git push -q origin main
'
```

## 4. Create the Application

Apps are runtime data — created via the HTTP API, not a CRD or
Helm value. Tell Synchrotron which repo path to render and which
cluster to push it to:

```bash
curl -fsS -X POST -H 'content-type: application/json' \
  http://127.0.0.1:8484/api/v1/apps \
  -d '{
    "name": "hello",
    "namespace": "default",
    "repo_url": "git://git-server.synchrotron-system.svc:9418/sample.git",
    "path": "manifests",
    "target_revision": "main",
    "dest_cluster": "in-cluster",
    "dest_namespace": "default"
  }'
```

The `POST` publishes an `AppChanged` event, the render loop
materializes the repo and writes desired state, and the reconciler
applies it. No additional push needed.

## 5. Watch it land

```bash
# Either watch the API for sync events…
curl -N http://127.0.0.1:8484/api/v1/apps/hello/watch

# …or just check the cluster:
kubectl -n default get configmap synchrotron-hello -o yaml
```

You should see the ConfigMap appear within a few seconds.

## 6. Iterate

Push a change to the repo and watch the next reconcile apply it:

```bash
kubectl -n synchrotron-system exec deploy/git-server -c daemon -- sh -c '
  set -e
  cd /tmp/work
  sed -i "s/hello from synchrotron/updated/" manifests/hello.yaml
  git commit -q -am "update greeting"
  git push -q origin main
'
# Wait for the next poll (configured at 10s above), or POST a sync:
curl -fsS -X POST http://127.0.0.1:8484/api/v1/apps/hello/sync

kubectl -n default get configmap synchrotron-hello \
  -o jsonpath='{.data.greeting}'   # → "updated"
```

## Cleanup

```bash
kill %1                                   # stop port-forward
helm uninstall synchrotron -n synchrotron-system
kubectl delete namespace synchrotron-system
kind delete cluster --name synchrotron-quickstart   # if you used kind
```

## Where to go next

- [`DESIGN.md`](../DESIGN.md) — architecture and reconcile model.
- [`deploy/charts/synchrotron`](../deploy/charts/synchrotron) —
  every Helm value, including OIDC kubeconfig, webhook secrets,
  notification sinks, and multi-cluster setup.
- [`docs/multi-cluster.md`](multi-cluster.md) — pointing one
  controller at several clusters.
