# Upgrades, migrations, and downgrades

How to move a Synchrotron-CD install from one release to the next, what
the controller does to its on-disk state during an upgrade, and what to
do when you need to roll back.

> **Audience:** platform operators running Synchrotron-CD in
> production. For the topology you chose at install time, see
> [`docs/multi-cluster.md`](multi-cluster.md). For the underlying
> schema model, see
> [`crates/synchrotron-core/src/db/migrations.rs`](../crates/synchrotron-core/src/db/migrations.rs).

## What's persisted

Synchrotron-CD keeps a single SQLite database (WAL mode) at the path
configured by `server.db_path` (default
`/var/lib/synchrotron/synchrotron.db`). It holds:

- `applications` — the registered `Application` set and last-known
  sync/health status.
- `sync_history` — bounded history of past sync attempts.
- `app_cache_entries` — rendered manifest cache, keyed by
  `(app_id, commit_hash, params_hash)`. Warmed into the in-memory
  `AppCache` on startup.
- `schema_migrations` — the version ledger this guide is about.

Everything else (informer caches, leader-election state, in-flight
reconciler tasks) is in-memory and rebuilt on startup. The SQLite file
is the only durable artifact you need to back up.

## Migration model

Migrations are **forward-only**, **versioned**, **idempotent**, and
**atomic per migration**.

- *Forward-only.* Every release ships a higher `LATEST_VERSION`. The
  migration runner advances the DB to that version on startup;
  there is no in-process down-migration.
- *Versioned.* Each migration ends with
  `INSERT OR IGNORE INTO schema_migrations (version) VALUES (N)`.
  The maximum recorded version is the canonical schema state.
- *Idempotent.* All DDL uses `IF NOT EXISTS`; the version-row insert
  uses `OR IGNORE`. Re-running the runner against an
  already-migrated DB is a no-op, and replaying a single migration's
  SQL by hand (e.g. while recovering a partial restore) is safe.
- *Atomic per migration.* Each migration runs inside an explicit
  transaction. A failing statement rolls back to the prior schema
  rather than leaving the DB half-applied.

The runner is exercised by unit tests at
[`crates/synchrotron-core/src/db/migrations.rs`](../crates/synchrotron-core/src/db/migrations.rs),
including a cross-version test that starts a DB at v1, inserts data, runs
the current binary's full migration set, and asserts both that v2
applied and that the v1 row is still readable. That is the
"tested across two release versions" guarantee.

## Rolling upgrade procedure

The recommended path. Works for both the
[hub-and-spoke and per-cluster topologies](multi-cluster.md).

### 1. Read the release notes

Specifically, look for:

- A `LATEST_VERSION` bump (means new migrations will run).
- Config schema changes (means your existing config may need edits).
- Removed or renamed flags.

If the release does not bump `LATEST_VERSION`, the upgrade is purely a
binary swap — no DB-side action is needed.

### 2. Back up the SQLite file

**Always do this before an upgrade**, even if no schema change is
advertised. It's the only escape hatch for a downgrade (see
[Downgrades](#downgrades) below).

```bash
# Inside the controller pod, or on the host running the controller:
sqlite3 /var/lib/synchrotron/synchrotron.db ".backup '/var/lib/synchrotron/synchrotron.db.bak-$(date +%Y%m%d-%H%M%S)'"
```

`.backup` is online-safe — it works while the controller is running
and respects WAL. Copying the raw `.db` file by hand without
checkpointing WAL can produce a corrupt snapshot; prefer `.backup`.

For Helm installs with a PVC, snapshot the PV through your
infrastructure (CSI VolumeSnapshot, EBS snapshot, etc.) in addition
to the SQLite-level backup.

### 3. Apply the new release

**Helm:**

```bash
helm repo update
helm upgrade synchrotron synchrotron/synchrotron \
  --namespace synchrotron-system \
  --values values.yaml
```

**Raw manifests:**

```bash
kubectl apply -f synchrotron-<version>.yaml
```

The controller image is rolled by the Deployment. With leader
election (`replicaCount >= 2`), one replica drains while the new
image starts up, and leader handoff happens once the new pod is
ready. Brief reconcile pauses during handoff are expected; in-flight
syncs resume from persisted state.

### 4. Verify

```bash
kubectl -n synchrotron-system logs deploy/synchrotron-server \
  | grep "applying migration"
```

You should see one `applying migration vN: <description>` line per
migration that was needed, followed by readiness. If the DB was
already at `LATEST_VERSION`, no such lines appear (the runner is a
no-op).

Then sanity-check:

```bash
kubectl -n synchrotron-system get applications
# All apps should reach a stable Synced/Healthy state within a few reconcile cycles.
```

## Downgrades

**There is no in-process downgrade.** A newer release's migrations
add columns, tables, or rows that the older binary does not know how
to read. Pointing an older binary at a newer DB will surface
`no such column` / `no such table` errors at startup or during query
execution.

The supported downgrade path is **restore the SQLite file from the
backup taken before the upgrade**, then redeploy the older binary.
Anything written between the backup and the restore is lost — sync
history rows, status updates, and warmed cache entries are
recomputable on the next reconcile, so this is usually acceptable,
but plan for it.

### Downgrade procedure

1. **Scale the controller to 0** so nothing writes to the DB during
   the restore.
   ```bash
   kubectl -n synchrotron-system scale deploy/synchrotron-server --replicas=0
   ```
2. **Restore the backup** in place of the live DB. Replace WAL/SHM
   sidecar files too — leftover WAL from the newer schema will
   re-apply newer state on top of your restored snapshot.
   ```bash
   # In a debug pod with the PVC mounted, or directly on the host:
   rm -f /var/lib/synchrotron/synchrotron.db \
         /var/lib/synchrotron/synchrotron.db-wal \
         /var/lib/synchrotron/synchrotron.db-shm
   cp /var/lib/synchrotron/synchrotron.db.bak-<timestamp> \
      /var/lib/synchrotron/synchrotron.db
   ```
3. **Redeploy the older release**.
   ```bash
   helm upgrade synchrotron synchrotron/synchrotron \
     --version <older-version> \
     --namespace synchrotron-system \
     --values values.yaml
   ```
4. **Scale back up**.
   ```bash
   kubectl -n synchrotron-system scale deploy/synchrotron-server --replicas=2
   ```
5. **Verify** — the controller should start, find the DB at the
   older `LATEST_VERSION`, and resume reconciling.

## Backups (independent of upgrades)

Even outside upgrade windows, take periodic backups: the SQLite file
is your authoritative copy of registered apps and sync history.

A minimal pattern is a CronJob that runs `sqlite3 .backup` against
the controller PVC and ships the result to object storage. This is
not built into the chart today; track
[`bd show synchrotron-cd-58v`](https://example/synchrotron-cd-58v) for
operator-tooling work in this area.

To restore a backup outside an upgrade — same procedure as a
downgrade, minus the binary swap: scale to 0, replace the file
(remove WAL/SHM), scale back up.

## Schema versioning policy

When adding a migration:

- Increment `LATEST_VERSION` in
  [`crates/synchrotron-core/src/db/migrations.rs`](../crates/synchrotron-core/src/db/migrations.rs)
  by exactly 1 per release.
- Add the new `schema_vN.sql` file alongside the existing ones; end
  it with `INSERT OR IGNORE INTO schema_migrations (version) VALUES (N)`.
- Use `IF NOT EXISTS` for every `CREATE` so the file remains
  replay-safe.
- Add a unit test mirroring `v1_to_v2_upgrade_path_lands_at_latest`
  that proves an older release's persisted state survives the new
  migration.
- Document any non-trivial data implications (e.g. backfills, column
  semantics changes) in the release notes.

## Related

- [`docs/multi-cluster.md`](multi-cluster.md) — topology choices.
- [`crates/synchrotron-core/src/db/migrations.rs`](../crates/synchrotron-core/src/db/migrations.rs) — runner + tests.
- [`DESIGN.md`](../DESIGN.md) — overall architecture.
