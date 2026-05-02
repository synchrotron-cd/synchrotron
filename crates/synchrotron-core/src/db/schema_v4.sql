-- v4: per-app revision snapshots for rollback to last-N successful syncs.
--
-- Distinct from `sync_history` (which is the event log of every sync
-- attempt with status, trigger, message). This table stores the
-- *manifests* of successful syncs so an operator can rebuild the
-- cluster state from a prior revision without re-rendering from git.
--
-- Each row is one successful sync: commit hash, params hash (so a
-- redeploy with different generator inputs is a distinct revision
-- even at the same commit), the manifests-as-rendered, and a
-- monotonic recorded_at timestamp for ordering. The reconciler trims
-- the table to N rows per app on insert; the rollback path picks any
-- prior row by id and re-applies its manifests.
CREATE TABLE IF NOT EXISTS app_sync_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app_id TEXT NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    commit_hash TEXT NOT NULL,
    params_hash BLOB NOT NULL,
    manifests_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_sync_revisions_app_recorded
    ON app_sync_revisions(app_id, id DESC);

INSERT OR IGNORE INTO schema_migrations (version) VALUES (4);
