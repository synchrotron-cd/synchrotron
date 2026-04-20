-- Synchrotron-CD schema v2: app manifest cache persistence.
--
-- Row per (app_id, commit_hash, params_hash) render. Populated by
-- the reconciler as it renders each app; consumed on startup to
-- warm the in-memory AppCache so recently-active apps hit
-- immediately after a process restart.

CREATE TABLE IF NOT EXISTS app_cache_entries (
    app_id TEXT NOT NULL,
    commit_hash TEXT NOT NULL,
    params_hash BLOB NOT NULL,
    manifests_json TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (app_id, commit_hash, params_hash)
);

-- Warm-up query orders by updated_at DESC to prefer recently-active
-- apps; index keeps that cheap without a sort.
CREATE INDEX IF NOT EXISTS idx_app_cache_updated_at
    ON app_cache_entries(updated_at DESC);

INSERT INTO schema_migrations (version) VALUES (2);
