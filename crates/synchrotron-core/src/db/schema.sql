-- Synchrotron-CD initial schema (v1)
-- SQLite with WAL mode

PRAGMA foreign_keys = ON;

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Application definitions
CREATE TABLE IF NOT EXISTS applications (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    namespace TEXT NOT NULL DEFAULT 'synchrotron-system',

    -- Source
    repo_url TEXT NOT NULL,
    path TEXT NOT NULL,
    target_revision TEXT NOT NULL DEFAULT 'main',
    plugin_config TEXT,

    -- Destination
    dest_cluster TEXT NOT NULL,
    dest_namespace TEXT NOT NULL,

    -- Sync policy (JSON for flexibility during early development)
    sync_policy TEXT NOT NULL DEFAULT '{}',

    -- Status (updated by reconciliation engine)
    sync_status TEXT NOT NULL DEFAULT 'Unknown',
    health_status TEXT NOT NULL DEFAULT 'Unknown',
    health_message TEXT,
    last_synced_at TEXT,
    last_synced_revision TEXT,

    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_applications_name ON applications(name);
CREATE INDEX IF NOT EXISTS idx_applications_cluster ON applications(dest_cluster);
CREATE INDEX IF NOT EXISTS idx_applications_sync_status ON applications(sync_status);

-- Sync history (bounded, retention-managed)
CREATE TABLE IF NOT EXISTS sync_history (
    id TEXT PRIMARY KEY,
    app_id TEXT NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    revision TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    resources_synced INTEGER DEFAULT 0,
    trigger TEXT NOT NULL DEFAULT 'manual'
);

CREATE INDEX IF NOT EXISTS idx_sync_history_app_id ON sync_history(app_id);
CREATE INDEX IF NOT EXISTS idx_sync_history_started_at ON sync_history(started_at);

INSERT INTO schema_migrations (version) VALUES (1);
