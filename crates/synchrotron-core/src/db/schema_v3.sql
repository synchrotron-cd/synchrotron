-- Synchrotron-CD schema v3: per-app owned-resource set.
--
-- Records the set of resources synchrotron-cd has applied for an
-- app, so a later sync can compute a prune set as
-- `previously_owned − currently_desired`. Without persistence we
-- couldn't safely prune across process restarts: an in-memory only
-- record would forget what we used to own and either skip valid
-- prunes or misidentify someone else's resources as ours.
--
-- The set is replaced atomically per sync (delete-then-insert in one
-- transaction at the repo layer). `prune_disabled` is captured at
-- record time from the manifest's annotation so a later prune sweep
-- can decide locally without a live API lookup.

CREATE TABLE IF NOT EXISTS app_owned_resources (
    app_id TEXT NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    -- GVK split into its three components so we can index/query
    -- without parsing a serialized form.
    gvk_group TEXT NOT NULL,
    gvk_version TEXT NOT NULL,
    gvk_kind TEXT NOT NULL,
    -- Empty string for cluster-scoped resources. Empty (not NULL)
    -- keeps the primary key total without a NULL-handling case.
    namespace TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    -- Sync wave at last apply time; reverse-wave order drives
    -- delete sequencing during prune.
    wave INTEGER NOT NULL DEFAULT 0,
    -- 1 if the manifest carried a Prune=false annotation. The prune
    -- sweep skips these rows entirely.
    prune_disabled INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (app_id, gvk_group, gvk_version, gvk_kind, namespace, name)
);

-- Bulk-load by app is the only access pattern, so a single covering
-- index on app_id is enough.
CREATE INDEX IF NOT EXISTS idx_owned_resources_app
    ON app_owned_resources(app_id);

INSERT OR IGNORE INTO schema_migrations (version) VALUES (3);
