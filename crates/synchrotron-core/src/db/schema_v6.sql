-- v6: git repos registered through the REST API.
--
-- `credentials_secret_ref` is a *pointer* into the operator's secret
-- store (Vault/SealedSecret/etc) — Synchrotron never sees the
-- credential value through this field. `password` is the inline
-- write-only fallback for environments without a secret store; it is
-- persisted but masked out of all API responses.
CREATE TABLE IF NOT EXISTS repos (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    branch TEXT,
    credentials_secret_ref TEXT,
    password TEXT,
    labels_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES (6);
