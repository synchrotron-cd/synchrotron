-- v5: clusters registered through the REST API.
--
-- The kube crate's in-memory ClusterRegistry is the runtime authority
-- for live clients; this table is the persistent registration store
-- the API uses for CRUD. `bearer_token` is the only sensitive column
-- and never round-trips through API responses (handlers mask it).
--
-- `auth_source` is one of: 'kubeconfig', 'in_cluster', 'default'. The
-- meaning of `kubeconfig_path` and `context` depends on the source —
-- both are unused for `in_cluster`. `labels_json` is a free-form JSON
-- object (e.g. `{"region":"us-east","tier":"prod"}`) for operators.
CREATE TABLE IF NOT EXISTS clusters (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    auth_source TEXT NOT NULL CHECK (auth_source IN ('kubeconfig', 'in_cluster', 'default')),
    kubeconfig_path TEXT,
    context TEXT,
    bearer_token TEXT,
    labels_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations (version) VALUES (5);
