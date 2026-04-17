/// Scope of a single informer.
///
/// `namespace = None` watches across all namespaces (cluster-scoped Api).
/// `label_selector` is the primary mechanism for restricting an
/// informer to Synchrotron-managed resources (e.g.
/// `app.kubernetes.io/managed-by=synchrotron`).
#[derive(Debug, Clone, Default)]
pub struct InformerConfig {
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
}

/// Events emitted by an informer. `Restarted` carries the full set of
/// objects observed at watch-start (initial list, or the list after a
/// reconnect-driven restart) so subscribers can resync deterministically.
#[derive(Debug, Clone)]
pub enum InformerEvent<K> {
    Applied(K),
    Deleted(K),
    Restarted(Vec<K>),
}
