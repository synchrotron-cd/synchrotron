//! `AppRenderer` — the cache-aware façade over [`Registry::render`].
//!
//! # Why this exists
//!
//! [`Registry::render`] is the raw plugin dispatch: given a plugin
//! name + a source-path-at-commit + render params, it produces
//! `Vec<Manifest>`. Re-running the same render is expensive (Helm
//! template, Kustomize build, Sidecar gRPC) and the inputs are
//! commit-stable. [`AppCache`] keys exactly the "same inputs"
//! shape: `(app_id, commit_hash, params_hash)`.
//!
//! `AppRenderer` glues the two: try the cache, dispatch on miss,
//! cache the result, hand back the shared [`Arc<[Manifest]>`]. The
//! cache layer is also responsible for byte-budgeted LRU eviction —
//! callers don't need to manage that.
//!
//! # Slice scope (c4c)
//!
//! What this module ships:
//!   - [`AppRenderSpec`] — the inputs to one render
//!   - [`AppRenderer`] — the cached dispatch
//!   - Error mapping that distinguishes "plugin failed" from
//!     "cache miss + plugin failed", since the caller usually wants
//!     to surface the underlying [`DispatchError`].
//!
//! Wiring this into a git → render → store pipeline is slice 4
//! (server bring-up). The renderer doesn't know about git; the
//! caller is expected to pass a `source_path` that's already
//! checked out at `commit_hash`.

use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;

use crate::app_cache::{AppCache, AppCacheKey};
use crate::manifest::Manifest;
use crate::registry::{DispatchError, Registry};

/// Inputs to a single render of one app.
///
/// `commit_hash` is the value the caller resolved before checking
/// out `source_path`; it goes straight into the cache key.
/// `params` are the plugin-specific render parameters (e.g. Helm
/// values, Kustomize overlay name); they're hashed canonically for
/// the cache key.
#[derive(Debug, Clone)]
pub struct AppRenderSpec {
    pub app_id: String,
    pub commit_hash: String,
    pub plugin: String,
    pub source_path: PathBuf,
    pub params: serde_json::Value,
}

impl AppRenderSpec {
    /// Materialize the cache key this spec maps to. Useful when
    /// callers want to peek the cache without triggering a render.
    pub fn cache_key(&self) -> AppCacheKey {
        AppCacheKey {
            app_id: self.app_id.clone(),
            commit_hash: self.commit_hash.clone(),
            params_hash: AppCacheKey::hash_params(&self.params),
        }
    }
}

#[derive(Debug, Error)]
pub enum RenderError {
    /// Underlying plugin dispatch failed.
    #[error("render dispatch failed: {0}")]
    Dispatch(#[from] DispatchError),
}

/// Cache-aware renderer. Cheap to clone (just an `Arc` bump on
/// each field).
#[derive(Clone)]
pub struct AppRenderer {
    registry: Arc<Registry>,
    cache: Arc<AppCache>,
}

impl AppRenderer {
    pub fn new(registry: Arc<Registry>, cache: Arc<AppCache>) -> Self {
        Self { registry, cache }
    }

    /// Render `spec`, consulting [`AppCache`] for a hit before
    /// dispatching. Cache hits return an `Arc<[Manifest]>` refcount
    /// bump; misses produce a fresh render, store it, and return
    /// the same shared slice.
    pub async fn render(&self, spec: &AppRenderSpec) -> Result<Arc<[Manifest]>, RenderError> {
        let key = spec.cache_key();
        if let Some(hit) = self.cache.get(&key) {
            return Ok(hit);
        }
        let rendered = self
            .registry
            .render(&spec.plugin, &spec.source_path, spec.params.clone())
            .await?;
        Ok(self.cache.put(key, rendered))
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn cache(&self) -> &AppCache {
        &self.cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Registry;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Build a tiny raw-plugin tree with one ConfigMap, so the
    /// renderer has something concrete to produce. raw doesn't
    /// invoke anything external — keeps these tests pure-in-process.
    fn raw_app_tree(name: &str) -> TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let yaml = format!(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {name}\n  namespace: default\ndata:\n  hello: world\n"
        );
        std::fs::write(dir.path().join("cm.yaml"), yaml).expect("write");
        dir
    }

    fn registry_with_raw() -> Arc<Registry> {
        // Raw plugin registration goes through the YAML config in
        // production; the test config has a single `raw`-kind entry.
        Arc::new(
            Registry::from_yaml(
                r#"
plugins:
  - name: raw
    kind: raw
"#,
            )
            .expect("yaml"),
        )
    }

    fn spec(dir: &TempDir, app: &str, commit: &str) -> AppRenderSpec {
        AppRenderSpec {
            app_id: app.into(),
            commit_hash: commit.into(),
            plugin: "raw".into(),
            source_path: PathBuf::from(dir.path()),
            params: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn render_miss_then_hit_reuses_cache_entry() {
        let dir = raw_app_tree("cm-a");
        let renderer = AppRenderer::new(registry_with_raw(), Arc::new(AppCache::with_defaults()));

        let first = renderer
            .render(&spec(&dir, "app-a", "abc123"))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].name, "cm-a");

        // Second call → hit. Different commits would re-render;
        // same commit + params shouldn't.
        let stats_before = renderer.cache().stats();
        let second = renderer
            .render(&spec(&dir, "app-a", "abc123"))
            .await
            .unwrap();
        let stats_after = renderer.cache().stats();
        assert!(
            Arc::ptr_eq(&first, &second),
            "cache hit should hand back the same Arc"
        );
        assert_eq!(stats_after.hits, stats_before.hits + 1);
    }

    #[tokio::test]
    async fn distinct_commits_produce_distinct_cache_entries() {
        let dir = raw_app_tree("cm-a");
        let renderer = AppRenderer::new(registry_with_raw(), Arc::new(AppCache::with_defaults()));

        let r1 = renderer.render(&spec(&dir, "app-a", "abc")).await.unwrap();
        let r2 = renderer.render(&spec(&dir, "app-a", "def")).await.unwrap();
        // Same source path → same content, but cache keys differ
        // by commit, so they're stored separately; ptr_eq is false.
        assert!(!Arc::ptr_eq(&r1, &r2));
        // Both populate the cache → two entries, two misses.
        let stats = renderer.cache().stats();
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.hits, 0);
    }

    #[tokio::test]
    async fn distinct_params_produce_distinct_cache_entries() {
        let dir = raw_app_tree("cm-a");
        let renderer = AppRenderer::new(registry_with_raw(), Arc::new(AppCache::with_defaults()));

        let mut s1 = spec(&dir, "app-a", "abc");
        let mut s2 = s1.clone();
        s1.params = serde_json::json!({"replicas": 1});
        s2.params = serde_json::json!({"replicas": 2});

        let _ = renderer.render(&s1).await.unwrap();
        let _ = renderer.render(&s2).await.unwrap();
        let stats = renderer.cache().stats();
        assert_eq!(stats.misses, 2, "differing params should miss separately");
    }

    #[tokio::test]
    async fn unknown_plugin_surfaces_dispatch_error() {
        let dir = raw_app_tree("cm-a");
        let renderer = AppRenderer::new(
            Arc::new(Registry::empty()),
            Arc::new(AppCache::with_defaults()),
        );
        let mut s = spec(&dir, "app-a", "abc");
        s.plugin = "nope".into();
        let err = renderer.render(&s).await.unwrap_err();
        assert!(matches!(
            err,
            RenderError::Dispatch(DispatchError::Unknown { .. })
        ));
    }
}
