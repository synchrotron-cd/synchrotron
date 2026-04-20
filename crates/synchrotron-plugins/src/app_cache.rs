//! In-memory LRU cache for rendered application manifest sets.
//!
//! This is the *app-level* cache from DESIGN.md §4, distinct from
//! the plugin-output [`crate::cache`] one layer below:
//! - Plugin cache keys on `(plugin_id, plugin_version, input_hash)`
//!   and shields plugin subprocess spawns.
//! - App cache keys on `(app_id, commit_hash, params_hash)` and
//!   shields the reconciler from re-running the whole render
//!   pipeline (raw/local/sidecar + parse) when nothing has changed.
//!
//! Both ride on the same LRU + byte-bound pattern — a count cap and
//! an approximate byte cap, with the "single oversized entry stays"
//! carve-out so a big app doesn't lock itself out of the cache.
//!
//! # Invalidation
//!
//! Two invalidation paths matter:
//! - **New commit** — the caller computes a fresh key for the new
//!   commit and misses naturally. No explicit purge needed; stale
//!   entries for the old commit age out via LRU.
//! - **Plugin type change** — when an app's config switches plugin
//!   kind (e.g. raw → helm), every rendered result for that app is
//!   suspect. The caller invokes [`AppCache::invalidate_app`] which
//!   drops every entry with that `app_id`, regardless of commit.
//!
//! # Metrics
//!
//! [`AppCache::stats`] returns hit/miss/eviction/invalidation
//! counters backed by atomics. HTTP `/metrics` exposition is a
//! separate layer (issue 58v.4) and consumes these counters.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use lru::LruCache;
use sha2::{Digest, Sha256};

use crate::manifest::Manifest;

/// Default byte cap: 350 MB. Matches the acceptance criterion and
/// sized to hold the rendered manifest sets of a few hundred apps
/// without eating the host's memory budget.
pub const DEFAULT_MAX_BYTES: usize = 350 * 1024 * 1024;

/// Default entry cap. Exists mainly to prevent pathological growth
/// if every app were tiny; the byte cap is the primary bound.
pub const DEFAULT_MAX_ENTRIES: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppCacheKey {
    pub app_id: String,
    pub commit_hash: String,
    pub params_hash: [u8; 32],
}

impl AppCacheKey {
    /// Hash a `serde_json::Value` of render params to a stable
    /// 32-byte digest. Canonicalizing at the Value layer (rather
    /// than the caller's raw text) means equivalent JSON with
    /// different whitespace produces the same hash.
    pub fn hash_params(params: &serde_json::Value) -> [u8; 32] {
        let mut h = Sha256::new();
        let bytes = serde_json::to_vec(params).expect("serialize params");
        h.update(&bytes);
        h.finalize().into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub entries: usize,
    pub bytes: usize,
}

pub struct AppCache {
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    max_entries: NonZeroUsize,
    max_bytes: usize,
}

struct Inner {
    lru: LruCache<AppCacheKey, Entry>,
    total_bytes: usize,
}

struct Entry {
    manifests: Vec<Manifest>,
    bytes: usize,
}

impl AppCache {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).unwrap();
        Self {
            inner: Mutex::new(Inner {
                lru: LruCache::new(cap),
                total_bytes: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            max_entries: cap,
            max_bytes,
        }
    }

    /// Build with the 350 MB default byte cap. Tests should prefer
    /// [`AppCache::new`] with small caps to exercise eviction paths.
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, DEFAULT_MAX_BYTES)
    }

    pub fn get(&self, key: &AppCacheKey) -> Option<Vec<Manifest>> {
        let mut guard = self.inner.lock().expect("app cache mutex poisoned");
        match guard.lru.get(key) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.manifests.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn put(&self, key: AppCacheKey, manifests: Vec<Manifest>) {
        let bytes = estimate_bytes(&manifests);
        let entry = Entry { manifests, bytes };

        let mut guard = self.inner.lock().expect("app cache mutex poisoned");

        if let Some(old) = guard.lru.pop(&key) {
            guard.total_bytes = guard.total_bytes.saturating_sub(old.bytes);
        }

        if let Some((_, evicted)) = guard.lru.push(key, entry) {
            guard.total_bytes = guard.total_bytes.saturating_sub(evicted.bytes);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        guard.total_bytes = guard.total_bytes.saturating_add(bytes);

        // Trim by bytes. Same carve-out as the plugin cache: never
        // drop below one entry, so a single oversized app doesn't
        // silently disable caching for itself.
        while guard.total_bytes > self.max_bytes && guard.lru.len() > 1 {
            if let Some((_, evicted)) = guard.lru.pop_lru() {
                guard.total_bytes = guard.total_bytes.saturating_sub(evicted.bytes);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    /// Drop every entry whose key has the given `app_id`. Called
    /// when an app's plugin kind changes — old renders for that app
    /// are potentially stale regardless of commit.
    ///
    /// Returns the number of entries removed. `invalidations` in
    /// [`AppCacheStats`] increments by that count so operators can
    /// see churn caused by config changes.
    pub fn invalidate_app(&self, app_id: &str) -> usize {
        let mut guard = self.inner.lock().expect("app cache mutex poisoned");
        let keys: Vec<AppCacheKey> = guard
            .lru
            .iter()
            .filter(|(k, _)| k.app_id == app_id)
            .map(|(k, _)| k.clone())
            .collect();
        let removed = keys.len();
        for k in keys {
            if let Some(e) = guard.lru.pop(&k) {
                guard.total_bytes = guard.total_bytes.saturating_sub(e.bytes);
            }
        }
        if removed > 0 {
            self.invalidations
                .fetch_add(removed as u64, Ordering::Relaxed);
        }
        removed
    }

    pub fn stats(&self) -> AppCacheStats {
        let guard = self.inner.lock().expect("app cache mutex poisoned");
        AppCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            entries: guard.lru.len(),
            bytes: guard.total_bytes,
        }
    }

    /// Populate the cache with pre-existing entries, typically loaded
    /// from persistent storage at startup. Honors the byte cap — if
    /// the warm-up set is larger than `max_bytes`, the oldest items
    /// in the supplied list are evicted first. Does not touch
    /// hit/miss counters; warm-up isn't a user-driven lookup.
    ///
    /// The input order is treated as least-recently-used first, so
    /// pass entries sorted by most-recent activity last.
    pub fn warm<I>(&self, entries: I)
    where
        I: IntoIterator<Item = (AppCacheKey, Vec<Manifest>)>,
    {
        for (key, manifests) in entries {
            self.put(key, manifests);
        }
    }

    pub fn clear(&self) {
        let mut guard = self.inner.lock().expect("app cache mutex poisoned");
        guard.lru.clear();
        guard.total_bytes = 0;
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries.get()
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
}

fn estimate_bytes(manifests: &[Manifest]) -> usize {
    manifests
        .iter()
        .map(|m| serde_json::to_vec(&m.body).map(|v| v.len()).unwrap_or(0))
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Gvk, Manifest};
    use serde_yaml_ng::Value;

    fn manifest(name: &str) -> Manifest {
        let body: Value = serde_yaml_ng::from_str(&format!(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {name}\n"
        ))
        .unwrap();
        Manifest {
            gvk: Gvk::parse("v1", "ConfigMap"),
            name: name.into(),
            namespace: None,
            body,
        }
    }

    fn key(app: &str, commit: &str, salt: u8) -> AppCacheKey {
        AppCacheKey {
            app_id: app.into(),
            commit_hash: commit.into(),
            params_hash: [salt; 32],
        }
    }

    #[test]
    fn miss_then_hit() {
        let c = AppCache::new(10, 1 << 20);
        let k = key("app-a", "abc", 0);
        assert!(c.get(&k).is_none());
        c.put(k.clone(), vec![manifest("x")]);
        assert_eq!(c.get(&k).unwrap().len(), 1);
        let s = c.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.entries, 1);
    }

    #[test]
    fn different_commit_is_a_miss() {
        let c = AppCache::new(10, 1 << 20);
        c.put(key("app-a", "c1", 0), vec![manifest("v1")]);
        assert!(c.get(&key("app-a", "c2", 0)).is_none());
    }

    #[test]
    fn different_params_is_a_miss() {
        let c = AppCache::new(10, 1 << 20);
        c.put(key("app-a", "c1", 1), vec![manifest("v1")]);
        assert!(c.get(&key("app-a", "c1", 2)).is_none());
    }

    #[test]
    fn count_eviction_drops_lru() {
        let c = AppCache::new(2, 1 << 20);
        c.put(key("a", "c", 1), vec![manifest("a")]);
        c.put(key("b", "c", 1), vec![manifest("b")]);
        c.put(key("d", "c", 1), vec![manifest("d")]);
        assert!(c.get(&key("a", "c", 1)).is_none());
        assert_eq!(c.stats().entries, 2);
        assert!(c.stats().evictions >= 1);
    }

    #[test]
    fn byte_eviction_kicks_in() {
        let c = AppCache::new(100, 100);
        c.put(key("a", "c", 1), vec![manifest("aaaaaa")]);
        c.put(key("b", "c", 1), vec![manifest("bbbbbb")]);
        let s = c.stats();
        assert!(s.bytes <= 100 || s.entries == 1, "stats={s:?}");
        assert!(s.evictions >= 1);
    }

    #[test]
    fn oversized_single_entry_stays() {
        let c = AppCache::new(10, 1);
        c.put(key("a", "c", 1), vec![manifest("huge")]);
        assert!(c.get(&key("a", "c", 1)).is_some());
    }

    #[test]
    fn invalidate_app_drops_every_commit_for_that_app() {
        let c = AppCache::new(10, 1 << 20);
        c.put(key("app-a", "c1", 1), vec![manifest("a1")]);
        c.put(key("app-a", "c2", 1), vec![manifest("a2")]);
        c.put(key("app-b", "c1", 1), vec![manifest("b1")]);

        let removed = c.invalidate_app("app-a");
        assert_eq!(removed, 2);
        assert!(c.get(&key("app-a", "c1", 1)).is_none());
        assert!(c.get(&key("app-a", "c2", 1)).is_none());
        assert!(c.get(&key("app-b", "c1", 1)).is_some());

        let s = c.stats();
        assert_eq!(s.invalidations, 2);
        assert_eq!(s.entries, 1);
    }

    #[test]
    fn invalidate_unknown_app_is_noop() {
        let c = AppCache::new(10, 1 << 20);
        c.put(key("app-a", "c1", 1), vec![manifest("a1")]);
        assert_eq!(c.invalidate_app("missing"), 0);
        assert_eq!(c.stats().invalidations, 0);
        assert_eq!(c.stats().entries, 1);
    }

    #[test]
    fn invalidate_reclaims_byte_budget() {
        let c = AppCache::new(10, 1 << 20);
        c.put(key("app-a", "c1", 1), vec![manifest("a1")]);
        let bytes_before = c.stats().bytes;
        assert!(bytes_before > 0);
        c.invalidate_app("app-a");
        assert_eq!(c.stats().bytes, 0);
    }

    #[test]
    fn overwrite_updates_bytes_without_growing_count() {
        let c = AppCache::new(10, 1 << 20);
        let k = key("app-a", "c1", 1);
        c.put(k.clone(), vec![manifest("small")]);
        let before = c.stats().bytes;
        c.put(k.clone(), vec![manifest("small"), manifest("more")]);
        let s = c.stats();
        assert_eq!(s.entries, 1);
        assert!(s.bytes > before);
    }

    #[test]
    fn lru_ordering_honors_recent_reads() {
        let c = AppCache::new(2, 1 << 20);
        c.put(key("a", "c", 1), vec![manifest("a")]);
        c.put(key("b", "c", 1), vec![manifest("b")]);
        let _ = c.get(&key("a", "c", 1));
        c.put(key("d", "c", 1), vec![manifest("d")]);
        assert!(c.get(&key("a", "c", 1)).is_some());
        assert!(c.get(&key("b", "c", 1)).is_none());
    }

    #[test]
    fn hash_params_is_deterministic_and_sensitive() {
        let a = AppCacheKey::hash_params(&serde_json::json!({"x": 1}));
        let b = AppCacheKey::hash_params(&serde_json::json!({"x": 1}));
        assert_eq!(a, b);
        let c = AppCacheKey::hash_params(&serde_json::json!({"x": 2}));
        assert_ne!(a, c);
    }

    #[test]
    fn with_defaults_uses_constants() {
        let c = AppCache::with_defaults();
        assert_eq!(c.max_entries(), DEFAULT_MAX_ENTRIES);
        assert_eq!(c.max_bytes(), DEFAULT_MAX_BYTES);
    }

    #[test]
    fn warm_populates_cache_without_touching_hit_counters() {
        let c = AppCache::new(10, 1 << 20);
        c.warm([
            (key("a", "c1", 1), vec![manifest("a1")]),
            (key("b", "c1", 1), vec![manifest("b1")]),
        ]);
        assert!(c.get(&key("a", "c1", 1)).is_some());
        assert!(c.get(&key("b", "c1", 1)).is_some());
        let s = c.stats();
        assert_eq!(s.entries, 2);
        // Warm-up itself shouldn't count as hits — the two gets above
        // account for all the hits. Misses stay at 0 since we only
        // got keys we had warmed.
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 0);
    }

    #[test]
    fn clear_empties_state() {
        let c = AppCache::new(10, 1 << 20);
        c.put(key("a", "c", 1), vec![manifest("a")]);
        c.clear();
        let s = c.stats();
        assert_eq!(s.entries, 0);
        assert_eq!(s.bytes, 0);
    }
}
