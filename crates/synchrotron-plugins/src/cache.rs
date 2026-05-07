//! In-memory LRU cache for plugin render outputs.
//!
//! Keyed by `(plugin_id, plugin_version, input_hash)` where
//! `input_hash` is a SHA-256 over the inputs that could change
//! rendering output — source commit, params, and any extra bytes the
//! caller considers part of "inputs" (chart lock file digest, etc.).
//! Mixing `plugin_version` into the key means a plugin upgrade
//! invalidates its old entries without any explicit purge.
//!
//! Size is bounded along two axes:
//! - **Count**: a hard cap on number of entries. The LRU evicts the
//!   oldest when exceeded.
//! - **Bytes**: an approximate cap on total payload bytes, measured
//!   at insert time via `serde_json::to_vec(body).len()` summed over
//!   each manifest. When total exceeds the cap the LRU repeatedly
//!   evicts oldest entries until it fits. "Approximate" because we
//!   don't account for Rust-internal overhead — the number is large
//!   enough that the overhead is noise.
//!
//! Hits/misses/evictions are surfaced via [`Cache::stats`]. They are
//! backed by atomics so readers don't block the hot path.
//!
//! This module is transport-agnostic: it doesn't know about the
//! local or sidecar runtimes. Wire-up into [`crate::Registry`] is a
//! follow-up slice.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lru::LruCache;
use sha2::{Digest, Sha256};

use crate::manifest::Manifest;

/// Inputs that uniquely identify a render output.
///
/// `plugin_version` is the version string the plugin reports during
/// the `initialize` handshake. Rolling it forward automatically
/// invalidates every previously-cached entry for that plugin, which
/// is the acceptance criterion "Invalidated on plugin version change".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub plugin_id: String,
    pub plugin_version: String,
    pub input_hash: [u8; 32],
}

impl CacheKey {
    /// Compute `input_hash` from a commit sha, params, and extra
    /// salt bytes (e.g. hashed chart dependencies). Callers that
    /// want a different input model can pass a pre-computed hash
    /// into [`CacheKey`] directly.
    pub fn hash_inputs(source_commit: &str, params: &serde_json::Value, extra: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(source_commit.as_bytes());
        // serde_json::to_vec is deterministic for Value — map keys
        // are iterated in insertion order (BTreeMap-backed).
        let params_bytes = serde_json::to_vec(params).expect("serialize params");
        h.update(&params_bytes);
        h.update(extra);
        h.finalize().into()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub bytes: usize,
}

pub struct Cache {
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    max_entries: NonZeroUsize,
    max_bytes: usize,
}

struct Inner {
    lru: LruCache<CacheKey, Entry>,
    total_bytes: usize,
}

struct Entry {
    /// `Arc<[Manifest]>` so hits return a refcount bump instead of
    /// cloning every manifest. Plugin renders are deeply immutable
    /// once produced — callers iterate, never mutate — so shared
    /// ownership is the obvious shape.
    manifests: Arc<[Manifest]>,
    bytes: usize,
}

impl Cache {
    /// Build a cache bounded by both entry count and approximate
    /// bytes. Both bounds are enforced on every insert.
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
            max_entries: cap,
            max_bytes,
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<Arc<[Manifest]>> {
        let mut guard = self.inner.lock().expect("cache mutex poisoned");
        match guard.lru.get(key) {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(Arc::clone(&entry.manifests))
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a render result. Evicts older entries as needed to
    /// honor both the count and byte bounds.
    pub fn put(&self, key: CacheKey, manifests: Vec<Manifest>) {
        let bytes = estimate_bytes(&manifests);
        let entry = Entry {
            manifests: Arc::from(manifests),
            bytes,
        };

        let mut guard = self.inner.lock().expect("cache mutex poisoned");

        // Overwriting an existing key replaces its byte accounting.
        if let Some(old) = guard.lru.pop(&key) {
            guard.total_bytes = guard.total_bytes.saturating_sub(old.bytes);
        }

        // LRU::put returns the evicted (key, value) if the count cap
        // bites on this insertion.
        if let Some((_, evicted)) = guard.lru.push(key, entry) {
            guard.total_bytes = guard.total_bytes.saturating_sub(evicted.bytes);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        guard.total_bytes = guard.total_bytes.saturating_add(bytes);

        // Now trim by bytes. Keep popping the least-recently-used
        // until we're back under the cap, but never empty the cache
        // entirely — a single oversized entry is allowed to stay so
        // that the caller isn't silently locked out.
        while guard.total_bytes > self.max_bytes && guard.lru.len() > 1 {
            if let Some((_, evicted)) = guard.lru.pop_lru() {
                guard.total_bytes = guard.total_bytes.saturating_sub(evicted.bytes);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    pub fn stats(&self) -> CacheStats {
        let guard = self.inner.lock().expect("cache mutex poisoned");
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries: guard.lru.len(),
            bytes: guard.total_bytes,
        }
    }

    pub fn clear(&self) {
        let mut guard = self.inner.lock().expect("cache mutex poisoned");
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
            body: body.into(),
        }
    }

    fn key(plugin: &str, version: &str, salt: u8) -> CacheKey {
        CacheKey {
            plugin_id: plugin.into(),
            plugin_version: version.into(),
            input_hash: [salt; 32],
        }
    }

    #[test]
    fn miss_on_empty_cache() {
        let c = Cache::new(10, 1 << 20);
        assert!(c.get(&key("p", "1", 0)).is_none());
        assert_eq!(c.stats().misses, 1);
        assert_eq!(c.stats().hits, 0);
    }

    #[test]
    fn hit_after_put() {
        let c = Cache::new(10, 1 << 20);
        let k = key("p", "1", 0);
        c.put(k.clone(), vec![manifest("x")]);
        let got = c.get(&k).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "x");
        assert_eq!(c.stats().hits, 1);
        assert_eq!(c.stats().entries, 1);
    }

    #[test]
    fn plugin_version_bump_invalidates() {
        let c = Cache::new(10, 1 << 20);
        c.put(key("helm", "1.0", 0), vec![manifest("old")]);
        // Same inputs, different plugin version — must miss.
        assert!(c.get(&key("helm", "2.0", 0)).is_none());
    }

    #[test]
    fn count_eviction_drops_lru() {
        let c = Cache::new(2, 1 << 20);
        c.put(key("p", "1", 1), vec![manifest("a")]);
        c.put(key("p", "1", 2), vec![manifest("b")]);
        c.put(key("p", "1", 3), vec![manifest("c")]);
        // "a" should be gone.
        assert!(c.get(&key("p", "1", 1)).is_none());
        assert!(c.get(&key("p", "1", 2)).is_some());
        assert!(c.get(&key("p", "1", 3)).is_some());
        assert_eq!(c.stats().entries, 2);
        assert!(c.stats().evictions >= 1);
    }

    #[test]
    fn byte_eviction_kicks_in() {
        // Cap at ~100 bytes — each ConfigMap serializes to ~60 bytes
        // so the second insert should evict the first.
        let c = Cache::new(100, 100);
        c.put(key("p", "1", 1), vec![manifest("aaaaaa")]);
        c.put(key("p", "1", 2), vec![manifest("bbbbbb")]);
        let stats = c.stats();
        assert!(stats.bytes <= 100 || stats.entries == 1, "stats={stats:?}");
        assert!(stats.evictions >= 1);
    }

    #[test]
    fn oversized_single_entry_stays() {
        // A single entry larger than max_bytes is kept — otherwise
        // the caller gets silently locked out of caching at all.
        let c = Cache::new(10, 1);
        c.put(key("p", "1", 1), vec![manifest("huge")]);
        assert!(c.get(&key("p", "1", 1)).is_some());
    }

    #[test]
    fn overwrite_updates_bytes_without_growing_count() {
        let c = Cache::new(10, 1 << 20);
        let k = key("p", "1", 1);
        c.put(k.clone(), vec![manifest("small")]);
        let bytes_before = c.stats().bytes;
        c.put(k.clone(), vec![manifest("small"), manifest("more")]);
        let stats = c.stats();
        assert_eq!(stats.entries, 1);
        assert!(stats.bytes > bytes_before);
    }

    #[test]
    fn lru_ordering_honors_recent_reads() {
        let c = Cache::new(2, 1 << 20);
        c.put(key("p", "1", 1), vec![manifest("a")]);
        c.put(key("p", "1", 2), vec![manifest("b")]);
        // Touch "a" so "b" becomes LRU.
        let _ = c.get(&key("p", "1", 1));
        c.put(key("p", "1", 3), vec![manifest("c")]);
        assert!(c.get(&key("p", "1", 1)).is_some());
        assert!(c.get(&key("p", "1", 2)).is_none());
        assert!(c.get(&key("p", "1", 3)).is_some());
    }

    #[test]
    fn hash_inputs_is_deterministic_and_sensitive() {
        let a = CacheKey::hash_inputs("abc123", &serde_json::json!({"x": 1}), b"");
        let b = CacheKey::hash_inputs("abc123", &serde_json::json!({"x": 1}), b"");
        assert_eq!(a, b);
        let c = CacheKey::hash_inputs("abc123", &serde_json::json!({"x": 2}), b"");
        assert_ne!(a, c);
        let d = CacheKey::hash_inputs("def456", &serde_json::json!({"x": 1}), b"");
        assert_ne!(a, d);
    }
}
