//! Persistence layer for the app manifest cache.
//!
//! The in-memory [`AppCache`](synchrotron_plugins::AppCache) is
//! process-local and vanishes on restart. This repo is the durable
//! backing store: each render writes a row keyed on
//! `(app_id, commit_hash, params_hash)`, and on startup the
//! reconciler loads the N most recently-written rows and hands them
//! to [`AppCache::warm`](synchrotron_plugins::AppCache::warm).
//!
//! That makes cache state survive restarts for recently-active
//! apps — the acceptance criterion from h48.2. Apps that have been
//! dormant for a while fall off the warm-up set and simply re-render
//! on their next reconcile, which is a cheap miss rather than a bug.

use anyhow::Context;
use rusqlite::params;

use synchrotron_plugins::{AppCacheKey, Manifest};

use crate::db::Database;

impl Database {
    /// Insert or replace a single cache entry. Called by the
    /// reconciler after each successful render.
    pub fn save_app_cache_entry(
        &self,
        key: &AppCacheKey,
        manifests: &[Manifest],
    ) -> anyhow::Result<()> {
        let manifests_json =
            serde_json::to_string(manifests).context("serialize manifests for cache")?;
        let bytes = manifests_json.len() as i64;
        self.conn().execute(
            "INSERT OR REPLACE INTO app_cache_entries
                (app_id, commit_hash, params_hash, manifests_json, bytes, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            params![
                &key.app_id,
                &key.commit_hash,
                &key.params_hash[..],
                manifests_json,
                bytes,
            ],
        )?;
        Ok(())
    }

    /// Load the `limit` most recently-written entries. Used at
    /// startup to warm the in-memory cache; pass the result straight
    /// into [`AppCache::warm`](synchrotron_plugins::AppCache::warm).
    ///
    /// Rows are returned oldest-first so the LRU ordering after
    /// warm-up matches the persistence ordering — the most recently
    /// written entry lands most-recently-used in the cache.
    pub fn load_recent_app_cache_entries(
        &self,
        limit: usize,
    ) -> anyhow::Result<Vec<(AppCacheKey, Vec<Manifest>)>> {
        // Take the top `limit` by recency, then flip to oldest-first
        // so the caller's LRU ordering is preserved.
        let mut stmt = self.conn().prepare(
            "SELECT app_id, commit_hash, params_hash, manifests_json
             FROM (
                SELECT app_id, commit_hash, params_hash, manifests_json, updated_at
                FROM app_cache_entries
                ORDER BY updated_at DESC
                LIMIT ?1
             )
             ORDER BY updated_at ASC",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let app_id: String = row.get(0)?;
                let commit_hash: String = row.get(1)?;
                let params_hash_vec: Vec<u8> = row.get(2)?;
                let manifests_json: String = row.get(3)?;
                Ok((app_id, commit_hash, params_hash_vec, manifests_json))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut out = Vec::with_capacity(rows.len());
        for (app_id, commit_hash, params_hash_vec, manifests_json) in rows {
            let params_hash: [u8; 32] = params_hash_vec
                .as_slice()
                .try_into()
                .context("params_hash column is not 32 bytes — schema drift?")?;
            let manifests: Vec<Manifest> = serde_json::from_str(&manifests_json)
                .context("deserialize manifests from cache row")?;
            out.push((
                AppCacheKey {
                    app_id,
                    commit_hash,
                    params_hash,
                },
                manifests,
            ));
        }
        Ok(out)
    }

    /// Drop every persisted entry for an app. Invoked alongside
    /// [`AppCache::invalidate_app`](synchrotron_plugins::AppCache::invalidate_app)
    /// when an app's plugin kind changes.
    pub fn invalidate_app_cache(&self, app_id: &str) -> anyhow::Result<usize> {
        let n = self.conn().execute(
            "DELETE FROM app_cache_entries WHERE app_id = ?1",
            params![app_id],
        )?;
        Ok(n)
    }

    /// Count of persisted entries. Useful for metrics and tests.
    pub fn app_cache_entry_count(&self) -> anyhow::Result<usize> {
        let n: i64 =
            self.conn()
                .query_row("SELECT COUNT(*) FROM app_cache_entries", [], |row| {
                    row.get(0)
                })?;
        Ok(n as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::Value;
    use synchrotron_plugins::{AppCache, Gvk};

    use crate::db::Database;

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
    fn save_and_load_round_trip() {
        let db = Database::open_in_memory().unwrap();
        let k = key("app-a", "c1", 1);
        let ms = vec![manifest("one"), manifest("two")];
        db.save_app_cache_entry(&k, &ms).unwrap();

        let loaded = db.load_recent_app_cache_entries(10).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, k);
        assert_eq!(loaded[0].1.len(), 2);
        assert_eq!(loaded[0].1[0].name, "one");
        assert_eq!(loaded[0].1[1].name, "two");
    }

    #[test]
    fn upsert_overwrites_same_key() {
        let db = Database::open_in_memory().unwrap();
        let k = key("app-a", "c1", 1);
        db.save_app_cache_entry(&k, &[manifest("v1")]).unwrap();
        db.save_app_cache_entry(&k, &[manifest("v2"), manifest("v3")])
            .unwrap();
        assert_eq!(db.app_cache_entry_count().unwrap(), 1);
        let loaded = db.load_recent_app_cache_entries(10).unwrap();
        assert_eq!(loaded[0].1.len(), 2);
        assert_eq!(loaded[0].1[0].name, "v2");
    }

    #[test]
    fn load_recent_honors_limit_and_recency_order() {
        let db = Database::open_in_memory().unwrap();
        // Insert three entries with distinct updated_at — sleep is
        // the cheap way to separate the strftime('%Y-%m-%dT%H:%M:%fZ')
        // values. Millisecond precision is enough.
        db.save_app_cache_entry(&key("a", "c", 1), &[manifest("a")])
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        db.save_app_cache_entry(&key("b", "c", 1), &[manifest("b")])
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        db.save_app_cache_entry(&key("d", "c", 1), &[manifest("d")])
            .unwrap();

        let loaded = db.load_recent_app_cache_entries(2).unwrap();
        // We asked for top 2, returned oldest-first: [b, d].
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].0.app_id, "b");
        assert_eq!(loaded[1].0.app_id, "d");
    }

    #[test]
    fn invalidate_app_cache_removes_all_rows_for_app() {
        let db = Database::open_in_memory().unwrap();
        db.save_app_cache_entry(&key("app-a", "c1", 1), &[manifest("a1")])
            .unwrap();
        db.save_app_cache_entry(&key("app-a", "c2", 1), &[manifest("a2")])
            .unwrap();
        db.save_app_cache_entry(&key("app-b", "c1", 1), &[manifest("b1")])
            .unwrap();

        let removed = db.invalidate_app_cache("app-a").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(db.app_cache_entry_count().unwrap(), 1);

        let loaded = db.load_recent_app_cache_entries(10).unwrap();
        assert_eq!(loaded[0].0.app_id, "app-b");
    }

    #[test]
    fn warm_from_persistence_produces_hits() {
        let db = Database::open_in_memory().unwrap();
        db.save_app_cache_entry(&key("app-a", "c1", 1), &[manifest("a1")])
            .unwrap();
        db.save_app_cache_entry(&key("app-b", "c1", 1), &[manifest("b1")])
            .unwrap();

        let cache = AppCache::new(100, 1 << 20);
        cache.warm(db.load_recent_app_cache_entries(10).unwrap());

        assert!(cache.get(&key("app-a", "c1", 1)).is_some());
        assert!(cache.get(&key("app-b", "c1", 1)).is_some());
        let s = cache.stats();
        assert_eq!(s.entries, 2);
        assert_eq!(s.hits, 2);
    }

    #[test]
    fn migration_v2_applied() {
        let db = Database::open_in_memory().unwrap();
        let v: i64 = db
            .conn()
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(v, 2);
    }
}
