//! Per-app manifest snapshots for rollback.
//!
//! Distinct from [`sync_history_repo`](super::sync_history_repo),
//! which is the event log of every sync attempt. This repo stores
//! the *rendered manifests* of each successful sync so an operator
//! can roll back to a known-good revision without having to fetch
//! and re-render the source.
//!
//! ## Rollback flow
//!
//! 1. After a sync succeeds, the reconciler calls
//!    [`Database::record_sync_revision`] with the manifests it just
//!    applied. The repo trims the per-app history to the most recent
//!    `retention` rows.
//! 2. To roll back, the operator (CLI / API) calls
//!    [`Database::list_sync_revisions`] to pick a target by
//!    `(commit_hash, recorded_at)`, then
//!    [`Database::load_sync_revision`] to fetch its manifests.
//! 3. The same apply path that handles a normal sync runs against
//!    those manifests — rollback is "apply this prior snapshot",
//!    nothing more.
//!
//! ## Why not just re-render from git?
//!
//! Two reasons:
//!
//! - **Generator determinism.** A Helm chart re-rendered six months
//!   later may produce different output (chart version drift,
//!   transitive subchart updates). The snapshot pins what was
//!   actually applied.
//! - **Source availability.** A repo / chart museum / OCI registry
//!   that's offline at rollback time would block a recovery — the
//!   snapshot lets the operator restore independently.

use anyhow::Context;
use rusqlite::params;
use synchrotron_plugins::Manifest;

use crate::db::Database;

/// Default retention if a caller passes `0` or doesn't specify one.
/// Ten is a reasonable default — enough history to roll back through
/// a bad day, not so much that the DB grows unbounded for noisy apps.
pub const DEFAULT_RETENTION: usize = 10;

/// One persisted revision. `id` is monotonic per-app: the highest id
/// is the most recent recorded revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRevision {
    pub id: i64,
    pub app_id: String,
    pub commit_hash: String,
    pub params_hash: [u8; 32],
    pub recorded_at: String,
}

impl Database {
    /// Append a successful sync to the per-app revision log and trim
    /// to `retention` rows.
    ///
    /// Returns the new row's `id`. Trim runs in the same transaction
    /// so an aborted insert doesn't drop history that was about to
    /// be replaced.
    ///
    /// `retention = 0` is treated as [`DEFAULT_RETENTION`] — a caller
    /// that passes the default sentinel doesn't need to know it.
    pub fn record_sync_revision(
        &self,
        app_id: &str,
        commit_hash: &str,
        params_hash: &[u8; 32],
        manifests: &[Manifest],
        retention: usize,
    ) -> anyhow::Result<i64> {
        let n = if retention == 0 {
            DEFAULT_RETENTION
        } else {
            retention
        };
        let manifests_json =
            serde_json::to_string(manifests).context("serialize manifests for revision")?;

        let conn = self.conn();
        let savepoint = "record_sync_revision";
        conn.execute_batch(&format!("SAVEPOINT {savepoint}"))
            .context("begin savepoint")?;

        let result = (|| -> anyhow::Result<i64> {
            conn.execute(
                "INSERT INTO app_sync_revisions
                    (app_id, commit_hash, params_hash, manifests_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![app_id, commit_hash, &params_hash[..], manifests_json],
            )?;
            let new_id = conn.last_insert_rowid();

            // Trim to the most recent `n` rows for this app. Pick the
            // ids to keep via a subquery rather than a window function
            // so this works on older SQLite builds too.
            conn.execute(
                "DELETE FROM app_sync_revisions
                 WHERE app_id = ?1
                   AND id NOT IN (
                       SELECT id FROM app_sync_revisions
                       WHERE app_id = ?1
                       ORDER BY id DESC
                       LIMIT ?2
                   )",
                params![app_id, n as i64],
            )?;

            Ok(new_id)
        })();

        match result {
            Ok(id) => {
                conn.execute_batch(&format!("RELEASE {savepoint}"))?;
                Ok(id)
            }
            Err(e) => {
                let _ = conn.execute_batch(&format!("ROLLBACK TO {savepoint}"));
                let _ = conn.execute_batch(&format!("RELEASE {savepoint}"));
                Err(e)
            }
        }
    }

    /// List the persisted revisions for `app_id`, most-recent first.
    /// `limit` caps the result; `0` means no limit.
    pub fn list_sync_revisions(
        &self,
        app_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<SyncRevision>> {
        let bound = if limit == 0 { i64::MAX } else { limit as i64 };
        let mut stmt = self.conn().prepare(
            "SELECT id, app_id, commit_hash, params_hash, recorded_at
             FROM app_sync_revisions
             WHERE app_id = ?1
             ORDER BY id DESC
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![app_id, bound], |row| {
                let id: i64 = row.get(0)?;
                let app_id: String = row.get(1)?;
                let commit_hash: String = row.get(2)?;
                let params_hash_vec: Vec<u8> = row.get(3)?;
                let recorded_at: String = row.get(4)?;
                Ok((id, app_id, commit_hash, params_hash_vec, recorded_at))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut out = Vec::with_capacity(rows.len());
        for (id, app_id, commit_hash, params_hash_vec, recorded_at) in rows {
            let params_hash: [u8; 32] = params_hash_vec
                .as_slice()
                .try_into()
                .context("params_hash column is not 32 bytes — schema drift?")?;
            out.push(SyncRevision {
                id,
                app_id,
                commit_hash,
                params_hash,
                recorded_at,
            });
        }
        Ok(out)
    }

    /// Fetch a specific revision's manifests. Returns `Ok(None)` if
    /// the row was trimmed out before the rollback could pick it up,
    /// so the caller can surface a "revision no longer in history"
    /// message rather than treating it as a generic SQL error.
    pub fn load_sync_revision(
        &self,
        revision_id: i64,
    ) -> anyhow::Result<Option<(SyncRevision, Vec<Manifest>)>> {
        let row = self
            .conn()
            .query_row(
                "SELECT id, app_id, commit_hash, params_hash, manifests_json, recorded_at
                 FROM app_sync_revisions WHERE id = ?1",
                params![revision_id],
                |row| {
                    let id: i64 = row.get(0)?;
                    let app_id: String = row.get(1)?;
                    let commit_hash: String = row.get(2)?;
                    let params_hash_vec: Vec<u8> = row.get(3)?;
                    let manifests_json: String = row.get(4)?;
                    let recorded_at: String = row.get(5)?;
                    Ok((
                        id,
                        app_id,
                        commit_hash,
                        params_hash_vec,
                        manifests_json,
                        recorded_at,
                    ))
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;

        let Some((id, app_id, commit_hash, params_hash_vec, manifests_json, recorded_at)) = row
        else {
            return Ok(None);
        };
        let params_hash: [u8; 32] = params_hash_vec
            .as_slice()
            .try_into()
            .context("params_hash column is not 32 bytes — schema drift?")?;
        let manifests: Vec<Manifest> = serde_json::from_str(&manifests_json)
            .context("deserialize manifests from revision row")?;
        Ok(Some((
            SyncRevision {
                id,
                app_id,
                commit_hash,
                params_hash,
                recorded_at,
            },
            manifests,
        )))
    }

    /// Count revisions for an app. Useful for tests and metrics.
    pub fn sync_revision_count(&self, app_id: &str) -> anyhow::Result<usize> {
        let n: i64 = self.conn().query_row(
            "SELECT COUNT(*) FROM app_sync_revisions WHERE app_id = ?1",
            params![app_id],
            |row| row.get(0),
        )?;
        Ok(n as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml_ng::Value;
    use synchrotron_plugins::Gvk;

    fn db_with_app(id: &str) -> Database {
        let db = Database::open_in_memory().unwrap();
        db.conn()
            .execute(
                "INSERT INTO applications (id, name, namespace, repo_url, path, dest_cluster, dest_namespace) \
                 VALUES (?1, ?1, 'default', 'https://example/r.git', '.', 'in-cluster', ?1)",
                params![id],
            )
            .unwrap();
        db
    }

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

    #[test]
    fn record_then_load_round_trips_manifests() {
        let db = db_with_app("app-a");
        let id = db
            .record_sync_revision(
                "app-a",
                "deadbeef",
                &[1u8; 32],
                &[manifest("one"), manifest("two")],
                10,
            )
            .unwrap();
        let (rev, manifests) = db.load_sync_revision(id).unwrap().expect("revision exists");
        assert_eq!(rev.app_id, "app-a");
        assert_eq!(rev.commit_hash, "deadbeef");
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].name, "one");
        assert_eq!(manifests[1].name, "two");
    }

    #[test]
    fn retention_trims_oldest_first() {
        let db = db_with_app("app-a");
        // Record 5 with retention 3 — only the last 3 should survive.
        for i in 0..5 {
            db.record_sync_revision(
                "app-a",
                &format!("c{i}"),
                &[i as u8; 32],
                &[manifest(&format!("m{i}"))],
                3,
            )
            .unwrap();
        }
        let revs = db.list_sync_revisions("app-a", 10).unwrap();
        assert_eq!(revs.len(), 3);
        // Most-recent first.
        assert_eq!(revs[0].commit_hash, "c4");
        assert_eq!(revs[1].commit_hash, "c3");
        assert_eq!(revs[2].commit_hash, "c2");
    }

    #[test]
    fn retention_zero_falls_back_to_default() {
        let db = db_with_app("app-a");
        for i in 0..(DEFAULT_RETENTION + 2) {
            db.record_sync_revision(
                "app-a",
                &format!("c{i}"),
                &[i as u8; 32],
                &[manifest("m")],
                0,
            )
            .unwrap();
        }
        assert_eq!(
            db.sync_revision_count("app-a").unwrap(),
            DEFAULT_RETENTION,
            "retention=0 should trim to DEFAULT_RETENTION"
        );
    }

    #[test]
    fn list_limit_caps_result() {
        let db = db_with_app("app-a");
        for i in 0..5 {
            db.record_sync_revision(
                "app-a",
                &format!("c{i}"),
                &[i as u8; 32],
                &[manifest("m")],
                10,
            )
            .unwrap();
        }
        assert_eq!(db.list_sync_revisions("app-a", 2).unwrap().len(), 2);
        assert_eq!(db.list_sync_revisions("app-a", 0).unwrap().len(), 5);
    }

    #[test]
    fn revisions_for_app_a_dont_pollute_app_b() {
        let db = db_with_app("app-a");
        db.conn()
            .execute(
                "INSERT INTO applications (id, name, namespace, repo_url, path, dest_cluster, dest_namespace) \
                 VALUES ('app-b', 'b', 'default', 'https://example/r.git', '.', 'in-cluster', 'b')",
                [],
            )
            .unwrap();
        for i in 0..5 {
            db.record_sync_revision(
                "app-a",
                &format!("a{i}"),
                &[i as u8; 32],
                &[manifest("m")],
                2,
            )
            .unwrap();
            db.record_sync_revision(
                "app-b",
                &format!("b{i}"),
                &[i as u8; 32],
                &[manifest("m")],
                4,
            )
            .unwrap();
        }
        assert_eq!(db.sync_revision_count("app-a").unwrap(), 2);
        assert_eq!(db.sync_revision_count("app-b").unwrap(), 4);
    }

    #[test]
    fn load_missing_revision_returns_none() {
        let db = db_with_app("app-a");
        assert!(db.load_sync_revision(999_999).unwrap().is_none());
    }

    #[test]
    fn application_delete_cascades_revisions() {
        let db = db_with_app("app-a");
        db.record_sync_revision("app-a", "c", &[0u8; 32], &[manifest("m")], 10)
            .unwrap();
        db.conn()
            .execute("DELETE FROM applications WHERE id = 'app-a'", [])
            .unwrap();
        assert_eq!(db.sync_revision_count("app-a").unwrap(), 0);
    }

    #[test]
    fn rollback_workflow_e2e() {
        // Acceptance criteria integration test: write three revisions,
        // then "rollback" by loading an older one and confirming we
        // can recover its manifests verbatim. This is what the apply
        // path consumes during a real rollback action.
        let db = db_with_app("app-a");
        let r1 = db
            .record_sync_revision("app-a", "v1", &[1u8; 32], &[manifest("v1-cm")], 10)
            .unwrap();
        let _r2 = db
            .record_sync_revision("app-a", "v2", &[2u8; 32], &[manifest("v2-cm")], 10)
            .unwrap();
        let _r3 = db
            .record_sync_revision("app-a", "v3", &[3u8; 32], &[manifest("v3-cm")], 10)
            .unwrap();

        // Operator picks v1 from the listing.
        let listing = db.list_sync_revisions("app-a", 10).unwrap();
        assert_eq!(listing.len(), 3);
        let target = listing.iter().find(|r| r.commit_hash == "v1").unwrap();
        assert_eq!(target.id, r1);

        // Rollback fetches the manifests for re-apply.
        let (rev, manifests) = db.load_sync_revision(target.id).unwrap().unwrap();
        assert_eq!(rev.commit_hash, "v1");
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].name, "v1-cm");
    }

    #[test]
    fn migration_v4_applied() {
        let db = Database::open_in_memory().unwrap();
        let v: i64 = db
            .conn()
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(v >= 4, "v4 migration should have run; got {v}");
    }
}
