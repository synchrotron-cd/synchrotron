//! Persistence for the per-app owned-resource set.
//!
//! Sits next to [`app_cache_repo`](super::app_cache_repo) but tracks
//! a different question: *what resources have we previously applied
//! for this app, against the live cluster?*  The reconciler writes
//! the set on each successful sync and reads it back on the next
//! sync to compute prune candidates as `previously − currently`.
//!
//! `replace_owned_resources` is delete-then-insert in a single
//! transaction so concurrent readers always see a complete owned
//! set; the prune sweep that consumes this state must never see a
//! transient empty row set or it would mass-delete every owned
//! resource.

use anyhow::Context;
use rusqlite::params;
use synchrotron_plugins::{Gvk, OwnedResource};

use crate::db::Database;

impl Database {
    /// Atomically replace the owned-set for `app_id` with `resources`.
    /// Returns the number of rows inserted (i.e. `resources.len()`).
    ///
    /// Callers should pass the *complete* current set, not a delta;
    /// the prune sweep relies on `previously_owned − currently_owned`
    /// being computable from a single load.
    pub fn replace_owned_resources(
        &self,
        app_id: &str,
        resources: &[OwnedResource],
    ) -> anyhow::Result<usize> {
        let conn = self.conn();
        let tx_savepoint = "replace_owned_resources";
        conn.execute_batch(&format!("SAVEPOINT {tx_savepoint}"))
            .context("begin savepoint")?;

        let result = (|| -> anyhow::Result<usize> {
            conn.execute(
                "DELETE FROM app_owned_resources WHERE app_id = ?1",
                params![app_id],
            )?;
            for r in resources {
                conn.execute(
                    "INSERT INTO app_owned_resources \
                       (app_id, gvk_group, gvk_version, gvk_kind, namespace, name, wave, prune_disabled) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        app_id,
                        &r.gvk.group,
                        &r.gvk.version,
                        &r.gvk.kind,
                        r.namespace.as_deref().unwrap_or(""),
                        &r.name,
                        r.wave,
                        if r.prune_disabled { 1 } else { 0 },
                    ],
                )?;
            }
            Ok(resources.len())
        })();

        match result {
            Ok(n) => {
                conn.execute_batch(&format!("RELEASE {tx_savepoint}"))?;
                Ok(n)
            }
            Err(e) => {
                let _ = conn.execute_batch(&format!("ROLLBACK TO {tx_savepoint}"));
                let _ = conn.execute_batch(&format!("RELEASE {tx_savepoint}"));
                Err(e)
            }
        }
    }

    /// Load the full owned-set for `app_id`. Order is unspecified —
    /// callers that need a sort (e.g. reverse-wave for prune) sort
    /// after loading.
    pub fn load_owned_resources(&self, app_id: &str) -> anyhow::Result<Vec<OwnedResource>> {
        let mut stmt = self.conn().prepare(
            "SELECT gvk_group, gvk_version, gvk_kind, namespace, name, wave, prune_disabled \
             FROM app_owned_resources WHERE app_id = ?1",
        )?;
        let rows = stmt.query_map(params![app_id], |row| {
            let group: String = row.get(0)?;
            let version: String = row.get(1)?;
            let kind: String = row.get(2)?;
            let namespace: String = row.get(3)?;
            let name: String = row.get(4)?;
            let wave: i32 = row.get(5)?;
            let prune_disabled: i64 = row.get(6)?;
            Ok(OwnedResource {
                gvk: Gvk {
                    group,
                    version,
                    kind,
                },
                namespace: if namespace.is_empty() {
                    None
                } else {
                    Some(namespace)
                },
                name,
                wave,
                prune_disabled: prune_disabled != 0,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn r(
        kind: &str,
        name: &str,
        ns: Option<&str>,
        wave: i32,
        prune_disabled: bool,
    ) -> OwnedResource {
        OwnedResource {
            gvk: Gvk {
                group: String::new(),
                version: "v1".into(),
                kind: kind.into(),
            },
            namespace: ns.map(str::to_string),
            name: name.into(),
            wave,
            prune_disabled,
        }
    }

    #[test]
    fn round_trips_owned_set() {
        let db = db_with_app("app-a");
        let set = vec![
            r("ConfigMap", "cm1", Some("ns"), 0, false),
            r("ConfigMap", "cm2", Some("ns"), 1, true),
            r("Namespace", "ns", None, -1, false),
        ];
        assert_eq!(db.replace_owned_resources("app-a", &set).unwrap(), 3);
        let mut got = db.load_owned_resources("app-a").unwrap();
        got.sort_by(|a, b| a.name.cmp(&b.name));
        let mut expected = set.clone();
        expected.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(got, expected);
    }

    #[test]
    fn replace_overwrites_previous_set_atomically() {
        let db = db_with_app("app-a");
        db.replace_owned_resources(
            "app-a",
            &[
                r("ConfigMap", "old1", Some("ns"), 0, false),
                r("ConfigMap", "old2", Some("ns"), 0, false),
            ],
        )
        .unwrap();
        db.replace_owned_resources("app-a", &[r("Service", "new", Some("ns"), 0, false)])
            .unwrap();
        let got = db.load_owned_resources("app-a").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "new");
        assert_eq!(got[0].gvk.kind, "Service");
    }

    #[test]
    fn cluster_scoped_round_trips_with_no_namespace() {
        let db = db_with_app("app-a");
        db.replace_owned_resources("app-a", &[r("Namespace", "platform", None, 0, false)])
            .unwrap();
        let got = db.load_owned_resources("app-a").unwrap();
        assert_eq!(got[0].namespace, None);
    }

    #[test]
    fn replace_for_app_a_does_not_touch_app_b() {
        let db = db_with_app("app-a");
        db.conn()
            .execute(
                "INSERT INTO applications (id, name, namespace, repo_url, path, dest_cluster, dest_namespace) \
                 VALUES ('app-b', 'b', 'default', 'https://example/r.git', '.', 'in-cluster', 'b')",
                [],
            )
            .unwrap();
        db.replace_owned_resources("app-a", &[r("ConfigMap", "a", Some("ns"), 0, false)])
            .unwrap();
        db.replace_owned_resources("app-b", &[r("ConfigMap", "b", Some("ns"), 0, false)])
            .unwrap();
        // Replace app-a again with a smaller set.
        db.replace_owned_resources("app-a", &[]).unwrap();
        assert!(db.load_owned_resources("app-a").unwrap().is_empty());
        assert_eq!(db.load_owned_resources("app-b").unwrap().len(), 1);
    }

    #[test]
    fn empty_set_clears_app() {
        let db = db_with_app("app-a");
        db.replace_owned_resources("app-a", &[r("ConfigMap", "x", Some("ns"), 0, false)])
            .unwrap();
        db.replace_owned_resources("app-a", &[]).unwrap();
        assert!(db.load_owned_resources("app-a").unwrap().is_empty());
    }

    #[test]
    fn application_delete_cascades_owned_set() {
        let db = db_with_app("app-a");
        db.replace_owned_resources("app-a", &[r("ConfigMap", "x", Some("ns"), 0, false)])
            .unwrap();
        db.conn()
            .execute("DELETE FROM applications WHERE id = 'app-a'", [])
            .unwrap();
        assert!(db.load_owned_resources("app-a").unwrap().is_empty());
    }
}
