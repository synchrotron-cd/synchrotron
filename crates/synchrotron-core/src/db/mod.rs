pub mod app_cache_repo;
pub mod app_repo;
pub mod cluster_repo;
pub mod migrations;
pub mod owned_resources_repo;
pub mod sync_history_repo;
pub mod sync_revisions_repo;

use std::path::Path;
use tracing::info;

pub use cluster_repo::{ClusterAuthSource, ClusterRegistration};
pub use sync_history_repo::{SyncRecord, SyncRecordStatus, SyncTrigger};
pub use sync_revisions_repo::{SyncRevision, DEFAULT_RETENTION};

pub struct Database {
    conn: rusqlite::Connection,
}

impl Database {
    /// Open (or create) the database at the given path.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = rusqlite::Connection::open(path)?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA synchronous = NORMAL;",
        )?;

        info!("database opened at {}", path.display());

        let db = Self { conn };
        migrations::run_migrations(&db.conn)?;

        Ok(db)
    }

    /// Open an in-memory database (for testing).
    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        let db = Self { conn };
        migrations::run_migrations(&db.conn)?;
        Ok(db)
    }

    pub fn conn(&self) -> &rusqlite::Connection {
        &self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_in_memory() {
        let db = Database::open_in_memory().expect("should open in-memory db");
        let version: i64 = db
            .conn()
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, crate::db::migrations::LATEST_VERSION);
    }

    #[test]
    fn test_tables_exist() {
        let db = Database::open_in_memory().unwrap();
        let tables: Vec<String> = {
            let mut stmt = db
                .conn()
                .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .unwrap();
            stmt.query_map([], |row| row.get(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert!(tables.contains(&"applications".to_string()));
        assert!(tables.contains(&"sync_history".to_string()));
        assert!(tables.contains(&"schema_migrations".to_string()));
    }
}
