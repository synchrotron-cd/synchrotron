//! Schema migration runner.
//!
//! Forward-only migrations applied at process startup. The runner is:
//!
//! - **Versioned.** Every migration ends with an
//!   `INSERT OR IGNORE INTO schema_migrations` row, so the maximum
//!   recorded version is the canonical schema state.
//! - **Idempotent.** Each migration's DDL uses `IF NOT EXISTS` and
//!   the version-row insert uses `OR IGNORE`. Re-running the runner
//!   against an already-migrated DB is a no-op. Re-applying an
//!   individual migration's SQL by hand is safe.
//! - **Atomic per migration.** Each migration runs inside an
//!   explicit transaction, so a failing statement leaves the prior
//!   schema untouched rather than half-applied.
//!
//! Downgrades are not supported in-process — see
//! [`docs/upgrades.md`](../../../../docs/upgrades.md). Restoring an
//! older schema means restoring the SQLite file from a backup taken
//! before the upgrade.

use rusqlite::Connection;
use tracing::info;

const SCHEMA_V1: &str = include_str!("schema.sql");
const SCHEMA_V2: &str = include_str!("schema_v2.sql");
const SCHEMA_V3: &str = include_str!("schema_v3.sql");
const SCHEMA_V4: &str = include_str!("schema_v4.sql");
const SCHEMA_V5: &str = include_str!("schema_v5.sql");

/// Highest schema version this binary knows about. The DB must be
/// at exactly this version after [`run_migrations`] returns.
pub const LATEST_VERSION: i64 = 5;

/// Run all pending migrations. Safe to call against a fresh DB or
/// one already at the latest version.
pub fn run_migrations(conn: &Connection) -> rusqlite::Result<()> {
    let current = get_current_version(conn);

    if current < 1 {
        info!("applying migration v1: initial schema");
        apply_migration(conn, SCHEMA_V1)?;
    }
    if current < 2 {
        info!("applying migration v2: app manifest cache");
        apply_migration(conn, SCHEMA_V2)?;
    }
    if current < 3 {
        info!("applying migration v3: app owned-resources tracking");
        apply_migration(conn, SCHEMA_V3)?;
    }
    if current < 4 {
        info!("applying migration v4: app sync history");
        apply_migration(conn, SCHEMA_V4)?;
    }
    if current < 5 {
        info!("applying migration v5: cluster registrations");
        apply_migration(conn, SCHEMA_V5)?;
    }

    Ok(())
}

/// Run one migration's SQL inside a transaction. The transaction
/// commits only if every statement succeeds; on error the DB is
/// rolled back to its prior state and the caller sees the
/// underlying `rusqlite::Error`.
fn apply_migration(conn: &Connection, sql: &str) -> rusqlite::Result<()> {
    conn.execute_batch("BEGIN")?;
    match conn.execute_batch(sql) {
        Ok(()) => conn.execute_batch("COMMIT"),
        Err(e) => {
            // Best-effort rollback; if it itself fails, surface the
            // original error which is the actionable one.
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn get_current_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn fresh_db_lands_at_latest_version() {
        let conn = fresh_conn();
        run_migrations(&conn).unwrap();
        assert_eq!(get_current_version(&conn), LATEST_VERSION);
    }

    /// Re-running the migration runner is a no-op. Without
    /// `INSERT OR IGNORE`, the schema_migrations primary-key
    /// constraint would surface here.
    #[test]
    fn run_migrations_is_idempotent() {
        let conn = fresh_conn();
        run_migrations(&conn).unwrap();
        run_migrations(&conn).unwrap();
        run_migrations(&conn).unwrap();
        assert_eq!(get_current_version(&conn), LATEST_VERSION);
    }

    /// Re-executing an individual migration's SQL by hand against a
    /// DB already at that version is also a no-op. This protects
    /// operators who replay a schema file out-of-band, e.g. while
    /// recovering a partially-restored backup.
    #[test]
    fn raw_schema_sql_is_idempotent() {
        let conn = fresh_conn();
        run_migrations(&conn).unwrap();
        // Apply each schema file twice — once already, once now.
        conn.execute_batch(SCHEMA_V1).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute_batch(SCHEMA_V4).unwrap();
        assert_eq!(get_current_version(&conn), LATEST_VERSION);
    }

    /// Simulates an operator upgrading from a binary that only knew
    /// schema v1: start the DB at v1, then run the current binary's
    /// migrations and verify v2 lands. This is the "tested across
    /// two release versions" guarantee — an older release's persisted
    /// state remains readable by a newer release.
    #[test]
    fn v1_to_v2_upgrade_path_lands_at_latest() {
        let conn = fresh_conn();
        // Stop at v1.
        apply_migration(&conn, SCHEMA_V1).unwrap();
        assert_eq!(get_current_version(&conn), 1);
        // Insert a row that v1 knows about so we can prove it
        // survives the v2 migration intact.
        conn.execute(
            "INSERT INTO applications (id, name, namespace, repo_url, path, dest_cluster, dest_namespace) \
             VALUES ('a', 'web', 'default', 'https://example/r.git', '.', 'in-cluster', 'web')",
            [],
        )
        .unwrap();

        // Now run the current binary's full migration set.
        run_migrations(&conn).unwrap();
        assert_eq!(get_current_version(&conn), LATEST_VERSION);

        // v1 data still readable.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM applications WHERE id = 'a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // v2 table exists.
        let v2_table: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='app_cache_entries'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v2_table, "app_cache_entries");
    }

    /// A failing statement in a migration must not leave the DB at a
    /// half-applied state. We force a failure by running a hand-rolled
    /// migration body whose later statement is invalid; the version
    /// should not advance and prior state should be intact.
    #[test]
    fn failing_migration_rolls_back_atomically() {
        let conn = fresh_conn();
        run_migrations(&conn).unwrap();
        let before = get_current_version(&conn);

        // Construct a synthetic future migration that creates a
        // table then immediately fails. The transaction should roll
        // back the CREATE so the table is gone afterwards. The
        // version (99) is deliberately well above LATEST_VERSION so
        // the test isn't accidentally exercising a real migration.
        let bad_sql = "
            CREATE TABLE IF NOT EXISTS bogus_v99 (id INTEGER);
            SELECT this_is_not_a_real_function();
            INSERT OR IGNORE INTO schema_migrations (version) VALUES (99);
        ";
        let err = apply_migration(&conn, bad_sql);
        assert!(err.is_err());
        assert_eq!(get_current_version(&conn), before);

        let surviving: Option<String> = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bogus_v99'",
                [],
                |r| r.get(0),
            )
            .ok();
        assert!(
            surviving.is_none(),
            "rolled-back CREATE TABLE should not persist"
        );
    }

    /// A DB that stopped at v2 (older binary) must reach v3 with v2
    /// data intact. Same shape as the v1→v2 test; together they prove
    /// the migration ladder works one rung at a time.
    #[test]
    fn v2_to_v3_upgrade_path_preserves_v2_data() {
        let conn = fresh_conn();
        apply_migration(&conn, SCHEMA_V1).unwrap();
        apply_migration(&conn, SCHEMA_V2).unwrap();
        assert_eq!(get_current_version(&conn), 2);

        // Insert an applications row (v1) and an app_cache_entries
        // row (v2) so we can prove both survive v3.
        conn.execute(
            "INSERT INTO applications (id, name, namespace, repo_url, path, dest_cluster, dest_namespace) \
             VALUES ('a', 'web', 'default', 'https://example/r.git', '.', 'in-cluster', 'web')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO app_cache_entries (app_id, commit_hash, params_hash, manifests_json, bytes) \
             VALUES ('a', 'deadbeef', X'00', '[]', 2)",
            [],
        )
        .unwrap();

        run_migrations(&conn).unwrap();
        assert_eq!(get_current_version(&conn), LATEST_VERSION);

        let v3_table: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='app_owned_resources'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v3_table, "app_owned_resources");

        let cache_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM app_cache_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cache_count, 1, "v2 cache row should survive v3");
    }
}
