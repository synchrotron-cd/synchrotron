use rusqlite::Connection;
use tracing::info;

const SCHEMA_V1: &str = include_str!("schema.sql");
const SCHEMA_V2: &str = include_str!("schema_v2.sql");

/// Run all pending migrations.
pub fn run_migrations(conn: &Connection) -> rusqlite::Result<()> {
    let current = get_current_version(conn);

    if current < 1 {
        info!("applying migration v1: initial schema");
        conn.execute_batch(SCHEMA_V1)?;
    }
    if current < 2 {
        info!("applying migration v2: app manifest cache");
        conn.execute_batch(SCHEMA_V2)?;
    }

    Ok(())
}

fn get_current_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}
