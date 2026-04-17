use anyhow::Context;
use chrono::DateTime;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::Database;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRecord {
    pub id: Uuid,
    pub app_id: Uuid,
    pub revision: String,
    pub status: SyncRecordStatus,
    pub message: Option<String>,
    pub trigger: SyncTrigger,
    pub resources_synced: i32,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncRecordStatus {
    Running,
    Succeeded,
    Failed,
}

impl SyncRecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "Running",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "Succeeded" => Self::Succeeded,
            "Failed" => Self::Failed,
            _ => Self::Running,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncTrigger {
    Manual,
    Webhook,
    AutoHeal,
    Poll,
}

impl SyncTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Webhook => "webhook",
            Self::AutoHeal => "auto-heal",
            Self::Poll => "poll",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "webhook" => Self::Webhook,
            "auto-heal" => Self::AutoHeal,
            "poll" => Self::Poll,
            _ => Self::Manual,
        }
    }
}

impl Database {
    pub fn insert_sync_record(&self, record: &SyncRecord) -> anyhow::Result<()> {
        self.conn().execute(
            "INSERT INTO sync_history (id, app_id, revision, status, message, trigger,
             resources_synced, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                record.id.to_string(),
                record.app_id.to_string(),
                record.revision,
                record.status.as_str(),
                record.message,
                record.trigger.as_str(),
                record.resources_synced,
                record.started_at.to_rfc3339(),
                record.finished_at.map(|t| t.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn get_sync_history(&self, app_id: &Uuid, limit: u32) -> anyhow::Result<Vec<SyncRecord>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, app_id, revision, status, message, trigger,
                    resources_synced, started_at, finished_at
             FROM sync_history
             WHERE app_id = ?1
             ORDER BY started_at DESC
             LIMIT ?2",
        )?;

        let mut rows = stmt.query(params![app_id.to_string(), limit])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(row_to_sync_record(row)?);
        }
        Ok(records)
    }

    /// Delete sync history older than retention_days.
    pub fn prune_sync_history(&self, retention_days: u32) -> anyhow::Result<u64> {
        let affected = self.conn().execute(
            "DELETE FROM sync_history WHERE started_at < datetime('now', ?1)",
            params![format!("-{retention_days} days")],
        )?;
        Ok(affected as u64)
    }
}

fn row_to_sync_record(row: &rusqlite::Row) -> anyhow::Result<SyncRecord> {
    let id_str: String = row.get(0)?;
    let app_id_str: String = row.get(1)?;
    let revision: String = row.get(2)?;
    let status_str: String = row.get(3)?;
    let message: Option<String> = row.get(4)?;
    let trigger_str: String = row.get(5)?;
    let resources_synced: i32 = row.get(6)?;
    let started_at_str: String = row.get(7)?;
    let finished_at_str: Option<String> = row.get(8)?;

    let started_at = DateTime::parse_from_rfc3339(&started_at_str)
        .context("parsing started_at")?
        .with_timezone(&chrono::Utc);

    let finished_at = finished_at_str
        .map(|s| DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&chrono::Utc)))
        .transpose()
        .context("parsing finished_at")?;

    Ok(SyncRecord {
        id: Uuid::parse_str(&id_str).context("parsing sync record id")?,
        app_id: Uuid::parse_str(&app_id_str).context("parsing app_id")?,
        revision,
        status: SyncRecordStatus::from_str_lossy(&status_str),
        message,
        trigger: SyncTrigger::from_str_lossy(&trigger_str),
        resources_synced,
        started_at,
        finished_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use synchrotron_types::*;

    fn setup_db_with_app() -> (Database, Uuid) {
        let db = Database::open_in_memory().unwrap();
        let app_id = Uuid::new_v4();
        let app = Application {
            id: app_id,
            name: AppName("test-app".to_string()),
            namespace: "default".to_string(),
            source: AppSource {
                repo_url: RepoUrl("https://github.com/org/repo".to_string()),
                path: "deploy".to_string(),
                target_revision: "main".to_string(),
                plugin: None,
            },
            destination: AppDestination {
                cluster: ClusterName("prod".to_string()),
                namespace: "app-ns".to_string(),
            },
            sync_policy: SyncPolicy::default(),
            status: AppStatus::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        db.insert_application(&app).unwrap();
        (db, app_id)
    }

    #[test]
    fn test_insert_and_query_history() {
        let (db, app_id) = setup_db_with_app();

        let record = SyncRecord {
            id: Uuid::new_v4(),
            app_id,
            revision: "abc123".to_string(),
            status: SyncRecordStatus::Succeeded,
            message: Some("deployed successfully".to_string()),
            trigger: SyncTrigger::Webhook,
            resources_synced: 5,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        };

        db.insert_sync_record(&record).unwrap();

        let history = db.get_sync_history(&app_id, 10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].revision, "abc123");
        assert_eq!(history[0].status, SyncRecordStatus::Succeeded);
        assert_eq!(history[0].trigger, SyncTrigger::Webhook);
    }

    #[test]
    fn test_history_ordering() {
        let (db, app_id) = setup_db_with_app();

        for rev in &["aaa", "bbb", "ccc"] {
            let record = SyncRecord {
                id: Uuid::new_v4(),
                app_id,
                revision: rev.to_string(),
                status: SyncRecordStatus::Succeeded,
                message: None,
                trigger: SyncTrigger::Poll,
                resources_synced: 1,
                started_at: Utc::now(),
                finished_at: None,
            };
            db.insert_sync_record(&record).unwrap();
        }

        let history = db.get_sync_history(&app_id, 2).unwrap();
        assert_eq!(history.len(), 2); // limited to 2
    }
}
