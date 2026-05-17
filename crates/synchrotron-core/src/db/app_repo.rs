use anyhow::Context;
use chrono::DateTime;
use rusqlite::params;
use uuid::Uuid;

use synchrotron_types::{
    AppDestination, AppName, AppSource, AppStatus, Application, ClusterName, HealthStatusCode,
    PluginRef, RepoUrl, SyncPolicy, SyncStatusCode,
};

use crate::db::Database;

impl Database {
    pub fn insert_application(&self, app: &Application) -> anyhow::Result<()> {
        let plugin_json = app
            .source
            .plugin
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let sync_policy_json = serde_json::to_string(&app.sync_policy)?;

        self.conn().execute(
            "INSERT INTO applications (
                id, name, namespace, repo_url, path, target_revision,
                plugin_config, dest_cluster, dest_namespace, sync_policy,
                sync_status, health_status, health_message,
                last_synced_at, last_synced_revision,
                created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                app.id.to_string(),
                app.name.0,
                app.namespace,
                app.source.repo_url.0,
                app.source.path,
                app.source.target_revision,
                plugin_json,
                app.destination.cluster.0,
                app.destination.namespace,
                sync_policy_json,
                app.status.sync.as_str(),
                app.status.health.as_str(),
                app.status.health_message,
                app.status.last_synced_at.map(|t| t.to_rfc3339()),
                app.status.last_synced_revision,
                app.created_at.to_rfc3339(),
                app.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_application(&self, name: &str) -> anyhow::Result<Option<Application>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, name, namespace, repo_url, path, target_revision,
                    plugin_config, dest_cluster, dest_namespace, sync_policy,
                    sync_status, health_status, health_message,
                    last_synced_at, last_synced_revision,
                    created_at, updated_at
             FROM applications WHERE name = ?1",
        )?;

        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_application(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_applications(&self) -> anyhow::Result<Vec<Application>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, name, namespace, repo_url, path, target_revision,
                    plugin_config, dest_cluster, dest_namespace, sync_policy,
                    sync_status, health_status, health_message,
                    last_synced_at, last_synced_revision,
                    created_at, updated_at
             FROM applications ORDER BY name",
        )?;

        let mut rows = stmt.query([])?;
        let mut apps = Vec::new();
        while let Some(row) = rows.next()? {
            apps.push(row_to_application(row)?);
        }
        Ok(apps)
    }

    pub fn delete_application(&self, name: &str) -> anyhow::Result<bool> {
        let affected = self
            .conn()
            .execute("DELETE FROM applications WHERE name = ?1", params![name])?;
        Ok(affected > 0)
    }

    pub fn update_application_status(
        &self,
        name: &str,
        sync_status: &SyncStatusCode,
        health_status: &HealthStatusCode,
        health_message: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let affected = self.conn().execute(
            "UPDATE applications SET sync_status = ?1, health_status = ?2,
             health_message = ?3, updated_at = ?4
             WHERE name = ?5",
            params![
                sync_status.as_str(),
                health_status.as_str(),
                health_message,
                now,
                name
            ],
        )?;
        Ok(affected > 0)
    }

    /// Update only the sync-side status: `sync_status`,
    /// `last_synced_at`, and optionally `last_synced_revision`.
    /// Health columns are left untouched. Used by the SyncOutcome
    /// writer so the two event streams (sync, health) don't race.
    pub fn update_application_sync(
        &self,
        name: &str,
        sync_status: &SyncStatusCode,
        last_synced_revision: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let affected = self.conn().execute(
            "UPDATE applications
             SET sync_status = ?1,
                 last_synced_at = ?2,
                 last_synced_revision = COALESCE(?3, last_synced_revision),
                 updated_at = ?2
             WHERE name = ?4",
            params![sync_status.as_str(), now, last_synced_revision, name],
        )?;
        Ok(affected > 0)
    }

    /// Update only the health-side status. Sync columns are left
    /// untouched. Counterpart to [`Self::update_application_sync`].
    pub fn update_application_health(
        &self,
        name: &str,
        health_status: &HealthStatusCode,
        health_message: Option<&str>,
    ) -> anyhow::Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let affected = self.conn().execute(
            "UPDATE applications
             SET health_status = ?1, health_message = ?2, updated_at = ?3
             WHERE name = ?4",
            params![health_status.as_str(), health_message, now, name],
        )?;
        Ok(affected > 0)
    }
}

fn row_to_application(row: &rusqlite::Row) -> anyhow::Result<Application> {
    let id_str: String = row.get(0)?;
    let name: String = row.get(1)?;
    let namespace: String = row.get(2)?;
    let repo_url: String = row.get(3)?;
    let path: String = row.get(4)?;
    let target_revision: String = row.get(5)?;
    let plugin_json: Option<String> = row.get(6)?;
    let dest_cluster: String = row.get(7)?;
    let dest_namespace: String = row.get(8)?;
    let sync_policy_json: String = row.get(9)?;
    let sync_status_str: String = row.get(10)?;
    let health_status_str: String = row.get(11)?;
    let health_message: Option<String> = row.get(12)?;
    let last_synced_at_str: Option<String> = row.get(13)?;
    let last_synced_revision: Option<String> = row.get(14)?;
    let created_at_str: String = row.get(15)?;
    let updated_at_str: String = row.get(16)?;

    let plugin: Option<PluginRef> = plugin_json
        .map(|j| serde_json::from_str(&j))
        .transpose()
        .context("deserializing plugin_config")?;

    let sync_policy: SyncPolicy =
        serde_json::from_str(&sync_policy_json).context("deserializing sync_policy")?;

    let last_synced_at = last_synced_at_str
        .map(|s| DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&chrono::Utc)))
        .transpose()
        .context("parsing last_synced_at")?;

    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .context("parsing created_at")?
        .with_timezone(&chrono::Utc);

    let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
        .context("parsing updated_at")?
        .with_timezone(&chrono::Utc);

    Ok(Application {
        id: Uuid::parse_str(&id_str).context("parsing application id")?,
        name: AppName(name),
        namespace,
        source: AppSource {
            repo_url: RepoUrl(repo_url),
            path,
            target_revision,
            plugin,
        },
        destination: AppDestination {
            cluster: ClusterName(dest_cluster),
            namespace: dest_namespace,
        },
        sync_policy,
        status: AppStatus {
            sync: SyncStatusCode::from_str_lossy(&sync_status_str),
            health: HealthStatusCode::from_str_lossy(&health_status_str),
            health_message,
            last_synced_at,
            last_synced_revision,
        },
        created_at,
        updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_test_app(name: &str) -> Application {
        Application {
            id: Uuid::new_v4(),
            name: AppName(name.to_string()),
            namespace: "synchrotron-system".to_string(),
            source: AppSource {
                repo_url: RepoUrl("https://github.com/org/repo".to_string()),
                path: "deploy/app".to_string(),
                target_revision: "main".to_string(),
                plugin: None,
            },
            destination: AppDestination {
                cluster: ClusterName("production".to_string()),
                namespace: "default".to_string(),
            },
            sync_policy: SyncPolicy::default(),
            status: AppStatus::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_insert_and_get() {
        let db = Database::open_in_memory().unwrap();
        let app = make_test_app("my-app");

        db.insert_application(&app).unwrap();
        let fetched = db.get_application("my-app").unwrap().unwrap();

        assert_eq!(fetched.name.0, "my-app");
        assert_eq!(fetched.source.repo_url.0, "https://github.com/org/repo");
        assert_eq!(fetched.destination.cluster.0, "production");
    }

    #[test]
    fn test_get_nonexistent() {
        let db = Database::open_in_memory().unwrap();
        let result = db.get_application("nope").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_applications() {
        let db = Database::open_in_memory().unwrap();
        db.insert_application(&make_test_app("app-b")).unwrap();
        db.insert_application(&make_test_app("app-a")).unwrap();

        let apps = db.list_applications().unwrap();
        assert_eq!(apps.len(), 2);
        assert_eq!(apps[0].name.0, "app-a"); // ordered by name
        assert_eq!(apps[1].name.0, "app-b");
    }

    #[test]
    fn test_delete() {
        let db = Database::open_in_memory().unwrap();
        db.insert_application(&make_test_app("doomed")).unwrap();
        assert!(db.delete_application("doomed").unwrap());
        assert!(!db.delete_application("doomed").unwrap());
        assert!(db.get_application("doomed").unwrap().is_none());
    }

    #[test]
    fn test_update_status() {
        let db = Database::open_in_memory().unwrap();
        db.insert_application(&make_test_app("status-app")).unwrap();

        db.update_application_status(
            "status-app",
            &SyncStatusCode::Synced,
            &HealthStatusCode::Healthy,
            Some("all good"),
        )
        .unwrap();

        let app = db.get_application("status-app").unwrap().unwrap();
        assert_eq!(app.status.sync, SyncStatusCode::Synced);
        assert_eq!(app.status.health, HealthStatusCode::Healthy);
        assert_eq!(app.status.health_message.as_deref(), Some("all good"));
    }
}
