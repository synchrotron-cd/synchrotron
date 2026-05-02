//! Persistent registration store for git repos managed through the
//! REST API.

use anyhow::Context;
use chrono::{DateTime, Utc};
use rusqlite::params;
use uuid::Uuid;

use crate::db::Database;

#[derive(Debug, Clone)]
pub struct RepoRegistration {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub branch: Option<String>,
    /// External secret-store pointer (e.g. Vault path). Returned as-is
    /// in API responses — the value behind the pointer is never seen
    /// by Synchrotron.
    pub credentials_secret_ref: Option<String>,
    /// Inline credential for environments without a secret store.
    /// Sensitive: never returned in API responses.
    pub password: Option<String>,
    pub labels: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Database {
    pub fn insert_repo(&self, r: &RepoRegistration) -> anyhow::Result<()> {
        let labels_json = serde_json::to_string(&r.labels)?;
        self.conn().execute(
            "INSERT INTO repos (
                id, name, url, branch, credentials_secret_ref,
                password, labels_json, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                r.id.to_string(),
                r.name,
                r.url,
                r.branch,
                r.credentials_secret_ref,
                r.password,
                labels_json,
                r.created_at.to_rfc3339(),
                r.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_repo(&self, name: &str) -> anyhow::Result<Option<RepoRegistration>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, name, url, branch, credentials_secret_ref,
                    password, labels_json, created_at, updated_at
             FROM repos WHERE name = ?1",
        )?;
        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_repo(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_repos(&self) -> anyhow::Result<Vec<RepoRegistration>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, name, url, branch, credentials_secret_ref,
                    password, labels_json, created_at, updated_at
             FROM repos ORDER BY name",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_repo(row)?);
        }
        Ok(out)
    }

    pub fn delete_repo(&self, name: &str) -> anyhow::Result<bool> {
        let n = self
            .conn()
            .execute("DELETE FROM repos WHERE name = ?1", params![name])?;
        Ok(n > 0)
    }
}

fn row_to_repo(row: &rusqlite::Row) -> anyhow::Result<RepoRegistration> {
    let id_str: String = row.get(0)?;
    let name: String = row.get(1)?;
    let url: String = row.get(2)?;
    let branch: Option<String> = row.get(3)?;
    let credentials_secret_ref: Option<String> = row.get(4)?;
    let password: Option<String> = row.get(5)?;
    let labels_json: String = row.get(6)?;
    let created_at_str: String = row.get(7)?;
    let updated_at_str: String = row.get(8)?;

    Ok(RepoRegistration {
        id: Uuid::parse_str(&id_str).context("parsing repo id")?,
        name,
        url,
        branch,
        credentials_secret_ref,
        password,
        labels: serde_json::from_str(&labels_json).context("parsing labels_json")?,
        created_at: DateTime::parse_from_rfc3339(&created_at_str)
            .context("parsing created_at")?
            .with_timezone(&Utc),
        updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
            .context("parsing updated_at")?
            .with_timezone(&Utc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> RepoRegistration {
        let now = Utc::now();
        RepoRegistration {
            id: Uuid::new_v4(),
            name: name.into(),
            url: format!("https://github.com/org/{name}.git"),
            branch: Some("main".into()),
            credentials_secret_ref: Some("vault://secret/git/org".into()),
            password: Some("inline-password".into()),
            labels: serde_json::json!({"tier": "core"}),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn insert_get_list_delete() {
        let db = Database::open_in_memory().unwrap();
        db.insert_repo(&fixture("manifests")).unwrap();
        db.insert_repo(&fixture("addons")).unwrap();

        let got = db.get_repo("manifests").unwrap().unwrap();
        assert_eq!(got.url, "https://github.com/org/manifests.git");
        assert_eq!(got.password.as_deref(), Some("inline-password"));

        let all = db.list_repos().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].name, "addons");

        assert!(db.delete_repo("addons").unwrap());
        assert!(!db.delete_repo("addons").unwrap());
    }
}
