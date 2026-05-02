//! Persistent registration store for clusters managed through the
//! REST API. The kube crate's in-memory `ClusterRegistry` is still
//! the runtime authority for live `KubeClient` handles; this repo
//! only persists the *configuration* needed to (re-)build them.

use anyhow::Context;
use chrono::{DateTime, Utc};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::db::Database;

/// Auth-source variant. Mirrors [`synchrotron_kube::AuthSource`] without
/// taking the kube crate as a dep here — the server crate translates
/// at the boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ClusterAuthSource {
    Kubeconfig,
    InCluster,
    Default,
}

impl ClusterAuthSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Kubeconfig => "kubeconfig",
            Self::InCluster => "in_cluster",
            Self::Default => "default",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "in_cluster" => Self::InCluster,
            "default" => Self::Default,
            _ => Self::Kubeconfig,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterRegistration {
    pub id: Uuid,
    pub name: String,
    pub auth_source: ClusterAuthSource,
    pub kubeconfig_path: Option<String>,
    pub context: Option<String>,
    /// Sensitive: never returned in API responses.
    pub bearer_token: Option<String>,
    pub labels: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Database {
    pub fn insert_cluster(&self, c: &ClusterRegistration) -> anyhow::Result<()> {
        let labels_json = serde_json::to_string(&c.labels)?;
        self.conn().execute(
            "INSERT INTO clusters (
                id, name, auth_source, kubeconfig_path, context,
                bearer_token, labels_json, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                c.id.to_string(),
                c.name,
                c.auth_source.as_str(),
                c.kubeconfig_path,
                c.context,
                c.bearer_token,
                labels_json,
                c.created_at.to_rfc3339(),
                c.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_cluster(&self, name: &str) -> anyhow::Result<Option<ClusterRegistration>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, name, auth_source, kubeconfig_path, context,
                    bearer_token, labels_json, created_at, updated_at
             FROM clusters WHERE name = ?1",
        )?;
        let mut rows = stmt.query(params![name])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_cluster(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_clusters(&self) -> anyhow::Result<Vec<ClusterRegistration>> {
        let mut stmt = self.conn().prepare(
            "SELECT id, name, auth_source, kubeconfig_path, context,
                    bearer_token, labels_json, created_at, updated_at
             FROM clusters ORDER BY name",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_cluster(row)?);
        }
        Ok(out)
    }

    pub fn delete_cluster(&self, name: &str) -> anyhow::Result<bool> {
        let n = self
            .conn()
            .execute("DELETE FROM clusters WHERE name = ?1", params![name])?;
        Ok(n > 0)
    }
}

fn row_to_cluster(row: &rusqlite::Row) -> anyhow::Result<ClusterRegistration> {
    let id_str: String = row.get(0)?;
    let name: String = row.get(1)?;
    let auth_source_str: String = row.get(2)?;
    let kubeconfig_path: Option<String> = row.get(3)?;
    let context: Option<String> = row.get(4)?;
    let bearer_token: Option<String> = row.get(5)?;
    let labels_json: String = row.get(6)?;
    let created_at_str: String = row.get(7)?;
    let updated_at_str: String = row.get(8)?;

    Ok(ClusterRegistration {
        id: Uuid::parse_str(&id_str).context("parsing cluster id")?,
        name,
        auth_source: ClusterAuthSource::from_str_lossy(&auth_source_str),
        kubeconfig_path,
        context,
        bearer_token,
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

    fn fixture(name: &str) -> ClusterRegistration {
        let now = Utc::now();
        ClusterRegistration {
            id: Uuid::new_v4(),
            name: name.into(),
            auth_source: ClusterAuthSource::Kubeconfig,
            kubeconfig_path: Some("/etc/synchrotron/kubeconfig".into()),
            context: Some("prod".into()),
            bearer_token: Some("super-secret".into()),
            labels: serde_json::json!({"region": "us-east"}),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn insert_get_list_delete() {
        let db = Database::open_in_memory().unwrap();
        db.insert_cluster(&fixture("prod")).unwrap();
        db.insert_cluster(&fixture("staging")).unwrap();

        let got = db.get_cluster("prod").unwrap().unwrap();
        assert_eq!(got.name, "prod");
        assert_eq!(got.bearer_token.as_deref(), Some("super-secret"));
        assert_eq!(got.labels["region"], "us-east");

        let all = db.list_clusters().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].name, "prod");
        assert_eq!(all[1].name, "staging");

        assert!(db.delete_cluster("staging").unwrap());
        assert!(!db.delete_cluster("staging").unwrap());
        assert_eq!(db.list_clusters().unwrap().len(), 1);
    }

    #[test]
    fn duplicate_name_errors() {
        let db = Database::open_in_memory().unwrap();
        db.insert_cluster(&fixture("prod")).unwrap();
        let err = db.insert_cluster(&fixture("prod")).unwrap_err();
        let s = format!("{err}");
        assert!(s.contains("UNIQUE") || s.contains("constraint"), "got: {s}");
    }
}
