use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::*;
use crate::health::HealthStatusCode;
use crate::sync_policy::SyncPolicy;

/// Core application definition, mirrors the CRD spec from DESIGN.md.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Application {
    pub id: Uuid,
    pub name: AppName,
    pub namespace: String,

    pub source: AppSource,
    pub destination: AppDestination,

    #[serde(default)]
    pub sync_policy: SyncPolicy,

    /// Current observed status (set by the reconciliation engine, not the user).
    #[serde(default)]
    pub status: AppStatus,

    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSource {
    pub repo_url: RepoUrl,
    pub path: String,
    #[serde(default = "default_revision")]
    pub target_revision: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<PluginRef>,
}

fn default_revision() -> String {
    "main".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<PluginParam>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginParam {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppDestination {
    pub cluster: ClusterName,
    pub namespace: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppStatus {
    pub sync: SyncStatusCode,
    pub health: HealthStatusCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_synced_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_synced_revision: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum SyncStatusCode {
    #[default]
    Unknown,
    Synced,
    OutOfSync,
    SyncFailed,
}

impl SyncStatusCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::Synced => "Synced",
            Self::OutOfSync => "OutOfSync",
            Self::SyncFailed => "SyncFailed",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "Synced" => Self::Synced,
            "OutOfSync" => Self::OutOfSync,
            "SyncFailed" => Self::SyncFailed,
            _ => Self::Unknown,
        }
    }
}
