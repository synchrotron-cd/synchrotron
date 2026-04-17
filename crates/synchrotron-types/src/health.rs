use serde::{Deserialize, Serialize};

/// Health status code, ordered by severity (worst wins in aggregation via max()).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum HealthStatusCode {
    Healthy = 0,
    Progressing = 1,
    Suspended = 2,
    #[default]
    Unknown = 3,
    Missing = 4,
    Degraded = 5,
}

impl HealthStatusCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Progressing => "Progressing",
            Self::Suspended => "Suspended",
            Self::Unknown => "Unknown",
            Self::Missing => "Missing",
            Self::Degraded => "Degraded",
        }
    }

    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "Healthy" => Self::Healthy,
            "Progressing" => Self::Progressing,
            "Suspended" => Self::Suspended,
            "Missing" => Self::Missing,
            "Degraded" => Self::Degraded,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HealthStatus {
    pub code: HealthStatusCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
