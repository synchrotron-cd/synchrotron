use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automated: Option<AutomatedPolicy>,
    #[serde(default)]
    pub drift: DriftConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomatedPolicy {
    /// Auto-correct drift (default: true)
    #[serde(default = "default_true")]
    pub self_heal: bool,
    /// Auto-delete resources removed from git (default: false)
    #[serde(default)]
    pub prune: bool,
    /// Self-heal check interval in seconds (default: 180 = 3 min)
    #[serde(default = "default_self_heal_interval")]
    pub self_heal_interval_secs: u64,
}

impl Default for AutomatedPolicy {
    fn default() -> Self {
        Self {
            self_heal: true,
            prune: false,
            self_heal_interval_secs: 180,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_self_heal_interval() -> u64 {
    180
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftConfig {
    /// Use server-side dry-run for normalization (default: true)
    #[serde(default = "default_true")]
    pub server_side_diff: bool,
    #[serde(default)]
    pub auto_ignore: AutoIgnore,
    #[serde(default)]
    pub ignore: Vec<IgnoreRule>,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            server_side_diff: true,
            auto_ignore: AutoIgnore::default(),
            ignore: Vec::new(),
        }
    }
}

/// Auto-ignore fields managed by Kubernetes controllers. All default to true.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoIgnore {
    /// Ignore spec.replicas when HPA targets this resource
    #[serde(default = "default_true")]
    pub hpa_replicas: bool,
    /// Ignore container resources when VPA targets this resource
    #[serde(default = "default_true")]
    pub vpa_resources: bool,
    /// Ignore fields set by defaulting (via server-side diff)
    #[serde(default = "default_true")]
    pub defaulted_fields: bool,
    /// Ignore fields added by mutating admission webhooks
    #[serde(default = "default_true")]
    pub mutating_webhooks: bool,
}

impl Default for AutoIgnore {
    fn default() -> Self {
        Self {
            hpa_replicas: true,
            vpa_resources: true,
            defaulted_fields: true,
            mutating_webhooks: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IgnoreRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub json_pointers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub managed_fields_managers: Vec<String>,
}
