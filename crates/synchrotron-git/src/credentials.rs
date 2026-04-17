use serde::{Deserialize, Serialize};

/// Authentication material for a remote repository.
///
/// HTTP basic is the only authenticated variant in the foundation slice;
/// SSH and GitHub App tokens are added by sibling sub-issues
/// (h48.1.2, h48.1.3) and will become additional variants here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credentials {
    None,
    HttpBasic { username: String, password: String },
}

impl Default for Credentials {
    fn default() -> Self {
        Self::None
    }
}
