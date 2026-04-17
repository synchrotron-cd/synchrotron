use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Authentication material for a remote repository.
///
/// SSH variants come in two flavours: `SshKey` references key files on
/// disk (private + optional public, with optional passphrase) while
/// `SshAgent` delegates to a running ssh-agent for environments where
/// raw key material shouldn't be readable by the operator process.
/// GitHub App tokens are added by sibling sub-issue h48.1.3.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Credentials {
    None,
    HttpBasic {
        username: String,
        password: String,
    },
    SshKey {
        /// SSH login user (typically `git` for GitHub/GitLab/etc.).
        username: String,
        private_key: PathBuf,
        /// Optional public key. If `None`, libgit2 derives it from the
        /// private key — works for OpenSSH-format keys but not all
        /// transports, so prefer specifying explicitly when available.
        #[serde(default)]
        public_key: Option<PathBuf>,
        /// Passphrase for an encrypted private key.
        #[serde(default)]
        passphrase: Option<String>,
    },
    SshAgent {
        username: String,
    },
    /// GitHub App installation. Note: this variant is *not* directly
    /// consumed by the libgit2 callback — it must be exchanged for an
    /// installation token via
    /// [`crate::github_app::TokenCache`] and presented as
    /// [`Credentials::HttpBasic`] (`username = "x-access-token"`,
    /// `password = token`) before the synchronous fetch call.
    GitHubApp {
        app_id: u64,
        installation_id: u64,
        private_key_path: PathBuf,
        #[serde(default)]
        api_base: Option<String>,
    },
}

impl Default for Credentials {
    fn default() -> Self {
        Self::None
    }
}
