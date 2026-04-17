use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use synchrotron_types::RepoUrl;

use crate::credentials::Credentials;

/// Stable, filesystem-safe identifier for a repo, derived from its URL.
///
/// Used to namespace bare-repo caches under the workspace root so the same
/// physical clone is reused across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepoId(String);

impl RepoId {
    pub fn from_url(url: &RepoUrl) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(url.0.as_bytes());
        Self(hex::encode(hasher.finalize()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A single git repository under management.
#[derive(Debug, Clone)]
pub struct Repo {
    pub id: RepoId,
    pub url: RepoUrl,
    /// Branch to track (e.g. "main", "master").
    pub branch: String,
    pub credentials: Credentials,
    /// Shallow clone depth (None = full history). Defaults to `Some(1)`.
    /// Note: libgit2's local transport does not support shallow fetches —
    /// callers using `file://` or raw filesystem paths (typically tests)
    /// must set this to `None`.
    pub depth: Option<i32>,
}

impl Repo {
    pub fn new(url: RepoUrl, branch: impl Into<String>, credentials: Credentials) -> Self {
        let id = RepoId::from_url(&url);
        Self {
            id,
            url,
            branch: branch.into(),
            credentials,
            depth: Some(1),
        }
    }

    pub fn with_depth(mut self, depth: Option<i32>) -> Self {
        self.depth = depth;
        self
    }
}
