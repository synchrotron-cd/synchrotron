use std::path::{Path, PathBuf};

use crate::error::GitError;
use crate::repo::RepoId;

/// On-disk layout:
///
/// ```text
/// <root>/
///   bare/<repo_id>.git/    bare repo cache, fetched into across restarts
///   work/                  ephemeral worktree materializations (caller-managed)
/// ```
#[derive(Debug, Clone)]
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn bare_path(&self, repo_id: &RepoId) -> PathBuf {
        self.root
            .join("bare")
            .join(format!("{}.git", repo_id.as_str()))
    }

    pub fn ensure_layout(&self) -> crate::Result<()> {
        for sub in ["bare", "work"] {
            let path = self.root.join(sub);
            std::fs::create_dir_all(&path).map_err(|e| GitError::Io {
                path: path.clone(),
                source: e,
            })?;
        }
        Ok(())
    }
}
