use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GitError {
    #[error("git2 error: {0}")]
    Git(#[from] git2::Error),

    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("commit not found: {0}")]
    CommitNotFound(String),

    #[error("repository not initialised at {0}")]
    NotInitialised(PathBuf),

    #[error("invalid repository state: {0}")]
    InvalidState(String),

    #[error("unknown SSH host key for {host} ({key_type}); add to known_hosts to trust")]
    UnknownHostKey { host: String, key_type: String },

    #[error(
        "SSH host key mismatch for {host} ({key_type}); known_hosts says different — possible MITM"
    )]
    HostKeyMismatch { host: String, key_type: String },
}
