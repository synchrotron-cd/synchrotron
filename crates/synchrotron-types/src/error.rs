use thiserror::Error;

#[derive(Debug, Error)]
pub enum SynchrotronError {
    #[error("application not found: {0}")]
    ApplicationNotFound(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("{0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, SynchrotronError>;
