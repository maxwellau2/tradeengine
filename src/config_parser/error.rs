use thiserror::Error;

use crate::types::common::PassportId;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config file not found: {0}")]
    FileNotFound(String),

    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("passport not found: {0}")]
    PassportNotFound(PassportId),
}

pub type ConfigResult<T> = Result<T, ConfigError>;
