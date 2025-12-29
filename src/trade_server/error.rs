use thiserror::Error;

use crate::types::common::Venue;

/// trade server central errors
#[derive(Debug, Error)]
pub enum CentralError {
    #[error("executor error: {0}")]
    Executor(#[from] crate::ts_connectors::orders::error::ExecutorError),

    #[error("iceoryx2 error: {0}")]
    Iceoryx2(String),

    #[error("venue not configured: {0:?}")]
    VenueNotConfigured(Venue),

    #[error("queue full for venue: {0:?}")]
    QueueFull(Venue),
}

pub type CentralResult<T> = Result<T, CentralError>;
