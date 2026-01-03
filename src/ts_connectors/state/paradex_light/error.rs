//! error types for lightweight paradex state subscriber

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PdxLightStateError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("config error: {0}")]
    Config(#[from] crate::config_parser::error::ConfigError),

    #[error("credentials not configured")]
    CredentialsNotConfigured,

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("signature error: {0}")]
    Signature(#[from] crate::ts_connectors::orders::paradex_light::SignatureError),

    #[error("auth failed: {0}")]
    AuthFailed(String),

    #[error("websocket not connected")]
    NotConnected,

    #[error("subscribe failed: {0}")]
    SubscribeFailed(String),
}

pub type PdxLightStateResult<T> = Result<T, PdxLightStateError>;
