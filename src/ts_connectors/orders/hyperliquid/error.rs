use thiserror::Error;

use crate::config_parser::error::ConfigError;

#[derive(Debug, Error)]
pub enum HLExecutorError {
    #[error("websocket not connected")]
    NotConnected,

    #[error("unknown symbol: {0}")]
    UnknownSymbol(String),

    #[error("missing client order id")]
    MissingCloid,

    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("signing failed: {0}")]
    SigningFailed(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("meta endpoint failed: {0}")]
    MetaFailed(String),

    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    #[error("private key parse failed")]
    PrivateKeyParseFailed,

    #[error("hyperliquid credentials not configured for passport")]
    CredentialsNotConfigured,

    #[error("hyperliquid executor not connected")]
    WebsocketNotConnected,
}

pub type HyperliquidResult<T> = Result<T, HLExecutorError>;
