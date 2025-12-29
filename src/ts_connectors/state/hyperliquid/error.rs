use thiserror::Error;

use crate::config_parser::error::ConfigError;

#[derive(Error, Debug)]
pub enum HLStateError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("hyperliquid credentials not configured")]
    CredentialsNotConfigured,
    #[error("Failed to connect to Hyperliquid")]
    HyperliquidConnectionFailed,
    #[error("Failed to fetch data from Hyperliquid")]
    HyperliquidFetchFailed,
    #[error("Failed to parse data from Hyperliquid")]
    HyperliquidParseFailed,
    #[error("Failed to deserialize data from Hyperliquid")]
    HyperliquidDeserializeFailed,
    #[error("Failed to serialize data to Hyperliquid")]
    HyperliquidSerializeFailed,
    #[error("Failed to send data to Hyperliquid")]
    HyperliquidSendFailed,
    #[error("Failed to receive data from Hyperliquid")]
    HyperliquidReceiveFailed,
    #[error("Failed to handle data from Hyperliquid")]
    HyperliquidHandleFailed,
}

pub type HLStateResult<T> = Result<T, HLStateError>;
