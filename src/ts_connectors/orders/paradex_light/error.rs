//! error types for paradex light executor

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParadexLightError {
    #[error("config error: {0}")]
    Config(String),

    #[error("credentials not configured")]
    CredentialsNotConfigured,

    #[error("signature error: {0}")]
    Signature(#[from] crate::ts_connectors::signature_utils::paradex::SignatureError),

    #[error("http error: {0}")]
    Http(#[from] pipelined_http::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("not connected")]
    NotConnected,

    #[error("auth failed: {0}")]
    AuthFailed(String),

    #[error("missing client order id")]
    MissingCloid,

    #[error("unknown symbol: {0}")]
    UnknownSymbol(String),

    #[error("invalid key: {0}")]
    InvalidKey(String),
}

pub type ParadexLightResult<T> = Result<T, ParadexLightError>;
