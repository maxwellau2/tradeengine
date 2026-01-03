use thiserror::Error;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("connect error: {0}")]
    Connect(#[from] tungstenite::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("simd json error: {0}")]
    SimdJson(#[from] simd_json::Error),

    #[error("not connected")]
    NotConnected,

    #[error("handler error: {0}")]
    Handler(String),
}

pub type WsResult<T> = Result<T, WsError>;
