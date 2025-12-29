use thiserror::Error;

use crate::ts_connectors::orders::hyperliquid::error::HLExecutorError;

/// generic executor error that wraps exchange-specific errors
#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("hyperliquid: {0}")]
    Hyperliquid(#[from] HLExecutorError),
    // future exchanges:
    // #[error("binance: {0}")]
    // Binance(#[from] BinanceError),
}

pub type ExecutorResult<T> = Result<T, ExecutorError>;
