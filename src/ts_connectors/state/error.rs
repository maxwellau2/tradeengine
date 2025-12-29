use ethers::types::spoof::State;
use thiserror::Error;

use crate::ts_connectors::state::hyperliquid::error::HLStateError;

#[derive(Error, Debug)]
pub enum StateError {
    #[error("hyperliquid: {0}")]
    Hyperliquid(#[from] HLStateError),
}

pub type StateResult<T> = Result<T, StateError>;
