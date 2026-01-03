use thiserror::Error;

use crate::ts_connectors::state::hyperliquid::error::HLStateError;
use crate::ts_connectors::state::paradex_light::error::PdxLightStateError;

#[derive(Error, Debug)]
pub enum StateError {
    #[error("hyperliquid: {0}")]
    Hyperliquid(#[from] HLStateError),

    #[error("paradex: {0}")]
    Paradex(#[from] PdxLightStateError),
}

pub type StateResult<T> = Result<T, StateError>;
