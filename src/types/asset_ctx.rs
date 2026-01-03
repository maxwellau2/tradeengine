use serde::{Deserialize, Serialize};

use crate::types::common::{Symbol, Venue, symbol_from_str};

/// asset context data - funding rate, open interest, mark price, etc
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetCtx {
    pub symbol: Symbol,
    pub venue: Venue,
    pub funding_rate: f64,
    pub open_interest: f64,
    pub mark_price: f64,
    pub oracle_price: f64,
    /// true if funding rate value changed from last update
    pub is_new_funding: bool,
    /// true if funding was just settled (crossed funding time boundary)
    pub is_funding_settled: bool,
    /// next funding settlement time (unix ms)
    pub next_funding_time: Option<u64>,
}

impl AssetCtx {
    pub fn new(
        symbol: Symbol,
        venue: Venue,
        funding_rate: f64,
        open_interest: f64,
        mark_price: f64,
        oracle_price: f64,
        is_new_funding: bool,
        is_funding_settled: bool,
        next_funding_time: Option<u64>,
    ) -> Self {
        Self {
            symbol,
            venue,
            funding_rate,
            open_interest,
            mark_price,
            oracle_price,
            is_new_funding,
            is_funding_settled,
            next_funding_time,
        }
    }
}
