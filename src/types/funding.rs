use serde::{Deserialize, Serialize};

use crate::types::common::{Symbol, Venue};

/// funding rate info from predictedFundings endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingInfo {
    pub symbol: Symbol,
    pub venue: Venue,
    pub funding_rate: f64,
    pub next_funding_time: u64,
    /// true if funding was just settled (nextFundingTime increased)
    pub is_settled: bool,
}

impl FundingInfo {
    pub fn new(
        symbol: Symbol,
        venue: Venue,
        funding_rate: f64,
        next_funding_time: u64,
        is_settled: bool,
    ) -> Self {
        Self {
            symbol,
            venue,
            funding_rate,
            next_funding_time,
            is_settled,
        }
    }
}
