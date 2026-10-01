//! trade event from exchange tape

use serde::{Deserialize, Serialize};

use crate::types::common::{Side, Symbol, Venue};

/// type of trade
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TradeType {
    #[default]
    Fill,
    Liquidation,
    Other,
}

/// trade event from exchange
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: Symbol,
    pub venue: Venue,
    pub price: f64,
    pub size: f64,
    pub side: Side, // aggressor side (BUY = buyer lifted ask, SELL = seller hit bid)
    pub timestamp: u64, // unix millis from exchange
    pub trade_type: TradeType,
}

impl Trade {
    /// parse from paradex ws message
    /// expects params.data object with: price, size, side, created_at, trade_type
    pub fn from_paradex(params: &simd_json::BorrowedValue) -> Option<Self> {
        use simd_json::base::ValueAsScalar;
        use simd_json::derived::ValueObjectAccess;

        let channel = params.get("channel")?.as_str()?;
        // channel format: trades.{symbol}
        let symbol_str = channel.strip_prefix("trades.")?;

        let data = params.get("data")?;
        let price: f64 = data.get("price")?.as_str()?.parse().ok()?;
        let size: f64 = data.get("size")?.as_str()?.parse().ok()?;
        let side_str = data.get("side")?.as_str()?;
        let timestamp = data.get("created_at")?.as_u64()?;
        let trade_type_str = data.get("trade_type")?.as_str().unwrap_or("FILL");

        let side = match side_str {
            "BUY" => Side::LONG,
            "SELL" => Side::SHORT,
            _ => Side::UNKNOWN,
        };

        let trade_type = match trade_type_str {
            "FILL" => TradeType::Fill,
            "LIQUIDATION" => TradeType::Liquidation,
            _ => TradeType::Other,
        };

        Some(Self {
            symbol: Symbol::new(symbol_str),
            venue: Venue::Paradex,
            price,
            size,
            side,
            timestamp,
            trade_type,
        })
    }
}
