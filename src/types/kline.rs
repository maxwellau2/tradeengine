use serde::{Deserialize, Serialize};

use crate::{
    md_connectors::hyperliquid::messages::{HyperliquidRawKline, SimdRawKline},
    types::common::{KlineInterval, Symbol, Venue, symbol_from_str},
};
use simd_json::prelude::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Kline {
    /// candle open time (t field from hyperliquid)
    pub open_time: u64,
    /// candle close time (T field from hyperliquid)
    pub close_time: u64,
    pub symbol: Symbol,
    pub venue: Venue,
    pub interval: KlineInterval,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_closed: bool,
}

impl Kline {
    pub fn new(
        open_time: u64,
        close_time: u64,
        symbol: Symbol,
        venue: Venue,
        interval: KlineInterval,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        is_closed: bool,
    ) -> Self {
        Self {
            open_time,
            close_time,
            symbol,
            venue,
            interval,
            open,
            high,
            low,
            close,
            volume,
            is_closed,
        }
    }

    /// create a default/empty kline
    pub fn default_with_venue(venue: Venue) -> Self {
        Self {
            open_time: 0,
            close_time: 0,
            symbol: symbol_from_str(""),
            venue,
            interval: KlineInterval::M1,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            volume: 0.0,
            is_closed: false,
        }
    }

    pub fn timeframe_from_hyperliquid(s: &str) -> KlineInterval {
        match s {
            "1m" => KlineInterval::M1,
            "3m" => KlineInterval::M3,
            "5m" => KlineInterval::M5,
            _ => KlineInterval::UNKNOWN,
            // there is defo more..
        }
    }
    /// update kline from hyperliquid json
    pub fn update_from_hyperliquid<'a>(
        &mut self,
        data: &simd_json::BorrowedValue<'a>,
    ) -> Result<(), &'static str> {
        self.open_time = data.get("t").and_then(|v| v.as_u64()).ok_or("missing t")?;
        self.close_time = data.get("T").and_then(|v| v.as_u64()).ok_or("missing T")?;

        let o = data.get("o").and_then(|v| v.as_str()).ok_or("missing o")?;
        let h = data.get("h").and_then(|v| v.as_str()).ok_or("missing h")?;
        let l = data.get("l").and_then(|v| v.as_str()).ok_or("missing l")?;
        let c = data.get("c").and_then(|v| v.as_str()).ok_or("missing c")?;
        let v = data.get("v").and_then(|v| v.as_str()).ok_or("missing v")?;
        let s = data.get("s").and_then(|v| v.as_str()).ok_or("missing s")?;
        let i = data.get("i").and_then(|v| v.as_str()).ok_or("missing i")?;

        self.open = fast_float::parse(o).unwrap_or(0.0);
        self.high = fast_float::parse(h).unwrap_or(0.0);
        self.low = fast_float::parse(l).unwrap_or(0.0);
        self.close = fast_float::parse(c).unwrap_or(0.0);
        self.volume = fast_float::parse(v).unwrap_or(0.0);
        self.symbol = symbol_from_str(s);
        self.interval = Kline::timeframe_from_hyperliquid(i);
        self.is_closed = false;

        Ok(())
    }
}
