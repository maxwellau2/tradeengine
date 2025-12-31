use serde::{Deserialize, Serialize};

use crate::types::common::{KlineInterval, Symbol, Venue, symbol_from_str};

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
    pub fn update_from_hyperliquid(&mut self, kline: &serde_json::Value) {
        self.open_time = kline.get("t").and_then(|v| v.as_u64()).unwrap_or(0);
        self.close_time = kline.get("T").and_then(|v| v.as_u64()).unwrap_or(0);
        self.open = kline
            .get("o")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);
        self.low = kline
            .get("l")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);
        self.high = kline
            .get("h")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);
        self.close = kline
            .get("c")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);
        self.volume = kline
            .get("v")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);
        self.symbol = symbol_from_str(kline.get("s").and_then(|v| v.as_str()).unwrap_or(""));
        let interval = kline.get("i").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
        self.interval = Kline::timeframe_from_hyperliquid(interval);

        // is_closed will be set by the handler when it detects a new candle
        self.is_closed = false;
    }
}
