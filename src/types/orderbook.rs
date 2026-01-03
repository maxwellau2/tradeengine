use crate::md_connectors::hyperliquid::messages::HyperliquidRawOrderbook;
use crate::md_connectors::hyperliquid::messages::HyperliquidWsMessage;
use crate::md_connectors::hyperliquid::messages::SimdRawOrderbook;
use crate::ts_connectors::orders::hyperliquid::error::HLExecutorError;
use crate::types::common::*;
use arrayvec::ArrayVec;
use fast_float;
use lexical;
use quanta::Clock;
use quanta::Instant;
use serde::{Deserialize, Serialize};
use simd_json::prelude::*;

/// max levels per side for stack-allocated orderbook
pub const MAX_LEVELS: usize = 30;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Orderbook {
    pub symbol: Symbol,
    pub venue: Venue,
    pub bids: ArrayVec<Level, MAX_LEVELS>,
    pub asks: ArrayVec<Level, MAX_LEVELS>,
    pub timestamp: u64,
}

impl Level {
    pub fn new(price: f64, size: f64) -> Self {
        return Self { price, size };
    }

    pub fn from_hyperliquid(level: &serde_json::Value) -> Self {
        let price = level
            .get("px")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);

        let size = level
            .get("sz")
            .and_then(|v| v.as_str())
            .and_then(|s| lexical::parse::<f64, _>(s).ok())
            .unwrap_or(0.0);

        Level { price, size }
    }
}

impl Orderbook {
    pub fn new(
        symbol: Symbol,
        venue: Venue,
        bids: ArrayVec<Level, MAX_LEVELS>,
        asks: ArrayVec<Level, MAX_LEVELS>,
        timestamp: u64,
    ) -> Orderbook {
        Self {
            symbol,
            venue,
            bids,
            asks,
            timestamp,
        }
    }

    /// create empty orderbook
    pub fn empty(venue: Venue) -> Orderbook {
        Self {
            symbol: symbol_from_str(""),
            venue,
            bids: ArrayVec::new(),
            asks: ArrayVec::new(),
            timestamp: 0,
        }
    }

    pub fn from_hyperliquid(book: &serde_json::Value) -> Orderbook {
        let mut bids = ArrayVec::new();
        let mut asks = ArrayVec::new();
        let mut timestamp = 0u64;
        let mut symbol: Symbol = symbol_from_str("");

        if let Some(levels) = book.get("levels").and_then(|v| v.as_array()) {
            if let Some(bid_levels) = levels.get(0).and_then(|v| v.as_array()) {
                for bid_level in bid_levels.iter().take(MAX_LEVELS) {
                    bids.push(Level::from_hyperliquid(bid_level));
                }
            }

            if let Some(ask_levels) = levels.get(1).and_then(|v| v.as_array()) {
                for ask_level in ask_levels.iter().take(MAX_LEVELS) {
                    asks.push(Level::from_hyperliquid(ask_level));
                }
            }
        }

        if let Some(time) = book.get("time").and_then(|v| v.as_u64()) {
            timestamp = time;
        }

        if let Some(sym) = book.get("coin").and_then(|v| v.as_str()) {
            symbol = symbol_from_str(sym);
        }

        Self {
            venue: Venue::Hyperliquid,
            symbol,
            bids,
            asks,
            timestamp,
        }
    }

    /// update orderbook in-place using typed struct
    pub fn update_from_hyperliquid<'a>(
        &mut self,
        data: &simd_json::BorrowedValue<'a>,
    ) -> Result<(), &'static str> {
        self.bids.clear();
        self.asks.clear();

        // extract coin
        let coin = data
            .get("coin")
            .and_then(|v| v.as_str())
            .ok_or("missing coin")?;
        self.symbol = symbol_from_str(coin);

        // extract time
        self.timestamp = data
            .get("time")
            .and_then(|v| v.as_u64())
            .ok_or("missing time")?;

        // extract levels array
        let levels = data
            .get("levels")
            .and_then(|v| v.as_array())
            .ok_or("missing levels")?;

        // parse bids (levels[0]) - cap at MAX_LEVELS for stack allocation
        if let Some(bids_arr) = levels.get(0).and_then(|v| v.as_array()) {
            for lvl in bids_arr.iter().take(MAX_LEVELS) {
                let px = lvl.get("px").and_then(|v| v.as_str()).unwrap_or("0");
                let sz = lvl.get("sz").and_then(|v| v.as_str()).unwrap_or("0");
                self.bids.push(Level {
                    price: fast_float::parse(px).unwrap_or(0.0),
                    size: fast_float::parse(sz).unwrap_or(0.0),
                });
            }
        }

        // parse asks (levels[1])
        if let Some(asks_arr) = levels.get(1).and_then(|v| v.as_array()) {
            for lvl in asks_arr.iter().take(MAX_LEVELS) {
                let px = lvl.get("px").and_then(|v| v.as_str()).unwrap_or("0");
                let sz = lvl.get("sz").and_then(|v| v.as_str()).unwrap_or("0");
                self.asks.push(Level {
                    price: fast_float::parse(px).unwrap_or(0.0),
                    size: fast_float::parse(sz).unwrap_or(0.0),
                });
            }
        }

        Ok(())
    }

    pub fn update_from_paradex<'a>(
        &mut self,
        payload: &simd_json::BorrowedValue<'a>,
    ) -> Result<(), &'static str> {
        self.bids.clear();
        self.asks.clear();

        // channel format: "order_book.ONDO-USD-PERP.snapshot@15@50ms"
        let channel = payload
            .get("channel")
            .and_then(|v| v.as_str())
            .ok_or("missing channel")?;

        // extract market symbol (middle segment between first two dots)
        let market = channel.split('.').nth(1).ok_or("invalid channel format")?;
        self.symbol = symbol_from_str(market);

        let data = payload.get("data").ok_or("missing data")?;

        // extract timestamp
        self.timestamp = data
            .get("last_updated_at")
            .and_then(|v| v.as_u64())
            .ok_or("missing last_updated_at")?;

        // parse inserts array - contains both bids and asks
        let inserts = data
            .get("inserts")
            .and_then(|v| v.as_array())
            .ok_or("missing inserts")?;

        for entry in inserts.iter().take(MAX_LEVELS * 2) {
            let side = entry.get("side").and_then(|v| v.as_str()).unwrap_or("");
            let price_str = entry.get("price").and_then(|v| v.as_str()).unwrap_or("0");
            let size_str = entry.get("size").and_then(|v| v.as_str()).unwrap_or("0");

            let level = Level {
                price: fast_float::parse(price_str).unwrap_or(0.0),
                size: fast_float::parse(size_str).unwrap_or(0.0),
            };

            match side {
                "BUY" if self.bids.len() < MAX_LEVELS => self.bids.push(level),
                "SELL" if self.asks.len() < MAX_LEVELS => self.asks.push(level),
                _ => {}
            }
        }

        // sort bids descending (best bid at index 0)
        self.bids.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // sort asks ascending (best ask at index 0)
        self.asks.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(())
    }

    // add your other adapters here
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_hyperliquid_parsing_valid() {
        let json = serde_json::json!({
            "coin": "ETH",
            "levels": [
                [{"px": "100.5", "sz": "1.2"}],
                [{"px": "101.5", "sz": "2.3"}]
            ],
            "time": 1234567890
        });

        let ob = Orderbook::from_hyperliquid(&json);
        assert_eq!(ob.bids.len(), 1);
        assert_eq!(ob.asks.len(), 1);
        assert_eq!(ob.timestamp, 1234567890);
    }

    #[test]
    fn test_orderbook_parsing_hyperliquid_corrupt_data() {
        let json = serde_json::json!({
            "coin": "ETH",
            "levels": [[], []],  // Empty levels
            "time": 1234567890
        });

        let ob = Orderbook::from_hyperliquid(&json);
        assert_eq!(ob.bids.len(), 0); // Should handle gracefully
        assert_eq!(ob.asks.len(), 0);
    }

    #[test]
    fn test_level_parsing_invalid_numbers() {
        let json = serde_json::json!({"px": "invalid", "sz": "1.0"});
        let level = Level::from_hyperliquid(&json);
        assert_eq!(level.price, 0.0); // Defaults to 0
    }

    fn make_test_json(bids: usize, asks: usize) -> serde_json::Value {
        let mut bid = Vec::new();
        let mut ask = Vec::new();
        for i in 0..bids {
            bid.push((i, i));
        }
        for i in 0..asks {
            ask.push((i, i));
        }
        serde_json::json!({
            "coin": "ETH",
            "levels": [bid, ask],  // Empty levels
            "time": 1234567890
        })
    }
    // #[test]
    // fn test_orderbook_buffer_reuse() {
    //     let mut ob = Orderbook::new(
    //         symbol_from_str("ethusdt"),
    //         Venue::Hyperliquid,
    //         vec![],
    //         vec![],
    //         0,
    //     );

    //     // First update - allocates
    //     let json1 = make_test_json(20, 20); // 20 bids, 20 asks
    //     ob.update_from_hyperliquid(&json1);
    //     let capacity_after_first = ob.bids.capacity();

    //     // Second update - should reuse capacity
    //     ob.update_from_hyperliquid(&json1);
    //     assert_eq!(ob.bids.capacity(), capacity_after_first);
    // }
}
