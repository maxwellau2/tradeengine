use crate::types::common::*;
use fast_float;
use lexical;
use quanta::Clock;
use quanta::Instant;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Orderbook {
    pub symbol: Symbol,
    pub venue: Venue,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
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
        bids: Vec<Level>,
        asks: Vec<Level>,
        timestamp: u64,
    ) -> Orderbook {
        return Self {
            symbol,
            venue,
            bids,
            asks,
            timestamp,
        };
    }

    pub fn from_hyperliquid(book: &serde_json::Value) -> Orderbook {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut timestamp = 0u64;
        let mut symbol: Symbol = symbol_from_str("");

        // Try to extract levels and time
        if let Some(levels) = book.get("levels").and_then(|v| v.as_array()) {
            // levels[0] = bids, levels[1] = asks
            if let Some(bid_levels) = levels.get(0).and_then(|v| v.as_array()) {
                bids = Vec::with_capacity(bid_levels.len());
                for bid_level in bid_levels {
                    bids.push(Level::from_hyperliquid(bid_level));
                }
            }

            if let Some(ask_levels) = levels.get(1).and_then(|v| v.as_array()) {
                asks = Vec::with_capacity(ask_levels.len());
                for ask_level in ask_levels {
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

    /// Update orderbook in-place, reusing existing Vec allocations
    pub fn update_from_hyperliquid(&mut self, book: &serde_json::Value) {
        self.timestamp = 0;
        self.bids.clear();
        self.asks.clear();
        self.symbol = symbol_from_str("");

        // Try to extract levels and time
        if let Some(levels) = book.get("levels").and_then(|v| v.as_array()) {
            // levels[0] = bids, levels[1] = asks
            if let Some(bid_levels) = levels.get(0).and_then(|v| v.as_array()) {
                self.bids.reserve(bid_levels.len());
                for bid_level in bid_levels {
                    self.bids.push(Level::from_hyperliquid(bid_level));
                }
            }

            if let Some(ask_levels) = levels.get(1).and_then(|v| v.as_array()) {
                self.asks.reserve(ask_levels.len());
                for ask_level in ask_levels {
                    self.asks.push(Level::from_hyperliquid(ask_level));
                }
            }

            if let Some(sym) = book.get("coin").and_then(|v| v.as_str()) {
                self.symbol = symbol_from_str(sym);
            }
        }

        if let Some(time) = book.get("time").and_then(|v| v.as_u64()) {
            self.timestamp = time;
        }
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
    #[test]
    fn test_orderbook_buffer_reuse() {
        let mut ob = Orderbook::new(
            symbol_from_str("ethusdt"),
            Venue::Hyperliquid,
            vec![],
            vec![],
            0,
        );

        // First update - allocates
        let json1 = make_test_json(20, 20); // 20 bids, 20 asks
        ob.update_from_hyperliquid(&json1);
        let capacity_after_first = ob.bids.capacity();

        // Second update - should reuse capacity
        ob.update_from_hyperliquid(&json1);
        assert_eq!(ob.bids.capacity(), capacity_after_first);
    }
}
