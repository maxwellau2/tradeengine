// paradex market info - precision and rounding
//
// fetches market metadata from paradex api and provides
// rounding helpers for price/size

use serde::Deserialize;
use std::collections::HashMap;
use tracing::{debug, info, warn};

const MAINNET_API: &str = "https://api.prod.paradex.trade/v1";
const TESTNET_API: &str = "https://api.testnet.paradex.trade/v1";

/// market metadata from paradex
#[derive(Debug, Clone, Deserialize)]
pub struct ParadexMarket {
    pub symbol: String,
    pub base_currency: String,
    pub quote_currency: String,
    /// price tick size (e.g., 0.1 means prices must be multiples of 0.1)
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub price_tick_size: f64,
    /// order size increment (e.g., 0.001 means sizes must be multiples of 0.001)
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub order_size_increment: f64,
    /// minimum notional value in usd
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub min_notional: f64,
    /// maximum order size
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub max_order_size: f64,
    /// maximum position limit
    #[serde(deserialize_with = "deserialize_f64_from_str")]
    pub position_limit: f64,
}

fn deserialize_f64_from_str<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    results: Vec<ParadexMarket>,
}

impl ParadexMarket {
    /// round price to valid tick size
    pub fn round_price(&self, price: f64) -> f64 {
        (price / self.price_tick_size).round() * self.price_tick_size
    }

    /// round price down to valid tick size
    pub fn floor_price(&self, price: f64) -> f64 {
        (price / self.price_tick_size).floor() * self.price_tick_size
    }

    /// round price up to valid tick size
    pub fn ceil_price(&self, price: f64) -> f64 {
        (price / self.price_tick_size).ceil() * self.price_tick_size
    }

    /// round size to valid increment
    pub fn round_size(&self, size: f64) -> f64 {
        (size / self.order_size_increment).round() * self.order_size_increment
    }

    /// round size down to valid increment
    pub fn floor_size(&self, size: f64) -> f64 {
        (size / self.order_size_increment).floor() * self.order_size_increment
    }

    /// check if notional (price * size) meets minimum
    pub fn meets_min_notional(&self, price: f64, size: f64) -> bool {
        price * size >= self.min_notional
    }

    /// get number of decimal places for price
    pub fn price_decimals(&self) -> u32 {
        decimal_places(self.price_tick_size)
    }

    /// get number of decimal places for size
    pub fn size_decimals(&self) -> u32 {
        decimal_places(self.order_size_increment)
    }

    /// format price as string with correct precision
    pub fn format_price(&self, price: f64) -> String {
        let rounded = self.round_price(price);
        format!("{:.prec$}", rounded, prec = self.price_decimals() as usize)
    }

    /// format size as string with correct precision
    pub fn format_size(&self, size: f64) -> String {
        let rounded = self.round_size(size);
        format!("{:.prec$}", rounded, prec = self.size_decimals() as usize)
    }
}

/// count decimal places in a number
fn decimal_places(n: f64) -> u32 {
    let s = format!("{}", n);
    if let Some(pos) = s.find('.') {
        (s.len() - pos - 1) as u32
    } else {
        0
    }
}

/// cache of market info keyed by symbol
#[derive(Debug, Default)]
pub struct MarketInfoCache {
    markets: HashMap<String, ParadexMarket>,
}

impl MarketInfoCache {
    pub fn new() -> Self {
        Self {
            markets: HashMap::new(),
        }
    }

    /// get market info by symbol (e.g., "BTC-USD-PERP" or "BTC")
    pub fn get(&self, symbol: &str) -> Option<&ParadexMarket> {
        // try exact match first
        if let Some(m) = self.markets.get(symbol) {
            return Some(m);
        }
        // try with -USD-PERP suffix
        let full = format!("{}-USD-PERP", symbol);
        self.markets.get(&full)
    }

    /// fetch all markets from paradex api (blocking)
    pub fn fetch_sync(is_mainnet: bool) -> Result<Self, String> {
        let base = if is_mainnet { MAINNET_API } else { TESTNET_API };
        let url = format!("{}/markets", base);

        info!("fetching paradex markets from {}", url);

        let resp = reqwest::blocking::get(&url).map_err(|e| format!("http error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("api error: {}", resp.status()));
        }

        let data: MarketsResponse = resp
            .json()
            .map_err(|e| format!("json parse error: {}", e))?;

        let mut cache = Self::new();
        for market in data.results {
            debug!(
                symbol = %market.symbol,
                tick = %market.price_tick_size,
                lot = %market.order_size_increment,
                "loaded market"
            );
            cache.markets.insert(market.symbol.clone(), market);
        }

        info!("loaded {} paradex markets", cache.markets.len());
        Ok(cache)
    }

    /// fetch all markets from paradex api (async)
    pub async fn fetch(is_mainnet: bool) -> Result<Self, String> {
        let base = if is_mainnet { MAINNET_API } else { TESTNET_API };
        let url = format!("{}/markets", base);

        info!("fetching paradex markets from {}", url);

        let resp = reqwest::get(&url)
            .await
            .map_err(|e| format!("http error: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("api error: {}", resp.status()));
        }

        let data: MarketsResponse = resp
            .json()
            .await
            .map_err(|e| format!("json parse error: {}", e))?;

        let mut cache = Self::new();
        for market in data.results {
            debug!(
                symbol = %market.symbol,
                tick = %market.price_tick_size,
                lot = %market.order_size_increment,
                "loaded market"
            );
            cache.markets.insert(market.symbol.clone(), market);
        }

        info!("loaded {} paradex markets", cache.markets.len());
        Ok(cache)
    }

    /// number of markets loaded
    pub fn len(&self) -> usize {
        self.markets.len()
    }

    /// check if empty
    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }

    /// list all symbols
    pub fn symbols(&self) -> Vec<&str> {
        self.markets.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_places() {
        assert_eq!(decimal_places(0.1), 1);
        assert_eq!(decimal_places(0.01), 2);
        assert_eq!(decimal_places(0.001), 3);
        assert_eq!(decimal_places(1.0), 1);
        assert_eq!(decimal_places(10.0), 1);
    }

    #[test]
    fn test_round_price() {
        let market = ParadexMarket {
            symbol: "BTC-USD-PERP".into(),
            base_currency: "BTC".into(),
            quote_currency: "USD".into(),
            price_tick_size: 0.1,
            order_size_increment: 0.001,
            min_notional: 10.0,
            max_order_size: 100.0,
            position_limit: 1000.0,
        };

        assert_eq!(market.round_price(100.05), 100.1);
        assert_eq!(market.round_price(100.04), 100.0);
        assert_eq!(market.floor_price(100.09), 100.0);
        assert_eq!(market.ceil_price(100.01), 100.1);
    }

    #[test]
    fn test_round_size() {
        let market = ParadexMarket {
            symbol: "BTC-USD-PERP".into(),
            base_currency: "BTC".into(),
            quote_currency: "USD".into(),
            price_tick_size: 0.1,
            order_size_increment: 0.001,
            min_notional: 10.0,
            max_order_size: 100.0,
            position_limit: 1000.0,
        };

        assert_eq!(market.round_size(0.0005), 0.001);
        assert_eq!(market.round_size(0.0004), 0.0);
        assert_eq!(market.floor_size(0.0019), 0.001);
    }

    #[test]
    fn test_format_price() {
        let market = ParadexMarket {
            symbol: "BTC-USD-PERP".into(),
            base_currency: "BTC".into(),
            quote_currency: "USD".into(),
            price_tick_size: 0.1,
            order_size_increment: 0.001,
            min_notional: 10.0,
            max_order_size: 100.0,
            position_limit: 1000.0,
        };

        assert_eq!(market.format_price(100.0), "100.0");
        assert_eq!(market.format_price(100.05), "100.1");
    }

    #[tokio::test]
    async fn test_fetch_markets() {
        // only run if network available
        if let Ok(cache) = MarketInfoCache::fetch(true).await {
            assert!(!cache.is_empty());
            // btc should exist
            assert!(cache.get("BTC-USD-PERP").is_some());
            assert!(cache.get("BTC").is_some()); // short form
        }
    }
}
