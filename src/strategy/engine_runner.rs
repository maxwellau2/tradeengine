// engine runner - loads config and runs trading runtime
//
// simplified design: single core for md + engine via TradingRuntime.
// no ring buffers, no separate io thread - everything async on one core.

use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::{error::Error, fs::File};

use tokio::runtime::Builder;
use tracing::info;

use crate::core_utils::pin_to_core;
use crate::md_connectors::hyperliquid::subscriber::{
    HyperliquidMDSubscriber, HyperliquidSubscription,
};
use crate::md_connectors::paradex::subscriber::{ParadexMDSubscriber, ParadexSubscription};
use crate::md_connectors::subscriber::AnyMDSubscriber;
use crate::strategy::engine::MarketDataEngine;
use crate::strategy::runtime::TradingRuntime;
use crate::strategy::strategy::Strategy;
use crate::tui::SharedTUIState;
use crate::types::common::{KlineInterval, PassportId, Venue};

/// subscription types for a venue
#[derive(Debug, Deserialize, Default, Clone)]
pub struct VenueSubscriptions {
    #[serde(default)]
    pub kline: Vec<String>,
    #[serde(default)]
    pub orderbook: Vec<String>,
}

/// engine configuration parsed from yaml
///
/// example yaml:
/// ```yaml
/// core: 0
/// channel_name: channel1
/// passport_id: 1234
/// testnet: true  # optional, defaults to false (mainnet)
/// subscriptions:
///     paradex:
///         orderbook: [BTC, ETH, LINK]
///     hyperliquid:
///         kline: [BTC::M1]
///         orderbook: [BTC]
/// ```
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    /// cpu core for trading runtime (md + engine on same core)
    pub core: usize,
    /// legacy field - ignored, kept for backwards compat
    #[serde(default)]
    pub main_core: Option<usize>,
    #[serde(default)]
    pub io_core: Option<usize>,
    pub channel_name: String,
    pub passport_id: PassportId,
    /// use testnet endpoints (defaults to false = mainnet)
    #[serde(default)]
    pub testnet: bool,
    /// map of venue name -> subscriptions (kline/orderbook symbols)
    pub subscriptions: HashMap<String, VenueSubscriptions>,
}

impl EngineConfig {
    pub fn from_file(filename: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = File::open(filename)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let cfg: EngineConfig = serde_yaml::from_str(&contents)?;
        Ok(cfg)
    }

    /// get subscriptions for a specific venue
    pub fn get_venue(&self, venue: &str) -> Option<&VenueSubscriptions> {
        self.subscriptions.get(venue)
    }

    /// get all configured venue names
    pub fn venues(&self) -> Vec<String> {
        self.subscriptions.keys().cloned().collect()
    }
}

/// converts venue string to Venue enum
fn parse_venue(s: &str) -> Option<Venue> {
    match s.to_lowercase().as_str() {
        "hyperliquid" => Some(Venue::Hyperliquid),
        "paradex" => Some(Venue::Paradex),
        _ => None,
    }
}

/// parse kline interval from string
fn parse_interval(s: &str) -> KlineInterval {
    match s.to_uppercase().as_str() {
        "M1" => KlineInterval::M1,
        "M5" => KlineInterval::M5,
        "M15" => KlineInterval::M15,
        "M30" => KlineInterval::M30,
        "H1" => KlineInterval::H1,
        "H4" => KlineInterval::H4,
        "D1" => KlineInterval::D1,
        "W1" => KlineInterval::W1,
        _ => KlineInterval::M1,
    }
}

/// parse "SYMBOL::INTERVAL" or just "SYMBOL"
fn parse_kline_sub(s: &str) -> (&str, KlineInterval) {
    if let Some((symbol, interval)) = s.split_once("::") {
        (symbol, parse_interval(interval))
    } else {
        (s, KlineInterval::M1)
    }
}

/// engine runner - simplified single-core design
///
/// uses TradingRuntime to run md feeds and engine on same core.
/// no ring buffers - md data flows directly to engine via callbacks.
pub struct EngineRunner {
    config: EngineConfig,
    engine: MarketDataEngine,
    feeds: Vec<AnyMDSubscriber>,
    tui_state: Option<SharedTUIState>,
}

impl EngineRunner {
    /// create engine runner from config file
    pub fn from_config(
        config_path: &str,
        strategy: Box<dyn Strategy>,
    ) -> Result<Self, Box<dyn Error>> {
        Self::from_config_with_tui(config_path, strategy, None)
    }

    /// create engine runner with TUI state
    pub fn from_config_with_tui(
        config_path: &str,
        strategy: Box<dyn Strategy>,
        tui_state: Option<SharedTUIState>,
    ) -> Result<Self, Box<dyn Error>> {
        let config = EngineConfig::from_file(config_path)?;

        // determine primary venue
        let primary_venue = config
            .venues()
            .first()
            .and_then(|v| parse_venue(v))
            .unwrap_or(Venue::Paradex);

        let mut engine = MarketDataEngine::new(
            strategy,
            config.passport_id,
            primary_venue,
            config.channel_name.clone(),
        );

        if let Some(ref state) = tui_state {
            engine.set_tui_state(state.clone());
        }

        // build md subscribers from config
        let feeds = Self::build_feeds(&config);

        Ok(Self {
            config,
            engine,
            feeds,
            tui_state,
        })
    }

    /// build md subscribers from config
    fn build_feeds(config: &EngineConfig) -> Vec<AnyMDSubscriber> {
        let mut feeds = Vec::new();

        for venue_name in config.venues() {
            let Some(venue) = parse_venue(&venue_name) else {
                continue;
            };
            let Some(subs) = config.get_venue(&venue_name) else {
                continue;
            };

            match venue {
                Venue::Paradex => {
                    let mut subscriptions = Vec::new();
                    for symbol in &subs.orderbook {
                        subscriptions.push(ParadexSubscription::orderbook(symbol));
                    }
                    // paradex doesn't support klines yet

                    if !subscriptions.is_empty() {
                        info!("paradex: {} orderbook subscriptions", subscriptions.len());
                        let is_mainnet = !config.testnet;
                        let subscriber = ParadexMDSubscriber::new(subscriptions, is_mainnet);
                        feeds.push(AnyMDSubscriber::Paradex(subscriber));
                    }
                }
                Venue::Hyperliquid => {
                    let mut subscriptions = Vec::new();
                    for symbol in &subs.orderbook {
                        subscriptions.push(HyperliquidSubscription::orderbook(symbol));
                    }
                    for kline_sub in &subs.kline {
                        let (symbol, interval) = parse_kline_sub(kline_sub);
                        subscriptions.push(HyperliquidSubscription::kline(symbol, interval));
                    }

                    if !subscriptions.is_empty() {
                        info!("hyperliquid: {} subscriptions", subscriptions.len());
                        let is_mainnet = !config.testnet;
                        let subscriber = HyperliquidMDSubscriber::new(subscriptions, is_mainnet);
                        feeds.push(AnyMDSubscriber::Hyperliquid(subscriber));
                    }
                }
                _ => {}
            }
        }

        feeds
    }

    /// run the trading runtime (blocking)
    ///
    /// pins to configured core and runs md + engine in single async runtime.
    pub fn run(self) {
        let core = self.config.core;

        // pin to core
        pin_to_core(core);
        info!("trading runtime pinned to core {}", core);

        // create single-threaded tokio runtime
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");

        // build trading runtime
        let mut runtime = TradingRuntime::new(self.engine);

        // pass tui state to runtime if available
        if let Some(state) = self.tui_state {
            runtime.set_tui_state(state);
        }

        for feed in self.feeds {
            runtime.add_feed(feed);
        }

        // run
        rt.block_on(runtime.run());
    }

    /// get reference to config
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let yaml = r#"
core: 0
channel_name: channel1
passport_id: 1234
subscriptions:
    hyperliquid:
        kline: [BTC::M5, ETH]
        orderbook: [BTC, ETH]
    paradex:
        orderbook: [BTC, ETH, LINK]
"#;
        let cfg: EngineConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(cfg.core, 0);
        assert_eq!(cfg.channel_name, "channel1");
        assert_eq!(cfg.passport_id, 1234);
        assert_eq!(cfg.venues().len(), 2);
    }

    #[test]
    fn test_parse_venue() {
        assert_eq!(parse_venue("hyperliquid"), Some(Venue::Hyperliquid));
        assert_eq!(parse_venue("paradex"), Some(Venue::Paradex));
        assert_eq!(parse_venue("unknown"), None);
    }

    #[test]
    fn test_parse_kline_sub() {
        let (sym, int) = parse_kline_sub("BTC::M5");
        assert_eq!(sym, "BTC");
        assert_eq!(int, KlineInterval::M5);

        let (sym2, int2) = parse_kline_sub("ETH");
        assert_eq!(sym2, "ETH");
        assert_eq!(int2, KlineInterval::M1);
    }
}
