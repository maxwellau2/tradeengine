use serde::Deserialize;
use std::collections::HashMap;
use std::io::Read;
use std::thread;

use std::{error::Error, fs::File};

use ringbuf::{HeapCons, HeapRb, traits::Split};

use crate::core_utils::{pin_to_core, validate_cores};
use crate::md_connectors::base::md_feed_base::MDFeed;
use crate::md_connectors::hyperliquid::feed::HyperliquidMDFeed;
use crate::strategy::engine::MarketDataEngine;
use crate::strategy::strategy::Strategy;
use crate::tui::SharedTUIState;
use crate::types::common::{KlineInterval, PassportId, Venue};
use crate::types::packet::{MDMessage, Packet};

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
/// main_core: 1
/// io_core: 3
/// channel_name: channel1
/// passport_id: 1234
/// subscriptions:
///     hyperliquid:
///         kline: [BTC, ETH, LINK]
///         orderbook: [BTC, ETH, LINK]
/// ```
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    /// cpu core for main engine loop (hot path)
    pub main_core: usize,
    /// cpu core for tokio runtime (md feeds, async i/o)
    pub io_core: usize,
    pub channel_name: String,
    pub passport_id: PassportId,
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

    /// get kline symbols for a venue
    pub fn kline_symbols(&self, venue: &str) -> Vec<String> {
        self.subscriptions
            .get(venue)
            .map(|v| v.kline.clone())
            .unwrap_or_default()
    }

    /// get orderbook symbols for a venue
    pub fn orderbook_symbols(&self, venue: &str) -> Vec<String> {
        self.subscriptions
            .get(venue)
            .map(|v| v.orderbook.clone())
            .unwrap_or_default()
    }

    /// get all configured venue names
    pub fn venues(&self) -> Vec<String> {
        self.subscriptions.keys().cloned().collect()
    }
}

/// default ring buffer size for md feeds
const DEFAULT_RING_SIZE: usize = 1 << 10;

/// converts venue string to Venue enum
fn parse_venue(s: &str) -> Option<Venue> {
    match s.to_lowercase().as_str() {
        "hyperliquid" => Some(Venue::Hyperliquid),
        // add more venues here as needed
        _ => None,
    }
}

/// parse kline interval from string (M1, M5, H1, D1, etc)
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
        _ => {
            tracing::warn!("unknown interval '{}', defaulting to M1", s);
            KlineInterval::M1
        }
    }
}

/// parse kline subscription string "SYMBOL::INTERVAL" or just "SYMBOL"
/// returns (symbol, interval)
fn parse_kline_sub(s: &str) -> (&str, KlineInterval) {
    if let Some((symbol, interval)) = s.split_once("::") {
        (symbol, parse_interval(interval))
    } else {
        // no interval specified, default to M1
        (s, KlineInterval::M1)
    }
}

/// engine runner - loads config and runs the strategy engine
///
/// handles:
/// - parsing yaml config
/// - setting up market data feeds based on subscriptions
/// - connecting ring buffers between feeds and engine
/// - spawning feed tasks
pub struct EngineRunner {
    config: EngineConfig,
    engine: MarketDataEngine,
    tui_state: Option<SharedTUIState>,
}

impl EngineRunner {
    /// create engine runner from config file
    ///
    /// **Concept Explanation:**
    /// This uses the "builder pattern" - we load config, create the engine,
    /// but don't start it yet. Call `run()` to actually start processing.
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

        // determine primary venue (first one in config)
        // used for engine's default venue parameter
        let primary_venue = config
            .venues()
            .first()
            .and_then(|v| parse_venue(v))
            .unwrap_or(Venue::Hyperliquid);

        let mut engine = MarketDataEngine::new(
            strategy,
            config.passport_id,
            primary_venue,
            config.channel_name.clone(),
        );

        // set TUI state on engine if provided
        if let Some(ref state) = tui_state {
            engine.set_tui_state(state.clone());
        }

        Ok(Self {
            config,
            engine,
            tui_state,
        })
    }

    /// setup market data feeds and connect to engine
    ///
    /// spawns async tasks for each venue's feed
    /// must be called from within a tokio runtime
    pub async fn setup_feeds(&mut self) {
        for venue_name in self.config.venues() {
            let Some(venue) = parse_venue(&venue_name) else {
                tracing::warn!("unknown venue: {}, skipping", venue_name);
                continue;
            };

            match venue {
                Venue::Hyperliquid => {
                    self.setup_hyperliquid_feed(&venue_name).await;
                }
                // add other venues here
                _ => {
                    tracing::warn!("venue {:?} not yet supported", venue);
                }
            }
        }
    }

    /// setup hyperliquid market data feed
    async fn setup_hyperliquid_feed(&mut self, venue_name: &str) {
        let subs = self.config.get_venue(venue_name);
        let Some(subs) = subs else { return };

        // build subscription list
        let mut subscriptions = Vec::new();

        // add orderbook subscriptions
        for symbol in &subs.orderbook {
            subscriptions.push(HyperliquidMDFeed::orderbook_subscription(symbol));
        }

        // add kline subscriptions with parsed intervals
        // format: "SYMBOL::INTERVAL" e.g. "BTC::M5" or just "BTC" (defaults to M1)
        for kline_sub in &subs.kline {
            let (symbol, interval) = parse_kline_sub(kline_sub);
            subscriptions.push(HyperliquidMDFeed::kline_subscription(symbol, interval));
        }

        if subscriptions.is_empty() {
            tracing::info!("no subscriptions for hyperliquid, skipping feed");
            return;
        }

        tracing::info!(
            "setting up hyperliquid feed with {} subscriptions",
            subscriptions.len()
        );

        // create ring buffer
        let rb = HeapRb::<Packet<MDMessage>>::new(DEFAULT_RING_SIZE);
        let (prod, cons) = rb.split();

        // create feed
        let feed = HyperliquidMDFeed::new(prod, false, subscriptions);

        // add consumer to engine
        self.engine.add_md_consumer(Venue::Hyperliquid, cons);

        // spawn feed task
        tokio::spawn(async move {
            feed.run_forever(5).await;
        });
    }

    /// run the engine (blocking)
    ///
    /// sets up core pinning and tokio runtime before starting engine.
    /// - main_core: pinned for engine sync loop (hot path)
    /// - io_core: pinned for tokio runtime (md feeds)
    ///
    /// note: do NOT call from within #[tokio::main] - this creates its own runtime
    pub fn run(mut self) {
        let main_core = self.config.main_core;
        let io_core = self.config.io_core;

        // validate cores before starting
        validate_cores(main_core, io_core).expect("invalid core configuration");

        // spawn io runtime and setup feeds
        self.spawn_io_runtime_with_feeds(io_core);

        // pin current thread to main_core for engine loop
        pin_to_core(main_core);

        // run engine on current thread (blocking)
        self.engine.start();
    }

    /// run engine for a specific duration (useful for testing)
    pub fn run_for(mut self, duration: std::time::Duration) {
        let main_core = self.config.main_core;
        let io_core = self.config.io_core;

        validate_cores(main_core, io_core).expect("invalid core configuration");

        self.spawn_io_runtime_with_feeds(io_core);

        pin_to_core(main_core);
        self.engine.start_for_duration(duration);
    }

    /// spawn tokio runtime on io_core and setup feeds inside it
    fn spawn_io_runtime_with_feeds(&mut self, io_core: usize) {
        // collect config data needed for feed setup
        let venues: Vec<String> = self.config.venues();
        let subs_map: std::collections::HashMap<String, VenueSubscriptions> = venues
            .iter()
            .filter_map(|v| self.config.get_venue(v).map(|s| (v.clone(), s.clone())))
            .collect();

        // channel to receive consumers from io thread
        let (tx, rx) = std::sync::mpsc::channel::<(Venue, HeapCons<Packet<MDMessage>>)>();
        // channel to signal feed is connected and ready
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        thread::Builder::new()
            .name("io-runtime".into())
            .spawn(move || {
                pin_to_core(io_core);

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime");

                rt.block_on(async {
                    // setup feeds for each venue
                    for venue_name in &venues {
                        if let Some(venue) = parse_venue(venue_name) {
                            if let Some(subs) = subs_map.get(venue_name) {
                                match venue {
                                    Venue::Hyperliquid => {
                                        let mut subscriptions = Vec::new();

                                        for symbol in &subs.orderbook {
                                            subscriptions.push(
                                                HyperliquidMDFeed::orderbook_subscription(symbol),
                                            );
                                        }

                                        for kline_sub in &subs.kline {
                                            let (symbol, interval) = parse_kline_sub(kline_sub);
                                            subscriptions.push(
                                                HyperliquidMDFeed::kline_subscription(
                                                    symbol, interval,
                                                ),
                                            );
                                        }

                                        if !subscriptions.is_empty() {
                                            tracing::info!(
                                                "setting up hyperliquid feed with {} subscriptions",
                                                subscriptions.len()
                                            );

                                            let rb =
                                                HeapRb::<Packet<MDMessage>>::new(DEFAULT_RING_SIZE);
                                            let (prod, cons) = rb.split();

                                            // send consumer back to main thread
                                            let _ = tx.send((Venue::Hyperliquid, cons));

                                            // create feed and attempt initial connection
                                            let feed =
                                                HyperliquidMDFeed::new(prod, false, subscriptions);
                                            let mut client = feed.into_client();

                                            // try to connect with retries
                                            let mut connected = false;
                                            for attempt in 1..=3 {
                                                tracing::info!(
                                                    "hyperliquid feed connection attempt {}/3",
                                                    attempt
                                                );
                                                match client.connect_once().await {
                                                    Ok(_) => {
                                                        tracing::info!(
                                                            "hyperliquid feed connected"
                                                        );
                                                        connected = true;
                                                        break;
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            "connection attempt {} failed: {}",
                                                            attempt,
                                                            e
                                                        );
                                                        tokio::time::sleep(
                                                            std::time::Duration::from_secs(2),
                                                        )
                                                        .await;
                                                    }
                                                }
                                            }

                                            if connected {
                                                let _ = ready_tx.send(Ok(()));
                                                // run this session, then reconnect loop
                                                loop {
                                                    if let Err(e) = client.run_once().await {
                                                        tracing::warn!("feed session error: {}", e);
                                                    }
                                                    tracing::info!(
                                                        "feed disconnected, reconnecting in 5s..."
                                                    );
                                                    tokio::time::sleep(
                                                        std::time::Duration::from_secs(5),
                                                    )
                                                    .await;
                                                    if let Err(e) = client.connect_once().await {
                                                        tracing::warn!("reconnect failed: {}", e);
                                                    }
                                                }
                                            } else {
                                                let _ = ready_tx.send(Err(
                                                    "failed to connect after 3 attempts".into(),
                                                ));
                                            }
                                        }
                                    }
                                    _ => {
                                        tracing::warn!("venue {:?} not yet supported", venue);
                                    }
                                }
                            }
                        }
                    }

                    // keep runtime alive
                    std::future::pending::<()>().await
                });
            })
            .expect("failed to spawn io runtime thread");

        // receive consumers and add to engine
        while let Ok((venue, cons)) = rx.recv_timeout(std::time::Duration::from_secs(1)) {
            self.engine.add_md_consumer(venue, cons);
            tracing::info!("added {:?} consumer to engine", venue);
        }

        // wait for feed to signal ready (connected)
        match ready_rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(Ok(())) => {
                tracing::info!("md feed connected and ready");
            }
            Ok(Err(e)) => {
                tracing::error!("md feed failed to connect: {}", e);
                panic!("md feed connection failed: {}", e);
            }
            Err(_) => {
                tracing::error!("timeout waiting for md feed connection");
                panic!("md feed connection timeout");
            }
        }
    }

    /// get reference to config
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// get mutable reference to engine (for advanced setup)
    pub fn engine_mut(&mut self) -> &mut MarketDataEngine {
        &mut self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let yaml = r#"
main_core: 1
io_core: 3
channel_name: channel1
passport_id: 1234
subscriptions:
    hyperliquid:
        kline: [BTC, ETH, LINK]
        orderbook: [BTC, ETH]
    binance:
        kline: [BTCUSDT]
        orderbook: [BTCUSDT, ETHUSDT]
"#;
        let cfg: EngineConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(cfg.main_core, 1);
        assert_eq!(cfg.io_core, 3);
        assert_eq!(cfg.channel_name, "channel1");
        assert_eq!(cfg.passport_id, 1234);

        // hyperliquid
        assert_eq!(cfg.kline_symbols("hyperliquid"), vec!["BTC", "ETH", "LINK"]);
        assert_eq!(cfg.orderbook_symbols("hyperliquid"), vec!["BTC", "ETH"]);

        // binance
        assert_eq!(cfg.kline_symbols("binance"), vec!["BTCUSDT"]);
        assert_eq!(cfg.orderbook_symbols("binance"), vec!["BTCUSDT", "ETHUSDT"]);

        // unknown venue returns empty
        assert_eq!(cfg.kline_symbols("unknown"), Vec::<String>::new());

        assert_eq!(cfg.venues().len(), 2);
    }

    #[test]
    fn test_parse_venue() {
        assert_eq!(parse_venue("hyperliquid"), Some(Venue::Hyperliquid));
        assert_eq!(parse_venue("HYPERLIQUID"), Some(Venue::Hyperliquid));
        assert_eq!(parse_venue("unknown"), None);
    }
}
