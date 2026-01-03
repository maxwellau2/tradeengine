// test md subscribers directly
//
// tests paradex and hyperliquid subscribers to verify they receive data

use md_feed::md_connectors::hyperliquid::subscriber::{
    HyperliquidMDSubscriber, HyperliquidSubscription,
};
use md_feed::md_connectors::paradex::subscriber::{ParadexMDSubscriber, ParadexSubscription};
use md_feed::types::common::KlineInterval;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() {
    // setup logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::registry()
        .with(fmt::layer().with_line_number(true))
        .with(filter)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let venue = args.get(1).map(|s| s.as_str()).unwrap_or("paradex");

    match venue {
        "paradex" => test_paradex().await,
        "hyperliquid" | "hl" => test_hyperliquid().await,
        "both" => {
            // test both concurrently
            tokio::join!(test_paradex(), test_hyperliquid());
        }
        _ => {
            eprintln!("usage: {} [paradex|hyperliquid|hl|both]", args[0]);
            std::process::exit(1);
        }
    }
}

async fn test_paradex() {
    info!("=== Testing ParadexMDSubscriber ===");

    let subscriptions = vec![
        ParadexSubscription::orderbook("BTC"),
        ParadexSubscription::orderbook("ETH"),
    ];

    info!("subscriptions: {:?}", subscriptions);

    let mut subscriber = ParadexMDSubscriber::new(subscriptions, true);

    info!("connecting...");
    subscriber.connect().await;

    info!("waiting for messages (30 second timeout)...");

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let mut msg_count = 0;

    while start.elapsed() < timeout {
        // use tokio timeout to avoid blocking forever
        match tokio::time::timeout(Duration::from_secs(5), subscriber.produce()).await {
            Ok(Some(msg)) => {
                msg_count += 1;
                info!("PARADEX MSG #{}: {:?}", msg_count, msg);

                // stop after 10 messages
                if msg_count >= 10 {
                    info!("received 10 messages, test passed!");
                    return;
                }
            }
            Ok(None) => {
                // produce returned None (control message or reconnect)
                info!("produce returned None");
            }
            Err(_) => {
                warn!("5 second timeout waiting for message");
            }
        }
    }

    if msg_count == 0 {
        warn!("PARADEX TEST FAILED: no messages received in 30 seconds");
    } else {
        info!("paradex test complete: {} messages received", msg_count);
    }
}

async fn test_hyperliquid() {
    info!("=== Testing HyperliquidMDSubscriber ===");

    let subscriptions = vec![
        HyperliquidSubscription::orderbook("BTC"),
        HyperliquidSubscription::orderbook("ETH"),
        HyperliquidSubscription::kline("BTC", KlineInterval::M1),
    ];

    info!("subscriptions: {:?}", subscriptions);

    let mut subscriber = HyperliquidMDSubscriber::new(subscriptions, true);

    info!("connecting...");
    subscriber.connect().await;

    info!("waiting for messages (30 second timeout)...");

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);
    let mut msg_count = 0;

    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_secs(5), subscriber.produce()).await {
            Ok(Some(msg)) => {
                msg_count += 1;
                info!("HYPERLIQUID MSG #{}: {:?}", msg_count, msg);

                if msg_count >= 10 {
                    info!("received 10 messages, test passed!");
                    return;
                }
            }
            Ok(None) => {
                info!("produce returned None");
            }
            Err(_) => {
                warn!("5 second timeout waiting for message");
            }
        }
    }

    if msg_count == 0 {
        warn!("HYPERLIQUID TEST FAILED: no messages received in 30 seconds");
    } else {
        info!("hyperliquid test complete: {} messages received", msg_count);
    }
}
