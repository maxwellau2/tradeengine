use md_feed::{
    exchange_connectors::{base::md_feed_base::MDFeed, hyperliquid::feed::HyperliquidMDFeed},
    strategy::{
        context::{OrderGatewayRecv, OrderGatewaySend, StrategyContext},
        engine::MarketDataEngine,
        strategy::Strategy,
    },
    types::{
        common::{Order, Venue},
        kline::Kline,
        orderbook::Orderbook,
        packet::{MDMessage, Packet},
        trade_server::{EngineTSMessage, TSEngineMessage},
    },
};
use ringbuf::{HeapRb, traits::Split};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Counting strategy that tracks how many orderbook updates were received
/// Uses Arc<Mutex<>> for thread-safe sharing with the test
struct CountingStrategy {
    orderbook_count: Arc<Mutex<usize>>,
}

impl CountingStrategy {
    fn new(counter: Arc<Mutex<usize>>) -> Self {
        Self {
            orderbook_count: counter,
        }
    }
}

impl Strategy for CountingStrategy {
    fn on_orderbook(&mut self, _orderbook: &Orderbook, _ctx: &StrategyContext) {
        // println!("ob recv");
        *self.orderbook_count.lock().unwrap() += 1;
    }

    fn on_kline(&mut self, _kline: &Kline, _ctx: &StrategyContext) {}
    fn on_start(&mut self, _ctx: &StrategyContext) {}
    fn on_disconnect(&mut self, _ctx: &StrategyContext) {}
    fn on_recon(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_done(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_success(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_fail(&mut self, _ctx: &StrategyContext) {}
    fn on_order_update(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    fn on_fill(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    fn on_position_update(&mut self, _ctx: &StrategyContext) {}
}

// Dummy trade server gateway for testing
struct DummySender;
impl OrderGatewaySend for DummySender {
    fn send(&mut self, _message: EngineTSMessage) -> Result<(), String> {
        Ok(())
    }
}

struct DummyReceiver;
impl OrderGatewayRecv for DummyReceiver {
    fn recv(&mut self) -> Option<TSEngineMessage> {
        None
    }
}

#[tokio::test]
async fn test_engine_receives_orderbook_data() {
    // Shared counter between test and strategy (thread-safe)
    let orderbook_count = Arc::new(Mutex::new(0_usize));

    // Create strategy with shared counter
    let strategy = Box::new(CountingStrategy::new(Arc::clone(&orderbook_count)));

    // Create dummy trade server gateway (no real trade server needed)
    let sender = DummySender;
    let receiver = DummyReceiver;

    // Set up market data feed (ringbuf for zero-copy data transfer)
    const RING_CAPACITY: usize = 1 << 10;
    let rb = HeapRb::<Packet<MDMessage>>::new(RING_CAPACITY);
    let (prod, cons) = rb.split();
    let subscriptions = vec![
        HyperliquidMDFeed::orderbook_subscription("ETH"),
        HyperliquidMDFeed::orderbook_subscription("BTC"),
    ];

    let feed = HyperliquidMDFeed::new(prod, false, subscriptions);
    tokio::spawn(async move { feed.run_forever(5).await });

    // Give the feed time to connect
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("Starting engine...");

    // Run engine in blocking task (doesn't block tokio runtime)
    let engine_handle = tokio::task::spawn_blocking(move || {
        let mut engine = MarketDataEngine::new(strategy, sender, receiver);
        engine.add_md_consumer(Venue::Hyperliquid, cons);
        engine.start_for_duration(Duration::from_secs(10));
    });

    // Wait for engine to finish
    engine_handle.await.unwrap();

    // Assert: We should have received at least 5 orderbook updates
    let count = *orderbook_count.lock().unwrap();
    assert!(
        count >= 5,
        "Expected at least 5 orderbook updates, got {}",
        count
    );

    println!(
        " Test passed! Received {} orderbook updates in 10 seconds",
        count
    );
}
