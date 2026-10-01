use std::sync::atomic::{AtomicI8, AtomicU8};

use log::warn;
use md_feed::{
    md_connectors::{base::md_feed_base::MDFeed, hyperliquid::feed::HyperliquidMDFeed},
    strategy::{
        context::{OrderGatewayRecv, OrderGatewaySend, StrategyContext},
        engine::MarketDataEngine,
        strategy::Strategy,
    },
    ts_protocol::iceoryx2_wrapper::EngineIceoryx2Wrapper,
    types::{
        common::{
            ClientOrderId, Order, OrderType, PassportId, Side, TimeInForce, Venue,
            client_order_id_from_u32,
        },
        kline::Kline,
        orderbook::Orderbook,
        packet::{MDMessage, Packet},
        trade_server::{EngineTSMessage, PlaceOrder, TSEngineMessage},
    },
};
use ringbuf::{HeapRb, traits::Split};
use tracing::{debug, info};

struct DummyStrategy {
    passport_id: PassportId,
    cloid: AtomicU8,
}

impl DummyStrategy {
    fn next_cloid(&mut self) -> u8 {
        self.cloid
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Strategy for DummyStrategy {
    fn on_orderbook(&mut self, ob: &Orderbook, _ctx: &StrategyContext) {
        debug!("Orderbook Received! {:?}", ob);
        // let id = self.next_cloid();
        // let order = PlaceOrder::new(
        //     ob.symbol,
        //     ob.venue,
        //     client_order_id_from_u8(id),
        //     ob.bids[3].price,
        //     11.0 / ob.bids[3].price,
        //     Side::LONG,
        //     TimeInForce::PO,
        //     OrderType::LIMIT,
        //     self.passport_id,
        // );

        // // check if duplicate exists before placing
        // if _ctx.has_duplicate(&order) {
        //     debug!("skipping duplicate order");
        //     return;
        // }
        // let res = _ctx.place_order(order);
        // match res {
        //     Ok(val) => {
        //         info!("Placed order {:?}", val);
        //     }
        //     Err(e) => {
        //         warn!("Error {:?}", e);
        //     }
        // }
    }
    fn on_kline(&mut self, _kline: &Kline, _ctx: &StrategyContext) {
        // info!("Kline Received! {:?}", _kline);
    }
    fn on_start(&mut self, _ctx: &StrategyContext) {}
    fn on_disconnect(&mut self, _ctx: &StrategyContext) {}
    fn on_recon(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_done(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_success(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_fail(&mut self, _ctx: &StrategyContext) {}
    fn on_order_update(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    fn on_fill(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    fn on_position_update(&mut self, _ctx: &StrategyContext) {}
    fn on_trade(&mut self, trade: &md_feed::types::trade::Trade, ctx: &StrategyContext) {}
}

async fn test_engine_creation() {
    let strategy = Box::new(DummyStrategy {
        passport_id: 123,
        cloid: AtomicU8::new(0),
    });
    // Dummy sender/receiver for testing (no real trade server)
    // struct DummySender;
    // impl OrderGatewaySend for DummySender {
    //     fn send(&mut self, _message: EngineTSMessage) -> Result<(), String> {
    //         Ok(())
    //     }
    // }

    // struct DummyReceiver;
    // impl OrderGatewayRecv for DummyReceiver {
    //     fn recv(&mut self) -> Option<TSEngineMessage> {
    //         None
    //     }
    // }

    // Create dummy sender/receiver for this demo
    // In production, you'd use: EngineIceoryx2Wrapper::new("channel").unwrap().split()
    // let sender = DummySender;
    // let receiver = DummyReceiver;
    const RING_CAPACITY: usize = 1 << 10;
    let channel_name = "channel1";
    let mut engine = MarketDataEngine::new(strategy, 123, Venue::Hyperliquid, channel_name.into());
    let rb = HeapRb::<Packet<MDMessage>>::new(RING_CAPACITY);
    let (prod, cons) = rb.split();
    let subscriptions = vec![
        HyperliquidMDFeed::orderbook_subscription("PURR"),
        // HyperliquidMDFeed::orderbook_subscription("BTC"),
        // HyperliquidMDFeed::kline_subscription("XLM", md_feed::types::common::KlineInterval::M1),
    ];
    let feed = HyperliquidMDFeed::new(prod, false, subscriptions);
    tokio::spawn(async move { feed.run_forever(5).await });
    engine.add_md_consumer(Venue::Hyperliquid, cons);
    engine.start();
}

#[tokio::main]
pub async fn main() {
    // use RUST_LOG env var, default to info if not set
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_line_number(true)
        .with_file(true)
        .init();

    test_engine_creation().await;
}
