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
        common::{ClientOrderId, Order, OrderType, PassportId, Side, TimeInForce, Venue},
        kline::Kline,
        orderbook::Orderbook,
        packet::{MDMessage, Packet},
        trade_server::{EngineTSMessage, PlaceOrder, TSEngineMessage},
    },
};
use ringbuf::{HeapRb, traits::Split};
use tracing::{Level, debug, info};
use tracing_subscriber;

struct DummyStrategy {
    passport_id: PassportId,
}

impl Strategy for DummyStrategy {
    fn on_orderbook(&mut self, _orderbook: &Orderbook, _ctx: &StrategyContext) {
        debug!("Orderbook Received! {:?}", _orderbook);
        // for i in 1..2{
        let order = PlaceOrder::new(
            _orderbook.symbol,
            _orderbook.venue,
            ClientOrderId::new("1234"),
            123.2,
            123.2,
            Side::LONG,
            TimeInForce::GTC,
            OrderType::LIMIT,
            self.passport_id.clone(),
        );
        let res = _ctx.order_gateway.borrow_mut().place_order(order);
        match res {
            Ok(val) => {
                "yay!";
            }
            Err(e) => {
                warn!("Error {:?}", e);
            }
        }
        // }
    }
    fn on_kline(&mut self, _kline: &Kline, _ctx: &StrategyContext) {
        info!("Kline Received! {:?}", _kline);
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
}

async fn test_engine_creation() {
    let strategy = Box::new(DummyStrategy {
        passport_id: PassportId::new("12345"),
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
    let mut engine = MarketDataEngine::new(strategy, PassportId::new("1234"), channel_name.into());
    let rb = HeapRb::<Packet<MDMessage>>::new(RING_CAPACITY);
    let (prod, cons) = rb.split();
    let subscriptions = vec![
        HyperliquidMDFeed::orderbook_subscription("ETH"),
        HyperliquidMDFeed::orderbook_subscription("BTC"),
        HyperliquidMDFeed::kline_subscription("XLM", md_feed::types::common::KlineInterval::M1),
    ];
    let feed = HyperliquidMDFeed::new(prod, false, subscriptions);
    tokio::spawn(async move { feed.run_forever(5).await });
    engine.add_md_consumer(Venue::Hyperliquid, cons);
    engine.start();
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG) // Filter events at INFO level and above
        .init(); // Install the subscriber
    test_engine_creation().await;
}
