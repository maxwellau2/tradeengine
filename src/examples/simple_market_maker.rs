/// Example: Simple market maker strategy using the MarketDataEngine
///
/// This demonstrates:
/// - Implementing the Strategy trait
/// - Reading state (positions, orders) via context
/// - Placing orders via context
/// - Connecting market data feeds to the engine

use md_feed::{
    exchange_connectors::{
        base::md_feed_base::MDFeed,
        hyperliquid::feed::HyperliquidMDFeed,
    },
    state_management::order_manager::{Order, OrderType, PlaceOrder, Side, TimeInForce},
    strategy::{
        context::StrategyContext,
        engine::MarketDataEngine,
        strategy::Strategy,
    },
    types::{
        common::Venue,
        kline::Kline,
        orderbook::Orderbook,
        packet::{MessageBody, Packet},
    },
};
use async_trait::async_trait;
use log::info;
use ringbuf::{HeapRb, traits::Split};

/// Simple market maker strategy
pub struct SimpleMarketMaker {
    spread_bps: f64,           // Spread in basis points (100 bps = 1%)
    order_size: f64,           // Size per order
    max_position: f64,         // Maximum position size
    symbol: String,            // Symbol to trade
    venue: Venue,              // Venue to trade on
}

impl SimpleMarketMaker {
    pub fn new(symbol: String, venue: Venue) -> Self {
        Self {
            spread_bps: 10.0,  // 10 bps = 0.1%
            order_size: 1.0,
            max_position: 10.0,
            symbol,
            venue,
        }
    }
}

#[async_trait]
impl Strategy for SimpleMarketMaker {
    async fn on_orderbook(&mut self, book: &Orderbook, ctx: &StrategyContext) {
        // Check if this is for our symbol
        if book.bids.is_empty() || book.asks.is_empty() {
            return;
        }

        // Read current position
        let position = ctx.get_position(&self.venue, &self.symbol).await
            .map(|p| p.quantity)
            .unwrap_or(0.0);

        info!("Current position: {} {}", position, self.symbol);

        // Read open orders
        let open_orders = ctx.get_open_orders().await;
        if !open_orders.is_empty() {
            info!("Already have {} open orders, skipping", open_orders.len());
            return;
        }

        // Check position limits
        if position.abs() >= self.max_position {
            info!("Position limit reached, not placing new orders");
            return;
        }

        // Calculate quote prices
        let mid = (book.bids[0].price + book.asks[0].price) / 2.0;
        let spread_multiplier = self.spread_bps / 10000.0;
        let buy_price = mid * (1.0 - spread_multiplier);
        let sell_price = mid * (1.0 + spread_multiplier);

        info!("Mid: {}, Buy: {}, Sell: {}", mid, buy_price, sell_price);

        // Place buy order if not too long
        if position < self.max_position {
            let order = PlaceOrder {
                symbol: self.symbol.clone(),
                venue: self.venue.clone(),
                side: Side::LONG,
                client_order_id: format!("buy_{}", book.timestamp),
                qty: self.order_size,
                price: buy_price,
                order_type: OrderType::LIMIT,
                time_in_force: TimeInForce::GTC,
            };

            if let Ok(order_id) = ctx.order_gateway.lock().await.place_order(order).await {
                info!("Placed buy order: {}", order_id);
            }
        }

        // Place sell order if not too short
        if position > -self.max_position {
            let order = PlaceOrder {
                symbol: self.symbol.clone(),
                venue: self.venue.clone(),
                side: Side::SHORT,
                client_order_id: format!("sell_{}", book.timestamp),
                qty: self.order_size,
                price: sell_price,
                order_type: OrderType::LIMIT,
                time_in_force: TimeInForce::GTC,
            };

            if let Ok(order_id) = ctx.order_gateway.lock().await.place_order(order).await {
                info!("Placed sell order: {}", order_id);
            }
        }
    }

    async fn on_kline(&mut self, _kline: &Kline, _ctx: &StrategyContext) {
        // Not using klines in this strategy
    }

    async fn on_start(&mut self, _ctx: &StrategyContext) {
        info!("SimpleMarketMaker starting for {} on {:?}", self.symbol, self.venue);
    }

    async fn on_disconnect(&mut self, _ctx: &StrategyContext) {
        info!("Disconnected from exchange");
    }

    async fn on_recon(&mut self, _ctx: &StrategyContext) {
        info!("Starting reconciliation");
    }

    async fn on_recon_done(&mut self, _ctx: &StrategyContext) {
        info!("Reconciliation complete");
    }

    async fn on_recon_success(&mut self, _ctx: &StrategyContext) {
        info!("Reconciliation succeeded");
    }

    async fn on_recon_fail(&mut self, _ctx: &StrategyContext) {
        info!("Reconciliation failed");
    }

    async fn on_order_update(&mut self, order: &Order, _ctx: &StrategyContext) {
        info!("Order update: {:?} -> {:?}", order.client_order_id, order.state);
    }

    async fn on_fill(&mut self, order: &Order, _ctx: &StrategyContext) {
        info!("Fill: {:?} {} @ {}", order.side, order.filled_qty, order.price);
    }

    async fn on_position_update(&mut self, ctx: &StrategyContext) {
        let position = ctx.get_position(&self.venue, &self.symbol).await;
        info!("Position updated: {:?}", position);
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Create ringbuffer for market data
    const RING_CAPACITY: usize = 1 << 15;
    let rb = HeapRb::<Packet<MessageBody>>::new(RING_CAPACITY);
    let (prod, cons) = rb.split();

    // Create market data feed
    let subscriptions = vec![
        serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": "l2Book", "coin": "ETH" }
        }),
    ];

    let feed = HyperliquidMDFeed::new(prod, false, subscriptions);

    // Create strategy
    let strategy = Box::new(SimpleMarketMaker::new("ETH".to_string(), Venue::Hyperliquid));

    // Create engine
    let mut engine = MarketDataEngine::new(strategy);

    // Add market data consumer
    engine.add_md_consumer(Venue::Hyperliquid, cons);

    // Spawn feed in background
    tokio::spawn(async move {
        feed.into_client().run_forever(5).await
    });

    // Start engine (blocking)
    engine.start().await;
}
