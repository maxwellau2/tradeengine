use crate::state_management::order_manager::{Order, OrderState, PlaceOrder, CancelOrder, ReplaceOrder};
use crate::strategy::context::{EngineState, OrderGateway, OrderResult, Position, StrategyContext};
use crate::strategy::strategy::Strategy;
use crate::types::{
    common::Venue,
    kline::Kline,
    orderbook::Orderbook,
    packet::{MessageBody, Packet},
};
use async_trait::async_trait;
use log::{debug, info, warn};
use ringbuf::{traits::Consumer, HeapCons};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

/// Aeron-based order gateway implementation
pub struct AeronOrderGateway {
    // aeron_publisher: AeronPublisher, // TODO: Add actual Aeron publisher
    orders_sent: u64,
}

impl AeronOrderGateway {
    pub fn new() -> Self {
        Self {
            // aeron_publisher: AeronPublisher::new(),
            orders_sent: 0,
        }
    }
}

#[async_trait]
impl OrderGateway for AeronOrderGateway {
    async fn place_order(&mut self, order: PlaceOrder) -> OrderResult<String> {
        // TODO: Serialize and publish to Aeron
        // let serialized = serialize_order(&order);
        // self.aeron_publisher.publish(serialized)?;

        self.orders_sent += 1;
        info!(
            "Placed order: {:?} {} @ {} (total sent: {})",
            order.side, order.qty, order.price, self.orders_sent
        );

        // Return client order ID
        Ok(order.client_order_id.clone())
    }

    async fn cancel_order(&mut self, cancel: CancelOrder) -> OrderResult<()> {
        // TODO: Serialize and publish to Aeron
        info!("Cancelled order: {}", cancel.client_order_id);
        Ok(())
    }

    async fn replace_order(&mut self, replace: ReplaceOrder) -> OrderResult<()> {
        // TODO: Serialize and publish to Aeron
        info!("Replaced order: {}", replace.client_order_id);
        Ok(())
    }
}

/// Reconciliation state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconState {
    NotStarted,
    InProgress,
    Success,
    Failed,
}

/// Market data engine - owns state, polls queues, dispatches to strategy
pub struct MarketDataEngine {
    /// The trading strategy
    strategy: Box<dyn Strategy>,

    /// Context passed to strategy (contains order gateway + state)
    context: StrategyContext,

    /// Engine state (owned by engine, shared with context via Arc<RwLock>)
    state: Arc<RwLock<EngineState>>,

    /// Market data consumers (one per exchange)
    md_consumers: HashMap<Venue, HeapCons<Packet<MessageBody>>>,

    /// Reconciliation state
    recon_state: ReconState,

    /// Running flag
    running: bool,
}

impl MarketDataEngine {
    /// Create a new engine with a strategy
    pub fn new(strategy: Box<dyn Strategy>) -> Self {
        // Create shared state
        let state = Arc::new(RwLock::new(EngineState::new()));

        // Create order gateway
        let order_gateway = Arc::new(Mutex::new(AeronOrderGateway::new()));

        // Create context
        let context = StrategyContext::new(order_gateway, Arc::clone(&state));

        Self {
            strategy,
            context,
            state,
            md_consumers: HashMap::new(),
            recon_state: ReconState::NotStarted,
            running: false,
        }
    }

    /// Add a market data consumer for a venue
    pub fn add_md_consumer(
        &mut self,
        venue: Venue,
        consumer: HeapCons<Packet<MessageBody>>,
    ) {
        info!("Added MD consumer for venue: {:?}", venue);
        self.md_consumers.insert(venue, consumer);
    }

    /// Start the engine - main event loop
    pub async fn start(&mut self) {
        info!("Starting MarketDataEngine");
        self.running = true;

        // Call strategy on_start
        self.strategy.on_start(&self.context).await;

        // Start reconciliation
        self.start_recon().await;

        // Main event loop
        while self.running {
            // Poll all market data consumers in round-robin fashion
            // Collect packets first to avoid borrow checker issues
            let mut packets = Vec::new();
            for (venue, consumer) in &mut self.md_consumers {
                while let Some(packet) = consumer.try_pop() {
                    packets.push((venue.clone(), packet));
                }
            }

            // Process collected packets
            for (venue, packet) in packets {
                self.handle_md_packet(&venue, packet).await;
            }

            // TODO: Poll execution feed (order updates, fills, position updates)
            // This would come from another SPSC queue or Aeron subscription

            // Yield to allow other tasks to run
            tokio::task::yield_now().await;
        }

        info!("MarketDataEngine stopped");
    }

    /// Handle market data packet
    async fn handle_md_packet(&mut self, _venue: &Venue, packet: Packet<MessageBody>) {
        match packet.body {
            MessageBody::Orderbook(orderbook) => {
                // Dispatch to strategy
                self.strategy.on_orderbook(&orderbook, &self.context).await;
            }
            // Add other message types (klines, trades, etc.) here
        }
    }

    /// Stop the engine
    pub fn stop(&mut self) {
        info!("Stopping MarketDataEngine");
        self.running = false;
    }

    /// Start reconciliation process
    pub async fn start_recon(&mut self) {
        info!("Starting reconciliation");
        self.recon_state = ReconState::InProgress;

        // Notify strategy
        self.strategy.on_recon(&self.context).await;

        // TODO: Request snapshots from trade server via Aeron
        // - Request all open orders
        // - Request all positions
        // - Request account balances

        // For now, simulate success
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        self.recon_success().await;
    }

    /// Mark reconciliation as done
    async fn recon_done(&mut self) {
        info!("Reconciliation done");
        self.strategy.on_recon_done(&self.context).await;
    }

    /// Handle reconciliation success
    async fn recon_success(&mut self) {
        info!("Reconciliation succeeded");
        self.recon_state = ReconState::Success;

        self.strategy.on_recon_success(&self.context).await;
        self.recon_done().await;
    }

    /// Handle reconciliation failure
    async fn recon_failure(&mut self) {
        warn!("Reconciliation failed");
        self.recon_state = ReconState::Failed;

        self.strategy.on_recon_fail(&self.context).await;
        self.recon_done().await;
    }

    /// Handle order update from trade server
    /// Updates internal state BEFORE calling strategy handler
    pub async fn on_order_update(&mut self, order: Order) {
        debug!("Order update: {:?} -> {:?}", order.client_order_id, order.state);

        // Update state first
        {
            let mut state = self.state.write().await;

            match order.state {
                OrderState::FILLED | OrderState::CANCELLED | OrderState::REJECTED => {
                    // Terminal states - remove from open orders
                    state.open_orders.remove(&order.client_order_id);
                }
                _ => {
                    // Non-terminal states - update/add to open orders
                    state.open_orders.insert(order.client_order_id.clone(), order.clone());
                }
            }
        }

        // Then notify strategy
        self.strategy.on_order_update(&order, &self.context).await;
    }

    /// Handle fill event from trade server
    /// Updates positions/balances BEFORE calling strategy handler
    pub async fn on_fill(&mut self, order: Order) {
        info!(
            "Fill: {:?} {} @ {} (filled: {}/{})",
            order.side, order.symbol, order.price, order.filled_qty, order.qty
        );

        // Update positions and balances
        {
            let _state = self.state.write().await;

            // TODO: Update position based on fill
            // This requires calculating:
            // - New quantity (existing + fill_qty, considering side)
            // - New average entry price
            // - Realized PnL (if closing position)

            // For now, just log
            debug!("Would update position for {:?} {}", order.venue, order.symbol);
        }

        // Notify strategy
        self.strategy.on_fill(&order, &self.context).await;
    }

    /// Handle position update from trade server
    pub async fn on_position_update(&mut self, position: Position) {
        debug!("Position update: {:?} {} qty={}", position.venue, position.symbol, position.quantity);

        // Update state
        {
            let mut state = self.state.write().await;
            state.positions.insert(
                (position.venue.clone(), position.symbol.clone()),
                position,
            );
        }

        // Notify strategy
        self.strategy.on_position_update(&self.context).await;
    }

    /// Get read-only access to engine state
    pub async fn get_state(&self) -> tokio::sync::RwLockReadGuard<EngineState> {
        self.state.read().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyStrategy;

    #[async_trait]
    impl Strategy for DummyStrategy {
        async fn on_orderbook(&mut self, _orderbook: &Orderbook, _ctx: &StrategyContext) {}
        async fn on_kline(&mut self, _kline: &Kline, _ctx: &StrategyContext) {}
        async fn on_start(&mut self, _ctx: &StrategyContext) {}
        async fn on_disconnect(&mut self, _ctx: &StrategyContext) {}
        async fn on_recon(&mut self, _ctx: &StrategyContext) {}
        async fn on_recon_done(&mut self, _ctx: &StrategyContext) {}
        async fn on_recon_success(&mut self, _ctx: &StrategyContext) {}
        async fn on_recon_fail(&mut self, _ctx: &StrategyContext) {}
        async fn on_order_update(&mut self, _order: &Order, _ctx: &StrategyContext) {}
        async fn on_fill(&mut self, _order: &Order, _ctx: &StrategyContext) {}
        async fn on_position_update(&mut self, _ctx: &StrategyContext) {}
    }

    #[tokio::test]
    async fn test_engine_creation() {
        let strategy = Box::new(DummyStrategy);
        let engine = MarketDataEngine::new(strategy);

        let state = engine.get_state().await;
        assert_eq!(state.open_orders.len(), 0);
        assert_eq!(state.positions.len(), 0);
    }
}
