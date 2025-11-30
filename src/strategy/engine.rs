use crate::strategy::context::{
    EngineState, OrderGateway, OrderGatewayRecv, OrderGatewaySend, Position, StrategyContext,
    TradeServerError, TradeServerResult,
};
use crate::strategy::strategy::Strategy;
use crate::ts_protocol::iceoryx2_wrapper::EngineIceoryx2Wrapper;
use crate::types::clock::timestamp_micros;
use crate::types::common::{
    Order, OrderState, PassportId, Venue, client_order_id_to_str, symbol_to_str,
};
use crate::types::trade_server::{
    CancelOrder, EngineTSMessage, EngineTSMessageType, Heartbeat, PlaceOrder, ReplaceOrder,
    TSEngineMessage, TSEngineMessageType,
};
use crate::types::{
    kline::Kline,
    orderbook::Orderbook,
    packet::{MDMessage, Packet},
};
use ringbuf::{HeapCons, traits::Consumer};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::thread;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Generic order gateway that wraps any low-level sender
///
/// **Concept Explanation:**
/// This struct implements the high-level `OrderGateway` trait (async, user-friendly API)
/// by wrapping any type that implements `OrderGatewaySend` (sync, low-level send).
///
/// This allows us to swap out the underlying transport (iceoryx2, Aeron, etc.)
/// without changing the engine or strategy code.
pub struct TradeServerGateway<S: OrderGatewaySend> {
    sender: S,
    orders_sent: u64,
}

impl<S: OrderGatewaySend> TradeServerGateway<S> {
    pub fn new(sender: S) -> Self {
        Self {
            sender,
            orders_sent: 0,
        }
    }
}

impl<S: OrderGatewaySend> OrderGateway for TradeServerGateway<S> {
    fn place_order(&mut self, order: PlaceOrder) -> TradeServerResult<String> {
        self.orders_sent += 1;

        // Create the low-level message with timestamp
        let message = EngineTSMessage {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            message: EngineTSMessageType::PlaceOrder(order),
        };

        // Send via the low-level sender
        self.sender
            .send(message)
            .map_err(TradeServerError::Iceoryx2PublishError)?;

        info!(
            "Placed order #{}: {:?} {} @ {}",
            self.orders_sent, order.side, order.qty, order.price
        );

        // Return client order ID
        Ok(client_order_id_to_str(&order.client_order_id).to_string())
    }

    fn cancel_order(&mut self, cancel: CancelOrder) -> TradeServerResult<()> {
        let message = EngineTSMessage {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            message: EngineTSMessageType::CancelOrder(cancel),
        };

        self.sender
            .send(message)
            .map_err(TradeServerError::Iceoryx2PublishError)?;

        info!(
            "Cancelled order: {}",
            client_order_id_to_str(&cancel.client_order_id)
        );
        Ok(())
    }

    fn replace_order(&mut self, replace: ReplaceOrder) -> TradeServerResult<()> {
        let message = EngineTSMessage {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            message: EngineTSMessageType::ReplaceOrder(replace),
        };

        self.sender
            .send(message)
            .map_err(TradeServerError::Iceoryx2PublishError)?;

        info!(
            "Replaced order: {}",
            client_order_id_to_str(&replace.client_order_id)
        );
        Ok(())
    }

    fn send_heartbeat(&mut self, hb: Heartbeat) -> TradeServerResult<()> {
        let message = EngineTSMessage {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64,
            message: EngineTSMessageType::Heartbeat(hb),
        };

        self.sender
            .send(message)
            .map_err(TradeServerError::Iceoryx2PublishError)?;
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

pub struct MarketDataEngine {
    /// The trading strategy
    strategy: Box<dyn Strategy>,
    context: StrategyContext,
    /// Engine state (owned by engine, shared with context via Rc<RefCell>)
    state: Rc<RefCell<EngineState>>,
    /// Trade server receiver - polls for incoming messages
    ts_receiver: Box<dyn OrderGatewayRecv>,
    /// Market data consumers (one per exchange)
    md_consumers: HashMap<Venue, HeapCons<Packet<MDMessage>>>,
    /// Reconciliation state
    recon_state: ReconState,
    /// Running flag
    running: bool,

    last_heartbeat_sent: u64,
    last_heartbeat_recv: u64,
    heartbeat_cycle: u64,
    passport_id: PassportId,
}

impl MarketDataEngine {
    pub fn new(
        strategy: Box<dyn Strategy>,
        // ts_sender: impl OrderGatewaySend + 'static,
        // ts_receiver: impl OrderGatewayRecv + 'static,
        passport_id: PassportId,
        channel_name: String,
    ) -> Self {
        let io: EngineIceoryx2Wrapper = EngineIceoryx2Wrapper::new(channel_name).expect("wtf");
        let (sender, receiver) = io.split();

        // Create shared state
        let state = Rc::new(RefCell::new(EngineState::new()));

        // Wrap the low-level sender in a high-level gateway
        let order_gateway = Rc::new(RefCell::new(TradeServerGateway::new(sender)));

        // Create context
        let context = StrategyContext::new(order_gateway, Rc::clone(&state));

        Self {
            strategy,
            context,
            state,
            ts_receiver: Box::new(receiver),
            md_consumers: HashMap::new(),
            recon_state: ReconState::NotStarted,
            running: false,
            last_heartbeat_sent: 0,
            last_heartbeat_recv: 0,
            heartbeat_cycle: Duration::from_secs(5).as_micros() as u64,
            passport_id,
        }
    }

    /// Add a market data consumer for a venue
    pub fn add_md_consumer(&mut self, venue: Venue, consumer: HeapCons<Packet<MDMessage>>) {
        info!("Added MD consumer for venue: {:?}", venue);
        self.md_consumers.insert(venue, consumer);
    }

    pub fn send_heartbeat(&mut self) {
        self.context
            .order_gateway
            .borrow_mut()
            .send_heartbeat(Heartbeat {
                passport_id: self.passport_id.clone(),
            });
        self.last_heartbeat_sent = timestamp_micros();
    }

    /// Run one iteration of the event loop
    pub fn run_once(&mut self) {
        // Poll trade server for incoming messages (order updates, fills, position updates)
        while let Some(ts_message) = self.ts_receiver.recv() {
            self.handle_ts_message(ts_message);
        }
        // we need to check if the TS is even alive to send the orders, this is a safety mechanism!
        let timenow = timestamp_micros();
        if timenow - self.last_heartbeat_sent > self.heartbeat_cycle {
            self.send_heartbeat();
        }
        if timenow - self.last_heartbeat_recv > self.heartbeat_cycle {
            warn!(
                "No response from TS in the last {} microseconds. Is the TS alive?",
                self.heartbeat_cycle
            );
            return;
        }
        // Poll all market data consumers in round-robin fashion
        // Collect packets first to avoid borrow checker issues
        // Venue is Copy (1-2 bytes), so clone is free
        let mut packets = Vec::new();
        for (venue, consumer) in &mut self.md_consumers {
            while let Some(packet) = consumer.try_pop() {
                packets.push((*venue, packet)); // Copy venue (1-2 bytes, no heap allocation)
            }
        }

        // Process collected packets
        for (venue, packet) in packets {
            self.handle_md_packet(&venue, packet);
        }
        // Yield to allow other tasks to run
        // thread::sleep(Duration::from_micros(1));
        // or we can hint to core we are busy waiting
        core::hint::spin_loop();
    }

    pub fn on_start_protocol(&mut self) {
        // then we call the strategy's on start hook
        self.strategy.on_start(&self.context);
    }

    /// Start the engine - main event loop
    pub fn start(&mut self) {
        info!("Starting MarketDataEngine");
        self.running = true;
        self.on_start_protocol();
        // Start reconciliation
        self.start_recon();

        // Main event loop
        while self.running {
            self.run_once();
        }

        info!("MarketDataEngine stopped");
    }

    /// This method runs the engine for the specified duration, then stops automatically.
    pub fn start_for_duration(&mut self, duration: Duration) {
        info!("Starting MarketDataEngine for {:?}", duration);
        self.running = true;
        self.on_start_protocol();
        // Start reconciliation
        self.start_recon();

        let start_time = std::time::Instant::now();

        // Main event loop with timeout
        while self.running && start_time.elapsed() < duration {
            self.run_once();
        }

        info!("MarketDataEngine stopped after {:?}", start_time.elapsed());
    }

    /// Handle market data packet from ring buffer
    fn handle_md_packet(&mut self, _venue: &Venue, packet: Packet<MDMessage>) {
        match packet.body {
            MDMessage::Orderbook(orderbook) => {
                // Dispatch to strategy
                self.on_orderbook(&orderbook);
            }
            MDMessage::Kline(kline) => {
                self.on_kline(&kline);
            } // Add other message types (klines, trades, etc.) here
        }
    }

    fn handle_ts_message(&mut self, ts_message: TSEngineMessage) {
        debug!(
            "Received TS message at timestamp {}: {:?}",
            ts_message.timestamp, ts_message.message
        );

        match ts_message.message {
            TSEngineMessageType::OrderUpdate => {
                // TODO: Implement order update handling
                // This would extract the order data and call self.on_order_update()
                warn!("OrderUpdate not yet implemented");
            }
            TSEngineMessageType::BalanceUpdate => {
                // TODO: Implement balance update handling
                debug!("Balance update received");
            }
            TSEngineMessageType::PositionUpdate => {
                // TODO: Implement position update handling
                debug!("Position update received");
            }
            TSEngineMessageType::HeartbeatResponse => {
                debug!("Heartbeat received");
                self.last_heartbeat_recv = timestamp_micros();
            }
        }
    }

    pub fn on_orderbook(&mut self, orderbook: &Orderbook) {
        // debug!("Received Obook Data");
        self.strategy.on_orderbook(&orderbook, &self.context);
    }

    pub fn on_kline(&mut self, kline: &Kline) {
        // debug!("Received Kline Data");
        self.strategy.on_kline(&kline, &self.context);
    }

    /// Stop the engine
    pub fn stop(&mut self) {
        info!("Stopping MarketDataEngine");
        self.running = false;
    }

    /// Start reconciliation process
    pub fn start_recon(&mut self) {
        info!("Starting reconciliation");
        self.recon_state = ReconState::InProgress;

        // Notify strategy
        self.strategy.on_recon(&self.context);

        // TODO: Request snapshots from trade server via Aeron
        // - Request all open orders
        // - Request all positions
        // - Request account balances

        // For now, simulate success
        thread::sleep(Duration::from_millis(100));

        self.recon_success();
        self.recon_done();
    }

    /// Mark reconciliation as done
    fn recon_done(&mut self) {
        info!("Reconciliation done");
        self.strategy.on_recon_done(&self.context);
    }

    /// Handle reconciliation success
    fn recon_success(&mut self) {
        info!("Reconciliation succeeded");
        self.recon_state = ReconState::Success;

        self.strategy.on_recon_success(&self.context);
        self.recon_done();
    }

    /// Handle reconciliation failure
    fn recon_failure(&mut self) {
        warn!("Reconciliation failed");
        self.recon_state = ReconState::Failed;

        self.strategy.on_recon_fail(&self.context);
        self.recon_done();
    }

    /// Handle order update from trade server
    /// Updates internal state BEFORE calling strategy handler
    pub fn on_order_update(&mut self, order: Order) {
        debug!(
            "Order update: {} -> {:?}",
            client_order_id_to_str(&order.client_order_id),
            order.state
        );

        // Update state first
        {
            let mut state = self.state.borrow_mut();

            match order.state {
                OrderState::FILLED | OrderState::CANCELLED | OrderState::REJECTED => {
                    // Terminal states - remove from open orders
                    state.open_orders.remove(&order.client_order_id);
                }
                _ => {
                    // Non-terminal states - update/add to open orders
                    state
                        .open_orders
                        .insert(order.client_order_id, order.clone());
                }
            }
        }

        // Then notify strategy
        self.strategy.on_order_update(&order, &self.context);
    }

    /// Handle fill event from trade server
    /// Updates positions/balances BEFORE calling strategy handler
    pub fn on_fill(&mut self, order: Order) {
        info!(
            "Fill: {:?} {} @ {} (filled: {}/{})",
            order.side,
            symbol_to_str(&order.symbol),
            order.price,
            order.filled_qty,
            order.qty
        );

        // Update positions and balances
        {
            let _state = self.state.borrow_mut();

            // TODO: Update position based on fill
            // This requires calculating:
            // - New quantity (existing + fill_qty, considering side)
            // - New average entry price
            // - Realized PnL (if closing position)

            // For now, just log
            debug!(
                "Would update position for {:?} {}",
                order.venue,
                symbol_to_str(&order.symbol)
            );
        }

        // Notify strategy
        self.strategy.on_fill(&order, &self.context);
    }

    /// Handle position update from trade server
    pub fn on_position_update(&mut self, position: Position) {
        debug!(
            "Position update: {:?} {} qty={}",
            position.venue,
            symbol_to_str(&position.symbol),
            position.quantity
        );

        // Update state
        {
            let mut state = self.state.borrow_mut();
            state
                .positions
                .insert((position.venue.clone(), position.symbol), position);
        }

        // Notify strategy
        self.strategy.on_position_update(&self.context);
    }

    /// Get read-only access to engine state
    pub fn get_state(&self) -> std::cell::Ref<EngineState> {
        self.state.borrow()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyStrategy;

    impl Strategy for DummyStrategy {
        fn on_orderbook(&mut self, _orderbook: &Orderbook, _ctx: &StrategyContext) {}
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

    // Dummy sender/receiver for testing
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

    #[test]
    fn test_engine_creation() {
        let strategy = Box::new(DummyStrategy);
        let sender = DummySender;
        let receiver = DummyReceiver;

        let engine = MarketDataEngine::new(strategy, PassportId::new("123"), "channel1".into());

        let state = engine.get_state();
        assert_eq!(state.open_orders.len(), 0);
        assert_eq!(state.positions.len(), 0);
    }
}
