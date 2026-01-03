use crate::strategy::context::{
    EngineState, OrderGateway, OrderGatewayRecv, OrderGatewaySend, StrategyContext,
    TradeServerError, TradeServerResult,
};
use crate::strategy::strategy::Strategy;
use crate::ts_protocol::iceoryx2_wrapper::EngineIceoryx2Wrapper;
use crate::tui::{SharedTUIState, TUI, TUIState, should_shutdown};
use crate::types::clock::{timestamp_micros, timestamp_nanos};
use crate::types::common::{
    Order, OrderState, PassportId, Position, Venue, client_order_id_to_str, symbol_to_str,
};
use crate::types::trade_server::{
    CancelOrder, EngineTSMessage, EngineTSMessageType, Heartbeat, PlaceOrder, QryBalance,
    QryBalanceResp, QryOpenOrders, QryOrderResp, QryPositionResp, QryPositions, ReplaceOrder,
    StateUpdate, TSEngineMessage, TSEngineMessageType,
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
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Generic order gateway that wraps any low-level sender
///
/// **Concept Explanation:**
/// This struct implements the high-level `OrderGateway` trait (async, user-friendly API)
/// by wrapping any type that implements `OrderGatewaySend` (sync, low-level send).
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
            timestamp: timestamp_nanos(),
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
            timestamp: timestamp_nanos(),
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
            timestamp: timestamp_nanos(),
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
            timestamp: timestamp_nanos(),
            message: EngineTSMessageType::Heartbeat(hb),
        };

        self.sender
            .send(message)
            .map_err(TradeServerError::Iceoryx2PublishError)?;
        Ok(())
    }

    fn send_raw(&mut self, message: EngineTSMessage) -> TradeServerResult<()> {
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

/// Tracks completion status of individual recon queries
///
/// **Concept Explanation:**
/// During reconciliation, the engine sends 3 separate queries (orders, positions, balances)
/// to the trade server. Each query streams back responses one at a time, with the last
/// response having `is_last = true`. This tracker monitors which queries have completed
/// and whether they succeeded, so we know when full recon is done.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReconTracker {
    /// orders query completed successfully
    pub orders_done: bool,
    pub orders_success: bool,
    /// positions query completed successfully
    pub positions_done: bool,
    pub positions_success: bool,
    /// balances query completed successfully
    pub balances_done: bool,
    pub balances_success: bool,
}

impl ReconTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// reset tracker for new recon attempt
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// mark orders query as complete
    pub fn complete_orders(&mut self, success: bool) {
        self.orders_done = true;
        self.orders_success = success;
    }

    /// mark positions query as complete
    pub fn complete_positions(&mut self, success: bool) {
        self.positions_done = true;
        self.positions_success = success;
    }

    /// mark balances query as complete
    pub fn complete_balances(&mut self, success: bool) {
        self.balances_done = true;
        self.balances_success = success;
    }

    /// check if all queries are done (regardless of success)
    pub fn all_done(&self) -> bool {
        self.orders_done && self.positions_done && self.balances_done
    }

    /// check if all queries completed successfully
    pub fn all_success(&self) -> bool {
        self.all_done() && self.orders_success && self.positions_success && self.balances_success
    }
}

/// recon timeout in microseconds (10 seconds)
const RECON_TIMEOUT_US: u64 = 10_000_000;
/// max recon retry attempts before giving up
const MAX_RECON_RETRIES: u32 = 3;

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
    /// Tracks individual query completion for recon
    recon_tracker: ReconTracker,
    /// Running flag
    running: bool,

    last_heartbeat_sent: u64,
    last_heartbeat_recv: u64,
    heartbeat_cycle: u64,
    passport_id: PassportId,
    venue: Venue,
    /// consecutive heartbeat failures - triggers recon when threshold exceeded
    heartbeat_failures: u32,
    /// optional TUI state for displaying orders/positions/balances
    tui_state: Option<SharedTUIState>,
    /// iteration counter for cold path operations
    run_iter: u64,
    /// timestamp when recon started (for timeout)
    recon_started_at: u64,
    /// number of recon retries attempted
    recon_retries: u32,
}

impl MarketDataEngine {
    pub fn new(
        strategy: Box<dyn Strategy>,
        passport_id: PassportId,
        venue: Venue,
        channel_name: String,
    ) -> Self {
        let io: EngineIceoryx2Wrapper = EngineIceoryx2Wrapper::new(channel_name)
            .expect("failed to create iceoryx2 channel - is the trade server running?");
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
            recon_tracker: ReconTracker::new(),
            running: false,
            // initialize to current time to avoid subtract overflow on first check
            last_heartbeat_sent: timestamp_micros(),
            last_heartbeat_recv: timestamp_micros(),
            heartbeat_cycle: Duration::from_secs(5).as_micros() as u64,
            passport_id,
            venue,
            heartbeat_failures: 0,
            tui_state: None,
            run_iter: 0,
            recon_started_at: 0,
            recon_retries: 0,
        }
    }

    /// set TUI state for displaying orders/positions/balances
    pub fn set_tui_state(&mut self, tui_state: SharedTUIState) {
        self.tui_state = Some(tui_state);
    }

    /// Add a market data consumer for a venue
    pub fn add_md_consumer(&mut self, venue: Venue, consumer: HeapCons<Packet<MDMessage>>) {
        info!(
            "added MD consumer for venue: {:?}, total consumers: {}",
            venue,
            self.md_consumers.len() + 1
        );
        self.md_consumers.insert(venue, consumer);
    }

    /// sync engine state to TUI state for display
    pub fn sync_tui_state(&self) {
        if let Some(tui_state) = &self.tui_state {
            let state = self.state.borrow();
            let current = tui_state.load();

            let new_state = TUIState {
                orders: state.get_all_orders(),
                positions: state.get_all_positions(),
                balances: state.get_all_balances(),
                logs: current.logs.clone(),
            };

            tui_state.store(Arc::new(new_state));
        }
    }

    pub fn send_heartbeat(&mut self) {
        self.context.send_heartbeat(Heartbeat {
            passport_id: self.passport_id.clone(),
        });
        self.last_heartbeat_sent = timestamp_micros();
    }

    /// Run one iteration of the event loop - pure business logic, no TUI
    pub fn run_once(&mut self) {
        // poll trade server for incoming messages (always, even during recon)
        while let Some(ts_message) = self.ts_receiver.recv() {
            self.handle_ts_message(ts_message);
        }

        // poll all market data consumers (always, even during recon)
        let mut packets = Vec::new();
        for (venue, consumer) in &mut self.md_consumers {
            while let Some(packet) = consumer.try_pop() {
                packets.push((*venue, packet));
            }
        }

        // process collected packets - only dispatch to strategy if recon done
        if !packets.is_empty() {
            debug!(
                "polled {} packets, recon_state={:?}, consumers={}",
                packets.len(),
                self.recon_state,
                self.md_consumers.len()
            );
        }

        for (venue, packet) in packets {
            if self.recon_state == ReconState::Success {
                self.handle_md_packet(&venue, packet);
            }
            // during recon, we drain the queue but don't process
            // this prevents the ring buffer from filling up
        }

        // check recon timeout
        if self.recon_state == ReconState::InProgress {
            let elapsed = timestamp_micros().saturating_sub(self.recon_started_at);
            if elapsed > RECON_TIMEOUT_US {
                warn!("recon timeout after {}ms", elapsed / 1000);
                self.recon_retries += 1;

                if self.recon_retries >= MAX_RECON_RETRIES {
                    warn!(
                        "max recon retries ({}) exceeded, giving up",
                        MAX_RECON_RETRIES
                    );
                    self.recon_failure();
                } else {
                    warn!(
                        "retrying recon (attempt {}/{})",
                        self.recon_retries + 1,
                        MAX_RECON_RETRIES
                    );
                    self.start_recon();
                }
            }
            return;
        }

        // skip heartbeat checks if recon not done
        if self.recon_state != ReconState::Success {
            return;
        }

        // check TS liveness - allow 2x heartbeat cycle before warning
        let timenow = timestamp_micros();
        let timeout = self.heartbeat_cycle * 2;
        if timenow.saturating_sub(self.last_heartbeat_recv) > timeout {
            self.heartbeat_failures += 1;
            warn!(
                "No response from TS in {} us (failure #{})",
                timeout, self.heartbeat_failures
            );

            // after 3 consecutive failures, trigger recon to resync
            if self.heartbeat_failures >= 3 {
                warn!("Too many heartbeat failures, triggering recon");
                self.strategy.on_disconnect(&self.context);
                self.start_recon();
            }
            return;
        }

        // reset failure counter on successful heartbeat
        self.heartbeat_failures = 0;

        // send heartbeat periodically
        if timenow.saturating_sub(self.last_heartbeat_sent) > self.heartbeat_cycle {
            self.send_heartbeat();
        }

        core::hint::spin_loop();
    }

    /// render TUI if enabled - call this in cold path, separate from run_once
    fn maybe_render_tui(&mut self, tui: &mut Option<TUI>) {
        self.run_iter = self.run_iter.wrapping_add(1);
        if self.run_iter % 1000 == 0 {
            if let Some(tui) = tui {
                self.sync_tui_state();
                let _ = tui.draw_once();
                tui.check_quit();
            }
        }
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
        self.start_recon();

        // create TUI if state provided
        let mut tui = self.create_tui();

        // main event loop
        while self.running && !should_shutdown() {
            self.run_once(); // hot path - pure business logic
            self.maybe_render_tui(&mut tui); // cold path - optional TUI
        }

        info!("MarketDataEngine stopped");
    }

    /// create TUI from state if available
    fn create_tui(&self) -> Option<TUI> {
        self.tui_state
            .as_ref()
            .and_then(|state| TUI::with_title(state.clone(), "Engine".to_string()).ok())
    }

    /// This method runs the engine for the specified duration, then stops automatically.
    pub fn start_for_duration(&mut self, duration: Duration) {
        info!("Starting MarketDataEngine for {:?}", duration);
        self.running = true;
        self.on_start_protocol();
        self.start_recon();

        // create TUI if state provided
        let mut tui = self.create_tui();

        let start_time = std::time::Instant::now();

        // main event loop with timeout
        while self.running && !should_shutdown() && start_time.elapsed() < duration {
            self.run_once(); // hot path - pure business logic
            self.maybe_render_tui(&mut tui); // cold path - optional TUI
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

            MDMessage::AssetCtx(_asset) => {
                // not implemented yet
            }
            MDMessage::FundingInfo(_funding) => {
                // not implemented yet
            }
        }
    }

    // =========================================================================
    // callback-based methods for TradingRuntime (no ring buffers)
    // =========================================================================

    /// handle market data directly (callback-based, no ring buffer)
    ///
    /// called by TradingRuntime when any md feed has data.
    /// only dispatches to strategy if recon is complete.
    pub fn on_md(&mut self, _venue: Venue, msg: MDMessage) {
        // skip md processing during recon
        if self.recon_state != ReconState::Success {
            return;
        }

        match msg {
            MDMessage::Orderbook(orderbook) => {
                self.on_orderbook(&orderbook);
            }
            MDMessage::Kline(kline) => {
                self.on_kline(&kline);
            }
            MDMessage::AssetCtx(_) => {}
            MDMessage::FundingInfo(_) => {}
        }
    }

    /// poll trade server for messages (non-blocking)
    ///
    /// called by TradingRuntime between md updates.
    pub fn poll_ts(&mut self) {
        while let Some(ts_message) = self.ts_receiver.recv() {
            self.handle_ts_message(ts_message);
        }

        // check recon timeout if in progress
        if self.recon_state == ReconState::InProgress {
            let elapsed = timestamp_micros().saturating_sub(self.recon_started_at);
            if elapsed > RECON_TIMEOUT_US {
                warn!("recon timeout after {}ms", elapsed / 1000);
                self.recon_retries += 1;

                if self.recon_retries >= MAX_RECON_RETRIES {
                    warn!("max recon retries ({}) exceeded", MAX_RECON_RETRIES);
                    self.recon_failure();
                } else {
                    warn!(
                        "retrying recon ({}/{})",
                        self.recon_retries + 1,
                        MAX_RECON_RETRIES
                    );
                    self.start_recon();
                }
            }
        }
    }

    /// poll heartbeat logic (non-blocking)
    ///
    /// checks ts liveness and sends periodic heartbeats.
    pub fn poll_heartbeat(&mut self) {
        // skip if recon not done
        if self.recon_state != ReconState::Success {
            return;
        }

        let timenow = timestamp_micros();
        let timeout = self.heartbeat_cycle * 2;

        // check ts liveness
        if timenow.saturating_sub(self.last_heartbeat_recv) > timeout {
            self.heartbeat_failures += 1;
            warn!(
                "no response from TS in {} us (failure #{})",
                timeout, self.heartbeat_failures
            );

            if self.heartbeat_failures >= 3 {
                warn!("too many heartbeat failures, triggering recon");
                self.strategy.on_disconnect(&self.context);
                self.start_recon();
            }
            return;
        }

        // reset failure counter
        self.heartbeat_failures = 0;

        // send heartbeat periodically
        if timenow.saturating_sub(self.last_heartbeat_sent) > self.heartbeat_cycle {
            self.send_heartbeat();
        }
    }

    /// start recon (public for TradingRuntime)
    pub fn start_recon(&mut self) {
        info!(
            "starting reconciliation (attempt {}/{})",
            self.recon_retries + 1,
            MAX_RECON_RETRIES
        );
        self.recon_state = ReconState::InProgress;
        self.recon_tracker.reset();
        self.recon_started_at = timestamp_micros();

        // clear existing state before recon
        self.state.borrow_mut().clear();

        // notify strategy
        self.strategy.on_recon(&self.context);

        // send all queries
        self.send_qry_orders();
        self.send_qry_positions();
        self.send_qry_balances();
    }

    fn handle_ts_message(&mut self, ts_message: TSEngineMessage) {
        // any message from TS means it's alive - update heartbeat tracker
        self.last_heartbeat_recv = timestamp_micros();

        match ts_message.message {
            TSEngineMessageType::OrderUpdate(order) => {
                self.on_order_update(order);
            }
            TSEngineMessageType::BalanceUpdate(balance) => {
                self.on_balance_update(balance);
            }
            TSEngineMessageType::PositionUpdate(position) => {
                self.on_position_update(position);
            }
            TSEngineMessageType::HeartbeatResponse => {
                debug!("engine rx: HeartbeatResponse");
            }
            TSEngineMessageType::QryOrdersResp(resp) => {
                self.on_qry_orders_resp(resp);
            }
            TSEngineMessageType::QryPositionsResp(resp) => {
                self.on_qry_positions_resp(resp);
            }
            TSEngineMessageType::QryBalancesResp(resp) => {
                self.on_qry_balances_resp(resp);
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

    /// Mark reconciliation as done
    fn recon_done(&mut self) {
        info!("Reconciliation done");
        self.strategy.on_recon_done(&self.context);
    }

    /// Handle reconciliation success
    fn recon_success(&mut self) {
        info!("reconciliation succeeded");
        self.recon_state = ReconState::Success;
        self.recon_retries = 0; // reset for next time
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

    /// handle order update from trade server
    /// updates internal state BEFORE calling strategy handler
    pub fn on_order_update(&mut self, order: Order) {
        debug!(
            "order update: {} -> {:?}",
            client_order_id_to_str(&order.client_order_id),
            order.state
        );

        // update state first via StateManager
        {
            let mut state = self.state.borrow_mut();
            state.apply(StateUpdate::OrderUpdate(order.clone()));
        }

        // check if this is a fill
        if order.filled_qty > 0.0 {
            self.strategy.on_fill(&order, &self.context);
        }

        // notify strategy
        self.strategy.on_order_update(&order, &self.context);
    }

    /// handle position update from trade server
    pub fn on_position_update(&mut self, position: Position) {
        debug!(
            "position update: {} qty={}",
            symbol_to_str(&position.symbol),
            position.qty
        );

        // update state via StateManager
        {
            let mut state = self.state.borrow_mut();
            state.apply(StateUpdate::PositionUpdate(position));
        }

        // notify strategy
        self.strategy.on_position_update(&self.context);
    }

    /// handle balance update from trade server
    pub fn on_balance_update(&mut self, balance: crate::types::common::Balance) {
        debug!(
            "balance update: {} qty={}",
            symbol_to_str(&balance.coin),
            balance.qty
        );

        // update state via StateManager
        {
            let mut state = self.state.borrow_mut();
            state.apply(StateUpdate::BalanceUpdate(balance));
        }
    }

    // =========================================================================
    // recon query handlers
    // =========================================================================

    /// handle orders query response - streams one order at a time
    fn on_qry_orders_resp(&mut self, resp: QryOrderResp) {
        if !resp.success {
            warn!("orders query failed");
            self.recon_tracker.complete_orders(false);
            self.check_recon_complete();
            return;
        }

        // if this is a real order (not empty), add to state
        // skip empty/default orders (state == UNKNOWN means default)
        if resp.order.state != OrderState::UNKNOWN {
            debug!(
                "recon order: {}",
                client_order_id_to_str(&resp.order.client_order_id)
            );
            let mut state = self.state.borrow_mut();
            state.apply(StateUpdate::OrderUpdate(resp.order));
        }

        // check if this was the last response
        if resp.is_last {
            info!("orders query complete");
            self.recon_tracker.complete_orders(true);
            self.check_recon_complete();
        }
    }

    /// handle positions query response - streams one position at a time
    fn on_qry_positions_resp(&mut self, resp: QryPositionResp) {
        if !resp.success {
            warn!("positions query failed");
            self.recon_tracker.complete_positions(false);
            self.check_recon_complete();
            return;
        }

        // if this is a real position (non-zero qty), add to state
        if resp.position.qty != 0.0 {
            debug!(
                "recon position: {} qty={}",
                symbol_to_str(&resp.position.symbol),
                resp.position.qty
            );
            let mut state = self.state.borrow_mut();
            state.apply(StateUpdate::PositionUpdate(resp.position));
        }

        if resp.is_last {
            info!("positions query complete");
            self.recon_tracker.complete_positions(true);
            self.check_recon_complete();
        }
    }

    /// handle balances query response - streams one balance at a time
    fn on_qry_balances_resp(&mut self, resp: QryBalanceResp) {
        if !resp.success {
            warn!("balances query failed");
            self.recon_tracker.complete_balances(false);
            self.check_recon_complete();
            return;
        }

        // if this is a real balance (non-zero qty), add to state
        if resp.balance.qty != 0.0 {
            debug!(
                "recon balance: {} qty={}",
                symbol_to_str(&resp.balance.coin),
                resp.balance.qty
            );
            let mut state = self.state.borrow_mut();
            state.apply(StateUpdate::BalanceUpdate(resp.balance));
        }

        if resp.is_last {
            info!("balances query complete");
            self.recon_tracker.complete_balances(true);
            self.check_recon_complete();
        }
    }

    /// check if all recon queries are complete and handle result
    fn check_recon_complete(&mut self) {
        if !self.recon_tracker.all_done() {
            return;
        }

        if self.recon_tracker.all_success() {
            self.recon_success();
        } else {
            self.recon_failure();
        }
    }

    // =========================================================================
    // recon query senders
    // =========================================================================

    /// send query for all open orders
    fn send_qry_orders(&mut self) {
        let message = EngineTSMessage {
            timestamp: timestamp_nanos(),
            message: EngineTSMessageType::QryOpenOrders(QryOpenOrders {
                venue: self.venue,
                passport_id: self.passport_id,
            }),
        };
        if let Err(e) = self.context.send_raw(message) {
            warn!("failed to send orders query: {}", e);
            self.recon_tracker.complete_orders(false);
        }
    }

    /// send query for all positions
    fn send_qry_positions(&mut self) {
        let message = EngineTSMessage {
            timestamp: timestamp_nanos(),
            message: EngineTSMessageType::QryPositions(QryPositions {
                venue: self.venue,
                passport_id: self.passport_id,
            }),
        };
        if let Err(e) = self.context.send_raw(message) {
            warn!("failed to send positions query: {}", e);
            self.recon_tracker.complete_positions(false);
        }
    }

    /// send query for all balances
    fn send_qry_balances(&mut self) {
        let message = EngineTSMessage {
            timestamp: timestamp_nanos(),
            message: EngineTSMessageType::QryBalance(QryBalance {
                venue: self.venue,
                passport_id: self.passport_id,
            }),
        };
        if let Err(e) = self.context.send_raw(message) {
            warn!("failed to send balances query: {}", e);
            self.recon_tracker.complete_balances(false);
        }
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

    // Note: full engine test requires iceoryx2 runtime
    // this test is commented out as it needs the trade server running
    // #[test]
    // fn test_engine_creation() {
    //     let strategy = Box::new(DummyStrategy);
    //     let engine = MarketDataEngine::new(
    //         strategy,
    //         123,
    //         Venue::Hyperliquid,
    //         "channel1".into()
    //     );
    //     let state = engine.get_state();
    //     assert_eq!(state.total_open_orders(), 0);
    //     assert_eq!(state.get_all_positions().len(), 0);
    // }

    #[test]
    fn test_recon_tracker() {
        let mut tracker = ReconTracker::new();

        assert!(!tracker.all_done());
        assert!(!tracker.all_success());

        tracker.complete_orders(true);
        assert!(!tracker.all_done());

        tracker.complete_positions(true);
        assert!(!tracker.all_done());

        tracker.complete_balances(true);
        assert!(tracker.all_done());
        assert!(tracker.all_success());

        // test failure case
        tracker.reset();
        tracker.complete_orders(true);
        tracker.complete_positions(false);
        tracker.complete_balances(true);
        assert!(tracker.all_done());
        assert!(!tracker.all_success());
    }
}
