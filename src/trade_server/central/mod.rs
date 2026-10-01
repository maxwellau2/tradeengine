use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::trade_server::state::StateManager;
use crate::tui::{SharedTUIState, TUI, should_shutdown};
use crate::types::common::ClientOrderId;
use crate::types::trade_server::EngineTSMessageType::*;
use crate::types::trade_server::TSEngineMessageType::*;
use crate::types::trade_server::{QryBalanceResp, QryOrderResp, QryPositionResp, QueryStaleOrder};
use crate::{
    ts_protocol::iceoryx2_wrapper::TSIceoryx2Wrapper,
    types::{
        clock::timestamp_nanos,
        common::{Order, OrderState, Venue},
        packet::{Packet, TSInternalMessage},
        trade_server::TSEngineMessage,
    },
};
use ringbuf::{
    HeapCons, HeapProd,
    traits::{Consumer, Producer},
};
use tracing::{debug, error, info, warn};

/// timeout for pending_new orders before querying exchange
const PENDING_ORDER_TIMEOUT: Duration = Duration::from_secs(10);
/// minimum interval between timeout checks
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(5);

const RB_SIZE: usize = 1 << 10;

pub struct Central {
    execution_senders: HashMap<Venue, HeapProd<Packet<TSInternalMessage>>>,
    execution_receivers: HashMap<Venue, HeapCons<Packet<TSInternalMessage>>>,
    state_senders: HashMap<Venue, HeapProd<Packet<TSInternalMessage>>>,
    state_receivers: HashMap<Venue, HeapCons<Packet<TSInternalMessage>>>,
    outer_protocol: TSIceoryx2Wrapper,
    seq_num: u64,
    state: StateManager,
    tui_state: Option<SharedTUIState>,
    // stale order detection
    last_timeout_check: Instant,
    pending_stale_queries: HashSet<ClientOrderId>,
}

impl Central {
    pub fn new(outer_protocol_name: String) -> Result<Self, String> {
        let protocol = TSIceoryx2Wrapper::new(outer_protocol_name)?;
        Ok(Self {
            execution_senders: HashMap::new(),
            execution_receivers: HashMap::new(),
            state_senders: HashMap::new(),
            state_receivers: HashMap::new(),
            outer_protocol: protocol,
            seq_num: 0,
            state: StateManager::new(),
            tui_state: None,
            last_timeout_check: Instant::now(),
            pending_stale_queries: HashSet::new(),
        })
    }

    pub fn set_tui_state(&mut self, tui_state: SharedTUIState) {
        self.tui_state = Some(tui_state);
    }

    pub fn add_execution_channel(
        &mut self,
        venue: Venue,
        sender: HeapProd<Packet<TSInternalMessage>>,
        receiver: HeapCons<Packet<TSInternalMessage>>,
    ) {
        self.execution_senders.insert(venue, sender);
        self.execution_receivers.insert(venue, receiver);
    }
    pub fn add_state_channel(
        &mut self,
        venue: Venue,
        sender: HeapProd<Packet<TSInternalMessage>>,
        receiver: HeapCons<Packet<TSInternalMessage>>,
    ) {
        self.state_senders.insert(venue, sender);
        self.state_receivers.insert(venue, receiver);
    }

    /// check for pending_new and pending_cancel orders that have timed out and query their status
    fn check_pending_timeouts(&mut self) {
        // throttle checks to avoid spamming
        if self.last_timeout_check.elapsed() < TIMEOUT_CHECK_INTERVAL {
            return;
        }
        self.last_timeout_check = Instant::now();

        // collect queries to send (avoid borrow conflicts)
        let mut queries_to_send: Vec<(ClientOrderId, Venue)> = Vec::new();

        // check timed out pending_new orders
        let timed_out_new = self
            .state
            .orders
            .get_timed_out_pending(PENDING_ORDER_TIMEOUT);

        for cloid in timed_out_new {
            if self.pending_stale_queries.contains(&cloid) {
                continue;
            }

            let venue = if let Some(order) = self.state.orders.get_pending_new(&cloid) {
                order.venue
            } else if let Some(order) = self.state.orders.get_executor_acked(&cloid) {
                order.venue
            } else {
                continue;
            };

            info!(
                cloid = %cloid,
                venue = ?venue,
                "pending order timed out, querying exchange"
            );

            self.pending_stale_queries.insert(cloid);
            queries_to_send.push((cloid, venue));
        }

        // check timed out pending_cancel orders
        let timed_out_cancel = self
            .state
            .orders
            .get_timed_out_pending_cancel(PENDING_ORDER_TIMEOUT);

        for cloid in timed_out_cancel {
            if self.pending_stale_queries.contains(&cloid) {
                continue;
            }

            // get venue from confirmed order (pending_cancel orders are in confirmed)
            let venue = if let Some(order) = self.state.orders.get_confirmed(&cloid) {
                order.venue
            } else {
                // order no longer in confirmed, remove from pending_cancel
                self.state.orders.remove_pending_cancel(&cloid);
                continue;
            };

            info!(
                cloid = %cloid,
                venue = ?venue,
                "pending cancel timed out, querying exchange"
            );

            self.pending_stale_queries.insert(cloid);
            queries_to_send.push((cloid, venue));
        }

        // send all queries
        for (cloid, venue) in queries_to_send {
            let query = QueryStaleOrder {
                venue,
                client_order_id: cloid,
            };
            self.forward_to_execution(TSInternalMessage::QueryStaleOrder(query), venue);
        }
    }

    fn forward_to_execution(&mut self, msg: TSInternalMessage, venue: Venue) {
        let sender = self.execution_senders.get_mut(&venue);
        match sender {
            Some(rb) => {
                self.seq_num = self.seq_num.wrapping_add(1);
                if let Err(e) = rb.try_push(Packet::new(msg, self.seq_num)) {
                    error!(venue = ?venue, "failed to push to execution channel: {:?}", e);
                }
            }
            None => {
                warn!(venue = ?venue, "execution channel not configured");
            }
        }
    }

    fn send_heartbeat(&mut self) {
        if let Err(e) = self.outer_protocol.send(TSEngineMessage {
            timestamp: timestamp_nanos(),
            message: HeartbeatResponse {},
        }) {
            error!("failed to send heartbeat response: {:?}", e);
        }
    }

    fn send_qry_orders_resp(&mut self) {
        let orders = self.state.orders.get_all_confirmed();
        if orders.is_empty() {
            let resp = QryOrderResp::empty();
            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                timestamp: timestamp_nanos(),
                message: QryOrdersResp(resp),
            }) {
                error!("failed to send qry orders resp: {:?}", e);
            }
            return;
        }

        for (i, order) in orders.iter().enumerate() {
            let is_last = i == orders.len() - 1;
            let resp = QryOrderResp::item(*order, is_last);
            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                timestamp: timestamp_nanos(),
                message: QryOrdersResp(resp),
            }) {
                error!("failed to send qry orders resp: {:?}", e);
                break;
            }
        }
        debug!("sent {} orders in qry response", orders.len());
    }

    fn send_qry_positions_resp(&mut self) {
        let positions = self.state.positions.get_all();
        if positions.is_empty() {
            let resp = QryPositionResp::empty();
            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                timestamp: timestamp_nanos(),
                message: QryPositionsResp(resp),
            }) {
                error!("failed to send qry positions resp: {:?}", e);
            }
            return;
        }

        for (i, position) in positions.iter().enumerate() {
            let is_last = i == positions.len() - 1;
            let resp = QryPositionResp::item(*position, is_last);
            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                timestamp: timestamp_nanos(),
                message: QryPositionsResp(resp),
            }) {
                error!("failed to send qry positions resp: {:?}", e);
                break;
            }
        }
        debug!("sent {} positions in qry response", positions.len());
    }

    fn send_qry_balances_resp(&mut self) {
        let balances = self.state.balances.get_all();
        if balances.is_empty() {
            let resp = QryBalanceResp::empty();
            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                timestamp: timestamp_nanos(),
                message: QryBalancesResp(resp),
            }) {
                error!("failed to send qry balances resp: {:?}", e);
            }
            return;
        }

        for (i, balance) in balances.iter().enumerate() {
            let is_last = i == balances.len() - 1;
            let resp = QryBalanceResp::item(*balance, is_last);
            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                timestamp: timestamp_nanos(),
                message: QryBalancesResp(resp),
            }) {
                error!("failed to send qry balances resp: {:?}", e);
                break;
            }
        }
        debug!("sent {} balances in qry response", balances.len());
    }

    fn poll_outer_protocol(&mut self) {
        match self.outer_protocol.recv() {
            Some(value) => {
                debug!("received from engine: {:?}", value);
                match value.message {
                    PlaceOrder(place_order) => {
                        info!(venue = ?place_order.venue, symbol = %place_order.symbol, "place order received");
                        self.state.orders.add_pending_new(place_order);
                        self.forward_to_execution(
                            TSInternalMessage::PlaceOrder(place_order),
                            place_order.venue,
                        );
                    }
                    CancelOrder(cancel_order) => {
                        info!(venue = ?cancel_order.venue, cloid = %cancel_order.client_order_id, "cancel order received");
                        self.state
                            .orders
                            .add_pending_cancel(cancel_order.client_order_id);
                        self.forward_to_execution(
                            TSInternalMessage::CancelOrder(cancel_order),
                            cancel_order.venue,
                        );
                    }
                    ReplaceOrder(replace_order) => {
                        info!(venue = ?replace_order.venue, cloid = %replace_order.client_order_id, "replace order received");
                        self.forward_to_execution(
                            TSInternalMessage::ReplaceOrder(replace_order),
                            replace_order.venue,
                        );
                    }
                    Heartbeat(_) => {
                        debug!("central rx: Heartbeat from engine");
                        self.send_heartbeat();
                    }
                    QryOpenOrders(_) => {
                        info!("qry open orders received");
                        self.send_qry_orders_resp();
                    }
                    QryPositions(_) => {
                        info!("qry positions received");
                        self.send_qry_positions_resp();
                    }
                    QryBalance(_) => {
                        info!("qry balances received");
                        self.send_qry_balances_resp();
                    }
                }
            }
            None => {
                // no-op
            }
        }
    }

    pub fn poll_state_receivers(&mut self) {
        // collect updates first to avoid borrow conflict
        let mut updates = Vec::new();
        for (_, v) in self.state_receivers.iter_mut() {
            while let Some(packet) = v.try_pop() {
                debug!("state update received: {:?}", packet);
                match packet.body {
                    TSInternalMessage::StateUpdate(update) => {
                        updates.push(update);
                    }
                    _ => {
                        warn!("unexpected message type in state receiver");
                    }
                }
            }
        }

        // now process collected updates
        for update in updates {
            // forward to engine first
            self.send_state_update_to_engine(&update);
            // then apply locally
            self.state.apply(update);
        }
    }

    /// forward state update to engine via iceoryx2
    fn send_state_update_to_engine(&mut self, update: &crate::types::trade_server::StateUpdate) {
        use crate::types::trade_server::StateUpdate;

        let message = match update {
            StateUpdate::OrderUpdate(order) => {
                info!(
                    "central tx: OrderUpdate to engine, cloid={}",
                    order.client_order_id
                );
                TSEngineMessage {
                    timestamp: timestamp_nanos(),
                    message: OrderUpdate(*order),
                }
            }
            StateUpdate::PositionUpdate(position) => {
                debug!("central tx: PositionUpdate to engine");
                TSEngineMessage {
                    timestamp: timestamp_nanos(),
                    message: PositionUpdate(*position),
                }
            }
            StateUpdate::BalanceUpdate(balance) => {
                debug!("central tx: BalanceUpdate to engine");
                TSEngineMessage {
                    timestamp: timestamp_nanos(),
                    message: BalanceUpdate(*balance),
                }
            }
        };

        if let Err(e) = self.outer_protocol.send(message) {
            error!("failed to send state update to engine: {:?}", e);
        }
    }

    fn sync_tui_state(&self) {
        if let Some(tui_state) = &self.tui_state {
            // load current state, clone it, update, and store back
            // this is lock-free - TUI can read without blocking
            let current = tui_state.load();
            let new_state = crate::tui::TUIState {
                orders: self.state.orders.get_all_with_pending(),
                positions: self.state.positions.get_all(),
                balances: self.state.balances.get_all(),
                logs: current.logs.clone(), // preserve logs
            };
            tui_state.store(std::sync::Arc::new(new_state));
        }
    }

    pub fn poll_execution_receivers(&mut self) {
        // collect stale order queries to send after processing
        // (avoids borrow conflicts with forward_to_execution)
        let mut stale_queries: Vec<(ClientOrderId, Venue)> = Vec::new();

        for (_, v) in self.execution_receivers.iter_mut() {
            while let Some(packet) = v.try_pop() {
                match packet.body {
                    TSInternalMessage::PlaceOrderResp(resp) => {
                        if resp.is_success() {
                            // executor confirmed - mark as executor_acked if not already confirmed by state subscriber
                            let marked = self
                                .state
                                .orders
                                .mark_executor_acked(&resp.request.client_order_id);

                            if marked {
                                info!(
                                    cloid = %resp.request.client_order_id,
                                    "executor ack received, awaiting state subscriber confirmation"
                                );
                            } else {
                                // already confirmed by state subscriber, nothing to do
                                debug!(
                                    cloid = %resp.request.client_order_id,
                                    "executor ack received but order already confirmed by state subscriber"
                                );
                            }
                        } else {
                            // order rejected - create rejected order and send to engine
                            let rejected_order = Order {
                                client_order_id: resp.request.client_order_id,
                                symbol: resp.request.symbol,
                                venue: resp.request.venue,
                                side: resp.request.side,
                                price: resp.request.price,
                                qty: resp.request.qty,
                                filled_qty: 0.0,
                                order_type: resp.request.order_type,
                                time_in_force: resp.request.time_in_force,
                                state: OrderState::REJECTED,
                            };

                            // send rejection to engine so it removes from pending
                            debug!(
                                cloid = %resp.request.client_order_id,
                                "sending rejected order to engine"
                            );
                            if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                                timestamp: timestamp_nanos(),
                                message: OrderUpdate(rejected_order),
                            }) {
                                error!("failed to send rejection to engine: {:?}", e);
                            }

                            // remove from local pending
                            self.state
                                .orders
                                .remove_pending_new(&resp.request.client_order_id);
                        }
                    }
                    TSInternalMessage::CancelOrderResp(resp) => {
                        let cloid = resp.request.client_order_id;
                        let venue = resp.request.venue;
                        let is_success = resp.is_success();
                        let error = resp.error().map(|e| e.to_string());

                        if is_success {
                            // cancel confirmed, order removed via state updates
                            self.state.orders.remove_pending_cancel(&cloid);
                        } else if let Some(err) = error {
                            // order not found - was likely filled before cancel arrived
                            // query to get actual state
                            if err.contains("CLIENT_ORDER_ID_NOT_FOUND") {
                                info!(
                                    cloid = %cloid,
                                    "cancel failed: order not found, querying actual state"
                                );

                                if !self.pending_stale_queries.contains(&cloid) {
                                    self.pending_stale_queries.insert(cloid);
                                    stale_queries.push((cloid, venue));
                                }
                            } else {
                                warn!(cloid = %cloid, error = %err, "cancel failed");
                            }
                        }
                    }
                    TSInternalMessage::ReplaceOrderResp(resp) => {
                        if !resp.is_success() {
                            warn!("replace order failed: {:?}", resp.error());
                        }
                    }
                    TSInternalMessage::QueryStaleOrderResp(resp) => {
                        // remove from pending queries
                        self.pending_stale_queries.remove(&resp.client_order_id);

                        if !resp.success {
                            warn!(
                                cloid = %resp.client_order_id,
                                error = ?resp.error,
                                "stale order query failed"
                            );
                            continue;
                        }

                        match resp.order {
                            Some(order) => {
                                // order exists on exchange - update state and notify engine
                                info!(
                                    cloid = %resp.client_order_id,
                                    state = ?order.state,
                                    "stale order found on exchange"
                                );

                                // remove from pending states
                                self.state.orders.remove_pending_new(&resp.client_order_id);
                                self.state
                                    .orders
                                    .remove_executor_acked(&resp.client_order_id);
                                // clear pending_cancel - we now know actual state
                                self.state
                                    .orders
                                    .remove_pending_cancel(&resp.client_order_id);

                                // if order is still active, add to confirmed
                                // terminal states (FILLED, CANCELLED, REJECTED) don't need tracking
                                if matches!(
                                    order.state,
                                    OrderState::NEW | OrderState::PARTIALLY_FILLED
                                ) {
                                    self.state.orders.upsert_confirmed(order);
                                }

                                // notify engine of order state - strategy decides what to do
                                if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                                    timestamp: timestamp_nanos(),
                                    message: OrderUpdate(order),
                                }) {
                                    error!("failed to send stale order update to engine: {:?}", e);
                                }
                            }
                            None => {
                                // order not found on exchange - was likely rejected/never received
                                info!(
                                    cloid = %resp.client_order_id,
                                    "stale order not found on exchange, marking as rejected"
                                );

                                // get original order info for the rejection (check both pending states)
                                let pending_order = self
                                    .state
                                    .orders
                                    .get_pending_new(&resp.client_order_id)
                                    .cloned()
                                    .or_else(|| {
                                        self.state
                                            .orders
                                            .get_executor_acked(&resp.client_order_id)
                                            .cloned()
                                    });

                                if let Some(pending) = pending_order {
                                    let rejected = Order {
                                        client_order_id: pending.client_order_id,
                                        symbol: pending.symbol,
                                        venue: pending.venue,
                                        side: pending.side,
                                        price: pending.price,
                                        qty: pending.qty,
                                        filled_qty: 0.0,
                                        order_type: pending.order_type,
                                        time_in_force: pending.time_in_force,
                                        state: OrderState::REJECTED,
                                    };

                                    // notify engine
                                    if let Err(e) = self.outer_protocol.send(TSEngineMessage {
                                        timestamp: timestamp_nanos(),
                                        message: OrderUpdate(rejected),
                                    }) {
                                        error!("failed to send rejection to engine: {:?}", e);
                                    }
                                }

                                // remove from both pending states
                                self.state.orders.remove_pending_new(&resp.client_order_id);
                                self.state
                                    .orders
                                    .remove_executor_acked(&resp.client_order_id);
                            }
                        }
                    }
                    _ => {
                        warn!("unexpected message type in execution receiver");
                    }
                }
            }
        }

        // send deferred stale order queries
        for (cloid, venue) in stale_queries {
            let query = QueryStaleOrder {
                venue,
                client_order_id: cloid,
            };
            self.forward_to_execution(TSInternalMessage::QueryStaleOrder(query), venue);
        }
    }

    /// run one iteration - pure business logic, no TUI
    pub fn run_once(&mut self) {
        self.poll_outer_protocol();
        self.poll_execution_receivers();
        self.poll_state_receivers();
        self.check_pending_timeouts();
    }

    /// render TUI if enabled - call this in cold path, separate from run_once
    fn maybe_render_tui(&mut self, tui: &mut Option<TUI>, iter: &mut u64) {
        *iter = iter.wrapping_add(1);
        if *iter % 1000 == 0 {
            if let Some(tui) = tui {
                self.sync_tui_state();
                let _ = tui.draw_once();
                tui.check_quit();
            }
        }
    }

    /// create TUI from state if available
    fn create_tui(&self) -> Option<TUI> {
        self.tui_state
            .as_ref()
            .and_then(|state| TUI::with_title(state.clone(), "Trade Server".to_string()).ok())
    }

    pub fn run(&mut self) {
        let mut tui = self.create_tui();
        let mut iter = 0u64;

        loop {
            if should_shutdown() {
                info!("central shutting down");
                break;
            }

            self.run_once(); // hot path - pure business logic
            self.maybe_render_tui(&mut tui, &mut iter); // cold path - optional TUI
        }
    }
}
