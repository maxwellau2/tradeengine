use std::collections::HashMap;

use crate::trade_server::state::StateManager;
use crate::tui::{SharedTUIState, TUI, should_shutdown};
use crate::types::trade_server::EngineTSMessageType::*;
use crate::types::trade_server::TSEngineMessageType::*;
use crate::types::trade_server::{QryBalanceResp, QryOrderResp, QryPositionResp};
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
use tracing::{debug, error, info, trace, warn};

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

    fn forward_to_execution(&mut self, msg: TSInternalMessage, venue: Venue) {
        let sender = self.execution_senders.get_mut(&venue);
        match sender {
            Some(rb) => {
                self.seq_num = self.seq_num.wrapping_add(1);
                let res = rb.try_push(Packet::new(msg, self.seq_num));
                match res {
                    Ok(()) => {
                        debug!(venue = ?venue, "forwarded to execution channel");
                    }
                    Err(e) => {
                        error!(venue = ?venue, "failed to push to execution channel: {:?}", e);
                    }
                }
            }
            None => {
                warn!(venue = ?venue, "execution channel not configured");
            }
        }
    }

    fn send_heartbeat(&mut self) {
        let res = self.outer_protocol.send(TSEngineMessage {
            timestamp: timestamp_nanos(),
            message: HeartbeatResponse {},
        });
        match res {
            Ok(_) => trace!("central tx: HeartbeatResponse"),
            Err(e) => error!("failed to send heartbeat response: {:?}", e),
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
        for (_, v) in self.execution_receivers.iter_mut() {
            while let Some(packet) = v.try_pop() {
                match packet.body {
                    TSInternalMessage::PlaceOrderResp(resp) => {
                        if resp.is_success() {
                            // order confirmed, will be tracked via state updates
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
                        if resp.is_success() {
                            // cancel confirmed, order removed via state updates
                        }
                        // either way, no longer pending cancel
                    }
                    TSInternalMessage::ReplaceOrderResp(resp) => {
                        if !resp.is_success() {
                            warn!("replace order failed: {:?}", resp.error());
                        }
                    }
                    _ => {
                        warn!("unexpected message type in execution receiver");
                    }
                }
            }
        }
    }

    /// run one iteration - pure business logic, no TUI
    pub fn run_once(&mut self) {
        self.poll_outer_protocol();
        self.poll_execution_receivers();
        self.poll_state_receivers();
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
