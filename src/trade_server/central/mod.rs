use std::collections::HashMap;

use crate::trade_server::state::StateManager;
use crate::tui::{SharedTUIState, should_shutdown};
use crate::types::trade_server::EngineTSMessageType::*;
use crate::types::trade_server::TSEngineMessageType::*;
use crate::{
    ts_protocol::iceoryx2_wrapper::TSIceoryx2Wrapper,
    types::{
        clock::timestamp_nanos,
        common::Venue,
        packet::{Packet, TSInternalMessage},
        trade_server::TSEngineMessage,
    },
};
use ringbuf::{
    HeapCons, HeapProd,
    traits::{Consumer, Producer},
};
use tracing::{debug, error, info, warn};

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
            Ok(_) => debug!("heartbeat response sent"),
            Err(e) => error!("failed to send heartbeat response: {:?}", e),
        }
    }

    fn poll_outer_protocol(&mut self) {
        match self.outer_protocol.recv() {
            Some(value) => {
                debug!("received from engine: {:?}", value);
                match value.message {
                    PlaceOrder(place_order) => {
                        info!(venue = ?place_order.venue, symbol = %place_order.symbol, "place order received");
                        self.state.orders.add_pending_new(place_order);
                        self.sync_tui_state(); // show pending order immediately
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
                    Heartbeat(_) => self.send_heartbeat(),
                    others => {
                        debug!("received unhandled message: {:?}", others);
                    }
                }
            }
            None => {
                // no-op
            }
        }
    }

    pub fn poll_state_receivers(&mut self) {
        let mut updated = false;
        for (_, v) in self.state_receivers.iter_mut() {
            while let Some(packet) = v.try_pop() {
                debug!("state update received: {:?}", packet);
                match packet.body {
                    TSInternalMessage::StateUpdate(update) => {
                        self.state.apply(update);
                        updated = true;
                    }
                    _ => {
                        warn!("unexpected message type in state receiver");
                    }
                }
            }
        }

        // sync to tui state if updated
        if updated {
            self.sync_tui_state();
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
        let mut needs_tui_sync = false;

        for (_, v) in self.execution_receivers.iter_mut() {
            while let Some(packet) = v.try_pop() {
                match packet.body {
                    TSInternalMessage::PlaceOrderResp(resp) => {
                        if resp.is_success() {
                            // order confirmed, will be tracked via state updates
                        } else {
                            // order rejected, remove from pending
                            debug!(
                                cloid = %resp.request.client_order_id,
                                "removing rejected order from pending"
                            );
                            let removed = self
                                .state
                                .orders
                                .remove_pending_new(&resp.request.client_order_id);
                            if removed.is_none() {
                                warn!(
                                    cloid = %resp.request.client_order_id,
                                    "rejected order not found in pending"
                                );
                            }
                            needs_tui_sync = true;
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

        if needs_tui_sync {
            self.sync_tui_state();
        }
    }

    pub fn poll_ingress_queues_once(&mut self) {
        self.poll_outer_protocol();
        self.poll_execution_receivers();
        self.poll_state_receivers();
    }

    pub fn run(&mut self) {
        loop {
            if should_shutdown() {
                info!("central shutting down");
                break;
            }
            self.poll_ingress_queues_once();
        }
    }
}
