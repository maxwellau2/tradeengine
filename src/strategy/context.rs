use crate::trade_server::state::StateManager;
use crate::types::common::{
    Balance, ClientOrderId, Order, PassportId, Position, Side, Symbol, Venue,
};
use crate::types::trade_server::{
    CancelOrder, EngineTSMessage, Heartbeat, PlaceOrder, ReplaceOrder, StateUpdate, TSEngineMessage,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

/// Result type for order operations
pub type TradeServerResult<T> = Result<T, TradeServerError>;

#[derive(Debug, Error)]
pub enum TradeServerError {
    #[error("iceoryx2 publish error: {0}")]
    Iceoryx2PublishError(String),

    #[error("invalid order: {0}")]
    InvalidOrder(String),

    #[error("timeout")]
    Timeout,

    #[error("state not found for venue={0:?} passport={1}")]
    StateNotFound(Venue, PassportId),
}

pub trait OrderGatewaySend {
    fn send(&mut self, message: EngineTSMessage) -> Result<(), String>;
}

pub trait OrderGatewayRecv {
    fn recv(&mut self) -> Option<TSEngineMessage>;
}

pub trait OrderGateway {
    fn place_order(&mut self, order: PlaceOrder) -> TradeServerResult<String>;
    fn cancel_order(&mut self, cancel: CancelOrder) -> TradeServerResult<()>;
    fn replace_order(&mut self, replace: ReplaceOrder) -> TradeServerResult<()>;
    fn send_heartbeat(&mut self, hb: Heartbeat) -> TradeServerResult<()>;
    /// send raw message (used for queries during recon)
    fn send_raw(&mut self, message: EngineTSMessage) -> TradeServerResult<()>;
}

/// engine state - wraps StateManager for unified state tracking
/// uses the same tracking units as trade server for consistency
#[derive(Debug)]
pub struct EngineState {
    pub state: StateManager,
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            state: StateManager::new(),
        }
    }

    /// generate next client order id as u32 string
    /// finds max cloid in current orders and returns max+1
    /// checks for collision before returning
    pub fn next_cloid(&self) -> ClientOrderId {
        // find max cloid from all orders (confirmed + pending_new + executor_acked)
        let mut max_cloid: u32 = 0;
        for order in self.state.orders.get_all_with_pending() {
            if let Ok(n) = order.client_order_id.as_str().parse::<u32>() {
                if n > max_cloid {
                    max_cloid = n;
                }
            }
        }

        // try incrementing from max until we find non-colliding id
        let mut next = max_cloid.wrapping_add(1);
        for _ in 0..1000 {
            let cloid = ClientOrderId::new(&next.to_string());
            // check no collision with any order state
            if self.state.orders.get_confirmed(&cloid).is_none()
                && !self.state.orders.is_pending_new(&cloid)
                && !self.state.orders.is_executor_acked(&cloid)
            {
                return cloid;
            }
            next = next.wrapping_add(1);
        }
        panic!("failed to generate unique cloid after 1000 attempts");
    }

    /// apply state update from trade server
    pub fn apply(&mut self, update: StateUpdate) {
        self.state.apply(update);
    }

    /// add pending order before sending to trade server
    pub fn add_pending_order(&mut self, order: PlaceOrder) {
        self.state.orders.add_pending_new(order);
    }

    /// add pending cancel before sending to trade server
    pub fn add_pending_cancel(&mut self, cloid: ClientOrderId) {
        self.state.orders.add_pending_cancel(cloid);
    }

    // --- order queries ---

    /// get all open orders (confirmed + pending)
    pub fn get_all_orders(&self) -> Vec<Order> {
        self.state.orders.get_all_with_pending()
    }

    /// get confirmed orders only
    pub fn get_confirmed_orders(&self) -> Vec<Order> {
        self.state.orders.get_all_confirmed()
    }

    /// get order by cloid
    pub fn get_order(&self, cloid: &ClientOrderId) -> Option<&Order> {
        self.state.orders.get_confirmed(cloid)
    }

    /// check if order is pending new
    pub fn is_pending_new(&self, cloid: &ClientOrderId) -> bool {
        self.state.orders.is_pending_new(cloid)
    }

    /// check if order is pending cancel
    pub fn is_pending_cancel(&self, cloid: &ClientOrderId) -> bool {
        self.state.orders.is_pending_cancel(cloid)
    }

    /// total open orders count
    pub fn total_open_orders(&self) -> usize {
        self.state.orders.total_open()
    }

    /// check if any order exists for symbol and side
    pub fn has_order_for(&self, symbol: &Symbol, side: &Side) -> bool {
        self.state.orders.has_order_for(symbol, side)
    }

    /// get all orders for symbol
    pub fn get_orders_for_symbol(&self, symbol: &Symbol) -> Vec<Order> {
        self.state.orders.get_orders_for_symbol(symbol)
    }

    /// check if duplicate order exists (same symbol, side, price, qty, tif)
    pub fn has_duplicate(&self, order: &PlaceOrder) -> bool {
        self.state.orders.has_duplicate(order)
    }

    // --- position queries ---

    /// get position for (symbol, venue)
    pub fn get_position(&self, symbol: &Symbol, venue: Venue) -> Option<&Position> {
        self.state.positions.get(symbol, venue)
    }

    /// get all positions across all venues
    pub fn get_all_positions(&self) -> Vec<Position> {
        self.state.positions.get_all()
    }

    /// get net position size (positive = long, negative = short)
    pub fn net_position(&self, symbol: &Symbol, venue: Venue) -> f64 {
        self.state.positions.net_size(symbol, venue)
    }

    /// check if we have a position in (symbol, venue)
    pub fn has_position(&self, symbol: &Symbol, venue: Venue) -> bool {
        self.state.positions.has_position(symbol, venue)
    }

    /// total unrealised pnl across all venues
    pub fn total_unrealised_pnl(&self) -> f64 {
        self.state.positions.total_unrealised_pnl()
    }

    // --- balance queries ---

    /// get balance for (coin, venue)
    pub fn get_balance(&self, coin: &Symbol, venue: Venue) -> Option<&Balance> {
        self.state.balances.get(coin, venue)
    }

    /// get balance qty for (coin, venue)
    pub fn balance_qty(&self, coin: &Symbol, venue: Venue) -> f64 {
        self.state.balances.qty(coin, venue)
    }

    /// get all balances across all venues
    pub fn get_all_balances(&self) -> Vec<Balance> {
        self.state.balances.get_all()
    }

    /// clear all state (on reconnect)
    pub fn clear(&mut self) {
        self.state.clear();
    }
}

impl Default for EngineState {
    fn default() -> Self {
        Self::new()
    }
}

/// context passed to strategy handlers
/// supports multiple exchanges via (venue, passport_id) keyed state
pub struct StrategyContext {
    /// order gateway for placing/canceling/replacing orders
    order_gateway: Rc<RefCell<dyn OrderGateway>>,

    /// state per (venue, passport_id)
    state: Rc<RefCell<HashMap<(Venue, PassportId), EngineState>>>,
}

impl StrategyContext {
    pub fn new(
        order_gateway: Rc<RefCell<dyn OrderGateway>>,
        state: Rc<RefCell<HashMap<(Venue, PassportId), EngineState>>>,
    ) -> Self {
        Self {
            order_gateway,
            state,
        }
    }

    /// register a (venue, passport) pair for state tracking
    pub fn register(&self, venue: Venue, passport_id: PassportId) {
        self.state
            .borrow_mut()
            .entry((venue, passport_id))
            .or_insert_with(EngineState::new);
    }

    /// generate next client order id for (venue, passport)
    /// returns u32-based id, checks for collision with existing orders
    /// auto-registers (venue, passport) if not already registered
    pub fn next_cloid(&self, venue: Venue, passport_id: PassportId) -> ClientOrderId {
        let mut state = self.state.borrow_mut();
        let engine_state = state
            .entry((venue, passport_id))
            .or_insert_with(EngineState::new);
        engine_state.next_cloid()
    }

    // --- heartbeat ---

    pub fn send_heartbeat(&self, hb: Heartbeat) {
        if let Err(e) = self.order_gateway.borrow_mut().send_heartbeat(hb) {
            tracing::error!("failed to send heartbeat: {:?}", e);
        } else {
            tracing::debug!("engine tx: Heartbeat");
        }
    }

    // --- order queries ---

    /// get all open orders for (venue, passport)
    pub fn get_all_orders(&self, venue: Venue, passport_id: PassportId) -> Vec<Order> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.get_all_orders())
            .unwrap_or_default()
    }

    /// get confirmed orders only for (venue, passport)
    pub fn get_confirmed_orders(&self, venue: Venue, passport_id: PassportId) -> Vec<Order> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.get_confirmed_orders())
            .unwrap_or_default()
    }

    /// get order by cloid for (venue, passport)
    pub fn get_order(
        &self,
        venue: Venue,
        passport_id: PassportId,
        cloid: &ClientOrderId,
    ) -> Option<Order> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .and_then(|s| s.get_order(cloid).cloned())
    }

    /// check if order is pending new
    pub fn is_pending_new(
        &self,
        venue: Venue,
        passport_id: PassportId,
        cloid: &ClientOrderId,
    ) -> bool {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.is_pending_new(cloid))
            .unwrap_or(false)
    }

    /// check if order is pending cancel
    pub fn is_pending_cancel(
        &self,
        venue: Venue,
        passport_id: PassportId,
        cloid: &ClientOrderId,
    ) -> bool {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.is_pending_cancel(cloid))
            .unwrap_or(false)
    }

    /// total open orders count for (venue, passport)
    pub fn total_open_orders(&self, venue: Venue, passport_id: PassportId) -> usize {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.total_open_orders())
            .unwrap_or(0)
    }

    /// check if any order exists for symbol and side
    pub fn has_order_for(
        &self,
        venue: Venue,
        passport_id: PassportId,
        symbol: &Symbol,
        side: &Side,
    ) -> bool {
        let state = self.state.borrow();
        let key = (venue, passport_id);
        let found_state = state.get(&key);

        if found_state.is_none() {
            tracing::warn!(
                "has_order_for: state not found for venue={:?} passport={}",
                venue,
                passport_id
            );
            // debug: show what keys exist
            tracing::debug!(
                "available state keys: {:?}",
                state.keys().collect::<Vec<_>>()
            );
            return false;
        }

        found_state.unwrap().has_order_for(symbol, side)
    }

    /// get all orders for symbol
    pub fn get_orders_for_symbol(
        &self,
        venue: Venue,
        passport_id: PassportId,
        symbol: &Symbol,
    ) -> Vec<Order> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.get_orders_for_symbol(symbol))
            .unwrap_or_default()
    }

    /// check if duplicate order exists
    pub fn has_duplicate(&self, venue: Venue, passport_id: PassportId, order: &PlaceOrder) -> bool {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.has_duplicate(order))
            .unwrap_or(false)
    }

    // --- order actions ---

    /// place order: adds to pending state, then sends to trade server
    pub fn place_order(&self, order: PlaceOrder) -> TradeServerResult<String> {
        let key = (order.venue, order.passport_id);

        // add to pending state first
        {
            let mut state = self.state.borrow_mut();
            if let Some(s) = state.get_mut(&key) {
                s.add_pending_order(order.clone());
            } else {
                return Err(TradeServerError::StateNotFound(key.0, key.1));
            }
        }

        // then send to trade server
        self.order_gateway.borrow_mut().place_order(order)
    }

    /// cancel order: tracks pending cancel to avoid spamming, then sends to trade server
    pub fn cancel_order(&self, cancel: CancelOrder) -> TradeServerResult<()> {
        let key = (cancel.venue, cancel.passport_id);

        // check if already pending cancel - skip to save rate limit
        {
            let state = self.state.borrow();
            if let Some(s) = state.get(&key) {
                if s.is_pending_cancel(&cancel.client_order_id) {
                    return Ok(()); // already pending, don't spam
                }
            }
        }

        // mark as pending cancel
        {
            let mut state = self.state.borrow_mut();
            if let Some(s) = state.get_mut(&key) {
                s.add_pending_cancel(cancel.client_order_id);
            }
        }

        self.order_gateway.borrow_mut().cancel_order(cancel)
    }

    /// replace order: sends to trade server (state updated on ack)
    pub fn replace_order(&self, replace: ReplaceOrder) -> TradeServerResult<()> {
        self.order_gateway.borrow_mut().replace_order(replace)
    }

    // --- position queries ---

    /// get position for symbol on (venue, passport)
    pub fn get_position(
        &self,
        venue: Venue,
        passport_id: PassportId,
        symbol: &Symbol,
    ) -> Option<Position> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .and_then(|s| s.get_position(symbol, venue).cloned())
    }

    /// get all positions for (venue, passport)
    pub fn get_all_positions(&self, venue: Venue, passport_id: PassportId) -> Vec<Position> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.get_all_positions())
            .unwrap_or_default()
    }

    /// get net position size
    pub fn net_position(&self, venue: Venue, passport_id: PassportId, symbol: &Symbol) -> f64 {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.net_position(symbol, venue))
            .unwrap_or(0.0)
    }

    /// check if we have a position
    pub fn has_position(&self, venue: Venue, passport_id: PassportId, symbol: &Symbol) -> bool {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.has_position(symbol, venue))
            .unwrap_or(false)
    }

    /// total unrealised pnl for (venue, passport)
    pub fn total_unrealised_pnl(&self, venue: Venue, passport_id: PassportId) -> f64 {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.total_unrealised_pnl())
            .unwrap_or(0.0)
    }

    // --- balance queries ---

    /// get balance for coin on (venue, passport)
    pub fn get_balance(
        &self,
        venue: Venue,
        passport_id: PassportId,
        coin: &Symbol,
    ) -> Option<Balance> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .and_then(|s| s.get_balance(coin, venue).cloned())
    }

    /// get balance qty
    pub fn balance_qty(&self, venue: Venue, passport_id: PassportId, coin: &Symbol) -> f64 {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.balance_qty(coin, venue))
            .unwrap_or(0.0)
    }

    /// get all balances for (venue, passport)
    pub fn get_all_balances(&self, venue: Venue, passport_id: PassportId) -> Vec<Balance> {
        self.state
            .borrow()
            .get(&(venue, passport_id))
            .map(|s| s.get_all_balances())
            .unwrap_or_default()
    }

    /// send raw message to trade server
    pub fn send_raw(&self, message: EngineTSMessage) -> TradeServerResult<()> {
        self.order_gateway.borrow_mut().send_raw(message)
    }

    /// apply state update to (venue, passport)
    pub fn apply_state_update(&self, venue: Venue, passport_id: PassportId, update: StateUpdate) {
        tracing::debug!(
            "apply_state_update: venue={:?} passport={} update={:?}",
            venue,
            passport_id,
            update
        );
        let mut state = self.state.borrow_mut();
        if let Some(s) = state.get_mut(&(venue, passport_id)) {
            s.apply(update);
        } else {
            tracing::warn!(
                "apply_state_update: state not found for venue={:?} passport={}",
                venue,
                passport_id
            );
        }
    }

    /// clear state for (venue, passport)
    pub fn clear_state(&self, venue: Venue, passport_id: PassportId) {
        let mut state = self.state.borrow_mut();
        if let Some(s) = state.get_mut(&(venue, passport_id)) {
            s.clear();
        }
    }

    /// get all registered (venue, passport) keys
    pub fn registered_keys(&self) -> Vec<(Venue, PassportId)> {
        self.state.borrow().keys().copied().collect()
    }
}
