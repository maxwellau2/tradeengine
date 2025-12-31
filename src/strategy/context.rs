use crate::trade_server::state::StateManager;
use crate::types::common::{Balance, ClientOrderId, Order, Position, Side, Symbol};
use crate::types::trade_server::{
    CancelOrder, EngineTSMessage, Heartbeat, PlaceOrder, ReplaceOrder, StateUpdate, TSEngineMessage,
};
use std::cell::RefCell;
use std::rc::Rc;
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
///
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

    /// get position for symbol
    pub fn get_position(&self, symbol: &Symbol) -> Option<&Position> {
        self.state.positions.get(symbol)
    }

    /// get all positions
    pub fn get_all_positions(&self) -> Vec<Position> {
        self.state.positions.get_all()
    }

    /// get net position size (positive = long, negative = short)
    pub fn net_position(&self, symbol: &Symbol) -> f64 {
        self.state.positions.net_size(symbol)
    }

    /// check if we have a position in symbol
    pub fn has_position(&self, symbol: &Symbol) -> bool {
        self.state.positions.has_position(symbol)
    }

    /// total unrealised pnl
    pub fn total_unrealised_pnl(&self) -> f64 {
        self.state.positions.total_unrealised_pnl()
    }

    // --- balance queries ---

    /// get balance for coin
    pub fn get_balance(&self, coin: &Symbol) -> Option<&Balance> {
        self.state.balances.get(coin)
    }

    /// get balance qty for coin
    pub fn balance_qty(&self, coin: &Symbol) -> f64 {
        self.state.balances.qty(coin)
    }

    /// get all balances
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
/// provides read access to state and order placement
pub struct StrategyContext {
    /// order gateway for placing/canceling/replacing orders
    order_gateway: Rc<RefCell<dyn OrderGateway>>,

    /// shared access to engine state
    state: Rc<RefCell<EngineState>>,
}

impl StrategyContext {
    pub fn new(
        order_gateway: Rc<RefCell<dyn OrderGateway>>,
        state: Rc<RefCell<EngineState>>,
    ) -> Self {
        Self {
            order_gateway,
            state,
        }
    }

    // --- order methods ---
    //
    pub fn send_heartbeat(&mut self, hb: Heartbeat) {
        if let Err(e) = self.order_gateway.borrow_mut().send_heartbeat(hb) {
            tracing::error!("failed to send heartbeat: {:?}", e);
        } else {
            tracing::debug!("engine tx: Heartbeat");
        }
    }

    /// get all open orders (confirmed + pending)
    pub fn get_all_orders(&self) -> Vec<Order> {
        self.state.borrow().get_all_orders()
    }

    /// get confirmed orders only
    pub fn get_confirmed_orders(&self) -> Vec<Order> {
        self.state.borrow().get_confirmed_orders()
    }

    /// get order by cloid
    pub fn get_order(&self, cloid: &ClientOrderId) -> Option<Order> {
        self.state.borrow().get_order(cloid).cloned()
    }

    /// check if order is pending new
    pub fn is_pending_new(&self, cloid: &ClientOrderId) -> bool {
        self.state.borrow().is_pending_new(cloid)
    }

    /// check if order is pending cancel
    pub fn is_pending_cancel(&self, cloid: &ClientOrderId) -> bool {
        self.state.borrow().is_pending_cancel(cloid)
    }

    /// total open orders count
    pub fn total_open_orders(&self) -> usize {
        self.state.borrow().total_open_orders()
    }

    /// check if any order exists for symbol and side (pending or confirmed)
    pub fn has_order_for(&self, symbol: &Symbol, side: &Side) -> bool {
        self.state.borrow().has_order_for(symbol, side)
    }

    /// get all orders for symbol (pending + confirmed)
    pub fn get_orders_for_symbol(&self, symbol: &Symbol) -> Vec<Order> {
        self.state.borrow().get_orders_for_symbol(symbol)
    }

    /// check if duplicate order exists (same symbol, side, price, qty, tif)
    pub fn has_duplicate(&self, order: &PlaceOrder) -> bool {
        self.state.borrow().has_duplicate(order)
    }

    // --- order actions (facade methods that update state + send) ---

    /// place order: adds to pending state, then sends to trade server
    pub fn place_order(&self, order: PlaceOrder) -> TradeServerResult<String> {
        // add to pending state first
        self.state.borrow_mut().add_pending_order(order.clone());
        // then send to trade server
        self.order_gateway.borrow_mut().place_order(order)
    }

    /// cancel order: adds to pending cancel state, then sends to trade server
    pub fn cancel_order(&self, cancel: CancelOrder) -> TradeServerResult<()> {
        // add to pending cancel state
        self.state
            .borrow_mut()
            .add_pending_cancel(cancel.client_order_id);
        // then send to trade server
        self.order_gateway.borrow_mut().cancel_order(cancel)
    }

    /// replace order: sends to trade server (state updated on ack)
    pub fn replace_order(&self, replace: ReplaceOrder) -> TradeServerResult<()> {
        self.order_gateway.borrow_mut().replace_order(replace)
    }

    // --- position methods ---

    /// get position for symbol
    pub fn get_position(&self, symbol: &Symbol) -> Option<Position> {
        self.state.borrow().get_position(symbol).cloned()
    }

    /// get all positions
    pub fn get_all_positions(&self) -> Vec<Position> {
        self.state.borrow().get_all_positions()
    }

    /// get net position size (positive = long, negative = short)
    pub fn net_position(&self, symbol: &Symbol) -> f64 {
        self.state.borrow().net_position(symbol)
    }

    /// check if we have a position in symbol
    pub fn has_position(&self, symbol: &Symbol) -> bool {
        self.state.borrow().has_position(symbol)
    }

    /// total unrealised pnl
    pub fn total_unrealised_pnl(&self) -> f64 {
        self.state.borrow().total_unrealised_pnl()
    }

    // --- balance methods ---

    /// get balance for coin
    pub fn get_balance(&self, coin: &Symbol) -> Option<Balance> {
        self.state.borrow().get_balance(coin).cloned()
    }

    /// get balance qty for coin
    pub fn balance_qty(&self, coin: &Symbol) -> f64 {
        self.state.borrow().balance_qty(coin)
    }

    /// get all balances
    pub fn get_all_balances(&self) -> Vec<Balance> {
        self.state.borrow().get_all_balances()
    }

    /// send raw message to trade server (used for recon queries)
    pub fn send_raw(&self, message: EngineTSMessage) -> TradeServerResult<()> {
        self.order_gateway.borrow_mut().send_raw(message)
    }
}
