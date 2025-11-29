use crate::types::common::{ClientOrderId, Order, Symbol, Venue};
use crate::types::trade_server::{
    CancelOrder, EngineTSMessage, Heartbeat, PlaceOrder, ReplaceOrder, TSEngineMessage
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Result type for order operations
pub type TradeServerResult<T> = Result<T, TradeServerError>;

#[derive(Debug)]
pub enum TradeServerError {
    AeronPublishError(String),
    Iceoryx2PublishError(String),
    InvalidOrder(String),
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
}

/// Position information
#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: Symbol,
    pub venue: Venue,
    pub quantity: f64, // Positive = long, negative = short
    pub avg_entry_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

/// Engine state - single source of truth
#[derive(Debug)]
pub struct EngineState {
    pub open_orders: HashMap<ClientOrderId, Order>, // client_order_id -> Order
    pub positions: HashMap<(Venue, Symbol), Position>, // (venue, symbol) -> Position
    pub balances: HashMap<(Venue, String), f64>,    // (venue, asset) -> balance
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            open_orders: HashMap::new(),
            positions: HashMap::new(),
            balances: HashMap::new(),
        }
    }

    /// Get all open orders for a specific venue
    pub fn get_venue_orders(&self, venue: &Venue) -> Vec<&Order> {
        self.open_orders
            .values()
            .filter(|o| &o.venue == venue)
            .collect()
    }

    /// Get position for a specific venue and symbol
    pub fn get_position(&self, venue: &Venue, symbol: &Symbol) -> Option<&Position> {
        self.positions.get(&(venue.clone(), *symbol))
    }

    /// Get all positions for a venue
    pub fn get_venue_positions(&self, venue: &Venue) -> Vec<&Position> {
        self.positions
            .iter()
            .filter(|((v, _), _)| v == venue)
            .map(|(_, pos)| pos)
            .collect()
    }

    /// Get balance for a specific asset on a venue
    pub fn get_balance(&self, venue: &Venue, asset: &str) -> f64 {
        *self
            .balances
            .get(&(venue.clone(), asset.to_string()))
            .unwrap_or(&0.0)
    }
}

// / Context passed to strategy handlers - provides read access to state and order placement
pub struct StrategyContext {
    /// Order gateway for placing/canceling/replacing orders
    pub order_gateway: Rc<RefCell<dyn OrderGateway>>,

    /// Shared access to engine state
    pub state: Rc<RefCell<EngineState>>,
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

    pub fn get_position(&self, venue: &Venue, symbol: &Symbol) -> Option<Position> {
        let state = self.state.borrow();
        state.get_position(venue, symbol).cloned()
    }

    pub fn get_open_orders(&self) -> Vec<Order> {
        let state = self.state.borrow();
        state.open_orders.values().cloned().collect()
    }
    pub fn get_balance(&self, venue: &Venue, asset: &str) -> f64 {
        let state = self.state.borrow();
        state.get_balance(venue, asset)
    }
}
