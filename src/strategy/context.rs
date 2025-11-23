use crate::state_management::order_manager::{Order, PlaceOrder, CancelOrder, ReplaceOrder};
use crate::types::common::Venue;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Result type for order operations
pub type OrderResult<T> = Result<T, OrderGatewayError>;

#[derive(Debug)]
pub enum OrderGatewayError {
    AeronPublishError(String),
    InvalidOrder(String),
    Timeout,
}

/// Order gateway trait - abstracts order placement to trade server
#[async_trait]
pub trait OrderGateway: Send + Sync {
    async fn place_order(&mut self, order: PlaceOrder) -> OrderResult<String>;
    async fn cancel_order(&mut self, cancel: CancelOrder) -> OrderResult<()>;
    async fn replace_order(&mut self, replace: ReplaceOrder) -> OrderResult<()>;
}

/// Position information
#[derive(Debug, Clone)]
pub struct Position {
    pub symbol: String,
    pub venue: Venue,
    pub quantity: f64,        // Positive = long, negative = short
    pub avg_entry_price: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
}

/// Engine state - single source of truth
#[derive(Debug)]
pub struct EngineState {
    pub open_orders: HashMap<String, Order>,           // client_order_id -> Order
    pub positions: HashMap<(Venue, String), Position>, // (venue, symbol) -> Position
    pub balances: HashMap<(Venue, String), f64>,       // (venue, asset) -> balance
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
    pub fn get_position(&self, venue: &Venue, symbol: &str) -> Option<&Position> {
        self.positions.get(&(venue.clone(), symbol.to_string()))
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
        *self.balances
            .get(&(venue.clone(), asset.to_string()))
            .unwrap_or(&0.0)
    }
}

/// Context passed to strategy handlers - provides read access to state and order placement
pub struct StrategyContext {
    /// Order gateway for placing/canceling/replacing orders
    pub order_gateway: Arc<tokio::sync::Mutex<dyn OrderGateway>>,

    /// Read-only access to engine state (RwLock allows many readers, one writer)
    pub state: Arc<RwLock<EngineState>>,
}

impl StrategyContext {
    pub fn new(
        order_gateway: Arc<tokio::sync::Mutex<dyn OrderGateway>>,
        state: Arc<RwLock<EngineState>>,
    ) -> Self {
        Self {
            order_gateway,
            state,
        }
    }

    /// Helper: Get current position for a symbol
    pub async fn get_position(&self, venue: &Venue, symbol: &str) -> Option<Position> {
        let state = self.state.read().await;
        state.get_position(venue, symbol).cloned()
    }

    /// Helper: Get all open orders
    pub async fn get_open_orders(&self) -> Vec<Order> {
        let state = self.state.read().await;
        state.open_orders.values().cloned().collect()
    }

    /// Helper: Get balance
    pub async fn get_balance(&self, venue: &Venue, asset: &str) -> f64 {
        let state = self.state.read().await;
        state.get_balance(venue, asset)
    }
}
