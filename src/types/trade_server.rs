use iceoryx2::prelude::*;

use crate::{
    config_parser::passport,
    types::common::{
        Balance, ClientOrderId, Order, OrderState, OrderType, PassportId, Position, Side, Symbol,
        TimeInForce, Venue,
    },
};

// Outgoing messages (Engine -> TS)

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("PlaceOrder")]
#[repr(C)]
pub struct PlaceOrder {
    pub symbol: Symbol,
    pub venue: Venue,
    pub client_order_id: ClientOrderId,
    pub price: f64,
    pub qty: f64,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub order_type: OrderType,
    pub passport_id: PassportId,
}
impl PlaceOrder {
    pub fn new(
        symbol: Symbol,
        venue: Venue,
        client_order_id: ClientOrderId,
        price: f64,
        qty: f64,
        side: Side,
        time_in_force: TimeInForce,
        order_type: OrderType,
        passport_id: PassportId,
    ) -> Self {
        Self {
            symbol,
            venue,
            client_order_id,
            price,
            qty,
            side,
            time_in_force,
            order_type,
            passport_id,
        }
    }
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("CancelOrder")]
#[repr(C)]
pub struct CancelOrder {
    pub symbol: Symbol,
    pub venue: Venue,
    pub client_order_id: ClientOrderId,
    pub passport_id: PassportId,
}

impl CancelOrder {
    pub fn new(
        symbol: Symbol,
        venue: Venue,
        client_order_id: ClientOrderId,
        passport_id: PassportId,
    ) -> Self {
        return Self {
            symbol,
            venue,
            client_order_id,
            passport_id,
        };
    }
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("ReplaceOrder")]
#[repr(C)]
pub struct ReplaceOrder {
    pub symbol: Symbol,
    pub venue: Venue,
    pub client_order_id: ClientOrderId,
    pub new_price: f64,
    pub new_qty: f64,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub order_type: OrderType,
    pub passport_id: PassportId,
}

impl ReplaceOrder {
    pub fn new(
        symbol: Symbol,
        venue: Venue,
        client_order_id: ClientOrderId,
        new_price: f64,
        new_qty: f64,
        side: Side,
        time_in_force: TimeInForce,
        order_type: OrderType,
        passport_id: PassportId,
    ) -> Self {
        return Self {
            symbol,
            venue,
            client_order_id,
            new_price,
            new_qty,
            side,
            time_in_force,
            order_type,
            passport_id,
        };
    }
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryOpenOrders")]
#[repr(C)]
pub struct QryOpenOrders {
    pub venue: Venue,
    pub passport_id: PassportId,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryPositions")]
#[repr(C)]
pub struct QryPositions {
    pub venue: Venue,
    pub passport_id: PassportId,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryBalance")]
#[repr(C)]
pub struct QryBalance {
    pub venue: Venue,
    pub passport_id: PassportId,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("Heartbeat")]
#[repr(C)]
pub struct Heartbeat {
    pub passport_id: PassportId,
}

// Engine to TradeServer message - the enum IS the message type and contains the body
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub enum EngineTSMessageType {
    PlaceOrder(PlaceOrder),
    CancelOrder(CancelOrder),
    ReplaceOrder(ReplaceOrder),
    QryOpenOrders(QryOpenOrders),
    QryPositions(QryPositions),
    QryBalance(QryBalance),
    Heartbeat(Heartbeat),
}

// Wrapper with metadata (timestamp)
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("EngineTSMessage")]
#[repr(C)]
pub struct EngineTSMessage {
    pub timestamp: u64,
    pub message: EngineTSMessageType,
}

// =============================================================================
//
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("EngineTSMessage")]
#[repr(C)]
pub enum StateUpdate {
    OrderUpdate(Order),
    BalanceUpdate(Balance),
    PositionUpdate(Position),
}

// incoming messages (TS -> Engine)

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub enum TSEngineMessageType {
    OrderUpdate(Order),
    BalanceUpdate(Balance),
    PositionUpdate(Position),
    HeartbeatResponse,
    // query responses (streamed one at a time)
    QryOrdersResp(QryOrderResp),
    QryPositionsResp(QryPositionResp),
    QryBalancesResp(QryBalanceResp),
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("TSEngineMessage")]
#[repr(C)]
pub struct TSEngineMessage {
    pub timestamp: u64,
    pub message: TSEngineMessageType,
}

// =============================================================================
// order response types - generic across all exchanges
// =============================================================================

/// place order response - includes original request context + exchange response
#[derive(Debug, Clone)]
pub struct PlaceOrderResp {
    pub request_id: u8,
    pub request: PlaceOrder,
    pub status: PlaceOrderStatus,
}

/// possible outcomes for a place order request
#[derive(Debug, Clone)]
pub enum PlaceOrderStatus {
    /// order resting on book
    Resting { oid: u64 },
    /// order filled immediately (ioc/market)
    Filled {
        oid: u64,
        avg_px: f64,
        total_sz: f64,
    },
    /// order rejected by exchange
    Rejected(String),
}

impl PlaceOrderResp {
    pub fn is_success(&self) -> bool {
        matches!(
            self.status,
            PlaceOrderStatus::Resting { .. } | PlaceOrderStatus::Filled { .. }
        )
    }

    pub fn oid(&self) -> Option<u64> {
        match &self.status {
            PlaceOrderStatus::Resting { oid } => Some(*oid),
            PlaceOrderStatus::Filled { oid, .. } => Some(*oid),
            _ => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match &self.status {
            PlaceOrderStatus::Rejected(e) => Some(e),
            _ => None,
        }
    }
}

/// cancel order response
#[derive(Debug, Clone)]
pub struct CancelOrderResp {
    pub request_id: u8,
    pub request: CancelOrder,
    pub status: CancelOrderStatus,
}

/// possible outcomes for a cancel order request
#[derive(Debug, Clone)]
pub enum CancelOrderStatus {
    /// cancel succeeded
    Success,
    /// cancel failed - order already filled, cancelled, or never existed
    Failed(String),
}

impl CancelOrderResp {
    pub fn is_success(&self) -> bool {
        matches!(self.status, CancelOrderStatus::Success)
    }

    pub fn error(&self) -> Option<&str> {
        match &self.status {
            CancelOrderStatus::Failed(e) => Some(e),
            _ => None,
        }
    }
}

/// replace/modify order response
#[derive(Debug, Clone)]
pub struct ReplaceOrderResp {
    pub request_id: u8,
    pub request: ReplaceOrder,
    pub status: ReplaceOrderStatus,
}

/// possible outcomes for a replace order request
#[derive(Debug, Clone)]
pub enum ReplaceOrderStatus {
    /// replace succeeded
    Success,
    /// replace failed
    Failed(String),
}

impl ReplaceOrderResp {
    pub fn is_success(&self) -> bool {
        matches!(self.status, ReplaceOrderStatus::Success)
    }

    pub fn error(&self) -> Option<&str> {
        match &self.status {
            ReplaceOrderStatus::Failed(e) => Some(e),
            _ => None,
        }
    }
}

/// query stale order - check status of a single pending order
#[derive(Debug, Clone, Copy)]
pub struct QueryStaleOrder {
    pub venue: Venue,
    pub client_order_id: ClientOrderId,
}

/// response for stale order query
#[derive(Debug, Clone)]
pub struct QueryStaleOrderResp {
    pub client_order_id: ClientOrderId,
    pub venue: Venue,
    /// None if order not found on exchange (likely rejected/never received)
    pub order: Option<Order>,
    pub success: bool,
    pub error: Option<String>,
}

impl QueryStaleOrderResp {
    pub fn found(client_order_id: ClientOrderId, venue: Venue, order: Order) -> Self {
        Self {
            client_order_id,
            venue,
            order: Some(order),
            success: true,
            error: None,
        }
    }

    pub fn not_found(client_order_id: ClientOrderId, venue: Venue) -> Self {
        Self {
            client_order_id,
            venue,
            order: None,
            success: true,
            error: None,
        }
    }

    pub fn failed(client_order_id: ClientOrderId, venue: Venue, error: String) -> Self {
        Self {
            client_order_id,
            venue,
            order: None,
            success: false,
            error: Some(error),
        }
    }
}

/// unified response enum for matching in produce()
#[derive(Debug)]
pub enum OrderResponse {
    Place(PlaceOrderResp),
    Cancel(CancelOrderResp),
    Replace(ReplaceOrderResp),
    QueryStaleOrder(QueryStaleOrderResp),
}

// =============================================================================
// query response types (IPC compatible - streaming one item at a time)
// =============================================================================

/// balance query response - streamed one at a time
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryBalanceResp")]
#[repr(C)]
pub struct QryBalanceResp {
    pub balance: Balance,
    pub is_last: bool,
    pub success: bool,
}

impl QryBalanceResp {
    pub fn item(balance: Balance, is_last: bool) -> Self {
        Self {
            balance,
            is_last,
            success: true,
        }
    }

    pub fn empty() -> Self {
        Self {
            balance: Balance::default(),
            is_last: true,
            success: true,
        }
    }

    pub fn failed() -> Self {
        Self {
            balance: Balance::default(),
            is_last: true,
            success: false,
        }
    }
}

/// order query response - streamed one at a time
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryOrderResp")]
#[repr(C)]
pub struct QryOrderResp {
    pub order: Order,
    pub is_last: bool,
    pub success: bool,
}

impl QryOrderResp {
    pub fn item(order: Order, is_last: bool) -> Self {
        Self {
            order,
            is_last,
            success: true,
        }
    }

    pub fn empty() -> Self {
        Self {
            order: Order::default(),
            is_last: true,
            success: true,
        }
    }

    pub fn failed() -> Self {
        Self {
            order: Order::default(),
            is_last: true,
            success: false,
        }
    }
}

/// position query response - streamed one at a time
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryPositionResp")]
#[repr(C)]
pub struct QryPositionResp {
    pub position: Position,
    pub is_last: bool,
    pub success: bool,
}

impl QryPositionResp {
    pub fn item(position: Position, is_last: bool) -> Self {
        Self {
            position,
            is_last,
            success: true,
        }
    }

    pub fn empty() -> Self {
        Self {
            position: Position::default(),
            is_last: true,
            success: true,
        }
    }

    pub fn failed() -> Self {
        Self {
            position: Position::default(),
            is_last: true,
            success: false,
        }
    }
}
