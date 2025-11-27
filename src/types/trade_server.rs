use iceoryx2::prelude::*;

use crate::types::common::{ClientOrderId, OrderType, Side, Symbol, TimeInForce, Venue};

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
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("CancelOrder")]
#[repr(C)]
pub struct CancelOrder {
    pub symbol: Symbol,
    pub venue: Venue,
    pub client_order_id: ClientOrderId,
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
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryOpenOrders")]
#[repr(C)]
pub struct QryOpenOrders {
    pub venue: Venue,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryPositions")]
#[repr(C)]
pub struct QryPositions {
    pub venue: Venue,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("QryBalance")]
#[repr(C)]
pub struct QryBalance {
    pub venue: Venue,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("Heartbeat")]
#[repr(C)]
pub struct Heartbeat {}

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

// incoming messages (TS -> Engine)

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub enum TSEngineMessageType {
    OrderUpdate,
    BalanceUpdate,
    PositionUpdate,
}

#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[type_name("EngineTSMessage")]
#[repr(C)]
pub struct TSEngineMessage {
    pub timestamp: u64,
    pub message: TSEngineMessageType,
}
