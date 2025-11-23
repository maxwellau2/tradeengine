use crate::types::common::{Symbol, Venue};

#[derive(Debug, Clone, Copy)]
pub enum Side{
    LONG,
    SHORT,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderType{
    LIMIT,
    MARKET,
}

#[derive(Debug, Clone, Copy)]
pub enum TimeInForce{
    GTC,
    PO,
    FOK,
    IOC,
}

pub type ClientOrderId = String;

// Outgoing messages to TradeServer
pub struct PlaceOrder{
    pub symbol: Symbol,
    pub venue: Venue,
    pub side: Side,
    pub client_order_id: ClientOrderId,
    pub qty: f64,
    pub price: f64,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
}

pub struct CancelOrder{
    pub symbol: Symbol,
    pub venue: Venue,
    pub client_order_id: ClientOrderId,
}

pub struct ReplaceOrder{
    pub symbol: Symbol,
    pub venue: Venue,
    pub side: Side,
    pub client_order_id: ClientOrderId,
    pub qty: f64,
    pub price: f64,
    pub order_type: OrderType,
}

// incoming messages from TradeServer
// pub struct EventOrderNew{
//     symbol: Symbol,
//     venue: Venue,
//     client_order_id: ClientOrderId,
//     side: Side,
//     qty: f64,
//     fee: f64,
//     order_type: OrderType,
//     time_in_force: TimeInForce,
// }
// pub struct EventOrderFill{
//     symbol: Symbol,
//     venue: Venue,
//     client_order_id: ClientOrderId,
//     side: Side,
//     qty: f64,
//     filled_qty: f64,
//     is_maker: bool,
//     order_type: OrderType,
//     time_in_force: TimeInForce,
//     fee: f64,
// }
// pub struct EventOrderCancelled{
//     symbol: Symbol,
//     venue: Venue,
//     client_order_id: ClientOrderId,
//     side: Side,
//     qty: f64,
//     filled_qty: f64,
//     order_type: OrderType,
//     time_in_force: TimeInForce
// }
// pub struct EventOrderReplaced{
//     symbol: Symbol,
//     venue: Venue,
//     client_order_id: ClientOrderId,
//     side: Side,
//     qty: f64,
//     filled_qty: f64,
//     is_maker: bool,
//     fee: f64,
//     order_type: OrderType,
//     time_in_force: TimeInForce
// }

pub struct OrderEvent{
    order: Order,
    status: OrderState,
}

// Internal tracking

#[derive(Debug, Clone)]
pub enum OrderState{
    PENDING_NEW,
    NEW,
    PARTIALLY_FILLED,
    // terminal states
    FILLED,
    CANCELLED,
    REJECTED,
    UNKNOWN,
}

#[derive(Debug, Clone)]
pub struct Order{
    pub symbol: Symbol,
    pub venue: Venue,
    pub side: Side,
    pub client_order_id: ClientOrderId,
    pub qty: f64,
    pub filled_qty: f64,
    pub price: f64,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub state: OrderState,
}


pub struct OrderManager{
    tracked_orders: Vec<Order>,
}

// impl OrderManager{
//     pub fn new ()->Self{
//         return Self { tracked_orders: Vec::new() }
//     }

//     pub fn 
// }