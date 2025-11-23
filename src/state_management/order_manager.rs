pub enum Side{
    LONG,
    SHORT,
}

pub enum OrderType{
    LIMIT,
    MARKET,
}

pub enum TimeInForce{
    GTC,
    PO,
    FOK,
    IOC,
}

type ClientOrderId = String;
type Venue = String;
type Symbol = u16;

// Outgoing messages to TradeServer
pub struct PlaceOrder{
    symbol: Symbol,
    venue: Venue,
    side: Side,
    client_order_id: ClientOrderId,
    qty: f64,
    price: f64,
    order_type: OrderType,
    time_in_force: TimeInForce,
}

pub struct CancelOrder{
    symbol: Symbol,
    venue: Venue,
    client_order_id: ClientOrderId,
}

pub struct ReplaceOrder{
    symbol: Symbol,
    venue: Venue,
    side: Side,
    client_order_id: ClientOrderId,
    qty: f64,
    price: f64,
    order_type: OrderType,
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

pub struct Order{
    symbol: Symbol,
    venue: Venue,
    side: Side,
    client_order_id: ClientOrderId,
    qty: f64,
    filled_qty: f64,
    price: f64,
    order_type: OrderType,
    time_in_force: TimeInForce,
    state: OrderState,
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