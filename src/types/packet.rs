use crate::types::{
    clock::timestamp_nanos,
    kline::Kline,
    orderbook::Orderbook,
    trade_server::{CancelOrder, PlaceOrder, ReplaceOrder},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Packet<T> {
    pub timestamp: u64,
    pub seq_num: u64,
    pub body: T,
}

impl<T> Packet<T> {
    pub fn new(body: T, seq_num: u64) -> Packet<T> {
        Self {
            timestamp: timestamp_nanos(),
            seq_num,
            body,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MDMessage {
    Orderbook(Orderbook),
    Kline(Kline),
}

#[derive(Debug, Clone)]
pub enum TSInternalMessage {
    PlaceOrder(PlaceOrder),
    CancelOrder(CancelOrder),
    ReplaceOrder(ReplaceOrder),
}
