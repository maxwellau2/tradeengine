use crate::types::{clock::timestamp_nanos, orderbook::Orderbook};
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
pub enum MessageBody {
    Orderbook(Orderbook),
}
