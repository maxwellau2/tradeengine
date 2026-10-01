use crate::types::{
    asset_ctx::AssetCtx,
    clock::timestamp_nanos_precise,
    funding::FundingInfo,
    kline::Kline,
    orderbook::Orderbook,
    trade::Trade,
    trade_server::{
        CancelOrder, CancelOrderResp, PlaceOrder, PlaceOrderResp, QueryStaleOrder,
        QueryStaleOrderResp, ReplaceOrder, ReplaceOrderResp, StateUpdate,
    },
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
            timestamp: timestamp_nanos_precise(),
            seq_num,
            body,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MDMessage {
    Orderbook(Orderbook),
    Kline(Kline),
    AssetCtx(AssetCtx),
    FundingInfo(FundingInfo),
    Trade(Trade),
}

#[derive(Debug, Clone)]
pub enum TSInternalMessage {
    PlaceOrder(PlaceOrder),
    CancelOrder(CancelOrder),
    ReplaceOrder(ReplaceOrder),
    QueryStaleOrder(QueryStaleOrder),
    // from execution
    CancelOrderResp(CancelOrderResp),
    PlaceOrderResp(PlaceOrderResp),
    ReplaceOrderResp(ReplaceOrderResp),
    QueryStaleOrderResp(QueryStaleOrderResp),
    // from state subscriber
    StateUpdate(StateUpdate),
}
