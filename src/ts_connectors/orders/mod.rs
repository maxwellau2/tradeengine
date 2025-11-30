pub mod hyperliquid;

use crate::types::{common::PassportId, trade_server::{CancelOrder, PlaceOrder, ReplaceOrder}};

pub trait Executor {
    fn new(passport_id: PassportId) -> Self;
    fn connect(&mut self);
    fn produce(&mut self);
    fn place_order(&mut self, order: PlaceOrder);
    fn cancel_order(&mut self, cancel: CancelOrder);
    fn replace_order(&mut self, replace: ReplaceOrder);
}