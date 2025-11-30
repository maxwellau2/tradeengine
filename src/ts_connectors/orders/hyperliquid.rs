use crate::{ts_connectors::orders::Executor, types::common::PassportId};


struct Hyperliquid{
    pub user_address: String,
    pub private_key: String,
}

impl Executor for Hyperliquid{
    fn new(passport_id: PassportId) -> Self {
        todo!()
    }

    fn connect(&mut self) {
        todo!()
    }

    fn place_order(&mut self, order: crate::types::trade_server::PlaceOrder) {
        todo!()
    }

    fn cancel_order(&mut self, cancel: crate::types::trade_server::CancelOrder) {
        todo!()
    }

    fn replace_order(&mut self, replace: crate::types::trade_server::ReplaceOrder) {
        todo!()
    }
    
    fn produce(&mut self) {
        todo!()
    }
}