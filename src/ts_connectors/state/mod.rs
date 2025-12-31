pub mod error;
pub mod hyperliquid;

use async_trait::async_trait;

use crate::ts_connectors::state::error::{StateError, StateResult};
use crate::ts_connectors::state::hyperliquid::HyperliquidStateSubscriber;
use crate::types::common::PassportId;
use crate::types::trade_server::StateUpdate;
use std::error::Error;

#[async_trait]
pub trait StateSubscriber: Send {
    type Error: Error + Send + Sync + 'static;
    async fn new(passport_id: PassportId, cfg_path: &str) -> StateResult<Self>
    where
        Self: Sized;
    async fn connect(&mut self);
    async fn subscribe_order_updates(&mut self) -> StateResult<()>;
    async fn subscribe_position_updates(&mut self) -> StateResult<()>;
    async fn subscribe_balance_updates(&mut self) -> StateResult<()>;
    async fn subscribe_all(&mut self) -> StateResult<()> {
        self.subscribe_order_updates().await?;
        self.subscribe_balance_updates().await?;
        self.subscribe_position_updates().await?;
        Ok(())
    }
    async fn get_orders();
    async fn get_positions();
    async fn get_balances();
    async fn produce(&mut self) -> Option<StateUpdate>;
}

pub enum AnyStateSubscriber {
    Hyperliquid(HyperliquidStateSubscriber),
}

impl AnyStateSubscriber {
    pub async fn connect(&mut self) {
        match self {
            AnyStateSubscriber::Hyperliquid(s) => s.connect().await,
        }
    }

    pub async fn produce(&mut self) -> Option<StateUpdate> {
        match self {
            AnyStateSubscriber::Hyperliquid(s) => s.produce().await,
        }
    }
}
