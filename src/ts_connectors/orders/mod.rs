pub mod error;
pub mod hyperliquid;
pub mod signature_utls;

use std::error::Error;

use anyhow::Result;
use async_trait::async_trait;

use crate::types::{
    common::PassportId,
    trade_server::{CancelOrder, OrderResponse, PlaceOrder, ReplaceOrder},
};

pub use hyperliquid::HyperliquidExecutor;

#[async_trait]
pub trait Executor: Send {
    type Error: Error + Send + Sync + 'static;

    async fn new(passport_id: PassportId, cfg_path: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;

    async fn connect(&mut self);

    /// poll for order responses, returns None for non-order messages (ping, reconnect, etc)
    async fn produce(&mut self) -> Option<OrderResponse>;

    async fn place_order(&mut self, order: PlaceOrder) -> Result<(), Self::Error>;
    async fn cancel_order(&mut self, cancel: CancelOrder) -> Result<(), Self::Error>;
    async fn replace_order(&mut self, replace: ReplaceOrder) -> Result<(), Self::Error>;
}

/// enum wrapper for all executor types, enables storing different executors in a vec
/// uses static dispatch internally (match arms) so no vtable overhead
pub enum AnyExecutor {
    Hyperliquid(HyperliquidExecutor),
    // add other venues here as they're implemented:
    // Binance(BinanceExecutor),
    // Paradex(ParadexExecutor),
}

impl AnyExecutor {
    pub async fn connect(&mut self) {
        match self {
            AnyExecutor::Hyperliquid(e) => e.connect().await,
        }
    }

    pub async fn produce(&mut self) -> Option<OrderResponse> {
        match self {
            AnyExecutor::Hyperliquid(e) => e.produce().await,
        }
    }

    pub async fn place_order(&mut self, order: PlaceOrder) -> Result<()> {
        match self {
            AnyExecutor::Hyperliquid(e) => e.place_order(order).await.map_err(Into::into),
        }
    }

    pub async fn cancel_order(&mut self, cancel: CancelOrder) -> Result<()> {
        match self {
            AnyExecutor::Hyperliquid(e) => e.cancel_order(cancel).await.map_err(Into::into),
        }
    }

    pub async fn replace_order(&mut self, replace: ReplaceOrder) -> Result<()> {
        match self {
            AnyExecutor::Hyperliquid(e) => e.replace_order(replace).await.map_err(Into::into),
        }
    }
}
