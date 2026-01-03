// md feed subscriber trait - consistent interface for all exchange md feeds
//
// each exchange implements this trait with direct websocket ownership.
// no generic abstractions - each feed handles its own auth, heartbeat, parsing.
// designed for select_all multiplexing in a single async runtime.

use crate::types::common::Venue;
use crate::types::packet::MDMessage;
use async_trait::async_trait;

/// market data feed subscriber
///
/// similar to state subscriber pattern - owns websocket directly,
/// produces messages via async produce() for select_all multiplexing.
#[async_trait]
pub trait MDSubscriber: Send {
    /// connect to the exchange websocket
    async fn connect(&mut self);

    /// produce next market data message (non-blocking via select)
    ///
    /// returns None if no data available, Some(msg) when data arrives.
    /// internally uses tokio::select! for ws messages vs heartbeat.
    async fn produce(&mut self) -> Option<MDMessage>;

    /// get the venue this subscriber is for
    fn venue(&self) -> Venue;
}

/// wrapper enum for type-erased md subscribers
pub enum AnyMDSubscriber {
    Paradex(super::paradex::subscriber::ParadexMDSubscriber),
    Hyperliquid(super::hyperliquid::subscriber::HyperliquidMDSubscriber),
}

impl AnyMDSubscriber {
    pub async fn connect(&mut self) {
        match self {
            Self::Paradex(s) => s.connect().await,
            Self::Hyperliquid(s) => s.connect().await,
        }
    }

    pub async fn produce(&mut self) -> Option<MDMessage> {
        match self {
            Self::Paradex(s) => s.produce().await,
            Self::Hyperliquid(s) => s.produce().await,
        }
    }

    pub fn venue(&self) -> Venue {
        match self {
            Self::Paradex(s) => s.venue(),
            Self::Hyperliquid(s) => s.venue(),
        }
    }
}
