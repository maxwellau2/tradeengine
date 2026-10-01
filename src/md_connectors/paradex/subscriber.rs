// paradex market data subscriber - direct websocket ownership
//
// no generic abstractions, handles paradex-specific:
// - json-rpc subscription format
// - orderbook parsing with simd-json
// - heartbeat/ping handling

use crate::types::common::Venue;
use crate::types::orderbook::Orderbook;
use crate::types::packet::MDMessage;
use crate::types::trade::Trade;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use simd_json::base::ValueAsScalar;
use simd_json::derived::ValueObjectAccess;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, trace, warn};
use tungstenite::Message;

const MAINNET_WS_URL: &str = "wss://ws.api.prod.paradex.trade/v1";
const TESTNET_WS_URL: &str = "wss://ws.api.testnet.paradex.trade/v1";

// subscription id counter
static SUB_ID: AtomicU64 = AtomicU64::new(1);

/// subscription config for paradex md feed
#[derive(Debug, Clone)]
pub struct ParadexSubscription {
    pub channel: String,
}

impl ParadexSubscription {
    /// create orderbook subscription
    /// format: order_book.{symbol}.snapshot@{depth}@{interval}
    pub fn orderbook(symbol: &str) -> Self {
        Self {
            channel: format!("order_book.{}.snapshot@15@50ms", symbol),
        }
    }

    /// create trades subscription
    /// format: trades.{symbol}
    pub fn trades(symbol: &str) -> Self {
        Self {
            channel: format!("trades.{}", symbol),
        }
    }

    /// convert to json-rpc subscribe message
    fn to_subscribe_msg(&self) -> Value {
        let id = SUB_ID.fetch_add(1, Ordering::Relaxed);
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {
                "channel": self.channel
            },
            "id": id
        })
    }
}

pub struct ParadexMDSubscriber {
    ws: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    subscriptions: Vec<ParadexSubscription>,
    orderbook_buffer: Orderbook,
    is_mainnet: bool,
}

impl ParadexMDSubscriber {
    pub fn new(subscriptions: Vec<ParadexSubscription>, is_mainnet: bool) -> Self {
        Self {
            ws: None,
            subscriptions,
            orderbook_buffer: Orderbook::empty(Venue::Paradex),
            is_mainnet,
        }
    }

    fn ws_url(&self) -> &str {
        if self.is_mainnet {
            MAINNET_WS_URL
        } else {
            TESTNET_WS_URL
        }
    }

    /// connect and subscribe
    pub async fn connect(&mut self) {
        let url = self.ws_url();
        info!("connecting to paradex md: {}", url);

        match connect_async(url).await {
            Ok((ws, _)) => {
                self.ws = Some(ws);
                info!("paradex md connected");

                // send subscriptions
                if let Err(e) = self.send_subscriptions().await {
                    error!("failed to send subscriptions: {}", e);
                    self.ws = None;
                }
            }
            Err(e) => {
                error!("failed to connect to paradex md: {}", e);
            }
        }
    }

    async fn send_subscriptions(&mut self) -> Result<(), tungstenite::Error> {
        let ws = self.ws.as_mut().ok_or(tungstenite::Error::AlreadyClosed)?;

        for sub in &self.subscriptions {
            let msg = sub.to_subscribe_msg();
            info!("subscribing to: {}", sub.channel);
            ws.send(Message::Text(msg.to_string())).await?;
        }

        Ok(())
    }

    /// parse ws message into MDMessage
    fn parse_message(&mut self, text: &str) -> Option<MDMessage> {
        // simd-json needs mutable bytes
        let mut bytes = text.as_bytes().to_vec();

        // quick filter: only process subscription messages
        if bytes.len() < 50 || !bytes.windows(12).any(|w| w == b"subscription") {
            trace!("control message, skipping");
            return None;
        }

        // parse with simd-json
        let val: simd_json::BorrowedValue = match simd_json::to_borrowed_value(&mut bytes) {
            Ok(v) => v,
            Err(e) => {
                debug!("failed to parse json: {}", e);
                return None;
            }
        };

        // extract params
        let params = val.get("params")?;
        let channel = params.get("channel")?.as_str()?;

        // route by channel type
        if channel.contains("order_book") {
            if let Err(e) = self.orderbook_buffer.update_from_paradex(params) {
                debug!("failed to update orderbook: {}", e);
                return None;
            }
            return Some(MDMessage::Orderbook(self.orderbook_buffer.clone()));
        }

        if channel.starts_with("trades.") {
            match Trade::from_paradex(params) {
                Some(trade) => return Some(MDMessage::Trade(trade)),
                None => {
                    debug!("failed to parse trade from channel: {}", channel);
                    return None;
                }
            }
        }

        None
    }

    /// produce next md message - blocks until message available
    ///
    /// note: this method awaits on ws.next() so it will block until data arrives.
    /// the runtime wraps this in a stream and uses now_or_never() for non-blocking poll.
    pub async fn produce(&mut self) -> Option<MDMessage> {
        if self.ws.is_none() {
            self.connect().await;
            return None;
        }

        let ws = self.ws.as_mut()?;

        // just await next message - let runtime handle non-blocking via now_or_never
        match ws.next().await {
            Some(Ok(Message::Text(txt))) => {
                return self.parse_message(&txt);
            }
            Some(Ok(Message::Ping(data))) => {
                let _ = ws.send(Message::Pong(data)).await;
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                warn!("paradex md ws disconnected");
                self.ws = None;
            }
            _ => {}
        }

        None
    }

    pub fn venue(&self) -> Venue {
        Venue::Paradex
    }
}
