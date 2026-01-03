// hyperliquid market data subscriber - direct websocket ownership
//
// handles hyperliquid-specific:
// - subscription format (method/subscription json)
// - orderbook (l2Book) parsing
// - kline (candle) parsing with close detection
// - json ping heartbeat

use crate::types::common::{KlineInterval, Symbol, Venue};
use crate::types::kline::Kline;
use crate::types::orderbook::Orderbook;
use crate::types::packet::MDMessage;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use simd_json::prelude::{ValueAsScalar, ValueObjectAccess};
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, trace, warn};
use tungstenite::Message;

const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";

/// key for tracking klines: (symbol, interval)
type KlineKey = (Symbol, KlineInterval);

/// subscription types for hyperliquid
#[derive(Debug, Clone)]
pub enum HyperliquidSubscription {
    Orderbook {
        coin: String,
    },
    Kline {
        coin: String,
        interval: KlineInterval,
    },
}

impl HyperliquidSubscription {
    pub fn orderbook(coin: &str) -> Self {
        Self::Orderbook {
            coin: coin.to_string(),
        }
    }

    pub fn kline(coin: &str, interval: KlineInterval) -> Self {
        Self::Kline {
            coin: coin.to_string(),
            interval,
        }
    }

    fn to_subscribe_msg(&self) -> Value {
        match self {
            Self::Orderbook { coin } => {
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": { "type": "l2Book", "coin": coin }
                })
            }
            Self::Kline { coin, interval } => {
                serde_json::json!({
                    "method": "subscribe",
                    "subscription": { "type": "candle", "coin": coin, "interval": interval.to_string() }
                })
            }
        }
    }
}

pub struct HyperliquidMDSubscriber {
    ws: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    subscriptions: Vec<HyperliquidSubscription>,
    orderbook_buffer: Orderbook,
    kline_tracker: HashMap<KlineKey, Kline>,
    kline_buffer: Kline,
    is_mainnet: bool,
}

impl HyperliquidMDSubscriber {
    pub fn new(subscriptions: Vec<HyperliquidSubscription>, is_mainnet: bool) -> Self {
        Self {
            ws: None,
            subscriptions,
            orderbook_buffer: Orderbook::empty(Venue::Hyperliquid),
            kline_tracker: HashMap::new(),
            kline_buffer: Kline::default_with_venue(Venue::Hyperliquid),
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

    pub async fn connect(&mut self) {
        let url = self.ws_url();
        info!("connecting to hyperliquid md: {}", url);

        match connect_async(url).await {
            Ok((ws, _)) => {
                self.ws = Some(ws);
                info!("hyperliquid md connected");

                if let Err(e) = self.send_subscriptions().await {
                    error!("failed to send subscriptions: {}", e);
                    self.ws = None;
                }
            }
            Err(e) => {
                error!("failed to connect to hyperliquid md: {}", e);
            }
        }
    }

    async fn send_subscriptions(&mut self) -> Result<(), tungstenite::Error> {
        let ws = self.ws.as_mut().ok_or(tungstenite::Error::AlreadyClosed)?;

        for sub in &self.subscriptions {
            let msg = sub.to_subscribe_msg();
            debug!("subscribing: {}", msg);
            ws.send(Message::Text(msg.to_string())).await?;
        }

        Ok(())
    }

    /// parse ws message into MDMessage
    fn parse_message(&mut self, text: &str) -> Option<MDMessage> {
        let mut bytes = text.as_bytes().to_vec();

        // quick filter: market data starts with {"channel":
        if bytes.len() < 12 || &bytes[0..11] != b"{\"channel\":" {
            trace!("control message, skipping");
            return None;
        }

        let val: simd_json::BorrowedValue = match simd_json::to_borrowed_value(&mut bytes) {
            Ok(v) => v,
            Err(e) => {
                debug!("failed to parse json: {}", e);
                return None;
            }
        };

        let channel = val.get("channel")?.as_str()?;

        match channel {
            "l2Book" => {
                let data = val.get("data")?;
                if let Err(e) = self.orderbook_buffer.update_from_hyperliquid(data) {
                    debug!("orderbook parse error: {}", e);
                    return None;
                }
                Some(MDMessage::Orderbook(self.orderbook_buffer.clone()))
            }
            "candle" => {
                let data = val.get("data")?;
                if let Err(e) = self.kline_buffer.update_from_hyperliquid(data) {
                    debug!("kline parse error: {}", e);
                    return None;
                }

                let key = (self.kline_buffer.symbol, self.kline_buffer.interval);

                // detect candle close: if open_time changed, previous candle closed
                if let Some(mut prev) = self.kline_tracker.remove(&key) {
                    if prev.open_time != self.kline_buffer.open_time {
                        prev.is_closed = true;
                        // emit closed candle first
                        self.kline_tracker.insert(key, self.kline_buffer.clone());
                        return Some(MDMessage::Kline(prev));
                    }
                }

                self.kline_tracker.insert(key, self.kline_buffer.clone());
                Some(MDMessage::Kline(self.kline_buffer.clone()))
            }
            "subscriptionResponse" => {
                debug!("subscription confirmed");
                None
            }
            _ => {
                trace!("unhandled channel: {}", channel);
                None
            }
        }
    }

    /// produce next md message - blocks until message available
    pub async fn produce(&mut self) -> Option<MDMessage> {
        if self.ws.is_none() {
            self.connect().await;
            return None;
        }

        let ws = self.ws.as_mut()?;

        // just await next message - runtime handles non-blocking via select timeout
        match ws.next().await {
            Some(Ok(Message::Text(txt))) => {
                return self.parse_message(&txt);
            }
            Some(Ok(Message::Ping(data))) => {
                let _ = ws.send(Message::Pong(data)).await;
            }
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                warn!("hyperliquid md ws disconnected");
                self.ws = None;
            }
            _ => {}
        }

        None
    }

    pub fn venue(&self) -> Venue {
        Venue::Hyperliquid
    }
}
