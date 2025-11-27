use crate::exchange_connectors::base::md_feed_base::MDFeed;
// Hyperliquid WebSocket feed handler with buffer reuse and proper error handling
use crate::exchange_connectors::hyperliquid::constants;
use crate::exchange_connectors::networking_base::websocket::websocket::WebSocketClient;
use crate::exchange_connectors::networking_base::websocket::{message_handler, websocket_error};
use crate::types::common::{KlineInterval, Venue, symbol_from_str};
use crate::types::kline::Kline;
use crate::types::{
    orderbook::Orderbook,
    packet::{MDMessage, Packet},
};
use async_trait::async_trait;
use ringbuf::{self, traits::Producer};
use serde_json::Value;
use tracing::{debug, warn};

pub struct HyperliquidHandler {
    pub ringbuf_producer: ringbuf::HeapProd<Packet<MDMessage>>,
    orderbook_buffer: Orderbook,
    kline_buffer: Kline,
    seq_num: u64,
    dropped_packets: u64,
    subscriptions: Vec<Value>,
}

impl HyperliquidHandler {
    pub fn new(
        ringbuf_producer: ringbuf::HeapProd<Packet<MDMessage>>,
        subscriptions: Vec<Value>,
    ) -> Self {
        Self {
            ringbuf_producer,
            orderbook_buffer: Orderbook::new(
                symbol_from_str(""),
                Venue::Hyperliquid,
                Vec::new(),
                Vec::new(),
                0,
            ),
            kline_buffer: Kline::new(
                0,
                symbol_from_str(""),
                Venue::Hyperliquid,
                KlineInterval::M1,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                false,
            ),
            seq_num: 0,
            dropped_packets: 0,
            subscriptions,
        }
    }

    pub fn get_dropped_packets(&self) -> u64 {
        self.dropped_packets
    }
}

#[async_trait]
impl message_handler::MessageHandler for HyperliquidHandler {
    fn publish(&mut self, packet: Packet<MDMessage>) {
        match self.ringbuf_producer.try_push(packet) {
            Ok(_) => {
                debug!("Published packet seq={}", self.seq_num);
            }
            Err(_packet) => {
                // Back pressure: queue is full, drop packet and track
                self.dropped_packets += 1;
                if self.dropped_packets % 100 == 0 {
                    warn!(
                        "Ringbuffer full! Dropped {} packets total. Consumer may be slow.",
                        self.dropped_packets
                    );
                }
            }
        }
    }
    async fn on_message(&mut self, text: String) -> websocket_error::WsResult<()> {
        let msg: Value = serde_json::from_str(&text).map_err(websocket_error::WsError::Json)?;

        // Extract channel and data
        let channel = msg.get("channel").and_then(|v| v.as_str());
        let data = msg.get("data");
        match channel {
            Some("l2Book") => {
                match data {
                    Some(value) => {
                        // Update buffer in-place (reuses Vec allocations)
                        self.orderbook_buffer.update_from_hyperliquid(value);

                        // Increment sequence number
                        self.seq_num = self.seq_num.wrapping_add(1);

                        // Clone the buffer (data is copied, but source keeps capacity)
                        let packet = Packet::new(
                            MDMessage::Orderbook(self.orderbook_buffer.clone()),
                            self.seq_num,
                        );

                        // Try to push to ringbuffer with back pressure handling
                        self.publish(packet);
                    }
                    None => {
                        warn!("Received l2Book message with no data field");
                    }
                }
            }
            Some("trade") => {
                debug!("Received trade message (not implemented)");
            }
            Some("candle") => {
                match data {
                    Some(value) => {
                        self.kline_buffer.update_from_hyperliquid(value);
                        self.seq_num = self.seq_num.wrapping_add(1);
                        // Clone the buffer (data is copied, but source keeps capacity)
                        let packet =
                            Packet::new(MDMessage::Kline(self.kline_buffer.clone()), self.seq_num);
                        // Try to push to ringbuffer with back pressure handling
                        self.publish(packet);
                    }
                    None => {
                        warn!("Received candle message with no data field");
                    }
                }
            }
            Some(other) => {
                debug!("Ignoring channel: {}", other);
            }
            None => {
                warn!("Received message with no channel field");
            }
        }

        Ok(())
    }

    fn get_subscription_messages(&self) -> Vec<Value> {
        self.subscriptions.clone()
    }
}

/// Generic market data feed trait for all exchanges

pub struct HyperliquidMDFeed {
    ws_client: WebSocketClient<HyperliquidHandler>,
}

impl MDFeed for HyperliquidMDFeed {
    type Handler = HyperliquidHandler;

    fn new(
        producer_rb: ringbuf::HeapProd<Packet<MDMessage>>,
        is_testnet: bool,
        subscriptions: Vec<Value>, // todo! aggregate into a std form
    ) -> Self {
        // Determine URL based on testnet flag
        let url = if is_testnet {
            constants::TEST_BASE_URL
        } else {
            constants::PROD_BASE_URL
        };

        // Create handler - handler takes ownership of producer
        let handler = HyperliquidHandler::new(producer_rb, subscriptions);
        // Create WebSocket client
        let ws_client = WebSocketClient::new(url.to_string(), "ws".to_string(), handler);
        Self { ws_client }
    }

    fn exchange_name() -> &'static str {
        "Hyperliquid"
    }

    fn into_client(self) -> WebSocketClient<Self::Handler> {
        self.ws_client
    }

    fn kline_subscription(symbol: &str, interval: KlineInterval) -> serde_json::Value {
        return serde_json::json!({
           "method": "subscribe",
            "subscription": { "type": symbol, "coin": "XLM", "interval": interval.to_string() }
        });
    }

    fn orderbook_subscription(symbol: &str) -> serde_json::Value {
        return serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": "l2Book", "coin": symbol }
        });
    }

    // run_forever() uses the default trait implementation!
}

impl HyperliquidMDFeed {
    /// Get a reference to the WebSocket client
    pub fn client(&self) -> &WebSocketClient<HyperliquidHandler> {
        &self.ws_client
    }

    /// Get a mutable reference to the WebSocket client
    pub fn client_mut(&mut self) -> &mut WebSocketClient<HyperliquidHandler> {
        &mut self.ws_client
    }
}
