use crate::md_connectors::base::md_feed_base::MDFeed;
use crate::md_connectors::networking_base::websocket::websocket::WebSocketClient;
use crate::md_connectors::networking_base::websocket::websocket_error::WsError;
use crate::md_connectors::networking_base::websocket::{message_handler, websocket_error};
use crate::md_connectors::paradex::constants;
use crate::types::clock::timestamp_nanos;
use crate::types::common::Venue;
use crate::types::orderbook::Orderbook;
use crate::types::packet::{MDMessage, Packet};
use async_trait::async_trait;
use ringbuf::traits::Producer;
use serde_json::Value;
use simd_json::base::ValueAsScalar;
use simd_json::derived::ValueObjectAccess;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

// global subscription id counter
static SUB_ID: AtomicU64 = AtomicU64::new(1);

pub struct ParadexHandler {
    pub ringbuf_producer: ringbuf::HeapProd<Packet<MDMessage>>,
    orderbook_buffer: Orderbook,
    subscriptions: Vec<Value>,
    seq_num: u64,
    dropped_packets: u64,
}

impl ParadexHandler {
    pub fn new(
        ringbuf_producer: ringbuf::HeapProd<Packet<MDMessage>>,
        subscriptions: Vec<Value>,
    ) -> Self {
        Self {
            ringbuf_producer,
            orderbook_buffer: Orderbook::empty(Venue::Hyperliquid),
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
impl message_handler::MessageHandler for ParadexHandler {
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
        let start = timestamp_nanos();

        // simd-json requires mutable bytes
        let mut bytes = text.into_bytes();

        // quick check: subscription messages contain "subscription" method
        // format: {"jsonrpc":"2.0","method":"subscription","params":{...}}
        // skip non-subscription messages (e.g. ping/pong, ack responses)
        if bytes.len() < 50 || !bytes.windows(12).any(|w| w == b"subscription") {
            debug!("control message (skipped parse)");
            return Ok(());
        }

        // zero-allocation parse using BorrowedValue - borrows strings directly from input
        let val: simd_json::BorrowedValue =
            simd_json::to_borrowed_value(&mut bytes).map_err(|e| {
                websocket_error::WsError::Json(serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                )))
            })?;

        // get channel type from params
        let params = val
            .get("params")
            .ok_or(WsError::Handler("missing params field".into()))?;
        let channel = params.get("channel").and_then(|v| v.as_str()).unwrap_or("");

        match channel {
            _ if channel.contains("order_book") => {
                self.orderbook_buffer.update_from_paradex(params);
                self.seq_num = self.seq_num.wrapping_add(1);

                let packet = Packet::new(
                    MDMessage::Orderbook(self.orderbook_buffer.clone()),
                    self.seq_num,
                );
                self.publish(packet);
            }

            _ => {
                warn!("unhandled message {:?}", params);
            }
        }

        Ok(())
    }

    fn get_subscription_messages(&self) -> Vec<Value> {
        self.subscriptions.clone()
    }
}

pub struct ParadexMDFeed {
    ws_client: WebSocketClient<ParadexHandler>,
}

impl MDFeed for ParadexMDFeed {
    type Handler = ParadexHandler;

    fn new(
        producer_rb: ringbuf::HeapProd<Packet<MDMessage>>,
        is_testnet: bool,
        subscriptions: Vec<Value>, // todo! aggregate into a std form
    ) -> Self {
        // Determine URL based on testnet flag
        let url = if is_testnet {
            constants::PARADEX_TEST_URL
        } else {
            constants::PARADEX_PROD_URL
        };

        // Create handler - handler takes ownership of producer
        let handler = ParadexHandler::new(producer_rb, subscriptions);
        // Create WebSocket client
        let ws_client = WebSocketClient::new(url.to_string(), "v1".to_string(), handler);
        Self { ws_client }
    }

    fn exchange_name() -> &'static str {
        "Hyperliquid"
    }

    fn into_client(self) -> WebSocketClient<Self::Handler> {
        self.ws_client
    }

    fn orderbook_subscription(symbol: &str) -> serde_json::Value {
        let channel = format!("order_book.{}.snapshot@15@50ms", symbol);
        let id = SUB_ID.fetch_add(1, Ordering::Relaxed);
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {
                "channel": channel
            },
            "id": id
        })
    }

    fn kline_subscription(
        symbol: &str,
        interval: crate::types::common::KlineInterval,
    ) -> serde_json::Value {
        panic!("Kline subscription not supported on paradex");
    }

    // run_forever() uses the default trait implementation!
}

impl ParadexMDFeed {
    /// Get a reference to the WebSocket client
    pub fn client(&self) -> &WebSocketClient<ParadexHandler> {
        &self.ws_client
    }

    /// Get a mutable reference to the WebSocket client
    pub fn client_mut(&mut self) -> &mut WebSocketClient<ParadexHandler> {
        &mut self.ws_client
    }
}
