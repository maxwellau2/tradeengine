use serde_json::Value;

use crate::{
    md_connectors::networking_base::websocket::{
        message_handler, websocket::WebSocketClient, websocket_error,
    },
    types::{
        common::KlineInterval,
        packet::{MDMessage, Packet},
    },
};

pub trait MDFeed: Sized + Send + 'static {
    /// The message handler type for this feed
    type Handler: message_handler::MessageHandler;

    /// Create a new feed with producer, testnet flag, and subscriptions
    fn new(
        producer_rb: ringbuf::HeapProd<Packet<MDMessage>>,
        is_testnet: bool,
        subscriptions: Vec<Value>,
    ) -> Self;

    /// Get the exchange name (e.g., "Hyperliquid", "Binance")
    fn exchange_name() -> &'static str;

    /// Convert the feed into its underlying WebSocket client
    fn into_client(self) -> WebSocketClient<Self::Handler>;

    fn kline_subscription(symbol: &str, interval: KlineInterval) -> serde_json::Value;

    fn orderbook_subscription(symbol: &str) -> serde_json::Value;

    /// Run the feed forever with automatic reconnection (default implementation)
    fn run_forever(
        self,
        reconnect_delay_secs: u64,
    ) -> tokio::task::JoinHandle<Result<(), websocket_error::WsError>> {
        tokio::spawn(async move {
            let mut client = self.into_client();
            client.run_forever(reconnect_delay_secs).await
        })
    }
}
