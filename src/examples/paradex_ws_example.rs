// this script is an example on connecting to an exchange (paradex) with the websocket wrapper

use async_trait::async_trait;
use md_feed::{
    exchange_connectors::networking_base::websocket::{
        message_handler, websocket, websocket_error,
    },
    types::packet::{MDMessage, Packet},
};
use serde_json::Value;

pub struct ParadexHandler {
    pub symbol: String, // e.g. "btcusdt"
    pub id: u64,
    // you can add references to your SPSC producer here
    // e.g. producer: SpscProducer<Packet<MDMessage>>,
}

impl ParadexHandler {
    pub fn new(symbol: String) -> Self {
        Self { symbol, id: 1 }
    }
}

#[async_trait]
impl message_handler::MessageHandler for ParadexHandler {
    fn publish(&mut self, packet: Packet<MDMessage>) {
        //
    }
    async fn on_message(&mut self, text: String) -> websocket_error::WsResult<()> {
        let msg: Value = serde_json::from_str(&text).map_err(websocket_error::WsError::Json)?;

        println!("raw msg: {msg}");
        // let data = msg.get("data");
        if let Some(event) = msg.get("channel").and_then(|v| v.as_str()) {
            match event {
                "l2Book" => {
                    // parse orderbook update
                    // convert to your internal Orderbook type
                    // push into your SPSC queue
                    // println!("Binance depth: {:?}", data);
                }
                "trade" => {
                    // parse trade
                }
                _ => {
                    // ignore other events
                }
            }
        }

        Ok(())
    }

    fn get_subscription_messages(&self) -> Vec<Value> {
        vec![serde_json::json!(
        {
        "jsonrpc": "2.0",
        "method": "subscribe",
        "params": {
            "channel": "order_book.ETH-USD-PERP.snapshot@15@100ms"
        },
        "id": 1
        }
        )]
    }
}

#[tokio::main]
async fn main() -> websocket_error::WsResult<()> {
    let handler = ParadexHandler::new("btcusdt".to_string());
    println!("hello");

    let mut client = websocket::WebSocketClient::new(
        "wss://ws.api.prod.paradex.trade".to_string(),
        "v1".to_string(),
        handler,
    );

    // this will run connect + run + reconnect forever
    client.run_forever(5).await?;

    Ok(())
}
