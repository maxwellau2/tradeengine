// this script is an example on connecting to an exchange (paradex) with the websocket wrapper

use async_trait::async_trait;
use md_feed::{
    md_connectors::networking_base::websocket::{message_handler, websocket, websocket_error},
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
            "channel": "order_book.ONDO-USD-PERP.snapshot@15@50ms"
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

//{"jsonrpc":"2.0","method":"subscription","params":{"channel":"order_book.ONDO-USD-PERP.snapshot@15@50ms","data":{"seq_no":690385857,"market":"ONDO-USD-PERP","last_updated_at":1767381936453,"update_type":"s","inserts":[{"side":"BUY","price":"0.4159","size":"4120.8"},{"side":"BUY","price":"0.4158","size":"1619.8"},{"side":"BUY","price":"0.4157","size":"9017.4"},{"side":"BUY","price":"0.4137","size":"5005"},{"side":"BUY","price":"0.4134","size":"1002.2"},{"side":"BUY","price":"0.4133","size":"5477.5"},{"side":"BUY","price":"0.4117","size":"8434.9"},{"side":"BUY","price":"0.4116","size":"6745.5"},{"side":"BUY","price":"0.4112","size":"5471.3"},{"side":"BUY","price":"0.4063","size":"5026.5"},{"side":"BUY","price":"0.4062","size":"33618"},{"side":"BUY","price":"0.3945","size":"825.5"},{"side":"BUY","price":"0.3912","size":"1743.5"},{"side":"BUY","price":"0.3825","size":"35814.9"},{"side":"BUY","price":"0.3783","size":"367.8"},{"side":"SELL","price":"0.4164","size":"2401.9"},{"side":"SELL","price":"0.4166","size":"1772.6"},{"side":"SELL","price":"0.4167","size":"1727.5"},{"side":"SELL","price":"0.4168","size":"8832.2"},{"side":"SELL","price":"0.4191","size":"904.5"},{"side":"SELL","price":"0.4192","size":"5291.5"},{"side":"SELL","price":"0.4195","size":"5168.3"},{"side":"SELL","price":"0.4214","size":"8761.1"},{"side":"SELL","price":"0.4215","size":"5422.3"},{"side":"SELL","price":"0.4217","size":"6793.7"},{"side":"SELL","price":"0.426","size":"5041.9"},{"side":"SELL","price":"0.4261","size":"31371.8"},{"side":"SELL","price":"0.4333","size":"8288"},{"side":"SELL","price":"0.4381","size":"1487.3"},{"side":"SELL","price":"0.4413","size":"1567"}],"updates":[],"deletes":[]}}}
