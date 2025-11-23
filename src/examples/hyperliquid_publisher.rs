// use md_feed::{exchange_connectors::{hyperliquid::feed::HyperliquidHandler, networking_base::websocket::{websocket, websocket_error}}, types::{clock::{timestamp_micros, timestamp_nanos}, orderbook::Orderbook, packet::{MessageBody, Packet}}};
// use ringbuf::{HeapRb, traits::{Consumer, Split}};

// #[tokio::main]
// async fn main() -> websocket_error::WsResult<()> {
//     const RING_CAPACITY: usize = 1 << 15; // 1024
//     let rb = HeapRb::<Packet<MessageBody>>::new(RING_CAPACITY);
//     let (prod, mut cons) = rb.split();

//     // Define subscriptions
//     let subscriptions = vec![
//         serde_json::json!({
//             "method": "subscribe",
//             "subscription": { "type": "l2Book", "coin": "ETH" }
//         })
//     ];

//     // Handler takes ownership of producer
//     let handler = HyperliquidHandler::new(prod, subscriptions);
//     println!("hello");

//     let mut client = websocket::WebSocketClient::new(
//         "wss://api.hyperliquid.xyz".to_string(),
//         "ws".to_string(),
//         handler,
//     );

//     // Spawn run_forever in a background task
//     let client_task = tokio::spawn(async move {
//         client.run_forever(5).await
//     });

//     // Poll the ringbuffer consumer for messages
//     loop {
//         while let Some(packet) = cons.try_pop() {
//             let mut now = timestamp_nanos();
//             println!("Packet timestamp: {}, Now: {}", packet.timestamp, now);

//             if now >= packet.timestamp {
//                 println!("TX Latency: {} ns", now - packet.timestamp);
//             } else {
//                 println!("WARNING: Clock went backwards! now={}, packet={}", now, packet.timestamp);
//             }

//             now = timestamp_micros();
//             match packet.body{
//                 MessageBody::Orderbook(body) =>{
//                     println!("Received in Consumer: {:?}", body);
//                 }
//             }
//         }
//     }
//     Ok(())
// }

use md_feed::{
    exchange_connectors::{
        base::md_feed_base::MDFeed, hyperliquid::feed::HyperliquidMDFeed,
        networking_base::websocket::websocket_error::WsResult,
    },
    types::{
        clock::timestamp_nanos,
        packet::{MessageBody, Packet},
    },
};
use ringbuf::{
    HeapRb,
    traits::{Consumer, Split},
};

#[tokio::main]
pub async fn main() -> WsResult<()> {
    const RING_CAPACITY: usize = 1 << 15; // 1024
    let rb = HeapRb::<Packet<MessageBody>>::new(RING_CAPACITY);
    let (prod, mut cons) = rb.split();
    let subscriptions = vec![
        serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": "l2Book", "coin": "ETH" }
        }),
        serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": "l2Book", "coin": "BTC" }
        }),
    ];
    let mut feed = HyperliquidMDFeed::new(prod, false, subscriptions);
    tokio::spawn(async move { feed.run_forever(5).await });

    loop {
        while let Some(packet) = cons.try_pop() {
            let mut now = timestamp_nanos();
            println!("Packet timestamp: {}, Now: {}", packet.timestamp, now);

            if now >= packet.timestamp {
                println!("TX Latency: {} ns", now - packet.timestamp);
            } else {
                println!(
                    "WARNING: Clock went backwards! now={}, packet={}",
                    now, packet.timestamp
                );
            }

            now = timestamp_nanos();
            match packet.body {
                MessageBody::Orderbook(body) => {
                    println!("Received in Consumer: {:?}", body);
                }
            }
        }
    }
    Ok(())
}
