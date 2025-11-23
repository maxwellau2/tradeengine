// prerequisite: need to have the media driver running
// see start_media_driver.sh
// also need a listener

use md_feed::channels::md_channel::{ChannelProtocol, MDChannel};
use md_feed::types::common::Venue;
use md_feed::types::orderbook::{Level, Orderbook};
use md_feed::types::packet::MessageBody;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a channel
    let mut channel = MDChannel::new(
        "binance".to_string(),
        "aeron:ipc".to_string(),
        1001,
        "BTCUSDT".to_string(),
        1,
        true,
        vec!["1m".to_string()],
        ChannelProtocol::SHM,
    )?;

    println!("MDChannel created successfully!");

    // Example: Publish a message
    let body = MessageBody::Orderbook(Orderbook::new(
        "ETHUSDT".into(),
        Venue::Binance,
        vec![Level::new(123.1, 12.0)],
        vec![Level::new(123.1, 12.0)],
        1234,
    ));
    channel.publish(body)?;

    Ok(())
}
