// prerequisite: need to have the media driver running
// see start_media_driver.sh
// also need a listener

use md_feed::channels::md_channel::{ChannelProtocol, MDChannel};
use md_feed::types::common::{Venue, symbol_from_str};
use md_feed::types::orderbook::{Level, Orderbook};
use md_feed::types::packet::MDMessage;

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
    let body = MDMessage::Orderbook(Orderbook::new(
        symbol_from_str("ETHUSDT"),
        Venue::Binance,
        vec![Level::new(123.1, 12.0)],
        vec![Level::new(123.1, 12.0)],
        1234,
    ));
    channel.publish(body)?;

    Ok(())
}
