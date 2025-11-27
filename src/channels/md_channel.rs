use crate::channels::utils;
use crate::types::packet::{MDMessage, Packet};
use bincode::serialize;
use quanta::Clock;
use rusteron_client::*;
use std::error;

pub enum ChannelProtocol {
    SHM,
    UDP,
}

pub struct MDChannel {
    exchange: String,
    channel: String,
    stream_id: i32,
    exchange_symbol: String,
    symbol_id: u16,
    orderbook_sub: bool,
    kline_sub: Vec<String>,
    aeron_publisher: AeronPublication,
    protocol: ChannelProtocol,
    sequence_num: u64,
    clock: quanta::Clock,
}

impl MDChannel {
    pub fn new(
        exchange: String,
        channel: String,
        stream_id: i32,
        exchange_symbol: String,
        symbol_id: u16,
        orderbook_sub: bool,
        kline_sub: Vec<String>,
        protocol: ChannelProtocol,
    ) -> Result<Self, Box<dyn error::Error>> {
        Ok(MDChannel {
            exchange,
            channel: channel.clone(),
            stream_id,
            exchange_symbol,
            symbol_id,
            orderbook_sub,
            kline_sub,
            aeron_publisher: utils::create_aeron_publisher(stream_id, channel)?,
            protocol,
            sequence_num: 0,
            clock: Clock::new(),
        })
    }

    pub fn publish(&mut self, body: MDMessage) -> Result<(), Box<dyn std::error::Error>> {
        let packet = Packet::new(body, self.sequence_num);
        let bytes = serialize(&packet)?;
        let res = self
            .aeron_publisher
            .offer(&bytes, Handlers::no_reserved_value_supplier_handler());

        match res {
            r if r >= 0 => {
                // Success - published to at least one subscriber
                self.sequence_num = self.sequence_num.wrapping_add(1);
                Ok(())
            }
            -1 => {
                // NOT_CONNECTED - no active subscribers (might be ok)
                println!("Warning: No subscribers connected");
                self.sequence_num = self.sequence_num.wrapping_add(1);
                Ok(()) // Don't fail, just warn
            }
            err => {
                // Other errors (back pressure, admin action, etc.)
                Err(format!("Publish failed with error code: {}", err).into())
            }
        }
    }
}
