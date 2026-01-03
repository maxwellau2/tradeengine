use serde::Deserialize;
use serde_json::Value;

/// wrapper to handle all ws messages, including non-channel messages like subscription confirmations
#[derive(Deserialize)]
#[serde(untagged)]
pub enum HyperliquidWsMessage<'a> {
    /// actual market data with channel/data structure
    #[serde(borrow)]
    Channel(ChannelMessage<'a>),
    /// subscription confirmations, errors, or other control messages
    Control(Value),
}

/// market data messages that have channel/data structure
#[derive(Deserialize)]
#[serde(tag = "channel", content = "data")]
pub enum ChannelMessage<'a> {
    #[serde(rename = "l2Book", borrow)]
    Orderbook(HyperliquidRawOrderbook<'a>),
    #[serde(rename = "candle", borrow)]
    Kline(HyperliquidRawKline<'a>),
    #[serde(other)]
    Unknown,
}
// step 2: typed structs for each channel
#[derive(Deserialize)]
pub struct HyperliquidRawOrderbook<'a> {
    pub coin: &'a str,
    pub time: u64,
    #[serde(borrow)]
    pub levels: [Vec<HyperliquidRawLevel<'a>>; 2],
}

#[derive(Deserialize)]
pub struct HyperliquidRawLevel<'a> {
    pub px: &'a str,
    pub sz: &'a str,
}

#[derive(Deserialize)]
pub struct HyperliquidRawKline<'a> {
    pub s: &'a str, // symbol
    pub i: &'a str, // interval
    pub t: u64,     // open time
    #[serde(rename = "T")]
    pub t_close: u64, // close time
    pub o: &'a str, // open
    pub h: &'a str, // high
    pub l: &'a str, // low
    pub c: &'a str, // close
    pub v: &'a str, // volume
}

// simd-json optimized types - owned strings for zero-copy from mutable buffer
// simd-json modifies the input buffer in place, so we use owned Box<str> which
// it can create directly without extra allocations

/// simd-json version of channel message
#[derive(Deserialize)]
#[serde(tag = "channel")]
pub enum SimdChannelMessage {
    #[serde(rename = "l2Book")]
    Orderbook { data: SimdRawOrderbook },
    #[serde(rename = "candle")]
    Kline { data: SimdRawKline },
    #[serde(rename = "subscriptionResponse")]
    SubscriptionResponse { data: serde_json::Value },
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
pub struct SimdRawOrderbook {
    pub coin: Box<str>,
    pub time: u64,
    pub levels: [Vec<SimdRawLevel>; 2],
}

#[derive(Deserialize)]
pub struct SimdRawLevel {
    pub px: Box<str>,
    pub sz: Box<str>,
}

#[derive(Deserialize)]
pub struct SimdRawKline {
    pub s: Box<str>, // symbol
    pub i: Box<str>, // interval
    pub t: u64,      // open time
    #[serde(rename = "T")]
    pub t_close: u64, // close time
    pub o: Box<str>, // open
    pub h: Box<str>, // high
    pub l: Box<str>, // low
    pub c: Box<str>, // close
    pub v: Box<str>, // volume
}
