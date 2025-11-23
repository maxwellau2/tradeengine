use crate::types::common::{KlineInterval, Symbol, Venue};

pub struct Kline{
    pub symbol: Symbol,
    pub venue: Venue,
    pub interval: KlineInterval,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_closed: bool,
}