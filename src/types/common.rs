use serde::{Deserialize, Serialize};

pub type Symbol = String;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Venue{
    Hyperliquid,
    Binance,
    Paradex,
    Okx,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum KlineInterval{
    // minutes
    M1,
    M3,
    M5,
    M15,
    M30,
    // hours
    H1,
    H2,
    H4,
    H8,
    // days
    D1,
    W1,
    // months
    MTH1,
}