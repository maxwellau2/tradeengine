use iceoryx2::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;

// Generic fixed-size string for zero-copy IPC with iceoryx2
#[derive(Clone, Copy, PartialEq, Eq, Hash, ZeroCopySend)]
#[repr(C)]
pub struct FixedString<const N: usize>([u8; N]);

impl<const N: usize> Default for FixedString<N> {
    fn default() -> Self {
        Self([0u8; N])
    }
}

impl<const N: usize> FixedString<N> {
    pub fn new(s: &str) -> Self {
        let mut array = [0u8; N];
        let bytes = s.as_bytes();
        let len = bytes.len().min(N);
        array[..len].copy_from_slice(&bytes[..len]);
        FixedString(array)
    }

    pub fn as_str(&self) -> &str {
        let len = self.0.iter().position(|&x| x == 0).unwrap_or(self.0.len());
        std::str::from_utf8(&self.0[..len]).unwrap_or("")
    }

    pub fn as_bytes(&self) -> &[u8; N] {
        &self.0
    }
}

impl<const N: usize> fmt::Debug for FixedString<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.as_str())
    }
}

impl<const N: usize> fmt::Display for FixedString<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl<const N: usize> Serialize for FixedString<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de, const N: usize> Deserialize<'de> for FixedString<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(FixedString::new(&s))
    }
}

// Type aliases for specific use cases
pub type Symbol = FixedString<32>;
pub type ClientOrderId = FixedString<64>;
pub type PassportId = u16;

// Helper functions for backward compatibility
pub fn symbol_from_str(s: &str) -> Symbol {
    Symbol::new(s)
}

pub fn symbol_to_str(symbol: &Symbol) -> &str {
    symbol.as_str()
}

pub fn client_order_id_from_str(s: &str) -> ClientOrderId {
    ClientOrderId::new(s)
}

pub fn client_order_id_from_u32(n: u32) -> ClientOrderId {
    ClientOrderId::new(n.to_string().as_str())
}

pub fn client_order_id_to_str(client_order_id: &ClientOrderId) -> &str {
    client_order_id.as_str()
}

#[derive(
    Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Default, ZeroCopySend,
)]
#[repr(C)]
pub enum Venue {
    #[default]
    Hyperliquid,
    Binance,
    Lighter,
    Paradex,
    Okx,
}

impl Venue {
    pub fn from_string(s: &String) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "hyperliquid" => Some(Venue::Hyperliquid),
            "binance" => Some(Venue::Binance),
            "lighter" => Some(Venue::Lighter),
            "paradex" => Some(Venue::Paradex),
            "okx" => Some(Venue::Okx),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KlineInterval {
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
    UNKNOWN,
}

impl KlineInterval {
    pub fn to_string(&self) -> String {
        let res = match self {
            KlineInterval::M1 => "1m",
            KlineInterval::M3 => "3m",
            KlineInterval::M5 => "5m",
            KlineInterval::M15 => "15m",
            KlineInterval::M30 => "30m",
            KlineInterval::H1 => "1h",
            KlineInterval::H2 => "2h",
            KlineInterval::H4 => "4h",
            KlineInterval::H8 => "8h",
            KlineInterval::D1 => "1d",
            KlineInterval::W1 => "1w",
            KlineInterval::MTH1 => "1mth",
            KlineInterval::UNKNOWN => "UNKNOWN",
        };
        return res.to_string();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default, ZeroCopySend)]
#[repr(C)]
pub enum Side {
    LONG,
    SHORT,
    #[default]
    UNKNOWN,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ZeroCopySend)]
#[repr(C)]
pub enum OrderType {
    LIMIT,
    MARKET,
    #[default]
    UNKNOWN,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ZeroCopySend)]
#[repr(C)]
pub enum TimeInForce {
    GTC,
    PO,
    FOK,
    IOC,
    #[default]
    UNKNOWN,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ZeroCopySend)]
#[repr(C)]
pub enum OrderState {
    PENDING_NEW,
    EXECUTOR_ACK, // executor confirmed, awaiting state subscriber confirmation
    NEW,
    PARTIALLY_FILLED,
    PENDING_CANCEL,
    // terminal states
    FILLED,
    CANCELLED,
    REJECTED,
    #[default]
    UNKNOWN,
}

#[derive(Debug, Clone, Copy, Default, ZeroCopySend)]
#[repr(C)]
pub struct Order {
    pub symbol: Symbol,
    pub venue: Venue,
    pub side: Side,
    pub client_order_id: ClientOrderId,
    pub qty: f64,
    pub filled_qty: f64,
    pub price: f64,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub state: OrderState,
}

impl Order {}

#[derive(Debug, Clone, Copy, Default, ZeroCopySend)]
#[repr(C)]
pub struct Balance {
    pub coin: Symbol,
    pub venue: Venue,
    pub qty: f64,
}

#[derive(Debug, Clone, Copy, Default, ZeroCopySend)]
#[repr(C)]
pub struct Position {
    pub symbol: Symbol,
    pub venue: Venue,
    pub side: Side,
    pub qty: f64,
    pub position_value: f64,
    pub unrealised_pnl: f64,
    pub margin: f64,
}
