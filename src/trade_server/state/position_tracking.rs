use std::collections::HashMap;

use crate::types::common::{Position, Side, Symbol, Venue};

/// composite key for position: (symbol, venue)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PositionKey {
    symbol: Symbol,
    venue: Venue,
}

impl PositionKey {
    fn new(symbol: Symbol, venue: Venue) -> Self {
        Self { symbol, venue }
    }

    fn from_position(position: &Position) -> Self {
        Self::new(position.symbol, position.venue)
    }
}

/// tracks position state per (symbol, venue)
/// positions with qty=0 are considered closed and removed
#[derive(Debug)]
pub struct PositionTrackingUnit {
    positions: HashMap<PositionKey, Position>,
}

impl PositionTrackingUnit {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
        }
    }

    /// upsert position, removes if qty is zero (closed)
    pub fn upsert(&mut self, position: Position) {
        let key = PositionKey::from_position(&position);
        if position.qty == 0.0 {
            self.positions.remove(&key);
        } else {
            self.positions.insert(key, position);
        }
    }

    /// get position for (symbol, venue)
    pub fn get(&self, symbol: &Symbol, venue: Venue) -> Option<&Position> {
        self.positions.get(&PositionKey::new(*symbol, venue))
    }

    /// check if we have a position in (symbol, venue)
    pub fn has_position(&self, symbol: &Symbol, venue: Venue) -> bool {
        self.positions
            .contains_key(&PositionKey::new(*symbol, venue))
    }

    /// remove position (mark as closed)
    pub fn remove(&mut self, symbol: &Symbol, venue: Venue) -> Option<Position> {
        self.positions.remove(&PositionKey::new(*symbol, venue))
    }

    /// get net position size (positive = long, negative = short)
    pub fn net_size(&self, symbol: &Symbol, venue: Venue) -> f64 {
        self.positions
            .get(&PositionKey::new(*symbol, venue))
            .map_or(0.0, |p| match p.side {
                Side::LONG => p.qty,
                Side::SHORT => -p.qty,
                Side::UNKNOWN => 0.0,
            })
    }

    /// all open positions
    pub fn all(&self) -> impl Iterator<Item = &Position> {
        self.positions.values()
    }

    /// get all positions as vec (for tui)
    pub fn get_all(&self) -> Vec<Position> {
        self.positions.values().cloned().collect()
    }

    /// all (symbol, venue) pairs with open positions
    pub fn keys(&self) -> impl Iterator<Item = (Symbol, Venue)> + '_ {
        self.positions.keys().map(|k| (k.symbol, k.venue))
    }

    /// count of open positions
    pub fn count(&self) -> usize {
        self.positions.len()
    }

    /// total unrealised pnl across all positions
    pub fn total_unrealised_pnl(&self) -> f64 {
        self.positions.values().map(|p| p.unrealised_pnl).sum()
    }

    /// total margin used across all positions
    pub fn total_margin(&self) -> f64 {
        self.positions.values().map(|p| p.margin).sum()
    }

    /// total position value across all positions
    pub fn total_position_value(&self) -> f64 {
        self.positions.values().map(|p| p.position_value).sum()
    }

    /// clear all positions (e.g., on reconnect for full resync)
    pub fn clear(&mut self) {
        self.positions.clear();
    }

    /// detect closed positions by comparing with new snapshot for a specific venue
    /// returns (symbol, venue) pairs that were in old state but not in new
    pub fn detect_closed(&self, new_keys: &[(Symbol, Venue)]) -> Vec<(Symbol, Venue)> {
        let new_set: std::collections::HashSet<_> = new_keys.iter().collect();
        self.positions
            .keys()
            .filter(|k| !new_set.contains(&(k.symbol, k.venue)))
            .map(|k| (k.symbol, k.venue))
            .collect()
    }
}

impl Default for PositionTrackingUnit {
    fn default() -> Self {
        Self::new()
    }
}
