use std::collections::HashMap;

use crate::types::common::{Position, Side, Symbol, Venue};

/// tracks position state per symbol
/// positions with qty=0 are considered closed and removed
#[derive(Debug)]
pub struct PositionTrackingUnit {
    positions: HashMap<Symbol, Position>,
}

impl PositionTrackingUnit {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
        }
    }

    /// upsert position, removes if qty is zero (closed)
    pub fn upsert(&mut self, position: Position) {
        if position.qty == 0.0 {
            self.positions.remove(&position.symbol);
        } else {
            self.positions.insert(position.symbol, position);
        }
    }

    /// get position for symbol
    pub fn get(&self, symbol: &Symbol) -> Option<&Position> {
        self.positions.get(symbol)
    }

    /// check if we have a position in symbol
    pub fn has_position(&self, symbol: &Symbol) -> bool {
        self.positions.contains_key(symbol)
    }

    /// remove position (mark as closed)
    pub fn remove(&mut self, symbol: &Symbol) -> Option<Position> {
        self.positions.remove(symbol)
    }

    /// get net position size (positive = long, negative = short)
    pub fn net_size(&self, symbol: &Symbol) -> f64 {
        self.positions.get(symbol).map_or(0.0, |p| match p.side {
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

    /// all symbols with open positions
    pub fn symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.positions.keys()
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

    /// detect closed positions by comparing with new snapshot
    /// returns symbols that were in old state but not in new
    pub fn detect_closed(&self, new_symbols: &[Symbol]) -> Vec<Symbol> {
        let new_set: std::collections::HashSet<_> = new_symbols.iter().collect();
        self.positions
            .keys()
            .filter(|s| !new_set.contains(s))
            .copied()
            .collect()
    }
}

impl Default for PositionTrackingUnit {
    fn default() -> Self {
        Self::new()
    }
}
