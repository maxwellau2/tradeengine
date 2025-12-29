use std::collections::HashMap;

use crate::types::common::{Balance, Symbol};

/// tracks balance state per coin
pub struct BalanceTrackingUnit {
    balances: HashMap<Symbol, Balance>,
}

impl BalanceTrackingUnit {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
        }
    }

    /// upsert balance for coin
    pub fn upsert(&mut self, balance: Balance) {
        self.balances.insert(balance.coin, balance);
    }

    /// get balance for coin
    pub fn get(&self, coin: &Symbol) -> Option<&Balance> {
        self.balances.get(coin)
    }

    /// get balance qty for coin, returns 0 if not found
    pub fn qty(&self, coin: &Symbol) -> f64 {
        self.balances.get(coin).map_or(0.0, |b| b.qty)
    }

    /// check if we have balance for coin
    pub fn has(&self, coin: &Symbol) -> bool {
        self.balances.contains_key(coin)
    }

    /// remove balance for coin
    pub fn remove(&mut self, coin: &Symbol) -> Option<Balance> {
        self.balances.remove(coin)
    }

    /// all balances
    pub fn all(&self) -> impl Iterator<Item = &Balance> {
        self.balances.values()
    }

    /// get all balances as vec (for tui)
    pub fn get_all(&self) -> Vec<Balance> {
        self.balances.values().cloned().collect()
    }

    /// all coins with balance
    pub fn coins(&self) -> impl Iterator<Item = &Symbol> {
        self.balances.keys()
    }

    /// count of tracked coins
    pub fn count(&self) -> usize {
        self.balances.len()
    }

    /// total balance value (sum of all coin qtys - only meaningful if same denomination)
    pub fn total(&self) -> f64 {
        self.balances.values().map(|b| b.qty).sum()
    }

    /// clear all balances (e.g., on reconnect for full resync)
    pub fn clear(&mut self) {
        self.balances.clear();
    }
}

impl Default for BalanceTrackingUnit {
    fn default() -> Self {
        Self::new()
    }
}
