use std::collections::HashMap;

use crate::types::common::{Balance, Symbol, Venue};

/// composite key for balance: (coin, venue)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BalanceKey {
    coin: Symbol,
    venue: Venue,
}

impl BalanceKey {
    fn new(coin: Symbol, venue: Venue) -> Self {
        Self { coin, venue }
    }

    fn from_balance(balance: &Balance) -> Self {
        Self::new(balance.coin, balance.venue)
    }
}

/// tracks balance state per (coin, venue)
#[derive(Debug)]
pub struct BalanceTrackingUnit {
    balances: HashMap<BalanceKey, Balance>,
}

impl BalanceTrackingUnit {
    pub fn new() -> Self {
        Self {
            balances: HashMap::new(),
        }
    }

    /// upsert balance for (coin, venue)
    pub fn upsert(&mut self, balance: Balance) {
        self.balances
            .insert(BalanceKey::from_balance(&balance), balance);
    }

    /// get balance for (coin, venue)
    pub fn get(&self, coin: &Symbol, venue: Venue) -> Option<&Balance> {
        self.balances.get(&BalanceKey::new(*coin, venue))
    }

    /// get balance qty for (coin, venue), returns 0 if not found
    pub fn qty(&self, coin: &Symbol, venue: Venue) -> f64 {
        self.balances
            .get(&BalanceKey::new(*coin, venue))
            .map_or(0.0, |b| b.qty)
    }

    /// check if we have balance for (coin, venue)
    pub fn has(&self, coin: &Symbol, venue: Venue) -> bool {
        self.balances.contains_key(&BalanceKey::new(*coin, venue))
    }

    /// remove balance for (coin, venue)
    pub fn remove(&mut self, coin: &Symbol, venue: Venue) -> Option<Balance> {
        self.balances.remove(&BalanceKey::new(*coin, venue))
    }

    /// all balances
    pub fn all(&self) -> impl Iterator<Item = &Balance> {
        self.balances.values()
    }

    /// get all balances as vec (for tui)
    pub fn get_all(&self) -> Vec<Balance> {
        self.balances.values().cloned().collect()
    }

    /// all (coin, venue) pairs with balance
    pub fn keys(&self) -> impl Iterator<Item = (Symbol, Venue)> + '_ {
        self.balances.keys().map(|k| (k.coin, k.venue))
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
