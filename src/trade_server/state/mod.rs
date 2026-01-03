// state tracking units for order, position, and balance management
// used by both central (trade server) and strategy (engine)

pub mod balance_tracking;
pub mod order_tracking;
pub mod position_tracking;
pub mod runtime;

pub use balance_tracking::BalanceTrackingUnit;
pub use order_tracking::OrderTrackingUnit;
pub use position_tracking::PositionTrackingUnit;

use crate::types::common::{Balance, Order, Position, Symbol};
use crate::types::trade_server::StateUpdate;

/// unified state manager combining all tracking units
#[derive(Debug)]
pub struct StateManager {
    pub orders: OrderTrackingUnit,
    pub positions: PositionTrackingUnit,
    pub balances: BalanceTrackingUnit,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            orders: OrderTrackingUnit::new(),
            positions: PositionTrackingUnit::new(),
            balances: BalanceTrackingUnit::new(),
        }
    }

    /// apply state update from exchange
    pub fn apply(&mut self, update: StateUpdate) {
        match update {
            StateUpdate::OrderUpdate(order) => {
                self.orders.upsert_confirmed(order);
            }
            StateUpdate::PositionUpdate(position) => {
                self.positions.upsert(position);
            }
            StateUpdate::BalanceUpdate(balance) => {
                self.balances.upsert(balance);
            }
        }
    }

    /// clear all state (e.g., on reconnect)
    pub fn clear(&mut self) {
        self.orders.clear();
        self.positions.clear();
        self.balances.clear();
    }
}

impl Default for StateManager {
    fn default() -> Self {
        Self::new()
    }
}
