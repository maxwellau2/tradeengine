use std::collections::{HashMap, HashSet};

use crate::types::common::{ClientOrderId, Order, OrderState, Side, Symbol};
use crate::types::trade_server::PlaceOrder;

/// tracks order state for central and strategy
/// - confirmed: orders acknowledged by exchange
/// - pending_new: orders sent but not yet acked
/// - pending_cancel: cancel requests sent but not yet acked
#[derive(Debug)]
pub struct OrderTrackingUnit {
    confirmed: HashMap<ClientOrderId, Order>,
    pending_new: HashMap<ClientOrderId, PlaceOrder>,
    pending_cancel: HashSet<ClientOrderId>,
}

impl OrderTrackingUnit {
    pub fn new() -> Self {
        Self {
            confirmed: HashMap::new(),
            pending_new: HashMap::new(),
            pending_cancel: HashSet::new(),
        }
    }

    // --- pending new operations ---

    /// add order to pending (before sending to exchange)
    pub fn add_pending_new(&mut self, order: PlaceOrder) {
        self.pending_new.insert(order.client_order_id, order);
    }

    /// check if order is pending new
    pub fn is_pending_new(&self, cloid: &ClientOrderId) -> bool {
        self.pending_new.contains_key(cloid)
    }

    /// get pending new order
    pub fn get_pending_new(&self, cloid: &ClientOrderId) -> Option<&PlaceOrder> {
        self.pending_new.get(cloid)
    }

    /// remove pending new order (on rejection)
    pub fn remove_pending_new(&mut self, cloid: &ClientOrderId) -> Option<PlaceOrder> {
        self.pending_new.remove(cloid)
    }

    // --- pending cancel operations ---

    /// mark order as pending cancel (before sending cancel to exchange)
    pub fn add_pending_cancel(&mut self, cloid: ClientOrderId) {
        self.pending_cancel.insert(cloid);
    }

    /// check if order is pending cancel
    pub fn is_pending_cancel(&self, cloid: &ClientOrderId) -> bool {
        self.pending_cancel.contains(cloid)
    }

    // --- confirmed operations ---

    /// upsert confirmed order (from exchange ack)
    pub fn upsert_confirmed(&mut self, order: Order) {
        use tracing::debug;

        // remove from pending_new by cloid match
        let was_pending = self.pending_new.remove(&order.client_order_id);
        if was_pending.is_some() {
            debug!(
                cloid = %order.client_order_id,
                "removed from pending_new"
            );
        }

        // handle terminal states
        match order.state {
            OrderState::FILLED
            | OrderState::CANCELLED
            | OrderState::REJECTED
            | OrderState::UNKNOWN => {
                debug!(
                    cloid = %order.client_order_id,
                    state = ?order.state,
                    "removing order (terminal state)"
                );
                self.confirmed.remove(&order.client_order_id);
                self.pending_cancel.remove(&order.client_order_id);
            }
            _ => {
                debug!(
                    cloid = %order.client_order_id,
                    state = ?order.state,
                    "upserting order"
                );
                self.confirmed.insert(order.client_order_id, order);
            }
        }
    }

    /// get confirmed order
    pub fn get_confirmed(&self, cloid: &ClientOrderId) -> Option<&Order> {
        self.confirmed.get(cloid)
    }

    /// remove confirmed order
    pub fn remove_confirmed(&mut self, cloid: &ClientOrderId) -> Option<Order> {
        self.pending_cancel.remove(cloid);
        self.confirmed.remove(cloid)
    }

    // --- queries ---

    /// all confirmed (resting) orders
    pub fn all_confirmed(&self) -> impl Iterator<Item = &Order> {
        self.confirmed.values()
    }

    /// get all confirmed orders as vec (for tui)
    pub fn get_all_confirmed(&self) -> Vec<Order> {
        self.confirmed.values().cloned().collect()
    }

    /// get all orders including pending new (for tui display)
    pub fn get_all_with_pending(&self) -> Vec<Order> {
        let mut orders: Vec<Order> = self.confirmed.values().cloned().collect();

        // convert pending_new PlaceOrders to Order with PENDING_NEW state
        for po in self.pending_new.values() {
            orders.push(Order {
                client_order_id: po.client_order_id,
                symbol: po.symbol,
                venue: po.venue,
                side: po.side,
                price: po.price,
                qty: po.qty,
                filled_qty: 0.0,
                order_type: po.order_type,
                time_in_force: po.time_in_force,
                state: OrderState::PENDING_NEW,
            });
        }

        orders
    }

    /// count of confirmed orders
    pub fn confirmed_count(&self) -> usize {
        self.confirmed.len()
    }

    /// count of pending new orders
    pub fn pending_new_count(&self) -> usize {
        self.pending_new.len()
    }

    /// count of pending cancel orders
    pub fn pending_cancel_count(&self) -> usize {
        self.pending_cancel.len()
    }

    /// total inflight orders (confirmed + pending_new)
    pub fn total_open(&self) -> usize {
        self.confirmed.len() + self.pending_new.len()
    }

    /// check if any order exists for symbol and side (pending or confirmed)
    pub fn has_order_for(&self, symbol: &Symbol, side: &Side) -> bool {
        // check pending_new
        for po in self.pending_new.values() {
            if po.symbol == *symbol && po.side == *side {
                return true;
            }
        }
        // check confirmed
        for order in self.confirmed.values() {
            if order.symbol == *symbol && order.side == *side {
                return true;
            }
        }
        false
    }

    /// check if duplicate order exists (same symbol, side, price, qty, tif)
    pub fn has_duplicate(&self, order: &PlaceOrder) -> bool {
        // check pending_new
        for po in self.pending_new.values() {
            if po.symbol == order.symbol
                && po.side == order.side
                && po.price == order.price
                && po.qty == order.qty
                && po.time_in_force == order.time_in_force
            {
                return true;
            }
        }
        // check confirmed
        for o in self.confirmed.values() {
            if o.symbol == order.symbol
                && o.side == order.side
                && o.price == order.price
                && o.qty == order.qty
                && o.time_in_force == order.time_in_force
            {
                return true;
            }
        }
        false
    }

    /// get all orders for symbol (pending + confirmed)
    pub fn get_orders_for_symbol(&self, symbol: &Symbol) -> Vec<Order> {
        let mut orders = Vec::new();
        for po in self.pending_new.values() {
            if po.symbol == *symbol {
                orders.push(Order {
                    client_order_id: po.client_order_id,
                    symbol: po.symbol,
                    venue: po.venue,
                    side: po.side,
                    price: po.price,
                    qty: po.qty,
                    filled_qty: 0.0,
                    order_type: po.order_type,
                    time_in_force: po.time_in_force,
                    state: OrderState::PENDING_NEW,
                });
            }
        }
        for order in self.confirmed.values() {
            if order.symbol == *symbol {
                orders.push(order.clone());
            }
        }
        orders
    }

    /// clear all state (e.g., on reconnect)
    pub fn clear(&mut self) {
        self.confirmed.clear();
        self.pending_new.clear();
        self.pending_cancel.clear();
    }
}

impl Default for OrderTrackingUnit {
    fn default() -> Self {
        Self::new()
    }
}
