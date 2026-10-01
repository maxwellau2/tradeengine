use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::types::common::{ClientOrderId, Order, OrderState, Side, Symbol};
use crate::types::trade_server::PlaceOrder;

/// pending order with timestamp for timeout detection
#[derive(Debug, Clone)]
pub struct PendingOrder {
    pub order: PlaceOrder,
    pub created_at: Instant,
}

/// tracks order state for central and strategy
/// - confirmed: orders acknowledged by exchange (state subscriber confirmed)
/// - pending_new: orders sent but not yet acked (with timestamp)
/// - executor_acked: executor confirmed but state subscriber hasn't yet (with timestamp)
/// - pending_cancel: cancel requests sent but not yet acked (with timestamp)
#[derive(Debug)]
pub struct OrderTrackingUnit {
    confirmed: HashMap<ClientOrderId, Order>,
    pending_new: HashMap<ClientOrderId, PendingOrder>,
    executor_acked: HashMap<ClientOrderId, PendingOrder>,
    pending_cancel: HashMap<ClientOrderId, Instant>,
}

impl OrderTrackingUnit {
    pub fn new() -> Self {
        Self {
            confirmed: HashMap::new(),
            pending_new: HashMap::new(),
            executor_acked: HashMap::new(),
            pending_cancel: HashMap::new(),
        }
    }

    // --- pending new operations ---

    /// add order to pending (before sending to exchange)
    pub fn add_pending_new(&mut self, order: PlaceOrder) {
        use tracing::info;
        info!(
            cloid = %order.client_order_id,
            symbol = %order.symbol,
            side = ?order.side,
            price = order.price,
            qty = order.qty,
            "[ORDER:PENDING_NEW] order added to pending_new"
        );
        let pending = PendingOrder {
            created_at: Instant::now(),
            order,
        };
        self.pending_new
            .insert(pending.order.client_order_id, pending);
    }

    /// check if order is pending new
    pub fn is_pending_new(&self, cloid: &ClientOrderId) -> bool {
        self.pending_new.contains_key(cloid)
    }

    /// get pending new order
    pub fn get_pending_new(&self, cloid: &ClientOrderId) -> Option<&PlaceOrder> {
        self.pending_new.get(cloid).map(|p| &p.order)
    }

    /// get pending order with timestamp info
    pub fn get_pending_with_time(&self, cloid: &ClientOrderId) -> Option<&PendingOrder> {
        self.pending_new.get(cloid)
    }

    /// remove pending new order (on rejection)
    pub fn remove_pending_new(&mut self, cloid: &ClientOrderId) -> Option<PlaceOrder> {
        self.pending_new.remove(cloid).map(|p| p.order)
    }

    /// get pending orders that have exceeded timeout threshold
    /// returns cloids of timed out orders (both pending_new and executor_acked)
    pub fn get_timed_out_pending(&self, timeout: std::time::Duration) -> Vec<ClientOrderId> {
        let now = Instant::now();
        let mut timed_out: Vec<ClientOrderId> = self
            .pending_new
            .iter()
            .filter(|(_, pending)| now.duration_since(pending.created_at) >= timeout)
            .map(|(cloid, _)| *cloid)
            .collect();

        // also check executor_acked orders
        timed_out.extend(
            self.executor_acked
                .iter()
                .filter(|(_, pending)| now.duration_since(pending.created_at) >= timeout)
                .map(|(cloid, _)| *cloid),
        );

        timed_out
    }

    // --- executor ack operations ---

    /// move order from pending_new to executor_acked
    /// returns false if order was already confirmed by state subscriber
    pub fn mark_executor_acked(&mut self, cloid: &ClientOrderId) -> bool {
        use tracing::{info, warn};

        // if already confirmed by state subscriber, don't change anything
        if self.confirmed.contains_key(cloid) {
            info!(
                cloid = %cloid,
                "[ORDER:EXECUTOR_ACK] already confirmed by state subscriber, ignoring executor ack"
            );
            return false;
        }

        // move from pending_new to executor_acked
        if let Some(pending) = self.pending_new.remove(cloid) {
            info!(
                cloid = %cloid,
                symbol = %pending.order.symbol,
                side = ?pending.order.side,
                price = pending.order.price,
                age_ms = pending.created_at.elapsed().as_millis(),
                "[ORDER:EXECUTOR_ACK] moved from pending_new to executor_acked"
            );
            self.executor_acked.insert(*cloid, pending);
            return true;
        }

        warn!(
            cloid = %cloid,
            "[ORDER:EXECUTOR_ACK] order not found in pending_new (lost order?)"
        );
        false
    }

    /// check if order is executor acked
    pub fn is_executor_acked(&self, cloid: &ClientOrderId) -> bool {
        self.executor_acked.contains_key(cloid)
    }

    /// get executor acked order
    pub fn get_executor_acked(&self, cloid: &ClientOrderId) -> Option<&PlaceOrder> {
        self.executor_acked.get(cloid).map(|p| &p.order)
    }

    /// remove from executor_acked (when state subscriber confirms)
    pub fn remove_executor_acked(&mut self, cloid: &ClientOrderId) -> Option<PlaceOrder> {
        self.executor_acked.remove(cloid).map(|p| p.order)
    }

    // --- pending cancel operations ---

    /// mark order as pending cancel (before sending cancel to exchange)
    /// also updates order state to PENDING_CANCEL in confirmed
    pub fn add_pending_cancel(&mut self, cloid: ClientOrderId) {
        self.pending_cancel.insert(cloid, Instant::now());
        // update order state in confirmed
        if let Some(order) = self.confirmed.get_mut(&cloid) {
            order.state = OrderState::PENDING_CANCEL;
        }
    }

    /// check if order is pending cancel
    pub fn is_pending_cancel(&self, cloid: &ClientOrderId) -> bool {
        self.pending_cancel.contains_key(cloid)
    }

    /// remove order from pending cancel set
    pub fn remove_pending_cancel(&mut self, cloid: &ClientOrderId) {
        self.pending_cancel.remove(cloid);
    }

    /// get pending cancel orders that have exceeded timeout threshold
    pub fn get_timed_out_pending_cancel(&self, timeout: std::time::Duration) -> Vec<ClientOrderId> {
        let now = Instant::now();
        self.pending_cancel
            .iter()
            .filter(|(_, created_at)| now.duration_since(**created_at) >= timeout)
            .map(|(cloid, _)| *cloid)
            .collect()
    }

    // --- confirmed operations ---

    /// upsert confirmed order (from exchange ack)
    pub fn upsert_confirmed(&mut self, order: Order) {
        use tracing::info;

        // track where the order came from for debugging stale orders
        let from_pending_new = self.pending_new.remove(&order.client_order_id).is_some();
        let from_executor_acked = self.executor_acked.remove(&order.client_order_id).is_some();

        // only log transitions, not updates to already confirmed orders
        if from_pending_new || from_executor_acked {
            info!(
                cloid = %order.client_order_id,
                state = ?order.state,
                from = if from_pending_new { "pending_new" } else { "executor_acked" },
                "[ORDER:CONFIRMED] state subscriber confirmed"
            );
        }

        // handle terminal states
        match order.state {
            OrderState::FILLED
            | OrderState::CANCELLED
            | OrderState::REJECTED
            | OrderState::UNKNOWN => {
                self.confirmed.remove(&order.client_order_id);
                self.pending_cancel.remove(&order.client_order_id);
            }
            _ => {
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

    /// get all orders including pending new and executor_acked (for tui display)
    pub fn get_all_with_pending(&self) -> Vec<Order> {
        let mut orders: Vec<Order> = self.confirmed.values().cloned().collect();

        // convert pending_new PlaceOrders to Order with PENDING_NEW state
        for pending in self.pending_new.values() {
            let po = &pending.order;
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

        // convert executor_acked PlaceOrders to Order with EXECUTOR_ACK state
        for pending in self.executor_acked.values() {
            let po = &pending.order;
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
                state: OrderState::EXECUTOR_ACK,
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

    /// check if any order exists for symbol and side (pending_new, executor_acked, or confirmed)
    /// note: orders pending cancel stay in confirmed with PENDING_CANCEL state
    pub fn has_order_for(&self, symbol: &Symbol, side: &Side) -> bool {
        // check pending_new
        for pending in self.pending_new.values() {
            let po = &pending.order;
            if po.symbol == *symbol && po.side == *side {
                return true;
            }
        }
        // check executor_acked
        for pending in self.executor_acked.values() {
            let po = &pending.order;
            if po.symbol == *symbol && po.side == *side {
                return true;
            }
        }
        // check confirmed (includes PENDING_CANCEL orders)
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
        for pending in self.pending_new.values() {
            let po = &pending.order;
            if po.symbol == order.symbol
                && po.side == order.side
                && po.price == order.price
                && po.qty == order.qty
                && po.time_in_force == order.time_in_force
            {
                return true;
            }
        }
        // check executor_acked
        for pending in self.executor_acked.values() {
            let po = &pending.order;
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
        for pending in self.pending_new.values() {
            let po = &pending.order;
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
        self.executor_acked.clear();
        self.pending_cancel.clear();
    }
}

impl Default for OrderTrackingUnit {
    fn default() -> Self {
        Self::new()
    }
}
