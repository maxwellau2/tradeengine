use crate::state_management::order_manager::Order;
use crate::strategy::context::StrategyContext;
use crate::types::{kline::Kline, orderbook::Orderbook};
use async_trait::async_trait;

/// Strategy trait - implement this to create your trading strategy
///
/// All state management is handled by the engine. Strategies receive:
/// - Market data events (orderbook, kline)
/// - Order/position update events
/// - Context with access to current state and order placement
#[async_trait]
pub trait Strategy: Send + Sync {
    /// Called when orderbook update is received
    async fn on_orderbook(&mut self, orderbook: &Orderbook, ctx: &StrategyContext);

    /// Called when kline (candlestick) update is received
    async fn on_kline(&mut self, kline: &Kline, ctx: &StrategyContext);

    /// Called when strategy starts (before market data flows)
    async fn on_start(&mut self, ctx: &StrategyContext);

    /// Called when disconnected from exchange
    async fn on_disconnect(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation process starts
    /// Recon = requesting snapshots from trade server (orders, positions, balances)
    async fn on_recon(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation completes (success or failure)
    async fn on_recon_done(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation succeeds
    async fn on_recon_success(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation fails
    async fn on_recon_fail(&mut self, ctx: &StrategyContext);

    /// Called when order state changes (new, filled, cancelled, etc.)
    /// Engine has already updated internal state before calling this
    async fn on_order_update(&mut self, order: &Order, ctx: &StrategyContext);

    /// Called when order is filled (partially or fully)
    /// Engine has already updated positions/balances before calling this
    async fn on_fill(&mut self, order: &Order, ctx: &StrategyContext);

    /// Called when position is updated
    /// Engine has already updated internal state before calling this
    async fn on_position_update(&mut self, ctx: &StrategyContext);
}

/// Default implementations for lifecycle hooks (can be overridden)
#[async_trait]
pub trait DefaultStrategyHooks: Strategy {
    async fn on_start_default(&mut self, _ctx: &StrategyContext) {}
    async fn on_disconnect_default(&mut self, _ctx: &StrategyContext) {}
    async fn on_recon_default(&mut self, _ctx: &StrategyContext) {}
    async fn on_recon_done_default(&mut self, _ctx: &StrategyContext) {}
    async fn on_recon_success_default(&mut self, _ctx: &StrategyContext) {}
    async fn on_recon_fail_default(&mut self, _ctx: &StrategyContext) {}
    async fn on_order_update_default(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    async fn on_fill_default(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    async fn on_position_update_default(&mut self, _ctx: &StrategyContext) {}
}
