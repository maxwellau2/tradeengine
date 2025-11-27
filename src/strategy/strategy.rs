use crate::strategy::context::StrategyContext;
use crate::types::common::Order;
use crate::types::{kline::Kline, orderbook::Orderbook};

/// Strategy trait - implement this to create your trading strategy
///
/// **Single-threaded for HFT performance:**
/// All methods are synchronous (no async) to avoid context switch overhead.
/// The engine calls these methods directly from its main loop.
///
/// All state management is handled by the engine. Strategies receive:
/// - Market data events (orderbook, kline)
/// - Order/position update events
/// - Context with access to current state and order placement
pub trait Strategy {
    /// Called when orderbook update is received
    fn on_orderbook(&mut self, orderbook: &Orderbook, ctx: &StrategyContext);

    /// Called when kline (candlestick) update is received
    fn on_kline(&mut self, kline: &Kline, ctx: &StrategyContext);

    /// Called when strategy starts (before market data flows)
    fn on_start(&mut self, ctx: &StrategyContext);

    /// Called when disconnected from exchange
    fn on_disconnect(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation process starts
    /// Recon = requesting snapshots from trade server (orders, positions, balances)
    fn on_recon(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation completes (success or failure)
    fn on_recon_done(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation succeeds
    fn on_recon_success(&mut self, ctx: &StrategyContext);

    /// Called when reconciliation fails
    fn on_recon_fail(&mut self, ctx: &StrategyContext);

    /// Called when order state changes (new, filled, cancelled, etc.)
    /// Engine has already updated internal state before calling this
    fn on_order_update(&mut self, order: &Order, ctx: &StrategyContext);

    /// Called when order is filled (partially or fully)
    /// Engine has already updated positions/balances before calling this
    fn on_fill(&mut self, order: &Order, ctx: &StrategyContext);

    /// Called when position is updated
    /// Engine has already updated internal state before calling this
    fn on_position_update(&mut self, ctx: &StrategyContext);
}
