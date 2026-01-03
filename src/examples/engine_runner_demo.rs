use std::sync::atomic::AtomicU8;

use md_feed::{
    strategy::{context::StrategyContext, engine_runner::EngineRunner, strategy::Strategy},
    tui::{TUILogLayer, new_shared_state},
    types::{
        common::{
            Order, OrderState, OrderType, PassportId, Side, TimeInForce, client_order_id_from_u8,
        },
        kline::Kline,
        orderbook::Orderbook,
        trade_server::{CancelOrder, PlaceOrder},
    },
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

struct DummyStrategy {
    passport_id: PassportId,
    cloid: AtomicU8,
}

impl DummyStrategy {
    fn next_cloid(&mut self) -> u8 {
        // increment and wrap, but skip 0 (hyperliquid forbids cloid 0)
        loop {
            let prev = self
                .cloid
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let next = prev.wrapping_add(1);
            if next != 0 {
                return next;
            }
            // if we hit 0, loop again to get 1
        }
    }
}

impl Strategy for DummyStrategy {
    fn on_orderbook(&mut self, ob: &Orderbook, _ctx: &StrategyContext) {
        debug!(
            "Orderbook Received! best bid {:?}, best ask {:?}",
            ob.bids[0], ob.asks[0]
        );
        let id = self.next_cloid();
        let order = PlaceOrder::new(
            ob.symbol,
            ob.venue,
            client_order_id_from_u8(id),
            ob.bids[3].price,
            100.0 / ob.bids[3].price,
            Side::LONG,
            TimeInForce::PO,
            OrderType::LIMIT,
            self.passport_id,
        );

        // check if duplicate exists before placing
        if _ctx.has_duplicate(&order) {
            debug!("skipping duplicate order");
            return;
        }
        let res = _ctx.place_order(order);
        match res {
            Ok(val) => {
                info!("Placed order {:?}", val);
            }
            Err(e) => {
                warn!("Error {:?}", e);
            }
        }
    }
    fn on_kline(&mut self, kline: &Kline, _ctx: &StrategyContext) {
        // debug!("kline recv {:?}", kline);
        // log all klines, mark closed ones
        // if kline.is_closed {
        //     info!(
        //         "[CLOSED] Kline: {:?} {:?} close={:?}",
        //         &kline.symbol, kline.interval, kline.close
        //     );
        //     let order = PlaceOrder {
        //         symbol: kline.symbol,
        //         venue: kline.venue,
        //         client_order_id: client_order_id_from_u8(self.next_cloid()),
        //         price: kline.low,
        //         qty: 12.0 / kline.low,
        //         side: Side::LONG,
        //         time_in_force: TimeInForce::PO,
        //         order_type: OrderType::LIMIT,
        //         passport_id: self.passport_id.clone(),
        //     };
        //     if !_ctx.has_duplicate(&order) {
        //         let res = _ctx.place_order(order);
        //         match res {
        //             Ok(_) => {}
        //             Err(e) => error!("{e}"),
        //         }
        //     }
        // } else {
        //     debug!(
        //         "[OPEN] Kline: {:?} {:?} close={} T={:?}",
        //         &kline.symbol, kline.interval, kline.close, kline.close_time
        //     );
        // }
    }
    fn on_start(&mut self, _ctx: &StrategyContext) {}
    fn on_disconnect(&mut self, _ctx: &StrategyContext) {}
    fn on_recon(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_done(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_success(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_fail(&mut self, _ctx: &StrategyContext) {}
    fn on_order_update(&mut self, _order: &Order, _ctx: &StrategyContext) {
        if _order.state == OrderState::NEW {
            let cancel = CancelOrder::new(
                _order.symbol,
                _order.venue,
                _order.client_order_id,
                self.passport_id.clone(),
            );
            let res = _ctx.cancel_order(cancel);
        }
    }
    fn on_fill(&mut self, _order: &Order, _ctx: &StrategyContext) {}
    fn on_position_update(&mut self, _ctx: &StrategyContext) {}
}

pub fn main() {
    let args: Vec<String> = std::env::args().collect();

    // check for --tui flag
    let use_tui = args.iter().any(|a| a == "--tui");

    // get config path (first non-flag argument after program name)
    let config_path = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .map(|s| s.as_str())
        .unwrap_or_else(|| {
            eprintln!("usage: {} <config_path> [--tui]", args[0]);
            eprintln!("  --tui  enable terminal UI");
            std::process::exit(1);
        });

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // setup logging based on tui flag
    let tui_state = if use_tui {
        let state = new_shared_state();

        // use TUI log layer to capture logs
        tracing_subscriber::registry()
            .with(TUILogLayer::new(state.clone()))
            .with(filter)
            .init();

        Some(state)
    } else {
        // use stdout for logs
        tracing_subscriber::registry()
            .with(fmt::layer().with_line_number(true))
            .with(filter)
            .init();

        None
    };

    // create your strategy
    let strategy = Box::new(DummyStrategy {
        passport_id: 1234,
        cloid: AtomicU8::new(0),
    });

    // create runner from config file
    // note: run() handles tokio runtime internally - do not use #[tokio::main]
    let runner = EngineRunner::from_config_with_tui(config_path, strategy, tui_state)
        .expect("failed to load config");

    // run the engine (blocking)
    // internally spawns io runtime for MD feeds, pins cores, and runs engine loop
    runner.run();
}
