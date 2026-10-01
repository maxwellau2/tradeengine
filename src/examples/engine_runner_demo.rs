use md_feed::{
    strategy::{context::StrategyContext, engine_runner::EngineRunner, strategy::Strategy},
    tui::{TUILogLayer, new_shared_state},
    types::{
        common::{Order, OrderState, OrderType, PassportId, Side, TimeInForce, Venue},
        kline::Kline,
        orderbook::Orderbook,
        trade_server::{CancelOrder, PlaceOrder},
    },
};
use tracing::{debug, info, warn};
use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

struct DummyStrategy {
    passport_id: PassportId,
    venue: Venue,
}

impl Strategy for DummyStrategy {
    fn on_orderbook(&mut self, ob: &Orderbook, ctx: &StrategyContext) {
        debug!(
            "Orderbook Received! best bid {:?}, best ask {:?}",
            ob.bids[0], ob.asks[0]
        );
        for _ in 0..1000 {
            // generate unique cloid via context
            let cloid = ctx.next_cloid(self.venue, self.passport_id);

            let order = PlaceOrder::new(
                ob.symbol,
                ob.venue,
                cloid,
                ob.bids[3].price,
                100.0 / ob.bids[3].price,
                Side::LONG,
                TimeInForce::PO,
                OrderType::LIMIT,
                self.passport_id,
            );

            // check if duplicate exists before placing
            if ctx.has_duplicate(self.venue, self.passport_id, &order) {
                debug!("skipping duplicate order");
                return;
            }

            let res = ctx.place_order(order);
        }
    }

    fn on_kline(&mut self, _kline: &Kline, _ctx: &StrategyContext) {
        // debug!("kline recv {:?}", kline);
    }
    fn on_trade(&mut self, trade: &md_feed::types::trade::Trade, ctx: &StrategyContext) {}
    fn on_start(&mut self, _ctx: &StrategyContext) {}
    fn on_disconnect(&mut self, _ctx: &StrategyContext) {}
    fn on_recon(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_done(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_success(&mut self, _ctx: &StrategyContext) {}
    fn on_recon_fail(&mut self, _ctx: &StrategyContext) {}

    fn on_order_update(&mut self, order: &Order, ctx: &StrategyContext) {
        if order.state == OrderState::NEW {
            let cancel = CancelOrder::new(
                order.symbol,
                order.venue,
                order.client_order_id,
                self.passport_id,
            );
            let _ = ctx.cancel_order(cancel);
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
        venue: Venue::Paradex,
    });

    // create runner from config file
    // note: run() handles tokio runtime internally - do not use #[tokio::main]
    let runner = EngineRunner::from_config_with_tui(config_path, strategy, tui_state)
        .expect("failed to load config");

    // run the engine (blocking)
    // internally spawns io runtime for MD feeds, pins cores, and runs engine loop
    runner.run();
}
