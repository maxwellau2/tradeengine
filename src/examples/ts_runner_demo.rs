use std::env;

use md_feed::ts_connectors::ts_runner::TSRunner;
use md_feed::tui::{TUILogLayer, new_shared_state};
use tracing_subscriber::{filter::EnvFilter, fmt, prelude::*};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: ts_runner_demo <config_path> [--tui]");
        eprintln!("  --tui  enable terminal UI");
        return;
    }

    let config_path = &args[1];
    let use_tui = args.iter().any(|a| a == "--tui");

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let tsrunner = if use_tui {
        // create shared state for TUI log layer
        let tui_state = new_shared_state();

        // use TUI log layer to capture logs
        tracing_subscriber::registry()
            .with(TUILogLayer::new(tui_state.clone()))
            .with(filter)
            .init();

        TSRunner::new_with_tui_state(config_path, tui_state)
    } else {
        // use stdout for logs
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(filter)
            .init();

        TSRunner::new(config_path)
    };

    let tsrunner = match tsrunner {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to create TSRunner: {e:?}");
            return;
        }
    };

    let handles = tsrunner.run();
    for h in handles {
        if h.join().is_err() {
            eprintln!("thread panicked");
        }
    }
}
