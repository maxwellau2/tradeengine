// cli tool to test trade server
// acts as an on-demand strategy to place, cancel, and replace orders

use md_feed::strategy::context::{OrderGateway, OrderGatewaySend};
use md_feed::strategy::engine::TradeServerGateway;
use md_feed::ts_protocol::iceoryx2_wrapper::EngineIceoryx2Wrapper;
use md_feed::types::clock::timestamp_nanos;
use md_feed::types::common::{
    ClientOrderId, OrderType, PassportId, Side, Symbol, TimeInForce, Venue,
};
use md_feed::types::trade_server::{
    CancelOrder, EngineTSMessage, EngineTSMessageType, PlaceOrder, ReplaceOrder,
};
use std::env;
use std::io::{self, Write};

fn print_usage() {
    println!("Trade Server CLI - Interactive Order Management");
    println!();
    println!("Commands:");
    println!("  place <symbol> <side> <price> <qty>       - Place a limit order");
    println!("  cancel <symbol> <cloid>                   - Cancel an order by client order id");
    println!("  replace <symbol> <cloid> <price> <qty>    - Replace an order");
    println!("  help                                      - Show this help");
    println!("  quit                                      - Exit the CLI");
    println!();
    println!("Examples:");
    println!("  place BTC buy 50000.0 0.1");
    println!("  cancel BTC cli_1");
    println!("  replace BTC cli_1 51000.0 0.2");
}

fn parse_side(s: &str) -> Option<Side> {
    match s.to_lowercase().as_str() {
        "buy" | "long" | "b" => Some(Side::LONG),
        "sell" | "short" | "s" => Some(Side::SHORT),
        _ => None,
    }
}

fn parse_venue(s: &str) -> Option<Venue> {
    match s.to_lowercase().as_str() {
        "hyperliquid" | "hl" => Some(Venue::Hyperliquid),
        "binance" | "bn" => Some(Venue::Binance),
        "paradex" | "pd" => Some(Venue::Paradex),
        "okx" => Some(Venue::Okx),
        _ => None,
    }
}

struct CliContext<S: OrderGatewaySend> {
    gateway: TradeServerGateway<S>,
    venue: Venue,
    passport_id: PassportId,
    order_counter: u64,
}

impl<S: OrderGatewaySend> CliContext<S> {
    fn new(sender: S, venue: Venue, passport_id: PassportId) -> Self {
        Self {
            gateway: TradeServerGateway::new(sender),
            venue,
            passport_id,
            order_counter: 0,
        }
    }

    fn next_cloid(&mut self) -> ClientOrderId {
        self.order_counter += 1;
        ClientOrderId::new(&format!("{}", self.order_counter))
    }

    fn handle_place(&mut self, args: &[&str]) {
        if args.len() < 4 {
            println!("Usage: place <symbol> <side> <price> <qty>");
            return;
        }

        let symbol = Symbol::new(args[0]);
        let side = match parse_side(args[1]) {
            Some(s) => s,
            None => {
                println!("Invalid side: {}. Use buy/sell or long/short", args[1]);
                return;
            }
        };
        let price: f64 = match args[2].parse() {
            Ok(p) => p,
            Err(_) => {
                println!("Invalid price: {}", args[2]);
                return;
            }
        };
        let qty: f64 = match args[3].parse() {
            Ok(q) => q,
            Err(_) => {
                println!("Invalid qty: {}", args[3]);
                return;
            }
        };

        let cloid = self.next_cloid();
        let order = PlaceOrder::new(
            symbol,
            self.venue,
            cloid,
            price,
            qty,
            side,
            TimeInForce::GTC,
            OrderType::LIMIT,
            self.passport_id,
        );

        match self.gateway.place_order(order) {
            Ok(id) => println!("Order placed: {}", id),
            Err(e) => println!("Failed to place order: {:?}", e),
        }
    }

    fn handle_cancel(&mut self, args: &[&str]) {
        if args.len() < 2 {
            println!("Usage: cancel <symbol> <cloid>");
            return;
        }

        let symbol = Symbol::new(args[0]);
        let cloid = ClientOrderId::new(args[1]);
        let cancel = CancelOrder::new(symbol, self.venue, cloid, self.passport_id);

        match self.gateway.cancel_order(cancel) {
            Ok(_) => println!("Cancel request sent for: {}", args[1]),
            Err(e) => println!("Failed to cancel order: {:?}", e),
        }
    }

    fn handle_replace(&mut self, args: &[&str]) {
        if args.len() < 4 {
            println!("Usage: replace <symbol> <cloid> <price> <qty>");
            return;
        }

        let symbol = Symbol::new(args[0]);
        let cloid = ClientOrderId::new(args[1]);
        let price: f64 = match args[2].parse() {
            Ok(p) => p,
            Err(_) => {
                println!("Invalid price: {}", args[2]);
                return;
            }
        };
        let qty: f64 = match args[3].parse() {
            Ok(q) => q,
            Err(_) => {
                println!("Invalid qty: {}", args[3]);
                return;
            }
        };

        let replace = ReplaceOrder::new(
            symbol,
            self.venue,
            cloid,
            price,
            qty,
            Side::LONG, // side preserved from original
            TimeInForce::GTC,
            OrderType::LIMIT,
            self.passport_id,
        );

        match self.gateway.replace_order(replace) {
            Ok(_) => println!("Replace request sent for: {}", args[1]),
            Err(e) => println!("Failed to replace order: {:?}", e),
        }
    }

    fn handle_command(&mut self, line: &str) -> bool {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            return true;
        }

        match parts[0].to_lowercase().as_str() {
            "place" | "p" => self.handle_place(&parts[1..]),
            "cancel" | "c" => self.handle_cancel(&parts[1..]),
            "replace" | "r" => self.handle_replace(&parts[1..]),
            "help" | "h" | "?" => print_usage(),
            "quit" | "q" | "exit" => return false,
            _ => println!("Unknown command: {}. Type 'help' for usage.", parts[0]),
        }

        true
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // parse cli args
    let channel_name = args
        .iter()
        .position(|a| a == "--channel")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "ts_channel".to_string());

    let venue = args
        .iter()
        .position(|a| a == "--venue")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| parse_venue(s))
        .unwrap_or(Venue::Hyperliquid);

    let passport_id: PassportId = args
        .iter()
        .position(|a| a == "--passport")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    println!("Trade Server CLI");
    println!("================");
    println!("Channel: {}", channel_name);
    println!("Venue: {:?}", venue);
    println!("Passport ID: {}", passport_id);
    println!();

    // connect to trade server
    let io = match EngineIceoryx2Wrapper::new(channel_name) {
        Ok(io) => io,
        Err(e) => {
            eprintln!("Failed to connect to trade server: {}", e);
            eprintln!("Is the trade server running?");
            std::process::exit(1);
        }
    };

    let (sender, _receiver) = io.split();
    let mut ctx = CliContext::new(sender, venue, passport_id);

    print_usage();
    println!();

    // repl loop
    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => break, // eof
            Ok(_) => {
                if !ctx.handle_command(&input) {
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                break;
            }
        }
    }

    println!("Goodbye!");
}
