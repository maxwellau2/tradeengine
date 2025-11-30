use md_feed::{trade_server::central::Central, types::common::PassportId};

fn main() {
    let mut central = Central::new("channel1".into(), PassportId::new("123")).expect("lol wtf");
    central.run();
}
