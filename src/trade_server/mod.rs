use crate::{trade_server::central::Central, types::common::PassportId};

pub mod central;
pub mod execution;

fn main() {
    let mut central = Central::new("channel1".into(), PassportId::new("1234")).expect("lol wtf");
    central.run();
}
