use crate::trade_server::central::Central;

pub mod central;
pub mod error;
pub mod execution;
pub mod state;

fn main() {
    let mut central =
        Central::new("channel1".into()).expect("failed to create trade server central");
    central.run();
}
