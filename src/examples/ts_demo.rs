use std::error::Error;

use md_feed::{
    trade_server::{central::Central, execution::ExecutorRunner},
    ts_connectors::orders::{AnyExecutor, Executor, hyperliquid::HyperliquidExecutor},
    types::{
        common::{PassportId, Venue},
        packet::{Packet, TSInternalMessage},
    },
};
use ringbuf::{HeapRb, traits::Split};

const BUFFER_SZ: usize = 1 << 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let passport_id = 1234;
    let cfg_path = "/home/maxwell/dev/personal/mdfeed/md_feed/DO_NOT_COMMIT/maxwell_passport.json";

    let central_to_exe = HeapRb::<Packet<TSInternalMessage>>::new(BUFFER_SZ);
    let (central_to_exe_in, central_to_exe_out) = central_to_exe.split();
    let exe_to_central = HeapRb::<Packet<TSInternalMessage>>::new(BUFFER_SZ);
    let (exe_to_central_in, exe_to_central_out) = exe_to_central.split();

    let exe = HyperliquidExecutor::new(passport_id, cfg_path)
        .await
        .unwrap();
    let mut runner = ExecutorRunner::new(
        Venue::Hyperliquid,
        central_to_exe_out,
        exe_to_central_in,
        AnyExecutor::Hyperliquid(exe),
    );

    tokio::spawn(async move { runner.start().await });

    // create central inside the thread where it will run
    let central_handle = std::thread::spawn(move || {
        let mut central =
            Central::new("channel1".into()).expect("failed to create trade server central");
        central.add_execution_channel(Venue::Hyperliquid, central_to_exe_in, exe_to_central_out);
        central.run();
    });
    println!("Yipeee");
    central_handle.join().expect("central panicked");
    println!("IM outta here");
    Ok(())
}
