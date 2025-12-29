// idea here is to use hashmap to add key value pairs,
// so essentially, we .add_executor(Executor), many times and call a .build() function
// this .build function will lookup hashmaps and shit for us and do the linking
// then the .run() method will spawn the threads and pin them appropriately

use std::error::Error;
use std::thread::{self, JoinHandle};

use ringbuf::{HeapCons, HeapProd, HeapRb, traits::Split};
use tokio::runtime::Builder;
use tracing::{error, info};

use crate::{
    config_parser::passport::PassportConfig,
    trade_server::{central::Central, execution::ExecutorRunner, state::runner::StateRunner},
    ts_connectors::{
        config::TSConfig,
        orders::{AnyExecutor, Executor, HyperliquidExecutor},
        state::{AnyStateSubscriber, StateSubscriber, hyperliquid::HyperliquidStateSubscriber},
    },
    tui::{SharedTUIState, TUI, new_shared_state},
    types::{
        common::Venue,
        packet::{Packet, TSInternalMessage},
    },
};

const MAX_RB_SZ: usize = 1 << 10;

/// channel halves that will be passed to central thread
struct CentralChannels {
    venue: Venue,
    sender: HeapProd<Packet<TSInternalMessage>>,
    receiver: HeapCons<Packet<TSInternalMessage>>,
}

/// config needed to create an executor inside its thread
struct ExecutorConfig {
    passport_id: u16,
    venue: Venue,
    passport_path: String,
    // executor's channel halves
    inbound: HeapCons<Packet<TSInternalMessage>>,
    outbound: HeapProd<Packet<TSInternalMessage>>,
}

/// config needed to create a state subscriber inside its thread
struct StateConfig {
    passport_id: u16,
    venue: Venue,
    passport_path: String,
    // state subscriber only sends to central, no inbound
    outbound: HeapProd<Packet<TSInternalMessage>>,
}

pub struct TSRunner {
    executor_configs: Vec<ExecutorConfig>,
    state_configs: Vec<StateConfig>,
    central_execution_channels: Vec<CentralChannels>,
    central_state_channels: Vec<CentralChannels>,
    ts_config: TSConfig,
    tui_state: Option<SharedTUIState>,
}

impl TSRunner {
    /// create ts runner without tui
    pub fn new(ts_cfg_path: &str) -> Result<Self, Box<dyn Error>> {
        let ts_cfg = TSConfig::from(ts_cfg_path)?;
        let mut e = Self {
            executor_configs: Vec::new(),
            state_configs: Vec::new(),
            central_execution_channels: Vec::new(),
            central_state_channels: Vec::new(),
            ts_config: ts_cfg,
            tui_state: None,
        };
        e.build();
        Ok(e)
    }

    /// create ts runner with tui enabled
    pub fn new_with_tui(ts_cfg_path: &str) -> Result<Self, Box<dyn Error>> {
        let ts_cfg = TSConfig::from(ts_cfg_path)?;
        let mut e = Self {
            executor_configs: Vec::new(),
            state_configs: Vec::new(),
            central_execution_channels: Vec::new(),
            central_state_channels: Vec::new(),
            ts_config: ts_cfg,
            tui_state: Some(new_shared_state()),
        };
        e.build();
        Ok(e)
    }

    pub fn new_with_tui_state(
        ts_cfg_path: &str,
        tui_state: SharedTUIState,
    ) -> Result<Self, Box<dyn Error>> {
        let ts_cfg = TSConfig::from(ts_cfg_path)?;
        let mut e = Self {
            executor_configs: Vec::new(),
            state_configs: Vec::new(),
            central_execution_channels: Vec::new(),
            central_state_channels: Vec::new(),
            ts_config: ts_cfg,
            tui_state: Some(tui_state),
        };
        e.build();
        Ok(e)
    }

    /// registers an executor to be created when run() is called
    /// executor is created inside its dedicated thread to avoid Send issues
    fn add_executor(&mut self, passport_id: u16, venue: Venue) {
        // create ring buffers for communication between executor and central
        let (executor_to_central_in, executor_to_central_out) =
            HeapRb::<Packet<TSInternalMessage>>::new(MAX_RB_SZ).split();
        let (central_to_executor_in, central_to_executor_out) =
            HeapRb::<Packet<TSInternalMessage>>::new(MAX_RB_SZ).split();

        // store central's halves for later
        self.central_execution_channels.push(CentralChannels {
            venue,
            sender: central_to_executor_in,
            receiver: executor_to_central_out,
        });

        // store executor config - actual executor created in thread
        self.executor_configs.push(ExecutorConfig {
            passport_id,
            venue,
            passport_path: self.ts_config.passport_path.clone(),
            inbound: central_to_executor_out,
            outbound: executor_to_central_in,
        });
    }

    /// registers a state subscriber to be created when run() is called
    fn add_state_subscriber(&mut self, passport_id: u16, venue: Venue) {
        // state subscriber only sends to central (no inbound needed)
        let (state_to_central_prod, state_to_central_cons) =
            HeapRb::<Packet<TSInternalMessage>>::new(MAX_RB_SZ).split();

        // central receives from state subscriber
        self.central_state_channels.push(CentralChannels {
            venue,
            sender: HeapRb::<Packet<TSInternalMessage>>::new(1).split().0, // dummy, unused
            receiver: state_to_central_cons,
        });

        // store state config - subscriber sends via this producer
        self.state_configs.push(StateConfig {
            passport_id,
            venue,
            passport_path: self.ts_config.passport_path.clone(),
            outbound: state_to_central_prod,
        });
    }

    fn build(&mut self) {
        let exchanges: Vec<_> = self
            .ts_config
            .exchanges
            .iter()
            .map(|e| Venue::from_string(e).expect(&format!("unknown venue: {}", e)))
            .collect();

        for venue in exchanges {
            self.add_executor(self.ts_config.passport_id, venue);
            self.add_state_subscriber(self.ts_config.passport_id, venue);
        }
    }

    /// consumes self and spawns threads for executors and central
    /// returns join handles so caller can wait for completion
    pub fn run(self) -> Vec<JoinHandle<()>> {
        let core_ids = core_affinity::get_core_ids().unwrap_or_default();
        if core_ids.len() < 2 {
            error!("need at least 2 cores for pinning");
        }

        let mut handles = Vec::new();
        let num_executors = self.executor_configs.len();

        // thread A: executor runners (starting from core 0)
        // each executor gets its own thread for isolation
        for (i, cfg) in self.executor_configs.into_iter().enumerate() {
            let core_id = core_ids.get(i).copied();
            let handle = thread::spawn(move || {
                // pin to core if available
                if let Some(id) = core_id {
                    if core_affinity::set_for_current(id) {
                        info!("executor runner pinned to core {:?}", id);
                    }
                }

                // create single-threaded tokio runtime for this executor
                // single-threaded avoids work-stealing overhead
                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create tokio runtime");

                rt.block_on(async {
                    // create executor inside runtime to avoid Send issues
                    // async resources (websocket, timers) bound to this runtime
                    let executor: AnyExecutor = match cfg.venue {
                        Venue::Hyperliquid => AnyExecutor::Hyperliquid(
                            HyperliquidExecutor::new(cfg.passport_id, &cfg.passport_path)
                                .await
                                .expect("failed to create hyperliquid executor"),
                        ),
                        Venue::Binance => todo!(),
                        Venue::Paradex => todo!(),
                        Venue::Okx => todo!(),
                    };

                    let mut runner =
                        ExecutorRunner::new(cfg.venue, cfg.inbound, cfg.outbound, executor);
                    info!("executor for {:?} has started", cfg.venue);
                    runner.start().await;
                });
            });
            handles.push(handle);
        }

        let num_state_runners = self.state_configs.len();

        // thread B: state runners (after executor cores)
        for (i, cfg) in self.state_configs.into_iter().enumerate() {
            let core_id = core_ids.get(num_executors + i).copied();
            let handle = thread::spawn(move || {
                if let Some(id) = core_id {
                    if core_affinity::set_for_current(id) {
                        info!("state runner pinned to core {:?}", id);
                    }
                }

                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create tokio runtime");

                rt.block_on(async {
                    let subscriber: AnyStateSubscriber = match cfg.venue {
                        Venue::Hyperliquid => AnyStateSubscriber::Hyperliquid(
                            HyperliquidStateSubscriber::new(cfg.passport_id, &cfg.passport_path)
                                .await
                                .expect("failed to create hyperliquid state subscriber"),
                        ),
                        Venue::Binance => todo!(),
                        Venue::Paradex => todo!(),
                        Venue::Okx => todo!(),
                    };

                    let mut runner = StateRunner::new(cfg.venue, cfg.outbound, subscriber);
                    info!("state runner for {:?} has started", cfg.venue);
                    runner.start().await;
                });
            });
            handles.push(handle);
        }

        // thread C: central on a dedicated core after executors and state runners
        let central_core = core_ids.get(num_executors + num_state_runners).copied();
        let central_execution_channels = self.central_execution_channels;
        let central_state_channels = self.central_state_channels;
        let tui_state_for_central = self.tui_state.clone();
        let central_handle = thread::spawn(move || {
            if let Some(id) = central_core {
                if core_affinity::set_for_current(id) {
                    info!("central pinned to core {:?}", id);
                }
            }

            // create central inside the thread to avoid Send issues with iceoryx2
            let mut central =
                Central::new(self.ts_config.channel_name).expect("failed to create central");

            // set tui state for central to update (if tui enabled)
            if let Some(tui_state) = tui_state_for_central {
                central.set_tui_state(tui_state);
            }

            // register execution channel halves
            for ch in central_execution_channels {
                central.add_execution_channel(ch.venue, ch.sender, ch.receiver);
            }

            // register state channel halves (only receiver used)
            for ch in central_state_channels {
                central.add_state_channel(ch.venue, ch.sender, ch.receiver);
            }

            info!("central has started");
            central.run();
        });
        handles.push(central_handle);

        // thread D: TUI (only if enabled)
        if let Some(tui_state) = self.tui_state {
            let tui_handle = thread::spawn(move || {
                // small delay to let other threads start
                std::thread::sleep(std::time::Duration::from_millis(500));

                match TUI::new(tui_state) {
                    Ok(mut tui) => {
                        if let Err(e) = tui.run() {
                            error!("TUI error: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("failed to create TUI: {}", e);
                    }
                }
            });
            handles.push(tui_handle);
        }

        handles
    }
}

pub mod test {

    use super::*;

    #[test]
    pub fn create() -> Result<(), Box<dyn std::error::Error>> {
        println!("testing");
        let tsrunner = TSRunner::new(
            "/home/maxwell/dev/personal/mdfeed/md_feed/DO_NOT_COMMIT/ts_config.yaml",
        )?;
        let handles = tsrunner.run();
        for h in handles {
            match h.join() {
                Ok(_) => {
                    println!("ok!")
                }
                Err(_) => {
                    println!("yo smth ahppend")
                }
            }
        }
        Ok(())
    }
}
