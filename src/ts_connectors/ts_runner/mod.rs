// ts runner - orchestrates trade server components with core pinning
//
// architecture:
// - main_core: central sync loop (hot path)
// - io_core: single tokio runtime for all executors + state subscribers

use std::error::Error;
use std::thread::{self, JoinHandle};

use ringbuf::{HeapCons, HeapProd, HeapRb, traits::Split};
use tokio::runtime::Builder;
use tracing::info;

use crate::{
    core_utils::{pin_to_core, validate_cores},
    trade_server::{central::Central, execution::ExecutorRunner, state::runner::StateRunner},
    ts_connectors::{
        config::TSConfig,
        orders::{AnyExecutor, Executor, HyperliquidExecutor},
        state::{AnyStateSubscriber, StateSubscriber, hyperliquid::HyperliquidStateSubscriber},
    },
    tui::{SharedTUIState, new_shared_state},
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

    /// consumes self and spawns threads for central and io runtime
    ///
    /// thread layout:
    /// - main_core: central sync loop (hot path)
    /// - io_core: tokio runtime for all executors + state subscribers
    pub fn run(self) -> Vec<JoinHandle<()>> {
        let main_core = self.ts_config.main_core;
        let io_core = self.ts_config.io_core;
        let channel_name = self.ts_config.channel_name;

        // validate cores before starting
        validate_cores(main_core, io_core).expect("invalid core configuration");

        let mut handles = Vec::new();

        // spawn io runtime on io_core - handles all async i/o
        let io_handle = Self::spawn_io_runtime(io_core, self.executor_configs, self.state_configs);
        handles.push(io_handle);

        // spawn central on main_core
        let central_handle = Self::spawn_central(
            main_core,
            channel_name,
            self.central_execution_channels,
            self.central_state_channels,
            self.tui_state,
        );
        handles.push(central_handle);

        handles
    }

    /// spawn tokio runtime on io_core for executors and state subscribers
    fn spawn_io_runtime(
        io_core: usize,
        executor_configs: Vec<ExecutorConfig>,
        state_configs: Vec<StateConfig>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("io-runtime".into())
            .spawn(move || {
                pin_to_core(io_core);

                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create tokio runtime");

                rt.block_on(async {
                    // spawn all executors as tasks on this runtime
                    for cfg in executor_configs {
                        tokio::spawn(async move {
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
                            info!("executor for {:?} started", cfg.venue);
                            runner.start().await;
                        });
                    }

                    // spawn all state subscribers as tasks on this runtime
                    for cfg in state_configs {
                        tokio::spawn(async move {
                            let subscriber: AnyStateSubscriber = match cfg.venue {
                                Venue::Hyperliquid => AnyStateSubscriber::Hyperliquid(
                                    HyperliquidStateSubscriber::new(
                                        cfg.passport_id,
                                        &cfg.passport_path,
                                    )
                                    .await
                                    .expect("failed to create hyperliquid state subscriber"),
                                ),
                                Venue::Binance => todo!(),
                                Venue::Paradex => todo!(),
                                Venue::Okx => todo!(),
                            };

                            let mut runner = StateRunner::new(cfg.venue, cfg.outbound, subscriber);
                            info!("state runner for {:?} started", cfg.venue);
                            runner.start().await;
                        });
                    }

                    // block forever - keep runtime alive
                    std::future::pending::<()>().await;
                });
            })
            .expect("failed to spawn io runtime thread")
    }

    /// spawn central on main_core
    fn spawn_central(
        main_core: usize,
        channel_name: String,
        central_execution_channels: Vec<CentralChannels>,
        central_state_channels: Vec<CentralChannels>,
        tui_state: Option<SharedTUIState>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("central".into())
            .spawn(move || {
                pin_to_core(main_core);

                let mut central = Central::new(channel_name).expect("failed to create central");

                if let Some(state) = tui_state {
                    central.set_tui_state(state);
                }

                for ch in central_execution_channels {
                    central.add_execution_channel(ch.venue, ch.sender, ch.receiver);
                }

                for ch in central_state_channels {
                    central.add_state_channel(ch.venue, ch.sender, ch.receiver);
                }

                info!("central started");
                central.run();
            })
            .expect("failed to spawn central thread")
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
