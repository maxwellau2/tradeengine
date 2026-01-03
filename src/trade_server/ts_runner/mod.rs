// ts runner - orchestrates trade server components with core pinning
//
// architecture:
// - main_core: central sync loop (hot path)
// - one thread per executor (each with own tokio runtime)
// - one thread for all state subscribers (shared tokio runtime)

use std::error::Error;
use std::sync::mpsc as std_mpsc;
use std::thread::{self, JoinHandle};

use ringbuf::{HeapCons, HeapProd, HeapRb, traits::Split};
use tokio::runtime::Builder;
use tracing::{error, info};

use crate::{
    core_utils::pin_to_core,
    trade_server::{central::Central, execution::ExecutorRunner, state::runtime::StateRuntime},
    ts_connectors::{
        config::TSConfig,
        orders::{AnyExecutor, Executor, HyperliquidExecutor, ParadexLightExecutor},
        state::{
            AnyStateSubscriber, StateSubscriber, hyperliquid::HyperliquidStateSubscriber,
            paradex_light::ParadexLightStateSubscriber,
        },
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
    /// dedicated cpu core for this executor
    core: usize,
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
    fn add_executor(&mut self, passport_id: u16, venue: Venue, core: usize) {
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
            core,
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

        // each executor gets its own dedicated core starting from executor_core_start
        for (idx, venue) in exchanges.into_iter().enumerate() {
            let core = self.ts_config.executor_core_start + idx;
            self.add_executor(self.ts_config.passport_id, venue, core);
            self.add_state_subscriber(self.ts_config.passport_id, venue);
        }
    }

    /// consumes self and spawns threads
    ///
    /// thread layout:
    /// - main_core: central sync loop (hot path)
    /// - one thread per executor (each with own tokio runtime, own dedicated core)
    /// - one thread for all state subscribers (shared tokio runtime on state_core)
    pub fn run(self) -> Vec<JoinHandle<()>> {
        let main_core = self.ts_config.main_core;
        let state_core = self.ts_config.state_core;
        let channel_name = self.ts_config.channel_name;

        // channel to signal when executors and state subscribers are ready
        let executor_count = self.executor_configs.len();
        let state_count = self.state_configs.len();
        let total_ready_count = executor_count + state_count;
        let (ready_tx, ready_rx) = std_mpsc::channel::<&'static str>();

        let mut handles = Vec::new();

        // spawn each executor on its own thread with dedicated core
        for cfg in self.executor_configs {
            let tx = ready_tx.clone();
            let handle = Self::spawn_executor_thread(cfg, tx);
            handles.push(handle);
        }

        // spawn state subscribers on shared runtime (state_core)
        let state_handle = Self::spawn_state_runtime(state_core, self.state_configs, ready_tx);
        handles.push(state_handle);

        // spawn central on main_core
        let central_handle = Self::spawn_central(
            main_core,
            channel_name,
            self.central_execution_channels,
            self.central_state_channels,
            self.tui_state,
            ready_rx,
            total_ready_count,
        );
        handles.push(central_handle);

        handles
    }

    /// spawn a single executor on its own thread with dedicated tokio runtime
    ///
    /// each executor gets its own dedicated cpu core for isolation
    /// all executors use current_thread runtime for minimal jitter
    fn spawn_executor_thread(
        cfg: ExecutorConfig,
        ready_tx: std_mpsc::Sender<&'static str>,
    ) -> JoinHandle<()> {
        let venue = cfg.venue;
        let core = cfg.core;
        thread::Builder::new()
            .name(format!("executor-{:?}", venue))
            .spawn(move || {
                pin_to_core(core);
                info!("executor {:?} pinned to core {}", venue, core);

                // current_thread runtime for all executors - minimal jitter
                // paradex uses FuturesUnordered (polled inline), not spawned tasks
                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create executor runtime");

                rt.block_on(async move {
                    let executor: AnyExecutor = match cfg.venue {
                        Venue::Hyperliquid => AnyExecutor::Hyperliquid(
                            HyperliquidExecutor::new(cfg.passport_id, &cfg.passport_path)
                                .await
                                .expect("failed to create hyperliquid executor"),
                        ),
                        Venue::Paradex => AnyExecutor::ParadexLight(
                            ParadexLightExecutor::new(cfg.passport_id, &cfg.passport_path)
                                .await
                                .expect("failed to create paradex executor"),
                        ),
                        Venue::Binance => todo!(),
                        Venue::Okx => todo!(),
                    };

                    let mut runner =
                        ExecutorRunner::new(cfg.venue, cfg.inbound, cfg.outbound, executor);

                    // connect first (warmup, auth, etc) before signaling ready
                    runner.connect().await;

                    // signal ready - only after connect completes
                    info!("executor for {:?} ready", cfg.venue);
                    let _ = ready_tx.send("executor");

                    info!("executor for {:?} started", cfg.venue);
                    runner.run_loop().await;
                });
            })
            .expect("failed to spawn executor thread")
    }

    /// spawn state runtime - single thread polls all subscribers concurrently
    ///
    /// uses select_all to multiplex all ws streams. when any has data,
    /// we wake and forward immediately. scales to many exchanges.
    fn spawn_state_runtime(
        state_core: usize,
        state_configs: Vec<StateConfig>,
        ready_tx: std_mpsc::Sender<&'static str>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("state-runtime".into())
            .spawn(move || {
                pin_to_core(state_core);

                let rt = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create state runtime");

                rt.block_on(async {
                    // build subscribers list: (venue, subscriber, outbound_queue)
                    let mut subscribers = Vec::new();

                    for cfg in state_configs {
                        let venue = cfg.venue;
                        let subscriber: AnyStateSubscriber = match venue {
                            Venue::Hyperliquid => {
                                match HyperliquidStateSubscriber::new(
                                    cfg.passport_id,
                                    &cfg.passport_path,
                                )
                                .await
                                {
                                    Ok(s) => AnyStateSubscriber::Hyperliquid(s),
                                    Err(e) => {
                                        error!(
                                            "failed to create hyperliquid state subscriber: {:?}",
                                            e
                                        );
                                        let _ = ready_tx.send("state");
                                        continue;
                                    }
                                }
                            }
                            Venue::Paradex => {
                                match ParadexLightStateSubscriber::new(
                                    cfg.passport_id,
                                    &cfg.passport_path,
                                )
                                .await
                                {
                                    Ok(s) => AnyStateSubscriber::ParadexLight(s),
                                    Err(e) => {
                                        error!(
                                            "failed to create paradex state subscriber: {:?}",
                                            e
                                        );
                                        let _ = ready_tx.send("state");
                                        continue;
                                    }
                                }
                            }
                            Venue::Binance => todo!(),
                            Venue::Okx => todo!(),
                        };

                        info!("state subscriber for {:?} ready", venue);
                        let _ = ready_tx.send("state");

                        subscribers.push((venue, subscriber, cfg.outbound));
                    }

                    // run the state runtime - polls all subscribers concurrently
                    let runtime = StateRuntime::new();
                    runtime.run(subscribers).await;
                });
            })
            .expect("failed to spawn state runtime thread")
    }

    /// spawn central on main_core
    fn spawn_central(
        main_core: usize,
        channel_name: String,
        central_execution_channels: Vec<CentralChannels>,
        central_state_channels: Vec<CentralChannels>,
        tui_state: Option<SharedTUIState>,
        ready_rx: std_mpsc::Receiver<&'static str>,
        total_ready_count: usize,
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

                // wait for all executors and state subscribers to signal ready
                info!(
                    "central waiting for {} components to be ready...",
                    total_ready_count
                );
                for i in 0..total_ready_count {
                    match ready_rx.recv() {
                        Ok(component) => {
                            info!("{} {}/{} ready", component, i + 1, total_ready_count)
                        }
                        Err(_) => error!("ready channel closed unexpectedly"),
                    }
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
