// trading runtime - unified async runtime for md feeds + engine
//
// runs md feeds and engine on single core with select_all multiplexing.
// when any feed has data, calls engine inline. no ring buffers.
//
// architecture:
// - md feeds wrapped as async streams
// - select_all races all feeds
// - when data arrives, engine.on_md() called inline
// - ts receiver polled between md waits via short timeout

use crate::md_connectors::subscriber::AnyMDSubscriber;
use crate::strategy::engine::MarketDataEngine;
use crate::tui::{SharedTUIState, TUI, should_shutdown};
use crate::types::common::Venue;
use crate::types::packet::MDMessage;
use async_stream::stream;
use futures_util::{Stream, StreamExt, stream::select_all};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// trading runtime - owns feeds and engine, runs unified async loop
pub struct TradingRuntime {
    feeds: Vec<AnyMDSubscriber>,
    engine: MarketDataEngine,
    tui_state: Option<SharedTUIState>,
}

impl TradingRuntime {
    pub fn new(engine: MarketDataEngine) -> Self {
        Self {
            feeds: Vec::new(),
            engine,
            tui_state: None,
        }
    }

    /// set tui state for rendering
    pub fn set_tui_state(&mut self, state: SharedTUIState) {
        self.tui_state = Some(state);
    }

    /// add an md feed subscriber
    pub fn add_feed(&mut self, feed: AnyMDSubscriber) {
        info!(venue = ?feed.venue(), "adding md feed to runtime");
        self.feeds.push(feed);
    }

    /// create tui if state is set
    fn create_tui(&self) -> Option<TUI> {
        self.tui_state
            .as_ref()
            .and_then(|state| TUI::with_title(state.clone(), "Engine".to_string()).ok())
    }

    /// render tui periodically (cold path, ~10fps)
    fn maybe_render_tui(&mut self, tui: &mut Option<TUI>, last_render: &mut Instant) {
        if last_render.elapsed() >= Duration::from_millis(100) {
            if let Some(tui) = tui {
                self.engine.sync_tui_state();
                let _ = tui.draw_once();
                tui.check_quit();
            }
            *last_render = Instant::now();
        }
    }

    /// run the trading runtime
    ///
    /// 1. connects all feeds
    /// 2. runs engine startup (recon, etc)
    /// 3. enters main loop: select_all on feeds, dispatch to engine
    pub async fn run(mut self) {
        let n = self.feeds.len();
        info!("trading runtime starting with {} feeds", n);

        // connect all feeds
        for feed in &mut self.feeds {
            feed.connect().await;
        }

        // run engine startup protocol (recon, etc)
        self.engine.on_start_protocol();
        self.engine.start_recon();

        // create tui if state provided
        let mut tui = self.create_tui();
        let mut last_render = Instant::now();

        if n == 0 {
            // no feeds, just run ts polling loop
            info!("no md feeds, running ts-only loop");
            while !should_shutdown() {
                self.engine.poll_ts();
                self.engine.poll_heartbeat();
                self.maybe_render_tui(&mut tui, &mut last_render);
                tokio::task::yield_now().await;
            }
            return;
        }

        // wrap feeds as streams yielding (venue, msg)
        let mut streams: Vec<Pin<Box<dyn Stream<Item = (Venue, MDMessage)> + Send>>> = Vec::new();

        let feeds = std::mem::take(&mut self.feeds);
        for mut feed in feeds {
            let venue = feed.venue();
            let s = stream! {
                loop {
                    if let Some(msg) = feed.produce().await {
                        yield (venue, msg);
                    }
                }
            };
            streams.push(Box::pin(s));
        }

        // merge all streams
        let stream_count = streams.len();
        let mut merged = select_all(streams);

        info!(
            "trading runtime entering main loop with {} streams",
            stream_count
        );

        // message counters for throughput logging
        let mut msg_count: u64 = 0;
        let mut last_stats = Instant::now();

        // main loop - use tokio::select with short timeout for responsiveness
        while !should_shutdown() {
            // poll ts messages (non-blocking)
            self.engine.poll_ts();
            self.engine.poll_heartbeat();

            // tui render (time-based, ~10fps)
            self.maybe_render_tui(&mut tui, &mut last_render);

            // log throughput every 5 seconds
            if last_stats.elapsed() >= Duration::from_secs(5) {
                let elapsed = last_stats.elapsed().as_secs_f64();
                let rate = msg_count as f64 / elapsed;
                info!(
                    "md throughput: {} msgs in {:.1}s ({:.1} msg/s)",
                    msg_count, elapsed, rate
                );
                msg_count = 0;
                last_stats = Instant::now();
            }

            // wait for md with minimal timeout for max responsiveness
            tokio::select! {
                biased;

                Some((venue, msg)) = merged.next() => {
                    msg_count += 1;
                    debug!(venue = ?venue, "md update received");
                    self.engine.on_md(venue, msg);
                }

                // minimal yield - just check if other work pending
                _ = tokio::task::yield_now() => {}
            }
        }

        info!("trading runtime stopped");
    }
}
