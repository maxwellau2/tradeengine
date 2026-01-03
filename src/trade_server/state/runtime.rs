// state runtime - polls multiple state subscribers concurrently
//
// wraps each subscriber as an async Stream, then uses stream::select_all
// to poll all streams concurrently. when any has data, we forward immediately.
// scales to many exchanges with O(1) overhead per update.

use crate::ts_connectors::state::AnyStateSubscriber;
use crate::types::{
    common::Venue,
    packet::{Packet, TSInternalMessage},
    trade_server::StateUpdate,
};
use async_stream::stream;
use futures_util::{Stream, StreamExt, stream::select_all};
use ringbuf::{HeapProd, traits::Producer};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error, info};

/// runtime that manages multiple state subscribers concurrently
pub struct StateRuntime {
    seq_num: AtomicU64,
}

impl StateRuntime {
    pub fn new() -> Self {
        Self {
            seq_num: AtomicU64::new(1),
        }
    }

    /// run the runtime with given subscribers and their output queues
    ///
    /// takes ownership of subscribers and runs forever, forwarding updates
    /// to the appropriate ring buffer based on venue.
    pub async fn run(
        self,
        mut subscribers: Vec<(
            Venue,
            AnyStateSubscriber,
            HeapProd<Packet<TSInternalMessage>>,
        )>,
    ) {
        let n = subscribers.len();
        if n == 0 {
            info!("state runtime: no subscribers, exiting");
            return;
        }

        info!("state runtime starting with {} subscribers", n);

        // connect all subscribers first
        for (venue, sub, _) in &mut subscribers {
            sub.connect().await;
            info!(venue = ?venue, "state subscriber connected");
        }

        // separate into (streams, queues) so we can mutate queues while polling streams
        let mut queues: Vec<(Venue, HeapProd<Packet<TSInternalMessage>>)> = Vec::with_capacity(n);
        let mut streams: Vec<Pin<Box<dyn Stream<Item = (usize, StateUpdate)> + Send>>> =
            Vec::with_capacity(n);

        for (idx, (venue, mut subscriber, outbound)) in subscribers.into_iter().enumerate() {
            queues.push((venue, outbound));

            // wrap subscriber in a stream that yields (idx, update)
            let s = stream! {
                loop {
                    if let Some(update) = subscriber.produce().await {
                        yield (idx, update);
                    }
                }
            };
            streams.push(Box::pin(s));
        }

        // merge all streams into one
        let mut merged = select_all(streams);

        // process updates as they arrive
        while let Some((idx, update)) = merged.next().await {
            let (venue, outbound) = &mut queues[idx];
            let seq = self.seq_num.fetch_add(1, Ordering::Relaxed);
            let packet = Packet::new(TSInternalMessage::StateUpdate(update), seq);

            if let Err(e) = outbound.try_push(packet) {
                error!(venue = ?venue, "failed to push state update: {:?}", e);
            } else {
                debug!(venue = ?venue, "forwarded state update to central");
            }
        }

        info!("state runtime: all streams ended");
    }
}
