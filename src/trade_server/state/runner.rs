use crate::ts_connectors::state::AnyStateSubscriber;
use crate::types::{
    common::Venue,
    packet::{Packet, TSInternalMessage},
};
use ringbuf::{HeapProd, traits::Producer};
use std::sync::atomic::AtomicU64;
use tracing::{debug, error, info};

/// runs state subscriber in dedicated thread, forwards updates to central
pub struct StateRunner {
    venue: Venue,
    outbound_queue: HeapProd<Packet<TSInternalMessage>>,
    subscriber: AnyStateSubscriber,
    seq_num: AtomicU64,
}

impl StateRunner {
    pub fn new(
        venue: Venue,
        outbound_queue: HeapProd<Packet<TSInternalMessage>>,
        subscriber: AnyStateSubscriber,
    ) -> Self {
        Self {
            venue,
            outbound_queue,
            subscriber,
            seq_num: AtomicU64::new(1),
        }
    }

    /// connect the subscriber
    pub async fn connect(&mut self) {
        self.subscriber.connect().await;
        info!(venue = ?self.venue, "state runner connected");
    }

    /// poll once async - used with block_on to provide runtime context
    ///
    /// produce() internally uses now_or_never for ws reads, so this
    /// returns quickly without blocking. the async fn is needed because
    /// tokio ws operations require runtime context to drive i/o polling.
    pub async fn poll_once_async(&mut self) -> bool {
        match self.subscriber.produce().await {
            Some(update) => {
                self.forward_to_central(update);
                true
            }
            None => false,
        }
    }

    pub async fn start(&mut self) {
        self.subscriber.connect().await;
        info!(venue = ?self.venue, "state runner started");
        loop {
            // poll subscriber for state updates, drain all available
            while let Some(update) = self.subscriber.produce().await {
                self.forward_to_central(update);
            }
        }
    }

    fn forward_to_central(&mut self, update: crate::types::trade_server::StateUpdate) {
        let seq = self.next_seq_num();
        let packet = Packet::new(TSInternalMessage::StateUpdate(update), seq);
        match self.outbound_queue.try_push(packet) {
            Ok(_) => {
                debug!(venue = ?self.venue, "forwarded state update to central");
            }
            Err(e) => {
                error!(venue = ?self.venue, "failed to push state update: {:?}", e);
            }
        }
    }

    fn next_seq_num(&mut self) -> u64 {
        self.seq_num
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
