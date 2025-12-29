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
