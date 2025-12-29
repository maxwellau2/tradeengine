use crate::ts_connectors::orders::AnyExecutor;
use crate::tui::should_shutdown;
use crate::types::clock::timestamp_nanos_precise;
use crate::types::trade_server::OrderResponse;
use crate::types::{
    common::Venue,
    packet::{Packet, TSInternalMessage},
};
use ringbuf::{
    HeapCons, HeapProd,
    traits::{Consumer, Producer},
};
use std::sync::atomic::AtomicU64;
use tracing::{debug, error, info, warn};

pub struct ExecutorRunner {
    venue: Venue,
    inbound_queue: HeapCons<Packet<TSInternalMessage>>,
    outbound_queue: HeapProd<Packet<TSInternalMessage>>,
    executor: AnyExecutor,
    seq_num: AtomicU64,
}

impl ExecutorRunner {
    pub fn new(
        venue: Venue,
        inbound_queue: HeapCons<Packet<TSInternalMessage>>,
        outbound_queue: HeapProd<Packet<TSInternalMessage>>,
        executor: AnyExecutor,
    ) -> Self {
        Self {
            venue,
            inbound_queue,
            outbound_queue,
            executor,
            seq_num: AtomicU64::new(1),
        }
    }

    pub async fn start(&mut self) {
        self.executor.connect().await;
        loop {
            // in: poll inbound queue (orders from central)
            self.ingress_once().await;

            // out: poll executor for ws responses (non-blocking)
            if let Some(val) = self.executor.produce().await {
                self.handle_response(val);
            }
        }
    }

    fn handle_response(&mut self, val: OrderResponse) {
        debug!(venue = ?self.venue, "executor received response: {:?}", val);
        let seq = self.next_seq_num();
        let packet = match val {
            OrderResponse::Place(resp) => {
                info!(venue = ?self.venue, "place order response: {:?}", resp);
                Packet::new(TSInternalMessage::PlaceOrderResp(resp), seq)
            }
            OrderResponse::Cancel(resp) => {
                info!(venue = ?self.venue, "cancel order response: {:?}", resp);
                Packet::new(TSInternalMessage::CancelOrderResp(resp), seq)
            }
            OrderResponse::Replace(resp) => {
                info!(venue = ?self.venue, "replace order response: {:?}", resp);
                Packet::new(TSInternalMessage::ReplaceOrderResp(resp), seq)
            }
        };
        let res = self.outbound_queue.try_push(packet);
        match res {
            Ok(_) => {
                debug!(venue = ?self.venue, "pushed response to central");
            }
            Err(e) => {
                error!(venue = ?self.venue, "failed to push response: {e:?}");
            }
        }
    }

    fn next_seq_num(&mut self) -> u64 {
        self.seq_num
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn ingress_once(&mut self) {
        let Some(val) = self.inbound_queue.try_pop() else {
            return;
        };

        let latency_us = (timestamp_nanos_precise() - val.timestamp) / 1000;
        info!(latency_us, "central->executor latency");
        debug!(venue = ?self.venue, "executor received: {:?}", val.body);

        let result = match val.body {
            TSInternalMessage::PlaceOrder(order) => {
                info!(venue = ?self.venue, "placing order: {:?}", order);
                self.executor.place_order(order).await
            }
            TSInternalMessage::CancelOrder(cancel) => {
                info!(venue = ?self.venue, "cancelling order: {:?}", cancel);
                self.executor.cancel_order(cancel).await
            }
            TSInternalMessage::ReplaceOrder(replace) => {
                info!(venue = ?self.venue, "replacing order: {:?}", replace);
                self.executor.replace_order(replace).await
            }
            _ => return,
        };

        match &result {
            Ok(_) => debug!(venue = ?self.venue, "executor operation succeeded"),
            Err(e) => error!(venue = ?self.venue, "executor operation failed: {}", e),
        }
    }
}
