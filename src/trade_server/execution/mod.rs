use crate::types::{common::Venue, packet::{Packet, TSInternalMessage}, trade_server::{CancelOrder, PlaceOrder, ReplaceOrder}};
use ringbuf::{HeapCons, HeapProd, traits::Consumer};

pub trait Executor{
    fn place_order(&self, order: PlaceOrder);
    fn cancel_order(&self, cancel: CancelOrder);
    fn replace_order(&self, replace: ReplaceOrder);
}

pub struct ExecutorRunner<E: Executor>{
    venue: Venue,
    inbound_queue: HeapCons<Packet<TSInternalMessage>>,
    executor_result_producer: HeapProd<Packet<TSInternalMessage>>,
    executor_result_consumer: HeapCons<Packet<TSInternalMessage>>,
    executor: E,
}


// static dispatch for speeeeeed
impl<E: Executor> ExecutorRunner<E>{
    pub fn new(venue: Venue, inbound_queue: HeapCons<Packet<TSInternalMessage>>, executor_result_consumer: HeapCons<Packet<TSInternalMessage>>, executor_result_producer: HeapProd<Packet<TSInternalMessage>>, executor: E) -> Self {
        return Self { venue, inbound_queue, executor_result_consumer, executor_result_producer, executor }
    }

    pub fn start(&mut self){
        loop{
            self.ingress_once(); // read msg from central
            self.outgress_once(); // push any message to central
        }
    }

    pub fn ingress_once(&mut self){
        match self.inbound_queue.try_pop() {
            Some(val) => {
                // place, cancel or replace
                match val.body{
                    TSInternalMessage::PlaceOrder(place_order) => self.executor.place_order(place_order),
                    TSInternalMessage::CancelOrder(cancel_order) => self.executor.cancel_order(cancel_order),
                    TSInternalMessage::ReplaceOrder(replace_order) => self.executor.replace_order(replace_order),
                    _ => println!("Wtf?"),
                }
            },
            None => {
                // no- op
            }
        }
    }

    pub fn outgress_once(&mut self){
        match self.executor_result_consumer.try_pop(){
            Some(val) => {
                // send update back to central
            },
            None => {
                // no op
            }
        };
    }
}