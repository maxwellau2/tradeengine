use std::collections::HashMap;

use crate::{
    ts_protocol::iceoryx2_wrapper::TSIceoryx2Wrapper,
    types::{
        common::Venue,
        packet::{Packet, TSInternalMessage},
    },
};
use ringbuf::{HeapCons, HeapProd, HeapRb, traits::Consumer};

pub struct Central {
    execution_senders: HashMap<Venue, HeapProd<Packet<TSInternalMessage>>>,
    execution_receivers: HashMap<Venue, HeapCons<Packet<TSInternalMessage>>>,
    outer_protocol: TSIceoryx2Wrapper,
    // state management  here also..
}

impl Central {
    pub fn new(outer_protocol_name: String) -> Result<Self, String> {
        let protocol = TSIceoryx2Wrapper::new(outer_protocol_name)?;
        Ok(Self {
            execution_senders: HashMap::new(),
            execution_receivers: HashMap::new(),
            outer_protocol: protocol,
        })
    }

    pub fn poll_outer_protocol(&mut self){
        match self.outer_protocol.recv() {
            Some(value) => {
                println!("Received {:?}", value);
            }
            None => {
                // no-op;
            }
        }
    }

    pub fn poll_execution_receivers(&mut self){
        for (_, v) in self.execution_receivers.iter_mut() {
            match v.try_pop() {
                Some(value) => {
                    println!("Received {:?}", value);
                }
                None => {
                    // no-op;
                }
            }
        }
    }

    pub fn poll_ingress_queues_once(&mut self) {
        self.poll_outer_protocol();
        self.poll_execution_receivers();
    }

    pub fn run(&mut self) {
        loop {
            self.poll_ingress_queues_once();
        }
    }
}
