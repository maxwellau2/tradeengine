use std::collections::HashMap;

use crate::{
    ts_protocol::iceoryx2_wrapper::TSIceoryx2Wrapper,
    types::{
        clock::timestamp_nanos, common::{PassportId, Venue}, packet::{Packet, TSInternalMessage}, trade_server::{EngineTSMessage, Heartbeat, TSEngineMessage}
    },
};
use ringbuf::{HeapCons, HeapProd, HeapRb, traits::Consumer};
use crate::types::trade_server::EngineTSMessageType::*;
use crate::types::trade_server::TSEngineMessageType::*;


pub struct Central {
    execution_senders: HashMap<Venue, HeapProd<Packet<TSInternalMessage>>>,
    execution_receivers: HashMap<Venue, HeapCons<Packet<TSInternalMessage>>>,
    outer_protocol: TSIceoryx2Wrapper,
    passport_id: PassportId,
    // state management  here also..
}

impl Central {
    pub fn new(outer_protocol_name: String, passport_id: PassportId) -> Result<Self, String> {
        let protocol = TSIceoryx2Wrapper::new(outer_protocol_name)?;
        Ok(Self {
            execution_senders: HashMap::new(),
            execution_receivers: HashMap::new(),
            outer_protocol: protocol,
            passport_id,
        })
    }

    pub fn poll_outer_protocol(&mut self){
        match self.outer_protocol.recv() {
            Some(value) => {
                println!("Received {:?}", value);
                match value.message{
                    // PlaceOrder(place_order) => todo!(),
                    // CancelOrder(cancel_order) => todo!(),
                    // ReplaceOrder(replace_order) => todo!(),
                    // QryOpenOrders(qry_open_orders) => todo!(),
                    // QryPositions(qry_positions) => todo!(),
                    // QryBalance(qry_balance) => todo!(),
                    Heartbeat(heartbeat) => {
                        let res = self.outer_protocol.send(
                            TSEngineMessage{
                                timestamp: timestamp_nanos(),
                                message: HeartbeatResponse{}
                            }
                        );
                        match res {
                            Ok(_) => println!("SENT!"),
                            Err(_) => println!("wwtf"),
                        }
                    },
                    others => {
                        println!("Received {:?}", others);
                    }
                }
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
