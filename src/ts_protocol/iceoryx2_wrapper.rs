use iceoryx2::prelude::*;

use crate::{
    strategy::context::{OrderGatewayRecv, OrderGatewaySend},
    types::trade_server::{EngineTSMessage, TSEngineMessage},
};

const SHM_IPC_Q_SIZE: usize = 1 << 10;

/// This struct wraps the iceoryx2 publisher and implements the `OrderGatewaySend` trait.
/// It's given to the strategy context so strategies can send orders to the trade server.
pub struct EngineTSSender {
    publisher: iceoryx2::port::publisher::Publisher<ipc::Service, EngineTSMessage, ()>,
}

impl OrderGatewaySend for EngineTSSender {
    fn send(&mut self, message: EngineTSMessage) -> Result<(), String> {
        // Loan an uninitialized sample from shared memory
        // This is zero-copy: we write directly to shared memory
        let sample = self
            .publisher
            .loan_uninit()
            .map_err(|e| format!("Failed to loan sample: {:?}", e))?;

        // Write our message payload into the shared memory
        let sample = sample.write_payload(message);

        // Send just notifies the subscriber - data is already in shared memory
        sample
            .send()
            .map_err(|e| format!("Failed to send: {:?}", e))?;

        Ok(())
    }
}

/// Receiver side - receives messages from TradeServer to Engine
pub struct TSEngineReceiver {
    subscriber: iceoryx2::port::subscriber::Subscriber<ipc::Service, TSEngineMessage, ()>,
}

impl OrderGatewayRecv for TSEngineReceiver {
    fn recv(&mut self) -> Option<TSEngineMessage> {
        // Non-blocking check for new messages
        match self.subscriber.receive() {
            Ok(Some(sample)) => {
                // Dereference the sample to copy the message
                // The original stays in shared memory
                tracing::debug!("engine recv: got message from TS");
                Some(*sample)
            }
            Ok(None) => None, // No message available
            Err(e) => {
                tracing::error!("engine recv error: {:?}", e);
                None
            }
        }
    }
}

pub struct EngineIceoryx2Wrapper {
    channel_name: String,
    publisher: iceoryx2::port::publisher::Publisher<ipc::Service, EngineTSMessage, ()>,
    subscriber: iceoryx2::port::subscriber::Subscriber<ipc::Service, TSEngineMessage, ()>,
}

impl EngineIceoryx2Wrapper {
    /// Create a new bidirectional iceoryx2 wrapper for the engine side
    pub fn new(channel_name: String) -> Result<Self, String> {
        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|e| format!("Failed to create node: {:?}", e))?;

        // Engine sends EngineTSMessage on "engine_to_ts"
        let send_name = format!("{}/engine_to_ts", channel_name);
        let service_name_send =
            ServiceName::new(&send_name).map_err(|e| format!("Invalid service name: {:?}", e))?;
        let service_send = node
            .service_builder(&service_name_send)
            .publish_subscribe::<EngineTSMessage>()
            .subscriber_max_buffer_size(SHM_IPC_Q_SIZE)
            .open_or_create()
            .map_err(|e| format!("Failed to open/create send service: {:?}", e))?;

        // Engine receives TSEngineMessage on "ts_to_engine"
        let recv_name = format!("{}/ts_to_engine", channel_name);
        let service_name_recv =
            ServiceName::new(&recv_name).map_err(|e| format!("Invalid service name: {:?}", e))?;
        let service_recv = node
            .service_builder(&service_name_recv)
            .publish_subscribe::<TSEngineMessage>()
            // increase buffer to avoid dropping messages during query responses
            .subscriber_max_buffer_size(SHM_IPC_Q_SIZE)
            .open_or_create()
            .map_err(|e| format!("Failed to open/create recv service: {:?}", e))?;

        let publisher = service_send
            .publisher_builder()
            .create()
            .map_err(|e| format!("Failed to create publisher: {:?}", e))?;

        let subscriber = service_recv
            .subscriber_builder()
            .create()
            .map_err(|e| format!("Failed to create subscriber: {:?}", e))?;

        Ok(Self {
            channel_name,
            publisher,
            subscriber,
        })
    }

    /// Split the wrapper into separate sender and receiver
    ///
    /// **Concept Explanation:**
    /// This method consumes `self` (takes ownership) and returns two new structs.
    /// In Rust, when a function takes `self` (not `&self` or `&mut self`), it means
    /// the original struct is "moved" and can no longer be used.
    ///
    /// This is perfect for our use case - we create the wrapper, immediately split it,
    /// and never use the combined wrapper again.
    ///
    /// Returns: (sender, receiver) tuple
    /// - sender: Given to TradeServerGateway wrapper, then to strategy context via Rc<RefCell<>>
    /// - receiver: Kept by the engine to poll in the main loop
    pub fn split(self) -> (EngineTSSender, TSEngineReceiver) {
        (
            EngineTSSender {
                publisher: self.publisher,
            },
            TSEngineReceiver {
                subscriber: self.subscriber,
            },
        )
    }
}

// ============================================================================
// TradeServer Side Wrapper
// ============================================================================

pub struct TSIceoryx2Wrapper {
    channel_name: String,
    publisher: iceoryx2::port::publisher::Publisher<ipc::Service, TSEngineMessage, ()>,
    subscriber: iceoryx2::port::subscriber::Subscriber<ipc::Service, EngineTSMessage, ()>,
}

impl TSIceoryx2Wrapper {
    pub fn new(channel_name: String) -> Result<Self, String> {
        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|e| format!("Failed to create node: {:?}", e))?;

        // TS sends TSEngineMessage on "ts_to_engine"
        let send_name = format!("{}/ts_to_engine", channel_name);
        let service_name_send =
            ServiceName::new(&send_name).map_err(|e| format!("Invalid service name: {:?}", e))?;
        let service_send = node
            .service_builder(&service_name_send)
            .publish_subscribe::<TSEngineMessage>()
            // increase buffer to avoid dropping messages during query responses
            .subscriber_max_buffer_size(SHM_IPC_Q_SIZE)
            .open_or_create()
            .map_err(|e| format!("Failed to open/create send service: {:?}", e))?;

        // TS receives EngineTSMessage on "engine_to_ts"
        let recv_name = format!("{}/engine_to_ts", channel_name);
        let service_name_recv =
            ServiceName::new(&recv_name).map_err(|e| format!("Invalid service name: {:?}", e))?;
        let service_recv = node
            .service_builder(&service_name_recv)
            .publish_subscribe::<EngineTSMessage>()
            .subscriber_max_buffer_size(SHM_IPC_Q_SIZE)
            .open_or_create()
            .map_err(|e| format!("Failed to open/create recv service: {:?}", e))?;

        let publisher = service_send
            .publisher_builder()
            .create()
            .map_err(|e| format!("Failed to create publisher: {:?}", e))?;

        let subscriber = service_recv
            .subscriber_builder()
            .create()
            .map_err(|e| format!("Failed to create subscriber: {:?}", e))?;

        Ok(Self {
            channel_name,
            publisher,
            subscriber,
        })
    }

    pub fn send(&mut self, packet: TSEngineMessage) -> Result<(), String> {
        let sample = self
            .publisher
            .loan_uninit()
            .map_err(|e| format!("Failed to loan sample: {:?}", e))?;

        let sample = sample.write_payload(packet);

        sample
            .send()
            .map_err(|e| format!("Failed to send: {:?}", e))?;

        Ok(())
    }

    pub fn recv(&mut self) -> Option<EngineTSMessage> {
        match self.subscriber.receive() {
            Ok(Some(sample)) => Some(*sample),
            Ok(None) => None,
            Err(_) => None,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod test {
    use crate::types::common::{Order, OrderState, PassportId};

    use super::*;

    #[test]
    fn test_create_both_sides() -> Result<(), Box<dyn std::error::Error>> {
        let channel_name = "testchannel";

        let _engine = EngineIceoryx2Wrapper::new(channel_name.into())?;
        let _ts = TSIceoryx2Wrapper::new(channel_name.into())?;

        Ok(())
    }

    #[test]
    fn test_send_recv() -> Result<(), Box<dyn std::error::Error>> {
        use crate::types::common::{ClientOrderId, OrderType, Side, Symbol, TimeInForce, Venue};
        use crate::types::trade_server::{EngineTSMessageType, PlaceOrder, TSEngineMessageType};
        use std::thread;
        use std::time::Duration;

        let channel_name = "test_send_recv_ch";

        // Create both sides
        let engine_wrapper = EngineIceoryx2Wrapper::new(channel_name.into())?;
        let mut ts = TSIceoryx2Wrapper::new(channel_name.into())?;

        // Split engine wrapper into sender and receiver
        let (mut engine_sender, mut engine_receiver) = engine_wrapper.split();

        // Give iceoryx2 time to set up connections
        thread::sleep(Duration::from_millis(100));

        // ========================================================================
        // Test 1: Engine sends PlaceOrder to TradeServer
        // ========================================================================
        println!("Test 1: Engine → TradeServer (PlaceOrder)");

        let original_order = PlaceOrder {
            symbol: Symbol::new("ETHUSDT"),
            venue: Venue::Binance,
            client_order_id: ClientOrderId::new("order_abc123"),
            price: 3456.78,
            qty: 2.5,
            side: Side::SHORT,
            time_in_force: TimeInForce::IOC,
            order_type: OrderType::LIMIT,
            passport_id: 123,
        };

        let engine_msg = EngineTSMessage {
            timestamp: 1234567890,
            message: EngineTSMessageType::PlaceOrder(original_order),
        };

        // Send from Engine
        engine_sender.send(engine_msg)?;
        println!("  ✓ Engine sent PlaceOrder");

        // Allow message propagation
        thread::sleep(Duration::from_millis(50));

        // Receive on TradeServer
        let received = ts.recv();
        assert!(received.is_some(), "TradeServer should receive message");
        let received_msg = received.unwrap();

        // Verify timestamp
        assert_eq!(received_msg.timestamp, 1234567890, "Timestamp mismatch");
        println!("  ✓ Timestamp verified: {}", received_msg.timestamp);

        // Verify message body - extract PlaceOrder and check all fields
        match received_msg.message {
            EngineTSMessageType::PlaceOrder(received_order) => {
                // Verify each field matches
                assert_eq!(
                    received_order.symbol.as_str(),
                    original_order.symbol.as_str(),
                    "Symbol mismatch"
                );
                assert_eq!(received_order.venue, original_order.venue, "Venue mismatch");
                assert_eq!(
                    received_order.client_order_id.as_str(),
                    original_order.client_order_id.as_str(),
                    "ClientOrderId mismatch"
                );
                assert_eq!(received_order.price, original_order.price, "Price mismatch");
                assert_eq!(received_order.qty, original_order.qty, "Qty mismatch");
                assert_eq!(received_order.side, original_order.side, "Side mismatch");
                assert_eq!(
                    received_order.time_in_force, original_order.time_in_force,
                    "TimeInForce mismatch"
                );
                assert_eq!(
                    received_order.order_type, original_order.order_type,
                    "OrderType mismatch"
                );

                println!("  ✓ All PlaceOrder fields verified (zero data loss):");
                println!("    - Symbol: {}", received_order.symbol);
                println!("    - Venue: {:?}", received_order.venue);
                println!("    - OrderId: {}", received_order.client_order_id);
                println!("    - Price: {}", received_order.price);
                println!("    - Qty: {}", received_order.qty);
                println!("    - Side: {:?}", received_order.side);
                println!("    - TIF: {:?}", received_order.time_in_force);
                println!("    - Type: {:?}", received_order.order_type);
            }
            _ => panic!("Expected PlaceOrder, got different message type"),
        }

        // ========================================================================
        // Test 2: TradeServer sends OrderUpdate back to Engine
        // ========================================================================
        println!("\nTest 2: TradeServer → Engine (OrderUpdate)");

        let ts_response = TSEngineMessage {
            timestamp: 9876543210,
            message: TSEngineMessageType::OrderUpdate(Order {
                symbol: Symbol::new("test"),
                venue: Venue::Hyperliquid,
                side: Side::LONG,
                client_order_id: ClientOrderId::new("12345"),
                qty: 123.4,
                filled_qty: 0.0,
                price: 123.3,
                order_type: OrderType::LIMIT,
                time_in_force: TimeInForce::GTC,
                state: OrderState::NEW,
            }),
        };

        // Send from TradeServer
        ts.send(ts_response)?;
        println!("  ✓ TradeServer sent OrderUpdate");

        // Allow message propagation
        thread::sleep(Duration::from_millis(50));

        // Receive on Engine
        let received = engine_receiver.recv();
        assert!(received.is_some(), "Engine should receive message");
        let received_msg = received.unwrap();
        println!("  ✓ Engine received: timestamp={}", received_msg.timestamp);
        assert_eq!(received_msg.timestamp, 9876543210);

        // ========================================================================
        // Test 3: Verify no extra messages
        // ========================================================================
        println!("\nTest 3: Verify no extra messages");
        assert!(
            engine_receiver.recv().is_none(),
            "No extra messages on engine"
        );
        assert!(ts.recv().is_none(), "No extra messages on ts");
        println!("  ✓ No spurious messages");

        println!("\n✅ All send/recv tests passed!");
        Ok(())
    }
    #[test]
    fn test_send_recv_twice_in_a_row() -> Result<(), Box<dyn std::error::Error>> {
        use crate::types::common::{ClientOrderId, OrderType, Side, Symbol, TimeInForce, Venue};
        use crate::types::trade_server::{EngineTSMessageType, PlaceOrder, TSEngineMessageType};
        use std::thread;
        use std::time::Duration;

        let channel_name = "test_send_twice_ch";

        // Create both sides
        let engine_wrapper = EngineIceoryx2Wrapper::new(channel_name.into())?;
        let mut ts = TSIceoryx2Wrapper::new(channel_name.into())?;

        // Split engine wrapper into sender and receiver
        let (mut engine_sender, mut engine_receiver) = engine_wrapper.split();

        // Give iceoryx2 time to set up connections
        thread::sleep(Duration::from_millis(100));

        // ========================================================================
        // Test: Send two messages in a row, verify both are received correctly
        // ========================================================================
        println!("Test: Engine → TradeServer (2 PlaceOrders in a row)");

        // First order
        let order1 = PlaceOrder {
            symbol: Symbol::new("BTC"),
            venue: Venue::Hyperliquid,
            client_order_id: ClientOrderId::new("order_123"),
            price: 50000.0,
            qty: 1.0,
            side: Side::LONG,
            time_in_force: TimeInForce::GTC,
            order_type: OrderType::LIMIT,
            passport_id: 123,
        };

        let engine_msg1 = EngineTSMessage {
            timestamp: 1111111111,
            message: EngineTSMessageType::PlaceOrder(order1),
        };

        // Second order
        let order2 = PlaceOrder {
            symbol: Symbol::new("ETH"),
            venue: Venue::Binance,
            client_order_id: ClientOrderId::new("order_456"),
            price: 3000.0,
            qty: 2.5,
            side: Side::SHORT,
            time_in_force: TimeInForce::IOC,
            order_type: OrderType::MARKET,
            passport_id: 123,
        };

        let engine_msg2 = EngineTSMessage {
            timestamp: 2222222222,
            message: EngineTSMessageType::PlaceOrder(order2),
        };

        // Send both messages
        engine_sender.send(engine_msg1)?;
        println!("  ✓ Engine sent PlaceOrder #1 (BTC)");

        engine_sender.send(engine_msg2)?;
        println!("  ✓ Engine sent PlaceOrder #2 (ETH)");

        // Allow message propagation
        thread::sleep(Duration::from_millis(50));

        // Receive first message
        let received1 = ts.recv();
        assert!(received1.is_some(), "TradeServer should receive message 1");
        let received_msg1 = received1.unwrap();
        assert_eq!(received_msg1.timestamp, 1111111111, "Message 1 timestamp");
        println!(
            "  ✓ TradeServer received message #1: timestamp={}",
            received_msg1.timestamp
        );

        // Verify first message body
        match received_msg1.message {
            EngineTSMessageType::PlaceOrder(recv_order) => {
                assert_eq!(recv_order.symbol.as_str(), "BTC", "Order 1 symbol");
                assert_eq!(recv_order.venue, Venue::Hyperliquid, "Order 1 venue");
                assert_eq!(recv_order.price, 50000.0, "Order 1 price");
                println!(
                    "    - Symbol: {}, Price: {}",
                    recv_order.symbol, recv_order.price
                );
            }
            _ => panic!("Expected PlaceOrder in message 1"),
        }

        // Receive second message
        let received2 = ts.recv();
        assert!(received2.is_some(), "TradeServer should receive message 2");
        let received_msg2 = received2.unwrap();
        assert_eq!(received_msg2.timestamp, 2222222222, "Message 2 timestamp");
        println!(
            "  ✓ TradeServer received message #2: timestamp={}",
            received_msg2.timestamp
        );

        // Verify second message body
        match received_msg2.message {
            EngineTSMessageType::PlaceOrder(recv_order) => {
                assert_eq!(recv_order.symbol.as_str(), "ETH", "Order 2 symbol");
                assert_eq!(recv_order.venue, Venue::Binance, "Order 2 venue");
                assert_eq!(recv_order.price, 3000.0, "Order 2 price");
                assert_eq!(recv_order.qty, 2.5, "Order 2 qty");
                assert_eq!(recv_order.side, Side::SHORT, "Order 2 side");
                println!(
                    "    - Symbol: {}, Price: {}, Qty: {}",
                    recv_order.symbol, recv_order.price, recv_order.qty
                );
            }
            _ => panic!("Expected PlaceOrder in message 2"),
        }
        // verify no extra messages
        // ========================================================================
        println!("\nTest 2: Verify no extra messages");
        assert!(
            engine_receiver.recv().is_none(),
            "No extra messages on engine"
        );
        assert!(ts.recv().is_none(), "No extra messages on ts");
        println!("  ✓ No spurious messages");

        println!("\n✅ All send/recv tests passed!");
        Ok(())
    }
}
