use crate::{
    exchange_connectors::networking_base::websocket::websocket_error::*,
    types::packet::{MDMessage, Packet},
};
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait MessageHandler: Send {
    /// Parse and handle incoming message - exchange-specific
    async fn on_message(&mut self, text: String) -> WsResult<()>;

    /// Return subscription JSON payloads to send after each connect
    fn get_subscription_messages(&self) -> Vec<Value>;

    fn publish(&mut self, packet: Packet<MDMessage>);

    /// Called after connect; default is to use get_subscription_messages()
    async fn on_connect(&mut self) -> WsResult<Vec<Value>> {
        println!("connected!");
        Ok(self.get_subscription_messages())
    }

    /// Called before/after disconnect; default: no-op
    async fn on_disconnect(&mut self) -> WsResult<()> {
        Ok(())
    }
}
