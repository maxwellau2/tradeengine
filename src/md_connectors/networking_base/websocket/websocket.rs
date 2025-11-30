use crate::md_connectors::networking_base::websocket::message_handler::MessageHandler;
use crate::md_connectors::networking_base::websocket::websocket;
use crate::md_connectors::networking_base::websocket::websocket_error::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{error, info, trace, warn};

pub struct WebSocketClient<H: MessageHandler> {
    base_url: String,
    endpoint: String,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    handler: H,
}

impl<H: MessageHandler> WebSocketClient<H> {
    pub fn new(base_url: String, endpoint: String, handler: H) -> Self {
        Self {
            base_url,
            endpoint,
            ws_stream: None,
            handler,
        }
    }

    /// Connect once and send initial subscriptions
    pub async fn connect_once(&mut self) -> WsResult<()> {
        let url = format!("{}/{}", self.base_url, self.endpoint);
        let (ws_stream, _resp) = connect_async(url).await.map_err(WsError::Connect)?;
        self.ws_stream = Some(ws_stream);

        let subs = self.handler.on_connect().await?;
        for sub in subs {
            self.send(sub).await?;
        }

        Ok(())
    }

    pub async fn send(&mut self, payload: Value) -> WsResult<()> {
        if let Some(ws) = &mut self.ws_stream {
            let msg = Message::Text(payload.to_string());
            ws.send(msg).await.map_err(WsError::Connect)?;
        } else {
            return Err(WsError::NotConnected);
        }
        Ok(())
    }

    /// Run a single WS session until close/error
    pub async fn run_once(&mut self) -> WsResult<()> {
        let ws = self.ws_stream.as_mut().ok_or(WsError::NotConnected)?;

        while let Some(msg_result) = ws.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    self.handler.on_message(text).await?;
                }
                Ok(Message::Ping(data)) => {
                    ws.send(Message::Pong(data))
                        .await
                        .map_err(WsError::Connect)?;
                }
                Ok(Message::Close(frame)) => {
                    // notify handler, then end this session
                    self.handler.on_disconnect().await?;
                    warn!("[ws] remote close: {:?}", frame);
                    break;
                }
                Ok(_) => {
                    // ignore Binary, Pong, etc. for now
                }
                Err(e) => {
                    // Connection error, notify handler then return error
                    self.handler.on_disconnect().await.unwrap_or_else(|err| {
                        error!("[ws] on_disconnect error after conn error: {err}");
                    });
                    return Err(WsError::Connect(e));
                }
            }
        }

        Ok(())
    }

    /// Run forever with reconnect/backoff
    pub async fn run_forever(&mut self, backoff_secs: u64) -> WsResult<()> {
        loop {
            // 1) connect + subscribe
            if let Err(e) = self.connect_once().await {
                warn!("[ws] connect error: {e}");
                sleep(Duration::from_secs(backoff_secs)).await;
                continue;
            }

            // 2) run this session
            if let Err(e) = self.run_once().await {
                warn!("[ws] session error: {e}");
            } else {
                println!("connected!");
            }

            // 3) wait & reconnect
            warn!("[ws] session ended, reconnecting in {backoff_secs}s…");
            sleep(Duration::from_secs(backoff_secs)).await;
        }
    }

    pub async fn disconnect(&mut self) -> WsResult<()> {
        self.handler.on_disconnect().await?;
        if let Some(mut ws) = self.ws_stream.take() {
            ws.close(None).await.map_err(WsError::Connect)?;
        }
        Ok(())
    }
}
