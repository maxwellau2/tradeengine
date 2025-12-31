use crate::md_connectors::networking_base::websocket::message_handler::MessageHandler;
use crate::md_connectors::networking_base::websocket::websocket_error::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::time::{Duration, Interval, interval, sleep};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, trace, warn};

const HEARTBEAT_INTERVAL_SECS: u64 = 30;

pub struct WebSocketClient<H: MessageHandler> {
    base_url: String,
    endpoint: String,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    handler: H,
    heartbeat_interval: Interval,
}

impl<H: MessageHandler> WebSocketClient<H> {
    pub fn new(base_url: String, endpoint: String, handler: H) -> Self {
        Self {
            base_url,
            endpoint,
            ws_stream: None,
            handler,
            heartbeat_interval: interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)),
        }
    }

    /// connect once and send initial subscriptions
    pub async fn connect_once(&mut self) -> WsResult<()> {
        let url = format!("{}/{}", self.base_url, self.endpoint);
        let (ws_stream, _resp) = connect_async(&url).await.map_err(WsError::Connect)?;
        self.ws_stream = Some(ws_stream);

        // reset heartbeat interval on new connection
        self.heartbeat_interval = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));

        let subs = self.handler.on_connect().await?;
        for sub in &subs {
            debug!("[ws] sending subscription: {}", sub);
            self.send(sub.clone()).await?;
        }

        debug!(
            "[ws] connected to {} with {} subscriptions",
            url,
            subs.len()
        );
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

    /// run a single ws session until close/error
    pub async fn run_once(&mut self) -> WsResult<()> {
        loop {
            let ws = match self.ws_stream.as_mut() {
                Some(ws) => ws,
                None => return Err(WsError::NotConnected),
            };

            tokio::select! {
                biased;

                // check for incoming messages
                msg_opt = ws.next() => {
                    match msg_opt {
                        Some(Ok(Message::Text(text))) => {
                            // ignore pong responses
                            if text.contains("\"channel\":\"pong\"") {
                                trace!("[ws] received pong");
                                continue;
                            }
                            self.handler.on_message(text).await?;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            if let Some(ws) = self.ws_stream.as_mut() {
                                ws.send(Message::Pong(data))
                                    .await
                                    .map_err(WsError::Connect)?;
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            self.handler.on_disconnect().await?;
                            warn!("[ws] remote close: {:?}", frame);
                            return Ok(());
                        }
                        Some(Ok(_)) => {
                            // ignore binary, pong, etc
                        }
                        Some(Err(e)) => {
                            self.handler.on_disconnect().await.unwrap_or_else(|err| {
                                error!("[ws] on_disconnect error: {err}");
                            });
                            return Err(WsError::Connect(e));
                        }
                        None => {
                            // stream ended
                            self.handler.on_disconnect().await?;
                            warn!("[ws] stream ended");
                            return Ok(());
                        }
                    }
                }

                // send heartbeat every 30 seconds
                _ = self.heartbeat_interval.tick() => {
                    trace!("[ws] sending heartbeat");
                    let ping = json!({"method": "ping"});
                    if let Some(ws) = self.ws_stream.as_mut() {
                        if let Err(e) = ws.send(Message::Text(ping.to_string())).await {
                            warn!("[ws] heartbeat failed: {e}");
                            self.handler.on_disconnect().await.unwrap_or_else(|err| {
                                error!("[ws] on_disconnect error: {err}");
                            });
                            return Err(WsError::Connect(e));
                        }
                    }
                }
            }
        }
    }

    /// run forever with reconnect/backoff
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
            }

            // 3) clear stream and wait before reconnect
            self.ws_stream = None;
            warn!("[ws] session ended, reconnecting in {backoff_secs}s...");
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
