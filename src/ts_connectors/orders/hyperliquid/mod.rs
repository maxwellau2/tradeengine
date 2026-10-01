pub mod error;
pub mod hyperliquid_utils;

use crate::config_parser::passport::PassportConfig;
use crate::ts_connectors::orders::Executor;
use crate::ts_connectors::orders::signature_utls::sign_l1_action;
use crate::types::clock::timestamp_millis;
use crate::types::common::{ClientOrderId, Order, PassportId, Side};
use crate::types::trade_server::OrderResponse;
use crate::types::trade_server::{self, EngineTSMessageType};
use error::{HLExecutorError, HyperliquidResult};
use ethers::prelude::LocalWallet;
use futures_util::{FutureExt, SinkExt, StreamExt};
pub use hyperliquid_utils::parse_cloid;
use hyperliquid_utils::{
    HyperliquidResponse, MetaResponse, format_cloid, format_price, format_size,
    parse_order_status_resp, tif_to_hl, to_cancel_resp, to_place_resp, to_replace_resp,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Interval, interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, trace, warn};
use tungstenite::Message;

const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
const MAINNET_HTTP_URL: &str = "https://api.hyperliquid.xyz";

pub struct HyperliquidExecutor {
    user_address: String,
    wallet: LocalWallet,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    is_mainnet: bool,
    vault_address: Option<String>,
    // symbol -> (asset index, sz_decimals)
    asset_map: HashMap<String, (u32, u8)>,
    req_id: AtomicU8,
    req_map: [Option<EngineTSMessageType>; 256],
    heartbeat_interval: Interval,
    // monotonic nonce to avoid duplicate nonce errors
    last_nonce: AtomicU64,
    // pending stale order queries: request_id -> cloid
    pending_stale_queries: HashMap<u8, ClientOrderId>,
}

impl HyperliquidExecutor {
    // build order action json
    fn build_order_action(
        asset_idx: u32,
        is_buy: bool,
        price: &str,
        size: &str,
        reduce_only: bool,
        tif: &str,
        cloid: Option<&str>,
    ) -> serde_json::Value {
        let mut order = json!({
            "a": asset_idx,
            "b": is_buy,
            "p": price,
            "s": size,
            "r": reduce_only,
            "t": {"limit": {"tif": tif}}
        });

        if let Some(id) = cloid {
            order["c"] = json!(id);
        }

        json!({
            "type": "order",
            "orders": [order],
            "grouping": "na"
        })
    }

    // build cancel action by cloid
    fn build_cancel_action(asset_idx: u32, cloid: &str) -> serde_json::Value {
        json!({
            "type": "cancelByCloid",
            "cancels": [{"asset": asset_idx, "cloid": cloid}]
        })
    }

    fn next_request_id(&mut self) -> u8 {
        self.req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// get next nonce, guaranteed to be monotonically increasing
    /// uses timestamp_millis but ensures it never returns the same value twice
    fn next_nonce(&self) -> u64 {
        let now = timestamp_millis();
        loop {
            let last = self.last_nonce.load(Ordering::Relaxed);
            let next = now.max(last + 1);
            if self
                .last_nonce
                .compare_exchange(last, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return next;
            }
            // another thread updated, retry
        }
    }

    // build modify action
    fn build_modify_action(
        cloid: &str,
        asset_idx: u32,
        is_buy: bool,
        price: &str,
        size: &str,
        reduce_only: bool,
        tif: &str,
    ) -> serde_json::Value {
        json!({
            "type": "modify",
            "oid": cloid,
            "order": {
                "a": asset_idx,
                "b": is_buy,
                "p": price,
                "s": size,
                "r": reduce_only,
                "t": {"limit": {"tif": tif}},
                "c": cloid
            },
        })
    }

    // wrap action in ws post format and send
    async fn ws_post(
        &mut self,
        action: serde_json::Value,
        request_id: u8,
    ) -> HyperliquidResult<()> {
        // auto-reconnect if disconnected
        if self.ws_stream.is_none() {
            self.connect().await;
        }

        // get nonce before borrowing ws_stream to avoid borrow conflict
        let nonce = self.next_nonce();
        let vault_ref = self.vault_address.as_deref();

        let ws = self
            .ws_stream
            .as_mut()
            .ok_or(HLExecutorError::NotConnected)?;

        // sign the action
        let sig = sign_l1_action(
            &self.wallet,
            &action,
            vault_ref,
            nonce,
            self.is_mainnet,
            None,
        )
        .map_err(|e| HLExecutorError::SigningFailed(e.to_string()))?;

        // build full payload
        let mut payload = json!({
            "action": action,
            "nonce": nonce,
            "signature": sig
        });

        if let Some(ref vault) = self.vault_address {
            payload["vaultAddress"] = json!(vault);
        }

        // wrap in ws post format
        let ws_msg = json!({
            "method": "post",
            "id": request_id,
            "request": {
                "type": "action",
                "payload": payload
            }
        });

        info!("ws post id={}: {:?}", request_id, ws_msg);

        let msg_str = serde_json::to_string(&ws_msg)?;
        if let Err(e) = ws.send(Message::Text(msg_str)).await {
            // connection broken, mark for reconnect
            warn!("ws send failed, marking for reconnect: {}", e);
            self.ws_stream = None;
            return Err(e.into());
        }

        Ok(())
    }

    // get asset info (index, sz_decimals) from symbol
    fn get_asset_info(&self, symbol: &str) -> Option<(u32, u8)> {
        if let Some(&info) = self.asset_map.get(symbol) {
            return Some(info);
        }
        // try base symbol (e.g. "BTC-PERP" -> "BTC")
        let base = symbol.split('-').next().unwrap_or(symbol);
        self.asset_map.get(base).copied()
    }

    /// send info request via ws (no signature needed)
    async fn ws_info(
        &mut self,
        payload: serde_json::Value,
        request_id: u8,
    ) -> HyperliquidResult<()> {
        if self.ws_stream.is_none() {
            self.connect().await;
        }

        let ws = self
            .ws_stream
            .as_mut()
            .ok_or(HLExecutorError::NotConnected)?;

        let ws_msg = json!({
            "method": "post",
            "id": request_id,
            "request": {
                "type": "info",
                "payload": payload
            }
        });

        info!("ws info id={}: {:?}", request_id, ws_msg);

        let msg_str = serde_json::to_string(&ws_msg)?;
        if let Err(e) = ws.send(Message::Text(msg_str)).await {
            warn!("ws send failed, marking for reconnect: {}", e);
            self.ws_stream = None;
            return Err(e.into());
        }

        Ok(())
    }

    // fetch asset mappings from /info meta endpoint
    // the asset index is the position in the universe array
    async fn load_asset_map(&mut self) -> HyperliquidResult<()> {
        let client = reqwest::Client::new();
        let res = client
            .post(format!("{}/info", MAINNET_HTTP_URL))
            .header("Content-Type", "application/json")
            .body(r#"{"type": "meta"}"#)
            .send()
            .await?;

        if !res.status().is_success() {
            return Err(HLExecutorError::MetaFailed(format!(
                "status: {}",
                res.status()
            )));
        }

        let meta: MetaResponse = res.json().await?;
        for (idx, asset) in meta.universe.iter().enumerate() {
            self.asset_map
                .insert(asset.name.clone(), (idx as u32, asset.sz_decimals));
        }
        info!(
            "loaded {} asset mappings from meta endpoint",
            self.asset_map.len()
        );
        Ok(())
    }

    /// parse ws text message and return typed response
    /// returns None if message is not a post response (e.g., error channel)
    pub fn parse(&mut self, text: &str) -> Option<OrderResponse> {
        debug!("parsing ws message: {}", text);

        // handle json pong response
        if text.contains(r#""channel":"pong""#) {
            debug!("received pong from server");
            return None;
        }

        // check if this is an info response (for stale order queries)
        if text.contains(r#""type":"info""#) {
            return self.parse_info_response(text);
        }

        let parsed = match HyperliquidResponse::from(text) {
            Ok(resp) => resp,
            Err(e) => {
                // might be error channel or other message type
                debug!("failed to parse ws message: {}", e);
                return None;
            }
        };

        debug!("parsed response: {:?}", parsed);

        // get the original request that triggered this response
        let request_id = parsed.request_id as usize;
        let intent = self.req_map[request_id].take()?;

        let response = match intent {
            EngineTSMessageType::PlaceOrder(req) => {
                OrderResponse::Place(to_place_resp(parsed, req))
            }
            EngineTSMessageType::CancelOrder(req) => {
                OrderResponse::Cancel(to_cancel_resp(parsed, req))
            }
            EngineTSMessageType::ReplaceOrder(req) => {
                OrderResponse::Replace(to_replace_resp(parsed, req))
            }
            _ => {
                warn!("unexpected request type in req_map");
                return None;
            }
        };

        Some(response)
    }

    /// parse info response (for stale order queries)
    fn parse_info_response(&mut self, text: &str) -> Option<OrderResponse> {
        // extract request id from the message to find the cloid
        let value: serde_json::Value = serde_json::from_str(text).ok()?;
        let request_id = value["data"]["id"].as_u64()? as u8;

        let cloid = self.pending_stale_queries.remove(&request_id)?;
        let resp = parse_order_status_resp(text, cloid)?;

        Some(OrderResponse::QueryStaleOrder(resp))
    }
}

#[async_trait::async_trait]
impl Executor for HyperliquidExecutor {
    type Error = HLExecutorError;

    async fn new(passport_id: PassportId, cfg_path: &str) -> HyperliquidResult<Self> {
        let cfg = PassportConfig::new(cfg_path)?;
        let passport = cfg.find_by_passport_id(passport_id)?;

        let cred = passport
            .details
            .hyperliquid
            .ok_or(HLExecutorError::CredentialsNotConfigured)?;

        let wallet: LocalWallet = cred
            .private_key
            .parse()
            .map_err(|_| HLExecutorError::PrivateKeyParseFailed)?;

        let is_mainnet = cred.is_mainnet.unwrap_or(true);
        let ws_url = if is_mainnet {
            MAINNET_WS_URL
        } else {
            TESTNET_WS_URL
        };

        let (ws_stream, _) = connect_async(ws_url).await?;
        info!("connected to hyperliquid ws: {}", ws_url);

        let mut executor = Self {
            user_address: cred.user_address,
            wallet,
            ws_stream: Some(ws_stream),
            is_mainnet,
            vault_address: cred.vault_address,
            asset_map: HashMap::new(),
            req_id: AtomicU8::new(0),
            req_map: [None; 256],
            heartbeat_interval: interval(Duration::from_secs(30)),
            last_nonce: AtomicU64::new(0),
            pending_stale_queries: HashMap::new(),
        };
        executor.load_asset_map().await?;

        Ok(executor)
    }

    async fn connect(&mut self) {
        if self.ws_stream.is_none() {
            let ws_url = if self.is_mainnet {
                MAINNET_WS_URL
            } else {
                TESTNET_WS_URL
            };
            match connect_async(ws_url).await {
                Ok((ws, _)) => {
                    self.ws_stream = Some(ws);
                    // reset heartbeat interval so we don't immediately ping
                    self.heartbeat_interval.reset();
                    info!("reconnected to hyperliquid ws");
                }
                Err(e) => error!("failed to reconnect: {}", e),
            }
        }
    }

    async fn place_order(&mut self, order: trade_server::PlaceOrder) -> HyperliquidResult<()> {
        let symbol = order.symbol.as_str();
        let (asset_idx, sz_decimals) = self
            .get_asset_info(symbol)
            .ok_or_else(|| HLExecutorError::UnknownSymbol(symbol.to_string()))?;

        let is_buy = matches!(order.side, Side::LONG);
        let price = format_price(order.price, sz_decimals);
        let size = format_size(order.qty, sz_decimals);
        info!(
            "place_order: raw_qty={}, sz_decimals={}, formatted_size={}, formatted_price={}",
            order.qty, sz_decimals, size, price
        );
        let tif = tif_to_hl(order.time_in_force);
        let cloid_raw = order.client_order_id.as_str();
        let cloid_formatted = if cloid_raw.is_empty() {
            None
        } else {
            Some(format_cloid(cloid_raw))
        };

        let action = HyperliquidExecutor::build_order_action(
            asset_idx,
            is_buy,
            &price,
            &size,
            false,
            tif,
            cloid_formatted.as_deref(),
        );
        let request_id = self.next_request_id();

        self.ws_post(action, request_id).await?;
        self.req_map[request_id as usize] = Some(EngineTSMessageType::PlaceOrder(order));
        debug!("place_order sent: {}", symbol);
        Ok(())
    }

    async fn cancel_order(&mut self, cancel: trade_server::CancelOrder) -> HyperliquidResult<()> {
        let symbol = cancel.symbol.as_str();
        let (asset_idx, _) = self
            .get_asset_info(symbol)
            .ok_or_else(|| HLExecutorError::UnknownSymbol(symbol.to_string()))?;

        let cloid_raw = cancel.client_order_id.as_str();
        if cloid_raw.is_empty() {
            return Err(HLExecutorError::MissingCloid);
        }
        let cloid = format_cloid(cloid_raw);

        let action = HyperliquidExecutor::build_cancel_action(asset_idx, &cloid);
        let request_id = self.next_request_id();

        self.ws_post(action, request_id).await?;
        self.req_map[request_id as usize] = Some(EngineTSMessageType::CancelOrder(cancel));
        debug!("cancel_order sent: {} cloid={}", symbol, cloid);
        Ok(())
    }

    async fn replace_order(
        &mut self,
        replace: trade_server::ReplaceOrder,
    ) -> HyperliquidResult<()> {
        let symbol = replace.symbol.as_str();
        let (asset_idx, sz_decimals) = self
            .get_asset_info(symbol)
            .ok_or_else(|| HLExecutorError::UnknownSymbol(symbol.to_string()))?;

        let cloid_raw = replace.client_order_id.as_str();
        if cloid_raw.is_empty() {
            return Err(HLExecutorError::MissingCloid);
        }
        let cloid = format_cloid(cloid_raw);

        let is_buy = matches!(replace.side, Side::LONG);
        let price = format_price(replace.new_price, sz_decimals);
        let size = format_size(replace.new_qty, sz_decimals);
        let tif = tif_to_hl(replace.time_in_force);

        let action = HyperliquidExecutor::build_modify_action(
            &cloid, asset_idx, is_buy, &price, &size, false, tif,
        );

        let request_id = self.next_request_id();
        self.ws_post(action, request_id).await?;
        self.req_map[request_id as usize] = Some(EngineTSMessageType::ReplaceOrder(replace));
        debug!("replace_order sent: {} cloid={}", symbol, cloid);
        Ok(())
    }

    async fn query_order_status(&mut self, cloid: ClientOrderId) -> HyperliquidResult<()> {
        let cloid_formatted = format_cloid(cloid.as_str());

        let payload = json!({
            "type": "orderStatus",
            "user": self.user_address,
            "oid": cloid_formatted
        });

        let request_id = self.next_request_id();
        self.ws_info(payload, request_id).await?;
        self.pending_stale_queries.insert(request_id, cloid);

        debug!("query_order_status sent: cloid={}", cloid_formatted);
        Ok(())
    }

    async fn produce(&mut self) -> Option<OrderResponse> {
        // reconnect if needed
        if self.ws_stream.is_none() {
            self.connect().await;
            return None;
        }

        let ws = self.ws_stream.as_mut()?;

        // check heartbeat first (non-blocking via poll_tick)
        let should_ping = std::future::poll_fn(|cx| match self.heartbeat_interval.poll_tick(cx) {
            std::task::Poll::Ready(_) => std::task::Poll::Ready(true),
            std::task::Poll::Pending => std::task::Poll::Ready(false),
        })
        .await;

        if should_ping {
            trace!("sending heartbeat ping");
            let ping_msg = Message::Text(r#"{"method":"ping"}"#.to_string());
            // actually await the send
            if let Err(e) = ws.send(ping_msg).await {
                warn!("heartbeat ping failed, marking for reconnect: {}", e);
                self.ws_stream = None;
                return None;
            }
        }

        // non-blocking check for ws message using now_or_never
        // this returns immediately if no data is ready
        let msg = ws.next().now_or_never();

        match msg {
            Some(Some(Ok(Message::Text(txt)))) => {
                return self.parse(&txt);
            }
            Some(Some(Ok(Message::Ping(data)))) => {
                // must await the pong send to ensure it actually goes out
                if let Err(e) = ws.send(Message::Pong(data)).await {
                    warn!("pong send failed, marking for reconnect: {}", e);
                    self.ws_stream = None;
                }
            }
            Some(Some(Ok(Message::Pong(_)))) => {
                trace!("received ws pong frame");
            }
            Some(Some(Ok(Message::Close(_)))) | Some(Some(Err(_))) | Some(None) => {
                warn!("ws disconnected, will reconnect");
                self.ws_stream = None;
            }
            // None means no data ready - this is the expected fast path
            None => {}
            _ => {}
        }
        None
    }
}

pub mod test {
    use std::time::Duration;

    use crate::types::{
        common::{ClientOrderId, OrderType, Symbol, TimeInForce, Venue},
        trade_server::{CancelOrder, PlaceOrder, ReplaceOrder},
    };

    use super::*;

    #[tokio::test]
    pub async fn create_executor() {
        let hl = HyperliquidExecutor::new(
            1234,
            "/home/maxwell/dev/personal/mdfeed/md_feed/DO_NOT_COMMIT/maxwell_passport.json",
        )
        .await;
        match hl {
            Ok(mut executor) => {
                executor
                    .place_order(PlaceOrder::new(
                        Symbol::new("BTC"),
                        Venue::Hyperliquid,
                        ClientOrderId::new("123"),
                        80000.0,
                        0.001,
                        Side::LONG,
                        TimeInForce::PO,
                        OrderType::LIMIT,
                        1234,
                    ))
                    .await
                    .unwrap();
                // let it chill for abit
                let _ = tokio::time::sleep(Duration::from_secs(2)).await;
                executor.produce().await;

                executor
                    .replace_order(ReplaceOrder::new(
                        Symbol::new("BTC"),
                        Venue::Hyperliquid,
                        ClientOrderId::new("12332423d"),
                        71000.0,
                        0.002,
                        Side::LONG,
                        TimeInForce::PO,
                        OrderType::LIMIT,
                        1234,
                    ))
                    .await
                    .unwrap();
                let _ = tokio::time::sleep(Duration::from_secs(2)).await;
                executor.produce().await;
                executor
                    .cancel_order(CancelOrder::new(
                        Symbol::new("BTC"),
                        Venue::Hyperliquid,
                        ClientOrderId::new("123"),
                        1234,
                    ))
                    .await
                    .unwrap();
                let _ = tokio::time::sleep(Duration::from_secs(2)).await;
                executor.produce().await;
            }
            Err(e) => {
                panic!("error {e}")
            }
        }
    }
}
