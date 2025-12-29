pub mod error;
pub mod hyperliquid_utils;

use crate::config_parser::passport::PassportConfig;
use crate::ts_connectors::orders::Executor;
use crate::ts_connectors::orders::signature_utls::sign_l1_action;
use crate::types::clock::timestamp_millis;
use crate::types::common::{PassportId, Side};
use crate::types::trade_server::OrderResponse;
use crate::types::trade_server::{self, EngineTSMessageType};
use error::{HLExecutorError, HyperliquidResult};
use ethers::prelude::LocalWallet;
use futures_util::{SinkExt, StreamExt};
pub use hyperliquid_utils::parse_cloid;
use hyperliquid_utils::{
    HyperliquidResponse, MetaResponse, format_cloid, format_float, tif_to_hl, to_cancel_resp,
    to_place_resp, to_replace_resp,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
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
    #[allow(dead_code)] // may be used for order response validation
    user_address: String,
    wallet: LocalWallet,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    is_mainnet: bool,
    vault_address: Option<String>,
    // symbol -> asset index mapping
    asset_map: HashMap<String, u32>,
    req_id: AtomicU8,
    req_map: [Option<EngineTSMessageType>; u8::MAX as usize],
    heartbeat_interval: Interval,
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

        let ws = self
            .ws_stream
            .as_mut()
            .ok_or(HLExecutorError::NotConnected)?;

        let nonce = timestamp_millis();
        let vault_ref = self.vault_address.as_deref();

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

        debug!("ws post id={}: {:?}", request_id, ws_msg);

        let msg_str = serde_json::to_string(&ws_msg)?;
        ws.send(Message::Text(msg_str)).await?;

        Ok(())
    }

    // get asset index from symbol
    fn get_asset_index(&self, symbol: &str) -> Option<u32> {
        if let Some(&idx) = self.asset_map.get(symbol) {
            return Some(idx);
        }
        // try base symbol (e.g. "BTC-PERP" -> "BTC")
        let base = symbol.split('-').next().unwrap_or(symbol);
        self.asset_map.get(base).copied()
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
            self.asset_map.insert(asset.name.clone(), idx as u32);
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
}

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
            req_map: [None; u8::MAX as usize],
            heartbeat_interval: interval(Duration::from_secs(30)),
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
                    info!("reconnected to hyperliquid ws");
                }
                Err(e) => error!("failed to reconnect: {}", e),
            }
        }
    }

    async fn place_order(&mut self, order: trade_server::PlaceOrder) -> HyperliquidResult<()> {
        let symbol = order.symbol.as_str();
        let asset_idx = self
            .get_asset_index(symbol)
            .ok_or_else(|| HLExecutorError::UnknownSymbol(symbol.to_string()))?;

        let is_buy = matches!(order.side, Side::LONG);
        let price = format_float(order.price);
        let size = format_float(order.qty);
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
        let asset_idx = self
            .get_asset_index(symbol)
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
        let asset_idx = self
            .get_asset_index(symbol)
            .ok_or_else(|| HLExecutorError::UnknownSymbol(symbol.to_string()))?;

        let cloid_raw = replace.client_order_id.as_str();
        if cloid_raw.is_empty() {
            return Err(HLExecutorError::MissingCloid);
        }
        let cloid = format_cloid(cloid_raw);

        let is_buy = matches!(replace.side, Side::LONG);
        let price = format_float(replace.new_price);
        let size = format_float(replace.new_qty);
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

    async fn produce(&mut self) -> Option<OrderResponse> {
        // reconnect if needed
        if self.ws_stream.is_none() {
            self.connect().await;
            return None;
        }

        let ws = self.ws_stream.as_mut()?;

        tokio::select! {
            biased;

            // non-blocking poll with immediate timeout
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        return self.parse(&txt);
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        trace!("received ws pong frame");
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        warn!("ws disconnected, will reconnect");
                        self.ws_stream = None;
                    }
                    _ => {}
                }
            }
            _ = self.heartbeat_interval.tick() => {
                trace!("sending heartbeat ping");
                let ping_msg = Message::Text(r#"{"method":"ping"}"#.to_string());
                if ws.send(ping_msg).await.is_err() {
                    warn!("heartbeat failed, ws disconnected");
                    self.ws_stream = None;
                }
            }
            // yield immediately if no data ready
            _ = tokio::task::yield_now() => {}
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
