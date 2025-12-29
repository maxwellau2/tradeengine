use std::collections::HashSet;

use crate::{
    config_parser::passport::PassportConfig,
    ts_connectors::{
        orders::hyperliquid::parse_cloid,
        state::{
            StateSubscriber,
            error::{StateError, StateResult},
            hyperliquid::error::{HLStateError, HLStateResult},
        },
    },
    types::common::{
        Balance, ClientOrderId, Order, OrderState, OrderType, PassportId, Position, Side, Symbol,
        TimeInForce, Venue,
    },
    types::trade_server::StateUpdate,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use ringbuf::{
    HeapRb,
    traits::{Consumer, Producer},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{net::TcpStream, time::Interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, trace, warn};
use tungstenite::Message;

pub mod error;

const RING_BUF_CAPACITY: usize = 1024;

// --- serde structs for parsing ws messages ---

#[derive(Debug, Deserialize)]
#[serde(tag = "channel")]
#[serde(rename_all = "camelCase")]
enum HLStateMessage {
    ClearinghouseState {
        data: ClearinghouseData,
    },
    OrderUpdates {
        data: Vec<HLOrderUpdate>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearinghouseData {
    clearinghouse_state: ClearinghouseState,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClearinghouseState {
    withdrawable: String,
    asset_positions: Vec<AssetPosition>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetPosition {
    position: HLPosition,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HLPosition {
    coin: String,
    szi: String, // signed size: positive = long, negative = short
    position_value: String,
    unrealized_pnl: String,
    margin_used: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HLOrderUpdate {
    order: HLOrder,
    status: String, // "open", "filled", "canceled", etc.
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HLOrder {
    coin: String,
    side: String, // "B" or "A"
    sz: String,
    limit_px: String,
    oid: u64,
    cloid: Option<String>,
    orig_sz: String,
}

// --- rest api response for open orders ---

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HLOpenOrder {
    coin: String,
    side: String,
    sz: String,
    limit_px: String,
    oid: u64,
    cloid: Option<String>,
    orig_sz: String,
}

// --- main struct ---

pub struct HyperliquidStateSubscriber {
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    heartbeat_interval: Interval,
    pending: HeapRb<StateUpdate>,
    // track known positions to detect closures
    known_positions: HashSet<Symbol>,
    // user wallet address for subscriptions
    user_address: String,
    // http client for rest api
    http_client: Client,
}

const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const TESTNET_WS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";
const MAINNET_HTTP_URL: &str = "https://api.hyperliquid.xyz";

impl HyperliquidStateSubscriber {
    fn create_subcription_payload(json: Value) -> Value {
        json!({"method": "subscribe", "subscription": json})
    }

    fn create_order_update_sub(address: String) -> Value {
        Self::create_subcription_payload(json!({ "type": "orderUpdates", "user": address }))
    }

    fn create_balance_update_sub(address: String) -> Value {
        Self::create_subcription_payload(json!({ "type": "clearinghouseState", "user": address }))
    }

    async fn ws_send(&mut self, value: Value) -> HLStateResult<()> {
        let ws = self
            .ws_stream
            .as_mut()
            .ok_or(HLStateError::HyperliquidSendFailed)?;
        ws.send(Message::Text(value.to_string())).await?;
        Ok(())
    }

    /// parse ws message and push updates to ring buffer
    fn parse_into_queue(&mut self, text: &str) {
        let msg: HLStateMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                debug!("failed to parse state message: {}", e);
                return;
            }
        };

        match msg {
            HLStateMessage::ClearinghouseState { data } => {
                self.parse_clearinghouse(data.clearinghouse_state);
            }
            HLStateMessage::OrderUpdates { data } => {
                for update in data {
                    if let Some(order_update) = Self::parse_order_update(update) {
                        let _ = self.pending.try_push(order_update);
                    }
                }
            }
            HLStateMessage::Unknown => {}
        }
    }

    fn parse_clearinghouse(&mut self, state: ClearinghouseState) {
        // balance update (withdrawable usdc)
        let balance = Balance {
            coin: Symbol::new("USDC"),
            venue: Venue::Hyperliquid,
            qty: state.withdrawable.parse().unwrap_or(0.0),
        };
        let _ = self.pending.try_push(StateUpdate::BalanceUpdate(balance));

        // collect current position symbols
        let mut current_symbols: HashSet<Symbol> = HashSet::new();

        // position updates
        for ap in state.asset_positions {
            let pos = ap.position;
            let symbol = Symbol::new(&pos.coin);
            current_symbols.insert(symbol);

            let szi: f64 = pos.szi.parse().unwrap_or(0.0);
            let (side, qty) = if szi >= 0.0 {
                (Side::LONG, szi)
            } else {
                (Side::SHORT, szi.abs())
            };

            let position = Position {
                symbol,
                venue: Venue::Hyperliquid,
                side,
                qty,
                position_value: pos.position_value.parse().unwrap_or(0.0),
                unrealised_pnl: pos.unrealized_pnl.parse().unwrap_or(0.0),
                margin: pos.margin_used.parse().unwrap_or(0.0),
            };
            let _ = self.pending.try_push(StateUpdate::PositionUpdate(position));
        }

        // detect closed positions (in known but not in current)
        for symbol in self.known_positions.difference(&current_symbols) {
            let closed_position = Position {
                symbol: *symbol,
                venue: Venue::Hyperliquid,
                side: Side::LONG, // side doesn't matter for closed
                qty: 0.0,
                position_value: 0.0,
                unrealised_pnl: 0.0,
                margin: 0.0,
            };
            let _ = self
                .pending
                .try_push(StateUpdate::PositionUpdate(closed_position));
        }

        // update known positions
        self.known_positions = current_symbols;
    }

    fn parse_order_update(update: HLOrderUpdate) -> Option<StateUpdate> {
        let order = update.order;
        let side = if order.side == "B" {
            Side::LONG
        } else {
            Side::SHORT
        };

        let state = match update.status.as_str() {
            "open" => OrderState::NEW,
            "filled" => OrderState::FILLED,
            "canceled" => OrderState::CANCELLED,
            "rejected" => OrderState::REJECTED,
            _ => OrderState::UNKNOWN,
        };

        let sz: f64 = order.sz.parse().unwrap_or(0.0);
        let orig_sz: f64 = order.orig_sz.parse().unwrap_or(0.0);
        let filled_qty = orig_sz - sz;

        // use cloid if present, otherwise fall back to oid as string
        // parse_cloid converts 0x00...05 back to "5" for cleaner display
        let client_order_id = order
            .cloid
            .filter(|c| !c.is_empty())
            .map(|c| ClientOrderId::new(&parse_cloid(&c)))
            .unwrap_or_else(|| ClientOrderId::new(&format!("oid:{}", order.oid)));

        Some(StateUpdate::OrderUpdate(Order {
            symbol: Symbol::new(&order.coin),
            venue: Venue::Hyperliquid,
            side,
            client_order_id,
            qty: orig_sz,
            filled_qty,
            price: order.limit_px.parse().unwrap_or(0.0),
            order_type: OrderType::LIMIT,
            time_in_force: TimeInForce::GTC,
            state,
        }))
    }

    /// fetch open orders via rest api and queue them
    async fn fetch_open_orders(&mut self) {
        let url = format!("{}/info", MAINNET_HTTP_URL);
        let body = json!({
            "type": "openOrders",
            "user": self.user_address
        });

        match self.http_client.post(&url).json(&body).send().await {
            Ok(resp) => {
                if let Ok(orders) = resp.json::<Vec<HLOpenOrder>>().await {
                    info!("fetched {} open orders on startup", orders.len());
                    for order in orders {
                        if let Some(update) = Self::parse_open_order(order) {
                            let _ = self.pending.try_push(update);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("failed to fetch open orders: {}", e);
            }
        }
    }

    /// parse rest api open order into state update
    fn parse_open_order(order: HLOpenOrder) -> Option<StateUpdate> {
        let side = if order.side == "B" {
            Side::LONG
        } else {
            Side::SHORT
        };

        let sz: f64 = order.sz.parse().unwrap_or(0.0);
        let orig_sz: f64 = order.orig_sz.parse().unwrap_or(0.0);
        let filled_qty = orig_sz - sz;

        let client_order_id = order
            .cloid
            .filter(|c| !c.is_empty())
            .map(|c| ClientOrderId::new(&parse_cloid(&c)))
            .unwrap_or_else(|| ClientOrderId::new(&format!("oid:{}", order.oid)));

        Some(StateUpdate::OrderUpdate(Order {
            symbol: Symbol::new(&order.coin),
            venue: Venue::Hyperliquid,
            side,
            client_order_id,
            qty: orig_sz,
            filled_qty,
            price: order.limit_px.parse().unwrap_or(0.0),
            order_type: OrderType::LIMIT,
            time_in_force: TimeInForce::GTC,
            state: OrderState::NEW,
        }))
    }
}

impl StateSubscriber for HyperliquidStateSubscriber {
    type Error = HLStateError;

    async fn new(passport_id: PassportId, cfg_path: &str) -> StateResult<Self> {
        // load passport config to get user address
        let cfg = PassportConfig::new(cfg_path).map_err(HLStateError::Config)?;
        let passport = cfg
            .find_by_passport_id(passport_id)
            .map_err(HLStateError::Config)?;
        let cred = passport
            .details
            .hyperliquid
            .ok_or(HLStateError::CredentialsNotConfigured)?;

        let (ws_stream, _) = connect_async(MAINNET_WS_URL)
            .await
            .map_err(HLStateError::WebSocket)?;
        let heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let http_client = Client::new();
        let mut instance = HyperliquidStateSubscriber {
            ws_stream: Some(ws_stream),
            heartbeat_interval,
            pending: HeapRb::new(RING_BUF_CAPACITY),
            known_positions: HashSet::new(),
            user_address: cred.user_address.clone(),
            http_client,
        };
        instance.subscribe_all().await?;

        // fetch initial open orders via rest api
        instance.fetch_open_orders().await;

        Ok(instance)
    }

    async fn connect(&mut self) {
        if self.ws_stream.is_none() {
            match connect_async(MAINNET_WS_URL).await {
                Ok((ws, _)) => {
                    self.ws_stream = Some(ws);
                }
                Err(e) => error!("failed to reconnect: {}", e),
            }
        }
    }

    async fn subscribe_order_updates(&mut self) -> StateResult<()> {
        self.ws_send(Self::create_order_update_sub(self.user_address.clone()))
            .await
            .map_err(StateError::Hyperliquid)
    }

    async fn subscribe_position_updates(&mut self) -> StateResult<()> {
        self.ws_send(Self::create_balance_update_sub(self.user_address.clone()))
            .await
            .map_err(StateError::Hyperliquid)
    }

    async fn subscribe_balance_updates(&mut self) -> StateResult<()> {
        // no-op as usdc balance is included in clearinghouseState
        Ok(())
    }

    async fn produce(&mut self) -> Option<StateUpdate> {
        // drain queue first before reading more
        if let Some(update) = self.pending.try_pop() {
            debug!("returning queued state update");
            return Some(update);
        }

        // queue empty, read from ws
        if self.ws_stream.is_none() {
            self.connect().await;
            return None;
        }
        let ws = self.ws_stream.as_mut()?;

        tokio::select! {
            biased;

            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        // debug!("state ws received: {}", txt);
                        self.parse_into_queue(&txt);
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        warn!("state ws disconnected, will reconnect");
                        self.ws_stream = None;
                    }
                    _ => {}
                }
            }
            _ = self.heartbeat_interval.tick() => {
                trace!("state ws sending heartbeat");
                // hyperliquid uses json ping, not websocket ping frame
                let ping_msg = Message::Text(r#"{"method":"ping"}"#.to_string());
                if ws.send(ping_msg).await.is_err() {
                    warn!("state ws heartbeat failed");
                    self.ws_stream = None;
                }
            }
            // yield immediately if no data ready
            _ = tokio::task::yield_now() => {}
        }

        // always try to drain queue after ws read
        self.pending.try_pop()
    }

    async fn get_orders() {
        // no-op: hyperliquid sends snapshot on subscribe (isSnapshot: true)
        // then streams updates automatically
    }

    async fn get_positions() {
        // no-op: positions included in clearinghouseState subscription snapshot
    }

    async fn get_balances() {
        // no-op: balance included in clearinghouseState subscription snapshot
    }
}
