//! lightweight paradex state subscriber
//!
//! inline websocket processing without spawning tasks,
//! similar to hyperliquid implementation for low-latency state updates.

pub mod error;

use std::collections::HashSet;
use std::str::FromStr;

use crate::{
    config_parser::passport::PassportConfig,
    ts_connectors::{
        signature_utils::paradex::{account_address, auth_headers, derive_l2_private_key},
        state::{
            StateSubscriber,
            error::{StateError, StateResult},
        },
    },
    types::common::{
        Balance, ClientOrderId, Order, OrderState, OrderType, PassportId, Position, Side, Symbol,
        TimeInForce, Venue,
    },
    types::trade_server::StateUpdate,
};
use alloy_signer_local::PrivateKeySigner;
use error::{PdxLightStateError, PdxLightStateResult};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use ringbuf::{
    HeapRb,
    traits::{Consumer, Producer},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use starknet_core::types::Felt;
use starknet_core::utils::cairo_short_string_to_felt;
use starknet_signers::SigningKey;
use tokio::{net::TcpStream, time::Interval};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, trace, warn};
use tungstenite::Message;

const RING_BUF_CAPACITY: usize = 1024;

/// jwt refresh threshold - refresh when 50 minutes old (token valid for 60 min)
const JWT_REFRESH_THRESHOLD_SECS: u64 = 50 * 60;

// websocket urls
const MAINNET_WS_URL: &str = "wss://ws.api.prod.paradex.trade/v1";
const TESTNET_WS_URL: &str = "wss://ws.api.testnet.paradex.trade/v1";

// rest api urls
const MAINNET_API_URL: &str = "https://api.prod.paradex.trade/v1";
const TESTNET_API_URL: &str = "https://api.testnet.paradex.trade/v1";

// --- serde structs for parsing ws messages ---

/// json-rpc response wrapper
#[derive(Debug, Deserialize)]
struct JsonRpcMessage {
    jsonrpc: String,
    method: Option<String>,
    params: Option<SubscriptionParams>,
    result: Option<Value>,
    error: Option<JsonRpcError>,
    id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct SubscriptionParams {
    channel: String,
    data: Value,
}

// --- order update ---

#[derive(Debug, Deserialize, Serialize)]
struct PdxOrderUpdate {
    id: String,
    client_id: String,
    market: String,
    side: String, // "BUY" or "SELL"
    size: String,
    remaining_size: String,
    price: Option<String>,
    status: String, // "NEW", "OPEN", "CLOSED", "UNTRIGGERED"
    #[serde(rename = "type")]
    order_type: String, // "MARKET", "LIMIT", etc
    instruction: Option<String>, // "GTC", "IOC", "POST_ONLY"
    avg_fill_price: Option<String>,
}

// --- position update ---

#[derive(Debug, Deserialize, Serialize)]
struct PdxPositionUpdate {
    market: String,
    side: String, // "LONG" or "SHORT"
    size: String,
    cost: String,
    cost_usd: String,
    unrealized_pnl: String,
    status: String, // "OPEN" or "CLOSED"
}

// --- balance event ---

#[derive(Debug, Deserialize)]
struct PdxBalanceEvent {
    settlement_asset_balance_after: String,
}

// --- system config (for chain_id and contract hashes) ---

#[derive(Debug, Deserialize)]
struct SystemConfig {
    starknet_chain_id: String,
    paraclear_account_hash: String,
    paraclear_account_proxy_hash: String,
}

// --- auth response ---

#[derive(Debug, Deserialize)]
struct AuthResponse {
    jwt_token: String,
}

// --- rest api responses for initial state ---

#[derive(Debug, Deserialize)]
struct OrdersResponse {
    results: Vec<PdxOrderUpdate>,
}

#[derive(Debug, Deserialize)]
struct PositionsResponse {
    results: Vec<PdxPositionUpdate>,
}

#[derive(Debug, Deserialize)]
struct BalanceResponse {
    results: Vec<PdxBalance>,
}

#[derive(Debug, Deserialize)]
struct PdxBalance {
    token: String,
    size: String,
}

// --- main struct ---

pub struct ParadexLightStateSubscriber {
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    heartbeat_interval: Interval,
    pending: HeapRb<StateUpdate>,
    known_positions: HashSet<Symbol>,
    // http client for rest api
    http_client: Client,
    // jwt token for auth
    jwt_token: Option<String>,
    // jwt obtained timestamp (unix secs)
    jwt_obtained_at: u64,
    // config
    is_mainnet: bool,
    // signing key for auth refresh
    signing_key: SigningKey,
    chain_id: Felt,
    account: Felt,
    // request id counter for json-rpc
    request_id: i64,
}

impl ParadexLightStateSubscriber {
    /// get current unix timestamp in seconds
    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// check if jwt needs refresh (older than threshold)
    fn jwt_needs_refresh(&self) -> bool {
        let age = Self::now_secs().saturating_sub(self.jwt_obtained_at);
        age >= JWT_REFRESH_THRESHOLD_SECS
    }

    /// refresh jwt token and re-authenticate websocket
    async fn refresh_jwt(&mut self) -> bool {
        info!("refreshing jwt token...");

        match self.fetch_jwt().await {
            Ok(token) => {
                self.jwt_token = Some(token);
                self.jwt_obtained_at = Self::now_secs();
                info!("jwt token refreshed");

                // re-authenticate websocket with new token
                if self.ws_stream.is_some() {
                    if let Err(e) = self.ws_auth().await {
                        error!("failed to re-authenticate ws after jwt refresh: {}", e);
                        self.ws_stream = None;
                        return false;
                    }
                    info!("websocket re-authenticated with new jwt");
                }
                true
            }
            Err(e) => {
                error!("failed to refresh jwt: {}", e);
                false
            }
        }
    }

    fn ws_url(&self) -> &str {
        if self.is_mainnet {
            MAINNET_WS_URL
        } else {
            TESTNET_WS_URL
        }
    }

    fn api_url(&self) -> &str {
        if self.is_mainnet {
            MAINNET_API_URL
        } else {
            TESTNET_API_URL
        }
    }

    /// get next request id for json-rpc
    fn next_id(&mut self) -> i64 {
        self.request_id += 1;
        self.request_id
    }

    /// fetch system config to get chain_id and contract hashes
    async fn fetch_system_config(
        http_client: &Client,
        api_url: &str,
    ) -> PdxLightStateResult<SystemConfig> {
        let url = format!("{}/system/config", api_url);
        let resp = http_client.get(&url).send().await?;
        let config: SystemConfig = resp.json().await?;
        Ok(config)
    }

    /// get jwt token via rest api
    async fn fetch_jwt(&self) -> PdxLightStateResult<String> {
        let url = format!("{}/auth", self.api_url());
        let (_, headers) = auth_headers(&self.chain_id, &self.signing_key, &self.account)?;

        let resp = self.http_client.post(&url).headers(headers).send().await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(PdxLightStateError::AuthFailed(format!(
                "{}: {}",
                status, body
            )));
        }

        let auth: AuthResponse = resp.json().await?;
        Ok(auth.jwt_token)
    }

    /// send ws message
    async fn ws_send(&mut self, value: Value) -> PdxLightStateResult<()> {
        let ws = self
            .ws_stream
            .as_mut()
            .ok_or(PdxLightStateError::NotConnected)?;
        ws.send(Message::Text(value.to_string())).await?;
        Ok(())
    }

    /// authenticate websocket with jwt
    async fn ws_auth(&mut self) -> PdxLightStateResult<()> {
        let token = if let Some(ref t) = self.jwt_token {
            t.clone()
        } else {
            let t = self.fetch_jwt().await?;
            self.jwt_token = Some(t.clone());
            t
        };

        let id = self.next_id();
        let auth_msg = json!({
            "jsonrpc": "2.0",
            "method": "auth",
            "params": { "bearer": token },
            "id": id
        });

        self.ws_send(auth_msg).await?;

        // wait for auth response
        if let Some(ws) = self.ws_stream.as_mut() {
            while let Some(msg) = ws.next().await {
                match msg {
                    Ok(Message::Text(txt)) => {
                        if let Ok(rpc) = serde_json::from_str::<JsonRpcMessage>(&txt) {
                            if rpc.id == Some(id) {
                                if let Some(err) = rpc.error {
                                    return Err(PdxLightStateError::AuthFailed(format!(
                                        "{}: {}",
                                        err.code, err.message
                                    )));
                                }
                                info!("paradex ws authenticated");
                                return Ok(());
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = ws.send(Message::Pong(data)).await;
                    }
                    Err(e) => return Err(e.into()),
                    _ => {}
                }
            }
        }

        Err(PdxLightStateError::AuthFailed("no response".into()))
    }

    /// subscribe to a channel
    async fn subscribe_channel(&mut self, channel: &str) -> PdxLightStateResult<()> {
        let id = self.next_id();
        let sub_msg = json!({
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": { "channel": channel },
            "id": id
        });

        self.ws_send(sub_msg).await?;
        debug!("subscribed to channel: {}", channel);
        Ok(())
    }

    /// parse ws message and push updates to ring buffer
    fn parse_into_queue(&mut self, text: &str) {
        let msg: JsonRpcMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                debug!("failed to parse ws message: {}", e);
                return;
            }
        };

        // only process subscription messages
        if msg.method.as_deref() != Some("subscription") {
            return;
        }

        let params = match msg.params {
            Some(p) => p,
            None => return,
        };

        // route by channel prefix
        if params.channel.starts_with("orders") {
            self.parse_order_update(&params.data);
        } else if params.channel == "positions" {
            self.parse_position_update(&params.data);
        } else if params.channel == "balance_events" {
            self.parse_balance_event(&params.data);
        }
    }

    fn parse_order_update(&mut self, data: &Value) {
        let update: PdxOrderUpdate = match serde_json::from_value(data.clone()) {
            Ok(u) => u,
            Err(e) => {
                debug!("failed to parse order update: {}", e);
                return;
            }
        };

        let side = if update.side == "BUY" {
            Side::LONG
        } else {
            Side::SHORT
        };

        let status = match update.status.as_str() {
            "NEW" | "OPEN" | "UNTRIGGERED" => OrderState::NEW,
            "CLOSED" => {
                // check if filled or cancelled based on remaining size
                let remaining: f64 = update.remaining_size.parse().unwrap_or(0.0);
                if remaining == 0.0 {
                    OrderState::FILLED
                } else {
                    OrderState::CANCELLED
                }
            }
            _ => OrderState::UNKNOWN,
        };

        let size: f64 = update.size.parse().unwrap_or(0.0);
        let remaining: f64 = update.remaining_size.parse().unwrap_or(0.0);
        let filled_qty = size - remaining;

        // strip -USD-PERP suffix from market
        let symbol = Symbol::new(&update.market.replace("-USD-PERP", ""));

        let order = Order {
            symbol,
            venue: Venue::Paradex,
            side,
            client_order_id: ClientOrderId::new(&update.client_id),
            qty: size,
            filled_qty,
            price: update
                .price
                .as_ref()
                .and_then(|p| p.parse().ok())
                .unwrap_or(0.0),
            order_type: OrderType::LIMIT,
            time_in_force: TimeInForce::GTC,
            state: status,
        };

        info!(
            market = %update.market,
            status = %update.status,
            size = size,
            filled = filled_qty,
            "pdx light: order update"
        );

        let _ = self.pending.try_push(StateUpdate::OrderUpdate(order));
    }

    fn parse_position_update(&mut self, data: &Value) {
        let update: PdxPositionUpdate = match serde_json::from_value(data.clone()) {
            Ok(u) => u,
            Err(e) => {
                debug!("failed to parse position update: {}", e);
                return;
            }
        };

        let symbol = Symbol::new(&update.market.replace("-USD-PERP", ""));
        let side = if update.side == "LONG" {
            Side::LONG
        } else {
            Side::SHORT
        };
        let size: f64 = update.size.parse().unwrap_or(0.0);

        // track known positions
        if update.status == "CLOSED" || size == 0.0 {
            self.known_positions.remove(&symbol);
        } else {
            self.known_positions.insert(symbol);
        }

        let position = Position {
            symbol,
            venue: Venue::Paradex,
            side,
            qty: size.abs(),
            position_value: update.cost_usd.parse().unwrap_or(0.0),
            unrealised_pnl: update.unrealized_pnl.parse().unwrap_or(0.0),
            margin: update.cost.parse().unwrap_or(0.0),
        };

        info!(
            market = %update.market,
            side = %update.side,
            size = size,
            "pdx light: position update"
        );

        let _ = self.pending.try_push(StateUpdate::PositionUpdate(position));
    }

    fn parse_balance_event(&mut self, data: &Value) {
        let event: PdxBalanceEvent = match serde_json::from_value(data.clone()) {
            Ok(e) => e,
            Err(e) => {
                debug!("failed to parse balance event: {}", e);
                return;
            }
        };

        let qty: f64 = event.settlement_asset_balance_after.parse().unwrap_or(0.0);

        let balance = Balance {
            coin: Symbol::new("USDC"),
            venue: Venue::Paradex,
            qty,
        };

        debug!(balance = qty, "pdx light: balance update");
        let _ = self.pending.try_push(StateUpdate::BalanceUpdate(balance));
    }

    /// fetch initial state via rest api
    async fn fetch_initial_state(&mut self) -> PdxLightStateResult<()> {
        let token = self
            .jwt_token
            .as_ref()
            .ok_or(PdxLightStateError::AuthFailed("no token".into()))?;
        let auth_header = format!("Bearer {}", token);

        // fetch open orders
        let url = format!("{}/orders", self.api_url());
        match self
            .http_client
            .get(&url)
            .header("Authorization", &auth_header)
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(orders) = resp.json::<OrdersResponse>().await {
                    info!("fetched {} open orders", orders.results.len());
                    for order in orders.results {
                        self.parse_order_update(&serde_json::to_value(&order).unwrap_or_default());
                    }
                }
            }
            Err(e) => warn!("failed to fetch orders: {}", e),
        }

        // fetch positions
        let url = format!("{}/positions", self.api_url());
        match self
            .http_client
            .get(&url)
            .header("Authorization", &auth_header)
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(positions) = resp.json::<PositionsResponse>().await {
                    info!("fetched {} positions", positions.results.len());
                    for pos in positions.results {
                        self.parse_position_update(&serde_json::to_value(&pos).unwrap_or_default());
                    }
                }
            }
            Err(e) => warn!("failed to fetch positions: {}", e),
        }

        // fetch balances
        let url = format!("{}/balance", self.api_url());
        match self
            .http_client
            .get(&url)
            .header("Authorization", &auth_header)
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(balances) = resp.json::<BalanceResponse>().await {
                    info!("fetched {} balances", balances.results.len());
                    for bal in balances.results {
                        let balance = Balance {
                            coin: Symbol::new(&bal.token),
                            venue: Venue::Paradex,
                            qty: bal.size.parse().unwrap_or(0.0),
                        };
                        let _ = self.pending.try_push(StateUpdate::BalanceUpdate(balance));
                    }
                }
            }
            Err(e) => warn!("failed to fetch balances: {}", e),
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl StateSubscriber for ParadexLightStateSubscriber {
    type Error = PdxLightStateError;

    async fn new(passport_id: PassportId, cfg_path: &str) -> StateResult<Self> {
        info!("creating paradex light state subscriber...");

        // load passport config
        let cfg = PassportConfig::new(cfg_path).map_err(PdxLightStateError::Config)?;
        let passport = cfg
            .find_by_passport_id(passport_id)
            .map_err(PdxLightStateError::Config)?;
        let cred = passport
            .details
            .paradex
            .ok_or(PdxLightStateError::CredentialsNotConfigured)?;

        let is_mainnet = cred.is_mainnet.unwrap_or(true);
        let api_url = if is_mainnet {
            MAINNET_API_URL
        } else {
            TESTNET_API_URL
        };
        let ws_url = if is_mainnet {
            MAINNET_WS_URL
        } else {
            TESTNET_WS_URL
        };

        let http_client = Client::new();

        // fetch system config for chain_id and contract hashes
        info!("fetching system config...");
        let sys_config = Self::fetch_system_config(&http_client, api_url).await?;

        // chain_id is a short string like "PRIVATE_SN_PARACLEAR_MAINNET"
        let chain_id = cairo_short_string_to_felt(&sys_config.starknet_chain_id)
            .map_err(|e| PdxLightStateError::InvalidKey(format!("invalid chain_id: {}", e)))?;
        let paraclear_account_hash = Felt::from_hex(&sys_config.paraclear_account_hash)
            .map_err(|e| PdxLightStateError::InvalidKey(format!("invalid account_hash: {}", e)))?;
        let paraclear_account_proxy_hash = Felt::from_hex(&sys_config.paraclear_account_proxy_hash)
            .map_err(|e| PdxLightStateError::InvalidKey(format!("invalid proxy_hash: {}", e)))?;

        // derive signing key
        let signing_key = if let Some(l2_key) = &cred.l2_private_key {
            info!("using l2 private key directly");
            let felt = Felt::from_hex(l2_key)
                .map_err(|e| PdxLightStateError::InvalidKey(format!("invalid l2 key: {}", e)))?;
            SigningKey::from_secret_scalar(felt)
        } else if let Some(eth_key) = &cred.eth_private_key {
            info!("deriving l2 key from eth key");
            let eth_signer = PrivateKeySigner::from_str(eth_key)
                .map_err(|e| PdxLightStateError::InvalidKey(format!("invalid eth key: {}", e)))?;
            let l2_felt =
                derive_l2_private_key(&eth_signer).map_err(PdxLightStateError::Signature)?;
            SigningKey::from_secret_scalar(l2_felt)
        } else {
            return Err(PdxLightStateError::CredentialsNotConfigured.into());
        };

        // compute account address
        let public_key = signing_key.verifying_key().scalar();
        let account = account_address(
            public_key,
            paraclear_account_proxy_hash,
            paraclear_account_hash,
        )
        .map_err(PdxLightStateError::Signature)?;
        info!("computed account address: {}", account.to_hex_string());

        // connect websocket
        info!("connecting to websocket: {}", ws_url);
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .map_err(PdxLightStateError::WebSocket)?;

        let heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));

        let mut instance = ParadexLightStateSubscriber {
            ws_stream: Some(ws_stream),
            heartbeat_interval,
            pending: HeapRb::new(RING_BUF_CAPACITY),
            known_positions: HashSet::new(),
            http_client,
            jwt_token: None,
            jwt_obtained_at: 0,
            is_mainnet,
            signing_key,
            chain_id,
            account,
            request_id: 0,
        };

        // authenticate and get jwt
        info!("fetching jwt token...");
        let jwt = instance.fetch_jwt().await?;
        instance.jwt_token = Some(jwt);
        instance.jwt_obtained_at = Self::now_secs();

        // authenticate websocket
        info!("authenticating websocket...");
        instance.ws_auth().await?;

        // subscribe to channels
        instance.subscribe_all().await?;

        // fetch initial state
        info!("fetching initial state...");
        instance.fetch_initial_state().await?;

        info!("paradex light state subscriber initialized");
        Ok(instance)
    }

    async fn connect(&mut self) {
        if self.ws_stream.is_none() {
            let ws_url = self.ws_url();
            match connect_async(ws_url).await {
                Ok((ws, _)) => {
                    self.ws_stream = Some(ws);
                    // re-authenticate
                    if let Err(e) = self.ws_auth().await {
                        error!("failed to re-authenticate: {}", e);
                        self.ws_stream = None;
                        return;
                    }
                    // re-subscribe
                    if let Err(e) = self.subscribe_all().await {
                        error!("failed to re-subscribe: {}", e);
                    }
                }
                Err(e) => error!("failed to reconnect: {}", e),
            }
        }
    }

    async fn subscribe_order_updates(&mut self) -> StateResult<()> {
        self.subscribe_channel("orders.ALL")
            .await
            .map_err(StateError::Paradex)
    }

    async fn subscribe_position_updates(&mut self) -> StateResult<()> {
        self.subscribe_channel("positions")
            .await
            .map_err(StateError::Paradex)
    }

    async fn subscribe_balance_updates(&mut self) -> StateResult<()> {
        self.subscribe_channel("balance_events")
            .await
            .map_err(StateError::Paradex)
    }

    async fn produce(&mut self) -> Option<StateUpdate> {
        // proactively refresh jwt if nearing expiration
        if self.jwt_needs_refresh() {
            self.refresh_jwt().await;
        }

        // drain queue first
        if let Some(update) = self.pending.try_pop() {
            return Some(update);
        }

        // queue empty, read from ws
        if self.ws_stream.is_none() {
            self.connect().await;
            return None;
        }
        let ws = self.ws_stream.as_mut()?;

        // select between heartbeat and ws message
        tokio::select! {
            biased;

            // prioritize ws messages
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        self.parse_into_queue(&txt);
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        warn!("pdx light ws disconnected, will reconnect");
                        self.ws_stream = None;
                    }
                    _ => {}
                }
            }
            _ = self.heartbeat_interval.tick() => {
                trace!("pdx light ws heartbeat tick");
            }
        }

        // try to drain queue after ws read
        self.pending.try_pop()
    }

    async fn get_orders() {
        // initial orders fetched in new()
    }

    async fn get_positions() {
        // initial positions fetched in new()
    }

    async fn get_balances() {
        // initial balances fetched in new()
    }
}
