//! lightweight paradex executor using pipelined-http PollableClient
//!
//! uses synchronous poll-driven http client directly in the executor.
//! no separate worker thread - poll() called in produce().

pub mod error;
pub mod market_info;

pub use market_info::{MarketInfoCache, ParadexMarket};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_signer_local::PrivateKeySigner;
use async_trait::async_trait;
use pipelined_http::{PollablePool, PollablePoolBuilder, ResponseResult};
use serde::{Deserialize, Serialize};
use starknet_core::types::Felt;
use starknet_core::utils::cairo_short_string_to_felt;
use starknet_signers::SigningKey;
use tracing::{debug, error, info};

use crate::config_parser::passport::PassportConfig;
use crate::ts_connectors::orders::Executor;
use crate::ts_connectors::signature_utils::paradex::{
    OrderType as SignOrderType, Side as SignSide, account_address, auth_headers,
    derive_l2_private_key, format_signature, sign_modify_order, sign_order,
};
use crate::types::common::{
    ClientOrderId, Order, OrderState, OrderType, PassportId, Side, Symbol, TimeInForce, Venue,
};
use crate::types::trade_server::{
    CancelOrder, CancelOrderResp, CancelOrderStatus, OrderResponse, PlaceOrder, PlaceOrderResp,
    PlaceOrderStatus, QueryStaleOrderResp, ReplaceOrder, ReplaceOrderResp, ReplaceOrderStatus,
};

use error::{ParadexLightError, ParadexLightResult};

// re-export signature utilities
pub use crate::ts_connectors::signature_utils::paradex::*;

// api urls
const MAINNET_API_HOST: &str = "api.prod.paradex.trade";
const TESTNET_API_HOST: &str = "api.testnet.paradex.trade";

// --- serde structs ---

#[derive(Debug, Deserialize)]
struct SystemConfig {
    starknet_chain_id: String,
    paraclear_account_hash: String,
    paraclear_account_proxy_hash: String,
}

#[derive(Debug, Deserialize)]
struct AuthResponse {
    jwt_token: String,
}

#[derive(Debug, Serialize, Clone)]
struct CreateOrderRequest {
    instruction: String,
    market: String,
    price: String,
    side: String,
    signature: String,
    signature_timestamp: u128,
    size: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct ModifyOrderRequest {
    id: String,
    market: String,
    price: String,
    side: String,
    signature: String,
    signature_timestamp: u128,
    size: String,
    #[serde(rename = "type")]
    order_type: String,
}

#[derive(Debug, Deserialize)]
struct OrderResp {
    id: String,
    status: String,
    remaining_size: String,
    avg_fill_price: Option<String>,
    size: String,
}

#[derive(Debug, Clone)]
enum RequestType {
    Place(PlaceOrder),
    Cancel(CancelOrder),
    Replace(ReplaceOrder),
    QueryStale(ClientOrderId),
}

/// paradex single order response from /v1/orders/by_client_id/:client_id
#[derive(Debug, Deserialize)]
struct OrderQueryItem {
    id: String,
    client_id: Option<String>,
    market: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    size: String,
    remaining_size: String,
    price: String,
    status: String,
    avg_fill_price: Option<String>,
    cancel_reason: Option<String>,
}

/// jwt refresh threshold - refresh when 50 minutes old (token valid for 60 min)
const JWT_REFRESH_THRESHOLD_SECS: u64 = 50 * 60;

pub struct ParadexLightExecutor {
    // pollable http pool (sync, no background tasks)
    pool: PollablePool,
    // track pending requests by http request id
    pending: HashMap<u64, RequestType>,
    // api host for connections
    api_host: String,
    // auth header value
    auth_header: String,
    // signing key for order signatures
    signing_key: SigningKey,
    chain_id: Felt,
    account: Felt,
    // monotonic request counter
    next_req_id: AtomicU64,
    // jwt obtained timestamp (unix secs)
    jwt_obtained_at: u64,
    // market info cache for precision/rounding
    market_info: MarketInfoCache,
}

impl ParadexLightExecutor {
    /// get market info for a symbol, returns None if not found
    pub fn get_market(&self, symbol: &str) -> Option<&ParadexMarket> {
        self.market_info.get(symbol)
    }

    /// get current unix timestamp in seconds
    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// check if jwt needs refresh (older than threshold or on 401 error)
    fn jwt_needs_refresh(&self) -> bool {
        let age = Self::now_secs().saturating_sub(self.jwt_obtained_at);
        age >= JWT_REFRESH_THRESHOLD_SECS
    }

    /// refresh jwt token synchronously using pollable pool
    /// returns true if refresh succeeded, false otherwise
    fn refresh_jwt_sync(&mut self) -> bool {
        info!("refreshing jwt token...");

        // generate auth headers
        let (_, auth_hdrs) = match auth_headers(&self.chain_id, &self.signing_key, &self.account) {
            Ok(h) => h,
            Err(e) => {
                error!("failed to generate auth headers: {}", e);
                return false;
            }
        };

        // build auth request body (empty for paradex auth)
        let client = match self.pool.get_connection_tls(&self.api_host, 443) {
            Ok(c) => c,
            Err(e) => {
                error!("failed to get connection for jwt refresh: {}", e);
                return false;
            }
        };

        // build request with auth headers
        let mut req = client.post("/v1/auth");
        for (k, v) in auth_hdrs.iter() {
            if let Ok(v_str) = v.to_str() {
                req = req.header(k.as_str(), v_str);
            }
        }

        let http_req_id = match req.send() {
            Ok(id) => id,
            Err(e) => {
                error!("failed to send jwt refresh request: {}", e);
                return false;
            }
        };

        // poll until we get the response (blocking)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if std::time::Instant::now() > deadline {
                error!("jwt refresh timeout");
                return false;
            }

            if let Err(e) = self.pool.poll() {
                error!("poll error during jwt refresh: {}", e);
                return false;
            }

            if let Some((_host, result)) = self.pool.recv() {
                if result.request_id() != http_req_id {
                    // not our request, put it back? no, just log
                    debug!("received unexpected response during jwt refresh");
                    continue;
                }

                match result {
                    ResponseResult::Success(resp) => {
                        if resp.is_success() {
                            match resp.json::<AuthResponse>() {
                                Ok(auth) => {
                                    self.auth_header = format!("Bearer {}", auth.jwt_token);
                                    self.jwt_obtained_at = Self::now_secs();
                                    info!("jwt token refreshed successfully");
                                    return true;
                                }
                                Err(e) => {
                                    error!("failed to parse jwt response: {}", e);
                                    return false;
                                }
                            }
                        } else {
                            error!("jwt refresh failed: http {}", resp.status);
                            return false;
                        }
                    }
                    ResponseResult::Timeout { .. } => {
                        error!("jwt refresh request timed out");
                        return false;
                    }
                    ResponseResult::Error { error, .. } => {
                        error!("jwt refresh request error: {}", error);
                        return false;
                    }
                }
            }

            // small sleep to avoid busy loop
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
    }

    /// check response for 401 and trigger refresh if needed
    /// returns true if this was an auth error (caller should retry)
    fn check_auth_error(&mut self, result: &ResponseResult) -> bool {
        if let ResponseResult::Success(resp) = result {
            if resp.status == 401 {
                info!("received 401, refreshing jwt");
                return self.refresh_jwt_sync();
            }
        }
        false
    }

    /// convert internal side to signing side
    fn to_sign_side(side: Side) -> SignSide {
        match side {
            Side::LONG => SignSide::Buy,
            Side::SHORT => SignSide::Sell,
            Side::UNKNOWN => SignSide::Buy,
        }
    }

    /// convert internal order type to signing order type
    fn to_sign_order_type(ot: OrderType) -> SignOrderType {
        match ot {
            OrderType::LIMIT => SignOrderType::Limit,
            OrderType::MARKET => SignOrderType::Market,
            OrderType::UNKNOWN => SignOrderType::Limit,
        }
    }

    /// convert internal time in force to paradex instruction string
    fn tif_to_instruction(tif: TimeInForce) -> &'static str {
        match tif {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::PO => "POST_ONLY",
            TimeInForce::FOK => "IOC",
            TimeInForce::UNKNOWN => "GTC",
        }
    }

    /// format market symbol
    fn format_market(symbol: &str) -> String {
        if symbol.contains("-USD-PERP") {
            symbol.to_string()
        } else {
            format!("{}-USD-PERP", symbol)
        }
    }

    /// get next request id
    fn next_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// parse place response, returns (response, optional exchange_order_id for mapping)
    fn parse_place_response(
        request_id: u64,
        result: ResponseResult,
        order: PlaceOrder,
    ) -> (OrderResponse, Option<String>) {
        let cloid = order.client_order_id;
        let (status, oid_str) = match result {
            ResponseResult::Success(resp) => {
                let body = resp.text().unwrap_or_default();
                info!(cloid = %cloid, status = resp.status, body = %body, "[RESP:PLACE]");

                if resp.is_success() {
                    match serde_json::from_str::<OrderResp>(&body) {
                        Ok(order_resp) => {
                            let oid = order_resp.id.parse::<u64>().unwrap_or(0);
                            let oid_string = order_resp.id.clone();
                            let remaining: f64 = order_resp.remaining_size.parse().unwrap_or(0.0);
                            if remaining == 0.0 {
                                let avg_px = order_resp
                                    .avg_fill_price
                                    .and_then(|s| s.parse().ok())
                                    .unwrap_or(0.0);
                                let total_sz: f64 = order_resp.size.parse().unwrap_or(0.0);
                                (
                                    PlaceOrderStatus::Filled {
                                        oid,
                                        avg_px,
                                        total_sz,
                                    },
                                    Some(oid_string),
                                )
                            } else {
                                (PlaceOrderStatus::Resting { oid }, Some(oid_string))
                            }
                        }
                        Err(e) => (
                            PlaceOrderStatus::Rejected(format!("parse error: {}", e)),
                            None,
                        ),
                    }
                } else {
                    (
                        PlaceOrderStatus::Rejected(format!("http {}: {}", resp.status, body)),
                        None,
                    )
                }
            }
            ResponseResult::Timeout { duration, .. } => {
                info!(cloid = %cloid, "[RESP:PLACE] timeout: {:?}", duration);
                (
                    PlaceOrderStatus::Rejected(format!("timeout: {:?}", duration)),
                    None,
                )
            }
            ResponseResult::Error { error, .. } => {
                info!(cloid = %cloid, "[RESP:PLACE] error: {}", error);
                (
                    PlaceOrderStatus::Rejected(format!("error: {}", error)),
                    None,
                )
            }
        };

        (
            OrderResponse::Place(PlaceOrderResp {
                request_id: request_id as u8,
                request: order,
                status,
            }),
            oid_str,
        )
    }

    fn parse_cancel_response(
        request_id: u64,
        result: ResponseResult,
        cancel: CancelOrder,
    ) -> OrderResponse {
        let cloid = cancel.client_order_id;
        let status = match result {
            ResponseResult::Success(resp) => {
                let body = resp.text().unwrap_or_default();
                info!(cloid = %cloid, status = resp.status, body = %body, "[RESP:CANCEL]");

                if resp.status == 204 || resp.is_success() {
                    CancelOrderStatus::Success
                } else {
                    CancelOrderStatus::Failed(format!("http {}: {}", resp.status, body))
                }
            }
            ResponseResult::Timeout { duration, .. } => {
                info!(cloid = %cloid, "[RESP:CANCEL] timeout: {:?}", duration);
                CancelOrderStatus::Failed(format!("timeout: {:?}", duration))
            }
            ResponseResult::Error { error, .. } => {
                info!(cloid = %cloid, "[RESP:CANCEL] error: {}", error);
                CancelOrderStatus::Failed(format!("error: {}", error))
            }
        };

        OrderResponse::Cancel(CancelOrderResp {
            request_id: request_id as u8,
            request: cancel,
            status,
        })
    }

    fn parse_replace_response(
        request_id: u64,
        result: ResponseResult,
        replace: ReplaceOrder,
    ) -> OrderResponse {
        let status = match result {
            ResponseResult::Success(resp) => {
                if resp.is_success() {
                    ReplaceOrderStatus::Success
                } else {
                    let body = resp.text().unwrap_or_default();
                    ReplaceOrderStatus::Failed(format!("http {}: {}", resp.status, body))
                }
            }
            ResponseResult::Timeout { duration, .. } => {
                ReplaceOrderStatus::Failed(format!("timeout: {:?}", duration))
            }
            ResponseResult::Error { error, .. } => {
                ReplaceOrderStatus::Failed(format!("error: {}", error))
            }
        };

        OrderResponse::Replace(ReplaceOrderResp {
            request_id: request_id as u8,
            request: replace,
            status,
        })
    }

    fn parse_query_stale_response(result: ResponseResult, cloid: ClientOrderId) -> OrderResponse {
        let resp = match result {
            ResponseResult::Success(resp) => {
                let body = resp.text().unwrap_or_default();
                info!(cloid = %cloid, status = resp.status, body = %body, "[RESP:QUERY_STALE]");

                if resp.is_success() {
                    // endpoint returns single order object, not array
                    match serde_json::from_str::<OrderQueryItem>(&body) {
                        Ok(item) => {
                            // determine state from status and cancel_reason
                            let state = match item.status.as_str() {
                                "NEW" | "OPEN" => OrderState::NEW,
                                "CLOSED" => {
                                    // CLOSED can mean filled or cancelled, check remaining_size
                                    let remaining: f64 = item.remaining_size.parse().unwrap_or(0.0);
                                    if remaining == 0.0 {
                                        OrderState::FILLED
                                    } else {
                                        // has remaining size, was cancelled
                                        OrderState::CANCELLED
                                    }
                                }
                                "CANCELED" => OrderState::CANCELLED,
                                "REJECTED" => OrderState::REJECTED,
                                _ => OrderState::UNKNOWN,
                            };
                            let side = if item.side == "BUY" {
                                Side::LONG
                            } else {
                                Side::SHORT
                            };
                            let order_type = if item.order_type == "MARKET" {
                                OrderType::MARKET
                            } else {
                                OrderType::LIMIT
                            };
                            let qty: f64 = item.size.parse().unwrap_or(0.0);
                            let remaining: f64 = item.remaining_size.parse().unwrap_or(0.0);

                            let order = Order {
                                client_order_id: cloid,
                                symbol: Symbol::new(&item.market),
                                venue: Venue::Paradex,
                                side,
                                price: item.price.parse().unwrap_or(0.0),
                                qty,
                                filled_qty: qty - remaining,
                                order_type,
                                time_in_force: TimeInForce::GTC,
                                state,
                            };
                            QueryStaleOrderResp::found(cloid, Venue::Paradex, order)
                        }
                        Err(e) => QueryStaleOrderResp::failed(cloid, Venue::Paradex, e.to_string()),
                    }
                } else if resp.status == 404 || resp.status == 400 {
                    // 404 = not found, 400 = order does not exist (same meaning)
                    QueryStaleOrderResp::not_found(cloid, Venue::Paradex)
                } else {
                    QueryStaleOrderResp::failed(
                        cloid,
                        Venue::Paradex,
                        format!("http {}: {}", resp.status, body),
                    )
                }
            }
            ResponseResult::Timeout { duration, .. } => QueryStaleOrderResp::failed(
                cloid,
                Venue::Paradex,
                format!("timeout: {:?}", duration),
            ),
            ResponseResult::Error { error, .. } => {
                QueryStaleOrderResp::failed(cloid, Venue::Paradex, format!("error: {}", error))
            }
        };

        OrderResponse::QueryStaleOrder(resp)
    }
}

#[async_trait]
impl Executor for ParadexLightExecutor {
    type Error = ParadexLightError;

    async fn new(passport_id: PassportId, cfg_path: &str) -> ParadexLightResult<Self> {
        info!("creating paradex light executor...");

        // load config
        let cfg =
            PassportConfig::new(cfg_path).map_err(|e| ParadexLightError::Config(e.to_string()))?;
        let passport = cfg
            .find_by_passport_id(passport_id)
            .map_err(|e| ParadexLightError::Config(e.to_string()))?;
        let cred = passport
            .details
            .paradex
            .ok_or(ParadexLightError::CredentialsNotConfigured)?;

        let is_mainnet = cred.is_mainnet.unwrap_or(true);
        let api_host = if is_mainnet {
            MAINNET_API_HOST
        } else {
            TESTNET_API_HOST
        };

        // fetch system config using reqwest
        let http = reqwest::Client::new();
        let sys_config: SystemConfig = http
            .get(format!("https://{}/v1/system/config", api_host))
            .send()
            .await
            .map_err(|e| ParadexLightError::Config(format!("fetch config: {}", e)))?
            .json()
            .await
            .map_err(|e| ParadexLightError::Config(format!("parse config: {}", e)))?;

        // parse chain_id as short string
        let chain_id = cairo_short_string_to_felt(&sys_config.starknet_chain_id)
            .map_err(|e| ParadexLightError::InvalidKey(format!("chain_id: {}", e)))?;
        let paraclear_account_hash = Felt::from_hex(&sys_config.paraclear_account_hash)
            .map_err(|e| ParadexLightError::InvalidKey(format!("account_hash: {}", e)))?;
        let paraclear_account_proxy_hash = Felt::from_hex(&sys_config.paraclear_account_proxy_hash)
            .map_err(|e| ParadexLightError::InvalidKey(format!("proxy_hash: {}", e)))?;

        // derive signing key
        let signing_key = if let Some(l2_key) = &cred.l2_private_key {
            info!("using l2 private key directly");
            let felt = Felt::from_hex(l2_key)
                .map_err(|e| ParadexLightError::InvalidKey(format!("l2 key: {}", e)))?;
            SigningKey::from_secret_scalar(felt)
        } else if let Some(eth_key) = &cred.eth_private_key {
            info!("deriving l2 key from eth key");
            let eth_signer = PrivateKeySigner::from_str(eth_key)
                .map_err(|e| ParadexLightError::InvalidKey(format!("eth key: {}", e)))?;
            let l2_felt = derive_l2_private_key(&eth_signer)?;
            SigningKey::from_secret_scalar(l2_felt)
        } else {
            return Err(ParadexLightError::CredentialsNotConfigured);
        };

        // compute account address
        let public_key = signing_key.verifying_key().scalar();
        let account = account_address(
            public_key,
            paraclear_account_proxy_hash,
            paraclear_account_hash,
        )?;
        info!("account address: {}", account.to_hex_string());

        // get jwt token
        let (_, auth_hdrs) = auth_headers(&chain_id, &signing_key, &account)?;
        let mut auth_req = http.post(format!("https://{}/v1/auth", api_host));
        for (k, v) in auth_hdrs.iter() {
            auth_req = auth_req.header(k.as_str(), v.to_str().unwrap_or(""));
        }
        let auth_resp: AuthResponse = auth_req
            .send()
            .await
            .map_err(|e| ParadexLightError::AuthFailed(format!("request: {}", e)))?
            .json()
            .await
            .map_err(|e| ParadexLightError::AuthFailed(format!("parse: {}", e)))?;
        info!("got jwt token");

        // create pollable pool (synchronous, no tokio needed for i/o)
        let pool = PollablePoolBuilder::new()
            .default_timeout(std::time::Duration::from_secs(30))
            .tcp_nodelay(true)
            .max_idle_per_host(4)
            .build();

        // fetch market info for precision/rounding
        let market_info = MarketInfoCache::fetch(is_mainnet)
            .await
            .map_err(|e| ParadexLightError::Config(format!("fetch markets: {}", e)))?;
        info!("loaded {} markets", market_info.len());

        info!("paradex light executor ready");

        Ok(Self {
            pool,
            pending: HashMap::new(),
            api_host: api_host.to_string(),
            auth_header: format!("Bearer {}", auth_resp.jwt_token),
            signing_key,
            chain_id,
            account,
            next_req_id: AtomicU64::new(1),
            jwt_obtained_at: Self::now_secs(),
            market_info,
        })
    }

    async fn connect(&mut self) {
        info!("paradex light: connect (no-op, pool connects on demand)");
    }

    async fn place_order(&mut self, order: PlaceOrder) -> ParadexLightResult<()> {
        let market = Self::format_market(order.symbol.as_str());
        let side = Self::to_sign_side(order.side);
        let order_type = Self::to_sign_order_type(order.order_type);

        // round price/size using market info
        // use format_price/format_size to avoid f64 precision issues - these format
        // to strings with exact decimal places, then parse to Decimal
        let (price, size) = if let Some(mkt) = self.market_info.get(&market) {
            let p_str = mkt.format_price(order.price);
            let s_str = mkt.format_size(order.qty);
            (
                rust_decimal::Decimal::from_str(&p_str).unwrap_or_default(),
                rust_decimal::Decimal::from_str(&s_str).unwrap_or_default(),
            )
        } else {
            debug!(
                "market {} not found in cache, using default precision",
                market
            );
            (
                rust_decimal::Decimal::from_f64_retain(order.price)
                    .unwrap_or_default()
                    .round_dp(8),
                rust_decimal::Decimal::from_f64_retain(order.qty)
                    .unwrap_or_default()
                    .round_dp(8),
            )
        };

        // sign the order
        let (sig, timestamp) = sign_order(
            &market,
            side,
            order_type,
            size,
            Some(price),
            &self.signing_key,
            self.chain_id,
            self.account,
        )?;

        let req = CreateOrderRequest {
            instruction: Self::tif_to_instruction(order.time_in_force).to_string(),
            market,
            price: price.to_string(),
            side: if matches!(order.side, Side::LONG) {
                "BUY"
            } else {
                "SELL"
            }
            .to_string(),
            signature: format_signature(&sig),
            signature_timestamp: timestamp,
            size: size.to_string(),
            order_type: if matches!(order.order_type, OrderType::MARKET) {
                "MARKET"
            } else {
                "LIMIT"
            }
            .to_string(),
            client_id: if order.client_order_id.as_str().is_empty() {
                None
            } else {
                Some(order.client_order_id.as_str().to_string())
            },
        };

        let body = serde_json::to_string(&req)?;

        // get connection and send
        let client = self.pool.get_connection_tls(&self.api_host, 443)?;

        let http_req_id = client
            .post("/v1/orders")
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .body(body)
            .send()?;

        self.pending.insert(http_req_id, RequestType::Place(order));
        debug!("place_order sent, http_req_id={}", http_req_id);
        Ok(())
    }

    async fn cancel_order(&mut self, cancel: CancelOrder) -> ParadexLightResult<()> {
        let client_id = cancel.client_order_id.as_str();
        if client_id.is_empty() {
            return Err(ParadexLightError::MissingCloid);
        }

        // use cancel by client_id endpoint
        let path = format!("/v1/orders/by_client_id/{}", client_id);

        let client = self.pool.get_connection_tls(&self.api_host, 443)?;

        let http_req_id = client
            .delete(&path)
            .header("Authorization", &self.auth_header)
            .send()?;

        self.pending
            .insert(http_req_id, RequestType::Cancel(cancel));
        debug!("cancel_order sent, http_req_id={}", http_req_id);
        Ok(())
    }

    async fn replace_order(&mut self, replace: ReplaceOrder) -> ParadexLightResult<()> {
        // replace implemented as cancel + place (no state dependency on oid mapping)
        let client_id = replace.client_order_id.as_str();
        if client_id.is_empty() {
            return Err(ParadexLightError::MissingCloid);
        }

        // 1. cancel existing order by cloid
        let cancel_path = format!("/v1/orders/by_client_id/{}", client_id);
        let client = self.pool.get_connection_tls(&self.api_host, 443)?;
        let _cancel_req_id = client
            .delete(&cancel_path)
            .header("Authorization", &self.auth_header)
            .send()?;
        // note: we don't track cancel response, fire and forget

        // 2. place new order with same cloid
        let market = Self::format_market(replace.symbol.as_str());
        let side = Self::to_sign_side(replace.side);
        let order_type = Self::to_sign_order_type(replace.order_type);

        let (price, size) = if let Some(mkt) = self.market_info.get(&market) {
            let p_str = mkt.format_price(replace.new_price);
            let s_str = mkt.format_size(replace.new_qty);
            (
                rust_decimal::Decimal::from_str(&p_str).unwrap_or_default(),
                rust_decimal::Decimal::from_str(&s_str).unwrap_or_default(),
            )
        } else {
            debug!(
                "market {} not found in cache, using default precision",
                market
            );
            (
                rust_decimal::Decimal::from_f64_retain(replace.new_price)
                    .unwrap_or_default()
                    .round_dp(8),
                rust_decimal::Decimal::from_f64_retain(replace.new_qty)
                    .unwrap_or_default()
                    .round_dp(8),
            )
        };

        let (sig, timestamp) = sign_order(
            &market,
            side,
            order_type,
            size,
            Some(price),
            &self.signing_key,
            self.chain_id,
            self.account,
        )?;

        let req = CreateOrderRequest {
            instruction: Self::tif_to_instruction(replace.time_in_force).to_string(),
            market,
            price: price.to_string(),
            side: if matches!(replace.side, Side::LONG) {
                "BUY"
            } else {
                "SELL"
            }
            .to_string(),
            signature: format_signature(&sig),
            signature_timestamp: timestamp,
            size: size.to_string(),
            order_type: if matches!(replace.order_type, OrderType::MARKET) {
                "MARKET"
            } else {
                "LIMIT"
            }
            .to_string(),
            client_id: Some(client_id.to_string()),
        };

        let body = serde_json::to_string(&req)?;
        let client = self.pool.get_connection_tls(&self.api_host, 443)?;

        let http_req_id = client
            .post("/v1/orders")
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .body(body)
            .send()?;

        self.pending
            .insert(http_req_id, RequestType::Replace(replace));
        debug!(
            "replace_order sent (cancel+place), http_req_id={}",
            http_req_id
        );
        Ok(())
    }

    async fn query_order_status(&mut self, cloid: ClientOrderId) -> ParadexLightResult<()> {
        let client_id = cloid.as_str();
        if client_id.is_empty() {
            return Err(ParadexLightError::MissingCloid);
        }

        // use by_client_id endpoint to get order regardless of status
        let path = format!("/v1/orders/by_client_id/{}", client_id);

        let client = self.pool.get_connection_tls(&self.api_host, 443)?;

        let http_req_id = client
            .get(&path)
            .header("Authorization", &self.auth_header)
            .send()?;

        self.pending
            .insert(http_req_id, RequestType::QueryStale(cloid));
        debug!("query_order_status sent, http_req_id={}", http_req_id);
        Ok(())
    }

    async fn produce(&mut self) -> Option<OrderResponse> {
        // proactively refresh jwt if nearing expiration
        if self.jwt_needs_refresh() {
            self.refresh_jwt_sync();
        }

        // drive network i/o - reads sockets, parses responses
        if let Err(e) = self.pool.poll() {
            error!("poll error: {}", e);
        }

        // check for responses
        let (_host, result) = self.pool.recv()?;
        let http_req_id = result.request_id();

        // check for 401 auth error and refresh if needed
        if self.check_auth_error(&result) {
            // jwt was refreshed, but this request already failed
            // the request will be retried by caller or just fail
            debug!(
                "request {} failed with 401, jwt refreshed for next requests",
                http_req_id
            );
        }

        let req_type = self.pending.remove(&http_req_id)?;

        debug!(
            "got response http_req_id={}, success={}",
            http_req_id,
            result.is_success()
        );

        match req_type {
            RequestType::Place(order) => {
                let (response, _oid_opt) = Self::parse_place_response(http_req_id, result, order);
                Some(response)
            }
            RequestType::Cancel(cancel) => {
                Some(Self::parse_cancel_response(http_req_id, result, cancel))
            }
            RequestType::Replace(replace) => {
                Some(Self::parse_replace_response(http_req_id, result, replace))
            }
            RequestType::QueryStale(cloid) => Some(Self::parse_query_stale_response(result, cloid)),
        }
    }
}
