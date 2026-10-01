use serde::{Deserialize, Deserializer};

use crate::types::common::TimeInForce;

// ============================================================================
// hyperliquid ws post response parsing
// ============================================================================
// the ws response structure is deeply nested:
// { "channel": "post", "data": { "id": N, "response": { "type": "action", "payload": {...} } } }

/// top-level ws message wrapper
#[derive(Debug, Deserialize)]
pub struct HlWsMessage {
    pub channel: String,
    pub data: HlWsData,
}

// ============================================================================
// order status info response parsing
// ============================================================================

/// info response wrapper for ws post
#[derive(Debug, Deserialize)]
pub struct HlInfoWsMessage {
    pub channel: String,
    pub data: HlInfoWsData,
}

#[derive(Debug, Deserialize)]
pub struct HlInfoWsData {
    pub id: u8,
    pub response: HlInfoResponse,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum HlInfoResponse {
    #[serde(rename = "info")]
    Info { payload: HlInfoPayload },
}

/// info payload - can be orderStatus or other info types
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum HlInfoPayload {
    OrderStatus(HlOrderStatusResp),
    Unknown(serde_json::Value),
}

/// orderStatus response from info endpoint
#[derive(Debug, Deserialize)]
pub struct HlOrderStatusResp {
    pub status: String, // "order" or "unknownOid"
    pub order: Option<HlOrderInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HlOrderInfo {
    pub order: HlOrderDetails,
    pub status: String, // "open", "filled", "canceled", etc
    pub status_timestamp: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HlOrderDetails {
    pub coin: String,
    pub side: String, // "A" for ask/sell, "B" for bid/buy
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub orig_sz: String,
    pub cloid: Option<String>,
}

/// data envelope containing request id and response
#[derive(Debug, Deserialize)]
pub struct HlWsData {
    pub id: u8,
    pub response: HlResponseType,
}

/// response type discriminator - currently only "action" is used for post responses
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum HlResponseType {
    #[serde(rename = "action")]
    Action { payload: HlPayload },
}

/// payload containing status and the actual response data
#[derive(Debug, Deserialize)]
pub struct HlPayload {
    pub status: HlStatus,
    pub response: HlResponse,
}

/// status can be "ok" or "err"
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HlStatus {
    Ok,
    Err,
}

/// the inner response - varies by action type (order, cancel, default)
/// or can be a plain error string when status is "err"
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum HlResponse {
    /// typed response with data
    Typed(HlTypedResponse),
    /// error string response
    Error(String),
}

/// typed responses based on action type
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum HlTypedResponse {
    #[serde(rename = "order")]
    Order { data: HlOrderData },
    #[serde(rename = "cancel")]
    Cancel { data: HlCancelData },
    #[serde(rename = "default")]
    Default,
}

/// order response data containing array of statuses
#[derive(Debug, Deserialize)]
pub struct HlOrderData {
    pub statuses: Vec<HlOrderStatus>,
}

/// individual order status - resting, filled, or error
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum HlOrderStatus {
    /// order placed on book
    Resting { resting: HlRestingInfo },
    /// order filled (partially or fully)
    Filled { filled: HlFilledInfo },
    /// order rejected with error
    Error { error: String },
}

/// info for resting (on-book) order
#[derive(Debug, Deserialize)]
pub struct HlRestingInfo {
    pub oid: u64,
    pub cloid: Option<String>,
}

/// info for filled order
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HlFilledInfo {
    pub oid: u64,
    /// total size filled as string
    pub total_sz: String,
    /// average fill price as string
    pub avg_px: String,
    pub cloid: Option<String>,
}

/// cancel response data
#[derive(Debug, Deserialize)]
pub struct HlCancelData {
    pub statuses: Vec<HlCancelStatus>,
}

/// cancel status - either "success" string or error object
#[derive(Debug)]
pub enum HlCancelStatus {
    Success,
    Error(String),
}

// custom deserializer for HlCancelStatus since it can be "success" string or {"error": "..."}
impl<'de> Deserialize<'de> for HlCancelStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // try deserializing as a generic json value first
        let value = serde_json::Value::deserialize(deserializer)?;

        match value {
            serde_json::Value::String(s) if s == "success" => Ok(HlCancelStatus::Success),
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::String(err)) = map.get("error") {
                    Ok(HlCancelStatus::Error(err.clone()))
                } else {
                    Err(serde::de::Error::custom("expected error field in object"))
                }
            }
            _ => Err(serde::de::Error::custom(
                "expected 'success' string or error object",
            )),
        }
    }
}

// ============================================================================
// HyperliquidResponse - high-level parsed response with helper methods
// ============================================================================

/// parsed hyperliquid response with convenience methods
#[derive(Debug)]
pub struct HyperliquidResponse {
    pub request_id: u8,
    pub status: HlStatus,
    pub result: HlResult,
}

/// simplified result enum for easier handling
#[derive(Debug)]
pub enum HlResult {
    /// order placed successfully - contains oid and optional cloid
    OrderResting { oid: u64, cloid: Option<String> },
    /// order filled - contains oid, fill size, avg price
    OrderFilled {
        oid: u64,
        total_sz: f64,
        avg_px: f64,
        cloid: Option<String>,
    },
    /// order rejected
    OrderError(String),
    /// cancel succeeded
    CancelSuccess,
    /// cancel failed
    CancelError(String),
    /// modify/replace succeeded (default response type)
    ModifySuccess,
    /// top-level error (status: err)
    Error(String),
}

impl HyperliquidResponse {
    /// parse ws text message into HyperliquidResponse
    pub fn from(text: &str) -> Result<Self, serde_json::Error> {
        let msg: HlWsMessage = serde_json::from_str(text)?;

        let request_id = msg.data.id;

        // extract payload from action response
        let HlResponseType::Action { payload } = msg.data.response;

        let status = payload.status;

        let result = match payload.response {
            HlResponse::Error(err) => HlResult::Error(err),
            HlResponse::Typed(typed) => match typed {
                HlTypedResponse::Order { data } => {
                    // take first status (batch orders would have multiple)
                    match data.statuses.into_iter().next() {
                        Some(HlOrderStatus::Resting { resting }) => HlResult::OrderResting {
                            oid: resting.oid,
                            cloid: resting.cloid,
                        },
                        Some(HlOrderStatus::Filled { filled }) => HlResult::OrderFilled {
                            oid: filled.oid,
                            total_sz: filled.total_sz.parse().unwrap_or(0.0),
                            avg_px: filled.avg_px.parse().unwrap_or(0.0),
                            cloid: filled.cloid,
                        },
                        Some(HlOrderStatus::Error { error }) => HlResult::OrderError(error),
                        None => HlResult::OrderError("empty statuses array".to_string()),
                    }
                }
                HlTypedResponse::Cancel { data } => match data.statuses.into_iter().next() {
                    Some(HlCancelStatus::Success) => HlResult::CancelSuccess,
                    Some(HlCancelStatus::Error(e)) => HlResult::CancelError(e),
                    None => HlResult::CancelError("empty statuses array".to_string()),
                },
                HlTypedResponse::Default => HlResult::ModifySuccess,
            },
        };

        Ok(HyperliquidResponse {
            request_id,
            status,
            result,
        })
    }

    /// check if request was successful
    pub fn is_ok(&self) -> bool {
        self.status == HlStatus::Ok
    }

    /// get order id if response contains one
    pub fn oid(&self) -> Option<u64> {
        match &self.result {
            HlResult::OrderResting { oid, .. } => Some(*oid),
            HlResult::OrderFilled { oid, .. } => Some(*oid),
            _ => None,
        }
    }

    /// get client order id if response contains one
    pub fn cloid(&self) -> Option<&str> {
        match &self.result {
            HlResult::OrderResting { cloid, .. } => cloid.as_deref(),
            HlResult::OrderFilled { cloid, .. } => cloid.as_deref(),
            _ => None,
        }
    }

    /// get error message if response is an error
    pub fn error(&self) -> Option<&str> {
        match &self.result {
            HlResult::OrderError(e) => Some(e),
            HlResult::CancelError(e) => Some(e),
            HlResult::Error(e) => Some(e),
            _ => None,
        }
    }
}

// ============================================================================
// hyperliquid response conversion - maps hl-specific parsed response to generic types
// ============================================================================

use crate::types::{
    common::{ClientOrderId, Order, OrderState, OrderType, Side, Symbol, Venue},
    trade_server::{
        CancelOrder, CancelOrderResp, CancelOrderStatus, PlaceOrder, PlaceOrderResp,
        PlaceOrderStatus, QueryStaleOrderResp, ReplaceOrder, ReplaceOrderResp, ReplaceOrderStatus,
    },
};

/// convert hyperliquid response to generic PlaceOrderResp
pub fn to_place_resp(resp: HyperliquidResponse, request: PlaceOrder) -> PlaceOrderResp {
    let status = match resp.result {
        HlResult::OrderResting { oid, .. } => PlaceOrderStatus::Resting { oid },
        HlResult::OrderFilled {
            oid,
            avg_px,
            total_sz,
            ..
        } => PlaceOrderStatus::Filled {
            oid,
            avg_px,
            total_sz,
        },
        HlResult::OrderError(e) => PlaceOrderStatus::Rejected(e),
        HlResult::Error(e) => PlaceOrderStatus::Rejected(e),
        _ => PlaceOrderStatus::Rejected("unexpected response type".to_string()),
    };

    PlaceOrderResp {
        request_id: resp.request_id,
        request,
        status,
    }
}

/// convert hyperliquid response to generic CancelOrderResp
pub fn to_cancel_resp(resp: HyperliquidResponse, request: CancelOrder) -> CancelOrderResp {
    let status = match resp.result {
        HlResult::CancelSuccess => CancelOrderStatus::Success,
        HlResult::CancelError(e) => CancelOrderStatus::Failed(e),
        HlResult::Error(e) => CancelOrderStatus::Failed(e),
        _ => CancelOrderStatus::Failed("unexpected response type".to_string()),
    };

    CancelOrderResp {
        request_id: resp.request_id,
        request,
        status,
    }
}

/// convert hyperliquid response to generic ReplaceOrderResp
pub fn to_replace_resp(resp: HyperliquidResponse, request: ReplaceOrder) -> ReplaceOrderResp {
    let status = match resp.result {
        HlResult::ModifySuccess => ReplaceOrderStatus::Success,
        HlResult::Error(e) => ReplaceOrderStatus::Failed(e),
        _ => ReplaceOrderStatus::Failed("unexpected response type".to_string()),
    };

    ReplaceOrderResp {
        request_id: resp.request_id,
        request,
        status,
    }
}

/// parse order status info response and convert to QueryStaleOrderResp
pub fn parse_order_status_resp(text: &str, cloid: ClientOrderId) -> Option<QueryStaleOrderResp> {
    let msg: HlInfoWsMessage = serde_json::from_str(text).ok()?;
    let HlInfoResponse::Info { payload } = msg.data.response;

    match payload {
        HlInfoPayload::OrderStatus(resp) => {
            if resp.status == "unknownOid" || resp.order.is_none() {
                // order not found on exchange
                return Some(QueryStaleOrderResp::not_found(cloid, Venue::Hyperliquid));
            }

            let info = resp.order?;
            let details = &info.order;

            // convert hl status to OrderState
            let state = match info.status.as_str() {
                "open" => OrderState::NEW,
                "filled" => OrderState::FILLED,
                "canceled" | "marginCanceled" => OrderState::CANCELLED,
                "rejected" => OrderState::REJECTED,
                "triggered" => OrderState::NEW, // triggered orders become open
                _ => OrderState::UNKNOWN,
            };

            // convert side: "A" = ask/sell, "B" = bid/buy
            let side = if details.side == "B" {
                Side::LONG
            } else {
                Side::SHORT
            };

            let order = Order {
                client_order_id: cloid,
                symbol: Symbol::new(&details.coin),
                venue: Venue::Hyperliquid,
                side,
                price: details.limit_px.parse().unwrap_or(0.0),
                qty: details.orig_sz.parse().unwrap_or(0.0),
                filled_qty: details.orig_sz.parse::<f64>().unwrap_or(0.0)
                    - details.sz.parse::<f64>().unwrap_or(0.0),
                order_type: OrderType::LIMIT,
                time_in_force: TimeInForce::GTC, // hl doesn't return tif in status
                state,
            };

            Some(QueryStaleOrderResp::found(cloid, Venue::Hyperliquid, order))
        }
        HlInfoPayload::Unknown(_) => None,
    }
}

// response struct for /info meta endpoint
// serde ignores unknown fields by default, so we only define what we need
#[derive(Deserialize)]
pub struct MetaResponse {
    pub universe: Vec<AssetInfo>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AssetInfo {
    pub name: String,
    pub sz_decimals: u8,
}

// hyperliquid time-in-force mapping
pub fn tif_to_hl(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::GTC => "Gtc",
        TimeInForce::IOC => "Ioc",
        TimeInForce::PO => "Alo",  // post-only = add liquidity only
        TimeInForce::FOK => "Ioc", // hl doesn't have FOK, use IOC
        TimeInForce::UNKNOWN => "UNKNOWN",
    }
}

// format price to 5 significant figures, max (6 - sz_decimals) decimal places
// sz_decimals is the asset's size decimal precision from meta endpoint
// integers always valid regardless of sig figs
pub fn format_price(val: f64, sz_decimals: u8) -> String {
    // if it's effectively an integer, return as integer
    if (val.round() - val).abs() < 1e-9 {
        return format!("{:.0}", val.round());
    }

    // max decimal places for price = 6 - sz_decimals (perps)
    let max_decimals = (6_i32 - sz_decimals as i32).max(0) as usize;

    // round to 5 significant figures
    let magnitude = val.abs().log10().floor() as i32;
    let scale = 10_f64.powi(4 - magnitude); // 5 sig figs means 4 digits after first
    let rounded = (val * scale).round() / scale;

    // use the lesser of: sig fig decimals needed, or max allowed decimals
    let sig_fig_decimals = (4 - magnitude).max(0) as usize;
    let decimals = sig_fig_decimals.min(max_decimals);

    // re-round to actual decimal places we'll use
    let final_scale = 10_f64.powi(decimals as i32);
    let final_rounded = (rounded * final_scale).round() / final_scale;

    let s = format!("{:.prec$}", final_rounded, prec = decimals);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

// format size with specific decimal precision (for sz_decimals)
// truncates to sz_decimals places, removes trailing decimal zeros only
pub fn format_size(val: f64, decimals: u8) -> String {
    let multiplier = 10_f64.powi(decimals as i32);
    let rounded = (val * multiplier).floor() / multiplier;

    if decimals == 0 {
        // no decimals, just format as integer
        return format!("{:.0}", rounded);
    }

    // format with decimals, then trim trailing zeros after decimal point
    let s = format!("{:.prec$}", rounded, prec = decimals as usize);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

// parse formatted cloid back to simple form
// 0x00000000000000000000000000000005 -> "5"
// returns original string if not a simple numeric cloid
pub fn parse_cloid(formatted: &str) -> String {
    if !formatted.starts_with("0x") || formatted.len() != 34 {
        return formatted.to_string();
    }

    // try parsing as u128 and convert back to string
    let hex_str = &formatted[2..]; // strip 0x
    if let Ok(n) = u128::from_str_radix(hex_str, 16) {
        return n.to_string();
    }

    formatted.to_string()
}

// format cloid as 128-bit hex string (0x + 32 hex chars)
// hyperliquid requires this exact format for client order ids
pub fn format_cloid(cloid: &str) -> String {
    // if already properly formatted, return as-is
    if cloid.starts_with("0x") && cloid.len() == 34 {
        return cloid.to_string();
    }

    // try parsing as integer and format to hex
    if let Ok(n) = cloid.parse::<u128>() {
        return format!("0x{:032x}", n);
    }

    // if it's a short hex without prefix, pad it
    let hex_str = cloid.strip_prefix("0x").unwrap_or(cloid);
    if hex_str.len() <= 32 && hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
        return format!("0x{:0>32}", hex_str);
    }

    // fallback: hash the string to get a valid 128-bit id
    let hash = ethers::utils::keccak256(cloid.as_bytes());
    format!("0x{}", hex::encode(&hash[..16]))
}

// ============================================================================
// unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resting_order() {
        let json = r#"{"channel":"post","data":{"id":1,"response":{"type":"action","payload":{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":277150066327,"cloid":"0x0000000000000000000000000000007b"}}]}}}}}}"#;
        let resp = HyperliquidResponse::from(json).unwrap();
        assert!(resp.is_ok());
        assert_eq!(resp.request_id, 1);
        assert_eq!(resp.oid(), Some(277150066327));
        assert_eq!(resp.cloid(), Some("0x0000000000000000000000000000007b"));
    }

    #[test]
    fn test_parse_filled_order() {
        let json = r#"{"channel":"post","data":{"id":5,"response":{"type":"action","payload":{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.02","avgPx":"1891.4","oid":77747314}}]}}}}}}"#;
        let resp = HyperliquidResponse::from(json).unwrap();
        assert!(resp.is_ok());
        assert_eq!(resp.request_id, 5);
        match resp.result {
            HlResult::OrderFilled {
                oid,
                total_sz,
                avg_px,
                ..
            } => {
                assert_eq!(oid, 77747314);
                assert!((total_sz - 0.02).abs() < 0.001);
                assert!((avg_px - 1891.4).abs() < 0.1);
            }
            _ => panic!("expected OrderFilled"),
        }
    }

    #[test]
    fn test_parse_order_error() {
        let json = r#"{"channel":"post","data":{"id":3,"response":{"type":"action","payload":{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"Order must have minimum value of $10."}]}}}}}}"#;
        let resp = HyperliquidResponse::from(json).unwrap();
        assert_eq!(resp.error(), Some("Order must have minimum value of $10."));
    }

    #[test]
    fn test_parse_cancel_success() {
        let json = r#"{"channel":"post","data":{"id":2,"response":{"type":"action","payload":{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}}}}"#;
        let resp = HyperliquidResponse::from(json).unwrap();
        assert!(resp.is_ok());
        assert!(matches!(resp.result, HlResult::CancelSuccess));
    }

    #[test]
    fn test_parse_cancel_error() {
        let json = r#"{"channel":"post","data":{"id":4,"response":{"type":"action","payload":{"status":"ok","response":{"type":"cancel","data":{"statuses":[{"error":"Order was never placed, already canceled, or filled."}]}}}}}}"#;
        let resp = HyperliquidResponse::from(json).unwrap();
        assert_eq!(
            resp.error(),
            Some("Order was never placed, already canceled, or filled.")
        );
    }

    #[test]
    fn test_parse_modify_success() {
        let json = r#"{"channel":"post","data":{"id":2,"response":{"type":"action","payload":{"status":"ok","response":{"type":"default"}}}}}"#;
        let resp = HyperliquidResponse::from(json).unwrap();
        assert!(resp.is_ok());
        assert!(matches!(resp.result, HlResult::ModifySuccess));
    }
}
