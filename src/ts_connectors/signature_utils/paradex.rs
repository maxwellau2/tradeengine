//! paradex signature utilities for order signing and authentication
//!
//! extracted from paradex sdk for use with custom lightweight http client.
//! handles:
//! - eth key -> l2 (stark) key derivation
//! - auth message signing (jwt acquisition)
//! - order signing
//! - modify order signing

use num_bigint::BigUint;
use num_traits::{Num, One};
use reqwest::header::{HeaderMap, HeaderValue};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use sha2::{Digest, Sha256};
use starknet_core::crypto::compute_hash_on_elements;
use starknet_core::types::Felt;
use starknet_core::utils::{cairo_short_string_to_felt, starknet_keccak};
use starknet_crypto::{PedersenHasher, Signature};
use starknet_signers::SigningKey;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

use alloy_primitives::{B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{Eip712Domain, SolStruct, sol};

// ============================================================================
// errors
// ============================================================================

#[derive(Debug, Error)]
pub enum SignatureError {
    #[error("starknet error: {0}")]
    Starknet(String),
    #[error("type conversion error: {0}")]
    TypeConversion(String),
    #[error("time error: {0}")]
    Time(String),
    #[error("key derivation error: {0}")]
    KeyDerivation(String),
}

pub type Result<T> = std::result::Result<T, SignatureError>;

// ============================================================================
// constants
// ============================================================================

// short string encoding of 'StarkNet Message'
const STARKNET_MESSAGE_PREFIX: Felt = Felt::from_raw([
    257012186512350467,
    18446744073709551605,
    10480951322775611302,
    16156019428408348868,
]);

// stark curve order for key derivation
const STARK_EC_ORDER_HEX: &str = "0800000000000010ffffffffffffffffb781126dcae7b2321e66a241adc64d2f";

// quantize factor for price/size (10^8)
const QUANTIZE_FACTOR: i64 = 100_000_000;

// type hashes (computed once via lazy_lock)
static REQUEST_TYPE_HASH: LazyLock<Felt> = LazyLock::new(|| {
    starknet_keccak(
        "Request(method:felt,path:felt,body:felt,timestamp:felt,expiration:felt)".as_bytes(),
    )
});

static ORDER_TYPE_HASH: LazyLock<Felt> = LazyLock::new(|| {
    starknet_keccak(
        "Order(timestamp:felt,market:felt,side:felt,orderType:felt,size:felt,price:felt)"
            .as_bytes(),
    )
});

static MODIFY_ORDER_TYPE_HASH: LazyLock<Felt> = LazyLock::new(|| {
    starknet_keccak(
        "ModifyOrder(timestamp:felt,market:felt,side:felt,orderType:felt,size:felt,price:felt,id:felt)"
            .as_bytes(),
    )
});

// ============================================================================
// key derivation (eth -> l2/stark)
// ============================================================================

sol! {
    struct Constant {
        string action;
    }
}

/// derive paradex l2 private key from ethereum private key
///
/// signs eip-712 message "STARK Key" and grinds the signature to valid stark scalar
pub fn derive_l2_private_key(eth_signer: &PrivateKeySigner) -> Result<Felt> {
    let domain = Eip712Domain::new(
        Some("Paradex".into()),
        Some("1".into()),
        Some(U256::from(1u64)),
        None,
        None,
    );

    let message = Constant {
        action: "STARK Key".into(),
    };

    let digest: B256 = message.eip712_signing_hash(&domain);

    let sig = eth_signer
        .sign_hash_sync(&digest)
        .map_err(|e| SignatureError::Starknet(format!("failed to sign: {}", e)))?;

    let sig_bytes = sig.as_bytes();
    private_key_from_signature(&sig_bytes)
}

/// grind signature r component to valid stark private key
fn private_key_from_signature(sig_bytes: &[u8]) -> Result<Felt> {
    if sig_bytes.len() < 64 {
        return Err(SignatureError::KeyDerivation("signature too short".into()));
    }

    // r component is first 32 bytes
    let r: &[u8] = &sig_bytes[..32];
    let priv_bytes = grind_key(r)?;
    Ok(Felt::from_bytes_be(&priv_bytes))
}

/// grind seed into scalar < stark_order using sha256(seed || counter)
fn grind_key(seed: &[u8]) -> Result<[u8; 32]> {
    let order = BigUint::from_str_radix(STARK_EC_ORDER_HEX, 16)
        .map_err(|_| SignatureError::KeyDerivation("bigint parse error".into()))?;

    let sha256_max = BigUint::one() << 256;
    let max_allowed = &sha256_max - (&sha256_max % &order);

    let mut counter: u32 = 0;
    loop {
        if counter == u32::MAX {
            return Err(SignatureError::KeyDerivation("counter overflow".into()));
        }

        let candidate = hash_with_index(seed, counter);

        if candidate < max_allowed {
            let reduced = candidate % &order;
            let mut out = [0u8; 32];
            let cand_be = reduced.to_bytes_be();
            if cand_be.len() > 32 {
                return Err(SignatureError::KeyDerivation("bigint too large".into()));
            }
            out[32 - cand_be.len()..].copy_from_slice(&cand_be);
            return Ok(out);
        }
        counter += 1;
    }
}

fn hash_with_index(seed: &[u8], index: u32) -> BigUint {
    let index_bytes = encode_counter(index);
    let mut buf = Vec::with_capacity(seed.len() + index_bytes.len());
    buf.extend_from_slice(seed);
    buf.extend_from_slice(&index_bytes);
    BigUint::from_bytes_be(&Sha256::digest(&buf))
}

fn encode_counter(counter: u32) -> Vec<u8> {
    if counter == 0 {
        return vec![0];
    }
    let mut value = counter;
    let mut bytes = Vec::new();
    while value > 0 {
        bytes.push((value & 0xff) as u8);
        value >>= 8;
    }
    bytes.reverse();
    bytes
}

// ============================================================================
// domain hash (cached)
// ============================================================================

/// compute domain hash for given chain_id
/// note: paradex swaps chainId/version order vs snip-12 spec
pub fn domain_hash(chain_id: Felt) -> Result<Felt> {
    let domain_name_hash =
        starknet_keccak("StarkNetDomain(name:felt,chainId:felt,version:felt)".as_bytes());
    Ok(compute_hash_on_elements(&[
        domain_name_hash,
        cairo_short_string_to_felt("Paradex")
            .map_err(|e| SignatureError::Starknet(e.to_string()))?,
        chain_id,
        Felt::ONE,
    ]))
}

// ============================================================================
// authentication (jwt)
// ============================================================================

/// generate auth headers for jwt acquisition
/// returns (system_time, headers) - system_time for jwt expiry tracking
pub fn auth_headers(
    chain_id: &Felt,
    signing_key: &SigningKey,
    account: &Felt,
) -> Result<(SystemTime, HeaderMap)> {
    let system_timestamp = SystemTime::now();
    let timestamp: u128 = system_timestamp
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SignatureError::Time(e.to_string()))?
        .as_secs()
        .into();

    let expiration = timestamp + 60 * 60; // 1 hour
    let message_hash = auth_message_hash(*chain_id, timestamp, expiration, *account)?;
    let signature = signing_key
        .sign(&message_hash)
        .map_err(|e| SignatureError::Starknet(e.to_string()))?;

    let account_str = account.to_hex_string();
    let signature_str = format!(r#"["{}","{}"]"#, signature.r, signature.s);

    let mut headers = HeaderMap::with_capacity(4);
    headers.insert("PARADEX-STARKNET-ACCOUNT", account_str.parse().unwrap());
    headers.insert("PARADEX-STARKNET-SIGNATURE", signature_str.parse().unwrap());
    headers.insert("PARADEX-TIMESTAMP", timestamp.to_string().parse().unwrap());
    headers.insert(
        "PARADEX-SIGNATURE-EXPIRATION",
        expiration.to_string().parse().unwrap(),
    );
    Ok((system_timestamp, headers))
}

fn auth_message_hash(
    chain_id: Felt,
    timestamp: u128,
    expiration: u128,
    address: Felt,
) -> Result<Felt> {
    let request_hash = compute_hash_on_elements(&[
        *REQUEST_TYPE_HASH,
        cairo_short_string_to_felt("POST").map_err(|e| SignatureError::Starknet(e.to_string()))?,
        cairo_short_string_to_felt("/v1/auth")
            .map_err(|e| SignatureError::Starknet(e.to_string()))?,
        cairo_short_string_to_felt("").map_err(|e| SignatureError::Starknet(e.to_string()))?,
        timestamp.into(),
        expiration.into(),
    ]);

    let mut hasher = PedersenHasher::default();
    hasher.update(STARKNET_MESSAGE_PREFIX);
    hasher.update(domain_hash(chain_id)?);
    hasher.update(address);
    hasher.update(request_hash);

    Ok(hasher.finalize())
}

// ============================================================================
// order signing
// ============================================================================

/// side enum for order signing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn felt(&self) -> Felt {
        match self {
            Side::Buy => Felt::ONE,
            Side::Sell => Felt::TWO,
        }
    }
}

/// order type enum for signing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit,
}

impl OrderType {
    pub fn felt(&self) -> Result<Felt> {
        match self {
            OrderType::Market => cairo_short_string_to_felt("MARKET"),
            OrderType::Limit => cairo_short_string_to_felt("LIMIT"),
        }
        .map_err(|e| SignatureError::Starknet(e.to_string()))
    }
}

/// sign an order, returns (signature, timestamp_ms)
pub fn sign_order(
    market: &str,
    side: Side,
    order_type: OrderType,
    size: Decimal,
    price: Option<Decimal>,
    signing_key: &SigningKey,
    chain_id: Felt,
    account: Felt,
) -> Result<(Signature, u128)> {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SignatureError::Time(e.to_string()))?
        .as_millis();

    let price_scaled = if let Some(p) = price {
        (p * Decimal::from(QUANTIZE_FACTOR))
            .to_i64()
            .ok_or_else(|| SignatureError::TypeConversion("price conversion failed".into()))?
    } else {
        0
    };

    let size_scaled = (size * Decimal::from(QUANTIZE_FACTOR))
        .to_i64()
        .ok_or_else(|| SignatureError::TypeConversion("size conversion failed".into()))?;

    let order_hash = compute_hash_on_elements(&[
        *ORDER_TYPE_HASH,
        timestamp_ms.into(),
        cairo_short_string_to_felt(market).map_err(|e| SignatureError::Starknet(e.to_string()))?,
        side.felt(),
        order_type.felt()?,
        size_scaled.into(),
        price_scaled.into(),
    ]);

    let mut hasher = PedersenHasher::default();
    hasher.update(STARKNET_MESSAGE_PREFIX);
    hasher.update(domain_hash(chain_id)?);
    hasher.update(account);
    hasher.update(order_hash);

    let hash = hasher.finalize();
    let signature = signing_key
        .sign(&hash)
        .map_err(|e| SignatureError::Starknet(e.to_string()))?;

    Ok((signature, timestamp_ms))
}

/// sign a modify order request, returns (signature, timestamp_ms)
pub fn sign_modify_order(
    order_id: &str,
    market: &str,
    side: Side,
    order_type: OrderType,
    new_size: Decimal,
    new_price: Option<Decimal>,
    signing_key: &SigningKey,
    chain_id: Felt,
    account: Felt,
) -> Result<(Signature, u128)> {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| SignatureError::Time(e.to_string()))?
        .as_millis();

    let price_scaled = if let Some(p) = new_price {
        (p * Decimal::from(QUANTIZE_FACTOR))
            .to_i64()
            .ok_or_else(|| SignatureError::TypeConversion("price conversion failed".into()))?
    } else {
        0
    };

    let size_scaled = (new_size * Decimal::from(QUANTIZE_FACTOR))
        .to_i64()
        .ok_or_else(|| SignatureError::TypeConversion("size conversion failed".into()))?;

    // order_id can be numeric string or short string
    let id_felt = str_to_felt(order_id)?;

    let order_hash = compute_hash_on_elements(&[
        *MODIFY_ORDER_TYPE_HASH,
        timestamp_ms.into(),
        cairo_short_string_to_felt(market).map_err(|e| SignatureError::Starknet(e.to_string()))?,
        side.felt(),
        order_type.felt()?,
        size_scaled.into(),
        price_scaled.into(),
        id_felt,
    ]);

    let mut hasher = PedersenHasher::default();
    hasher.update(STARKNET_MESSAGE_PREFIX);
    hasher.update(domain_hash(chain_id)?);
    hasher.update(account);
    hasher.update(order_hash);

    let hash = hasher.finalize();
    let signature = signing_key
        .sign(&hash)
        .map_err(|e| SignatureError::Starknet(e.to_string()))?;

    Ok((signature, timestamp_ms))
}

fn str_to_felt(s: &str) -> Result<Felt> {
    if s.chars().all(|c| c.is_ascii_digit()) {
        Felt::from_dec_str(s).map_err(|e| SignatureError::Starknet(e.to_string()))
    } else {
        cairo_short_string_to_felt(s).map_err(|e| SignatureError::Starknet(e.to_string()))
    }
}

// ============================================================================
// account address derivation
// ============================================================================

use starknet_core::utils::{get_contract_address, get_selector_from_name};

/// compute account address from public key and contract hashes
pub fn account_address(
    public_key: Felt,
    paraclear_account_proxy_hash: Felt,
    paraclear_account_hash: Felt,
) -> Result<Felt> {
    let calldata: [Felt; 5] = [
        paraclear_account_hash,
        get_selector_from_name("initialize")
            .map_err(|e| SignatureError::Starknet(e.to_string()))?,
        Felt::TWO,
        public_key,
        Felt::ZERO,
    ];
    Ok(get_contract_address(
        public_key,
        paraclear_account_proxy_hash,
        &calldata,
        Felt::ZERO,
    ))
}

// ============================================================================
// helper: format signature for api
// ============================================================================

/// format signature as json array string for paradex api
pub fn format_signature(sig: &Signature) -> String {
    format!(r#"["{}","{}"]"#, sig.r.to_bigint(), sig.s.to_bigint())
}

// ============================================================================
// tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_derive_l2_key() {
        let eth_signer = PrivateKeySigner::from_str(
            "0x58d27b1d66da0dee9193105c848855b43eeceb14844f2b1de00cdcb1bdce3643",
        )
        .expect("failed to create signer");

        let l2_key = derive_l2_private_key(&eth_signer).expect("failed to derive key");
        let expected =
            Felt::from_str("0x549aa9cb8328a12b1394f99f9430ba2dbc2b5c26b8a4c3b9d2b3ca3765669b2")
                .unwrap();
        assert_eq!(l2_key, expected);
    }

    #[test]
    fn test_domain_hash() {
        let chain_id = cairo_short_string_to_felt("PRIVATE_SN_PARACLEAR_MAINNET").unwrap();
        let hash = domain_hash(chain_id).unwrap();
        assert_eq!(
            hash,
            Felt::from_hex_unchecked(
                "0x6f74f207280b65cf663fb8d7763fac1e7398cd6d7da5d7681dc300ee4278a0a"
            )
        );
    }
}
