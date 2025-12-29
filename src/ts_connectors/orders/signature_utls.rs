use ethers::prelude::*;
use ethers::utils::keccak256;
use rmp_serde::to_vec_named;

// eip-712 domain uses chain_id 1337 (not arbitrum chain ids)
const EIP712_CHAIN_ID: u64 = 1337;

// eip-712 type hash for Agent(string source,bytes32 connectionId)
fn agent_type_hash() -> [u8; 32] {
    keccak256(b"Agent(string source,bytes32 connectionId)")
}

// eip-712 domain separator for hyperliquid exchange
fn domain_separator() -> [u8; 32] {
    // EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)
    let domain_type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );

    let name_hash = keccak256(b"Exchange");
    let version_hash = keccak256(b"1");
    let verifying_contract = H160::zero();

    // encode domain struct
    let mut encoded = Vec::with_capacity(160);
    encoded.extend_from_slice(&domain_type_hash);
    encoded.extend_from_slice(&name_hash);
    encoded.extend_from_slice(&version_hash);
    encoded.extend_from_slice(&H256::from_low_u64_be(EIP712_CHAIN_ID).0);
    encoded.extend_from_slice(&H256::from(verifying_contract).0);

    keccak256(&encoded)
}

// convert address string to 20-byte array
fn address_to_bytes(addr: &str) -> [u8; 20] {
    let mut out = [0u8; 20];
    let addr = addr.trim().to_lowercase();
    let addr = addr.strip_prefix("0x").unwrap_or(&addr);
    if let Ok(raw) = hex::decode(addr) {
        if raw.len() == 20 {
            out.copy_from_slice(&raw);
        }
    }
    out
}

// compute action hash: msgpack(action) + nonce(8BE) + vault_flag + [vault_addr] + [expires_flag + expires(8BE)]
fn action_hash(
    action: &serde_json::Value,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
) -> Result<[u8; 32], rmp_serde::encode::Error> {
    let mut data = to_vec_named(action)?;

    // nonce as 8 bytes big-endian
    data.extend_from_slice(&nonce.to_be_bytes());

    // vault flag and optional address
    match vault_address {
        None => data.push(0x00),
        Some(addr) => {
            data.push(0x01);
            data.extend_from_slice(&address_to_bytes(addr));
        }
    }

    // optional expires_after
    if let Some(expires) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&expires.to_be_bytes());
    }

    Ok(keccak256(&data))
}

/// sign an l1 action for hyperliquid
///
/// # arguments
/// * `wallet` - local wallet with private key
/// * `action` - action json (must use serde_json preserve_order feature)
/// * `vault_address` - optional vault for sub-account trading
/// * `nonce` - timestamp in milliseconds
/// * `is_mainnet` - true for mainnet ("a"), false for testnet ("b")
/// * `expires_after` - optional expiration timestamp
///
/// # returns
/// signature object with r, s, v fields as hyperliquid expects
pub fn sign_l1_action(
    wallet: &LocalWallet,
    action: &serde_json::Value,
    vault_address: Option<&str>,
    nonce: u64,
    is_mainnet: bool,
    expires_after: Option<u64>,
) -> Result<HyperliquidSignature, Box<dyn std::error::Error + Send + Sync>> {
    // 1. compute connection_id from action hash
    let connection_id = action_hash(action, vault_address, nonce, expires_after)?;

    // 2. build eip-712 struct hash for phantom Agent
    let source = if is_mainnet { "a" } else { "b" };
    let source_hash = keccak256(source.as_bytes());

    let mut struct_data = Vec::with_capacity(96);
    struct_data.extend_from_slice(&agent_type_hash());
    struct_data.extend_from_slice(&source_hash);
    struct_data.extend_from_slice(&connection_id);

    let struct_hash = keccak256(&struct_data);

    // 3. build final eip-712 digest: \x19\x01 + domain_separator + struct_hash
    let domain_sep = domain_separator();
    let mut final_msg = Vec::with_capacity(66);
    final_msg.push(0x19);
    final_msg.push(0x01);
    final_msg.extend_from_slice(&domain_sep);
    final_msg.extend_from_slice(&struct_hash);

    let digest = keccak256(&final_msg);

    // 4. sign the digest
    let sig = wallet.sign_hash(H256::from(digest))?;

    Ok(HyperliquidSignature {
        r: format!("0x{:064x}", sig.r),
        s: format!("0x{:064x}", sig.s),
        v: sig.v as u8,
    })
}

/// signature format expected by hyperliquid api
#[derive(Debug, Clone, serde::Serialize)]
pub struct HyperliquidSignature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_basic() {
        // test with hardhat default private key
        let wallet: LocalWallet =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .unwrap();

        let action = serde_json::json!({
            "type": "order",
            "orders": [{
                "a": 0,
                "b": true,
                "p": "30000",
                "s": "0.1",
                "r": false,
                "t": {"limit": {"tif": "Gtc"}}
            }],
            "grouping": "na"
        });

        let result = sign_l1_action(&wallet, &action, None, 1234567890, true, None);
        assert!(result.is_ok());

        let sig = result.unwrap();
        assert!(sig.r.starts_with("0x"));
        assert!(sig.s.starts_with("0x"));
        assert!(sig.v == 27 || sig.v == 28);
    }

    #[test]
    fn test_sign_with_vault() {
        let wallet: LocalWallet =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .unwrap();

        let action = serde_json::json!({
            "type": "order",
            "orders": [{
                "a": 0,
                "b": true,
                "p": "30000",
                "s": "0.1",
                "r": false,
                "t": {"limit": {"tif": "Gtc"}}
            }],
            "grouping": "na"
        });

        let vault = "0x1234567890123456789012345678901234567890";
        let result = sign_l1_action(&wallet, &action, Some(vault), 1234567890, true, None);
        assert!(result.is_ok());
    }
}
