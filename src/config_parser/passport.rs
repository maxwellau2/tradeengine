use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;

use super::error::{ConfigError, ConfigResult};
use crate::types::common::PassportId;

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPassport {
    pub user_address: String,
    pub private_key: String,
    #[serde(default)]
    pub is_mainnet: Option<bool>, // defaults to true if not specified
    pub vault_address: Option<String>, // optional sub-account vault
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParadexPassport {
    pub l2_private_key: Option<String>, // starknet/paradex private key (hex) - for already onboarded
    pub eth_private_key: Option<String>, // ethereum private key (hex) - for onboarding
    #[serde(default)]
    pub is_mainnet: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinancePassport {
    pub api_key: String,
    pub secret_key: String,
}
#[derive(Debug, Clone, Deserialize)]
pub struct PassportDetails {
    pub hyperliquid: Option<HyperliquidPassport>,
    pub binance: Option<BinancePassport>,
    pub paradex: Option<ParadexPassport>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Passport {
    pub id: u16,
    pub details: PassportDetails,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PassportConfig {
    pub passport_configs: Vec<Passport>,
}

impl PassportConfig {
    pub fn new(path: &str) -> ConfigResult<Self> {
        let file = File::open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ConfigError::FileNotFound(path.to_string())
            } else {
                ConfigError::Io(e)
            }
        })?;
        let reader = BufReader::new(file);
        let data: Self = serde_json::from_reader(reader)?;
        Ok(data)
    }

    pub fn find_by_passport_id(&self, passport_id: PassportId) -> ConfigResult<Passport> {
        self.passport_configs
            .iter()
            .find(|p| p.id == passport_id)
            .cloned()
            .ok_or(ConfigError::PassportNotFound(passport_id))
    }
}

mod test {
    use super::*;
    #[test]
    fn test_read() {
        let cfg = PassportConfig::new(
            "/home/maxwell/dev/personal/mdfeed/md_feed/templates/passport_template.json",
        );
        match cfg {
            Ok(data) => {
                println!("Data loaded: {:#?}", data);
            }
            Err(e) => {
                panic!("Failed: {:?}", e);
            }
        }
    }
}
