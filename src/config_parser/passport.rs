use std::fs::File;
use std::io::BufReader;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidPassport {
    pub user_address: String,
    pub private_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParadexPassport {
    pub user_address: String,
    pub private_key: String,
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
    pub paradex: Option<ParadexPassport>
}

#[derive(Debug, Clone, Deserialize)]
struct Passport {
    pub id: u16,
    pub details: PassportDetails,
}

#[derive(Debug, Clone, Deserialize)]
struct PassportConfig {
    pub passport_configs: Vec<Passport>,
}

impl PassportConfig {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>>{
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let data: Self = serde_json::from_reader(reader)?;
        Ok(data)
    }
}


mod test{
    use super::*;
    #[test]
    fn test_read(){
        let cfg = PassportConfig::new("/home/maxwell/dev/personal/mdfeed/md_feed/templates/passport_template.json");
        match cfg{
            Ok(data) => {
                println!("Data loaded: {:#?}", data);
            },
            Err(e) => {
                panic!("Failed: {:?}", e);
            },
        }
    }
}