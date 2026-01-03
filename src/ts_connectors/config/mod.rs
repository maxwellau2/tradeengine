use serde::Deserialize;
use std::io::Read;
use std::{error::Error, fs::File};

use crate::types::common::{PassportId, Venue};

#[derive(Debug, Deserialize)]
pub struct TSConfig {
    /// cpu core for central sync loop (hot path)
    pub main_core: usize,
    /// starting cpu core for executors - each executor gets main_core + 1 + index
    /// e.g. if executor_core_start=2 and you have 2 exchanges, they get cores 2 and 3
    pub executor_core_start: usize,
    /// cpu core for state subscribers (shared runtime, less latency sensitive)
    pub state_core: usize,
    pub passport_path: String,
    pub passport_id: PassportId,
    pub exchanges: Vec<String>,
    pub channel_name: String,
}

impl TSConfig {
    pub fn from(filename: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = File::open(filename)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: TSConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
}

pub mod test {
    use super::*;
    #[test]
    pub fn test_read() -> Result<(), Box<dyn std::error::Error>> {
        let config = TSConfig::from("templates/ts_config.yaml")?;
        println!("Parsed Config: {:?}", config);
        Ok(())
    }
}
