use crate::constants;

macro_rules! from_env_or_default {
    ($key:expr, $default:expr) => {{
        std::env::var($key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or($default)
    }};
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc_url: String,
    pub ws_url: String,
    pub server_address: String,
    pub db_path: String,
    pub redis_url: String,
    pub concurrency: usize,
}

impl Config {
    pub fn get_config() -> Self {
        Self {
            rpc_url: from_env_or_default!("RPC_URL", "https://testnet.riselabs.xyz".to_owned()),
            ws_url: from_env_or_default!("WS_URL", "wss://testnet.riselabs.xyz/ws".to_owned()),
            server_address: from_env_or_default!("SERVER_ADDR", "0.0.0.0:8545".to_owned()),
            db_path: from_env_or_default!("DB_PATH", "rise_index.db".to_owned()),
            redis_url: from_env_or_default!("REDIS_URL", "redis://127.0.0.1:6379".to_owned()),
            concurrency: from_env_or_default!("CONCURRENCY", constants::DEFAULT_CONCURRENCY),
        }
    }
}
