use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub db_path: String,
    pub music_root: String,
    pub port: u16,
    pub api_key: Option<String>,
    pub pool_size: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let db_path = env::var("MA_DB_PATH")
            .map_err(|_| anyhow::anyhow!("MA_DB_PATH is required"))?;
        let music_root = env::var("MA_MUSIC_ROOT")
            .map_err(|_| anyhow::anyhow!("MA_MUSIC_ROOT is required"))?;
        let port = env::var("PORT")
            .unwrap_or_else(|_| "8096".into())
            .parse::<u16>()?;
        let api_key = env::var("MA_BRIDGE_API_KEY").ok().filter(|s| !s.is_empty());
        let pool_size = env::var("DB_POOL_SIZE")
            .unwrap_or_else(|_| "4".into())
            .parse::<usize>()?;
        Ok(Self { db_path, music_root, port, api_key, pool_size })
    }
}
