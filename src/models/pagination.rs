use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    #[serde(default)]
    pub offset: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 { 100 }

impl PaginationParams {
    pub fn clamped_limit(&self) -> i64 {
        self.limit.clamp(1, 1000)
    }
}

#[derive(Debug, Serialize)]
pub struct Page<T: Serialize> {
    pub total: i64,
    pub offset: i64,
    pub limit: i64,
    pub items: Vec<T>,
}
