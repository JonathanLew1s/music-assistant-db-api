use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Page<T: Serialize> {
    pub total: i64,
    pub offset: i64,
    pub limit: i64,
    pub items: Vec<T>,
}
