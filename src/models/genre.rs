use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Genre {
    pub id: i64,
    pub name: Option<String>,
    pub description: Option<String>,
    pub aliases: Vec<String>,
    pub track_count: i64,
}
