use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Artist {
    pub id: i64,
    pub name: Option<String>,
    pub track_count: i64,
    pub album_count: i64,
}
