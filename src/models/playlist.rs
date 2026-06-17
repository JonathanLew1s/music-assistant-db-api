use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Playlist {
    pub id: i64,
    pub name: Option<String>,
    pub track_count: i64,
    pub timestamp_modified: Option<i64>,
}
