use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Album {
    pub id: i64,
    pub name: Option<String>,
    pub artist: Option<String>,
    pub artist_id: Option<i64>,
    pub year: Option<i64>,
    pub track_count: i64,
    pub timestamp_added: Option<i64>,
    pub cover_url: String,
    pub album_type: Option<String>,
    pub label: Option<String>,
    pub release_date: Option<String>,
}
