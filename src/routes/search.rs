use axum::extract::{Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::{Deserialize, Serialize};
use crate::{db::queries, error::AppError, models::{Track, Album, Artist}};

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    #[serde(default = "dl")] pub limit: i64,
}
fn dl() -> i64 { 10 }

#[derive(Serialize)]
pub struct SearchResponse {
    pub tracks: Vec<Track>,
    pub albums: Vec<Album>,
    pub artists: Vec<Artist>,
}

pub async fn search(
    State(pool): State<Pool>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, AppError> {
    if params.q.trim().is_empty() {
        return Err(AppError::BadRequest("q is required".into()));
    }
    let q = params.q.clone();
    let limit = params.limit.clamp(1, 50);
    let results = pool.get().await?
        .interact(move |conn| queries::search(conn, &q, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(SearchResponse {
        tracks: results.tracks,
        albums: results.albums,
        artists: results.artists,
    }))
}
