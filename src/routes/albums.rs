use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use crate::{db::{self, queries, SharedPool}, error::AppError, models::{Album, Page, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct AlbumParams {
    #[serde(default)] pub offset: i64,
    #[serde(default = "default_limit")] pub limit: i64,
    pub since: Option<i64>,
    pub order: Option<String>,
    pub dir: Option<String>,
    pub artist_id: Option<i64>,
}
fn default_limit() -> i64 { 100 }

pub async fn list_albums(
    State(shared): State<SharedPool>,
    Query(p): Query<AlbumParams>,
) -> Result<Json<Page<Album>>, AppError> {
    let pool = db::current(&shared).await;
    let limit = p.limit.clamp(1, 1000);
    let order = p.order.as_deref().unwrap_or("name").to_string();
    let dir = p.dir.as_deref().unwrap_or("asc").to_string();
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_albums(conn, p.offset, limit, p.since, &order, &dir, p.artist_id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}

pub async fn get_album(
    State(shared): State<SharedPool>,
    Path(id): Path<i64>,
) -> Result<Json<Album>, AppError> {
    let pool = db::current(&shared).await;
    let album = pool.get().await?
        .interact(move |conn| queries::get_album(conn, id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("album {id} not found")))?;
    Ok(Json(album))
}

pub async fn album_tracks(
    State(shared): State<SharedPool>,
    Path(id): Path<i64>,
    Query(mut params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    let pool = db::current(&shared).await;
    params.album_id = Some(id);
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}
