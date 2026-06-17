use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::Deserialize;
use crate::{db::queries, error::AppError, models::{Artist, Page, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct Paged {
    #[serde(default)] pub offset: i64,
    #[serde(default = "default_limit")] pub limit: i64,
}
fn default_limit() -> i64 { 100 }

pub async fn list_artists(
    State(pool): State<Pool>,
    Query(p): Query<Paged>,
) -> Result<Json<Page<Artist>>, AppError> {
    let limit = p.limit.clamp(1, 1000);
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_artists(conn, p.offset, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}

pub async fn get_artist(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
) -> Result<Json<Artist>, AppError> {
    let artist = pool.get().await?
        .interact(move |conn| queries::get_artist(conn, id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("artist {id} not found")))?;
    Ok(Json(artist))
}

pub async fn artist_tracks(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
    Query(mut params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    params.artist_id = Some(id);
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}
