use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use crate::{db::{self, queries, SharedPool}, error::AppError, models::{Genre, Page, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct Paged {
    #[serde(default)] pub offset: i64,
    #[serde(default = "default_limit")] pub limit: i64,
}
fn default_limit() -> i64 { 100 }

pub async fn list_genres(
    State(shared): State<SharedPool>,
    Query(p): Query<Paged>,
) -> Result<Json<Page<Genre>>, AppError> {
    let pool = db::current(&shared).await;
    let limit = p.limit.clamp(1, 1000);
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_genres(conn, p.offset, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}

pub async fn get_genre(
    State(shared): State<SharedPool>,
    Path(id): Path<i64>,
) -> Result<Json<Genre>, AppError> {
    let pool = db::current(&shared).await;
    let genre = pool.get().await?
        .interact(move |conn| queries::get_genre(conn, id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("genre {id} not found")))?;
    Ok(Json(genre))
}

pub async fn genre_tracks(
    State(shared): State<SharedPool>,
    Path(id): Path<i64>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    let pool = db::current(&shared).await;
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::genre_tracks(conn, id, offset, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}
