use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use crate::{db::queries, error::AppError, models::{Page, track::{Track, TrackQueryParams}}};

pub async fn list_tracks(
    State(pool): State<Pool>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}

pub async fn get_track(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Track>, AppError> {
    let include_analysis = params.include_analysis();
    let include_clap = params.include_clap();
    let track = pool.get().await?
        .interact(move |conn| queries::get_track(conn, id, include_analysis, include_clap))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("track {id} not found")))?;
    Ok(Json(track))
}
