use std::sync::Arc;
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use parking_lot::Mutex;
use serde::Serialize;
use crate::{db::{self, queries, SharedPool}, error::AppError, models::{Page, track::{Track, TrackQueryParams}}};

#[derive(Clone)]
pub struct ObservatoryCache(pub Arc<Mutex<Option<Vec<Track>>>>);

impl ObservatoryCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
}

#[derive(Serialize)]
pub struct ObservatoryPage {
    pub total: usize,
    pub items: Vec<Track>,
}

/// Returns all tracks that have sonic_analysis, optimised for the observatory bulk fetch.
/// Drives the JOIN from track_audio_features (one row per analysed track) rather than
/// tracks (37K+). Cached indefinitely, not on a TTL: the underlying clone is static for
/// the entire life of this process — the only thing that ever changes it is the hourly
/// refresh, which bounces the pod and wipes this cache along with it anyway, so a
/// time-based expiry would only throw away hits for no correctness benefit.
/// Uses Extension (not State) for the cache so this handler shares the Pool state type
/// with all other track handlers — avoiding Axum route-priority issues with static vs
/// parameterised paths.
pub async fn observatory_tracks(
    State(shared): State<SharedPool>,
    Extension(cache): Extension<ObservatoryCache>,
) -> Result<Json<ObservatoryPage>, AppError> {
    if let Some(ref tracks) = *cache.0.lock() {
        return Ok(Json(ObservatoryPage { total: tracks.len(), items: tracks.clone() }));
    }

    // Cache miss — run the query.
    let pool = db::current(&shared).await;
    let tracks = pool.get().await?
        .interact(|conn| queries::observatory_tracks(conn))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    let total = tracks.len();
    *cache.0.lock() = Some(tracks.clone());
    Ok(Json(ObservatoryPage { total, items: tracks }))
}

pub async fn list_tracks(
    State(shared): State<SharedPool>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    let pool = db::current(&shared).await;
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}

pub async fn get_track(
    State(shared): State<SharedPool>,
    Path(id): Path<i64>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Track>, AppError> {
    let pool = db::current(&shared).await;
    let include_analysis = params.include_analysis();
    let include_clap = params.include_clap();
    let include_lyrics = params.include_lyrics();
    let track = pool.get().await?
        .interact(move |conn| queries::get_track(conn, id, include_analysis, include_clap, include_lyrics))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("track {id} not found")))?;
    Ok(Json(track))
}
