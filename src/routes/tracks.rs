use std::sync::Arc;
use std::time::{Duration, Instant};
use axum::extract::{Extension, Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use parking_lot::Mutex;
use serde::Serialize;
use crate::{db::queries, error::AppError, models::{Page, track::{Track, TrackQueryParams}}};

const OBSERVATORY_TTL: Duration = Duration::from_secs(1800);

#[derive(Clone)]
pub struct ObservatoryCache(pub Arc<Mutex<Option<(Instant, Vec<Track>)>>>);

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
/// Drives the JOIN from audio_analysis (7K rows) rather than tracks (37K+), and caches
/// the full result for OBSERVATORY_TTL so repeated page-loads are instant.
/// Uses Extension (not State) for the cache so this handler shares the Pool state type
/// with all other track handlers — avoiding Axum route-priority issues with static vs
/// parameterised paths.
pub async fn observatory_tracks(
    State(pool): State<Pool>,
    Extension(cache): Extension<ObservatoryCache>,
) -> Result<Json<ObservatoryPage>, AppError> {
    // Serve from cache if still fresh.
    {
        let guard = cache.0.lock();
        if let Some((ts, ref tracks)) = *guard {
            if ts.elapsed() < OBSERVATORY_TTL {
                return Ok(Json(ObservatoryPage { total: tracks.len(), items: tracks.clone() }));
            }
        }
    }

    // Cache miss — run the query.
    let tracks = pool.get().await?
        .interact(|conn| queries::observatory_tracks(conn))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    let total = tracks.len();
    *cache.0.lock() = Some((Instant::now(), tracks.clone()));
    Ok(Json(ObservatoryPage { total, items: tracks }))
}

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
