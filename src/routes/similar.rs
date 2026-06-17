use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use deadpool_sqlite::Pool;
use std::sync::Arc;
use crate::{error::AppError, similarity::SimilarityIndex};

#[derive(Deserialize)]
pub struct SimilarParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub exclude: Option<String>,
}
fn default_limit() -> usize { 10 }

#[derive(Serialize)]
pub struct SimilarResult {
    pub source_id: i64,
    pub results: Vec<SimilarEntry>,
}

#[derive(Serialize)]
pub struct SimilarEntry {
    pub id: i64,
    pub score: f32,
}

pub async fn similar_tracks(
    State((_pool, index)): State<(Pool, Arc<SimilarityIndex>)>,
    Path(id): Path<i64>,
    Query(params): Query<SimilarParams>,
) -> Result<Json<SimilarResult>, AppError> {
    let limit = params.limit.clamp(1, 50);
    let exclude_ids: Vec<i64> = params.exclude.as_deref().unwrap_or("")
        .split(',').filter_map(|s| s.trim().parse().ok()).collect();

    let results = index.find_similar(id, limit, &exclude_ids)
        .into_iter()
        .map(|(rid, score)| SimilarEntry { id: rid, score })
        .collect();

    Ok(Json(SimilarResult { source_id: id, results }))
}
