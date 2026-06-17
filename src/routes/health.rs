use axum::{extract::State, Json};
use serde_json::{json, Value};
use deadpool_sqlite::Pool;
use crate::{db::queries, error::AppError};

// Lightweight — no DB query. Used by k8s liveness/readiness/startup probes.
pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// Full stats — used for operational visibility. Not in the probe path.
pub async fn health_detailed(State(pool): State<Pool>) -> Result<Json<Value>, AppError> {
    let stats = pool.get().await?
        .interact(|conn| queries::health_stats(conn))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    Ok(Json(json!({
        "status": "ok",
        "db_schema_version": stats.schema_version,
        "track_count": stats.track_count,
        "analysis_coverage": {
            "loudness": stats.loudness_count,
            "bpm": stats.bpm_count,
            "clap": stats.clap_count,
            "sonic": stats.sonic_count,
        }
    })))
}
