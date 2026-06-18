use axum::{extract::State, Json, http::StatusCode, response::IntoResponse};
use serde_json::{json, Value};
use deadpool_sqlite::Pool;
use crate::{db::queries, error::AppError};

// Liveness probe — runs a trivial DB query so k8s detects corruption/lock and
// restarts the container automatically. Returns 500 on any DB error.
pub async fn health(State(pool): State<Pool>) -> impl IntoResponse {
    let result: Result<(), anyhow::Error> = async {
        let conn = pool.get().await.map_err(|e| anyhow::anyhow!("{e}"))?;
        conn.interact(|c| c.execute_batch("SELECT COUNT(*) FROM tracks")).await
            .map_err(|e| anyhow::anyhow!("{e}"))??;
        Ok(())
    }.await;

    match result {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "status": "error", "error": e.to_string() }))).into_response(),
    }
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
