use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use crate::{db::{self, queries, SharedPool}, error::AppError, models::{Page, Playlist}};

#[derive(Deserialize)]
pub struct Paged {
    #[serde(default)] pub offset: i64,
    #[serde(default = "default_limit")] pub limit: i64,
}
fn default_limit() -> i64 { 100 }

pub async fn list_playlists(
    State(shared): State<SharedPool>,
    Query(p): Query<Paged>,
) -> Result<Json<Page<Playlist>>, AppError> {
    let pool = db::current(&shared).await;
    let limit = p.limit.clamp(1, 1000);
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_playlists(conn, p.offset, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}
