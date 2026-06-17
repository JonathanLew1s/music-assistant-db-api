use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::Deserialize;
use crate::{db::queries, error::AppError, models::{Page, Playlist, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct Paged {
    #[serde(default)] pub offset: i64,
    #[serde(default = "dl")] pub limit: i64,
}
fn dl() -> i64 { 100 }

pub async fn list_playlists(
    State(pool): State<Pool>,
    Query(p): Query<Paged>,
) -> Result<Json<Page<Playlist>>, AppError> {
    let limit = p.limit.clamp(1, 1000);
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_playlists(conn, p.offset, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}

pub async fn playlist_tracks(
    State(pool): State<Pool>,
    Path(playlist_id): Path<i64>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    let limit = params.clamped_limit();
    let offset = params.offset;
    let include_analysis = params.include_analysis();
    let include_clap = params.include_clap();

    let (total, items) = pool.get().await?
        .interact(move |conn| {
            let ids = queries::get_playlist_track_ids(conn, playlist_id)?;
            if ids.is_empty() {
                return Ok::<(i64, Vec<Track>), anyhow::Error>((0, vec![]));
            }

            let placeholders: Vec<String> = ids.iter().enumerate()
                .map(|(i, _)| format!("?{}", i + 1))
                .collect();
            let sql = format!(
                "SELECT t.item_id, t.name, t.duration, t.favorite, t.timestamp_added, t.timestamp_modified,
                         t.metadata, GROUP_CONCAT(DISTINCT a.name) AS artists,
                         alb.name, alb.year, alb.item_id, pm.provider_item_id,
                         aa_loud.analysis_data, aa_fades.analysis_data, aa_sonic.analysis_data
                 FROM tracks t
                 LEFT JOIN track_artists ta ON ta.track_id = t.item_id
                 LEFT JOIN artists a ON a.item_id = ta.artist_id
                 LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
                 LEFT JOIN albums alb ON alb.item_id = at2.album_id
                 LEFT JOIN provider_mappings pm ON pm.item_id = t.item_id AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
                 LEFT JOIN audio_analysis aa_loud ON aa_loud.item_id = pm.provider_item_id AND aa_loud.aa_provider_domain='loudness_analysis'
                 LEFT JOIN audio_analysis aa_fades ON aa_fades.item_id = pm.provider_item_id AND aa_fades.aa_provider_domain='smart_fades'
                 LEFT JOIN audio_analysis aa_sonic ON aa_sonic.item_id = pm.provider_item_id AND aa_sonic.aa_provider_domain='sonic_analysis'
                 WHERE t.item_id IN ({}) AND pm.provider_item_id IS NOT NULL
                 GROUP BY t.item_id
                 LIMIT ?{} OFFSET ?{}",
                placeholders.join(","),
                ids.len() + 1,
                ids.len() + 2,
            );

            let mut all_params: Vec<Box<dyn rusqlite::ToSql>> = ids.iter()
                .map(|id| Box::new(*id) as Box<dyn rusqlite::ToSql>)
                .collect();
            all_params.push(Box::new(limit));
            all_params.push(Box::new(offset));

            let total = ids.len() as i64;
            let mut stmt = conn.prepare(&sql)?;
            let tracks: Vec<Track> = stmt.query_map(
                rusqlite::params_from_iter(all_params.iter()),
                |row| queries::parse_track_row(row, include_analysis, include_clap),
            )?.collect::<rusqlite::Result<_>>()?;

            Ok((total, tracks))
        })
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    Ok(Json(Page { total, offset, limit, items }))
}
