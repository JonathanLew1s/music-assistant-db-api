use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::Response,
};
use bytes::Bytes;
use lofty::{file::TaggedFileExt, probe::Probe, config::ParseOptions};
use lru::LruCache;
use parking_lot::Mutex;
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use crate::{db::{self, queries, SharedPool}, error::AppError};

pub type CoverCache = Arc<Mutex<LruCache<i64, Option<(Bytes, String)>>>>;

pub fn new_cover_cache() -> CoverCache {
    Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(512).unwrap())))
}

pub async fn track_cover(
    State((shared, music_root, cache)): State<(SharedPool, String, CoverCache)>,
    Path(id): Path<i64>,
) -> Result<Response<Body>, AppError> {
    if let Some(cached) = cache.lock().get(&id) {
        return Ok(build_cover_response(cached));
    }

    let pool = db::current(&shared).await;
    let file_path = pool.get().await?
        .interact(move |conn| queries::get_track_file_path(conn, id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    let result = file_path
        .as_deref()
        .and_then(|rel| extract_cover(&PathBuf::from(&music_root).join(rel)));

    cache.lock().put(id, result.clone());
    Ok(build_cover_response(&result))
}

pub async fn album_cover(
    State((shared, music_root, cache)): State<(SharedPool, String, CoverCache)>,
    Path(album_id): Path<i64>,
) -> Result<Response<Body>, AppError> {
    let cache_key = -album_id;
    if let Some(cached) = cache.lock().get(&cache_key) {
        return Ok(build_cover_response(cached));
    }

    let pool = db::current(&shared).await;
    let file_path: Option<String> = pool.get().await?
        .interact(move |conn| queries::get_album_first_file_path(conn, album_id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    let result = file_path
        .as_deref()
        .and_then(|rel| extract_cover(&PathBuf::from(&music_root).join(rel)));

    cache.lock().put(cache_key, result.clone());
    Ok(build_cover_response(&result))
}

fn extract_cover(path: &std::path::Path) -> Option<(Bytes, String)> {
    let tagged = Probe::open(path)
        .ok()?
        .options(ParseOptions::new().read_properties(false))
        .read()
        .ok()?;
    let tag = tagged.primary_tag().or_else(|| tagged.first_tag())?;
    let pic = tag.pictures().first()?;
    let mime = pic.mime_type()
        .map(|m| m.to_string())
        .unwrap_or_else(|| "image/jpeg".into());
    Some((Bytes::copy_from_slice(pic.data()), mime))
}

fn build_cover_response(result: &Option<(Bytes, String)>) -> Response<Body> {
    match result {
        Some((data, mime)) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime.as_str())
            .header(header::CACHE_CONTROL, "public, max-age=86400")
            .body(Body::from(data.clone()))
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}
