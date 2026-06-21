use axum::extract::{Extension, Query, State};
use axum::Json;
use lru::LruCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::Arc;
use crate::{db::{self, queries::{self, SearchTypes}, SharedPool}, error::AppError, models::{Track, Album, Artist}};

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    #[serde(default = "dl")] pub limit: i64,
    /// Comma-separated subset of tracks,albums,artists. Defaults to all three.
    /// Callers that only want one kind (e.g. an artist-name lookup) skip the
    /// others' queries entirely rather than paying for them and discarding it.
    pub types: Option<String>,
}
fn dl() -> i64 { 10 }

fn parse_types(types: &Option<String>) -> SearchTypes {
    match types {
        None => SearchTypes::ALL,
        Some(s) => {
            let wanted: Vec<&str> = s.split(',').map(str::trim).collect();
            SearchTypes {
                tracks: wanted.contains(&"tracks"),
                albums: wanted.contains(&"albums"),
                artists: wanted.contains(&"artists"),
            }
        }
    }
}

#[derive(Serialize)]
pub struct SearchResponse {
    pub tracks: Vec<Track>,
    pub albums: Vec<Album>,
    pub artists: Vec<Artist>,
}

// ---------------------------------------------------------------------------
// Cache — LRU, no TTL. The underlying data is static for the entire life of
// this process: the only thing that ever changes it is the hourly clone
// refresh, which bounces the pod and wipes this cache (and every other
// in-memory cache here) anyway. A time-based expiry would only throw away
// hits for no correctness benefit, so eviction is LRU-size-only.
// ---------------------------------------------------------------------------

const SEARCH_CACHE_SIZE: usize = 256;

type SearchCacheKey = (String, i64, bool, bool, bool);

#[derive(Clone)]
pub struct SearchCache(Arc<Mutex<LruCache<SearchCacheKey, queries::SearchResults>>>);

impl SearchCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(SEARCH_CACHE_SIZE).unwrap()))))
    }
}

// Search is case-insensitive (LIKE is case-insensitive for ASCII in SQLite),
// so "Boards" and "boards" must hit the same cache entry — normalise the
// query text into the key rather than caching each casing separately.
fn cache_key(q: &str, limit: i64, types: SearchTypes) -> SearchCacheKey {
    (q.trim().to_lowercase(), limit, types.tracks, types.albums, types.artists)
}

pub async fn search(
    State(shared): State<SharedPool>,
    Extension(cache): Extension<SearchCache>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, AppError> {
    if params.q.trim().is_empty() {
        return Err(AppError::BadRequest("q is required".into()));
    }
    let limit = params.limit.clamp(1, 50);
    let types = parse_types(&params.types);
    let key = cache_key(&params.q, limit, types);

    if let Some(cached) = cache.0.lock().get(&key) {
        return Ok(Json(SearchResponse {
            tracks: cached.tracks.clone(),
            albums: cached.albums.clone(),
            artists: cached.artists.clone(),
        }));
    }

    let pool = db::current(&shared).await;
    let q = params.q.clone();
    let results = pool.get().await?
        .interact(move |conn| queries::search(conn, &q, limit, types))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    cache.0.lock().put(key, results.clone());

    Ok(Json(SearchResponse {
        tracks: results.tracks,
        albums: results.albums,
        artists: results.artists,
    }))
}

#[cfg(test)]
mod cache_key_tests {
    use super::{cache_key, SearchTypes};

    #[test]
    fn case_insensitive_query_collapses_to_same_key() {
        assert_eq!(
            cache_key("Boards", 10, SearchTypes::ALL),
            cache_key("boards", 10, SearchTypes::ALL),
        );
    }

    #[test]
    fn leading_trailing_whitespace_collapses_to_same_key() {
        assert_eq!(
            cache_key("  boards  ", 10, SearchTypes::ALL),
            cache_key("boards", 10, SearchTypes::ALL),
        );
    }

    #[test]
    fn different_limit_is_a_different_key() {
        assert_ne!(
            cache_key("boards", 10, SearchTypes::ALL),
            cache_key("boards", 20, SearchTypes::ALL),
        );
    }

    #[test]
    fn different_types_is_a_different_key() {
        assert_ne!(
            cache_key("boards", 10, SearchTypes::ALL),
            cache_key("boards", 10, SearchTypes::ARTISTS_ONLY),
        );
    }
}
