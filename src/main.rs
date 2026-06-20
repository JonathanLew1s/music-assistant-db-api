mod auth;
mod camelot;
mod config;
mod db;
mod error;
mod models;
mod routes;
mod similarity;

use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{middleware, routing::get, Router};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::Config;
use routes::cover::{new_cover_cache, album_cover, track_cover};
use routes::tracks::ObservatoryCache;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    tracing::info!("connecting to {}", cfg.db_path);

    // MA_DB_PATH now points at a Longhorn PVC-to-PVC clone of MA's live
    // library.db, refreshed hourly by a CronJob (see the talos GitOps repo:
    // kubernetes/apps/music-assistant/ma-db-api-clone-refresh-cronjob.yaml)
    // — never at the live, actively-written file directly. The clone is
    // static between refreshes (a refresh remounts via a full pod restart,
    // not an in-place update), so there's no write contention to retry
    // against here. The one thing still needed: a clone taken mid-write can
    // have a torn tail in its -wal file, so open it once with normal
    // (non-immutable) flags first to let SQLite's own WAL recovery discard
    // that tail — confirmed live that this produces a clean,
    // integrity-check-passing database — before building the real
    // immutable=1 serving pool.
    db::recover_wal(&cfg.db_path).await?;

    let pool = db::build_pool(&cfg.db_path, cfg.pool_size).await?;
    let shared_pool: db::SharedPool = Arc::new(RwLock::new(pool));

    let sim_index = Arc::new(similarity::SimilarityIndex::empty());
    {
        let shared_pool = shared_pool.clone();
        let idx = sim_index.clone();
        tokio::spawn(async move {
            let pool2 = db::current(&shared_pool).await;
            match pool2.get().await {
                Ok(conn) => {
                    match conn.interact(|c| db::queries::all_clap_vectors(c)).await {
                        Ok(Ok(vecs)) => idx.reload(vecs),
                        Ok(Err(e)) => tracing::warn!("CLAP load error: {e}"),
                        Err(e) => tracing::warn!("CLAP pool error: {e}"),
                    }
                }
                Err(e) => tracing::warn!("CLAP pool get error: {e}"),
            }
        });
    }

    let observatory_cache = ObservatoryCache::new();
    // Pre-warm the observatory cache on startup so the first user request is instant.
    {
        let shared_pool = shared_pool.clone();
        let obs_cache = observatory_cache.clone();
        tokio::spawn(async move {
            let pool2 = db::current(&shared_pool).await;
            match pool2.get().await {
                Ok(conn) => {
                    match conn.interact(|c| db::queries::observatory_tracks(c)).await {
                        Ok(Ok(tracks)) => {
                            let total = tracks.len();
                            *obs_cache.0.lock() = Some((std::time::Instant::now(), tracks));
                            tracing::info!("observatory cache pre-warmed: {} tracks", total);
                        }
                        Ok(Err(e)) => tracing::warn!("observatory pre-warm query error: {e}"),
                        Err(e) => tracing::warn!("observatory pre-warm pool error: {e}"),
                    }
                }
                Err(e) => tracing::warn!("observatory pre-warm pool get error: {e}"),
            }
        });
    }
    let cover_cache = new_cover_cache();
    let music_root = cfg.music_root.clone();
    let track_cover_state = (shared_pool.clone(), music_root.clone(), cover_cache.clone());
    let album_cover_state = (shared_pool.clone(), music_root.clone(), cover_cache.clone());

    let api = Router::new()
        .route("/health", get(routes::health::health).with_state(shared_pool.clone()))
        .route("/health/detailed", get(routes::health::health_detailed).with_state(shared_pool.clone()))
        .route("/tracks", get(routes::tracks::list_tracks).with_state(shared_pool.clone()))
        .route("/tracks/observatory", get(routes::tracks::observatory_tracks).with_state(shared_pool.clone()))
        .route("/tracks/:id", get(routes::tracks::get_track).with_state(shared_pool.clone()))
        .route("/tracks/:id/similar",
            get(routes::similar::similar_tracks)
                .with_state((shared_pool.clone(), sim_index.clone())))
        .route("/tracks/:id/cover", get(track_cover).with_state(track_cover_state))
        .route("/albums", get(routes::albums::list_albums).with_state(shared_pool.clone()))
        .route("/albums/:id", get(routes::albums::get_album).with_state(shared_pool.clone()))
        .route("/albums/:id/tracks", get(routes::albums::album_tracks).with_state(shared_pool.clone()))
        .route("/albums/:id/cover", get(album_cover).with_state(album_cover_state))
        .route("/artists", get(routes::artists::list_artists).with_state(shared_pool.clone()))
        .route("/artists/:id", get(routes::artists::get_artist).with_state(shared_pool.clone()))
        .route("/artists/:id/tracks", get(routes::artists::artist_tracks).with_state(shared_pool.clone()))
        .route("/playlists", get(routes::playlists::list_playlists).with_state(shared_pool.clone()))
        .route("/search", get(routes::search::search).with_state(shared_pool.clone()));

    let api = api.layer(axum::Extension(observatory_cache));

    let api = if let Some(key) = cfg.api_key.clone() {
        api.layer(middleware::from_fn(move |req, next| {
            let k = key.clone();
            auth::require_api_key(k, req, next)
        }))
    } else {
        api
    };

    let app = Router::new()
        .nest("/api/v1", api)
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
