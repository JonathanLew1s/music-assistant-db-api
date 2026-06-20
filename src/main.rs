mod auth;
mod camelot;
mod config;
mod db;
mod error;
mod models;
mod routes;
mod similarity;
mod snapshot;

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
    tracing::info!("connecting to {} (snapshot path: {})", cfg.db_path, cfg.snapshot_path);

    // Take an initial snapshot before serving any traffic. If one already
    // exists on disk (e.g. surviving a pod restart) and this attempt fails
    // (MA mid-write right now), fall back to it rather than refusing to
    // boot — only error out if there is truly no usable snapshot anywhere.
    match snapshot::take_snapshot(cfg.db_path.clone(), cfg.snapshot_path.clone()).await {
        Ok(()) => tracing::info!("initial snapshot taken"),
        Err(e) if std::path::Path::new(&cfg.snapshot_path).exists() => {
            tracing::warn!("initial snapshot failed, serving existing snapshot from a previous run: {e}");
        }
        Err(e) => return Err(anyhow::anyhow!("initial snapshot failed and no existing snapshot to fall back to: {e}")),
    }

    let pool = db::build_pool(&cfg.snapshot_path, cfg.pool_size).await?;
    let shared_pool: db::SharedPool = Arc::new(RwLock::new(pool));

    {
        let shared_pool = shared_pool.clone();
        let source_path = cfg.db_path.clone();
        let snapshot_path = cfg.snapshot_path.clone();
        let pool_size = cfg.pool_size;
        let interval = cfg.snapshot_interval;
        tokio::spawn(async move {
            snapshot::run_periodic(shared_pool, source_path, snapshot_path, pool_size, interval).await;
        });
    }

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
