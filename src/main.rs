mod auth;
mod camelot;
mod config;
mod db;
mod error;
mod models;
mod routes;
mod similarity;

use std::sync::Arc;
use axum::{middleware, routing::get, Router};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::Config;
use routes::cover::{new_cover_cache, album_cover, track_cover};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    tracing::info!("connecting to {}", cfg.db_path);

    let pool = db::build_pool(&cfg.db_path, cfg.pool_size).await?;

    let sim_index = Arc::new(similarity::SimilarityIndex::empty());
    {
        let pool2 = pool.clone();
        let idx = sim_index.clone();
        tokio::spawn(async move {
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

    let cover_cache = new_cover_cache();
    let music_root = cfg.music_root.clone();
    let track_cover_state = (pool.clone(), music_root.clone(), cover_cache.clone());
    let album_cover_state = (pool.clone(), music_root.clone(), cover_cache.clone());

    let api = Router::new()
        .route("/health", get(routes::health::health))
        .route("/health/detailed", get(routes::health::health_detailed).with_state(pool.clone()))
        .route("/tracks", get(routes::tracks::list_tracks).with_state(pool.clone()))
        .route("/tracks/:id", get(routes::tracks::get_track).with_state(pool.clone()))
        .route("/tracks/:id/similar",
            get(routes::similar::similar_tracks)
                .with_state((pool.clone(), sim_index.clone())))
        .route("/tracks/:id/cover", get(track_cover).with_state(track_cover_state))
        .route("/albums", get(routes::albums::list_albums).with_state(pool.clone()))
        .route("/albums/:id", get(routes::albums::get_album).with_state(pool.clone()))
        .route("/albums/:id/tracks", get(routes::albums::album_tracks).with_state(pool.clone()))
        .route("/albums/:id/cover", get(album_cover).with_state(album_cover_state))
        .route("/artists", get(routes::artists::list_artists).with_state(pool.clone()))
        .route("/artists/:id", get(routes::artists::get_artist).with_state(pool.clone()))
        .route("/artists/:id/tracks", get(routes::artists::artist_tracks).with_state(pool.clone()))
        .route("/playlists", get(routes::playlists::list_playlists).with_state(pool.clone()))
        .route("/playlists/:id/tracks", get(routes::playlists::playlist_tracks).with_state(pool.clone()))
        .route("/search", get(routes::search::search).with_state(pool.clone()));

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
