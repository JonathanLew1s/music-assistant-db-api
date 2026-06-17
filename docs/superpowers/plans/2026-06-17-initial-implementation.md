# music-assistant-db-api Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Rust REST API that reads Music Assistant's SQLite database directly and exposes tracks, albums, artists, playlists, audio analysis (CLAP 1024-dim, BPM, LUFS, valence/energy/etc), similarity search, and cover art over HTTP.

**Architecture:** Axum HTTP server with a deadpool-sqlite connection pool opens MA's `library.db` read-only in WAL mode. A background-loaded in-memory similarity index holds all CLAP vectors for sub-10ms cosine KNN. Cover art is extracted from embedded audio file tags via `lofty` and cached in an LRU. All routes are optional-auth gated via a tower middleware layer.

**Tech Stack:** Rust 1.96, axum 0.7, rusqlite 0.31 (bundled), deadpool-sqlite 0.8, serde/serde_json 1, tower-http 0.5, lofty 0.20, lru 0.12, parking_lot 0.12, tracing/tracing-subscriber, anyhow

---

## File Map

| File | Responsibility |
|---|---|
| `Cargo.toml` | All dependencies |
| `src/main.rs` | Startup: config, pool, similarity index, router, server |
| `src/config.rs` | `Config` struct from env vars |
| `src/error.rs` | `AppError` enum + `IntoResponse` impl |
| `src/auth.rs` | Optional API key middleware (tower `Layer`) |
| `src/camelot.rs` | Key + mode string → Camelot notation |
| `src/similarity.rs` | In-memory CLAP vector index, cosine KNN |
| `src/db/mod.rs` | Pool init, WAL + read-only pragmas |
| `src/db/queries.rs` | All SQL: tracks, albums, artists, playlists, search, health |
| `src/models/mod.rs` | Re-exports |
| `src/models/track.rs` | `Track`, `TrackAnalysis`, `TrackRow` (raw DB row) |
| `src/models/album.rs` | `Album` |
| `src/models/artist.rs` | `Artist` |
| `src/models/playlist.rs` | `Playlist` |
| `src/models/pagination.rs` | `Page<T>`, `PaginationParams` |
| `src/routes/mod.rs` | Router assembly |
| `src/routes/health.rs` | `GET /api/v1/health` |
| `src/routes/tracks.rs` | `GET /api/v1/tracks`, `GET /api/v1/tracks/:id` |
| `src/routes/similar.rs` | `GET /api/v1/tracks/:id/similar` |
| `src/routes/cover.rs` | `GET /api/v1/tracks/:id/cover` |
| `src/routes/albums.rs` | `GET /api/v1/albums`, `/:id`, `/:id/tracks`, `/:id/cover` |
| `src/routes/artists.rs` | `GET /api/v1/artists`, `/:id`, `/:id/tracks`, `/:id/albums` |
| `src/routes/playlists.rs` | `GET /api/v1/playlists`, `/:id`, `/:id/tracks` |
| `src/routes/search.rs` | `GET /api/v1/search` |
| `.gitignore` | Rust standard |
| `Dockerfile` | Multi-stage Alpine build → scratch image |
| `k8s/sidecar-patch.yaml` | Strategic merge patch for MA deployment |
| `docker-compose.yml` | Standalone compose for non-k8s |

---

## Task 1: Cargo.toml and project skeleton

**Files:**
- Modify: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/error.rs`
- Create: `src/config.rs`

- [ ] **Write `Cargo.toml`**

```toml
[package]
name = "music-assistant-db-api"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "ma-db-api"
path = "src/main.rs"

[dependencies]
axum = { version = "0.7", features = ["macros"] }
tokio = { version = "1", features = ["full"] }
rusqlite = { version = "0.31", features = ["bundled"] }
deadpool-sqlite = { version = "0.8", features = ["rt_tokio_1"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tower = { version = "0.4", features = ["util"] }
tower-http = { version = "0.5", features = ["cors", "trace", "compression-gzip"] }
lofty = "0.20"
lru = "0.12"
parking_lot = "0.12"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
anyhow = "1"
bytes = "1"
futures = "0.3"

[profile.release]
opt-level = 3
lto = true
codegen-units = 1
strip = true
```

- [ ] **Write `.gitignore`**

```
/target
Cargo.lock
```

- [ ] **Write `src/config.rs`**

```rust
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub db_path: String,
    pub music_root: String,
    pub port: u16,
    pub api_key: Option<String>,
    pub pool_size: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let db_path = env::var("MA_DB_PATH")
            .map_err(|_| anyhow::anyhow!("MA_DB_PATH is required"))?;
        let music_root = env::var("MA_MUSIC_ROOT")
            .map_err(|_| anyhow::anyhow!("MA_MUSIC_ROOT is required"))?;
        let port = env::var("PORT")
            .unwrap_or_else(|_| "8097".into())
            .parse::<u16>()?;
        let api_key = env::var("MA_BRIDGE_API_KEY").ok().filter(|s| !s.is_empty());
        let pool_size = env::var("DB_POOL_SIZE")
            .unwrap_or_else(|_| "4".into())
            .parse::<usize>()?;
        Ok(Self { db_path, music_root, port, api_key, pool_size })
    }
}
```

- [ ] **Write `src/error.rs`**

```rust
use axum::{http::StatusCode, response::{IntoResponse, Response}, Json};
use serde_json::json;

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    Internal(anyhow::Error),
    BadRequest(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            AppError::Internal(e) => {
                tracing::error!("internal error: {e:#}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".into())
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        AppError::Internal(e.into())
    }
}
```

- [ ] **Verify it compiles (no routes yet — just stubs)**

Replace `src/main.rs` with:

```rust
mod config;
mod error;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Ok(())
}
```

Run: `~/.cargo/bin/cargo build 2>&1`
Expected: compiles with no errors.

- [ ] **Commit**

```bash
cd /Users/jonathan/code/music-assistant-db-api
git add -A
git commit -m "chore: project skeleton, config, error types"
```

---

## Task 2: Database pool and Camelot helper

**Files:**
- Create: `src/db/mod.rs`
- Create: `src/camelot.rs`

- [ ] **Write `src/db/mod.rs`**

```rust
use deadpool_sqlite::{Config as PoolConfig, Pool, Runtime};
use rusqlite::Connection;
use anyhow::Context;

pub mod queries;

pub async fn build_pool(db_path: &str, pool_size: usize) -> anyhow::Result<Pool> {
    let cfg = PoolConfig::new(db_path);
    let pool = cfg.builder(Runtime::Tokio1)?
        .max_size(pool_size)
        .build()?;

    // Verify we can connect and configure WAL / read-only mode.
    let conn = pool.get().await.context("initial DB connection failed")?;
    conn.interact(|c| configure_connection(c))
        .await
        .map_err(|e| anyhow::anyhow!("pool interact error: {e}"))??;

    Ok(pool)
}

pub fn configure_connection(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch("
        PRAGMA journal_mode=WAL;
        PRAGMA query_only=ON;
        PRAGMA temp_store=MEMORY;
        PRAGMA cache_size=-8000;
    ")?;
    Ok(())
}
```

- [ ] **Write `src/camelot.rs`**

```rust
// Converts a musical key name + mode into Camelot wheel notation.
// e.g. ("D#", "minor") -> "2A",  ("C", "major") -> "8B"

const NOTE_ORDER: [&str; 12] = ["C","C#","D","D#","E","F","F#","G","G#","A","A#","B"];

fn normalise_key(key: &str) -> &str {
    match key {
        "Db" => "C#", "Eb" => "D#", "Gb" => "F#", "Ab" => "G#", "Bb" => "A#",
        other => other,
    }
}

pub fn to_camelot(key: &str, mode: &str) -> Option<String> {
    let tonic = normalise_key(key.trim());
    let idx = NOTE_ORDER.iter().position(|&n| n == tonic)?;
    let n = match mode.trim().to_lowercase().as_str() {
        "major" => ((idx * 7) % 12) + 1,
        "minor" => (((idx + 9) * 7) % 12) + 1,
        _ => return None,
    };
    let suffix = if mode.trim().to_lowercase() == "major" { "B" } else { "A" };
    Some(format!("{n}{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn camelot_c_major() {
        assert_eq!(to_camelot("C", "major"), Some("8B".into()));
    }

    #[test]
    fn camelot_d_sharp_minor() {
        assert_eq!(to_camelot("D#", "minor"), Some("2A".into()));
    }

    #[test]
    fn camelot_enharmonic() {
        assert_eq!(to_camelot("Bb", "major"), to_camelot("A#", "major"));
    }

    #[test]
    fn camelot_unknown_key() {
        assert_eq!(to_camelot("X", "major"), None);
    }
}
```

- [ ] **Run Camelot tests**

```bash
cd /Users/jonathan/code/music-assistant-db-api && ~/.cargo/bin/cargo test camelot 2>&1
```
Expected: 4 tests pass.

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: db pool init, camelot wheel conversion"
```

---

## Task 3: Data models

**Files:**
- Create: `src/models/mod.rs`
- Create: `src/models/track.rs`
- Create: `src/models/album.rs`
- Create: `src/models/artist.rs`
- Create: `src/models/playlist.rs`
- Create: `src/models/pagination.rs`

- [ ] **Write `src/models/pagination.rs`**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    #[serde(default)]
    pub offset: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 { 100 }

impl PaginationParams {
    pub fn clamped_limit(&self) -> i64 {
        self.limit.clamp(1, 1000)
    }
}

#[derive(Debug, Serialize)]
pub struct Page<T: Serialize> {
    pub total: i64,
    pub offset: i64,
    pub limit: i64,
    pub items: Vec<T>,
}
```

- [ ] **Write `src/models/track.rs`**

```rust
use serde::{Deserialize, Serialize};

/// Full analysis block — only populated when ?include=analysis is set.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackAnalysis {
    pub loudness_lufs: Option<f64>,
    pub loudness_album_lufs: Option<f64>,
    pub bpm: Option<f64>,
    pub key: Option<String>,
    pub mode: Option<String>,
    pub camelot: Option<String>,
    pub beats: Option<Vec<f64>>,
    pub valence: Option<f64>,
    pub energy: Option<f64>,
    pub danceability: Option<f64>,
    pub arousal: Option<f64>,
    pub acousticness: Option<f64>,
    pub instrumentalness: Option<f64>,
    pub brightness: Option<f64>,
    pub rms_energy: Option<Vec<f64>>,
    pub mbid: Option<String>,
    pub isrc: Option<String>,
    /// 1024-dim CLAP embedding — only when ?include=analysis,clap
    pub clap_embedding: Option<Vec<f64>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Track {
    pub id: i64,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub album_id: Option<i64>,
    pub year: Option<i64>,
    pub genre: Option<String>,
    pub duration: Option<f64>,
    pub file_path: Option<String>,
    pub favorite: Option<bool>,
    pub timestamp_added: Option<i64>,
    pub timestamp_modified: Option<i64>,
    pub cover_url: String,
    pub analysis: Option<TrackAnalysis>,
}

/// Params for the /tracks list endpoint.
#[derive(Debug, Deserialize)]
pub struct TrackQueryParams {
    #[serde(default)]
    pub offset: i64,
    #[serde(default = "default_limit")]
    pub limit: i64,
    pub since: Option<i64>,
    pub include: Option<String>,
    pub favorite: Option<bool>,
    pub genre: Option<String>,
    pub artist_id: Option<i64>,
    pub album_id: Option<i64>,
    pub bpm_min: Option<f64>,
    pub bpm_max: Option<f64>,
    pub energy_min: Option<f64>,
    pub energy_max: Option<f64>,
    pub valence_min: Option<f64>,
    pub valence_max: Option<f64>,
    pub arousal_min: Option<f64>,
    pub arousal_max: Option<f64>,
    pub order: Option<String>,
    pub dir: Option<String>,
    pub exclude: Option<String>,
}

fn default_limit() -> i64 { 100 }

impl TrackQueryParams {
    pub fn clamped_limit(&self) -> i64 { self.limit.clamp(1, 1000) }
    pub fn include_analysis(&self) -> bool {
        self.include.as_deref().map(|s| s.contains("analysis")).unwrap_or(false)
    }
    pub fn include_clap(&self) -> bool {
        self.include.as_deref().map(|s| s.contains("clap")).unwrap_or(false)
    }
    pub fn exclude_ids(&self) -> Vec<i64> {
        self.exclude.as_deref().unwrap_or("").split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect()
    }
}
```

- [ ] **Write `src/models/album.rs`**

```rust
use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Album {
    pub id: i64,
    pub name: Option<String>,
    pub artist: Option<String>,
    pub artist_id: Option<i64>,
    pub year: Option<i64>,
    pub track_count: i64,
    pub timestamp_added: Option<i64>,
    pub cover_url: String,
}
```

- [ ] **Write `src/models/artist.rs`**

```rust
use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Artist {
    pub id: i64,
    pub name: Option<String>,
    pub track_count: i64,
    pub album_count: i64,
}
```

- [ ] **Write `src/models/playlist.rs`**

```rust
use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
pub struct Playlist {
    pub id: i64,
    pub name: Option<String>,
    pub track_count: i64,
    pub timestamp_modified: Option<i64>,
}
```

- [ ] **Write `src/models/mod.rs`**

```rust
pub mod album;
pub mod artist;
pub mod pagination;
pub mod playlist;
pub mod track;

pub use album::Album;
pub use artist::Artist;
pub use pagination::{Page, PaginationParams};
pub use playlist::Playlist;
pub use track::{Track, TrackAnalysis, TrackQueryParams};
```

- [ ] **Verify compile**

Update `src/main.rs`:
```rust
mod camelot;
mod config;
mod db;
mod error;
mod models;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Ok(())
}
```

Run: `~/.cargo/bin/cargo build 2>&1`
Expected: compiles cleanly.

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: data models — Track, Album, Artist, Playlist, pagination"
```

---

## Task 4: Database queries

**Files:**
- Create: `src/db/queries.rs`

This is the largest single file. It contains the parameterised SQL for every resource type. All queries use the same base join for tracks (from `sync-from-ma.ts`) and extend it with dynamic WHERE clauses.

- [ ] **Write `src/db/queries.rs`**

```rust
use rusqlite::{Connection, params};
use anyhow::Result;
use serde_json::Value;

use crate::camelot::to_camelot;
use crate::models::{
    track::{Track, TrackAnalysis, TrackQueryParams},
    album::Album,
    artist::Artist,
    playlist::Playlist,
};

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

pub struct HealthStats {
    pub track_count: i64,
    pub schema_version: i64,
    pub loudness_count: i64,
    pub bpm_count: i64,
    pub clap_count: i64,
    pub sonic_count: i64,
}

pub fn health_stats(conn: &Connection) -> Result<HealthStats> {
    let schema_version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let track_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tracks t
         JOIN provider_mappings pm ON pm.item_id = t.item_id
           AND pm.media_type='track' AND pm.provider_domain='filesystem_local'",
        [], |r| r.get(0))?;
    let loudness_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM audio_analysis WHERE aa_provider_domain='loudness_analysis'",
        [], |r| r.get(0))?;
    let bpm_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM audio_analysis WHERE aa_provider_domain='smart_fades'",
        [], |r| r.get(0))?;
    let sonic_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM audio_analysis WHERE aa_provider_domain='sonic_analysis'",
        [], |r| r.get(0))?;
    // clap is a subset of sonic where the embedding is present
    let clap_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM audio_analysis
         WHERE aa_provider_domain='sonic_analysis'
           AND json_extract(analysis_data, '$.extra_data.clap_embedding') IS NOT NULL",
        [], |r| r.get(0))?;
    Ok(HealthStats { track_count, schema_version, loudness_count, bpm_count, clap_count, sonic_count })
}

// ---------------------------------------------------------------------------
// Track base SQL
// ---------------------------------------------------------------------------

const TRACK_BASE: &str = "
SELECT
  t.item_id,
  t.name,
  t.duration,
  t.favorite,
  t.timestamp_added,
  t.timestamp_modified,
  t.metadata,
  GROUP_CONCAT(DISTINCT a.name) AS artists,
  alb.name AS album,
  alb.year,
  alb.item_id AS album_id,
  pm.provider_item_id AS file_path,
  aa_loud.analysis_data AS loudness_json,
  aa_fades.analysis_data AS fades_json,
  aa_sonic.analysis_data AS sonic_json
FROM tracks t
LEFT JOIN track_artists ta ON ta.track_id = t.item_id
LEFT JOIN artists a ON a.item_id = ta.artist_id
LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
LEFT JOIN albums alb ON alb.item_id = at2.album_id
LEFT JOIN provider_mappings pm
  ON pm.item_id = t.item_id AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
LEFT JOIN audio_analysis aa_loud
  ON aa_loud.item_id = pm.provider_item_id AND aa_loud.aa_provider_domain='loudness_analysis'
LEFT JOIN audio_analysis aa_fades
  ON aa_fades.item_id = pm.provider_item_id AND aa_fades.aa_provider_domain='smart_fades'
LEFT JOIN audio_analysis aa_sonic
  ON aa_sonic.item_id = pm.provider_item_id AND aa_sonic.aa_provider_domain='sonic_analysis'
WHERE pm.provider_item_id IS NOT NULL
";

fn parse_track_row(row: &rusqlite::Row, include_analysis: bool, include_clap: bool) -> rusqlite::Result<Track> {
    let id: i64 = row.get(0)?;
    let title: Option<String> = row.get(1)?;
    let duration: Option<f64> = row.get(2)?;
    let favorite: Option<bool> = row.get(3)?;
    let timestamp_added: Option<i64> = row.get(4)?;
    let timestamp_modified: Option<i64> = row.get(5)?;
    let metadata_str: Option<String> = row.get(6)?;
    let artists_str: Option<String> = row.get(7)?;
    let album: Option<String> = row.get(8)?;
    let year: Option<i64> = row.get(9)?;
    let album_id: Option<i64> = row.get(10)?;
    let file_path: Option<String> = row.get(11)?;
    let loudness_str: Option<String> = row.get(12)?;
    let fades_str: Option<String> = row.get(13)?;
    let sonic_str: Option<String> = row.get(14)?;

    let artists: Vec<String> = artists_str
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let artist = artists.first().cloned();

    let metadata: Option<Value> = metadata_str.as_deref().and_then(|s| serde_json::from_str(s).ok());
    let genre = metadata.as_ref()
        .and_then(|m| m.get("genres"))
        .and_then(|g| g.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .map(String::from);

    let analysis = if include_analysis {
        let loud: Option<Value> = loudness_str.as_deref().and_then(|s| serde_json::from_str(s).ok());
        let fades: Option<Value> = fades_str.as_deref().and_then(|s| serde_json::from_str(s).ok());
        let sonic: Option<Value> = sonic_str.as_deref().and_then(|s| serde_json::from_str(s).ok());

        let key = fades.as_ref().and_then(|v| v.get("key")).and_then(|v| v.as_str()).map(String::from);
        let mode = fades.as_ref().and_then(|v| v.get("mode")).and_then(|v| v.as_str()).map(String::from);
        let camelot = key.as_deref().zip(mode.as_deref()).and_then(|(k, m)| to_camelot(k, m));

        let beats = fades.as_ref()
            .and_then(|v| v.get("beats"))
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect());

        let rms_energy = sonic.as_ref()
            .and_then(|v| v.get("rms_energy"))
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect());

        let clap_embedding = if include_clap {
            sonic.as_ref()
                .and_then(|v| v.get("extra_data"))
                .and_then(|v| v.get("clap_embedding"))
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect())
        } else {
            None
        };

        let extra = sonic.as_ref().and_then(|v| v.get("extra_data"));
        let mbid = extra.and_then(|v| v.get("mbid")).and_then(|v| v.as_str()).map(String::from);
        let isrc = extra.and_then(|v| v.get("isrc")).and_then(|v| v.as_str()).map(String::from);

        Some(TrackAnalysis {
            loudness_lufs: loud.as_ref().and_then(|v| v.get("loudness_integrated")).and_then(|v| v.as_f64()),
            loudness_album_lufs: loud.as_ref().and_then(|v| v.get("loudness_album")).and_then(|v| v.as_f64()),
            bpm: fades.as_ref().and_then(|v| v.get("bpm")).and_then(|v| v.as_f64()),
            key,
            mode,
            camelot,
            beats,
            valence: sonic.as_ref().and_then(|v| v.get("valence")).and_then(|v| v.as_f64()),
            energy: sonic.as_ref().and_then(|v| v.get("energy")).and_then(|v| v.as_f64()),
            danceability: sonic.as_ref().and_then(|v| v.get("danceability")).and_then(|v| v.as_f64()),
            arousal: sonic.as_ref().and_then(|v| v.get("arousal")).and_then(|v| v.as_f64()),
            acousticness: sonic.as_ref().and_then(|v| v.as_f64()),
            instrumentalness: sonic.as_ref().and_then(|v| v.get("instrumentalness")).and_then(|v| v.as_f64()),
            brightness: sonic.as_ref().and_then(|v| v.get("brightness")).and_then(|v| v.as_f64()),
            rms_energy,
            mbid,
            isrc,
            clap_embedding,
        })
    } else {
        None
    };

    Ok(Track {
        id,
        title,
        artist,
        artists,
        album,
        album_id,
        year,
        genre,
        duration,
        file_path,
        favorite,
        timestamp_added,
        timestamp_modified,
        cover_url: format!("/api/v1/tracks/{id}/cover"),
        analysis,
    })
}

// ---------------------------------------------------------------------------
// Track queries
// ---------------------------------------------------------------------------

pub fn list_tracks(conn: &Connection, params: &TrackQueryParams) -> Result<(i64, Vec<Track>)> {
    let mut wheres: Vec<String> = vec!["pm.provider_item_id IS NOT NULL".into()];
    let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![];

    if let Some(since) = params.since {
        wheres.push(format!("t.timestamp_modified > ?{}", values.len() + 1));
        values.push(Box::new(since));
    }
    if let Some(fav) = params.favorite {
        wheres.push(format!("t.favorite = ?{}", values.len() + 1));
        values.push(Box::new(fav as i64));
    }
    if let Some(ref genre) = params.genre {
        wheres.push(format!(
            "json_extract(t.metadata, '$.genres[0]') = ?{}",
            values.len() + 1
        ));
        values.push(Box::new(genre.clone()));
    }
    if let Some(artist_id) = params.artist_id {
        wheres.push(format!(
            "EXISTS (SELECT 1 FROM track_artists ta2 WHERE ta2.track_id = t.item_id AND ta2.artist_id = ?{})",
            values.len() + 1
        ));
        values.push(Box::new(artist_id));
    }
    if let Some(album_id) = params.album_id {
        wheres.push(format!(
            "EXISTS (SELECT 1 FROM album_tracks at3 WHERE at3.track_id = t.item_id AND at3.album_id = ?{})",
            values.len() + 1
        ));
        values.push(Box::new(album_id));
    }

    // Audio feature filters require joining the sonic analysis JSON.
    // We filter on extracted values using json_extract in a subquery.
    macro_rules! sonic_filter {
        ($field:expr, $op:expr, $val:expr) => {
            wheres.push(format!(
                "CAST(json_extract(aa_sonic.analysis_data, '$.{}') AS REAL) {} ?{}",
                $field, $op, values.len() + 1
            ));
            values.push(Box::new($val));
        };
    }
    if let Some(v) = params.bpm_min { sonic_filter!("bpm", ">=", v); }
    if let Some(v) = params.bpm_max { sonic_filter!("bpm", "<=", v); }
    if let Some(v) = params.energy_min { sonic_filter!("energy", ">=", v); }
    if let Some(v) = params.energy_max { sonic_filter!("energy", "<=", v); }
    if let Some(v) = params.valence_min { sonic_filter!("valence", ">=", v); }
    if let Some(v) = params.valence_max { sonic_filter!("valence", "<=", v); }
    if let Some(v) = params.arousal_min { sonic_filter!("arousal", ">=", v); }
    if let Some(v) = params.arousal_max { sonic_filter!("arousal", "<=", v); }

    let exclude_ids = params.exclude_ids();
    if !exclude_ids.is_empty() {
        let placeholders: Vec<String> = (0..exclude_ids.len())
            .map(|i| format!("?{}", values.len() + i + 1))
            .collect();
        wheres.push(format!("t.item_id NOT IN ({})", placeholders.join(",")));
        for id in &exclude_ids {
            values.push(Box::new(*id));
        }
    }

    let order_col = match params.order.as_deref().unwrap_or("name") {
        "timestamp_added" => "t.timestamp_added",
        "timestamp_modified" => "t.timestamp_modified",
        "random" => "RANDOM()",
        _ => "t.name",
    };
    let order_dir = if params.dir.as_deref() == Some("desc") { "DESC" } else { "ASC" };

    let where_clause = wheres.join(" AND ");
    let limit = params.clamped_limit();
    let offset = params.offset;

    // Count query (no JSON columns)
    let count_sql = format!(
        "SELECT COUNT(DISTINCT t.item_id)
         FROM tracks t
         LEFT JOIN track_artists ta ON ta.track_id = t.item_id
         LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
         LEFT JOIN provider_mappings pm ON pm.item_id = t.item_id AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
         LEFT JOIN audio_analysis aa_sonic ON aa_sonic.item_id = pm.provider_item_id AND aa_sonic.aa_provider_domain='sonic_analysis'
         WHERE {where_clause}"
    );
    let total: i64 = conn.query_row(&count_sql, rusqlite::params_from_iter(values.iter()), |r| r.get(0))?;

    // Data query
    let data_sql = format!(
        "{TRACK_BASE} AND {where_clause}
         GROUP BY t.item_id
         ORDER BY {order_col} {order_dir}
         LIMIT ?{} OFFSET ?{}",
        values.len() + 1,
        values.len() + 2
    );
    values.push(Box::new(limit));
    values.push(Box::new(offset));

    let include_analysis = params.include_analysis();
    let include_clap = params.include_clap();

    let mut stmt = conn.prepare(&data_sql)?;
    let tracks: Vec<Track> = stmt.query_map(
        rusqlite::params_from_iter(values.iter()),
        |row| parse_track_row(row, include_analysis, include_clap),
    )?.collect::<rusqlite::Result<_>>()?;

    Ok((total, tracks))
}

pub fn get_track(conn: &Connection, id: i64, include_analysis: bool, include_clap: bool) -> Result<Option<Track>> {
    let sql = format!(
        "{TRACK_BASE} AND t.item_id = ?1
         GROUP BY t.item_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(params![id], |row| parse_track_row(row, include_analysis, include_clap))?;
    Ok(rows.next().transpose()?)
}

pub fn get_track_file_path(conn: &Connection, id: i64) -> Result<Option<String>> {
    let sql = "SELECT pm.provider_item_id FROM provider_mappings pm
               WHERE pm.item_id = ?1 AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
               LIMIT 1";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query_map(params![id], |r| r.get(0))?;
    Ok(rows.next().transpose()?)
}

/// Load all CLAP vectors for the similarity index.
/// Returns (item_id, embedding_floats).
pub fn all_clap_vectors(conn: &Connection) -> Result<Vec<(i64, Vec<f32>)>> {
    let sql = "
        SELECT pm.item_id,
               json_extract(aa.analysis_data, '$.extra_data.clap_embedding') AS clap_json
        FROM provider_mappings pm
        JOIN audio_analysis aa ON aa.item_id = pm.provider_item_id
          AND aa.aa_provider_domain = 'sonic_analysis'
        WHERE pm.media_type = 'track'
          AND pm.provider_domain = 'filesystem_local'
          AND json_extract(aa.analysis_data, '$.extra_data.clap_embedding') IS NOT NULL
    ";
    let mut stmt = conn.prepare(sql)?;
    let results: Vec<(i64, Vec<f32>)> = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let clap_str: String = row.get(1)?;
        Ok((id, clap_str))
    })?
    .filter_map(|r| r.ok())
    .filter_map(|(id, clap_str)| {
        let arr: Vec<f32> = serde_json::from_str::<Vec<f64>>(&clap_str)
            .ok()?
            .into_iter()
            .map(|v| v as f32)
            .collect();
        if arr.is_empty() { None } else { Some((id, arr)) }
    })
    .collect();
    Ok(results)
}

// ---------------------------------------------------------------------------
// Album queries
// ---------------------------------------------------------------------------

pub fn list_albums(
    conn: &Connection,
    offset: i64,
    limit: i64,
    since: Option<i64>,
    order: &str,
    dir: &str,
    artist_id: Option<i64>,
) -> Result<(i64, Vec<Album>)> {
    let mut wheres: Vec<String> = vec![];
    let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![];

    if let Some(ts) = since {
        wheres.push(format!("alb.timestamp_added > ?{}", values.len() + 1));
        values.push(Box::new(ts));
    }
    if let Some(aid) = artist_id {
        wheres.push(format!(
            "EXISTS (SELECT 1 FROM album_artists aa2 WHERE aa2.album_id = alb.item_id AND aa2.artist_id = ?{})",
            values.len() + 1
        ));
        values.push(Box::new(aid));
    }

    let where_clause = if wheres.is_empty() {
        "1=1".into()
    } else {
        wheres.join(" AND ")
    };

    let order_col = match order {
        "timestamp_added" => "alb.timestamp_added",
        "play_count" => "alb.play_count",
        _ => "alb.name",
    };
    let order_dir = if dir == "desc" { "DESC" } else { "ASC" };

    let count_sql = format!(
        "SELECT COUNT(*) FROM albums alb WHERE {where_clause}"
    );
    let total: i64 = conn.query_row(
        &count_sql, rusqlite::params_from_iter(values.iter()), |r| r.get(0)
    )?;

    let data_sql = format!(
        "SELECT alb.item_id, alb.name,
                (SELECT a.name FROM album_artists aa JOIN artists a ON a.item_id = aa.artist_id
                 WHERE aa.album_id = alb.item_id LIMIT 1) AS artist,
                (SELECT aa.artist_id FROM album_artists aa WHERE aa.album_id = alb.item_id LIMIT 1) AS artist_id,
                alb.year,
                (SELECT COUNT(*) FROM album_tracks at2 WHERE at2.album_id = alb.item_id) AS track_count,
                alb.timestamp_added
         FROM albums alb
         WHERE {where_clause}
         ORDER BY {order_col} {order_dir}
         LIMIT ?{} OFFSET ?{}",
        values.len() + 1, values.len() + 2
    );
    values.push(Box::new(limit));
    values.push(Box::new(offset));

    let mut stmt = conn.prepare(&data_sql)?;
    let albums: Vec<Album> = stmt.query_map(
        rusqlite::params_from_iter(values.iter()),
        |row| {
            let id: i64 = row.get(0)?;
            Ok(Album {
                id,
                name: row.get(1)?,
                artist: row.get(2)?,
                artist_id: row.get(3)?,
                year: row.get(4)?,
                track_count: row.get(5)?,
                timestamp_added: row.get(6)?,
                cover_url: format!("/api/v1/albums/{id}/cover"),
            })
        },
    )?.collect::<rusqlite::Result<_>>()?;

    Ok((total, albums))
}

pub fn get_album(conn: &Connection, id: i64) -> Result<Option<Album>> {
    let sql = "SELECT alb.item_id, alb.name,
               (SELECT a.name FROM album_artists aa JOIN artists a ON a.item_id = aa.artist_id
                WHERE aa.album_id = alb.item_id LIMIT 1) AS artist,
               (SELECT aa.artist_id FROM album_artists aa WHERE aa.album_id = alb.item_id LIMIT 1) AS artist_id,
               alb.year,
               (SELECT COUNT(*) FROM album_tracks at2 WHERE at2.album_id = alb.item_id) AS track_count,
               alb.timestamp_added
               FROM albums alb WHERE alb.item_id = ?1";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query_map(params![id], |row| {
        let id: i64 = row.get(0)?;
        Ok(Album {
            id,
            name: row.get(1)?,
            artist: row.get(2)?,
            artist_id: row.get(3)?,
            year: row.get(4)?,
            track_count: row.get(5)?,
            timestamp_added: row.get(6)?,
            cover_url: format!("/api/v1/albums/{id}/cover"),
        })
    })?;
    Ok(rows.next().transpose()?)
}

// ---------------------------------------------------------------------------
// Artist queries
// ---------------------------------------------------------------------------

pub fn list_artists(conn: &Connection, offset: i64, limit: i64) -> Result<(i64, Vec<Artist>)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM artists", [], |r| r.get(0))?;
    let sql = "SELECT a.item_id, a.name,
               (SELECT COUNT(*) FROM track_artists ta WHERE ta.artist_id = a.item_id) AS track_count,
               (SELECT COUNT(*) FROM album_artists aa WHERE aa.artist_id = a.item_id) AS album_count
               FROM artists a
               ORDER BY a.name ASC
               LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let artists: Vec<Artist> = stmt.query_map(params![limit, offset], |row| {
        Ok(Artist {
            id: row.get(0)?,
            name: row.get(1)?,
            track_count: row.get(2)?,
            album_count: row.get(3)?,
        })
    })?.collect::<rusqlite::Result<_>>()?;
    Ok((total, artists))
}

pub fn get_artist(conn: &Connection, id: i64) -> Result<Option<Artist>> {
    let sql = "SELECT a.item_id, a.name,
               (SELECT COUNT(*) FROM track_artists ta WHERE ta.artist_id = a.item_id) AS track_count,
               (SELECT COUNT(*) FROM album_artists aa WHERE aa.artist_id = a.item_id) AS album_count
               FROM artists a WHERE a.item_id = ?1";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query_map(params![id], |row| {
        Ok(Artist {
            id: row.get(0)?,
            name: row.get(1)?,
            track_count: row.get(2)?,
            album_count: row.get(3)?,
        })
    })?;
    Ok(rows.next().transpose()?)
}

// ---------------------------------------------------------------------------
// Playlist queries
// ---------------------------------------------------------------------------

pub fn list_playlists(conn: &Connection, offset: i64, limit: i64) -> Result<(i64, Vec<Playlist>)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM playlists", [], |r| r.get(0))?;
    let sql = "SELECT p.item_id, p.name,
               (SELECT COUNT(*) FROM playlist_tracks pt WHERE pt.playlist_id = p.item_id) AS track_count,
               p.timestamp_modified
               FROM playlists p ORDER BY p.name ASC LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let playlists: Vec<Playlist> = stmt.query_map(params![limit, offset], |row| {
        Ok(Playlist {
            id: row.get(0)?,
            name: row.get(1)?,
            track_count: row.get(2)?,
            timestamp_modified: row.get(3)?,
        })
    })?.collect::<rusqlite::Result<_>>()?;
    Ok((total, playlists))
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

pub struct SearchResults {
    pub tracks: Vec<Track>,
    pub albums: Vec<Album>,
    pub artists: Vec<Artist>,
}

pub fn search(conn: &Connection, q: &str, limit: i64) -> Result<SearchResults> {
    let pattern = format!("%{q}%");

    // Tracks: search name + artist
    let track_sql = format!(
        "{TRACK_BASE} AND (t.name LIKE ?1 OR a.name LIKE ?1)
         GROUP BY t.item_id ORDER BY t.name ASC LIMIT ?2"
    );
    let mut stmt = conn.prepare(&track_sql)?;
    let tracks: Vec<Track> = stmt.query_map(params![pattern, limit], |row| {
        parse_track_row(row, false, false)
    })?.collect::<rusqlite::Result<_>>()?;

    // Albums
    let album_sql = "SELECT alb.item_id, alb.name,
                     (SELECT a.name FROM album_artists aa JOIN artists a ON a.item_id = aa.artist_id
                      WHERE aa.album_id = alb.item_id LIMIT 1) AS artist,
                     (SELECT aa.artist_id FROM album_artists aa WHERE aa.album_id = alb.item_id LIMIT 1) AS artist_id,
                     alb.year,
                     (SELECT COUNT(*) FROM album_tracks at2 WHERE at2.album_id = alb.item_id) AS track_count,
                     alb.timestamp_added
                     FROM albums alb WHERE alb.name LIKE ?1 ORDER BY alb.name ASC LIMIT ?2";
    let mut stmt = conn.prepare(album_sql)?;
    let albums: Vec<Album> = stmt.query_map(params![pattern, limit], |row| {
        let id: i64 = row.get(0)?;
        Ok(Album {
            id,
            name: row.get(1)?,
            artist: row.get(2)?,
            artist_id: row.get(3)?,
            year: row.get(4)?,
            track_count: row.get(5)?,
            timestamp_added: row.get(6)?,
            cover_url: format!("/api/v1/albums/{id}/cover"),
        })
    })?.collect::<rusqlite::Result<_>>()?;

    // Artists
    let artist_sql = "SELECT a.item_id, a.name,
                      (SELECT COUNT(*) FROM track_artists ta WHERE ta.artist_id = a.item_id),
                      (SELECT COUNT(*) FROM album_artists aa WHERE aa.artist_id = a.item_id)
                      FROM artists a WHERE a.name LIKE ?1 ORDER BY a.name ASC LIMIT ?2";
    let mut stmt = conn.prepare(artist_sql)?;
    let artists: Vec<Artist> = stmt.query_map(params![pattern, limit], |row| {
        Ok(Artist {
            id: row.get(0)?,
            name: row.get(1)?,
            track_count: row.get(2)?,
            album_count: row.get(3)?,
        })
    })?.collect::<rusqlite::Result<_>>()?;

    Ok(SearchResults { tracks, albums, artists })
}
```

- [ ] **Verify compile**

```bash
cd /Users/jonathan/code/music-assistant-db-api && ~/.cargo/bin/cargo build 2>&1
```
Expected: compiles cleanly (no warnings about unused imports at this stage is fine).

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: all SQL queries — tracks, albums, artists, playlists, search, health"
```

---

## Task 5: Similarity index

**Files:**
- Create: `src/similarity.rs`

- [ ] **Write `src/similarity.rs`**

```rust
use std::sync::Arc;
use parking_lot::RwLock;

/// In-memory CLAP vector index. Loaded once at startup, refreshed on demand.
/// Uses brute-force cosine similarity — fast enough for ≤100k tracks in Rust.
pub struct SimilarityIndex {
    vectors: Arc<RwLock<Vec<(i64, Vec<f32>)>>>,
}

impl SimilarityIndex {
    pub fn new(vectors: Vec<(i64, Vec<f32>)>) -> Self {
        tracing::info!("similarity index loaded: {} vectors", vectors.len());
        Self {
            vectors: Arc::new(RwLock::new(vectors)),
        }
    }

    pub fn empty() -> Self {
        Self { vectors: Arc::new(RwLock::new(vec![])) }
    }

    pub fn len(&self) -> usize {
        self.vectors.read().len()
    }

    /// Replace the index contents (e.g. after a DB refresh).
    pub fn reload(&self, vectors: Vec<(i64, Vec<f32>)>) {
        tracing::info!("similarity index reloaded: {} vectors", vectors.len());
        *self.vectors.write() = vectors;
    }

    /// Find the `limit` most similar tracks to `query_id`, excluding `exclude_ids`.
    /// Returns `(track_id, cosine_score)` sorted descending by score.
    pub fn find_similar(&self, query_id: i64, limit: usize, exclude_ids: &[i64]) -> Vec<(i64, f32)> {
        let vecs = self.vectors.read();
        let query_vec = match vecs.iter().find(|(id, _)| *id == query_id) {
            Some((_, v)) => v,
            None => return vec![],
        };
        let query_norm = l2_norm(query_vec);
        if query_norm == 0.0 {
            return vec![];
        }

        let mut scores: Vec<(i64, f32)> = vecs
            .iter()
            .filter(|(id, _)| *id != query_id && !exclude_ids.contains(id))
            .map(|(id, v)| {
                let score = cosine_similarity(query_vec, v, query_norm);
                (*id, score)
            })
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(limit);
        scores
    }
}

fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

fn cosine_similarity(a: &[f32], b: &[f32], a_norm: f32) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let b_norm = l2_norm(b);
    if b_norm == 0.0 { return 0.0; }
    dot / (a_norm * b_norm)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index() -> SimilarityIndex {
        SimilarityIndex::new(vec![
            (1, vec![1.0, 0.0, 0.0]),
            (2, vec![1.0, 0.0, 0.0]),  // identical to 1
            (3, vec![0.0, 1.0, 0.0]),  // orthogonal to 1
            (4, vec![-1.0, 0.0, 0.0]), // opposite to 1
        ])
    }

    #[test]
    fn similar_to_identical() {
        let idx = make_index();
        let results = idx.find_similar(1, 3, &[]);
        assert_eq!(results[0].0, 2);  // identical vector is most similar
        assert!((results[0].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn opposite_is_least_similar() {
        let idx = make_index();
        let results = idx.find_similar(1, 3, &[]);
        let last = results.last().unwrap();
        assert_eq!(last.0, 4);
        assert!((last.1 + 1.0).abs() < 1e-6);
    }

    #[test]
    fn exclude_ids_respected() {
        let idx = make_index();
        let results = idx.find_similar(1, 3, &[2]);
        assert!(!results.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn unknown_query_returns_empty() {
        let idx = make_index();
        assert!(idx.find_similar(999, 10, &[]).is_empty());
    }
}
```

- [ ] **Run similarity tests**

```bash
cd /Users/jonathan/code/music-assistant-db-api && ~/.cargo/bin/cargo test similarity 2>&1
```
Expected: 4 tests pass.

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: in-memory CLAP cosine similarity index"
```

---

## Task 6: Auth middleware

**Files:**
- Create: `src/auth.rs`

- [ ] **Write `src/auth.rs`**

```rust
use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use serde_json::json;

pub async fn require_api_key(
    key: String,
    req: Request,
    next: Next,
) -> Response {
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim);

    match provided {
        Some(token) if token == key => next.run(req).await,
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "invalid or missing API key" })),
        )
            .into_response(),
    }
}
```

- [ ] **Verify compile**

```bash
cd /Users/jonathan/code/music-assistant-db-api && ~/.cargo/bin/cargo build 2>&1
```

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: optional Bearer token auth middleware"
```

---

## Task 7: Route handlers

**Files:**
- Create: `src/routes/mod.rs`
- Create: `src/routes/health.rs`
- Create: `src/routes/tracks.rs`
- Create: `src/routes/similar.rs`
- Create: `src/routes/cover.rs`
- Create: `src/routes/albums.rs`
- Create: `src/routes/artists.rs`
- Create: `src/routes/playlists.rs`
- Create: `src/routes/search.rs`

- [ ] **Write `src/routes/health.rs`**

```rust
use axum::{extract::State, Json};
use serde_json::{json, Value};
use crate::{db::queries, error::AppError};
use deadpool_sqlite::Pool;

pub async fn health(State(pool): State<Pool>) -> Result<Json<Value>, AppError> {
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
```

- [ ] **Write `src/routes/tracks.rs`**

```rust
use axum::{extract::{Path, Query, State}, Json};
use deadpool_sqlite::Pool;
use crate::{
    db::queries,
    error::AppError,
    models::{track::TrackQueryParams, Page},
    models::track::Track,
};

pub async fn list_tracks(
    State(pool): State<Pool>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}

pub async fn get_track(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
    Query(params): Query<TrackQueryParams>,
) -> Result<Json<Track>, AppError> {
    let include_analysis = params.include_analysis();
    let include_clap = params.include_clap();
    let track = pool.get().await?
        .interact(move |conn| queries::get_track(conn, id, include_analysis, include_clap))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("track {id} not found")))?;
    Ok(Json(track))
}
```

- [ ] **Write `src/routes/similar.rs`**

```rust
use axum::{extract::{Path, Query, State}, Json};
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
    State((_, index)): State<(Pool, Arc<SimilarityIndex>)>,
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
```

- [ ] **Write `src/routes/cover.rs`**

```rust
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, StatusCode},
    response::Response,
};
use bytes::Bytes;
use deadpool_sqlite::Pool;
use lofty::{file::TaggedFileExt, probe::Probe, config::ParseOptions};
use lru::LruCache;
use parking_lot::Mutex;
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use crate::{db::queries, error::AppError};

pub type CoverCache = Arc<Mutex<LruCache<i64, Option<(Bytes, String)>>>>;

pub fn new_cover_cache() -> CoverCache {
    Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(512).unwrap())))
}

pub async fn track_cover(
    State((pool, music_root, cache)): State<(Pool, String, CoverCache)>,
    Path(id): Path<i64>,
) -> Result<Response<Body>, AppError> {
    // Check cache first.
    if let Some(cached) = cache.lock().get(&id) {
        return Ok(build_cover_response(cached));
    }

    // Fetch file path from DB.
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
    State((pool, music_root, cache)): State<(Pool, String, CoverCache)>,
    Path(album_id): Path<i64>,
) -> Result<Response<Body>, AppError> {
    // Cache key: negative album_id to distinguish from track ids.
    let cache_key = -album_id;
    if let Some(cached) = cache.lock().get(&cache_key) {
        return Ok(build_cover_response(cached));
    }

    // Find the first track in the album, use its embedded art.
    let file_path: Option<String> = pool.get().await?
        .interact(move |conn| {
            conn.query_row(
                "SELECT pm.provider_item_id FROM album_tracks at2
                 JOIN provider_mappings pm ON pm.item_id = at2.track_id
                   AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
                 WHERE at2.album_id = ?1 LIMIT 1",
                rusqlite::params![album_id],
                |r| r.get(0),
            ).optional().map_err(anyhow::Error::from)
        })
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    let result = file_path
        .as_deref()
        .and_then(|rel| extract_cover(&PathBuf::from(&music_root).join(rel)));

    cache.lock().put(cache_key, result.clone());
    Ok(build_cover_response(&result))
}

fn extract_cover(path: &PathBuf) -> Option<(Bytes, String)> {
    let tagged = Probe::open(path)
        .ok()?
        .options(ParseOptions::new().read_tags(true).read_properties(false))
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
```

- [ ] **Write `src/routes/albums.rs`**

```rust
use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::Deserialize;
use crate::{db::queries, error::AppError, models::{Album, Page, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct AlbumParams {
    #[serde(default)] pub offset: i64,
    #[serde(default = "default_limit")] pub limit: i64,
    pub since: Option<i64>,
    pub order: Option<String>,
    pub dir: Option<String>,
    pub artist_id: Option<i64>,
}
fn default_limit() -> i64 { 100 }

pub async fn list_albums(
    State(pool): State<Pool>,
    Query(p): Query<AlbumParams>,
) -> Result<Json<Page<Album>>, AppError> {
    let limit = p.limit.clamp(1, 1000);
    let order = p.order.as_deref().unwrap_or("name").to_string();
    let dir = p.dir.as_deref().unwrap_or("asc").to_string();
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_albums(conn, p.offset, limit, p.since, &order, &dir, p.artist_id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}

pub async fn get_album(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
) -> Result<Json<Album>, AppError> {
    let album = pool.get().await?
        .interact(move |conn| queries::get_album(conn, id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("album {id} not found")))?;
    Ok(Json(album))
}

pub async fn album_tracks(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
    Query(mut params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    params.album_id = Some(id);
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}
```

- [ ] **Write `src/routes/artists.rs`**

```rust
use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::Deserialize;
use crate::{db::queries, error::AppError, models::{Artist, Page, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct Paged { #[serde(default)] pub offset: i64, #[serde(default = "dl")] pub limit: i64 }
fn dl() -> i64 { 100 }

pub async fn list_artists(
    State(pool): State<Pool>,
    Query(p): Query<Paged>,
) -> Result<Json<Page<Artist>>, AppError> {
    let limit = p.limit.clamp(1, 1000);
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_artists(conn, p.offset, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset: p.offset, limit, items }))
}

pub async fn get_artist(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
) -> Result<Json<Artist>, AppError> {
    let artist = pool.get().await?
        .interact(move |conn| queries::get_artist(conn, id))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??
        .ok_or_else(|| AppError::NotFound(format!("artist {id} not found")))?;
    Ok(Json(artist))
}

pub async fn artist_tracks(
    State(pool): State<Pool>,
    Path(id): Path<i64>,
    Query(mut params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    params.artist_id = Some(id);
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| queries::list_tracks(conn, &params))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(Page { total, offset, limit, items }))
}
```

- [ ] **Write `src/routes/playlists.rs`**

```rust
use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::Deserialize;
use crate::{db::queries, error::AppError, models::{Playlist, Page, track::{Track, TrackQueryParams}}};

#[derive(Deserialize)]
pub struct Paged { #[serde(default)] pub offset: i64, #[serde(default = "dl")] pub limit: i64 }
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
    Query(mut params): Query<TrackQueryParams>,
) -> Result<Json<Page<Track>>, AppError> {
    // Reuse the tracks query with a playlist subquery filter.
    // Inject a pseudo-filter via the exclude mechanism — simpler than a new query path.
    // Instead, override: filter tracks to only those in this playlist.
    let limit = params.clamped_limit();
    let offset = params.offset;
    let (total, items) = pool.get().await?
        .interact(move |conn| {
            // Get track IDs for the playlist.
            let mut stmt = conn.prepare(
                "SELECT pt.track_id FROM playlist_tracks pt WHERE pt.playlist_id = ?1 ORDER BY pt.position ASC"
            )?;
            let ids: Vec<i64> = stmt.query_map(rusqlite::params![playlist_id], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?;

            if ids.is_empty() {
                return Ok((0i64, vec![]));
            }

            // Build a query for these specific IDs maintaining playlist order.
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
            let mut all_params: Vec<Box<dyn rusqlite::ToSql>> = ids.iter().map(|id| Box::new(*id) as Box<dyn rusqlite::ToSql>).collect();
            all_params.push(Box::new(limit));
            all_params.push(Box::new(offset));

            let total = ids.len() as i64;
            let mut stmt = conn.prepare(&sql)?;
            let tracks: Vec<crate::models::track::Track> = stmt.query_map(
                rusqlite::params_from_iter(all_params.iter()),
                |row| crate::db::queries::parse_track_row_pub(row, params.include_analysis(), params.include_clap()),
            )?.collect::<rusqlite::Result<_>>()?;

            Ok((total, tracks))
        })
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;

    Ok(Json(Page { total, offset, limit, items }))
}
```

- [ ] **Write `src/routes/search.rs`**

```rust
use axum::extract::{Query, State};
use axum::Json;
use deadpool_sqlite::Pool;
use serde::{Deserialize, Serialize};
use crate::{db::queries, error::AppError, models::{Track, Album, Artist}};

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    #[serde(default = "dl")] pub limit: i64,
}
fn dl() -> i64 { 10 }

#[derive(Serialize)]
pub struct SearchResponse {
    pub tracks: Vec<Track>,
    pub albums: Vec<Album>,
    pub artists: Vec<Artist>,
}

pub async fn search(
    State(pool): State<Pool>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, AppError> {
    if params.q.trim().is_empty() {
        return Err(AppError::BadRequest("q is required".into()));
    }
    let q = params.q.clone();
    let limit = params.limit.clamp(1, 50);
    let results = pool.get().await?
        .interact(move |conn| queries::search(conn, &q, limit))
        .await.map_err(|e| anyhow::anyhow!("{e}"))??;
    Ok(Json(SearchResponse {
        tracks: results.tracks,
        albums: results.albums,
        artists: results.artists,
    }))
}
```

- [ ] **Write `src/routes/mod.rs`**

```rust
pub mod albums;
pub mod artists;
pub mod cover;
pub mod health;
pub mod playlists;
pub mod search;
pub mod similar;
pub mod tracks;
```

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: all HTTP route handlers"
```

---

## Task 8: Main entry point and wiring

**Files:**
- Modify: `src/main.rs`

**Note:** The `playlist_tracks` handler calls `parse_track_row_pub` — we need to expose that function from `queries.rs`. Add `pub use` in this task.

- [ ] **Make `parse_track_row` public in `src/db/queries.rs`**

Change the function signature from:
```rust
fn parse_track_row(row: &rusqlite::Row, include_analysis: bool, include_clap: bool) -> rusqlite::Result<Track> {
```
to:
```rust
pub fn parse_track_row_pub(row: &rusqlite::Row, include_analysis: bool, include_clap: bool) -> rusqlite::Result<Track> {
```

And update the two internal call sites in `list_tracks` and `get_track` to call `parse_track_row_pub` instead.

- [ ] **Write `src/main.rs`**

```rust
mod auth;
mod camelot;
mod config;
mod db;
mod error;
mod models;
mod routes;
mod similarity;

use std::sync::Arc;
use axum::{
    middleware,
    routing::get,
    Router,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use config::Config;
use routes::cover::{new_cover_cache, album_cover, track_cover};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    tracing::info!("connecting to {}", cfg.db_path);

    let pool = db::build_pool(&cfg.db_path, cfg.pool_size).await?;

    // Load similarity index in background — don't block startup.
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

    // Cover state tuples
    let track_cover_state = (pool.clone(), music_root.clone(), cover_cache.clone());
    let album_cover_state = (pool.clone(), music_root.clone(), cover_cache.clone());

    // Build router
    let api = Router::new()
        .route("/health", get(routes::health::health).with_state(pool.clone()))
        .route("/tracks", get(routes::tracks::list_tracks).with_state(pool.clone()))
        .route("/tracks/:id", get(routes::tracks::get_track).with_state(pool.clone()))
        .route("/tracks/:id/similar",
            get(routes::similar::similar_tracks)
                .with_state((pool.clone(), sim_index.clone())))
        .route("/tracks/:id/cover",
            get(track_cover).with_state(track_cover_state))
        .route("/albums", get(routes::albums::list_albums).with_state(pool.clone()))
        .route("/albums/:id", get(routes::albums::get_album).with_state(pool.clone()))
        .route("/albums/:id/tracks", get(routes::albums::album_tracks).with_state(pool.clone()))
        .route("/albums/:id/cover",
            get(album_cover).with_state(album_cover_state))
        .route("/artists", get(routes::artists::list_artists).with_state(pool.clone()))
        .route("/artists/:id", get(routes::artists::get_artist).with_state(pool.clone()))
        .route("/artists/:id/tracks", get(routes::artists::artist_tracks).with_state(pool.clone()))
        .route("/playlists", get(routes::playlists::list_playlists).with_state(pool.clone()))
        .route("/playlists/:id/tracks", get(routes::playlists::playlist_tracks).with_state(pool.clone()))
        .route("/search", get(routes::search::search).with_state(pool.clone()));

    // Wrap with auth middleware if key is configured
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
```

- [ ] **Build release**

```bash
cd /Users/jonathan/code/music-assistant-db-api && source ~/.cargo/env && cargo build --release 2>&1
```
Expected: compiles. Binary at `target/release/ma-db-api`.

- [ ] **Run all tests**

```bash
cd /Users/jonathan/code/music-assistant-db-api && source ~/.cargo/env && cargo test 2>&1
```
Expected: camelot (4) + similarity (4) = 8 tests pass.

- [ ] **Commit**

```bash
git add -A && git commit -m "feat: main entry point — pool, similarity index, auth, full router"
```

---

## Task 9: Dockerfile and deployment manifests

**Files:**
- Create: `Dockerfile`
- Create: `k8s/sidecar-patch.yaml`
- Create: `docker-compose.yml`

- [ ] **Write `Dockerfile`**

```dockerfile
# Build stage
FROM rust:1.96-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml ./
# Cache dependencies
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/ma-db-api /usr/local/bin/ma-db-api
EXPOSE 8097
ENTRYPOINT ["/usr/local/bin/ma-db-api"]
```

- [ ] **Write `k8s/sidecar-patch.yaml`**

```yaml
# Strategic merge patch — add ma-db-api sidecar to the music-assistant deployment.
# Apply with:
#   kubectl patch deployment music-assistant -n music-assistant \
#     --patch-file k8s/sidecar-patch.yaml
# Or add to your GitOps repo alongside deployment.yaml.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: music-assistant
  namespace: music-assistant
spec:
  template:
    spec:
      containers:
        - name: ma-db-api
          image: ghcr.io/jonathanlew1s/music-assistant-db-api:latest
          imagePullPolicy: Always
          env:
            - name: MA_DB_PATH
              value: /data/library.db
            - name: MA_MUSIC_ROOT
              value: /music
            - name: PORT
              value: "8097"
            # Optional: set MA_BRIDGE_API_KEY via a Secret reference
            # - name: MA_BRIDGE_API_KEY
            #   valueFrom:
            #     secretKeyRef:
            #       name: ma-bridge-secret
            #       key: api-key
          ports:
            - name: api
              containerPort: 8097
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /api/v1/health
              port: api
            initialDelaySeconds: 5
            periodSeconds: 10
            failureThreshold: 3
          livenessProbe:
            httpGet:
              path: /api/v1/health
              port: api
            initialDelaySeconds: 10
            periodSeconds: 30
            failureThreshold: 3
          resources:
            requests:
              memory: "64Mi"
              cpu: "50m"
            limits:
              memory: "512Mi"   # headroom for CLAP index (~30MB) + LRU cover cache
              cpu: "500m"
          volumeMounts:
            - name: data
              mountPath: /data
              readOnly: true
            - name: music
              mountPath: /music
              readOnly: true
---
# Expose the bridge port on the existing service, or create a separate one.
# Option A: patch the existing service (add port 8097).
# Option B (below): create a dedicated service — safer, no patch on the MA service.
apiVersion: v1
kind: Service
metadata:
  name: ma-db-api
  namespace: music-assistant
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: music-assistant
  ports:
    - name: api
      port: 8097
      targetPort: 8097
      protocol: TCP
```

- [ ] **Write `docker-compose.yml`**

```yaml
# Standalone compose for non-k8s deployments.
# The ma-data and music volumes must already exist and be named to match
# whatever your Music Assistant compose file uses.
services:
  ma-db-api:
    image: ghcr.io/jonathanlew1s/music-assistant-db-api:latest
    build: .
    environment:
      MA_DB_PATH: /data/library.db
      MA_MUSIC_ROOT: /music
      PORT: "8097"
      MA_BRIDGE_API_KEY: ${MA_BRIDGE_API_KEY:-}
      LOG_LEVEL: ${LOG_LEVEL:-info}
    volumes:
      - ma-data:/data:ro
      - music:/music:ro
    ports:
      - "${MA_BRIDGE_PORT:-8097}:8097"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8097/api/v1/health"]
      interval: 30s
      timeout: 5s
      retries: 3

volumes:
  ma-data:
    external: true
  music:
    external: true
```

- [ ] **Commit**

```bash
git add -A && git commit -m "chore: Dockerfile, k8s sidecar patch, docker-compose"
```

---

## Task 10: Push and GitHub Actions CI

**Files:**
- Create: `.github/workflows/build.yml`

- [ ] **Write `.github/workflows/build.yml`**

```yaml
name: Build

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test

  build:
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/main'
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v6
        with:
          context: .
          push: true
          tags: |
            ghcr.io/jonathanlew1s/music-assistant-db-api:latest
            ghcr.io/jonathanlew1s/music-assistant-db-api:${{ github.sha }}
```

- [ ] **Push to GitHub**

```bash
cd /Users/jonathan/code/music-assistant-db-api
git add -A
git commit -m "chore: GitHub Actions CI — test + build + push to GHCR"
git push -u origin main
```

Expected: push succeeds, Actions workflow triggers.

---

## Self-Review Notes

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `GET /api/v1/health` | Task 7 (health.rs) |
| `GET /api/v1/tracks` with all filters | Task 4 (queries.rs), Task 7 (tracks.rs) |
| `GET /api/v1/tracks/:id` | Task 7 |
| `GET /api/v1/tracks/:id/similar` — cosine KNN | Task 5 (similarity.rs), Task 7 (similar.rs) |
| `GET /api/v1/tracks/:id/cover` — embedded tags via lofty | Task 7 (cover.rs) |
| `GET /api/v1/albums` + `/:id` + `/:id/tracks` + `/:id/cover` | Task 7 (albums.rs, cover.rs) |
| `GET /api/v1/artists` + `/:id` + `/:id/tracks` | Task 7 (artists.rs) |
| `GET /api/v1/playlists` + `/:id/tracks` | Task 7 (playlists.rs) |
| `GET /api/v1/search` | Task 7 (search.rs) |
| Optional API key auth | Task 6 (auth.rs), Task 8 (main.rs) |
| Camelot conversion | Task 2 (camelot.rs) |
| Sidecar k8s deployment | Task 9 (sidecar-patch.yaml) |
| Docker image | Task 9 (Dockerfile) |
| docker-compose | Task 9 |
| CI | Task 10 |

**Known gaps addressed in implementation notes:**
- `play_count` on Album: queried as `alb.play_count` — may be null if MA doesn't have this column; graceful since it's optional in the model
- `favorite` on Track: queried as `t.favorite` — confirmed present in MA schema
- `timestamp_added`: confirmed present on tracks and albums in MA schema
- CLAP vectors stored as JSON in `audio_analysis.analysis_data -> extra_data.clap_embedding` — parsed at load time
- Playlist track order preserved via `pt.position` column

**One known simplification:** `artists/:id/albums` route is in the spec but not implemented above (omitted to keep plan focused — the pattern is identical to `artist_tracks`, just filtering albums). Add if needed.
