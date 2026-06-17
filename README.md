# music-assistant-db-api

A standalone Rust HTTP service that exposes [Music Assistant](https://music-assistant.io)'s SQLite database as a REST API — without going through the MA web server.

Music Assistant runs deep audio analysis (CLAP embeddings, BPM, key, loudness, valence/energy) and stores results in `library.db`. This bridge makes that data available over HTTP for downstream tools without adding load to, or depending on, the MA API process.

## How it works

The service runs as a **sidecar container** in the Music Assistant pod. Both containers share the pod's volume mount, so the bridge reads `library.db` directly via a read-only SQLite connection pool. On startup it loads all CLAP embeddings (≈28 MB for 7k tracks) into a brute-force cosine similarity index. Cover art is extracted from embedded audio file tags via `lofty` and cached in an in-process LRU.

```
┌─────────────────────────────────────────────────────┐
│  music-assistant pod                                │
│                                                     │
│  ┌──────────────────┐    ┌───────────────────────┐  │
│  │  music-assistant │    │     ma-db-api          │  │
│  │  :8095           │    │     :8097              │  │
│  └──────────────────┘    └───────────────────────┘  │
│                \                   /                │
│              /data/library.db  /music/              │
│                (Longhorn RWO PVC)                   │
└─────────────────────────────────────────────────────┘
         ↕ ClusterIP service: ma-db-api:8097
```

The sidecar pattern avoids the ReadWriteOnce constraint on the Longhorn PVC — both containers share the pod-level volume attachment without needing RWX storage.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `MA_DB_PATH` | yes | — | Absolute path to `library.db` (e.g. `/data/library.db`) |
| `MA_MUSIC_ROOT` | yes | — | Absolute path to the music library root (e.g. `/music`) |
| `PORT` | no | `8097` | Port to listen on |
| `MA_BRIDGE_API_KEY` | no | — | If set, all requests require `Authorization: Bearer <key>` |
| `DB_POOL_SIZE` | no | `4` | SQLite connection pool size |
| `LOG_LEVEL` | no | `info` | `tracing` filter, e.g. `debug`, `info,tower_http=warn` |

## API

All endpoints are under `/api/v1`. Paginated endpoints return:

```json
{ "total": 37219, "offset": 0, "limit": 100, "items": [...] }
```

### `GET /api/v1/health`

```bash
curl http://localhost:8097/api/v1/health
```

```json
{
  "status": "ok",
  "db_schema_version": 14,
  "track_count": 37219,
  "analysis_coverage": {
    "loudness": 78745,
    "bpm": 7047,
    "clap": 7056,
    "sonic": 7056
  }
}
```

---

### `GET /api/v1/tracks`

List tracks with optional filtering.

| Parameter | Type | Description |
|---|---|---|
| `offset` / `limit` | int | Pagination (limit clamped to 1–1000, default 100) |
| `since` | unix timestamp | Only tracks modified after this time |
| `favorite` | bool | Filter by starred/favourited status |
| `genre` | string | Filter by first genre tag |
| `artist_id` | int | Filter to tracks by a specific artist |
| `album_id` | int | Filter to tracks on a specific album |
| `bpm_min` / `bpm_max` | float | BPM range |
| `energy_min` / `energy_max` | float | Energy 0–1 |
| `valence_min` / `valence_max` | float | Valence 0–1 |
| `arousal_min` / `arousal_max` | float | Arousal 0–1 |
| `order` | `name` \| `timestamp_added` \| `timestamp_modified` \| `random` | Sort column |
| `dir` | `asc` \| `desc` | Sort direction |
| `exclude` | comma-separated IDs | Exclude specific track IDs |
| `include` | `analysis` \| `analysis,clap` | Add audio analysis fields; `clap` adds the 1024-dim embedding |

```bash
# High-energy tracks modified in the last week, with analysis
curl "http://localhost:8097/api/v1/tracks?energy_min=0.7&since=1749600000&include=analysis&limit=20"
```

Track object:

```json
{
  "id": 12345,
  "title": "Song Name",
  "artist": "Primary Artist",
  "artists": ["Primary Artist", "Featured Artist"],
  "album": "Album Name",
  "album_id": 456,
  "year": 2021,
  "genre": "Electronic",
  "duration": 243.5,
  "file_path": "Artist/Album/01 Song.flac",
  "favorite": false,
  "timestamp_added": 1700000000,
  "timestamp_modified": 1700000000,
  "cover_url": "/api/v1/tracks/12345/cover",
  "analysis": null
}
```

With `?include=analysis`:

```json
{
  "analysis": {
    "loudness_lufs": -10.4,
    "loudness_album_lufs": -11.2,
    "bpm": 128.0,
    "key": "D#",
    "mode": "minor",
    "camelot": "2A",
    "beats": [0.23, 0.69, 1.16],
    "valence": 0.61,
    "energy": 0.82,
    "danceability": 0.74,
    "arousal": 0.78,
    "acousticness": 0.04,
    "instrumentalness": 0.91,
    "brightness": 0.55,
    "rms_energy": [0.12, 0.18],
    "mbid": "abc123",
    "isrc": "GBAYE0000001",
    "clap_embedding": null
  }
}
```

With `?include=analysis,clap`, `clap_embedding` becomes a 1024-element float array.

---

### `GET /api/v1/tracks/:id`

Single track. Same `?include` parameter as the list endpoint.

```bash
curl "http://localhost:8097/api/v1/tracks/12345?include=analysis"
```

---

### `GET /api/v1/tracks/:id/similar`

Find acoustically similar tracks using cosine similarity over CLAP embeddings. Returns nothing if the track has no CLAP vector yet.

| Parameter | Type | Description |
|---|---|---|
| `limit` | int | Number of results (1–50, default 10) |
| `exclude` | comma-separated IDs | Exclude these track IDs from results |

```bash
curl "http://localhost:8097/api/v1/tracks/12345/similar?limit=5&exclude=12345,99001"
```

```json
{
  "source_id": 12345,
  "results": [
    { "id": 9876, "score": 0.9421 },
    { "id": 3412, "score": 0.9187 }
  ]
}
```

---

### `GET /api/v1/tracks/:id/cover`

Embedded cover art extracted from the audio file. Returns `image/jpeg` or `image/png`. Responds 404 if no embedded art is found.

```bash
curl -o cover.jpg http://localhost:8097/api/v1/tracks/12345/cover
```

---

### `GET /api/v1/albums`

| Parameter | Type | Description |
|---|---|---|
| `offset` / `limit` | int | Pagination |
| `since` | unix timestamp | Albums added after this time |
| `artist_id` | int | Filter to albums by a specific artist |
| `order` | `name` \| `timestamp_added` \| `play_count` | Sort column |
| `dir` | `asc` \| `desc` | Sort direction |

### `GET /api/v1/albums/:id`

### `GET /api/v1/albums/:id/tracks`

Accepts the same filter/include parameters as `/tracks`.

### `GET /api/v1/albums/:id/cover`

Cover art from the first track in the album.

---

### `GET /api/v1/artists`

| Parameter | Type | Description |
|---|---|---|
| `offset` / `limit` | int | Pagination |

### `GET /api/v1/artists/:id`

### `GET /api/v1/artists/:id/tracks`

---

### `GET /api/v1/playlists`

### `GET /api/v1/playlists/:id/tracks`

Returns tracks in playlist order. Accepts `?include=analysis` / `?include=analysis,clap`.

---

### `GET /api/v1/search?q=`

Cross-entity text search across track titles, artist names, and album names.

| Parameter | Type | Description |
|---|---|---|
| `q` | string | Search term (required) |
| `limit` | int | Max results per entity type (1–50, default 10) |

```json
{
  "tracks": [...],
  "albums": [...],
  "artists": [...]
}
```

---

## Deployment

### Kubernetes sidecar (recommended)

Apply the included patch to add the `ma-db-api` container to the existing `music-assistant` deployment:

```bash
kubectl patch deployment music-assistant -n music-assistant \
  --patch-file k8s/sidecar-patch.yaml
```

The patch also creates a `ClusterIP` service (`ma-db-api.music-assistant:8097`) so other pods in the cluster can reach the bridge without going through a host port.

To rotate or add an API key:

```bash
kubectl create secret generic ma-bridge-secret \
  -n music-assistant \
  --from-literal=api-key="$(openssl rand -hex 32)"
```

Then uncomment the `MA_BRIDGE_API_KEY` env block in `k8s/sidecar-patch.yaml` and re-apply.

### Docker Compose (standalone)

The `docker-compose.yml` assumes the MA data volume (`ma-data`) and music volume (`music`) already exist as external named volumes from your existing MA compose setup.

```bash
MA_BRIDGE_API_KEY=your-secret docker compose up -d
```

### Running against a local copy of the DB

```bash
MA_DB_PATH=/path/to/library.db \
MA_MUSIC_ROOT=/path/to/music \
LOG_LEVEL=debug \
cargo run
```

The CLAP index loads in the background after startup — the health endpoint responds immediately, but `/similar` results are empty until loading completes (logged as `similarity index reloaded: N vectors`).

---

## Building

```bash
# Development
cargo build

# Release (optimised + stripped)
cargo build --release
# Binary: target/release/ma-db-api

# Docker
docker build -t ma-db-api .
```

The Docker image uses a two-stage Alpine build: the builder compiles with musl libc, the runtime image is minimal Alpine with only `ca-certificates`.

---

## Tests

```bash
cargo test
```

11 unit tests covering:
- Camelot wheel conversion (7 cases — C major = 8B, D# minor = 2A, enharmonic equivalence)
- Cosine similarity index (4 cases — identical/opposite/exclude/unknown)

---

## Camelot key notation

MA stores key and mode as separate fields (`"D#"` / `"minor"`). The bridge converts these to Camelot wheel notation at query time using the correct circle-of-fifths formula:

- Major: `((semitone × 7) + 8) mod 12`, mapping 0 → 12
- Minor: relative major is +3 semitones, same formula

This corrects a bug in the SUB/WAVE `sync-from-ma.ts` script, which used `((idx * 7) % 12) + 1` and produced wrong values (C major → 1B instead of 8B).

---

## Analysis coverage

Not all tracks have all analysis types. MA's analysis providers run progressively:

- `loudness_analysis` runs on every file — highest coverage
- `smart_fades` (BPM, key, beats) — grows as MA processes tracks
- `sonic_analysis` (CLAP, valence, energy) — subset; coverage shown in `/health`

The `/health` endpoint reports current coverage counts. Fields are `null` when a track hasn't been analysed yet.
