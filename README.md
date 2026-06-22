# music-assistant-db-api

A standalone Rust HTTP service that exposes [Music Assistant](https://music-assistant.io)'s SQLite database as a REST API — without going through the MA web server.

Music Assistant runs deep audio analysis (CLAP embeddings, BPM, key, loudness, valence/energy) and stores results in `library.db`. This bridge makes that data available over HTTP for downstream tools without adding load to, or depending on, the MA API process.

## How it works

The service runs as its own Deployment in the `music-assistant` namespace (k8s manifests live in the operator's GitOps repo, not here). On startup it loads all CLAP embeddings (≈28 MB for 7k tracks) into a brute-force cosine similarity index. Cover art is extracted from embedded audio file tags via `lofty` and cached in an in-process LRU.

**`MA_DB_PATH` must point at a periodically-refreshed clone of `library.db`, never the live file MA itself writes to.** This was a deliberate architecture change, not the original design. Two things were tried first and both failed under sustained load:

1. Reading the live file directly with `immutable=1` (fast, but assumes the file never changes while open) — broke with "database disk image is malformed" once MA's library.db (confirmed: genuinely in WAL mode) was held under sustained write contention during a full analysis pass (observed: continuous for 8+ days). It also explained an earlier symptom that looked unrelated: the `/health` liveness probe ran the same live-file query, so MA's own write activity could fail the probe and trigger pointless restart loops that never fixed anything.
2. An in-process background task taking periodic copies via SQLite's `VACUUM INTO` — more robust than a raw file copy, but still has to acquire at least a read snapshot against the live file, which can itself stay blocked for as long as MA's write burst lasts. With no quiet gap for days at a time, this never succeeded either.

What actually works: a **Longhorn PVC-to-PVC volume clone**, refreshed hourly by a CronJob outside this process. A block-level clone copies the volume below SQLite's locking layer entirely — it doesn't care whether MA is mid-write, because it isn't going through SQLite at all. Confirmed live: cloning mid-write, then opening the result, gives `journal_mode=wal`, `integrity_check=ok`, and a correct track count — SQLite's own crash-recovery (the same mechanism that protects against a real power loss) discards whatever torn transaction tail was mid-flight at the instant of the clone. This service does its part of that: `db::recover_wal()` opens `MA_DB_PATH` once with normal (non-immutable) flags at startup to force that recovery, *then* builds the real `immutable=1` serving pool against the now-clean file. The clone itself, the CronJob that refreshes it, and the RBAC scoping it, are GitOps-managed infrastructure outside this repo.

A clone is a one-time copy, not a live mirror, so "refresh" means delete-and-recreate the PVC plus a pod restart to remount it — data can be up to ~1 hour stale. That's an explicit, accepted tradeoff for never reading a database that's actively being written.

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `MA_DB_PATH` | yes | — | Absolute path to the **clone** of `library.db` your deployment maintains (e.g. `/data-clone/library.db`) — not the live file MA writes to |
| `MA_MUSIC_ROOT` | yes | — | Absolute path to the music library root (e.g. `/music`) |
| `PORT` | no | `8096` | Port to listen on |
| `MA_BRIDGE_API_KEY` | no | — | If set, all requests require `Authorization: Bearer <key>` |
| `DB_POOL_SIZE` | no | `4` | SQLite connection pool size |
| `LOG_LEVEL` | no | `info` | `tracing` filter, e.g. `debug`, `info,tower_http=warn` |

## API

All endpoints are under `/api/v1`. Paginated endpoints return:

```json
{ "total": 37219, "offset": 0, "limit": 100, "items": [...] }
```

### `GET /api/v1/health`

Lightweight — no DB query. Used by k8s liveness/readiness/startup probes.

```bash
curl http://localhost:8096/api/v1/health
# {"status":"ok"}
```

### `GET /api/v1/health/detailed`

Full stats — DB query, not in the probe path.

```bash
curl http://localhost:8096/api/v1/health/detailed
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

**Coverage is not evenly distributed across the library yet.** MA's analysis providers process tracks roughly in scan order, which in practice tracks alphabetically by artist — as of writing, ~90% of analysed tracks (`bpm`/`clap`/`sonic` coverage) are artists starting with A, B, or C, with a steep drop-off after that. Any consumer doing random sampling restricted to analysed tracks (e.g. `energy_min`/`energy_max`/`valence_min` filters) will see this skew until the analysis pass progresses further into the alphabet — it is not a bug in this service's query logic, confirmed by checking the underlying `tracks`/`audio_analysis` tables directly.

---

### `GET /api/v1/tracks`

List tracks with optional filtering.

| Parameter | Type | Description |
|---|---|---|
| `offset` / `limit` | int | Pagination (limit clamped to 1–1000, default 100) |
| `since` | unix timestamp | Only tracks modified after this time |
| `favorite` | bool | Filter by starred/favourited status |
| `genre` | string | Matches any tag in the track's `genres` array, not just the first |
| `artist_id` | int | Filter to tracks by a specific artist |
| `album_id` | int | Filter to tracks on a specific album |
| `bpm_min` / `bpm_max` | float | BPM range |
| `energy_min` / `energy_max` | float | Energy 0–1 |
| `valence_min` / `valence_max` | float | Valence 0–1 |
| `arousal_min` / `arousal_max` | float | Arousal 0–1 |
| `order` | `name` \| `timestamp_added` \| `timestamp_modified` \| `random` | Sort column |
| `dir` | `asc` \| `desc` | Sort direction |
| `exclude` | comma-separated IDs | Exclude specific track IDs |
| `include` | `analysis` \| `analysis,scalar` \| `clap` \| `lyrics` | Add audio analysis fields (`scalar` omits array fields for a smaller payload); `clap` adds the 1024-dim embedding; `lyrics` adds full lyrics text — all independent, combine as needed (e.g. `analysis,lyrics`) |

```bash
# High-energy tracks modified in the last week, with analysis
curl "http://localhost:8096/api/v1/tracks?energy_min=0.7&since=1749600000&include=analysis&limit=20"
```

**`order=random` internals:** plain random order (no audio filters) and random order restricted to `energy`/`valence`/`arousal` filters both use a two-stage fast path — randomly sample matching `item_id`s from a lightweight index first, then fetch full rows for just those ids — instead of `ORDER BY RANDOM()` over the full multi-table join, which takes seconds on a large library. As of the most recent fix, the stage-2 row fetch is also itself randomly ordered (`GROUP BY item_id ORDER BY RANDOM()`); earlier versions sampled a genuinely random *set* of ids but returned them in ascending-id order, which looked alphabetically clustered to callers relying on response order for variety (e.g. a stable sort over near-tied scores). `order=random` combined with `bpm_min`/`bpm_max` falls through to a plain `ORDER BY RANDOM()` over the full join — slower, but correctly randomized today regardless.

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
  "genres": ["Electronic", "Ambient"],
  "popularity": null,
  "duration": 243.5,
  "file_path": "Artist/Album/01 Song.flac",
  "favorite": false,
  "timestamp_added": 1700000000,
  "timestamp_modified": 1700000000,
  "cover_url": "/api/v1/tracks/12345/cover",
  "analysis": null,
  "lyrics": null
}
```

`popularity` is extracted from `tracks.metadata.popularity` (top-level, not under `analysis` — it's a metadata-provider field, not an audio-analysis one). It's `null` for almost every track today since most metadata providers don't populate it, but a small and growing number now carry a real 0–100-ish score where a provider has supplied one — null-safe by design, consumers should never assume it's populated.

With `?include=analysis`:

```json
{
  "analysis": {
    "loudness_lufs": -10.4,
    "loudness_album_lufs": -11.2,
    "loudness_range": 6.2,
    "true_peak": -0.3,
    "bpm": 128.0,
    "key": "D#",
    "mode": "minor",
    "camelot": "2A",
    "beats": [0.23, 0.69, 1.16],
    "beats_per_bar": 4.0,
    "downbeats": [0.23, 1.16],
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
curl "http://localhost:8096/api/v1/tracks/12345?include=analysis"
```

---

### `GET /api/v1/tracks/:id/similar`

Find acoustically similar tracks using cosine similarity over CLAP embeddings. Returns nothing if the track has no CLAP vector yet.

| Parameter | Type | Description |
|---|---|---|
| `limit` | int | Number of results (1–50, default 10) |
| `exclude` | comma-separated IDs | Exclude these track IDs from results |

```bash
curl "http://localhost:8096/api/v1/tracks/12345/similar?limit=5&exclude=12345,99001"
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
curl -o cover.jpg http://localhost:8096/api/v1/tracks/12345/cover
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

Album object also carries `album_type` (`album`/`single`/`ep`/`compilation`), `label`, and `release_date`, read from `albums.metadata`.

### `GET /api/v1/albums/:id`

### `GET /api/v1/albums/:id/tracks`

Accepts the same filter/include parameters as `/tracks`. Defaults to physical disc/track order
(`disc_number`, `track_number`) when no `order` param is given — pass an explicit `order` to override.
Backed by `idx_album_tracks_album_id`, created at pod startup in `db::recover_wal` (same place
`track_audio_features` is rebuilt every boot), so this is an indexed lookup, not a full table scan.

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

Lists playlists (`id`, `name`, `timestamp_modified`) only. **There is no `/playlists/:id/tracks` endpoint** —
confirmed live against the cluster that MA stores playlist track membership in `.m3u`/JSON files on the mounted
volume, not in `library.db`. Exposing playlist contents would mean parsing those files, a different I/O model
than every other endpoint here (file parsing vs. SQL), and is out of scope for this service today.

---

### `GET /api/v1/genres`

MA's real genre taxonomy (`genres` + `genre_media_item_mapping` tables) — distinct from a track's `genres`
array, which is just the flat tag list copied out of `tracks.metadata`. Genres here carry alias rollups (e.g.
"ambient" aliases "Ambient Dub", "Kankyō Ongaku", "Space Ambient", etc.).

| Parameter | Type | Description |
|---|---|---|
| `offset` / `limit` | int | Pagination |

```json
{
  "id": 2,
  "name": "ambient",
  "description": null,
  "aliases": ["ambient", "Ambient Dub", "Kankyō Ongaku", "Space Ambient"],
  "track_count": 1840
}
```

### `GET /api/v1/genres/:id`

### `GET /api/v1/genres/:id/tracks`

Tracks tagged with this genre via the real `genre_media_item_mapping` join — not a string match against a
track's `genres` array. Standard `offset`/`limit` pagination.

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

The patch also creates a `ClusterIP` service (`ma-db-api.music-assistant:8096`) so other pods in the cluster can reach the bridge without going through a host port.

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

49 unit tests covering Camelot wheel conversion, the cosine similarity index, audio-feature flattening
(including the newly added `loudness_range`/`true_peak`/`beats_per_bar`/`downbeats`), track listing/filtering
(genre array membership, disc/track ordering, audio-scalar filters), lyrics opt-in gating, album metadata, the
genre taxonomy queries, search, and the `recover_wal` startup path's idempotency across simulated pod restarts.

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
- `sonic_analysis` (CLAP, valence, energy, danceability, acousticness, instrumentalness, brightness, speechiness, roughness, harmonic_complexity, rhythmic_regularity, spectral_centroid, loudness_range, true_peak) — subset; coverage shown in `/health`

The `/health` endpoint reports current coverage counts. Fields are `null` when a track hasn't been analysed yet.

**Coverage skews alphabetically by artist today.** Checked directly against the underlying `tracks`/`audio_analysis` tables: of all analysed artist-track rows, roughly 90% start with A, B, or C, with under 1% covering the rest of the alphabet combined. This is consistent with MA's analysis pass working through the library in something close to scan order rather than a random or priority order. It self-corrects as analysis continues — there's nothing to fix in this service — but any downstream consumer doing analysis-restricted random sampling (e.g. `energy_min`/`energy_max`, mood/vibe matching) should expect heavily A/B/C-skewed results until coverage broadens. This was the root cause of an apparent "random picks aren't really random" bug report from a downstream consumer (SUB/WAVE) — the query-level randomization was and is correct; the skew is entirely in which tracks have analysis to sample from.
