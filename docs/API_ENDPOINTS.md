# API Endpoints

All routes are mounted under the prefix `/api/v1`. If `API_KEY` is configured (see [config.rs](../src/config.rs)),
every request must include `Authorization: Bearer <key>`, or it gets `401` with `{ "error": "invalid or missing API key" }`.

Pagination responses share a common envelope (`Page<T>`):

```json
{ "total": 12345, "offset": 0, "limit": 100, "items": [ ... ] }
```

> See [DB_API_GAP_ANALYSIS.md](DB_API_GAP_ANALYSIS.md) for a data-verified comparison of what's in the underlying MA database vs. what this API exposes (lyrics, genre taxonomy, playlist contents, loudness/beat-grid fields, etc. are all unexposed gaps; `mood` turned out to be a red herring — see that doc).

---

## Health

### `GET /health`
Liveness probe — runs a trivial `SELECT COUNT(*) FROM tracks` against the snapshot DB.

- `200` → `{ "status": "ok" }`
- `500` → `{ "status": "error", "error": "<message>" }`

**Example — `GET /health`:**
```json
{ "status": "ok" }
```

### `GET /health/detailed`
Operational stats, not used by the k8s probe.

**Example — `GET /health/detailed`:**
```json
{
  "status": "ok",
  "db_schema_version": 7,
  "track_count": 38214,
  "analysis_coverage": {
    "loudness": 37980,
    "bpm": 37980,
    "clap": 36510,
    "sonic": 37980
  }
}
```

---

## Tracks

### `GET /tracks`
Paginated track list with filtering.

Query params:
| Param | Type | Notes |
|---|---|---|
| `offset` | int | default `0` |
| `limit` | int | default `100`, max `1000` (or `10000` if `include` contains `scalar`) |
| `since` | int | filter by `timestamp_added`/`timestamp_modified` cursor |
| `include` | string | comma-ish flags: `analysis`, `clap`, `scalar`, `lyrics` (see below) |
| `favorite` | bool | filter to favorited tracks |
| `genre` | string | matches any tag in the track's `genres` array, not just the first |
| `artist_id` | int | filter by artist |
| `album_id` | int | filter by album |
| `bpm_min`/`bpm_max` | float | BPM range |
| `energy_min`/`energy_max` | float | energy range |
| `valence_min`/`valence_max` | float | valence range |
| `arousal_min`/`arousal_max` | float | arousal range |
| `order` | string | sort column |
| `dir` | string | `asc`/`desc` |
| `exclude` | string | comma-separated track IDs to exclude |

`include` flags:
- `analysis` → adds the full `analysis` object, including array fields (`beats`, `downbeats`, `rms_energy`)
- `analysis,scalar` → adds `analysis` but **omits** array fields (much smaller payload)
- `clap` (with `analysis`) → also includes the 1024-dim `clap_embedding` array
- `lyrics` → adds the `lyrics` field (full track lyrics text, ~40% of tracks have it). Independent of `analysis` — combine as needed, e.g. `include=analysis,lyrics`.

Returns `Page<Track>` (see [Track shape](#track-shape) below).

**Example — `GET /tracks?limit=1&include=analysis,lyrics`:**
```json
{
  "total": 38214,
  "offset": 0,
  "limit": 1,
  "items": [
    {
      "id": 4821,
      "title": "Gemini",
      "artist": "Boards of Canada",
      "artists": ["Boards of Canada"],
      "album": "Geogaddi",
      "album_id": 312,
      "year": 2002,
      "genres": ["Electronic", "Ambient"],
      "popularity": 0.62,
      "duration": 354.2,
      "file_path": "Boards of Canada/Geogaddi/02 Gemini.flac",
      "favorite": true,
      "timestamp_added": 1700000000,
      "timestamp_modified": 1700000400,
      "cover_url": "/api/v1/tracks/4821/cover",
      "lyrics": null,
      "analysis": {
        "loudness_lufs": -11.2,
        "loudness_album_lufs": -10.8,
        "bpm": 88.0,
        "key": "A",
        "mode": "minor",
        "camelot": "8A",
        "beats": [0.42, 1.10, 1.78, 2.46],
        "beats_per_bar": 4.0,
        "downbeats": [0.42, 2.46],
        "valence": 0.31,
        "energy": 0.44,
        "danceability": 0.39,
        "arousal": 0.36,
        "acousticness": 0.18,
        "instrumentalness": 0.92,
        "brightness": 0.27,
        "speechiness": 0.03,
        "roughness": 0.21,
        "harmonic_complexity": 0.58,
        "rhythmic_regularity": 0.71,
        "spectral_centroid": 1840.5,
        "loudness_range": 6.2,
        "true_peak": -0.3,
        "rms_energy": [0.11, 0.14, 0.13, 0.18],
        "mbid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "isrc": "GBARL0200304",
        "clap_embedding": null
      }
    }
  ]
}
```

### `GET /tracks/observatory`
Bulk fetch of every track that has `sonic_analysis` data, optimized for a full-corpus dump (drives off `track_audio_features`, not the full `tracks` table). Always includes full analysis. Cached indefinitely in-process (cache cleared by hourly pod restarts when the DB clone refreshes).

```json
{ "total": 36500, "items": [ /* Track[], same shape as above, always with full analysis */ ] }
```

### `GET /tracks/:id`
Single track by ID. Same `include` query semantics as `GET /tracks`. `404` if not found.

Returns a `Track` object.

**Example — `GET /tracks/4821` (no `include`):**
```json
{
  "id": 4821,
  "title": "Gemini",
  "artist": "Boards of Canada",
  "artists": ["Boards of Canada"],
  "album": "Geogaddi",
  "album_id": 312,
  "year": 2002,
  "genres": ["Electronic", "Ambient"],
  "popularity": 0.62,
  "duration": 354.2,
  "file_path": "Boards of Canada/Geogaddi/02 Gemini.flac",
  "favorite": true,
  "timestamp_added": 1700000000,
  "timestamp_modified": 1700000400,
  "cover_url": "/api/v1/tracks/4821/cover",
  "lyrics": null,
  "analysis": null
}
```

### `GET /tracks/:id/similar`
Nearest-neighbor lookup using an in-memory CLAP-embedding similarity index (built at startup from `track_audio_features.clap_embedding`).

Query params:
| Param | Type | Notes |
|---|---|---|
| `limit` | int | default `10`, max `50` |
| `exclude` | string | comma-separated track IDs to exclude from results |

**Example — `GET /tracks/4821/similar?limit=3`:**
```json
{
  "source_id": 4821,
  "results": [
    { "id": 5678, "score": 0.91 },
    { "id": 9012, "score": 0.87 },
    { "id": 3344, "score": 0.83 }
  ]
}
```

### `GET /tracks/:id/cover`
Returns the embedded cover art image (extracted from the track's audio file tags via `lofty`), cached in an LRU. `Content-Type` matches the embedded picture's MIME type (defaults to `image/jpeg`). `404` (empty body) if no file path or no embedded picture.

---

## Albums

### `GET /albums`
Paginated album list.

Query params: `offset`, `limit` (default `100`, max `1000`), `since`, `order`, `dir`, `artist_id`.

Returns `Page<Album>` (see [Album shape](#album-shape)).

**Example — `GET /albums?limit=1`:**
```json
{
  "total": 4012,
  "offset": 0,
  "limit": 1,
  "items": [
    {
      "id": 312,
      "name": "Geogaddi",
      "artist": "Boards of Canada",
      "artist_id": 88,
      "year": 2002,
      "track_count": 23,
      "timestamp_added": 1699000000,
      "cover_url": "/api/v1/albums/312/cover",
      "album_type": "album",
      "label": "Warp Records",
      "release_date": "2002-02-18"
    }
  ]
}
```

### `GET /albums/:id`
Single album by ID. `404` if not found. Returns an `Album` object.

**Example — `GET /albums/312`:**
```json
{
  "id": 312,
  "name": "Geogaddi",
  "artist": "Boards of Canada",
  "artist_id": 88,
  "year": 2002,
  "track_count": 23,
  "timestamp_added": 1699000000,
  "cover_url": "/api/v1/albums/312/cover",
  "album_type": "album",
  "label": "Warp Records",
  "release_date": "2002-02-18"
}
```

### `GET /albums/:id/tracks`
Tracks belonging to the album — delegates to the same track listing/filtering as `GET /tracks`, with `album_id` pinned. Accepts the same query params as `GET /tracks` (`include`, `bpm_min`, etc.).

Defaults to physical disc/track order (`disc_number`, `track_number`) when no `order` param is given — pass an explicit `order` (e.g. `order=name`) to override. Backed by `idx_album_tracks_album_id`, created at pod startup alongside `track_audio_features` (see [src/db/mod.rs](../src/db/mod.rs) `recover_wal`), so this is an indexed lookup, not a full scan.

Returns `Page<Track>`.

### `GET /albums/:id/cover`
Cover art for the album, derived from the first track file in the album. Same caching/response semantics as `/tracks/:id/cover`.

---

## Artists

### `GET /artists`
Paginated artist list.

Query params: `offset`, `limit` (default `100`, max `1000`).

Returns `Page<Artist>` (see [Artist shape](#artist-shape)).

**Example — `GET /artists?limit=1`:**
```json
{
  "total": 1840,
  "offset": 0,
  "limit": 1,
  "items": [
    { "id": 88, "name": "Boards of Canada", "track_count": 142, "album_count": 7 }
  ]
}
```

### `GET /artists/:id`
Single artist by ID. `404` if not found. Returns an `Artist` object.

**Example — `GET /artists/88`:**
```json
{ "id": 88, "name": "Boards of Canada", "track_count": 142, "album_count": 7 }
```

### `GET /artists/:id/tracks`
Tracks by the artist — same semantics as `/albums/:id/tracks`, with `artist_id` pinned.

Returns `Page<Track>`.

---

## Playlists

### `GET /playlists`
Paginated playlist list.

Query params: `offset`, `limit` (default `100`, max `1000`).

Returns `Page<Playlist>` (see [Playlist shape](#playlist-shape)).

**Example — `GET /playlists?limit=1`:**
```json
{
  "total": 56,
  "offset": 0,
  "limit": 1,
  "items": [
    { "id": 7, "name": "Late Night Coding", "timestamp_modified": 1716400000 }
  ]
}
```

---

## Genres

MA's real genre taxonomy (`genres` + `genre_media_item_mapping` tables) — distinct from `Track.genres`, which is
just the flat tag array copied out of `tracks.metadata`. Genres here have alias rollups (e.g. "ambient" aliases
"Ambient Dub", "Kankyō Ongaku", "Space Ambient", etc.) and a real track-membership join, already indexed
upstream by MA (`genre_media_item_mapping_genre_alias_idx`, leading column `genre_id`) — no new index was needed
for these endpoints.

### `GET /genres`
Paginated list of all genres.

Query params: `offset`, `limit` (default `100`, max `1000`).

**Example — `GET /genres?limit=1`:**
```json
{
  "total": 147,
  "offset": 0,
  "limit": 1,
  "items": [
    {
      "id": 2,
      "name": "ambient",
      "description": null,
      "aliases": ["ambient", "Ambient Dub", "Kankyō Ongaku", "Space Ambient"],
      "track_count": 1840
    }
  ]
}
```

### `GET /genres/:id`
Single genre by ID. `404` if not found.

**Example — `GET /genres/2`:**
```json
{
  "id": 2,
  "name": "ambient",
  "description": null,
  "aliases": ["ambient", "Ambient Dub", "Kankyō Ongaku", "Space Ambient"],
  "track_count": 1840
}
```

### `GET /genres/:id/tracks`
Tracks tagged with this genre via the real `genre_media_item_mapping` join — not a string match against
`Track.genres`. Accepts standard `offset`/`limit` pagination.

Returns `Page<Track>`.

---

## Search

### `GET /search`
Cross-entity search (LRU-cached, no TTL — invalidated only by the hourly pod restart on DB refresh).

Query params:
| Param | Type | Notes |
|---|---|---|
| `q` | string | **required**, non-empty |
| `limit` | int | default `10`, max `50`, applies per entity type |
| `types` | string | comma-separated subset of `tracks,albums,artists`; defaults to all three |

**Example — `GET /search?q=boards&types=artists,albums&limit=2`:**
```json
{
  "tracks": [],
  "albums": [
    {
      "id": 312,
      "name": "Geogaddi",
      "artist": "Boards of Canada",
      "artist_id": 88,
      "year": 2002,
      "track_count": 23,
      "timestamp_added": 1699000000,
      "cover_url": "/api/v1/albums/312/cover",
      "album_type": "album",
      "label": "Warp Records",
      "release_date": "2002-02-18"
    }
  ],
  "artists": [
    { "id": 88, "name": "Boards of Canada", "track_count": 142, "album_count": 7 }
  ]
}
```

---

## Data shapes

### Track shape

```ts
{
  id: number,
  title: string | null,
  artist: string | null,
  artists: string[],
  album: string | null,
  album_id: number | null,
  year: number | null,
  genres: string[],               // full tag array — filter with ?genre=X (matches any element)
  popularity: number | null,
  duration: number | null,        // seconds
  file_path: string | null,
  favorite: boolean | null,
  timestamp_added: number | null,
  timestamp_modified: number | null,
  cover_url: string,
  analysis: TrackAnalysis | null, // present only when ?include=analysis[,scalar][,clap]
  lyrics: string | null           // present only when ?include=lyrics
}
```

`TrackAnalysis` (audio analysis, flattened from `track_audio_features`):

```ts
{
  loudness_lufs: number | null,
  loudness_album_lufs: number | null,
  loudness_range: number | null,
  true_peak: number | null,
  bpm: number | null,
  key: string | null,
  mode: string | null,
  camelot: string | null,
  beats: number[] | null,          // omitted when include contains "scalar"
  beats_per_bar: number | null,
  downbeats: number[] | null,      // omitted when include contains "scalar"
  valence: number | null,
  energy: number | null,
  danceability: number | null,
  arousal: number | null,
  acousticness: number | null,
  instrumentalness: number | null,
  brightness: number | null,
  speechiness: number | null,
  roughness: number | null,
  harmonic_complexity: number | null,
  rhythmic_regularity: number | null,
  spectral_centroid: number | null,
  rms_energy: number[] | null,     // omitted when include contains "scalar"
  mbid: string | null,
  isrc: string | null,
  clap_embedding: number[] | null  // 1024-dim, only when include=analysis,clap
}
```

### Album shape

```ts
{
  id: number,
  name: string | null,
  artist: string | null,
  artist_id: number | null,
  year: number | null,
  track_count: number,
  timestamp_added: number | null,
  cover_url: string,
  album_type: string | null,      // e.g. "album", "single", "ep", "compilation"
  label: string | null,
  release_date: string | null
}
```

### Artist shape

```ts
{
  id: number,
  name: string | null,
  track_count: number,
  album_count: number
}
```

### Playlist shape

```ts
{
  id: number,
  name: string | null,
  timestamp_modified: number | null
}
```

### Genre shape

```ts
{
  id: number,
  name: string | null,
  description: string | null,
  aliases: string[],
  track_count: number
}
```

---

## Known gaps

See [DB_API_GAP_ANALYSIS.md](DB_API_GAP_ANALYSIS.md) for the original data-verified comparison (queried directly
against the live MA database, plus a read of the [music-assistant/server](https://github.com/music-assistant/server)
source) of what the underlying schema has that this API didn't expose. Most of it has since been closed — see
[docs/specs/expose-unexposed-ma-data.md](specs/expose-unexposed-ma-data.md) for what shipped: full `genres` array,
`loudness_range`/`true_peak`/`beats_per_bar`/`downbeats`, opt-in `lyrics`, album `label`/`release_date`/`album_type`,
default disc/track ordering on `/albums/:id/tracks`, and the `/genres` taxonomy endpoints.

Still not exposed, by design (see that spec's Non-Goals): **playlist contents** (MA stores playlist track
membership in `.m3u`/JSON files on disk, not in `library.db` — a different feature, file parsing rather than SQL),
**play history** (`playlog`, 117 rows) and the **`radios`** media type (both real but lower-value, deferred), and
multiple ISRCs / artist MBIDs from `external_ids` / provider codec details from `provider_mappings`.

`mood` specifically was never a real gap: MA's CLAP pipeline never computes a `mood` field — it documents
`valence`+`arousal` (plus `instrumentalness`/`acousticness`) *as* its mood representation, and those are already
fully exposed via `TrackAnalysis`. The free-text `mood` tag in track/album metadata is a separate, unrelated
field sourced from external tag providers, populated for only 12 of ~37,878 tracks.
