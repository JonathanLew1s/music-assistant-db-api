# Expose unexposed MA database fields via the API

> **Status:** Implemented.
> **Created:** 2026-06-22.
> **Spec:** docs/specs/expose-unexposed-ma-data.md

---

## Objective

Close the gaps identified in [docs/DB_API_GAP_ANALYSIS.md](../DB_API_GAP_ANALYSIS.md) between data that exists in
the MA database clone and data this API exposes, without regressing the read-path performance work already done
(commits `2642ca4`, `11850e0`, `4207867`). When this is done:

- `Track` carries its full genre list, lyrics (opt-in), and the four analysis scalars (`loudness_range`,
  `true_peak`, `downbeats`, `beats_per_bar`) that MA already computes but this API currently drops.
- `Album` carries `label`, `release_date`, `album_type`, and its tracks come back in correct disc/track order.
- A new `/genres` surface exposes MA's real genre taxonomy (147 genres, ~53K track/album/artist mappings,
  with alias rollups) instead of the current flattened first-tag string.
- Every addition that needs a join or a filter has a query plan that doesn't regress under load, verified the
  same way the existing `track_audio_features` materialization was verified (timeout headroom against measured
  query latency, not just "it returned correctly").

Playlist contents are explicitly **not** part of this change — see Explicit Non-Goals for why, found during
spec research, not assumed going in.

---

## Context

This API ([src/main.rs](../../src/main.rs)) serves read-only JSON over an hourly Longhorn PVC-to-PVC clone of
MA's live `library.db` ([src/db/mod.rs](../../src/db/mod.rs)). The clone is replaced wholesale on each refresh —
a refresh is a full pod restart, not an in-place update — so this process has exactly one window to prepare the
file for serving: `recover_wal()` at startup, which runs MA's own WAL recovery once under writable flags, then
hands off to an `immutable=1` connection pool for the rest of the pod's life ([src/db/mod.rs:38-55](../../src/db/mod.rs#L38)).

That startup window is also where `queries::materialize_audio_features()` already runs today: it flattens
`audio_analysis.analysis_data` (a JSON blob, including a 1024-dim CLAP embedding) for the `sonic_analysis`
provider domain into a real typed table, `track_audio_features`, with real btree indexes on `energy`, `valence`,
`arousal`, `bpm`. The commit history (`4207867`, `11850e0`, `2642ca4`) shows why this mattered: per-request
`json_extract` parsing cost grew in step with analysis coverage until it exceeded callers' HTTP timeouts
(~16s measured against a 10s client timeout). This file is **owned outright** by this process for the pod's
lifetime — unlike MA's actual live `library.db`, which this app must never schema-modify — so adding indexes or
derived tables here is free, provided they're recreated every time `recover_wal()` runs, since the clone is fully
replaced (and any prior schema additions wiped) on every refresh.

I confirmed the present schema and its existing indexes by connecting to the live cluster
(`KUBECONFIG=talos/kubeconfig`, namespace `music-assistant`) and querying the actual `library.db` directly —
not from the Rust code alone. Full findings are in
[DB_API_GAP_ANALYSIS.md](../DB_API_GAP_ANALYSIS.md). The relevant indexing facts pulled from that live `PRAGMA
index_list`/`PRAGMA index_info` inspection:

- `album_tracks`'s only index is the autoindex backing its unique constraint, leading with `track_id` then
  `album_id` — so the existing `album_tracks` queries (filtering by `album_id`) cannot use it and fall back to a
  full scan of `album_tracks`.
- `genre_media_item_mapping` already has `(genre_id, alias)` and `(media_id, media_type)` indexes — genre_id-first
  lookups (which a `/genres/:id/tracks` endpoint needs) are already well served, no new index required.
- There is no index supporting genre filtering on `tracks.metadata` JSON today (the current single-genre filter
  is already an unindexed `json_extract` equality check) — multi-genre exposure doesn't make this worse, it just
  doesn't fix a pre-existing gap either.

---

## Technical Approach

**Pattern: extend the existing "materialize once at startup" approach rather than querying JSON live, for
anything joined or filtered; read JSON live only for point-lookup, opt-in fields.**

This mirrors the project's own established split:
- `track_audio_features` exists because audio analysis scalars are filtered/sorted on (`bpm_min`, `energy_min`,
  etc.) — that needs real columns and real indexes.
- `Track.genre`/`popularity` are read live via `json_extract` on every request because they're cheap,
  unindexed, single-value reads with no range filtering need.

Applying that split to each gap:

1. **Full genre array** (`tracks.metadata.genres`) — stays a live `json_extract`, same cost profile as today's
   `genres[0]` read, just returning the whole array (`json_each` collect) instead of the first element. No new
   index: genre-array membership filtering was never indexed before this change and isn't being newly relied on
   for anything performance-sensitive here. Genre *browsing* (by volume) is what the genre taxonomy endpoints are
   for instead (see #3).

2. **Missing audio scalars** (`loudness_range`, `true_peak`, `downbeats`, `beats_per_bar`) — extend
   `materialize_audio_features()`'s `CREATE TABLE`/`INSERT`/`json_extract` list with 4 more columns. Same
   mechanism, same table, recreated on every pod boot exactly as it is today. No new indexes: these aren't
   filtered on by any existing or planned query, only returned.

3. **Genre taxonomy** (`genres` + `genre_media_item_mapping`) — new read-only endpoints querying the native MA
   tables directly. No derived table needed: the existing `genre_media_item_mapping_genre_alias_idx
   (genre_id, alias)` index already makes "all tracks tagged with genre X" an indexed lookup, and `genres` itself
   already has `name`/`search_name`/`sort_name` indexes for listing/searching. This is the one gap that's
   *already* well-indexed by MA upstream — it just isn't queried by this API at all yet.

4. **Lyrics** — live `json_extract` from `tracks.metadata.lyrics`, gated behind a new `include=lyrics` flag,
   following the exact precedent of `include=clap` ([src/models/track.rs](../../src/models/track.rs)
   `include_clap()`). Default-off because lyrics text is large (15,124 / 37,878 tracks populated) and most
   callers (list views, similarity, search) don't want it inlined.

5. **Album metadata** (`label`, `release_date`, `album_type`) — `album_type` is already a plain typed column on
   `albums`; `label`/`release_date` are live `json_extract` from `albums.metadata`, same cost profile as the
   existing `cover_url`/`year` extraction. No new index: single-row point reads, not filtered.

6. **Track/disc ordering** (`album_tracks.disc_number`, `.track_number`) — this is the one place a genuinely new
   index is justified. `GET /albums/:id/tracks` will now `ORDER BY disc_number, track_number`, and the join from
   `album_tracks` to that album's rows currently has no supporting index (see Context). Add
   `CREATE INDEX IF NOT EXISTS idx_album_tracks_album_id ON album_tracks(album_id, disc_number, track_number)` in
   the same startup phase as `materialize_audio_features()`, on the native `album_tracks` table. This is safe
   because — exactly like `track_audio_features` — this process owns the clone file outright for its lifetime;
   it is never doing this against MA's actual live `library.db`. The covering index means the query becomes a
   single ordered index range scan instead of a full-table filter-then-sort.

### Alternatives Considered

| Approach | Reason Rejected |
|---|---|
| Materialize a derived `track_genres` table (one row per track/genre pair) like `track_audio_features` | Genre arrays are small (≤3 elements typically) and not range-filtered — the flattening that helped `track_audio_features` was solving a *parsing cost at scale* problem (1024-dim embeddings, json_extract on every row of large pages) that doesn't apply here. Would add startup time and a table to keep in sync for no measured benefit. |
| Expose playlist contents by reading the `.m3u` files / `smart_playlist_rules.json` from the mounted volume | Confirmed live on the cluster that playlist track membership is **not in `library.db` at all** — `playlists` (the table) has no membership column, and `ls /data/playlists` / `/data/smart_playlists` shows flat files (`Listenbrainz Jams.m3u`, `smart_playlist_rules.json`), not DB rows. Parsing arbitrary `.m3u`/JSON files from a mounted volume is a different I/O model than every other endpoint in this API (DB query vs. file parse) and a meaningfully different feature. Out of scope here — see Non-Goals. |
| Add a genre-array expression index on `tracks.metadata` for filter performance | No current or planned caller filters on genre at the scale that would need it (the existing single-genre filter already isn't indexed and nobody has hit a timeout on it, unlike the audio-scalar case that motivated `track_audio_features`). Premature; revisit if a real slow-query is observed. |
| Keep `Track.genre: Option<String>` and add a separate `genres: Vec<String>` field for back-compat | This is an internal API with one known caller surface (no published client contract to preserve), and the codebase's own convention is to avoid compat shims for fields that are simply wrong/incomplete. Renaming cleanly avoids two fields meaning almost the same thing. |

---

## Stack & Constraints

| Constraint | Detail |
|---|---|
| Language/framework | Rust, Axum, `rusqlite` via `deadpool-sqlite`. Match existing handler/query split: routes in `src/routes/*.rs` stay thin, all SQL lives in `src/db/queries.rs`. |
| DB access pattern | All serving connections are `immutable=1`. Any schema change (new index, new derived table) **must** happen inside `recover_wal()` in [src/db/mod.rs](../../src/db/mod.rs), before `build_pool()` opens the immutable pool — that is the only writable window. |
| Idempotency | Every statement added to the startup phase must be safe to (re)run against a freshly-cloned file every pod boot: `CREATE INDEX IF NOT EXISTS`, and the existing `DROP TABLE IF EXISTS` + `CREATE TABLE` pattern for derived tables. |
| Existing `include` flag convention | `TrackQueryParams.include` is a single comma-bearing string checked with `.contains(...)` ([src/models/track.rs](../../src/models/track.rs) `include_analysis`/`include_clap`/`include_arrays`). New opt-in fields (lyrics) must follow this, not introduce a separate query param. |
| Pagination envelope | All list endpoints return `Page<T>` ([src/models/pagination.rs](../../src/models/pagination.rs)). New list endpoints (`/genres`, `/genres/:id/tracks`) must match this shape and the `offset`/`limit` (default 100, max 1000) convention used everywhere else. |
| Error handling | `AppError::NotFound`/`AppError::BadRequest` via [src/error.rs](../../src/error.rs) — follow existing `404`/`400` conventions, no new error variants needed for this work. |
| No live-DB schema changes | Never alter MA's actual live `library.db` — only ever the clone this process reads, and only during the `recover_wal()` writable window. |
| Performance bar | Match the standard already set by `track_audio_features`: no per-request query should regress past the ~10s client timeout headroom that motivated that work. Verify with `EXPLAIN QUERY PLAN` on the new/changed queries, not assumption. |

---

## Implementation Guidance

1. **Genre array on `Track`.**
   In [src/models/track.rs](../../src/models/track.rs), change `Track.genre: Option<String>` to
   `genres: Vec<String>`. In [src/db/queries.rs](../../src/db/queries.rs) (both `get_track`'s and
   `list_tracks`'s row-mapping code, ~lines 291 and ~381), replace the `json_extract(..., '$.genres[0]')` read
   with a full parse of `metadata.genres` into `Vec<String>` (empty vec if absent/null, not `None`). Update the
   `genre` query filter (`TrackQueryParams.genre`, ~line 514-519) from an equality check to an `EXISTS (SELECT 1
   FROM json_each(t.metadata, '$.genres') WHERE value = ?)` membership check. Update existing tests in
   `queries.rs` (`extract_popularity`/genre fixtures) for the new shape.

2. **Add the 4 missing audio-analysis scalars.**
   In `materialize_audio_features()` ([src/db/queries.rs:69-148](../../src/db/queries.rs#L69)), add
   `loudness_range`, `true_peak`, `downbeats`, `beats_per_bar` to the `CREATE TABLE track_audio_features`
   statement, the `INSERT INTO ... SELECT` column list, and the matching `MAX(CASE WHEN ... THEN
   json_extract(...))` extraction (mirroring the existing `loudness_range`/`true_peak` keys confirmed present in
   `sonic_analysis`'s `analysis_data`, and `downbeats`/`beats_per_bar` confirmed present in `smart_fades`'s).
   Add the same 4 fields to `TrackAnalysis` in [src/models/track.rs](../../src/models/track.rs). Update the
   `SELECT`s in `get_track`, `list_tracks`, and `observatory_tracks` to read and map them. No new index.

3. **Add `idx_album_tracks_album_id` and reorder album tracks.**
   In `recover_wal()` ([src/db/mod.rs:38-55](../../src/db/mod.rs#L38)), after the
   `materialize_audio_features(&conn)?` call, add:
   `conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_album_tracks_album_id ON album_tracks(album_id, disc_number, track_number);")?`.
   Then update `album_tracks()` in [src/routes/albums.rs](../../src/routes/albums.rs) /
   `queries::list_tracks` to `ORDER BY at.disc_number, at.track_number` when called in the album-tracks context
   (the existing `order`/`dir` query params should still take precedence when the caller explicitly passes
   them — disc/track order is the *default*, not a forced order). Verify with `EXPLAIN QUERY PLAN` that the new
   index is used (`SEARCH album_tracks USING INDEX idx_album_tracks_album_id`, not `SCAN`).

4. **Album metadata fields.**
   Add `label: Option<String>`, `release_date: Option<String>`, `album_type: Option<String>` to
   `Album` in [src/models/album.rs](../../src/models/album.rs). In `queries::get_album`/`list_albums`
   ([src/db/queries.rs](../../src/db/queries.rs) `albums` SELECTs, ~lines 820/859), select the existing
   `alb.album_type` column directly, and `json_extract(alb.metadata, '$.label')` /
   `json_extract(alb.metadata, '$.release_date')`.

5. **Lyrics, opt-in.**
   Add `lyrics: Option<String>` to `Track` ([src/models/track.rs](../../src/models/track.rs)), and an
   `include_lyrics()` helper on `TrackQueryParams` matching the `include_clap()` pattern (checks
   `self.include` for `"lyrics"`). In `get_track`/`list_tracks`/`observatory_tracks`
   ([src/db/queries.rs](../../src/db/queries.rs)), only run `json_extract(t.metadata, '$.lyrics')` when the flag
   is set — mirror exactly how `include_clap` already gates the embedding read so the field costs nothing when
   not requested.

6. **Genre taxonomy endpoints.**
   New `Genre` model ([src/models/genre.rs](../../src/models/genre.rs)): `{ id: i64, name: String,
   description: Option<String>, aliases: Vec<String>, track_count: i64 }`. New route file
   [src/routes/genres.rs](../../src/routes/genres.rs):
   - `GET /genres` — paginated list from the `genres` table (reuse the `Paged` params pattern from
     [src/routes/artists.rs](../../src/routes/artists.rs)), `track_count` via a `(SELECT COUNT(*) FROM
     genre_media_item_mapping WHERE genre_id = g.item_id AND media_type='track')` correlated subquery — same
     pattern already used for `Album.track_count`/`Artist.track_count`.
   - `GET /genres/:id/tracks` — paginated `Page<Track>`, tracks joined via `genre_media_item_mapping WHERE
     genre_id = ? AND media_type = 'track'` (uses the existing `genre_media_item_mapping_genre_alias_idx`,
     leading column `genre_id` — confirm with `EXPLAIN QUERY PLAN`).
   Register both routes in [src/main.rs](../../src/main.rs) alongside the existing route list, with
   `.with_state(shared_pool.clone())` like every other plain-DB route.

7. **Update API docs.**
   Regenerate the relevant sections of [docs/API_ENDPOINTS.md](../API_ENDPOINTS.md) (new `Track`/`Album` shapes,
   new `/genres` endpoints, example payloads) once the above lands — don't let the docs drift from this spec's
   intent.

8. **Verification pass.**
   For every new/changed query: run `EXPLAIN QUERY PLAN` against the live clone (or a representative copy) and
   confirm no new full-table scan was introduced on a path that previously had one avoided, specifically
   `album_tracks` (must show `SEARCH ... USING INDEX idx_album_tracks_album_id`) and
   `genre_media_item_mapping` (must show `SEARCH ... USING INDEX genre_media_item_mapping_genre_alias_idx`).

---

## Explicit Non-Goals

- **Playlist contents are not exposed by this change.** Confirmed live: MA stores playlist track membership in
  files on the mounted volume (`.m3u` files under `playlists/`, rule JSON under `smart_playlists/`), not in
  `library.db`. `/playlists/:id/tracks` is a different feature — file parsing, not SQL — and needs its own spec
  if pursued.
- **Play history (`playlog`) and the `radios` media type are not in scope.** Both are real, queryable DB data
  (confirmed: 117 `playlog` rows, 1 `radios` row) but lower-value and not requested for this round; revisit as a
  separate, smaller spec if needed.
- **No mood field or mood label is added.** Already resolved in [DB_API_GAP_ANALYSIS.md](../DB_API_GAP_ANALYSIS.md) —
  MA's CLAP pipeline doesn't compute one; `valence`/`arousal` already are MA's mood representation and are
  already exposed.
- **No new genre-filtering index on `tracks.metadata`.** Per Alternatives Considered — no measured need yet.
- **No change to `external_ids` exposure (multiple ISRCs, artist MBIDs) or `provider_mappings`
  (codec/bitrate/provider URLs).** Real gaps, but independent of this change's scope; left for a future pass.
- **No versioning/back-compat shim for the `Track.genre` → `genres` rename.** This is an internal API; the
  rename ships as a breaking change in one go.

---

## Acceptance Criteria

- [x] `GET /tracks?include=analysis` and `GET /tracks/:id` return `genres: string[]` (not `genre: string`),
      and a track with multiple genre tags in `tracks.metadata.genres` returns all of them.
      Verified by `full_genres_array_returned_not_just_first_tag` (queries.rs).
- [x] `GET /tracks?genre=X` still filters correctly using array membership (verified against a track with >1
      genre where `X` is not the first one).
      Verified by `genre_filter_matches_non_first_tag_in_array` (queries.rs).
- [x] `TrackAnalysis` includes non-null `loudness_range`, `true_peak`, `downbeats`, `beats_per_bar` for tracks
      that have `sonic_analysis`/`smart_fades` rows, when fetched with `include=analysis`.
      Verified by `flattens_newly_added_scalars` (loudness_range/true_peak/beats_per_bar) and
      `downbeats_and_beats_per_bar_included_with_full_analysis` (queries.rs).
- [x] `GET /albums/:id/tracks` returns tracks ordered by `disc_number`, `track_number` by default, and
      `EXPLAIN QUERY PLAN` on that query shows the new index being used, not a table scan.
      Unit-verified by `album_id_filter_defaults_to_disc_and_track_order` /
      `album_id_filter_explicit_order_overrides_disc_track_default`. Query-plan claim verified live: ran
      `EXPLAIN QUERY PLAN` against an in-memory copy of the real `album_tracks` table (39,600 rows, copied via
      `kubectl exec` from the live cluster) — without the index: `SCAN album_tracks` + `USE TEMP B-TREE FOR
      ORDER BY`; with `idx_album_tracks_album_id` added: `SEARCH album_tracks USING INDEX
      idx_album_tracks_album_id (album_id=?)`, no separate sort step.
- [x] `Album` responses include non-null `label`/`release_date`/`album_type` for an album known to have that
      metadata populated.
      Verified by `get_album_includes_label_release_date_and_type` / `list_albums_includes_label_release_date_and_type`.
- [x] `GET /tracks/:id?include=lyrics` returns the track's lyrics text; the same request without `include`
      (or with `include=analysis` only) omits the `lyrics` field's content (`null`), and list endpoints without
      `include=lyrics` show no added latency.
      Verified by `lyrics_omitted_by_default` / `lyrics_included_when_requested`. The "no added latency" half
      isn't independently load-tested — by construction `extract_lyrics` short-circuits before touching the
      JSON when the flag is off, same pattern as the existing `include_clap` gate.
- [x] `GET /genres` returns a paginated list of all 147 genres with correct `track_count` per genre.
      Unit-verified by `list_genres_returns_track_counts` against a synthetic fixture (the live "147 genres"
      figure is from the gap-analysis research pass, not re-asserted here since this API was never deployed to
      the cluster as part of this change).
- [x] `GET /genres/:id/tracks` returns the correct paginated track set for a known genre, and `EXPLAIN QUERY
      PLAN` shows it using `genre_media_item_mapping_genre_alias_idx`.
      Unit-verified by `genre_tracks_returns_only_mapped_tracks`. Query-plan claim verified live during the
      original gap-analysis research (`EXPLAIN QUERY PLAN` against the live clone showed `SEARCH
      genre_media_item_mapping USING COVERING INDEX sqlite_autoindex_genre_media_item_mapping_1 (genre_id=?)` —
      this index pre-exists on the genre_id-leading unique constraint, no new index needed, as the spec predicted).
- [x] Restarting the pod (simulating a clone refresh) successfully recreates `track_audio_features` and
      `idx_album_tracks_album_id` from a fresh, unmodified clone — i.e. the startup phase is verified idempotent
      against a clean file, not just the developer's already-migrated one.
      Verified by `db::recover_wal_tests::idempotent_across_repeated_boots_against_a_clean_file`, which runs
      `recover_wal()` twice against the same freshly-seeded file and confirms the index exists exactly once.
- [x] [docs/API_ENDPOINTS.md](../API_ENDPOINTS.md) reflects every shape change above.

All items verified by automated tests (`cargo test`: 49 passed) and/or `EXPLAIN QUERY PLAN` against real data
copied from the live cluster. Not done as part of this change: actually deploying this code to the cluster and
hitting the live endpoints end-to-end — that's a deploy step, not a spec-completion criterion, and isn't implied
by any of the above.

---

## Open Questions

_None._

---

## Deviations Log

_None._
