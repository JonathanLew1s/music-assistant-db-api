# Gap Analysis: MA database vs. exposed API

Method: connected to the live cluster (`KUBECONFIG=/Users/jonathan/code/talos/kubeconfig`, namespace `music-assistant`),
shelled into the `music-assistant` pod (which has Python/sqlite3 against the same `library.db` the API reads a clone of),
and queried `PRAGMA table_info`, row samples, and `json_extract` counts directly — not inferred from code alone.

Schema observed: `tracks`, `albums`, `artists`, `playlists`, `genres`, `radios`, `playlog`, `album_artists`,
`album_tracks`, `track_artists`, `genre_media_item_mapping`, `genre_media_item_exclusion`, `provider_mappings`,
`audio_analysis`, `audiobooks`, `podcasts`, `settings`.

API source of truth: [src/db/queries.rs](../src/db/queries.rs) and [src/models/](../src/models).

---

## 1. Mood — resolved by reading the MA server source, not an assumption

Two distinct "mood" concepts exist upstream, traced via [music-assistant/server](https://github.com/music-assistant/server):

**a) `tracks.metadata.mood` / `albums.metadata.mood`** — a free-text tag field populated only by external
metadata providers: `opensubsonic` (`metadata.mood = sonic_song.moods[0]`) and `theaudiodb`
(`metadata.mood = artist_obj.get("strMood")`). It has nothing to do with CLAP/audio analysis — it's whatever mood
tag exists in your files' own metadata or on TheAudioDB. Checked actual values in the live DB:

```
tracks with non-null mood:  12  out of 37,878  (0.03%)
tracks with non-null style: 12  out of 37,878
```

Essentially unpopulated for a filesystem-sourced library. Not a credible data source regardless of API exposure.

**b) CLAP-derived "mood"** — MA's `sonic_analysis` provider does not compute a `mood` field at all. Its CLAP
zero-shot prompt pairs (`music_assistant/providers/sonic_analysis/clap_prompts.py`,
`SCALAR_PROMPT_PAIRS`) are exactly five: `danceability`, `valence`, `arousal`, `instrumentalness`,
`acousticness` — nothing else is asked of the model. And in `music_assistant/models/audio_analysis.py`,
`valence` is *documented by MA itself* as:

```python
# Musical mood: 0.0 = dark/sad, 1.0 = bright/happy.
valence: float | None = None
# Intensity/activation: 0.0 = calm/relaxed, 1.0 = energetic/aggressive.
arousal: float | None = None
```

And `music_assistant/providers/sonic_similarity/vectors.py` builds a named `"mood"` similarity-vector slice from
exactly `(instrumentalness, valence, arousal, acousticness)`. So MA's own codebase treats those four scalars
*as* its mood representation — there is no more granular/separate mood classifier anywhere in the CLAP path
to be missing.

**Conclusion: not an API gap.** `valence`/`arousal`/`instrumentalness`/`acousticness` aren't a proxy this API
substitutes for real mood data — they *are* MA's mood data, already fully exposed via `TrackAnalysis`, with
68% population (25,662 / 37,878 tracks have `sonic_analysis` rows).

---

## 2. Real gaps — data that exists and is populated, but isn't in the API

| DB field | Coverage | Currently exposed? | Notes |
|---|---|---|---|
| `tracks.metadata.lyrics` | 15,124 / 37,878 tracks (40%) | **No** | Full timestamped lyrics text, e.g. `[00:12.25] ...`. Sizeable payload — would want its own endpoint or an opt-in `include` flag, not inline on every track. |
| `tracks.metadata.genres` (array) | up to several per track | **Partial** — only `genres[0]` is read ([queries.rs:292](../src/db/queries.rs#L292)) | 1,708 tracks have >1 genre; the rest are silently dropped. |
| `genres` table + `genre_media_item_mapping` | 147 genres, 52,949 mappings | **No** | Real taxonomy with `genre_aliases` (e.g. "ambient" aliases to 10+ related tags), `is_default`/`is_excluded` flags. The API's `genre` field is just a raw string copied out of track metadata — none of this structure (hierarchy, aliasing, browse-by-genre) is reachable. |
| `tracks.metadata.explicit` | 859 / 37,878 tracks (2.3%) | **No** | Explicit-content flag. |
| `tracks.play_count` / `tracks.last_played` | populated wherever a track has been played | **No** | Listening history per track. Albums/artists/playlists have the same two columns, also unexposed (`albums.play_count` is used internally for one `order=play_count` sort option but never returned in the response body). |
| `albums.metadata` extras: `label`, `release_date`, `copyright`, `links`, `performers`, `popularity` (album-level), `lrc_lyrics` | varies | **No** | Album response only surfaces `name`/`artist`/`year`/`track_count`/`cover_url`. |
| `tracks.metadata.images` / `albums.metadata.images` (array, with `provider`, `remotely_accessible`) | most tracks/albums | **Partial** — only a derived `cover_url` (extracted from the audio file's embedded tag) is exposed | The richer image metadata, including remote provider artwork (e.g. Spotify CDN URLs) used as a fallback when there's no embedded picture, isn't surfaced. |
| `album_tracks.disc_number` / `track_number` | all album tracks | **No** | Track ordering within an album/disc isn't returned anywhere — can't reconstruct correct track order via the API. |
| `tracks.version` / `albums.version` | sparse (e.g. "Remastered", "Deluxe") | **No** | Release-version disambiguation. |
| `albums.album_type` | all albums (`album`, `single`, `ep`, `compilation`) | **No** | Useful filter that doesn't exist on `/albums`. |
| `tracks.external_ids` / `artists.external_ids` (full list: `isrc`, `musicbrainz_recordingid`/`musicbrainz_artistid`, etc.) | most tracks/artists | **Partial** — `mbid`/`isrc` are only populated from the `sonic_analysis` JSON's `extra_data`, not from this column, and only on tracks (never artists) | A track can have *multiple* ISRCs (seen in samples); the API only returns one. Artist MBIDs aren't exposed at all. |
| `playlists.is_dynamic`, `is_editable`, `owner`, `supported_mediatypes` | all playlists | **No** | `/playlists` only returns `id`/`name`/`timestamp_modified`. |
| Playlist **contents** (track ordering for a given playlist) | — | **No** | There is no `/playlists/:id/tracks` endpoint at all — playlists are listed but their contents are unreachable. |
| `playlog` (play history table) | 117 rows | **No** | Recently-played tracks/albums/artists/radios with timestamps and `seconds_played`/`fully_played` — no `/history` or `/recently-played` endpoint. |
| `radios` table | 1 row | **No** | Entire media type absent from the API. |
| `audio_analysis.analysis_data.loudness_range`, `.true_peak` | populated wherever `sonic_analysis`/`loudness_analysis` ran | **No** | Two more loudness/dynamics metrics computed but not flattened into `track_audio_features` or `TrackAnalysis`. |
| `audio_analysis.analysis_data.beats_per_bar`, `.downbeats` | populated by `smart_fades` | **No** | Beat-grid/downbeat data used for DJ-style mixing — not in the API at all. |
| `provider_mappings` (per-track provider availability/URLs/`audio_format`) | all tracks | **No** | No way to see which providers (filesystem, Spotify, etc.) a track is mapped to, its bitrate/codec/sample rate, or its provider URL. |

---

## Summary

- **Mood is a non-issue**: the field exists upstream but is essentially unpopulated (12/37,878 tracks) — exposing it wouldn't add real coverage.
- **Genuine gaps worth prioritizing**, roughly by value:
  1. **Playlist contents** — playlists are listed but can't be read (`/playlists/:id/tracks` doesn't exist).
  2. **Genre taxonomy** — the `genres`/`genre_media_item_mapping` tables would enable real browse-by-genre instead of a flattened first-tag string.
  3. **Lyrics** — populated for 40% of the library, currently fully inaccessible.
  4. **`loudness_range`/`true_peak`/`downbeats`/`beats_per_bar`** — cheap additions, already computed, just need to be added to `materialize_audio_features` and `TrackAnalysis`.
  5. **Album/track ordering** (`disc_number`/`track_number`) and **album metadata** (`label`, `release_date`, `album_type`) — needed for any client rendering an album view correctly.
  6. **Play history** (`playlog`) and **radios** — smaller, more optional additions.
