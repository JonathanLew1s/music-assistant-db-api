use rusqlite::{params, Connection, OptionalExtension};
use anyhow::Result;
use serde_json::Value;

use crate::camelot::to_camelot;
use crate::models::{
    track::{Track, TrackAnalysis, TrackQueryParams},
    album::Album,
    artist::Artist,
    genre::Genre,
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
    let clap_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM audio_analysis
         WHERE aa_provider_domain='sonic_analysis'
           AND json_extract(analysis_data, '$.extra_data.clap_embedding') IS NOT NULL",
        [], |r| r.get(0))?;
    Ok(HealthStats { track_count, schema_version, loudness_count, bpm_count, clap_count, sonic_count })
}

// ---------------------------------------------------------------------------
// Audio feature materialization
// ---------------------------------------------------------------------------

// Flattens the 3 audio_analysis JSON domains (loudness_analysis, smart_fades,
// sonic_analysis) into one real, typed table with real btree indexes —
// called once during the clone refresh (see db::recover_wal), not per query.
// Every list_tracks/observatory_tracks/search filter against energy, valence,
// arousal, bpm etc. used to re-run json_extract over the JSON blob on every
// row of every request; this pays that parsing cost exactly once per refresh
// instead, and gives the query planner plain numeric columns it can actually
// use range scans against — CAST(json_extract(...)) AS REAL needs an
// expression index, which works for direct comparisons but can't be combined
// or composed the way a normal column index can.
//
// Keyed by tracks.item_id (not provider_item_id — that's audio_analysis's
// own join key, a filesystem path string) so every other query can join to
// it with a single cheap equi-join on the same integer column it already
// joins everything else on.
pub fn materialize_audio_features(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS track_audio_features;
        CREATE TABLE track_audio_features (
            item_id INTEGER PRIMARY KEY,
            loudness_lufs REAL,
            loudness_album_lufs REAL,
            bpm REAL,
            key TEXT,
            mode TEXT,
            energy REAL,
            valence REAL,
            danceability REAL,
            arousal REAL,
            acousticness REAL,
            instrumentalness REAL,
            brightness REAL,
            speechiness REAL,
            roughness REAL,
            harmonic_complexity REAL,
            rhythmic_regularity REAL,
            spectral_centroid REAL,
            loudness_range REAL,
            true_peak REAL,
            beats_per_bar REAL
        );

        INSERT INTO track_audio_features (
            item_id, loudness_lufs, loudness_album_lufs, bpm, key, mode,
            energy, valence, danceability, arousal, acousticness,
            instrumentalness, brightness, speechiness, roughness,
            harmonic_complexity, rhythmic_regularity, spectral_centroid,
            loudness_range, true_peak, beats_per_bar
        )
        -- Each domain's fields are pulled via one multi-path json_extract
        -- call instead of one call per field: sonic_analysis rows carry a
        -- 1024-dim CLAP embedding alongside these scalars (~88KB/row at
        -- current library size), and json_extract reparses the whole blob
        -- on every call, so 14 separate calls meant 14 full reparses of that
        -- ~88KB text per row. A single call returns all 14 values as one
        -- small JSON array, which is then cheap to unpack by index below.
        SELECT
            item_id,
            CAST(json_extract(loud, '$[0]') AS REAL),
            CAST(json_extract(loud, '$[1]') AS REAL),
            CAST(json_extract(fades, '$[0]') AS REAL),
            json_extract(fades, '$[1]'),
            json_extract(fades, '$[2]'),
            CAST(json_extract(sonic, '$[0]') AS REAL),
            CAST(json_extract(sonic, '$[1]') AS REAL),
            CAST(json_extract(sonic, '$[2]') AS REAL),
            CAST(json_extract(sonic, '$[3]') AS REAL),
            CAST(json_extract(sonic, '$[4]') AS REAL),
            CAST(json_extract(sonic, '$[5]') AS REAL),
            CAST(json_extract(sonic, '$[6]') AS REAL),
            CAST(json_extract(sonic, '$[7]') AS REAL),
            CAST(json_extract(sonic, '$[8]') AS REAL),
            CAST(json_extract(sonic, '$[9]') AS REAL),
            CAST(json_extract(sonic, '$[10]') AS REAL),
            CAST(json_extract(sonic, '$[11]') AS REAL),
            CAST(json_extract(sonic, '$[12]') AS REAL),
            CAST(json_extract(sonic, '$[13]') AS REAL),
            CAST(json_extract(fades, '$[3]') AS REAL)
        FROM (
            SELECT
                pm.item_id,
                MAX(CASE WHEN aa.aa_provider_domain='loudness_analysis'
                    THEN json_extract(aa.analysis_data,
                        '$.loudness_integrated', '$.loudness_album') END) AS loud,
                MAX(CASE WHEN aa.aa_provider_domain='smart_fades'
                    THEN json_extract(aa.analysis_data,
                        '$.bpm', '$.key', '$.mode', '$.beats_per_bar') END) AS fades,
                MAX(CASE WHEN aa.aa_provider_domain='sonic_analysis'
                    THEN json_extract(aa.analysis_data,
                        '$.energy', '$.valence', '$.danceability', '$.arousal',
                        '$.acousticness', '$.instrumentalness', '$.brightness',
                        '$.speechiness', '$.roughness', '$.harmonic_complexity',
                        '$.rhythmic_regularity', '$.spectral_centroid',
                        '$.loudness_range', '$.true_peak') END) AS sonic
            FROM provider_mappings pm
            JOIN audio_analysis aa ON aa.item_id = pm.provider_item_id
            WHERE pm.media_type='track' AND pm.provider_domain='filesystem_local'
            GROUP BY pm.item_id
        );

        CREATE INDEX idx_taf_energy ON track_audio_features(energy);
        CREATE INDEX idx_taf_valence ON track_audio_features(valence);
        CREATE INDEX idx_taf_arousal ON track_audio_features(arousal);
        CREATE INDEX idx_taf_bpm ON track_audio_features(bpm);
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Track row parser (shared by list and get)
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

// Scalar-only variant: extracts individual fields via json_extract() so SQLite
// transmits only the values we need instead of the full analysis_data blobs.
// The sonic_analysis blob contains a 1024-dim CLAP embedding + beats array;
// transmitting it for thousands of rows is expensive. This reduces per-row
// transfer from ~10KB to ~200 bytes — used when include=analysis_scalar.
// Joins the flattened track_audio_features table (see queries::materialize_
// audio_features) instead of the 3x audio_analysis LEFT JOINs TRACK_BASE
// still needs — this variant only returns scalars, never the arrays
// (beats/rms_energy/clap_embedding) that table doesn't carry, so it never
// needs the raw JSON blobs in the first place.
const TRACK_BASE_SCALAR: &str = "
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
  tf.loudness_lufs,
  tf.loudness_album_lufs,
  tf.bpm,
  tf.key AS fkey,
  tf.mode AS fmode,
  tf.energy,
  tf.valence,
  tf.danceability,
  tf.arousal,
  tf.acousticness,
  tf.instrumentalness,
  tf.brightness,
  tf.speechiness,
  tf.roughness,
  tf.harmonic_complexity,
  tf.rhythmic_regularity,
  tf.spectral_centroid,
  tf.loudness_range,
  tf.true_peak,
  tf.beats_per_bar
FROM tracks t
LEFT JOIN track_artists ta ON ta.track_id = t.item_id
LEFT JOIN artists a ON a.item_id = ta.artist_id
LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
LEFT JOIN albums alb ON alb.item_id = at2.album_id
LEFT JOIN provider_mappings pm
  ON pm.item_id = t.item_id AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
LEFT JOIN track_audio_features tf ON tf.item_id = t.item_id
WHERE pm.provider_item_id IS NOT NULL
";

// Extracts the `popularity` field from a track's metadata JSON blob, if present.
fn extract_popularity(metadata: &Option<Value>) -> Option<f64> {
    metadata.as_ref()
        .and_then(|m| m.get("popularity"))
        .and_then(|p| p.as_f64())
}

// Extracts the full `genres` array from a track/album's metadata JSON blob.
// Unlike the old genres[0]-only read, this keeps every tag MA stored.
fn extract_genres(metadata: &Option<Value>) -> Vec<String> {
    metadata.as_ref()
        .and_then(|m| m.get("genres"))
        .and_then(|g| g.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).map(String::from).collect())
        .unwrap_or_default()
}

// Extracts `lyrics` from a track's metadata JSON blob, gated by the caller's
// ?include=lyrics flag — lyrics text can be large, so it's never parsed out
// unless explicitly requested (same gating pattern as include_clap).
fn extract_lyrics(metadata: &Option<Value>, include_lyrics: bool) -> Option<String> {
    if !include_lyrics {
        return None;
    }
    metadata.as_ref()
        .and_then(|m| m.get("lyrics"))
        .and_then(|v| v.as_str())
        .map(String::from)
}

// Row parser for TRACK_BASE_SCALAR — reads pre-extracted scalar columns instead
// of full JSON blobs. Column layout must match TRACK_BASE_SCALAR exactly.
pub fn parse_track_scalar_row(row: &rusqlite::Row, include_lyrics: bool) -> rusqlite::Result<Track> {
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
    // Pre-extracted scalars from json_extract() — column 12 onwards.
    let loudness_lufs: Option<f64> = row.get(12)?;
    let loudness_album_lufs: Option<f64> = row.get(13)?;
    let bpm: Option<f64> = row.get(14)?;
    let key: Option<String> = row.get(15)?;
    let mode: Option<String> = row.get(16)?;
    let energy: Option<f64> = row.get(17)?;
    let valence: Option<f64> = row.get(18)?;
    let danceability: Option<f64> = row.get(19)?;
    let arousal: Option<f64> = row.get(20)?;
    let acousticness: Option<f64> = row.get(21)?;
    let instrumentalness: Option<f64> = row.get(22)?;
    let brightness: Option<f64> = row.get(23)?;
    let speechiness: Option<f64> = row.get(24)?;
    let roughness: Option<f64> = row.get(25)?;
    let harmonic_complexity: Option<f64> = row.get(26)?;
    let rhythmic_regularity: Option<f64> = row.get(27)?;
    let spectral_centroid: Option<f64> = row.get(28)?;
    let loudness_range: Option<f64> = row.get(29)?;
    let true_peak: Option<f64> = row.get(30)?;
    let beats_per_bar: Option<f64> = row.get(31)?;

    let artists: Vec<String> = artists_str
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let artist = artists.first().cloned();

    let metadata: Option<serde_json::Value> = metadata_str.as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    let genres = extract_genres(&metadata);
    let popularity = extract_popularity(&metadata);
    let lyrics = extract_lyrics(&metadata, include_lyrics);

    let camelot = key.as_deref().zip(mode.as_deref())
        .and_then(|(k, m)| to_camelot(k, m));

    let has_any_analysis = loudness_lufs.is_some() || bpm.is_some() || energy.is_some();
    let analysis = if has_any_analysis {
        Some(TrackAnalysis {
            loudness_lufs,
            loudness_album_lufs,
            bpm,
            key,
            mode,
            camelot,
            beats: None,
            valence,
            energy,
            danceability,
            arousal,
            acousticness,
            instrumentalness,
            brightness,
            speechiness,
            roughness,
            harmonic_complexity,
            rhythmic_regularity,
            spectral_centroid,
            loudness_range,
            true_peak,
            beats_per_bar,
            downbeats: None,
            rms_energy: None,
            mbid: None,
            isrc: None,
            clap_embedding: None,
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
        genres,
        popularity,
        duration,
        file_path,
        favorite,
        timestamp_added,
        timestamp_modified,
        cover_url: format!("/api/v1/tracks/{id}/cover"),
        analysis,
        lyrics,
    })
}

pub fn parse_track_row(
    row: &rusqlite::Row,
    include_analysis: bool,
    include_arrays: bool,
    include_clap: bool,
    include_lyrics: bool,
) -> rusqlite::Result<Track> {
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
    let genres = extract_genres(&metadata);
    let popularity = extract_popularity(&metadata);
    let lyrics = extract_lyrics(&metadata, include_lyrics);

    let analysis = if include_analysis {
        let loud: Option<Value> = loudness_str.as_deref().and_then(|s| serde_json::from_str(s).ok());
        let fades: Option<Value> = fades_str.as_deref().and_then(|s| serde_json::from_str(s).ok());
        let sonic: Option<Value> = sonic_str.as_deref().and_then(|s| serde_json::from_str(s).ok());

        let key = fades.as_ref().and_then(|v| v.get("key")).and_then(|v| v.as_str()).map(String::from);
        let mode = fades.as_ref().and_then(|v| v.get("mode")).and_then(|v| v.as_str()).map(String::from);
        let camelot = key.as_deref().zip(mode.as_deref()).and_then(|(k, m)| to_camelot(k, m));

        let beats = if include_arrays {
            fades.as_ref()
                .and_then(|v| v.get("beats"))
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect())
        } else {
            None
        };

        let downbeats = if include_arrays {
            fades.as_ref()
                .and_then(|v| v.get("downbeats"))
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect())
        } else {
            None
        };

        let rms_energy = if include_arrays {
            sonic.as_ref()
                .and_then(|v| v.get("rms_energy"))
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect())
        } else {
            None
        };

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
            beats_per_bar: fades.as_ref().and_then(|v| v.get("beats_per_bar")).and_then(|v| v.as_f64()),
            downbeats,
            valence: sonic.as_ref().and_then(|v| v.get("valence")).and_then(|v| v.as_f64()),
            energy: sonic.as_ref().and_then(|v| v.get("energy")).and_then(|v| v.as_f64()),
            danceability: sonic.as_ref().and_then(|v| v.get("danceability")).and_then(|v| v.as_f64()),
            arousal: sonic.as_ref().and_then(|v| v.get("arousal")).and_then(|v| v.as_f64()),
            acousticness: sonic.as_ref().and_then(|v| v.get("acousticness")).and_then(|v| v.as_f64()),
            instrumentalness: sonic.as_ref().and_then(|v| v.get("instrumentalness")).and_then(|v| v.as_f64()),
            brightness: sonic.as_ref().and_then(|v| v.get("brightness")).and_then(|v| v.as_f64()),
            speechiness: sonic.as_ref().and_then(|v| v.get("speechiness")).and_then(|v| v.as_f64()),
            roughness: sonic.as_ref().and_then(|v| v.get("roughness")).and_then(|v| v.as_f64()),
            harmonic_complexity: sonic.as_ref().and_then(|v| v.get("harmonic_complexity")).and_then(|v| v.as_f64()),
            rhythmic_regularity: sonic.as_ref().and_then(|v| v.get("rhythmic_regularity")).and_then(|v| v.as_f64()),
            spectral_centroid: sonic.as_ref().and_then(|v| v.get("spectral_centroid")).and_then(|v| v.as_f64()),
            loudness_range: sonic.as_ref().and_then(|v| v.get("loudness_range")).and_then(|v| v.as_f64()),
            true_peak: sonic.as_ref().and_then(|v| v.get("true_peak")).and_then(|v| v.as_f64()),
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
        genres,
        popularity,
        duration,
        file_path,
        favorite,
        timestamp_added,
        timestamp_modified,
        cover_url: format!("/api/v1/tracks/{id}/cover"),
        analysis,
        lyrics,
    })
}

// ---------------------------------------------------------------------------
// Track queries
// ---------------------------------------------------------------------------

// One general two-stage path covers every filter/order combination:
// Stage 1 finds matching item_ids against tracks/provider_mappings, plus a
// single equi-join to track_audio_features only when an audio filter is
// actually requested — never the full per-track join with 3x audio_analysis.
// Ordering and LIMIT/OFFSET (or RANDOM()) apply here, so stage 2 only ever
// hydrates at most `limit` rows. This replaced 3 hand-rolled fast-path
// branches (random+no-filters, random+sonic-filters-but-not-bpm, and a
// narrow non-random "has sonic_analysis" shape) plus a slow fallback that
// caught everything else — including any bpm filter combined with
// order=random, or any audio filter at all combined with a non-random
// order, neither of which any branch covered, so both still ran the full
// 9-17s join the other branches existed to avoid.
pub fn list_tracks(conn: &Connection, p: &TrackQueryParams) -> Result<(i64, Vec<Track>)> {
    let has_audio_filters = p.bpm_min.is_some() || p.bpm_max.is_some()
        || p.energy_min.is_some() || p.energy_max.is_some()
        || p.valence_min.is_some() || p.valence_max.is_some()
        || p.arousal_min.is_some() || p.arousal_max.is_some();

    let mut wheres: Vec<String> = vec![
        "pm.media_type='track'".into(),
        "pm.provider_domain='filesystem_local'".into(),
    ];
    let mut values: Vec<Box<dyn rusqlite::ToSql>> = vec![];

    if let Some(since) = p.since {
        wheres.push(format!("t.timestamp_modified > ?{}", values.len() + 1));
        values.push(Box::new(since));
    }
    if let Some(fav) = p.favorite {
        wheres.push(format!("t.favorite = ?{}", values.len() + 1));
        values.push(Box::new(fav as i64));
    }
    if let Some(ref genre) = p.genre {
        // Membership check against the full genres array, not just the first
        // tag — a track tagged ["Indie Rock", "Shoegaze"] must match either.
        // Unindexed either way (same as the genres[0]-only check this
        // replaced); see docs/specs/expose-unexposed-ma-data.md Alternatives.
        wheres.push(format!(
            "EXISTS (SELECT 1 FROM json_each(t.metadata, '$.genres') WHERE json_each.value = ?{})",
            values.len() + 1
        ));
        values.push(Box::new(genre.clone()));
    }
    if let Some(artist_id) = p.artist_id {
        wheres.push(format!(
            "EXISTS (SELECT 1 FROM track_artists ta2 WHERE ta2.track_id = t.item_id AND ta2.artist_id = ?{})",
            values.len() + 1
        ));
        values.push(Box::new(artist_id));
    }
    if let Some(album_id) = p.album_id {
        wheres.push(format!(
            "EXISTS (SELECT 1 FROM album_tracks at3 WHERE at3.track_id = t.item_id AND at3.album_id = ?{})",
            values.len() + 1
        ));
        values.push(Box::new(album_id));
    }

    // Plain comparisons against the flattened track_audio_features columns —
    // a real btree index per column instead of CAST(json_extract(...)).
    macro_rules! tf_filter {
        ($field:expr, $op:expr, $val:expr) => {
            wheres.push(format!("tf.{} {} ?{}", $field, $op, values.len() + 1));
            values.push(Box::new($val));
        };
    }
    if let Some(v) = p.bpm_min { tf_filter!("bpm", ">=", v); }
    if let Some(v) = p.bpm_max { tf_filter!("bpm", "<=", v); }
    if let Some(v) = p.energy_min { tf_filter!("energy", ">=", v); }
    if let Some(v) = p.energy_max { tf_filter!("energy", "<=", v); }
    if let Some(v) = p.valence_min { tf_filter!("valence", ">=", v); }
    if let Some(v) = p.valence_max { tf_filter!("valence", "<=", v); }
    if let Some(v) = p.arousal_min { tf_filter!("arousal", ">=", v); }
    if let Some(v) = p.arousal_max { tf_filter!("arousal", "<=", v); }

    let exclude_ids = p.exclude_ids();
    if !exclude_ids.is_empty() {
        let placeholders: Vec<String> = (0..exclude_ids.len())
            .map(|i| format!("?{}", values.len() + i + 1))
            .collect();
        wheres.push(format!("t.item_id NOT IN ({})", placeholders.join(",")));
        for id in &exclude_ids {
            values.push(Box::new(*id));
        }
    }

    let where_clause = wheres.join(" AND ");
    let limit = p.clamped_limit();
    let offset = p.offset;
    let is_random = p.order.as_deref() == Some("random");

    // Default to disc/track order within an album when no explicit order
    // was requested — matches physical album track order. Needs its own
    // join (not the EXISTS filter above) because ORDER BY needs columns,
    // not just a boolean match. Relies on idx_album_tracks_album_id
    // (created in db::recover_wal) so this is an indexed lookup, not a
    // full scan of album_tracks — see docs/specs/expose-unexposed-ma-data.md.
    let album_order_pos = if p.order.is_none() {
        p.album_id.map(|album_id| {
            let pos = values.len() + 1;
            values.push(Box::new(album_id));
            pos
        })
    } else {
        None
    };
    let album_order_join = match album_order_pos {
        Some(pos) => format!("JOIN album_tracks at_order ON at_order.track_id = t.item_id AND at_order.album_id = ?{pos}"),
        None => String::new(),
    };
    let order_col = if album_order_pos.is_some() {
        "at_order.disc_number, at_order.track_number"
    } else {
        match p.order.as_deref().unwrap_or("name") {
            "timestamp_added" => "t.timestamp_added",
            "timestamp_modified" => "t.timestamp_modified",
            _ => "t.name",
        }
    };
    let order_dir = if p.dir.as_deref() == Some("desc") { "DESC" } else { "ASC" };

    // Only joined when actually needed: an unfiltered or non-audio-filtered
    // listing never touches track_audio_features (or audio_analysis) at all.
    let audio_join = if has_audio_filters {
        "JOIN track_audio_features tf ON tf.item_id = t.item_id"
    } else {
        ""
    };

    let count_sql = format!(
        "SELECT COUNT(DISTINCT t.item_id)
         FROM tracks t
         JOIN provider_mappings pm ON pm.item_id = t.item_id
         {audio_join}
         {album_order_join}
         WHERE {where_clause}"
    );
    let total: i64 = conn.query_row(&count_sql, rusqlite::params_from_iter(values.iter()), |r| r.get(0))?;

    let limit_pos = values.len() + 1;
    let id_sql = if is_random {
        format!(
            "SELECT DISTINCT t.item_id FROM tracks t
             JOIN provider_mappings pm ON pm.item_id = t.item_id
             {audio_join}
             {album_order_join}
             WHERE {where_clause}
             ORDER BY RANDOM()
             LIMIT ?{limit_pos}"
        )
    } else {
        format!(
            "SELECT DISTINCT t.item_id FROM tracks t
             JOIN provider_mappings pm ON pm.item_id = t.item_id
             {audio_join}
             {album_order_join}
             WHERE {where_clause}
             ORDER BY {order_col} {order_dir}
             LIMIT ?{limit_pos} OFFSET ?{}",
            limit_pos + 1
        )
    };
    values.push(Box::new(limit));
    if !is_random {
        values.push(Box::new(offset));
    }

    let mut id_stmt = conn.prepare(&id_sql)?;
    let page_ids: Vec<i64> = id_stmt.query_map(
        rusqlite::params_from_iter(values.iter()),
        |row| row.get(0),
    )?.collect::<rusqlite::Result<_>>()?;

    if page_ids.is_empty() {
        return Ok((total, vec![]));
    }

    // Stage 2: fetch full joined rows for only the matched/paged ids.
    let id_placeholders: Vec<String> = (1..=page_ids.len())
        .map(|i| format!("?{i}"))
        .collect();
    let include_analysis = p.include_analysis();
    let include_arrays = p.include_arrays();
    let include_clap = p.include_clap();
    let include_lyrics = p.include_lyrics();

    let mut tracks: Vec<Track> = if include_analysis && !include_arrays {
        let data_sql = format!(
            "{TRACK_BASE_SCALAR} AND t.item_id IN ({}) GROUP BY t.item_id",
            id_placeholders.join(",")
        );
        let mut stmt = conn.prepare(&data_sql)?;
        let x = stmt.query_map(
            rusqlite::params_from_iter(page_ids.iter()),
            |row| parse_track_scalar_row(row, include_lyrics),
        )?.collect::<rusqlite::Result<_>>()?; x
    } else {
        let data_sql = format!(
            "{TRACK_BASE} AND t.item_id IN ({}) GROUP BY t.item_id",
            id_placeholders.join(",")
        );
        let mut stmt = conn.prepare(&data_sql)?;
        let x = stmt.query_map(
            rusqlite::params_from_iter(page_ids.iter()),
            |row| parse_track_row(row, include_analysis, include_arrays, include_clap, include_lyrics),
        )?.collect::<rusqlite::Result<_>>()?; x
    };

    // Stage 2's GROUP BY t.item_id doesn't preserve stage 1's order (and
    // for disc/track order specifically, TRACK_BASE's own album_tracks join
    // is unscoped to any one album, so it couldn't be used for ORDER BY
    // here even if re-stated) — re-sort by stage 1's id order in Rust
    // instead of re-running ORDER BY against a differently-shaped query.
    let order_index: std::collections::HashMap<i64, usize> =
        page_ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    tracks.sort_by_key(|t| order_index.get(&t.id).copied().unwrap_or(usize::MAX));

    Ok((total, tracks))
}

pub fn get_track(
    conn: &Connection,
    id: i64,
    include_analysis: bool,
    include_clap: bool,
    include_lyrics: bool,
) -> Result<Option<Track>> {
    let sql = format!(
        "{TRACK_BASE} AND t.item_id = ?1
         GROUP BY t.item_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(params![id], |row| {
        parse_track_row(row, include_analysis, true, include_clap, include_lyrics)
    })?;
    Ok(rows.next().transpose()?)
}

pub fn get_track_file_path(conn: &Connection, id: i64) -> Result<Option<String>> {
    let sql = "SELECT pm.provider_item_id FROM provider_mappings pm
               WHERE pm.item_id = ?1 AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
               LIMIT 1";
    conn.query_row(sql, params![id], |r| r.get(0)).optional().map_err(Into::into)
}

/// Observatory bulk fetch: all tracks that have sonic_analysis, returning scalar fields only.
/// Drives the join from audio_analysis aa_sonic (7K rows) not tracks (37K+) so the planner
/// can use aa_sonic's implicit rowid ordering as the outer loop.  Returns every matching track
/// in a single query — callers cache the result instead of paginating.
pub fn observatory_tracks(conn: &Connection) -> Result<Vec<Track>> {
    // Drives from track_audio_features (one row per analysed track) instead
    // of audio_analysis directly — what used to be conditional aggregation
    // across however many JSON rows a track has, re-parsing each one's JSON
    // blob, is now a single cheap equi-join on an already-flattened row.
    let sql = "
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
  tf.loudness_lufs,
  tf.loudness_album_lufs,
  tf.bpm,
  tf.key AS fkey,
  tf.mode AS fmode,
  tf.energy,
  tf.valence,
  tf.danceability,
  tf.arousal,
  tf.acousticness,
  tf.instrumentalness,
  tf.brightness,
  tf.speechiness,
  tf.roughness,
  tf.harmonic_complexity,
  tf.rhythmic_regularity,
  tf.spectral_centroid,
  tf.loudness_range,
  tf.true_peak,
  tf.beats_per_bar
FROM track_audio_features tf
JOIN tracks t ON t.item_id = tf.item_id
JOIN provider_mappings pm
  ON pm.item_id = t.item_id AND pm.media_type = 'track' AND pm.provider_domain = 'filesystem_local'
LEFT JOIN track_artists ta ON ta.track_id = t.item_id
LEFT JOIN artists a ON a.item_id = ta.artist_id
LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
LEFT JOIN albums alb ON alb.item_id = at2.album_id
WHERE tf.energy IS NOT NULL
GROUP BY t.item_id
ORDER BY t.item_id ASC
";
    let mut stmt = conn.prepare(sql)?;
    // Observatory is the bulk discovery-feature fetch, not a single-track
    // detail view — lyrics text for 37K+ rows would balloon the cached
    // payload for no caller need, so it's never included here.
    let tracks = stmt.query_map([], |row| parse_track_scalar_row(row, false))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(tracks)
}

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

const ALBUM_COLUMNS: &str = "
  alb.item_id, alb.name,
  (SELECT a.name FROM album_artists aa JOIN artists a ON a.item_id = aa.artist_id
   WHERE aa.album_id = alb.item_id LIMIT 1) AS artist,
  (SELECT aa.artist_id FROM album_artists aa WHERE aa.album_id = alb.item_id LIMIT 1) AS artist_id,
  alb.year,
  (SELECT COUNT(*) FROM album_tracks at2 WHERE at2.album_id = alb.item_id) AS track_count,
  alb.timestamp_added,
  alb.album_type,
  alb.metadata
";

// Shared row parser for every query selecting ALBUM_COLUMNS in that exact
// order — label/release_date come from albums.metadata (no dedicated
// columns on the native table), read live the same way Track's
// popularity/genres are: cheap, unindexed, point reads with no filter need.
fn parse_album_row(row: &rusqlite::Row) -> rusqlite::Result<Album> {
    let id: i64 = row.get(0)?;
    let metadata_str: Option<String> = row.get(8)?;
    let metadata: Option<Value> = metadata_str.as_deref().and_then(|s| serde_json::from_str(s).ok());
    let label = metadata.as_ref().and_then(|m| m.get("label")).and_then(|v| v.as_str()).map(String::from);
    let release_date = metadata.as_ref().and_then(|m| m.get("release_date")).and_then(|v| v.as_str()).map(String::from);

    Ok(Album {
        id,
        name: row.get(1)?,
        artist: row.get(2)?,
        artist_id: row.get(3)?,
        year: row.get(4)?,
        track_count: row.get(5)?,
        timestamp_added: row.get(6)?,
        cover_url: format!("/api/v1/albums/{id}/cover"),
        album_type: row.get(7)?,
        label,
        release_date,
    })
}

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

    let count_sql = format!("SELECT COUNT(*) FROM albums alb WHERE {where_clause}");
    let total: i64 = conn.query_row(
        &count_sql, rusqlite::params_from_iter(values.iter()), |r| r.get(0)
    )?;

    let data_sql = format!(
        "SELECT {ALBUM_COLUMNS}
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
        parse_album_row,
    )?.collect::<rusqlite::Result<_>>()?;

    Ok((total, albums))
}

pub fn get_album(conn: &Connection, id: i64) -> Result<Option<Album>> {
    let sql = format!("SELECT {ALBUM_COLUMNS} FROM albums alb WHERE alb.item_id = ?1");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(params![id], parse_album_row)?;
    Ok(rows.next().transpose()?)
}

pub fn get_album_first_file_path(conn: &Connection, album_id: i64) -> Result<Option<String>> {
    conn.query_row(
        "SELECT pm.provider_item_id FROM album_tracks at2
         JOIN provider_mappings pm ON pm.item_id = at2.track_id
           AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
         WHERE at2.album_id = ?1 LIMIT 1",
        params![album_id],
        |r| r.get(0),
    ).optional().map_err(Into::into)
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
// Genre queries
// ---------------------------------------------------------------------------

// MA's real genre taxonomy (genres + genre_media_item_mapping) — separate
// from the flat tracks.metadata.genres array. Already well-indexed upstream:
// genre_media_item_mapping_genre_alias_idx leads with genre_id, so a lookup
// by genre is an indexed search, not a scan (confirmed via PRAGMA index_info
// against the live clone — see docs/DB_API_GAP_ANALYSIS.md). No new index
// needed for either query below.
fn parse_genre_row(row: &rusqlite::Row) -> rusqlite::Result<Genre> {
    let aliases_str: Option<String> = row.get(3)?;
    let aliases: Vec<String> = aliases_str
        .as_deref()
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default();
    Ok(Genre {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        aliases,
        track_count: row.get(4)?,
    })
}

pub fn list_genres(conn: &Connection, offset: i64, limit: i64) -> Result<(i64, Vec<Genre>)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM genres", [], |r| r.get(0))?;
    let sql = "SELECT g.item_id, g.name, g.description, g.genre_aliases,
               (SELECT COUNT(*) FROM genre_media_item_mapping gmm
                WHERE gmm.genre_id = g.item_id AND gmm.media_type = 'track') AS track_count
               FROM genres g
               ORDER BY g.name ASC
               LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let genres: Vec<Genre> = stmt.query_map(params![limit, offset], parse_genre_row)?
        .collect::<rusqlite::Result<_>>()?;
    Ok((total, genres))
}

pub fn get_genre(conn: &Connection, id: i64) -> Result<Option<Genre>> {
    let sql = "SELECT g.item_id, g.name, g.description, g.genre_aliases,
               (SELECT COUNT(*) FROM genre_media_item_mapping gmm
                WHERE gmm.genre_id = g.item_id AND gmm.media_type = 'track') AS track_count
               FROM genres g WHERE g.item_id = ?1";
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query_map(params![id], parse_genre_row)?;
    Ok(rows.next().transpose()?)
}

/// Tracks tagged with a given genre, via genre_media_item_mapping — the real
/// taxonomy join, not the tracks.metadata.genres[0]/array string match.
pub fn genre_tracks(conn: &Connection, genre_id: i64, offset: i64, limit: i64) -> Result<(i64, Vec<Track>)> {
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM genre_media_item_mapping gmm
         WHERE gmm.genre_id = ?1 AND gmm.media_type = 'track'",
        params![genre_id],
        |r| r.get(0),
    )?;

    let id_sql = "SELECT gmm.media_id FROM genre_media_item_mapping gmm
                  WHERE gmm.genre_id = ?1 AND gmm.media_type = 'track'
                  ORDER BY gmm.media_id ASC
                  LIMIT ?2 OFFSET ?3";
    let mut id_stmt = conn.prepare(id_sql)?;
    let page_ids: Vec<i64> = id_stmt.query_map(params![genre_id, limit, offset], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;

    if page_ids.is_empty() {
        return Ok((total, vec![]));
    }

    let id_placeholders: Vec<String> = (1..=page_ids.len()).map(|i| format!("?{i}")).collect();
    let data_sql = format!(
        "{TRACK_BASE} AND t.item_id IN ({}) GROUP BY t.item_id ORDER BY t.name ASC",
        id_placeholders.join(",")
    );
    let mut stmt = conn.prepare(&data_sql)?;
    let tracks: Vec<Track> = stmt.query_map(
        rusqlite::params_from_iter(page_ids.iter()),
        |row| parse_track_row(row, false, false, false, false),
    )?.collect::<rusqlite::Result<_>>()?;

    Ok((total, tracks))
}

// ---------------------------------------------------------------------------
// Playlist queries
// ---------------------------------------------------------------------------

pub fn list_playlists(conn: &Connection, offset: i64, limit: i64) -> Result<(i64, Vec<Playlist>)> {
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM playlists", [], |r| r.get(0))?;
    // MA playlist tracks are resolved dynamically at runtime — no junction table exists in the DB.
    let sql = "SELECT p.item_id, p.name, p.timestamp_modified
               FROM playlists p ORDER BY p.name ASC LIMIT ?1 OFFSET ?2";
    let mut stmt = conn.prepare(sql)?;
    let playlists: Vec<Playlist> = stmt.query_map(params![limit, offset], |row| {
        Ok(Playlist {
            id: row.get(0)?,
            name: row.get(1)?,
            timestamp_modified: row.get(2)?,
        })
    })?.collect::<rusqlite::Result<_>>()?;
    Ok((total, playlists))
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SearchResults {
    pub tracks: Vec<Track>,
    pub albums: Vec<Album>,
    pub artists: Vec<Artist>,
}

/// Which result sets a caller actually wants. `/search` callers that only
/// need artists (e.g. subwave's resolveArtist retry loop) skip the track
/// and album queries entirely instead of paying for them and discarding
/// the result.
#[derive(Clone, Copy)]
pub struct SearchTypes {
    pub tracks: bool,
    pub albums: bool,
    pub artists: bool,
}

impl SearchTypes {
    pub const ALL: SearchTypes = SearchTypes { tracks: true, albums: true, artists: true };
    #[allow(dead_code)] // convenience constant for callers/tests; routes/search.rs builds this shape from ?types= itself
    pub const ARTISTS_ONLY: SearchTypes = SearchTypes { tracks: false, albums: false, artists: true };
}

pub fn search(conn: &Connection, q: &str, limit: i64, types: SearchTypes) -> Result<SearchResults> {
    let pattern = format!("%{q}%");

    let tracks = if types.tracks {
        // Two-stage lookup, same idea as list_tracks' fast paths: the LIKE
        // pattern has a leading wildcard so it can never use an index either
        // way, but matching against the full 5-way join (3x audio_analysis)
        // turns every search into the ~9-17s scan list_tracks' random-order
        // fast paths were built to avoid. Find matching ids against just
        // tracks/track_artists/artists first, then fetch full rows for only
        // those ids.
        let id_sql = "
            SELECT t.item_id
            FROM tracks t
            LEFT JOIN track_artists ta ON ta.track_id = t.item_id
            LEFT JOIN artists a ON a.item_id = ta.artist_id
            WHERE (t.name LIKE ?1 OR a.name LIKE ?1)
            GROUP BY t.item_id
            ORDER BY t.name ASC
            LIMIT ?2
        ";
        let mut id_stmt = conn.prepare(id_sql)?;
        let matched_ids: Vec<i64> = id_stmt.query_map(params![pattern, limit], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;

        if matched_ids.is_empty() {
            vec![]
        } else {
            let placeholders: Vec<String> = (1..=matched_ids.len()).map(|i| format!("?{i}")).collect();
            let track_sql = format!(
                "{TRACK_BASE} AND t.item_id IN ({})
                 GROUP BY t.item_id ORDER BY t.name ASC",
                placeholders.join(",")
            );
            let mut stmt = conn.prepare(&track_sql)?;
            let x = stmt.query_map(rusqlite::params_from_iter(matched_ids.iter()), |row| {
                parse_track_row(row, false, false, false, false)
            })?.collect::<rusqlite::Result<_>>()?; x
        }
    } else {
        vec![]
    };

    let albums = if types.albums {
        let album_sql = format!(
            "SELECT {ALBUM_COLUMNS} FROM albums alb WHERE alb.name LIKE ?1 ORDER BY alb.name ASC LIMIT ?2"
        );
        let mut stmt = conn.prepare(&album_sql)?;
        let x = stmt.query_map(params![pattern, limit], parse_album_row)?
            .collect::<rusqlite::Result<_>>()?; x
    } else {
        vec![]
    };

    let artists = if types.artists {
        let artist_sql = "SELECT a.item_id, a.name,
                          (SELECT COUNT(*) FROM track_artists ta WHERE ta.artist_id = a.item_id),
                          (SELECT COUNT(*) FROM album_artists aa WHERE aa.artist_id = a.item_id)
                          FROM artists a WHERE a.name LIKE ?1 ORDER BY a.name ASC LIMIT ?2";
        let mut stmt = conn.prepare(artist_sql)?;
        let x = stmt.query_map(params![pattern, limit], |row| {
            Ok(Artist {
                id: row.get(0)?,
                name: row.get(1)?,
                track_count: row.get(2)?,
                album_count: row.get(3)?,
            })
        })?.collect::<rusqlite::Result<_>>()?; x
    } else {
        vec![]
    };

    Ok(SearchResults { tracks, albums, artists })
}

#[cfg(test)]
mod popularity_tests {
    use super::extract_popularity;
    use serde_json::json;

    #[test]
    fn extracts_popularity_when_present() {
        let metadata = Some(json!({ "genres": ["Indie Rock"], "popularity": 0.73 }));
        let popularity = extract_popularity(&metadata);
        assert_eq!(popularity, Some(0.73));
    }

    #[test]
    fn returns_none_when_absent() {
        let metadata = Some(json!({ "genres": ["Indie Rock"] }));
        let popularity = extract_popularity(&metadata);
        assert_eq!(popularity, None);
    }
}

#[cfg(test)]
mod search_tests {
    use super::{search, SearchTypes};
    use rusqlite::Connection;

    // Minimal schema mirroring the columns queries.rs actually touches on
    // MA's real library.db — enough to exercise search()'s joins without
    // needing the full production schema.
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tracks (
                item_id INTEGER PRIMARY KEY, name TEXT, duration REAL,
                favorite INTEGER, timestamp_added INTEGER, timestamp_modified INTEGER,
                metadata TEXT
            );
            CREATE TABLE artists (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE track_artists (track_id INTEGER, artist_id INTEGER);
            CREATE TABLE albums (item_id INTEGER PRIMARY KEY, name TEXT, year INTEGER, timestamp_added INTEGER, album_type TEXT, metadata TEXT);
            CREATE TABLE album_tracks (track_id INTEGER, album_id INTEGER, disc_number INTEGER, track_number INTEGER);
            CREATE TABLE album_artists (album_id INTEGER, artist_id INTEGER);
            CREATE TABLE provider_mappings (
                item_id INTEGER, media_type TEXT, provider_domain TEXT, provider_item_id TEXT
            );
            CREATE TABLE audio_analysis (item_id TEXT, aa_provider_domain TEXT, analysis_data TEXT);
            ",
        )
        .unwrap();

        // Two tracks on the same artist/album, one unrelated artist with no tracks.
        conn.execute_batch(
            "
            INSERT INTO artists (item_id, name) VALUES (1, 'Boards of Canada'), (2, 'Autechre');
            INSERT INTO albums (item_id, name, year) VALUES (1, 'Geogaddi', 2002);
            INSERT INTO tracks (item_id, name) VALUES (10, 'Music Is Math'), (11, 'Sunshine Recorder');
            INSERT INTO track_artists (track_id, artist_id) VALUES (10, 1), (11, 1);
            INSERT INTO album_tracks (track_id, album_id) VALUES (10, 1), (11, 1);
            INSERT INTO album_artists (album_id, artist_id) VALUES (1, 1);
            INSERT INTO provider_mappings (item_id, media_type, provider_domain, provider_item_id)
                VALUES (10, 'track', 'filesystem_local', '/music/10.flac'),
                       (11, 'track', 'filesystem_local', '/music/11.flac');
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn finds_track_by_title_substring() {
        let conn = test_conn();
        let results = search(&conn, "Math", 10, SearchTypes::ALL).unwrap();
        assert_eq!(results.tracks.len(), 1);
        assert_eq!(results.tracks[0].title.as_deref(), Some("Music Is Math"));
    }

    #[test]
    fn finds_track_by_artist_name() {
        let conn = test_conn();
        let results = search(&conn, "Boards", 10, SearchTypes::ALL).unwrap();
        assert_eq!(results.tracks.len(), 2);
    }

    #[test]
    fn respects_limit() {
        let conn = test_conn();
        let results = search(&conn, "e", 1, SearchTypes::ALL).unwrap();
        assert_eq!(results.tracks.len(), 1);
    }

    #[test]
    fn finds_albums_and_artists() {
        let conn = test_conn();
        let results = search(&conn, "Geogaddi", 10, SearchTypes::ALL).unwrap();
        assert_eq!(results.albums.len(), 1);

        let results = search(&conn, "Autechre", 10, SearchTypes::ALL).unwrap();
        assert_eq!(results.artists.len(), 1);
        assert_eq!(results.artists[0].name.as_deref(), Some("Autechre"));
    }

    #[test]
    fn artists_only_skips_tracks_and_albums() {
        let conn = test_conn();
        let results = search(&conn, "Boards", 10, SearchTypes::ARTISTS_ONLY).unwrap();
        assert!(results.tracks.is_empty());
        assert!(results.albums.is_empty());
        assert_eq!(results.artists.len(), 1);
    }
}

#[cfg(test)]
mod materialize_tests {
    use super::materialize_audio_features;
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tracks (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE provider_mappings (
                item_id INTEGER, media_type TEXT, provider_domain TEXT, provider_item_id TEXT
            );
            CREATE TABLE audio_analysis (item_id TEXT, aa_provider_domain TEXT, analysis_data TEXT);

            INSERT INTO tracks (item_id, name) VALUES (10, 'Music Is Math'), (11, 'Sunshine Recorder');
            INSERT INTO provider_mappings (item_id, media_type, provider_domain, provider_item_id)
                VALUES (10, 'track', 'filesystem_local', '/music/10.flac'),
                       (11, 'track', 'filesystem_local', '/music/11.flac');

            INSERT INTO audio_analysis (item_id, aa_provider_domain, analysis_data) VALUES
                ('/music/10.flac', 'sonic_analysis', '{\"energy\": 0.8, \"valence\": 0.6, \"arousal\": 0.7, \"loudness_range\": 6.2, \"true_peak\": -0.3}'),
                ('/music/10.flac', 'smart_fades', '{\"bpm\": 128.0, \"key\": \"C\", \"mode\": \"major\", \"beats_per_bar\": 4.0}'),
                ('/music/10.flac', 'loudness_analysis', '{\"loudness_integrated\": -9.5}'),
                ('/music/11.flac', 'sonic_analysis', '{\"energy\": 0.2, \"valence\": 0.3, \"arousal\": 0.1}');
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn flattens_json_into_typed_columns() {
        let conn = test_conn();
        materialize_audio_features(&conn).unwrap();

        let (energy, bpm, key, loudness): (f64, f64, String, f64) = conn.query_row(
            "SELECT energy, bpm, key, loudness_lufs FROM track_audio_features WHERE item_id = 10",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        ).unwrap();
        assert_eq!(energy, 0.8);
        assert_eq!(bpm, 128.0);
        assert_eq!(key, "C");
        assert_eq!(loudness, -9.5);
    }

    #[test]
    fn flattens_newly_added_scalars() {
        let conn = test_conn();
        materialize_audio_features(&conn).unwrap();

        let (loudness_range, true_peak, beats_per_bar): (f64, f64, f64) = conn.query_row(
            "SELECT loudness_range, true_peak, beats_per_bar FROM track_audio_features WHERE item_id = 10",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).unwrap();
        assert_eq!(loudness_range, 6.2);
        assert_eq!(true_peak, -0.3);
        assert_eq!(beats_per_bar, 4.0);
    }

    #[test]
    fn track_with_no_bpm_analysis_has_null_bpm() {
        let conn = test_conn();
        materialize_audio_features(&conn).unwrap();

        let bpm: Option<f64> = conn.query_row(
            "SELECT bpm FROM track_audio_features WHERE item_id = 11",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(bpm, None);
    }

    #[test]
    fn rerunning_is_idempotent() {
        let conn = test_conn();
        materialize_audio_features(&conn).unwrap();
        materialize_audio_features(&conn).unwrap();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM track_audio_features", [], |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn creates_real_btree_indexes() {
        let conn = test_conn();
        materialize_audio_features(&conn).unwrap();

        let index_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='track_audio_features'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(index_count, 4);
    }
}

#[cfg(test)]
mod list_tracks_tests {
    use super::{list_tracks, materialize_audio_features};
    use crate::models::track::TrackQueryParams;
    use rusqlite::Connection;

    // 5 tracks: 3 with sonic_analysis+smart_fades (varying energy/bpm), 1 with
    // only loudness (no sonic/bpm), 1 with no analysis at all. Two artists,
    // one genre split, one favorite.
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tracks (
                item_id INTEGER PRIMARY KEY, name TEXT, duration REAL,
                favorite INTEGER, timestamp_added INTEGER, timestamp_modified INTEGER,
                metadata TEXT
            );
            CREATE TABLE artists (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE track_artists (track_id INTEGER, artist_id INTEGER);
            CREATE TABLE albums (item_id INTEGER PRIMARY KEY, name TEXT, year INTEGER, timestamp_added INTEGER, album_type TEXT, metadata TEXT);
            CREATE TABLE album_tracks (track_id INTEGER, album_id INTEGER, disc_number INTEGER, track_number INTEGER);
            CREATE TABLE provider_mappings (
                item_id INTEGER, media_type TEXT, provider_domain TEXT, provider_item_id TEXT
            );
            CREATE TABLE audio_analysis (item_id TEXT, aa_provider_domain TEXT, analysis_data TEXT);

            INSERT INTO artists (item_id, name) VALUES (1, 'Boards of Canada');
            INSERT INTO tracks (item_id, name, favorite, metadata) VALUES
                (10, 'Track A', 1, '{\"genres\": [\"Ambient\"]}'),
                (11, 'Track B', 0, '{\"genres\": [\"Ambient\"]}'),
                (12, 'Track C', 0, '{\"genres\": [\"Techno\"]}'),
                (13, 'Track D', 0, '{\"genres\": [\"Ambient\"]}'),
                (14, 'Track E', 0, '{\"genres\": [\"Ambient\"]}');
            INSERT INTO track_artists (track_id, artist_id) VALUES (10, 1), (11, 1), (12, 1), (13, 1), (14, 1);
            INSERT INTO albums (item_id, name) VALUES (1, 'Geogaddi');
            -- Deliberately inserted out of disc/track order to prove ordering
            -- isn't coming from insertion order or name order (B < A < C by
            -- name too, which would mask a bug) — disc/track order here is
            -- B (disc1/track1), A (disc1/track2), C (disc2/track1).
            INSERT INTO album_tracks (track_id, album_id, disc_number, track_number) VALUES
                (10, 1, 1, 2),
                (11, 1, 1, 1),
                (12, 1, 2, 1);
            INSERT INTO provider_mappings (item_id, media_type, provider_domain, provider_item_id) VALUES
                (10, 'track', 'filesystem_local', '/m/10.flac'),
                (11, 'track', 'filesystem_local', '/m/11.flac'),
                (12, 'track', 'filesystem_local', '/m/12.flac'),
                (13, 'track', 'filesystem_local', '/m/13.flac'),
                (14, 'track', 'filesystem_local', '/m/14.flac');

            -- Track A: high energy, fast bpm
            INSERT INTO audio_analysis (item_id, aa_provider_domain, analysis_data) VALUES
                ('/m/10.flac', 'sonic_analysis', '{\"energy\": 0.9}'),
                ('/m/10.flac', 'smart_fades', '{\"bpm\": 140.0}');
            -- Track B: low energy, slow bpm
            INSERT INTO audio_analysis (item_id, aa_provider_domain, analysis_data) VALUES
                ('/m/11.flac', 'sonic_analysis', '{\"energy\": 0.1}'),
                ('/m/11.flac', 'smart_fades', '{\"bpm\": 80.0}');
            -- Track C: high energy, fast bpm, different genre
            INSERT INTO audio_analysis (item_id, aa_provider_domain, analysis_data) VALUES
                ('/m/12.flac', 'sonic_analysis', '{\"energy\": 0.95}'),
                ('/m/12.flac', 'smart_fades', '{\"bpm\": 145.0}');
            -- Track D: loudness only, no sonic/bpm analysis at all
            INSERT INTO audio_analysis (item_id, aa_provider_domain, analysis_data) VALUES
                ('/m/13.flac', 'loudness_analysis', '{\"loudness_integrated\": -8.0}');
            -- Track E: no analysis whatsoever
            ",
        )
        .unwrap();
        materialize_audio_features(&conn).unwrap();
        conn
    }

    fn ids(tracks: &[crate::models::Track]) -> Vec<i64> {
        tracks.iter().map(|t| t.id).collect()
    }

    #[test]
    fn no_filters_returns_all_tracks() {
        let conn = test_conn();
        let p = TrackQueryParams { limit: 50, ..Default::default() };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 5);
        assert_eq!(tracks.len(), 5);
    }

    #[test]
    fn no_filters_paginated_honors_requested_order() {
        let conn = test_conn();
        // Default order is by name; previously the fast path for this exact
        // shape (no filters, not random) ignored the order param entirely
        // and always sorted by item_id — name order happens to match here,
        // but dir=desc would expose the bug if it regressed.
        let p = TrackQueryParams { limit: 50, dir: Some("desc".into()), ..Default::default() };
        let (_, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(tracks[0].title.as_deref(), Some("Track E"));
        assert_eq!(tracks[4].title.as_deref(), Some("Track A"));
    }

    #[test]
    fn bpm_filter_random_order_previously_uncovered_shape() {
        // This combination (order=random + a bpm filter) fell through every
        // existing fast path before this rewrite and ran the full 9-17s
        // join. It must now return only the matching tracks.
        let conn = test_conn();
        let p = TrackQueryParams {
            limit: 50,
            bpm_min: Some(130.0),
            order: Some("random".into()),
            ..Default::default()
        };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 2);
        let mut got = ids(&tracks);
        got.sort();
        assert_eq!(got, vec![10, 12]);
    }

    #[test]
    fn energy_filter_non_random_previously_uncovered_shape() {
        // Any audio filter combined with a non-random order also fell
        // through to the slow fallback before this rewrite.
        let conn = test_conn();
        let p = TrackQueryParams {
            limit: 50,
            energy_min: Some(0.5),
            order: Some("name".into()),
            ..Default::default()
        };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 2);
        assert_eq!(tracks[0].title.as_deref(), Some("Track A"));
        assert_eq!(tracks[1].title.as_deref(), Some("Track C"));
    }

    #[test]
    fn bpm_and_energy_combined() {
        let conn = test_conn();
        let p = TrackQueryParams {
            limit: 50,
            bpm_min: Some(130.0),
            energy_max: Some(0.92),
            ..Default::default()
        };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 1);
        assert_eq!(tracks[0].title.as_deref(), Some("Track A"));
    }

    #[test]
    fn genre_filter_combined_with_audio_filter() {
        let conn = test_conn();
        let p = TrackQueryParams {
            limit: 50,
            genre: Some("Techno".into()),
            energy_min: Some(0.5),
            ..Default::default()
        };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 1);
        assert_eq!(tracks[0].title.as_deref(), Some("Track C"));
    }

    #[test]
    fn genre_filter_matches_non_first_tag_in_array() {
        // Track C is tagged ["Techno", "Industrial"] — filtering by the
        // second tag must still match, unlike the old genres[0]-only check.
        let conn = test_conn();
        conn.execute(
            "UPDATE tracks SET metadata = '{\"genres\": [\"Techno\", \"Industrial\"]}' WHERE item_id = 12",
            [],
        ).unwrap();
        let p = TrackQueryParams { limit: 50, genre: Some("Industrial".into()), ..Default::default() };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 1);
        assert_eq!(tracks[0].title.as_deref(), Some("Track C"));
    }

    #[test]
    fn full_genres_array_returned_not_just_first_tag() {
        let conn = test_conn();
        conn.execute(
            "UPDATE tracks SET metadata = '{\"genres\": [\"Techno\", \"Industrial\"]}' WHERE item_id = 12",
            [],
        ).unwrap();
        let p = TrackQueryParams { limit: 50, favorite: None, ..Default::default() };
        let (_, tracks) = list_tracks(&conn, &p).unwrap();
        let track_c = tracks.iter().find(|t| t.id == 12).unwrap();
        assert_eq!(track_c.genres, vec!["Techno".to_string(), "Industrial".to_string()]);
    }

    #[test]
    fn favorite_filter_no_audio_filter() {
        let conn = test_conn();
        let p = TrackQueryParams { limit: 50, favorite: Some(true), ..Default::default() };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 1);
        assert_eq!(tracks[0].title.as_deref(), Some("Track A"));
    }

    #[test]
    fn album_id_filter_defaults_to_disc_and_track_order() {
        // Inserted out of disc/track order and out of name order (see fixture
        // comment) — only a real disc_number/track_number ORDER BY can produce
        // B, A, C here.
        let conn = test_conn();
        let p = TrackQueryParams { limit: 50, album_id: Some(1), ..Default::default() };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 3);
        assert_eq!(
            tracks.iter().map(|t| t.title.clone()).collect::<Vec<_>>(),
            vec![Some("Track B".to_string()), Some("Track A".to_string()), Some("Track C".to_string())],
        );
    }

    #[test]
    fn album_id_filter_explicit_order_overrides_disc_track_default() {
        let conn = test_conn();
        let p = TrackQueryParams {
            limit: 50,
            album_id: Some(1),
            order: Some("name".into()),
            ..Default::default()
        };
        let (_, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(
            tracks.iter().map(|t| t.title.clone()).collect::<Vec<_>>(),
            vec![Some("Track A".to_string()), Some("Track B".to_string()), Some("Track C".to_string())],
        );
    }

    #[test]
    fn exclude_ids_respected_with_audio_filter() {
        let conn = test_conn();
        let p = TrackQueryParams {
            limit: 50,
            bpm_min: Some(100.0),
            exclude: Some("10".into()),
            ..Default::default()
        };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 1);
        assert_eq!(tracks[0].title.as_deref(), Some("Track C"));
    }

    #[test]
    fn tracks_without_analysis_excluded_by_audio_filter() {
        // Track D (loudness only) and Track E (no analysis) must not match
        // any energy/bpm filter — confirms the JOIN to track_audio_features
        // is an inner join, not accidentally permissive.
        let conn = test_conn();
        let p = TrackQueryParams { limit: 50, energy_min: Some(0.0), ..Default::default() };
        let (total, tracks) = list_tracks(&conn, &p).unwrap();
        assert_eq!(total, 3);
        let mut got = ids(&tracks);
        got.sort();
        assert_eq!(got, vec![10, 11, 12]);
    }
}

#[cfg(test)]
mod lyrics_tests {
    use super::get_track;
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tracks (
                item_id INTEGER PRIMARY KEY, name TEXT, duration REAL,
                favorite INTEGER, timestamp_added INTEGER, timestamp_modified INTEGER,
                metadata TEXT
            );
            CREATE TABLE artists (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE track_artists (track_id INTEGER, artist_id INTEGER);
            CREATE TABLE albums (item_id INTEGER PRIMARY KEY, name TEXT, year INTEGER, timestamp_added INTEGER, album_type TEXT, metadata TEXT);
            CREATE TABLE album_tracks (track_id INTEGER, album_id INTEGER, disc_number INTEGER, track_number INTEGER);
            CREATE TABLE provider_mappings (
                item_id INTEGER, media_type TEXT, provider_domain TEXT, provider_item_id TEXT
            );
            CREATE TABLE audio_analysis (item_id TEXT, aa_provider_domain TEXT, analysis_data TEXT);

            INSERT INTO tracks (item_id, name, metadata) VALUES
                (20, 'Dead People', '{\"genres\": [\"Hip Hop\"], \"lyrics\": \"I''m a handle business\"}');
            INSERT INTO provider_mappings (item_id, media_type, provider_domain, provider_item_id)
                VALUES (20, 'track', 'filesystem_local', '/m/20.flac');
            INSERT INTO audio_analysis (item_id, aa_provider_domain, analysis_data) VALUES
                ('/m/20.flac', 'smart_fades', '{\"bpm\": 90.0, \"downbeats\": [0.5, 2.5], \"beats_per_bar\": 4.0}');
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn lyrics_omitted_by_default() {
        let conn = test_conn();
        let track = get_track(&conn, 20, false, false, false).unwrap().unwrap();
        assert_eq!(track.lyrics, None);
    }

    #[test]
    fn lyrics_included_when_requested() {
        let conn = test_conn();
        let track = get_track(&conn, 20, false, false, true).unwrap().unwrap();
        assert_eq!(track.lyrics.as_deref(), Some("I'm a handle business"));
    }

    #[test]
    fn downbeats_and_beats_per_bar_included_with_full_analysis() {
        let conn = test_conn();
        let track = get_track(&conn, 20, true, false, false).unwrap().unwrap();
        let analysis = track.analysis.unwrap();
        assert_eq!(analysis.downbeats, Some(vec![0.5, 2.5]));
        assert_eq!(analysis.beats_per_bar, Some(4.0));
    }
}

#[cfg(test)]
mod album_metadata_tests {
    use super::{get_album, list_albums};
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE albums (
                item_id INTEGER PRIMARY KEY, name TEXT, sort_name TEXT, year INTEGER,
                timestamp_added INTEGER, play_count INTEGER, album_type TEXT, metadata TEXT
            );
            CREATE TABLE album_artists (album_id INTEGER, artist_id INTEGER);
            CREATE TABLE artists (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE album_tracks (track_id INTEGER, album_id INTEGER, disc_number INTEGER, track_number INTEGER);

            INSERT INTO artists (item_id, name) VALUES (88, 'Boards of Canada');
            INSERT INTO albums (item_id, name, year, album_type, metadata) VALUES
                (312, 'Geogaddi', 2002, 'album',
                 '{\"label\": \"Warp Records\", \"release_date\": \"2002-02-18\"}');
            INSERT INTO album_artists (album_id, artist_id) VALUES (312, 88);
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn get_album_includes_label_release_date_and_type() {
        let conn = test_conn();
        let album = get_album(&conn, 312).unwrap().unwrap();
        assert_eq!(album.album_type.as_deref(), Some("album"));
        assert_eq!(album.label.as_deref(), Some("Warp Records"));
        assert_eq!(album.release_date.as_deref(), Some("2002-02-18"));
    }

    #[test]
    fn list_albums_includes_label_release_date_and_type() {
        let conn = test_conn();
        let (total, albums) = list_albums(&conn, 0, 100, None, "name", "asc", None).unwrap();
        assert_eq!(total, 1);
        assert_eq!(albums[0].label.as_deref(), Some("Warp Records"));
    }
}

#[cfg(test)]
mod genre_taxonomy_tests {
    use super::{get_genre, genre_tracks, list_genres};
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tracks (
                item_id INTEGER PRIMARY KEY, name TEXT, duration REAL,
                favorite INTEGER, timestamp_added INTEGER, timestamp_modified INTEGER,
                metadata TEXT
            );
            CREATE TABLE artists (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE track_artists (track_id INTEGER, artist_id INTEGER);
            CREATE TABLE albums (item_id INTEGER PRIMARY KEY, name TEXT, year INTEGER, timestamp_added INTEGER, album_type TEXT, metadata TEXT);
            CREATE TABLE album_tracks (track_id INTEGER, album_id INTEGER, disc_number INTEGER, track_number INTEGER);
            CREATE TABLE provider_mappings (
                item_id INTEGER, media_type TEXT, provider_domain TEXT, provider_item_id TEXT
            );
            CREATE TABLE audio_analysis (item_id TEXT, aa_provider_domain TEXT, analysis_data TEXT);
            CREATE TABLE genres (
                item_id INTEGER PRIMARY KEY, name TEXT, description TEXT, genre_aliases TEXT
            );
            CREATE TABLE genre_media_item_mapping (
                genre_id INTEGER, media_id INTEGER, media_type TEXT
            );

            INSERT INTO genres (item_id, name, description, genre_aliases) VALUES
                (1, 'ambient', NULL, '[\"ambient\", \"Ambient Dub\"]'),
                (2, 'techno', NULL, '[]');
            INSERT INTO tracks (item_id, name) VALUES (10, 'Music Is Math'), (11, 'Sunshine Recorder'), (12, 'Drone');
            INSERT INTO provider_mappings (item_id, media_type, provider_domain, provider_item_id) VALUES
                (10, 'track', 'filesystem_local', '/m/10.flac'),
                (11, 'track', 'filesystem_local', '/m/11.flac'),
                (12, 'track', 'filesystem_local', '/m/12.flac');
            INSERT INTO genre_media_item_mapping (genre_id, media_id, media_type) VALUES
                (1, 10, 'track'), (1, 11, 'track'), (2, 12, 'track');
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn list_genres_returns_track_counts() {
        let conn = test_conn();
        let (total, genres) = list_genres(&conn, 0, 100).unwrap();
        assert_eq!(total, 2);
        let ambient = genres.iter().find(|g| g.name.as_deref() == Some("ambient")).unwrap();
        assert_eq!(ambient.track_count, 2);
        assert_eq!(ambient.aliases, vec!["ambient".to_string(), "Ambient Dub".to_string()]);
    }

    #[test]
    fn get_genre_by_id() {
        let conn = test_conn();
        let genre = get_genre(&conn, 2).unwrap().unwrap();
        assert_eq!(genre.name.as_deref(), Some("techno"));
        assert_eq!(genre.track_count, 1);
    }

    #[test]
    fn genre_tracks_returns_only_mapped_tracks() {
        let conn = test_conn();
        let (total, tracks) = genre_tracks(&conn, 1, 0, 100).unwrap();
        assert_eq!(total, 2);
        let mut ids: Vec<i64> = tracks.iter().map(|t| t.id).collect();
        ids.sort();
        assert_eq!(ids, vec![10, 11]);
    }
}
