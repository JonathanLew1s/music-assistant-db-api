use rusqlite::{params, Connection, OptionalExtension};
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
    let clap_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM audio_analysis
         WHERE aa_provider_domain='sonic_analysis'
           AND json_extract(analysis_data, '$.extra_data.clap_embedding') IS NOT NULL",
        [], |r| r.get(0))?;
    Ok(HealthStats { track_count, schema_version, loudness_count, bpm_count, clap_count, sonic_count })
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
  CAST(json_extract(aa_loud.analysis_data, '$.loudness_integrated') AS REAL) AS loudness_lufs,
  CAST(json_extract(aa_loud.analysis_data, '$.loudness_album') AS REAL) AS loudness_album_lufs,
  CAST(json_extract(aa_fades.analysis_data, '$.bpm') AS REAL) AS bpm,
  json_extract(aa_fades.analysis_data, '$.key') AS fkey,
  json_extract(aa_fades.analysis_data, '$.mode') AS fmode,
  CAST(json_extract(aa_sonic.analysis_data, '$.energy') AS REAL) AS energy,
  CAST(json_extract(aa_sonic.analysis_data, '$.valence') AS REAL) AS valence,
  CAST(json_extract(aa_sonic.analysis_data, '$.danceability') AS REAL) AS danceability,
  CAST(json_extract(aa_sonic.analysis_data, '$.arousal') AS REAL) AS arousal,
  CAST(json_extract(aa_sonic.analysis_data, '$.acousticness') AS REAL) AS acousticness,
  CAST(json_extract(aa_sonic.analysis_data, '$.instrumentalness') AS REAL) AS instrumentalness,
  CAST(json_extract(aa_sonic.analysis_data, '$.brightness') AS REAL) AS brightness,
  CAST(json_extract(aa_sonic.analysis_data, '$.speechiness') AS REAL) AS speechiness,
  CAST(json_extract(aa_sonic.analysis_data, '$.roughness') AS REAL) AS roughness,
  CAST(json_extract(aa_sonic.analysis_data, '$.harmonic_complexity') AS REAL) AS harmonic_complexity,
  CAST(json_extract(aa_sonic.analysis_data, '$.rhythmic_regularity') AS REAL) AS rhythmic_regularity,
  CAST(json_extract(aa_sonic.analysis_data, '$.spectral_centroid') AS REAL) AS spectral_centroid
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

// Extracts the `popularity` field from a track's metadata JSON blob, if present.
fn extract_popularity(metadata: &Option<Value>) -> Option<f64> {
    metadata.as_ref()
        .and_then(|m| m.get("popularity"))
        .and_then(|p| p.as_f64())
}

// Row parser for TRACK_BASE_SCALAR — reads pre-extracted scalar columns instead
// of full JSON blobs. Column layout must match TRACK_BASE_SCALAR exactly.
pub fn parse_track_scalar_row(row: &rusqlite::Row) -> rusqlite::Result<Track> {
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
    let genre = metadata.as_ref()
        .and_then(|m| m.get("genres"))
        .and_then(|g| g.as_array())
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .map(String::from);
    let popularity = extract_popularity(&metadata);

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
        genre,
        popularity,
        duration,
        file_path,
        favorite,
        timestamp_added,
        timestamp_modified,
        cover_url: format!("/api/v1/tracks/{id}/cover"),
        analysis,
    })
}

pub fn parse_track_row(row: &rusqlite::Row, include_analysis: bool, include_arrays: bool, include_clap: bool) -> rusqlite::Result<Track> {
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
    let popularity = extract_popularity(&metadata);

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
        popularity,
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

pub fn list_tracks(conn: &Connection, p: &TrackQueryParams) -> Result<(i64, Vec<Track>)> {
    let mut wheres: Vec<String> = vec!["pm.provider_item_id IS NOT NULL".into()];
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
        wheres.push(format!(
            "json_extract(t.metadata, '$.genres[0]') = ?{}",
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

    let has_audio_filters = p.bpm_min.is_some() || p.bpm_max.is_some()
        || p.energy_min.is_some() || p.energy_max.is_some()
        || p.valence_min.is_some() || p.valence_max.is_some()
        || p.arousal_min.is_some() || p.arousal_max.is_some();

    macro_rules! sonic_filter {
        ($field:expr, $op:expr, $val:expr) => {
            wheres.push(format!(
                "CAST(json_extract(aa_sonic.analysis_data, '$.{}') AS REAL) {} ?{}",
                $field, $op, values.len() + 1
            ));
            values.push(Box::new($val));
        };
    }
    macro_rules! fades_filter {
        ($field:expr, $op:expr, $val:expr) => {
            wheres.push(format!(
                "CAST(json_extract(aa_fades.analysis_data, '$.{}') AS REAL) {} ?{}",
                $field, $op, values.len() + 1
            ));
            values.push(Box::new($val));
        };
    }
    // BPM lives in smart_fades (sonic_analysis.bpm is always null)
    if let Some(v) = p.bpm_min { fades_filter!("bpm", ">=", v); }
    if let Some(v) = p.bpm_max { fades_filter!("bpm", "<=", v); }
    if let Some(v) = p.energy_min { sonic_filter!("energy", ">=", v); }
    if let Some(v) = p.energy_max { sonic_filter!("energy", "<=", v); }
    if let Some(v) = p.valence_min { sonic_filter!("valence", ">=", v); }
    if let Some(v) = p.valence_max { sonic_filter!("valence", "<=", v); }
    if let Some(v) = p.arousal_min { sonic_filter!("arousal", ">=", v); }
    if let Some(v) = p.arousal_max { sonic_filter!("arousal", "<=", v); }

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

    let is_random = p.order.as_deref() == Some("random");
    let order_col = match p.order.as_deref().unwrap_or("name") {
        "timestamp_added" => "t.timestamp_added",
        "timestamp_modified" => "t.timestamp_modified",
        "random" => "RANDOM()",
        _ => "t.name",
    };
    let order_dir = if p.dir.as_deref() == Some("desc") { "DESC" } else { "ASC" };
    let where_clause = wheres.join(" AND ");
    let limit = p.clamped_limit();
    let offset = p.offset;

    // Fast path: random order without audio filters.
    // ORDER BY RANDOM() on the full 5-way join (including 3 audio_analysis tables) across
    // 37K+ rows takes ~9s. Instead: sample item_ids from provider_mappings (indexed, no
    // large JSON columns) then fetch full rows for only those N ids.
    if is_random && !has_audio_filters {
        // Stage 1: randomly sample item_ids from the lightweight provider_mappings join.
        // provider_mappings has an index on (media_type, provider_domain) so this is fast.
        // Build lightweight where clauses that don't need audio_analysis joins.
        let mut pm_wheres: Vec<String> = vec![
            "pm.media_type='track'".into(),
            "pm.provider_domain='filesystem_local'".into(),
        ];
        let mut pm_values: Vec<Box<dyn rusqlite::ToSql>> = vec![];

        if let Some(since) = p.since {
            pm_wheres.push(format!("t.timestamp_modified > ?{}", pm_values.len() + 1));
            pm_values.push(Box::new(since));
        }
        if let Some(fav) = p.favorite {
            pm_wheres.push(format!("t.favorite = ?{}", pm_values.len() + 1));
            pm_values.push(Box::new(fav as i64));
        }
        if let Some(ref genre) = p.genre {
            pm_wheres.push(format!(
                "json_extract(t.metadata, '$.genres[0]') = ?{}",
                pm_values.len() + 1
            ));
            pm_values.push(Box::new(genre.clone()));
        }
        if let Some(artist_id) = p.artist_id {
            pm_wheres.push(format!(
                "EXISTS (SELECT 1 FROM track_artists ta2 WHERE ta2.track_id = t.item_id AND ta2.artist_id = ?{})",
                pm_values.len() + 1
            ));
            pm_values.push(Box::new(artist_id));
        }
        if let Some(album_id) = p.album_id {
            pm_wheres.push(format!(
                "EXISTS (SELECT 1 FROM album_tracks at3 WHERE at3.track_id = t.item_id AND at3.album_id = ?{})",
                pm_values.len() + 1
            ));
            pm_values.push(Box::new(album_id));
        }
        if !exclude_ids.is_empty() {
            let placeholders: Vec<String> = (0..exclude_ids.len())
                .map(|i| format!("?{}", pm_values.len() + i + 1))
                .collect();
            pm_wheres.push(format!("t.item_id NOT IN ({})", placeholders.join(",")));
            for id in &exclude_ids {
                pm_values.push(Box::new(*id));
            }
        }
        let pm_where = pm_wheres.join(" AND ");

        // Lightweight count — same join, no audio_analysis needed.
        let count_sql = format!(
            "SELECT COUNT(DISTINCT t.item_id)
             FROM tracks t
             JOIN provider_mappings pm ON pm.item_id = t.item_id
             WHERE {pm_where}"
        );
        let total: i64 = conn.query_row(
            &count_sql,
            rusqlite::params_from_iter(pm_values.iter()),
            |r| r.get(0),
        )?;

        pm_values.push(Box::new(limit));
        let id_sql = format!(
            "SELECT pm.item_id FROM provider_mappings pm
             JOIN tracks t ON t.item_id = pm.item_id
             WHERE {pm_where}
             ORDER BY RANDOM()
             LIMIT ?{}",
            pm_values.len()
        );
        let mut id_stmt = conn.prepare(&id_sql)?;
        let sampled_ids: Vec<i64> = id_stmt.query_map(
            rusqlite::params_from_iter(pm_values.iter()),
            |row| row.get(0),
        )?.collect::<rusqlite::Result<_>>()?;

        if sampled_ids.is_empty() {
            return Ok((total, vec![]));
        }

        // Stage 2: fetch full joined rows for the sampled ids only.
        let id_placeholders: Vec<String> = (1..=sampled_ids.len())
            .map(|i| format!("?{i}"))
            .collect();
        let include_analysis = p.include_analysis();
        let include_arrays = p.include_arrays();
        let include_clap = p.include_clap();

        let tracks: Vec<Track> = if include_analysis && !include_arrays {
            let data_sql = format!(
                "{TRACK_BASE_SCALAR} AND t.item_id IN ({})
                 GROUP BY t.item_id ORDER BY RANDOM()",
                id_placeholders.join(",")
            );
            let mut stmt = conn.prepare(&data_sql)?;
            let x = stmt.query_map(
                rusqlite::params_from_iter(sampled_ids.iter()),
                |row| parse_track_scalar_row(row),
            )?.collect::<rusqlite::Result<_>>()?; x
        } else {
            let data_sql = format!(
                "{TRACK_BASE} AND t.item_id IN ({})
                 GROUP BY t.item_id ORDER BY RANDOM()",
                id_placeholders.join(",")
            );
            let mut stmt = conn.prepare(&data_sql)?;
            let x = stmt.query_map(
                rusqlite::params_from_iter(sampled_ids.iter()),
                |row| parse_track_row(row, include_analysis, include_arrays, include_clap),
            )?.collect::<rusqlite::Result<_>>()?; x
        };

        return Ok((total, tracks));
    }

    // Fast path: random order with sonic filters only (energy / valence / arousal, no BPM).
    // ORDER BY RANDOM() on the full 5-way join takes ~17s for 5K+ matching rows.
    // Instead: scan the small sonic_analysis table (~7K rows) which already holds the
    // analysis_data we need for filtering, sample item_ids there, then fetch full rows
    // for only those N ids — same two-stage idea as the no-filter random fast path.
    let has_bpm_filters = p.bpm_min.is_some() || p.bpm_max.is_some();
    let has_sonic_only_filters = has_audio_filters && !has_bpm_filters;

    if is_random && has_sonic_only_filters {
        let mut aa_wheres: Vec<String> = vec![
            "aa.aa_provider_domain = 'sonic_analysis'".into(),
            "pm.media_type = 'track'".into(),
            "pm.provider_domain = 'filesystem_local'".into(),
        ];
        let mut aa_values: Vec<Box<dyn rusqlite::ToSql>> = vec![];

        // Energy / valence / arousal — all live in sonic_analysis.analysis_data
        if let Some(v) = p.energy_min {
            aa_wheres.push(format!(
                "CAST(json_extract(aa.analysis_data, '$.energy') AS REAL) >= ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(v));
        }
        if let Some(v) = p.energy_max {
            aa_wheres.push(format!(
                "CAST(json_extract(aa.analysis_data, '$.energy') AS REAL) <= ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(v));
        }
        if let Some(v) = p.valence_min {
            aa_wheres.push(format!(
                "CAST(json_extract(aa.analysis_data, '$.valence') AS REAL) >= ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(v));
        }
        if let Some(v) = p.valence_max {
            aa_wheres.push(format!(
                "CAST(json_extract(aa.analysis_data, '$.valence') AS REAL) <= ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(v));
        }
        if let Some(v) = p.arousal_min {
            aa_wheres.push(format!(
                "CAST(json_extract(aa.analysis_data, '$.arousal') AS REAL) >= ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(v));
        }
        if let Some(v) = p.arousal_max {
            aa_wheres.push(format!(
                "CAST(json_extract(aa.analysis_data, '$.arousal') AS REAL) <= ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(v));
        }

        // Standard filters (since, favorite, genre, artist_id, album_id, exclude)
        if let Some(since) = p.since {
            aa_wheres.push(format!("t.timestamp_modified > ?{}", aa_values.len() + 1));
            aa_values.push(Box::new(since));
        }
        if let Some(fav) = p.favorite {
            aa_wheres.push(format!("t.favorite = ?{}", aa_values.len() + 1));
            aa_values.push(Box::new(fav as i64));
        }
        if let Some(ref genre) = p.genre {
            aa_wheres.push(format!(
                "json_extract(t.metadata, '$.genres[0]') = ?{}",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(genre.clone()));
        }
        if let Some(artist_id) = p.artist_id {
            aa_wheres.push(format!(
                "EXISTS (SELECT 1 FROM track_artists ta2 WHERE ta2.track_id = t.item_id AND ta2.artist_id = ?{})",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(artist_id));
        }
        if let Some(album_id) = p.album_id {
            aa_wheres.push(format!(
                "EXISTS (SELECT 1 FROM album_tracks at3 WHERE at3.track_id = t.item_id AND at3.album_id = ?{})",
                aa_values.len() + 1
            ));
            aa_values.push(Box::new(album_id));
        }
        if !exclude_ids.is_empty() {
            let placeholders: Vec<String> = (0..exclude_ids.len())
                .map(|i| format!("?{}", aa_values.len() + i + 1))
                .collect();
            aa_wheres.push(format!("t.item_id NOT IN ({})", placeholders.join(",")));
            for id in &exclude_ids {
                aa_values.push(Box::new(*id));
            }
        }
        let aa_where = aa_wheres.join(" AND ");

        let count_sql = format!(
            "SELECT COUNT(DISTINCT pm.item_id)
             FROM audio_analysis aa
             JOIN provider_mappings pm ON pm.provider_item_id = aa.item_id
             JOIN tracks t ON t.item_id = pm.item_id
             WHERE {aa_where}"
        );
        let total: i64 = conn.query_row(
            &count_sql,
            rusqlite::params_from_iter(aa_values.iter()),
            |r| r.get(0),
        )?;

        aa_values.push(Box::new(limit));
        let id_sql = format!(
            "SELECT DISTINCT pm.item_id
             FROM audio_analysis aa
             JOIN provider_mappings pm ON pm.provider_item_id = aa.item_id
             JOIN tracks t ON t.item_id = pm.item_id
             WHERE {aa_where}
             ORDER BY RANDOM()
             LIMIT ?{}",
            aa_values.len()
        );
        let mut id_stmt = conn.prepare(&id_sql)?;
        let sampled_ids: Vec<i64> = id_stmt.query_map(
            rusqlite::params_from_iter(aa_values.iter()),
            |row| row.get(0),
        )?.collect::<rusqlite::Result<_>>()?;

        if sampled_ids.is_empty() {
            return Ok((total, vec![]));
        }

        // Stage 2: fetch full joined rows for the sampled ids only.
        let id_placeholders: Vec<String> = (1..=sampled_ids.len())
            .map(|i| format!("?{i}"))
            .collect();
        let include_analysis = p.include_analysis();
        let include_arrays = p.include_arrays();
        let include_clap = p.include_clap();

        let tracks: Vec<Track> = if include_analysis && !include_arrays {
            let data_sql = format!(
                "{TRACK_BASE_SCALAR} AND t.item_id IN ({})
                 GROUP BY t.item_id ORDER BY RANDOM()",
                id_placeholders.join(",")
            );
            let mut stmt = conn.prepare(&data_sql)?;
            let x = stmt.query_map(
                rusqlite::params_from_iter(sampled_ids.iter()),
                |row| parse_track_scalar_row(row),
            )?.collect::<rusqlite::Result<_>>()?; x
        } else {
            let data_sql = format!(
                "{TRACK_BASE} AND t.item_id IN ({})
                 GROUP BY t.item_id ORDER BY RANDOM()",
                id_placeholders.join(",")
            );
            let mut stmt = conn.prepare(&data_sql)?;
            let x = stmt.query_map(
                rusqlite::params_from_iter(sampled_ids.iter()),
                |row| parse_track_row(row, include_analysis, include_arrays, include_clap),
            )?.collect::<rusqlite::Result<_>>()?; x
        };

        return Ok((total, tracks));
    }

    // Two-stage pagination fast path: use lightweight provider_mappings enumeration
    // then fetch full rows only for the page's IDs. Same idea as the random fast path
    // but with deterministic LIMIT/OFFSET ordering instead of RANDOM().
    //
    // Used when: not random AND (no audio filters OR sole filter is energy_min=0,
    // which means "has sonic_analysis" — a JOIN presence check, not a value filter).
    // This covers the Observatory bulk fetch (energy_min=0 to select analysed tracks).
    let is_sonic_presence_only = p.energy_min == Some(0.0)
        && p.energy_max.is_none()
        && p.bpm_min.is_none() && p.bpm_max.is_none()
        && p.valence_min.is_none() && p.valence_max.is_none()
        && p.arousal_min.is_none() && p.arousal_max.is_none();
    let use_two_stage_paged = !is_random && (!has_audio_filters || is_sonic_presence_only);

    if use_two_stage_paged {
        let mut pm_wheres: Vec<String> = vec![
            "pm.media_type='track'".into(),
            "pm.provider_domain='filesystem_local'".into(),
        ];
        let mut pm_values: Vec<Box<dyn rusqlite::ToSql>> = vec![];

        if let Some(since) = p.since {
            pm_wheres.push(format!("t.timestamp_modified > ?{}", pm_values.len() + 1));
            pm_values.push(Box::new(since));
        }
        if let Some(fav) = p.favorite {
            pm_wheres.push(format!("t.favorite = ?{}", pm_values.len() + 1));
            pm_values.push(Box::new(fav as i64));
        }
        if let Some(ref genre) = p.genre {
            pm_wheres.push(format!(
                "json_extract(t.metadata, '$.genres[0]') = ?{}",
                pm_values.len() + 1
            ));
            pm_values.push(Box::new(genre.clone()));
        }
        if let Some(artist_id) = p.artist_id {
            pm_wheres.push(format!(
                "EXISTS (SELECT 1 FROM track_artists ta2 WHERE ta2.track_id = t.item_id AND ta2.artist_id = ?{})",
                pm_values.len() + 1
            ));
            pm_values.push(Box::new(artist_id));
        }
        if let Some(album_id) = p.album_id {
            pm_wheres.push(format!(
                "EXISTS (SELECT 1 FROM album_tracks at3 WHERE at3.track_id = t.item_id AND at3.album_id = ?{})",
                pm_values.len() + 1
            ));
            pm_values.push(Box::new(album_id));
        }
        if !exclude_ids.is_empty() {
            let placeholders: Vec<String> = (0..exclude_ids.len())
                .map(|i| format!("?{}", pm_values.len() + i + 1))
                .collect();
            pm_wheres.push(format!("t.item_id NOT IN ({})", placeholders.join(",")));
            for id in &exclude_ids {
                pm_values.push(Box::new(*id));
            }
        }
        let pm_where = pm_wheres.join(" AND ");

        // For sonic_presence_only: inner-join to audio_analysis to keep only analysed tracks.
        let (count_sql, id_sql_template) = if is_sonic_presence_only {
            let count = format!(
                "SELECT COUNT(DISTINCT pm.item_id)
                 FROM provider_mappings pm
                 JOIN tracks t ON t.item_id = pm.item_id
                 JOIN audio_analysis aa_sonic ON aa_sonic.item_id = pm.provider_item_id
                   AND aa_sonic.aa_provider_domain = 'sonic_analysis'
                 WHERE {pm_where}"
            );
            // Use DISTINCT so duplicates from multiple aa_sonic rows per track don't
            // eat into the LIMIT (the UNIQUE constraint is on 4 cols; same item_id can
            // appear more than once if 'provider' differs).
            let ids = format!(
                "SELECT DISTINCT pm.item_id FROM provider_mappings pm
                 JOIN tracks t ON t.item_id = pm.item_id
                 JOIN audio_analysis aa_sonic ON aa_sonic.item_id = pm.provider_item_id
                   AND aa_sonic.aa_provider_domain = 'sonic_analysis'
                 WHERE {pm_where}
                 ORDER BY pm.item_id ASC
                 LIMIT ?{{limit}} OFFSET ?{{offset}}"
            );
            (count, ids)
        } else {
            let count = format!(
                "SELECT COUNT(DISTINCT t.item_id)
                 FROM tracks t
                 JOIN provider_mappings pm ON pm.item_id = t.item_id
                 WHERE {pm_where}"
            );
            let ids = format!(
                "SELECT DISTINCT pm.item_id FROM provider_mappings pm
                 JOIN tracks t ON t.item_id = pm.item_id
                 WHERE {pm_where}
                 ORDER BY pm.item_id ASC
                 LIMIT ?{{limit}} OFFSET ?{{offset}}"
            );
            (count, ids)
        };

        let total: i64 = conn.query_row(
            &count_sql,
            rusqlite::params_from_iter(pm_values.iter()),
            |r| r.get(0),
        )?;

        // Bind LIMIT and OFFSET, replacing the placeholder templates.
        let limit_pos = pm_values.len() + 1;
        let offset_pos = pm_values.len() + 2;
        let id_sql = id_sql_template
            .replace("{limit}", &limit_pos.to_string())
            .replace("{offset}", &offset_pos.to_string());
        pm_values.push(Box::new(limit));
        pm_values.push(Box::new(offset));

        let mut id_stmt = conn.prepare(&id_sql)?;
        let page_ids: Vec<i64> = id_stmt.query_map(
            rusqlite::params_from_iter(pm_values.iter()),
            |row| row.get(0),
        )?.collect::<rusqlite::Result<_>>()?;

        if page_ids.is_empty() {
            return Ok((total, vec![]));
        }

        let id_placeholders: Vec<String> = (1..=page_ids.len())
            .map(|i| format!("?{i}"))
            .collect();
        let include_analysis = p.include_analysis();
        let include_arrays = p.include_arrays();
        let include_clap = p.include_clap();

        // Use the scalar base query when arrays aren't needed — avoids transmitting
        // the full analysis_data blobs (CLAP embeddings, beats arrays) from SQLite.
        let tracks: Vec<Track> = if include_analysis && !include_arrays {
            let data_sql = format!(
                "{TRACK_BASE_SCALAR} AND t.item_id IN ({})
                 GROUP BY t.item_id",
                id_placeholders.join(",")
            );
            let mut stmt = conn.prepare(&data_sql)?;
            let x = stmt.query_map(
                rusqlite::params_from_iter(page_ids.iter()),
                |row| parse_track_scalar_row(row),
            )?.collect::<rusqlite::Result<_>>()?; x
        } else {
            let data_sql = format!(
                "{TRACK_BASE} AND t.item_id IN ({})
                 GROUP BY t.item_id",
                id_placeholders.join(",")
            );
            let mut stmt = conn.prepare(&data_sql)?;
            let x = stmt.query_map(
                rusqlite::params_from_iter(page_ids.iter()),
                |row| parse_track_row(row, include_analysis, include_arrays, include_clap),
            )?.collect::<rusqlite::Result<_>>()?; x
        };

        return Ok((total, tracks));
    }

    let count_sql = format!(
        "SELECT COUNT(DISTINCT t.item_id)
         FROM tracks t
         LEFT JOIN track_artists ta ON ta.track_id = t.item_id
         LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
         LEFT JOIN provider_mappings pm ON pm.item_id = t.item_id AND pm.media_type='track' AND pm.provider_domain='filesystem_local'
         LEFT JOIN audio_analysis aa_sonic ON aa_sonic.item_id = pm.provider_item_id AND aa_sonic.aa_provider_domain='sonic_analysis'
         LEFT JOIN audio_analysis aa_fades ON aa_fades.item_id = pm.provider_item_id AND aa_fades.aa_provider_domain='smart_fades'
         WHERE {where_clause}"
    );
    let total: i64 = conn.query_row(&count_sql, rusqlite::params_from_iter(values.iter()), |r| r.get(0))?;

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

    let include_analysis = p.include_analysis();
    let include_arrays = p.include_arrays();
    let include_clap = p.include_clap();
    let mut stmt = conn.prepare(&data_sql)?;
    let tracks: Vec<Track> = stmt.query_map(
        rusqlite::params_from_iter(values.iter()),
        |row| parse_track_row(row, include_analysis, include_arrays, include_clap),
    )?.collect::<rusqlite::Result<_>>()?;

    Ok((total, tracks))
}

pub fn get_track(conn: &Connection, id: i64, include_analysis: bool, include_clap: bool) -> Result<Option<Track>> {
    let sql = format!(
        "{TRACK_BASE} AND t.item_id = ?1
         GROUP BY t.item_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(params![id], |row| parse_track_row(row, include_analysis, true, include_clap))?;
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
    // Single LEFT JOIN with conditional aggregation instead of two separate
    // audio_analysis LEFT JOINs — halves the number of full-table scans on the
    // 93K-row unindexed audio_analysis table (read-only mount, can't add indexes).
    let sql = format!("
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
  MAX(CASE WHEN aa_extra.aa_provider_domain='loudness_analysis'
      THEN CAST(json_extract(aa_extra.analysis_data, '$.loudness_integrated') AS REAL) END) AS loudness_lufs,
  MAX(CASE WHEN aa_extra.aa_provider_domain='loudness_analysis'
      THEN CAST(json_extract(aa_extra.analysis_data, '$.loudness_album') AS REAL) END) AS loudness_album_lufs,
  MAX(CASE WHEN aa_extra.aa_provider_domain='smart_fades'
      THEN CAST(json_extract(aa_extra.analysis_data, '$.bpm') AS REAL) END) AS bpm,
  MAX(CASE WHEN aa_extra.aa_provider_domain='smart_fades'
      THEN json_extract(aa_extra.analysis_data, '$.key') END) AS fkey,
  MAX(CASE WHEN aa_extra.aa_provider_domain='smart_fades'
      THEN json_extract(aa_extra.analysis_data, '$.mode') END) AS fmode,
  CAST(json_extract(aa_sonic.analysis_data, '$.energy') AS REAL) AS energy,
  CAST(json_extract(aa_sonic.analysis_data, '$.valence') AS REAL) AS valence,
  CAST(json_extract(aa_sonic.analysis_data, '$.danceability') AS REAL) AS danceability,
  CAST(json_extract(aa_sonic.analysis_data, '$.arousal') AS REAL) AS arousal,
  CAST(json_extract(aa_sonic.analysis_data, '$.acousticness') AS REAL) AS acousticness,
  CAST(json_extract(aa_sonic.analysis_data, '$.instrumentalness') AS REAL) AS instrumentalness,
  CAST(json_extract(aa_sonic.analysis_data, '$.brightness') AS REAL) AS brightness,
  CAST(json_extract(aa_sonic.analysis_data, '$.speechiness') AS REAL) AS speechiness,
  CAST(json_extract(aa_sonic.analysis_data, '$.roughness') AS REAL) AS roughness,
  CAST(json_extract(aa_sonic.analysis_data, '$.harmonic_complexity') AS REAL) AS harmonic_complexity,
  CAST(json_extract(aa_sonic.analysis_data, '$.rhythmic_regularity') AS REAL) AS rhythmic_regularity,
  CAST(json_extract(aa_sonic.analysis_data, '$.spectral_centroid') AS REAL) AS spectral_centroid
FROM audio_analysis aa_sonic
JOIN provider_mappings pm
  ON pm.provider_item_id = aa_sonic.item_id
  AND pm.media_type = 'track'
  AND pm.provider_domain = 'filesystem_local'
JOIN tracks t ON t.item_id = pm.item_id
LEFT JOIN track_artists ta ON ta.track_id = t.item_id
LEFT JOIN artists a ON a.item_id = ta.artist_id
LEFT JOIN album_tracks at2 ON at2.track_id = t.item_id
LEFT JOIN albums alb ON alb.item_id = at2.album_id
LEFT JOIN audio_analysis aa_extra
  ON aa_extra.item_id = aa_sonic.item_id
  AND aa_extra.aa_provider_domain IN ('loudness_analysis', 'smart_fades')
WHERE aa_sonic.aa_provider_domain = 'sonic_analysis'
GROUP BY t.item_id
ORDER BY t.item_id ASC
");
    let mut stmt = conn.prepare(&sql)?;
    let tracks = stmt.query_map([], |row| parse_track_scalar_row(row))?
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

pub struct SearchResults {
    pub tracks: Vec<Track>,
    pub albums: Vec<Album>,
    pub artists: Vec<Artist>,
}

pub fn search(conn: &Connection, q: &str, limit: i64) -> Result<SearchResults> {
    let pattern = format!("%{q}%");

    let track_sql = format!(
        "{TRACK_BASE} AND (t.name LIKE ?1 OR a.name LIKE ?1)
         GROUP BY t.item_id ORDER BY t.name ASC LIMIT ?2"
    );
    let mut stmt = conn.prepare(&track_sql)?;
    let tracks: Vec<Track> = stmt.query_map(params![pattern, limit], |row| {
        parse_track_row(row, false, false, false)
    })?.collect::<rusqlite::Result<_>>()?;

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
