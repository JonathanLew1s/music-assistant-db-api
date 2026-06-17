use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Clone)]
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
    pub speechiness: Option<f64>,
    pub roughness: Option<f64>,
    pub harmonic_complexity: Option<f64>,
    pub rhythmic_regularity: Option<f64>,
    pub spectral_centroid: Option<f64>,
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
