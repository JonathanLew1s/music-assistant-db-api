use deadpool_sqlite::{Config as PoolConfig, Pool, Runtime};
use rusqlite::Connection;
use anyhow::Context;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod queries;

// db_path always points at a Longhorn PVC-to-PVC clone of MA's live
// library.db, refreshed hourly by a CronJob outside this process (see the
// talos GitOps repo) — never at the live, actively-written file directly.
// Reading the live DB under immutable=1 while MA is mid-write produces
// "database disk image is malformed" errors (confirmed in production: MA's
// library.db is in WAL mode and gets held under sustained write contention
// during a full analysis pass — observed continuous for 8+ days — so there
// is no quiet gap to read during). A block-level clone sidesteps SQLite's
// locking entirely and produces a fully recoverable, integrity-check-clean
// copy (confirmed live). The clone is static between refreshes — a refresh
// remounts via a full pod restart, not an in-place update — so SharedPool
// here never actually swaps in this process's lifetime; it's kept purely so
// every route handler has one consistent way to resolve the current pool.
pub type SharedPool = Arc<RwLock<Pool>>;

pub async fn current(shared: &SharedPool) -> Pool {
    shared.read().await.clone()
}

// A freshly-cloned volume can have a torn tail in its -wal file if Longhorn
// captured it mid-write — opening with NORMAL (non-immutable) flags here,
// once, lets SQLite's own WAL recovery discard that tail and checkpoint to
// a consistent state, exactly as it would after an unclean shutdown. Must
// run before build_pool, which deliberately uses immutable=1 and therefore
// skips this recovery step entirely.
//
// Also where we materialize track_audio_features (see queries::materialize_
// audio_features) — flattening the energy/valence/arousal/bpm JSON fields
// in audio_analysis into a real typed table with real btree indexes, once,
// rather than leaving every query to re-run json_extract over the JSON blob
// (which includes a 1024-dim CLAP embedding) on every row at request time.
// Confirmed live: as MA's analysis coverage has grown (~7.8K -> 13.3K
// analysed tracks in one day), that per-request parsing cost grew right
// alongside it, to the point of exceeding callers' HTTP timeouts (measured
// ~16s against a 10s client timeout). An expression index on
// CAST(json_extract(...)) (the previous approach here) helps single-column
// equality/range filters but can't be combined or composed the way a normal
// column index can — a real flattened table fixes that generically instead
// of one filter shape at a time. We could never do this against MA's own
// live library.db (an unrequested schema change to someone else's
// database), but we own this clone outright, so it's free to optimize.
pub async fn recover_wal(db_path: &str) -> anyhow::Result<()> {
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let conn = Connection::open(&db_path)?;
        conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |_| Ok(()))?;
        queries::materialize_audio_features(&conn)?;
        // album_tracks' only native index leads with track_id, not album_id
        // (confirmed via PRAGMA index_info against the live clone), so every
        // album-scoped track listing was an unindexed scan of the whole
        // table. This process owns the clone outright for its lifetime —
        // same justification as materialize_audio_features above — but
        // unlike that derived table, this indexes a native MA table
        // directly, since no data transformation is needed here, just an
        // index SQLite's own schema never shipped. IF NOT EXISTS because a
        // freshly-cloned file never has it (each refresh replaces the file
        // wholesale, wiping any index added in a prior pod's boot), so this
        // must be safe to (re)run unconditionally every boot.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_album_tracks_album_id
             ON album_tracks(album_id, disc_number, track_number);",
        )?;
        conn.execute_batch("ANALYZE;")?;
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("WAL recovery / index prep task panicked: {e}"))?
}

pub async fn build_pool(db_path: &str, pool_size: usize) -> anyhow::Result<Pool> {
    // immutable=1: bypass all locking and WAL recovery — safe here only
    // because recover_wal() has already run against this exact path first.
    let uri = format!("file:{}?immutable=1", db_path);
    let cfg = PoolConfig::new(uri);
    let pool = cfg.builder(Runtime::Tokio1)?
        .max_size(pool_size)
        .build()?;

    let conn = pool.get().await.context("initial DB connection failed")?;
    conn.interact(|c| configure_connection(c))
        .await
        .map_err(|e| anyhow::anyhow!("pool interact error: {e}"))??;

    Ok(pool)
}

pub fn configure_connection(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch("
        PRAGMA query_only=ON;
        PRAGMA temp_store=MEMORY;
        PRAGMA cache_size=-32000;
        PRAGMA mmap_size=268435456;
    ")?;
    Ok(())
}

#[cfg(test)]
mod recover_wal_tests {
    use super::recover_wal;
    use rusqlite::Connection;
    use std::path::PathBuf;

    // recover_wal() needs a real file path (it opens with non-immutable
    // flags for WAL recovery, then later build_pool() reopens the same path
    // immutable=1) — an in-memory connection can't stand in for that, so
    // this writes a real temp file rather than adding a tempfile dependency
    // for one test.
    fn temp_db_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ma-db-api-test-{name}-{}.db", std::process::id()))
    }

    fn seed_minimal_schema(path: &PathBuf) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(
            "
            CREATE TABLE tracks (item_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE provider_mappings (
                item_id INTEGER, media_type TEXT, provider_domain TEXT, provider_item_id TEXT
            );
            CREATE TABLE audio_analysis (item_id TEXT, aa_provider_domain TEXT, analysis_data TEXT);
            CREATE TABLE album_tracks (track_id INTEGER, album_id INTEGER, disc_number INTEGER, track_number INTEGER);
            ",
        )
        .unwrap();
    }

    // Simulates a pod restart against a freshly-cloned file: every refresh
    // replaces library.db wholesale, so any index this process added in a
    // prior boot is gone — recover_wal() must succeed and produce the same
    // schema starting from a clean file every single time, not just once.
    #[test]
    fn idempotent_across_repeated_boots_against_a_clean_file() {
        let path = temp_db_path("idempotent");
        let _ = std::fs::remove_file(&path);
        seed_minimal_schema(&path);

        let path_str = path.to_str().unwrap().to_string();
        for _ in 0..2 {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(recover_wal(&path_str))
                .unwrap();
        }

        let conn = Connection::open(&path).unwrap();
        let index_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_album_tracks_album_id'",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(index_count, 1);

        let _ = std::fs::remove_file(&path);
    }
}
