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
pub async fn recover_wal(db_path: &str) -> anyhow::Result<()> {
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let conn = Connection::open(&db_path)?;
        conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |_| Ok(()))?;
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("WAL recovery task panicked: {e}"))?
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
