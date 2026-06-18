use deadpool_sqlite::{Config as PoolConfig, Pool, Runtime};
use rusqlite::Connection;
use anyhow::Context;

pub mod queries;

pub async fn build_pool(db_path: &str, pool_size: usize) -> anyhow::Result<Pool> {
    // mode=ro opens read-only without taking any file locks. SQLite in WAL mode
    // uses an in-memory shm for read-only openers so the read-only volume mount
    // is fine. Unlike immutable=1, pages are re-read from disk on each query so
    // the sidecar always sees MA's latest writes rather than a stale snapshot
    // that goes "malformed" after a WAL checkpoint.
    let uri = format!("file:{}?mode=ro", db_path);
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
