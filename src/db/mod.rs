use deadpool_sqlite::{Config as PoolConfig, Pool, Runtime};
use rusqlite::Connection;
use anyhow::Context;

pub mod queries;

pub async fn build_pool(db_path: &str, pool_size: usize) -> anyhow::Result<Pool> {
    let cfg = PoolConfig::new(db_path);
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
    // Don't set journal_mode — MA owns that; setting it requires a write lock
    // and triggers SQLITE_BUSY. In WAL mode concurrent readers work without it.
    conn.execute_batch("
        PRAGMA query_only=ON;
        PRAGMA busy_timeout=5000;
        PRAGMA temp_store=MEMORY;
        PRAGMA cache_size=-8000;
    ")?;
    Ok(())
}
