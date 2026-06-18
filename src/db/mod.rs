use deadpool_sqlite::{Config as PoolConfig, Pool, Runtime};
use rusqlite::Connection;
use anyhow::Context;

pub mod queries;

pub async fn build_pool(db_path: &str, pool_size: usize) -> anyhow::Result<Pool> {
    // immutable=1: bypass all locking and WAL recovery. MA uses DELETE journal
    // mode but a stale -wal file is present; without immutable, SQLite tries
    // to recover it with a WRITE lock which fails before busy_timeout applies.
    // In DELETE mode all committed data is in the main file, so immutable=1
    // reads the last committed state safely (no stale WAL pages to worry about).
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
