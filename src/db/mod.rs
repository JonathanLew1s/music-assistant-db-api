use deadpool_sqlite::{Config as PoolConfig, Pool, Runtime};
use rusqlite::Connection;
use anyhow::Context;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod queries;

// The query-serving pool always points at a periodically-refreshed snapshot
// file (see snapshot.rs), never at MA's live, actively-written library.db —
// reading a live DB under immutable=1 while MA is mid-write produces
// "database disk image is malformed" errors (confirmed in production: an
// actively-growing -wal file means immutable=1's "no stale WAL pages to
// worry about" assumption no longer holds). Each successful snapshot
// rebuilds an entirely new Pool and swaps it in here — existing deadpool
// connections hold their own open file descriptors and won't pick up a
// rename on their own, so the only correct way to serve fresh data is to
// discard the old Pool and build a new one against the newly-published file.
pub type SharedPool = Arc<RwLock<Pool>>;

pub async fn current(shared: &SharedPool) -> Pool {
    shared.read().await.clone()
}

pub async fn build_pool(db_path: &str, pool_size: usize) -> anyhow::Result<Pool> {
    // immutable=1: bypass all locking and WAL recovery — safe and fast here
    // because db_path is always a snapshot file (see snapshot.rs) that is
    // genuinely never written to again once this Pool exists; the next
    // refresh builds a brand new Pool against a brand new file rather than
    // mutating this one. Using immutable=1 directly against MA's live,
    // actively-written library.db (the old approach) is what produced
    // "database disk image is malformed" errors once MA started writing
    // through an active WAL rather than a stale one.
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
