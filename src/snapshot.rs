// Periodic snapshotting of MA's live library.db — decouples every query the
// API serves from MA's own write activity entirely, instead of trying to
// read the live file directly under increasingly fragile open-mode flags.
//
// `take_snapshot` uses SQLite's `VACUUM INTO` — the engine's own
// purpose-built mechanism for producing a fully consistent, single-file
// copy of a live database regardless of journal mode, run against a plain
// read connection (not immutable) so it correctly participates in whatever
// locking/WAL coordination the live file actually needs at that moment. If
// MA happens to be mid-write when a snapshot attempt runs, the attempt
// simply fails for that cycle (logged, not fatal) and the API keeps serving
// the last successfully published snapshot — never the live file.

use std::path::Path;
use std::time::Duration;
use rusqlite::Connection;

use crate::db::{self, SharedPool};

fn tmp_path_for(snapshot_path: &str) -> String {
    format!("{snapshot_path}.tmp")
}

// Blocking — must be called via `spawn_blocking`, never directly on the
// async runtime (rusqlite is synchronous).
fn take_snapshot_blocking(source_path: &str, snapshot_path: &str) -> anyhow::Result<()> {
    let tmp = tmp_path_for(snapshot_path);
    // VACUUM INTO refuses to write to a file that already exists — clear any
    // leftover tmp from a prior failed/interrupted attempt first.
    if Path::new(&tmp).exists() {
        std::fs::remove_file(&tmp)?;
    }

    // Plain read connection, NOT immutable — this needs to correctly perform
    // a live read against a database another process may be actively
    // writing, which is exactly what normal (non-immutable) SQLite locking
    // and WAL-aware reads are designed to do.
    let conn = Connection::open_with_flags(
        source_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    conn.busy_timeout(Duration::from_secs(10))?;
    conn.execute("VACUUM INTO ?1", [&tmp])?;
    drop(conn);

    // Atomic on the same filesystem — readers that open snapshot_path either
    // see the old complete file or the new complete file, never a partial one.
    std::fs::rename(&tmp, snapshot_path)?;
    Ok(())
}

pub async fn take_snapshot(source_path: String, snapshot_path: String) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || take_snapshot_blocking(&source_path, &snapshot_path))
        .await
        .map_err(|e| anyhow::anyhow!("snapshot task panicked: {e}"))?
}

// Background loop: every `interval`, attempt a fresh snapshot and — only on
// success — build an entirely new Pool against it and swap it into
// `shared_pool`. A failed attempt (e.g. MA mid-write right now) is logged
// and skipped; the previous Pool keeps serving traffic unchanged.
pub async fn run_periodic(
    shared_pool: SharedPool,
    source_path: String,
    snapshot_path: String,
    pool_size: usize,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await; // first tick fires immediately; the caller already took the initial snapshot
    loop {
        ticker.tick().await;
        match take_snapshot(source_path.clone(), snapshot_path.clone()).await {
            Ok(()) => {
                match db::build_pool(&snapshot_path, pool_size).await {
                    Ok(new_pool) => {
                        *shared_pool.write().await = new_pool;
                        tracing::info!("snapshot refreshed: {snapshot_path}");
                    }
                    Err(e) => tracing::warn!("snapshot refresh succeeded but rebuilding the pool failed (keeping previous pool): {e}"),
                }
            }
            Err(e) => tracing::warn!("snapshot attempt failed, keeping previous snapshot (will retry next interval): {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn temp_path(name: &str) -> String {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        format!("{}/ma-db-api-test-{pid}-{name}", dir.display())
    }

    fn cleanup(paths: &[&str]) {
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn snapshot_copies_committed_data_into_a_fresh_file() {
        let source = temp_path("source.db");
        let snapshot = temp_path("snapshot.db");
        cleanup(&[&source, &snapshot, &format!("{snapshot}.tmp")]);

        {
            let conn = Connection::open(&source).unwrap();
            conn.execute_batch(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);
                 INSERT INTO t (name) VALUES ('hello');",
            ).unwrap();
        }

        take_snapshot_blocking(&source, &snapshot).unwrap();

        assert!(Path::new(&snapshot).exists());
        assert!(!Path::new(&format!("{snapshot}.tmp")).exists(), "tmp file should be renamed away, not left behind");

        let conn = Connection::open_with_flags(&snapshot, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let name: String = conn.query_row("SELECT name FROM t WHERE id = 1", [], |r| r.get(0)).unwrap();
        assert_eq!(name, "hello");

        cleanup(&[&source, &snapshot]);
    }

    #[test]
    fn snapshot_overwrites_a_stale_leftover_tmp_file() {
        let source = temp_path("source2.db");
        let snapshot = temp_path("snapshot2.db");
        let tmp = format!("{snapshot}.tmp");
        cleanup(&[&source, &snapshot, &tmp]);

        {
            let conn = Connection::open(&source).unwrap();
            conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY);").unwrap();
        }
        // Simulate a prior interrupted attempt that left a tmp file behind —
        // VACUUM INTO refuses to write to a path that already exists, so this
        // must be cleared before the real attempt, not just on first run.
        std::fs::write(&tmp, b"leftover garbage from an interrupted run").unwrap();

        take_snapshot_blocking(&source, &snapshot).unwrap();

        assert!(Path::new(&snapshot).exists());
        assert!(!Path::new(&tmp).exists());

        cleanup(&[&source, &snapshot]);
    }
}
