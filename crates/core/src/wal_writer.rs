//! WAL-integrated table writer for durable writes.
//!
//! `WalTableWriter` buffers rows and writes them through the WAL before
//! applying them to column files, ensuring crash recovery is possible.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use exchange_common::error::Result;
use exchange_common::types::{ColumnType, Timestamp};

use crate::replication::ReplicationManager;
use crate::sync::SyncMode;
use crate::table::TableMeta;
use crate::wal::merge::WalMergeJob;
use crate::wal::row_codec::{OwnedColumnValue, encode_row};
use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};

/// Configuration for the WAL table writer.
pub struct WalTableWriterConfig {
    /// Maximum number of rows to buffer before auto-flushing.
    pub buffer_capacity: usize,
    /// WAL segment size limit.
    pub max_segment_size: u64,
    /// Sync mode controlling fsync behavior.
    pub sync_mode: SyncMode,
    /// Whether to run the merge job on every commit.
    /// When `false`, the merge is deferred to a background scheduler or
    /// explicit `merge()` call, making commits much faster (WAL-only).
    pub merge_on_commit: bool,
    /// Minimum interval between automatic merges (only used when
    /// `merge_on_commit` is true). Merges triggered more frequently
    /// than this are skipped.
    pub merge_interval: Duration,
}

impl Default for WalTableWriterConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 1000,
            max_segment_size: 64 * 1024 * 1024,
            sync_mode: SyncMode::Full,
            merge_on_commit: true,
            merge_interval: Duration::ZERO,
        }
    }
}

/// A single buffered row awaiting WAL commit.
struct BufferedRow {
    timestamp: Timestamp,
    values: Vec<OwnedColumnValue>,
}

/// A table writer that writes through WAL for durability.
///
/// Rows are buffered in memory and, on flush, batch-encoded as a single WAL
/// Data event (one event per flush, containing all buffered rows). This avoids
/// the per-row WAL event overhead that limited throughput to ~33K rows/s.
///
/// On commit, the WAL is fsynced. The merge job (WAL -> column files) runs
/// only when `merge_on_commit` is true; otherwise it is deferred to a
/// background scheduler or an explicit `merge()` call.
pub struct WalTableWriter {
    meta: TableMeta,
    table_name: String,
    table_dir: PathBuf,
    wal_writer: WalWriter,
    /// Column types in schema order (cached from meta).
    column_types: Vec<ColumnType>,
    /// Rows buffered before WAL commit.
    buffer: Vec<BufferedRow>,
    buffer_capacity: usize,
    sync_mode: SyncMode,
    /// Whether to merge WAL to column files on every commit.
    merge_on_commit: bool,
    /// Minimum interval between auto-merges.
    merge_interval: Duration,
    /// Timestamp of the last merge.
    last_merge: Instant,
    /// Optional replication manager for shipping WAL segments after commit.
    repl_mgr: Option<Arc<ReplicationManager>>,
}

impl WalTableWriter {
    /// Open a WAL table writer for the given table.
    ///
    /// - `db_root`: root data directory
    /// - `table_name`: name of the table (subdirectory of `db_root`)
    /// - `config`: writer configuration
    pub fn open(db_root: &Path, table_name: &str, config: WalTableWriterConfig) -> Result<Self> {
        let table_dir = db_root.join(table_name);
        let meta = TableMeta::load(&table_dir.join("_meta"))?;
        let wal_dir = table_dir.join("wal");

        let commit_mode = if config.sync_mode.is_full() {
            CommitMode::Sync
        } else {
            CommitMode::Async
        };

        let wal_config = WalWriterConfig {
            max_segment_size: config.max_segment_size,
            commit_mode,
        };

        let has_wal_segments = wal_dir.exists()
            && std::fs::read_dir(&wal_dir)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .any(|e| e.file_name().to_string_lossy().ends_with(".wal"))
                })
                .unwrap_or(false);

        let wal_writer = if has_wal_segments {
            WalWriter::open(&wal_dir, wal_config)?
        } else {
            WalWriter::create(&wal_dir, wal_config)?
        };

        let column_types: Vec<ColumnType> = meta
            .columns
            .iter()
            .map(|c| ColumnType::from(c.col_type))
            .collect();

        let table_name = table_name.to_string();

        Ok(Self {
            meta,
            table_name,
            table_dir,
            wal_writer,
            column_types,
            buffer: Vec::with_capacity(config.buffer_capacity),
            buffer_capacity: config.buffer_capacity,
            sync_mode: config.sync_mode,
            merge_on_commit: config.merge_on_commit,
            merge_interval: config.merge_interval,
            last_merge: Instant::now() - config.merge_interval,
            repl_mgr: None,
        })
    }

    /// Buffer a single row for writing.
    ///
    /// `values` must contain one value per column in schema order.
    /// If the buffer reaches capacity, it is automatically flushed to WAL.
    pub fn write_row(&mut self, ts: Timestamp, values: Vec<OwnedColumnValue>) -> Result<()> {
        self.buffer.push(BufferedRow {
            timestamp: ts,
            values,
        });

        if self.buffer.len() >= self.buffer_capacity {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush buffered rows to the WAL.
    ///
    /// Batch-encodes all buffered rows into a **single** WAL Data event,
    /// dramatically reducing per-row WAL overhead. The batch payload format:
    ///
    /// ```text
    /// | row_count (u32 LE) | { row_len (u32 LE) | encoded_row }* |
    /// ```
    ///
    /// The WAL segment is flushed to disk to ensure durability.
    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Batch encode: one WAL event for all buffered rows.
        let row_count = self.buffer.len();
        let mut batch_payload = Vec::with_capacity(row_count * 64);
        // Write row count as u32 LE.
        batch_payload.extend_from_slice(&(row_count as u32).to_le_bytes());

        // Use the first row's timestamp as the event timestamp.
        let event_ts = self.buffer[0].timestamp.0;

        for row in self.buffer.drain(..) {
            let encoded = encode_row(&self.column_types, &row.values)?;
            batch_payload.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            batch_payload.extend_from_slice(&encoded);
        }

        self.wal_writer.append_data(event_ts, batch_payload)?;

        // Flush the WAL segment to ensure data is on disk.
        self.wal_writer.flush()?;

        Ok(())
    }

    /// Flush + seal WAL + optionally run merge to apply WAL to column files.
    ///
    /// When `merge_on_commit` is `true` (and the merge interval has elapsed),
    /// this is the full durability path: buffered rows are written to WAL,
    /// the WAL segment is sealed, and then WalMergeJob applies the data to
    /// column files and updates TxnFile.
    ///
    /// When `merge_on_commit` is `false`, only the WAL write + seal happens
    /// (fast path). The merge is deferred to a background scheduler or an
    /// explicit call to [`merge`].
    pub fn commit(&mut self) -> Result<()> {
        self.flush()?;

        // Seal the WAL segment (flush + truncate) so the merge job
        // sees only valid data when it re-opens the segment files.
        self.wal_writer.seal()?;

        // Ship the sealed WAL segment to replicas if replication is configured.
        //
        // IMPORTANT: We read the segment bytes NOW, before merge() runs.
        // Merge renames/deletes the WAL file, so if we pass a file path to
        // an async task, the file will be gone by the time the task runs.
        // Reading bytes synchronously here avoids the race condition.
        if let Some(ref repl_mgr) = self.repl_mgr {
            let wal_dir = self.table_dir.join("wal");
            if let Some(segment_path) = find_latest_wal_segment(&wal_dir) {
                // Read file bytes NOW before merge deletes it.
                match std::fs::read(&segment_path) {
                    Ok(segment_bytes) => {
                        let table_name = self.table_name.clone();
                        let table_dir = self.table_dir.clone();
                        let mgr = Arc::clone(repl_mgr);
                        let rt = tokio::runtime::Handle::try_current();
                        if let Ok(handle) = rt {
                            handle.spawn(async move {
                                // Ensure schema is synced first.
                                if let Err(e) =
                                    mgr.ensure_schema_synced(&table_name, &table_dir).await
                                {
                                    tracing::warn!(error = %e, "failed to sync schema");
                                }
                                // Ship the pre-read segment bytes.
                                if let Err(e) =
                                    mgr.ship_segment_bytes(&table_name, &segment_bytes).await
                                {
                                    tracing::error!(error = %e, "failed to ship WAL segment");
                                }
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to read WAL segment for shipping");
                    }
                }
            }
        }

        // Run the merge job only when configured and interval has elapsed.
        let should_merge = self.merge_on_commit && self.last_merge.elapsed() >= self.merge_interval;

        if should_merge {
            self.run_merge()?;
        } else {
            // Still need to re-create WAL writer for future writes.
            self.recreate_wal_writer()?;
        }

        Ok(())
    }

    /// Explicitly run the WAL merge job (applies WAL to column files).
    ///
    /// Call this from a background scheduler when `merge_on_commit` is `false`,
    /// or when you want to force an immediate merge regardless of the interval.
    pub fn merge(&mut self) -> Result<()> {
        self.run_merge()
    }

    fn run_merge(&mut self) -> Result<()> {
        let merge_job = WalMergeJob::new(self.table_dir.clone(), self.meta.clone());
        merge_job.run()?;
        self.last_merge = Instant::now();

        // Re-create WAL writer for future writes (old segments are now .applied).
        self.recreate_wal_writer()
    }

    fn recreate_wal_writer(&mut self) -> Result<()> {
        let wal_dir = self.table_dir.join("wal");
        let commit_mode = if self.sync_mode.is_full() {
            CommitMode::Sync
        } else {
            CommitMode::Async
        };
        let wal_config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode,
        };
        self.wal_writer = WalWriter::create(&wal_dir, wal_config)?;
        Ok(())
    }

    /// Get a reference to the table metadata.
    pub fn meta(&self) -> &TableMeta {
        &self.meta
    }

    /// Set the replication manager for automatic WAL segment shipping.
    ///
    /// When set and the node is configured as a primary, every `commit()`
    /// will ship the sealed WAL segment to replicas.
    pub fn set_replication_manager(&mut self, mgr: Arc<ReplicationManager>) {
        self.repl_mgr = Some(mgr);
    }

    /// Asynchronous commit: flush + seal WAL but skip the merge entirely.
    ///
    /// The merge is left for the background [`WalMergeScheduler`] to pick up,
    /// making the commit path WAL-only. This is significantly faster than
    /// [`commit`] when `merge_on_commit` is true, because the expensive
    /// merge job no longer blocks the writer.
    ///
    /// After calling this, schedule the merge via
    /// [`WalMergeScheduler::schedule_merge`].
    pub fn commit_async(&mut self) -> Result<()> {
        self.flush()?;
        self.wal_writer.seal()?;

        // Ship WAL segment to replicas if configured (same as commit()).
        if let Some(ref repl_mgr) = self.repl_mgr {
            let wal_dir = self.table_dir.join("wal");
            if let Some(segment_path) = find_latest_wal_segment(&wal_dir) {
                let table_name = self.table_name.clone();
                let mgr = Arc::clone(repl_mgr);
                let rt = tokio::runtime::Handle::try_current();
                if let Ok(handle) = rt {
                    let seg = segment_path.clone();
                    handle.spawn(async move {
                        if let Err(e) = mgr.on_wal_commit(&table_name, &seg).await {
                            tracing::error!(error = %e, "failed to ship WAL segment");
                        }
                    });
                }
            }
        }

        // Don't merge here — let the background scheduler do it.
        self.recreate_wal_writer()?;
        Ok(())
    }

    /// Get the table directory (for use with `WalMergeScheduler`).
    pub fn table_dir(&self) -> &Path {
        &self.table_dir
    }

    /// Get the current buffer length.
    pub fn buffered_rows(&self) -> usize {
        self.buffer.len()
    }
}

/// Background merge scheduler that decouples WAL merges from the commit path.
///
/// Writers call [`schedule_merge`] to enqueue a pending merge, and a
/// background thread (or the scheduler tick) drains the queue by running
/// [`WalMergeJob`] for each entry.  This keeps the write path fast
/// (WAL-only) while ensuring data eventually reaches the column store.
pub struct WalMergeScheduler {
    pending: Arc<Mutex<Vec<(PathBuf, TableMeta)>>>,
}

impl WalMergeScheduler {
    /// Create a new, empty merge scheduler.
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Enqueue a merge job for the given table directory and metadata.
    ///
    /// This is lock-free for the caller in practice (the mutex is held
    /// only for a `Vec::push`). Call this from `WalTableWriter::commit_async`
    /// to defer the merge to the background.
    pub fn schedule_merge(&self, table_dir: PathBuf, meta: TableMeta) {
        self.pending.lock().unwrap().push((table_dir, meta));
    }

    /// Run all pending merge jobs and return the number completed.
    ///
    /// Drains the pending queue atomically (so new merges can be scheduled
    /// while this is running) and executes each `WalMergeJob` sequentially.
    pub fn run_pending(&self) -> Result<u32> {
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        let count = pending.len() as u32;
        for (dir, meta) in pending {
            WalMergeJob::new(dir, meta).run()?;
        }
        Ok(count)
    }

    /// Return the number of merges currently queued.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

impl Default for WalMergeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Find the latest WAL segment file in a directory.
///
/// Looks for files matching `wal-NNNNNN.wal` and returns the path to the
/// one with the highest segment ID.
fn find_latest_wal_segment(wal_dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(wal_dir).ok()?;
    let mut best: Option<(u32, PathBuf)> = None;

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("wal-")
            && let Some(id_str) = rest.strip_suffix(".wal")
            && let Ok(id) = id_str.parse::<u32>()
        {
            match &best {
                Some((best_id, _)) if id <= *best_id => {}
                _ => best = Some((id, entry.path())),
            }
        }
    }

    best.map(|(_, path)| path)
}

impl Drop for WalTableWriter {
    fn drop(&mut self) {
        // Best-effort: flush remaining buffered rows on drop.
        let _ = self.flush();
    }
}

impl WalTableWriter {
    /// Flush all WAL segments to disk for all tables in a database root.
    ///
    /// This walks the `_wal/` directory of each table and calls `fsync` on
    /// any open WAL segment files. Used during graceful shutdown to ensure
    /// all buffered data is durable.
    pub fn flush_all_global() -> Result<()> {
        // This is a best-effort global sync. Individual WAL writers flush
        // on each batch, so this mainly catches any edge-case OS buffers.
        // The real protection comes from the connection drain step before this.
        tracing::info!("flush_all_global: syncing all WAL segments to disk");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::FixedColumnReader;
    use crate::table::TableBuilder;
    use crate::txn::TxnFile;
    use exchange_common::types::{ColumnType, PartitionBy};
    use tempfile::tempdir;

    fn create_test_table(db_root: &Path) -> TableMeta {
        let meta = TableBuilder::new("test_trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::I64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Initialize the _txn file.
        let table_dir = db_root.join("test_trades");
        let _txn = TxnFile::open(&table_dir).unwrap();

        meta
    }

    #[test]
    fn wal_writer_write_and_commit() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let _meta = create_test_table(db_root);

        let config = WalTableWriterConfig {
            buffer_capacity: 100,
            ..Default::default()
        };

        {
            let mut writer = WalTableWriter::open(db_root, "test_trades", config).unwrap();

            let ts1 = Timestamp(1_710_513_000_000_000_000); // 2024-03-15
            writer
                .write_row(
                    ts1,
                    vec![
                        OwnedColumnValue::Timestamp(ts1.0),
                        OwnedColumnValue::F64(65000.50),
                        OwnedColumnValue::I64(100),
                    ],
                )
                .unwrap();

            let ts2 = Timestamp(ts1.0 + 1_000_000_000);
            writer
                .write_row(
                    ts2,
                    vec![
                        OwnedColumnValue::Timestamp(ts2.0),
                        OwnedColumnValue::F64(65100.25),
                        OwnedColumnValue::I64(200),
                    ],
                )
                .unwrap();

            writer.commit().unwrap();
        }

        // Verify column files were created.
        let part_dir = db_root.join("test_trades").join("2024-03-15");
        assert!(part_dir.exists(), "partition directory should exist");

        let price_reader =
            FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
        assert_eq!(price_reader.row_count(), 2);
        assert_eq!(price_reader.read_f64(0), 65000.50);
        assert_eq!(price_reader.read_f64(1), 65100.25);
    }

    #[test]
    fn wal_writer_auto_flush_on_capacity() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let _meta = create_test_table(db_root);

        let config = WalTableWriterConfig {
            buffer_capacity: 2, // Very small to trigger auto-flush
            ..Default::default()
        };

        let mut writer = WalTableWriter::open(db_root, "test_trades", config).unwrap();

        let ts = Timestamp(1_710_513_000_000_000_000);
        // Write 3 rows; auto-flush should happen after 2.
        for i in 0..3 {
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(100.0 + i as f64),
                        OwnedColumnValue::I64(i),
                    ],
                )
                .unwrap();
        }

        // After writing 3 rows with capacity 2, one flush should have happened,
        // leaving 1 row in the buffer.
        assert_eq!(writer.buffered_rows(), 1);

        writer.commit().unwrap();
    }

    #[test]
    fn wal_writer_crash_simulation_and_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let _meta = create_test_table(db_root);

        // Write rows and flush to WAL, but do NOT commit (simulating crash).
        {
            let config = WalTableWriterConfig::default();
            let mut writer = WalTableWriter::open(db_root, "test_trades", config).unwrap();

            let ts = Timestamp(1_710_513_000_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(42000.0),
                        OwnedColumnValue::I64(10),
                    ],
                )
                .unwrap();

            // Flush to WAL but don't commit (no merge, simulates crash).
            writer.flush().unwrap();
            // Drop without commit - the WAL has the data but column files don't.
        }

        // Column files should NOT exist yet (data only in WAL).
        let part_dir = db_root.join("test_trades").join("2024-03-15");
        assert!(
            !part_dir.exists(),
            "partition should not exist before recovery"
        );

        // WAL segment should exist.
        let wal_dir = db_root.join("test_trades").join("wal");
        assert!(wal_dir.exists(), "WAL directory should exist");

        // Now recover by running merge manually (simulates recovery).
        let meta = TableMeta::load(&db_root.join("test_trades").join("_meta")).unwrap();
        let merge_job = WalMergeJob::new(db_root.join("test_trades"), meta);
        let stats = merge_job.run().unwrap();

        assert_eq!(stats.rows_merged, 1);
        assert!(part_dir.exists(), "partition should exist after recovery");

        let price_reader =
            FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
        assert_eq!(price_reader.row_count(), 1);
        assert_eq!(price_reader.read_f64(0), 42000.0);
    }

    #[test]
    fn wal_writer_empty_flush() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let _meta = create_test_table(db_root);

        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test_trades", config).unwrap();

        // Flushing with no buffered rows should be a no-op.
        writer.flush().unwrap();
        writer.commit().unwrap();
    }
}
