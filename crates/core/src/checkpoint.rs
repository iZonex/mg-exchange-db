//! Checkpoint manager for ExchangeDB.
//!
//! Periodically ensures that all WAL data has been applied to column files,
//! and writes a checkpoint marker file with a timestamp.

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use exchange_common::error::Result;

use crate::recovery::RecoveryManager;

/// Statistics from a checkpoint run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointStats {
    /// Number of tables checkpointed (had pending WAL data).
    pub tables_checkpointed: u32,
    /// Total rows flushed from WAL to column files.
    pub rows_flushed: u64,
    /// Duration of the checkpoint in milliseconds.
    pub duration_ms: u64,
}

/// Manages periodic checkpoints to ensure WAL data is applied to column files.
pub struct CheckpointManager {
    db_root: PathBuf,
    /// Checkpoint interval (default: 5 minutes).
    interval: Duration,
}

impl CheckpointManager {
    /// Create a new checkpoint manager with the default interval (5 minutes).
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            db_root,
            interval: Duration::from_secs(5 * 60),
        }
    }

    /// Create a checkpoint manager with a custom interval.
    pub fn with_interval(db_root: PathBuf, interval: Duration) -> Self {
        Self { db_root, interval }
    }

    /// Get the configured checkpoint interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Run a checkpoint: for each table, apply any pending WAL data to
    /// column files. This is effectively the same as recovery but intended
    /// to be run while the database is operational.
    pub fn checkpoint(&self) -> Result<CheckpointStats> {
        let start = Instant::now();

        // Use RecoveryManager to apply any pending WAL data.
        let recovery_stats = RecoveryManager::recover_all(&self.db_root)?;

        let stats = CheckpointStats {
            tables_checkpointed: recovery_stats.tables_recovered,
            rows_flushed: recovery_stats.rows_recovered,
            duration_ms: start.elapsed().as_millis() as u64,
        };

        // Write a checkpoint marker.
        self.write_checkpoint_marker()?;

        Ok(stats)
    }

    /// Write a checkpoint marker file with the current timestamp.
    ///
    /// The marker file is written to `<db_root>/_checkpoint` and contains
    /// a Unix timestamp in seconds.
    pub fn write_checkpoint_marker(&self) -> Result<()> {
        let marker_path = self.db_root.join("_checkpoint");
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        std::fs::write(&marker_path, timestamp.to_string())?;
        Ok(())
    }

    /// Read the last checkpoint timestamp, if available.
    pub fn last_checkpoint_time(&self) -> Option<u64> {
        let marker_path = self.db_root.join("_checkpoint");
        std::fs::read_to_string(&marker_path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableBuilder;
    use crate::txn::TxnFile;
    use crate::wal::row_codec::{OwnedColumnValue, encode_row};
    use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
    use exchange_common::types::{ColumnType, PartitionBy};
    use tempfile::tempdir;

    #[test]
    fn checkpoint_creates_marker_file() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let mgr = CheckpointManager::new(db_root.to_path_buf());
        mgr.write_checkpoint_marker().unwrap();

        let marker_path = db_root.join("_checkpoint");
        assert!(marker_path.exists(), "checkpoint marker should exist");

        let ts = mgr.last_checkpoint_time();
        assert!(ts.is_some());
        assert!(ts.unwrap() > 0);
    }

    #[test]
    fn checkpoint_empty_db() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let mgr = CheckpointManager::new(db_root.to_path_buf());
        let stats = mgr.checkpoint().unwrap();

        assert_eq!(stats.tables_checkpointed, 0);
        assert_eq!(stats.rows_flushed, 0);

        // Marker file should still be created.
        assert!(db_root.join("_checkpoint").exists());
    }

    #[test]
    fn checkpoint_with_pending_wal() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create a table.
        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let table_dir = db_root.join("trades");
        let _txn = TxnFile::open(&table_dir).unwrap();

        // Write WAL data.
        let wal_dir = table_dir.join("wal");
        let column_types = vec![ColumnType::Timestamp, ColumnType::F64];
        {
            let config = WalWriterConfig {
                max_segment_size: 64 * 1024 * 1024,
                commit_mode: CommitMode::Sync,
            };
            let mut writer = WalWriter::create(&wal_dir, config).unwrap();
            let ts: i64 = 1_710_513_000_000_000_000;
            let row = vec![
                OwnedColumnValue::Timestamp(ts),
                OwnedColumnValue::F64(65000.0),
            ];
            let payload = encode_row(&column_types, &row).unwrap();
            writer.append_data(ts, payload).unwrap();
            writer.flush().unwrap();
        }

        // Run checkpoint.
        let mgr = CheckpointManager::new(db_root.to_path_buf());
        let stats = mgr.checkpoint().unwrap();

        assert_eq!(stats.tables_checkpointed, 1);
        assert_eq!(stats.rows_flushed, 1);

        // Verify column files exist after checkpoint.
        let part_dir = table_dir.join("2024-03-15");
        assert!(part_dir.exists());
    }

    #[test]
    fn checkpoint_custom_interval() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let mgr = CheckpointManager::with_interval(db_root.to_path_buf(), Duration::from_secs(30));
        assert_eq!(mgr.interval(), Duration::from_secs(30));
    }

    #[test]
    fn no_checkpoint_marker_returns_none() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let mgr = CheckpointManager::new(db_root.to_path_buf());
        assert!(mgr.last_checkpoint_time().is_none());
    }
}
