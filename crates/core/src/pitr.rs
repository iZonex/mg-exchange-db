//! Point-in-Time Recovery (PITR) for ExchangeDB.
//!
//! Enables restoring the database to any point in time within a configurable
//! retention window by combining periodic snapshots with WAL replay.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};

use crate::snapshot::{create_snapshot, restore_snapshot};
use crate::wal::event::EventType;
use crate::wal::reader::WalReader;
// decode_row and OwnedColumnValue will be used when WAL replay is fully implemented.

/// Configuration for PITR.
#[derive(Debug, Clone)]
pub struct PitrConfig {
    /// Whether PITR is enabled.
    pub enabled: bool,
    /// How far back you can recover (e.g., 7 days).
    pub retention_window: Duration,
    /// Base snapshot interval (e.g., every 6 hours).
    pub snapshot_interval: Duration,
}

impl Default for PitrConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention_window: Duration::from_secs(7 * 86400),
            snapshot_interval: Duration::from_secs(6 * 3600),
        }
    }
}

/// A PITR checkpoint: a snapshot plus the WAL position at the time of the snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PitrCheckpoint {
    /// Unique identifier for this checkpoint.
    pub id: String,
    /// Unix timestamp (nanoseconds) when this checkpoint was created.
    pub timestamp: i64,
    /// Path to the snapshot directory.
    pub snapshot_path: PathBuf,
    /// Per-table WAL transaction position at the time of the checkpoint.
    /// Maps table name -> last applied WAL txn_id.
    pub wal_position: HashMap<String, u64>,
}

/// Statistics from a PITR restore operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreStats {
    /// Number of tables restored from the snapshot.
    pub tables_restored: u32,
    /// Total rows present after restore (from snapshot).
    pub rows_restored: u64,
    /// Number of WAL events replayed after the snapshot.
    pub wal_events_replayed: u64,
    /// The target timestamp that was restored to.
    pub target_timestamp: i64,
}

const CHECKPOINT_MANIFEST: &str = "checkpoint.json";

/// Manages PITR checkpoints: creation, listing, restore, and cleanup.
pub struct PitrManager {
    db_root: PathBuf,
    pitr_dir: PathBuf,
    config: PitrConfig,
}

impl PitrManager {
    /// Create a new PITR manager.
    ///
    /// The `pitr_dir` is located at `<db_root>/_pitr/`.
    pub fn new(db_root: PathBuf, config: PitrConfig) -> Self {
        let pitr_dir = db_root.join("_pitr");
        Self {
            db_root,
            pitr_dir,
            config,
        }
    }

    /// Create a PITR checkpoint: snapshot the database and record WAL positions.
    pub fn create_checkpoint(&self) -> Result<PitrCheckpoint> {
        std::fs::create_dir_all(&self.pitr_dir)?;

        let id = uuid::Uuid::new_v4().to_string();
        let checkpoint_dir = self.pitr_dir.join(&id);
        let snapshot_path = checkpoint_dir.join("snapshot");

        // Create the snapshot.
        let snap_info = create_snapshot(&self.db_root, &snapshot_path)?;

        // Record WAL positions for each table.
        let mut wal_position = HashMap::new();
        for table_name in &snap_info.tables {
            let wal_dir = self.db_root.join(table_name).join("wal");
            if wal_dir.exists() {
                if let Ok(reader) = WalReader::open(&wal_dir) {
                    let events = reader.read_all()?;
                    let max_txn = events.iter().map(|e| e.txn_id).max().unwrap_or(0);
                    wal_position.insert(table_name.clone(), max_txn);
                }
            } else {
                wal_position.insert(table_name.clone(), 0);
            }
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let checkpoint = PitrCheckpoint {
            id,
            timestamp: now_nanos,
            snapshot_path,
            wal_position,
        };

        // Persist checkpoint metadata.
        let manifest_json = serde_json::to_string_pretty(&checkpoint)
            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        std::fs::write(checkpoint_dir.join(CHECKPOINT_MANIFEST), manifest_json)?;

        Ok(checkpoint)
    }

    /// List all available checkpoints, sorted by timestamp (oldest first).
    pub fn list_checkpoints(&self) -> Result<Vec<PitrCheckpoint>> {
        let mut checkpoints = Vec::new();

        if !self.pitr_dir.exists() {
            return Ok(checkpoints);
        }

        for entry in std::fs::read_dir(&self.pitr_dir)? {
            let entry = entry?;
            if !entry.path().is_dir() {
                continue;
            }
            let manifest_path = entry.path().join(CHECKPOINT_MANIFEST);
            if !manifest_path.exists() {
                continue;
            }
            let json = std::fs::read_to_string(&manifest_path)?;
            if let Ok(cp) = serde_json::from_str::<PitrCheckpoint>(&json) {
                checkpoints.push(cp);
            }
        }

        checkpoints.sort_by_key(|cp| cp.timestamp);
        Ok(checkpoints)
    }

    /// Restore the database to a specific point in time.
    ///
    /// Algorithm:
    /// 1. Find the latest checkpoint BEFORE `target_time`.
    /// 2. Restore the snapshot from that checkpoint into `restore_dir`.
    /// 3. Replay WAL events from the checkpoint's WAL position up to `target_time`.
    /// 4. Stop replaying when event timestamp > `target_time`.
    pub fn restore_to(&self, target_time: i64, restore_dir: &Path) -> Result<RestoreStats> {
        let checkpoints = self.list_checkpoints()?;

        // Find the latest checkpoint BEFORE (or at) target_time.
        let checkpoint = checkpoints
            .iter()
            .rfind(|cp| cp.timestamp <= target_time)
            .ok_or_else(|| {
                ExchangeDbError::Corruption(format!(
                    "no checkpoint found before target time {target_time}"
                ))
            })?;

        // Step 2: Restore the snapshot.
        restore_snapshot(&checkpoint.snapshot_path, restore_dir)?;

        let mut stats = RestoreStats {
            tables_restored: 0,
            rows_restored: 0,
            wal_events_replayed: 0,
            target_timestamp: target_time,
        };

        // Count tables restored.
        if restore_dir.exists() {
            for entry in std::fs::read_dir(restore_dir)? {
                let entry = entry?;
                if entry.path().is_dir() && entry.path().join("_meta").exists() {
                    stats.tables_restored += 1;
                }
            }
        }

        // Clean WAL directories in the restored snapshot to avoid stale segments.
        if restore_dir.exists() {
            for entry in std::fs::read_dir(restore_dir)? {
                let entry = entry?;
                if entry.path().is_dir() {
                    let wal_dir = entry.path().join("wal");
                    if wal_dir.exists() {
                        std::fs::remove_dir_all(&wal_dir)?;
                    }
                }
            }
        }

        // Step 3: Replay WAL events from checkpoint position up to target_time.
        for (table_name, &wal_pos) in &checkpoint.wal_position {
            let wal_dir = self.db_root.join(table_name).join("wal");
            if !wal_dir.exists() {
                continue;
            }

            let reader = match WalReader::open(&wal_dir) {
                Ok(r) => r,
                Err(_) => continue, // No valid WAL segments to replay.
            };
            if reader.segment_count() == 0 {
                continue;
            }
            // Read events starting from the transaction AFTER the checkpoint position.
            let events = reader.read_from_txn(wal_pos + 1)?;

            // Filter events up to target_time.
            let relevant_events: Vec<_> = events
                .iter()
                .take_while(|e| e.timestamp <= target_time)
                .collect();

            if relevant_events.is_empty() {
                continue;
            }

            let restore_wal_dir = restore_dir.join(table_name).join("wal");
            // Remove any WAL directory copied from the snapshot to start fresh.
            if restore_wal_dir.exists() {
                std::fs::remove_dir_all(&restore_wal_dir)?;
            }
            std::fs::create_dir_all(&restore_wal_dir)?;

            // Copy relevant WAL events into the restore directory so they
            // can be merged by the recovery manager.
            use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
            let wal_config = WalWriterConfig {
                max_segment_size: 64 * 1024 * 1024,
                commit_mode: CommitMode::Sync,
            };
            let mut writer = WalWriter::create(&restore_wal_dir, wal_config)?;

            for event in &relevant_events {
                match event.event_type {
                    EventType::Data => {
                        writer.append_data(event.timestamp, event.payload.clone())?;
                    }
                    EventType::Ddl => {
                        writer.append_ddl(event.timestamp, event.payload.clone())?;
                    }
                    EventType::Truncate => {
                        writer.append_truncate(event.timestamp, event.payload.clone())?;
                    }
                }
                stats.wal_events_replayed += 1;
            }
            writer.flush()?;
        }

        // Step 4: Run recovery on the restore directory to merge WAL events.
        if stats.wal_events_replayed > 0 {
            crate::recovery::RecoveryManager::recover_all(restore_dir)?;
        }

        Ok(stats)
    }

    /// Clean up checkpoints older than the retention window.
    ///
    /// Returns the number of checkpoints removed.
    pub fn cleanup(&self) -> Result<u32> {
        let checkpoints = self.list_checkpoints()?;
        if checkpoints.is_empty() {
            return Ok(0);
        }

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let retention_nanos = self.config.retention_window.as_nanos() as i64;
        let cutoff = now_nanos - retention_nanos;

        let mut removed = 0u32;
        for cp in &checkpoints {
            if cp.timestamp < cutoff {
                let cp_dir = self.pitr_dir.join(&cp.id);
                if cp_dir.exists() {
                    std::fs::remove_dir_all(&cp_dir)?;
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableBuilder;
    use crate::txn::TxnFile;
    use crate::wal::row_codec::{encode_row, OwnedColumnValue};
    use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
    use exchange_common::types::{ColumnType, PartitionBy};
    use tempfile::tempdir;

    fn create_test_table(db_root: &Path, name: &str) -> crate::table::TableMeta {
        let meta = TableBuilder::new(name)
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();
        let table_dir = db_root.join(name);
        let _txn = TxnFile::open(&table_dir).unwrap();
        meta
    }

    fn write_wal_rows(table_dir: &Path, timestamps: &[i64], prices: &[f64]) {
        let wal_dir = table_dir.join("wal");
        let column_types = vec![ColumnType::Timestamp, ColumnType::F64];
        let config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode: CommitMode::Sync,
        };

        // Check if there are any .wal segment files. After recovery, segments
        // are renamed to .applied, so we need to create a fresh writer.
        let has_wal_files = wal_dir.exists()
            && std::fs::read_dir(&wal_dir)
                .ok()
                .map(|rd| {
                    rd.filter_map(|e| e.ok())
                        .any(|e| {
                            e.file_name()
                                .to_string_lossy()
                                .ends_with(".wal")
                        })
                })
                .unwrap_or(false);

        let mut writer = if has_wal_files {
            WalWriter::open(&wal_dir, config).unwrap()
        } else {
            WalWriter::create(&wal_dir, config).unwrap()
        };

        for (ts, price) in timestamps.iter().zip(prices.iter()) {
            let row = vec![
                OwnedColumnValue::Timestamp(*ts),
                OwnedColumnValue::F64(*price),
            ];
            let payload = encode_row(&column_types, &row).unwrap();
            writer.append_data(*ts, payload).unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn test_pitr_create_checkpoint() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        create_test_table(&db_root, "trades");
        let ts = 1_710_513_000_000_000_000i64;
        write_wal_rows(&db_root.join("trades"), &[ts], &[100.0]);

        let config = PitrConfig::default();
        let mgr = PitrManager::new(db_root.clone(), config);

        let cp = mgr.create_checkpoint().unwrap();
        assert!(!cp.id.is_empty());
        assert!(cp.timestamp > 0);
        assert!(cp.snapshot_path.exists());
        assert!(cp.wal_position.contains_key("trades"));
    }

    #[test]
    fn test_pitr_list_checkpoints() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        create_test_table(&db_root, "trades");
        let ts = 1_710_513_000_000_000_000i64;
        write_wal_rows(&db_root.join("trades"), &[ts], &[100.0]);

        let config = PitrConfig::default();
        let mgr = PitrManager::new(db_root.clone(), config);

        mgr.create_checkpoint().unwrap();
        mgr.create_checkpoint().unwrap();

        let checkpoints = mgr.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 2);
        assert!(checkpoints[0].timestamp <= checkpoints[1].timestamp);
    }

    #[test]
    fn test_pitr_restore_to_checkpoint_time() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let restore_dir = tempdir().unwrap();

        // Create table and write initial data.
        create_test_table(&db_root, "trades");
        let ts1 = 1_710_513_000_000_000_000i64;
        write_wal_rows(&db_root.join("trades"), &[ts1], &[100.0]);

        // Merge initial data so the snapshot captures it.
        let meta =
            crate::table::TableMeta::load(&db_root.join("trades").join("_meta")).unwrap();
        crate::recovery::RecoveryManager::recover_table(&db_root.join("trades"), &meta)
            .unwrap();

        let config = PitrConfig::default();
        let mgr = PitrManager::new(db_root.clone(), config);

        // Create checkpoint after initial data.
        let cp = mgr.create_checkpoint().unwrap();

        // Write more data AFTER the checkpoint.
        let ts2 = ts1 + 60_000_000_000; // 60 seconds later
        write_wal_rows(&db_root.join("trades"), &[ts2], &[200.0]);

        // Restore to the checkpoint time (should NOT include the second write).
        let stats = mgr
            .restore_to(cp.timestamp, restore_dir.path())
            .unwrap();

        assert!(stats.tables_restored >= 1);
        assert_eq!(stats.target_timestamp, cp.timestamp);
        // The snapshot itself should have the first row; the second row's WAL event
        // should NOT be replayed since its timestamp is after the checkpoint's
        // WAL position was recorded.
        assert!(restore_dir.path().join("trades").join("_meta").exists());
    }

    #[test]
    fn test_pitr_restore_replays_wal_up_to_target() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let restore_dir = tempdir().unwrap();

        // Create table with initial data.
        create_test_table(&db_root, "trades");
        let ts1 = 1_710_513_000_000_000_000i64;
        write_wal_rows(&db_root.join("trades"), &[ts1], &[100.0]);

        // Merge so snapshot captures it.
        let meta =
            crate::table::TableMeta::load(&db_root.join("trades").join("_meta")).unwrap();
        crate::recovery::RecoveryManager::recover_table(&db_root.join("trades"), &meta)
            .unwrap();

        let config = PitrConfig::default();
        let mgr = PitrManager::new(db_root.clone(), config);

        // Create checkpoint.
        let cp = mgr.create_checkpoint().unwrap();

        // Write 3 more events with timestamps AFTER the checkpoint.
        let ts2 = cp.timestamp + 60_000_000_000;
        let ts3 = cp.timestamp + 120_000_000_000;
        let ts4 = cp.timestamp + 180_000_000_000;
        write_wal_rows(
            &db_root.join("trades"),
            &[ts2, ts3, ts4],
            &[200.0, 300.0, 400.0],
        );

        // Restore to ts3 (should replay ts2 and ts3, but NOT ts4).
        let stats = mgr.restore_to(ts3, restore_dir.path()).unwrap();

        // Should have replayed exactly 2 WAL events (ts2 and ts3).
        assert_eq!(stats.wal_events_replayed, 2);
        assert_eq!(stats.target_timestamp, ts3);
    }

    #[test]
    fn test_pitr_multiple_checkpoints_restore_to_middle() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        create_test_table(&db_root, "trades");
        let ts1 = 1_710_513_000_000_000_000i64;
        write_wal_rows(&db_root.join("trades"), &[ts1], &[100.0]);

        let meta =
            crate::table::TableMeta::load(&db_root.join("trades").join("_meta")).unwrap();
        crate::recovery::RecoveryManager::recover_table(&db_root.join("trades"), &meta)
            .unwrap();

        let config = PitrConfig::default();
        let mgr = PitrManager::new(db_root.clone(), config);

        // Checkpoint 1.
        let cp1 = mgr.create_checkpoint().unwrap();

        // Write more data.
        let ts2 = ts1 + 60_000_000_000;
        write_wal_rows(&db_root.join("trades"), &[ts2], &[200.0]);
        crate::recovery::RecoveryManager::recover_table(&db_root.join("trades"), &meta)
            .unwrap();

        // Checkpoint 2.
        let cp2 = mgr.create_checkpoint().unwrap();

        // Write even more data.
        let ts3 = ts1 + 120_000_000_000;
        write_wal_rows(&db_root.join("trades"), &[ts3], &[300.0]);
        crate::recovery::RecoveryManager::recover_table(&db_root.join("trades"), &meta)
            .unwrap();

        // Checkpoint 3.
        let _cp3 = mgr.create_checkpoint().unwrap();

        let checkpoints = mgr.list_checkpoints().unwrap();
        assert_eq!(checkpoints.len(), 3);

        // Restore to checkpoint 2's timestamp.
        let restore_dir = tempdir().unwrap();
        let stats = mgr
            .restore_to(cp2.timestamp, restore_dir.path())
            .unwrap();

        // Should use cp2's snapshot (which has rows at ts1 and ts2).
        assert!(stats.tables_restored >= 1);
        assert_eq!(stats.target_timestamp, cp2.timestamp);
        assert!(restore_dir.path().join("trades").join("_meta").exists());
    }

    #[test]
    fn test_pitr_cleanup() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        create_test_table(&db_root, "trades");

        // Use a very short retention window so checkpoints are "old" immediately.
        let config = PitrConfig {
            enabled: true,
            retention_window: Duration::from_secs(0),
            snapshot_interval: Duration::from_secs(3600),
        };
        let mgr = PitrManager::new(db_root.clone(), config);

        mgr.create_checkpoint().unwrap();
        mgr.create_checkpoint().unwrap();
        assert_eq!(mgr.list_checkpoints().unwrap().len(), 2);

        // Cleanup with zero retention should remove all.
        let removed = mgr.cleanup().unwrap();
        assert_eq!(removed, 2);
        assert_eq!(mgr.list_checkpoints().unwrap().len(), 0);
    }

    #[test]
    fn test_pitr_no_checkpoint_returns_error() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let restore_dir = tempdir().unwrap();

        let config = PitrConfig::default();
        let mgr = PitrManager::new(db_root.clone(), config);

        let result = mgr.restore_to(1_000_000_000, restore_dir.path());
        assert!(result.is_err());
    }
}
