//! Crash recovery manager for ExchangeDB.
//!
//! On startup, before serving queries, the recovery manager scans all tables
//! for unapplied WAL segments and replays them via `WalMergeJob`.

use std::path::Path;
use std::time::Instant;

use exchange_common::error::Result;

use crate::table::TableMeta;
use crate::wal::merge::WalMergeJob;

/// Statistics from a recovery run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Number of tables that had pending WAL data and were recovered.
    pub tables_recovered: u32,
    /// Number of WAL segments replayed across all tables.
    pub segments_replayed: u32,
    /// Total rows recovered across all tables.
    pub rows_recovered: u64,
    /// Time taken for recovery in milliseconds.
    pub duration_ms: u64,
}

/// Manages crash recovery by replaying unapplied WAL segments.
pub struct RecoveryManager;

impl RecoveryManager {
    /// Recover all tables in the database after a crash.
    ///
    /// Scans each subdirectory of `db_root` for tables (directories containing
    /// a `_meta` file) and, for each table that has a `wal/` directory with
    /// unapplied `.wal` segments, runs `WalMergeJob` to apply them.
    pub fn recover_all(db_root: &Path) -> Result<RecoveryStats> {
        let start = Instant::now();
        let mut stats = RecoveryStats {
            tables_recovered: 0,
            segments_replayed: 0,
            rows_recovered: 0,
            duration_ms: 0,
        };

        if !db_root.exists() {
            stats.duration_ms = start.elapsed().as_millis() as u64;
            return Ok(stats);
        }

        let entries: Vec<_> = std::fs::read_dir(db_root)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .collect();

        for entry in entries {
            let table_dir = entry.path();
            let meta_path = table_dir.join("_meta");

            if !meta_path.exists() {
                continue;
            }

            let meta = match TableMeta::load(&meta_path) {
                Ok(m) => m,
                Err(_) => continue, // Skip tables with corrupt metadata.
            };

            let table_stats = Self::recover_table(&table_dir, &meta)?;
            if table_stats.tables_recovered > 0 {
                stats.tables_recovered += 1;
                stats.segments_replayed += table_stats.segments_replayed;
                stats.rows_recovered += table_stats.rows_recovered;
            }
        }

        stats.duration_ms = start.elapsed().as_millis() as u64;
        Ok(stats)
    }

    /// Recover a single table by replaying any unapplied WAL segments.
    pub fn recover_table(table_dir: &Path, meta: &TableMeta) -> Result<RecoveryStats> {
        let start = Instant::now();
        let wal_dir = table_dir.join("wal");

        if !wal_dir.exists() {
            return Ok(RecoveryStats {
                tables_recovered: 0,
                segments_replayed: 0,
                rows_recovered: 0,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        // Check if there are any unapplied .wal segments.
        let has_wal_segments = Self::has_unapplied_segments(&wal_dir)?;

        if !has_wal_segments {
            return Ok(RecoveryStats {
                tables_recovered: 0,
                segments_replayed: 0,
                rows_recovered: 0,
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        // Run the merge job to replay WAL events.
        let merge_job = WalMergeJob::new(table_dir.to_path_buf(), meta.clone());
        let merge_stats = merge_job.run()?;

        Ok(RecoveryStats {
            tables_recovered: if merge_stats.rows_merged > 0 || merge_stats.segments_processed > 0 {
                1
            } else {
                0
            },
            segments_replayed: merge_stats.segments_processed,
            rows_recovered: merge_stats.rows_merged,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }

    /// Check if a WAL directory contains any unapplied `.wal` segment files.
    fn has_unapplied_segments(wal_dir: &Path) -> Result<bool> {
        if !wal_dir.exists() {
            return Ok(false);
        }

        for entry in std::fs::read_dir(wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal-") && name.ends_with(".wal") {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::FixedColumnReader;
    use crate::table::TableBuilder;
    use crate::txn::TxnFile;
    use crate::wal::row_codec::{OwnedColumnValue, encode_row};
    use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
    use exchange_common::types::{ColumnType, PartitionBy};
    use tempfile::tempdir;

    fn create_test_table(db_root: &Path, name: &str) -> TableMeta {
        let meta = TableBuilder::new(name)
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Initialize _txn.
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

        let mut writer = if wal_dir.exists() {
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
    fn recover_single_table() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let meta = create_test_table(db_root, "trades");
        let table_dir = db_root.join("trades");

        // Write WAL data (simulating a crash before merge).
        let ts = 1_710_513_000_000_000_000i64;
        write_wal_rows(&table_dir, &[ts], &[65000.0]);

        // Verify data is NOT in column files yet.
        let part_dir = table_dir.join("2024-03-15");
        assert!(!part_dir.exists());

        // Run recovery.
        let stats = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
        assert_eq!(stats.tables_recovered, 1);
        assert_eq!(stats.rows_recovered, 1);
        assert!(stats.segments_replayed >= 1);

        // Verify data IS now in column files.
        assert!(part_dir.exists());
        let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
        assert_eq!(reader.row_count(), 1);
        assert_eq!(reader.read_f64(0), 65000.0);
    }

    #[test]
    fn recover_all_multiple_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta1 = create_test_table(db_root, "trades1");
        let _meta2 = create_test_table(db_root, "trades2");

        let ts = 1_710_513_000_000_000_000i64;
        write_wal_rows(&db_root.join("trades1"), &[ts], &[100.0]);
        write_wal_rows(
            &db_root.join("trades2"),
            &[ts, ts + 1_000_000_000],
            &[200.0, 300.0],
        );

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.tables_recovered, 2);
        assert_eq!(stats.rows_recovered, 3);
    }

    #[test]
    fn recover_empty_wal() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let meta = create_test_table(db_root, "trades");
        let table_dir = db_root.join("trades");

        // Create empty WAL directory with no segments.
        let wal_dir = table_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let stats = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
        assert_eq!(stats.tables_recovered, 0);
        assert_eq!(stats.rows_recovered, 0);
        assert_eq!(stats.segments_replayed, 0);
    }

    #[test]
    fn recover_no_wal_dir() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let meta = create_test_table(db_root, "trades");
        let table_dir = db_root.join("trades");

        let stats = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
        assert_eq!(stats.tables_recovered, 0);
        assert_eq!(stats.rows_recovered, 0);
    }

    #[test]
    fn recover_all_nonexistent_db_root() {
        let dir = tempdir().unwrap();
        let db_root = dir.path().join("nonexistent");

        let stats = RecoveryManager::recover_all(&db_root).unwrap();
        assert_eq!(stats.tables_recovered, 0);
        assert_eq!(stats.rows_recovered, 0);
    }

    #[test]
    fn recover_already_applied() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        let meta = create_test_table(db_root, "trades");
        let table_dir = db_root.join("trades");

        let ts = 1_710_513_000_000_000_000i64;
        write_wal_rows(&table_dir, &[ts], &[65000.0]);

        // First recovery.
        let stats1 = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
        assert_eq!(stats1.rows_recovered, 1);

        // Second recovery should find nothing to do (segments renamed to .applied).
        let stats2 = RecoveryManager::recover_table(&table_dir, &meta).unwrap();
        assert_eq!(stats2.tables_recovered, 0);
        assert_eq!(stats2.rows_recovered, 0);
    }
}
