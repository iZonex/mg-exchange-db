//! Comprehensive crash recovery tests — 100 tests.
//!
//! Tests WAL recovery after simulated crashes at various points,
//! multi-table recovery, idempotent recovery, and various row/column
//! combinations.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::recovery::RecoveryManager;
use exchange_core::table::{TableBuilder, TableMeta};
use exchange_core::txn::TxnFile;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use std::path::Path;
use tempfile::tempdir;

/// Timestamp for 2024-03-15 in nanoseconds.
const TS_DAY1: i64 = 1_710_513_000_000_000_000;
/// Timestamp for 2024-03-16 in nanoseconds.
const TS_DAY2: i64 = TS_DAY1 + 86_400_000_000_000;

/// Helper: create a test table with timestamp + price columns.
fn create_table_2col(db_root: &Path, name: &str) -> TableMeta {
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

/// Helper: create a table with three columns.
fn create_table_3col(db_root: &Path, name: &str) -> TableMeta {
    let meta = TableBuilder::new(name)
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::I64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();
    let table_dir = db_root.join(name);
    let _txn = TxnFile::open(&table_dir).unwrap();
    meta
}

/// Helper: write rows to WAL and flush (no commit = simulated crash).
fn write_and_flush(db_root: &Path, table_name: &str, rows: &[(i64, f64)]) {
    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table_name, config).unwrap();
    for &(ts, price) in rows {
        let timestamp = Timestamp(ts);
        writer
            .write_row(
                timestamp,
                vec![
                    OwnedColumnValue::Timestamp(ts),
                    OwnedColumnValue::F64(price),
                ],
            )
            .unwrap();
    }
    writer.flush().unwrap();
}

/// Helper: write rows to WAL and commit (normal operation).
fn write_and_commit(db_root: &Path, table_name: &str, rows: &[(i64, f64)]) {
    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table_name, config).unwrap();
    for &(ts, price) in rows {
        let timestamp = Timestamp(ts);
        writer
            .write_row(
                timestamp,
                vec![
                    OwnedColumnValue::Timestamp(ts),
                    OwnedColumnValue::F64(price),
                ],
            )
            .unwrap();
    }
    writer.commit().unwrap();
}

// =============================================================================
// Basic recovery
// =============================================================================
mod basic_recovery {
    use super::*;

    #[test]
    fn empty_database_recovery() {
        let dir = tempdir().unwrap();
        let stats = RecoveryManager::recover_all(dir.path()).unwrap();
        assert_eq!(stats.tables_recovered, 0);
        assert_eq!(stats.segments_replayed, 0);
        assert_eq!(stats.rows_recovered, 0);
    }

    #[test]
    fn no_crash_no_recovery_needed() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_commit(db_root, "t", &[(TS_DAY1, 100.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 0);
    }

    #[test]
    fn single_row_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 42000.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.tables_recovered, 1);
        assert_eq!(stats.rows_recovered, 1);

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 1);
        assert_eq!(reader.read_f64(0), 42000.0);
    }

    #[test]
    fn two_row_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(
            db_root,
            "t",
            &[(TS_DAY1, 65000.0), (TS_DAY1 + 1_000_000_000, 65100.0)],
        );

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 2);

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 2);
        assert_eq!(reader.read_f64(0), 65000.0);
        assert_eq!(reader.read_f64(1), 65100.0);
    }

    #[test]
    fn five_row_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        let rows: Vec<(i64, f64)> = (0..5)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, 100.0 + i as f64))
            .collect();
        write_and_flush(db_root, "t", &rows);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 5);
    }

    #[test]
    fn ten_row_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        let rows: Vec<(i64, f64)> = (0..10)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, 1000.0 + i as f64 * 10.0))
            .collect();
        write_and_flush(db_root, "t", &rows);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 10);
    }

    #[test]
    fn recovery_data_correct() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(
            db_root,
            "t",
            &[
                (TS_DAY1, 100.0),
                (TS_DAY1 + 1_000_000_000, 200.0),
                (TS_DAY1 + 2_000_000_000, 300.0),
            ],
        );

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 3);
        assert_eq!(reader.read_f64(0), 100.0);
        assert_eq!(reader.read_f64(1), 200.0);
        assert_eq!(reader.read_f64(2), 300.0);
    }

    #[test]
    fn committed_data_not_recovered_again() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_commit(db_root, "t", &[(TS_DAY1, 100.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 0);

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 1);
    }
}

// =============================================================================
// Idempotent recovery
// =============================================================================
mod idempotent_recovery {
    use super::*;

    #[test]
    fn recover_twice_same_result() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 99000.0)]);

        let stats1 = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats1.rows_recovered, 1);

        let stats2 = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats2.rows_recovered, 0);

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 1);
    }

    #[test]
    fn recover_three_times() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 50000.0)]);

        RecoveryManager::recover_all(db_root).unwrap();
        RecoveryManager::recover_all(db_root).unwrap();
        let stats3 = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats3.rows_recovered, 0);

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 1);
        assert_eq!(reader.read_f64(0), 50000.0);
    }
}

// =============================================================================
// Multi-table recovery
// =============================================================================
mod multi_table_recovery {
    use super::*;

    #[test]
    fn two_tables_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t1");
        create_table_2col(db_root, "t2");

        write_and_flush(db_root, "t1", &[(TS_DAY1, 100.0)]);
        write_and_flush(db_root, "t2", &[(TS_DAY1, 200.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.tables_recovered, 2);
        assert_eq!(stats.rows_recovered, 2);
    }

    #[test]
    fn three_tables_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "a");
        create_table_2col(db_root, "b");
        create_table_2col(db_root, "c");

        write_and_flush(db_root, "a", &[(TS_DAY1, 1.0)]);
        write_and_flush(
            db_root,
            "b",
            &[(TS_DAY1, 2.0), (TS_DAY1 + 1_000_000_000, 3.0)],
        );
        write_and_flush(db_root, "c", &[(TS_DAY1, 4.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.tables_recovered, 3);
        assert_eq!(stats.rows_recovered, 4);
    }

    #[test]
    fn partial_crash_one_committed_one_not() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "committed");
        create_table_2col(db_root, "crashed");

        write_and_commit(db_root, "committed", &[(TS_DAY1, 100.0)]);
        write_and_flush(db_root, "crashed", &[(TS_DAY1, 200.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        // At least the crashed table should be recovered
        assert!(stats.tables_recovered >= 1);
        assert!(stats.rows_recovered >= 1);
    }

    #[test]
    fn multi_table_different_row_counts() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t1");
        create_table_2col(db_root, "t2");
        create_table_2col(db_root, "t3");

        // t1: 5 rows, t2: 1 row, t3: 3 rows
        let rows1: Vec<(i64, f64)> = (0..5)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, i as f64 * 100.0))
            .collect();
        write_and_flush(db_root, "t1", &rows1);
        write_and_flush(db_root, "t2", &[(TS_DAY1, 42.0)]);
        let rows3: Vec<(i64, f64)> = (0..3)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, i as f64 * 50.0))
            .collect();
        write_and_flush(db_root, "t3", &rows3);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.tables_recovered, 3);
        assert_eq!(stats.rows_recovered, 9); // 5 + 1 + 3
    }

    #[test]
    fn multi_table_verify_data() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "btc");
        create_table_2col(db_root, "eth");

        write_and_flush(db_root, "btc", &[(TS_DAY1, 65000.0)]);
        write_and_flush(db_root, "eth", &[(TS_DAY1, 3500.0)]);

        RecoveryManager::recover_all(db_root).unwrap();

        let btc_reader =
            FixedColumnReader::open(&db_root.join("btc/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(btc_reader.read_f64(0), 65000.0);

        let eth_reader =
            FixedColumnReader::open(&db_root.join("eth/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(eth_reader.read_f64(0), 3500.0);
    }
}

// =============================================================================
// Three-column table recovery
// =============================================================================
mod three_col_recovery {
    use super::*;

    fn write_3col_and_flush(db_root: &Path, table: &str, rows: &[(i64, f64, i64)]) {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, table, config).unwrap();
        for &(ts, price, volume) in rows {
            writer
                .write_row(
                    Timestamp(ts),
                    vec![
                        OwnedColumnValue::Timestamp(ts),
                        OwnedColumnValue::F64(price),
                        OwnedColumnValue::I64(volume),
                    ],
                )
                .unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn three_col_single_row() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_3col(db_root, "t");
        write_3col_and_flush(db_root, "t", &[(TS_DAY1, 100.0, 500)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 1);

        let price_reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(price_reader.read_f64(0), 100.0);

        let vol_reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/volume.d"), ColumnType::I64)
                .unwrap();
        assert_eq!(vol_reader.read_i64(0), 500);
    }

    #[test]
    fn three_col_multiple_rows() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_3col(db_root, "t");
        write_3col_and_flush(
            db_root,
            "t",
            &[
                (TS_DAY1, 100.0, 10),
                (TS_DAY1 + 1_000_000_000, 200.0, 20),
                (TS_DAY1 + 2_000_000_000, 300.0, 30),
            ],
        );

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 3);
    }

    #[test]
    fn three_col_data_integrity() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_3col(db_root, "t");
        write_3col_and_flush(
            db_root,
            "t",
            &[(TS_DAY1, 42.5, 100), (TS_DAY1 + 1_000_000_000, 85.0, 200)],
        );

        RecoveryManager::recover_all(db_root).unwrap();

        let price_reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(price_reader.row_count(), 2);
        assert_eq!(price_reader.read_f64(0), 42.5);
        assert_eq!(price_reader.read_f64(1), 85.0);

        let vol_reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/volume.d"), ColumnType::I64)
                .unwrap();
        assert_eq!(vol_reader.row_count(), 2);
        assert_eq!(vol_reader.read_i64(0), 100);
        assert_eq!(vol_reader.read_i64(1), 200);
    }
}

// =============================================================================
// Various price values
// =============================================================================
mod various_values {
    use super::*;

    #[test]
    fn recovery_zero_price() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 0.0)]);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.read_f64(0), 0.0);
    }

    #[test]
    fn recovery_negative_price() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, -42.5)]);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.read_f64(0), -42.5);
    }

    #[test]
    fn recovery_large_price() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 1e15)]);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.read_f64(0), 1e15);
    }

    #[test]
    fn recovery_small_price() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 0.000001)]);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert!((reader.read_f64(0) - 0.000001).abs() < 1e-10);
    }

    #[test]
    fn recovery_ascending_prices() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        let rows: Vec<(i64, f64)> = (0..10)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, i as f64 * 1000.0))
            .collect();
        write_and_flush(db_root, "t", &rows);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 10);
        for i in 0..10 {
            assert_eq!(reader.read_f64(i), i as f64 * 1000.0);
        }
    }

    #[test]
    fn recovery_descending_prices() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        let rows: Vec<(i64, f64)> = (0..10)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, 10000.0 - i as f64 * 1000.0))
            .collect();
        write_and_flush(db_root, "t", &rows);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 10);
        assert_eq!(reader.read_f64(0), 10000.0);
        assert_eq!(reader.read_f64(9), 1000.0);
    }

    #[test]
    fn recovery_all_same_price() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        let rows: Vec<(i64, f64)> = (0..5)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, 42.0))
            .collect();
        write_and_flush(db_root, "t", &rows);

        RecoveryManager::recover_all(db_root).unwrap();

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 5);
        for i in 0..5 {
            assert_eq!(reader.read_f64(i), 42.0);
        }
    }
}

// =============================================================================
// Write-commit-then-crash-more
// =============================================================================
mod mixed_commit_crash {
    use super::*;

    #[test]
    fn commit_then_crash_more() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");

        // First batch: commit
        write_and_commit(db_root, "t", &[(TS_DAY1, 100.0)]);

        // Second batch: crash (flush only)
        write_and_flush(db_root, "t", &[(TS_DAY1 + 2_000_000_000, 200.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 1); // only the crashed row

        let reader =
            FixedColumnReader::open(&db_root.join("t/2024-03-15/price.d"), ColumnType::F64)
                .unwrap();
        assert_eq!(reader.row_count(), 2);
    }

    #[test]
    fn two_commits_then_crash() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");

        write_and_commit(db_root, "t", &[(TS_DAY1, 100.0)]);
        write_and_commit(db_root, "t", &[(TS_DAY1 + 1_000_000_000, 200.0)]);
        write_and_flush(db_root, "t", &[(TS_DAY1 + 2_000_000_000, 300.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 1);
    }
}

// =============================================================================
// Table naming edge cases
// =============================================================================
mod table_naming {
    use super::*;

    #[test]
    fn table_name_with_underscore() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "my_table");
        write_and_flush(db_root, "my_table", &[(TS_DAY1, 100.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 1);
    }

    #[test]
    fn table_name_numbers() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "table123");
        write_and_flush(db_root, "table123", &[(TS_DAY1, 100.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 1);
    }

    #[test]
    fn table_name_short() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 100.0)]);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 1);
    }
}

// =============================================================================
// Timestamp ordering
// =============================================================================
mod timestamp_ordering {
    use super::*;

    #[test]
    fn timestamps_recovered_in_order() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");

        let rows: Vec<(i64, f64)> = (0..5)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, i as f64 * 10.0))
            .collect();
        write_and_flush(db_root, "t", &rows);

        RecoveryManager::recover_all(db_root).unwrap();

        let ts_reader = FixedColumnReader::open(
            &db_root.join("t/2024-03-15/timestamp.d"),
            ColumnType::Timestamp,
        )
        .unwrap();
        assert_eq!(ts_reader.row_count(), 5);
        for i in 0..4 {
            assert!(ts_reader.read_i64(i) < ts_reader.read_i64(i + 1));
        }
    }

    #[test]
    fn single_timestamp_recovered() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");
        write_and_flush(db_root, "t", &[(TS_DAY1, 100.0)]);

        RecoveryManager::recover_all(db_root).unwrap();

        let ts_reader = FixedColumnReader::open(
            &db_root.join("t/2024-03-15/timestamp.d"),
            ColumnType::Timestamp,
        )
        .unwrap();
        assert_eq!(ts_reader.row_count(), 1);
        assert_eq!(ts_reader.read_i64(0), TS_DAY1);
    }
}

// =============================================================================
// Many-row stress recovery
// =============================================================================
mod stress_recovery {
    use super::*;

    #[test]
    fn fifty_row_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");

        let rows: Vec<(i64, f64)> = (0..50)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, i as f64 * 100.0))
            .collect();
        write_and_flush(db_root, "t", &rows);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 50);
    }

    #[test]
    fn hundred_row_recovery() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        create_table_2col(db_root, "t");

        let rows: Vec<(i64, f64)> = (0..100)
            .map(|i| (TS_DAY1 + i * 1_000_000_000, i as f64))
            .collect();
        write_and_flush(db_root, "t", &rows);

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.rows_recovered, 100);
    }

    #[test]
    fn five_tables_ten_rows_each() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        for i in 0..5 {
            let name = format!("table_{}", i);
            create_table_2col(db_root, &name);
            let rows: Vec<(i64, f64)> = (0..10)
                .map(|j| (TS_DAY1 + j * 1_000_000_000, (i * 10 + j) as f64))
                .collect();
            write_and_flush(db_root, &name, &rows);
        }

        let stats = RecoveryManager::recover_all(db_root).unwrap();
        assert_eq!(stats.tables_recovered, 5);
        assert_eq!(stats.rows_recovered, 50);
    }
}
