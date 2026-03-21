//! Crash recovery integration tests.
//!
//! These tests simulate various crash scenarios by writing to WAL without
//! committing, then verifying that `RecoveryManager` correctly replays
//! the WAL data into column files.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::recovery::RecoveryManager;
use exchange_core::table::{TableBuilder, TableMeta};
use exchange_core::txn::TxnFile;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use std::path::Path;
use tempfile::tempdir;

/// Helper: create a test table with timestamp + price columns.
fn create_test_table(db_root: &Path, name: &str) -> TableMeta {
    let meta = TableBuilder::new(name)
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    // Initialize _txn file.
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

/// Timestamp for 2024-03-15 in nanoseconds.
const TS_2024_03_15: i64 = 1_710_513_000_000_000_000;

#[test]
fn crash_during_wal_write_recovers_data() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");
    let table_dir = db_root.join("trades");

    // Write rows through WalTableWriter, flush but do NOT commit.
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

        let ts = Timestamp(TS_2024_03_15);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(65000.0),
                ],
            )
            .unwrap();

        let ts2 = Timestamp(TS_2024_03_15 + 1_000_000_000);
        writer
            .write_row(
                ts2,
                vec![
                    OwnedColumnValue::Timestamp(ts2.0),
                    OwnedColumnValue::F64(65100.0),
                ],
            )
            .unwrap();

        // Flush to WAL but do NOT commit (simulates crash).
        writer.flush().unwrap();
        // Drop simulates process death.
    }

    // Verify data is NOT in column files yet.
    let part_dir = table_dir.join("2024-03-15");
    assert!(
        !part_dir.exists(),
        "partition should not exist before recovery"
    );

    // Run recovery.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 1);
    assert_eq!(stats.rows_recovered, 2);
    assert!(stats.segments_replayed >= 1);

    // Verify data IS now in column files.
    assert!(part_dir.exists(), "partition should exist after recovery");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 2);
    assert_eq!(reader.read_f64(0), 65000.0);
    assert_eq!(reader.read_f64(1), 65100.0);
}

#[test]
fn crash_after_wal_commit_data_persisted() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");
    let table_dir = db_root.join("trades");

    // Write rows, commit (full path: WAL -> merge -> column files).
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

        let ts = Timestamp(TS_2024_03_15);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(42000.0),
                ],
            )
            .unwrap();

        writer.commit().unwrap();
        // Drop writer.
    }

    // Data should be in column files already (committed).
    let part_dir = table_dir.join("2024-03-15");
    assert!(part_dir.exists(), "partition should exist after commit");

    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 1);
    assert_eq!(reader.read_f64(0), 42000.0);

    // Recovery should find no new data to recover.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.rows_recovered, 0);
}

#[test]
fn recovery_idempotent() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");
    let table_dir = db_root.join("trades");

    // Write and flush (no commit).
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

        let ts = Timestamp(TS_2024_03_15);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(99000.0),
                ],
            )
            .unwrap();

        writer.flush().unwrap();
    }

    // First recovery.
    let stats1 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats1.tables_recovered, 1);
    assert_eq!(stats1.rows_recovered, 1);

    // Second recovery should find nothing (segments renamed to .applied).
    let stats2 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats2.tables_recovered, 0);
    assert_eq!(stats2.rows_recovered, 0);

    // Verify data is still correct (no duplicates).
    let part_dir = table_dir.join("2024-03-15");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 1);
    assert_eq!(reader.read_f64(0), 99000.0);
}

#[test]
fn empty_database_recovery() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // Recovery on empty directory does nothing.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 0);
    assert_eq!(stats.segments_replayed, 0);
    assert_eq!(stats.rows_recovered, 0);
}

#[test]
fn multi_table_recovery() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // Create 3 tables.
    let _meta1 = create_test_table(db_root, "btc_trades");
    let _meta2 = create_test_table(db_root, "eth_trades");
    let _meta3 = create_table_3col(db_root, "orders");

    // Write to all tables, flush but do NOT commit.
    {
        // btc_trades: 2 rows.
        let config = WalTableWriterConfig::default();
        let mut w1 = WalTableWriter::open(db_root, "btc_trades", config).unwrap();
        let ts = Timestamp(TS_2024_03_15);
        w1.write_row(
            ts,
            vec![
                OwnedColumnValue::Timestamp(ts.0),
                OwnedColumnValue::F64(65000.0),
            ],
        )
        .unwrap();
        let ts2 = Timestamp(TS_2024_03_15 + 1_000_000_000);
        w1.write_row(
            ts2,
            vec![
                OwnedColumnValue::Timestamp(ts2.0),
                OwnedColumnValue::F64(65100.0),
            ],
        )
        .unwrap();
        w1.flush().unwrap();

        // eth_trades: 1 row.
        let config2 = WalTableWriterConfig::default();
        let mut w2 = WalTableWriter::open(db_root, "eth_trades", config2).unwrap();
        w2.write_row(
            ts,
            vec![
                OwnedColumnValue::Timestamp(ts.0),
                OwnedColumnValue::F64(3500.0),
            ],
        )
        .unwrap();
        w2.flush().unwrap();

        // orders: 1 row.
        let config3 = WalTableWriterConfig::default();
        let mut w3 = WalTableWriter::open(db_root, "orders", config3).unwrap();
        w3.write_row(
            ts,
            vec![
                OwnedColumnValue::Timestamp(ts.0),
                OwnedColumnValue::F64(100.0),
                OwnedColumnValue::I64(500),
            ],
        )
        .unwrap();
        w3.flush().unwrap();
    }

    // Recover all tables.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 3);
    assert_eq!(stats.rows_recovered, 4); // 2 + 1 + 1

    // Verify each table's data.
    let btc_dir = db_root.join("btc_trades").join("2024-03-15");
    let reader = FixedColumnReader::open(&btc_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 2);

    let eth_dir = db_root.join("eth_trades").join("2024-03-15");
    let reader = FixedColumnReader::open(&eth_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 1);
    assert_eq!(reader.read_f64(0), 3500.0);

    let orders_dir = db_root.join("orders").join("2024-03-15");
    let reader = FixedColumnReader::open(&orders_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 1);
    assert_eq!(reader.read_f64(0), 100.0);
}
