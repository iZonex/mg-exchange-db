//! Integration test for real-time WAL streaming replication.
//!
//! Tests the full pipeline: primary INSERT -> WAL commit -> TCP ship to
//! replica -> replica merge -> replica has data.
//!
//! Uses two in-process nodes (shared data directories) with actual TCP
//! connections between the primary's WalShipper and the replica's
//! replication_server listener.

use std::path::Path;
use std::sync::Arc;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::replication::ReplicationManager;
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::table::TableBuilder;
use exchange_core::txn::TxnFile;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use tempfile::tempdir;

/// Create the "trades" table with schema (timestamp, price, volume).
fn create_trades_table(db_root: &Path) {
    let _meta = TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::I64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    // Initialize the _txn file.
    let table_dir = db_root.join("trades");
    let _txn = TxnFile::open(&table_dir).unwrap();
}

/// Count rows in a fixed-width column file for verification.
fn count_rows(db_root: &Path, table: &str, column: &str, col_type: ColumnType) -> u64 {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return 0;
    }

    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(&table_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let col_path = entry.path().join(format!("{column}.d"));
                if col_path.exists() {
                    if let Ok(reader) = FixedColumnReader::open(&col_path, col_type) {
                        total += reader.row_count();
                    }
                }
            }
        }
    }
    total
}

/// End-to-end test: primary writes rows through WAL, ships them over TCP
/// to a replica's replication server, and the replica merges them into
/// column files that can be read back.
#[tokio::test]
async fn wal_streaming_replication_e2e() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Create the same table schema on both primary and replica.
    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    // --- Start the replica's replication TCP listener ---
    // Use port 0 to let the OS assign a free port.
    let replica_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_repl_addr = replica_listener.local_addr().unwrap();
    drop(replica_listener); // Release so replication_server can bind.

    let replica_root = replica_dir.path().to_path_buf();
    let repl_server_handle = tokio::spawn(async move {
        exchange_net::replication_server::start_replication_server(replica_repl_addr, replica_root)
            .await
    });

    // Give the listener a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // --- Set up the primary's replication manager ---
    let primary_config = ReplicationConfig {
        role: ReplicationRole::Primary,
        primary_addr: None,
        replica_addrs: vec![replica_repl_addr.to_string()],
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    };

    let mut primary_mgr = ReplicationManager::new(primary_dir.path().to_path_buf(), primary_config);
    primary_mgr.start().await.unwrap();
    let primary_mgr = Arc::new(primary_mgr);

    // --- Write rows on the primary through WalTableWriter ---
    let mgr_clone = Arc::clone(&primary_mgr);
    let primary_root = primary_dir.path().to_path_buf();

    tokio::task::spawn_blocking(move || {
        let config = WalTableWriterConfig {
            buffer_capacity: 100,
            merge_on_commit: true,
            ..Default::default()
        };

        let mut writer = WalTableWriter::open(&primary_root, "trades", config).unwrap();
        writer.set_replication_manager(mgr_clone);

        let base_ts: i64 = 1_710_513_000_000_000_000; // 2024-03-15

        for i in 0..5 {
            let ts = Timestamp(base_ts + (i as i64) * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(100.0 + i as f64),
                        OwnedColumnValue::I64(i as i64),
                    ],
                )
                .unwrap();
        }

        writer.commit().unwrap();
    })
    .await
    .unwrap();

    // --- Verify primary has the data ---
    let primary_rows = count_rows(primary_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(primary_rows, 5, "primary should have 5 rows");

    // --- Wait for replication to complete (up to 2 seconds) ---
    let start = std::time::Instant::now();
    let mut replica_rows = 0;
    while start.elapsed() < std::time::Duration::from_secs(2) {
        replica_rows = count_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
        if replica_rows >= 5 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(
        replica_rows, 5,
        "replica should have 5 rows within 2 seconds (got {replica_rows})"
    );

    // Verify data correctness on the replica.
    let replica_part_dir = replica_dir.path().join("trades").join("2024-03-15");
    assert!(
        replica_part_dir.exists(),
        "replica partition directory should exist"
    );

    let price_reader =
        FixedColumnReader::open(&replica_part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(price_reader.row_count(), 5);
    assert_eq!(price_reader.read_f64(0), 100.0);
    assert_eq!(price_reader.read_f64(4), 104.0);

    let volume_reader =
        FixedColumnReader::open(&replica_part_dir.join("volume.d"), ColumnType::I64).unwrap();
    assert_eq!(volume_reader.row_count(), 5);

    // Clean up: abort the replication server.
    repl_server_handle.abort();
}

/// Test that multiple batches of writes are all replicated.
#[tokio::test]
async fn wal_streaming_multiple_batches() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    // Start replica listener.
    let replica_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let replica_repl_addr = replica_listener.local_addr().unwrap();
    drop(replica_listener);

    let replica_root = replica_dir.path().to_path_buf();
    let repl_server_handle = tokio::spawn(async move {
        exchange_net::replication_server::start_replication_server(replica_repl_addr, replica_root)
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let primary_config = ReplicationConfig {
        role: ReplicationRole::Primary,
        primary_addr: None,
        replica_addrs: vec![replica_repl_addr.to_string()],
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    };

    let mut primary_mgr = ReplicationManager::new(primary_dir.path().to_path_buf(), primary_config);
    primary_mgr.start().await.unwrap();
    let primary_mgr = Arc::new(primary_mgr);

    // Write two separate batches.
    for batch in 0..2 {
        let mgr = Arc::clone(&primary_mgr);
        let root = primary_dir.path().to_path_buf();
        let base_ts: i64 = 1_710_513_000_000_000_000 + batch * 10_000_000_000;

        tokio::task::spawn_blocking(move || {
            let config = WalTableWriterConfig {
                buffer_capacity: 100,
                merge_on_commit: true,
                ..Default::default()
            };

            let mut writer = WalTableWriter::open(&root, "trades", config).unwrap();
            writer.set_replication_manager(mgr);

            for i in 0..3 {
                let ts = Timestamp(base_ts + (i as i64) * 1_000_000_000);
                writer
                    .write_row(
                        ts,
                        vec![
                            OwnedColumnValue::Timestamp(ts.0),
                            OwnedColumnValue::F64(200.0 + i as f64),
                            OwnedColumnValue::I64(i as i64),
                        ],
                    )
                    .unwrap();
            }

            writer.commit().unwrap();
        })
        .await
        .unwrap();

        // Small delay between batches.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Wait for replication.
    let start = std::time::Instant::now();
    let mut replica_rows = 0;
    while start.elapsed() < std::time::Duration::from_secs(2) {
        replica_rows = count_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
        if replica_rows >= 6 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(
        replica_rows, 6,
        "replica should have 6 rows from two batches (got {replica_rows})"
    );

    repl_server_handle.abort();
}
