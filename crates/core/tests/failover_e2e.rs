//! End-to-end automatic failover integration test.
//!
//! Verifies that when a primary becomes unreachable, a replica with
//! failover enabled detects the failure and promotes itself to primary,
//! after which it accepts writes.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::replication::config::{
    ReplicationConfig, ReplicationRole, ReplicationSyncMode,
};
use exchange_core::replication::health_monitor::PrimaryHealthMonitor;
use exchange_core::replication::manager::ReplicationManager;
use exchange_core::replication::wal_receiver::WalReceiver;
use exchange_core::table::TableBuilder;
use exchange_core::txn::TxnFile;
use exchange_core::wal::merge::WalMergeJob;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use tempfile::tempdir;

/// Helper: create the "trades" table at the given db_root.
fn create_trades_table(db_root: &Path) {
    let _meta = TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::I64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    let table_dir = db_root.join("trades");
    let _txn = TxnFile::open(&table_dir).unwrap();
}

/// Helper: write N rows with merge.
fn write_rows(db_root: &Path, count: usize, base_ts: i64) {
    let config = WalTableWriterConfig {
        buffer_capacity: count + 1,
        merge_on_commit: true,
        ..Default::default()
    };

    let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

    for i in 0..count {
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
}

/// Helper: write N rows without merge (WAL stays as .wal files).
fn write_rows_no_merge(db_root: &Path, count: usize, base_ts: i64) {
    let config = WalTableWriterConfig {
        buffer_capacity: count + 1,
        merge_on_commit: false,
        ..Default::default()
    };

    let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();

    for i in 0..count {
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
}

/// Helper: run merge on a table.
fn merge_table(db_root: &Path, table: &str) {
    let table_dir = db_root.join(table);
    let meta = exchange_core::table::TableMeta::load(&table_dir.join("_meta")).unwrap();
    let merge_job = WalMergeJob::new(table_dir, meta);
    let _stats = merge_job.run().unwrap();
}

/// Helper: ship WAL segments from primary to replica by local file copy.
fn ship_segments_locally(primary_root: &Path, replica_root: &Path, table: &str) {
    let primary_wal_dir = primary_root.join(table).join("wal");
    if !primary_wal_dir.exists() {
        return;
    }

    let mut segments: Vec<_> = std::fs::read_dir(&primary_wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
        .collect();
    segments.sort_by_key(|e| e.file_name());

    let mut receiver = WalReceiver::new(replica_root.to_path_buf(), "127.0.0.1:0".into());

    for entry in &segments {
        let data = std::fs::read(entry.path()).unwrap();
        receiver.apply_segment(table, &data).unwrap();
    }
}

/// Count rows across all partitions for a table.
fn count_rows(root: &Path, table: &str, column: &str, col_type: ColumnType) -> u64 {
    let table_dir = root.join(table);
    let mut total = 0u64;

    if !table_dir.exists() {
        return 0;
    }

    for entry in std::fs::read_dir(&table_dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('_') || name == "wal" {
                continue;
            }
            let col_path = path.join(format!("{column}.d"));
            if col_path.exists() {
                let reader = FixedColumnReader::open(&col_path, col_type).unwrap();
                total += reader.row_count() as u64;
            }
        }
    }

    total
}

// ===========================================================================
// Test: full_failover_cycle
// ===========================================================================
//
// 1. Start primary (TCP listener simulates primary).
// 2. Start replica with failover_enabled.
// 3. Write data to primary, ship to replica, merge.
// 4. Kill primary (drop TCP listener).
// 5. Wait for health monitor to detect failure and promote.
// 6. Verify replica now accepts writes.

#[tokio::test]
async fn full_failover_cycle() {
    let primary_dir = tempdir().unwrap();
    let replica_dir = tempdir().unwrap();

    // Create table on both primary and replica.
    create_trades_table(primary_dir.path());
    create_trades_table(replica_dir.path());

    // Start a TCP listener to simulate a reachable primary.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let primary_addr = listener.local_addr().unwrap().to_string();

    // 1. Write data on primary and ship to replica.
    let base_ts: i64 = 1_710_513_000_000_000_000; // 2024-03-15
    write_rows_no_merge(primary_dir.path(), 50, base_ts);
    ship_segments_locally(primary_dir.path(), replica_dir.path(), "trades");
    merge_table(primary_dir.path(), "trades");
    merge_table(replica_dir.path(), "trades");

    // Verify replica has 50 rows.
    let row_count = count_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(row_count, 50, "replica should have 50 rows before failover");

    // 2. Create replica ReplicationManager.
    let replica_config = ReplicationConfig {
        role: ReplicationRole::Replica,
        primary_addr: Some(primary_addr.clone()),
        replica_addrs: Vec::new(),
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        failover_enabled: true,
        health_check_interval: Duration::from_millis(50),
        failure_threshold: 3,
        ..Default::default()
    };

    let repl_mgr = Arc::new(ReplicationManager::new(
        replica_dir.path().to_path_buf(),
        replica_config,
    ));

    // Replica should be read-only initially.
    assert!(repl_mgr.is_read_only(), "replica must be read-only before failover");

    // 3. Start health monitor.
    let promoted = Arc::new(AtomicBool::new(false));
    let promoted_clone = promoted.clone();
    let mgr_for_failover = repl_mgr.clone();

    let monitor = Arc::new(PrimaryHealthMonitor::new(
        primary_addr.clone(),
        Duration::from_millis(50), // fast checks for testing
        3,                          // fail after 3 consecutive failures
    ));

    let monitor_handle = tokio::spawn({
        let monitor = monitor.clone();
        async move {
            monitor
                .start(move || {
                    mgr_for_failover.promote_to_primary();
                    promoted_clone.store(true, Ordering::SeqCst);
                })
                .await;
        }
    });

    // Let it run a couple of checks while primary is still up.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !promoted.load(Ordering::SeqCst),
        "should not promote while primary is alive"
    );
    assert!(
        repl_mgr.is_read_only(),
        "replica should still be read-only while primary is alive"
    );

    // 4. Kill primary (drop the listener).
    drop(listener);

    // 5. Wait for failover detection (3 failures * 50ms + buffer).
    // Give it up to 2 seconds.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !promoted.load(Ordering::SeqCst) {
        if tokio::time::Instant::now() >= deadline {
            panic!("Failover did not trigger within the expected time");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for the monitor task to finish.
    let _ = tokio::time::timeout(Duration::from_secs(1), monitor_handle).await;

    // 6. Verify promotion.
    assert!(
        promoted.load(Ordering::SeqCst),
        "failover callback must have been called"
    );
    assert!(
        !repl_mgr.is_read_only(),
        "promoted replica must accept writes (read_only = false)"
    );

    // 7. Write new data on the promoted replica (now acting as primary).
    let base_ts2 = base_ts + 50 * 1_000_000_000;
    write_rows(replica_dir.path(), 30, base_ts2);

    // 8. Verify data integrity: 50 original + 30 new = 80 rows.
    let row_count = count_rows(replica_dir.path(), "trades", "price", ColumnType::F64);
    assert_eq!(
        row_count, 80,
        "promoted replica should have 80 rows (50 replicated + 30 new)"
    );

    // 9. Verify data correctness.
    let part_dir = replica_dir.path().join("trades").join("2024-03-15");
    let price_reader =
        FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    // First row from replication.
    assert_eq!(price_reader.read_f64(0), 100.0);
    // Last replicated row.
    assert_eq!(price_reader.read_f64(49), 149.0);
    // First row written after promotion.
    assert_eq!(price_reader.read_f64(50), 100.0);
}

// ===========================================================================
// Test: health_monitor_does_not_promote_while_primary_alive
// ===========================================================================

#[tokio::test]
async fn health_monitor_does_not_promote_while_primary_alive() {
    // Start a TCP listener that stays alive throughout the test.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let primary_addr = listener.local_addr().unwrap().to_string();

    let promoted = Arc::new(AtomicBool::new(false));
    let promoted_clone = promoted.clone();

    let monitor = Arc::new(PrimaryHealthMonitor::new(
        primary_addr,
        Duration::from_millis(30),
        2,
    ));

    // Spawn the monitor; it should never trigger.
    let _handle = tokio::spawn({
        let monitor = monitor.clone();
        async move {
            monitor
                .start(move || {
                    promoted_clone.store(true, Ordering::SeqCst);
                })
                .await;
        }
    });

    // Let it run for 300ms (about 10 check intervals).
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        !promoted.load(Ordering::SeqCst),
        "should never promote while primary is reachable"
    );

    // Keep listener alive until test ends.
    drop(listener);
}

// ===========================================================================
// Test: promotion_enables_writes_on_replica_manager
// ===========================================================================

#[test]
fn promotion_enables_writes_on_replica_manager() {
    let dir = tempdir().unwrap();
    let config = ReplicationConfig {
        role: ReplicationRole::Replica,
        primary_addr: Some("127.0.0.1:19001".to_string()),
        replica_addrs: Vec::new(),
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        failover_enabled: true,
        ..Default::default()
    };

    let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);

    // Initially read-only.
    assert!(mgr.is_read_only());

    // Promote.
    mgr.promote_to_primary();

    // Now read-write.
    assert!(!mgr.is_read_only());
}

// ===========================================================================
// Test: failover_with_threshold_one
// ===========================================================================

#[tokio::test]
async fn failover_with_threshold_one() {
    // With threshold=1, failover should trigger after the very first failure.
    let promoted = Arc::new(AtomicBool::new(false));
    let promoted_clone = promoted.clone();

    let monitor = Arc::new(PrimaryHealthMonitor::new(
        "127.0.0.1:19996".into(), // unreachable
        Duration::from_millis(10),
        1,
    ));

    monitor
        .start(move || {
            promoted_clone.store(true, Ordering::SeqCst);
        })
        .await;

    assert!(promoted.load(Ordering::SeqCst));
}

// ===========================================================================
// Test: replication_config_failover_defaults
// ===========================================================================

#[test]
fn replication_config_failover_defaults() {
    let config = ReplicationConfig::default();
    assert!(!config.failover_enabled);
    assert_eq!(config.health_check_interval, Duration::from_secs(2));
    assert_eq!(config.failure_threshold, 3);
}
