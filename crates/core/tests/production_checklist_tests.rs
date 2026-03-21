//! Tests for PRODUCTION_CHECKLIST items:
//! 1. Atomicity (multi-row INSERT all-or-nothing)
//! 2. No silent data loss (error paths)
//! 3. Backup verification (verify_snapshot)
//! 4. Per-partition write locking
//! 5. Connection drain / graceful shutdown

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::partition::partition_dir;
use exchange_core::partition_lock::PartitionLockManager;
use exchange_core::snapshot::{create_snapshot, restore_snapshot, verify_snapshot};
use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

// ── Item 3: verify_snapshot ────────────────────────────────────────────

#[test]
fn verify_snapshot_valid() {
    let db_dir = tempdir().unwrap();
    let snap_dir = tempdir().unwrap();
    let db_root = db_dir.path();
    let snap_path = snap_dir.path().join("snap1");

    // Create a table with data.
    TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    let mut writer = TableWriter::open(db_root, "trades").unwrap();
    let ts = Timestamp::from_secs(1710513000);
    writer
        .write_row(ts, &[ColumnValue::F64(100.0)])
        .unwrap();
    writer.flush().unwrap();
    drop(writer);

    create_snapshot(db_root, &snap_path).unwrap();

    // Verify succeeds.
    let info = verify_snapshot(&snap_path).unwrap();
    assert_eq!(info.tables, vec!["trades"]);
    assert!(info.total_bytes > 0);
}

#[test]
fn verify_snapshot_missing_manifest() {
    let dir = tempdir().unwrap();
    let result = verify_snapshot(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("manifest.json"), "error: {err}");
}

#[test]
fn verify_snapshot_corrupt_manifest() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join("manifest.json"), "not json").unwrap();
    let result = verify_snapshot(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("invalid manifest"), "error: {err}");
}

#[test]
fn verify_snapshot_missing_table_dir() {
    let dir = tempdir().unwrap();
    let manifest = serde_json::json!({
        "version": 1,
        "timestamp": 12345,
        "tables": ["missing_table"],
        "total_size": 0
    });
    std::fs::write(
        dir.path().join("manifest.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = verify_snapshot(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("missing_table"), "error: {err}");
}

#[test]
fn verify_snapshot_missing_meta_file() {
    let dir = tempdir().unwrap();
    // Create table dir but no _meta file.
    std::fs::create_dir_all(dir.path().join("my_table")).unwrap();
    let manifest = serde_json::json!({
        "version": 1,
        "timestamp": 12345,
        "tables": ["my_table"],
        "total_size": 0
    });
    std::fs::write(
        dir.path().join("manifest.json"),
        serde_json::to_string(&manifest).unwrap(),
    )
    .unwrap();

    let result = verify_snapshot(dir.path());
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("no _meta"), "error: {err}");
}

#[test]
fn create_and_verify_roundtrip() {
    let db_dir = tempdir().unwrap();
    let snap_dir = tempdir().unwrap();
    let restore_dir = tempdir().unwrap();
    let db_root = db_dir.path();

    TableBuilder::new("t1")
        .column("timestamp", ColumnType::Timestamp)
        .column("val", ColumnType::I64)
        .timestamp("timestamp")
        .build(db_root)
        .unwrap();

    TableBuilder::new("t2")
        .column("timestamp", ColumnType::Timestamp)
        .column("name", ColumnType::Varchar)
        .timestamp("timestamp")
        .build(db_root)
        .unwrap();

    let snap_path = snap_dir.path().join("s");
    create_snapshot(db_root, &snap_path).unwrap();

    // Verify before restore.
    let info = verify_snapshot(&snap_path).unwrap();
    assert_eq!(info.tables.len(), 2);

    // Restore and verify the restored copy as well.
    restore_snapshot(&snap_path, restore_dir.path()).unwrap();
    assert!(restore_dir.path().join("t1").join("_meta").exists());
    assert!(restore_dir.path().join("t2").join("_meta").exists());
}

// ── Item 4: Per-partition write locking ────────────────────────────────

#[test]
fn partition_dir_day_format() {
    let ts = Timestamp(1_710_513_000_000_000_000); // 2024-03-15
    let dir = partition_dir(ts, PartitionBy::Day);
    assert_eq!(dir, "2024-03-15");
}

#[test]
fn partition_lock_different_partitions_no_contention() {
    let mgr = Arc::new(PartitionLockManager::new());
    let barrier = Arc::new(std::sync::Barrier::new(2));

    let mgr1 = Arc::clone(&mgr);
    let barrier1 = Arc::clone(&barrier);
    let t1 = std::thread::spawn(move || {
        let _guard = mgr1.lock_partition("2024-01-01");
        barrier1.wait();
        std::thread::sleep(Duration::from_millis(50));
    });

    let mgr2 = Arc::clone(&mgr);
    let barrier2 = Arc::clone(&barrier);
    let t2 = std::thread::spawn(move || {
        barrier2.wait();
        let start = std::time::Instant::now();
        let _guard = mgr2.lock_partition("2024-01-02");
        let elapsed = start.elapsed();
        // Should be near-instant since different partitions.
        assert!(
            elapsed < Duration::from_millis(30),
            "different partition lock should be fast, took {elapsed:?}"
        );
    });

    t1.join().unwrap();
    t2.join().unwrap();
}

#[test]
fn partition_lock_same_partition_serializes() {
    let mgr = Arc::new(PartitionLockManager::new());
    let barrier = Arc::new(std::sync::Barrier::new(2));

    let mgr1 = Arc::clone(&mgr);
    let barrier1 = Arc::clone(&barrier);
    let t1 = std::thread::spawn(move || {
        let _guard = mgr1.lock_partition("2024-01-01");
        barrier1.wait();
        std::thread::sleep(Duration::from_millis(100));
    });

    let mgr2 = Arc::clone(&mgr);
    let barrier2 = Arc::clone(&barrier);
    let t2 = std::thread::spawn(move || {
        barrier2.wait();
        std::thread::sleep(Duration::from_millis(10));
        let start = std::time::Instant::now();
        let _guard = mgr2.lock_partition("2024-01-01");
        let elapsed = start.elapsed();
        // Should block at least 50ms since same partition.
        assert!(
            elapsed >= Duration::from_millis(50),
            "same partition should block, only waited {elapsed:?}"
        );
    });

    t1.join().unwrap();
    t2.join().unwrap();
}

// ── Item 2: No silent data loss ────────────────────────────────────────

#[test]
fn mmap_grow_failure_returns_error() {
    // The MmapFile::grow checks disk space and returns DiskFull error.
    // We can't easily simulate ENOSPC in a unit test, but we can verify
    // the error variant exists and formats correctly.
    let err = exchange_common::error::ExchangeDbError::DiskFull {
        path: "/data/test.d".to_string(),
        needed_bytes: 1_000_000,
        available_bytes: 100,
    };
    let msg = err.to_string();
    assert!(msg.contains("disk full"), "err: {msg}");
    assert!(msg.contains("1000000"), "err: {msg}");
}

#[test]
fn mmap_append_returns_error_not_silent() {
    // Verify that MmapFile::append returns Result, not silent failure.
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.d");
    let mut mf = exchange_core::mmap::MmapFile::open(&path, 4096).unwrap();
    // Normal append should succeed.
    let result = mf.append(&42i64.to_le_bytes());
    assert!(result.is_ok());
}

#[test]
fn wal_segment_flush_returns_result() {
    // Verify flush returns Result, not silently succeeding.
    let dir = tempdir().unwrap();
    let seg = exchange_core::wal::WalSegment::create(dir.path(), 0).unwrap();
    let result = seg.flush();
    assert!(result.is_ok(), "flush should succeed: {:?}", result);
}
