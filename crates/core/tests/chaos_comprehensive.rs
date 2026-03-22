//! Comprehensive chaos testing for ExchangeDB.
//!
//! Simulates crash, corruption, disk-full, and concurrent stress scenarios
//! to verify WAL recovery, metadata resilience, and data consistency.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::engine::Engine;
use exchange_core::recovery::RecoveryManager;
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta};
use exchange_core::txn::TxnFile;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Timestamp for 2024-03-15 in nanoseconds.
const TS_BASE: i64 = 1_710_513_000_000_000_000;

fn create_test_table(db_root: &Path, name: &str) -> TableMeta {
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

fn write_rows_flush_no_commit(db_root: &Path, table: &str, n: usize) {
    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table, config).unwrap();
    for i in 0..n {
        let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(60000.0 + i as f64),
                ],
            )
            .unwrap();
    }
    writer.flush().unwrap();
    // Intentionally do NOT commit -- simulates crash.
}

fn write_rows_and_commit(db_root: &Path, table: &str, n: usize) {
    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table, config).unwrap();
    for i in 0..n {
        let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(60000.0 + i as f64),
                ],
            )
            .unwrap();
    }
    writer.commit().unwrap();
}

// ===========================================================================
// Test 1: Kill during write
// ===========================================================================

/// Start writing rows, drop the writer mid-flush (simulating a kill),
/// then verify WAL recovery restores a consistent state.
#[test]
fn kill_during_write() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");

    // Write 500 rows, flush to WAL, then drop without committing.
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "trades", config).unwrap();
        for i in 0..500 {
            let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(65000.0 + i as f64),
                    ],
                )
                .unwrap();
        }
        writer.flush().unwrap();
        // Drop without commit -- simulates kill.
    }

    // Verify data is NOT in column files before recovery.
    let part_dir = db_root.join("trades").join("2024-03-15");
    assert!(
        !part_dir.exists(),
        "partition should not exist before recovery"
    );

    // Run WAL recovery.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 1);
    assert_eq!(stats.rows_recovered, 500);

    // Verify all rows are now in column files.
    assert!(part_dir.exists(), "partition should exist after recovery");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 500);
    assert_eq!(reader.read_f64(0), 65000.0);
    assert_eq!(reader.read_f64(499), 65499.0);
}

// ===========================================================================
// Test 2: Corrupt WAL segment
// ===========================================================================

/// Write data, corrupt a WAL segment file by flipping random bytes, then
/// verify recovery handles corruption gracefully (skips or reports error,
/// does not panic).
#[test]
fn corrupt_wal_segment() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");

    // Write 100 rows, flush to WAL.
    write_rows_flush_no_commit(db_root, "trades", 100);

    // Find the WAL segment and corrupt it.
    let wal_dir = db_root.join("trades").join("wal");
    assert!(wal_dir.exists(), "WAL directory should exist");

    let mut wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("wal-") && name.ends_with(".wal")
        })
        .collect();
    assert!(
        !wal_files.is_empty(),
        "should have at least one WAL segment"
    );

    let seg_path = wal_files.remove(0).path();
    let mut data = std::fs::read(&seg_path).unwrap();
    let len = data.len();

    // Flip bytes in the middle of the file (corrupts event data/checksums).
    if len > 40 {
        for offset in [len / 2, len / 2 + 1, len / 2 + 2, len / 2 + 3] {
            data[offset] ^= 0xFF;
        }
    }
    std::fs::write(&seg_path, &data).unwrap();

    // Recovery should NOT panic. It may recover partial data or skip the
    // corrupted segment entirely.
    let result = RecoveryManager::recover_all(db_root);
    // The key assertion: no panic occurred. Recovery either succeeds with
    // partial data or returns an error gracefully.
    match result {
        Ok(stats) => {
            // Some rows may have been recovered before the corruption point.
            // The exact count depends on where the corruption landed.
            assert!(
                stats.rows_recovered <= 100,
                "should not recover more rows than written"
            );
        }
        Err(e) => {
            // Graceful error is acceptable (not a panic).
            let msg = format!("{e}");
            assert!(!msg.is_empty(), "error message should not be empty");
        }
    }
}

// ===========================================================================
// Test 3: Disk full simulation
// ===========================================================================

/// Write to a WAL with a very small max segment size, forcing frequent
/// rotation. Verify proper error handling: no panic, no data loss of
/// previously committed data.
#[test]
fn disk_full_simulation() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");

    // First, commit some data that should survive no matter what.
    write_rows_and_commit(db_root, "trades", 50);

    // Verify the committed data is present.
    let part_dir = db_root.join("trades").join("2024-03-15");
    assert!(part_dir.exists());
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 50);

    // Now write to a raw WAL with a tiny segment size to simulate disk pressure.
    let wal_dir = dir.path().join("diskfull_wal");
    let config = WalWriterConfig {
        max_segment_size: 64, // Very small -- forces frequent rotation
        commit_mode: CommitMode::Sync,
    };

    let mut writer = WalWriter::create(&wal_dir, config).unwrap();
    let mut success_count = 0u64;
    for i in 0..200 {
        match writer.append_data(i, vec![0xAA; 32]) {
            Ok(_) => success_count += 1,
            Err(_) => {
                // Should not panic -- graceful error is expected.
                break;
            }
        }
    }
    let _ = writer.flush(); // Should not panic.

    assert!(
        success_count > 0,
        "should have written at least some events before disk pressure"
    );

    // Verify the previously committed data is still intact.
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(
        reader.row_count(),
        50,
        "previously committed data must survive disk-full scenario"
    );
    assert_eq!(reader.read_f64(0), 60000.0);
    assert_eq!(reader.read_f64(49), 60049.0);
}

// ===========================================================================
// Test 4: Concurrent read/write stress
// ===========================================================================

/// 8 threads writing + 8 threads reading simultaneously for 5 seconds.
/// Uses Engine API for safe concurrent access (serialized writes, concurrent reads).
/// Verifies no panics, no data corruption, and row counts are consistent.
#[test]
fn concurrent_read_write_stress() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // Use Engine for thread-safe concurrent access.
    let engine = Arc::new(Engine::open(db_root).unwrap());
    engine
        .create_table(
            TableBuilder::new("stress")
                .column("timestamp", ColumnType::Timestamp)
                .column("open", ColumnType::F64)
                .column("high", ColumnType::F64)
                .column("low", ColumnType::F64)
                .column("close", ColumnType::F64)
                .column("volume", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    // Seed initial data so readers have something to read.
    {
        let mut handle = engine.get_writer("stress").unwrap();
        let writer = handle.writer();
        for i in 0..100_u64 {
            let ts = Timestamp(TS_BASE + (i as i64) * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    &[
                        ColumnValue::F64(100.0 + i as f64),
                        ColumnValue::F64(110.0 + i as f64),
                        ColumnValue::F64(90.0 + i as f64),
                        ColumnValue::F64(105.0 + i as f64),
                        ColumnValue::F64(1000.0 + i as f64),
                    ],
                )
                .unwrap();
        }
        writer.flush().unwrap();
        drop(handle);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_reads = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));
    let read_errors = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // 8 writer threads (Engine serializes writes via its Mutex).
    for tid in 0..8_u64 {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let total_writes = Arc::clone(&total_writes);
        let write_errors = Arc::clone(&write_errors);
        handles.push(thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match engine.get_writer("stress") {
                    Ok(mut handle) => {
                        let writer = handle.writer();
                        for _ in 0..10 {
                            let ts = Timestamp(
                                TS_BASE
                                    + (tid as i64) * 1_000_000_000_000_000
                                    + (i as i64) * 1_000_000_000,
                            );
                            let result = writer.write_row(
                                ts,
                                &[
                                    ColumnValue::F64(100.0 + i as f64),
                                    ColumnValue::F64(110.0),
                                    ColumnValue::F64(90.0),
                                    ColumnValue::F64(105.0),
                                    ColumnValue::F64(500.0),
                                ],
                            );
                            match result {
                                Ok(_) => {
                                    total_writes.fetch_add(1, Ordering::Relaxed);
                                    i += 1;
                                }
                                Err(_) => {
                                    write_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        let _ = writer.flush();
                        drop(handle);
                    }
                    Err(_) => {
                        write_errors.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }));
    }

    // 8 reader threads.
    for _tid in 0..8 {
        let db_root = db_root.to_path_buf();
        let stop = Arc::clone(&stop);
        let total_reads = Arc::clone(&total_reads);
        let read_errors = Arc::clone(&read_errors);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let part_dir = db_root.join("stress").join("2024-03-15");
                if part_dir.exists() {
                    match FixedColumnReader::open(&part_dir.join("open.d"), ColumnType::F64) {
                        Ok(reader) => {
                            let _count = reader.row_count();
                            total_reads.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            read_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    // Run for 5 seconds.
    thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);

    // Join all threads -- if any panicked, this will propagate the panic.
    for h in handles {
        h.join()
            .expect("thread panicked during concurrent stress test");
    }

    let writes = total_writes.load(Ordering::Relaxed);
    let reads = total_reads.load(Ordering::Relaxed);
    let w_errors = write_errors.load(Ordering::Relaxed);
    let r_errors = read_errors.load(Ordering::Relaxed);

    // Must have performed some operations.
    assert!(writes > 0, "should have completed some writes, got 0");
    assert!(reads > 0, "should have completed some reads, got 0");

    // Verify the seeded partition has consistent column row counts.
    let part_dir = db_root.join("stress").join("2024-03-15");
    if part_dir.exists() {
        let open_count = FixedColumnReader::open(&part_dir.join("open.d"), ColumnType::F64)
            .unwrap()
            .row_count();
        let high_count = FixedColumnReader::open(&part_dir.join("high.d"), ColumnType::F64)
            .unwrap()
            .row_count();
        let low_count = FixedColumnReader::open(&part_dir.join("low.d"), ColumnType::F64)
            .unwrap()
            .row_count();
        let close_count = FixedColumnReader::open(&part_dir.join("close.d"), ColumnType::F64)
            .unwrap()
            .row_count();
        let volume_count = FixedColumnReader::open(&part_dir.join("volume.d"), ColumnType::F64)
            .unwrap()
            .row_count();

        // All columns must have the same row count.
        assert_eq!(open_count, high_count, "open vs high row count mismatch");
        assert_eq!(open_count, low_count, "open vs low row count mismatch");
        assert_eq!(open_count, close_count, "open vs close row count mismatch");
        assert_eq!(
            open_count, volume_count,
            "open vs volume row count mismatch"
        );

        // At minimum, the 100 seeded rows should be present.
        assert!(
            open_count >= 100,
            "row count {} is less than seeded 100",
            open_count
        );
    }

    eprintln!(
        "Stress test completed: {} writes, {} reads, {} write errors, {} read errors",
        writes, reads, w_errors, r_errors
    );
}

// ===========================================================================
// Test 5: Crash recovery idempotent
// ===========================================================================

/// Write data, run recovery twice, verify same result both times.
/// The second recovery should find no pending data.
#[test]
fn crash_recovery_idempotent() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");

    // Write 200 rows, flush but do not commit.
    write_rows_flush_no_commit(db_root, "trades", 200);

    // First recovery.
    let stats1 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats1.tables_recovered, 1);
    assert_eq!(stats1.rows_recovered, 200);

    // Verify data.
    let part_dir = db_root.join("trades").join("2024-03-15");
    let reader1 = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    let count_after_first = reader1.row_count();
    assert_eq!(count_after_first, 200);

    // Read all values for comparison.
    let values_after_first: Vec<f64> = (0..count_after_first)
        .map(|i| reader1.read_f64(i))
        .collect();

    // Second recovery -- should find nothing new.
    let stats2 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(
        stats2.tables_recovered, 0,
        "second recovery should find no tables to recover"
    );
    assert_eq!(
        stats2.rows_recovered, 0,
        "second recovery should recover no rows"
    );

    // Verify data is identical after second recovery.
    let reader2 = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    let count_after_second = reader2.row_count();
    assert_eq!(
        count_after_first, count_after_second,
        "row count must be the same after idempotent recovery"
    );

    let values_after_second: Vec<f64> = (0..count_after_second)
        .map(|i| reader2.read_f64(i))
        .collect();
    assert_eq!(
        values_after_first, values_after_second,
        "data must be identical after idempotent recovery"
    );
}

// ===========================================================================
// Test 6: Metadata corruption
// ===========================================================================

/// Corrupt the _meta file, verify graceful error on table open (not panic).
#[test]
fn metadata_corruption() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "trades");

    // Write and commit some data first.
    write_rows_and_commit(db_root, "trades", 50);

    // Invalidate the cache so we actually read from disk.
    let meta_path = db_root.join("trades").join("_meta");
    TableMeta::invalidate_cache(&meta_path);

    // Corrupt the _meta file.
    let mut meta_bytes = std::fs::read(&meta_path).unwrap();
    assert!(
        meta_bytes.len() > 10,
        "_meta file should have meaningful content"
    );

    // Overwrite with garbage.
    for byte in meta_bytes.iter_mut().take(20) {
        *byte = 0xFF;
    }
    std::fs::write(&meta_path, &meta_bytes).unwrap();

    // Loading the corrupted metadata should return an error, not panic.
    let result = TableMeta::load(&meta_path);
    assert!(
        result.is_err(),
        "loading corrupted _meta should return an error"
    );

    // Recovery should gracefully skip the table with corrupted metadata.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    // The corrupted table is skipped; no panic.
    assert_eq!(
        stats.tables_recovered, 0,
        "corrupted table should be skipped during recovery"
    );
}
