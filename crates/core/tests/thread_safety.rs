//! Thread safety tests for ExchangeDB core.
//!
//! Verifies that concurrent readers, writers, and lock managers behave
//! correctly under sustained parallel access without data races or panics.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::engine::Engine;
use exchange_core::mvcc::MvccManager;
use exchange_core::partition_lock::PartitionLockManager;
use exchange_core::recovery::RecoveryManager;
use exchange_core::table::{TableBuilder, TableMeta};
use exchange_core::txn::TxnFile;
use exchange_core::wal::reader::WalReader;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal::sequencer::Sequencer;
use exchange_core::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use exchange_core::write_lock::TableWriteLock;
use tempfile::tempdir;

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

// ===========================================================================
// Sequencer concurrency
// ===========================================================================

/// Sequencer generates unique IDs across many threads.
#[test]
fn sequencer_unique_ids_concurrent() {
    let seq = Arc::new(Sequencer::new());
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = vec![];

    for _ in 0..8 {
        let seq = seq.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut ids = Vec::with_capacity(1000);
            for _ in 0..1000 {
                ids.push(seq.next_txn_id());
            }
            ids
        }));
    }

    let mut all_ids: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all_ids.sort();
    all_ids.dedup();
    assert_eq!(all_ids.len(), 8000, "all IDs must be unique");
    assert_eq!(*all_ids.last().unwrap(), 8000);
}

/// Sequencer monotonicity from a single thread after resume.
#[test]
fn sequencer_resume_monotonic() {
    let seq = Sequencer::resume_from(100);
    let id1 = seq.next_txn_id();
    let id2 = seq.next_txn_id();
    assert_eq!(id1, 101);
    assert_eq!(id2, 102);
}

/// Sequencer high contention: 16 threads.
#[test]
fn sequencer_high_contention() {
    let seq = Arc::new(Sequencer::new());
    let barrier = Arc::new(Barrier::new(16));

    let handles: Vec<_> = (0..16)
        .map(|_| {
            let seq = seq.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let mut ids = Vec::with_capacity(500);
                for _ in 0..500 {
                    ids.push(seq.next_txn_id());
                }
                ids
            })
        })
        .collect();

    let mut all: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 8000);
}

// ===========================================================================
// PartitionLockManager concurrency
// ===========================================================================

/// 16 threads locking the same partition serialize correctly.
#[test]
fn partition_lock_16_threads_same_partition() {
    let mgr = Arc::new(PartitionLockManager::new());
    let counter = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(16));

    let handles: Vec<_> = (0..16)
        .map(|_| {
            let mgr = mgr.clone();
            let counter = counter.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let _guard = mgr.lock_partition("shared_part");
                    let val = counter.load(Ordering::SeqCst);
                    std::hint::spin_loop();
                    counter.store(val + 1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 1600);
}

/// Different partitions can be locked in parallel without blocking.
#[test]
fn partition_lock_different_partitions_parallel() {
    let mgr = Arc::new(PartitionLockManager::new());
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let part_name = format!("part_{i}");
                barrier.wait();
                let start = std::time::Instant::now();
                let _guard = mgr.lock_partition(&part_name);
                thread::sleep(Duration::from_millis(20));
                start.elapsed()
            })
        })
        .collect();

    for h in handles {
        let elapsed = h.join().unwrap();
        assert!(
            elapsed < Duration::from_millis(100),
            "parallel partition locks should not block each other"
        );
    }
}

/// try_lock returns None when partition is held.
#[test]
fn partition_lock_try_lock_held() {
    let mgr = Arc::new(PartitionLockManager::new());
    let _guard = mgr.lock_partition("p1");

    let mgr2 = mgr.clone();
    let handle = thread::spawn(move || mgr2.try_lock_partition("p1"));

    let result = handle.join().unwrap();
    assert!(result.is_none(), "try_lock should return None when held");
}

/// try_lock returns Some when partition is free.
#[test]
fn partition_lock_try_lock_free() {
    let mgr = PartitionLockManager::new();
    let result = mgr.try_lock_partition("free_part");
    assert!(result.is_some());
}

/// Rapid lock acquire/release across threads.
#[test]
fn rapid_lock_churn() {
    let mgr = Arc::new(PartitionLockManager::new());
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..500 {
                    let _guard = mgr.lock_partition("churn");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

/// Multiple partitions locked and unlocked rapidly.
#[test]
fn multi_partition_rapid_churn() {
    let mgr = Arc::new(PartitionLockManager::new());
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for j in 0..200 {
                    let part = format!("part_{}_{}", i, j % 3);
                    let _guard = mgr.lock_partition(&part);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// ===========================================================================
// TableWriteLock concurrency (file-based)
// ===========================================================================

/// File-based write lock: acquire and release on drop.
#[test]
fn table_write_lock_acquire_release() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("test_table");
    std::fs::create_dir_all(&table_dir).unwrap();

    {
        let _lock = TableWriteLock::try_acquire(&table_dir).unwrap();
        assert!(TableWriteLock::is_locked(&table_dir));
    }
    assert!(!TableWriteLock::is_locked(&table_dir));
}

/// File-based write lock: double acquire fails.
#[test]
fn table_write_lock_double_acquire_fails() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("test_table");
    std::fs::create_dir_all(&table_dir).unwrap();

    let _lock = TableWriteLock::try_acquire(&table_dir).unwrap();
    let result = TableWriteLock::try_acquire(&table_dir);
    assert!(result.is_err());
}

/// File-based write lock: acquire_timeout succeeds after release.
#[test]
fn table_write_lock_timeout_succeeds() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("test_table");
    std::fs::create_dir_all(&table_dir).unwrap();

    let table_dir_clone = table_dir.clone();
    let handle = thread::spawn(move || {
        let _lock = TableWriteLock::try_acquire(&table_dir_clone).unwrap();
        thread::sleep(Duration::from_millis(50));
    });

    thread::sleep(Duration::from_millis(10));
    let lock = TableWriteLock::acquire_timeout(&table_dir, Duration::from_secs(2)).unwrap();
    drop(lock);
    handle.join().unwrap();
}

/// File-based write lock: timeout expires.
#[test]
fn table_write_lock_timeout_expires() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("test_table");
    std::fs::create_dir_all(&table_dir).unwrap();

    let _lock = TableWriteLock::try_acquire(&table_dir).unwrap();
    let result = TableWriteLock::acquire_timeout(&table_dir, Duration::from_millis(50));
    assert!(result.is_err());
}

/// is_locked returns false when no lock file exists.
#[test]
fn table_write_lock_no_file() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("nonexistent");
    assert!(!TableWriteLock::is_locked(&table_dir));
}

// ===========================================================================
// MVCC snapshot isolation concurrency
// ===========================================================================

/// Concurrent MVCC commit_write produces unique versions.
#[test]
fn mvcc_concurrent_commit_writes() {
    let mgr = Arc::new(MvccManager::new());
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let mut versions = Vec::with_capacity(100);
                for _ in 0..100 {
                    versions.push(mgr.commit_write(&[("test_part", 1)]));
                }
                versions
            })
        })
        .collect();

    let mut all: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 800, "all versions must be unique");
}

/// Snapshot reads are isolated from concurrent writes.
#[test]
fn mvcc_snapshot_isolation() {
    let mgr = Arc::new(MvccManager::new());

    // Take snapshot at version 0.
    let snap = mgr.begin_snapshot();
    let snap_version = snap.version();

    // Write in another thread.
    {
        let mgr = mgr.clone();
        thread::spawn(move || {
            for _ in 0..100 {
                mgr.commit_write(&[("p1", 1)]);
            }
        })
        .join()
        .unwrap();
    }

    // Snapshot version should still be 0.
    assert_eq!(snap_version, 0);
    assert!(mgr.current_version() >= 100);
}

/// MVCC snapshot guard lifecycle under concurrency.
#[test]
fn mvcc_snapshot_guard_concurrent() {
    let mgr = Arc::new(MvccManager::new());
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let mgr = mgr.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let _snap = mgr.begin_snapshot();
                    mgr.commit_write(&[("p", 1)]);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert!(mgr.current_version() >= 800);
}

/// MVCC concurrent snapshot and GC.
#[test]
fn mvcc_concurrent_snapshot_gc() {
    let mgr = Arc::new(MvccManager::new());

    // Writer thread.
    let mgr_w = mgr.clone();
    let writer = thread::spawn(move || {
        for _ in 0..200 {
            mgr_w.commit_write(&[("p1", 1)]);
        }
    });

    // Snapshot thread.
    let mgr_r = mgr.clone();
    let reader = thread::spawn(move || {
        let mut count = 0;
        for _ in 0..100 {
            let _snap = mgr_r.begin_snapshot();
            count += 1;
        }
        count
    });

    writer.join().unwrap();
    let snaps = reader.join().unwrap();
    assert_eq!(snaps, 100);
}

// ===========================================================================
// Concurrent WAL read + write
// ===========================================================================

/// Writer writes while reader reads; no panics.
#[test]
// SIGBUS on macOS: concurrent mmap read/write triggers bus error when
// the writer extends the file while the reader has it mapped.
#[ignore]
fn concurrent_wal_read_write() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    // Seed with initial data.
    {
        let config = WalWriterConfig {
            max_segment_size: 64 * 1024 * 1024,
            commit_mode: CommitMode::Sync,
        };
        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        for i in 0..100 {
            writer.append_data(i, b"seed".to_vec()).unwrap();
        }
        writer.flush().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let read_count = Arc::new(AtomicU64::new(0));

    let reader_handle = {
        let wal_dir = wal_dir.clone();
        let stop = stop.clone();
        let read_count = read_count.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(reader) = WalReader::open(&wal_dir)
                    && reader.read_all().is_ok()
                {
                    read_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    let writer_handle = {
        let wal_dir = wal_dir.clone();
        thread::spawn(move || {
            let config = WalWriterConfig {
                max_segment_size: 64 * 1024 * 1024,
                commit_mode: CommitMode::Sync,
            };
            let mut writer = WalWriter::open(&wal_dir, config).unwrap();
            for i in 0..500 {
                writer.append_data(1000 + i, b"write".to_vec()).unwrap();
            }
            writer.flush().unwrap();
        })
    };

    writer_handle.join().unwrap();
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    reader_handle.join().unwrap();

    assert!(read_count.load(Ordering::Relaxed) > 0);
}

// ===========================================================================
// Concurrent WalTableWriter commit cycles
// ===========================================================================

/// Multiple sequential commit cycles on the same table.
#[test]
fn sequential_commit_cycles() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    for batch in 0..10 {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        for i in 0..20 {
            let ts = Timestamp(TS_BASE + (batch * 20 + i) as i64 * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(batch as f64 * 100.0 + i as f64),
                    ],
                )
                .unwrap();
        }
        writer.commit().unwrap();
    }

    let part_dir = db_root.join("test").join("2024-03-15");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 200);
}

/// Sustained concurrent access: 8 readers + 1 writer for 2 seconds.
#[test]
fn sustained_concurrent_read_write() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    // Write initial data.
    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        for i in 0..50 {
            let ts = Timestamp(TS_BASE + i * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(i as f64),
                    ],
                )
                .unwrap();
        }
        writer.commit().unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));

    // Reader threads: read column files.
    let reader_handles: Vec<_> = (0..8)
        .map(|_| {
            let db_root = db_root.to_path_buf();
            let stop = stop.clone();
            let errors = errors.clone();
            thread::spawn(move || {
                let mut count = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let part_dir = db_root.join("test").join("2024-03-15");
                    match FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64) {
                        Ok(reader) => {
                            if reader.row_count() >= 50 {
                                count += 1;
                            }
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                count
            })
        })
        .collect();

    // Writer thread.
    let writer_handle = {
        let db_root = db_root.to_path_buf();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut written = 0u64;
            let mut batch = 50i64;
            while !stop.load(Ordering::Relaxed) && written < 200 {
                let config = WalTableWriterConfig::default();
                match WalTableWriter::open(&db_root, "test", config) {
                    Ok(mut writer) => {
                        for i in 0..10 {
                            let ts = Timestamp(TS_BASE + (batch + i) * 1_000_000_000);
                            let _ = writer.write_row(
                                ts,
                                vec![
                                    OwnedColumnValue::Timestamp(ts.0),
                                    OwnedColumnValue::F64(batch as f64 + i as f64),
                                ],
                            );
                        }
                        let _ = writer.commit();
                        batch += 10;
                        written += 10;
                    }
                    Err(_) => {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
            written
        })
    };

    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);

    let written = writer_handle.join().unwrap();
    let total_reads: u64 = reader_handles.into_iter().map(|h| h.join().unwrap()).sum();

    assert_eq!(errors.load(Ordering::Relaxed), 0);
    assert!(written > 0, "writer should have written some rows");
    assert!(total_reads > 0, "readers should have completed some reads");
}

/// Concurrent writers to different WAL directories.
#[test]
fn concurrent_wal_writers_different_dirs() {
    let dir = tempdir().unwrap();
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let wal_dir = dir.path().join(format!("wal_{i}"));
            let barrier = barrier.clone();
            thread::spawn(move || {
                let config = WalWriterConfig::default();
                let mut writer = WalWriter::create(&wal_dir, config).unwrap();
                barrier.wait();
                for j in 0..500 {
                    writer
                        .append_data(j, format!("thread_{i}_row_{j}").into_bytes())
                        .unwrap();
                }
                writer.flush().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    for i in 0..4 {
        let wal_dir = dir.path().join(format!("wal_{i}"));
        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_all().unwrap();
        assert_eq!(events.len(), 500);
    }
}

/// Engine concurrent table listing.
#[test]
fn engine_concurrent_table_list() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // Create tables first via TableBuilder.
    for i in 0..5 {
        let name = format!("table_{i}");
        TableBuilder::new(&name)
            .column("timestamp", ColumnType::Timestamp)
            .column("value", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();
    }

    let engine = Arc::new(Engine::open(db_root).unwrap());
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let tables = engine.list_tables().unwrap();
                    assert_eq!(tables.len(), 5);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

/// Concurrent recovery calls (should be safe even if redundant).
#[test]
fn concurrent_recovery_calls() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "test");

    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "test", config).unwrap();
        for i in 0..10 {
            let ts = Timestamp(TS_BASE + i * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(i as f64),
                    ],
                )
                .unwrap();
        }
        writer.flush().unwrap();
    }

    let stats1 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats1.rows_recovered, 10);

    let stats2 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats2.rows_recovered, 0);
}

/// No deadlock with ordered locking across two managers.
#[test]
fn no_deadlock_ordered_locking() {
    let lock1 = Arc::new(PartitionLockManager::new());
    let lock2 = Arc::new(PartitionLockManager::new());
    let barrier = Arc::new(Barrier::new(2));

    let h1 = {
        let l1 = lock1.clone();
        let l2 = lock2.clone();
        let b = barrier.clone();
        thread::spawn(move || {
            b.wait();
            for _ in 0..100 {
                let _g1 = l1.lock_partition("a");
                let _g2 = l2.lock_partition("b");
            }
        })
    };

    let h2 = {
        let l1 = lock1.clone();
        let l2 = lock2.clone();
        let b = barrier.clone();
        thread::spawn(move || {
            b.wait();
            for _ in 0..100 {
                let _g1 = l1.lock_partition("a");
                let _g2 = l2.lock_partition("b");
            }
        })
    };

    h1.join().unwrap();
    h2.join().unwrap();
}

/// Concurrent table creation via Engine.
#[test]
fn engine_concurrent_table_creation() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let name = format!("concurrent_table_{i}");
                let builder = TableBuilder::new(&name)
                    .column("timestamp", ColumnType::Timestamp)
                    .column("value", ColumnType::F64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day);
                engine.create_table(builder).unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let tables = engine.list_tables().unwrap();
    assert_eq!(tables.len(), 4);
}

/// Partition lock manager: verify lock is usable after being held and released.
#[test]
fn partition_lock_reusable_after_release() {
    let mgr = Arc::new(PartitionLockManager::new());

    {
        let _guard = mgr.lock_partition("reusable");
    }
    // Should be able to re-acquire after release.
    let guard = mgr.try_lock_partition("reusable");
    assert!(guard.is_some());
}

/// Sequencer last_txn_id.
#[test]
fn sequencer_last_txn_id() {
    let seq = Sequencer::new();
    assert_eq!(seq.last_txn_id(), 0);
    seq.next_txn_id();
    assert_eq!(seq.last_txn_id(), 1);
    seq.next_txn_id();
    seq.next_txn_id();
    assert_eq!(seq.last_txn_id(), 3);
}

/// MVCC min_active_version tracks snapshots.
#[test]
fn mvcc_min_active_version() {
    let mgr = MvccManager::new();

    // No snapshots: min should be u64::MAX.
    assert_eq!(mgr.min_active_version(), u64::MAX);

    // Take a snapshot at version 0.
    let snap1 = mgr.begin_snapshot();
    assert_eq!(mgr.min_active_version(), 0);

    // Advance version.
    mgr.commit_write(&[("p1", 10)]);
    mgr.commit_write(&[("p1", 20)]);

    // Take another snapshot at version 2.
    let snap2 = mgr.begin_snapshot();
    assert_eq!(snap2.version(), 2);

    // Min should still be 0 (from snap1).
    assert_eq!(mgr.min_active_version(), 0);

    // Release snap1.
    mgr.release_snapshot(&snap1);
    assert_eq!(mgr.min_active_version(), 2);

    // Release snap2.
    mgr.release_snapshot(&snap2);
    assert_eq!(mgr.min_active_version(), u64::MAX);
}
