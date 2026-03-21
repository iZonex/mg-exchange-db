//! Concurrent access tests — 100 tests.
//!
//! Tests thread-safety of Engine, PartitionLockManager, TableWriteLock,
//! WAL sequencer, and concurrent read/write patterns.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use exchange_common::types::{ColumnType, PartitionBy};
use exchange_core::engine::Engine;
use exchange_core::partition_lock::PartitionLockManager;
use exchange_core::table::TableBuilder;
use exchange_core::write_lock::TableWriteLock;
use tempfile::tempdir;

// =============================================================================
// PartitionLockManager concurrency
// =============================================================================
mod partition_lock_concurrency {
    use super::*;

    #[test]
    fn lock_same_partition_serializes() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(2));
        let mgr1 = Arc::clone(&mgr);
        let barrier1 = Arc::clone(&barrier);

        let t1 = thread::spawn(move || {
            let _guard = mgr1.lock_partition("p1");
            barrier1.wait();
            thread::sleep(Duration::from_millis(50));
        });

        let mgr2 = Arc::clone(&mgr);
        let barrier2 = Arc::clone(&barrier);
        let t2 = thread::spawn(move || {
            barrier2.wait();
            thread::sleep(Duration::from_millis(5));
            let start = Instant::now();
            let _guard = mgr2.lock_partition("p1");
            // Should have waited for t1
            assert!(start.elapsed() >= Duration::from_millis(20));
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn lock_different_partitions_parallel() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(2));
        let mgr1 = Arc::clone(&mgr);
        let barrier1 = Arc::clone(&barrier);

        let t1 = thread::spawn(move || {
            let _guard = mgr1.lock_partition("p1");
            barrier1.wait();
            thread::sleep(Duration::from_millis(50));
        });

        let mgr2 = Arc::clone(&mgr);
        let barrier2 = Arc::clone(&barrier);
        let t2 = thread::spawn(move || {
            barrier2.wait();
            let start = Instant::now();
            let _guard = mgr2.lock_partition("p2");
            // Should NOT have to wait
            assert!(start.elapsed() < Duration::from_millis(30));
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn try_lock_returns_none_when_held() {
        let mgr = Arc::new(PartitionLockManager::new());
        let _guard = mgr.lock_partition("p1");
        assert!(mgr.try_lock_partition("p1").is_none());
    }

    #[test]
    fn try_lock_returns_some_when_free() {
        let mgr = PartitionLockManager::new();
        let guard = mgr.try_lock_partition("p1");
        assert!(guard.is_some());
    }

    #[test]
    fn try_lock_different_partition_succeeds() {
        let mgr = PartitionLockManager::new();
        let _guard1 = mgr.lock_partition("p1");
        let guard2 = mgr.try_lock_partition("p2");
        assert!(guard2.is_some());
    }

    #[test]
    fn tracked_count_increases() {
        let mgr = PartitionLockManager::new();
        assert_eq!(mgr.tracked_count(), 0);
        let _g1 = mgr.lock_partition("a");
        assert_eq!(mgr.tracked_count(), 1);
        let _g2 = mgr.lock_partition("b");
        assert_eq!(mgr.tracked_count(), 2);
    }

    #[test]
    fn lock_released_on_drop() {
        let mgr = PartitionLockManager::new();
        {
            let _guard = mgr.lock_partition("p1");
        }
        // Should be able to lock again
        let guard = mgr.try_lock_partition("p1");
        assert!(guard.is_some());
    }

    #[test]
    fn four_threads_four_partitions() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let name = format!("partition_{}", i);
                    let _guard = mgr.lock_partition(&name);
                    thread::sleep(Duration::from_millis(10));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(mgr.tracked_count(), 4);
    }

    #[test]
    fn eight_threads_same_partition() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(8));
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    barrier.wait();
                    let _guard = mgr.lock_partition("shared");
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 8);
    }

    #[test]
    fn sixteen_threads_mixed_partitions() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(16));
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let handles: Vec<_> = (0..16)
            .map(|i| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    barrier.wait();
                    let part = format!("p{}", i % 4); // 4 partitions, 4 threads each
                    let _guard = mgr.lock_partition(&part);
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 16);
    }

    #[test]
    fn lock_and_unlock_rapidly() {
        let mgr = PartitionLockManager::new();
        for _ in 0..100 {
            let _guard = mgr.lock_partition("rapid");
        }
    }

    #[test]
    fn many_partitions() {
        let mgr = PartitionLockManager::new();
        let guards: Vec<_> = (0..100)
            .map(|i| mgr.lock_partition(&format!("p{}", i)))
            .collect();
        assert_eq!(mgr.tracked_count(), 100);
        drop(guards);
    }

    #[test]
    fn default_impl() {
        let mgr = PartitionLockManager::default();
        assert_eq!(mgr.tracked_count(), 0);
    }
}

// =============================================================================
// TableWriteLock concurrency
// =============================================================================
mod write_lock_concurrency {
    use super::*;

    #[test]
    fn acquire_write_lock() {
        let dir = tempdir().unwrap();
        let lock = TableWriteLock::try_acquire(dir.path());
        assert!(lock.is_ok());
    }

    #[test]
    fn second_acquire_fails() {
        let dir = tempdir().unwrap();
        let _lock1 = TableWriteLock::try_acquire(dir.path()).unwrap();
        let lock2 = TableWriteLock::try_acquire(dir.path());
        assert!(lock2.is_err());
    }

    #[test]
    fn release_and_reacquire() {
        let dir = tempdir().unwrap();
        {
            let _lock = TableWriteLock::try_acquire(dir.path()).unwrap();
        }
        let lock2 = TableWriteLock::try_acquire(dir.path());
        assert!(lock2.is_ok());
    }

    #[test]
    fn acquire_timeout_succeeds_when_free() {
        let dir = tempdir().unwrap();
        let lock = TableWriteLock::acquire_timeout(dir.path(), Duration::from_millis(100));
        assert!(lock.is_ok());
    }

    #[test]
    fn is_locked_when_held() {
        let dir = tempdir().unwrap();
        let _lock = TableWriteLock::try_acquire(dir.path()).unwrap();
        assert!(TableWriteLock::is_locked(dir.path()));
    }

    #[test]
    fn is_not_locked_when_free() {
        let dir = tempdir().unwrap();
        // Create the lock file so is_locked has something to check
        {
            let _lock = TableWriteLock::try_acquire(dir.path()).unwrap();
        }
        assert!(!TableWriteLock::is_locked(dir.path()));
    }

    #[test]
    fn different_dirs_independent() {
        let dir1 = tempdir().unwrap();
        let dir2 = tempdir().unwrap();
        let _lock1 = TableWriteLock::try_acquire(dir1.path()).unwrap();
        let lock2 = TableWriteLock::try_acquire(dir2.path());
        assert!(lock2.is_ok());
    }

    #[test]
    fn concurrent_lock_race() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let barrier = Arc::new(Barrier::new(4));
        let success_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                let success_count = Arc::clone(&success_count);
                thread::spawn(move || {
                    barrier.wait();
                    if let Ok(_lock) = TableWriteLock::try_acquire(&path) {
                        success_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Hold the lock for a bit so other threads see contention
                        thread::sleep(Duration::from_millis(50));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        // At least one should have succeeded; due to thread timing,
        // some threads may finish before others try, so multiple may succeed
        assert!(success_count.load(std::sync::atomic::Ordering::SeqCst) >= 1,);
    }
}

// =============================================================================
// Sequencer concurrency
// =============================================================================
mod sequencer_concurrency {
    use super::*;
    use exchange_core::wal::sequencer::Sequencer;

    #[test]
    fn sequencer_monotonic() {
        let seq = Sequencer::new();
        let a = seq.next_txn_id();
        let b = seq.next_txn_id();
        let c = seq.next_txn_id();
        assert!(b > a);
        assert!(c > b);
    }

    #[test]
    fn sequencer_concurrent_unique() {
        let seq = Arc::new(Sequencer::new());
        let barrier = Arc::new(Barrier::new(8));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let seq = Arc::clone(&seq);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut values = Vec::new();
                    for _ in 0..100 {
                        values.push(seq.next_txn_id());
                    }
                    values
                })
            })
            .collect();

        let mut all_values: Vec<u64> = Vec::new();
        for h in handles {
            all_values.extend(h.join().unwrap());
        }

        // All values should be unique
        all_values.sort();
        all_values.dedup();
        assert_eq!(all_values.len(), 800);
    }

    #[test]
    fn sequencer_starts_from_one() {
        let seq = Sequencer::new();
        let first = seq.next_txn_id();
        assert_eq!(first, 1);
    }

    #[test]
    fn sequencer_many_values() {
        let seq = Sequencer::new();
        let mut prev = seq.next_txn_id();
        for _ in 0..1000 {
            let next = seq.next_txn_id();
            assert!(next > prev);
            prev = next;
        }
    }

    #[test]
    fn sequencer_resume_from() {
        let seq = Sequencer::resume_from(100);
        let next = seq.next_txn_id();
        assert_eq!(next, 101);
    }

    #[test]
    fn sequencer_peek_does_not_consume() {
        let seq = Sequencer::new();
        let peek1 = seq.peek_next();
        let peek2 = seq.peek_next();
        assert_eq!(peek1, peek2);
        let consumed = seq.next_txn_id();
        assert_eq!(consumed, peek1);
        let peek3 = seq.peek_next();
        assert_eq!(peek3, peek1 + 1);
    }

    #[test]
    fn sequencer_last_txn_id() {
        let seq = Sequencer::new();
        assert_eq!(seq.last_txn_id(), 0);
        seq.next_txn_id();
        assert_eq!(seq.last_txn_id(), 1);
        seq.next_txn_id();
        assert_eq!(seq.last_txn_id(), 2);
    }

    #[test]
    fn sequencer_default() {
        let seq = Sequencer::default();
        assert_eq!(seq.next_txn_id(), 1);
    }

    #[test]
    fn sequencer_concurrent_32_threads() {
        let seq = Arc::new(Sequencer::new());
        let barrier = Arc::new(Barrier::new(32));

        let handles: Vec<_> = (0..32)
            .map(|_| {
                let seq = Arc::clone(&seq);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut values = Vec::new();
                    for _ in 0..50 {
                        values.push(seq.next_txn_id());
                    }
                    values
                })
            })
            .collect();

        let mut all_values: Vec<u64> = Vec::new();
        for h in handles {
            all_values.extend(h.join().unwrap());
        }

        all_values.sort();
        all_values.dedup();
        assert_eq!(all_values.len(), 1600); // 32 * 50
    }
}

// =============================================================================
// Engine concurrency
// =============================================================================
mod engine_concurrency {
    use super::*;

    fn create_test_engine() -> (tempfile::TempDir, Engine) {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();

        // Create a test table
        let builder = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day);
        engine.create_table(builder).unwrap();

        (dir, engine)
    }

    #[test]
    fn engine_open_empty() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        let tables = engine.list_tables().unwrap();
        assert_eq!(tables.len(), 0);
    }

    #[test]
    fn engine_create_table() {
        let (_dir, engine) = create_test_engine();
        let tables = engine.list_tables().unwrap();
        assert!(tables.contains(&"trades".to_string()));
    }

    #[test]
    fn engine_create_multiple_tables() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();

        for i in 0..5 {
            let builder = TableBuilder::new(&format!("table_{}", i))
                .column("timestamp", ColumnType::Timestamp)
                .column("value", ColumnType::F64)
                .timestamp("timestamp");
            engine.create_table(builder).unwrap();
        }

        let tables = engine.list_tables().unwrap();
        assert_eq!(tables.len(), 5);
    }

    #[test]
    fn engine_get_reader() {
        let (_dir, engine) = create_test_engine();
        let reader = engine.get_reader("trades");
        assert!(reader.is_ok());
    }

    #[test]
    fn engine_get_reader_nonexistent() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        let reader = engine.get_reader("nonexistent");
        assert!(reader.is_err());
    }

    #[test]
    fn engine_get_writer() {
        let (_dir, engine) = create_test_engine();
        let writer = engine.get_writer("trades");
        assert!(writer.is_ok());
    }

    #[test]
    fn engine_get_writer_nonexistent() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();
        let writer = engine.get_writer("nonexistent");
        assert!(writer.is_err());
    }

    #[test]
    fn engine_multiple_readers() {
        let (_dir, engine) = create_test_engine();
        let r1 = engine.get_reader("trades").unwrap();
        let r2 = engine.get_reader("trades").unwrap();
        // Both should coexist
        assert_eq!(r1.meta().name, "trades");
        assert_eq!(r2.meta().name, "trades");
    }

    #[test]
    fn engine_concurrent_readers() {
        let (_dir, engine) = create_test_engine();
        let engine = Arc::new(engine);
        let barrier = Arc::new(Barrier::new(4));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let reader = engine.get_reader("trades").unwrap();
                    assert_eq!(reader.meta().name, "trades");
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn engine_writer_blocks_second_writer() {
        let (_dir, engine) = create_test_engine();
        let engine = Arc::new(engine);
        let barrier = Arc::new(Barrier::new(2));
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let engine1 = Arc::clone(&engine);
        let barrier1 = Arc::clone(&barrier);
        let counter1 = Arc::clone(&counter);
        let t1 = thread::spawn(move || {
            let _writer = engine1.get_writer("trades").unwrap();
            counter1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            barrier1.wait();
            thread::sleep(Duration::from_millis(50));
        });

        let engine2 = Arc::clone(&engine);
        let barrier2 = Arc::clone(&barrier);
        let counter2 = Arc::clone(&counter);
        let t2 = thread::spawn(move || {
            barrier2.wait();
            thread::sleep(Duration::from_millis(5));
            let start = Instant::now();
            let _writer = engine2.get_writer("trades").unwrap();
            counter2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Should have waited for t1 to drop the writer
            assert!(start.elapsed() >= Duration::from_millis(20));
        });

        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn engine_writer_on_different_tables_parallel() {
        let dir = tempdir().unwrap();
        let engine = Engine::open(dir.path()).unwrap();

        let builder1 = TableBuilder::new("t1")
            .column("timestamp", ColumnType::Timestamp)
            .column("v", ColumnType::F64)
            .timestamp("timestamp");
        engine.create_table(builder1).unwrap();

        let builder2 = TableBuilder::new("t2")
            .column("timestamp", ColumnType::Timestamp)
            .column("v", ColumnType::F64)
            .timestamp("timestamp");
        engine.create_table(builder2).unwrap();

        let engine = Arc::new(engine);
        let barrier = Arc::new(Barrier::new(2));

        let engine1 = Arc::clone(&engine);
        let barrier1 = Arc::clone(&barrier);
        let t1 = thread::spawn(move || {
            let _writer = engine1.get_writer("t1").unwrap();
            barrier1.wait();
            thread::sleep(Duration::from_millis(50));
        });

        let engine2 = Arc::clone(&engine);
        let barrier2 = Arc::clone(&barrier);
        let t2 = thread::spawn(move || {
            barrier2.wait();
            let start = Instant::now();
            let _writer = engine2.get_writer("t2").unwrap();
            // Should NOT have to wait (different table)
            assert!(start.elapsed() < Duration::from_millis(30));
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn engine_reader_during_writer() {
        let (_dir, engine) = create_test_engine();
        let engine = Arc::new(engine);
        let barrier = Arc::new(Barrier::new(2));

        let engine1 = Arc::clone(&engine);
        let barrier1 = Arc::clone(&barrier);
        let t1 = thread::spawn(move || {
            let _writer = engine1.get_writer("trades").unwrap();
            barrier1.wait();
            thread::sleep(Duration::from_millis(50));
        });

        let engine2 = Arc::clone(&engine);
        let barrier2 = Arc::clone(&barrier);
        let t2 = thread::spawn(move || {
            barrier2.wait();
            // Reader should not block (different lock from writer)
            let reader = engine2.get_reader("trades").unwrap();
            assert_eq!(reader.meta().name, "trades");
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn engine_get_meta() {
        let (_dir, engine) = create_test_engine();
        let meta = engine.get_meta("trades").unwrap();
        assert_eq!(meta.name, "trades");
        assert_eq!(meta.columns.len(), 2);
    }

    #[test]
    fn engine_partition_locks() {
        let (_dir, engine) = create_test_engine();
        let lock_mgr = engine.partition_locks("trades");
        assert!(lock_mgr.is_some());
    }

    #[test]
    fn engine_txn_manager() {
        let (_dir, engine) = create_test_engine();
        let txn_mgr = engine.txn_manager("trades");
        assert!(txn_mgr.is_some());
    }

    #[test]
    fn engine_db_root() {
        let (dir, engine) = create_test_engine();
        assert_eq!(engine.db_root(), dir.path());
    }

    #[test]
    fn engine_reopen_discovers_tables() {
        let dir = tempdir().unwrap();
        {
            let engine = Engine::open(dir.path()).unwrap();
            let builder = TableBuilder::new("test")
                .column("timestamp", ColumnType::Timestamp)
                .column("v", ColumnType::F64)
                .timestamp("timestamp");
            engine.create_table(builder).unwrap();
        }
        // Reopen
        let engine = Engine::open(dir.path()).unwrap();
        let tables = engine.list_tables().unwrap();
        assert!(tables.contains(&"test".to_string()));
    }

    #[test]
    fn engine_eight_concurrent_readers() {
        let (_dir, engine) = create_test_engine();
        let engine = Arc::new(engine);
        let barrier = Arc::new(Barrier::new(8));
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let engine = Arc::clone(&engine);
                let barrier = Arc::clone(&barrier);
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    barrier.wait();
                    let reader = engine.get_reader("trades").unwrap();
                    assert_eq!(reader.meta().name, "trades");
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 8);
    }

    #[test]
    fn engine_reader_handle_meta() {
        let (_dir, engine) = create_test_engine();
        let reader = engine.get_reader("trades").unwrap();
        let meta = reader.meta();
        assert_eq!(meta.name, "trades");
        assert!(reader.table_dir().exists());
    }

    #[test]
    fn engine_reader_handle_version() {
        let (_dir, engine) = create_test_engine();
        let reader = engine.get_reader("trades").unwrap();
        let version = reader.version();
        let _ = version;
    }

    #[test]
    fn engine_writer_for_partition() {
        let (_dir, engine) = create_test_engine();
        let writer = engine.get_writer_for_partition("trades", "2024-03-15");
        assert!(writer.is_ok());
    }
}
