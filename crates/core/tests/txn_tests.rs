//! Comprehensive tests for transaction management: TxnFile, Scoreboard, TxnManager.
//!
//! 50 tests covering persistent transaction state, lock-free reader tracking,
//! and coordinated read/write transactions.

use exchange_core::txn::{PartitionEntry, Scoreboard, TxnFile, TxnHeader, TxnManager};
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

// ============================================================================
// TxnFile
// ============================================================================

mod txn_file {
    use super::*;

    #[test]
    fn create_and_read_empty() {
        let dir = tempdir().unwrap();
        let txn = TxnFile::open(dir.path()).unwrap();
        let hdr = txn.read_header();
        assert_eq!(hdr.version, 0);
        assert_eq!(hdr.row_count, 0);
        assert_eq!(hdr.partition_count, 0);
    }

    #[test]
    fn write_and_read_header() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let hdr = TxnHeader {
            version: 42,
            row_count: 1000,
            min_timestamp: 100,
            max_timestamp: 999,
            partition_count: 0,
        };
        txn.write_header(&hdr).unwrap();
        assert_eq!(txn.read_header(), hdr);
    }

    #[test]
    fn commit_with_partitions() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let parts = vec![
            PartitionEntry {
                timestamp: 1000,
                row_count: 500,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 2000,
                row_count: 300,
                name_offset: 20,
            },
        ];
        let hdr = TxnHeader {
            version: 1,
            row_count: 800,
            min_timestamp: 1000,
            max_timestamp: 2000,
            partition_count: 2,
        };
        txn.commit(&hdr, &parts).unwrap();
        let read_hdr = txn.read_header();
        assert_eq!(read_hdr, hdr);
        let read_parts = txn.read_partitions();
        assert_eq!(read_parts, parts);
    }

    #[test]
    fn reopen_persists_data() {
        let dir = tempdir().unwrap();
        let hdr = TxnHeader {
            version: 7,
            row_count: 42,
            min_timestamp: -100,
            max_timestamp: 200,
            partition_count: 1,
        };
        let parts = vec![PartitionEntry {
            timestamp: -100,
            row_count: 42,
            name_offset: 0,
        }];
        {
            let mut txn = TxnFile::open(dir.path()).unwrap();
            txn.commit(&hdr, &parts).unwrap();
        }
        let txn = TxnFile::open(dir.path()).unwrap();
        assert_eq!(txn.read_header().version, 7);
        assert_eq!(txn.read_header().row_count, 42);
        assert_eq!(txn.read_partitions().len(), 1);
    }

    #[test]
    fn overwrite_reduces_partitions() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        // Write 3 partitions
        let parts3 = vec![
            PartitionEntry {
                timestamp: 1,
                row_count: 1,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 2,
                row_count: 2,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 3,
                row_count: 3,
                name_offset: 0,
            },
        ];
        let hdr3 = TxnHeader {
            version: 1,
            row_count: 6,
            min_timestamp: 1,
            max_timestamp: 3,
            partition_count: 3,
        };
        txn.commit(&hdr3, &parts3).unwrap();
        assert_eq!(txn.read_partitions().len(), 3);

        // Overwrite with 1 partition
        let parts1 = vec![PartitionEntry {
            timestamp: 10,
            row_count: 10,
            name_offset: 0,
        }];
        let hdr1 = TxnHeader {
            version: 2,
            row_count: 10,
            min_timestamp: 10,
            max_timestamp: 10,
            partition_count: 1,
        };
        txn.commit(&hdr1, &parts1).unwrap();
        let read = txn.read_partitions();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].timestamp, 10);
    }

    #[test]
    fn read_single_partition() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let parts = vec![
            PartitionEntry {
                timestamp: 100,
                row_count: 10,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 200,
                row_count: 20,
                name_offset: 0,
            },
        ];
        let hdr = TxnHeader {
            version: 1,
            row_count: 30,
            min_timestamp: 100,
            max_timestamp: 200,
            partition_count: 2,
        };
        txn.commit(&hdr, &parts).unwrap();
        let p0 = txn.read_partition(0);
        assert_eq!(p0.timestamp, 100);
        assert_eq!(p0.row_count, 10);
        let p1 = txn.read_partition(1);
        assert_eq!(p1.timestamp, 200);
    }

    #[test]
    fn write_partition_individually() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let parts = vec![
            PartitionEntry {
                timestamp: 1,
                row_count: 1,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 2,
                row_count: 2,
                name_offset: 0,
            },
        ];
        let hdr = TxnHeader {
            version: 1,
            row_count: 3,
            min_timestamp: 1,
            max_timestamp: 2,
            partition_count: 2,
        };
        txn.commit(&hdr, &parts).unwrap();

        // Overwrite partition 1
        let new_entry = PartitionEntry {
            timestamp: 2,
            row_count: 99,
            name_offset: 0,
        };
        txn.write_partition(1, &new_entry).unwrap();
        let read = txn.read_partition(1);
        assert_eq!(read.row_count, 99);
    }

    #[test]
    fn empty_commit() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let hdr = TxnHeader {
            version: 1,
            row_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            partition_count: 0,
        };
        txn.commit(&hdr, &[]).unwrap();
        assert_eq!(txn.read_header(), hdr);
        assert!(txn.read_partitions().is_empty());
    }

    #[test]
    fn negative_timestamps() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let hdr = TxnHeader {
            version: 1,
            row_count: 1,
            min_timestamp: i64::MIN,
            max_timestamp: -1,
            partition_count: 0,
        };
        txn.write_header(&hdr).unwrap();
        let read = txn.read_header();
        assert_eq!(read.min_timestamp, i64::MIN);
        assert_eq!(read.max_timestamp, -1);
    }

    #[test]
    fn many_partitions() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let parts: Vec<PartitionEntry> = (0..100)
            .map(|i| PartitionEntry {
                timestamp: i * 1000,
                row_count: (i + 1) as u64,
                name_offset: 0,
            })
            .collect();
        let hdr = TxnHeader {
            version: 1,
            row_count: parts.iter().map(|p| p.row_count).sum(),
            min_timestamp: 0,
            max_timestamp: 99_000,
            partition_count: 100,
        };
        txn.commit(&hdr, &parts).unwrap();
        let read = txn.read_partitions();
        assert_eq!(read.len(), 100);
        assert_eq!(read[50].timestamp, 50_000);
    }
}

// ============================================================================
// Scoreboard
// ============================================================================

mod scoreboard {
    use super::*;

    #[test]
    fn acquire_and_release() {
        let sb = Scoreboard::new();
        assert_eq!(sb.active_count(), 0);
        let r = sb.acquire(10).unwrap();
        assert_eq!(sb.active_count(), 1);
        assert_eq!(sb.min_active_version(), 10);
        sb.release(r);
        assert_eq!(sb.active_count(), 0);
    }

    #[test]
    fn multiple_readers() {
        let sb = Scoreboard::new();
        let r1 = sb.acquire(10).unwrap();
        let r2 = sb.acquire(20).unwrap();
        let r3 = sb.acquire(15).unwrap();
        assert_eq!(sb.active_count(), 3);
        assert_eq!(sb.min_active_version(), 10);

        sb.release(r1);
        assert_eq!(sb.min_active_version(), 15);

        sb.release(r3);
        assert_eq!(sb.min_active_version(), 20);

        sb.release(r2);
        assert_eq!(sb.min_active_version(), u64::MAX);
    }

    #[test]
    fn slot_reuse() {
        let sb = Scoreboard::new();
        let r1 = sb.acquire(5).unwrap();
        let slot = r1.slot();
        sb.release(r1);
        let r2 = sb.acquire(6).unwrap();
        assert_eq!(r2.slot(), slot);
        sb.release(r2);
    }

    #[test]
    fn full_scoreboard() {
        let sb = Scoreboard::new();
        let cap = sb.capacity();
        let mut readers = Vec::new();
        for i in 0..cap {
            readers.push(sb.acquire(i as u64).unwrap());
        }
        assert_eq!(sb.active_count(), cap);
        assert!(sb.acquire(999).is_err());

        sb.release(readers.pop().unwrap());
        assert!(sb.acquire(999).is_ok());
    }

    #[test]
    fn min_active_no_readers() {
        let sb = Scoreboard::new();
        assert_eq!(sb.min_active_version(), u64::MAX);
    }

    #[test]
    fn capacity_is_256() {
        let sb = Scoreboard::new();
        assert_eq!(sb.capacity(), 256);
    }

    #[test]
    fn concurrent_acquire_release() {
        let sb = Arc::new(Scoreboard::new());
        let barrier = Arc::new(Barrier::new(16));
        let handles: Vec<_> = (0..16)
            .map(|t| {
                let sb = Arc::clone(&sb);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..500 {
                        let version = (t * 500 + i) as u64;
                        let rid = sb.acquire(version).unwrap();
                        std::hint::black_box(sb.min_active_version());
                        sb.release(rid);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(sb.active_count(), 0);
    }

    #[test]
    fn min_version_always_le_active() {
        let sb = Arc::new(Scoreboard::new());
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let sb = Arc::clone(&sb);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..500 {
                        let version = 100 + t as u64;
                        let rid = sb.acquire(version).unwrap();
                        let min = sb.min_active_version();
                        assert!(min <= version);
                        sb.release(rid);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn reader_id_slot_range() {
        let sb = Scoreboard::new();
        let r = sb.acquire(1).unwrap();
        assert!(r.slot() < sb.capacity() as u32);
        sb.release(r);
    }

    #[test]
    fn default_impl() {
        let sb = Scoreboard::default();
        assert_eq!(sb.active_count(), 0);
        assert_eq!(sb.capacity(), 256);
    }

    #[test]
    fn release_then_acquire_same_version() {
        let sb = Scoreboard::new();
        let r1 = sb.acquire(1).unwrap();
        sb.release(r1);
        let r2 = sb.acquire(1).unwrap();
        assert_eq!(sb.min_active_version(), 1);
        sb.release(r2);
    }

    #[test]
    fn acquire_descending_versions() {
        let sb = Scoreboard::new();
        let r1 = sb.acquire(100).unwrap();
        let r2 = sb.acquire(50).unwrap();
        let r3 = sb.acquire(10).unwrap();
        assert_eq!(sb.min_active_version(), 10);
        sb.release(r3);
        assert_eq!(sb.min_active_version(), 50);
        sb.release(r2);
        assert_eq!(sb.min_active_version(), 100);
        sb.release(r1);
    }

    #[test]
    fn all_slots_same_version() {
        let sb = Scoreboard::new();
        let mut readers = Vec::new();
        for _ in 0..10 {
            readers.push(sb.acquire(42).unwrap());
        }
        assert_eq!(sb.min_active_version(), 42);
        for r in readers {
            sb.release(r);
        }
        assert_eq!(sb.min_active_version(), u64::MAX);
    }
}

// ============================================================================
// TxnManager
// ============================================================================

mod txn_manager {
    use super::*;

    #[test]
    fn basic_flow() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        assert_eq!(mgr.current_version(), 0);

        let read_txn = mgr.begin_read().unwrap();
        assert_eq!(read_txn.version(), 0);
        assert_eq!(mgr.scoreboard().active_count(), 1);

        let parts = vec![PartitionEntry {
            timestamp: 1000,
            row_count: 100,
            name_offset: 0,
        }];
        let v = mgr.commit_write(100, 1000, 2000, &parts).unwrap();
        assert_eq!(v, 1);
        assert_eq!(mgr.current_version(), 1);

        drop(read_txn);
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn commit_simple() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        mgr.commit_write_simple(50, 100, 200).unwrap();
        let hdr = mgr.read_header();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.row_count, 50);
        assert_eq!(hdr.min_timestamp, 100);
        assert_eq!(hdr.max_timestamp, 200);
    }

    #[test]
    fn commit_simple_merges_timestamps() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        mgr.commit_write_simple(50, 100, 200).unwrap();
        mgr.commit_write_simple(150, 50, 300).unwrap();
        let hdr = mgr.read_header();
        assert_eq!(hdr.min_timestamp, 50);
        assert_eq!(hdr.max_timestamp, 300);
    }

    #[test]
    fn read_txn_auto_release_on_drop() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        {
            let _r1 = mgr.begin_read().unwrap();
            let _r2 = mgr.begin_read().unwrap();
            assert_eq!(mgr.scoreboard().active_count(), 2);
        }
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn new_read_sees_latest_version() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        mgr.commit_write_simple(10, 1, 10).unwrap();
        let read = mgr.begin_read().unwrap();
        assert_eq!(read.version(), 1);
        assert_eq!(read.header().row_count, 10);
        drop(read);
    }

    #[test]
    fn old_read_pinned_at_version() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let old_read = mgr.begin_read().unwrap();
        assert_eq!(old_read.version(), 0);

        mgr.commit_write_simple(100, 1, 100).unwrap();
        assert_eq!(old_read.version(), 0); // still pinned

        assert_eq!(mgr.scoreboard().min_active_version(), 0);
        drop(old_read);
        assert_eq!(mgr.scoreboard().min_active_version(), u64::MAX);
    }

    #[test]
    fn concurrent_reads_and_writes() {
        let dir = tempdir().unwrap();
        let mgr = Arc::new(TxnManager::open(dir.path()).unwrap());
        let barrier = Arc::new(Barrier::new(4));

        // Writer
        let mgr_w = Arc::clone(&mgr);
        let barrier_w = Arc::clone(&barrier);
        let writer = std::thread::spawn(move || {
            barrier_w.wait();
            for i in 1..=50u64 {
                mgr_w
                    .commit_write_simple(i * 10, i as i64, (i * 100) as i64)
                    .unwrap();
            }
        });

        // Readers
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let mgr_r = Arc::clone(&mgr);
                let barrier_r = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier_r.wait();
                    for _ in 0..100 {
                        let rtx = mgr_r.begin_read().unwrap();
                        let v = rtx.version();
                        assert_eq!(rtx.header().version, v);
                        drop(rtx);
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(mgr.current_version(), 50);
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let mgr = TxnManager::open(dir.path()).unwrap();
            let parts = vec![
                PartitionEntry {
                    timestamp: 10,
                    row_count: 5,
                    name_offset: 0,
                },
                PartitionEntry {
                    timestamp: 20,
                    row_count: 7,
                    name_offset: 0,
                },
            ];
            mgr.commit_write(12, 10, 20, &parts).unwrap();
        }
        let mgr = TxnManager::open(dir.path()).unwrap();
        assert_eq!(mgr.current_version(), 1);
        let hdr = mgr.read_header();
        assert_eq!(hdr.row_count, 12);
        assert_eq!(hdr.partition_count, 2);
    }

    #[test]
    fn multiple_commits_increment_version() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        for i in 1..=10 {
            let v = mgr.commit_write_simple(i * 10, 0, i as i64 * 100).unwrap();
            assert_eq!(v, i);
        }
        assert_eq!(mgr.current_version(), 10);
    }

    #[test]
    fn reader_id_accessible() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let read = mgr.begin_read().unwrap();
        let _id = read.reader_id();
        drop(read);
    }

    #[test]
    fn scoreboard_ref() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let sb = mgr.scoreboard();
        assert_eq!(sb.active_count(), 0);
    }

    #[test]
    fn commit_write_returns_version() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let v1 = mgr.commit_write(10, 0, 100, &[]).unwrap();
        assert_eq!(v1, 1);
        let v2 = mgr.commit_write(20, 0, 200, &[]).unwrap();
        assert_eq!(v2, 2);
    }

    #[test]
    fn read_header_reflects_commit() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let hdr0 = mgr.read_header();
        assert_eq!(hdr0.version, 0);
        mgr.commit_write_simple(100, 1, 100).unwrap();
        let hdr1 = mgr.read_header();
        assert_eq!(hdr1.version, 1);
        assert_eq!(hdr1.row_count, 100);
    }

    #[test]
    fn many_readers_simultaneous() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        mgr.commit_write_simple(10, 0, 10).unwrap();
        let mut reads = Vec::new();
        for _ in 0..50 {
            reads.push(mgr.begin_read().unwrap());
        }
        assert_eq!(mgr.scoreboard().active_count(), 50);
        assert_eq!(mgr.scoreboard().min_active_version(), 1);
        drop(reads);
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn version_zero_header_correct() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let hdr = mgr.read_header();
        assert_eq!(hdr.version, 0);
        assert_eq!(hdr.row_count, 0);
        assert_eq!(hdr.partition_count, 0);
    }

    #[test]
    fn commit_with_multiple_partitions() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let parts = vec![
            PartitionEntry {
                timestamp: 100,
                row_count: 10,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 200,
                row_count: 20,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 300,
                row_count: 30,
                name_offset: 0,
            },
        ];
        mgr.commit_write(60, 100, 300, &parts).unwrap();
        let hdr = mgr.read_header();
        assert_eq!(hdr.partition_count, 3);
    }

    #[test]
    fn begin_read_at_version_zero() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        let read = mgr.begin_read().unwrap();
        assert_eq!(read.version(), 0);
        assert_eq!(read.header().row_count, 0);
    }

    #[test]
    fn read_txn_header_snapshot() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();
        mgr.commit_write_simple(100, 1, 100).unwrap();
        let read = mgr.begin_read().unwrap();
        // Commit again while read is active
        mgr.commit_write_simple(200, 1, 200).unwrap();
        // Read txn should still see the old header
        assert_eq!(read.header().row_count, 100);
        assert_eq!(read.version(), 1);
        drop(read);
    }
}

// ============================================================================
// Additional TxnFile tests
// ============================================================================

mod txn_file_extra {
    use super::*;

    #[test]
    fn write_header_large_values() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let hdr = TxnHeader {
            version: u64::MAX - 1,
            row_count: u64::MAX / 2,
            min_timestamp: i64::MIN,
            max_timestamp: i64::MAX,
            partition_count: 0,
        };
        txn.write_header(&hdr).unwrap();
        assert_eq!(txn.read_header(), hdr);
    }

    #[test]
    fn multiple_commits_sequential() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        for v in 1..=20u64 {
            let hdr = TxnHeader {
                version: v,
                row_count: v * 100,
                min_timestamp: 0,
                max_timestamp: v as i64 * 1000,
                partition_count: 0,
            };
            txn.write_header(&hdr).unwrap();
        }
        let final_hdr = txn.read_header();
        assert_eq!(final_hdr.version, 20);
        assert_eq!(final_hdr.row_count, 2000);
    }

    #[test]
    fn partition_entries_correct_after_commit() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();
        let parts: Vec<PartitionEntry> = (0..5)
            .map(|i| PartitionEntry {
                timestamp: i * 86400,
                row_count: (i + 1) as u64 * 100,
                name_offset: i as u32 * 10,
            })
            .collect();
        let hdr = TxnHeader {
            version: 1,
            row_count: 1500,
            min_timestamp: 0,
            max_timestamp: 4 * 86400,
            partition_count: 5,
        };
        txn.commit(&hdr, &parts).unwrap();
        for i in 0..5 {
            let p = txn.read_partition(i as u32);
            assert_eq!(p.timestamp, i * 86400);
            assert_eq!(p.row_count, (i + 1) as u64 * 100);
            assert_eq!(p.name_offset, i as u32 * 10);
        }
    }

    #[test]
    fn reopen_after_multiple_commits() {
        let dir = tempdir().unwrap();
        {
            let mut txn = TxnFile::open(dir.path()).unwrap();
            for v in 1..=5u64 {
                let hdr = TxnHeader {
                    version: v,
                    row_count: v * 10,
                    min_timestamp: 0,
                    max_timestamp: v as i64,
                    partition_count: 0,
                };
                txn.write_header(&hdr).unwrap();
            }
        }
        let txn = TxnFile::open(dir.path()).unwrap();
        let hdr = txn.read_header();
        assert_eq!(hdr.version, 5);
        assert_eq!(hdr.row_count, 50);
    }
}
