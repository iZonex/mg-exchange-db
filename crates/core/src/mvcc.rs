//! MVCC (Multi-Version Concurrency Control) snapshot isolation for ExchangeDB.
//!
//! Provides true snapshot isolation: readers see a consistent view of data as of
//! the moment they began their transaction, regardless of concurrent writes.
//!
//! Each write operation atomically bumps a global version counter. Read snapshots
//! capture the current version and the row counts per partition at that instant.
//! Queries using a snapshot only see rows that were committed before the snapshot
//! was taken.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dashmap::DashMap;

// ---------------------------------------------------------------------------
// PartitionVersion -- tracks committed row counts per version
// ---------------------------------------------------------------------------

/// A versioned row count for a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionVersion {
    /// The write version that produced this row count.
    pub version: u64,
    /// Total committed row count in this partition at this version.
    pub row_count: u64,
}

/// Version history for a single partition: a list of (version, row_count) pairs,
/// kept sorted by version ascending.
#[derive(Debug, Clone)]
struct PartitionHistory {
    /// Sorted by version ascending. Each entry records the row_count at that version.
    versions: Vec<PartitionVersion>,
}

impl PartitionHistory {
    fn new() -> Self {
        Self {
            versions: Vec::new(),
        }
    }

    /// Record a new version's row count.
    fn push(&mut self, version: u64, row_count: u64) {
        self.versions.push(PartitionVersion { version, row_count });
    }

    /// Get the row count visible at the given snapshot version.
    /// Returns the row_count from the latest version <= snapshot_version.
    fn row_count_at(&self, snapshot_version: u64) -> u64 {
        // Binary search for the latest version <= snapshot_version
        let mut result = 0u64;
        for pv in &self.versions {
            if pv.version <= snapshot_version {
                result = pv.row_count;
            } else {
                break;
            }
        }
        result
    }

    /// Garbage-collect versions older than min_version, keeping at most one
    /// version at or below min_version (the latest one, which is still needed
    /// by any snapshot at min_version).
    fn gc(&mut self, min_version: u64) {
        // Find the last index with version <= min_version
        let mut keep_from = 0;
        for (i, pv) in self.versions.iter().enumerate() {
            if pv.version <= min_version {
                keep_from = i;
            } else {
                break;
            }
        }
        // Remove everything before keep_from (keep the entry at keep_from)
        if keep_from > 0 {
            self.versions.drain(0..keep_from);
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// A read snapshot capturing a consistent point-in-time view of the database.
///
/// The snapshot records which version it was taken at and the visible row counts
/// for each partition. Any rows written after this snapshot was taken are invisible.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The version at which this snapshot was taken.
    version: u64,
    /// Snapshot ID for tracking in the active snapshots map.
    snapshot_id: u64,
}

impl Snapshot {
    /// The version this snapshot is pinned to.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The unique snapshot ID.
    pub fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }
}

/// Detailed information about an active snapshot (stored in the manager).
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// The version at which this snapshot was taken.
    pub version: u64,
    /// Timestamp (epoch nanos) when the snapshot was created.
    pub created_at: u64,
}

// ---------------------------------------------------------------------------
// MvccManager
// ---------------------------------------------------------------------------

/// Manages MVCC snapshot isolation for a single table.
///
/// Coordinates read snapshots and write commits. Writers call `commit_write` to
/// atomically bump the version and record new partition row counts. Readers call
/// `begin_snapshot` to get a consistent view, and `visible_row_count` to determine
/// how many rows in a partition are visible to their snapshot.
pub struct MvccManager {
    /// Current write version (monotonically increasing).
    current_version: AtomicU64,

    /// Next snapshot ID (monotonically increasing).
    next_snapshot_id: AtomicU64,

    /// Active read snapshots. Key is snapshot_id.
    active_snapshots: DashMap<u64, SnapshotInfo>,

    /// Per-partition version history. Key is partition name.
    partition_history: Mutex<DashMap<String, PartitionHistory>>,
}

impl MvccManager {
    /// Create a new MVCC manager starting at version 0.
    pub fn new() -> Self {
        Self {
            current_version: AtomicU64::new(0),
            next_snapshot_id: AtomicU64::new(1),
            active_snapshots: DashMap::new(),
            partition_history: Mutex::new(DashMap::new()),
        }
    }

    /// Create a new MVCC manager starting at a specific version.
    pub fn with_version(version: u64) -> Self {
        Self {
            current_version: AtomicU64::new(version),
            next_snapshot_id: AtomicU64::new(1),
            active_snapshots: DashMap::new(),
            partition_history: Mutex::new(DashMap::new()),
        }
    }

    /// The current committed version.
    pub fn current_version(&self) -> u64 {
        self.current_version.load(Ordering::Acquire)
    }

    /// Begin a read snapshot at the current committed version.
    ///
    /// The snapshot captures a consistent view: it will only see rows that were
    /// committed at or before the current version at the time of this call.
    pub fn begin_snapshot(&self) -> Snapshot {
        // Acquire the partition_history lock to ensure we read a version that
        // has its corresponding partition history fully committed. Without this,
        // we could read a version that a concurrent writer has bumped but not
        // yet recorded partition data for.
        let _history = self.partition_history.lock().unwrap();
        let version = self.current_version.load(Ordering::Acquire);
        drop(_history);

        let snapshot_id = self.next_snapshot_id.fetch_add(1, Ordering::Relaxed);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.active_snapshots.insert(
            snapshot_id,
            SnapshotInfo {
                version,
                created_at: now,
            },
        );

        Snapshot {
            version,
            snapshot_id,
        }
    }

    /// Release a snapshot, allowing its pinned version to be garbage-collected.
    pub fn release_snapshot(&self, snapshot: &Snapshot) {
        self.active_snapshots.remove(&snapshot.snapshot_id);
    }

    /// Commit a write, bumping the version and recording new partition row counts.
    ///
    /// `partitions` is a list of (partition_name, new_total_row_count) pairs.
    /// Returns the new version number.
    pub fn commit_write(&self, partitions: &[(&str, u64)]) -> u64 {
        // Acquire the history lock BEFORE bumping the version to ensure that
        // any snapshot taken at the new version will see the partition updates.
        // Without this, a reader could see the bumped version but stale history.
        let history = self.partition_history.lock().unwrap();

        let new_version = self.current_version.fetch_add(1, Ordering::AcqRel) + 1;

        for &(partition_name, row_count) in partitions {
            let mut entry = history
                .entry(partition_name.to_string())
                .or_insert_with(PartitionHistory::new);
            entry.push(new_version, row_count);
        }

        drop(history);
        new_version
    }

    /// Check the number of rows visible to a snapshot in a specific partition.
    ///
    /// Returns the total row count that was committed at or before the snapshot's version.
    pub fn visible_row_count(&self, snapshot: &Snapshot, partition: &str) -> u64 {
        let history = self.partition_history.lock().unwrap();
        match history.get(partition) {
            Some(ph) => ph.row_count_at(snapshot.version),
            None => 0,
        }
    }

    /// Check if a specific row (identified by row_id, 0-based) is visible to a snapshot.
    ///
    /// A row is visible if its row_id is less than the committed row count at the
    /// snapshot's version.
    pub fn is_visible(&self, snapshot: &Snapshot, partition: &str, row_id: u64) -> bool {
        let visible_count = self.visible_row_count(snapshot, partition);
        row_id < visible_count
    }

    /// Get the minimum version across all active snapshots.
    ///
    /// Returns `u64::MAX` if there are no active snapshots, meaning all old
    /// versions can be garbage-collected.
    pub fn min_active_version(&self) -> u64 {
        let mut min = u64::MAX;
        for entry in self.active_snapshots.iter() {
            let v = entry.value().version;
            if v < min {
                min = v;
            }
        }
        min
    }

    /// Number of currently active snapshots.
    pub fn active_snapshot_count(&self) -> usize {
        self.active_snapshots.len()
    }

    /// Garbage-collect old partition version entries that are no longer needed
    /// by any active snapshot.
    pub fn gc(&self) {
        let min_version = self.min_active_version();
        if min_version == u64::MAX {
            // No active snapshots; can GC everything up to current_version
            let current = self.current_version.load(Ordering::Acquire);
            self.gc_up_to(current);
        } else {
            self.gc_up_to(min_version);
        }
    }

    fn gc_up_to(&self, min_version: u64) {
        let history = self.partition_history.lock().unwrap();
        for mut entry in history.iter_mut() {
            entry.value_mut().gc(min_version);
        }
    }
}

impl Default for MvccManager {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// RAII Snapshot Guard
// ---------------------------------------------------------------------------

/// An RAII guard that automatically releases the snapshot when dropped.
pub struct SnapshotGuard {
    snapshot: Snapshot,
    manager: Arc<MvccManager>,
}

impl SnapshotGuard {
    /// Create a new snapshot guard.
    pub fn new(manager: Arc<MvccManager>) -> Self {
        let snapshot = manager.begin_snapshot();
        Self { snapshot, manager }
    }

    /// Access the underlying snapshot.
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Check visible row count for a partition.
    pub fn visible_row_count(&self, partition: &str) -> u64 {
        self.manager.visible_row_count(&self.snapshot, partition)
    }

    /// Check if a row is visible.
    pub fn is_visible(&self, partition: &str, row_id: u64) -> bool {
        self.manager.is_visible(&self.snapshot, partition, row_id)
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.manager.release_snapshot(&self.snapshot);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn basic_snapshot_sees_committed_data() {
        let mgr = MvccManager::new();

        // Write 100 rows to partition "p1"
        mgr.commit_write(&[("p1", 100)]);

        // Snapshot sees them
        let snap = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 100);
        assert!(mgr.is_visible(&snap, "p1", 0));
        assert!(mgr.is_visible(&snap, "p1", 99));
        assert!(!mgr.is_visible(&snap, "p1", 100));
        mgr.release_snapshot(&snap);
    }

    #[test]
    fn snapshot_isolation_hides_future_writes() {
        let mgr = MvccManager::new();

        // Write 50 rows
        mgr.commit_write(&[("p1", 50)]);

        // Take snapshot at version 1
        let snap = mgr.begin_snapshot();
        assert_eq!(snap.version(), 1);
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 50);

        // Write 50 more rows (version 2)
        mgr.commit_write(&[("p1", 100)]);

        // Old snapshot still sees only 50
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 50);
        assert!(mgr.is_visible(&snap, "p1", 49));
        assert!(!mgr.is_visible(&snap, "p1", 50));

        // New snapshot sees 100
        let snap2 = mgr.begin_snapshot();
        assert_eq!(snap2.version(), 2);
        assert_eq!(mgr.visible_row_count(&snap2, "p1"), 100);
        assert!(mgr.is_visible(&snap2, "p1", 99));
        assert!(!mgr.is_visible(&snap2, "p1", 100));

        mgr.release_snapshot(&snap);
        mgr.release_snapshot(&snap2);
    }

    #[test]
    fn snapshot_unknown_partition_returns_zero() {
        let mgr = MvccManager::new();
        let snap = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap, "nonexistent"), 0);
        assert!(!mgr.is_visible(&snap, "nonexistent", 0));
        mgr.release_snapshot(&snap);
    }

    #[test]
    fn multiple_partitions_independent() {
        let mgr = MvccManager::new();

        // Write to two partitions in one commit
        mgr.commit_write(&[("p1", 100), ("p2", 200)]);

        let snap = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 100);
        assert_eq!(mgr.visible_row_count(&snap, "p2"), 200);

        // Write more to p1 only
        mgr.commit_write(&[("p1", 150)]);

        // Old snapshot unchanged
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 100);
        assert_eq!(mgr.visible_row_count(&snap, "p2"), 200);

        // New snapshot sees updated p1
        let snap2 = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap2, "p1"), 150);
        assert_eq!(mgr.visible_row_count(&snap2, "p2"), 200);

        mgr.release_snapshot(&snap);
        mgr.release_snapshot(&snap2);
    }

    #[test]
    fn min_active_version_tracks_oldest_snapshot() {
        let mgr = MvccManager::new();

        // No snapshots => MAX
        assert_eq!(mgr.min_active_version(), u64::MAX);

        mgr.commit_write(&[("p1", 10)]);
        let snap1 = mgr.begin_snapshot(); // version 1

        mgr.commit_write(&[("p1", 20)]);
        let snap2 = mgr.begin_snapshot(); // version 2

        assert_eq!(mgr.min_active_version(), 1);

        mgr.release_snapshot(&snap1);
        assert_eq!(mgr.min_active_version(), 2);

        mgr.release_snapshot(&snap2);
        assert_eq!(mgr.min_active_version(), u64::MAX);
    }

    #[test]
    fn active_snapshot_count() {
        let mgr = MvccManager::new();
        assert_eq!(mgr.active_snapshot_count(), 0);

        let s1 = mgr.begin_snapshot();
        assert_eq!(mgr.active_snapshot_count(), 1);

        let s2 = mgr.begin_snapshot();
        assert_eq!(mgr.active_snapshot_count(), 2);

        mgr.release_snapshot(&s1);
        assert_eq!(mgr.active_snapshot_count(), 1);

        mgr.release_snapshot(&s2);
        assert_eq!(mgr.active_snapshot_count(), 0);
    }

    #[test]
    fn version_monotonically_increases() {
        let mgr = MvccManager::new();
        assert_eq!(mgr.current_version(), 0);

        let v1 = mgr.commit_write(&[("p1", 10)]);
        assert_eq!(v1, 1);

        let v2 = mgr.commit_write(&[("p1", 20)]);
        assert_eq!(v2, 2);

        let v3 = mgr.commit_write(&[("p1", 30)]);
        assert_eq!(v3, 3);

        assert_eq!(mgr.current_version(), 3);
    }

    #[test]
    fn snapshot_guard_raii() {
        let mgr = Arc::new(MvccManager::new());
        mgr.commit_write(&[("p1", 50)]);

        {
            let guard = SnapshotGuard::new(Arc::clone(&mgr));
            assert_eq!(guard.visible_row_count("p1"), 50);
            assert!(guard.is_visible("p1", 49));
            assert!(!guard.is_visible("p1", 50));
            assert_eq!(mgr.active_snapshot_count(), 1);
        }
        // Guard dropped, snapshot released
        assert_eq!(mgr.active_snapshot_count(), 0);
    }

    #[test]
    fn gc_removes_old_versions() {
        let mgr = MvccManager::new();

        // Create multiple versions
        mgr.commit_write(&[("p1", 10)]); // v1
        mgr.commit_write(&[("p1", 20)]); // v2
        mgr.commit_write(&[("p1", 30)]); // v3
        mgr.commit_write(&[("p1", 40)]); // v4

        // With no active snapshots, GC should clean up
        mgr.gc();

        // Should still work for new snapshots
        let snap = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 40);
        mgr.release_snapshot(&snap);
    }

    #[test]
    fn gc_preserves_active_snapshot_data() {
        let mgr = MvccManager::new();

        mgr.commit_write(&[("p1", 10)]); // v1
        let snap = mgr.begin_snapshot(); // pinned at v1

        mgr.commit_write(&[("p1", 20)]); // v2
        mgr.commit_write(&[("p1", 30)]); // v3

        // GC should preserve data needed by snap (v1)
        mgr.gc();

        // Snapshot at v1 should still see 10 rows
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 10);

        mgr.release_snapshot(&snap);
    }

    #[test]
    fn snapshot_at_version_zero_sees_nothing() {
        let mgr = MvccManager::new();
        let snap = mgr.begin_snapshot();
        assert_eq!(snap.version(), 0);
        assert_eq!(mgr.visible_row_count(&snap, "p1"), 0);
        assert!(!mgr.is_visible(&snap, "p1", 0));
        mgr.release_snapshot(&snap);
    }

    #[test]
    fn with_version_starts_at_given_version() {
        let mgr = MvccManager::with_version(42);
        assert_eq!(mgr.current_version(), 42);

        let snap = mgr.begin_snapshot();
        assert_eq!(snap.version(), 42);
        mgr.release_snapshot(&snap);

        let v = mgr.commit_write(&[("p1", 10)]);
        assert_eq!(v, 43);
    }

    // -- End-to-end scenario tests ------------------------------------------

    #[test]
    fn e2e_write_snapshot_write_read() {
        // Scenario: write rows, take snapshot, write more, verify snapshot sees only old rows
        let mgr = MvccManager::new();

        // Phase 1: write 100 rows to "orders"
        mgr.commit_write(&[("orders", 100)]);

        // Phase 2: take snapshot
        let reader_snap = mgr.begin_snapshot();
        assert_eq!(reader_snap.version(), 1);

        // Phase 3: write 50 more rows (total 150)
        mgr.commit_write(&[("orders", 150)]);

        // Phase 4: verify snapshot sees only 100 rows
        assert_eq!(mgr.visible_row_count(&reader_snap, "orders"), 100);
        for row_id in 0..100 {
            assert!(
                mgr.is_visible(&reader_snap, "orders", row_id),
                "row {row_id} should be visible"
            );
        }
        for row_id in 100..150 {
            assert!(
                !mgr.is_visible(&reader_snap, "orders", row_id),
                "row {row_id} should NOT be visible"
            );
        }

        // Phase 5: new reader sees all 150
        let reader_snap2 = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&reader_snap2, "orders"), 150);

        mgr.release_snapshot(&reader_snap);
        mgr.release_snapshot(&reader_snap2);
    }

    #[test]
    fn e2e_concurrent_readers_see_consistent_data() {
        // Multiple readers taken at different times see different but consistent data
        let mgr = Arc::new(MvccManager::new());

        mgr.commit_write(&[("trades", 1000)]);
        let snap_v1 = mgr.begin_snapshot();

        mgr.commit_write(&[("trades", 2000)]);
        let snap_v2 = mgr.begin_snapshot();

        mgr.commit_write(&[("trades", 3000)]);
        let snap_v3 = mgr.begin_snapshot();

        // Each snapshot sees its own consistent view
        assert_eq!(mgr.visible_row_count(&snap_v1, "trades"), 1000);
        assert_eq!(mgr.visible_row_count(&snap_v2, "trades"), 2000);
        assert_eq!(mgr.visible_row_count(&snap_v3, "trades"), 3000);

        // Row visibility is consistent within each snapshot
        assert!(mgr.is_visible(&snap_v1, "trades", 999));
        assert!(!mgr.is_visible(&snap_v1, "trades", 1000));

        assert!(mgr.is_visible(&snap_v2, "trades", 1999));
        assert!(!mgr.is_visible(&snap_v2, "trades", 2000));

        assert!(mgr.is_visible(&snap_v3, "trades", 2999));
        assert!(!mgr.is_visible(&snap_v3, "trades", 3000));

        mgr.release_snapshot(&snap_v1);
        mgr.release_snapshot(&snap_v2);
        mgr.release_snapshot(&snap_v3);
    }

    #[test]
    fn e2e_concurrent_writers_and_readers() {
        let mgr = Arc::new(MvccManager::new());
        let num_writers = 4;
        let writes_per_thread = 50;
        let num_readers = 4;
        let reads_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_writers + num_readers));

        // Writer threads: each commits writes with increasing row counts
        let writer_handles: Vec<_> = (0..num_writers)
            .map(|w| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 1..=writes_per_thread {
                        let partition = format!("w{w}");
                        mgr.commit_write(&[(&partition, (i * 10) as u64)]);
                    }
                })
            })
            .collect();

        // Reader threads: take snapshots and verify consistency
        let reader_handles: Vec<_> = (0..num_readers)
            .map(|_| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..reads_per_thread {
                        let snap = mgr.begin_snapshot();
                        let _version = snap.version();

                        // For each writer's partition, the visible count should be
                        // consistent: if we see N rows, all rows 0..N should be visible
                        for w in 0..num_writers {
                            let partition = format!("w{w}");
                            let count = mgr.visible_row_count(&snap, &partition);
                            // Verify all rows below count are visible
                            if count > 0 {
                                assert!(mgr.is_visible(&snap, &partition, 0));
                                assert!(mgr.is_visible(&snap, &partition, count - 1));
                            }
                            // Row at count should not be visible
                            assert!(!mgr.is_visible(&snap, &partition, count));
                        }

                        mgr.release_snapshot(&snap);
                    }
                })
            })
            .collect();

        for h in writer_handles {
            h.join().unwrap();
        }
        for h in reader_handles {
            h.join().unwrap();
        }

        assert_eq!(mgr.active_snapshot_count(), 0);
    }

    #[test]
    fn e2e_snapshot_guard_concurrent() {
        let mgr = Arc::new(MvccManager::new());
        mgr.commit_write(&[("data", 500)]);

        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..100 {
                        let guard = SnapshotGuard::new(Arc::clone(&mgr));
                        let count = guard.visible_row_count("data");
                        assert!(count >= 500);
                        // guard drops here, releasing snapshot
                    }
                })
            })
            .collect();

        // Concurrent writer
        for i in 1..=50 {
            mgr.commit_write(&[("data", 500 + i * 10)]);
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(mgr.active_snapshot_count(), 0);
    }

    #[test]
    fn e2e_incremental_writes_across_partitions() {
        let mgr = MvccManager::new();

        // Simulate time-series writes across day partitions
        mgr.commit_write(&[("2024-01-01", 1000)]);
        mgr.commit_write(&[("2024-01-02", 500)]);

        let snap = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap, "2024-01-01"), 1000);
        assert_eq!(mgr.visible_row_count(&snap, "2024-01-02"), 500);
        assert_eq!(mgr.visible_row_count(&snap, "2024-01-03"), 0);

        // Write to a new partition and update existing one
        mgr.commit_write(&[("2024-01-03", 200), ("2024-01-01", 1200)]);

        // Old snapshot unchanged
        assert_eq!(mgr.visible_row_count(&snap, "2024-01-01"), 1000);
        assert_eq!(mgr.visible_row_count(&snap, "2024-01-02"), 500);
        assert_eq!(mgr.visible_row_count(&snap, "2024-01-03"), 0);

        // New snapshot sees everything
        let snap2 = mgr.begin_snapshot();
        assert_eq!(mgr.visible_row_count(&snap2, "2024-01-01"), 1200);
        assert_eq!(mgr.visible_row_count(&snap2, "2024-01-02"), 500);
        assert_eq!(mgr.visible_row_count(&snap2, "2024-01-03"), 200);

        mgr.release_snapshot(&snap);
        mgr.release_snapshot(&snap2);
    }
}
