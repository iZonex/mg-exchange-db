//! Fine-grained per-partition locking.
//!
//! Multiple writers can write to DIFFERENT partitions simultaneously,
//! but only one writer per partition. Uses in-process `Mutex` locks
//! tracked in a `DashMap`.

use dashmap::DashMap;
use std::sync::{Arc, Mutex};

/// Fine-grained lock per partition directory.
///
/// Multiple writers can write to DIFFERENT partitions simultaneously,
/// but only one writer per partition.
pub struct PartitionLockManager {
    locks: DashMap<String, Arc<Mutex<()>>>,
}

impl PartitionLockManager {
    pub fn new() -> Self {
        Self {
            locks: DashMap::new(),
        }
    }

    /// Lock a specific partition. Returns a guard that unlocks on drop.
    /// Blocks if the partition is already locked by another thread.
    pub fn lock_partition(&self, partition_name: &str) -> PartitionGuard {
        let mutex = self
            .locks
            .entry(partition_name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        // Drop the DashMap ref before locking the mutex to avoid
        // potential deadlock between DashMap shard locks and partition mutexes.
        let guard = mutex.lock().unwrap();

        // SAFETY: The `MutexGuard` borrows from `mutex`, but we store both
        // in the returned struct. The `Arc<Mutex<()>>` is kept alive by
        // `_mutex`, and the guard is dropped before `_mutex` (field order).
        // We transmute the lifetime to 'static so it can be stored alongside
        // its owning Arc.
        let guard: std::sync::MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };

        PartitionGuard {
            _guard: Some(guard),
            _mutex: mutex,
        }
    }

    /// Try to lock without blocking. Returns `None` if the partition is
    /// already locked by another thread.
    pub fn try_lock_partition(&self, partition_name: &str) -> Option<PartitionGuard> {
        let mutex = self
            .locks
            .entry(partition_name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        // Clone the Arc so we can move one into the result while the
        // try_lock borrow is on the other.
        let mutex2 = Arc::clone(&mutex);

        // Drop the DashMap ref before trying the mutex.
        let result = mutex.try_lock();
        match result {
            Ok(guard) => {
                let guard: std::sync::MutexGuard<'static, ()> =
                    unsafe { std::mem::transmute(guard) };
                Some(PartitionGuard {
                    _guard: Some(guard),
                    _mutex: mutex2,
                })
            }
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(e)) => {
                let guard: std::sync::MutexGuard<'static, ()> =
                    unsafe { std::mem::transmute(e.into_inner()) };
                Some(PartitionGuard {
                    _guard: Some(guard),
                    _mutex: mutex2,
                })
            }
        }
    }

    /// Number of tracked partitions (for diagnostics).
    pub fn tracked_count(&self) -> usize {
        self.locks.len()
    }
}

impl Default for PartitionLockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that releases the partition lock on drop.
///
/// The `_mutex` field keeps the `Arc<Mutex<()>>` alive so the guard
/// always points to valid memory. `_guard` is dropped before `_mutex`
/// because fields are dropped in declaration order.
pub struct PartitionGuard {
    // Must be declared before _mutex so it is dropped first.
    _guard: Option<std::sync::MutexGuard<'static, ()>>,
    _mutex: Arc<Mutex<()>>,
}

// SAFETY: PartitionGuard is effectively a MutexGuard wrapper.
// MutexGuard is Send when the inner type is Send (which () is).
unsafe impl Send for PartitionGuard {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    #[test]
    fn lock_different_partitions_concurrently() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(2));

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
            let start = Instant::now();
            let _guard = mgr2.lock_partition("2024-01-02");
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_millis(30),
                "locking a different partition should be near-instant, took {elapsed:?}"
            );
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn lock_same_partition_blocks() {
        let mgr = Arc::new(PartitionLockManager::new());
        let barrier = Arc::new(Barrier::new(2));

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
            let start = Instant::now();
            let _guard = mgr2.lock_partition("2024-01-01");
            let elapsed = start.elapsed();
            assert!(
                elapsed >= Duration::from_millis(50),
                "expected blocking wait, but only waited {elapsed:?}"
            );
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn try_lock_returns_none_when_held() {
        let mgr = Arc::new(PartitionLockManager::new());
        let mgr2 = Arc::clone(&mgr);

        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = Arc::clone(&barrier);

        let handle = std::thread::spawn(move || {
            let _guard = mgr2.lock_partition("2024-01-01");
            barrier2.wait();
            std::thread::sleep(Duration::from_millis(100));
        });

        barrier.wait();
        std::thread::sleep(Duration::from_millis(10));
        let result = mgr.try_lock_partition("2024-01-01");
        assert!(result.is_none());

        handle.join().unwrap();
    }

    #[test]
    fn try_lock_returns_some_when_free() {
        let mgr = PartitionLockManager::new();
        let guard = mgr.try_lock_partition("2024-01-01");
        assert!(guard.is_some());
    }

    #[test]
    fn guard_releases_on_drop() {
        let mgr = Arc::new(PartitionLockManager::new());
        {
            let _guard = mgr.lock_partition("p1");
        }
        // Should be able to lock again
        let _guard = mgr.try_lock_partition("p1");
        assert!(_guard.is_some());
    }
}
