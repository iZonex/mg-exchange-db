//! Per-query memory budget tracking to prevent OOM.

use std::sync::atomic::{AtomicU64, Ordering};

use exchange_common::error::{ExchangeDbError, Result};

/// Default memory limit per query: 256 MB.
pub const DEFAULT_MEMORY_LIMIT: u64 = 256 * 1024 * 1024;

/// Tracks memory usage per query to prevent OOM.
pub struct QueryMemoryTracker {
    allocated: AtomicU64,
    limit: u64,
    query_id: u64,
}

impl QueryMemoryTracker {
    /// Create a new tracker with the given byte limit and query id.
    pub fn new(limit: u64, query_id: u64) -> Self {
        Self {
            allocated: AtomicU64::new(0),
            limit,
            query_id,
        }
    }

    /// Try to allocate `bytes` of memory. Returns an error if the
    /// allocation would exceed the per-query limit.
    pub fn try_allocate(&self, bytes: u64) -> Result<()> {
        loop {
            let current = self.allocated.load(Ordering::Relaxed);
            let new = current + bytes;
            if new > self.limit {
                return Err(ExchangeDbError::ResourceExhausted(format!(
                    "query {} memory limit exceeded: {} + {} > {} bytes",
                    self.query_id, current, bytes, self.limit
                )));
            }
            match self.allocated.compare_exchange_weak(
                current,
                new,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue,
            }
        }
    }

    /// Release previously allocated memory.
    pub fn release(&self, bytes: u64) {
        self.allocated.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Current memory usage in bytes.
    pub fn used(&self) -> u64 {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Remaining budget in bytes.
    pub fn remaining(&self) -> u64 {
        let used = self.used();
        if used >= self.limit {
            0
        } else {
            self.limit - used
        }
    }

    /// The configured limit.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// The query id.
    pub fn query_id(&self) -> u64 {
        self.query_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_within_limit_succeeds() {
        let tracker = QueryMemoryTracker::new(1024, 1);
        assert!(tracker.try_allocate(512).is_ok());
        assert_eq!(tracker.used(), 512);
        assert_eq!(tracker.remaining(), 512);

        assert!(tracker.try_allocate(256).is_ok());
        assert_eq!(tracker.used(), 768);
        assert_eq!(tracker.remaining(), 256);

        // Exactly at limit should succeed.
        assert!(tracker.try_allocate(256).is_ok());
        assert_eq!(tracker.used(), 1024);
        assert_eq!(tracker.remaining(), 0);
    }

    #[test]
    fn allocation_exceeding_limit_returns_error() {
        let tracker = QueryMemoryTracker::new(1024, 42);

        assert!(tracker.try_allocate(512).is_ok());
        // 512 + 600 = 1112 > 1024
        let err = tracker.try_allocate(600);
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("memory limit exceeded"));
        // Original allocation should not have changed.
        assert_eq!(tracker.used(), 512);
    }

    #[test]
    fn release_frees_budget() {
        let tracker = QueryMemoryTracker::new(1024, 1);
        tracker.try_allocate(800).unwrap();
        assert!(tracker.try_allocate(300).is_err());

        tracker.release(500);
        assert_eq!(tracker.used(), 300);
        assert_eq!(tracker.remaining(), 724);

        assert!(tracker.try_allocate(300).is_ok());
        assert_eq!(tracker.used(), 600);
    }

    #[test]
    fn zero_limit_rejects_all() {
        let tracker = QueryMemoryTracker::new(0, 1);
        assert!(tracker.try_allocate(1).is_err());
    }
}
