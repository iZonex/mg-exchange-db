//! Resource management and query admission control.
//!
//! Enforces per-node limits on concurrent queries, memory usage,
//! scan bytes, and query duration. Provides a circuit-breaker
//! mechanism to abort runaway queries.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use exchange_common::error::{ExchangeDbError, Result};

/// Configurable resource limits for query execution.
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory (bytes) a single query may use.
    pub max_memory_bytes: u64,
    /// Maximum wall-clock duration for a query.
    pub max_query_time: Duration,
    /// Maximum number of rows in a result set.
    pub max_result_rows: u64,
    /// Maximum number of concurrent queries per node.
    pub max_concurrent_queries: u32,
    /// Maximum bytes to scan per query.
    pub max_scan_bytes: u64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024,     // 1 GB
            max_query_time: Duration::from_secs(300), // 5 min
            max_result_rows: 10_000_000,
            max_concurrent_queries: 64,
            max_scan_bytes: 10 * 1024 * 1024 * 1024, // 10 GB
        }
    }
}

/// Manages resource consumption and admission control for queries.
pub struct ResourceManager {
    limits: ResourceLimits,
    active_queries: AtomicU32,
    memory_used: AtomicU64,
    next_token_id: AtomicU64,
}

/// Token returned when a query is admitted. Must be passed to
/// [`ResourceManager::release`] when the query completes.
pub struct QueryToken {
    id: u64,
    admitted_at: Instant,
}

impl QueryToken {
    /// Unique identifier for this admitted query.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// When this query was admitted.
    pub fn admitted_at(&self) -> Instant {
        self.admitted_at
    }

    /// How long the query has been running.
    pub fn elapsed(&self) -> Duration {
        self.admitted_at.elapsed()
    }
}

impl ResourceManager {
    /// Create a new resource manager with the given limits.
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            limits,
            active_queries: AtomicU32::new(0),
            memory_used: AtomicU64::new(0),
            next_token_id: AtomicU64::new(1),
        }
    }

    /// Try to admit a new query. Returns a [`QueryToken`] on success,
    /// or an error if the concurrent query limit has been reached.
    pub fn try_admit(&self) -> Result<QueryToken> {
        let prev = self.active_queries.fetch_add(1, Ordering::SeqCst);
        if prev >= self.limits.max_concurrent_queries {
            self.active_queries.fetch_sub(1, Ordering::SeqCst);
            return Err(ExchangeDbError::Query(format!(
                "concurrent query limit reached ({}/{})",
                prev, self.limits.max_concurrent_queries
            )));
        }

        let id = self.next_token_id.fetch_add(1, Ordering::Relaxed);
        Ok(QueryToken {
            id,
            admitted_at: Instant::now(),
        })
    }

    /// Release resources held by a completed query.
    pub fn release(&self, _token: QueryToken) {
        self.active_queries.fetch_sub(1, Ordering::SeqCst);
    }

    /// Check whether allocating `additional_bytes` would exceed the
    /// per-query memory limit.
    pub fn check_memory(&self, additional_bytes: u64) -> Result<()> {
        let current = self.memory_used.load(Ordering::Relaxed);
        if current + additional_bytes > self.limits.max_memory_bytes {
            return Err(ExchangeDbError::Query(format!(
                "memory limit exceeded: {} + {} > {} bytes",
                current, additional_bytes, self.limits.max_memory_bytes
            )));
        }
        Ok(())
    }

    /// Record memory allocation. Call this when a query allocates memory.
    pub fn alloc_memory(&self, bytes: u64) -> Result<()> {
        self.check_memory(bytes)?;
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Free previously allocated memory.
    pub fn free_memory(&self, bytes: u64) {
        self.memory_used.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Circuit breaker: check if a query has exceeded scan or time limits.
    pub fn check_limits(&self, scan_bytes: u64, elapsed: Duration) -> Result<()> {
        if scan_bytes > self.limits.max_scan_bytes {
            return Err(ExchangeDbError::Query(format!(
                "scan limit exceeded: {} > {} bytes",
                scan_bytes, self.limits.max_scan_bytes
            )));
        }
        if elapsed > self.limits.max_query_time {
            return Err(ExchangeDbError::Query(format!(
                "query timeout: {:?} > {:?}",
                elapsed, self.limits.max_query_time
            )));
        }
        Ok(())
    }

    /// Check if a result set has exceeded the row limit.
    pub fn check_result_rows(&self, row_count: u64) -> Result<()> {
        if row_count > self.limits.max_result_rows {
            return Err(ExchangeDbError::Query(format!(
                "result row limit exceeded: {} > {}",
                row_count, self.limits.max_result_rows
            )));
        }
        Ok(())
    }

    /// Current number of active queries.
    pub fn active_query_count(&self) -> u32 {
        self.active_queries.load(Ordering::Relaxed)
    }

    /// Current memory usage in bytes.
    pub fn memory_used(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Get the configured limits.
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_limits() -> ResourceLimits {
        ResourceLimits {
            max_memory_bytes: 1024,
            max_query_time: Duration::from_millis(100),
            max_result_rows: 10,
            max_concurrent_queries: 2,
            max_scan_bytes: 4096,
        }
    }

    #[test]
    fn admit_and_release() {
        let mgr = ResourceManager::new(small_limits());
        assert_eq!(mgr.active_query_count(), 0);

        let token = mgr.try_admit().unwrap();
        assert_eq!(mgr.active_query_count(), 1);

        mgr.release(token);
        assert_eq!(mgr.active_query_count(), 0);
    }

    #[test]
    fn concurrent_query_limit() {
        let mgr = ResourceManager::new(small_limits());

        let t1 = mgr.try_admit().unwrap();
        let t2 = mgr.try_admit().unwrap();

        // Third admission should fail
        let result = mgr.try_admit();
        assert!(result.is_err());
        assert_eq!(mgr.active_query_count(), 2);

        // Release one, then try again
        mgr.release(t1);
        let _t3 = mgr.try_admit().unwrap();
        assert_eq!(mgr.active_query_count(), 2);

        mgr.release(t2);
        mgr.release(_t3);
    }

    #[test]
    fn memory_limit_enforcement() {
        let mgr = ResourceManager::new(small_limits());

        // Should succeed: 512 < 1024
        mgr.alloc_memory(512).unwrap();
        assert_eq!(mgr.memory_used(), 512);

        // Should succeed: 512 + 256 = 768 < 1024
        mgr.alloc_memory(256).unwrap();
        assert_eq!(mgr.memory_used(), 768);

        // Should fail: 768 + 512 = 1280 > 1024
        let result = mgr.alloc_memory(512);
        assert!(result.is_err());

        // Free some memory
        mgr.free_memory(256);
        assert_eq!(mgr.memory_used(), 512);

        // Now should succeed: 512 + 512 = 1024 <= 1024
        mgr.alloc_memory(512).unwrap();
    }

    #[test]
    fn check_memory_does_not_allocate() {
        let mgr = ResourceManager::new(small_limits());

        // check_memory only checks, does not allocate
        mgr.check_memory(512).unwrap();
        assert_eq!(mgr.memory_used(), 0);

        // Should fail for over-limit check
        let result = mgr.check_memory(2048);
        assert!(result.is_err());
    }

    #[test]
    fn scan_limit_circuit_breaker() {
        let mgr = ResourceManager::new(small_limits());

        // Under limit
        mgr.check_limits(2048, Duration::from_millis(10)).unwrap();

        // Over scan limit
        let result = mgr.check_limits(8192, Duration::from_millis(10));
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("scan limit"));
    }

    #[test]
    fn timeout_circuit_breaker() {
        let mgr = ResourceManager::new(small_limits());

        // Under time limit
        mgr.check_limits(100, Duration::from_millis(50)).unwrap();

        // Over time limit
        let result = mgr.check_limits(100, Duration::from_millis(200));
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("query timeout"));
    }

    #[test]
    fn result_row_limit() {
        let mgr = ResourceManager::new(small_limits());

        mgr.check_result_rows(5).unwrap();
        mgr.check_result_rows(10).unwrap();

        let result = mgr.check_result_rows(11);
        assert!(result.is_err());
    }

    #[test]
    fn query_token_tracks_time() {
        let mgr = ResourceManager::new(ResourceLimits::default());
        let token = mgr.try_admit().unwrap();
        assert!(token.id() > 0);

        // Elapsed should be very small
        let elapsed = token.elapsed();
        assert!(elapsed < Duration::from_secs(1));

        mgr.release(token);
    }

    #[test]
    fn concurrent_admits_from_threads() {
        use std::sync::Arc;

        let mgr = Arc::new(ResourceManager::new(ResourceLimits {
            max_concurrent_queries: 4,
            ..ResourceLimits::default()
        }));

        let barrier = std::sync::Barrier::new(4);
        let barrier = Arc::new(barrier);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let mgr = Arc::clone(&mgr);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let token = mgr.try_admit().unwrap();
                    std::thread::sleep(Duration::from_millis(10));
                    mgr.release(token);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(mgr.active_query_count(), 0);
    }
}
