//! Automatic failover detection for ExchangeDB replication.
//!
//! Monitors primary health and auto-promotes the replica when the
//! primary has been unreachable for a configurable number of
//! consecutive health check failures.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use exchange_common::error::Result;
use tokio::sync::Mutex;

use super::failover::FailoverManager;

/// Automatic failover monitor that tracks consecutive primary health
/// check failures and triggers promotion when the threshold is reached.
pub struct AutoFailover {
    /// How often to check primary health.
    pub check_interval: Duration,
    /// Number of consecutive failures required before auto-promoting.
    pub failure_threshold: u32,
    /// Current count of consecutive failures.
    pub consecutive_failures: AtomicU32,
}

impl AutoFailover {
    /// Create a new auto-failover monitor.
    pub fn new(check_interval: Duration, failure_threshold: u32) -> Self {
        Self {
            check_interval,
            failure_threshold,
            consecutive_failures: AtomicU32::new(0),
        }
    }

    /// Get the current number of consecutive failures.
    pub fn current_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::SeqCst)
    }

    /// Record a successful health check (resets the failure counter).
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
    }

    /// Record a failed health check. Returns `true` if the failure
    /// threshold has been reached and failover should be triggered.
    pub fn record_failure(&self) -> bool {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::SeqCst);
        prev + 1 >= self.failure_threshold
    }

    /// Start monitoring primary health in background.
    ///
    /// Runs an infinite loop that:
    /// 1. Checks primary health via `FailoverManager::check_primary_health`
    /// 2. On success, resets the failure counter
    /// 3. On failure, increments the counter
    /// 4. If counter reaches `failure_threshold`, promotes this replica
    ///
    /// Returns after the first promotion (or error).
    pub async fn start_monitoring(
        &self,
        failover_mgr: Arc<Mutex<FailoverManager>>,
        _primary_addr: &str,
    ) -> Result<()> {
        loop {
            tokio::time::sleep(self.check_interval).await;

            let healthy = {
                let mgr = failover_mgr.lock().await;
                mgr.check_primary_health().await
            };

            if healthy {
                self.record_success();
            } else {
                let should_failover = self.record_failure();
                if should_failover {
                    tracing::warn!(
                        consecutive_failures = self.current_failures(),
                        threshold = self.failure_threshold,
                        "auto-failover threshold reached, promoting to primary"
                    );
                    let mut mgr = failover_mgr.lock().await;
                    mgr.promote_to_primary()?;
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_success_resets_counter() {
        let af = AutoFailover::new(Duration::from_secs(1), 3);
        af.record_failure();
        af.record_failure();
        assert_eq!(af.current_failures(), 2);

        af.record_success();
        assert_eq!(af.current_failures(), 0);
    }

    #[test]
    fn record_failure_increments() {
        let af = AutoFailover::new(Duration::from_secs(1), 5);
        assert!(!af.record_failure()); // 1 < 5
        assert!(!af.record_failure()); // 2 < 5
        assert!(!af.record_failure()); // 3 < 5
        assert!(!af.record_failure()); // 4 < 5
        assert!(af.record_failure()); // 5 >= 5 -> should failover
    }

    #[test]
    fn threshold_of_one_triggers_immediately() {
        let af = AutoFailover::new(Duration::from_secs(1), 1);
        assert!(af.record_failure());
    }

    #[tokio::test]
    async fn auto_failover_triggers_after_threshold() {
        use crate::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};

        let config = ReplicationConfig {
            role: ReplicationRole::Replica,
            // Use an unreachable address so health checks fail.
            primary_addr: Some("127.0.0.1:19998".to_string()),
            replica_addrs: Vec::new(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };

        let failover_mgr = Arc::new(Mutex::new(FailoverManager::new(
            config,
            Duration::from_millis(50),
        )));

        let af = AutoFailover::new(
            Duration::from_millis(10), // very fast checks
            3,                         // fail after 3 consecutive failures
        );

        // start_monitoring should return after promoting.
        let result = af.start_monitoring(failover_mgr.clone(), "127.0.0.1:19998").await;
        assert!(result.is_ok());

        // Verify the node was promoted.
        let mgr = failover_mgr.lock().await;
        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);
    }

    #[test]
    fn failure_counter_tracks_correctly() {
        let af = AutoFailover::new(Duration::from_secs(1), 10);

        for _ in 0..5 {
            af.record_failure();
        }
        assert_eq!(af.current_failures(), 5);

        af.record_success();
        assert_eq!(af.current_failures(), 0);

        for _ in 0..3 {
            af.record_failure();
        }
        assert_eq!(af.current_failures(), 3);
    }
}
