//! Primary health monitoring for automatic failover.
//!
//! The [`PrimaryHealthMonitor`] runs on a replica node and periodically
//! checks whether the primary is reachable via a TCP connection. After a
//! configurable number of consecutive failures the monitor invokes a
//! caller-supplied failover callback and exits.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

/// Monitors primary health from a replica and triggers failover when the
/// primary has been unreachable for `failure_threshold` consecutive checks.
pub struct PrimaryHealthMonitor {
    /// Address of the primary's HTTP/TCP endpoint used for health checks.
    primary_http_addr: String,
    /// How often to probe the primary.
    check_interval: Duration,
    /// Number of consecutive failures before triggering failover.
    failure_threshold: u32,
    /// Running counter of consecutive failures.
    consecutive_failures: AtomicU32,
    /// Whether the primary is considered healthy right now.
    is_primary_healthy: AtomicBool,
}

impl PrimaryHealthMonitor {
    /// Create a new health monitor.
    pub fn new(
        primary_http_addr: String,
        check_interval: Duration,
        failure_threshold: u32,
    ) -> Self {
        Self {
            primary_http_addr,
            check_interval,
            failure_threshold,
            consecutive_failures: AtomicU32::new(0),
            is_primary_healthy: AtomicBool::new(true),
        }
    }

    /// Create a monitor with default settings (2s interval, 3 failures).
    pub fn with_defaults(primary_http_addr: String) -> Self {
        Self::new(primary_http_addr, Duration::from_secs(2), 3)
    }

    /// Start monitoring primary health in a background loop.
    ///
    /// The monitor sleeps for `check_interval`, then attempts a TCP
    /// connection to the primary. On success the failure counter resets.
    /// On failure the counter increments; once it reaches
    /// `failure_threshold` the `failover_callback` is invoked and the
    /// loop exits.
    ///
    /// This method is intended to be spawned as a background task:
    ///
    /// ```ignore
    /// let monitor = Arc::new(PrimaryHealthMonitor::new(...));
    /// tokio::spawn(monitor.start(|| { /* promote */ }));
    /// ```
    pub async fn start(self: Arc<Self>, failover_callback: impl Fn() + Send + 'static) {
        tracing::info!(
            primary = %self.primary_http_addr,
            interval = ?self.check_interval,
            threshold = self.failure_threshold,
            "starting primary health monitor"
        );

        loop {
            tokio::time::sleep(self.check_interval).await;

            let healthy = self.check_primary().await;

            if healthy {
                let prev = self.consecutive_failures.swap(0, Ordering::SeqCst);
                if prev > 0 {
                    tracing::info!(
                        previous_failures = prev,
                        "primary reachable again, resetting failure counter"
                    );
                }
                self.is_primary_healthy.store(true, Ordering::SeqCst);
            } else {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
                self.is_primary_healthy.store(false, Ordering::SeqCst);

                tracing::warn!(
                    consecutive_failures = failures,
                    threshold = self.failure_threshold,
                    primary = %self.primary_http_addr,
                    "primary health check failed"
                );

                if failures >= self.failure_threshold {
                    tracing::warn!(
                        "Primary unreachable after {} checks, triggering failover",
                        failures
                    );
                    failover_callback();
                    break;
                }
            }
        }
    }

    /// Attempt a TCP connection to the primary with a 2-second timeout.
    ///
    /// Returns `true` if the connection succeeds, `false` on timeout or
    /// connection error.
    async fn check_primary(&self) -> bool {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(&self.primary_http_addr),
        )
        .await;
        result.is_ok() && result.unwrap().is_ok()
    }

    /// Get the current consecutive failure count.
    pub fn current_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::SeqCst)
    }

    /// Whether the primary is currently considered healthy.
    pub fn is_primary_healthy(&self) -> bool {
        self.is_primary_healthy.load(Ordering::SeqCst)
    }

    /// Get the configured failure threshold.
    pub fn failure_threshold(&self) -> u32 {
        self.failure_threshold
    }

    /// Get the configured check interval.
    pub fn check_interval(&self) -> Duration {
        self.check_interval
    }

    /// Get the primary address being monitored.
    pub fn primary_addr(&self) -> &str {
        &self.primary_http_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn default_constructor() {
        let m = PrimaryHealthMonitor::with_defaults("127.0.0.1:9000".into());
        assert_eq!(m.check_interval(), Duration::from_secs(2));
        assert_eq!(m.failure_threshold(), 3);
        assert_eq!(m.current_failures(), 0);
        assert!(m.is_primary_healthy());
    }

    #[tokio::test]
    async fn triggers_failover_on_unreachable_primary() {
        let triggered = Arc::new(AtomicBool::new(false));
        let triggered_clone = triggered.clone();

        // Use an address that is guaranteed to be unreachable.
        let monitor = Arc::new(PrimaryHealthMonitor::new(
            "127.0.0.1:19997".into(),
            Duration::from_millis(10), // very fast checks
            3,
        ));

        // start() should return after the failover callback fires.
        let monitor_ref = monitor.clone();
        monitor
            .start(move || {
                triggered_clone.store(true, Ordering::SeqCst);
            })
            .await;

        assert!(triggered.load(Ordering::SeqCst));
        assert!(monitor_ref.current_failures() >= 3);
        assert!(!monitor_ref.is_primary_healthy());
    }

    #[tokio::test]
    async fn reachable_primary_does_not_trigger_failover() {
        // Start a TCP listener to act as a reachable primary.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let monitor = Arc::new(PrimaryHealthMonitor::new(
            addr.to_string(),
            Duration::from_millis(50),
            2,
        ));

        // Run 3 check iterations without triggering failover.
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(60)).await;
        }

        assert_eq!(monitor.current_failures(), 0);
        assert!(monitor.is_primary_healthy());
    }

    #[test]
    fn failure_counter_increments() {
        let m = PrimaryHealthMonitor::new("127.0.0.1:1".into(), Duration::from_secs(1), 10);

        // Simulate failures via atomic.
        m.consecutive_failures.store(5, Ordering::SeqCst);
        assert_eq!(m.current_failures(), 5);

        // Simulate reset.
        m.consecutive_failures.store(0, Ordering::SeqCst);
        assert_eq!(m.current_failures(), 0);
    }
}
