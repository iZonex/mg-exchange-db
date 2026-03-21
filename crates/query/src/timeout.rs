//! Query deadline / timeout support.

use std::time::{Duration, Instant};

use exchange_common::error::{ExchangeDbError, Result};

/// Tracks a per-query deadline and allows periodic checks.
pub struct QueryDeadline {
    deadline: Instant,
}

impl QueryDeadline {
    /// Create a new deadline that expires after `timeout` from now.
    pub fn new(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
        }
    }

    /// Check whether the deadline has been exceeded.
    /// Returns `Err` if the query has timed out.
    pub fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err(ExchangeDbError::Query(
                "query timeout: deadline exceeded".into(),
            ))
        } else {
            Ok(())
        }
    }

    /// How much time remains before the deadline.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Whether the deadline has already passed.
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_not_expired() {
        let deadline = QueryDeadline::new(Duration::from_secs(60));
        assert!(deadline.check().is_ok());
        assert!(!deadline.is_expired());
        assert!(deadline.remaining() > Duration::from_secs(50));
    }

    #[test]
    fn expired_deadline_returns_error() {
        let deadline = QueryDeadline::new(Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(10));
        assert!(deadline.is_expired());
        let err = deadline.check();
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("query timeout"));
        assert_eq!(deadline.remaining(), Duration::ZERO);
    }

    #[test]
    fn long_running_query_cancelled() {
        // Simulate a long-running query that checks the deadline periodically.
        let deadline = QueryDeadline::new(Duration::from_millis(20));
        let mut iterations = 0u64;
        loop {
            iterations += 1;
            // Simulate work.
            std::thread::sleep(Duration::from_millis(1));
            if deadline.check().is_err() {
                break;
            }
        }
        // Should have done some iterations before being cancelled.
        assert!(iterations > 0);
        assert!(iterations < 1000);
        assert!(deadline.is_expired());
    }
}
