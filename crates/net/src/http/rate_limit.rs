//! Token-bucket rate limiter for HTTP requests, keyed by client IP address.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// A concurrent, per-IP rate limiter using a sliding-window counter.
///
/// Each IP address is tracked with a count and a window start time.
/// When the window expires, the counter resets.
pub struct RateLimiter {
    /// Map of IP -> (request count, window start).
    requests: dashmap::DashMap<IpAddr, (u64, Instant)>,
    /// Maximum number of requests allowed per window.
    max_per_window: u64,
    /// Duration of the sliding window.
    window: Duration,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `max_per_second`: maximum requests per second.
    /// - `window`: the time window over which to count requests.
    pub fn new(max_per_second: u64, window: Duration) -> Self {
        Self {
            requests: dashmap::DashMap::new(),
            max_per_window: max_per_second * window.as_secs().max(1),
            window,
        }
    }

    /// Create a rate limiter with sensible defaults (100 req/s, 1s window).
    pub fn default_config() -> Self {
        Self::new(100, Duration::from_secs(1))
    }

    /// Check whether a request from the given IP is allowed.
    ///
    /// Returns `true` if the request is within the rate limit.
    /// Returns `false` if the request exceeds the limit.
    pub fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut entry = self.requests.entry(ip).or_insert((0, now));
        let (count, window_start) = entry.value_mut();

        // If the window has expired, reset.
        if now.duration_since(*window_start) >= self.window {
            *count = 1;
            *window_start = now;
            return true;
        }

        // Within the window — check the count.
        if *count >= self.max_per_window {
            return false;
        }

        *count += 1;
        true
    }

    /// Periodically clean up expired entries to prevent unbounded memory growth.
    /// Call this from a background task.
    pub fn cleanup(&self) {
        let now = Instant::now();
        self.requests
            .retain(|_, (_, window_start)| now.duration_since(*window_start) < self.window * 2);
    }
}

/// Axum middleware layer that applies rate limiting based on client IP.
///
/// Returns 429 Too Many Requests when the rate limit is exceeded.
pub async fn rate_limit_middleware(
    State(limiter): State<Arc<RateLimiter>>,
    connect_info: Option<ConnectInfo<std::net::SocketAddr>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let ip = connect_info
        .map(|ci| ci.0.ip())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

    if !limiter.check(ip) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "Rate limit exceeded. Try again later.",
        )
            .into_response();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_allows_within_limit() {
        let limiter = RateLimiter::new(5, Duration::from_secs(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));

        for _ in 0..5 {
            assert!(limiter.check(ip), "should allow requests within limit");
        }
    }

    #[test]
    fn test_blocks_over_limit() {
        let limiter = RateLimiter::new(3, Duration::from_secs(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        // First 3 should pass
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));

        // 4th should be blocked
        assert!(!limiter.check(ip), "should block requests over limit");
    }

    #[test]
    fn test_different_ips_independent() {
        let limiter = RateLimiter::new(2, Duration::from_secs(1));
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

        // Exhaust ip1
        assert!(limiter.check(ip1));
        assert!(limiter.check(ip1));
        assert!(!limiter.check(ip1));

        // ip2 should still work
        assert!(limiter.check(ip2));
        assert!(limiter.check(ip2));
        assert!(!limiter.check(ip2));
    }

    #[test]
    fn test_window_reset() {
        // Use a very short window so it expires quickly.
        let limiter = RateLimiter::new(1, Duration::from_millis(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3));

        assert!(limiter.check(ip));
        assert!(!limiter.check(ip)); // blocked

        // Wait for the window to expire.
        std::thread::sleep(Duration::from_millis(5));

        assert!(limiter.check(ip), "should allow after window expires");
    }

    #[test]
    fn test_default_config() {
        let limiter = RateLimiter::default_config();
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // Should allow at least 1 request.
        assert!(limiter.check(ip));
    }

    #[test]
    fn test_cleanup_removes_expired() {
        let limiter = RateLimiter::new(1, Duration::from_millis(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4));

        limiter.check(ip);
        assert!(limiter.requests.len() == 1);

        std::thread::sleep(Duration::from_millis(5));
        limiter.cleanup();
        assert!(limiter.requests.is_empty(), "expired entries should be removed");
    }
}
