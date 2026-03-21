//! Connection pool / connection lifecycle management.
//!
//! Provides a simple connection-count limiter. When the active connection count
//! reaches `max_connections`, new requests are rejected with HTTP 503.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use axum::response::IntoResponse;

/// A shared pool handle that can be cloned cheaply.
///
/// Tracks the number of active connections and enforces a maximum.
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    inner: Arc<PoolInner>,
}

#[derive(Debug)]
struct PoolInner {
    max_connections: u32,
    active: AtomicU32,
}

/// RAII guard returned by [`ConnectionPool::try_acquire`].
///
/// Decrements the active connection count when dropped.
#[derive(Debug)]
pub struct ConnectionGuard {
    pool: Arc<PoolInner>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.pool.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ConnectionPool {
    /// Create a new connection pool with the given maximum number of
    /// concurrent connections.
    pub fn new(max_connections: u32) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                max_connections,
                active: AtomicU32::new(0),
            }),
        }
    }

    /// Attempt to acquire a connection guard. Returns `None` if the pool
    /// has reached its maximum number of active connections.
    pub fn try_acquire(&self) -> Option<ConnectionGuard> {
        loop {
            let current = self.inner.active.load(Ordering::SeqCst);
            if current >= self.inner.max_connections {
                return None;
            }
            if self
                .inner
                .active
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(ConnectionGuard {
                    pool: Arc::clone(&self.inner),
                });
            }
        }
    }

    /// Return the current number of active connections.
    pub fn active_count(&self) -> u32 {
        self.inner.active.load(Ordering::SeqCst)
    }

    /// Return the configured maximum.
    pub fn max_connections(&self) -> u32 {
        self.inner.max_connections
    }
}

/// Axum middleware that rejects requests with HTTP 503 when the connection
/// pool is exhausted.
pub async fn connection_limit_middleware(
    pool: ConnectionPool,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let guard = match pool.try_acquire() {
        Some(g) => g,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "connection limit reached",
            )
                .into_response();
        }
    };

    let response = next.run(request).await;
    drop(guard);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_acquire_release() {
        let pool = ConnectionPool::new(2);
        assert_eq!(pool.active_count(), 0);

        let g1 = pool.try_acquire();
        assert!(g1.is_some());
        assert_eq!(pool.active_count(), 1);

        let g2 = pool.try_acquire();
        assert!(g2.is_some());
        assert_eq!(pool.active_count(), 2);

        // Pool is full.
        let g3 = pool.try_acquire();
        assert!(g3.is_none());
        assert_eq!(pool.active_count(), 2);

        // Release one.
        drop(g1);
        assert_eq!(pool.active_count(), 1);

        // Can acquire again.
        let g4 = pool.try_acquire();
        assert!(g4.is_some());
        assert_eq!(pool.active_count(), 2);

        drop(g2);
        drop(g4);
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_pool_max_connections() {
        let pool = ConnectionPool::new(100);
        assert_eq!(pool.max_connections(), 100);
    }

    #[test]
    fn test_pool_zero_capacity() {
        let pool = ConnectionPool::new(0);
        let g = pool.try_acquire();
        assert!(g.is_none());
    }

    #[test]
    fn test_pool_clone() {
        let pool = ConnectionPool::new(1);
        let pool2 = pool.clone();

        let g = pool.try_acquire();
        assert!(g.is_some());
        assert_eq!(pool2.active_count(), 1);

        // Both handles see the same state.
        let g2 = pool2.try_acquire();
        assert!(g2.is_none());

        drop(g);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(pool2.active_count(), 0);
    }
}
