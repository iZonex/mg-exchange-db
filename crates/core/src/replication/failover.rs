use std::time::Duration;

use exchange_common::error::{ExchangeDbError, Result};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::config::{ReplicationConfig, ReplicationRole};

/// Manages failover operations: promoting replicas to primary and demoting
/// primaries to replica.
pub struct FailoverManager {
    config: ReplicationConfig,
    /// Interval between health checks when this node is a replica monitoring
    /// its primary.
    health_check_interval: Duration,
}

impl FailoverManager {
    /// Create a new failover manager.
    pub fn new(config: ReplicationConfig, health_check_interval: Duration) -> Self {
        Self {
            config,
            health_check_interval,
        }
    }

    /// Promote this replica to primary.
    ///
    /// This changes the node's role from `Replica` to `Primary`, clears the
    /// `primary_addr`, and prepares the node to accept writes and ship WAL
    /// segments to other replicas.
    pub fn promote_to_primary(&mut self) -> Result<()> {
        if self.config.role == ReplicationRole::Primary {
            return Err(ExchangeDbError::Wal(
                "node is already a primary".into(),
            ));
        }

        tracing::info!(
            previous_role = ?self.config.role,
            "promoting node to primary"
        );

        self.config.role = ReplicationRole::Primary;
        self.config.primary_addr = None;

        Ok(())
    }

    /// Demote this primary to a replica of the given new primary.
    ///
    /// Changes the node's role from `Primary` to `Replica`, sets the
    /// `primary_addr` to the new primary, and clears the replica list
    /// (since this node is no longer responsible for shipping).
    pub fn demote_to_replica(&mut self, new_primary: &str) -> Result<()> {
        if self.config.role == ReplicationRole::Replica {
            return Err(ExchangeDbError::Wal(
                "node is already a replica".into(),
            ));
        }

        tracing::info!(
            new_primary = %new_primary,
            "demoting node to replica"
        );

        self.config.role = ReplicationRole::Replica;
        self.config.primary_addr = Some(new_primary.to_string());
        self.config.replica_addrs.clear();

        Ok(())
    }

    /// Check whether the primary is reachable by attempting a TCP connection.
    ///
    /// Returns `true` if the connection succeeds within the health check
    /// interval, `false` otherwise.
    pub async fn check_primary_health(&self) -> bool {
        let addr = match &self.config.primary_addr {
            Some(addr) => addr.clone(),
            None => return false,
        };

        match timeout(
            self.health_check_interval,
            TcpStream::connect(&addr),
        )
        .await
        {
            Ok(Ok(_stream)) => true,
            Ok(Err(_)) => false,
            Err(_) => false, // Timeout
        }
    }

    /// Get the current role of this node.
    pub fn current_role(&self) -> &ReplicationRole {
        &self.config.role
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    /// Get the health check interval.
    pub fn health_check_interval(&self) -> Duration {
        self.health_check_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::config::ReplicationSyncMode;

    fn replica_config(primary: &str) -> ReplicationConfig {
        ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: Some(primary.to_string()),
            replica_addrs: Vec::new(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        }
    }

    fn primary_config(replicas: Vec<&str>) -> ReplicationConfig {
        ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            replica_addrs: replicas.into_iter().map(String::from).collect(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        }
    }

    #[test]
    fn promote_replica_to_primary() {
        let mut mgr = FailoverManager::new(
            replica_config("10.0.0.1:9100"),
            Duration::from_secs(5),
        );

        assert_eq!(*mgr.current_role(), ReplicationRole::Replica);

        mgr.promote_to_primary().unwrap();

        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);
        assert!(mgr.config().primary_addr.is_none());
    }

    #[test]
    fn promote_primary_fails() {
        let mut mgr = FailoverManager::new(
            primary_config(vec!["10.0.0.2:9100"]),
            Duration::from_secs(5),
        );

        let result = mgr.promote_to_primary();
        assert!(result.is_err());
    }

    #[test]
    fn demote_primary_to_replica() {
        let mut mgr = FailoverManager::new(
            primary_config(vec!["10.0.0.2:9100", "10.0.0.3:9100"]),
            Duration::from_secs(5),
        );

        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);

        mgr.demote_to_replica("10.0.0.2:9100").unwrap();

        assert_eq!(*mgr.current_role(), ReplicationRole::Replica);
        assert_eq!(
            mgr.config().primary_addr.as_deref(),
            Some("10.0.0.2:9100")
        );
        assert!(mgr.config().replica_addrs.is_empty());
    }

    #[test]
    fn demote_replica_fails() {
        let mut mgr = FailoverManager::new(
            replica_config("10.0.0.1:9100"),
            Duration::from_secs(5),
        );

        let result = mgr.demote_to_replica("10.0.0.5:9100");
        assert!(result.is_err());
    }

    #[test]
    fn promote_then_demote_roundtrip() {
        let mut mgr = FailoverManager::new(
            replica_config("10.0.0.1:9100"),
            Duration::from_secs(5),
        );

        // Promote.
        mgr.promote_to_primary().unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);

        // Demote back.
        mgr.demote_to_replica("10.0.0.99:9100").unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Replica);
        assert_eq!(
            mgr.config().primary_addr.as_deref(),
            Some("10.0.0.99:9100")
        );
    }

    #[test]
    fn promote_standalone_to_primary() {
        let config = ReplicationConfig {
            role: ReplicationRole::Standalone,
            primary_addr: None,
            replica_addrs: Vec::new(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        let mut mgr = FailoverManager::new(config, Duration::from_secs(5));

        mgr.promote_to_primary().unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);
    }

    #[tokio::test]
    async fn health_check_no_primary_addr() {
        let config = ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: None,
            replica_addrs: Vec::new(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        let mgr = FailoverManager::new(config, Duration::from_secs(1));

        // No primary_addr set, should return false.
        assert!(!mgr.check_primary_health().await);
    }

    #[tokio::test]
    async fn health_check_unreachable_primary() {
        let mgr = FailoverManager::new(
            // Use an address that is very unlikely to be listening.
            replica_config("127.0.0.1:19999"),
            Duration::from_millis(100),
        );

        assert!(!mgr.check_primary_health().await);
    }

    #[tokio::test]
    async fn health_check_reachable_primary() {
        // Start a TCP listener to simulate a reachable primary.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let mgr = FailoverManager::new(
            replica_config(&addr.to_string()),
            Duration::from_secs(2),
        );

        assert!(mgr.check_primary_health().await);
    }
}
