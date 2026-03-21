use serde::{Deserialize, Serialize};

/// Role of this node in the replication topology.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationRole {
    /// This node accepts writes and ships WAL segments to replicas.
    Primary,
    /// This node receives WAL segments from a primary and applies them locally.
    Replica,
    /// This node operates independently with no replication.
    Standalone,
}

/// How many replicas must acknowledge a WAL segment before the primary
/// considers the write durable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationSyncMode {
    /// Fire-and-forget: the primary does not wait for any replica acknowledgment.
    Async,
    /// The primary waits for at least one replica to acknowledge before returning.
    SemiSync,
    /// The primary waits for all replicas to acknowledge before returning.
    Sync,
}

/// Configuration for replication on this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Role of this node.
    pub role: ReplicationRole,
    /// For replicas: the primary's address (e.g. "10.0.0.1:9100").
    pub primary_addr: Option<String>,
    /// For primaries: list of replica addresses.
    pub replica_addrs: Vec<String>,
    /// Synchronization mode for writes.
    pub sync_mode: ReplicationSyncMode,
    /// Maximum replication lag in bytes before the primary applies backpressure.
    pub max_lag_bytes: u64,
    /// Whether automatic failover is enabled (replica only).
    pub failover_enabled: bool,
    /// Interval between health checks when monitoring the primary.
    pub health_check_interval: std::time::Duration,
    /// Number of consecutive health check failures before triggering failover.
    pub failure_threshold: u32,
    /// TCP port for the replication listener (default: 19100).
    pub replication_port: u16,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Standalone,
            primary_addr: None,
            replica_addrs: Vec::new(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024, // 256 MB
            failover_enabled: false,
            health_check_interval: std::time::Duration::from_secs(2),
            failure_threshold: 3,
            replication_port: 19100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_standalone() {
        let config = ReplicationConfig::default();
        assert_eq!(config.role, ReplicationRole::Standalone);
        assert!(config.primary_addr.is_none());
        assert!(config.replica_addrs.is_empty());
        assert_eq!(config.sync_mode, ReplicationSyncMode::Async);
    }

    #[test]
    fn config_serialization_roundtrip() {
        let config = ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            replica_addrs: vec!["10.0.0.2:9100".into(), "10.0.0.3:9100".into()],
            sync_mode: ReplicationSyncMode::SemiSync,
            max_lag_bytes: 128 * 1024 * 1024,
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let restored: ReplicationConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.role, ReplicationRole::Primary);
        assert_eq!(restored.replica_addrs.len(), 2);
        assert_eq!(restored.sync_mode, ReplicationSyncMode::SemiSync);
    }
}
