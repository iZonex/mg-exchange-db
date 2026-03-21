//! Cluster node types and status tracking.

use std::time::Duration;

/// Role a node plays in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
    /// Accepts writes, serves reads.
    Primary,
    /// Serves reads only.
    ReadReplica,
    /// Routes queries, does not store data.
    Coordinator,
}

/// Current status of a cluster node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Online,
    Offline,
    Syncing,
    Draining,
}

/// Load metrics for a cluster node.
#[derive(Debug, Clone)]
pub struct NodeLoad {
    pub active_queries: u32,
    pub active_writers: u32,
    pub cpu_percent: f32,
    pub memory_used_bytes: u64,
    pub disk_used_bytes: u64,
}

impl Default for NodeLoad {
    fn default() -> Self {
        Self {
            active_queries: 0,
            active_writers: 0,
            cpu_percent: 0.0,
            memory_used_bytes: 0,
            disk_used_bytes: 0,
        }
    }
}

/// Represents a single node in the cluster.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub addr: String,
    pub role: NodeRole,
    pub status: NodeStatus,
    /// Unix timestamp (seconds) of last heartbeat received.
    pub last_heartbeat: i64,
    /// Tables this node serves.
    pub tables: Vec<String>,
    /// Current load metrics.
    pub load: NodeLoad,
}

impl ClusterNode {
    /// Create a new node descriptor.
    pub fn new(id: String, addr: String, role: NodeRole) -> Self {
        Self {
            id,
            addr,
            role,
            status: NodeStatus::Online,
            last_heartbeat: current_timestamp_secs(),
            tables: Vec::new(),
            load: NodeLoad::default(),
        }
    }

    /// Whether the node is healthy enough to serve traffic.
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Online | NodeStatus::Syncing)
    }

    /// Whether this node can accept writes.
    pub fn can_write(&self) -> bool {
        self.role == NodeRole::Primary && self.status == NodeStatus::Online
    }

    /// Whether this node can serve reads.
    pub fn can_read(&self) -> bool {
        self.is_healthy() && self.role != NodeRole::Coordinator
    }

    /// Update heartbeat to current time.
    pub fn touch(&mut self) {
        self.last_heartbeat = current_timestamp_secs();
    }

    /// Check if the node has exceeded the heartbeat timeout.
    pub fn is_expired(&self, timeout: Duration) -> bool {
        let now = current_timestamp_secs();
        let elapsed = (now - self.last_heartbeat).max(0) as u64;
        elapsed > timeout.as_secs()
    }
}

/// Returns the current wall-clock time as seconds since Unix epoch.
fn current_timestamp_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_node_is_online() {
        let node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
        assert_eq!(node.status, NodeStatus::Online);
        assert!(node.is_healthy());
        assert!(node.can_write());
        assert!(node.can_read());
    }

    #[test]
    fn replica_cannot_write() {
        let node = ClusterNode::new("n2".into(), "127.0.0.1:9001".into(), NodeRole::ReadReplica);
        assert!(!node.can_write());
        assert!(node.can_read());
    }

    #[test]
    fn coordinator_cannot_read() {
        let node = ClusterNode::new("n3".into(), "127.0.0.1:9002".into(), NodeRole::Coordinator);
        assert!(!node.can_write());
        assert!(!node.can_read());
    }

    #[test]
    fn offline_node_is_unhealthy() {
        let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
        node.status = NodeStatus::Offline;
        assert!(!node.is_healthy());
        assert!(!node.can_write());
        assert!(!node.can_read());
    }

    #[test]
    fn heartbeat_expiry() {
        let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
        // Set heartbeat far in the past
        node.last_heartbeat = current_timestamp_secs() - 120;
        assert!(node.is_expired(Duration::from_secs(60)));
        assert!(!node.is_expired(Duration::from_secs(300)));
    }

    #[test]
    fn touch_resets_heartbeat() {
        let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
        node.last_heartbeat = 0; // ancient
        assert!(node.is_expired(Duration::from_secs(10)));
        node.touch();
        assert!(!node.is_expired(Duration::from_secs(10)));
    }
}
