//! Cluster management for horizontal scaling.
//!
//! Provides node discovery, heartbeat-based failure detection, and
//! query routing across a multi-node ExchangeDB deployment.

pub mod node;
pub mod router;

use std::sync::RwLock;
use std::time::Duration;

use exchange_common::error::{ExchangeDbError, Result};

use node::{ClusterNode, NodeRole, NodeStatus};

/// Configuration for joining or forming a cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Unique identifier for this node.
    pub node_id: String,
    /// Network address this node listens on.
    pub node_addr: String,
    /// Seed nodes used for initial cluster discovery.
    pub seed_nodes: Vec<String>,
    /// Role this node plays.
    pub role: NodeRole,
}

/// Manages cluster membership, heartbeats, and node health.
pub struct ClusterManager {
    config: ClusterConfig,
    /// All known nodes (including self).
    // Exposed as pub(crate) so the router tests can manipulate load directly.
    pub(crate) nodes: RwLock<Vec<ClusterNode>>,
}

impl ClusterManager {
    /// Create a new cluster manager with the given configuration.
    /// The local node is **not** automatically registered; call [`register`]
    /// to add it.
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            config,
            nodes: RwLock::new(Vec::new()),
        }
    }

    /// Register this node with the cluster by adding it to the node list.
    pub fn register(&self) -> Result<()> {
        let mut nodes = self.nodes.write().map_err(|e| {
            ExchangeDbError::LockContention(format!("cluster nodes lock poisoned: {e}"))
        })?;

        // Avoid duplicate registration.
        if nodes.iter().any(|n| n.id == self.config.node_id) {
            return Ok(());
        }

        let node = ClusterNode::new(
            self.config.node_id.clone(),
            self.config.node_addr.clone(),
            self.config.role.clone(),
        );
        nodes.push(node);
        Ok(())
    }

    /// Add an externally-discovered node to the cluster.
    pub fn add_node(&self, node: ClusterNode) {
        let mut nodes = self.nodes.write().unwrap();
        // Replace if the node id already exists.
        if let Some(existing) = nodes.iter_mut().find(|n| n.id == node.id) {
            *existing = node;
        } else {
            nodes.push(node);
        }
    }

    /// Send a heartbeat: updates this node's last_heartbeat timestamp.
    pub fn heartbeat(&self) -> Result<()> {
        let mut nodes = self.nodes.write().map_err(|e| {
            ExchangeDbError::LockContention(format!("cluster nodes lock poisoned: {e}"))
        })?;

        if let Some(me) = nodes.iter_mut().find(|n| n.id == self.config.node_id) {
            me.touch();
        }

        Ok(())
    }

    /// Return snapshots of all healthy nodes.
    pub fn healthy_nodes(&self) -> Vec<ClusterNode> {
        let nodes = self.nodes.read().unwrap();
        nodes.iter().filter(|n| n.is_healthy()).cloned().collect()
    }

    /// Route a query to the best (least-loaded) healthy node serving the
    /// given table.
    pub fn route_query(&self, table: &str) -> Option<ClusterNode> {
        let nodes = self.nodes.read().unwrap();
        nodes
            .iter()
            .filter(|n| n.can_read() && n.tables.contains(&table.to_string()))
            .min_by_key(|n| n.load.active_queries)
            .cloned()
    }

    /// Detect dead nodes whose heartbeat has exceeded `timeout` and mark
    /// them offline. Returns the IDs of newly-detected dead nodes.
    pub fn detect_failures(&self, timeout: Duration) -> Vec<String> {
        let mut nodes = self.nodes.write().unwrap();
        let mut dead = Vec::new();

        for node in nodes.iter_mut() {
            if node.is_expired(timeout) && node.status != NodeStatus::Offline {
                node.status = NodeStatus::Offline;
                dead.push(node.id.clone());
            }
        }

        dead
    }

    /// Remove a node from the cluster by id.
    pub fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.retain(|n| n.id != node_id);
    }

    /// Get the number of registered nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.read().unwrap().len()
    }

    /// Get configuration.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Add a new node to the cluster at runtime (no restart needed).
    ///
    /// The node is added with `Syncing` status and will become `Online`
    /// once it confirms it has caught up.
    pub fn add_node_dynamic(&self, mut node: ClusterNode) -> Result<()> {
        let mut nodes = self.nodes.write().map_err(|e| {
            ExchangeDbError::LockContention(format!("cluster nodes lock poisoned: {e}"))
        })?;

        if nodes.iter().any(|n| n.id == node.id) {
            return Err(ExchangeDbError::Corruption(format!(
                "node '{}' already exists in cluster",
                node.id
            )));
        }

        node.status = NodeStatus::Syncing;
        nodes.push(node);
        Ok(())
    }

    /// Remove a node from the cluster at runtime.
    ///
    /// Marks the node as `Draining` first, then removes it.
    /// Returns an error if the node does not exist.
    pub fn remove_node_dynamic(&self, node_id: &str) -> Result<()> {
        let mut nodes = self.nodes.write().map_err(|e| {
            ExchangeDbError::LockContention(format!("cluster nodes lock poisoned: {e}"))
        })?;

        let idx = nodes
            .iter()
            .position(|n| n.id == node_id)
            .ok_or_else(|| {
                ExchangeDbError::TableNotFound(format!("node '{node_id}' not in cluster"))
            })?;

        nodes.remove(idx);
        Ok(())
    }

    /// Generate a rebalance plan that redistributes tables evenly across
    /// healthy, non-coordinator nodes.
    pub fn rebalance(&self) -> Result<RebalancePlan> {
        let nodes = self.nodes.read().map_err(|e| {
            ExchangeDbError::LockContention(format!("cluster nodes lock poisoned: {e}"))
        })?;

        // Collect data nodes (non-coordinator, healthy).
        let data_nodes: Vec<&ClusterNode> = nodes
            .iter()
            .filter(|n| n.is_healthy() && n.role != NodeRole::Coordinator)
            .collect();

        if data_nodes.is_empty() {
            return Ok(RebalancePlan { moves: Vec::new() });
        }

        // Collect all tables and their current owners.
        let mut all_tables: Vec<(String, String)> = Vec::new(); // (table, node_id)
        for node in &data_nodes {
            for table in &node.tables {
                all_tables.push((table.clone(), node.id.clone()));
            }
        }

        all_tables.sort_by(|a, b| a.0.cmp(&b.0));

        // Calculate ideal distribution.
        let total = all_tables.len();
        let n = data_nodes.len();
        let base = total / n;
        let remainder = total % n;

        // Assign tables round-robin to nodes.
        let mut moves = Vec::new();
        let mut idx = 0;
        for (i, node) in data_nodes.iter().enumerate() {
            let count = base + if i < remainder { 1 } else { 0 };
            for j in 0..count {
                if idx + j < all_tables.len() {
                    let (ref table, ref from) = all_tables[idx + j];
                    if from != &node.id {
                        moves.push(TableMove {
                            table: table.clone(),
                            from_node: from.clone(),
                            to_node: node.id.clone(),
                        });
                    }
                }
            }
            idx += count;
        }

        Ok(RebalancePlan { moves })
    }
}

/// Plan describing how to redistribute tables across cluster nodes.
#[derive(Debug, Clone)]
pub struct RebalancePlan {
    pub moves: Vec<TableMove>,
}

/// A single table move from one node to another.
#[derive(Debug, Clone)]
pub struct TableMove {
    pub table: String,
    pub from_node: String,
    pub to_node: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(id: &str) -> ClusterConfig {
        ClusterConfig {
            node_id: id.into(),
            node_addr: format!("127.0.0.1:900{}", &id[1..]),
            seed_nodes: vec![],
            role: NodeRole::Primary,
        }
    }

    #[test]
    fn register_adds_self() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);

        let healthy = mgr.healthy_nodes();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, "n1");
    }

    #[test]
    fn register_is_idempotent() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn add_node_and_discover() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let replica = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node(replica);

        assert_eq!(mgr.node_count(), 2);
        let healthy = mgr.healthy_nodes();
        assert_eq!(healthy.len(), 2);
    }

    #[test]
    fn heartbeat_updates_timestamp() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        // Artificially age the heartbeat
        {
            let mut nodes = mgr.nodes.write().unwrap();
            nodes[0].last_heartbeat = 0;
        }

        mgr.heartbeat().unwrap();

        let nodes = mgr.nodes.read().unwrap();
        assert!(nodes[0].last_heartbeat > 0);
    }

    #[test]
    fn detect_failures_marks_dead_nodes() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let mut stale = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        stale.last_heartbeat = 0; // epoch = very old
        mgr.add_node(stale);

        let dead = mgr.detect_failures(Duration::from_secs(30));
        assert_eq!(dead, vec!["n2".to_string()]);

        // n2 should now be offline
        let nodes = mgr.nodes.read().unwrap();
        let n2 = nodes.iter().find(|n| n.id == "n2").unwrap();
        assert_eq!(n2.status, NodeStatus::Offline);
    }

    #[test]
    fn detect_failures_ignores_healthy_nodes() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let dead = mgr.detect_failures(Duration::from_secs(30));
        assert!(dead.is_empty());
    }

    #[test]
    fn route_query_picks_least_loaded() {
        let mgr = ClusterManager::new(test_config("n1"));

        let mut p = ClusterNode::new("p1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
        p.tables = vec!["trades".into()];
        p.load.active_queries = 10;
        mgr.add_node(p);

        let mut r = ClusterNode::new("r1".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
        r.tables = vec!["trades".into()];
        r.load.active_queries = 2;
        mgr.add_node(r);

        let best = mgr.route_query("trades").unwrap();
        assert_eq!(best.addr, "10.0.0.2:9000");
    }

    #[test]
    fn route_query_returns_none_for_unknown_table() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        assert!(mgr.route_query("nonexistent").is_none());
    }

    #[test]
    fn remove_node_works() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let node = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node(node);
        assert_eq!(mgr.node_count(), 2);

        mgr.remove_node("n2");
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn add_node_dynamic_starts_syncing() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let node = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node_dynamic(node).unwrap();

        let nodes = mgr.nodes.read().unwrap();
        let n2 = nodes.iter().find(|n| n.id == "n2").unwrap();
        assert_eq!(n2.status, NodeStatus::Syncing);
    }

    #[test]
    fn add_node_dynamic_duplicate_fails() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let node = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node_dynamic(node.clone()).unwrap();
        assert!(mgr.add_node_dynamic(node).is_err());
    }

    #[test]
    fn remove_node_dynamic_works() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();

        let node = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node_dynamic(node).unwrap();
        assert_eq!(mgr.node_count(), 2);

        mgr.remove_node_dynamic("n2").unwrap();
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn remove_node_dynamic_nonexistent_fails() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        assert!(mgr.remove_node_dynamic("n99").is_err());
    }

    #[test]
    fn rebalance_generates_moves() {
        let mgr = ClusterManager::new(test_config("n1"));

        // Node 1 has 3 tables, node 2 has none.
        let mut n1 = ClusterNode::new("n1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
        n1.tables = vec!["t1".into(), "t2".into(), "t3".into(), "t4".into()];
        mgr.add_node(n1);

        let n2 = ClusterNode::new("n2".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node(n2);

        let plan = mgr.rebalance().unwrap();
        // With 4 tables and 2 nodes, each should get 2.
        // So 2 tables should move from n1 to n2.
        assert_eq!(plan.moves.len(), 2);
        for m in &plan.moves {
            assert_eq!(m.to_node, "n2");
            assert_eq!(m.from_node, "n1");
        }
    }

    #[test]
    fn rebalance_no_moves_when_balanced() {
        let mgr = ClusterManager::new(test_config("n1"));

        let mut n1 = ClusterNode::new("n1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
        n1.tables = vec!["t1".into()];
        mgr.add_node(n1);

        let mut n2 = ClusterNode::new("n2".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
        n2.tables = vec!["t2".into()];
        mgr.add_node(n2);

        let plan = mgr.rebalance().unwrap();
        assert!(plan.moves.is_empty());
    }

    #[test]
    fn rebalance_empty_cluster() {
        let mgr = ClusterManager::new(test_config("n1"));
        let plan = mgr.rebalance().unwrap();
        assert!(plan.moves.is_empty());
    }
}
