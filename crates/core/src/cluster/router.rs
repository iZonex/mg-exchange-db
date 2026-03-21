//! Query routing for distributed queries across cluster nodes.

use std::sync::Arc;

use exchange_common::error::{ExchangeDbError, Result};

use super::ClusterManager;
#[cfg(test)]
use super::node::ClusterNode;
use super::node::NodeRole;

/// Routes queries to appropriate cluster nodes.
pub struct QueryRouter {
    cluster: Arc<ClusterManager>,
}

impl QueryRouter {
    /// Create a new router backed by the given cluster manager.
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self { cluster }
    }

    /// Route a read query: pick the least-loaded healthy replica (or primary)
    /// that serves the given table.
    pub fn route_read(&self, table: &str) -> Result<String> {
        let node = self.cluster.route_query(table).ok_or_else(|| {
            ExchangeDbError::Query(format!(
                "no healthy node available for reading table '{table}'"
            ))
        })?;
        Ok(node.addr.clone())
    }

    /// Route a write query: find the primary node that serves the given table.
    pub fn route_write(&self, table: &str) -> Result<String> {
        let nodes = self.cluster.healthy_nodes();
        let primary = nodes
            .into_iter()
            .find(|n| n.role == NodeRole::Primary && n.tables.contains(&table.to_string()))
            .ok_or_else(|| {
                ExchangeDbError::Query(format!(
                    "no primary node available for writing table '{table}'"
                ))
            })?;
        Ok(primary.addr.clone())
    }

    /// Plan a distributed query that fans out to all nodes serving the table,
    /// then merges results according to the given strategy.
    pub fn plan_distributed(&self, table: &str, merge_strategy: MergeStrategy) -> DistributedPlan {
        let nodes = self.cluster.healthy_nodes();

        let node_plans: Vec<NodePlan> = nodes
            .into_iter()
            .filter(|n| n.can_read() && n.tables.contains(&table.to_string()))
            .map(|n| NodePlan {
                node_addr: n.addr.clone(),
                partitions: n.tables.clone(), // In a real system these would be partition assignments
            })
            .collect();

        DistributedPlan {
            node_plans,
            merge_strategy,
        }
    }
}

/// A plan describing how a query is distributed across cluster nodes.
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// Sub-plans to execute on each node.
    pub node_plans: Vec<NodePlan>,
    /// How to merge results from the nodes.
    pub merge_strategy: MergeStrategy,
}

/// A sub-plan for a single node.
#[derive(Debug, Clone)]
pub struct NodePlan {
    /// Network address of the target node.
    pub node_addr: String,
    /// Which partitions this node handles.
    pub partitions: Vec<String>,
}

/// Strategy for merging results from multiple nodes.
#[derive(Debug, Clone)]
pub enum MergeStrategy {
    /// UNION-like: just append results from all nodes.
    Concatenate,
    /// ORDER BY: merge pre-sorted streams from nodes.
    MergeSort { key: String, desc: bool },
    /// Combine partial aggregates (SUM, COUNT, AVG, etc.).
    ReduceAggregate,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterConfig;
    use crate::cluster::node::{ClusterNode, NodeRole, NodeStatus};

    fn make_cluster(nodes_spec: Vec<(&str, &str, NodeRole, Vec<&str>)>) -> Arc<ClusterManager> {
        let config = ClusterConfig {
            node_id: "coordinator".into(),
            node_addr: "127.0.0.1:9000".into(),
            seed_nodes: vec![],
            role: NodeRole::Coordinator,
        };
        let mgr = ClusterManager::new(config);

        for (id, addr, role, tables) in nodes_spec {
            let mut node = ClusterNode::new(id.into(), addr.into(), role);
            node.tables = tables.into_iter().map(|s| s.to_string()).collect();
            mgr.add_node(node);
        }

        Arc::new(mgr)
    }

    #[test]
    fn route_read_picks_least_loaded() {
        let cluster = make_cluster(vec![
            ("p1", "10.0.0.1:9000", NodeRole::Primary, vec!["trades"]),
            ("r1", "10.0.0.2:9000", NodeRole::ReadReplica, vec!["trades"]),
        ]);

        // Update load: primary has more queries
        {
            let mut nodes = cluster.nodes.write().unwrap();
            nodes[0].load.active_queries = 10;
            nodes[1].load.active_queries = 2;
        }

        let router = QueryRouter::new(cluster);
        let addr = router.route_read("trades").unwrap();
        // Should pick the replica since it has lower load
        assert_eq!(addr, "10.0.0.2:9000");
    }

    #[test]
    fn route_write_goes_to_primary() {
        let cluster = make_cluster(vec![
            ("p1", "10.0.0.1:9000", NodeRole::Primary, vec!["trades"]),
            ("r1", "10.0.0.2:9000", NodeRole::ReadReplica, vec!["trades"]),
        ]);

        let router = QueryRouter::new(cluster);
        let addr = router.route_write("trades").unwrap();
        assert_eq!(addr, "10.0.0.1:9000");
    }

    #[test]
    fn route_write_fails_without_primary() {
        let cluster = make_cluster(vec![(
            "r1",
            "10.0.0.2:9000",
            NodeRole::ReadReplica,
            vec!["trades"],
        )]);

        let router = QueryRouter::new(cluster);
        assert!(router.route_write("trades").is_err());
    }

    #[test]
    fn route_read_fails_for_unknown_table() {
        let cluster = make_cluster(vec![(
            "p1",
            "10.0.0.1:9000",
            NodeRole::Primary,
            vec!["trades"],
        )]);

        let router = QueryRouter::new(cluster);
        assert!(router.route_read("unknown_table").is_err());
    }

    #[test]
    fn distributed_plan_fans_out() {
        let cluster = make_cluster(vec![
            ("p1", "10.0.0.1:9000", NodeRole::Primary, vec!["trades"]),
            ("r1", "10.0.0.2:9000", NodeRole::ReadReplica, vec!["trades"]),
            ("r2", "10.0.0.3:9000", NodeRole::ReadReplica, vec!["quotes"]),
        ]);

        let router = QueryRouter::new(cluster);
        let plan = router.plan_distributed("trades", MergeStrategy::Concatenate);
        // Only nodes serving "trades" and able to read: p1 and r1
        assert_eq!(plan.node_plans.len(), 2);
    }

    #[test]
    fn distributed_plan_skips_offline_nodes() {
        let cluster = make_cluster(vec![
            ("p1", "10.0.0.1:9000", NodeRole::Primary, vec!["trades"]),
            ("r1", "10.0.0.2:9000", NodeRole::ReadReplica, vec!["trades"]),
        ]);

        // Mark r1 as offline
        {
            let mut nodes = cluster.nodes.write().unwrap();
            nodes[1].status = NodeStatus::Offline;
        }

        let router = QueryRouter::new(cluster);
        let plan = router.plan_distributed("trades", MergeStrategy::ReduceAggregate);
        assert_eq!(plan.node_plans.len(), 1);
        assert_eq!(plan.node_plans[0].node_addr, "10.0.0.1:9000");
    }
}
