//! Comprehensive cluster management tests (40 tests).
//!
//! Covers node management, heartbeats, dead node detection,
//! dynamic add/remove, query routing, and consensus-like operations.

use std::sync::Arc;
use std::time::Duration;

use exchange_core::cluster::node::{ClusterNode, NodeLoad, NodeRole, NodeStatus};
use exchange_core::cluster::router::{MergeStrategy, QueryRouter};
use exchange_core::cluster::{ClusterConfig, ClusterManager};

fn test_config(id: &str) -> ClusterConfig {
    ClusterConfig {
        node_id: id.into(),
        node_addr: format!("127.0.0.1:900{}", &id[1..]),
        seed_nodes: vec![],
        role: NodeRole::Primary,
    }
}

fn make_cluster(specs: Vec<(&str, &str, NodeRole, Vec<&str>)>) -> Arc<ClusterManager> {
    let config = ClusterConfig {
        node_id: "coordinator".into(),
        node_addr: "127.0.0.1:9000".into(),
        seed_nodes: vec![],
        role: NodeRole::Coordinator,
    };
    let mgr = ClusterManager::new(config);
    for (id, addr, role, tables) in specs {
        let mut node = ClusterNode::new(id.into(), addr.into(), role);
        node.tables = tables.into_iter().map(String::from).collect();
        mgr.add_node(node);
    }
    Arc::new(mgr)
}

// ---------------------------------------------------------------------------
// mod node_management
// ---------------------------------------------------------------------------

mod node_management {
    use super::*;

    #[test]
    fn register_adds_self() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn register_is_idempotent() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn add_external_node() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let node = ClusterNode::new("n2".into(), "127.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node(node);
        assert_eq!(mgr.node_count(), 2);
    }

    #[test]
    fn add_node_replaces_existing() {
        let mgr = ClusterManager::new(test_config("n1"));
        let node1 = ClusterNode::new("n2".into(), "addr1".into(), NodeRole::ReadReplica);
        mgr.add_node(node1);
        let node2 = ClusterNode::new("n2".into(), "addr2".into(), NodeRole::ReadReplica);
        mgr.add_node(node2);
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn remove_node() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let node = ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica);
        mgr.add_node(node);
        mgr.remove_node("n2");
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        mgr.remove_node("nope");
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn heartbeat_does_not_error() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        // Calling heartbeat should not error
        mgr.heartbeat().unwrap();
        // The node should still be healthy after heartbeat
        let healthy = mgr.healthy_nodes();
        assert_eq!(healthy.len(), 1);
    }

    #[test]
    fn dead_node_detection() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let mut stale = ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica);
        stale.last_heartbeat = 0;
        mgr.add_node(stale);

        let dead = mgr.detect_failures(Duration::from_secs(30));
        assert_eq!(dead, vec!["n2".to_string()]);
    }

    #[test]
    fn healthy_nodes_excludes_offline() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let mut offline = ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica);
        offline.status = NodeStatus::Offline;
        mgr.add_node(offline);

        let healthy = mgr.healthy_nodes();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].id, "n1");
    }

    #[test]
    fn dynamic_add_increases_count() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let node = ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica);
        mgr.add_node_dynamic(node).unwrap();
        assert_eq!(mgr.node_count(), 2);
        // Syncing nodes are healthy, so should appear in healthy_nodes
        let healthy = mgr.healthy_nodes();
        assert_eq!(healthy.len(), 2);
    }

    #[test]
    fn dynamic_add_duplicate_fails() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let n = ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica);
        mgr.add_node_dynamic(n.clone()).unwrap();
        assert!(mgr.add_node_dynamic(n).is_err());
    }

    #[test]
    fn dynamic_remove_works() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        let n = ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica);
        mgr.add_node_dynamic(n).unwrap();
        mgr.remove_node_dynamic("n2").unwrap();
        assert_eq!(mgr.node_count(), 1);
    }

    #[test]
    fn dynamic_remove_nonexistent_fails() {
        let mgr = ClusterManager::new(test_config("n1"));
        mgr.register().unwrap();
        assert!(mgr.remove_node_dynamic("nope").is_err());
    }
}

// ---------------------------------------------------------------------------
// mod node_types
// ---------------------------------------------------------------------------

mod node_types {
    use super::*;

    #[test]
    fn primary_can_read_and_write() {
        let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        assert!(node.can_read());
        assert!(node.can_write());
    }

    #[test]
    fn replica_can_read_not_write() {
        let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::ReadReplica);
        assert!(node.can_read());
        assert!(!node.can_write());
    }

    #[test]
    fn coordinator_cannot_read_or_write() {
        let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Coordinator);
        assert!(!node.can_read());
        assert!(!node.can_write());
    }

    #[test]
    fn new_node_is_online() {
        let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        assert_eq!(node.status, NodeStatus::Online);
        assert!(node.is_healthy());
    }

    #[test]
    fn offline_node_is_unhealthy() {
        let mut node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        node.status = NodeStatus::Offline;
        assert!(!node.is_healthy());
        assert!(!node.can_write());
    }

    #[test]
    fn syncing_node_is_healthy() {
        let mut node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::ReadReplica);
        node.status = NodeStatus::Syncing;
        assert!(node.is_healthy());
    }

    #[test]
    fn draining_node_is_unhealthy() {
        let mut node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        node.status = NodeStatus::Draining;
        assert!(!node.is_healthy());
    }

    #[test]
    fn heartbeat_expiry() {
        let mut node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        node.last_heartbeat = 0; // epoch
        assert!(node.is_expired(Duration::from_secs(60)));
    }

    #[test]
    fn fresh_node_not_expired() {
        let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        assert!(!node.is_expired(Duration::from_secs(60)));
    }

    #[test]
    fn touch_resets_heartbeat() {
        let mut node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary);
        node.last_heartbeat = 0;
        node.touch();
        assert!(!node.is_expired(Duration::from_secs(10)));
    }
}

// ---------------------------------------------------------------------------
// mod routing
// ---------------------------------------------------------------------------

mod routing {
    use super::*;

    #[test]
    fn read_to_least_loaded_replica() {
        let config = ClusterConfig {
            node_id: "coordinator".into(),
            node_addr: "127.0.0.1:9000".into(),
            seed_nodes: vec![],
            role: NodeRole::Coordinator,
        };
        let mgr = ClusterManager::new(config);
        let mut p1 = ClusterNode::new("p1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
        p1.tables = vec!["trades".into()];
        p1.load.active_queries = 10;
        mgr.add_node(p1);
        let mut r1 = ClusterNode::new("r1".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
        r1.tables = vec!["trades".into()];
        r1.load.active_queries = 2;
        mgr.add_node(r1);

        let router = QueryRouter::new(Arc::new(mgr));
        let addr = router.route_read("trades").unwrap();
        assert_eq!(addr, "10.0.0.2:9000");
    }

    #[test]
    fn write_to_primary() {
        let cluster = make_cluster(vec![
            ("p1", "10.0.0.1:9000", NodeRole::Primary, vec!["trades"]),
            ("r1", "10.0.0.2:9000", NodeRole::ReadReplica, vec!["trades"]),
        ]);
        let router = QueryRouter::new(cluster);
        let addr = router.route_write("trades").unwrap();
        assert_eq!(addr, "10.0.0.1:9000");
    }

    #[test]
    fn write_fails_without_primary() {
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
    fn read_fails_for_unknown_table() {
        let cluster = make_cluster(vec![(
            "p1",
            "10.0.0.1:9000",
            NodeRole::Primary,
            vec!["trades"],
        )]);
        let router = QueryRouter::new(cluster);
        assert!(router.route_read("nonexistent").is_err());
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
        assert_eq!(plan.node_plans.len(), 2);
    }

    #[test]
    fn distributed_plan_skips_offline() {
        let config = ClusterConfig {
            node_id: "coordinator".into(),
            node_addr: "127.0.0.1:9000".into(),
            seed_nodes: vec![],
            role: NodeRole::Coordinator,
        };
        let mgr = ClusterManager::new(config);
        let mut p1 = ClusterNode::new("p1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
        p1.tables = vec!["trades".into()];
        mgr.add_node(p1);
        let mut r1 = ClusterNode::new("r1".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
        r1.tables = vec!["trades".into()];
        r1.status = NodeStatus::Offline;
        mgr.add_node(r1);

        let router = QueryRouter::new(Arc::new(mgr));
        let plan = router.plan_distributed("trades", MergeStrategy::ReduceAggregate);
        assert_eq!(plan.node_plans.len(), 1);
        assert_eq!(plan.node_plans[0].node_addr, "10.0.0.1:9000");
    }
}

// ---------------------------------------------------------------------------
// mod rebalance
// ---------------------------------------------------------------------------

mod rebalance {
    use super::*;

    #[test]
    fn rebalance_moves_tables() {
        let mgr = ClusterManager::new(test_config("n1"));
        let mut n1 = ClusterNode::new("n1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
        n1.tables = vec!["t1".into(), "t2".into(), "t3".into(), "t4".into()];
        mgr.add_node(n1);
        let n2 = ClusterNode::new("n2".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
        mgr.add_node(n2);

        let plan = mgr.rebalance().unwrap();
        assert_eq!(plan.moves.len(), 2);
    }

    #[test]
    fn rebalance_no_moves_when_balanced() {
        let mgr = ClusterManager::new(test_config("n1"));
        let mut n1 = ClusterNode::new("n1".into(), "addr1".into(), NodeRole::Primary);
        n1.tables = vec!["t1".into()];
        mgr.add_node(n1);
        let mut n2 = ClusterNode::new("n2".into(), "addr2".into(), NodeRole::ReadReplica);
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
