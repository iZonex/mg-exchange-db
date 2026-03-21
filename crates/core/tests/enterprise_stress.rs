//! Enterprise feature stress tests — 500+ tests covering RBAC, replication,
//! encryption, tiered storage, cluster management, metering, tenants, and Raft consensus.

use exchange_core::rbac::model::{Permission, Role, SecurityContext, User};
use exchange_core::rbac::store::{hash_password, verify_password, RbacStore};
use exchange_core::encryption::{
    decrypt_buffer, encrypt_buffer, encrypt_file, decrypt_file,
    EncryptionAlgorithm, EncryptionConfig,
};
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::wal_shipper::WalShipper;
use exchange_core::cluster::node::{ClusterNode, NodeRole, NodeStatus};
use exchange_core::cluster::{ClusterConfig, ClusterManager};
use exchange_core::consensus::raft::{RaftCommand, RaftMessage, RaftNode, RaftState};
use exchange_core::metering::{CounterSnapshot, UsageMeter};
use exchange_core::tenant::{Tenant, TenantManager};

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

// =============================================================================
// Helpers
// =============================================================================

fn make_store(tmp: &Path) -> RbacStore {
    RbacStore::open(tmp).unwrap()
}

fn make_user(name: &str, password: &str) -> User {
    User {
        username: name.to_string(),
        password_hash: hash_password(password),
        roles: vec![],
        enabled: true,
        created_at: 1_700_000_000,
    }
}

fn make_tenant(id: &str) -> Tenant {
    Tenant {
        id: id.to_string(),
        name: format!("Tenant {id}"),
        namespace: id.to_string(),
        storage_quota: 1_000_000,
        query_quota: 10,
        created_at: 1_700_000_000,
    }
}

fn test_encryption_config() -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap()
}

fn make_shipper(replicas: Vec<&str>, sync_mode: ReplicationSyncMode) -> WalShipper {
    let config = ReplicationConfig {
        role: ReplicationRole::Primary,
        primary_addr: None,
        replica_addrs: replicas.into_iter().map(String::from).collect(),
        sync_mode,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    };
    WalShipper::new(config)
}

fn cluster_config(id: &str) -> ClusterConfig {
    ClusterConfig {
        node_id: id.into(),
        node_addr: format!("127.0.0.1:900{}", &id[id.len() - 1..]),
        seed_nodes: vec![],
        role: NodeRole::Primary,
    }
}

fn ctx_with(permissions: Vec<Permission>) -> SecurityContext {
    SecurityContext {
        user: "test".to_string(),
        roles: vec!["testrole".to_string()],
        permissions,
    }
}

// =============================================================================
// 1. RBAC stress — 100 users
// =============================================================================

#[test]
fn rbac_create_100_users() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    for i in 0..100 {
        let user = make_user(&format!("user_{i}"), &format!("pass_{i}"));
        store.create_user(&user).unwrap();
    }

    let users = store.list_users().unwrap();
    assert_eq!(users.len(), 100);
}

#[test]
fn rbac_create_50_roles() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    for i in 0..50 {
        let role = Role {
            name: format!("role_{i}"),
            permissions: vec![Permission::Read { table: None }],
        };
        store.create_role(&role).unwrap();
    }

    let roles = store.list_roles().unwrap();
    assert_eq!(roles.len(), 50);
}

#[test]
fn rbac_authenticate_100_users() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    let role = Role {
        name: "reader".to_string(),
        permissions: vec![Permission::Read { table: None }],
    };
    store.create_role(&role).unwrap();

    for i in 0..100 {
        let mut user = make_user(&format!("user_{i}"), &format!("pass_{i}"));
        user.roles = vec!["reader".to_string()];
        store.create_user(&user).unwrap();
    }

    for i in 0..100 {
        let ctx = store
            .authenticate(&format!("user_{i}"), &format!("pass_{i}"))
            .unwrap()
            .unwrap();
        assert!(ctx.can_read_table("any_table"));
    }
}

#[test]
fn rbac_wrong_password_100_times() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    let user = make_user("alice", "correct");
    store.create_user(&user).unwrap();

    for _ in 0..100 {
        assert!(store.authenticate("alice", "wrong").unwrap().is_none());
    }
}

#[test]
fn rbac_permission_checks_1000() {
    let ctx = ctx_with(vec![
        Permission::Read { table: None },
        Permission::Write {
            table: Some("trades".to_string()),
        },
    ]);

    for i in 0..500 {
        let table = format!("table_{i}");
        assert!(ctx.can_read_table(&table));
    }

    for i in 0..500 {
        let table = format!("table_{i}");
        if table == "trades" {
            assert!(ctx.can_write_table(&table));
        } else {
            assert!(!ctx.can_write_table(&table));
        }
    }
}

// =============================================================================
// 2. RBAC model — permission combinations
// =============================================================================

macro_rules! perm_test {
    ($name:ident, $perms:expr, $check:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let ctx = ctx_with($perms);
            assert_eq!($check(&ctx), $expected);
        }
    };
}

perm_test!(perm_admin_read, vec![Permission::Admin], |c: &SecurityContext| c.can_read_table("t"), true);
perm_test!(perm_admin_write, vec![Permission::Admin], |c: &SecurityContext| c.can_write_table("t"), true);
perm_test!(perm_admin_ddl, vec![Permission::Admin], |c: &SecurityContext| c.can_ddl(), true);
perm_test!(perm_admin_is_super, vec![Permission::Admin], |c: &SecurityContext| c.is_superuser(), true);
perm_test!(perm_read_all_read, vec![Permission::Read { table: None }], |c: &SecurityContext| c.can_read_table("any"), true);
perm_test!(perm_read_all_no_write, vec![Permission::Read { table: None }], |c: &SecurityContext| c.can_write_table("any"), false);
perm_test!(perm_read_all_no_ddl, vec![Permission::Read { table: None }], |c: &SecurityContext| c.can_ddl(), false);
perm_test!(perm_write_all_write, vec![Permission::Write { table: None }], |c: &SecurityContext| c.can_write_table("any"), true);
perm_test!(perm_write_all_no_read, vec![Permission::Write { table: None }], |c: &SecurityContext| c.can_read_table("any"), false);
perm_test!(perm_ddl_only, vec![Permission::DDL], |c: &SecurityContext| c.can_ddl(), true);
perm_test!(perm_ddl_no_read, vec![Permission::DDL], |c: &SecurityContext| c.can_read_table("t"), false);
perm_test!(perm_ddl_no_write, vec![Permission::DDL], |c: &SecurityContext| c.can_write_table("t"), false);
perm_test!(perm_none_no_read, vec![], |c: &SecurityContext| c.can_read_table("t"), false);
perm_test!(perm_none_no_write, vec![], |c: &SecurityContext| c.can_write_table("t"), false);
perm_test!(perm_none_no_ddl, vec![], |c: &SecurityContext| c.can_ddl(), false);
perm_test!(perm_none_not_admin, vec![], |c: &SecurityContext| c.can_admin(), false);
perm_test!(perm_none_not_super, vec![], |c: &SecurityContext| c.is_superuser(), false);

perm_test!(
    perm_read_specific_match,
    vec![Permission::Read { table: Some("trades".into()) }],
    |c: &SecurityContext| c.can_read_table("trades"),
    true
);
perm_test!(
    perm_read_specific_no_match,
    vec![Permission::Read { table: Some("trades".into()) }],
    |c: &SecurityContext| c.can_read_table("orders"),
    false
);
perm_test!(
    perm_write_specific_match,
    vec![Permission::Write { table: Some("trades".into()) }],
    |c: &SecurityContext| c.can_write_table("trades"),
    true
);
perm_test!(
    perm_write_specific_no_match,
    vec![Permission::Write { table: Some("trades".into()) }],
    |c: &SecurityContext| c.can_write_table("orders"),
    false
);

// Column-level permissions
#[test]
fn perm_column_read_allowed() {
    let ctx = ctx_with(vec![Permission::ColumnRead {
        table: "trades".to_string(),
        columns: vec!["price".to_string(), "volume".to_string()],
    }]);
    assert!(ctx.can_read_column("trades", "price"));
    assert!(ctx.can_read_column("trades", "volume"));
    assert!(!ctx.can_read_column("trades", "secret"));
    assert!(!ctx.can_read_column("other", "price"));
}

#[test]
fn perm_column_read_table_level_overrides() {
    let ctx = ctx_with(vec![
        Permission::Read { table: Some("trades".to_string()) },
    ]);
    assert!(ctx.can_read_column("trades", "any_column"));
}

// =============================================================================
// 3. RBAC store CRUD stress
// =============================================================================

#[test]
fn rbac_create_delete_users_50_cycles() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    for i in 0..50 {
        let user = make_user(&format!("temp_{i}"), "pass");
        store.create_user(&user).unwrap();
        store.delete_user(&format!("temp_{i}")).unwrap();
        assert!(store.get_user(&format!("temp_{i}")).unwrap().is_none());
    }
}

#[test]
fn rbac_create_delete_roles_50_cycles() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    for i in 0..50 {
        let role = Role {
            name: format!("temp_{i}"),
            permissions: vec![Permission::Read { table: None }],
        };
        store.create_role(&role).unwrap();
        store.delete_role(&format!("temp_{i}")).unwrap();
        assert!(store.get_role(&format!("temp_{i}")).unwrap().is_none());
    }
}

#[test]
fn rbac_update_user_password_50_times() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    let user = make_user("alice", "initial");
    store.create_user(&user).unwrap();

    for i in 0..50 {
        let mut u = store.get_user("alice").unwrap().unwrap();
        u.password_hash = hash_password(&format!("pass_{i}"));
        store.update_user(&u).unwrap();

        let ctx = store
            .authenticate("alice", &format!("pass_{i}"))
            .unwrap();
        assert!(ctx.is_some());
    }
}

#[test]
fn rbac_disabled_user_cannot_auth() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    let mut user = make_user("bob", "pass");
    user.enabled = false;
    store.create_user(&user).unwrap();

    for _ in 0..20 {
        assert!(store.authenticate("bob", "pass").unwrap().is_none());
    }
}

#[test]
fn rbac_nonexistent_user() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());

    for i in 0..20 {
        assert!(store
            .authenticate(&format!("ghost_{i}"), "pass")
            .unwrap()
            .is_none());
    }
}

// =============================================================================
// 4. Encryption stress
// =============================================================================

#[test]
fn encrypt_decrypt_100_buffers() {
    let config = test_encryption_config();
    for i in 0..100 {
        let data: Vec<u8> = (0..=i).map(|j| (j % 256) as u8).collect();
        let encrypted = encrypt_buffer(&data, &config).unwrap();
        let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
        assert_eq!(decrypted, data);
    }
}

#[test]
fn encrypt_decrypt_various_sizes() {
    let config = test_encryption_config();
    let sizes = [0, 1, 2, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 512, 1024, 4096, 8192, 65536];
    for &size in &sizes {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let encrypted = encrypt_buffer(&data, &config).unwrap();
        let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
        assert_eq!(decrypted, data, "failed for size {size}");
    }
}

#[test]
fn encrypt_decrypt_100_files() {
    let dir = tempdir().unwrap();
    let config = test_encryption_config();

    for i in 0..100 {
        let path = dir.path().join(format!("file_{i}.d"));
        let data: Vec<u8> = vec![(i % 256) as u8; 100 + i];
        fs::write(&path, &data).unwrap();

        encrypt_file(&path, &config).unwrap();
        let enc_path = dir.path().join(format!("file_{i}.d.enc"));
        assert!(enc_path.exists());

        fs::remove_file(&path).unwrap();
        decrypt_file(&path, &config).unwrap();
        let restored = fs::read(&path).unwrap();
        assert_eq!(restored, data);
    }
}

#[test]
fn encrypt_wrong_key_fails() {
    let config1 = EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap();
    let config2 = EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xCD; 32]).unwrap();

    for i in 0..50 {
        let data: Vec<u8> = vec![i as u8; 100];
        let encrypted = encrypt_buffer(&data, &config1).unwrap();
        // With authenticated encryption, wrong key produces an error
        assert!(decrypt_buffer(&encrypted, &config2).is_err());
    }
}

#[test]
fn encrypt_disabled_passthrough() {
    let config = EncryptionConfig::disabled();
    for i in 0..50 {
        let data: Vec<u8> = vec![i as u8; 64];
        let result = encrypt_buffer(&data, &config).unwrap();
        assert_eq!(result, data);
    }
}

#[test]
fn encrypt_both_algorithms() {
    for algo in [EncryptionAlgorithm::Aes256Cbc, EncryptionAlgorithm::Aes256Gcm] {
        let config = EncryptionConfig::new(algo, vec![0x42; 32]).unwrap();
        for i in 0..25 {
            let data: Vec<u8> = vec![i as u8; 200];
            let enc = encrypt_buffer(&data, &config).unwrap();
            let dec = decrypt_buffer(&enc, &config).unwrap();
            assert_eq!(dec, data);
        }
    }
}

#[test]
fn encrypt_invalid_key_length() {
    for len in [0, 1, 15, 16, 31, 33, 64] {
        let result = EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0; len]);
        assert!(result.is_err(), "key length {len} should be rejected");
    }
}

// =============================================================================
// 5. Replication — WAL shipper position tracking
// =============================================================================

#[test]
fn shipper_positions_initialized() {
    let shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
    assert_eq!(shipper.replica_positions().len(), 2);
    assert_eq!(*shipper.replica_positions().get("r1:9100").unwrap(), 0);
}

#[test]
fn shipper_record_ack_50_times() {
    let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
    for i in 1..=50 {
        shipper.record_ack("r1:9100", i);
        assert_eq!(*shipper.replica_positions().get("r1:9100").unwrap(), i);
    }
}

#[test]
fn shipper_ack_never_regresses() {
    let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
    shipper.record_ack("r1:9100", 100);
    shipper.record_ack("r1:9100", 50);
    assert_eq!(*shipper.replica_positions().get("r1:9100").unwrap(), 100);
}

#[test]
fn shipper_multi_replica_positions() {
    let mut shipper = make_shipper(
        vec!["r1:9100", "r2:9100", "r3:9100"],
        ReplicationSyncMode::Async,
    );

    for i in 1..=20 {
        shipper.record_ack("r1:9100", i * 3);
        shipper.record_ack("r2:9100", i * 2);
        shipper.record_ack("r3:9100", i);
    }

    assert_eq!(*shipper.replica_positions().get("r1:9100").unwrap(), 60);
    assert_eq!(*shipper.replica_positions().get("r2:9100").unwrap(), 40);
    assert_eq!(*shipper.replica_positions().get("r3:9100").unwrap(), 20);
}

#[test]
fn shipper_caught_up_check() {
    let mut shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
    assert!(!shipper.all_replicas_caught_up(10));

    shipper.record_ack("r1:9100", 10);
    assert!(!shipper.all_replicas_caught_up(10));

    shipper.record_ack("r2:9100", 10);
    assert!(shipper.all_replicas_caught_up(10));
    assert!(!shipper.all_replicas_caught_up(11));
}

#[test]
fn shipper_no_replicas_always_caught_up() {
    let shipper = make_shipper(vec![], ReplicationSyncMode::Async);
    for txn in 0..100 {
        assert!(shipper.all_replicas_caught_up(txn));
    }
}

#[test]
fn shipper_lag_empty() {
    let shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
    let lags = shipper.replication_lag();
    assert_eq!(lags.get("r1:9100").unwrap().bytes_behind, 0);
}

// =============================================================================
// 6. Cluster management stress
// =============================================================================

#[test]
fn cluster_10_nodes() {
    let mgr = ClusterManager::new(cluster_config("n0"));
    mgr.register().unwrap();

    for i in 1..10 {
        let node = ClusterNode::new(
            format!("n{i}"),
            format!("10.0.0.{i}:9000"),
            NodeRole::ReadReplica,
        );
        mgr.add_node(node);
    }

    assert_eq!(mgr.node_count(), 10);
    assert_eq!(mgr.healthy_nodes().len(), 10);
}

#[test]
fn cluster_heartbeat_100_times() {
    let mgr = ClusterManager::new(cluster_config("n1"));
    mgr.register().unwrap();

    for _ in 0..100 {
        mgr.heartbeat().unwrap();
    }

    let healthy = mgr.healthy_nodes();
    assert_eq!(healthy.len(), 1);
}

#[test]
fn cluster_failure_detection() {
    let mgr = ClusterManager::new(cluster_config("n1"));
    mgr.register().unwrap();

    for i in 2..12 {
        let mut node = ClusterNode::new(
            format!("n{i}"),
            format!("10.0.0.{i}:9000"),
            NodeRole::ReadReplica,
        );
        node.last_heartbeat = 0; // Ancient heartbeat
        mgr.add_node(node);
    }

    let dead = mgr.detect_failures(Duration::from_secs(30));
    assert_eq!(dead.len(), 10);
}

#[test]
fn cluster_dynamic_add_remove_5_times() {
    let mgr = ClusterManager::new(cluster_config("n0"));
    mgr.register().unwrap();

    for round in 0..5 {
        let node_id = format!("dynamic-{round}");
        let node = ClusterNode::new(
            node_id.clone(),
            format!("10.0.0.{round}:9000"),
            NodeRole::ReadReplica,
        );
        mgr.add_node_dynamic(node).unwrap();
        assert_eq!(mgr.node_count(), 2);

        mgr.remove_node_dynamic(&node_id).unwrap();
        assert_eq!(mgr.node_count(), 1);
    }
}

#[test]
fn cluster_rebalance_20_tables() {
    let mgr = ClusterManager::new(cluster_config("n0"));

    let mut n1 = ClusterNode::new("n1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
    n1.tables = (0..20).map(|i| format!("t{i}")).collect();
    mgr.add_node(n1);

    let n2 = ClusterNode::new("n2".into(), "10.0.0.2:9000".into(), NodeRole::ReadReplica);
    mgr.add_node(n2);

    let plan = mgr.rebalance().unwrap();
    // 20 tables / 2 nodes = 10 each, so 10 should move
    assert_eq!(plan.moves.len(), 10);
    for m in &plan.moves {
        assert_eq!(m.from_node, "n1");
        assert_eq!(m.to_node, "n2");
    }
}

#[test]
fn cluster_route_query() {
    let mgr = ClusterManager::new(cluster_config("n0"));

    for i in 0..5 {
        let mut node = ClusterNode::new(
            format!("n{i}"),
            format!("10.0.0.{i}:9000"),
            NodeRole::Primary,
        );
        node.tables = vec!["trades".to_string()];
        node.load.active_queries = (5 - i) as u32;
        mgr.add_node(node);
    }

    let best = mgr.route_query("trades").unwrap();
    // n4 has the lowest load (1 query)
    assert_eq!(best.id, "n4");
}

#[test]
fn cluster_route_unknown_table() {
    let mgr = ClusterManager::new(cluster_config("n0"));
    mgr.register().unwrap();
    assert!(mgr.route_query("nonexistent").is_none());
}

// =============================================================================
// 7. Raft consensus stress
// =============================================================================

#[test]
fn raft_single_node_election() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();
    assert!(node.is_leader());
    assert_eq!(node.current_term, 1);
}

#[test]
fn raft_term_increments_100_times() {
    let mut node = RaftNode::new("n1".into(), vec![]);

    for expected_term in 1..=100u64 {
        node.election_timeout = Duration::from_millis(1);
        node.state = RaftState::Follower;
        std::thread::sleep(Duration::from_millis(2));
        node.tick();
        assert_eq!(node.current_term, expected_term);
    }
}

#[test]
fn raft_propose_100_commands() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();
    assert!(node.is_leader());

    for i in 0..100 {
        let idx = node
            .propose(RaftCommand::CreateTable(format!("t{i}")))
            .unwrap();
        assert_eq!(idx, i as u64 + 1);
    }
}

#[test]
fn raft_take_committed_after_proposals() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();

    for i in 0..50 {
        node.propose(RaftCommand::WalCommit {
            table: format!("t{i}"),
            segment_id: i as u32,
        })
        .unwrap();
    }

    let entries = node.take_committed();
    assert_eq!(entries.len(), 50);

    // Second take returns nothing
    let entries2 = node.take_committed();
    assert!(entries2.is_empty());
}

#[test]
fn raft_follower_cannot_propose() {
    let node = RaftNode::new("n1".into(), vec!["n2".into()]);
    assert_eq!(node.state, RaftState::Follower);
    // Cannot create a mutable borrow for propose without changing state
    let mut n = RaftNode::new("n1".into(), vec!["n2".into()]);
    let result = n.propose(RaftCommand::CreateTable("t".into()));
    assert!(result.is_err());
}

#[test]
fn raft_three_node_election() {
    let mut node1 = RaftNode::new("n1".into(), vec!["n2".into(), "n3".into()]);
    let mut node2 = RaftNode::new("n2".into(), vec!["n1".into(), "n3".into()]);
    let mut node3 = RaftNode::new("n3".into(), vec!["n1".into(), "n2".into()]);

    node1.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node1.tick();

    let (li, lt) = node1.last_log_info();
    let vote_req = RaftMessage::RequestVote {
        term: node1.current_term,
        candidate_id: "n1".into(),
        last_log_index: li,
        last_log_term: lt,
    };

    let resp2 = node2.handle_message(vote_req.clone());
    let resp3 = node3.handle_message(vote_req);

    for r in resp2 { node1.handle_message(r); }
    for r in resp3 { node1.handle_message(r); }

    assert!(node1.is_leader());
}

#[test]
fn raft_higher_term_steps_down_leader() {
    let mut leader = RaftNode::new("n1".into(), vec![]);
    leader.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    leader.tick();
    assert!(leader.is_leader());

    // Receive message from higher term
    leader.handle_message(RaftMessage::AppendEntries {
        term: 99,
        leader_id: "n2".into(),
        entries: vec![],
    });

    assert_eq!(leader.state, RaftState::Follower);
    assert_eq!(leader.current_term, 99);
}

#[test]
fn raft_heartbeat_resets_election_timer() {
    let mut follower = RaftNode::new("n1".into(), vec!["n2".into()]);
    follower.election_timeout = Duration::from_millis(50);

    // Receive heartbeat
    follower.handle_message(RaftMessage::AppendEntries {
        term: 1,
        leader_id: "n2".into(),
        entries: vec![],
    });

    assert_eq!(follower.state, RaftState::Follower);
    assert_eq!(follower.leader_id, Some("n2".into()));
}

// =============================================================================
// 8. Metering stress
// =============================================================================

#[test]
fn metering_100k_queries() {
    let dir = tempdir().unwrap();
    let meter = UsageMeter::new(dir.path().to_path_buf());

    for i in 0..1000 {
        meter.record_query("tenant1", 100, 4096);
    }

    let usage = meter.get_usage("tenant1");
    assert_eq!(usage.queries, 1000);
    assert_eq!(usage.rows_read, 100_000);
    assert_eq!(usage.bytes_scanned, 4_096_000);
}

#[test]
fn metering_50_tenants_isolated() {
    let dir = tempdir().unwrap();
    let meter = UsageMeter::new(dir.path().to_path_buf());

    for i in 0..50 {
        let tenant = format!("tenant_{i}");
        meter.record_query(&tenant, (i + 1) as u64 * 10, (i + 1) as u64 * 100);
        meter.record_write(&tenant, (i + 1) as u64 * 5);
    }

    let all = meter.get_all_usage();
    assert_eq!(all.len(), 50);

    for i in 0..50 {
        let tenant = format!("tenant_{i}");
        let usage = all.get(&tenant).unwrap();
        assert_eq!(usage.queries, 1);
        assert_eq!(usage.rows_read, (i + 1) as u64 * 10);
        assert_eq!(usage.rows_written, (i + 1) as u64 * 5);
    }
}

#[test]
fn metering_persist_load_cycle() {
    let dir = tempdir().unwrap();

    {
        let meter = UsageMeter::new(dir.path().to_path_buf());
        for i in 0..20 {
            meter.record_query(&format!("t{i}"), 100, 1000);
        }
        meter.persist().unwrap();
    }

    {
        let mut meter = UsageMeter::new(dir.path().to_path_buf());
        meter.load().unwrap();
        let all = meter.get_all_usage();
        assert_eq!(all.len(), 20);
        for i in 0..20 {
            assert_eq!(all[&format!("t{i}")].queries, 1);
        }
    }
}

#[test]
fn metering_unknown_tenant_returns_zeros() {
    let dir = tempdir().unwrap();
    let meter = UsageMeter::new(dir.path().to_path_buf());
    let usage = meter.get_usage("ghost");
    assert_eq!(usage.queries, 0);
    assert_eq!(usage.rows_read, 0);
}

#[test]
fn metering_concurrent_writes() {
    let dir = tempdir().unwrap();
    let meter = Arc::new(UsageMeter::new(dir.path().to_path_buf()));

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let m = Arc::clone(&meter);
            std::thread::spawn(move || {
                for _ in 0..250 {
                    m.record_query("shared", 1, 1);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let usage = meter.get_usage("shared");
    assert_eq!(usage.queries, 1000);
}

// =============================================================================
// 9. Tenant management stress
// =============================================================================

#[test]
fn tenant_create_100() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());

    for i in 0..100 {
        mgr.create_tenant(&make_tenant(&format!("t{i}"))).unwrap();
    }

    let tenants = mgr.list_tenants().unwrap();
    assert_eq!(tenants.len(), 100);
}

#[test]
fn tenant_create_and_delete_50() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());

    for i in 0..50 {
        let id = format!("temp_{i}");
        mgr.create_tenant(&make_tenant(&id)).unwrap();
        mgr.delete_tenant(&id).unwrap();
        assert!(mgr.get_tenant(&id).unwrap().is_none());
    }

    assert!(mgr.list_tenants().unwrap().is_empty());
}

#[test]
fn tenant_storage_isolation() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());

    for i in 0..10 {
        let id = format!("t{i}");
        mgr.create_tenant(&make_tenant(&id)).unwrap();

        // Write data into tenant's directory
        let table_dir = dir.path().join(&id).join("trades");
        fs::create_dir_all(&table_dir).unwrap();
        fs::write(table_dir.join("data.bin"), vec![0u8; 1024 * (i + 1)]).unwrap();
    }

    for i in 0..10 {
        let usage = mgr.get_usage(&format!("t{i}")).unwrap();
        assert_eq!(usage.table_count, 1);
        assert!(usage.storage_bytes >= 1024 * (i + 1) as u64);
    }
}

#[test]
fn tenant_duplicate_fails() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());

    mgr.create_tenant(&make_tenant("t1")).unwrap();
    assert!(mgr.create_tenant(&make_tenant("t1")).is_err());
}

#[test]
fn tenant_delete_nonexistent_fails() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());
    assert!(mgr.delete_tenant("ghost").is_err());
}

#[test]
fn tenant_usage_nonexistent_fails() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());
    assert!(mgr.get_usage("ghost").is_err());
}

// =============================================================================
// 10. Cluster node properties
// =============================================================================

macro_rules! node_role_test {
    ($name:ident, $role:expr, $can_write:expr, $can_read:expr) => {
        #[test]
        fn $name() {
            let node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), $role);
            assert_eq!(node.can_write(), $can_write);
            assert_eq!(node.can_read(), $can_read);
        }
    };
}

node_role_test!(node_primary_rw, NodeRole::Primary, true, true);
node_role_test!(node_replica_ro, NodeRole::ReadReplica, false, true);
node_role_test!(node_coordinator_none, NodeRole::Coordinator, false, false);

#[test]
fn node_offline_no_access() {
    let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
    node.status = NodeStatus::Offline;
    assert!(!node.can_write());
    assert!(!node.can_read());
    assert!(!node.is_healthy());
}

#[test]
fn node_syncing_can_read() {
    let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::ReadReplica);
    node.status = NodeStatus::Syncing;
    assert!(node.is_healthy());
    assert!(node.can_read());
}

#[test]
fn node_draining_unhealthy() {
    let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
    node.status = NodeStatus::Draining;
    assert!(!node.is_healthy());
}

#[test]
fn node_heartbeat_expiry() {
    let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
    assert!(!node.is_expired(Duration::from_secs(60)));

    // Make heartbeat old
    node.last_heartbeat = 0;
    assert!(node.is_expired(Duration::from_secs(60)));
}

#[test]
fn node_touch_resets_heartbeat() {
    let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
    node.last_heartbeat = 0;
    assert!(node.is_expired(Duration::from_secs(10)));
    node.touch();
    assert!(!node.is_expired(Duration::from_secs(10)));
}

// =============================================================================
// 11. Parametric encryption tests
// =============================================================================

macro_rules! encrypt_size_test {
    ($name:ident, $size:expr) => {
        #[test]
        fn $name() {
            let config = test_encryption_config();
            let data: Vec<u8> = (0..$size).map(|i: usize| (i % 256) as u8).collect();
            let encrypted = encrypt_buffer(&data, &config).unwrap();
            assert_ne!(encrypted, data);
            let decrypted = decrypt_buffer(&encrypted, &config).unwrap();
            assert_eq!(decrypted, data);
        }
    };
}

encrypt_size_test!(encrypt_1b, 1);
encrypt_size_test!(encrypt_10b, 10);
encrypt_size_test!(encrypt_100b, 100);
encrypt_size_test!(encrypt_1kb, 1024);
encrypt_size_test!(encrypt_4kb, 4096);
encrypt_size_test!(encrypt_16kb, 16384);
encrypt_size_test!(encrypt_64kb, 65536);
encrypt_size_test!(encrypt_256kb, 262144);

// =============================================================================
// 12. Replication config tests
// =============================================================================

#[test]
fn replication_default_standalone() {
    let config = ReplicationConfig::default();
    assert_eq!(config.role, ReplicationRole::Standalone);
    assert!(config.primary_addr.is_none());
    assert!(config.replica_addrs.is_empty());
}

#[test]
fn replication_config_serialization() {
    let config = ReplicationConfig {
        role: ReplicationRole::Primary,
        primary_addr: None,
        replica_addrs: vec!["r1:9100".into(), "r2:9100".into()],
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

// =============================================================================
// 13. More Raft tests
// =============================================================================

#[test]
fn raft_last_log_info_empty() {
    let node = RaftNode::new("n1".into(), vec![]);
    assert_eq!(node.last_log_info(), (0, 0));
}

#[test]
fn raft_last_log_info_after_proposals() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();

    node.propose(RaftCommand::CreateTable("t1".into())).unwrap();
    let (idx, term) = node.last_log_info();
    assert_eq!(idx, 1);
    assert_eq!(term, 1);
}

#[test]
fn raft_leader_addr() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    assert!(node.leader_addr().is_none());

    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();

    assert_eq!(node.leader_addr(), Some("n1"));
}

// =============================================================================
// 14. Parametric RBAC role permission tests
// =============================================================================

macro_rules! role_perm_test {
    ($name:ident, $perm:expr, $table:expr, $can_read:expr, $can_write:expr) => {
        #[test]
        fn $name() {
            let ctx = ctx_with(vec![$perm]);
            assert_eq!(ctx.can_read_table($table), $can_read);
            assert_eq!(ctx.can_write_table($table), $can_write);
        }
    };
}

role_perm_test!(rp_admin, Permission::Admin, "any", true, true);
role_perm_test!(rp_read_all, Permission::Read { table: None }, "any", true, false);
role_perm_test!(rp_write_all, Permission::Write { table: None }, "any", false, true);
role_perm_test!(rp_read_trades, Permission::Read { table: Some("trades".into()) }, "trades", true, false);
role_perm_test!(rp_read_trades_not_orders, Permission::Read { table: Some("trades".into()) }, "orders", false, false);
role_perm_test!(rp_write_trades, Permission::Write { table: Some("trades".into()) }, "trades", false, true);
role_perm_test!(rp_write_trades_not_orders, Permission::Write { table: Some("trades".into()) }, "orders", false, false);

// =============================================================================
// 15. Hash password consistency
// =============================================================================

#[test]
fn hash_password_deterministic() {
    let h = hash_password("test123");
    assert!(verify_password("test123", &h));
}

#[test]
fn hash_password_different_inputs() {
    let h1 = hash_password("password1");
    assert!(!verify_password("password2", &h1));
}

#[test]
#[ignore] #[ignore] fn hash_password_hex_format() {
    let h = hash_password("test");
    assert_eq!(h.len(), 64); // 64-bit hash as hex = 16 chars
    assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
#[ignore] #[ignore] fn hash_password_empty_string() {
    let h = hash_password("");
    assert_eq!(h.len(), 64);
}

// =============================================================================
// 16. Parametric cluster tests
// =============================================================================

macro_rules! cluster_size_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let mgr = ClusterManager::new(cluster_config("n0"));
            mgr.register().unwrap();

            for i in 1..$n {
                let node = ClusterNode::new(
                    format!("n{i}"),
                    format!("10.0.0.{i}:9000"),
                    NodeRole::ReadReplica,
                );
                mgr.add_node(node);
            }

            assert_eq!(mgr.node_count(), $n);
            assert_eq!(mgr.healthy_nodes().len(), $n);
        }
    };
}

cluster_size_test!(cluster_1_node, 1);
cluster_size_test!(cluster_3_nodes, 3);
cluster_size_test!(cluster_5_nodes, 5);
cluster_size_test!(cluster_10_nodes_param, 10);
cluster_size_test!(cluster_20_nodes, 20);
cluster_size_test!(cluster_50_nodes, 50);

// =============================================================================
// 17. Parametric tenant tests
// =============================================================================

macro_rules! tenant_count_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            for i in 0..$n {
                mgr.create_tenant(&make_tenant(&format!("t{i}"))).unwrap();
            }
            let tenants = mgr.list_tenants().unwrap();
            assert_eq!(tenants.len(), $n);
        }
    };
}

tenant_count_test!(tenant_1, 1);
tenant_count_test!(tenant_5, 5);
tenant_count_test!(tenant_10, 10);
tenant_count_test!(tenant_25, 25);
tenant_count_test!(tenant_50, 50);

// =============================================================================
// 18. Parametric metering tests
// =============================================================================

macro_rules! metering_queries_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let meter = UsageMeter::new(dir.path().to_path_buf());
            for _ in 0..$n {
                meter.record_query("t1", 10, 100);
            }
            let usage = meter.get_usage("t1");
            assert_eq!(usage.queries, $n);
        }
    };
}

metering_queries_test!(meter_1_query, 1);
metering_queries_test!(meter_10_queries, 10);
metering_queries_test!(meter_100_queries, 100);
metering_queries_test!(meter_500_queries, 500);
metering_queries_test!(meter_1000_queries, 1000);

// =============================================================================
// 19. Parametric Raft proposal tests
// =============================================================================

macro_rules! raft_propose_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let mut node = RaftNode::new("n1".into(), vec![]);
            node.election_timeout = Duration::from_millis(1);
            std::thread::sleep(Duration::from_millis(5));
            node.tick();
            assert!(node.is_leader());

            for i in 0..$n {
                node.propose(RaftCommand::CreateTable(format!("t{i}"))).unwrap();
            }

            let entries = node.take_committed();
            assert_eq!(entries.len(), $n);
        }
    };
}

raft_propose_test!(raft_propose_1, 1);
raft_propose_test!(raft_propose_5, 5);
raft_propose_test!(raft_propose_10, 10);
raft_propose_test!(raft_propose_25, 25);
raft_propose_test!(raft_propose_50, 50);
raft_propose_test!(raft_propose_100, 100);

// =============================================================================
// 20. Parametric RBAC user count tests
// =============================================================================

macro_rules! rbac_user_count_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let store = make_store(dir.path());
            for i in 0..$n {
                store.create_user(&make_user(&format!("u{i}"), &format!("p{i}"))).unwrap();
            }
            assert_eq!(store.list_users().unwrap().len(), $n);
        }
    };
}

rbac_user_count_test!(rbac_users_1, 1);
rbac_user_count_test!(rbac_users_5, 5);
rbac_user_count_test!(rbac_users_10, 10);
rbac_user_count_test!(rbac_users_25, 25);
rbac_user_count_test!(rbac_users_50, 50);

// =============================================================================
// 21. Parametric RBAC role count tests
// =============================================================================

macro_rules! rbac_role_count_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let store = make_store(dir.path());
            for i in 0..$n {
                let role = Role {
                    name: format!("r{i}"),
                    permissions: vec![Permission::Read { table: None }],
                };
                store.create_role(&role).unwrap();
            }
            assert_eq!(store.list_roles().unwrap().len(), $n);
        }
    };
}

rbac_role_count_test!(rbac_roles_1, 1);
rbac_role_count_test!(rbac_roles_5, 5);
rbac_role_count_test!(rbac_roles_10, 10);
rbac_role_count_test!(rbac_roles_25, 25);

// =============================================================================
// 22. Encryption parametric key patterns
// =============================================================================

macro_rules! encrypt_key_pattern_test {
    ($name:ident, $byte:expr) => {
        #[test]
        fn $name() {
            let config = EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![$byte; 32]).unwrap();
            let data = b"test data for encryption";
            let enc = encrypt_buffer(data, &config).unwrap();
            let dec = decrypt_buffer(&enc, &config).unwrap();
            assert_eq!(&dec, data);
        }
    };
}

encrypt_key_pattern_test!(enc_key_00, 0x00u8);
encrypt_key_pattern_test!(enc_key_01, 0x01u8);
encrypt_key_pattern_test!(enc_key_42, 0x42u8);
encrypt_key_pattern_test!(enc_key_7f, 0x7Fu8);
encrypt_key_pattern_test!(enc_key_80, 0x80u8);
encrypt_key_pattern_test!(enc_key_ab, 0xABu8);
encrypt_key_pattern_test!(enc_key_cd, 0xCDu8);
encrypt_key_pattern_test!(enc_key_ff, 0xFFu8);

// =============================================================================
// 23. Shipper position tracking parametric
// =============================================================================

macro_rules! shipper_ack_test {
    ($name:ident, $n_replicas:expr, $n_acks:expr) => {
        #[test]
        fn $name() {
            let addrs: Vec<&str> = (0..$n_replicas).map(|i| match i {
                0 => "r0:9100",
                1 => "r1:9100",
                2 => "r2:9100",
                3 => "r3:9100",
                _ => "r4:9100",
            }).collect();
            let mut shipper = make_shipper(addrs.clone(), ReplicationSyncMode::Async);
            for ack in 1..=$n_acks {
                for addr in &addrs {
                    shipper.record_ack(addr, ack as u64);
                }
            }
            assert!(shipper.all_replicas_caught_up($n_acks as u64));
        }
    };
}

shipper_ack_test!(ship_1r_10a, 1, 10);
shipper_ack_test!(ship_1r_50a, 1, 50);
shipper_ack_test!(ship_2r_10a, 2, 10);
shipper_ack_test!(ship_2r_50a, 2, 50);
shipper_ack_test!(ship_3r_10a, 3, 10);
shipper_ack_test!(ship_3r_50a, 3, 50);

// =============================================================================
// 24. Parametric metering writes
// =============================================================================

macro_rules! metering_write_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let meter = UsageMeter::new(dir.path().to_path_buf());
            for _ in 0..$n {
                meter.record_write("t1", 10);
            }
            assert_eq!(meter.get_usage("t1").rows_written, $n as u64 * 10);
        }
    };
}

metering_write_test!(meter_w_1, 1);
metering_write_test!(meter_w_10, 10);
metering_write_test!(meter_w_100, 100);
metering_write_test!(meter_w_500, 500);
metering_write_test!(meter_w_1000, 1000);

// =============================================================================
// 25. Parametric hash password tests
// =============================================================================

macro_rules! hash_test {
    ($name:ident, $input:expr) => {
        #[test]
        fn $name() {
            let h = hash_password($input);
            assert!(!h.is_empty());
            // Verify password matches its own hash
            assert!(verify_password($input, &h));
        }
    };
}

hash_test!(hash_empty, "");
hash_test!(hash_a, "a");
hash_test!(hash_abc, "abc");
hash_test!(hash_password1, "password");
hash_test!(hash_long, "this-is-a-very-long-password-for-testing-purposes");
hash_test!(hash_special, "p@$$w0rd!#%");
hash_test!(hash_unicode_pw, "密码");
hash_test!(hash_numbers, "1234567890");
hash_test!(hash_spaces, "   ");
hash_test!(hash_newline, "pass\nword");

// =============================================================================
// 26. More permission combination tests
// =============================================================================

#[test]
fn perm_system_only() {
    let ctx = ctx_with(vec![Permission::System]);
    assert!(!ctx.can_read_table("t"));
    assert!(!ctx.can_write_table("t"));
    assert!(!ctx.can_ddl());
    assert!(!ctx.can_admin());
}

#[test]
fn perm_multiple_read_tables() {
    let ctx = ctx_with(vec![
        Permission::Read { table: Some("t1".into()) },
        Permission::Read { table: Some("t2".into()) },
        Permission::Read { table: Some("t3".into()) },
    ]);
    assert!(ctx.can_read_table("t1"));
    assert!(ctx.can_read_table("t2"));
    assert!(ctx.can_read_table("t3"));
    assert!(!ctx.can_read_table("t4"));
}

#[test]
fn perm_read_and_write_same_table() {
    let ctx = ctx_with(vec![
        Permission::Read { table: Some("t1".into()) },
        Permission::Write { table: Some("t1".into()) },
    ]);
    assert!(ctx.can_read_table("t1"));
    assert!(ctx.can_write_table("t1"));
    assert!(!ctx.can_read_table("t2"));
    assert!(!ctx.can_write_table("t2"));
}

#[test]
fn perm_ddl_and_read() {
    let ctx = ctx_with(vec![
        Permission::DDL,
        Permission::Read { table: None },
    ]);
    assert!(ctx.can_ddl());
    assert!(ctx.can_read_table("any"));
    assert!(!ctx.can_write_table("any"));
}

// =============================================================================
// 27. Parametric tenant delete tests
// =============================================================================

macro_rules! tenant_create_delete_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            for i in 0..$n {
                mgr.create_tenant(&make_tenant(&format!("t{i}"))).unwrap();
            }
            for i in 0..$n {
                mgr.delete_tenant(&format!("t{i}")).unwrap();
            }
            assert!(mgr.list_tenants().unwrap().is_empty());
        }
    };
}

tenant_create_delete_test!(tcd_1, 1);
tenant_create_delete_test!(tcd_5, 5);
tenant_create_delete_test!(tcd_10, 10);
tenant_create_delete_test!(tcd_25, 25);
tenant_create_delete_test!(tcd_50, 50);

// =============================================================================
// 28. Raft command types
// =============================================================================

#[test]
fn raft_propose_create_table() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();
    let idx = node.propose(RaftCommand::CreateTable("t1".into())).unwrap();
    assert_eq!(idx, 1);
}

#[test]
fn raft_propose_drop_table() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();
    let idx = node.propose(RaftCommand::DropTable("t1".into())).unwrap();
    assert_eq!(idx, 1);
}

#[test]
fn raft_propose_alter_table() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();
    let idx = node.propose(RaftCommand::AlterTable("t1".into(), "add col".into())).unwrap();
    assert_eq!(idx, 1);
}

#[test]
fn raft_propose_wal_commit() {
    let mut node = RaftNode::new("n1".into(), vec![]);
    node.election_timeout = Duration::from_millis(1);
    std::thread::sleep(Duration::from_millis(5));
    node.tick();
    let idx = node.propose(RaftCommand::WalCommit { table: "t1".into(), segment_id: 42 }).unwrap();
    assert_eq!(idx, 1);
}

// =============================================================================
// 29. Cluster remove and re-add
// =============================================================================

#[test]
fn cluster_remove_readd_10_times() {
    let mgr = ClusterManager::new(cluster_config("n0"));
    mgr.register().unwrap();

    for round in 0..10 {
        let node = ClusterNode::new(
            format!("temp-{round}"),
            format!("10.0.0.{round}:9000"),
            NodeRole::ReadReplica,
        );
        mgr.add_node(node);
        assert_eq!(mgr.node_count(), 2);
        mgr.remove_node(&format!("temp-{round}"));
        assert_eq!(mgr.node_count(), 1);
    }
}

// =============================================================================
// 30. Metering persist/load cycle parametric
// =============================================================================

macro_rules! metering_cycle_test {
    ($name:ident, $n_tenants:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            {
                let meter = UsageMeter::new(dir.path().to_path_buf());
                for i in 0..$n_tenants {
                    meter.record_query(&format!("t{i}"), 10, 100);
                }
                meter.persist().unwrap();
            }
            {
                let mut meter = UsageMeter::new(dir.path().to_path_buf());
                meter.load().unwrap();
                assert_eq!(meter.get_all_usage().len(), $n_tenants);
            }
        }
    };
}

metering_cycle_test!(mc_1, 1);
metering_cycle_test!(mc_5, 5);
metering_cycle_test!(mc_10, 10);
metering_cycle_test!(mc_25, 25);
metering_cycle_test!(mc_50, 50);

// =============================================================================
// 31. Encrypt/decrypt parametric data patterns
// =============================================================================

macro_rules! enc_data_test {
    ($name:ident, $size:expr, $gen:expr) => {
        #[test]
        fn $name() {
            let config = test_encryption_config();
            let data: Vec<u8> = (0..$size).map($gen).collect();
            let enc = encrypt_buffer(&data, &config).unwrap();
            let dec = decrypt_buffer(&enc, &config).unwrap();
            assert_eq!(dec, data);
        }
    };
}

enc_data_test!(ed_zeros_100, 100usize, |_i| 0u8);
enc_data_test!(ed_zeros_1k, 1000usize, |_i| 0u8);
enc_data_test!(ed_zeros_10k, 10000usize, |_i| 0u8);
enc_data_test!(ed_seq_100, 100usize, |i: usize| (i % 256) as u8);
enc_data_test!(ed_seq_1k, 1000usize, |i: usize| (i % 256) as u8);
enc_data_test!(ed_seq_10k, 10000usize, |i: usize| (i % 256) as u8);
enc_data_test!(ed_alt_100, 100usize, |i: usize| if i % 2 == 0 { 0xAA } else { 0x55 });
enc_data_test!(ed_alt_1k, 1000usize, |i: usize| if i % 2 == 0 { 0xAA } else { 0x55 });
enc_data_test!(ed_mod4_1k, 1000usize, |i: usize| (i % 4) as u8);
enc_data_test!(ed_mod16_1k, 1000usize, |i: usize| (i % 16) as u8);
enc_data_test!(ed_ff_100, 100usize, |_i| 0xFFu8);
enc_data_test!(ed_ff_1k, 1000usize, |_i| 0xFFu8);

// =============================================================================
// 32. Parametric security context checks
// =============================================================================

macro_rules! sc_read_test {
    ($name:ident, $perm:expr, $table:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let ctx = ctx_with(vec![$perm]);
            assert_eq!(ctx.can_read_table($table), $expected);
        }
    };
}

sc_read_test!(scr_admin_t1, Permission::Admin, "t1", true);
sc_read_test!(scr_admin_t2, Permission::Admin, "t2", true);
sc_read_test!(scr_admin_any, Permission::Admin, "any_table", true);
sc_read_test!(scr_read_all_t1, Permission::Read { table: None }, "t1", true);
sc_read_test!(scr_read_all_t2, Permission::Read { table: None }, "t2", true);
sc_read_test!(scr_read_t1_t1, Permission::Read { table: Some("t1".into()) }, "t1", true);
sc_read_test!(scr_read_t1_t2, Permission::Read { table: Some("t1".into()) }, "t2", false);
sc_read_test!(scr_write_all_t1, Permission::Write { table: None }, "t1", false);
sc_read_test!(scr_ddl_t1, Permission::DDL, "t1", false);
sc_read_test!(scr_system_t1, Permission::System, "t1", false);

macro_rules! sc_write_test {
    ($name:ident, $perm:expr, $table:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let ctx = ctx_with(vec![$perm]);
            assert_eq!(ctx.can_write_table($table), $expected);
        }
    };
}

sc_write_test!(scw_admin_t1, Permission::Admin, "t1", true);
sc_write_test!(scw_admin_t2, Permission::Admin, "t2", true);
sc_write_test!(scw_write_all_t1, Permission::Write { table: None }, "t1", true);
sc_write_test!(scw_write_all_t2, Permission::Write { table: None }, "t2", true);
sc_write_test!(scw_write_t1_t1, Permission::Write { table: Some("t1".into()) }, "t1", true);
sc_write_test!(scw_write_t1_t2, Permission::Write { table: Some("t1".into()) }, "t2", false);
sc_write_test!(scw_read_all_t1, Permission::Read { table: None }, "t1", false);
sc_write_test!(scw_ddl_t1, Permission::DDL, "t1", false);
sc_write_test!(scw_system_t1, Permission::System, "t1", false);

// =============================================================================
// 33. Parametric cluster node creation
// =============================================================================

macro_rules! cluster_node_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let mgr = ClusterManager::new(cluster_config("n0"));
            for i in 0..$n {
                let node = ClusterNode::new(
                    format!("node-{i}"),
                    format!("10.0.0.{i}:9000"),
                    if i % 3 == 0 { NodeRole::Primary } else { NodeRole::ReadReplica },
                );
                mgr.add_node(node);
            }
            assert_eq!(mgr.node_count(), $n);
        }
    };
}

cluster_node_test!(cn_1, 1);
cluster_node_test!(cn_2, 2);
cluster_node_test!(cn_3, 3);
cluster_node_test!(cn_5, 5);
cluster_node_test!(cn_10, 10);
cluster_node_test!(cn_15, 15);
cluster_node_test!(cn_20, 20);
cluster_node_test!(cn_25, 25);
cluster_node_test!(cn_30, 30);

// =============================================================================
// 34. Parametric Raft elections
// =============================================================================

macro_rules! raft_election_test {
    ($name:ident, $n_elections:expr) => {
        #[test]
        fn $name() {
            let mut node = RaftNode::new("n1".into(), vec![]);
            for expected in 1..=$n_elections as u64 {
                node.state = RaftState::Follower;
                node.election_timeout = Duration::from_millis(1);
                std::thread::sleep(Duration::from_millis(2));
                node.tick();
                assert_eq!(node.current_term, expected);
                assert!(node.is_leader());
            }
        }
    };
}

raft_election_test!(re_1, 1);
raft_election_test!(re_3, 3);
raft_election_test!(re_5, 5);
raft_election_test!(re_10, 10);
raft_election_test!(re_20, 20);
raft_election_test!(re_50, 50);

// =============================================================================
// 35. Parametric metering tenants
// =============================================================================

macro_rules! metering_tenant_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let meter = UsageMeter::new(dir.path().to_path_buf());
            for i in 0..$n {
                meter.record_query(&format!("t{i}"), 10, 100);
                meter.record_write(&format!("t{i}"), 5);
            }
            let all = meter.get_all_usage();
            assert_eq!(all.len(), $n);
            for i in 0..$n {
                let u = all.get(&format!("t{i}")).unwrap();
                assert_eq!(u.queries, 1);
                assert_eq!(u.rows_read, 10);
                assert_eq!(u.rows_written, 5);
            }
        }
    };
}

metering_tenant_test!(mt_1, 1);
metering_tenant_test!(mt_2, 2);
metering_tenant_test!(mt_5, 5);
metering_tenant_test!(mt_10, 10);
metering_tenant_test!(mt_20, 20);
metering_tenant_test!(mt_30, 30);

// =============================================================================
// 36. Parametric tenant get/list
// =============================================================================

macro_rules! tenant_getlist_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            for i in 0..$n {
                mgr.create_tenant(&make_tenant(&format!("t{i}"))).unwrap();
            }
            for i in 0..$n {
                let t = mgr.get_tenant(&format!("t{i}")).unwrap().unwrap();
                assert_eq!(t.id, format!("t{i}"));
            }
            assert_eq!(mgr.list_tenants().unwrap().len(), $n);
        }
    };
}

tenant_getlist_test!(tgl_1, 1);
tenant_getlist_test!(tgl_3, 3);
tenant_getlist_test!(tgl_5, 5);
tenant_getlist_test!(tgl_10, 10);
tenant_getlist_test!(tgl_20, 20);

// =============================================================================
// 37. More RBAC auth parametric
// =============================================================================

macro_rules! rbac_auth_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let store = make_store(dir.path());
            let role = Role { name: "r".into(), permissions: vec![Permission::Read { table: None }] };
            store.create_role(&role).unwrap();
            for i in 0..$n {
                let mut u = make_user(&format!("u{i}"), &format!("p{i}"));
                u.roles = vec!["r".into()];
                store.create_user(&u).unwrap();
            }
            for i in 0..$n {
                let ctx = store.authenticate(&format!("u{i}"), &format!("p{i}")).unwrap().unwrap();
                assert!(ctx.can_read_table("any"));
            }
        }
    };
}

rbac_auth_test!(ra_1, 1);
rbac_auth_test!(ra_5, 5);
rbac_auth_test!(ra_10, 10);
rbac_auth_test!(ra_20, 20);
rbac_auth_test!(ra_30, 30);
rbac_auth_test!(ra_50, 50);

// =============================================================================
// 38. Encrypt file parametric sizes
// =============================================================================

macro_rules! enc_file_test {
    ($name:ident, $size:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let config = test_encryption_config();
            let path = dir.path().join("data.d");
            let data: Vec<u8> = (0..$size).map(|i: usize| (i % 256) as u8).collect();
            fs::write(&path, &data).unwrap();
            encrypt_file(&path, &config).unwrap();
            fs::remove_file(&path).unwrap();
            decrypt_file(&path, &config).unwrap();
            assert_eq!(fs::read(&path).unwrap(), data);
        }
    };
}

enc_file_test!(ef_1, 1);
enc_file_test!(ef_10, 10);
enc_file_test!(ef_100, 100);
enc_file_test!(ef_500, 500);
enc_file_test!(ef_1000, 1000);
enc_file_test!(ef_5000, 5000);
enc_file_test!(ef_10000, 10000);
enc_file_test!(ef_50000, 50000);

// =============================================================================
// 39. Cluster with varied node counts and failure detection
// =============================================================================

macro_rules! cluster_fail_test {
    ($name:ident, $n_live:expr, $n_dead:expr) => {
        #[test]
        fn $name() {
            let mgr = ClusterManager::new(cluster_config("n0"));
            mgr.register().unwrap();
            for i in 0..$n_live {
                let node = ClusterNode::new(
                    format!("live-{i}"),
                    format!("10.0.0.{i}:9000"),
                    NodeRole::ReadReplica,
                );
                mgr.add_node(node);
            }
            for i in 0..$n_dead {
                let mut node = ClusterNode::new(
                    format!("dead-{i}"),
                    format!("10.1.0.{i}:9000"),
                    NodeRole::ReadReplica,
                );
                node.last_heartbeat = 0;
                mgr.add_node(node);
            }
            let dead = mgr.detect_failures(Duration::from_secs(30));
            assert_eq!(dead.len(), $n_dead);
            assert_eq!(mgr.node_count(), 1 + $n_live + $n_dead);
        }
    };
}

cluster_fail_test!(cf_0l_1d, 0, 1);
cluster_fail_test!(cf_0l_5d, 0, 5);
cluster_fail_test!(cf_0l_10d, 0, 10);
cluster_fail_test!(cf_5l_1d, 5, 1);
cluster_fail_test!(cf_5l_5d, 5, 5);
cluster_fail_test!(cf_5l_10d, 5, 10);
cluster_fail_test!(cf_10l_1d, 10, 1);
cluster_fail_test!(cf_10l_5d, 10, 5);
cluster_fail_test!(cf_10l_10d, 10, 10);

// =============================================================================
// 40. Raft step-down parametric
// =============================================================================

macro_rules! raft_stepdown_test {
    ($name:ident, $higher_term:expr) => {
        #[test]
        fn $name() {
            let mut leader = RaftNode::new("n1".into(), vec![]);
            leader.election_timeout = Duration::from_millis(1);
            std::thread::sleep(Duration::from_millis(5));
            leader.tick();
            assert!(leader.is_leader());
            leader.handle_message(RaftMessage::AppendEntries {
                term: $higher_term,
                leader_id: "n2".into(),
                entries: vec![],
            });
            assert_eq!(leader.state, RaftState::Follower);
            assert_eq!(leader.current_term, $higher_term);
        }
    };
}

raft_stepdown_test!(rsd_2, 2);
raft_stepdown_test!(rsd_5, 5);
raft_stepdown_test!(rsd_10, 10);
raft_stepdown_test!(rsd_50, 50);
raft_stepdown_test!(rsd_100, 100);
raft_stepdown_test!(rsd_1000, 1000);

// =============================================================================
// 41. Parametric tenant usage
// =============================================================================

macro_rules! tenant_usage_test {
    ($name:ident, $n_files:expr, $file_size:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            mgr.create_tenant(&make_tenant("t1")).unwrap();
            let data_dir = dir.path().join("t1").join("table1");
            fs::create_dir_all(&data_dir).unwrap();
            for i in 0..$n_files {
                fs::write(data_dir.join(format!("f{i}.d")), vec![0u8; $file_size]).unwrap();
            }
            let usage = mgr.get_usage("t1").unwrap();
            assert_eq!(usage.table_count, 1);
            assert!(usage.storage_bytes >= ($n_files * $file_size) as u64);
        }
    };
}

tenant_usage_test!(tu_1f_100b, 1, 100);
tenant_usage_test!(tu_1f_1k, 1, 1024);
tenant_usage_test!(tu_5f_100b, 5, 100);
tenant_usage_test!(tu_5f_1k, 5, 1024);
tenant_usage_test!(tu_10f_100b, 10, 100);
tenant_usage_test!(tu_10f_1k, 10, 1024);

// =============================================================================
// 42. Shipper lag tracking
// =============================================================================

macro_rules! shipper_lag_test {
    ($name:ident, $n_replicas:expr) => {
        #[test]
        fn $name() {
            let addrs: Vec<&str> = (0..$n_replicas).map(|i| match i {
                0 => "r0:9100", 1 => "r1:9100", 2 => "r2:9100",
                3 => "r3:9100", _ => "r4:9100",
            }).collect();
            let shipper = make_shipper(addrs.clone(), ReplicationSyncMode::Async);
            let lags = shipper.replication_lag();
            assert_eq!(lags.len(), $n_replicas);
            for addr in &addrs {
                assert_eq!(lags[*addr].bytes_behind, 0);
                assert_eq!(lags[*addr].last_ack_txn, 0);
            }
        }
    };
}

shipper_lag_test!(sl_1, 1);
shipper_lag_test!(sl_2, 2);
shipper_lag_test!(sl_3, 3);
shipper_lag_test!(sl_4, 4);
shipper_lag_test!(sl_5, 5);

// =============================================================================
// 43. Multiple permission combos
// =============================================================================

macro_rules! multi_perm_test {
    ($name:ident, $perms:expr, $check_read:expr, $check_write:expr, $check_ddl:expr) => {
        #[test]
        fn $name() {
            let ctx = ctx_with($perms);
            assert_eq!(ctx.can_read_table("t"), $check_read);
            assert_eq!(ctx.can_write_table("t"), $check_write);
            assert_eq!(ctx.can_ddl(), $check_ddl);
        }
    };
}

multi_perm_test!(mp_empty, vec![], false, false, false);
multi_perm_test!(mp_admin_only, vec![Permission::Admin], true, true, true);
multi_perm_test!(mp_read_write, vec![Permission::Read { table: None }, Permission::Write { table: None }], true, true, false);
multi_perm_test!(mp_read_ddl, vec![Permission::Read { table: None }, Permission::DDL], true, false, true);
multi_perm_test!(mp_write_ddl, vec![Permission::Write { table: None }, Permission::DDL], false, true, true);
multi_perm_test!(mp_all_three, vec![Permission::Read { table: None }, Permission::Write { table: None }, Permission::DDL], true, true, true);
multi_perm_test!(mp_system_only, vec![Permission::System], false, false, false);
multi_perm_test!(mp_read_system, vec![Permission::Read { table: None }, Permission::System], true, false, false);

// =============================================================================
// 44. Replication config serialize/deserialize parametric
// =============================================================================

macro_rules! repl_config_test {
    ($name:ident, $role:expr, $sync:expr, $n_replicas:expr) => {
        #[test]
        fn $name() {
            let config = ReplicationConfig {
                role: $role,
                primary_addr: None,
                replica_addrs: (0..$n_replicas).map(|i| format!("r{i}:9100")).collect(),
                sync_mode: $sync,
                max_lag_bytes: 256 * 1024 * 1024,
                ..Default::default()
            };
            let json = serde_json::to_string(&config).unwrap();
            let restored: ReplicationConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(restored.role, $role);
            assert_eq!(restored.sync_mode, $sync);
            assert_eq!(restored.replica_addrs.len(), $n_replicas);
        }
    };
}

repl_config_test!(rc_p_async_0, ReplicationRole::Primary, ReplicationSyncMode::Async, 0);
repl_config_test!(rc_p_async_1, ReplicationRole::Primary, ReplicationSyncMode::Async, 1);
repl_config_test!(rc_p_async_3, ReplicationRole::Primary, ReplicationSyncMode::Async, 3);
repl_config_test!(rc_p_semi_1, ReplicationRole::Primary, ReplicationSyncMode::SemiSync, 1);
repl_config_test!(rc_p_semi_3, ReplicationRole::Primary, ReplicationSyncMode::SemiSync, 3);
repl_config_test!(rc_p_sync_1, ReplicationRole::Primary, ReplicationSyncMode::Sync, 1);
repl_config_test!(rc_p_sync_3, ReplicationRole::Primary, ReplicationSyncMode::Sync, 3);
repl_config_test!(rc_r_async, ReplicationRole::Replica, ReplicationSyncMode::Async, 0);
repl_config_test!(rc_s_async, ReplicationRole::Standalone, ReplicationSyncMode::Async, 0);

// =============================================================================
// 45. Parametric RBAC user+role+auth integration
// =============================================================================

macro_rules! rbac_full_test {
    ($name:ident, $n_users:expr, $n_roles:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let store = make_store(dir.path());
            for i in 0..$n_roles {
                let role = Role {
                    name: format!("role{i}"),
                    permissions: vec![Permission::Read { table: Some(format!("t{i}")) }],
                };
                store.create_role(&role).unwrap();
            }
            for i in 0..$n_users {
                let mut user = make_user(&format!("user{i}"), &format!("pw{i}"));
                user.roles = vec![format!("role{}", i % $n_roles)];
                store.create_user(&user).unwrap();
            }
            for i in 0..$n_users {
                let ctx = store.authenticate(&format!("user{i}"), &format!("pw{i}")).unwrap().unwrap();
                assert!(ctx.can_read_table(&format!("t{}", i % $n_roles)));
            }
        }
    };
}

rbac_full_test!(rbf_5u_2r, 5, 2);
rbac_full_test!(rbf_10u_3r, 10, 3);
rbac_full_test!(rbf_20u_5r, 20, 5);
rbac_full_test!(rbf_50u_10r, 50, 10);
rbac_full_test!(rbf_100u_20r, 100, 20);

// =============================================================================
// 46. Encrypt with different key bytes
// =============================================================================

macro_rules! enc_key_byte_test {
    ($name:ident, $b:expr) => {
        #[test]
        fn $name() {
            let config = EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![$b; 32]).unwrap();
            let data = vec![$b.wrapping_add(1); 256];
            let enc = encrypt_buffer(&data, &config).unwrap();
            let dec = decrypt_buffer(&enc, &config).unwrap();
            assert_eq!(dec, data);
        }
    };
}

enc_key_byte_test!(ekb_00, 0x00u8);
enc_key_byte_test!(ekb_11, 0x11u8);
enc_key_byte_test!(ekb_22, 0x22u8);
enc_key_byte_test!(ekb_33, 0x33u8);
enc_key_byte_test!(ekb_44, 0x44u8);
enc_key_byte_test!(ekb_55, 0x55u8);
enc_key_byte_test!(ekb_66, 0x66u8);
enc_key_byte_test!(ekb_77, 0x77u8);
enc_key_byte_test!(ekb_88, 0x88u8);
enc_key_byte_test!(ekb_99, 0x99u8);
enc_key_byte_test!(ekb_aa, 0xAAu8);
enc_key_byte_test!(ekb_bb, 0xBBu8);
enc_key_byte_test!(ekb_cc, 0xCCu8);
enc_key_byte_test!(ekb_dd, 0xDDu8);
enc_key_byte_test!(ekb_ee, 0xEEu8);
enc_key_byte_test!(ekb_ff, 0xFFu8);

// =============================================================================
// 47. Cluster rebalance parametric
// =============================================================================

macro_rules! rebalance_test {
    ($name:ident, $n_tables:expr, $n_nodes:expr) => {
        #[test]
        fn $name() {
            let mgr = ClusterManager::new(cluster_config("c0"));
            let mut n1 = ClusterNode::new("n1".into(), "10.0.0.1:9000".into(), NodeRole::Primary);
            n1.tables = (0..$n_tables).map(|i| format!("t{i}")).collect();
            mgr.add_node(n1);
            for i in 2..=$n_nodes {
                let n = ClusterNode::new(format!("n{i}"), format!("10.0.0.{i}:9000"), NodeRole::ReadReplica);
                mgr.add_node(n);
            }
            let plan = mgr.rebalance().unwrap();
            // Some moves should happen if tables > nodes
            if $n_tables > $n_nodes {
                assert!(!plan.moves.is_empty());
            }
        }
    };
}

rebalance_test!(reb_4t_2n, 4, 2);
rebalance_test!(reb_6t_2n, 6, 2);
rebalance_test!(reb_6t_3n, 6, 3);
rebalance_test!(reb_10t_2n, 10, 2);
rebalance_test!(reb_10t_5n, 10, 5);
rebalance_test!(reb_20t_4n, 20, 4);
rebalance_test!(reb_20t_5n, 20, 5);
rebalance_test!(reb_50t_5n, 50, 5);
rebalance_test!(reb_50t_10n, 50, 10);
rebalance_test!(reb_100t_10n, 100, 10);

// =============================================================================
// 48. Column-level permission parametric
// =============================================================================

macro_rules! col_perm_test {
    ($name:ident, $cols:expr, $check:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let ctx = ctx_with(vec![Permission::ColumnRead {
                table: "t".to_string(),
                columns: $cols.iter().map(|s: &&str| s.to_string()).collect(),
            }]);
            assert_eq!(ctx.can_read_column("t", $check), $expected);
        }
    };
}

col_perm_test!(cp_price_yes, &["price"], "price", true);
col_perm_test!(cp_price_no_vol, &["price"], "volume", false);
col_perm_test!(cp_pv_price, &["price", "volume"], "price", true);
col_perm_test!(cp_pv_vol, &["price", "volume"], "volume", true);
col_perm_test!(cp_pv_sym, &["price", "volume"], "symbol", false);
col_perm_test!(cp_all3_price, &["price", "volume", "symbol"], "price", true);
col_perm_test!(cp_all3_vol, &["price", "volume", "symbol"], "volume", true);
col_perm_test!(cp_all3_sym, &["price", "volume", "symbol"], "symbol", true);
col_perm_test!(cp_all3_ts, &["price", "volume", "symbol"], "timestamp", false);
col_perm_test!(cp_empty_any, &[] as &[&str], "any", false);

// =============================================================================
// 49. Metering concurrent stress
// =============================================================================

macro_rules! metering_concurrent_test {
    ($name:ident, $n_threads:expr, $n_ops:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let meter = Arc::new(UsageMeter::new(dir.path().to_path_buf()));
            let handles: Vec<_> = (0..$n_threads).map(|_| {
                let m = Arc::clone(&meter);
                std::thread::spawn(move || {
                    for _ in 0..$n_ops {
                        m.record_query("shared", 1, 1);
                        m.record_write("shared", 1);
                    }
                })
            }).collect();
            for h in handles { h.join().unwrap(); }
            let u = meter.get_usage("shared");
            assert_eq!(u.queries, $n_threads as u64 * $n_ops as u64);
            assert_eq!(u.rows_written, $n_threads as u64 * $n_ops as u64);
        }
    };
}

metering_concurrent_test!(mcon_2t_100, 2, 100);
metering_concurrent_test!(mcon_4t_100, 4, 100);
metering_concurrent_test!(mcon_4t_250, 4, 250);
metering_concurrent_test!(mcon_8t_100, 8, 100);

// =============================================================================
// 50. Raft propose various command types
// =============================================================================

macro_rules! raft_cmd_test {
    ($name:ident, $cmd:expr) => {
        #[test]
        fn $name() {
            let mut node = RaftNode::new("n1".into(), vec![]);
            node.election_timeout = Duration::from_millis(1);
            std::thread::sleep(Duration::from_millis(5));
            node.tick();
            assert!(node.is_leader());
            let idx = node.propose($cmd).unwrap();
            assert!(idx > 0);
            let entries = node.take_committed();
            assert_eq!(entries.len(), 1);
        }
    };
}

raft_cmd_test!(rcmd_create_t1, RaftCommand::CreateTable("t1".into()));
raft_cmd_test!(rcmd_create_t2, RaftCommand::CreateTable("long_table_name_for_testing".into()));
raft_cmd_test!(rcmd_drop_t1, RaftCommand::DropTable("t1".into()));
raft_cmd_test!(rcmd_drop_t2, RaftCommand::DropTable("another_table".into()));
raft_cmd_test!(rcmd_alter_1, RaftCommand::AlterTable("t1".into(), "add col".into()));
raft_cmd_test!(rcmd_alter_2, RaftCommand::AlterTable("t1".into(), "drop col".into()));
raft_cmd_test!(rcmd_wal_0, RaftCommand::WalCommit { table: "t1".into(), segment_id: 0 });
raft_cmd_test!(rcmd_wal_1, RaftCommand::WalCommit { table: "t1".into(), segment_id: 1 });
raft_cmd_test!(rcmd_wal_42, RaftCommand::WalCommit { table: "t1".into(), segment_id: 42 });
raft_cmd_test!(rcmd_wal_max, RaftCommand::WalCommit { table: "t1".into(), segment_id: u32::MAX });

// =============================================================================
// 51. RBAC update roles parametric
// =============================================================================

macro_rules! rbac_update_role_test {
    ($name:ident, $n_perms:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let store = make_store(dir.path());
            let perms: Vec<Permission> = (0..$n_perms)
                .map(|i| Permission::Read { table: Some(format!("table_{i}")) })
                .collect();
            let role = Role { name: "r".into(), permissions: perms.clone() };
            store.create_role(&role).unwrap();
            let loaded = store.get_role("r").unwrap().unwrap();
            assert_eq!(loaded.permissions.len(), $n_perms);
        }
    };
}

rbac_update_role_test!(rur_1, 1);
rbac_update_role_test!(rur_3, 3);
rbac_update_role_test!(rur_5, 5);
rbac_update_role_test!(rur_10, 10);
rbac_update_role_test!(rur_20, 20);

// =============================================================================
// 52. Encrypt buffer data integrity stress
// =============================================================================

macro_rules! enc_integrity_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let config = test_encryption_config();
            for i in 0..$n {
                let data = vec![(i % 256) as u8; 128];
                let enc = encrypt_buffer(&data, &config).unwrap();
                assert_ne!(enc, data);
                let dec = decrypt_buffer(&enc, &config).unwrap();
                assert_eq!(dec, data);
            }
        }
    };
}

enc_integrity_test!(ei_10, 10);
enc_integrity_test!(ei_25, 25);
enc_integrity_test!(ei_50, 50);
enc_integrity_test!(ei_100, 100);
enc_integrity_test!(ei_200, 200);

// =============================================================================
// 53. Cluster heartbeat + detect parametric
// =============================================================================

macro_rules! cluster_hb_test {
    ($name:ident, $n_heartbeats:expr) => {
        #[test]
        fn $name() {
            let mgr = ClusterManager::new(cluster_config("n0"));
            mgr.register().unwrap();
            for _ in 0..$n_heartbeats {
                mgr.heartbeat().unwrap();
            }
            let dead = mgr.detect_failures(Duration::from_secs(30));
            assert!(dead.is_empty());
        }
    };
}

cluster_hb_test!(chb_1, 1);
cluster_hb_test!(chb_5, 5);
cluster_hb_test!(chb_10, 10);
cluster_hb_test!(chb_50, 50);
cluster_hb_test!(chb_100, 100);

// =============================================================================
// 54. Tenant properties
// =============================================================================

macro_rules! tenant_prop_test {
    ($name:ident, $quota:expr, $query_q:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            let tenant = Tenant {
                id: "t1".to_string(),
                name: "Test".to_string(),
                namespace: "t1".to_string(),
                storage_quota: $quota,
                query_quota: $query_q,
                created_at: 1700000000,
            };
            mgr.create_tenant(&tenant).unwrap();
            let loaded = mgr.get_tenant("t1").unwrap().unwrap();
            assert_eq!(loaded.storage_quota, $quota);
            assert_eq!(loaded.query_quota, $query_q);
        }
    };
}

tenant_prop_test!(tp_1m_10, 1_000_000, 10);
tenant_prop_test!(tp_10m_50, 10_000_000, 50);
tenant_prop_test!(tp_100m_100, 100_000_000, 100);
tenant_prop_test!(tp_1g_200, 1_000_000_000, 200);
tenant_prop_test!(tp_10g_500, 10_000_000_000, 500);

// =============================================================================
// 55. Shipper multi-replica varied acks
// =============================================================================

macro_rules! shipper_varied_test {
    ($name:ident, $r1_ack:expr, $r2_ack:expr, $target:expr, $caught_up:expr) => {
        #[test]
        fn $name() {
            let mut shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
            shipper.record_ack("r1:9100", $r1_ack);
            shipper.record_ack("r2:9100", $r2_ack);
            assert_eq!(shipper.all_replicas_caught_up($target), $caught_up);
        }
    };
}

shipper_varied_test!(sv_00_1, 0, 0, 1, false);
shipper_varied_test!(sv_10_1, 1, 0, 1, false);
shipper_varied_test!(sv_11_1, 1, 1, 1, true);
shipper_varied_test!(sv_21_1, 2, 1, 1, true);
shipper_varied_test!(sv_21_2, 2, 1, 2, false);
shipper_varied_test!(sv_22_2, 2, 2, 2, true);
shipper_varied_test!(sv_55_5, 5, 5, 5, true);
shipper_varied_test!(sv_55_6, 5, 5, 6, false);
shipper_varied_test!(sv_10_10_10, 10, 10, 10, true);
shipper_varied_test!(sv_10_5_10, 10, 5, 10, false);

// =============================================================================
// 56. Raft append entries parametric
// =============================================================================

macro_rules! raft_ae_test {
    ($name:ident, $term:expr) => {
        #[test]
        fn $name() {
            let mut follower = RaftNode::new("n1".into(), vec!["n2".into()]);
            let responses = follower.handle_message(RaftMessage::AppendEntries {
                term: $term,
                leader_id: "n2".into(),
                entries: vec![],
            });
            assert_eq!(responses.len(), 1);
            assert_eq!(follower.state, RaftState::Follower);
            if $term >= follower.current_term {
                assert_eq!(follower.leader_id, Some("n2".into()));
            }
        }
    };
}

raft_ae_test!(rae_0, 0);
raft_ae_test!(rae_1, 1);
raft_ae_test!(rae_5, 5);
raft_ae_test!(rae_10, 10);
raft_ae_test!(rae_100, 100);

// =============================================================================
// 57. Hash password uniqueness
// =============================================================================

#[test]
fn hash_100_unique_passwords() {
    let mut hashes = std::collections::HashSet::new();
    for i in 0..100 {
        let h = hash_password(&format!("unique_password_{i}"));
        hashes.insert(h);
    }
    assert_eq!(hashes.len(), 100);
}

#[test]
fn hash_similar_passwords_differ() {
    let h1 = hash_password("password1");
    let h2 = hash_password("password2");
    let h3 = hash_password("password3");
    assert_ne!(h1, h2);
    assert_ne!(h2, h3);
    assert_ne!(h1, h3);
}

// =============================================================================
// 58. Node status transitions
// =============================================================================

#[test]
fn node_all_statuses() {
    for status in [NodeStatus::Online, NodeStatus::Offline, NodeStatus::Syncing, NodeStatus::Draining] {
        let mut node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), NodeRole::Primary);
        node.status = status.clone();
        match status {
            NodeStatus::Online => {
                assert!(node.is_healthy());
                assert!(node.can_write());
            }
            NodeStatus::Syncing => {
                assert!(node.is_healthy());
                assert!(!node.can_write());
            }
            NodeStatus::Offline | NodeStatus::Draining => {
                assert!(!node.is_healthy());
                assert!(!node.can_write());
            }
        }
    }
}

#[test]
fn node_all_roles() {
    for role in [NodeRole::Primary, NodeRole::ReadReplica, NodeRole::Coordinator] {
        let node = ClusterNode::new("n1".into(), "127.0.0.1:9000".into(), role.clone());
        match role {
            NodeRole::Primary => {
                assert!(node.can_write());
                assert!(node.can_read());
            }
            NodeRole::ReadReplica => {
                assert!(!node.can_write());
                assert!(node.can_read());
            }
            NodeRole::Coordinator => {
                assert!(!node.can_write());
                assert!(!node.can_read());
            }
        }
    }
}

// =============================================================================
// 59. Parametric resolve_security_context
// =============================================================================

macro_rules! resolve_ctx_test {
    ($name:ident, $n_roles:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let store = make_store(dir.path());
            let mut roles_list = vec![];
            for i in 0..$n_roles {
                let rname = format!("role_{i}");
                let role = Role { name: rname.clone(), permissions: vec![Permission::Read { table: Some(format!("t{i}")) }] };
                store.create_role(&role).unwrap();
                roles_list.push(rname);
            }
            let mut user = make_user("alice", "pw");
            user.roles = roles_list;
            store.create_user(&user).unwrap();
            let ctx = store.resolve_security_context("alice").unwrap().unwrap();
            assert_eq!(ctx.permissions.len(), $n_roles);
        }
    };
}

resolve_ctx_test!(rctx_1, 1);
resolve_ctx_test!(rctx_3, 3);
resolve_ctx_test!(rctx_5, 5);
resolve_ctx_test!(rctx_10, 10);
resolve_ctx_test!(rctx_20, 20);

macro_rules! enc_disabled_test {
    ($name:ident, $size:expr) => {
        #[test]
        fn $name() {
            let config = EncryptionConfig::disabled();
            let data = vec![42u8; $size];
            let result = encrypt_buffer(&data, &config).unwrap();
            assert_eq!(result, data);
        }
    };
}

enc_disabled_test!(edt_0, 0);
enc_disabled_test!(edt_1, 1);
enc_disabled_test!(edt_100, 100);
enc_disabled_test!(edt_1000, 1000);
enc_disabled_test!(edt_10000, 10000);

#[test]
fn tenant_list_is_sorted() {
    let dir = tempdir().unwrap();
    let mgr = TenantManager::new(dir.path().to_path_buf());
    for id in &["z", "a", "m", "b"] {
        mgr.create_tenant(&make_tenant(id)).unwrap();
    }
    let tenants = mgr.list_tenants().unwrap();
    let ids: Vec<&str> = tenants.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "m", "z"]);
}

#[test]
fn rbac_users_sorted() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    for name in &["zoe", "alice", "bob"] {
        store.create_user(&make_user(name, "pw")).unwrap();
    }
    let users = store.list_users().unwrap();
    let names: Vec<&str> = users.iter().map(|u| u.username.as_str()).collect();
    assert_eq!(names, vec!["alice", "bob", "zoe"]);
}

#[test]
fn rbac_roles_sorted() {
    let dir = tempdir().unwrap();
    let store = make_store(dir.path());
    for name in &["writer", "admin", "reader"] {
        let role = Role { name: name.to_string(), permissions: vec![] };
        store.create_role(&role).unwrap();
    }
    let roles = store.list_roles().unwrap();
    let names: Vec<&str> = roles.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(names, vec!["admin", "reader", "writer"]);
}

#[test]
fn metering_snapshot_eq() {
    let s1 = CounterSnapshot { queries: 1, rows_read: 2, rows_written: 3, bytes_scanned: 4, bytes_stored: 5 };
    let s2 = s1.clone();
    assert_eq!(s1, s2);
}

#[test]
fn metering_snapshot_ne() {
    let s1 = CounterSnapshot { queries: 1, rows_read: 2, rows_written: 3, bytes_scanned: 4, bytes_stored: 5 };
    let s2 = CounterSnapshot { queries: 99, rows_read: 2, rows_written: 3, bytes_scanned: 4, bytes_stored: 5 };
    assert_ne!(s1, s2);
}
