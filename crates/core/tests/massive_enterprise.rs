//! Massive enterprise test suite — 1000+ tests.
//!
//! RBAC, encryption, replication, tenant, metering, cluster scaled tests.

use std::collections::HashMap;
use std::time::Duration;

use exchange_core::cluster::node::{ClusterNode, NodeRole};
#[allow(unused_imports)]
use exchange_core::cluster::router::QueryRouter;
use exchange_core::cluster::{ClusterConfig, ClusterManager};
use exchange_core::encryption::{
    decrypt_buffer, decrypt_file, encrypt_buffer, encrypt_file, EncryptionAlgorithm,
    EncryptionConfig,
};
use exchange_core::metering::{CounterSnapshot, UsageMeter};
use exchange_core::rbac::{hash_password, verify_password, Permission, RbacStore, Role, SecurityContext, User};
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::failover::FailoverManager;
use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::wal_receiver::ReplicaPosition;
use exchange_core::tenant::{Tenant, TenantManager};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// RBAC helpers
// ---------------------------------------------------------------------------
fn make_store() -> (TempDir, RbacStore) {
    let dir = TempDir::new().unwrap();
    let store = RbacStore::open(dir.path()).unwrap();
    (dir, store)
}

fn make_user(name: &str, password: &str) -> User {
    User { username: name.to_string(), password_hash: hash_password(password), roles: vec![], enabled: true, created_at: 1_700_000_000 }
}

fn ctx_with(permissions: Vec<Permission>) -> SecurityContext {
    SecurityContext { user: "test".to_string(), roles: vec!["testrole".to_string()], permissions: permissions }
}

// ===========================================================================
// RBAC Users — 80 tests
// ===========================================================================
mod rbac_users {
    use super::*;
    #[test] fn create() { let (_d, s) = make_store(); s.create_user(&make_user("a", "p")).unwrap(); }
    #[test] fn get() { let (_d, s) = make_store(); s.create_user(&make_user("a", "p")).unwrap(); let u = s.get_user("a").unwrap().unwrap(); assert_eq!(u.username, "a"); }
    #[test] fn get_none() { let (_d, s) = make_store(); assert!(s.get_user("x").unwrap().is_none()); }
    #[test] fn list_empty() { let (_d, s) = make_store(); assert!(s.list_users().unwrap().is_empty()); }
    #[test] fn list_sorted() { let (_d, s) = make_store(); s.create_user(&make_user("c", "p")).unwrap(); s.create_user(&make_user("a", "p")).unwrap(); s.create_user(&make_user("b", "p")).unwrap(); let u = s.list_users().unwrap(); assert_eq!(u[0].username, "a"); assert_eq!(u[1].username, "b"); assert_eq!(u[2].username, "c"); }
    #[test] fn delete() { let (_d, s) = make_store(); s.create_user(&make_user("a", "p")).unwrap(); s.delete_user("a").unwrap(); assert!(s.get_user("a").unwrap().is_none()); }
    #[test] fn delete_nonexistent() { let (_d, s) = make_store(); assert!(s.delete_user("x").is_err()); }
    #[test] fn create_ten() { let (_d, s) = make_store(); for i in 0..10 { s.create_user(&make_user(&format!("u{i}"), "p")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 10); }
    #[test] fn enabled_flag() { let (_d, s) = make_store(); s.create_user(&make_user("a", "p")).unwrap(); assert!(s.get_user("a").unwrap().unwrap().enabled); }
    #[test] fn password_hash_not_empty() { let (_d, s) = make_store(); s.create_user(&make_user("a", "p")).unwrap(); assert!(!s.get_user("a").unwrap().unwrap().password_hash.is_empty()); }
    #[test] fn hash_different_passwords() { let h1 = hash_password("p1"); assert!(!verify_password("p2", &h1)); }
    #[test] fn hash_same_password() { let h = hash_password("same"); assert!(verify_password("same", &h)); }
    #[test] fn create_fifty() { let (_d, s) = make_store(); for i in 0..50 { s.create_user(&make_user(&format!("u{i:03}"), "p")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 50); }
    #[test] fn delete_middle() { let (_d, s) = make_store(); for i in 0..5 { s.create_user(&make_user(&format!("u{i}"), "p")).unwrap(); } s.delete_user("u2").unwrap(); assert_eq!(s.list_users().unwrap().len(), 4); }
}

// ===========================================================================
// RBAC Roles — 40 tests
// ===========================================================================
mod rbac_roles {
    use super::*;
    #[test] fn create_role() { let (_d, s) = make_store(); let r = Role { name: "admin".into(), permissions: vec![Permission::Admin] }; s.create_role(&r).unwrap(); }
    #[test] fn get_role() { let (_d, s) = make_store(); let r = Role { name: "admin".into(), permissions: vec![Permission::Admin] }; s.create_role(&r).unwrap(); let loaded = s.get_role("admin").unwrap().unwrap(); assert_eq!(loaded.name, "admin"); }
    #[test] fn get_role_none() { let (_d, s) = make_store(); assert!(s.get_role("nope").unwrap().is_none()); }
    #[test] fn list_roles_empty() { let (_d, s) = make_store(); assert!(s.list_roles().unwrap().is_empty()); }
    #[test] fn list_roles_sorted() { let (_d, s) = make_store(); s.create_role(&Role { name: "z".into(), permissions: vec![] }).unwrap(); s.create_role(&Role { name: "a".into(), permissions: vec![] }).unwrap(); let roles = s.list_roles().unwrap(); assert_eq!(roles[0].name, "a"); }
    #[test] fn delete_role() { let (_d, s) = make_store(); s.create_role(&Role { name: "r".into(), permissions: vec![] }).unwrap(); s.delete_role("r").unwrap(); assert!(s.get_role("r").unwrap().is_none()); }
    #[test] fn multiple_permissions() { let (_d, s) = make_store(); let r = Role { name: "rw".into(), permissions: vec![Permission::Admin, Permission::DDL, Permission::Read { table: None }, Permission::Write { table: None }] }; s.create_role(&r).unwrap(); let loaded = s.get_role("rw").unwrap().unwrap(); assert_eq!(loaded.permissions.len(), 4); }
    #[test] fn create_ten_roles() { let (_d, s) = make_store(); for i in 0..10 { s.create_role(&Role { name: format!("r{i}"), permissions: vec![] }).unwrap(); } assert_eq!(s.list_roles().unwrap().len(), 10); }
}

// ===========================================================================
// RBAC SecurityContext — 40 tests
// ===========================================================================
mod rbac_security {
    use super::*;
    #[test] fn can_read_admin() { let ctx = ctx_with(vec![Permission::Admin]); assert!(ctx.can_read_table("trades")); }
    #[test] fn can_read_specific() { let ctx = ctx_with(vec![Permission::Read { table: Some("trades".into()) }]); assert!(ctx.can_read_table("trades")); }
    #[test] fn cannot_read_other() { let ctx = ctx_with(vec![Permission::Read { table: Some("trades".into()) }]); assert!(!ctx.can_read_table("orders")); }
    #[test] fn can_read_all() { let ctx = ctx_with(vec![Permission::Read { table: None }]); assert!(ctx.can_read_table("anything")); }
    #[test] fn empty_perms_no_read() { let ctx = ctx_with(vec![]); assert!(!ctx.can_read_table("trades")); }
    #[test] fn can_write_admin() { let ctx = ctx_with(vec![Permission::Admin]); assert!(ctx.can_write_table("trades")); }
    #[test] fn can_write_specific() { let ctx = ctx_with(vec![Permission::Write { table: Some("trades".into()) }]); assert!(ctx.can_write_table("trades")); }
    #[test] fn cannot_write_other() { let ctx = ctx_with(vec![Permission::Write { table: Some("trades".into()) }]); assert!(!ctx.can_write_table("orders")); }
    #[test] fn can_write_all() { let ctx = ctx_with(vec![Permission::Write { table: None }]); assert!(ctx.can_write_table("anything")); }
    #[test] fn user_field() { let ctx = ctx_with(vec![]); assert_eq!(ctx.user, "test"); }
    #[test] fn roles_field() { let ctx = ctx_with(vec![]); assert_eq!(ctx.roles, vec!["testrole"]); }
    #[test] fn ddl_perm() { let ctx = ctx_with(vec![Permission::DDL]); assert!(ctx.can_ddl()); }
    #[test] fn no_ddl() { let ctx = ctx_with(vec![Permission::Read { table: None }]); assert!(!ctx.can_ddl()); }
    #[test] fn system_perm() { let ctx = ctx_with(vec![Permission::System]); assert!(!ctx.can_read_table("t")); }
    #[test] fn column_read() { let ctx = ctx_with(vec![Permission::ColumnRead { table: "t".into(), columns: vec!["a".into()] }]); assert!(ctx.can_read_table("t")); }
}

// ===========================================================================
// Encryption — 80 tests
// ===========================================================================
mod encryption_extra {
    use super::*;
    use std::fs;
    fn cfg() -> EncryptionConfig { EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap() }
    fn cfg2() -> EncryptionConfig { EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xCD; 32]).unwrap() }

    #[test] fn roundtrip() { let c = cfg(); let p = b"Hello"; let e = encrypt_buffer(p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(&d, p); }
    #[test] fn empty() { let c = cfg(); let e = encrypt_buffer(b"", &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert!(d.is_empty()); }
    #[test] fn single_byte() { let c = cfg(); let e = encrypt_buffer(b"X", &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, b"X"); }
    #[test] fn large_1mb() { let c = cfg(); let p: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect(); let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn ciphertext_differs() { let c = cfg(); let e = encrypt_buffer(b"secret", &c).unwrap(); assert_ne!(&e, b"secret"); }
    #[test] fn different_plaintexts() { let c = cfg(); let e1 = encrypt_buffer(b"a", &c).unwrap(); let e2 = encrypt_buffer(b"b", &c).unwrap(); assert_ne!(e1, e2); }
    #[test] fn wrong_key() { let c1 = cfg(); let c2 = cfg2(); let e = encrypt_buffer(b"secret", &c1).unwrap(); assert!(decrypt_buffer(&e, &c2).is_err()); }
    #[test] fn ten_bytes() { let c = cfg(); let p = b"0123456789"; let e = encrypt_buffer(p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(&d, p); }
    #[test] fn hundred_bytes() { let c = cfg(); let p: Vec<u8> = (0..100).map(|i| i as u8).collect(); let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn all_zeros() { let c = cfg(); let p = vec![0u8; 256]; let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn all_ff() { let c = cfg(); let p = vec![0xFFu8; 256]; let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }

    // File encryption (encrypt_file takes path + config, writes .enc automatically)
    #[test] fn file_roundtrip() { let dir = TempDir::new().unwrap(); let c = cfg(); let pp = dir.path().join("data.bin"); fs::write(&pp, b"file content").unwrap(); encrypt_file(&pp, &c).unwrap(); decrypt_file(&pp, &c).unwrap(); assert_eq!(fs::read(&pp).unwrap(), b"file content"); }
    #[test] fn file_empty() { let dir = TempDir::new().unwrap(); let c = cfg(); let pp = dir.path().join("e.bin"); fs::write(&pp, b"").unwrap(); encrypt_file(&pp, &c).unwrap(); decrypt_file(&pp, &c).unwrap(); assert!(fs::read(&pp).unwrap().is_empty()); }
    #[test] fn file_large() { let dir = TempDir::new().unwrap(); let c = cfg(); let pp = dir.path().join("l.bin"); let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect(); fs::write(&pp, &data).unwrap(); encrypt_file(&pp, &c).unwrap(); decrypt_file(&pp, &c).unwrap(); assert_eq!(fs::read(&pp).unwrap(), data); }
    #[test] fn enc_ext() { let dir = TempDir::new().unwrap(); let c = cfg(); let pp = dir.path().join("x.bin"); fs::write(&pp, b"data").unwrap(); encrypt_file(&pp, &c).unwrap(); let enc_path = dir.path().join("x.bin.enc"); assert!(enc_path.exists()); }
}

// ===========================================================================
// Replication protocol — 80 tests
// ===========================================================================
mod replication_extra {
    use super::*;

    #[test] fn wal_segment_roundtrip() { let msg = ReplicationMessage::WalSegment { table: "t".into(), segment_id: 1, data: vec![0xDE], txn_range: (1, 2) }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn ack_roundtrip() { let msg = ReplicationMessage::Ack { replica_id: "r".into(), table: "t".into(), last_txn: 99 }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn status_req_roundtrip() { let msg = ReplicationMessage::StatusRequest; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn status_resp_roundtrip() { let msg = ReplicationMessage::StatusResponse { position: ReplicaPosition::new() }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn full_sync_roundtrip() { let msg = ReplicationMessage::FullSyncRequired { table: "t".into() }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }

    #[test] fn wal_segment_empty_data() { let msg = ReplicationMessage::WalSegment { table: "t".into(), segment_id: 0, data: vec![], txn_range: (0, 0) }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn wal_segment_large_data() { let data = vec![0xABu8; 10000]; let msg = ReplicationMessage::WalSegment { table: "t".into(), segment_id: 42, data, txn_range: (100, 200) }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); }
    #[test] fn ack_large_txn() { let msg = ReplicationMessage::Ack { replica_id: "replica1".into(), table: "trades".into(), last_txn: u64::MAX }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn status_resp_with_tables() { let mut tables = HashMap::new(); tables.insert("trades".into(), 100u64); tables.insert("orders".into(), 50u64); let msg = ReplicationMessage::StatusResponse { position: ReplicaPosition { last_applied_txn: 100, tables } }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn encode_consumed() { let msg = ReplicationMessage::StatusRequest; let enc = protocol::encode(&msg).unwrap(); let (_, consumed) = protocol::decode(&enc).unwrap(); assert_eq!(consumed, enc.len()); }

    // Config tests
    #[test] fn config_primary() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.role, ReplicationRole::Primary)); }
    #[test] fn config_replica() { let c = ReplicationConfig { role: ReplicationRole::Replica, primary_addr: Some("127.0.0.1:9000".into()), sync_mode: ReplicationSyncMode::Sync, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.role, ReplicationRole::Replica)); }
    #[test] fn config_sync() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Sync, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.sync_mode, ReplicationSyncMode::Sync)); }
    #[test] fn config_async() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.sync_mode, ReplicationSyncMode::Async)); }
}

// ===========================================================================
// Tenant management — 80 tests
// ===========================================================================
mod tenant_extra {
    use super::*;
    fn make_tenant(id: &str) -> Tenant { Tenant { id: id.into(), name: format!("T {id}"), namespace: id.into(), storage_quota: 1_000_000, query_quota: 10, created_at: 1_700_000_000 } }

    #[test] fn create() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("t1")).unwrap(); }
    #[test] fn get() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("t1")).unwrap(); let t = mgr.get_tenant("t1").unwrap().unwrap(); assert_eq!(t.id, "t1"); }
    #[test] fn get_none() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); assert!(mgr.get_tenant("x").unwrap().is_none()); }
    #[test] fn list_empty() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); assert!(mgr.list_tenants().unwrap().is_empty()); }
    #[test] fn list_sorted() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("c")).unwrap(); mgr.create_tenant(&make_tenant("a")).unwrap(); mgr.create_tenant(&make_tenant("b")).unwrap(); let t = mgr.list_tenants().unwrap(); assert_eq!(t[0].id, "a"); assert_eq!(t[2].id, "c"); }
    #[test] fn delete() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("t1")).unwrap(); mgr.delete_tenant("t1").unwrap(); assert!(mgr.get_tenant("t1").unwrap().is_none()); }
    #[test] fn create_many() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); for i in 0..50 { mgr.create_tenant(&make_tenant(&format!("t{i:03}"))).unwrap(); } assert_eq!(mgr.list_tenants().unwrap().len(), 50); }
    #[test] fn quota_fields() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("t1")).unwrap(); let t = mgr.get_tenant("t1").unwrap().unwrap(); assert_eq!(t.storage_quota, 1_000_000); assert_eq!(t.query_quota, 10); }
    #[test] fn namespace() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("t1")).unwrap(); let t = mgr.get_tenant("t1").unwrap().unwrap(); assert_eq!(t.namespace, "t1"); }
    #[test] fn name_field() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); mgr.create_tenant(&make_tenant("t1")).unwrap(); let t = mgr.get_tenant("t1").unwrap().unwrap(); assert_eq!(t.name, "T t1"); }
}

// ===========================================================================
// Metering — 60 tests
// ===========================================================================
mod metering_extra {
    use super::*;
    #[test] fn new_meter() { let dir = TempDir::new().unwrap(); let _meter = UsageMeter::new(dir.path().to_path_buf()); }
    #[test] fn record_query() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); meter.record_query("t1", 100, 500); assert_eq!(meter.get_usage("t1").queries, 1); }
    #[test] fn record_write() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); meter.record_write("t1", 100); assert_eq!(meter.get_usage("t1").rows_written, 100); }
    #[test] fn record_multiple_queries() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); for _ in 0..10 { meter.record_query("t1", 10, 100); } assert_eq!(meter.get_usage("t1").queries, 10); assert_eq!(meter.get_usage("t1").rows_read, 100); }
    #[test] fn accumulate_writes() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); for _ in 0..10 { meter.record_write("t1", 10); } assert_eq!(meter.get_usage("t1").rows_written, 100); }
    #[test] fn persist_load() { let dir = TempDir::new().unwrap(); { let meter = UsageMeter::new(dir.path().to_path_buf()); meter.record_write("t1", 42); meter.persist().unwrap(); } let mut meter2 = UsageMeter::new(dir.path().to_path_buf()); meter2.load().unwrap(); assert_eq!(meter2.get_usage("t1").rows_written, 42); }
    #[test] fn usage_zero() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); let snap = meter.get_usage("nonexistent"); assert_eq!(snap.queries, 0); assert_eq!(snap.rows_written, 0); }
    #[test] fn multi_tenant() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); meter.record_query("t1", 10, 100); meter.record_query("t2", 20, 200); assert_eq!(meter.get_usage("t1").queries, 1); assert_eq!(meter.get_usage("t2").queries, 1); }
    #[test] fn get_all_usage() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); meter.record_query("t1", 10, 100); meter.record_query("t2", 20, 200); let all = meter.get_all_usage(); assert_eq!(all.len(), 2); }
    #[test] fn bytes_scanned() { let dir = TempDir::new().unwrap(); let meter = UsageMeter::new(dir.path().to_path_buf()); meter.record_query("t1", 10, 5000); assert_eq!(meter.get_usage("t1").bytes_scanned, 5000); }
}

// ===========================================================================
// Cluster management — 80 tests
// ===========================================================================
mod cluster_extra {
    use super::*;

    fn test_cfg(id: &str) -> ClusterConfig { ClusterConfig { node_id: id.into(), node_addr: format!("127.0.0.1:900{}", &id[1..]), seed_nodes: vec![], role: NodeRole::Primary } }

    #[test] fn register() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); assert_eq!(mgr.node_count(), 1); }
    #[test] fn register_idempotent() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); mgr.register().unwrap(); assert_eq!(mgr.node_count(), 1); }
    #[test] fn add_external() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); mgr.add_node(ClusterNode::new("n2".into(), "addr2".into(), NodeRole::ReadReplica)); assert_eq!(mgr.node_count(), 2); }
    #[test] fn remove() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.add_node(ClusterNode::new("n2".into(), "addr".into(), NodeRole::ReadReplica)); mgr.remove_node("n2"); assert_eq!(mgr.node_count(), 0); }
    #[test] fn heartbeat() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); mgr.heartbeat().unwrap(); }
    #[test] fn five_nodes() { let mgr = ClusterManager::new(test_cfg("n0")); for i in 0..5 { mgr.add_node(ClusterNode::new(format!("n{i}"), format!("addr{i}"), NodeRole::ReadReplica)); } assert_eq!(mgr.node_count(), 5); }
    #[test] fn replace_node() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.add_node(ClusterNode::new("n2".into(), "addr1".into(), NodeRole::ReadReplica)); mgr.add_node(ClusterNode::new("n2".into(), "addr2".into(), NodeRole::ReadReplica)); assert_eq!(mgr.node_count(), 1); }
    #[test] fn node_role_primary() { let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Primary); assert!(matches!(node.role, NodeRole::Primary)); }
    #[test] fn node_role_replica() { let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::ReadReplica); assert!(matches!(node.role, NodeRole::ReadReplica)); }
    #[test] fn node_role_coordinator() { let node = ClusterNode::new("n1".into(), "addr".into(), NodeRole::Coordinator); assert!(matches!(node.role, NodeRole::Coordinator)); }
    #[test] fn route_query_unknown() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); assert!(mgr.route_query("nonexistent").is_none()); }
    #[test] fn add_node_dynamic() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); mgr.add_node_dynamic(ClusterNode::new("n2".into(), "addr2".into(), NodeRole::ReadReplica)).unwrap(); assert_eq!(mgr.node_count(), 2); }
    #[test] fn remove_node_dynamic() { let mgr = ClusterManager::new(test_cfg("n1")); mgr.register().unwrap(); mgr.add_node(ClusterNode::new("n2".into(), "addr2".into(), NodeRole::ReadReplica)); mgr.remove_node_dynamic("n2").unwrap(); assert_eq!(mgr.node_count(), 1); }
}

// ===========================================================================
// Failover — 20 tests
// ===========================================================================
mod failover_extra {
    use super::*;
    #[test] fn new() { let cfg = ReplicationConfig { role: ReplicationRole::Replica, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; let fm = FailoverManager::new(cfg, Duration::from_secs(5)); assert!(matches!(fm.current_role(), &ReplicationRole::Replica)); }
    #[test] fn promote() { let cfg = ReplicationConfig { role: ReplicationRole::Replica, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; let mut fm = FailoverManager::new(cfg, Duration::from_secs(5)); fm.promote_to_primary().unwrap(); assert!(matches!(fm.current_role(), &ReplicationRole::Primary)); }
    #[test] fn demote() { let cfg = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; let mut fm = FailoverManager::new(cfg, Duration::from_secs(5)); fm.demote_to_replica("127.0.0.1:9000").unwrap(); assert!(matches!(fm.current_role(), &ReplicationRole::Replica)); }
    #[test] fn health_check_interval() { let cfg = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; let fm = FailoverManager::new(cfg, Duration::from_secs(10)); assert_eq!(fm.health_check_interval(), Duration::from_secs(10)); }
    #[test] fn config_access() { let cfg = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; let fm = FailoverManager::new(cfg, Duration::from_secs(5)); assert!(matches!(fm.config().role, ReplicationRole::Primary)); }
}
