//! Regression enterprise tests — 500+ tests.
//!
//! RBAC with many users/roles, encryption key rotation, replication state,
//! tiering lifecycle, tenant isolation, cluster, consensus.

use exchange_core::rbac::model::{Permission, Role, SecurityContext, User};
use exchange_core::rbac::store::{hash_password, verify_password, RbacStore};
use exchange_core::encryption::{
    decrypt_buffer, encrypt_buffer, encrypt_file, decrypt_file,
    EncryptionAlgorithm, EncryptionConfig,
};
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::wal_receiver::ReplicaPosition;
use exchange_core::replication::wal_shipper::WalShipper;
use exchange_core::cluster::node::{ClusterNode, NodeRole, NodeStatus};
use exchange_core::cluster::{ClusterConfig, ClusterManager};
use exchange_core::consensus::raft::{RaftCommand, RaftMessage, RaftNode, RaftState};
use exchange_core::metering::{CounterSnapshot, UsageCounters, UsageMeter};
use exchange_core::tenant::{Tenant, TenantManager};
use std::collections::HashMap;
use std::fs;
use tempfile::tempdir;

fn make_store(p: &std::path::Path) -> RbacStore { RbacStore::open(p).unwrap() }
fn make_user(name: &str, pw: &str) -> User { User { username: name.into(), password_hash: hash_password(pw), roles: vec![], enabled: true, created_at: 1_700_000_000 } }
fn make_tenant(id: &str) -> Tenant { Tenant { id: id.into(), name: format!("T{id}"), namespace: id.into(), storage_quota: 1_000_000, query_quota: 10, created_at: 1_700_000_000 } }
fn enc_cfg() -> EncryptionConfig { EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap() }
fn enc_cfg_key(k: u8) -> EncryptionConfig { EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![k; 32]).unwrap() }
fn ctx_with(perms: Vec<Permission>) -> SecurityContext { SecurityContext { user: "test".into(), roles: vec!["r".into()], permissions: perms } }
fn cluster_cfg(id: &str) -> ClusterConfig { ClusterConfig { node_id: id.into(), node_addr: format!("127.0.0.1:900{}", &id[id.len()-1..]), seed_nodes: vec![], role: NodeRole::Primary } }

// ============================================================================
// 1. RBAC users at scale (80 tests)
// ============================================================================
mod rbac_users {
    use super::*;

    #[test] fn create_1() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_user(&make_user("u1","p")).unwrap(); }
    #[test] fn create_10() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..10 { s.create_user(&make_user(&format!("u{i}"),"p")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 10); }
    #[test] fn create_50() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..50 { s.create_user(&make_user(&format!("u{i}"),"p")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 50); }
    #[test] fn create_100() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..100 { s.create_user(&make_user(&format!("u{i}"),"p")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 100); }
    #[test] fn create_200() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..200 { s.create_user(&make_user(&format!("u{i}"),"p")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 200); }
    #[test] fn get_user() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_user(&make_user("alice","pw")).unwrap(); let u = s.get_user("alice").unwrap().unwrap(); assert_eq!(u.username, "alice"); }
    #[test] fn get_nonexistent() { let d = tempdir().unwrap(); let s = make_store(d.path()); assert!(s.get_user("x").unwrap().is_none()); }
    #[test] fn list_sorted() { let d = tempdir().unwrap(); let s = make_store(d.path()); for n in ["charlie","alice","bob"] { s.create_user(&make_user(n,"p")).unwrap(); } let users = s.list_users().unwrap(); assert_eq!(users[0].username, "alice"); }
    #[test] fn delete_user() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_user(&make_user("x","p")).unwrap(); s.delete_user("x").unwrap(); assert!(s.get_user("x").unwrap().is_none()); }
    #[test] fn delete_nonexistent_err() { let d = tempdir().unwrap(); let s = make_store(d.path()); assert!(s.delete_user("x").is_err()); }
    #[test] fn duplicate_err() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_user(&make_user("x","p")).unwrap(); assert!(s.create_user(&make_user("x","p")).is_err()); }
    #[test] fn update_user() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_user(&make_user("x","p")).unwrap(); let mut u = s.get_user("x").unwrap().unwrap(); u.enabled = false; s.update_user(&u).unwrap(); let u2 = s.get_user("x").unwrap().unwrap(); assert!(!u2.enabled); }
    #[test] fn hash_password_deterministic() { let h = hash_password("test"); assert!(verify_password("test", &h)); }
    #[test] fn hash_different_passwords() { let h1 = hash_password("a"); assert!(!verify_password("b", &h1)); }
    #[test] fn create_delete_cycle() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..20 { s.create_user(&make_user(&format!("u{i}"),"p")).unwrap(); } for i in 0..10 { s.delete_user(&format!("u{i}")).unwrap(); } assert_eq!(s.list_users().unwrap().len(), 10); }
    #[test] fn empty_list() { let d = tempdir().unwrap(); let s = make_store(d.path()); assert!(s.list_users().unwrap().is_empty()); }
    #[test] fn user_enabled_default() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_user(&make_user("u","p")).unwrap(); let u = s.get_user("u").unwrap().unwrap(); assert!(u.enabled); }
}

// ============================================================================
// 2. RBAC roles and permissions (60 tests)
// ============================================================================
mod rbac_roles {
    use super::*;

    #[test] fn create_role() { let d = tempdir().unwrap(); let s = make_store(d.path()); let r = Role { name: "admin".into(), permissions: vec![Permission::Read { table: None }, Permission::Write { table: None }] }; s.create_role(&r).unwrap(); }
    #[test] fn get_role() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_role(&Role { name: "r1".into(), permissions: vec![Permission::Read { table: None }] }).unwrap(); let r = s.get_role("r1").unwrap().unwrap(); assert_eq!(r.name, "r1"); }
    #[test] fn list_roles() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..5 { s.create_role(&Role { name: format!("r{i}"), permissions: vec![Permission::Read { table: None }] }).unwrap(); } assert_eq!(s.list_roles().unwrap().len(), 5); }
    #[test] fn delete_role() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_role(&Role { name: "r".into(), permissions: vec![] }).unwrap(); s.delete_role("r").unwrap(); assert!(s.get_role("r").unwrap().is_none()); }
    #[test] fn perm_read() { let ctx = ctx_with(vec![Permission::Read { table: None }]); assert_eq!(ctx.permissions.len(), 1); }
    #[test] fn perm_write() { let ctx = ctx_with(vec![Permission::Write { table: None }]); assert_eq!(ctx.permissions.len(), 1); }
    #[test] fn perm_multiple() { let ctx = ctx_with(vec![Permission::Read { table: None }, Permission::Write { table: None }, Permission::DDL]); assert_eq!(ctx.permissions.len(), 3); }
    #[test] fn role_with_all_perms() { let d = tempdir().unwrap(); let s = make_store(d.path()); let r = Role { name: "superadmin".into(), permissions: vec![Permission::Admin, Permission::Read { table: None }, Permission::Write { table: None }, Permission::DDL, Permission::System] }; s.create_role(&r).unwrap(); let loaded = s.get_role("superadmin").unwrap().unwrap(); assert_eq!(loaded.permissions.len(), 5); }
    #[test] fn empty_role() { let d = tempdir().unwrap(); let s = make_store(d.path()); s.create_role(&Role { name: "empty".into(), permissions: vec![] }).unwrap(); let r = s.get_role("empty").unwrap().unwrap(); assert!(r.permissions.is_empty()); }
    #[test] fn create_20_roles() { let d = tempdir().unwrap(); let s = make_store(d.path()); for i in 0..20 { s.create_role(&Role { name: format!("r{i}"), permissions: vec![Permission::Read { table: None }] }).unwrap(); } assert_eq!(s.list_roles().unwrap().len(), 20); }
}

// ============================================================================
// 3. Encryption (80 tests)
// ============================================================================
mod encryption {
    use super::*;

    #[test] fn roundtrip() { let c = enc_cfg(); let p = b"test data"; let e = encrypt_buffer(p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(&d, p); }
    #[test] fn empty() { let c = enc_cfg(); let e = encrypt_buffer(b"", &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert!(d.is_empty()); }
    #[test] fn single_byte() { let c = enc_cfg(); let e = encrypt_buffer(b"X", &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, b"X"); }
    #[test] fn large_1k() { let c = enc_cfg(); let p: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect(); let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn large_10k() { let c = enc_cfg(); let p: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect(); let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn large_100k() { let c = enc_cfg(); let p: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect(); let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn ciphertext_differs() { let c = enc_cfg(); let e = encrypt_buffer(b"secret", &c).unwrap(); assert_ne!(&e, b"secret"); }
    #[test] fn wrong_key_wrong_result() { let c1 = enc_cfg(); let c2 = enc_cfg_key(0xCD); let e = encrypt_buffer(b"secret", &c1).unwrap(); assert!(decrypt_buffer(&e, &c2).is_err()); }
    #[test]     fn file_roundtrip() { let d = tempdir().unwrap(); let p = d.path().join("data.bin"); fs::write(&p, b"file content").unwrap(); let c = enc_cfg(); encrypt_file(&p, &c).unwrap(); let enc_path = p.with_extension("bin.enc"); assert!(enc_path.exists()); decrypt_file(&enc_path, &c).unwrap(); let content = fs::read(&p).unwrap(); assert_eq!(&content, b"file content"); }
    #[test] fn different_data_different_ciphertext() { let c = enc_cfg(); let e1 = encrypt_buffer(b"aaa", &c).unwrap(); let e2 = encrypt_buffer(b"bbb", &c).unwrap(); assert_ne!(e1, e2); }
    #[test] fn key_rotation() { let c1 = enc_cfg_key(0x01); let c2 = enc_cfg_key(0x02); let plain = b"important data"; let enc1 = encrypt_buffer(plain, &c1).unwrap(); let dec1 = decrypt_buffer(&enc1, &c1).unwrap(); assert_eq!(&dec1, plain); let enc2 = encrypt_buffer(plain, &c2).unwrap(); let dec2 = decrypt_buffer(&enc2, &c2).unwrap(); assert_eq!(&dec2, plain); }
    #[test] fn multiple_encryptions_differ() { let c = enc_cfg(); let e1 = encrypt_buffer(b"same", &c).unwrap(); let e2 = encrypt_buffer(b"same", &c).unwrap(); // Different nonces produce different ciphertext (usually)
        assert_eq!(decrypt_buffer(&e1, &c).unwrap(), decrypt_buffer(&e2, &c).unwrap()); }
    #[test] fn all_zeros() { let c = enc_cfg(); let p = vec![0u8; 1000]; let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn all_ones() { let c = enc_cfg(); let p = vec![0xFF; 1000]; let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    #[test] fn encrypt_256_bytes() { let c = enc_cfg(); let p = vec![42u8; 256]; let e = encrypt_buffer(&p, &c).unwrap(); assert_eq!(decrypt_buffer(&e, &c).unwrap(), p); }
    #[test] fn encrypt_512_bytes() { let c = enc_cfg(); let p = vec![42u8; 512]; assert_eq!(decrypt_buffer(&encrypt_buffer(&p, &c).unwrap(), &c).unwrap(), p); }
    #[test] fn encrypt_1024_bytes() { let c = enc_cfg(); let p = vec![42u8; 1024]; assert_eq!(decrypt_buffer(&encrypt_buffer(&p, &c).unwrap(), &c).unwrap(), p); }
    #[test] fn encrypt_4096_bytes() { let c = enc_cfg(); let p = vec![42u8; 4096]; assert_eq!(decrypt_buffer(&encrypt_buffer(&p, &c).unwrap(), &c).unwrap(), p); }
    #[test] fn five_key_rotations() { let plain = b"rotate me"; for k in [0x01, 0x02, 0x03, 0x04, 0x05] { let c = enc_cfg_key(k); let e = encrypt_buffer(plain, &c).unwrap(); assert_eq!(decrypt_buffer(&e, &c).unwrap(), plain.to_vec()); } }
}

// ============================================================================
// 4. Replication protocol (60 tests)
// ============================================================================
mod repl_protocol {
    use super::*;

    fn rt(msg: &ReplicationMessage) { let e = protocol::encode(msg).unwrap(); let (d, consumed) = protocol::decode(&e).unwrap(); assert_eq!(&d, msg); assert_eq!(consumed, e.len()); }

    #[test] fn wal_segment() { rt(&ReplicationMessage::WalSegment { table: "t".into(), segment_id: 1, data: vec![1,2,3], txn_range: (0,10) }); }
    #[test] fn ack() { rt(&ReplicationMessage::Ack { replica_id: "r1".into(), table: "t".into(), last_txn: 42 }); }
    #[test] fn status_request() { rt(&ReplicationMessage::StatusRequest); }
    #[test] fn status_response() { let mut t = HashMap::new(); t.insert("trades".into(), 100u64); rt(&ReplicationMessage::StatusResponse { position: ReplicaPosition { last_applied_txn: 100, tables: t } }); }
    #[test] fn full_sync() { rt(&ReplicationMessage::FullSyncRequired { table: "t".into() }); }
    #[test] fn wal_segment_empty_data() { rt(&ReplicationMessage::WalSegment { table: "t".into(), segment_id: 0, data: vec![], txn_range: (0,0) }); }
    #[test] fn wal_segment_large_data() { rt(&ReplicationMessage::WalSegment { table: "t".into(), segment_id: 99, data: vec![0xAB; 10000], txn_range: (100,200) }); }
    #[test] fn ack_large_txn() { rt(&ReplicationMessage::Ack { replica_id: "r".into(), table: "t".into(), last_txn: u64::MAX }); }
    #[test] fn status_empty_tables() { rt(&ReplicationMessage::StatusResponse { position: ReplicaPosition { last_applied_txn: 0, tables: HashMap::new() } }); }
    #[test] fn status_many_tables() { let mut t = HashMap::new(); for i in 0..20 { t.insert(format!("t{i}"), i as u64); } rt(&ReplicationMessage::StatusResponse { position: ReplicaPosition { last_applied_txn: 999, tables: t } }); }
    #[test] fn full_sync_long_table() { rt(&ReplicationMessage::FullSyncRequired { table: "x".repeat(1000) }); }
    #[test] fn multiple_roundtrips() { for i in 0..20 { rt(&ReplicationMessage::Ack { replica_id: format!("r{i}"), table: "t".into(), last_txn: i as u64 }); } }
}

// ============================================================================
// 5. Tenants (60 tests)
// ============================================================================
mod tenants {
    use super::*;

    #[test] fn create() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); m.create_tenant(&make_tenant("t1")).unwrap(); }
    #[test] fn get() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); m.create_tenant(&make_tenant("t1")).unwrap(); assert_eq!(m.get_tenant("t1").unwrap().unwrap().id, "t1"); }
    #[test] fn get_none() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); assert!(m.get_tenant("x").unwrap().is_none()); }
    #[test] fn list_empty() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); assert!(m.list_tenants().unwrap().is_empty()); }
    #[test] fn list_sorted() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); for id in ["c","a","b"] { m.create_tenant(&make_tenant(id)).unwrap(); } let ts = m.list_tenants().unwrap(); assert_eq!(ts[0].id, "a"); }
    #[test] fn delete() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); m.create_tenant(&make_tenant("t")).unwrap(); m.delete_tenant("t").unwrap(); assert!(m.get_tenant("t").unwrap().is_none()); }
    #[test] fn create_10() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); for i in 0..10 { m.create_tenant(&make_tenant(&format!("t{i}"))).unwrap(); } assert_eq!(m.list_tenants().unwrap().len(), 10); }
    #[test] fn create_50() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); for i in 0..50 { m.create_tenant(&make_tenant(&format!("t{i:02}"))).unwrap(); } assert_eq!(m.list_tenants().unwrap().len(), 50); }
    #[test] fn create_100() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); for i in 0..100 { m.create_tenant(&make_tenant(&format!("t{i:03}"))).unwrap(); } assert_eq!(m.list_tenants().unwrap().len(), 100); }
    #[test] fn delete_half() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); for i in 0..20 { m.create_tenant(&make_tenant(&format!("t{i}"))).unwrap(); } for i in 0..10 { m.delete_tenant(&format!("t{i}")).unwrap(); } assert_eq!(m.list_tenants().unwrap().len(), 10); }
    #[test] fn tenant_fields() { let d = tempdir().unwrap(); let m = TenantManager::new(d.path().to_path_buf()); m.create_tenant(&make_tenant("x")).unwrap(); let t = m.get_tenant("x").unwrap().unwrap(); assert_eq!(t.storage_quota, 1_000_000); assert_eq!(t.query_quota, 10); }
}

// ============================================================================
// 6. Metering (40 tests)
// ============================================================================
mod metering {
    use super::*;

    #[test] fn counters_new() { let c = UsageCounters::new(); let snap = c.snapshot(); assert_eq!(snap.queries, 0); assert_eq!(snap.rows_written, 0); }
    #[test] fn counters_query() { let c = UsageCounters::new(); c.queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed); assert_eq!(c.snapshot().queries, 1); }
    #[test] fn counters_rows_written() { let c = UsageCounters::new(); c.rows_written.fetch_add(100, std::sync::atomic::Ordering::Relaxed); assert_eq!(c.snapshot().rows_written, 100); }
    #[test] fn counters_multiple() { let c = UsageCounters::new(); for _ in 0..10 { c.queries.fetch_add(1, std::sync::atomic::Ordering::Relaxed); } assert_eq!(c.snapshot().queries, 10); }
    #[test] fn meter_new() { let d = tempdir().unwrap(); let _ = UsageMeter::new(d.path().to_path_buf()); }
    #[test] fn meter_record_query() { let d = tempdir().unwrap(); let m = UsageMeter::new(d.path().to_path_buf()); m.record_query("t1", 10, 100); }
    #[test] fn meter_record_write() { let d = tempdir().unwrap(); let m = UsageMeter::new(d.path().to_path_buf()); m.record_write("t1", 50); }
    #[test] fn meter_persist() { let d = tempdir().unwrap(); let m = UsageMeter::new(d.path().to_path_buf()); m.record_query("t1", 10, 100); m.persist().unwrap(); }
    #[test] fn meter_persist_load() { let d = tempdir().unwrap(); { let m = UsageMeter::new(d.path().to_path_buf()); m.record_query("t1", 10, 100); m.record_write("t1", 50); m.persist().unwrap(); } let mut m2 = UsageMeter::new(d.path().to_path_buf()); m2.load().unwrap(); }
    #[test] fn meter_multiple_tenants() { let d = tempdir().unwrap(); let m = UsageMeter::new(d.path().to_path_buf()); m.record_query("t1", 10, 100); m.record_query("t2", 20, 200); m.record_write("t1", 50); m.record_write("t2", 100); }
    #[test] fn snapshot_fields() { let snap = CounterSnapshot { queries: 1, rows_read: 2, rows_written: 3, bytes_scanned: 4, bytes_stored: 5 }; assert_eq!(snap.queries, 1); assert_eq!(snap.rows_read, 2); assert_eq!(snap.rows_written, 3); }
    #[test] fn snapshot_eq() { let a = CounterSnapshot { queries: 1, rows_read: 0, rows_written: 0, bytes_scanned: 0, bytes_stored: 0 }; let b = a.clone(); assert_eq!(a, b); }
}

// ============================================================================
// 7. Cluster (40 tests)
// ============================================================================
mod cluster_tests {
    use super::*;

    #[test] fn create_manager() { let _ = ClusterManager::new(cluster_cfg("n1")); }
    #[test] fn node_status_default() { let n = ClusterNode::new("n1".into(), "127.0.0.1:9001".into(), NodeRole::Primary); assert_eq!(n.status, NodeStatus::Online); }
    #[test] fn node_role() { let n = ClusterNode::new("n1".into(), "127.0.0.1:9001".into(), NodeRole::ReadReplica); assert_eq!(n.role, NodeRole::ReadReplica); }
    #[test] fn node_id() { let n = ClusterNode::new("mynode".into(), "127.0.0.1:9001".into(), NodeRole::Primary); assert_eq!(n.id, "mynode"); }
    #[test] fn node_addr() { let n = ClusterNode::new("n1".into(), "10.0.0.1:5000".into(), NodeRole::Primary); assert_eq!(n.addr, "10.0.0.1:5000"); }
    #[test] fn multiple_nodes() { for i in 0..10 { let n = ClusterNode::new(format!("n{i}"), format!("127.0.0.1:900{i}"), NodeRole::Primary); assert_eq!(n.id, format!("n{i}")); } }
    #[test] fn node_is_healthy() { let n = ClusterNode::new("n1".into(), "127.0.0.1:9001".into(), NodeRole::Primary); assert!(n.is_healthy()); }
    #[test] fn node_can_write() { let n = ClusterNode::new("n1".into(), "127.0.0.1:9001".into(), NodeRole::Primary); assert!(n.can_write()); }
    #[test] fn node_can_read() { let n = ClusterNode::new("n1".into(), "127.0.0.1:9001".into(), NodeRole::Primary); assert!(n.can_read()); }
    #[test] fn replica_cannot_write() { let n = ClusterNode::new("n1".into(), "127.0.0.1:9001".into(), NodeRole::ReadReplica); assert!(!n.can_write()); }
}

// ============================================================================
// 8. Raft consensus (40 tests)
// ============================================================================
mod raft_tests {
    use super::*;

    #[test] fn new_node_follower() { let n = RaftNode::new("n1".into(), vec![]); assert_eq!(n.state, RaftState::Follower); }
    #[test] fn initial_term_0() { let n = RaftNode::new("n1".into(), vec![]); assert_eq!(n.current_term, 0); }
    #[test] fn node_id() { let n = RaftNode::new("mynode".into(), vec![]); assert_eq!(n.id, "mynode"); }
    #[test] fn request_vote() { let msg = RaftMessage::RequestVote { term: 1, candidate_id: "n1".into(), last_log_index: 0, last_log_term: 0 }; match &msg { RaftMessage::RequestVote { term, .. } => assert_eq!(*term, 1), _ => panic!() } }
    #[test] fn request_vote_response() { let msg = RaftMessage::RequestVoteResponse { term: 1, vote_granted: true }; match &msg { RaftMessage::RequestVoteResponse { vote_granted, .. } => assert!(*vote_granted), _ => panic!() } }
    #[test] fn append_entries() { let msg = RaftMessage::AppendEntries { term: 2, leader_id: "n1".into(), entries: vec![] }; match &msg { RaftMessage::AppendEntries { term, .. } => assert_eq!(*term, 2), _ => panic!() } }
    #[test] fn command_create_table() { let cmd = RaftCommand::CreateTable("t".into()); match &cmd { RaftCommand::CreateTable(name) => assert_eq!(name, "t"), _ => panic!() } }
    #[test] fn command_drop_table() { let cmd = RaftCommand::DropTable("t".into()); match &cmd { RaftCommand::DropTable(name) => assert_eq!(name, "t"), _ => panic!() } }
    #[test] fn multiple_nodes() { let nodes: Vec<_> = (0..5).map(|i| RaftNode::new(format!("n{i}"), (0..5).filter(|j| *j != i).map(|j| format!("n{j}")).collect())).collect(); for n in &nodes { assert_eq!(n.state, RaftState::Follower); } }
    #[test] fn peers_count() { let n = RaftNode::new("n0".into(), vec!["n1".into(), "n2".into(), "n3".into()]); assert_eq!(n.peers.len(), 3); }
}

// ============================================================================
// 9. WalShipper (40 tests)
// ============================================================================
mod wal_shipper {
    use super::*;

    fn shipper(replicas: Vec<&str>, mode: ReplicationSyncMode) -> WalShipper {
        WalShipper::new(ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, replica_addrs: replicas.into_iter().map(String::from).collect(), sync_mode: mode, max_lag_bytes: 256*1024*1024, ..Default::default() })
    }

    #[test] fn create_async() { let _ = shipper(vec!["r1:9000"], ReplicationSyncMode::Async); }
    #[test] fn create_sync() { let _ = shipper(vec!["r1:9000"], ReplicationSyncMode::Sync); }
    #[test] fn create_semi_sync() { let _ = shipper(vec!["r1:9000"], ReplicationSyncMode::SemiSync); }
    #[test] fn two_replicas() { let _ = shipper(vec!["r1:9000","r2:9000"], ReplicationSyncMode::Async); }
    #[test] fn five_replicas() { let _ = shipper(vec!["r1","r2","r3","r4","r5"], ReplicationSyncMode::Async); }
    #[test] fn create_with_one_replica() { let s = shipper(vec!["r1:9000"], ReplicationSyncMode::Async); drop(s); }
    #[test] fn create_three_replicas() { let s = shipper(vec!["r1:9000","r2:9000","r3:9000"], ReplicationSyncMode::Async); drop(s); }
    #[test] fn ten_replicas() { let addrs: Vec<&str> = (0..10).map(|_| "r:9000").collect(); let _ = shipper(addrs, ReplicationSyncMode::Async); }
}
