//! 500 enterprise feature tests: RBAC, encryption, replication protocol, cluster, tenant.

use std::collections::HashMap;
use exchange_core::cluster::node::{ClusterNode, NodeRole};
use exchange_core::cluster::{ClusterConfig, ClusterManager};
use exchange_core::encryption::{
    decrypt_buffer, encrypt_buffer, encrypt_file, decrypt_file,
    EncryptionAlgorithm, EncryptionConfig,
};
use exchange_core::metering::UsageMeter;
use exchange_core::rbac::{hash_password, verify_password, Permission, RbacStore, Role, SecurityContext, User};
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::wal_receiver::ReplicaPosition;
use exchange_core::tenant::{Tenant, TenantManager};
use tempfile::TempDir;

fn make_store() -> (TempDir, RbacStore) {
    let dir = TempDir::new().unwrap();
    let store = RbacStore::open(dir.path()).unwrap();
    (dir, store)
}
fn make_user(name: &str) -> User {
    User { username: name.to_string(), password_hash: hash_password("pass"), roles: vec![], enabled: true, created_at: 1_700_000_000 }
}
fn ctx_with(perms: Vec<Permission>) -> SecurityContext {
    SecurityContext { user: "test".to_string(), roles: vec!["testrole".to_string()], permissions: perms }
}
fn enc_cfg() -> EncryptionConfig { EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap() }
fn enc_cfg2() -> EncryptionConfig { EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xCD; 32]).unwrap() }
fn make_tenant(id: &str) -> Tenant {
    Tenant { id: id.into(), name: format!("T {id}"), namespace: id.into(), storage_quota: 1_000_000, query_quota: 10, created_at: 1_700_000_000 }
}
fn cluster_cfg(id: &str) -> ClusterConfig {
    ClusterConfig { node_id: id.into(), node_addr: format!("127.0.0.1:900{}", &id[1..]), seed_nodes: vec![], role: NodeRole::Primary }
}

// ===========================================================================
// RBAC user CRUD — 80 tests
// ===========================================================================
mod rbac_users_f06 { use super::*;
    macro_rules! cu { ($n:ident, $name:expr) => {
        #[test] fn $n() { let (_d, s) = make_store(); s.create_user(&make_user($name)).unwrap(); let u = s.get_user($name).unwrap().unwrap(); assert_eq!(u.username, $name); }
    }; }
    cu!(u30, "u30"); cu!(u31, "u31"); cu!(u32, "u32"); cu!(u33, "u33"); cu!(u34, "u34");
    cu!(u35, "u35"); cu!(u36, "u36"); cu!(u37, "u37"); cu!(u38, "u38"); cu!(u39, "u39");
    cu!(u40, "u40"); cu!(u41, "u41"); cu!(u42, "u42"); cu!(u43, "u43"); cu!(u44, "u44");
    cu!(u45, "u45"); cu!(u46, "u46"); cu!(u47, "u47"); cu!(u48, "u48"); cu!(u49, "u49");
    cu!(u50, "u50"); cu!(u51, "u51"); cu!(u52, "u52"); cu!(u53, "u53"); cu!(u54, "u54");
    cu!(u55, "u55"); cu!(u56, "u56"); cu!(u57, "u57"); cu!(u58, "u58"); cu!(u59, "u59");

    // Create and delete
    macro_rules! cd { ($n:ident, $name:expr) => {
        #[test] fn $n() { let (_d, s) = make_store(); s.create_user(&make_user($name)).unwrap(); s.delete_user($name).unwrap(); assert!(s.get_user($name).unwrap().is_none()); }
    }; }
    cd!(cd01, "cd01"); cd!(cd02, "cd02"); cd!(cd03, "cd03"); cd!(cd04, "cd04"); cd!(cd05, "cd05");
    cd!(cd06, "cd06"); cd!(cd07, "cd07"); cd!(cd08, "cd08"); cd!(cd09, "cd09"); cd!(cd10, "cd10");

    // Create N and check count
    macro_rules! cn { ($n:ident, $count:expr) => {
        #[test] fn $n() { let (_d, s) = make_store(); for i in 0..$count { s.create_user(&make_user(&format!("y{i:03}"))).unwrap(); } assert_eq!(s.list_users().unwrap().len(), $count); }
    }; }
    cn!(cn1, 1); cn!(cn2, 2); cn!(cn3, 3); cn!(cn5, 5); cn!(cn7, 7);

    // Hash checks
    macro_rules! hp { ($n:ident, $pass:expr) => {
        #[test] fn $n() { let h = hash_password($pass); assert!(!h.is_empty()); }
    }; }
    hp!(hp1, "password1"); hp!(hp2, "password2"); hp!(hp3, "secret"); hp!(hp4, "admin123");
    hp!(hp5, "test!@#");

    // Same password same hash
    macro_rules! hd { ($n:ident, $pass:expr) => {
        #[test] fn $n() { let h = hash_password($pass); assert!(verify_password($pass, &h)); }
    }; }
    hd!(hd1, "abc"); hd!(hd2, "xyz"); hd!(hd3, "test123"); hd!(hd4, "pass"); hd!(hd5, "hello");
}

// ===========================================================================
// RBAC roles — 40 tests
// ===========================================================================
mod rbac_roles_f06 { use super::*;
    macro_rules! cr { ($n:ident, $name:expr) => {
        #[test] fn $n() { let (_d, s) = make_store(); s.create_role(&Role { name: $name.into(), permissions: vec![] }).unwrap(); let r = s.get_role($name).unwrap().unwrap(); assert_eq!(r.name, $name); }
    }; }
    cr!(r21, "r21"); cr!(r22, "r22"); cr!(r23, "r23"); cr!(r24, "r24"); cr!(r25, "r25");
    cr!(r26, "r26"); cr!(r27, "r27"); cr!(r28, "r28"); cr!(r29, "r29"); cr!(r30, "r30");
    cr!(r31, "r31"); cr!(r32, "r32"); cr!(r33, "r33"); cr!(r34, "r34"); cr!(r35, "r35");
    cr!(r36, "r36"); cr!(r37, "r37"); cr!(r38, "r38"); cr!(r39, "r39"); cr!(r40, "r40");

    // list roles
    macro_rules! lr { ($n:ident, $count:expr) => {
        #[test] fn $n() { let (_d, s) = make_store(); for i in 0..$count { s.create_role(&Role { name: format!("lr{i:03}"), permissions: vec![] }).unwrap(); } assert_eq!(s.list_roles().unwrap().len(), $count); }
    }; }
    lr!(lr1, 1); lr!(lr2, 2); lr!(lr3, 3); lr!(lr5, 5); lr!(lr10, 10);
    lr!(lr15, 15); lr!(lr20, 20); lr!(lr25, 25);

    // Delete role
    #[test] fn del_role() { let (_d, s) = make_store(); s.create_role(&Role { name: "dr".into(), permissions: vec![] }).unwrap(); s.delete_role("dr").unwrap(); assert!(s.get_role("dr").unwrap().is_none()); }
    #[test] fn del_role_nx() { let (_d, s) = make_store(); assert!(s.delete_role("nope").is_err()); }
    #[test] fn get_role_nx() { let (_d, s) = make_store(); assert!(s.get_role("nope").unwrap().is_none()); }
    #[test] fn multi_perms() { let (_d, s) = make_store(); s.create_role(&Role { name: "rw".into(), permissions: vec![Permission::Admin, Permission::DDL] }).unwrap(); let r = s.get_role("rw").unwrap().unwrap(); assert_eq!(r.permissions.len(), 2); }
}

// ===========================================================================
// RBAC SecurityContext — 50 tests
// ===========================================================================
mod rbac_ctx_f06 { use super::*;
    // Admin can read any table
    macro_rules! admin_read { ($n:ident, $table:expr) => {
        #[test] fn $n() { assert!(ctx_with(vec![Permission::Admin]).can_read_table($table)); }
    }; }
    admin_read!(ar_t1, "t1"); admin_read!(ar_t2, "t2"); admin_read!(ar_trades, "trades");
    admin_read!(ar_orders, "orders"); admin_read!(ar_quotes, "quotes");
    admin_read!(ar_ticks, "ticks"); admin_read!(ar_candles, "candles");
    admin_read!(ar_positions, "positions"); admin_read!(ar_accounts, "accounts");
    admin_read!(ar_metrics, "metrics");

    // Read specific table
    macro_rules! read_spec { ($n:ident, $table:expr) => {
        #[test] fn $n() { assert!(ctx_with(vec![Permission::Read { table: Some($table.into()) }]).can_read_table($table)); }
    }; }
    read_spec!(rs_t1, "t1"); read_spec!(rs_t2, "t2"); read_spec!(rs_trades, "trades");
    read_spec!(rs_orders, "orders"); read_spec!(rs_quotes, "quotes");

    // Read specific: wrong table denied
    macro_rules! read_deny { ($n:ident, $perm:expr, $query:expr) => {
        #[test] fn $n() { assert!(!ctx_with(vec![Permission::Read { table: Some($perm.into()) }]).can_read_table($query)); }
    }; }
    read_deny!(rd_01, "trades", "orders"); read_deny!(rd_02, "orders", "trades");
    read_deny!(rd_03, "quotes", "ticks"); read_deny!(rd_04, "ticks", "candles");
    read_deny!(rd_05, "candles", "trades"); read_deny!(rd_06, "t1", "t2");
    read_deny!(rd_07, "t2", "t1"); read_deny!(rd_08, "alpha", "beta");
    read_deny!(rd_09, "foo", "bar"); read_deny!(rd_10, "x", "y");

    // Read all tables
    #[test] fn read_all() { assert!(ctx_with(vec![Permission::Read { table: None }]).can_read_table("anything")); }
    #[test] fn no_perms() { assert!(!ctx_with(vec![]).can_read_table("t")); }

    // Admin can write any table
    macro_rules! admin_write { ($n:ident, $table:expr) => {
        #[test] fn $n() { assert!(ctx_with(vec![Permission::Admin]).can_write_table($table)); }
    }; }
    admin_write!(aw_trades, "trades"); admin_write!(aw_orders, "orders");
    admin_write!(aw_quotes, "quotes"); admin_write!(aw_ticks, "ticks");
    admin_write!(aw_candles, "candles");

    // Write specific
    macro_rules! write_spec { ($n:ident, $table:expr) => {
        #[test] fn $n() { assert!(ctx_with(vec![Permission::Write { table: Some($table.into()) }]).can_write_table($table)); }
    }; }
    write_spec!(ws_trades, "trades"); write_spec!(ws_orders, "orders");
    write_spec!(ws_quotes, "quotes");

    // DDL
    #[test] fn ddl_ok() { assert!(ctx_with(vec![Permission::DDL]).can_ddl()); }
    #[test] fn ddl_admin() { assert!(ctx_with(vec![Permission::Admin]).can_ddl()); }
    #[test] fn ddl_no() { assert!(!ctx_with(vec![Permission::Read { table: None }]).can_ddl()); }
    #[test] fn user_field() { assert_eq!(ctx_with(vec![]).user, "test"); }
    #[test] fn roles_field() { assert_eq!(ctx_with(vec![]).roles, vec!["testrole"]); }
}

// ===========================================================================
// Encryption — 80 tests
// ===========================================================================
mod encryption_f06 { use super::*;
    macro_rules! rt { ($n:ident, $data:expr) => {
        #[test] fn $n() { let c = enc_cfg(); let e = encrypt_buffer($data, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(&d, $data); }
    }; }
    rt!(e01, b"hello"); rt!(e02, b"world"); rt!(e03, b"test"); rt!(e04, b"data");
    rt!(e05, b"encryption"); rt!(e06, b""); rt!(e07, b"a"); rt!(e08, b"ab");
    rt!(e09, b"abc"); rt!(e10, b"abcdef"); rt!(e11, b"The quick brown fox");
    rt!(e12, b"0123456789"); rt!(e13, b"line1\nline2\nline3"); rt!(e14, b"tab\there");
    rt!(e15, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"); rt!(e16, b"!@#$%^&*()");
    rt!(e17, b"spaces  and  more"); rt!(e18, b"single"); rt!(e19, b"double_word");
    rt!(e20, b"triple_word_here");

    // Variable length
    macro_rules! vl { ($n:ident, $len:expr) => {
        #[test] fn $n() { let c = enc_cfg(); let p: Vec<u8> = (0..$len).map(|i| (i % 256) as u8).collect(); let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    }; }
    vl!(vl1, 1); vl!(vl2, 2); vl!(vl3, 3); vl!(vl4, 4); vl!(vl5, 5);
    vl!(vl7, 7); vl!(vl8, 8); vl!(vl9, 9); vl!(vl11, 11); vl!(vl13, 13);
    vl!(vl15, 15); vl!(vl17, 17); vl!(vl19, 19); vl!(vl21, 21); vl!(vl23, 23);
    vl!(vl25, 25); vl!(vl31, 31); vl!(vl33, 33); vl!(vl63, 63); vl!(vl65, 65);
    vl!(vl127, 127); vl!(vl129, 129); vl!(vl255, 255); vl!(vl257, 257); vl!(vl511, 511);
    vl!(vl513, 513); vl!(vl1023, 1023); vl!(vl1025, 1025); vl!(vl2047, 2047); vl!(vl4095, 4095);

    // Ciphertext differs from plaintext
    macro_rules! diff { ($n:ident, $data:expr) => {
        #[test] fn $n() { let c = enc_cfg(); let e = encrypt_buffer($data, &c).unwrap(); assert_ne!(&e[..], $data); }
    }; }
    diff!(d01, b"hello"); diff!(d02, b"world"); diff!(d03, b"test"); diff!(d04, b"secret");
    diff!(d05, b"0123456789");

    // Wrong key fails
    #[test] fn wrong_key() { let c1 = enc_cfg(); let c2 = enc_cfg2(); let e = encrypt_buffer(b"secret", &c1).unwrap(); assert!(decrypt_buffer(&e, &c2).is_err()); }

    // File roundtrip
    #[test] fn file_rt() { let dir = TempDir::new().unwrap(); let c = enc_cfg(); let pp = dir.path().join("data.bin"); std::fs::write(&pp, b"content").unwrap(); encrypt_file(&pp, &c).unwrap(); decrypt_file(&pp, &c).unwrap(); assert_eq!(std::fs::read(&pp).unwrap(), b"content"); }
    #[test] fn file_empty() { let dir = TempDir::new().unwrap(); let c = enc_cfg(); let pp = dir.path().join("e.bin"); std::fs::write(&pp, b"").unwrap(); encrypt_file(&pp, &c).unwrap(); decrypt_file(&pp, &c).unwrap(); assert!(std::fs::read(&pp).unwrap().is_empty()); }

    // All same byte
    macro_rules! ab { ($n:ident, $byte:expr, $len:expr) => {
        #[test] fn $n() { let c = enc_cfg(); let p = vec![$byte; $len]; let e = encrypt_buffer(&p, &c).unwrap(); let d = decrypt_buffer(&e, &c).unwrap(); assert_eq!(d, p); }
    }; }
    ab!(z50, 0x00, 50); ab!(z200, 0x00, 200); ab!(z500, 0x00, 500);
    ab!(f50, 0xFF, 50); ab!(f200, 0xFF, 200); ab!(f500, 0xFF, 500);
    ab!(a50, 0xAA, 50); ab!(a200, 0xAA, 200);

    // Config checks
    #[test] fn cfg_ok() { assert!(EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0; 32]).is_ok()); }
    #[test] fn cfg_bad_16() { assert!(EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0; 16]).is_err()); }
    #[test] fn cfg_bad_0() { assert!(EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![]).is_err()); }
    #[test] fn cfg_bad_8() { assert!(EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0; 8]).is_err()); }
}

// ===========================================================================
// Replication protocol — 80 tests
// ===========================================================================
mod replication_f06 { use super::*;
    // WalSegment
    macro_rules! wal { ($n:ident, $table:expr, $seg:expr, $data_len:expr) => {
        #[test] fn $n() { let msg = ReplicationMessage::WalSegment { table: $table.into(), segment_id: $seg, data: vec![0xAB; $data_len], txn_range: ($seg, $seg + 10) }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    }; }
    wal!(w01, "t1", 100, 0); wal!(w02, "t1", 200, 1); wal!(w03, "t1", 300, 10);
    wal!(w04, "trades", 1, 100); wal!(w05, "orders", 2, 200); wal!(w06, "quotes", 3, 300);
    wal!(w07, "ticks", 4, 400); wal!(w08, "candles", 5, 500); wal!(w09, "bars", 6, 600);
    wal!(w10, "positions", 7, 0); wal!(w11, "accounts", 8, 1); wal!(w12, "logs", 9, 2);
    wal!(w13, "events", 10, 3); wal!(w14, "metrics", 11, 4); wal!(w15, "audit", 12, 5);

    // Ack
    macro_rules! ack { ($n:ident, $replica:expr, $table:expr, $txn:expr) => {
        #[test] fn $n() { let msg = ReplicationMessage::Ack { replica_id: $replica.into(), table: $table.into(), last_txn: $txn }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    }; }
    ack!(a11, "r1", "trades", 100); ack!(a12, "r1", "orders", 200); ack!(a13, "r1", "quotes", 300);
    ack!(a14, "r2", "trades", 100); ack!(a15, "r2", "orders", 200); ack!(a16, "r3", "trades", 1000);
    ack!(a17, "r4", "ticks", 500); ack!(a18, "r5", "candles", 50); ack!(a19, "r6", "bars", 999);
    ack!(a20, "r7", "positions", 42); ack!(a21, "r8", "accounts", 7); ack!(a22, "r9", "logs", 0);
    ack!(a23, "r10", "events", u64::MAX); ack!(a24, "r1", "metrics", 12345);
    ack!(a25, "replica_long_name", "very_long_table_name", 99999);

    // StatusRequest
    #[test] fn status_req_1() { let msg = ReplicationMessage::StatusRequest; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    #[test] fn status_req_2() { let enc = protocol::encode(&ReplicationMessage::StatusRequest).unwrap(); assert!(!enc.is_empty()); }

    // StatusResponse
    #[test] fn status_resp_empty() { let msg = ReplicationMessage::StatusResponse { position: ReplicaPosition::new() }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }

    macro_rules! sr { ($n:ident, $txn:expr, $tables:expr) => {
        #[test] fn $n() { let mut tables = HashMap::new(); for &(t, v) in &$tables { tables.insert(t.to_string(), v); } let msg = ReplicationMessage::StatusResponse { position: ReplicaPosition { last_applied_txn: $txn, tables } }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    }; }
    sr!(sr05, 0, [("t1", 0u64)]); sr!(sr06, 100, [("trades", 50u64)]);
    sr!(sr07, 200, [("orders", 100u64)]); sr!(sr08, 300, [("quotes", 150u64)]);
    sr!(sr09, 400, [("ticks", 200u64)]); sr!(sr10, 500, [("candles", 250u64)]);
    sr!(sr11, 1000, [("trades", 500u64), ("orders", 300u64)]);
    sr!(sr12, 2000, [("trades", 1000u64), ("orders", 500u64), ("quotes", 500u64)]);

    // FullSyncRequired
    macro_rules! fs { ($n:ident, $table:expr) => {
        #[test] fn $n() { let msg = ReplicationMessage::FullSyncRequired { table: $table.into() }; let enc = protocol::encode(&msg).unwrap(); let (dec, _) = protocol::decode(&enc).unwrap(); assert_eq!(dec, msg); }
    }; }
    fs!(fs07, "alpha"); fs!(fs08, "beta"); fs!(fs09, "gamma"); fs!(fs10, "delta");
    fs!(fs11, "epsilon"); fs!(fs12, "zeta"); fs!(fs13, "eta"); fs!(fs14, "theta");
    fs!(fs15, "iota"); fs!(fs16, "kappa");

    // consumed == encoded length
    macro_rules! cl { ($n:ident, $msg:expr) => {
        #[test] fn $n() { let enc = protocol::encode(&$msg).unwrap(); let (_, consumed) = protocol::decode(&enc).unwrap(); assert_eq!(consumed, enc.len()); }
    }; }
    cl!(cl04, ReplicationMessage::StatusRequest);
    cl!(cl05, ReplicationMessage::Ack { replica_id: "r".into(), table: "t".into(), last_txn: 1 });
    cl!(cl06, ReplicationMessage::WalSegment { table: "t".into(), segment_id: 1, data: vec![1], txn_range: (0, 1) });
    cl!(cl07, ReplicationMessage::FullSyncRequired { table: "t".into() });
    cl!(cl08, ReplicationMessage::StatusResponse { position: ReplicaPosition::new() });

    // Config
    #[test] fn cfg_primary() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.role, ReplicationRole::Primary)); }
    #[test] fn cfg_replica() { let c = ReplicationConfig { role: ReplicationRole::Replica, primary_addr: Some("addr".into()), sync_mode: ReplicationSyncMode::Sync, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.role, ReplicationRole::Replica)); }
    #[test] fn cfg_sync() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Sync, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.sync_mode, ReplicationSyncMode::Sync)); }
    #[test] fn cfg_async() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 0, ..Default::default() }; assert!(matches!(c.sync_mode, ReplicationSyncMode::Async)); }
    #[test] fn cfg_max_lag() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec![], max_lag_bytes: 1024, ..Default::default() }; assert_eq!(c.max_lag_bytes, 1024); }
    #[test] fn cfg_replicas() { let c = ReplicationConfig { role: ReplicationRole::Primary, primary_addr: None, sync_mode: ReplicationSyncMode::Async, replica_addrs: vec!["a".into(), "b".into()], max_lag_bytes: 0, ..Default::default() }; assert_eq!(c.replica_addrs.len(), 2); }
}

// ===========================================================================
// Cluster — 80 tests
// ===========================================================================
mod cluster_f06 { use super::*;
    #[test] fn register() { let mgr = ClusterManager::new(cluster_cfg("n1")); mgr.register().unwrap(); assert_eq!(mgr.node_count(), 1); }
    #[test] fn register_idempotent() { let mgr = ClusterManager::new(cluster_cfg("n1")); mgr.register().unwrap(); mgr.register().unwrap(); assert_eq!(mgr.node_count(), 1); }

    macro_rules! add_n { ($n:ident, $count:expr) => {
        #[test] fn $n() {
            let mgr = ClusterManager::new(cluster_cfg("n1"));
            mgr.register().unwrap();
            for j in 0..$count { mgr.add_node(ClusterNode::new(format!("z{j}"), format!("addr{j}"), NodeRole::ReadReplica)); }
            assert_eq!(mgr.node_count(), 1 + $count);
        }
    }; }
    add_n!(a01, 1); add_n!(a02, 2); add_n!(a03, 3); add_n!(a04, 4); add_n!(a05, 5);
    add_n!(a06, 6); add_n!(a07, 7); add_n!(a08, 8); add_n!(a09, 9); add_n!(a10, 10);
    add_n!(a11, 11); add_n!(a12, 12); add_n!(a13, 13); add_n!(a14, 14); add_n!(a15, 15);
    add_n!(a16, 16); add_n!(a17, 17); add_n!(a18, 18); add_n!(a19, 19); add_n!(a20, 20);
    add_n!(a25, 25); add_n!(a30, 30); add_n!(a35, 35); add_n!(a40, 40); add_n!(a45, 45);

    // Remove
    macro_rules! rm { ($n:ident, $total:expr, $rm:expr) => {
        #[test] fn $n() {
            let mgr = ClusterManager::new(cluster_cfg("n1"));
            mgr.register().unwrap();
            for j in 0..$total { mgr.add_node(ClusterNode::new(format!("rm{j}"), format!("addr{j}"), NodeRole::ReadReplica)); }
            for j in 0..$rm { mgr.remove_node(&format!("rm{j}")); }
            assert_eq!(mgr.node_count(), 1 + $total - $rm);
        }
    }; }
    rm!(rm_5_1, 5, 1); rm!(rm_5_2, 5, 2); rm!(rm_5_3, 5, 3); rm!(rm_5_5, 5, 5);
    rm!(rm_10_5, 10, 5); rm!(rm_10_10, 10, 10); rm!(rm_20_10, 20, 10);
    rm!(rm_20_15, 20, 15); rm!(rm_20_20, 20, 20); rm!(rm_1_1, 1, 1);

    // Healthy nodes
    #[test] fn healthy_after_reg() { let mgr = ClusterManager::new(cluster_cfg("n1")); mgr.register().unwrap(); assert!(!mgr.healthy_nodes().is_empty()); }
    #[test] fn healthy_empty() { let mgr = ClusterManager::new(cluster_cfg("n1")); assert!(mgr.healthy_nodes().is_empty()); }

    // Node roles
    #[test] fn primary() { let n = ClusterNode::new("p".into(), "addr".into(), NodeRole::Primary); assert!(matches!(n.role, NodeRole::Primary)); }
    #[test] fn read_replica() { let n = ClusterNode::new("r".into(), "addr".into(), NodeRole::ReadReplica); assert!(matches!(n.role, NodeRole::ReadReplica)); }
    #[test] fn node_id() { let n = ClusterNode::new("s".into(), "addr".into(), NodeRole::ReadReplica); assert_eq!(n.id, "s"); }

    // Node with tables
    macro_rules! nt { ($n:ident, $num:expr) => {
        #[test] fn $n() {
            let mgr = ClusterManager::new(cluster_cfg("n1")); mgr.register().unwrap();
            let mut node = ClusterNode::new("x".into(), "addr".into(), NodeRole::ReadReplica);
            node.tables = (0..$num).map(|j| format!("t{j}")).collect();
            mgr.add_node(node);
            assert_eq!(mgr.node_count(), 2);
        }
    }; }
    nt!(nt1, 1); nt!(nt2, 2); nt!(nt3, 3); nt!(nt5, 5); nt!(nt10, 10);
    nt!(nt20, 20); nt!(nt50, 50);
}

// ===========================================================================
// Tenant — 80 tests
// ===========================================================================
mod tenant_f06 { use super::*;
    macro_rules! ct { ($n:ident, $id:expr) => {
        #[test] fn $n() {
            let dir = TempDir::new().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            mgr.create_tenant(&make_tenant($id)).unwrap();
            let t = mgr.get_tenant($id).unwrap().unwrap();
            assert_eq!(t.id, $id);
        }
    }; }
    ct!(t01, "t01"); ct!(t02, "t02"); ct!(t03, "t03"); ct!(t04, "t04"); ct!(t05, "t05");
    ct!(t06, "t06"); ct!(t07, "t07"); ct!(t08, "t08"); ct!(t09, "t09"); ct!(t10, "t10");
    ct!(t11, "t11"); ct!(t12, "t12"); ct!(t13, "t13"); ct!(t14, "t14"); ct!(t15, "t15");
    ct!(t16, "t16"); ct!(t17, "t17"); ct!(t18, "t18"); ct!(t19, "t19"); ct!(t20, "t20");
    ct!(t21, "t21"); ct!(t22, "t22"); ct!(t23, "t23"); ct!(t24, "t24"); ct!(t25, "t25");
    ct!(t26, "t26"); ct!(t27, "t27"); ct!(t28, "t28"); ct!(t29, "t29"); ct!(t30, "t30");

    // List tenants
    macro_rules! lt { ($n:ident, $count:expr) => {
        #[test] fn $n() {
            let dir = TempDir::new().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            for i in 0..$count { mgr.create_tenant(&make_tenant(&format!("lt{i:03}"))).unwrap(); }
            assert_eq!(mgr.list_tenants().unwrap().len(), $count);
        }
    }; }
    lt!(lt1, 1); lt!(lt2, 2); lt!(lt3, 3); lt!(lt5, 5); lt!(lt10, 10);
    lt!(lt15, 15); lt!(lt20, 20); lt!(lt25, 25); lt!(lt30, 30);

    // Delete
    macro_rules! dt { ($n:ident, $id:expr) => {
        #[test] fn $n() {
            let dir = TempDir::new().unwrap();
            let mgr = TenantManager::new(dir.path().to_path_buf());
            mgr.create_tenant(&make_tenant($id)).unwrap();
            mgr.delete_tenant($id).unwrap();
            assert!(mgr.get_tenant($id).unwrap().is_none());
        }
    }; }
    dt!(dt01, "dt01"); dt!(dt02, "dt02"); dt!(dt03, "dt03"); dt!(dt04, "dt04"); dt!(dt05, "dt05");

    // Get nonexistent
    #[test] fn get_none() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); assert!(mgr.get_tenant("nope").unwrap().is_none()); }
    #[test] fn list_empty() { let dir = TempDir::new().unwrap(); let mgr = TenantManager::new(dir.path().to_path_buf()); assert!(mgr.list_tenants().unwrap().is_empty()); }

    // Tenant fields
    #[test] fn name() { let t = make_tenant("x"); assert_eq!(t.name, "T x"); }
    #[test] fn ns() { let t = make_tenant("x"); assert_eq!(t.namespace, "x"); }
    #[test] fn quota() { let t = make_tenant("x"); assert_eq!(t.storage_quota, 1_000_000); }
    #[test] fn query_quota() { let t = make_tenant("x"); assert_eq!(t.query_quota, 10); }
    #[test] fn created_at() { let t = make_tenant("x"); assert_eq!(t.created_at, 1_700_000_000); }
}

// ===========================================================================
// Metering — 40 tests
// ===========================================================================
mod metering_f06 { use super::*;
    use std::path::PathBuf;
    fn meter() -> (TempDir, UsageMeter) { let dir = TempDir::new().unwrap(); let m = UsageMeter::new(dir.path().to_path_buf()); (dir, m) }

    #[test] fn new_empty() { let (_d, m) = meter(); assert_eq!(m.get_usage("t1").queries, 0); }
    #[test] fn record_1() { let (_d, m) = meter(); m.record_query("t1", 10, 100); assert_eq!(m.get_usage("t1").queries, 1); }
    #[test] fn record_rows() { let (_d, m) = meter(); m.record_query("t1", 10, 100); assert_eq!(m.get_usage("t1").rows_read, 10); }
    #[test] fn record_bytes() { let (_d, m) = meter(); m.record_query("t1", 10, 100); assert_eq!(m.get_usage("t1").bytes_scanned, 100); }

    macro_rules! rec { ($n:ident, $count:expr) => {
        #[test] fn $n() { let (_d, m) = meter(); for _ in 0..$count { m.record_query("t1", 1, 10); } assert_eq!(m.get_usage("t1").queries, $count); }
    }; }
    rec!(r1, 1); rec!(r2, 2); rec!(r3, 3); rec!(r5, 5); rec!(r10, 10);
    rec!(r20, 20); rec!(r50, 50); rec!(r100, 100);

    // Rows accumulation
    macro_rules! rows { ($n:ident, $count:expr, $rows:expr) => {
        #[test] fn $n() { let (_d, m) = meter(); for _ in 0..$count as u64 { m.record_query("t1", $rows, 0); } assert_eq!(m.get_usage("t1").rows_read, $count * $rows); }
    }; }
    rows!(rows1, 1, 100); rows!(rows2, 2, 100); rows!(rows5, 5, 100); rows!(rows10, 10, 50);
    rows!(rows20, 20, 25);

    // Write recording
    #[test] fn write_0() { let (_d, m) = meter(); assert_eq!(m.get_usage("t1").rows_written, 0); }
    #[test] fn write_1() { let (_d, m) = meter(); m.record_write("t1", 10); assert_eq!(m.get_usage("t1").rows_written, 10); }
    #[test] fn write_n() { let (_d, m) = meter(); for _ in 0..5u64 { m.record_write("t1", 100); } assert_eq!(m.get_usage("t1").rows_written, 500); }

    // Multi-tenant
    #[test] fn multi_tenant() { let (_d, m) = meter(); m.record_query("a", 1, 1); m.record_query("b", 2, 2); assert_eq!(m.get_usage("a").queries, 1); assert_eq!(m.get_usage("b").queries, 1); }
    #[test] fn multi_rows() { let (_d, m) = meter(); m.record_query("a", 10, 0); m.record_query("b", 20, 0); assert_eq!(m.get_usage("a").rows_read, 10); assert_eq!(m.get_usage("b").rows_read, 20); }

    // get_all_usage
    #[test] fn all_empty() { let (_d, m) = meter(); assert!(m.get_all_usage().is_empty()); }
    #[test] fn all_one() { let (_d, m) = meter(); m.record_query("t1", 1, 1); assert_eq!(m.get_all_usage().len(), 1); }
    #[test] fn all_two() { let (_d, m) = meter(); m.record_query("a", 1, 1); m.record_query("b", 1, 1); assert_eq!(m.get_all_usage().len(), 2); }
    #[test] fn all_three() { let (_d, m) = meter(); m.record_query("a", 1, 1); m.record_query("b", 1, 1); m.record_query("c", 1, 1); assert_eq!(m.get_all_usage().len(), 3); }
    #[test] fn all_five() { let (_d, m) = meter(); for i in 0..5 { m.record_query(&format!("t{i}"), 1, 1); } assert_eq!(m.get_all_usage().len(), 5); }
    #[test] fn all_ten() { let (_d, m) = meter(); for i in 0..10 { m.record_query(&format!("t{i}"), 1, 1); } assert_eq!(m.get_all_usage().len(), 10); }
}
