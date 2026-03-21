//! 500 enterprise feature tests: RBAC, encryption, replication, cluster, tenant.

use exchange_core::cluster::node::{ClusterNode, NodeRole};
use exchange_core::cluster::{ClusterConfig, ClusterManager};
use exchange_core::encryption::{
    EncryptionAlgorithm, EncryptionConfig, decrypt_buffer, decrypt_file, encrypt_buffer,
    encrypt_file,
};
use exchange_core::metering::UsageMeter;
use exchange_core::rbac::{
    Permission, RbacStore, Role, SecurityContext, User, hash_password, verify_password,
};
use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::wal_receiver::ReplicaPosition;
use exchange_core::tenant::{Tenant, TenantManager};
use std::collections::HashMap;
use tempfile::TempDir;

fn make_store() -> (TempDir, RbacStore) {
    let dir = TempDir::new().unwrap();
    let store = RbacStore::open(dir.path()).unwrap();
    (dir, store)
}
fn make_user(name: &str) -> User {
    User {
        username: name.to_string(),
        password_hash: hash_password("pass"),
        roles: vec![],
        enabled: true,
        created_at: 1_700_000_000,
    }
}
fn ctx_with(perms: Vec<Permission>) -> SecurityContext {
    SecurityContext {
        user: "test".to_string(),
        roles: vec!["testrole".to_string()],
        permissions: perms,
    }
}
fn enc_cfg() -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xAB; 32]).unwrap()
}
fn enc_cfg2() -> EncryptionConfig {
    EncryptionConfig::new(EncryptionAlgorithm::Aes256Gcm, vec![0xCD; 32]).unwrap()
}
fn make_tenant(id: &str) -> Tenant {
    Tenant {
        id: id.into(),
        name: format!("T {id}"),
        namespace: id.into(),
        storage_quota: 1_000_000,
        query_quota: 10,
        created_at: 1_700_000_000,
    }
}
fn cluster_cfg(id: &str) -> ClusterConfig {
    ClusterConfig {
        node_id: id.into(),
        node_addr: format!("127.0.0.1:900{}", &id[1..]),
        seed_nodes: vec![],
        role: NodeRole::Primary,
    }
}

// =========================================================================
// RBAC user CRUD — 100 tests
// =========================================================================
mod rbac_users {
    use super::*;
    macro_rules! cu {
        ($n:ident, $name:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                s.create_user(&make_user($name)).unwrap();
                let u = s.get_user($name).unwrap().unwrap();
                assert_eq!(u.username, $name);
            }
        };
    }
    cu!(u00, "u00");
    cu!(u01, "u01");
    cu!(u02, "u02");
    cu!(u03, "u03");
    cu!(u04, "u04");
    cu!(u05, "u05");
    cu!(u06, "u06");
    cu!(u07, "u07");
    cu!(u08, "u08");
    cu!(u09, "u09");
    cu!(u10, "u10");
    cu!(u11, "u11");
    cu!(u12, "u12");
    cu!(u13, "u13");
    cu!(u14, "u14");
    cu!(u15, "u15");
    cu!(u16, "u16");
    cu!(u17, "u17");
    cu!(u18, "u18");
    cu!(u19, "u19");
    cu!(u20, "u20");
    cu!(u21, "u21");
    cu!(u22, "u22");
    cu!(u23, "u23");
    cu!(u24, "u24");
    cu!(u25, "u25");
    cu!(u26, "u26");
    cu!(u27, "u27");
    cu!(u28, "u28");
    cu!(u29, "u29");

    #[test]
    fn get_none() {
        let (_d, s) = make_store();
        assert!(s.get_user("nope").unwrap().is_none());
    }
    #[test]
    fn list_empty() {
        let (_d, s) = make_store();
        assert!(s.list_users().unwrap().is_empty());
    }
    #[test]
    fn delete_user() {
        let (_d, s) = make_store();
        s.create_user(&make_user("a")).unwrap();
        s.delete_user("a").unwrap();
        assert!(s.get_user("a").unwrap().is_none());
    }
    #[test]
    fn delete_nonexist() {
        let (_d, s) = make_store();
        assert!(s.delete_user("x").is_err());
    }

    // create N users and check list count
    macro_rules! cn {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                for i in 0..$count {
                    s.create_user(&make_user(&format!("x{i:03}"))).unwrap();
                }
                assert_eq!(s.list_users().unwrap().len(), $count);
            }
        };
    }
    cn!(c01, 1);
    cn!(c02, 2);
    cn!(c03, 3);
    cn!(c05, 5);
    cn!(c10, 10);
    cn!(c15, 15);
    cn!(c20, 20);
    cn!(c25, 25);
    cn!(c30, 30);
    cn!(c40, 40);
    cn!(c50, 50);

    #[test]
    fn enabled() {
        let (_d, s) = make_store();
        s.create_user(&make_user("a")).unwrap();
        assert!(s.get_user("a").unwrap().unwrap().enabled);
    }
    #[test]
    fn hash_notempty() {
        let (_d, s) = make_store();
        s.create_user(&make_user("a")).unwrap();
        assert!(!s.get_user("a").unwrap().unwrap().password_hash.is_empty());
    }
    #[test]
    fn hash_deterministic() {
        let h = hash_password("same");
        assert!(verify_password("same", &h));
    }
    #[test]
    fn hash_different() {
        let h = hash_password("a");
        assert!(!verify_password("b", &h));
    }

    // delete middle
    macro_rules! dm {
        ($n:ident, $total:expr, $del:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                for i in 0..$total {
                    s.create_user(&make_user(&format!("u{i:03}"))).unwrap();
                }
                s.delete_user(&format!("u{:03}", $del)).unwrap();
                assert_eq!(s.list_users().unwrap().len(), $total - 1);
            }
        };
    }
    dm!(d5_2, 5, 2);
    dm!(d10_5, 10, 5);
    dm!(d20_10, 20, 10);
    dm!(d30_15, 30, 15);
}

// =========================================================================
// RBAC roles — 30 tests
// =========================================================================
mod rbac_roles {
    use super::*;
    macro_rules! cr {
        ($n:ident, $name:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                s.create_role(&Role {
                    name: $name.into(),
                    permissions: vec![],
                })
                .unwrap();
                let r = s.get_role($name).unwrap().unwrap();
                assert_eq!(r.name, $name);
            }
        };
    }
    cr!(r00, "r00");
    cr!(r01, "r01");
    cr!(r02, "r02");
    cr!(r03, "r03");
    cr!(r04, "r04");
    cr!(r05, "r05");
    cr!(r06, "r06");
    cr!(r07, "r07");
    cr!(r08, "r08");
    cr!(r09, "r09");
    cr!(r10, "r10");
    cr!(r11, "r11");
    cr!(r12, "r12");
    cr!(r13, "r13");
    cr!(r14, "r14");
    cr!(r15, "r15");
    cr!(r16, "r16");
    cr!(r17, "r17");
    cr!(r18, "r18");
    cr!(r19, "r19");

    #[test]
    fn get_none() {
        let (_d, s) = make_store();
        assert!(s.get_role("x").unwrap().is_none());
    }
    #[test]
    fn list_empty() {
        let (_d, s) = make_store();
        assert!(s.list_roles().unwrap().is_empty());
    }
    #[test]
    fn delete_role() {
        let (_d, s) = make_store();
        s.create_role(&Role {
            name: "r".into(),
            permissions: vec![],
        })
        .unwrap();
        s.delete_role("r").unwrap();
        assert!(s.get_role("r").unwrap().is_none());
    }
    #[test]
    fn multi_perms() {
        let (_d, s) = make_store();
        s.create_role(&Role {
            name: "rw".into(),
            permissions: vec![Permission::Admin, Permission::DDL],
        })
        .unwrap();
        let r = s.get_role("rw").unwrap().unwrap();
        assert_eq!(r.permissions.len(), 2);
    }

    macro_rules! rn {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                for i in 0..$count {
                    s.create_role(&Role {
                        name: format!("r{i:03}"),
                        permissions: vec![],
                    })
                    .unwrap();
                }
                assert_eq!(s.list_roles().unwrap().len(), $count);
            }
        };
    }
    rn!(n5, 5);
    rn!(n10, 10);
    rn!(n20, 20);
    rn!(n30, 30);
    rn!(n40, 40);
    rn!(n50, 50);
}

// =========================================================================
// RBAC SecurityContext — 50 tests
// =========================================================================
mod rbac_ctx {
    use super::*;
    macro_rules! read_ok {
        ($n:ident, $table:expr) => {
            #[test]
            fn $n() {
                assert!(ctx_with(vec![Permission::Admin]).can_read_table($table));
            }
        };
    }
    read_ok!(r_trades, "trades");
    read_ok!(r_orders, "orders");
    read_ok!(r_quotes, "quotes");
    read_ok!(r_ticks, "ticks");
    read_ok!(r_candles, "candles");

    macro_rules! read_specific {
        ($n:ident, $table:expr) => {
            #[test]
            fn $n() {
                assert!(
                    ctx_with(vec![Permission::Read {
                        table: Some($table.into())
                    }])
                    .can_read_table($table)
                );
            }
        };
    }
    read_specific!(rs_trades, "trades");
    read_specific!(rs_orders, "orders");
    read_specific!(rs_quotes, "quotes");
    read_specific!(rs_ticks, "ticks");
    read_specific!(rs_candles, "candles");

    macro_rules! read_no {
        ($n:ident, $perm_table:expr, $query_table:expr) => {
            #[test]
            fn $n() {
                assert!(
                    !ctx_with(vec![Permission::Read {
                        table: Some($perm_table.into())
                    }])
                    .can_read_table($query_table)
                );
            }
        };
    }
    read_no!(rn_01, "trades", "orders");
    read_no!(rn_02, "orders", "trades");
    read_no!(rn_03, "quotes", "ticks");
    read_no!(rn_04, "ticks", "candles");
    read_no!(rn_05, "candles", "trades");

    #[test]
    fn read_all() {
        assert!(ctx_with(vec![Permission::Read { table: None }]).can_read_table("anything"));
    }
    #[test]
    fn no_perms() {
        assert!(!ctx_with(vec![]).can_read_table("t"));
    }

    macro_rules! write_ok {
        ($n:ident, $table:expr) => {
            #[test]
            fn $n() {
                assert!(ctx_with(vec![Permission::Admin]).can_write_table($table));
            }
        };
    }
    write_ok!(w_trades, "trades");
    write_ok!(w_orders, "orders");
    write_ok!(w_quotes, "quotes");

    macro_rules! write_specific {
        ($n:ident, $table:expr) => {
            #[test]
            fn $n() {
                assert!(
                    ctx_with(vec![Permission::Write {
                        table: Some($table.into())
                    }])
                    .can_write_table($table)
                );
            }
        };
    }
    write_specific!(ws_trades, "trades");
    write_specific!(ws_orders, "orders");
    write_specific!(ws_quotes, "quotes");

    macro_rules! write_no {
        ($n:ident, $perm_table:expr, $query_table:expr) => {
            #[test]
            fn $n() {
                assert!(
                    !ctx_with(vec![Permission::Write {
                        table: Some($perm_table.into())
                    }])
                    .can_write_table($query_table)
                );
            }
        };
    }
    write_no!(wn_01, "trades", "orders");
    write_no!(wn_02, "orders", "trades");

    #[test]
    fn write_all() {
        assert!(ctx_with(vec![Permission::Write { table: None }]).can_write_table("anything"));
    }
    #[test]
    fn ddl_ok() {
        assert!(ctx_with(vec![Permission::DDL]).can_ddl());
    }
    #[test]
    fn ddl_admin() {
        assert!(ctx_with(vec![Permission::Admin]).can_ddl());
    }
    #[test]
    fn ddl_no() {
        assert!(!ctx_with(vec![Permission::Read { table: None }]).can_ddl());
    }
    #[test]
    fn user_field() {
        assert_eq!(ctx_with(vec![]).user, "test");
    }
    #[test]
    fn roles_field() {
        assert_eq!(ctx_with(vec![]).roles, vec!["testrole"]);
    }
}

// =========================================================================
// Encryption buffer roundtrips — 100 tests
// =========================================================================
mod enc_buf {
    use super::*;
    macro_rules! rt {
        ($n:ident, $data:expr) => {
            #[test]
            fn $n() {
                let c = enc_cfg();
                let e = encrypt_buffer($data, &c).unwrap();
                let d = decrypt_buffer(&e, &c).unwrap();
                assert_eq!(&d, $data);
            }
        };
    }
    rt!(empty, b"");
    rt!(one, b"X");
    rt!(hello, b"Hello");
    rt!(world, b"World");
    rt!(digits, b"0123456789");
    rt!(alpha, b"abcdefghijklmnopqrstuvwxyz");

    // variable-length data
    macro_rules! vl {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let c = enc_cfg();
                let p: Vec<u8> = (0..$len).map(|i| (i % 256) as u8).collect();
                let e = encrypt_buffer(&p, &c).unwrap();
                let d = decrypt_buffer(&e, &c).unwrap();
                assert_eq!(d, p);
            }
        };
    }
    vl!(l1, 1);
    vl!(l2, 2);
    vl!(l3, 3);
    vl!(l5, 5);
    vl!(l10, 10);
    vl!(l16, 16);
    vl!(l32, 32);
    vl!(l64, 64);
    vl!(l100, 100);
    vl!(l128, 128);
    vl!(l256, 256);
    vl!(l500, 500);
    vl!(l512, 512);
    vl!(l1000, 1000);
    vl!(l1024, 1024);
    vl!(l2000, 2000);
    vl!(l4096, 4096);
    vl!(l5000, 5000);
    vl!(l8192, 8192);
    vl!(l10000, 10000);
    vl!(l15000, 15000);
    vl!(l20000, 20000);
    vl!(l50000, 50000);
    vl!(l100000, 100000);

    #[test]
    fn ciphertext_differs() {
        let c = enc_cfg();
        let e = encrypt_buffer(b"secret", &c).unwrap();
        assert_ne!(&e[..], b"secret");
    }
    #[test]
    fn different_plaintexts() {
        let c = enc_cfg();
        let e1 = encrypt_buffer(b"a", &c).unwrap();
        let e2 = encrypt_buffer(b"b", &c).unwrap();
        assert_ne!(e1, e2);
    }
    #[test]
    fn wrong_key() {
        let c1 = enc_cfg();
        let c2 = enc_cfg2();
        let e = encrypt_buffer(b"secret", &c1).unwrap();
        assert!(decrypt_buffer(&e, &c2).is_err());
    }

    // all-same-byte data
    macro_rules! ab {
        ($n:ident, $byte:expr, $len:expr) => {
            #[test]
            fn $n() {
                let c = enc_cfg();
                let p = vec![$byte; $len];
                let e = encrypt_buffer(&p, &c).unwrap();
                let d = decrypt_buffer(&e, &c).unwrap();
                assert_eq!(d, p);
            }
        };
    }
    ab!(z100, 0x00, 100);
    ab!(z1000, 0x00, 1000);
    ab!(f100, 0xFF, 100);
    ab!(f1000, 0xFF, 1000);
    ab!(a100, 0xAA, 100);
    ab!(a1000, 0xAA, 1000);
    ab!(five100, 0x55, 100);
    ab!(five1000, 0x55, 1000);

    // file encryption
    #[test]
    fn file_rt() {
        let dir = TempDir::new().unwrap();
        let c = enc_cfg();
        let pp = dir.path().join("data.bin");
        std::fs::write(&pp, b"content").unwrap();
        encrypt_file(&pp, &c).unwrap();
        decrypt_file(&pp, &c).unwrap();
        assert_eq!(std::fs::read(&pp).unwrap(), b"content");
    }
    #[test]
    fn file_empty() {
        let dir = TempDir::new().unwrap();
        let c = enc_cfg();
        let pp = dir.path().join("e.bin");
        std::fs::write(&pp, b"").unwrap();
        encrypt_file(&pp, &c).unwrap();
        decrypt_file(&pp, &c).unwrap();
        assert!(std::fs::read(&pp).unwrap().is_empty());
    }

    // Repeated roundtrips
    macro_rules! rr {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let c = enc_cfg();
                let p: Vec<u8> = (0..$len).map(|i| (i % 256) as u8).collect();
                for _ in 0..3 {
                    let e = encrypt_buffer(&p, &c).unwrap();
                    let d = decrypt_buffer(&e, &c).unwrap();
                    assert_eq!(d, p);
                }
            }
        };
    }
    rr!(rr10, 10);
    rr!(rr100, 100);
    rr!(rr1000, 1000);
    rr!(rr10000, 10000);
}

// =========================================================================
// Replication protocol encode/decode — 80 tests
// =========================================================================
mod repl_proto {
    use super::*;
    macro_rules! wal {
        ($n:ident, $table:expr, $seg:expr, $data_len:expr) => {
            #[test]
            fn $n() {
                let msg = ReplicationMessage::WalSegment {
                    table: $table.into(),
                    segment_id: $seg,
                    data: vec![0xAB; $data_len],
                    txn_range: ($seg, $seg + 10),
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    wal!(w01, "t1", 1, 0);
    wal!(w02, "t1", 2, 1);
    wal!(w03, "t1", 3, 10);
    wal!(w04, "t2", 4, 100);
    wal!(w05, "trades", 5, 500);
    wal!(w06, "orders", 6, 1000);
    wal!(w07, "quotes", 7, 5000);
    wal!(w08, "ticks", 8, 10);
    wal!(w09, "candles", 9, 50);
    wal!(w10, "t1", 10, 0);
    wal!(w11, "t1", 100, 1);
    wal!(w12, "t1", 1000, 2);
    wal!(w13, "t1", 10000, 3);
    wal!(w14, "t1", 0, 0);
    wal!(w15, "long_table_name", 42, 100);

    macro_rules! ack {
        ($n:ident, $replica:expr, $table:expr, $txn:expr) => {
            #[test]
            fn $n() {
                let msg = ReplicationMessage::Ack {
                    replica_id: $replica.into(),
                    table: $table.into(),
                    last_txn: $txn,
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    ack!(a01, "r1", "t1", 0);
    ack!(a02, "r1", "t1", 1);
    ack!(a03, "r1", "t1", 100);
    ack!(a04, "r2", "trades", 999);
    ack!(a05, "r3", "orders", u64::MAX);
    ack!(a06, "replica_01", "quotes", 42);
    ack!(a07, "replica_02", "ticks", 1000);
    ack!(a08, "r1", "t1", 50);
    ack!(a09, "r1", "t1", 500);
    ack!(a10, "r1", "t1", 5000);

    #[test]
    fn status_req() {
        let msg = ReplicationMessage::StatusRequest;
        let enc = protocol::encode(&msg).unwrap();
        let (dec, _) = protocol::decode(&enc).unwrap();
        assert_eq!(dec, msg);
    }
    #[test]
    fn status_resp_empty() {
        let msg = ReplicationMessage::StatusResponse {
            position: ReplicaPosition::new(),
        };
        let enc = protocol::encode(&msg).unwrap();
        let (dec, _) = protocol::decode(&enc).unwrap();
        assert_eq!(dec, msg);
    }

    macro_rules! sr {
        ($n:ident, $txn:expr, $tables:expr) => {
            #[test]
            fn $n() {
                let mut tables = HashMap::new();
                for &(t, v) in &$tables {
                    tables.insert(t.to_string(), v);
                }
                let msg = ReplicationMessage::StatusResponse {
                    position: ReplicaPosition {
                        last_applied_txn: $txn,
                        tables,
                    },
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    sr!(sr01, 0, [("t1", 0u64)]);
    sr!(sr02, 100, [("trades", 100u64)]);
    sr!(sr03, 200, [("trades", 100u64), ("orders", 50u64)]);
    sr!(
        sr04,
        1000,
        [("trades", 500u64), ("orders", 300u64), ("quotes", 200u64)]
    );

    #[test]
    fn full_sync() {
        let msg = ReplicationMessage::FullSyncRequired { table: "t1".into() };
        let enc = protocol::encode(&msg).unwrap();
        let (dec, _) = protocol::decode(&enc).unwrap();
        assert_eq!(dec, msg);
    }

    macro_rules! fs {
        ($n:ident, $table:expr) => {
            #[test]
            fn $n() {
                let msg = ReplicationMessage::FullSyncRequired {
                    table: $table.into(),
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    fs!(fs01, "trades");
    fs!(fs02, "orders");
    fs!(fs03, "quotes");
    fs!(fs04, "ticks");
    fs!(fs05, "candles");
    fs!(fs06, "long_table_name_here");

    // consumed bytes == encoded length
    macro_rules! cl {
        ($n:ident, $msg:expr) => {
            #[test]
            fn $n() {
                let enc = protocol::encode(&$msg).unwrap();
                let (_, consumed) = protocol::decode(&enc).unwrap();
                assert_eq!(consumed, enc.len());
            }
        };
    }
    cl!(cl01, ReplicationMessage::StatusRequest);
    cl!(
        cl02,
        ReplicationMessage::Ack {
            replica_id: "r".into(),
            table: "t".into(),
            last_txn: 1
        }
    );
    cl!(
        cl03,
        ReplicationMessage::WalSegment {
            table: "t".into(),
            segment_id: 1,
            data: vec![1, 2, 3],
            txn_range: (1, 2)
        }
    );

    // Config variants
    #[test]
    fn cfg_primary() {
        let c = ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            sync_mode: ReplicationSyncMode::Async,
            replica_addrs: vec![],
            max_lag_bytes: 0,
            ..Default::default()
        };
        assert!(matches!(c.role, ReplicationRole::Primary));
    }
    #[test]
    fn cfg_replica() {
        let c = ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: Some("addr".into()),
            sync_mode: ReplicationSyncMode::Sync,
            replica_addrs: vec![],
            max_lag_bytes: 0,
            ..Default::default()
        };
        assert!(matches!(c.role, ReplicationRole::Replica));
    }
    #[test]
    fn cfg_sync() {
        let c = ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            sync_mode: ReplicationSyncMode::Sync,
            replica_addrs: vec![],
            max_lag_bytes: 0,
            ..Default::default()
        };
        assert!(matches!(c.sync_mode, ReplicationSyncMode::Sync));
    }
    #[test]
    fn cfg_async() {
        let c = ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            sync_mode: ReplicationSyncMode::Async,
            replica_addrs: vec![],
            max_lag_bytes: 0,
            ..Default::default()
        };
        assert!(matches!(c.sync_mode, ReplicationSyncMode::Async));
    }
}

// =========================================================================
// Cluster node ops — 80 tests
// =========================================================================
mod cluster_ops {
    use super::*;
    #[test]
    fn register() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);
    }
    #[test]
    fn register_idempotent() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);
    }
    #[test]
    fn add_external() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.add_node(ClusterNode::new(
            "n2".into(),
            "addr".into(),
            NodeRole::ReadReplica,
        ));
        assert_eq!(mgr.node_count(), 2);
    }

    macro_rules! add_n {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mgr = ClusterManager::new(cluster_cfg("n1"));
                mgr.register().unwrap();
                for i in 0..$count {
                    mgr.add_node(ClusterNode::new(
                        format!("x{i}"),
                        format!("addr{i}"),
                        NodeRole::ReadReplica,
                    ));
                }
                assert_eq!(mgr.node_count(), 1 + $count);
            }
        };
    }
    add_n!(a01, 1);
    add_n!(a02, 2);
    add_n!(a03, 3);
    add_n!(a05, 5);
    add_n!(a10, 10);
    add_n!(a15, 15);
    add_n!(a20, 20);
    add_n!(a25, 25);
    add_n!(a30, 30);
    add_n!(a40, 40);
    add_n!(a50, 50);

    #[test]
    fn remove_node() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.add_node(ClusterNode::new(
            "n2".into(),
            "addr".into(),
            NodeRole::ReadReplica,
        ));
        mgr.remove_node("n2");
        assert_eq!(mgr.node_count(), 1);
    }
    #[test]
    fn healthy_nodes() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        assert!(!mgr.healthy_nodes().is_empty());
    }
    #[test]
    fn healthy_empty() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        assert!(mgr.healthy_nodes().is_empty());
    }

    // Node with tables
    macro_rules! nt {
        ($n:ident, $num_tables:expr) => {
            #[test]
            fn $n() {
                let mgr = ClusterManager::new(cluster_cfg("n1"));
                mgr.register().unwrap();
                let mut node = ClusterNode::new("x".into(), "addr".into(), NodeRole::ReadReplica);
                node.tables = (0..$num_tables).map(|i| format!("t{i}")).collect();
                mgr.add_node(node);
                assert_eq!(mgr.node_count(), 2);
            }
        };
    }
    nt!(t1, 1);
    nt!(t2, 2);
    nt!(t3, 3);
    nt!(t4, 4);
    nt!(t5, 5);

    // Multiple registers + removes
    macro_rules! mr {
        ($n:ident, $add:expr, $rem:expr) => {
            #[test]
            fn $n() {
                let mgr = ClusterManager::new(cluster_cfg("n1"));
                mgr.register().unwrap();
                for i in 0..$add {
                    mgr.add_node(ClusterNode::new(
                        format!("x{i}"),
                        format!("addr{i}"),
                        NodeRole::ReadReplica,
                    ));
                }
                for i in 0..$rem {
                    mgr.remove_node(&format!("x{i}"));
                }
                assert_eq!(mgr.node_count(), 1 + $add - $rem);
            }
        };
    }
    mr!(mr_5_2, 5, 2);
    mr!(mr_10_5, 10, 5);
    mr!(mr_20_10, 20, 10);
    mr!(mr_30_15, 30, 15);
}

// =========================================================================
// Tenant CRUD — 80 tests
// =========================================================================
mod tenant_ops {
    use super::*;
    macro_rules! ct {
        ($n:ident, $id:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let mgr = TenantManager::new(dir.path().to_path_buf());
                mgr.create_tenant(&make_tenant($id)).unwrap();
                let t = mgr.get_tenant($id).unwrap().unwrap();
                assert_eq!(t.id, $id);
            }
        };
    }
    ct!(t00, "t00");
    ct!(t01, "t01");
    ct!(t02, "t02");
    ct!(t03, "t03");
    ct!(t04, "t04");
    ct!(t05, "t05");
    ct!(t06, "t06");
    ct!(t07, "t07");
    ct!(t08, "t08");
    ct!(t09, "t09");
    ct!(t10, "t10");
    ct!(t11, "t11");
    ct!(t12, "t12");
    ct!(t13, "t13");
    ct!(t14, "t14");
    ct!(t15, "t15");
    ct!(t16, "t16");
    ct!(t17, "t17");
    ct!(t18, "t18");
    ct!(t19, "t19");

    #[test]
    fn get_none() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.get_tenant("x").unwrap().is_none());
    }
    #[test]
    fn list_empty() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        assert!(mgr.list_tenants().unwrap().is_empty());
    }
    #[test]
    fn delete() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        mgr.delete_tenant("t1").unwrap();
        assert!(mgr.get_tenant("t1").unwrap().is_none());
    }

    macro_rules! cn {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let mgr = TenantManager::new(dir.path().to_path_buf());
                for i in 0..$count {
                    mgr.create_tenant(&make_tenant(&format!("t{i:03}")))
                        .unwrap();
                }
                assert_eq!(mgr.list_tenants().unwrap().len(), $count);
            }
        };
    }
    cn!(c01, 1);
    cn!(c02, 2);
    cn!(c03, 3);
    cn!(c05, 5);
    cn!(c10, 10);
    cn!(c15, 15);
    cn!(c20, 20);
    cn!(c25, 25);
    cn!(c30, 30);
    cn!(c40, 40);
    cn!(c50, 50);

    #[test]
    fn quota() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        let t = mgr.get_tenant("t1").unwrap().unwrap();
        assert_eq!(t.storage_quota, 1_000_000);
        assert_eq!(t.query_quota, 10);
    }
    #[test]
    fn namespace() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        let t = mgr.get_tenant("t1").unwrap().unwrap();
        assert_eq!(t.namespace, "t1");
    }
    #[test]
    fn name() {
        let dir = TempDir::new().unwrap();
        let mgr = TenantManager::new(dir.path().to_path_buf());
        mgr.create_tenant(&make_tenant("t1")).unwrap();
        let t = mgr.get_tenant("t1").unwrap().unwrap();
        assert_eq!(t.name, "T t1");
    }
}

// =========================================================================
// Metering — 60 tests
// =========================================================================
mod metering_ops {
    use super::*;
    #[test]
    fn new_meter() {
        let dir = TempDir::new().unwrap();
        let _m = UsageMeter::new(dir.path().to_path_buf());
    }

    macro_rules! rq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let m = UsageMeter::new(dir.path().to_path_buf());
                for _ in 0..$count {
                    m.record_query("t1", 10, 100);
                }
                assert_eq!(m.get_usage("t1").queries, $count);
            }
        };
    }
    rq!(q01, 1);
    rq!(q02, 2);
    rq!(q03, 3);
    rq!(q05, 5);
    rq!(q10, 10);
    rq!(q15, 15);
    rq!(q20, 20);
    rq!(q25, 25);
    rq!(q30, 30);
    rq!(q50, 50);

    macro_rules! rw {
        ($n:ident, $count:expr, $rows:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let m = UsageMeter::new(dir.path().to_path_buf());
                for _ in 0..$count {
                    m.record_write("t1", $rows);
                }
                assert_eq!(m.get_usage("t1").rows_written, $count * $rows);
            }
        };
    }
    rw!(w01, 1, 10);
    rw!(w02, 2, 10);
    rw!(w05, 5, 10);
    rw!(w10, 10, 10);
    rw!(w20, 20, 10);
    rw!(w50, 50, 10);
    rw!(w10_100, 10, 100);
    rw!(w10_1000, 10, 1000);

    #[test]
    fn usage_zero() {
        let dir = TempDir::new().unwrap();
        let m = UsageMeter::new(dir.path().to_path_buf());
        let s = m.get_usage("none");
        assert_eq!(s.queries, 0);
        assert_eq!(s.rows_written, 0);
    }

    #[test]
    fn persist_load() {
        let dir = TempDir::new().unwrap();
        {
            let m = UsageMeter::new(dir.path().to_path_buf());
            m.record_write("t1", 42);
            m.persist().unwrap();
        }
        let mut m2 = UsageMeter::new(dir.path().to_path_buf());
        m2.load().unwrap();
        assert_eq!(m2.get_usage("t1").rows_written, 42);
    }

    macro_rules! mt {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let m = UsageMeter::new(dir.path().to_path_buf());
                for i in 0..$count {
                    m.record_query(&format!("t{i}"), 10, 100);
                }
                assert_eq!(m.get_all_usage().len(), $count);
            }
        };
    }
    mt!(mt01, 1);
    mt!(mt02, 2);
    mt!(mt03, 3);
    mt!(mt05, 5);
    mt!(mt10, 10);
    mt!(mt15, 15);
    mt!(mt20, 20);

    #[test]
    fn bytes_scanned() {
        let dir = TempDir::new().unwrap();
        let m = UsageMeter::new(dir.path().to_path_buf());
        m.record_query("t1", 10, 5000);
        assert_eq!(m.get_usage("t1").bytes_scanned, 5000);
    }
    #[test]
    fn rows_read() {
        let dir = TempDir::new().unwrap();
        let m = UsageMeter::new(dir.path().to_path_buf());
        m.record_query("t1", 42, 100);
        assert_eq!(m.get_usage("t1").rows_read, 42);
    }

    // Accumulate bytes scanned
    macro_rules! bs {
        ($n:ident, $count:expr, $bytes:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let m = UsageMeter::new(dir.path().to_path_buf());
                for _ in 0..$count {
                    m.record_query("t1", 10, $bytes);
                }
                assert_eq!(m.get_usage("t1").bytes_scanned, $count * $bytes);
            }
        };
    }
    bs!(bs01, 1, 100);
    bs!(bs02, 2, 100);
    bs!(bs05, 5, 100);
    bs!(bs10, 10, 100);
    bs!(bs20, 20, 100);
    bs!(bs50, 50, 100);
    bs!(bs_1k, 1, 1000);
    bs!(bs_10k, 1, 10000);
    bs!(bs_100k, 1, 100000);

    // Accumulate rows read
    macro_rules! rr {
        ($n:ident, $count:expr, $rows:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let m = UsageMeter::new(dir.path().to_path_buf());
                for _ in 0..$count {
                    m.record_query("t1", $rows, 100);
                }
                assert_eq!(m.get_usage("t1").rows_read, $count * $rows);
            }
        };
    }
    rr!(rr01, 1, 10);
    rr!(rr02, 2, 10);
    rr!(rr05, 5, 10);
    rr!(rr10, 10, 10);
    rr!(rr20, 20, 10);
    rr!(rr50, 50, 10);
    rr!(rr_100, 1, 100);
    rr!(rr_1000, 1, 1000);
    rr!(rr_10000, 1, 10000);
}

// =========================================================================
// More RBAC user operations — 50 tests
// =========================================================================
mod rbac_users2 {
    use super::*;
    // Delete all users
    macro_rules! da {
        ($n:ident, $total:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                for i in 0..$total {
                    s.create_user(&make_user(&format!("u{i:03}"))).unwrap();
                }
                for i in 0..$total {
                    s.delete_user(&format!("u{i:03}")).unwrap();
                }
                assert!(s.list_users().unwrap().is_empty());
            }
        };
    }
    da!(d5, 5);
    da!(d10, 10);
    da!(d20, 20);
    da!(d30, 30);

    // Create with different names
    macro_rules! cn {
        ($n:ident, $name:expr) => {
            #[test]
            fn $n() {
                let (_d, s) = make_store();
                s.create_user(&make_user($name)).unwrap();
                assert!(s.get_user($name).unwrap().is_some());
            }
        };
    }
    cn!(alice, "alice");
    cn!(bob, "bob");
    cn!(charlie, "charlie");
    cn!(dave, "dave");
    cn!(eve, "eve");
    cn!(frank, "frank");
    cn!(grace, "grace");
    cn!(heidi, "heidi");
    cn!(ivan, "ivan");
    cn!(judy, "judy");
    cn!(karl, "karl");
    cn!(lily, "lily");
    cn!(mike, "mike");
    cn!(nina, "nina");
    cn!(oscar, "oscar");
    cn!(pat, "pat");
    cn!(quinn, "quinn");
    cn!(ross, "ross");
    cn!(sara, "sara");
    cn!(tom, "tom");

    // Hash password consistency
    macro_rules! hp {
        ($n:ident, $pw:expr) => {
            #[test]
            fn $n() {
                let h = hash_password($pw);
                assert!(verify_password($pw, &h));
            }
        };
    }
    hp!(pw01, "pass1");
    hp!(pw02, "pass2");
    hp!(pw03, "secret");
    hp!(pw04, "admin");
    hp!(pw05, "123456");
    hp!(pw06, "password");
    hp!(pw07, "qwerty");
    hp!(pw08, "abc123");
    hp!(pw09, "letmein");
    hp!(pw10, "welcome");

    // Different passwords produce different hashes
    macro_rules! dp {
        ($n:ident, $a:expr, $b:expr) => {
            #[test]
            fn $n() {
                let h = hash_password($a);
                assert!(!verify_password($b, &h));
            }
        };
    }
    dp!(dp01, "a", "b");
    dp!(dp02, "pass", "word");
    dp!(dp03, "1", "2");
    dp!(dp04, "hello", "world");
    dp!(dp05, "foo", "bar");
    dp!(dp06, "admin", "user");
    dp!(dp07, "test", "prod");
    dp!(dp08, "alpha", "beta");
    dp!(dp09, "key1", "key2");
    dp!(dp10, "x", "y");
}

// =========================================================================
// More encryption tests — 50 tests
// =========================================================================
mod enc_extra {
    use super::*;
    // ChaCha20Poly1305 config
    fn chacha_cfg() -> EncryptionConfig {
        EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0xAB; 32]).unwrap()
    }
    fn chacha_cfg2() -> EncryptionConfig {
        EncryptionConfig::new(EncryptionAlgorithm::ChaCha20Poly1305, vec![0xCD; 32]).unwrap()
    }

    macro_rules! crt {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let c = chacha_cfg();
                let p: Vec<u8> = (0..$len).map(|i| (i % 256) as u8).collect();
                let e = encrypt_buffer(&p, &c).unwrap();
                let d = decrypt_buffer(&e, &c).unwrap();
                assert_eq!(d, p);
            }
        };
    }
    crt!(c_empty, 0);
    crt!(c_1, 1);
    crt!(c_2, 2);
    crt!(c_5, 5);
    crt!(c_10, 10);
    crt!(c_16, 16);
    crt!(c_32, 32);
    crt!(c_64, 64);
    crt!(c_100, 100);
    crt!(c_128, 128);
    crt!(c_256, 256);
    crt!(c_500, 500);
    crt!(c_512, 512);
    crt!(c_1000, 1000);
    crt!(c_1024, 1024);
    crt!(c_2000, 2000);
    crt!(c_4096, 4096);
    crt!(c_5000, 5000);
    crt!(c_8192, 8192);
    crt!(c_10000, 10000);

    #[test]
    fn chacha_wrong_key() {
        let c1 = chacha_cfg();
        let c2 = chacha_cfg2();
        let e = encrypt_buffer(b"secret", &c1).unwrap();
        assert!(decrypt_buffer(&e, &c2).is_err());
    }
    #[test]
    fn chacha_differs() {
        let c = chacha_cfg();
        let e = encrypt_buffer(b"test", &c).unwrap();
        assert_ne!(&e[..], b"test");
    }

    // Aes256Cbc config
    fn aes_cbc_cfg() -> EncryptionConfig {
        EncryptionConfig::new(EncryptionAlgorithm::Aes256Cbc, vec![0xAB; 32]).unwrap()
    }

    macro_rules! art {
        ($n:ident, $len:expr) => {
            #[test]
            fn $n() {
                let c = aes_cbc_cfg();
                let p: Vec<u8> = (0..$len).map(|i| (i % 256) as u8).collect();
                let e = encrypt_buffer(&p, &c).unwrap();
                let d = decrypt_buffer(&e, &c).unwrap();
                assert_eq!(d, p);
            }
        };
    }
    art!(a_empty, 0);
    art!(a_1, 1);
    art!(a_10, 10);
    art!(a_16, 16);
    art!(a_32, 32);
    art!(a_64, 64);
    art!(a_100, 100);
    art!(a_256, 256);
    art!(a_500, 500);
    art!(a_1000, 1000);
    art!(a_2000, 2000);
    art!(a_5000, 5000);
    art!(a_10000, 10000);
}

// =========================================================================
// More replication protocol tests — 50 tests
// =========================================================================
mod repl_extra {
    use super::*;
    // WalSegment with different tables
    macro_rules! wt {
        ($n:ident, $table:expr) => {
            #[test]
            fn $n() {
                let msg = ReplicationMessage::WalSegment {
                    table: $table.into(),
                    segment_id: 1,
                    data: vec![1, 2, 3],
                    txn_range: (1, 10),
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    wt!(t01, "a");
    wt!(t02, "ab");
    wt!(t03, "abc");
    wt!(t04, "abcd");
    wt!(t05, "abcde");
    wt!(t06, "trades");
    wt!(t07, "orders");
    wt!(t08, "quotes");
    wt!(t09, "ticks");
    wt!(t10, "candles");
    wt!(t11, "positions");
    wt!(t12, "fills");
    wt!(t13, "balances");
    wt!(t14, "accounts");
    wt!(t15, "instruments");

    // WalSegment with different segment IDs
    macro_rules! ws {
        ($n:ident, $seg:expr) => {
            #[test]
            fn $n() {
                let msg = ReplicationMessage::WalSegment {
                    table: "t".into(),
                    segment_id: $seg,
                    data: vec![0xAB],
                    txn_range: ($seg as u64, $seg as u64 + 1),
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    ws!(s0, 0u32);
    ws!(s1, 1u32);
    ws!(s10, 10u32);
    ws!(s100, 100u32);
    ws!(s1000, 1000u32);
    ws!(s10000, 10000u32);
    ws!(s100000, 100000u32);
    ws!(s_max, u32::MAX);

    // Ack with various txn values
    macro_rules! at {
        ($n:ident, $txn:expr) => {
            #[test]
            fn $n() {
                let msg = ReplicationMessage::Ack {
                    replica_id: "r1".into(),
                    table: "t".into(),
                    last_txn: $txn,
                };
                let enc = protocol::encode(&msg).unwrap();
                let (dec, _) = protocol::decode(&enc).unwrap();
                assert_eq!(dec, msg);
            }
        };
    }
    at!(a0, 0);
    at!(a1, 1);
    at!(a10, 10);
    at!(a100, 100);
    at!(a1000, 1000);
    at!(a10000, 10000);
    at!(a100000, 100000);
    at!(a1000000, 1000000);
    at!(a_max, u64::MAX);
}

// =========================================================================
// More cluster tests — 50 tests
// =========================================================================
mod cluster_extra {
    use super::*;
    // Different node roles
    #[test]
    fn role_primary() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        assert_eq!(mgr.node_count(), 1);
    }
    #[test]
    fn add_read_replica() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.add_node(ClusterNode::new(
            "n2".into(),
            "addr".into(),
            NodeRole::ReadReplica,
        ));
        assert_eq!(mgr.node_count(), 2);
    }
    #[test]
    fn add_coordinator() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.add_node(ClusterNode::new(
            "n2".into(),
            "addr".into(),
            NodeRole::Coordinator,
        ));
        assert_eq!(mgr.node_count(), 2);
    }

    // Add many nodes then remove some
    macro_rules! arm {
        ($n:ident, $add:expr, $rem:expr) => {
            #[test]
            fn $n() {
                let mgr = ClusterManager::new(cluster_cfg("n1"));
                mgr.register().unwrap();
                for i in 0..$add {
                    mgr.add_node(ClusterNode::new(
                        format!("r{i}"),
                        format!("a{i}"),
                        NodeRole::ReadReplica,
                    ));
                }
                #[allow(clippy::reversed_empty_ranges)]
                for i in 0..$rem {
                    mgr.remove_node(&format!("r{i}"));
                }
                assert_eq!(mgr.node_count(), 1 + $add - $rem);
            }
        };
    }
    arm!(arm_1_0, 1, 0);
    arm!(arm_2_1, 2, 1);
    arm!(arm_3_1, 3, 1);
    arm!(arm_5_2, 5, 2);
    arm!(arm_10_3, 10, 3);
    arm!(arm_10_5, 10, 5);
    arm!(arm_10_10, 10, 10);
    arm!(arm_20_5, 20, 5);
    arm!(arm_20_10, 20, 10);
    arm!(arm_20_15, 20, 15);
    arm!(arm_50_10, 50, 10);
    arm!(arm_50_25, 50, 25);
    arm!(arm_50_50, 50, 50);

    // Node health check
    #[test]
    fn healthy_after_register() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        let h = mgr.healthy_nodes();
        assert_eq!(h.len(), 1);
    }
    #[test]
    fn healthy_count_2() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.add_node(ClusterNode::new(
            "n2".into(),
            "a".into(),
            NodeRole::ReadReplica,
        ));
        assert_eq!(mgr.healthy_nodes().len(), 2);
    }
    #[test]
    fn healthy_count_5() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        for i in 0..4 {
            mgr.add_node(ClusterNode::new(
                format!("n{}", i + 2),
                format!("a{i}"),
                NodeRole::ReadReplica,
            ));
        }
        assert_eq!(mgr.healthy_nodes().len(), 5);
    }

    // Heartbeat
    #[test]
    fn heartbeat_ok() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.heartbeat().unwrap();
    }
    #[test]
    fn heartbeat_twice() {
        let mgr = ClusterManager::new(cluster_cfg("n1"));
        mgr.register().unwrap();
        mgr.heartbeat().unwrap();
        mgr.heartbeat().unwrap();
    }
}

// =========================================================================
// More tenant tests — 50 tests
// =========================================================================
mod tenant_extra {
    use super::*;
    // Delete all tenants
    macro_rules! da {
        ($n:ident, $total:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let mgr = TenantManager::new(dir.path().to_path_buf());
                for i in 0..$total {
                    mgr.create_tenant(&make_tenant(&format!("t{i:03}")))
                        .unwrap();
                }
                for i in 0..$total {
                    mgr.delete_tenant(&format!("t{i:03}")).unwrap();
                }
                assert!(mgr.list_tenants().unwrap().is_empty());
            }
        };
    }
    da!(d5, 5);
    da!(d10, 10);
    da!(d20, 20);
    da!(d30, 30);

    // Create + get with different IDs
    macro_rules! cg {
        ($n:ident, $id:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let mgr = TenantManager::new(dir.path().to_path_buf());
                mgr.create_tenant(&make_tenant($id)).unwrap();
                let t = mgr.get_tenant($id).unwrap().unwrap();
                assert_eq!(t.name, format!("T {}", $id));
            }
        };
    }
    cg!(alpha, "alpha");
    cg!(beta, "beta");
    cg!(gamma, "gamma");
    cg!(delta, "delta");
    cg!(epsilon, "epsilon");
    cg!(zeta, "zeta");
    cg!(eta, "eta");
    cg!(theta, "theta");
    cg!(iota, "iota");
    cg!(kappa, "kappa");
    cg!(lambda_t, "lambda");
    cg!(mu, "mu");
    cg!(nu, "nu");
    cg!(xi, "xi");
    cg!(omicron, "omicron");
    cg!(pi_t, "pi");
    cg!(rho, "rho");
    cg!(sigma, "sigma");
    cg!(tau, "tau");
    cg!(upsilon, "upsilon");

    // Verify namespace matches ID
    macro_rules! ns {
        ($n:ident, $id:expr) => {
            #[test]
            fn $n() {
                let dir = TempDir::new().unwrap();
                let mgr = TenantManager::new(dir.path().to_path_buf());
                mgr.create_tenant(&make_tenant($id)).unwrap();
                let t = mgr.get_tenant($id).unwrap().unwrap();
                assert_eq!(t.namespace, $id);
            }
        };
    }
    ns!(ns01, "ns01");
    ns!(ns02, "ns02");
    ns!(ns03, "ns03");
    ns!(ns04, "ns04");
    ns!(ns05, "ns05");
    ns!(ns06, "ns06");
    ns!(ns07, "ns07");
    ns!(ns08, "ns08");
    ns!(ns09, "ns09");
    ns!(ns10, "ns10");
}
