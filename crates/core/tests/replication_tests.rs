//! Comprehensive replication tests (60 tests).
//!
//! Covers protocol encode/decode, WAL shipper, WAL receiver, failover,
//! S3 shipper, auto-failover, and replication manager.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use exchange_core::replication::config::{
    ReplicationConfig, ReplicationRole, ReplicationSyncMode,
};
use exchange_core::replication::failover::FailoverManager;
use exchange_core::replication::protocol::{self, ReplicationMessage};
use exchange_core::replication::s3_shipper::{S3WalReceiver, S3WalShipper};
use exchange_core::replication::wal_receiver::{ReplicaPosition, WalReceiver};
use exchange_core::replication::wal_shipper::WalShipper;
use exchange_core::replication::AutoFailover;
use exchange_core::tiered::MemoryObjectStore;

// ---------------------------------------------------------------------------
// mod protocol
// ---------------------------------------------------------------------------

mod protocol_tests {
    use super::*;

    #[test]
    fn encode_decode_wal_segment() {
        let msg = ReplicationMessage::WalSegment {
            table: "trades".into(),
            segment_id: 42,
            data: vec![0xDE, 0xAD],
            txn_range: (100, 200),
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, consumed) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn encode_decode_ack() {
        let msg = ReplicationMessage::Ack {
            replica_id: "r1".into(),
            table: "orders".into(),
            last_txn: 999,
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_status_request() {
        let msg = ReplicationMessage::StatusRequest;
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_status_response() {
        let mut tables = HashMap::new();
        tables.insert("trades".into(), 50u64);
        tables.insert("orders".into(), 30u64);
        let msg = ReplicationMessage::StatusResponse {
            position: ReplicaPosition {
                last_applied_txn: 50,
                tables,
            },
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_full_sync_required() {
        let msg = ReplicationMessage::FullSyncRequired {
            table: "quotes".into(),
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn decode_too_short() {
        assert!(protocol::decode(&[0x01, 0x02]).is_err());
    }

    #[test]
    fn decode_incomplete_frame() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]);
        assert!(protocol::decode(&buf).is_err());
    }

    #[test]
    fn decode_invalid_json() {
        let garbage = b"not valid json";
        let len = garbage.len() as u32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(garbage);
        assert!(protocol::decode(&buf).is_err());
    }

    #[test]
    fn decode_empty_payload() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert!(protocol::decode(&buf).is_err());
    }

    #[test]
    fn multiple_messages_in_stream() {
        let msg1 = ReplicationMessage::StatusRequest;
        let msg2 = ReplicationMessage::Ack {
            replica_id: "r1".into(),
            table: "t".into(),
            last_txn: 1,
        };
        let mut stream = protocol::encode(&msg1).unwrap();
        stream.extend_from_slice(&protocol::encode(&msg2).unwrap());

        let (d1, c1) = protocol::decode(&stream).unwrap();
        assert_eq!(d1, msg1);
        let (d2, c2) = protocol::decode(&stream[c1..]).unwrap();
        assert_eq!(d2, msg2);
        assert_eq!(c1 + c2, stream.len());
    }

    #[test]
    fn large_wal_segment_data() {
        let msg = ReplicationMessage::WalSegment {
            table: "big".into(),
            segment_id: 1,
            data: vec![0xAA; 100_000],
            txn_range: (0, 1000),
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_produces_length_prefix() {
        let msg = ReplicationMessage::StatusRequest;
        let encoded = protocol::encode(&msg).unwrap();
        let len = u32::from_le_bytes(encoded[0..4].try_into().unwrap()) as usize;
        assert_eq!(len + 4, encoded.len());
    }

    #[test]
    fn encode_decode_schema_sync() {
        let msg = ReplicationMessage::SchemaSync {
            table: "orders".into(),
            meta_json: r#"{"name":"orders","version":7}"#.into(),
            version: 7,
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, consumed) = protocol::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn schema_sync_in_message_stream() {
        let msg1 = ReplicationMessage::SchemaSync {
            table: "t1".into(),
            meta_json: "{}".into(),
            version: 1,
        };
        let msg2 = ReplicationMessage::WalSegment {
            table: "t1".into(),
            segment_id: 0,
            data: vec![0xAB],
            txn_range: (1, 1),
        };

        let mut stream = protocol::encode(&msg1).unwrap();
        stream.extend_from_slice(&protocol::encode(&msg2).unwrap());

        let (d1, c1) = protocol::decode(&stream).unwrap();
        assert_eq!(d1, msg1);
        let (d2, _) = protocol::decode(&stream[c1..]).unwrap();
        assert_eq!(d2, msg2);
    }

    #[test]
    fn schema_sync_large_meta_json() {
        // Ensure large schema payloads round-trip correctly.
        let big_json = "x".repeat(100_000);
        let msg = ReplicationMessage::SchemaSync {
            table: "wide_table".into(),
            meta_json: big_json.clone(),
            version: 42,
        };
        let encoded = protocol::encode(&msg).unwrap();
        let (decoded, _) = protocol::decode(&encoded).unwrap();
        match decoded {
            ReplicationMessage::SchemaSync { meta_json, .. } => {
                assert_eq!(meta_json.len(), 100_000);
            }
            _ => panic!("expected SchemaSync"),
        }
    }
}

// ---------------------------------------------------------------------------
// mod wal_shipper
// ---------------------------------------------------------------------------

mod wal_shipper_tests {
    use super::*;

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

    #[test]
    fn initializes_positions() {
        let shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
        assert_eq!(shipper.replica_positions().len(), 2);
        assert_eq!(shipper.replica_positions().get("r1:9100"), Some(&0));
    }

    #[test]
    fn record_ack_updates() {
        let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("r1:9100", 42);
        assert_eq!(shipper.replica_positions().get("r1:9100"), Some(&42));
    }

    #[test]
    fn record_ack_does_not_regress() {
        let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("r1:9100", 42);
        shipper.record_ack("r1:9100", 10);
        assert_eq!(shipper.replica_positions().get("r1:9100"), Some(&42));
    }

    #[test]
    fn record_ack_advances() {
        let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("r1:9100", 42);
        shipper.record_ack("r1:9100", 100);
        assert_eq!(shipper.replica_positions().get("r1:9100"), Some(&100));
    }

    #[test]
    fn record_ack_unknown_replica_ignored() {
        let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("unknown:9100", 42);
        // Should not crash, unknown replica is ignored
        assert_eq!(shipper.replica_positions().len(), 1);
    }

    #[test]
    fn all_caught_up_none_acked() {
        let shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
        assert!(!shipper.all_replicas_caught_up(10));
    }

    #[test]
    fn all_caught_up_partial() {
        let mut shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("r1:9100", 10);
        assert!(!shipper.all_replicas_caught_up(10));
    }

    #[test]
    fn all_caught_up_complete() {
        let mut shipper = make_shipper(vec!["r1:9100", "r2:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("r1:9100", 10);
        shipper.record_ack("r2:9100", 10);
        assert!(shipper.all_replicas_caught_up(10));
    }

    #[test]
    fn all_caught_up_higher_target() {
        let mut shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
        shipper.record_ack("r1:9100", 10);
        assert!(!shipper.all_replicas_caught_up(11));
    }

    #[test]
    fn no_replicas_always_caught_up() {
        let shipper = make_shipper(vec![], ReplicationSyncMode::Async);
        assert!(shipper.all_replicas_caught_up(999));
    }

    #[test]
    fn replication_lag_initial() {
        let shipper = make_shipper(vec!["r1:9100"], ReplicationSyncMode::Async);
        let lags = shipper.replication_lag();
        assert_eq!(lags.len(), 1);
        assert_eq!(lags.get("r1:9100").unwrap().last_ack_txn, 0);
        assert_eq!(lags.get("r1:9100").unwrap().bytes_behind, 0);
    }
}

// ---------------------------------------------------------------------------
// mod wal_receiver
// ---------------------------------------------------------------------------

mod wal_receiver_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn new_position() {
        let pos = ReplicaPosition::new();
        assert_eq!(pos.last_applied_txn, 0);
        assert!(pos.tables.is_empty());
    }

    #[test]
    fn position_update() {
        let mut pos = ReplicaPosition::new();
        pos.update("trades", 10);
        assert_eq!(pos.last_applied_txn, 10);
        assert_eq!(pos.tables.get("trades"), Some(&10));
    }

    #[test]
    fn position_no_regression() {
        let mut pos = ReplicaPosition::new();
        pos.update("t1", 50);
        pos.update("t1", 30);
        assert_eq!(pos.tables.get("t1"), Some(&50));
    }

    #[test]
    fn position_multiple_tables() {
        let mut pos = ReplicaPosition::new();
        pos.update("trades", 10);
        pos.update("orders", 5);
        assert_eq!(pos.last_applied_txn, 10);
        assert_eq!(pos.tables.get("orders"), Some(&5));
    }

    #[test]
    fn position_serialization_roundtrip() {
        let mut pos = ReplicaPosition::new();
        pos.update("trades", 42);
        let json = serde_json::to_string(&pos).unwrap();
        let restored: ReplicaPosition = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_applied_txn, 42);
    }

    #[test]
    fn apply_segment_creates_wal_dir() {
        let dir = tempdir().unwrap();
        let mut receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:9100".into());
        let data = b"XWAL-test-segment";
        let bytes = receiver.apply_segment("test_table", data).unwrap();
        assert_eq!(bytes, data.len() as u64);
        assert!(dir.path().join("test_table/wal").exists());
    }

    #[test]
    fn apply_segment_increments_id() {
        let dir = tempdir().unwrap();
        let mut receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:9100".into());
        receiver.apply_segment("tbl", b"seg1").unwrap();
        receiver.apply_segment("tbl", b"seg2").unwrap();
        let wal_dir = dir.path().join("tbl/wal");
        assert!(wal_dir.join("wal-000000.wal").exists());
        assert!(wal_dir.join("wal-000001.wal").exists());
    }

    #[test]
    fn current_position_initially_empty() {
        let dir = tempdir().unwrap();
        let receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:9100".into());
        let pos = receiver.current_position();
        assert_eq!(pos.last_applied_txn, 0);
    }

    #[test]
    fn apply_schema_sync_creates_meta() {
        let dir = tempdir().unwrap();
        let receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:9100".into());

        let meta_json = r#"{
            "name": "test_table",
            "columns": [
                {"name": "ts", "col_type": "Timestamp", "indexed": false},
                {"name": "val", "col_type": "F64", "indexed": false}
            ],
            "partition_by": "Day",
            "timestamp_column": 0,
            "version": 1
        }"#;

        receiver.apply_schema_sync("test_table", meta_json, 1).unwrap();

        let meta_path = dir.path().join("test_table").join("_meta");
        assert!(meta_path.exists(), "_meta should be created");

        // _txn should also exist.
        let txn_path = dir.path().join("test_table").join("_txn");
        assert!(txn_path.exists(), "_txn should be created");
    }

    #[test]
    fn apply_schema_sync_does_not_regress() {
        let dir = tempdir().unwrap();
        let receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:9100".into());

        let meta_v2 = r#"{
            "name": "tbl",
            "columns": [
                {"name": "ts", "col_type": "Timestamp", "indexed": false},
                {"name": "a", "col_type": "F64", "indexed": false}
            ],
            "partition_by": "Day",
            "timestamp_column": 0,
            "version": 2
        }"#;

        let meta_v1 = r#"{
            "name": "tbl",
            "columns": [
                {"name": "ts", "col_type": "Timestamp", "indexed": false}
            ],
            "partition_by": "Day",
            "timestamp_column": 0,
            "version": 1
        }"#;

        // Apply v2 first.
        receiver.apply_schema_sync("tbl", meta_v2, 2).unwrap();

        // Attempt v1 -- should be a no-op.
        receiver.apply_schema_sync("tbl", meta_v1, 1).unwrap();

        // Read back and verify still v2.
        let content = std::fs::read_to_string(dir.path().join("tbl").join("_meta")).unwrap();
        assert!(content.contains("\"version\": 2") || content.contains("\"version\":2"));
    }

    #[test]
    fn apply_schema_sync_updates_on_newer_version() {
        let dir = tempdir().unwrap();
        let receiver = WalReceiver::new(dir.path().to_path_buf(), "127.0.0.1:9100".into());

        let meta_v1 = r#"{
            "name": "tbl",
            "columns": [
                {"name": "ts", "col_type": "Timestamp", "indexed": false}
            ],
            "partition_by": "Day",
            "timestamp_column": 0,
            "version": 1
        }"#;

        let meta_v3 = r#"{
            "name": "tbl",
            "columns": [
                {"name": "ts", "col_type": "Timestamp", "indexed": false},
                {"name": "a", "col_type": "F64", "indexed": false},
                {"name": "b", "col_type": "I64", "indexed": false}
            ],
            "partition_by": "Day",
            "timestamp_column": 0,
            "version": 3
        }"#;

        receiver.apply_schema_sync("tbl", meta_v1, 1).unwrap();
        receiver.apply_schema_sync("tbl", meta_v3, 3).unwrap();

        let content = std::fs::read_to_string(dir.path().join("tbl").join("_meta")).unwrap();
        assert!(content.contains("\"version\": 3") || content.contains("\"version\":3"));
    }
}

// ---------------------------------------------------------------------------
// mod failover
// ---------------------------------------------------------------------------

mod failover_tests {
    use super::*;

    fn replica_config(primary: &str) -> ReplicationConfig {
        ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: Some(primary.into()),
            replica_addrs: vec![],
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        }
    }

    fn primary_config(replicas: Vec<&str>) -> ReplicationConfig {
        ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            replica_addrs: replicas.into_iter().map(String::from).collect(),
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        }
    }

    #[test]
    fn promote_replica() {
        let mut mgr = FailoverManager::new(replica_config("10.0.0.1:9100"), Duration::from_secs(5));
        assert_eq!(*mgr.current_role(), ReplicationRole::Replica);
        mgr.promote_to_primary().unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);
    }

    #[test]
    fn promote_primary_fails() {
        let mut mgr = FailoverManager::new(primary_config(vec![]), Duration::from_secs(5));
        assert!(mgr.promote_to_primary().is_err());
    }

    #[test]
    fn demote_primary() {
        let mut mgr = FailoverManager::new(
            primary_config(vec!["10.0.0.2:9100"]),
            Duration::from_secs(5),
        );
        mgr.demote_to_replica("10.0.0.2:9100").unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Replica);
    }

    #[test]
    fn demote_replica_fails() {
        let mut mgr = FailoverManager::new(replica_config("10.0.0.1:9100"), Duration::from_secs(5));
        assert!(mgr.demote_to_replica("x").is_err());
    }

    #[test]
    fn promote_then_demote_roundtrip() {
        let mut mgr = FailoverManager::new(replica_config("10.0.0.1:9100"), Duration::from_secs(5));
        mgr.promote_to_primary().unwrap();
        mgr.demote_to_replica("10.0.0.99:9100").unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Replica);
        assert_eq!(
            mgr.config().primary_addr.as_deref(),
            Some("10.0.0.99:9100")
        );
    }

    #[test]
    fn promote_standalone() {
        let config = ReplicationConfig {
            role: ReplicationRole::Standalone,
            primary_addr: None,
            replica_addrs: vec![],
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        let mut mgr = FailoverManager::new(config, Duration::from_secs(5));
        mgr.promote_to_primary().unwrap();
        assert_eq!(*mgr.current_role(), ReplicationRole::Primary);
    }

    #[tokio::test]
    async fn health_check_no_primary_addr() {
        let config = ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: None,
            replica_addrs: vec![],
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        let mgr = FailoverManager::new(config, Duration::from_secs(1));
        assert!(!mgr.check_primary_health().await);
    }

    #[tokio::test]
    async fn health_check_unreachable() {
        let mgr = FailoverManager::new(
            replica_config("127.0.0.1:19999"),
            Duration::from_millis(100),
        );
        assert!(!mgr.check_primary_health().await);
    }

    #[tokio::test]
    async fn health_check_reachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mgr = FailoverManager::new(
            replica_config(&addr.to_string()),
            Duration::from_secs(2),
        );
        assert!(mgr.check_primary_health().await);
    }
}

// ---------------------------------------------------------------------------
// mod auto_failover
// ---------------------------------------------------------------------------

mod auto_failover_tests {
    use super::*;

    #[test]
    fn record_success_resets() {
        let af = AutoFailover::new(Duration::from_secs(1), 3);
        af.record_failure();
        af.record_failure();
        af.record_success();
        assert_eq!(af.current_failures(), 0);
    }

    #[test]
    fn record_failure_increments() {
        let af = AutoFailover::new(Duration::from_secs(1), 5);
        for _ in 0..4 {
            assert!(!af.record_failure());
        }
        assert!(af.record_failure());
    }

    #[test]
    fn threshold_of_one_triggers_immediately() {
        let af = AutoFailover::new(Duration::from_secs(1), 1);
        assert!(af.record_failure());
    }

    #[test]
    fn failure_counter_tracks() {
        let af = AutoFailover::new(Duration::from_secs(1), 10);
        for _ in 0..5 {
            af.record_failure();
        }
        assert_eq!(af.current_failures(), 5);
        af.record_success();
        assert_eq!(af.current_failures(), 0);
    }
}

// ---------------------------------------------------------------------------
// mod s3_shipper
// ---------------------------------------------------------------------------

mod s3_shipper_tests {
    use super::*;
    use exchange_core::tiered::ObjectStore;

    #[test]
    fn ship_and_list() {
        let store = Box::new(MemoryObjectStore::new());
        let shipper = S3WalShipper::new(store, "repl");
        shipper.ship_segment_data("trades", "wal-000001.wal", b"seg1").unwrap();
        shipper.ship_segment_data("trades", "wal-000002.wal", b"seg2").unwrap();
        let segs = shipper.list_segments("trades").unwrap();
        assert_eq!(segs.len(), 2);
    }

    #[test]
    fn ship_different_tables() {
        let store = Box::new(MemoryObjectStore::new());
        let shipper = S3WalShipper::new(store, "repl");
        shipper.ship_segment_data("trades", "wal-000001.wal", b"t1").unwrap();
        shipper.ship_segment_data("quotes", "wal-000001.wal", b"q1").unwrap();
        assert_eq!(shipper.list_segments("trades").unwrap().len(), 1);
        assert_eq!(shipper.list_segments("quotes").unwrap().len(), 1);
    }

    #[test]
    fn ship_empty_prefix() {
        let store = Box::new(MemoryObjectStore::new());
        let shipper = S3WalShipper::new(store, "");
        shipper.ship_segment_data("tbl", "wal-000001.wal", b"data").unwrap();
        assert_eq!(shipper.list_segments("tbl").unwrap().len(), 1);
    }

    #[test]
    fn receiver_sync_downloads_new() {
        let store = MemoryObjectStore::new();
        store.put("repl/trades/wal-000001.wal", b"s1").unwrap();
        store.put("repl/trades/wal-000002.wal", b"s2").unwrap();
        store.put("repl/trades/wal-000003.wal", b"s3").unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let receiver = S3WalReceiver::new(
            Box::new(store),
            "repl",
            tmpdir.path().to_path_buf(),
        );
        let max = receiver.sync("trades", 1).unwrap();
        assert_eq!(max, 3);
        assert!(tmpdir.path().join("trades/wal-000002.wal").exists());
        assert!(tmpdir.path().join("trades/wal-000003.wal").exists());
    }

    #[test]
    fn receiver_sync_no_new() {
        let store = MemoryObjectStore::new();
        store.put("repl/trades/wal-000001.wal", b"s1").unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let receiver = S3WalReceiver::new(
            Box::new(store),
            "repl",
            tmpdir.path().to_path_buf(),
        );
        let max = receiver.sync("trades", 5).unwrap();
        assert_eq!(max, 5);
    }

    #[test]
    fn shipper_receiver_roundtrip() {
        let store = MemoryObjectStore::new();
        store.put("wal/orders/wal-000001.wal", b"data-one").unwrap();
        store.put("wal/orders/wal-000002.wal", b"data-two").unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let receiver = S3WalReceiver::new(
            Box::new(store),
            "wal",
            tmpdir.path().to_path_buf(),
        );
        let max = receiver.sync("orders", 0).unwrap();
        assert_eq!(max, 2);
        let c1 = std::fs::read(tmpdir.path().join("orders/wal-000001.wal")).unwrap();
        assert_eq!(c1, b"data-one");
    }
}

// ---------------------------------------------------------------------------
// mod config
// ---------------------------------------------------------------------------

mod config_tests {
    use super::*;

    #[test]
    fn default_is_standalone() {
        let config = ReplicationConfig::default();
        assert_eq!(config.role, ReplicationRole::Standalone);
        assert!(config.primary_addr.is_none());
        assert!(config.replica_addrs.is_empty());
    }

    #[test]
    fn serialization_roundtrip() {
        let config = ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            replica_addrs: vec!["10.0.0.2:9100".into()],
            sync_mode: ReplicationSyncMode::SemiSync,
            max_lag_bytes: 128 * 1024 * 1024,
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let restored: ReplicationConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.role, ReplicationRole::Primary);
        assert_eq!(restored.sync_mode, ReplicationSyncMode::SemiSync);
    }

    #[test]
    fn default_max_lag_is_256mb() {
        let config = ReplicationConfig::default();
        assert_eq!(config.max_lag_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn replica_config_has_primary_addr() {
        let config = ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: Some("10.0.0.1:9100".into()),
            replica_addrs: vec![],
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        assert_eq!(config.primary_addr.as_deref(), Some("10.0.0.1:9100"));
    }

    #[test]
    fn sync_mode_serialization() {
        for mode in [
            ReplicationSyncMode::Async,
            ReplicationSyncMode::SemiSync,
            ReplicationSyncMode::Sync,
        ] {
            let json = serde_json::to_string(&mode).unwrap();
            let restored: ReplicationSyncMode = serde_json::from_str(&json).unwrap();
            assert_eq!(restored, mode);
        }
    }

    #[test]
    fn role_serialization() {
        for role in [
            ReplicationRole::Primary,
            ReplicationRole::Replica,
            ReplicationRole::Standalone,
        ] {
            let json = serde_json::to_string(&role).unwrap();
            let restored: ReplicationRole = serde_json::from_str(&json).unwrap();
            assert_eq!(restored, role);
        }
    }
}

// ---------------------------------------------------------------------------
// mod manager_tests — ReplicationManager
// ---------------------------------------------------------------------------

mod manager_tests {
    use super::*;
    use exchange_core::replication::manager::ReplicationManager;
    use tempfile::tempdir;

    #[test]
    fn new_primary_is_not_read_only() {
        let dir = tempdir().unwrap();
        let config = ReplicationConfig {
            role: ReplicationRole::Primary,
            primary_addr: None,
            replica_addrs: vec![],
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        assert!(!mgr.is_read_only());
    }

    #[test]
    fn new_replica_is_read_only() {
        let dir = tempdir().unwrap();
        let config = ReplicationConfig {
            role: ReplicationRole::Replica,
            primary_addr: Some("127.0.0.1:9100".into()),
            replica_addrs: vec![],
            sync_mode: ReplicationSyncMode::Async,
            max_lag_bytes: 256 * 1024 * 1024,
            ..Default::default()
        };
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        assert!(mgr.is_read_only());
    }

    #[test]
    fn new_standalone_is_not_read_only() {
        let dir = tempdir().unwrap();
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), ReplicationConfig::default());
        assert!(!mgr.is_read_only());
    }

    #[test]
    fn status_before_start() {
        let dir = tempdir().unwrap();
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), ReplicationConfig::default());
        let status = mgr.status();
        assert_eq!(status.role, ReplicationRole::Standalone);
        assert!(!status.is_healthy); // Not started yet
    }
}
