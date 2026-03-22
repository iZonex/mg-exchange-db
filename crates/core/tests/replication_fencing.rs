//! Integration tests for fencing tokens and split-brain prevention.

use exchange_core::replication::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use exchange_core::replication::fencing::{create_fence, read_fence, validate_fence};
use exchange_core::replication::manager::ReplicationManager;
use tempfile::tempdir;

#[test]
fn test_fencing_token_create_and_read() {
    let dir = tempdir().unwrap();
    let token = create_fence(dir.path(), "node-alpha").unwrap();

    assert_eq!(token.epoch, 1);
    assert_eq!(token.node_id, "node-alpha");
    assert!(token.timestamp > 0);

    let read_back = read_fence(dir.path()).unwrap();
    assert!(read_back.is_some());
    let read_back = read_back.unwrap();
    assert_eq!(read_back.epoch, token.epoch);
    assert_eq!(read_back.node_id, token.node_id);
    assert_eq!(read_back.timestamp, token.timestamp);
}

#[test]
fn test_fencing_epoch_increment() {
    let dir = tempdir().unwrap();
    let t1 = create_fence(dir.path(), "node-1").unwrap();
    let t2 = create_fence(dir.path(), "node-1").unwrap();
    let t3 = create_fence(dir.path(), "node-2").unwrap();
    let t4 = create_fence(dir.path(), "node-2").unwrap();

    assert_eq!(t1.epoch, 1);
    assert_eq!(t2.epoch, 2);
    assert_eq!(t3.epoch, 3);
    assert_eq!(t4.epoch, 4);

    // Each subsequent fence has a strictly greater epoch.
    assert!(t2.epoch > t1.epoch);
    assert!(t3.epoch > t2.epoch);
    assert!(t4.epoch > t3.epoch);
}

#[test]
fn test_validate_fence_current() {
    let dir = tempdir().unwrap();
    let token = create_fence(dir.path(), "primary-node").unwrap();
    assert!(validate_fence(dir.path(), &token));

    // Creating a second fence for the same node still validates the latest.
    let token2 = create_fence(dir.path(), "primary-node").unwrap();
    assert!(validate_fence(dir.path(), &token2));
}

#[test]
fn test_validate_fence_stale() {
    let dir = tempdir().unwrap();
    let old_token = create_fence(dir.path(), "old-primary").unwrap();
    let _new_token = create_fence(dir.path(), "new-primary").unwrap();

    // The old token is stale and should not validate.
    assert!(!validate_fence(dir.path(), &old_token));
}

fn replica_config(primary: &str) -> ReplicationConfig {
    ReplicationConfig {
        role: ReplicationRole::Replica,
        primary_addr: Some(primary.to_string()),
        replica_addrs: Vec::new(),
        sync_mode: ReplicationSyncMode::Async,
        max_lag_bytes: 256 * 1024 * 1024,
        ..Default::default()
    }
}

#[test]
fn test_promote_increments_epoch() {
    let dir = tempdir().unwrap();
    let config = replica_config("127.0.0.1:9100");
    let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);

    // Initial epoch should be 0 (no fence file exists yet).
    assert_eq!(mgr.current_epoch(), 0);

    // Promote should increment the epoch.
    mgr.promote_to_primary();
    assert_eq!(mgr.current_epoch(), 1);

    // Promote again should increment further.
    mgr.promote_to_primary();
    assert_eq!(mgr.current_epoch(), 2);

    // Validate epoch: a remote with epoch >= local should pass.
    assert!(mgr.validate_epoch(2));
    assert!(mgr.validate_epoch(3));
    // A remote with a lower epoch should fail.
    assert!(!mgr.validate_epoch(1));
    assert!(!mgr.validate_epoch(0));
}
