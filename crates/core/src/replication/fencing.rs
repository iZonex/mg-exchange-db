//! Fencing tokens for split-brain prevention.
//!
//! A fencing token is a monotonically increasing epoch number combined with a
//! node identifier. When a node is promoted to primary it creates a new fence
//! which is persisted to disk. All subsequent WAL segments carry the epoch so
//! that replicas can reject stale data from a deposed primary.

use std::path::Path;

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};

const FENCE_FILENAME: &str = "_fence";

/// A fencing token written to disk on promotion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FencingToken {
    /// Monotonically increasing epoch number.
    pub epoch: u64,
    /// Identifier of the node that created this fence.
    pub node_id: String,
    /// Unix timestamp (milliseconds) when the fence was created.
    pub timestamp: i64,
}

/// Create a new fence file atomically in `db_root`.
///
/// The epoch is determined by reading the current fence (if any) and
/// incrementing it. If no fence exists the epoch starts at 1.
///
/// Returns the newly created [`FencingToken`].
pub fn create_fence(db_root: &Path, node_id: &str) -> Result<FencingToken> {
    let current = read_fence(db_root)?;
    let next_epoch = current.map(|f| f.epoch + 1).unwrap_or(1);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let token = FencingToken {
        epoch: next_epoch,
        node_id: node_id.to_string(),
        timestamp: now_ms,
    };

    // Write atomically: write to a temp file then rename.
    std::fs::create_dir_all(db_root)?;
    let fence_path = db_root.join(FENCE_FILENAME);
    let tmp_path = db_root.join("_fence.tmp");

    let json = serde_json::to_string_pretty(&token)
        .map_err(|e| ExchangeDbError::Corruption(format!("failed to serialize fence: {e}")))?;

    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &fence_path)?;

    tracing::info!(epoch = next_epoch, node_id = %node_id, "created new fence");

    Ok(token)
}

/// Read the current fence from `db_root`, if one exists.
pub fn read_fence(db_root: &Path) -> Result<Option<FencingToken>> {
    let fence_path = db_root.join(FENCE_FILENAME);
    if !fence_path.exists() {
        return Ok(None);
    }

    let data = std::fs::read_to_string(&fence_path)?;
    let token: FencingToken = serde_json::from_str(&data)
        .map_err(|e| ExchangeDbError::Corruption(format!("invalid fence file: {e}")))?;

    Ok(Some(token))
}

/// Validate that `token` matches the current fence on disk.
///
/// Returns `true` if the token's epoch and node_id match the persisted fence,
/// meaning it is the current (non-stale) primary.
pub fn validate_fence(db_root: &Path, token: &FencingToken) -> bool {
    match read_fence(db_root) {
        Ok(Some(current)) => current.epoch == token.epoch && current.node_id == token.node_id,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let token = create_fence(dir.path(), "node-1").unwrap();
        assert_eq!(token.epoch, 1);
        assert_eq!(token.node_id, "node-1");

        let read_back = read_fence(dir.path()).unwrap().unwrap();
        assert_eq!(token, read_back);
    }

    #[test]
    fn epoch_increments() {
        let dir = tempdir().unwrap();
        let t1 = create_fence(dir.path(), "node-1").unwrap();
        let t2 = create_fence(dir.path(), "node-1").unwrap();
        let t3 = create_fence(dir.path(), "node-2").unwrap();
        assert_eq!(t1.epoch, 1);
        assert_eq!(t2.epoch, 2);
        assert_eq!(t3.epoch, 3);
    }

    #[test]
    fn validate_current_fence() {
        let dir = tempdir().unwrap();
        let token = create_fence(dir.path(), "node-1").unwrap();
        assert!(validate_fence(dir.path(), &token));
    }

    #[test]
    fn validate_stale_fence() {
        let dir = tempdir().unwrap();
        let old = create_fence(dir.path(), "node-1").unwrap();
        let _new = create_fence(dir.path(), "node-2").unwrap();
        assert!(!validate_fence(dir.path(), &old));
    }
}
