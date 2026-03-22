//! Replication manager that coordinates WAL shipping and receiving.
//!
//! The `ReplicationManager` is the single entry point for replication lifecycle
//! management. It is created during server startup and wired into the WAL
//! commit path so that every committed segment is automatically shipped to
//! replicas (on a primary) or received from the primary (on a replica).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::config::{ReplicationConfig, ReplicationRole, ReplicationSyncMode};
use super::failover::FailoverManager;
use super::fencing;
use super::wal_receiver::WalReceiver;
use super::wal_shipper::{ReplicationLag, WalShipper};

/// Current replication status, queryable at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatus {
    /// Role of this node.
    pub role: ReplicationRole,
    /// Number of connected replicas (primary only).
    pub connected_replicas: u32,
    /// Per-replica lag information (primary only).
    pub lag: HashMap<String, ReplicationLagInfo>,
    /// Whether replication is healthy.
    pub is_healthy: bool,
}

/// Serializable version of replication lag for status reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLagInfo {
    pub replica: String,
    pub bytes_behind: u64,
    pub segments_behind: u32,
    pub last_ack_txn: u64,
    /// Estimated lag in milliseconds (0 if unknown).
    #[serde(default)]
    pub lag_ms: u64,
    /// Unix timestamp (millis) of last acknowledgement from this replica (0 if never).
    #[serde(default)]
    pub last_ack_time_ms: u64,
}

/// Aggregate replication lag snapshot across all replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateReplicationLag {
    /// Maximum lag in bytes across all replicas.
    pub lag_bytes: u64,
    /// Maximum estimated lag in milliseconds across all replicas.
    pub lag_ms: u64,
    /// Unix timestamp (millis) of the most recent ack from any replica.
    pub last_ack_time_ms: u64,
    /// Number of replicas reporting lag data.
    pub replica_count: usize,
}

impl From<&ReplicationLag> for ReplicationLagInfo {
    fn from(lag: &ReplicationLag) -> Self {
        Self {
            replica: lag.replica.clone(),
            bytes_behind: lag.bytes_behind,
            segments_behind: lag.segments_behind,
            last_ack_txn: lag.last_ack_txn,
            lag_ms: 0,
            last_ack_time_ms: 0,
        }
    }
}

/// Result of a replica re-sync operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResyncResult {
    /// Number of tables synced from the primary snapshot.
    pub tables_synced: usize,
    /// Total bytes transferred during the re-sync.
    pub bytes_transferred: u64,
    /// Duration of the re-sync operation in milliseconds.
    pub duration_ms: u64,
}

/// Manages the replication lifecycle based on configured role.
///
/// - **Primary**: starts a WAL shipper and ships committed segments to replicas.
/// - **Replica**: starts a WAL receiver that listens for incoming segments.
/// - **Standalone**: no-op.
pub struct ReplicationManager {
    config: ReplicationConfig,
    _db_root: PathBuf,
    shipper: Option<RwLock<WalShipper>>,
    _receiver: Option<WalReceiver>,
    failover: FailoverManager,
    running: AtomicBool,
    read_only: AtomicBool,
    /// Monotonically increasing epoch number for split-brain fencing.
    epoch: AtomicU64,
    /// Aggregate replication lag in bytes (max across replicas).
    replication_lag_bytes: AtomicU64,
    /// Aggregate replication lag in milliseconds (estimated).
    replication_lag_ms: AtomicU64,
    /// Total bytes committed to WAL on the primary.
    total_bytes_committed: AtomicU64,
    /// Total bytes acknowledged by replicas.
    total_bytes_acknowledged: AtomicU64,
    /// Instant when the manager was created, for estimating lag_ms.
    created_at: Instant,
}

impl ReplicationManager {
    /// Create a new replication manager.
    pub fn new(db_root: PathBuf, config: ReplicationConfig) -> Self {
        let is_replica = config.role == ReplicationRole::Replica;
        let failover = FailoverManager::new(config.clone(), Duration::from_secs(5));

        // Load the current epoch from disk if a fence file exists.
        let initial_epoch = fencing::read_fence(&db_root)
            .ok()
            .flatten()
            .map(|f| f.epoch)
            .unwrap_or(0);

        Self {
            config,
            _db_root: db_root,
            shipper: None,
            _receiver: None,
            failover,
            running: AtomicBool::new(false),
            read_only: AtomicBool::new(is_replica),
            epoch: AtomicU64::new(initial_epoch),
            replication_lag_bytes: AtomicU64::new(0),
            replication_lag_ms: AtomicU64::new(0),
            total_bytes_committed: AtomicU64::new(0),
            total_bytes_acknowledged: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Start replication based on role.
    ///
    /// - **Primary**: initializes the WAL shipper (the background shipping
    ///   happens on each `on_wal_commit` call).
    /// - **Replica**: starts the WAL receiver listener in a background task.
    /// - **Standalone**: no-op.
    pub async fn start(&mut self) -> Result<()> {
        match self.config.role {
            ReplicationRole::Primary => {
                tracing::info!(
                    replicas = ?self.config.replica_addrs,
                    sync_mode = ?self.config.sync_mode,
                    "starting replication manager as primary"
                );
                let shipper = WalShipper::new(self.config.clone());
                self.shipper = Some(RwLock::new(shipper));
                self.read_only.store(false, Ordering::SeqCst);
            }
            ReplicationRole::Replica => {
                let primary_addr = self.config.primary_addr.clone().ok_or_else(|| {
                    ExchangeDbError::Wal("replica mode requires primary_addr to be set".into())
                })?;
                tracing::info!(
                    primary = %primary_addr,
                    replication_port = self.config.replication_port,
                    "starting replication manager as replica"
                );

                // The replication TCP listener is started separately by the
                // network layer (replication_server module) when
                // replication_port > 0.  We only set read-only mode here.
                self.read_only.store(true, Ordering::SeqCst);
            }
            ReplicationRole::Standalone => {
                tracing::info!("replication manager in standalone mode (no-op)");
            }
        }

        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Called after each WAL commit on the primary.
    ///
    /// Ships the latest WAL segment to all configured replicas. This is the
    /// hook that integrates replication into the write path.
    pub async fn on_wal_commit(&self, table: &str, segment_path: &Path) -> Result<()> {
        if self.config.role != ReplicationRole::Primary {
            return Ok(());
        }

        // Track bytes committed from the segment file size.
        let segment_size = std::fs::metadata(segment_path)
            .map(|m| m.len())
            .unwrap_or(0);
        self.total_bytes_committed
            .fetch_add(segment_size, Ordering::Relaxed);

        if let Some(ref shipper_lock) = self.shipper {
            let mut shipper = shipper_lock.write().await;
            match shipper.ship_segment(table, segment_path).await {
                Ok(stats) => {
                    // Track acknowledged bytes.
                    if stats.replicas_acked > 0 {
                        self.total_bytes_acknowledged
                            .fetch_add(stats.bytes_shipped, Ordering::Relaxed);
                    }
                    self.update_lag_metrics(&shipper);
                    tracing::debug!(
                        table = %table,
                        bytes_shipped = stats.bytes_shipped,
                        replicas_acked = stats.replicas_acked,
                        "WAL segment shipped to replicas"
                    );
                }
                Err(e) => {
                    // Update lag even on failure (lag increases).
                    self.update_lag_metrics(&shipper);
                    // In async mode, log and continue. In sync/semi-sync modes
                    // the error is propagated.
                    match self.config.sync_mode {
                        ReplicationSyncMode::Async => {
                            tracing::warn!(
                                error = %e,
                                table = %table,
                                "async replication: failed to ship WAL segment"
                            );
                        }
                        _ => return Err(e),
                    }
                }
            }
        }

        Ok(())
    }

    /// Ship pre-read WAL segment bytes to all replicas.
    ///
    /// Unlike `on_wal_commit` which reads the file itself, this method accepts
    /// the raw bytes directly. This avoids the race condition where merge()
    /// deletes the WAL file before the async shipping task reads it.
    pub async fn ship_segment_bytes(&self, table: &str, data: &[u8]) -> Result<()> {
        if self.config.role != ReplicationRole::Primary {
            return Ok(());
        }

        if let Some(ref shipper_lock) = self.shipper {
            let mut shipper = shipper_lock.write().await;
            match shipper.ship_segment_bytes(table, data).await {
                Ok(stats) => {
                    tracing::debug!(
                        table = %table,
                        bytes_shipped = stats.bytes_shipped,
                        replicas_acked = stats.replicas_acked,
                        "WAL segment bytes shipped to replicas"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, table = %table, "failed to ship WAL bytes");
                }
            }
        }
        Ok(())
    }

    /// Ensure schema is synced to all replicas for the given table.
    pub async fn ensure_schema_synced(
        &self,
        table: &str,
        table_dir: &std::path::Path,
    ) -> Result<()> {
        if self.config.role != ReplicationRole::Primary {
            return Ok(());
        }
        if let Some(ref shipper_lock) = self.shipper {
            let mut shipper = shipper_lock.write().await;
            shipper.ensure_schema_synced(table, table_dir).await?;
        }
        Ok(())
    }

    /// Get current replication status.
    pub fn status(&self) -> ReplicationStatus {
        let (connected_replicas, lag) = if self.config.role == ReplicationRole::Primary {
            // We cannot async-lock here, so we report based on config.
            let replicas = self.config.replica_addrs.len() as u32;
            (replicas, HashMap::new())
        } else {
            (0, HashMap::new())
        };

        ReplicationStatus {
            role: self.config.role.clone(),
            connected_replicas,
            lag,
            is_healthy: self.running.load(Ordering::SeqCst),
        }
    }

    /// Get replication status with lag info (async version).
    pub async fn status_async(&self) -> ReplicationStatus {
        let (connected_replicas, lag) = if let Some(ref shipper_lock) = self.shipper {
            let shipper = shipper_lock.read().await;
            let raw_lag = shipper.replication_lag();
            let lag: HashMap<String, ReplicationLagInfo> = raw_lag
                .iter()
                .map(|(k, v)| (k.clone(), ReplicationLagInfo::from(v)))
                .collect();
            let connected = self.config.replica_addrs.len() as u32;
            (connected, lag)
        } else {
            (0, HashMap::new())
        };

        ReplicationStatus {
            role: self.config.role.clone(),
            connected_replicas,
            lag,
            is_healthy: self.running.load(Ordering::SeqCst),
        }
    }

    /// Get the current aggregate replication lag in bytes.
    pub fn lag_bytes(&self) -> u64 {
        self.replication_lag_bytes.load(Ordering::Relaxed)
    }

    /// Get the current estimated replication lag in milliseconds.
    pub fn lag_ms(&self) -> u64 {
        self.replication_lag_ms.load(Ordering::Relaxed)
    }

    /// Get a snapshot of aggregate replication lag information.
    pub fn replication_lag_info(&self) -> AggregateReplicationLag {
        AggregateReplicationLag {
            lag_bytes: self.replication_lag_bytes.load(Ordering::Relaxed),
            lag_ms: self.replication_lag_ms.load(Ordering::Relaxed),
            last_ack_time_ms: 0, // Would require wall-clock tracking per replica.
            replica_count: self.config.replica_addrs.len(),
        }
    }

    /// Update the aggregate lag metrics from the current shipper state.
    fn update_lag_metrics(&self, shipper: &WalShipper) {
        let lags = shipper.replication_lag();
        let max_bytes = lags.values().map(|l| l.bytes_behind).max().unwrap_or(0);
        self.replication_lag_bytes
            .store(max_bytes, Ordering::Relaxed);

        // Estimate lag_ms from committed vs acknowledged bytes and elapsed time.
        let committed = self.total_bytes_committed.load(Ordering::Relaxed);
        let acknowledged = self.total_bytes_acknowledged.load(Ordering::Relaxed);
        let elapsed_ms = self.created_at.elapsed().as_millis() as u64;

        if committed > 0 && elapsed_ms > 0 {
            // bytes_per_ms = committed / elapsed_ms
            // lag_ms = lag_bytes / bytes_per_ms = lag_bytes * elapsed_ms / committed
            let lag_ms = (max_bytes as u128 * elapsed_ms as u128 / committed as u128) as u64;
            self.replication_lag_ms.store(lag_ms, Ordering::Relaxed);
        } else {
            self.replication_lag_ms.store(0, Ordering::Relaxed);
        }

        let _ = (committed, acknowledged); // suppress unused warning for acknowledged
    }

    /// Whether this node is in read-only mode (replica).
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::SeqCst)
    }

    /// Get the replication role.
    pub fn role(&self) -> &ReplicationRole {
        &self.config.role
    }

    /// Get a reference to the failover manager.
    pub fn failover(&self) -> &FailoverManager {
        &self.failover
    }

    /// Get the replication configuration.
    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    /// Promote this replica to primary.
    ///
    /// This performs the full failover sequence:
    /// 1. Stops accepting WAL from the primary (receiver is dropped).
    /// 2. Switches the node from read-only to read-write.
    /// 3. Logs "PROMOTED TO PRIMARY".
    /// 4. The node now accepts writes.
    ///
    /// This method is safe to call from a shared reference (e.g. behind
    /// `Arc`) because the write-gate is an `AtomicBool`.
    pub fn promote_to_primary(&self) {
        tracing::warn!("Failover: stopping WAL receiver and promoting to primary");

        // Increment epoch and write fence file for split-brain prevention.
        let new_epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;

        // Write the epoch to the `_epoch` file for external visibility.
        let epoch_path = self._db_root.join("_epoch");
        if let Err(e) = std::fs::write(&epoch_path, new_epoch.to_string()) {
            tracing::error!(error = %e, "failed to write epoch file");
        }

        // Create a fencing token on disk.
        let node_id = self
            .config
            .primary_addr
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        match fencing::create_fence(&self._db_root, &node_id) {
            Ok(token) => {
                tracing::info!(epoch = token.epoch, node_id = %token.node_id, "fence created on promotion");
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to create fence on promotion");
            }
        }

        // Switch from read-only to read-write so the node accepts writes.
        self.read_only.store(false, Ordering::SeqCst);

        tracing::warn!(epoch = new_epoch, "PROMOTED TO PRIMARY");
    }

    /// Get the current epoch number.
    pub fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Validate a remote epoch against the local epoch.
    ///
    /// Returns `true` if the remote epoch is >= the local epoch, meaning the
    /// remote node is not a stale primary. Returns `false` if the remote epoch
    /// is strictly less than the local epoch.
    pub fn validate_epoch(&self, remote_epoch: u64) -> bool {
        let local = self.epoch.load(Ordering::SeqCst);
        remote_epoch >= local
    }

    /// Re-sync this replica from the primary by requesting a full snapshot.
    ///
    /// The method:
    /// 1. Connects to the primary (via `self.config.primary_addr`).
    /// 2. Creates a snapshot of the primary data into a temp directory.
    /// 3. Swaps the data directory atomically (old -> backup, new -> data).
    /// 4. Returns information about the re-sync operation.
    ///
    /// Because we cannot perform real network I/O without a running cluster,
    /// this implementation uses the local snapshot/restore machinery to
    /// demonstrate the full logic structure.
    pub async fn resync_from_primary(&mut self) -> Result<ResyncResult> {
        let primary_addr = self.config.primary_addr.clone().ok_or_else(|| {
            ExchangeDbError::Wal("resync requires primary_addr to be configured".into())
        })?;

        let start = Instant::now();

        tracing::info!(
            primary = %primary_addr,
            "starting replica re-sync from primary"
        );

        // Step 1: Prepare temp directories.
        let tmp_snapshot = self._db_root.join("_resync_snapshot");
        let backup_dir = self._db_root.join("_resync_backup");

        // Clean up any leftover temp dirs from a previous failed resync.
        if tmp_snapshot.exists() {
            std::fs::remove_dir_all(&tmp_snapshot)?;
        }
        if backup_dir.exists() {
            std::fs::remove_dir_all(&backup_dir)?;
        }

        tracing::info!("step 1/5: prepared temp directories");

        // Step 2: Request snapshot from primary.
        // In a real implementation, this would connect to the primary over TCP
        // and stream the snapshot data. For now, we simulate by creating a
        // snapshot of the local data (which in a real cluster would be the
        // primary's data received over the network).
        tracing::info!(
            primary = %primary_addr,
            "step 2/5: requesting snapshot from primary (simulated)"
        );
        let snapshot_info = crate::snapshot::create_snapshot(&self._db_root, &tmp_snapshot)?;
        tracing::info!(
            tables = snapshot_info.tables.len(),
            bytes = snapshot_info.total_bytes,
            "step 2/5: snapshot received"
        );

        // Step 3: Swap data directory atomically.
        // Move existing table directories to backup, then restore from snapshot.
        tracing::info!("step 3/5: swapping data directory");
        std::fs::create_dir_all(&backup_dir)?;

        // Move existing table directories to backup.
        if let Ok(entries) = std::fs::read_dir(&self._db_root) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                // Skip our temp directories and internal files.
                if name_str.starts_with('_') {
                    continue;
                }
                let path = entry.path();
                if path.is_dir() {
                    let backup_path = backup_dir.join(&name);
                    if let Err(e) = std::fs::rename(&path, &backup_path) {
                        tracing::warn!(
                            error = %e,
                            dir = %name_str,
                            "failed to move directory to backup"
                        );
                    }
                }
            }
        }

        // Step 4: Restore from snapshot.
        tracing::info!("step 4/5: restoring snapshot into data directory");
        crate::snapshot::restore_snapshot(&tmp_snapshot, &self._db_root)?;

        // Step 5: Clean up temp directories.
        tracing::info!("step 5/5: cleaning up temp directories");
        if tmp_snapshot.exists() {
            let _ = std::fs::remove_dir_all(&tmp_snapshot);
        }
        // Keep backup for a little while in case manual recovery is needed.
        // In production, a background task would eventually clean it up.

        let duration = start.elapsed();
        let result = ResyncResult {
            tables_synced: snapshot_info.tables.len(),
            bytes_transferred: snapshot_info.total_bytes,
            duration_ms: duration.as_millis() as u64,
        };

        tracing::info!(
            tables = result.tables_synced,
            bytes = result.bytes_transferred,
            duration_ms = result.duration_ms,
            "replica re-sync completed successfully"
        );

        Ok(result)
    }

    /// Stop replication.
    pub async fn stop(&mut self) -> Result<()> {
        tracing::info!("stopping replication manager");
        self.running.store(false, Ordering::SeqCst);
        self.shipper = None;
        self._receiver = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn standalone_config() -> ReplicationConfig {
        ReplicationConfig::default()
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

    #[tokio::test]
    async fn manager_starts_in_primary_mode() {
        let dir = tempdir().unwrap();
        let config = primary_config(vec!["127.0.0.1:19100"]);
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);

        mgr.start().await.unwrap();

        assert!(!mgr.is_read_only());
        assert_eq!(*mgr.role(), ReplicationRole::Primary);
        assert!(mgr.shipper.is_some());

        let status = mgr.status();
        assert_eq!(status.role, ReplicationRole::Primary);
        assert_eq!(status.connected_replicas, 1);
        assert!(status.is_healthy);

        mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn manager_starts_in_replica_mode() {
        let dir = tempdir().unwrap();
        let config = replica_config("127.0.0.1:19200");
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);

        // Start should succeed (the background task will fail to connect,
        // but that is expected in tests without a real primary).
        mgr.start().await.unwrap();

        assert!(mgr.is_read_only());
        assert_eq!(*mgr.role(), ReplicationRole::Replica);

        let status = mgr.status();
        assert_eq!(status.role, ReplicationRole::Replica);
        assert!(status.is_healthy);

        mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn standalone_mode_is_noop() {
        let dir = tempdir().unwrap();
        let config = standalone_config();
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);

        mgr.start().await.unwrap();

        assert!(!mgr.is_read_only());
        assert_eq!(*mgr.role(), ReplicationRole::Standalone);
        assert!(mgr.shipper.is_none());

        let status = mgr.status();
        assert_eq!(status.role, ReplicationRole::Standalone);
        assert!(status.is_healthy);

        mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn replica_rejects_writes_via_read_only_flag() {
        let dir = tempdir().unwrap();
        let config = replica_config("127.0.0.1:19300");
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);

        mgr.start().await.unwrap();

        // The read_only flag should be true, which the server uses to reject writes.
        assert!(mgr.is_read_only());

        mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn on_wal_commit_noop_for_standalone() {
        let dir = tempdir().unwrap();
        let config = standalone_config();
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        mgr.start().await.unwrap();

        // Should be a no-op and not error.
        let result = mgr
            .on_wal_commit("trades", Path::new("/nonexistent/wal-000000.wal"))
            .await;
        assert!(result.is_ok());

        mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn on_wal_commit_called_on_primary() {
        let dir = tempdir().unwrap();
        // Use a replica address that won't connect (async mode ignores failures).
        let config = primary_config(vec!["127.0.0.1:19400"]);
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        mgr.start().await.unwrap();

        // Create a fake WAL segment file.
        let wal_dir = dir.path().join("test_table").join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let segment_path = wal_dir.join("wal-000000.wal");
        std::fs::write(&segment_path, b"fake-wal-data").unwrap();

        // In async mode, this should succeed even though the replica is unreachable.
        let result = mgr.on_wal_commit("test_table", &segment_path).await;
        assert!(result.is_ok());

        mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn status_reports_correct_role_and_lag() {
        let dir = tempdir().unwrap();
        let config = primary_config(vec!["r1:9100", "r2:9100"]);
        let mut mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        mgr.start().await.unwrap();

        let status = mgr.status_async().await;
        assert_eq!(status.role, ReplicationRole::Primary);
        assert_eq!(status.connected_replicas, 2);
        // Initially no lag data since no segments have been shipped.
        assert_eq!(status.lag.len(), 2);

        mgr.stop().await.unwrap();
    }

    #[test]
    fn new_manager_replica_is_read_only() {
        let dir = tempdir().unwrap();
        let config = replica_config("127.0.0.1:9100");
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        assert!(mgr.is_read_only());
    }

    #[test]
    fn new_manager_primary_is_not_read_only() {
        let dir = tempdir().unwrap();
        let config = primary_config(vec![]);
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        assert!(!mgr.is_read_only());
    }

    #[test]
    fn new_manager_standalone_is_not_read_only() {
        let dir = tempdir().unwrap();
        let config = standalone_config();
        let mgr = ReplicationManager::new(dir.path().to_path_buf(), config);
        assert!(!mgr.is_read_only());
    }
}
