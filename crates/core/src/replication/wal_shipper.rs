use std::collections::HashMap;
use std::path::Path;

use exchange_common::error::{ExchangeDbError, Result};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use super::config::{ReplicationConfig, ReplicationSyncMode};
use super::protocol::{self, ReplicationMessage};
use crate::wal::segment::{WalSegment, SEGMENT_HEADER_SIZE};

/// Statistics returned after shipping a WAL segment to replicas.
#[derive(Debug, Clone)]
pub struct ShipStats {
    /// Total bytes shipped across all replicas.
    pub bytes_shipped: u64,
    /// Number of replicas that acknowledged the segment.
    pub replicas_acked: u32,
}

/// Per-replica replication lag information.
#[derive(Debug, Clone)]
pub struct ReplicationLag {
    /// Replica address.
    pub replica: String,
    /// How many bytes the replica is behind the primary.
    pub bytes_behind: u64,
    /// How many segments the replica is behind.
    pub segments_behind: u32,
    /// Last transaction ID acknowledged by this replica.
    pub last_ack_txn: u64,
}

/// Ships WAL segments from the primary to all configured replicas.
///
/// Tracks per-replica replication positions and computes lag metrics.
/// Also tracks which schema versions have been synced to each replica
/// so that `SchemaSync` messages are sent exactly once per version bump.
pub struct WalShipper {
    config: ReplicationConfig,
    /// Per-replica last acknowledged transaction ID, keyed by replica address.
    replica_positions: HashMap<String, u64>,
    /// Total bytes shipped so far (used for lag calculation).
    total_bytes_shipped: u64,
    /// Per-replica bytes shipped.
    replica_bytes_shipped: HashMap<String, u64>,
    /// Per-table, per-replica last synced schema version.
    /// `synced_schemas[table][replica_addr] = version`
    synced_schemas: HashMap<String, HashMap<String, u64>>,
}

impl WalShipper {
    /// Create a new WAL shipper with the given configuration.
    pub fn new(config: ReplicationConfig) -> Self {
        let mut replica_positions = HashMap::new();
        let mut replica_bytes_shipped = HashMap::new();
        for addr in &config.replica_addrs {
            replica_positions.insert(addr.clone(), 0);
            replica_bytes_shipped.insert(addr.clone(), 0);
        }

        Self {
            config,
            replica_positions,
            total_bytes_shipped: 0,
            replica_bytes_shipped,
            synced_schemas: HashMap::new(),
        }
    }

    /// Ensure the table schema has been sent to all replicas.
    ///
    /// Reads the table's `_meta` file, checks its `version` against the last
    /// version synced to each replica, and sends a `SchemaSync` message for
    /// every replica that is behind.  This is called automatically by
    /// `ship_segment` before sending WAL data so replicas always have the
    /// schema they need to merge.
    pub async fn ensure_schema_synced(
        &mut self,
        table: &str,
        table_dir: &Path,
    ) -> Result<()> {
        use crate::table::TableMeta;

        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            return Ok(());
        }

        let meta = TableMeta::load(&meta_path)?;
        let meta_json = std::fs::read_to_string(&meta_path).map_err(|e| {
            ExchangeDbError::Wal(format!(
                "failed to read _meta for table {table}: {e}"
            ))
        })?;

        let addrs: Vec<String> = self.config.replica_addrs.clone();

        // Collect replicas that need a schema update (to avoid holding a
        // mutable borrow on synced_schemas across the async send).
        let mut need_sync: Vec<String> = Vec::new();
        for addr in &addrs {
            let cur = self
                .synced_schemas
                .entry(table.to_string())
                .or_default()
                .entry(addr.clone())
                .or_insert(0);
            if meta.version > *cur {
                need_sync.push(addr.clone());
            }
        }

        if need_sync.is_empty() {
            return Ok(());
        }

        let msg = ReplicationMessage::SchemaSync {
            table: table.to_string(),
            meta_json,
            version: meta.version,
        };
        let encoded = protocol::encode(&msg)?;

        for addr in need_sync {
            match self.send_to_replica(&addr, &encoded).await {
                Ok(()) => {
                    tracing::info!(
                        table = %table,
                        version = meta.version,
                        replica = %addr,
                        "schema synced to replica"
                    );
                    // Update the synced version now that the send succeeded.
                    *self
                        .synced_schemas
                        .entry(table.to_string())
                        .or_default()
                        .entry(addr)
                        .or_insert(0) = meta.version;
                }
                Err(e) => {
                    tracing::warn!(
                        table = %table,
                        replica = %addr,
                        error = %e,
                        "failed to sync schema to replica"
                    );
                }
            }
        }

        Ok(())
    }

    /// Ship a WAL segment to all replicas.
    ///
    /// Before sending the WAL data, ensures the table schema has been
    /// replicated via `SchemaSync`.  Reads the segment file at
    /// `segment_path`, wraps it in a `WalSegment` replication message,
    /// and sends it over TCP to each replica.  Depending on the configured
    /// sync mode, the method may wait for acknowledgments.
    pub async fn ship_segment(
        &mut self,
        table: &str,
        segment_path: &Path,
    ) -> Result<ShipStats> {
        let data = std::fs::read(segment_path).map_err(|e| {
            ExchangeDbError::Wal(format!(
                "failed to read WAL segment {}: {e}",
                segment_path.display()
            ))
        })?;

        let segment_id = parse_segment_id_from_path(segment_path)?;
        let data_len = data.len() as u64;

        // Scan the segment to extract the actual txn_id range.
        let txn_range = extract_txn_range_from_segment(segment_path).unwrap_or((0, 0));

        // Ensure the replica has the table schema before we ship WAL data.
        if let Some(table_dir) = segment_path.parent().and_then(|p| p.parent()) {
            self.ensure_schema_synced(table, table_dir).await?;
        }

        let msg = ReplicationMessage::WalSegment {
            table: table.to_string(),
            segment_id,
            data: data.clone(),
            txn_range,
        };

        let encoded = protocol::encode(&msg)?;
        let mut replicas_acked: u32 = 0;
        let mut total_bytes: u64 = 0;

        let addrs: Vec<String> = self.config.replica_addrs.clone();

        for addr in &addrs {
            match self.send_to_replica(addr, &encoded).await {
                Ok(()) => {
                    total_bytes += data_len;
                    self.total_bytes_shipped += data_len;
                    if let Some(bytes) = self.replica_bytes_shipped.get_mut(addr) {
                        *bytes += data_len;
                    }
                    replicas_acked += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        replica = %addr,
                        error = %e,
                        "failed to ship WAL segment to replica"
                    );
                    // In async mode we continue; in sync/semi-sync we may
                    // still continue but log the failure.
                }
            }
        }

        // Enforce sync mode requirements.
        match self.config.sync_mode {
            ReplicationSyncMode::Sync => {
                if replicas_acked < addrs.len() as u32 {
                    return Err(ExchangeDbError::Wal(format!(
                        "sync replication failed: only {replicas_acked}/{} replicas acknowledged",
                        addrs.len()
                    )));
                }
            }
            ReplicationSyncMode::SemiSync => {
                if replicas_acked == 0 && !addrs.is_empty() {
                    return Err(ExchangeDbError::Wal(
                        "semi-sync replication failed: no replicas acknowledged".into(),
                    ));
                }
            }
            ReplicationSyncMode::Async => {
                // No enforcement needed.
            }
        }

        Ok(ShipStats {
            bytes_shipped: total_bytes,
            replicas_acked,
        })
    }

    /// Ship pre-read WAL segment bytes to all replicas.
    ///
    /// Unlike `ship_segment` which reads from a file path, this accepts raw
    /// bytes directly. This avoids race conditions where the file is deleted
    /// by a merge before the async shipping task runs.
    pub async fn ship_segment_bytes(
        &mut self,
        table: &str,
        data: &[u8],
    ) -> Result<ShipStats> {
        let data_len = data.len() as u64;

        let msg = ReplicationMessage::WalSegment {
            table: table.to_string(),
            segment_id: 0, // Not meaningful for byte-based shipping
            data: data.to_vec(),
            txn_range: (0, 0), // Could extract from segment header if needed
        };

        let encoded = protocol::encode(&msg)?;
        let mut replicas_acked: u32 = 0;
        let mut total_bytes: u64 = 0;

        let addrs: Vec<String> = self.config.replica_addrs.clone();

        for addr in &addrs {
            match self.send_to_replica(addr, &encoded).await {
                Ok(()) => {
                    total_bytes += data_len;
                    self.total_bytes_shipped += data_len;
                    if let Some(bytes) = self.replica_bytes_shipped.get_mut(addr) {
                        *bytes += data_len;
                    }
                    replicas_acked += 1;
                    tracing::debug!(
                        table = %table,
                        bytes = data_len,
                        replica = %addr,
                        "shipped WAL segment bytes"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        replica = %addr,
                        error = %e,
                        "failed to ship WAL bytes to replica"
                    );
                }
            }
        }

        Ok(ShipStats {
            bytes_shipped: total_bytes,
            replicas_acked,
        })
    }

    /// Get current replication lag per replica.
    pub fn replication_lag(&self) -> HashMap<String, ReplicationLag> {
        let mut lags = HashMap::new();
        for addr in &self.config.replica_addrs {
            let last_ack = self.replica_positions.get(addr).copied().unwrap_or(0);
            let shipped = self.replica_bytes_shipped.get(addr).copied().unwrap_or(0);
            let bytes_behind = self.total_bytes_shipped.saturating_sub(shipped);

            lags.insert(
                addr.clone(),
                ReplicationLag {
                    replica: addr.clone(),
                    bytes_behind,
                    segments_behind: 0, // Would require segment-level tracking.
                    last_ack_txn: last_ack,
                },
            );
        }
        lags
    }

    /// Check if all replicas have acknowledged up to a given transaction ID.
    pub fn all_replicas_caught_up(&self, txn_id: u64) -> bool {
        self.replica_positions
            .values()
            .all(|&ack_txn| ack_txn >= txn_id)
    }

    /// Record an acknowledgment from a replica.
    pub fn record_ack(&mut self, replica_addr: &str, txn_id: u64) {
        if let Some(pos) = self.replica_positions.get_mut(replica_addr)
            && txn_id > *pos {
                *pos = txn_id;
            }
    }

    /// Get the replica positions map (for testing/diagnostics).
    pub fn replica_positions(&self) -> &HashMap<String, u64> {
        &self.replica_positions
    }

    /// Get the synced schema versions map (for testing/diagnostics).
    pub fn synced_schemas(&self) -> &HashMap<String, HashMap<String, u64>> {
        &self.synced_schemas
    }

    /// Send encoded data to a single replica over TCP.
    async fn send_to_replica(&self, addr: &str, data: &[u8]) -> Result<()> {
        let mut stream = TcpStream::connect(addr).await.map_err(|e| {
            ExchangeDbError::Wal(format!("failed to connect to replica {addr}: {e}"))
        })?;

        stream.write_all(data).await.map_err(|e| {
            ExchangeDbError::Wal(format!("failed to send data to replica {addr}: {e}"))
        })?;

        stream.flush().await.map_err(|e| {
            ExchangeDbError::Wal(format!("failed to flush data to replica {addr}: {e}"))
        })?;

        Ok(())
    }
}

/// Scan a WAL segment file to extract the min and max transaction IDs.
///
/// Opens the segment at the given path (which must reside in its parent
/// directory) and iterates all events, returning `(min_txn_id, max_txn_id)`.
/// Returns `(0, 0)` if the segment contains no events.
fn extract_txn_range_from_segment(segment_path: &Path) -> Result<(u64, u64)> {
    let dir = segment_path
        .parent()
        .ok_or_else(|| ExchangeDbError::Wal("segment path has no parent directory".into()))?;
    let segment_id = parse_segment_id_from_path(segment_path)?;
    let segment = WalSegment::open(dir, segment_id)?;

    let mut min_txn: u64 = u64::MAX;
    let mut max_txn: u64 = 0;

    // Iterate over all events in the segment, starting after the header.
    let mut offset = SEGMENT_HEADER_SIZE as u64;
    while offset < segment.len() {
        match segment.read_event_at(offset) {
            Ok((event, next_offset)) => {
                if event.txn_id < min_txn {
                    min_txn = event.txn_id;
                }
                if event.txn_id > max_txn {
                    max_txn = event.txn_id;
                }
                offset = next_offset;
            }
            Err(_) => break,
        }
    }

    if max_txn == 0 {
        Ok((0, 0))
    } else {
        Ok((min_txn, max_txn))
    }
}

/// Extract the segment ID from a WAL segment file path.
///
/// Expects paths like `/path/to/wal-000042.wal`.
fn parse_segment_id_from_path(path: &Path) -> Result<u32> {
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| ExchangeDbError::Wal("invalid segment path".into()))?;

    let id_str = filename
        .strip_prefix("wal-")
        .and_then(|s| s.strip_suffix(".wal"))
        .ok_or_else(|| {
            ExchangeDbError::Wal(format!("unexpected segment filename: {filename}"))
        })?;

    id_str
        .parse::<u32>()
        .map_err(|e| ExchangeDbError::Wal(format!("invalid segment ID in {filename}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::config::{ReplicationRole, ReplicationSyncMode};

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
    fn new_shipper_initializes_positions() {
        let shipper = make_shipper(
            vec!["10.0.0.2:9100", "10.0.0.3:9100"],
            ReplicationSyncMode::Async,
        );

        assert_eq!(shipper.replica_positions().len(), 2);
        assert_eq!(
            shipper.replica_positions().get("10.0.0.2:9100"),
            Some(&0)
        );
        assert_eq!(
            shipper.replica_positions().get("10.0.0.3:9100"),
            Some(&0)
        );
    }

    #[test]
    fn record_ack_updates_position() {
        let mut shipper = make_shipper(
            vec!["10.0.0.2:9100"],
            ReplicationSyncMode::Async,
        );

        shipper.record_ack("10.0.0.2:9100", 42);
        assert_eq!(
            shipper.replica_positions().get("10.0.0.2:9100"),
            Some(&42)
        );

        // Ack with a lower txn should not regress.
        shipper.record_ack("10.0.0.2:9100", 10);
        assert_eq!(
            shipper.replica_positions().get("10.0.0.2:9100"),
            Some(&42)
        );

        // Ack with a higher txn updates.
        shipper.record_ack("10.0.0.2:9100", 100);
        assert_eq!(
            shipper.replica_positions().get("10.0.0.2:9100"),
            Some(&100)
        );
    }

    #[test]
    fn all_replicas_caught_up() {
        let mut shipper = make_shipper(
            vec!["r1:9100", "r2:9100"],
            ReplicationSyncMode::Async,
        );

        assert!(!shipper.all_replicas_caught_up(10));

        shipper.record_ack("r1:9100", 10);
        assert!(!shipper.all_replicas_caught_up(10));

        shipper.record_ack("r2:9100", 10);
        assert!(shipper.all_replicas_caught_up(10));

        // Higher target means not caught up.
        assert!(!shipper.all_replicas_caught_up(11));
    }

    #[test]
    fn replication_lag_empty() {
        let shipper = make_shipper(
            vec!["r1:9100", "r2:9100"],
            ReplicationSyncMode::Async,
        );

        let lags = shipper.replication_lag();
        assert_eq!(lags.len(), 2);
        assert_eq!(lags.get("r1:9100").unwrap().last_ack_txn, 0);
        assert_eq!(lags.get("r1:9100").unwrap().bytes_behind, 0);
    }

    #[test]
    fn parse_segment_id_valid() {
        let path = Path::new("/data/wal/wal-000042.wal");
        assert_eq!(parse_segment_id_from_path(path).unwrap(), 42);

        let path = Path::new("wal-000000.wal");
        assert_eq!(parse_segment_id_from_path(path).unwrap(), 0);
    }

    #[test]
    fn parse_segment_id_invalid() {
        let path = Path::new("/data/wal/not-a-segment.dat");
        assert!(parse_segment_id_from_path(path).is_err());
    }

    #[test]
    fn no_replicas_always_caught_up() {
        let shipper = make_shipper(vec![], ReplicationSyncMode::Async);
        assert!(shipper.all_replicas_caught_up(999));
    }
}
