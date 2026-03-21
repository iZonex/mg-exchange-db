use std::collections::HashMap;
use std::path::PathBuf;

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use super::protocol::{self, ReplicationMessage};
use crate::table::TableMeta;
use crate::wal::merge::WalMergeJob;

/// Tracks the replication position of a replica: which transaction each table
/// has been applied up to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaPosition {
    /// The highest transaction ID applied across all tables.
    pub last_applied_txn: u64,
    /// Per-table last applied transaction ID.
    pub tables: HashMap<String, u64>,
}

impl ReplicaPosition {
    /// Create a new empty position.
    pub fn new() -> Self {
        Self {
            last_applied_txn: 0,
            tables: HashMap::new(),
        }
    }

    /// Update the position for a given table.
    pub fn update(&mut self, table: &str, txn_id: u64) {
        let entry = self.tables.entry(table.to_string()).or_insert(0);
        if txn_id > *entry {
            *entry = txn_id;
        }
        if txn_id > self.last_applied_txn {
            self.last_applied_txn = txn_id;
        }
    }
}

impl Default for ReplicaPosition {
    fn default() -> Self {
        Self::new()
    }
}

/// Receives WAL segments from a primary and applies them to local storage.
pub struct WalReceiver {
    /// Root directory of the local database.
    db_root: PathBuf,
    /// Address of the primary to receive from.
    primary_addr: String,
    /// Current replication position.
    position: ReplicaPosition,
}

impl WalReceiver {
    /// Create a new WAL receiver.
    pub fn new(db_root: PathBuf, primary_addr: String) -> Self {
        Self {
            db_root,
            primary_addr,
            position: ReplicaPosition::new(),
        }
    }

    /// Start receiving WAL segments from the primary.
    ///
    /// This listens on a local TCP port for incoming replication connections
    /// from the primary. Each incoming connection is expected to send
    /// length-prefixed replication messages.
    pub async fn start(&mut self) -> Result<()> {
        // Bind to a local port (port 0 = OS-assigned, or we could use
        // a configured port). For now we listen on 0.0.0.0:0.
        let listener = TcpListener::bind("0.0.0.0:0").await.map_err(|e| {
            ExchangeDbError::Wal(format!("failed to bind receiver: {e}"))
        })?;

        let local_addr = listener.local_addr().map_err(|e| {
            ExchangeDbError::Wal(format!("failed to get local addr: {e}"))
        })?;

        tracing::info!(
            addr = %local_addr,
            primary = %self.primary_addr,
            "WAL receiver started"
        );

        loop {
            let (mut stream, peer) = listener.accept().await.map_err(|e| {
                ExchangeDbError::Wal(format!("accept failed: {e}"))
            })?;

            tracing::debug!(peer = %peer, "accepted replication connection");

            // Read the length prefix.
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                continue;
            }
            let payload_len = u32::from_le_bytes(len_buf) as usize;

            // Read the payload.
            let mut payload = vec![0u8; payload_len];
            if stream.read_exact(&mut payload).await.is_err() {
                continue;
            }

            // Reassemble the full frame for decoding.
            let mut frame = Vec::with_capacity(4 + payload_len);
            frame.extend_from_slice(&len_buf);
            frame.extend_from_slice(&payload);

            match protocol::decode(&frame) {
                Ok((msg, _)) => {
                    if let Err(e) = self.handle_message(msg) {
                        tracing::error!(error = %e, "failed to handle replication message");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to decode replication message");
                }
            }
        }
    }

    /// Apply a received WAL segment to local storage.
    ///
    /// Writes the segment data to the appropriate table's WAL directory
    /// and updates the replication position. Returns the number of bytes
    /// written.
    pub fn apply_segment(&mut self, table: &str, segment_data: &[u8]) -> Result<u64> {
        let table_wal_dir = self.db_root.join(table).join("wal");
        std::fs::create_dir_all(&table_wal_dir).map_err(|e| {
            ExchangeDbError::Wal(format!(
                "failed to create WAL dir for table {table}: {e}"
            ))
        })?;

        // Determine the next segment file name by scanning existing files.
        let next_id = self.next_segment_id(&table_wal_dir)?;
        let segment_path =
            table_wal_dir.join(format!("wal-{next_id:06}.wal"));

        // The raw segment data contains a header with the primary's segment_id.
        // We need to rewrite the segment_id in the header (bytes 6..10) to match
        // the local filename ID, otherwise WalSegment::open() will reject it with
        // "segment ID mismatch".
        let mut data = segment_data.to_vec();
        if data.len() >= 10 {
            data[6..10].copy_from_slice(&next_id.to_le_bytes());
        }

        std::fs::write(&segment_path, &data).map_err(|e| {
            ExchangeDbError::Wal(format!(
                "failed to write WAL segment {}: {e}",
                segment_path.display()
            ))
        })?;

        let bytes_written = segment_data.len() as u64;

        tracing::debug!(
            table = %table,
            segment_id = next_id,
            bytes = bytes_written,
            "applied WAL segment"
        );

        Ok(bytes_written)
    }

    /// Get the current replica position.
    pub fn current_position(&self) -> ReplicaPosition {
        self.position.clone()
    }

    /// Handle an incoming replication message.
    fn handle_message(&mut self, msg: ReplicationMessage) -> Result<()> {
        match msg {
            ReplicationMessage::WalSegment {
                table,
                data,
                txn_range,
                ..
            } => {
                self.apply_segment(&table, &data)?;
                // Automatically merge the WAL into column files on the replica.
                self.try_merge(&table)?;
                // Update position using the high end of the txn range.
                let high_txn = txn_range.1;
                if high_txn > 0 {
                    self.position.update(&table, high_txn);
                }
                Ok(())
            }
            ReplicationMessage::StatusRequest => {
                // In a full implementation, we would send back a StatusResponse.
                tracing::debug!("received status request (response not yet implemented)");
                Ok(())
            }
            ReplicationMessage::SchemaSync {
                table,
                meta_json,
                version,
            } => {
                self.apply_schema_sync(&table, &meta_json, version)?;
                Ok(())
            }
            ReplicationMessage::FullSyncRequired { table } => {
                tracing::warn!(
                    table = %table,
                    "full sync required (not yet implemented)"
                );
                Ok(())
            }
            _ => {
                tracing::debug!("ignoring unexpected message type on receiver");
                Ok(())
            }
        }
    }

    /// Attempt to run the WAL merge job for a table on the replica side.
    ///
    /// Loads the table metadata from `{db_root}/{table}/_meta` and runs
    /// `WalMergeJob` to apply WAL events to column files. If metadata
    /// is not found (e.g. the table was not yet created on the replica),
    /// this is a no-op.
    fn try_merge(&self, table: &str) -> Result<()> {
        let table_dir = self.db_root.join(table);
        let meta_path = table_dir.join("_meta");

        if !meta_path.exists() {
            tracing::debug!(
                table = %table,
                "skipping merge: table metadata not found on replica"
            );
            return Ok(());
        }

        let meta = TableMeta::load(&meta_path)?;

        // Ensure the _txn file exists (WalMergeJob requires it).
        let txn_path = table_dir.join("_txn");
        if !txn_path.exists() {
            let _txn = crate::txn::TxnFile::open(&table_dir)?;
        }

        let merge_job = WalMergeJob::new(table_dir, meta);
        match merge_job.run() {
            Ok(stats) => {
                tracing::debug!(
                    table = %table,
                    rows_merged = stats.rows_merged,
                    segments_processed = stats.segments_processed,
                    "replica merge completed"
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    table = %table,
                    error = %e,
                    "replica merge failed"
                );
                Err(e)
            }
        }
    }

    /// Apply a `SchemaSync` message: write (or update) the table's `_meta`
    /// file on the replica side.  Only overwrites when the incoming version
    /// is strictly newer than what is already on disk.
    pub fn apply_schema_sync(
        &self,
        table: &str,
        meta_json: &str,
        version: u64,
    ) -> Result<()> {
        let table_dir = self.db_root.join(table);
        std::fs::create_dir_all(&table_dir).map_err(|e| {
            ExchangeDbError::Wal(format!(
                "failed to create table dir for {table}: {e}"
            ))
        })?;

        let meta_path = table_dir.join("_meta");

        let should_update = if meta_path.exists() {
            match TableMeta::load(&meta_path) {
                Ok(existing) => version > existing.version,
                Err(_) => true, // corrupt, overwrite
            }
        } else {
            true
        };

        if should_update {
            std::fs::write(&meta_path, meta_json).map_err(|e| {
                ExchangeDbError::Wal(format!(
                    "failed to write _meta for table {table}: {e}"
                ))
            })?;

            // Ensure the _txn file exists so merges can proceed.
            let _ = crate::txn::TxnFile::open(&table_dir);

            tracing::info!(
                table = %table,
                version,
                "schema synced from primary"
            );
        }

        Ok(())
    }

    /// Find the next available segment ID for a table's WAL directory.
    fn next_segment_id(&self, wal_dir: &std::path::Path) -> Result<u32> {
        let mut max_id: Option<u32> = None;

        if wal_dir.exists() {
            for entry in std::fs::read_dir(wal_dir)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();

                if let Some(rest) = name.strip_prefix("wal-") {
                    if let Some(id_str) = rest.strip_suffix(".wal") {
                        if let Ok(id) = id_str.parse::<u32>() {
                            max_id = Some(max_id.map_or(id, |m: u32| m.max(id)));
                        }
                    }
                }
            }
        }

        Ok(max_id.map_or(0, |id| id + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn replica_position_new() {
        let pos = ReplicaPosition::new();
        assert_eq!(pos.last_applied_txn, 0);
        assert!(pos.tables.is_empty());
    }

    #[test]
    fn replica_position_update() {
        let mut pos = ReplicaPosition::new();

        pos.update("trades", 10);
        assert_eq!(pos.last_applied_txn, 10);
        assert_eq!(pos.tables.get("trades"), Some(&10));

        pos.update("orders", 5);
        assert_eq!(pos.last_applied_txn, 10); // Should not regress.
        assert_eq!(pos.tables.get("orders"), Some(&5));

        pos.update("trades", 20);
        assert_eq!(pos.last_applied_txn, 20);
        assert_eq!(pos.tables.get("trades"), Some(&20));
    }

    #[test]
    fn replica_position_no_regression() {
        let mut pos = ReplicaPosition::new();
        pos.update("t1", 50);
        pos.update("t1", 30); // Should not regress.
        assert_eq!(pos.tables.get("t1"), Some(&50));
        assert_eq!(pos.last_applied_txn, 50);
    }

    #[test]
    fn apply_segment_creates_wal_dir() {
        let dir = tempdir().unwrap();
        let mut receiver = WalReceiver::new(
            dir.path().to_path_buf(),
            "127.0.0.1:9100".into(),
        );

        let fake_data = b"XWAL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let bytes = receiver.apply_segment("test_table", fake_data).unwrap();

        assert_eq!(bytes, fake_data.len() as u64);

        // Verify file was created.
        let wal_dir = dir.path().join("test_table").join("wal");
        assert!(wal_dir.exists());
        let seg_path = wal_dir.join("wal-000000.wal");
        assert!(seg_path.exists());

        let contents = std::fs::read(&seg_path).unwrap();
        assert_eq!(contents, fake_data);
    }

    #[test]
    fn apply_segment_increments_id() {
        let dir = tempdir().unwrap();
        let mut receiver = WalReceiver::new(
            dir.path().to_path_buf(),
            "127.0.0.1:9100".into(),
        );

        let data1 = b"segment-1-data";
        let data2 = b"segment-2-data";

        receiver.apply_segment("my_table", data1).unwrap();
        receiver.apply_segment("my_table", data2).unwrap();

        let wal_dir = dir.path().join("my_table").join("wal");
        assert!(wal_dir.join("wal-000000.wal").exists());
        assert!(wal_dir.join("wal-000001.wal").exists());
    }

    #[test]
    fn current_position_reflects_state() {
        let dir = tempdir().unwrap();
        let receiver = WalReceiver::new(
            dir.path().to_path_buf(),
            "127.0.0.1:9100".into(),
        );

        let pos = receiver.current_position();
        assert_eq!(pos.last_applied_txn, 0);
        assert!(pos.tables.is_empty());
    }

    #[test]
    fn handle_wal_segment_message() {
        let dir = tempdir().unwrap();
        let mut receiver = WalReceiver::new(
            dir.path().to_path_buf(),
            "127.0.0.1:9100".into(),
        );

        let msg = ReplicationMessage::WalSegment {
            table: "trades".into(),
            segment_id: 0,
            data: vec![0xAA; 32],
            txn_range: (1, 10),
        };

        receiver.handle_message(msg).unwrap();

        let pos = receiver.current_position();
        assert_eq!(pos.last_applied_txn, 10);
        assert_eq!(pos.tables.get("trades"), Some(&10));

        // Verify file was written.
        let seg_path = dir.path().join("trades").join("wal").join("wal-000000.wal");
        assert!(seg_path.exists());
    }

    #[test]
    fn replica_position_serialization_roundtrip() {
        let mut pos = ReplicaPosition::new();
        pos.update("trades", 42);
        pos.update("orders", 17);

        let json = serde_json::to_string(&pos).unwrap();
        let restored: ReplicaPosition = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.last_applied_txn, 42);
        assert_eq!(restored.tables.get("trades"), Some(&42));
        assert_eq!(restored.tables.get("orders"), Some(&17));
    }
}
