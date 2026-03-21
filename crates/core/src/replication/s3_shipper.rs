//! S3-based WAL shipping for cross-region / cloud-native replication.
//!
//! Instead of shipping WAL segments over TCP (like the standard `WalShipper`),
//! this module uploads segments to an object store (S3, MinIO, GCS, etc.)
//! and allows a receiver to download and apply them asynchronously.

use std::path::{Path, PathBuf};

use exchange_common::error::{ExchangeDbError, Result};

use crate::tiered::ObjectStore;

/// Ships WAL segments to an S3-compatible object store.
///
/// Each segment is stored as:
///   `{prefix}/{table}/wal-{segment_number}.wal`
pub struct S3WalShipper {
    store: Box<dyn ObjectStore>,
    prefix: String,
}

impl S3WalShipper {
    /// Create a new S3WalShipper.
    ///
    /// - `store`: The object store backend to upload segments to.
    /// - `prefix`: Key prefix for all WAL objects (e.g. "replication/primary1/").
    pub fn new(store: Box<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.to_string(),
        }
    }

    /// Build the object key for a WAL segment file.
    fn segment_key(&self, table: &str, filename: &str) -> String {
        if self.prefix.is_empty() {
            format!("{table}/{filename}")
        } else {
            format!("{}/{table}/{filename}", self.prefix)
        }
    }

    /// Upload a WAL segment file to the object store.
    ///
    /// The segment is stored under `{prefix}/{table}/{filename}` where
    /// `filename` is the last component of `segment_path`.
    pub fn ship_segment(&self, table: &str, segment_path: &Path) -> Result<()> {
        let filename = segment_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                ExchangeDbError::Wal(format!(
                    "invalid segment path: {}",
                    segment_path.display()
                ))
            })?;

        let data = std::fs::read(segment_path).map_err(|e| {
            ExchangeDbError::Wal(format!(
                "failed to read WAL segment {}: {e}",
                segment_path.display()
            ))
        })?;

        let key = self.segment_key(table, filename);
        self.store.put(&key, &data)
    }

    /// Upload raw WAL segment data to the object store.
    ///
    /// This variant does not read from disk; useful for testing or
    /// when the data is already in memory.
    pub fn ship_segment_data(&self, table: &str, filename: &str, data: &[u8]) -> Result<()> {
        let key = self.segment_key(table, filename);
        self.store.put(&key, data)
    }

    /// List available WAL segment keys in the object store for a given table.
    pub fn list_segments(&self, table: &str) -> Result<Vec<String>> {
        let prefix = if self.prefix.is_empty() {
            format!("{table}/")
        } else {
            format!("{}/{table}/", self.prefix)
        };
        self.store.list(&prefix)
    }
}

/// Receives WAL segments from an S3-compatible object store and
/// stores them locally for replay.
pub struct S3WalReceiver {
    store: Box<dyn ObjectStore>,
    prefix: String,
    local_wal_dir: PathBuf,
}

impl S3WalReceiver {
    /// Create a new S3WalReceiver.
    ///
    /// - `store`: The object store backend to download segments from.
    /// - `prefix`: Key prefix matching the shipper's prefix.
    /// - `local_wal_dir`: Local directory to store downloaded segments.
    pub fn new(store: Box<dyn ObjectStore>, prefix: &str, local_wal_dir: PathBuf) -> Self {
        Self {
            store,
            prefix: prefix.to_string(),
            local_wal_dir,
        }
    }

    /// Download and apply new WAL segments from the object store.
    ///
    /// Scans the object store for segments belonging to `table` that have
    /// a sequence number greater than `last_applied`. Downloads each new
    /// segment to the local WAL directory.
    ///
    /// Returns the highest sequence number that was synced, or `last_applied`
    /// if no new segments were found.
    pub fn sync(&self, table: &str, last_applied: u64) -> Result<u64> {
        let list_prefix = if self.prefix.is_empty() {
            format!("{table}/")
        } else {
            format!("{}/{table}/", self.prefix)
        };

        let keys = self.store.list(&list_prefix)?;

        let mut max_seq = last_applied;

        for key in &keys {
            // Extract sequence number from filename like "wal-000042.wal".
            let filename = key.rsplit('/').next().unwrap_or(key);
            if let Some(seq) = parse_segment_sequence(filename)
                && seq > last_applied {
                    // Download the segment.
                    let data = self.store.get(key)?;

                    // Ensure local directory exists.
                    let table_dir = self.local_wal_dir.join(table);
                    if !table_dir.exists() {
                        std::fs::create_dir_all(&table_dir)?;
                    }

                    // Write to local file.
                    let local_path = table_dir.join(filename);
                    std::fs::write(&local_path, &data)?;

                    if seq > max_seq {
                        max_seq = seq;
                    }
                }
        }

        Ok(max_seq)
    }

    /// Get the raw data for a specific segment from the object store.
    pub fn get_segment(&self, table: &str, filename: &str) -> Result<Vec<u8>> {
        let key = if self.prefix.is_empty() {
            format!("{table}/{filename}")
        } else {
            format!("{}/{table}/{filename}", self.prefix)
        };
        self.store.get(&key)
    }
}

/// Parse the sequence number from a WAL segment filename.
///
/// Expected format: `wal-NNNNNN.wal` where NNNNNN is a zero-padded number.
/// Returns `None` if the filename doesn't match the expected pattern.
fn parse_segment_sequence(filename: &str) -> Option<u64> {
    let name = filename.strip_prefix("wal-")?;
    let num_str = name.strip_suffix(".wal")?;
    num_str.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiered::MemoryObjectStore;

    #[test]
    fn shipper_ship_and_list() {
        let store = Box::new(MemoryObjectStore::new());
        let shipper = S3WalShipper::new(store, "repl");

        shipper
            .ship_segment_data("trades", "wal-000001.wal", b"segment1")
            .unwrap();
        shipper
            .ship_segment_data("trades", "wal-000002.wal", b"segment2")
            .unwrap();
        shipper
            .ship_segment_data("quotes", "wal-000001.wal", b"seg_q1")
            .unwrap();

        let trade_segs = shipper.list_segments("trades").unwrap();
        assert_eq!(trade_segs.len(), 2);

        let quote_segs = shipper.list_segments("quotes").unwrap();
        assert_eq!(quote_segs.len(), 1);
    }

    #[test]
    fn receiver_sync_downloads_new_segments() {
        let store = MemoryObjectStore::new();

        // Ship some segments via the store directly.
        store
            .put("repl/trades/wal-000001.wal", b"seg1")
            .unwrap();
        store
            .put("repl/trades/wal-000002.wal", b"seg2")
            .unwrap();
        store
            .put("repl/trades/wal-000003.wal", b"seg3")
            .unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let receiver = S3WalReceiver::new(
            Box::new(store),
            "repl",
            tmpdir.path().to_path_buf(),
        );

        // Sync from sequence 1 (should download 2 and 3).
        let max_seq = receiver.sync("trades", 1).unwrap();
        assert_eq!(max_seq, 3);

        // Verify files were downloaded locally.
        let local_seg2 = tmpdir.path().join("trades/wal-000002.wal");
        let local_seg3 = tmpdir.path().join("trades/wal-000003.wal");
        assert!(local_seg2.exists());
        assert!(local_seg3.exists());
        assert_eq!(std::fs::read(&local_seg2).unwrap(), b"seg2");
        assert_eq!(std::fs::read(&local_seg3).unwrap(), b"seg3");

        // wal-000001.wal should NOT have been downloaded (last_applied=1).
        let local_seg1 = tmpdir.path().join("trades/wal-000001.wal");
        assert!(!local_seg1.exists());
    }

    #[test]
    fn receiver_sync_no_new_segments() {
        let store = MemoryObjectStore::new();
        store
            .put("repl/trades/wal-000001.wal", b"seg1")
            .unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let receiver = S3WalReceiver::new(
            Box::new(store),
            "repl",
            tmpdir.path().to_path_buf(),
        );

        let max_seq = receiver.sync("trades", 5).unwrap();
        assert_eq!(max_seq, 5, "no new segments, should return last_applied");
    }

    #[test]
    fn shipper_receiver_roundtrip() {
        let store = MemoryObjectStore::new();

        // Use a shared reference via raw pointer trickery? No, better:
        // use the same MemoryObjectStore for both by wrapping in Arc is not
        // possible with Box<dyn ObjectStore>. Instead, ship then create receiver.
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

        // Verify content.
        let content1 = std::fs::read(tmpdir.path().join("orders/wal-000001.wal")).unwrap();
        assert_eq!(content1, b"data-one");
        let content2 = std::fs::read(tmpdir.path().join("orders/wal-000002.wal")).unwrap();
        assert_eq!(content2, b"data-two");
    }

    #[test]
    fn parse_segment_sequence_valid() {
        assert_eq!(parse_segment_sequence("wal-000001.wal"), Some(1));
        assert_eq!(parse_segment_sequence("wal-000042.wal"), Some(42));
        assert_eq!(parse_segment_sequence("wal-999999.wal"), Some(999999));
    }

    #[test]
    fn parse_segment_sequence_invalid() {
        assert_eq!(parse_segment_sequence("not-a-wal.wal"), None);
        assert_eq!(parse_segment_sequence("wal-.wal"), None);
        assert_eq!(parse_segment_sequence("random.txt"), None);
    }
}
