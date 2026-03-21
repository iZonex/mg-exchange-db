use exchange_common::error::Result;
use std::path::{Path, PathBuf};

use super::event::{EventType, WalEvent};
use super::segment::WalSegment;
use super::sequencer::Sequencer;

/// Commit mode for WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Flush every write to disk immediately (durable but slower).
    Sync,
    /// Buffer writes and let the OS flush asynchronously (fast but may lose
    /// recent writes on crash).
    Async,
}

/// Default maximum segment size: 64 MB.
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Configuration for the WAL writer.
pub struct WalWriterConfig {
    pub max_segment_size: u64,
    pub commit_mode: CommitMode,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            commit_mode: CommitMode::Sync,
        }
    }
}

/// Appends events to WAL segments, rotating to a new segment when the current
/// one exceeds the configured maximum size.
pub struct WalWriter {
    wal_dir: PathBuf,
    current_segment: WalSegment,
    sequencer: Sequencer,
    config: WalWriterConfig,
}

impl WalWriter {
    /// Create a new WAL writer in the given directory, starting from segment 0.
    pub fn create(wal_dir: &Path, config: WalWriterConfig) -> Result<Self> {
        std::fs::create_dir_all(wal_dir)?;
        let segment = WalSegment::create(wal_dir, 0)?;

        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            current_segment: segment,
            sequencer: Sequencer::new(),
            config,
        })
    }

    /// Open an existing WAL directory and resume from the last segment.
    /// Scans for the highest segment ID and restores the sequencer from
    /// the last event's txn_id.
    pub fn open(wal_dir: &Path, config: WalWriterConfig) -> Result<Self> {
        let last_segment_id = find_last_segment_id(wal_dir)?;
        let segment = WalSegment::open(wal_dir, last_segment_id)?;

        // Scan the last segment to find the highest txn_id.
        let mut max_txn_id: u64 = 0;
        for event in segment.iter_events().flatten() {
            max_txn_id = max_txn_id.max(event.txn_id);
        }

        // Also scan all previous segments in case the last one is empty.
        // This is a simplified approach; in production you might persist
        // the sequencer state separately.
        if max_txn_id == 0 && last_segment_id > 0 {
            for seg_id in (0..last_segment_id).rev() {
                let prev_seg = WalSegment::open(wal_dir, seg_id)?;
                for event in prev_seg.iter_events().flatten() {
                    max_txn_id = max_txn_id.max(event.txn_id);
                }
                if max_txn_id > 0 {
                    break;
                }
            }
        }

        let sequencer = if max_txn_id > 0 {
            Sequencer::resume_from(max_txn_id)
        } else {
            Sequencer::new()
        };

        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            current_segment: segment,
            sequencer,
            config,
        })
    }

    /// Append a data event. Returns the assigned transaction ID.
    pub fn append_data(&mut self, timestamp: i64, payload: Vec<u8>) -> Result<u64> {
        self.append(EventType::Data, timestamp, payload)
    }

    /// Append a DDL event. Returns the assigned transaction ID.
    pub fn append_ddl(&mut self, timestamp: i64, payload: Vec<u8>) -> Result<u64> {
        self.append(EventType::Ddl, timestamp, payload)
    }

    /// Append a truncate event. Returns the assigned transaction ID.
    pub fn append_truncate(&mut self, timestamp: i64, payload: Vec<u8>) -> Result<u64> {
        self.append(EventType::Truncate, timestamp, payload)
    }

    /// Get the current segment ID.
    pub fn current_segment_id(&self) -> u32 {
        self.current_segment.segment_id()
    }

    /// Flush the current segment to disk.
    pub fn flush(&self) -> Result<()> {
        self.current_segment.flush()
    }

    /// Flush and truncate the current segment so the on-disk file
    /// matches the logical data length. This is needed before another
    /// process (e.g. merge job) re-opens the segment for reading.
    ///
    /// After calling this, do NOT write more data — create a new writer instead.
    pub fn seal(&mut self) -> Result<()> {
        self.current_segment.sync_and_seal()
    }

    /// Get the last issued transaction ID.
    pub fn last_txn_id(&self) -> u64 {
        self.sequencer.last_txn_id()
    }

    fn append(
        &mut self,
        event_type: EventType,
        timestamp: i64,
        payload: Vec<u8>,
    ) -> Result<u64> {
        let txn_id = self.sequencer.next_txn_id();

        let event = WalEvent {
            event_type,
            txn_id,
            timestamp,
            payload,
        };

        // Check if we need to rotate to a new segment.
        let event_size = event.wire_size() as u64;
        if self.current_segment.len() + event_size > self.config.max_segment_size {
            self.rotate()?;
        }

        self.current_segment.append_event(&event)?;

        if self.config.commit_mode == CommitMode::Sync {
            self.current_segment.flush()?;
        }

        Ok(txn_id)
    }

    fn rotate(&mut self) -> Result<()> {
        // Flush the current segment before rotating.
        self.current_segment.flush()?;

        let new_id = self.current_segment.segment_id() + 1;
        self.current_segment = WalSegment::create(&self.wal_dir, new_id)?;

        Ok(())
    }
}

/// Find the highest segment ID in a WAL directory.
fn find_last_segment_id(dir: &Path) -> Result<u32> {
    let mut max_id: Option<u32> = None;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(id) = parse_segment_filename(&name) {
            max_id = Some(max_id.map_or(id, |m: u32| m.max(id)));
        }
    }

    Ok(max_id.unwrap_or(0))
}

/// Parse a segment filename like "wal-000042.wal" into the segment ID (42).
fn parse_segment_filename(name: &str) -> Option<u32> {
    let name = name.strip_prefix("wal-")?;
    let name = name.strip_suffix(".wal")?;
    name.parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_write() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        let txn1 = writer.append_data(1000, b"row1".to_vec()).unwrap();
        let txn2 = writer.append_data(2000, b"row2".to_vec()).unwrap();

        assert_eq!(txn1, 1);
        assert_eq!(txn2, 2);
        assert_eq!(writer.last_txn_id(), 2);
        assert_eq!(writer.current_segment_id(), 0);
    }

    #[test]
    fn reopen_resumes_sequencer() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
            writer.append_data(100, b"a".to_vec()).unwrap();
            writer.append_data(200, b"b".to_vec()).unwrap();
            writer.append_ddl(300, b"create".to_vec()).unwrap();
            writer.flush().unwrap();
        }

        let mut writer = WalWriter::open(&wal_dir, WalWriterConfig::default()).unwrap();
        let txn = writer.append_data(400, b"c".to_vec()).unwrap();
        assert_eq!(txn, 4);
    }

    #[test]
    fn segment_rotation() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalWriterConfig {
            max_segment_size: 128, // Very small to trigger rotation.
            commit_mode: CommitMode::Async,
        };

        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        assert_eq!(writer.current_segment_id(), 0);

        // Write enough data to trigger rotation.
        let big_payload = vec![0xAA; 64];
        writer.append_data(1, big_payload.clone()).unwrap();
        writer.append_data(2, big_payload.clone()).unwrap();
        writer.append_data(3, big_payload.clone()).unwrap();
        writer.flush().unwrap();

        // Should have rotated at least once.
        assert!(writer.current_segment_id() > 0);

        // Verify segment files exist.
        let seg0 = super::super::segment::segment_path(&wal_dir, 0);
        assert!(seg0.exists());
        let seg1 = super::super::segment::segment_path(&wal_dir, 1);
        assert!(seg1.exists());
    }

    #[test]
    fn async_commit_mode_does_not_flush() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = WalWriterConfig {
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            commit_mode: CommitMode::Async,
        };

        let mut writer = WalWriter::create(&wal_dir, config).unwrap();
        // This should not panic even in async mode.
        writer.append_data(1, b"test".to_vec()).unwrap();
    }

    #[test]
    fn append_all_event_types() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::create(&wal_dir, WalWriterConfig::default()).unwrap();
        let t1 = writer.append_data(1, b"data".to_vec()).unwrap();
        let t2 = writer.append_ddl(2, b"ddl".to_vec()).unwrap();
        let t3 = writer.append_truncate(3, b"trunc".to_vec()).unwrap();

        assert_eq!(t1, 1);
        assert_eq!(t2, 2);
        assert_eq!(t3, 3);
    }

    #[test]
    fn parse_segment_filenames() {
        assert_eq!(parse_segment_filename("wal-000000.wal"), Some(0));
        assert_eq!(parse_segment_filename("wal-000042.wal"), Some(42));
        assert_eq!(parse_segment_filename("wal-999999.wal"), Some(999999));
        assert_eq!(parse_segment_filename("other.wal"), None);
        assert_eq!(parse_segment_filename("wal-abc.wal"), None);
    }
}
