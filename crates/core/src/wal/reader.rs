use exchange_common::error::{ExchangeDbError, Result};
use std::path::{Path, PathBuf};

use super::event::WalEvent;
use super::segment::{SEGMENT_HEADER_SIZE, WalSegment};

/// Reads events from WAL segments, supporting iteration across multiple
/// segments in order.
pub struct WalReader {
    wal_dir: PathBuf,
    segment_ids: Vec<u32>,
}

impl WalReader {
    /// Open a WAL directory for reading. Discovers all segment files.
    pub fn open(wal_dir: &Path) -> Result<Self> {
        let segment_ids = discover_segments(wal_dir)?;
        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            segment_ids,
        })
    }

    /// Return the number of segments.
    pub fn segment_count(&self) -> usize {
        self.segment_ids.len()
    }

    /// Return the sorted list of segment IDs.
    pub fn segment_ids(&self) -> &[u32] {
        &self.segment_ids
    }

    /// Read all events from a specific segment.
    pub fn read_segment(&self, segment_id: u32) -> Result<Vec<WalEvent>> {
        let segment = WalSegment::open(&self.wal_dir, segment_id)?;
        segment.iter_events().collect()
    }

    /// Read all events from all segments in order.
    ///
    /// Segments that fail to open (e.g. stale segment ID mismatch) are
    /// silently skipped so that recovery is not blocked by leftover files.
    pub fn read_all(&self) -> Result<Vec<WalEvent>> {
        let mut events = Vec::new();
        for &seg_id in &self.segment_ids {
            match self.read_segment(seg_id) {
                Ok(seg_events) => events.extend(seg_events),
                Err(e) => {
                    tracing::warn!(segment_id = seg_id, error = %e, "skipping unreadable WAL segment");
                }
            }
        }
        Ok(events)
    }

    /// Read events starting from a given transaction ID (inclusive).
    /// Scans all segments to find matching events.
    pub fn read_from_txn(&self, min_txn_id: u64) -> Result<Vec<WalEvent>> {
        let mut events = Vec::new();
        for &seg_id in &self.segment_ids {
            let segment = WalSegment::open(&self.wal_dir, seg_id)?;
            for event_result in segment.iter_events() {
                let event = event_result?;
                if event.txn_id >= min_txn_id {
                    events.push(event);
                }
            }
        }
        Ok(events)
    }

    /// Iterate events across all segments lazily.
    pub fn iter(&self) -> Result<WalEventIter<'_>> {
        let segment = if self.segment_ids.is_empty() {
            None
        } else {
            Some(WalSegment::open(&self.wal_dir, self.segment_ids[0])?)
        };

        Ok(WalEventIter {
            wal_dir: &self.wal_dir,
            segment_ids: &self.segment_ids,
            current_seg_idx: 0,
            current_segment: segment,
            offset: SEGMENT_HEADER_SIZE as u64,
        })
    }
}

/// Lazy iterator over events across all WAL segments.
pub struct WalEventIter<'a> {
    wal_dir: &'a Path,
    segment_ids: &'a [u32],
    current_seg_idx: usize,
    current_segment: Option<WalSegment>,
    offset: u64,
}

impl<'a> Iterator for WalEventIter<'a> {
    type Item = Result<WalEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let segment = self.current_segment.as_ref()?;

            if self.offset < segment.len() {
                match segment.read_event_at(self.offset) {
                    Ok((event, next_offset)) => {
                        self.offset = next_offset;
                        return Some(Ok(event));
                    }
                    Err(e) => {
                        self.current_segment = None;
                        return Some(Err(e));
                    }
                }
            }

            // Move to the next segment.
            self.current_seg_idx += 1;
            if self.current_seg_idx >= self.segment_ids.len() {
                self.current_segment = None;
                return None;
            }

            let seg_id = self.segment_ids[self.current_seg_idx];
            match WalSegment::open(self.wal_dir, seg_id) {
                Ok(seg) => {
                    self.current_segment = Some(seg);
                    self.offset = SEGMENT_HEADER_SIZE as u64;
                }
                Err(e) => {
                    self.current_segment = None;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Discover all WAL segment files in a directory, returning sorted segment IDs.
fn discover_segments(dir: &Path) -> Result<Vec<u32>> {
    if !dir.exists() {
        return Err(ExchangeDbError::Wal(format!(
            "WAL directory does not exist: {}",
            dir.display()
        )));
    }

    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(id) = parse_segment_filename(&name) {
            ids.push(id);
        }
    }

    ids.sort();
    Ok(ids)
}

/// Parse a segment filename like "wal-000042.wal" into the segment ID.
fn parse_segment_filename(name: &str) -> Option<u32> {
    let name = name.strip_prefix("wal-")?;
    let name = name.strip_suffix(".wal")?;
    name.parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::{CommitMode, WalWriter, WalWriterConfig};
    use tempfile::tempdir;

    fn make_test_wal(dir: &Path) -> WalWriter {
        let config = WalWriterConfig {
            max_segment_size: 256,
            commit_mode: CommitMode::Sync,
        };
        WalWriter::create(dir, config).unwrap()
    }

    #[test]
    fn read_empty_wal() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Create an empty segment file so the reader finds something.
        {
            let seg = WalSegment::create(&wal_dir, 0).unwrap();
            seg.flush().unwrap();
        } // Drop truncates file to header-only

        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_all().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn read_all_events() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = make_test_wal(&wal_dir);
            writer.append_data(100, b"event1".to_vec()).unwrap();
            writer.append_data(200, b"event2".to_vec()).unwrap();
            writer.append_ddl(300, b"event3".to_vec()).unwrap();
            writer.flush().unwrap();
        }

        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_all().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].txn_id, 1);
        assert_eq!(events[1].txn_id, 2);
        assert_eq!(events[2].txn_id, 3);
    }

    #[test]
    fn read_across_segments() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = make_test_wal(&wal_dir);
            // Write enough to trigger at least one rotation.
            let payload = vec![0xCC; 80];
            for _ in 0..10 {
                writer.append_data(1, payload.clone()).unwrap();
            }
            writer.flush().unwrap();
            assert!(writer.current_segment_id() > 0, "should have rotated");
        }

        let reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.segment_count() > 1);

        let events = reader.read_all().unwrap();
        assert_eq!(events.len(), 10);

        // Verify txn IDs are sequential.
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.txn_id, (i + 1) as u64);
        }
    }

    #[test]
    fn read_from_txn_id() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = make_test_wal(&wal_dir);
            for i in 0..5 {
                writer.append_data(i * 100, b"data".to_vec()).unwrap();
            }
            writer.flush().unwrap();
        }

        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_from_txn(3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].txn_id, 3);
        assert_eq!(events[1].txn_id, 4);
        assert_eq!(events[2].txn_id, 5);
    }

    #[test]
    fn lazy_iterator() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = make_test_wal(&wal_dir);
            let payload = vec![0xDD; 80];
            for _ in 0..8 {
                writer.append_data(1, payload.clone()).unwrap();
            }
            writer.flush().unwrap();
        }

        let reader = WalReader::open(&wal_dir).unwrap();
        let events: Vec<WalEvent> = reader.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 8);
    }

    #[test]
    fn nonexistent_dir_returns_error() {
        let result = WalReader::open(Path::new("/nonexistent/wal/dir"));
        assert!(result.is_err());
    }

    #[test]
    fn read_single_segment() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let config = WalWriterConfig::default();
            let mut writer = WalWriter::create(&wal_dir, config).unwrap();
            writer.append_data(1, b"hello".to_vec()).unwrap();
            writer.append_data(2, b"world".to_vec()).unwrap();
            writer.flush().unwrap();
        }

        let reader = WalReader::open(&wal_dir).unwrap();
        let events = reader.read_segment(0).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].payload, b"hello");
        assert_eq!(events[1].payload, b"world");
    }
}
