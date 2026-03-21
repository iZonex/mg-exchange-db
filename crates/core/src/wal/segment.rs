use crate::mmap::MmapFile;
use exchange_common::error::{ExchangeDbError, Result};
use std::path::{Path, PathBuf};

use super::event::{EVENT_HEADER_SIZE, EVENT_OVERHEAD, WalEvent};

/// Default initial capacity for a WAL segment file (1 MB).
const SEGMENT_INITIAL_CAPACITY: u64 = 1024 * 1024;

/// WAL segment header layout (at the start of each segment file):
///
/// | field       | type  | bytes |
/// |-------------|-------|-------|
/// | magic       | [u8;4]| 4     | "XWAL"
/// | version     | u16   | 2     |
/// | segment_id  | u32   | 4     |
/// | reserved    | [u8;6]| 6     |
///
/// Total: 16 bytes
const SEGMENT_MAGIC: &[u8; 4] = b"XWAL";
const SEGMENT_VERSION: u16 = 1;
pub const SEGMENT_HEADER_SIZE: usize = 16;

/// A single WAL segment file. Contains a header followed by a sequence of events.
pub struct WalSegment {
    mmap: MmapFile,
    segment_id: u32,
    path: PathBuf,
}

impl WalSegment {
    /// Create a new WAL segment file.
    pub fn create(dir: &Path, segment_id: u32) -> Result<Self> {
        let path = segment_path(dir, segment_id);
        let mut mmap = MmapFile::open(&path, SEGMENT_INITIAL_CAPACITY)?;

        // Write segment header.
        if mmap.is_empty() {
            let mut header = [0u8; SEGMENT_HEADER_SIZE];
            header[0..4].copy_from_slice(SEGMENT_MAGIC);
            header[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
            header[6..10].copy_from_slice(&segment_id.to_le_bytes());
            // bytes 10..16 reserved
            mmap.append(&header)?;
        }

        Ok(Self {
            mmap,
            segment_id,
            path,
        })
    }

    /// Open an existing WAL segment, validating the header.
    pub fn open(dir: &Path, segment_id: u32) -> Result<Self> {
        let path = segment_path(dir, segment_id);
        let mmap = MmapFile::open(&path, SEGMENT_INITIAL_CAPACITY)?;

        if mmap.len() < SEGMENT_HEADER_SIZE as u64 {
            return Err(ExchangeDbError::Corruption(format!(
                "WAL segment {segment_id} too small: {} bytes",
                mmap.len()
            )));
        }

        let header = mmap.read_at(0, SEGMENT_HEADER_SIZE);
        if &header[0..4] != SEGMENT_MAGIC {
            return Err(ExchangeDbError::Corruption(format!(
                "WAL segment {segment_id} bad magic"
            )));
        }

        let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
        if version != SEGMENT_VERSION {
            return Err(ExchangeDbError::Corruption(format!(
                "WAL segment {segment_id} unsupported version: {version}"
            )));
        }

        let stored_id = u32::from_le_bytes(header[6..10].try_into().unwrap());
        if stored_id != segment_id {
            tracing::warn!(
                expected = segment_id,
                stored = stored_id,
                "WAL segment ID mismatch — skipping stale segment"
            );
            return Err(ExchangeDbError::Corruption(format!(
                "WAL segment ID mismatch (stale): expected {segment_id}, got {stored_id}"
            )));
        }

        Ok(Self {
            mmap,
            segment_id,
            path,
        })
    }

    /// Append a serialized event to the segment.
    ///
    /// Returns the offset at which the event was written, or an error if
    /// the write failed (e.g., disk full, grow failure). Errors are never
    /// silently swallowed.
    pub fn append_event(&mut self, event: &WalEvent) -> Result<u64> {
        let data = event.serialize();
        self.mmap.append(&data).map_err(|e| {
            tracing::error!(
                segment_id = self.segment_id,
                path = %self.path.display(),
                error = %e,
                "WAL segment append failed — potential data loss"
            );
            e
        })
    }

    /// Current data length of this segment (including header).
    pub fn len(&self) -> u64 {
        self.mmap.len()
    }

    /// Data size (excluding header).
    pub fn data_len(&self) -> u64 {
        self.mmap.len().saturating_sub(SEGMENT_HEADER_SIZE as u64)
    }

    pub fn is_empty(&self) -> bool {
        self.data_len() == 0
    }

    pub fn segment_id(&self) -> u32 {
        self.segment_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush segment data to disk (fsync).
    ///
    /// Returns an error if fsync fails. A failed fsync means the data may
    /// not have reached persistent storage and could be lost on crash.
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush().map_err(|e| {
            tracing::error!(
                segment_id = self.segment_id,
                path = %self.path.display(),
                error = %e,
                "WAL segment fsync failed — committed data may be lost on crash"
            );
            e
        })
    }

    /// Flush data to disk and truncate the backing file to the logical
    /// data length. This ensures that when the file is re-opened, the
    /// reader sees only valid data and not zero-filled capacity.
    ///
    /// **Warning**: After calling this, do NOT append more data to the same
    /// segment — the file has been truncated and subsequent writes may be
    /// lost. Use this only when you are done writing to this segment.
    pub fn sync_and_seal(&mut self) -> Result<()> {
        self.mmap.flush()?;
        self.mmap.truncate_to_len()
    }

    /// Read a single event at the given offset. Returns the event and the
    /// offset of the next event.
    pub fn read_event_at(&self, offset: u64) -> Result<(WalEvent, u64)> {
        let file_len = self.mmap.len();

        if offset + EVENT_HEADER_SIZE as u64 > file_len {
            return Err(ExchangeDbError::Wal("read past end of WAL segment".into()));
        }

        // Read header to determine payload length.
        let header = self.mmap.read_at(offset, EVENT_HEADER_SIZE);
        let payload_len = u32::from_le_bytes(header[17..21].try_into().unwrap()) as usize;

        let event_total = EVENT_OVERHEAD + payload_len;
        if offset + event_total as u64 > file_len {
            return Err(ExchangeDbError::Corruption(
                "WAL event extends past segment end".into(),
            ));
        }

        let event_bytes = self.mmap.read_at(offset, event_total);
        let event = WalEvent::deserialize(event_bytes)?;

        Ok((event, offset + event_total as u64))
    }

    /// Iterate all events in this segment.
    pub fn iter_events(&self) -> SegmentEventIter<'_> {
        SegmentEventIter {
            segment: self,
            offset: SEGMENT_HEADER_SIZE as u64,
        }
    }
}

/// Iterator over events in a WAL segment.
pub struct SegmentEventIter<'a> {
    segment: &'a WalSegment,
    offset: u64,
}

impl<'a> Iterator for SegmentEventIter<'a> {
    type Item = Result<WalEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.segment.len() {
            return None;
        }

        // Check if there are enough bytes for at least a header.
        if self.offset + EVENT_HEADER_SIZE as u64 > self.segment.len() {
            return None;
        }

        match self.segment.read_event_at(self.offset) {
            Ok((event, next_offset)) => {
                self.offset = next_offset;
                Some(Ok(event))
            }
            Err(e) => {
                // Stop iteration on error.
                self.offset = self.segment.len();
                Some(Err(e))
            }
        }
    }
}

/// Generate the filename for a given segment ID.
pub fn segment_filename(segment_id: u32) -> String {
    format!("wal-{segment_id:06}.wal")
}

/// Generate the full path for a segment file.
pub fn segment_path(dir: &Path, segment_id: u32) -> PathBuf {
    dir.join(segment_filename(segment_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_open_segment() {
        let dir = tempdir().unwrap();
        {
            let seg = WalSegment::create(dir.path(), 0).unwrap();
            assert_eq!(seg.segment_id(), 0);
            assert!(seg.is_empty());
            seg.flush().unwrap();
        }

        let seg = WalSegment::open(dir.path(), 0).unwrap();
        assert_eq!(seg.segment_id(), 0);
    }

    #[test]
    fn append_and_read_events() {
        let dir = tempdir().unwrap();
        let mut seg = WalSegment::create(dir.path(), 0).unwrap();

        let e1 = WalEvent::data(1, 1000, b"row1".to_vec());
        let e2 = WalEvent::ddl(2, 2000, b"alter table".to_vec());
        let e3 = WalEvent::truncate(3, 3000, b"table_x".to_vec());

        seg.append_event(&e1).unwrap();
        seg.append_event(&e2).unwrap();
        seg.append_event(&e3).unwrap();
        seg.flush().unwrap();

        let events: Vec<WalEvent> = seg.iter_events().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], e1);
        assert_eq!(events[1], e2);
        assert_eq!(events[2], e3);
    }

    #[test]
    fn reopen_segment_preserves_events() {
        let dir = tempdir().unwrap();

        let e1 = WalEvent::data(1, 100, b"persist me".to_vec());
        {
            let mut seg = WalSegment::create(dir.path(), 5).unwrap();
            seg.append_event(&e1).unwrap();
            seg.flush().unwrap();
        }

        let seg = WalSegment::open(dir.path(), 5).unwrap();
        let events: Vec<WalEvent> = seg.iter_events().map(|r| r.unwrap()).collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], e1);
    }

    #[test]
    fn segment_filename_format() {
        assert_eq!(segment_filename(0), "wal-000000.wal");
        assert_eq!(segment_filename(1), "wal-000001.wal");
        assert_eq!(segment_filename(999999), "wal-999999.wal");
    }

    #[test]
    fn bad_magic_rejected() {
        let dir = tempdir().unwrap();
        // Create a segment then corrupt the magic bytes.
        {
            let seg = WalSegment::create(dir.path(), 0).unwrap();
            seg.flush().unwrap();
        }
        // Corrupt the file.
        let path = segment_path(dir.path(), 0);
        let mut data = std::fs::read(&path).unwrap();
        data[0] = b'Z';
        std::fs::write(&path, &data).unwrap();

        let result = WalSegment::open(dir.path(), 0);
        assert!(result.is_err());
    }

    #[test]
    fn empty_segment_iter_yields_nothing() {
        let dir = tempdir().unwrap();
        let seg = WalSegment::create(dir.path(), 0).unwrap();
        let events: Vec<_> = seg.iter_events().collect();
        assert!(events.is_empty());
    }
}
