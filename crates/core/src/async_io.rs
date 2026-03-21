//! Async I/O abstraction layer.
//!
//! Provides a unified `AsyncColumnReader` trait with multiple backend
//! implementations: memory-mapped files (default) and standard blocking I/O.

use exchange_common::error::{ExchangeDbError, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::mmap::MmapReadOnly;

/// Available I/O backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoBackend {
    /// Memory-mapped files (default, best for large sequential scans).
    #[default]
    Mmap,
    /// Standard blocking I/O with `read` syscalls (fallback / comparison).
    StdIo,
    /// Linux io_uring (future: when the io-uring crate is added).
    #[cfg(target_os = "linux")]
    IoUring,
}

/// Trait for reading column data from storage.
///
/// Implementations are `Send` so they can be used across threads.
/// Despite the "Async" name, the current implementations are synchronous
/// under the hood; the trait exists to allow swapping in truly async
/// backends (e.g., io_uring) in the future.
pub trait AsyncColumnReader: Send {
    /// Read `len` bytes starting at `offset`.
    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// Read the entire column file.
    fn read_all(&self) -> Result<Vec<u8>>;

    /// Total length of the column file in bytes.
    fn len(&self) -> u64;

    /// Whether the file is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ── Mmap-based reader ──────────────────────────────────────────────────────

/// Column reader backed by a read-only memory-mapped file.
///
/// This is the default and fastest path for large sequential scans.
pub struct MmapColumnReader {
    mmap: MmapReadOnly,
}

impl MmapColumnReader {
    /// Open a column file via mmap.
    pub fn open(path: &Path) -> Result<Self> {
        let mmap = MmapReadOnly::open(path)?;
        Ok(Self { mmap })
    }
}

impl AsyncColumnReader for MmapColumnReader {
    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let end = (offset + len).min(self.mmap.len());
        if offset >= end {
            return Ok(Vec::new());
        }
        Ok(self.mmap.read_at(offset, (end - offset) as usize).to_vec())
    }

    fn read_all(&self) -> Result<Vec<u8>> {
        Ok(self.mmap.as_slice().to_vec())
    }

    fn len(&self) -> u64 {
        self.mmap.len()
    }
}

// ── Standard I/O reader ────────────────────────────────────────────────────

/// Column reader using standard `read` / `seek` syscalls.
///
/// Useful as a fallback when mmap is not available (e.g., network
/// filesystems) or for comparison benchmarks.
pub struct StdColumnReader {
    path: PathBuf,
    file_len: u64,
}

impl StdColumnReader {
    /// Open a column file using standard I/O.
    pub fn open(path: &Path) -> Result<Self> {
        let meta = std::fs::metadata(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file_len: meta.len(),
        })
    }
}

impl AsyncColumnReader for StdColumnReader {
    fn read_range(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let actual_len = len.min(self.file_len.saturating_sub(offset)) as usize;
        let mut buf = vec![0u8; actual_len];
        file.read_exact(&mut buf).map_err(ExchangeDbError::Io)?;
        Ok(buf)
    }

    fn read_all(&self) -> Result<Vec<u8>> {
        std::fs::read(&self.path).map_err(ExchangeDbError::Io)
    }

    fn len(&self) -> u64 {
        self.file_len
    }
}

/// Open a column reader for the given path using the specified backend.
pub fn open_column_reader(path: &Path, backend: IoBackend) -> Result<Box<dyn AsyncColumnReader>> {
    match backend {
        IoBackend::Mmap => Ok(Box::new(MmapColumnReader::open(path)?)),
        IoBackend::StdIo => Ok(Box::new(StdColumnReader::open(path)?)),
        #[cfg(target_os = "linux")]
        IoBackend::IoUring => {
            // Fallback to StdIo until io-uring crate is integrated.
            Ok(Box::new(StdColumnReader::open(path)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_test_file(dir: &Path, name: &str, data: &[u8]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, data).unwrap();
        path
    }

    #[test]
    fn mmap_reader_read_all() {
        let dir = tempdir().unwrap();
        let data: Vec<u8> = (0..=255u8).collect();
        let path = write_test_file(dir.path(), "col.d", &data);

        let reader = MmapColumnReader::open(&path).unwrap();
        assert_eq!(reader.len(), 256);
        assert_eq!(reader.read_all().unwrap(), data);
    }

    #[test]
    fn mmap_reader_read_range() {
        let dir = tempdir().unwrap();
        let data: Vec<u8> = (0..100u8).collect();
        let path = write_test_file(dir.path(), "col.d", &data);

        let reader = MmapColumnReader::open(&path).unwrap();
        let chunk = reader.read_range(10, 20).unwrap();
        assert_eq!(chunk, &data[10..30]);
    }

    #[test]
    fn std_reader_read_all() {
        let dir = tempdir().unwrap();
        let data: Vec<u8> = (0..=255u8).collect();
        let path = write_test_file(dir.path(), "col.d", &data);

        let reader = StdColumnReader::open(&path).unwrap();
        assert_eq!(reader.len(), 256);
        assert_eq!(reader.read_all().unwrap(), data);
    }

    #[test]
    fn std_reader_read_range() {
        let dir = tempdir().unwrap();
        let data: Vec<u8> = (0..100u8).collect();
        let path = write_test_file(dir.path(), "col.d", &data);

        let reader = StdColumnReader::open(&path).unwrap();
        let chunk = reader.read_range(10, 20).unwrap();
        assert_eq!(chunk, &data[10..30]);
    }

    #[test]
    fn std_and_mmap_produce_same_results() {
        let dir = tempdir().unwrap();
        // Write 1000 f64 values
        let mut data = Vec::new();
        for i in 0..1000u64 {
            data.extend_from_slice(&(i as f64 * 1.5).to_le_bytes());
        }
        let path = write_test_file(dir.path(), "prices.d", &data);

        let mmap_reader = MmapColumnReader::open(&path).unwrap();
        let std_reader = StdColumnReader::open(&path).unwrap();

        assert_eq!(mmap_reader.len(), std_reader.len());
        assert_eq!(
            mmap_reader.read_all().unwrap(),
            std_reader.read_all().unwrap()
        );

        // Compare range reads
        for offset in (0..data.len()).step_by(800) {
            let len = 80.min(data.len() - offset);
            let mmap_chunk = mmap_reader.read_range(offset as u64, len as u64).unwrap();
            let std_chunk = std_reader.read_range(offset as u64, len as u64).unwrap();
            assert_eq!(mmap_chunk, std_chunk, "mismatch at offset {offset}");
        }
    }

    #[test]
    fn open_column_reader_mmap() {
        let dir = tempdir().unwrap();
        let path = write_test_file(dir.path(), "test.d", &[1, 2, 3, 4]);
        let reader = open_column_reader(&path, IoBackend::Mmap).unwrap();
        assert_eq!(reader.len(), 4);
    }

    #[test]
    fn open_column_reader_stdio() {
        let dir = tempdir().unwrap();
        let path = write_test_file(dir.path(), "test.d", &[1, 2, 3, 4]);
        let reader = open_column_reader(&path, IoBackend::StdIo).unwrap();
        assert_eq!(reader.len(), 4);
    }

    #[test]
    fn empty_file() {
        let dir = tempdir().unwrap();
        let path = write_test_file(dir.path(), "empty.d", &[]);
        let mmap = MmapColumnReader::open(&path).unwrap();
        assert!(mmap.is_empty());
        assert_eq!(mmap.read_all().unwrap(), Vec::<u8>::new());

        let std = StdColumnReader::open(&path).unwrap();
        assert!(std.is_empty());
        assert_eq!(std.read_all().unwrap(), Vec::<u8>::new());
    }
}
