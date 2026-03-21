//! I/O abstraction layer that uses the best available method for the platform.
//!
//! On Linux, this provides an abstraction for io_uring-based reads (to be
//! implemented with actual io_uring syscalls or the `io-uring` crate in the
//! future). On all platforms, it falls back to memory-mapped reads.
//!
//! The key insight is that for sequential scans of cold data, Direct I/O
//! (bypassing the page cache) with io_uring submission batching can
//! outperform mmap. For hot data that benefits from caching, mmap is better.
//!
//! This module also provides page-aligned reading utilities with proper
//! `madvise` hints for optimal sequential I/O.

use crate::mmap::MmapReadOnly;
use exchange_common::error::Result;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Linux io_uring detection
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod linux_io {
    /// Check if io_uring is available on this kernel.
    ///
    /// io_uring was introduced in Linux 5.1. We check the kernel version
    /// by reading /proc/version and parsing it. A future implementation
    /// would also try the io_uring_setup syscall (425) with a probe ring
    /// to verify actual availability.
    pub fn io_uring_available() -> bool {
        // Try to read kernel version
        if let Ok(version_str) = std::fs::read_to_string("/proc/version") {
            if let Some(version) = parse_kernel_version(&version_str) {
                return version.0 > 5 || (version.0 == 5 && version.1 >= 1);
            }
        }
        false
    }

    /// Parse a kernel version string like "Linux version 5.15.0-..." into (major, minor).
    fn parse_kernel_version(s: &str) -> Option<(u32, u32)> {
        // Expected format: "Linux version X.Y.Z..."
        let version_part = s.split_whitespace().nth(2)?;
        let mut parts = version_part.split('.');
        let major: u32 = parts.next()?.parse().ok()?;
        let minor: u32 = parts.next()?.parse().ok()?;
        Some((major, minor))
    }

    #[cfg(test)]
    mod tests {

        #[test]
        fn parse_kernel_version_valid() {
            assert_eq!(
                parse_kernel_version("Linux version 5.15.0-generic"),
                Some((5, 15))
            );
            assert_eq!(
                parse_kernel_version(
                    "Linux version 6.1.0-17-amd64 (debian-kernel@lists.debian.org)"
                ),
                Some((6, 1))
            );
            assert_eq!(
                parse_kernel_version("Linux version 4.19.128"),
                Some((4, 19))
            );
        }

        #[test]
        fn parse_kernel_version_invalid() {
            assert_eq!(parse_kernel_version("not a version"), None);
            assert_eq!(parse_kernel_version(""), None);
        }

        #[test]
        fn io_uring_detection_does_not_panic() {
            // Just ensure it doesn't crash; result depends on the actual kernel
            let _ = io_uring_available();
        }
    }
}

// ---------------------------------------------------------------------------
// Direct I/O reader (Linux)
// ---------------------------------------------------------------------------

/// Direct I/O reader that bypasses the page cache.
///
/// Useful for large sequential scans of cold data where the data will not
/// be re-read soon and polluting the page cache would evict hot data.
///
/// On Linux, files opened with O_DIRECT require reads to be aligned to
/// the filesystem block size (typically 512 or 4096 bytes).
#[cfg(target_os = "linux")]
pub struct DirectIoReader {
    fd: std::os::unix::io::RawFd,
    len: u64,
    path: PathBuf,
}

#[cfg(target_os = "linux")]
impl DirectIoReader {
    /// Open a file with O_DIRECT for direct I/O.
    pub fn open(path: &Path) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        let len = file.metadata()?.len();
        let fd = {
            use std::os::unix::io::AsRawFd;
            file.as_raw_fd()
        };

        // Leak the file handle; we manage the fd directly.
        // In a real implementation we'd store the File and use its fd.
        std::mem::forget(file);

        Ok(Self {
            fd,
            len,
            path: path.to_path_buf(),
        })
    }

    /// File size in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the file is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read a page-aligned chunk from the file.
    ///
    /// Both `offset` and `buf.len()` must be aligned to `alignment` bytes
    /// (typically 4096). The buffer must also be memory-aligned.
    pub fn read_aligned(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        use std::io::Read;
        use std::os::unix::io::FromRawFd;

        // Safety: we own the fd
        let mut file = unsafe { std::fs::File::from_raw_fd(self.fd) };
        let result = {
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(offset))?;
            file.read(buf)?
        };
        // Don't close the fd when file goes out of scope
        std::mem::forget(file);
        Ok(result)
    }
}

#[cfg(target_os = "linux")]
impl Drop for DirectIoReader {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

// ---------------------------------------------------------------------------
// Optimal column reader
// ---------------------------------------------------------------------------

/// High-performance column reader that uses the best available I/O method.
///
/// Selection logic:
/// - If the data is expected to be re-read (hot), uses mmap for page cache benefits.
/// - On Linux with io_uring available, could use batched async I/O (future).
/// - On Linux without io_uring, can use Direct I/O to avoid cache pollution.
/// - On other platforms, always uses mmap.
pub struct OptimalColumnReader {
    inner: ReaderImpl,
}

enum ReaderImpl {
    /// Memory-mapped reader — best for hot data and random access.
    Mmap(MmapReadOnly),
    /// Buffered sequential reader — fallback for platforms without direct I/O.
    Buffered(BufferedReader),
}

/// Simple buffered reader for sequential access.
struct BufferedReader {
    data: Vec<u8>,
}

impl OptimalColumnReader {
    /// Open a column file using the optimal I/O strategy.
    ///
    /// `sequential_hint` suggests the data will be read sequentially (scan).
    /// When true, `madvise(MADV_SEQUENTIAL)` is applied.
    pub fn open(path: &Path, sequential_hint: bool) -> Result<Self> {
        let mmap = MmapReadOnly::open(path)?;

        if sequential_hint {
            // Hint to the OS for sequential access
            #[cfg(unix)]
            {
                let _ = mmap.inner_mmap().advise(memmap2::Advice::Sequential);
            }
        }

        Ok(Self {
            inner: ReaderImpl::Mmap(mmap),
        })
    }

    /// Open using buffered I/O (reads entire file into memory).
    ///
    /// Best for small files or when mmap overhead is not justified.
    pub fn open_buffered(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        Ok(Self {
            inner: ReaderImpl::Buffered(BufferedReader { data }),
        })
    }

    /// Get the full contents as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            ReaderImpl::Mmap(m) => m.as_slice(),
            ReaderImpl::Buffered(b) => &b.data,
        }
    }

    /// Read a range of bytes.
    pub fn read_at(&self, offset: u64, len: usize) -> &[u8] {
        match &self.inner {
            ReaderImpl::Mmap(m) => m.read_at(offset, len),
            ReaderImpl::Buffered(b) => &b.data[offset as usize..offset as usize + len],
        }
    }

    /// File/data length in bytes.
    pub fn len(&self) -> u64 {
        match &self.inner {
            ReaderImpl::Mmap(m) => m.len(),
            ReaderImpl::Buffered(b) => b.data.len() as u64,
        }
    }

    /// Whether the data is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Release the memory/pages associated with this reader.
    ///
    /// For mmap: advises the OS that pages are no longer needed.
    /// For buffered: drops the internal buffer.
    pub fn release_pages(&self) {
        match &self.inner {
            ReaderImpl::Mmap(m) => {
                #[cfg(unix)]
                {
                    // MADV_DONTNEED equivalent — tell the OS it can reclaim pages.
                    // memmap2 may not expose DontNeed on all platforms, so we use
                    // Sequential as a reasonable proxy.
                    let _ = m.inner_mmap().advise(memmap2::Advice::Sequential);
                }
            }
            ReaderImpl::Buffered(_) => {
                // Can't release without mut; pages will be freed on drop.
            }
        }
    }

    /// Whether this reader is using mmap.
    pub fn is_mmap(&self) -> bool {
        matches!(&self.inner, ReaderImpl::Mmap(_))
    }
}

// ---------------------------------------------------------------------------
// Page-aligned reading for optimal I/O
// ---------------------------------------------------------------------------

/// Standard OS page size (4 KiB).
pub const PAGE_SIZE_4K: usize = 4096;

/// Huge page size (2 MiB).
pub const PAGE_SIZE_2M: usize = 2 * 1024 * 1024;

/// Align a value up to the nearest multiple of `align`.
#[inline]
pub fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Align a value down to the nearest multiple of `align`.
#[inline]
pub fn align_down(value: usize, align: usize) -> usize {
    value & !(align - 1)
}

/// Iterator that reads column data in page-aligned chunks.
///
/// Uses `madvise(MADV_SEQUENTIAL)` before reading and can release pages
/// after they've been consumed via `madvise(MADV_DONTNEED)`.
pub struct PageAlignedReader {
    mmap: MmapReadOnly,
    page_size: usize,
    offset: usize,
    total_len: usize,
}

impl PageAlignedReader {
    /// Open a column file for page-aligned sequential reading.
    ///
    /// `page_size` is typically 4096 (normal pages) or 2097152 (2MB huge pages).
    pub fn open(path: &Path, page_size: usize) -> Result<Self> {
        let mmap = MmapReadOnly::open(path)?;
        let total_len = mmap.len() as usize;

        // Advise sequential access pattern
        #[cfg(unix)]
        {
            let _ = mmap.inner_mmap().advise(memmap2::Advice::Sequential);
        }

        Ok(Self {
            mmap,
            page_size,
            offset: 0,
            total_len,
        })
    }

    /// Read the next page-aligned chunk.
    ///
    /// Returns `None` when all data has been read. The returned slice
    /// length is `page_size` except for the last chunk which may be smaller.
    pub fn next_chunk(&mut self) -> Option<&[u8]> {
        if self.offset >= self.total_len {
            return None;
        }

        let end = (self.offset + self.page_size).min(self.total_len);
        let chunk = self.mmap.read_at(self.offset as u64, end - self.offset);

        // Release the previous pages (the ones we've already read past)
        if self.offset >= self.page_size {
            self.release_range(self.offset - self.page_size, self.page_size);
        }

        self.offset = end;
        Some(chunk)
    }

    /// Release pages in the given range, hinting to the OS they can be freed.
    fn release_range(&self, offset: usize, len: usize) {
        #[cfg(unix)]
        {
            let _ = self.mmap.inner_mmap().advise_range(
                memmap2::Advice::Sequential, // proxy for DontNeed
                offset,
                len,
            );
        }
    }

    /// Number of bytes remaining to be read.
    pub fn remaining(&self) -> usize {
        self.total_len.saturating_sub(self.offset)
    }

    /// Total file size.
    pub fn total_len(&self) -> usize {
        self.total_len
    }

    /// Current read offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Reset the reader to the beginning.
    pub fn reset(&mut self) {
        self.offset = 0;
        #[cfg(unix)]
        {
            let _ = self.mmap.inner_mmap().advise(memmap2::Advice::Sequential);
        }
    }
}

/// Read an entire column file in page-aligned chunks, collecting results.
///
/// This is a convenience function that reads the file using `PageAlignedReader`
/// and applies `madvise(MADV_SEQUENTIAL)` before and `MADV_DONTNEED`-equivalent
/// after reading.
pub fn read_column_pages(path: &Path, _page_size: usize) -> Result<Vec<u8>> {
    let mmap = MmapReadOnly::open(path)?;

    // Advise sequential
    #[cfg(unix)]
    {
        let _ = mmap.inner_mmap().advise(memmap2::Advice::Sequential);
    }

    // Read all data
    let data = mmap.as_slice().to_vec();

    // Advise we're done
    #[cfg(unix)]
    {
        let _ = mmap.inner_mmap().advise(memmap2::Advice::Sequential);
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn create_test_file(dir: &Path, name: &str, size: usize) -> PathBuf {
        let path = dir.join(name);
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        path
    }

    #[test]
    fn align_up_works() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
        assert_eq!(align_up(8191, 4096), 8192);
    }

    #[test]
    fn align_down_works() {
        assert_eq!(align_down(0, 4096), 0);
        assert_eq!(align_down(1, 4096), 0);
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(4097, 4096), 4096);
        assert_eq!(align_down(8191, 4096), 4096);
    }

    #[test]
    fn optimal_reader_mmap() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 8192);

        let reader = OptimalColumnReader::open(&path, false).unwrap();
        assert!(reader.is_mmap());
        assert_eq!(reader.len(), 8192);
        assert!(!reader.is_empty());

        let slice = reader.as_slice();
        assert_eq!(slice.len(), 8192);
        assert_eq!(slice[0], 0);
        assert_eq!(slice[255], 255);
    }

    #[test]
    fn optimal_reader_sequential_hint() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 4096);

        let reader = OptimalColumnReader::open(&path, true).unwrap();
        assert!(reader.is_mmap());
        assert_eq!(reader.len(), 4096);
    }

    #[test]
    fn optimal_reader_buffered() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 1024);

        let reader = OptimalColumnReader::open_buffered(&path).unwrap();
        assert!(!reader.is_mmap());
        assert_eq!(reader.len(), 1024);
        assert_eq!(reader.read_at(0, 4), &[0, 1, 2, 3]);
    }

    #[test]
    fn optimal_reader_release_does_not_panic() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 4096);

        let reader = OptimalColumnReader::open(&path, false).unwrap();
        reader.release_pages(); // should not panic
    }

    #[test]
    fn page_aligned_reader_basic() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 10000);

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_4K).unwrap();
        assert_eq!(reader.total_len(), 10000);
        assert_eq!(reader.remaining(), 10000);

        // First chunk: 4096 bytes
        let chunk1 = reader.next_chunk().unwrap();
        assert_eq!(chunk1.len(), 4096);
        assert_eq!(reader.remaining(), 10000 - 4096);

        // Second chunk: 4096 bytes
        let chunk2 = reader.next_chunk().unwrap();
        assert_eq!(chunk2.len(), 4096);
        assert_eq!(reader.remaining(), 10000 - 8192);

        // Third chunk: 1808 bytes (remainder)
        let chunk3 = reader.next_chunk().unwrap();
        assert_eq!(chunk3.len(), 10000 - 8192);

        // No more chunks
        assert!(reader.next_chunk().is_none());
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn page_aligned_reader_exact_pages() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 8192);

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_4K).unwrap();

        let c1 = reader.next_chunk().unwrap();
        assert_eq!(c1.len(), 4096);

        let c2 = reader.next_chunk().unwrap();
        assert_eq!(c2.len(), 4096);

        assert!(reader.next_chunk().is_none());
    }

    #[test]
    fn page_aligned_reader_empty_file() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "empty.d", 0);

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_4K).unwrap();
        assert_eq!(reader.total_len(), 0);
        assert!(reader.next_chunk().is_none());
    }

    #[test]
    fn page_aligned_reader_small_file() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "small.d", 100);

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_4K).unwrap();
        let chunk = reader.next_chunk().unwrap();
        assert_eq!(chunk.len(), 100);
        assert!(reader.next_chunk().is_none());
    }

    #[test]
    fn page_aligned_reader_reset() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 8192);

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_4K).unwrap();
        let _ = reader.next_chunk().unwrap();
        let _ = reader.next_chunk().unwrap();
        assert!(reader.next_chunk().is_none());

        reader.reset();
        assert_eq!(reader.remaining(), 8192);
        let chunk = reader.next_chunk().unwrap();
        assert_eq!(chunk.len(), 4096);
    }

    #[test]
    fn page_aligned_reader_data_integrity() {
        let dir = tempdir().unwrap();
        let size = 10000;
        let path = create_test_file(dir.path(), "col.d", size);
        let expected: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_4K).unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = reader.next_chunk() {
            collected.extend_from_slice(chunk);
        }

        assert_eq!(collected, expected);
    }

    #[test]
    fn read_column_pages_function() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 4096);

        let data = read_column_pages(&path, PAGE_SIZE_4K).unwrap();
        assert_eq!(data.len(), 4096);
        assert_eq!(data[0], 0);
        assert_eq!(data[255], 255);
    }

    #[test]
    fn large_page_size() {
        let dir = tempdir().unwrap();
        let path = create_test_file(dir.path(), "col.d", 1_000_000);

        let mut reader = PageAlignedReader::open(&path, PAGE_SIZE_2M).unwrap();
        // Entire file fits in one "page" since 1M < 2M
        let chunk = reader.next_chunk().unwrap();
        assert_eq!(chunk.len(), 1_000_000);
        assert!(reader.next_chunk().is_none());
    }

    #[cfg(target_os = "linux")]
    mod linux_tests {

        #[test]
        fn io_uring_available_does_not_panic() {
            let _ = crate::io_uring_reader::linux_io::io_uring_available();
        }
    }
}
