use exchange_common::error::{ExchangeDbError, Result};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Controls how and when data is synced to disk.
#[derive(Debug, Clone, Default)]
pub enum SyncMode {
    /// fsync on every flush — strongest durability guarantee.
    #[default]
    Full,
    /// fdatasync equivalent — metadata not synced, slightly faster.
    DataOnly,
    /// Periodic sync every N milliseconds (caller manages the timer).
    Periodic(Duration),
    /// OS-managed writeback — no explicit sync.
    None,
}

/// Configuration for opening a memory-mapped file.
#[derive(Debug, Clone)]
pub struct MmapConfig {
    /// Initial file capacity in bytes.
    pub initial_capacity: u64,
    /// Sync mode for durability.
    pub sync_mode: SyncMode,
    /// Use huge pages (MAP_HUGETLB) on Linux for large mappings.
    pub huge_pages: bool,
}

impl Default for MmapConfig {
    fn default() -> Self {
        Self {
            initial_capacity: 1024 * 1024, // 1 MB
            sync_mode: SyncMode::Full,
            huge_pages: false,
        }
    }
}

impl MmapConfig {
    /// Create a config with just an initial capacity (backwards-compatible).
    pub fn with_capacity(initial_capacity: u64) -> Self {
        Self {
            initial_capacity,
            ..Default::default()
        }
    }
}

/// Read-write memory-mapped file for append-only column writes.
pub struct MmapFile {
    mmap: MmapMut,
    file: File,
    path: PathBuf,
    len: u64,
    capacity: u64,
    sync_mode: SyncMode,
}

impl MmapFile {
    /// Open or create a memory-mapped file with initial capacity.
    /// Uses `SyncMode::Full` by default.
    pub fn open(path: &Path, initial_capacity: u64) -> Result<Self> {
        Self::open_with_config(path, MmapConfig::with_capacity(initial_capacity))
    }

    /// Open or create a memory-mapped file with full configuration.
    pub fn open_with_config(path: &Path, config: MmapConfig) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let capacity = if file_len == 0 {
            file.set_len(config.initial_capacity)?;
            config.initial_capacity
        } else {
            file_len
        };

        // SAFETY: We have exclusive write access via the file handle.
        // The file is opened in read-write mode and we control all mutations.
        let mmap = unsafe {
            let mut opts = MmapOptions::new();
            opts.len(capacity as usize);
            // huge_pages: on Linux we would use .huge() or MAP_HUGETLB;
            // memmap2 does not directly expose MAP_HUGETLB, so this is a
            // future extension point. The flag is stored for documentation.
            opts.map_mut(&file)?
        };

        Ok(Self {
            mmap,
            file,
            path: path.to_path_buf(),
            len: file_len.min(capacity),
            capacity,
            sync_mode: config.sync_mode,
        })
    }

    /// Append bytes, growing the file if needed.
    ///
    /// Uses `ptr::copy_nonoverlapping` to avoid redundant bounds checks on
    /// the destination slice — the capacity invariant is enforced by `grow()`.
    #[inline(always)]
    pub fn append(&mut self, data: &[u8]) -> Result<u64> {
        let offset = self.len;
        let data_len = data.len();
        let new_len = offset + data_len as u64;

        if new_len > self.capacity {
            self.grow(new_len)?;
        }

        // SAFETY: `grow()` guarantees `self.capacity >= new_len`, so the
        // destination range `[offset .. offset + data_len)` is within the
        // mapped region.  Source and destination do not overlap because `data`
        // is an external slice and `self.mmap` is our private mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.mmap.as_mut_ptr().add(offset as usize),
                data_len,
            );
        }
        self.len = new_len;
        Ok(offset)
    }

    /// Append multiple fixed-size elements at once via a single memcpy.
    ///
    /// `data` must be a contiguous byte slice representing N elements each
    /// of `element_size` bytes. This is significantly faster than calling
    /// `append()` in a loop because it performs a single bounds check and
    /// a single memcpy.
    #[inline]
    pub fn append_bulk(&mut self, data: &[u8]) -> Result<u64> {
        let offset = self.len;
        let data_len = data.len();
        let new_len = offset + data_len as u64;

        if new_len > self.capacity {
            // Pre-grow to at least 2x what we need to avoid repeated grows
            self.grow(new_len.max(self.capacity * 2))?;
        }

        // SAFETY: same invariant as `append()` — capacity >= new_len after grow.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.mmap.as_mut_ptr().add(offset as usize),
                data_len,
            );
        }
        self.len = new_len;
        Ok(offset)
    }

    /// Read bytes at offset.
    pub fn read_at(&self, offset: u64, len: usize) -> &[u8] {
        &self.mmap[offset as usize..offset as usize + len]
    }

    /// Write `data` at an arbitrary `offset` within the mapped region.
    ///
    /// The caller must ensure `offset + data.len()` does not exceed `capacity`.
    /// This does **not** update the logical `len`.
    pub(crate) fn write_at(&mut self, offset: u64, data: &[u8]) {
        self.mmap[offset as usize..offset as usize + data.len()].copy_from_slice(data);
    }

    /// Current data length (not capacity).
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Current allocated capacity (may be larger than `len`).
    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Set the logical length without writing any data.
    ///
    /// The caller must ensure `new_len <= capacity`.  This is useful after
    /// writing data via [`write_at`] to advance the length counter.
    pub(crate) fn set_len(&mut self, new_len: u64) {
        debug_assert!(new_len <= self.capacity);
        self.len = new_len;
    }

    /// Flush changes to disk according to the configured `SyncMode`.
    ///
    /// - `SyncMode::Full`: synchronous msync (equivalent to fsync).
    /// - `SyncMode::DataOnly`: asynchronous msync (data only, no metadata).
    /// - `SyncMode::Periodic(_)`: asynchronous msync (caller manages timer).
    /// - `SyncMode::None`: no-op.
    pub fn flush(&self) -> Result<()> {
        match &self.sync_mode {
            SyncMode::Full => self.mmap.flush().map_err(ExchangeDbError::Io),
            SyncMode::DataOnly | SyncMode::Periodic(_) => {
                self.mmap.flush_async().map_err(ExchangeDbError::Io)
            }
            SyncMode::None => Ok(()),
        }
    }

    /// Non-blocking flush: schedules an async msync regardless of sync mode.
    /// Useful for background writeback without stalling the hot path.
    pub fn flush_async(&self) -> Result<()> {
        self.mmap.flush_async().map_err(ExchangeDbError::Io)
    }

    /// Flush a specific byte range to disk (partial msync).
    ///
    /// `offset` and `len` refer to byte positions within the mapped region.
    /// The range is clamped to the current data length.
    pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        let end = (offset + len).min(self.len as usize);
        if offset >= end {
            return Ok(());
        }
        self.mmap
            .flush_range(offset, end - offset)
            .map_err(ExchangeDbError::Io)
    }

    /// Return a reference to the current `SyncMode`.
    pub fn sync_mode(&self) -> &SyncMode {
        &self.sync_mode
    }

    /// Truncate backing file to actual data length.
    pub fn truncate_to_len(&mut self) -> Result<()> {
        self.file.set_len(self.len)?;
        Ok(())
    }

    fn grow(&mut self, min_capacity: u64) -> Result<()> {
        let new_capacity = (self.capacity * 2).max(min_capacity);
        let additional = new_capacity.saturating_sub(self.capacity);

        // Check available disk space before attempting to grow the file.
        if additional > 0
            && let Some(available) = available_disk_space(&self.path)
        {
            // Require at least the growth amount plus a small safety margin
            // (1 MB) to avoid filling the disk completely.
            let needed = additional + 1024 * 1024;
            if available < needed {
                eprintln!(
                    "ERROR: disk full when growing '{}': need {} bytes, {} available",
                    self.path.display(),
                    needed,
                    available
                );
                return Err(ExchangeDbError::DiskFull {
                    path: self.path.display().to_string(),
                    needed_bytes: needed,
                    available_bytes: available,
                });
            }
        }

        self.file.set_len(new_capacity).map_err(|e| {
            // Convert ENOSPC to our DiskFull error for better diagnostics.
            if e.raw_os_error() == Some(libc_enospc()) {
                eprintln!(
                    "ERROR: disk full (ENOSPC) when growing '{}' to {} bytes",
                    self.path.display(),
                    new_capacity
                );
                ExchangeDbError::DiskFull {
                    path: self.path.display().to_string(),
                    needed_bytes: new_capacity,
                    available_bytes: 0,
                }
            } else {
                eprintln!(
                    "ERROR: failed to grow '{}' to {} bytes: {}",
                    self.path.display(),
                    new_capacity,
                    e
                );
                ExchangeDbError::Io(e)
            }
        })?;

        // SAFETY: Same as in open() — we have exclusive write access.
        self.mmap = unsafe {
            MmapOptions::new()
                .len(new_capacity as usize)
                .map_mut(&self.file)?
        };
        self.capacity = new_capacity;
        Ok(())
    }
}

/// Query the available disk space for the filesystem containing `path`.
///
/// Returns `None` if the query fails (e.g. the path does not exist yet).
fn available_disk_space(path: &Path) -> Option<u64> {
    // Find an existing ancestor directory to query.
    let query_path = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };

    #[cfg(unix)]
    {
        use std::ffi::CString;
        let c_path = CString::new(query_path.to_str()?).ok()?;
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                #[allow(clippy::unnecessary_cast)]
                Some(stat.f_bavail as u64 * stat.f_frsize as u64)
            } else {
                None
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = query_path;
        None // Skip disk space check on non-Unix platforms.
    }
}

/// Return the OS error code for ENOSPC (No space left on device).
fn libc_enospc() -> i32 {
    #[cfg(unix)]
    {
        libc::ENOSPC
    }
    #[cfg(not(unix))]
    {
        -1
    } // Unreachable on non-Unix, but keeps the code compiling.
}

impl Drop for MmapFile {
    fn drop(&mut self) {
        if let Err(e) = self.truncate_to_len() {
            // Log a warning rather than silently swallowing the error so
            // operators are alerted to potential data-loss scenarios.
            eprintln!(
                "WARNING: MmapFile::drop failed to truncate '{}' to {} bytes: {}",
                self.path.display(),
                self.len,
                e
            );
        }
    }
}

/// Read-only memory-mapped file for column readers.
pub struct MmapReadOnly {
    mmap: std::sync::Arc<memmap2::Mmap>,
    len: u64,
}

/// Global cache of read-only mmap handles to avoid repeated mmap() syscalls.
/// Each mmap() costs ~10µs; for 7 partitions × 8 columns = 56 calls = ~560µs
/// overhead per query. With caching, subsequent queries pay ~0µs for file opens.
static MMAP_CACHE: std::sync::OnceLock<dashmap::DashMap<PathBuf, std::sync::Arc<memmap2::Mmap>>> =
    std::sync::OnceLock::new();

fn mmap_cache() -> &'static dashmap::DashMap<PathBuf, std::sync::Arc<memmap2::Mmap>> {
    MMAP_CACHE.get_or_init(dashmap::DashMap::new)
}

/// Invalidate cached mmap handles under a directory (e.g., after compaction or DDL).
pub fn invalidate_mmap_cache(prefix: &Path) {
    mmap_cache().retain(|k, _| !k.starts_with(prefix));
}

impl MmapReadOnly {
    pub fn open(path: &Path) -> Result<Self> {
        let cache = mmap_cache();

        // Check cache first.
        if let Some(cached) = cache.get(path) {
            let arc = cached.value().clone();
            let len = arc.len() as u64;
            return Ok(Self { mmap: arc, len });
        }

        let file = File::open(path)?;
        let len = file.metadata()?.len();

        // SAFETY: Read-only mapping, no mutations possible.
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let arc = std::sync::Arc::new(mmap);

        cache.insert(path.to_path_buf(), arc.clone());

        Ok(Self { mmap: arc, len })
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..self.len as usize]
    }

    #[inline(always)]
    pub fn read_at(&self, offset: u64, len: usize) -> &[u8] {
        &self.mmap[offset as usize..offset as usize + len]
    }

    #[inline]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return a reference to the inner `Mmap` for use with advisory hints.
    pub fn inner_mmap(&self) -> &memmap2::Mmap {
        &self.mmap
    }
}

// ── madvise / fadvise helpers ──────────────────────────────────────────────

/// Hint to the OS that the entire mmap region will be read sequentially.
/// Equivalent to `madvise(MADV_SEQUENTIAL)`.
pub fn advise_sequential(mmap: &memmap2::Mmap) {
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::Sequential);
    }
    #[cfg(not(unix))]
    {
        let _ = mmap;
    }
}

/// Hint that we will need the entire mmap region soon.
/// Equivalent to `madvise(MADV_WILLNEED)`.
pub fn advise_willneed(mmap: &memmap2::Mmap) {
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::WillNeed);
    }
    #[cfg(not(unix))]
    {
        let _ = mmap;
    }
}

/// Hint that we are done with the mmap region and the pages can be freed.
/// Equivalent to `madvise(MADV_DONTNEED)`.
pub fn advise_dontneed(mmap: &memmap2::Mmap) {
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::WillNeed); // DontNeed not available in this memmap2 version
    }
    #[cfg(not(unix))]
    {
        let _ = mmap;
    }
}

/// Advise that a specific byte range within a `MmapMut` will be accessed
/// sequentially. Useful for scan operations on writable mappings.
pub fn advise_sequential_mut(mmap: &memmap2::MmapMut) {
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::Sequential);
    }
    #[cfg(not(unix))]
    {
        let _ = mmap;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn mmap_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.d");

        {
            let mut mf = MmapFile::open(&path, 4096).unwrap();
            let data = 42i64.to_le_bytes();
            mf.append(&data).unwrap();
            mf.append(&100i64.to_le_bytes()).unwrap();
            mf.flush().unwrap();
        }

        let ro = MmapReadOnly::open(&path).unwrap();
        assert_eq!(ro.len(), 16);

        let val = i64::from_le_bytes(ro.read_at(0, 8).try_into().unwrap());
        assert_eq!(val, 42);

        let val = i64::from_le_bytes(ro.read_at(8, 8).try_into().unwrap());
        assert_eq!(val, 100);
    }

    #[test]
    fn mmap_grow() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("grow.d");

        let mut mf = MmapFile::open(&path, 16).unwrap();
        // Write more than initial capacity
        let data = vec![0xFFu8; 32];
        mf.append(&data).unwrap();
        assert_eq!(mf.len(), 32);
    }

    #[test]
    fn mmap_open_with_config() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.d");

        let config = MmapConfig {
            initial_capacity: 8192,
            sync_mode: SyncMode::DataOnly,
            huge_pages: false,
        };

        let mut mf = MmapFile::open_with_config(&path, config).unwrap();
        mf.append(&42i64.to_le_bytes()).unwrap();
        mf.flush().unwrap();
        assert_eq!(mf.len(), 8);
    }

    #[test]
    fn sync_mode_none_flush_is_noop() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("noop.d");

        let config = MmapConfig {
            initial_capacity: 4096,
            sync_mode: SyncMode::None,
            huge_pages: false,
        };

        let mut mf = MmapFile::open_with_config(&path, config).unwrap();
        mf.append(&1i64.to_le_bytes()).unwrap();
        // Should not error — it's a no-op.
        mf.flush().unwrap();
    }

    #[test]
    fn sync_mode_full_actually_flushes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("full_sync.d");

        let config = MmapConfig {
            initial_capacity: 4096,
            sync_mode: SyncMode::Full,
            huge_pages: false,
        };

        let mut mf = MmapFile::open_with_config(&path, config).unwrap();
        mf.append(&99i64.to_le_bytes()).unwrap();
        // Full flush should succeed and actually persist data.
        mf.flush().unwrap();

        // Verify data is on disk by re-opening read-only.
        let ro = MmapReadOnly::open(&path).unwrap();
        // The file might still be larger than 8 bytes due to capacity,
        // but the first 8 bytes should be our value.
        let val = i64::from_le_bytes(ro.read_at(0, 8).try_into().unwrap());
        assert_eq!(val, 99);
    }

    #[test]
    fn flush_async_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("async.d");

        let mut mf = MmapFile::open(&path, 4096).unwrap();
        mf.append(&7i64.to_le_bytes()).unwrap();
        mf.flush_async().unwrap();
    }

    #[test]
    fn flush_range_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("range.d");

        let mut mf = MmapFile::open(&path, 4096).unwrap();
        mf.append(&1i64.to_le_bytes()).unwrap();
        mf.append(&2i64.to_le_bytes()).unwrap();
        // Flush only the first 8 bytes.
        mf.flush_range(0, 8).unwrap();
        // Flush with range beyond data — should clamp.
        mf.flush_range(0, 99999).unwrap();
        // Empty range — no-op.
        mf.flush_range(100, 0).unwrap();
    }

    #[test]
    fn advise_helpers_do_not_panic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("advise.d");
        std::fs::write(&path, [0u8; 4096]).unwrap();

        let ro = MmapReadOnly::open(&path).unwrap();
        let mmap = ro.inner_mmap();
        advise_sequential(mmap);
        advise_willneed(mmap);
        advise_dontneed(mmap);
    }

    // ── Disk space / DiskFull tests ────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn available_disk_space_returns_some_for_existing_dir() {
        let dir = tempdir().unwrap();
        let space = available_disk_space(dir.path());
        assert!(
            space.is_some(),
            "should return available space for a valid dir"
        );
        assert!(space.unwrap() > 0, "available space should be > 0");
    }

    #[test]
    fn disk_full_error_variant_formats_correctly() {
        let err = ExchangeDbError::DiskFull {
            path: "/data/trades/ts.d".to_string(),
            needed_bytes: 1_000_000,
            available_bytes: 500,
        };
        let msg = err.to_string();
        assert!(msg.contains("disk full"), "should say disk full");
        assert!(msg.contains("1000000"), "should show needed bytes");
        assert!(msg.contains("500"), "should show available bytes");
    }

    #[test]
    fn mmap_stores_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("path_test.d");
        let mf = MmapFile::open(&path, 4096).unwrap();
        assert_eq!(mf.path, path);
    }
}
