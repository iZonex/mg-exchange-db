use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use exchange_common::error::{ExchangeDbError, Result};

use crate::mmap::MmapFile;

// ---------------------------------------------------------------------------
// TxnFile – persistent transaction metadata stored in `_txn`
// ---------------------------------------------------------------------------

/// On-disk layout (all little-endian):
///
/// ```text
/// offset  size   field
/// ------  -----  -----
///  0       8     version          (u64)
///  8       8     row_count        (u64)
/// 16       8     min_timestamp    (i64)
/// 24       8     max_timestamp    (i64)
/// 32       4     partition_count  (u32)
/// 36       ..    partition entries (20 bytes each)
/// ```
///
/// Each partition entry:
/// ```text
///  0       8     timestamp   (i64)  – partition key timestamp
///  8       8     row_count   (u64)
/// 16       4     name_offset (u32)  – byte offset into a name table (future)
/// ```

const TXN_HEADER_SIZE: usize = 36;
const PARTITION_ENTRY_SIZE: usize = 20;
const TXN_INITIAL_CAPACITY: u64 = 4096;

/// A single partition entry as stored in the `_txn` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionEntry {
    pub timestamp: i64,
    pub row_count: u64,
    pub name_offset: u32,
}

/// Snapshot of the transaction header (fixed fields only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnHeader {
    pub version: u64,
    pub row_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub partition_count: u32,
}

/// Reads and writes the `_txn` file that records committed transaction state.
pub struct TxnFile {
    mmap: MmapFile,
}

impl TxnFile {
    /// Open (or create) the `_txn` file at the given directory.
    pub fn open(dir: &Path) -> Result<Self> {
        let path = dir.join("_txn");
        let mmap = MmapFile::open(&path, TXN_INITIAL_CAPACITY)?;

        let mut txn_file = Self { mmap };

        // If the file was freshly created (all zeros), the header fields are
        // already zero which is a valid initial state (version 0, no rows, no
        // partitions).  We only need to make sure `min_timestamp` / `max_timestamp`
        // are set to sensible sentinel values when there are no rows.
        if txn_file.mmap.len() == 0 || txn_file.read_header().version == 0 {
            // First open – write a clean header.
            txn_file.write_header(&TxnHeader {
                version: 0,
                row_count: 0,
                min_timestamp: i64::MAX,
                max_timestamp: i64::MIN,
                partition_count: 0,
            })?;
        }

        Ok(txn_file)
    }

    /// Read the fixed-size header.
    pub fn read_header(&self) -> TxnHeader {
        let buf = self.mmap.read_at(0, TXN_HEADER_SIZE);
        TxnHeader {
            version: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            row_count: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            min_timestamp: i64::from_le_bytes(buf[16..24].try_into().unwrap()),
            max_timestamp: i64::from_le_bytes(buf[24..32].try_into().unwrap()),
            partition_count: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
        }
    }

    /// Persist a new header (without partition entries) to disk.
    pub fn write_header(&mut self, hdr: &TxnHeader) -> Result<()> {
        let needed = TXN_HEADER_SIZE as u64
            + (hdr.partition_count as u64) * (PARTITION_ENTRY_SIZE as u64);

        self.ensure_capacity(needed)?;

        let hdr_bytes = Self::encode_header(hdr);
        self.write_at(0, &hdr_bytes);
        self.ensure_len(needed);

        self.mmap.flush()?;
        Ok(())
    }

    /// Read partition entry at `index`.
    pub fn read_partition(&self, index: u32) -> PartitionEntry {
        let offset = TXN_HEADER_SIZE + index as usize * PARTITION_ENTRY_SIZE;
        let buf = self.mmap.read_at(offset as u64, PARTITION_ENTRY_SIZE);
        PartitionEntry {
            timestamp: i64::from_le_bytes(buf[0..8].try_into().unwrap()),
            row_count: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            name_offset: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        }
    }

    /// Read all partition entries.
    pub fn read_partitions(&self) -> Vec<PartitionEntry> {
        let hdr = self.read_header();
        (0..hdr.partition_count)
            .map(|i| self.read_partition(i))
            .collect()
    }

    /// Write a single partition entry at `index`.  Caller must ensure index < partition_count.
    pub fn write_partition(&mut self, index: u32, entry: &PartitionEntry) -> Result<()> {
        let offset = TXN_HEADER_SIZE + index as usize * PARTITION_ENTRY_SIZE;
        let bytes = Self::encode_partition(entry);

        let needed = (offset + PARTITION_ENTRY_SIZE) as u64;
        self.ensure_capacity(needed)?;
        self.write_at(offset, &bytes);
        self.ensure_len(needed);
        Ok(())
    }

    /// Write the full transaction: header + all partitions in one shot.
    pub fn commit(&mut self, hdr: &TxnHeader, partitions: &[PartitionEntry]) -> Result<()> {
        assert_eq!(hdr.partition_count as usize, partitions.len());

        let total_size =
            TXN_HEADER_SIZE as u64 + partitions.len() as u64 * PARTITION_ENTRY_SIZE as u64;

        self.ensure_capacity(total_size)?;

        let hdr_bytes = Self::encode_header(hdr);
        self.write_at(0, &hdr_bytes);

        for (i, part) in partitions.iter().enumerate() {
            let offset = TXN_HEADER_SIZE + i * PARTITION_ENTRY_SIZE;
            let bytes = Self::encode_partition(part);
            self.write_at(offset, &bytes);
        }

        self.ensure_len(total_size);
        self.mmap.flush()?;
        Ok(())
    }

    // -- private helpers ----------------------------------------------------

    fn encode_header(hdr: &TxnHeader) -> [u8; TXN_HEADER_SIZE] {
        let mut buf = [0u8; TXN_HEADER_SIZE];
        buf[0..8].copy_from_slice(&hdr.version.to_le_bytes());
        buf[8..16].copy_from_slice(&hdr.row_count.to_le_bytes());
        buf[16..24].copy_from_slice(&hdr.min_timestamp.to_le_bytes());
        buf[24..32].copy_from_slice(&hdr.max_timestamp.to_le_bytes());
        buf[32..36].copy_from_slice(&hdr.partition_count.to_le_bytes());
        buf
    }

    fn encode_partition(entry: &PartitionEntry) -> [u8; PARTITION_ENTRY_SIZE] {
        let mut buf = [0u8; PARTITION_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&entry.timestamp.to_le_bytes());
        buf[8..16].copy_from_slice(&entry.row_count.to_le_bytes());
        buf[16..20].copy_from_slice(&entry.name_offset.to_le_bytes());
        buf
    }

    /// Grow the backing mmap so that `capacity >= min_capacity`.
    fn ensure_capacity(&mut self, min_capacity: u64) -> Result<()> {
        if min_capacity > self.mmap.capacity() {
            // `append` triggers a grow when the new length exceeds capacity.
            let current_len = self.mmap.len();
            let grow_by = min_capacity.saturating_sub(current_len);
            if grow_by > 0 {
                let padding = vec![0u8; grow_by as usize];
                self.mmap.append(&padding)?;
            }
        }
        Ok(())
    }

    /// Write bytes at an arbitrary offset within the mmap.
    fn write_at(&mut self, offset: usize, data: &[u8]) {
        self.mmap.write_at(offset as u64, data);
    }

    /// Ensure the logical length of the mmap file is at least `min_len`.
    ///
    /// After writing data via `write_at`, the mmap's internal `len` field
    /// may be stale.  This advances it without overwriting any data.
    fn ensure_len(&mut self, min_len: u64) {
        if self.mmap.len() < min_len {
            self.mmap.set_len(min_len);
        }
    }
}

// ---------------------------------------------------------------------------
// Scoreboard – lock-free reader tracking for MVCC-like isolation
// ---------------------------------------------------------------------------

/// Number of concurrent reader slots.
const SCOREBOARD_CAPACITY: usize = 256;

/// Sentinel indicating an empty (unused) scoreboard slot.
const SLOT_EMPTY: u64 = u64::MAX;

/// Cache-line size on most modern x86-64 / ARM64 CPUs.
#[allow(dead_code)]
const CACHE_LINE: usize = 64;

/// Opaque handle returned by [`Scoreboard::acquire`] that identifies a reader slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderId(u32);

impl ReaderId {
    /// Return the raw slot index (useful for testing / debugging).
    pub fn slot(self) -> u32 {
        self.0
    }
}

/// A cache-line-padded atomic u64 to avoid false sharing between cores.
#[repr(align(64))]
struct PaddedAtomicU64 {
    value: AtomicU64,
}

impl PaddedAtomicU64 {
    const fn new(v: u64) -> Self {
        Self {
            value: AtomicU64::new(v),
        }
    }
}

/// Lock-free scoreboard tracking which transaction versions are still being read.
///
/// Each slot stores either [`SLOT_EMPTY`] (available) or the `version` at which a
/// reader started.  Readers call [`acquire`] to claim a slot and [`release`] to free
/// it.  Writers call [`min_active_version`] to determine the oldest version that is
/// still in use so that old data is not garbage-collected prematurely.
pub struct Scoreboard {
    slots: Box<[PaddedAtomicU64; SCOREBOARD_CAPACITY]>,
}

// `Scoreboard` is safe to share across threads — all mutation goes through atomics.
unsafe impl Sync for Scoreboard {}
unsafe impl Send for Scoreboard {}

impl Scoreboard {
    /// Create a new scoreboard with all slots empty.
    pub fn new() -> Self {
        // We cannot use array init syntax with const generics for Box, so we
        // use `Box::new` with a fixed-size array initializer.
        let slots: Box<[PaddedAtomicU64; SCOREBOARD_CAPACITY]> = {
            // SAFETY: PaddedAtomicU64 is a repr(align(64)) wrapper around
            // AtomicU64 which is valid for any bit pattern. We initialise
            // every element to SLOT_EMPTY.
            let mut v: Vec<PaddedAtomicU64> = Vec::with_capacity(SCOREBOARD_CAPACITY);
            for _ in 0..SCOREBOARD_CAPACITY {
                v.push(PaddedAtomicU64::new(SLOT_EMPTY));
            }
            // Convert Vec -> Box<[_; N]>.
            let boxed_slice = v.into_boxed_slice();
            // SAFETY: We pushed exactly SCOREBOARD_CAPACITY elements.
            unsafe {
                let raw = Box::into_raw(boxed_slice) as *mut [PaddedAtomicU64; SCOREBOARD_CAPACITY];
                Box::from_raw(raw)
            }
        };
        Self { slots }
    }

    /// The fixed capacity of the scoreboard.
    pub fn capacity(&self) -> usize {
        SCOREBOARD_CAPACITY
    }

    /// Register a reader at the given transaction `version`.
    ///
    /// Returns `Ok(ReaderId)` on success or an error if all slots are occupied.
    pub fn acquire(&self, txn_version: u64) -> Result<ReaderId> {
        debug_assert_ne!(txn_version, SLOT_EMPTY, "txn_version must not be u64::MAX");

        for i in 0..SCOREBOARD_CAPACITY {
            // Try to CAS from EMPTY -> txn_version.
            if self.slots[i]
                .value
                .compare_exchange(SLOT_EMPTY, txn_version, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(ReaderId(i as u32));
            }
        }
        Err(ExchangeDbError::TxnConflict(
            "scoreboard full: too many concurrent readers".into(),
        ))
    }

    /// Release a previously acquired reader slot.
    pub fn release(&self, reader_id: ReaderId) {
        let idx = reader_id.0 as usize;
        debug_assert!(idx < SCOREBOARD_CAPACITY);
        self.slots[idx].value.store(SLOT_EMPTY, Ordering::Release);
    }

    /// Return the minimum transaction version still held by any active reader.
    ///
    /// Returns `u64::MAX` if there are no active readers.
    pub fn min_active_version(&self) -> u64 {
        let mut min = u64::MAX;
        for i in 0..SCOREBOARD_CAPACITY {
            let v = self.slots[i].value.load(Ordering::Acquire);
            if v != SLOT_EMPTY && v < min {
                min = v;
            }
        }
        min
    }

    /// Number of currently active readers (mostly for diagnostics / tests).
    pub fn active_count(&self) -> usize {
        let mut count = 0;
        for i in 0..SCOREBOARD_CAPACITY {
            if self.slots[i].value.load(Ordering::Relaxed) != SLOT_EMPTY {
                count += 1;
            }
        }
        count
    }
}

impl Default for Scoreboard {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TxnManager – coordinate read snapshots and write commits
// ---------------------------------------------------------------------------

/// Coordinates read and write transactions.
///
/// * Reads are non-blocking snapshots at the current committed version.
/// * Writes are serialised via a mutex and atomically bump the version.
pub struct TxnManager {
    /// Path to the table directory containing `_txn`.
    _dir: std::path::PathBuf,
    /// The persistent transaction file.
    txn_file: Mutex<TxnFile>,
    /// Current committed version (mirrors `TxnFile.header.version`).
    version: AtomicU64,
    /// Reader scoreboard.
    scoreboard: Arc<Scoreboard>,
}

impl TxnManager {
    /// Open (or create) a transaction manager rooted at `dir`.
    pub fn open(dir: &Path) -> Result<Self> {
        let txn_file = TxnFile::open(dir)?;
        let hdr = txn_file.read_header();
        let version = AtomicU64::new(hdr.version);

        Ok(Self {
            _dir: dir.to_path_buf(),
            txn_file: Mutex::new(txn_file),
            version,
            scoreboard: Arc::new(Scoreboard::new()),
        })
    }

    /// Start a read transaction at the current committed version.
    pub fn begin_read(&self) -> Result<ReadTxn> {
        // Read version and header atomically under the txn file lock to avoid
        // a race where a concurrent commit bumps the version between our
        // atomic load and the header read.
        let (version, header) = {
            let txn_file = self.txn_file.lock().unwrap();
            let hdr = txn_file.read_header();
            (hdr.version, hdr)
        };

        let reader_id = self.scoreboard.acquire(version)?;

        Ok(ReadTxn {
            version,
            header,
            reader_id,
            scoreboard: Arc::clone(&self.scoreboard),
        })
    }

    /// Commit a write transaction.
    ///
    /// This bumps the version, updates row counts and timestamp bounds, and
    /// persists the new state to the `_txn` file.
    ///
    /// `partitions` is the full list of partition entries *after* the write.
    pub fn commit_write(
        &self,
        new_row_count: u64,
        min_ts: i64,
        max_ts: i64,
        partitions: &[PartitionEntry],
    ) -> Result<u64> {
        let mut txn_file = self.txn_file.lock().unwrap();
        let current = txn_file.read_header();
        let new_version = current.version + 1;

        let hdr = TxnHeader {
            version: new_version,
            row_count: new_row_count,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            partition_count: partitions.len() as u32,
        };

        txn_file.commit(&hdr, partitions)?;

        // Publish the new version so subsequent readers see it.
        self.version.store(new_version, Ordering::Release);

        Ok(new_version)
    }

    /// Convenience: commit without changing partitions (just update counts/timestamps).
    pub fn commit_write_simple(
        &self,
        new_row_count: u64,
        min_ts: i64,
        max_ts: i64,
    ) -> Result<u64> {
        let mut txn_file = self.txn_file.lock().unwrap();
        let current = txn_file.read_header();
        let new_version = current.version + 1;

        // Merge timestamp bounds.
        let merged_min = if current.row_count == 0 {
            min_ts
        } else {
            current.min_timestamp.min(min_ts)
        };
        let merged_max = if current.row_count == 0 {
            max_ts
        } else {
            current.max_timestamp.max(max_ts)
        };

        let partitions = txn_file.read_partitions();

        let hdr = TxnHeader {
            version: new_version,
            row_count: new_row_count,
            min_timestamp: merged_min,
            max_timestamp: merged_max,
            partition_count: partitions.len() as u32,
        };

        txn_file.commit(&hdr, &partitions)?;
        self.version.store(new_version, Ordering::Release);
        Ok(new_version)
    }

    /// Current committed version (non-blocking read).
    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Reference to the scoreboard (e.g. for GC decisions).
    pub fn scoreboard(&self) -> &Scoreboard {
        &self.scoreboard
    }

    /// Read the current header (acquires the txn file lock briefly).
    pub fn read_header(&self) -> TxnHeader {
        let txn_file = self.txn_file.lock().unwrap();
        txn_file.read_header()
    }
}

// ---------------------------------------------------------------------------
// ReadTxn – snapshot read handle
// ---------------------------------------------------------------------------

/// A read transaction that holds a scoreboard slot for the duration of its
/// lifetime.  Dropping the `ReadTxn` automatically releases the slot.
pub struct ReadTxn {
    version: u64,
    header: TxnHeader,
    reader_id: ReaderId,
    scoreboard: Arc<Scoreboard>,
}

impl ReadTxn {
    /// Transaction version this read is pinned to.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Snapshot of the transaction header at the time the read began.
    pub fn header(&self) -> &TxnHeader {
        &self.header
    }

    /// The reader slot id.
    pub fn reader_id(&self) -> ReaderId {
        self.reader_id
    }
}

impl Drop for ReadTxn {
    fn drop(&mut self) {
        self.scoreboard.release(self.reader_id);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use tempfile::tempdir;

    // -- TxnFile tests ------------------------------------------------------

    #[test]
    fn txn_file_create_and_read_empty() {
        let dir = tempdir().unwrap();
        let txn = TxnFile::open(dir.path()).unwrap();
        let hdr = txn.read_header();
        assert_eq!(hdr.version, 0);
        assert_eq!(hdr.row_count, 0);
        assert_eq!(hdr.min_timestamp, i64::MAX);
        assert_eq!(hdr.max_timestamp, i64::MIN);
        assert_eq!(hdr.partition_count, 0);
    }

    #[test]
    fn txn_file_write_and_read_header() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();

        let hdr = TxnHeader {
            version: 42,
            row_count: 1000,
            min_timestamp: 100,
            max_timestamp: 999,
            partition_count: 0,
        };
        txn.write_header(&hdr).unwrap();

        let read_back = txn.read_header();
        assert_eq!(read_back, hdr);
    }

    #[test]
    fn txn_file_commit_with_partitions() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();

        let partitions = vec![
            PartitionEntry {
                timestamp: 1000,
                row_count: 500,
                name_offset: 0,
            },
            PartitionEntry {
                timestamp: 2000,
                row_count: 300,
                name_offset: 20,
            },
            PartitionEntry {
                timestamp: 3000,
                row_count: 200,
                name_offset: 40,
            },
        ];

        let hdr = TxnHeader {
            version: 1,
            row_count: 1000,
            min_timestamp: 1000,
            max_timestamp: 3000,
            partition_count: 3,
        };

        txn.commit(&hdr, &partitions).unwrap();

        // Read back.
        let read_hdr = txn.read_header();
        assert_eq!(read_hdr, hdr);

        let read_parts = txn.read_partitions();
        assert_eq!(read_parts.len(), 3);
        assert_eq!(read_parts[0], partitions[0]);
        assert_eq!(read_parts[1], partitions[1]);
        assert_eq!(read_parts[2], partitions[2]);
    }

    #[test]
    fn txn_file_reopen_persists() {
        let dir = tempdir().unwrap();

        let hdr = TxnHeader {
            version: 7,
            row_count: 42,
            min_timestamp: -100,
            max_timestamp: 200,
            partition_count: 1,
        };
        let parts = vec![PartitionEntry {
            timestamp: -100,
            row_count: 42,
            name_offset: 0,
        }];

        {
            let mut txn = TxnFile::open(dir.path()).unwrap();
            txn.commit(&hdr, &parts).unwrap();
        }

        // Reopen and verify.
        let txn = TxnFile::open(dir.path()).unwrap();
        let read_hdr = txn.read_header();
        assert_eq!(read_hdr.version, 7);
        assert_eq!(read_hdr.row_count, 42);
        let read_parts = txn.read_partitions();
        assert_eq!(read_parts.len(), 1);
        assert_eq!(read_parts[0].timestamp, -100);
    }

    #[test]
    fn txn_file_overwrite_reduces_partitions() {
        let dir = tempdir().unwrap();
        let mut txn = TxnFile::open(dir.path()).unwrap();

        // Write 3 partitions.
        let parts3 = vec![
            PartitionEntry { timestamp: 1, row_count: 1, name_offset: 0 },
            PartitionEntry { timestamp: 2, row_count: 2, name_offset: 0 },
            PartitionEntry { timestamp: 3, row_count: 3, name_offset: 0 },
        ];
        let hdr3 = TxnHeader {
            version: 1, row_count: 6, min_timestamp: 1, max_timestamp: 3, partition_count: 3,
        };
        txn.commit(&hdr3, &parts3).unwrap();
        assert_eq!(txn.read_partitions().len(), 3);

        // Overwrite with 1 partition.
        let parts1 = vec![
            PartitionEntry { timestamp: 10, row_count: 10, name_offset: 0 },
        ];
        let hdr1 = TxnHeader {
            version: 2, row_count: 10, min_timestamp: 10, max_timestamp: 10, partition_count: 1,
        };
        txn.commit(&hdr1, &parts1).unwrap();
        let read = txn.read_partitions();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].timestamp, 10);
    }

    // -- Scoreboard tests ---------------------------------------------------

    #[test]
    fn scoreboard_acquire_release() {
        let sb = Scoreboard::new();
        assert_eq!(sb.active_count(), 0);
        assert_eq!(sb.min_active_version(), u64::MAX);

        let r1 = sb.acquire(10).unwrap();
        assert_eq!(sb.active_count(), 1);
        assert_eq!(sb.min_active_version(), 10);

        let r2 = sb.acquire(20).unwrap();
        assert_eq!(sb.active_count(), 2);
        assert_eq!(sb.min_active_version(), 10);

        sb.release(r1);
        assert_eq!(sb.active_count(), 1);
        assert_eq!(sb.min_active_version(), 20);

        sb.release(r2);
        assert_eq!(sb.active_count(), 0);
        assert_eq!(sb.min_active_version(), u64::MAX);
    }

    #[test]
    fn scoreboard_slot_reuse() {
        let sb = Scoreboard::new();
        let r1 = sb.acquire(5).unwrap();
        let slot = r1.slot();
        sb.release(r1);

        // The same slot should be available again.
        let r2 = sb.acquire(6).unwrap();
        assert_eq!(r2.slot(), slot);
        sb.release(r2);
    }

    #[test]
    fn scoreboard_full() {
        let sb = Scoreboard::new();
        let mut readers = Vec::new();
        for i in 0..SCOREBOARD_CAPACITY {
            readers.push(sb.acquire(i as u64).unwrap());
        }
        assert_eq!(sb.active_count(), SCOREBOARD_CAPACITY);

        // Next acquire should fail.
        let result = sb.acquire(999);
        assert!(result.is_err());

        // Release one and retry.
        sb.release(readers.pop().unwrap());
        let r = sb.acquire(999);
        assert!(r.is_ok());
        sb.release(r.unwrap());

        for r in readers {
            sb.release(r);
        }
    }

    #[test]
    fn scoreboard_concurrent_acquire_release() {
        let sb = Arc::new(Scoreboard::new());
        let num_threads = 16;
        let iterations = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let sb = Arc::clone(&sb);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..iterations {
                        let version = (t * iterations + i) as u64;
                        let rid = sb.acquire(version).unwrap();
                        // Simulate some work.
                        std::hint::black_box(sb.min_active_version());
                        sb.release(rid);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(sb.active_count(), 0);
    }

    #[test]
    fn scoreboard_min_version_concurrent() {
        // Ensure min_active_version is always <= any active version.
        let sb = Arc::new(Scoreboard::new());
        let barrier = Arc::new(Barrier::new(8));

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let sb = Arc::clone(&sb);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..500 {
                        let version = 100 + t as u64;
                        let rid = sb.acquire(version).unwrap();
                        let min = sb.min_active_version();
                        assert!(min <= version, "min {min} > version {version}");
                        sb.release(rid);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn scoreboard_cache_line_padding() {
        // Verify the struct is properly padded.
        assert!(
            std::mem::size_of::<PaddedAtomicU64>() >= CACHE_LINE,
            "PaddedAtomicU64 should be at least {} bytes, got {}",
            CACHE_LINE,
            std::mem::size_of::<PaddedAtomicU64>()
        );
    }

    // -- TxnManager tests ---------------------------------------------------

    #[test]
    fn txn_manager_basic_flow() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();

        assert_eq!(mgr.current_version(), 0);

        // Start a read at version 0.
        let read_txn = mgr.begin_read().unwrap();
        assert_eq!(read_txn.version(), 0);
        assert_eq!(mgr.scoreboard().active_count(), 1);

        // Commit a write.
        let parts = vec![PartitionEntry {
            timestamp: 1000,
            row_count: 100,
            name_offset: 0,
        }];
        let v = mgr.commit_write(100, 1000, 2000, &parts).unwrap();
        assert_eq!(v, 1);
        assert_eq!(mgr.current_version(), 1);

        // The old read is still pinned at version 0.
        assert_eq!(read_txn.version(), 0);
        assert_eq!(mgr.scoreboard().min_active_version(), 0);

        // New read sees version 1.
        let read_txn2 = mgr.begin_read().unwrap();
        assert_eq!(read_txn2.version(), 1);
        assert_eq!(read_txn2.header().row_count, 100);

        drop(read_txn);
        assert_eq!(mgr.scoreboard().min_active_version(), 1);

        drop(read_txn2);
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn txn_manager_commit_simple() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();

        mgr.commit_write_simple(50, 100, 200).unwrap();
        let hdr = mgr.read_header();
        assert_eq!(hdr.version, 1);
        assert_eq!(hdr.row_count, 50);
        assert_eq!(hdr.min_timestamp, 100);
        assert_eq!(hdr.max_timestamp, 200);

        mgr.commit_write_simple(150, 50, 300).unwrap();
        let hdr = mgr.read_header();
        assert_eq!(hdr.version, 2);
        assert_eq!(hdr.row_count, 150);
        assert_eq!(hdr.min_timestamp, 50);
        assert_eq!(hdr.max_timestamp, 300);
    }

    #[test]
    fn txn_manager_read_txn_auto_release() {
        let dir = tempdir().unwrap();
        let mgr = TxnManager::open(dir.path()).unwrap();

        {
            let _r1 = mgr.begin_read().unwrap();
            let _r2 = mgr.begin_read().unwrap();
            assert_eq!(mgr.scoreboard().active_count(), 2);
        }
        // Both dropped.
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn txn_manager_concurrent_reads_and_writes() {
        let dir = tempdir().unwrap();
        let mgr = Arc::new(TxnManager::open(dir.path()).unwrap());
        let barrier = Arc::new(Barrier::new(4));

        // Writer thread.
        let mgr_w = Arc::clone(&mgr);
        let barrier_w = Arc::clone(&barrier);
        let writer = std::thread::spawn(move || {
            barrier_w.wait();
            for i in 1..=100u64 {
                mgr_w
                    .commit_write_simple(i * 10, i as i64, (i * 100) as i64)
                    .unwrap();
            }
        });

        // Reader threads.
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let mgr_r = Arc::clone(&mgr);
                let barrier_r = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier_r.wait();
                    for _ in 0..200 {
                        let rtx = mgr_r.begin_read().unwrap();
                        // The version should be consistent with the header.
                        let v = rtx.version();
                        let hdr = rtx.header();
                        assert_eq!(hdr.version, v);
                        drop(rtx);
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        assert_eq!(mgr.current_version(), 100);
        assert_eq!(mgr.scoreboard().active_count(), 0);
    }

    #[test]
    fn txn_manager_persists_across_reopen() {
        let dir = tempdir().unwrap();

        {
            let mgr = TxnManager::open(dir.path()).unwrap();
            let parts = vec![
                PartitionEntry { timestamp: 10, row_count: 5, name_offset: 0 },
                PartitionEntry { timestamp: 20, row_count: 7, name_offset: 10 },
            ];
            mgr.commit_write(12, 10, 20, &parts).unwrap();
        }

        // Reopen.
        let mgr = TxnManager::open(dir.path()).unwrap();
        assert_eq!(mgr.current_version(), 1);
        let hdr = mgr.read_header();
        assert_eq!(hdr.row_count, 12);
        assert_eq!(hdr.min_timestamp, 10);
        assert_eq!(hdr.max_timestamp, 20);
        assert_eq!(hdr.partition_count, 2);
    }
}
