//! Bitmap index: maps an i32 key to a sorted list of row IDs (u64).
//!
//! Inspired by QuestDB's bitmap indexes. Uses two files:
//! - `.k` (keys file): header + array of key entries, each pointing into the values file.
//! - `.v` (values file): linked list of blocks, each holding up to `block_capacity` row IDs.
//!
//! Layout:
//!
//! Keys file (.k):
//!   [header: 16 bytes]
//!     - key_count: u32         (number of distinct keys currently stored)
//!     - max_key: i32           (highest key value seen, or -1 if empty)
//!     - block_capacity: u32    (row IDs per value block)
//!     - reserved: u32
//!   [key entries: (max_key + 1) * 24 bytes]
//!     Each KeyEntry:
//!       - last_block_offset: u64   (offset in .v file of the tail block for this key)
//!       - count: u64               (total number of row IDs for this key)
//!       - first_block_offset: u64  (offset in .v file of the head block for this key)
//!
//! Values file (.v):
//!   Sequence of ValueBlock:
//!     - next_block_offset: u64     (0 if this is the tail)
//!     - count: u32                 (number of row IDs stored in this block)
//!     - reserved: u32
//!     - row_ids: [u64; block_capacity]

use crate::mmap::MmapFile;
use exchange_common::error::{ExchangeDbError, Result};
use std::path::Path;

const HEADER_SIZE: u64 = 16;
const KEY_ENTRY_SIZE: u64 = 24;
const VALUE_BLOCK_HEADER_SIZE: u64 = 16; // next_offset(8) + count(4) + reserved(4)
const DEFAULT_BLOCK_CAPACITY: u32 = 256;

/// A single key entry in the keys file.
#[derive(Debug, Clone, Copy)]
struct KeyEntry {
    last_block_offset: u64,
    count: u64,
    first_block_offset: u64,
}

/// Writer for a bitmap index. Appends row IDs for integer keys.
pub struct BitmapIndexWriter {
    keys: MmapFile,
    values: MmapFile,
    block_capacity: u32,
    max_key: i32,
    key_count: u32,
}

impl BitmapIndexWriter {
    /// Open or create a bitmap index at the given directory with the given name.
    /// Files created: `{dir}/{name}.k` and `{dir}/{name}.v`.
    pub fn open(dir: &Path, name: &str, block_capacity: u32) -> Result<Self> {
        let key_path = dir.join(format!("{}.k", name));
        let val_path = dir.join(format!("{}.v", name));

        let mut keys = MmapFile::open(&key_path, 4096)?;
        let mut values = MmapFile::open(&val_path, 64 * 1024)?;

        let (max_key, key_count, blk_cap) = if keys.len() >= HEADER_SIZE {
            // Read existing header.
            let hdr = keys.read_at(0, HEADER_SIZE as usize);
            let key_count = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let max_key = i32::from_le_bytes(hdr[4..8].try_into().unwrap());
            let blk_cap = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
            (max_key, key_count, blk_cap)
        } else {
            // Initialize header.
            let mut hdr = [0u8; HEADER_SIZE as usize];
            hdr[0..4].copy_from_slice(&0u32.to_le_bytes()); // key_count
            hdr[4..8].copy_from_slice(&(-1i32).to_le_bytes()); // max_key = -1
            hdr[8..12].copy_from_slice(&block_capacity.to_le_bytes());
            hdr[12..16].copy_from_slice(&0u32.to_le_bytes());
            keys.append(&hdr)?;
            // Reserve first 16 bytes in values file as unused (offset 0 means "no block").
            if values.is_empty() {
                values.append(&[0u8; VALUE_BLOCK_HEADER_SIZE as usize])?;
            }
            (-1i32, 0u32, block_capacity)
        };

        Ok(Self {
            keys,
            values,
            block_capacity: blk_cap,
            max_key,
            key_count,
        })
    }

    /// Open with the default block capacity (256).
    pub fn open_default(dir: &Path, name: &str) -> Result<Self> {
        Self::open(dir, name, DEFAULT_BLOCK_CAPACITY)
    }

    /// Add a row ID for the given key. Keys must be >= 0.
    pub fn add(&mut self, key: i32, row_id: u64) -> Result<()> {
        if key < 0 {
            return Err(ExchangeDbError::Corruption(
                "bitmap index key must be >= 0".into(),
            ));
        }

        // Ensure we have key entries up to `key`.
        if key > self.max_key {
            self.extend_keys(key)?;
        }

        let entry = self.read_key_entry(key);

        if entry.count == 0 {
            // First row ID for this key — allocate a new block.
            let block_offset = self.alloc_block()?;
            self.write_row_id_in_block(block_offset, 0, row_id);
            self.set_block_count(block_offset, 1);

            self.write_key_entry(
                key,
                KeyEntry {
                    last_block_offset: block_offset,
                    count: 1,
                    first_block_offset: block_offset,
                },
            );
            self.key_count += 1;
            self.write_header()?;
        } else {
            let last_offset = entry.last_block_offset;
            let block_count = self.read_block_count(last_offset);

            if block_count < self.block_capacity {
                // Room in current block.
                self.write_row_id_in_block(last_offset, block_count, row_id);
                self.set_block_count(last_offset, block_count + 1);
            } else {
                // Allocate new block and link from the old tail.
                let new_offset = self.alloc_block()?;
                self.write_row_id_in_block(new_offset, 0, row_id);
                self.set_block_count(new_offset, 1);

                // Link old tail -> new block.
                self.set_block_next(last_offset, new_offset);

                self.write_key_entry(
                    key,
                    KeyEntry {
                        last_block_offset: new_offset,
                        count: entry.count + 1,
                        first_block_offset: entry.first_block_offset,
                    },
                );
                // Count update below will be skipped, handle here and return early.
                return Ok(());
            }

            // Update total count in key entry.
            self.write_key_entry(
                key,
                KeyEntry {
                    last_block_offset: entry.last_block_offset,
                    count: entry.count + 1,
                    first_block_offset: entry.first_block_offset,
                },
            );
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.keys.flush()?;
        self.values.flush()
    }

    // ---- internal helpers ----

    fn extend_keys(&mut self, new_max_key: i32) -> Result<()> {
        let old_count = (self.max_key + 1) as u64;
        let new_count = (new_max_key + 1) as u64;
        let needed = (new_count - old_count) * KEY_ENTRY_SIZE;
        let zeros = vec![0u8; needed as usize];
        self.keys.append(&zeros)?;
        self.max_key = new_max_key;
        self.write_header()?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        let hdr_bytes = self.keys.read_at(0, HEADER_SIZE as usize);
        let mut hdr = [0u8; HEADER_SIZE as usize];
        hdr.copy_from_slice(hdr_bytes);
        hdr[0..4].copy_from_slice(&self.key_count.to_le_bytes());
        hdr[4..8].copy_from_slice(&self.max_key.to_le_bytes());
        // block_capacity and reserved unchanged.
        // Write back in-place via the mmap.
        let ptr = self.keys.read_at(0, HEADER_SIZE as usize).as_ptr();
        // SAFETY: we hold &mut self so exclusive access is guaranteed.
        unsafe {
            let dst = ptr as *mut u8;
            std::ptr::copy_nonoverlapping(hdr.as_ptr(), dst, hdr.len());
        }
        Ok(())
    }

    fn key_entry_offset(key: i32) -> u64 {
        HEADER_SIZE + key as u64 * KEY_ENTRY_SIZE
    }

    fn read_key_entry(&self, key: i32) -> KeyEntry {
        let off = Self::key_entry_offset(key);
        let b = self.keys.read_at(off, KEY_ENTRY_SIZE as usize);
        KeyEntry {
            last_block_offset: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            count: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            first_block_offset: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        }
    }

    fn write_key_entry(&mut self, key: i32, entry: KeyEntry) {
        let off = Self::key_entry_offset(key) as usize;
        let b = self.keys.read_at(off as u64, KEY_ENTRY_SIZE as usize);
        let ptr = b.as_ptr();
        let mut buf = [0u8; KEY_ENTRY_SIZE as usize];
        buf[0..8].copy_from_slice(&entry.last_block_offset.to_le_bytes());
        buf[8..16].copy_from_slice(&entry.count.to_le_bytes());
        buf[16..24].copy_from_slice(&entry.first_block_offset.to_le_bytes());
        unsafe {
            let dst = ptr as *mut u8;
            std::ptr::copy_nonoverlapping(buf.as_ptr(), dst, buf.len());
        }
    }

    fn block_size(&self) -> u64 {
        VALUE_BLOCK_HEADER_SIZE + self.block_capacity as u64 * 8
    }

    fn alloc_block(&mut self) -> Result<u64> {
        let offset = self.values.len();
        let block = vec![0u8; self.block_size() as usize];
        self.values.append(&block)?;
        Ok(offset)
    }

    fn read_block_count(&self, block_offset: u64) -> u32 {
        let b = self.values.read_at(block_offset + 8, 4);
        u32::from_le_bytes(b.try_into().unwrap())
    }

    fn set_block_count(&mut self, block_offset: u64, count: u32) {
        let b = self.values.read_at(block_offset + 8, 4);
        let ptr = b.as_ptr();
        unsafe {
            let dst = ptr as *mut u8;
            std::ptr::copy_nonoverlapping(count.to_le_bytes().as_ptr(), dst, 4);
        }
    }

    fn set_block_next(&mut self, block_offset: u64, next_offset: u64) {
        let b = self.values.read_at(block_offset, 8);
        let ptr = b.as_ptr();
        unsafe {
            let dst = ptr as *mut u8;
            std::ptr::copy_nonoverlapping(next_offset.to_le_bytes().as_ptr(), dst, 8);
        }
    }

    fn write_row_id_in_block(&mut self, block_offset: u64, index: u32, row_id: u64) {
        let rid_offset = block_offset + VALUE_BLOCK_HEADER_SIZE + index as u64 * 8;
        let b = self.values.read_at(rid_offset, 8);
        let ptr = b.as_ptr();
        unsafe {
            let dst = ptr as *mut u8;
            std::ptr::copy_nonoverlapping(row_id.to_le_bytes().as_ptr(), dst, 8);
        }
    }
}

/// Read-only cursor for a bitmap index. Retrieves row IDs for a given key.
#[allow(dead_code)]
pub struct BitmapIndexReader {
    keys: crate::mmap::MmapReadOnly,
    values: crate::mmap::MmapReadOnly,
    block_capacity: u32,
    max_key: i32,
}

impl BitmapIndexReader {
    /// Open an existing bitmap index for reading.
    pub fn open(dir: &Path, name: &str) -> Result<Self> {
        let key_path = dir.join(format!("{}.k", name));
        let val_path = dir.join(format!("{}.v", name));

        let keys = crate::mmap::MmapReadOnly::open(&key_path)?;
        let values = crate::mmap::MmapReadOnly::open(&val_path)?;

        if keys.len() < HEADER_SIZE {
            return Err(ExchangeDbError::Corruption(
                "bitmap index keys file too small".into(),
            ));
        }

        let hdr = keys.read_at(0, HEADER_SIZE as usize);
        let max_key = i32::from_le_bytes(hdr[4..8].try_into().unwrap());
        let block_capacity = u32::from_le_bytes(hdr[8..12].try_into().unwrap());

        Ok(Self {
            keys,
            values,
            block_capacity,
            max_key,
        })
    }

    /// Return all row IDs associated with the given key, in insertion order.
    pub fn get_row_ids(&self, key: i32) -> Vec<u64> {
        if key < 0 || key > self.max_key {
            return Vec::new();
        }

        let entry = self.read_key_entry(key);
        if entry.count == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(entry.count as usize);
        let mut block_offset = entry.first_block_offset;

        while block_offset != 0 {
            let count = self.read_block_count(block_offset);
            for i in 0..count {
                let rid_offset = block_offset + VALUE_BLOCK_HEADER_SIZE + i as u64 * 8;
                let b = self.values.read_at(rid_offset, 8);
                let row_id = u64::from_le_bytes(b.try_into().unwrap());
                result.push(row_id);
            }
            // Follow the chain.
            let next_bytes = self.values.read_at(block_offset, 8);
            block_offset = u64::from_le_bytes(next_bytes.try_into().unwrap());
        }

        result
    }

    /// Number of row IDs for a given key.
    pub fn count(&self, key: i32) -> u64 {
        if key < 0 || key > self.max_key {
            return 0;
        }
        self.read_key_entry(key).count
    }

    fn read_key_entry(&self, key: i32) -> KeyEntry {
        let off = HEADER_SIZE + key as u64 * KEY_ENTRY_SIZE;
        let b = self.keys.read_at(off, KEY_ENTRY_SIZE as usize);
        KeyEntry {
            last_block_offset: u64::from_le_bytes(b[0..8].try_into().unwrap()),
            count: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            first_block_offset: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        }
    }

    fn read_block_count(&self, block_offset: u64) -> u32 {
        let b = self.values.read_at(block_offset + 8, 4);
        u32::from_le_bytes(b.try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn basic_add_and_read() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "test_idx").unwrap();
            w.add(0, 100).unwrap();
            w.add(0, 200).unwrap();
            w.add(0, 300).unwrap();
            w.add(1, 50).unwrap();
            w.add(1, 150).unwrap();
            w.add(2, 999).unwrap();
            w.flush().unwrap();
        }

        let r = BitmapIndexReader::open(dir.path(), "test_idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![100, 200, 300]);
        assert_eq!(r.get_row_ids(1), vec![50, 150]);
        assert_eq!(r.get_row_ids(2), vec![999]);
        assert_eq!(r.get_row_ids(3), Vec::<u64>::new());
        assert_eq!(r.get_row_ids(-1), Vec::<u64>::new());

        assert_eq!(r.count(0), 3);
        assert_eq!(r.count(1), 2);
        assert_eq!(r.count(2), 1);
        assert_eq!(r.count(5), 0);
    }

    #[test]
    fn block_overflow() {
        let dir = tempdir().unwrap();
        let cap = 4u32; // tiny blocks for testing
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "small", cap).unwrap();
            for i in 0..10u64 {
                w.add(0, i).unwrap();
            }
            w.flush().unwrap();
        }

        let r = BitmapIndexReader::open(dir.path(), "small").unwrap();
        let ids = r.get_row_ids(0);
        assert_eq!(ids, (0..10u64).collect::<Vec<_>>());
        assert_eq!(r.count(0), 10);
    }

    #[test]
    fn non_contiguous_keys() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "sparse").unwrap();
            w.add(5, 10).unwrap();
            w.add(10, 20).unwrap();
            w.flush().unwrap();
        }

        let r = BitmapIndexReader::open(dir.path(), "sparse").unwrap();
        assert_eq!(r.get_row_ids(5), vec![10]);
        assert_eq!(r.get_row_ids(10), vec![20]);
        // Keys between should exist but be empty.
        assert_eq!(r.get_row_ids(0), Vec::<u64>::new());
        assert_eq!(r.get_row_ids(7), Vec::<u64>::new());
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "reopen").unwrap();
            w.add(0, 1).unwrap();
            w.add(0, 2).unwrap();
            w.flush().unwrap();
        }
        // Reopen writer, add more.
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "reopen").unwrap();
            w.add(0, 3).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "reopen").unwrap();
        assert_eq!(r.get_row_ids(0), vec![1, 2, 3]);
    }

    #[test]
    fn negative_key_errors() {
        let dir = tempdir().unwrap();
        let mut w = BitmapIndexWriter::open_default(dir.path(), "neg").unwrap();
        assert!(w.add(-1, 0).is_err());
    }

    #[test]
    fn many_keys_many_rows() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "big", 8).unwrap();
            for key in 0..20i32 {
                for row in 0..25u64 {
                    w.add(key, key as u64 * 1000 + row).unwrap();
                }
            }
            w.flush().unwrap();
        }

        let r = BitmapIndexReader::open(dir.path(), "big").unwrap();
        for key in 0..20i32 {
            let expected: Vec<u64> = (0..25u64).map(|r| key as u64 * 1000 + r).collect();
            assert_eq!(r.get_row_ids(key), expected);
            assert_eq!(r.count(key), 25);
        }
    }
}
