use crate::mmap::{MmapFile, MmapReadOnly};
use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use std::path::Path;

const INITIAL_COLUMN_CAPACITY: u64 = 1024 * 1024; // 1MB

/// Append-only writer for a fixed-width column.
pub struct FixedColumnWriter {
    data: MmapFile,
    element_size: usize,
    row_count: u64,
}

impl FixedColumnWriter {
    pub fn open(path: &Path, col_type: ColumnType) -> Result<Self> {
        let element_size = col_type
            .fixed_size()
            .expect("FixedColumnWriter requires fixed-width type");

        let data = MmapFile::open(path, INITIAL_COLUMN_CAPACITY)?;
        let row_count = data.len() / element_size as u64;

        Ok(Self {
            data,
            element_size,
            row_count,
        })
    }

    /// Append a single value (raw bytes, must be element_size long).
    #[inline]
    pub fn append(&mut self, value: &[u8]) -> Result<()> {
        debug_assert_eq!(value.len(), self.element_size);
        self.data.append(value)?;
        self.row_count += 1;
        Ok(())
    }

    /// Append typed value.
    #[inline]
    pub fn append_i64(&mut self, value: i64) -> Result<()> {
        self.append(&value.to_le_bytes())
    }

    #[inline]
    pub fn append_f64(&mut self, value: f64) -> Result<()> {
        self.append(&value.to_le_bytes())
    }

    #[inline]
    pub fn append_i32(&mut self, value: i32) -> Result<()> {
        self.append(&value.to_le_bytes())
    }

    /// Append N values at once from a contiguous byte buffer.
    ///
    /// `data.len()` must be a multiple of `element_size`. This performs a
    /// single memcpy instead of N individual appends, which is dramatically
    /// faster for bulk inserts.
    #[inline]
    pub fn append_bulk(&mut self, data: &[u8]) -> Result<()> {
        debug_assert_eq!(data.len() % self.element_size, 0);
        let count = data.len() / self.element_size;
        self.data.append_bulk(data)?;
        self.row_count += count as u64;
        Ok(())
    }

    /// Append N i64 values at once. Zero-copy: reinterprets the slice as bytes.
    #[inline]
    pub fn append_bulk_i64(&mut self, values: &[i64]) -> Result<()> {
        debug_assert_eq!(self.element_size, 8);
        // SAFETY: i64 has no padding and le bytes representation is well-defined
        // on little-endian systems. On big-endian we'd need conversion, but
        // ExchangeDB targets x86_64/aarch64 (both LE).
        let bytes =
            unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, values.len() * 8) };
        self.data.append_bulk(bytes)?;
        self.row_count += values.len() as u64;
        Ok(())
    }

    /// Append N f64 values at once. Zero-copy: reinterprets the slice as bytes.
    #[inline]
    pub fn append_bulk_f64(&mut self, values: &[f64]) -> Result<()> {
        debug_assert_eq!(self.element_size, 8);
        let bytes =
            unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, values.len() * 8) };
        self.data.append_bulk(bytes)?;
        self.row_count += values.len() as u64;
        Ok(())
    }

    /// Append N i32 values at once. Zero-copy: reinterprets the slice as bytes.
    #[inline]
    pub fn append_bulk_i32(&mut self, values: &[i32]) -> Result<()> {
        debug_assert_eq!(self.element_size, 4);
        let bytes =
            unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, values.len() * 4) };
        self.data.append_bulk(bytes)?;
        self.row_count += values.len() as u64;
        Ok(())
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn flush(&self) -> Result<()> {
        self.data.flush()
    }
}

/// Read-only accessor for a fixed-width column.
pub struct FixedColumnReader {
    data: MmapReadOnly,
    element_size: usize,
    row_count: u64,
}

impl FixedColumnReader {
    pub fn open(path: &Path, col_type: ColumnType) -> Result<Self> {
        let element_size = col_type
            .fixed_size()
            .expect("FixedColumnReader requires fixed-width type");

        let data = MmapReadOnly::open(path)?;
        let row_count = data.len() / element_size as u64;

        Ok(Self {
            data,
            element_size,
            row_count,
        })
    }

    /// Read raw bytes at row index.
    #[inline(always)]
    pub fn read_raw(&self, row: u64) -> &[u8] {
        let offset = row * self.element_size as u64;
        self.data.read_at(offset, self.element_size)
    }

    #[inline(always)]
    pub fn read_i64(&self, row: u64) -> i64 {
        i64::from_le_bytes(self.read_raw(row).try_into().unwrap())
    }

    #[inline(always)]
    pub fn read_f64(&self, row: u64) -> f64 {
        f64::from_le_bytes(self.read_raw(row).try_into().unwrap())
    }

    #[inline(always)]
    pub fn read_i32(&self, row: u64) -> i32 {
        i32::from_le_bytes(self.read_raw(row).try_into().unwrap())
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Return the entire column as a contiguous `&[f64]` slice.
    ///
    /// Zero-copy: returns a reference directly into the mmap'd region.
    /// The caller must ensure the column actually contains f64 data
    /// (element_size == 8).
    #[inline]
    pub fn as_f64_slice(&self) -> &[f64] {
        debug_assert_eq!(self.element_size, 8);
        let bytes = self.data.as_slice();
        let count = bytes.len() / 8;
        // SAFETY: f64 has no alignment requirement stricter than u8 on
        // x86_64/aarch64, and the mmap region is page-aligned.  The bytes
        // were written as little-endian f64 by `FixedColumnWriter::append_f64`.
        unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f64, count) }
    }

    /// Return the entire column as a contiguous `&[i64]` slice.
    ///
    /// Zero-copy: returns a reference directly into the mmap'd region.
    #[inline]
    pub fn as_i64_slice(&self) -> &[i64] {
        debug_assert_eq!(self.element_size, 8);
        let bytes = self.data.as_slice();
        let count = bytes.len() / 8;
        unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const i64, count) }
    }

    /// Return the entire column as a contiguous `&[i32]` slice.
    ///
    /// Zero-copy: returns a reference directly into the mmap'd region.
    #[inline]
    pub fn as_i32_slice(&self) -> &[i32] {
        debug_assert_eq!(self.element_size, 4);
        let bytes = self.data.as_slice();
        let count = bytes.len() / 4;
        unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const i32, count) }
    }

    /// Return the raw byte slice of the underlying mmap.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_slice()
    }
}

/// Append-only writer for variable-length columns (varchar, binary).
/// Uses two files: data (.d) and offsets (.i).
pub struct VarColumnWriter {
    data: MmapFile,
    index: MmapFile,
    row_count: u64,
}

impl VarColumnWriter {
    pub fn open(data_path: &Path, index_path: &Path) -> Result<Self> {
        let data = MmapFile::open(data_path, INITIAL_COLUMN_CAPACITY)?;
        let index = MmapFile::open(index_path, INITIAL_COLUMN_CAPACITY / 8)?;
        let row_count = index.len() / 8; // 8 bytes per offset

        Ok(Self {
            data,
            index,
            row_count,
        })
    }

    pub fn append(&mut self, value: &[u8]) -> Result<()> {
        let offset = self.data.len();
        // Write length prefix + data
        self.data.append(&(value.len() as u32).to_le_bytes())?;
        self.data.append(value)?;
        // Write offset to index
        self.index.append(&offset.to_le_bytes())?;
        self.row_count += 1;
        Ok(())
    }

    pub fn append_str(&mut self, value: &str) -> Result<()> {
        self.append(value.as_bytes())
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn flush(&self) -> Result<()> {
        self.data.flush()?;
        self.index.flush()
    }
}

/// Read-only accessor for variable-length columns.
pub struct VarColumnReader {
    data: MmapReadOnly,
    index: MmapReadOnly,
    row_count: u64,
}

impl VarColumnReader {
    pub fn open(data_path: &Path, index_path: &Path) -> Result<Self> {
        let data = MmapReadOnly::open(data_path)?;
        let index = MmapReadOnly::open(index_path)?;
        let row_count = index.len() / 8;

        Ok(Self {
            data,
            index,
            row_count,
        })
    }

    /// Read the raw bytes for a row (without length prefix).
    pub fn read(&self, row: u64) -> &[u8] {
        let idx_offset = row * 8;
        let data_offset = u64::from_le_bytes(self.index.read_at(idx_offset, 8).try_into().unwrap());

        // Read length prefix
        let len =
            u32::from_le_bytes(self.data.read_at(data_offset, 4).try_into().unwrap()) as usize;

        self.data.read_at(data_offset + 4, len)
    }

    pub fn read_str(&self, row: u64) -> &str {
        std::str::from_utf8(self.read(row)).expect("invalid UTF-8 in varchar column")
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}

/// A value read from a column, including a Null variant.
#[derive(Debug, Clone)]
pub enum Value {
    I32(i32),
    I64(i64),
    F64(f64),
    Null,
}

/// Read a column with column-top awareness.
///
/// When a column is added to a table that already has data, existing
/// partitions do not have that column's data file. The "column top" tells
/// readers where real data starts — rows before that offset are NULL.
pub struct ColumnTopReader {
    reader: Option<FixedColumnReader>,
    column_top: u64,
    total_rows: u64,
}

impl ColumnTopReader {
    /// Open a column reader with column-top awareness.
    ///
    /// If the column file at `path` does not exist (the column was added
    /// after this partition was created), all `total_rows` rows are NULL.
    ///
    /// If the file exists but has fewer rows than `total_rows`, the
    /// difference is the column top (leading NULLs).
    pub fn open(path: &std::path::Path, col_type: ColumnType, total_rows: u64) -> Result<Self> {
        if !path.exists() {
            return Ok(Self {
                reader: None,
                column_top: total_rows,
                total_rows,
            });
        }

        let reader = FixedColumnReader::open(path, col_type)?;
        let file_rows = reader.row_count();

        // If the file has fewer rows than total_rows, the first
        // (total_rows - file_rows) rows are NULL (column top).
        let column_top = total_rows.saturating_sub(file_rows);

        Ok(Self {
            reader: Some(reader),
            column_top,
            total_rows,
        })
    }

    /// Read a value at a given row index.
    ///
    /// Returns `Value::Null` if `row < column_top` (the column did not
    /// exist when this row was written).
    pub fn read_value(&self, row: u64) -> Value {
        if row < self.column_top {
            return Value::Null;
        }
        match &self.reader {
            Some(reader) => {
                let adjusted_row = row - self.column_top;
                // Determine type from element_size.
                let raw = reader.read_raw(adjusted_row);
                match raw.len() {
                    4 => Value::I32(i32::from_le_bytes(raw.try_into().unwrap())),
                    8 => {
                        // Could be i64 or f64 — return i64 by default; callers
                        // should use read_i64 / read_f64 on the underlying
                        // reader when they know the type.
                        Value::I64(i64::from_le_bytes(raw.try_into().unwrap()))
                    }
                    _ => Value::Null,
                }
            }
            None => Value::Null,
        }
    }

    /// The column top offset (rows before this are NULL).
    pub fn column_top(&self) -> u64 {
        self.column_top
    }

    /// Total rows in the partition.
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn fixed_column_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");

        {
            let mut writer = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            writer.append_f64(100.50).unwrap();
            writer.append_f64(101.25).unwrap();
            writer.append_f64(99.75).unwrap();
            writer.flush().unwrap();
            assert_eq!(writer.row_count(), 3);
        }

        let reader = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(reader.row_count(), 3);
        assert_eq!(reader.read_f64(0), 100.50);
        assert_eq!(reader.read_f64(1), 101.25);
        assert_eq!(reader.read_f64(2), 99.75);
    }

    #[test]
    fn var_column_write_read() {
        let dir = tempdir().unwrap();
        let data_path = dir.path().join("name.d");
        let index_path = dir.path().join("name.i");

        {
            let mut writer = VarColumnWriter::open(&data_path, &index_path).unwrap();
            writer.append_str("BTC/USD").unwrap();
            writer.append_str("ETH/USD").unwrap();
            writer.append_str("SOL/USDT").unwrap();
            writer.flush().unwrap();
            assert_eq!(writer.row_count(), 3);
        }

        let reader = VarColumnReader::open(&data_path, &index_path).unwrap();
        assert_eq!(reader.row_count(), 3);
        assert_eq!(reader.read_str(0), "BTC/USD");
        assert_eq!(reader.read_str(1), "ETH/USD");
        assert_eq!(reader.read_str(2), "SOL/USDT");
    }

    #[test]
    fn column_top_reader_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.d");

        let reader = ColumnTopReader::open(&path, ColumnType::I64, 10).unwrap();
        assert_eq!(reader.column_top(), 10);
        assert_eq!(reader.total_rows(), 10);

        // All rows should be NULL.
        for i in 0..10 {
            match reader.read_value(i) {
                Value::Null => {}
                other => panic!("expected Null, got {:?}", other),
            }
        }
    }

    #[test]
    fn column_top_reader_partial_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");

        // Write 3 rows.
        {
            let mut writer = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            writer.append_i64(100).unwrap();
            writer.append_i64(200).unwrap();
            writer.append_i64(300).unwrap();
            writer.flush().unwrap();
        }

        // But the partition has 5 rows total.
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 5).unwrap();
        assert_eq!(reader.column_top(), 2); // First 2 rows are NULL.
        assert_eq!(reader.total_rows(), 5);

        // Rows 0 and 1 are NULL.
        match reader.read_value(0) {
            Value::Null => {}
            other => panic!("expected Null, got {:?}", other),
        }
        match reader.read_value(1) {
            Value::Null => {}
            other => panic!("expected Null, got {:?}", other),
        }

        // Rows 2, 3, 4 have data.
        match reader.read_value(2) {
            Value::I64(v) => assert_eq!(v, 100),
            other => panic!("expected I64(100), got {:?}", other),
        }
        match reader.read_value(3) {
            Value::I64(v) => assert_eq!(v, 200),
            other => panic!("expected I64(200), got {:?}", other),
        }
        match reader.read_value(4) {
            Value::I64(v) => assert_eq!(v, 300),
            other => panic!("expected I64(300), got {:?}", other),
        }
    }

    #[test]
    fn column_top_reader_full_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");

        {
            let mut writer = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            writer.append_i64(10).unwrap();
            writer.append_i64(20).unwrap();
            writer.flush().unwrap();
        }

        let reader = ColumnTopReader::open(&path, ColumnType::I64, 2).unwrap();
        assert_eq!(reader.column_top(), 0);

        match reader.read_value(0) {
            Value::I64(v) => assert_eq!(v, 10),
            other => panic!("expected I64(10), got {:?}", other),
        }
        match reader.read_value(1) {
            Value::I64(v) => assert_eq!(v, 20),
            other => panic!("expected I64(20), got {:?}", other),
        }
    }
}
