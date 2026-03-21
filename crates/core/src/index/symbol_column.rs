//! Symbol column: combines a FixedColumnWriter/Reader (for i32 symbol IDs) with a
//! SymbolMap (for String <-> i32 encoding/decoding).
//!
//! This is the primary interface for writing and reading dictionary-encoded string columns.
//! The column data file stores i32 IDs, and the symbol map files (.c, .o) store the
//! string dictionary.

use crate::column::{FixedColumnReader, FixedColumnWriter};
use crate::index::symbol_map::{SYMBOL_NULL, SymbolMap};
use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use std::path::Path;

/// Writer for a symbol-encoded column. Encodes strings to i32 IDs via a SymbolMap
/// and writes the IDs to a fixed-width i32 column.
pub struct SymbolColumnWriter {
    column: FixedColumnWriter,
    symbol_map: SymbolMap,
}

impl SymbolColumnWriter {
    /// Open or create a symbol column.
    ///
    /// Files used:
    /// - `{dir}/{name}.d` — i32 column data
    /// - `{dir}/{name}.c` — symbol chars
    /// - `{dir}/{name}.o` — symbol offsets
    pub fn open(dir: &Path, name: &str) -> Result<Self> {
        let col_path = dir.join(format!("{}.d", name));
        let column = FixedColumnWriter::open(&col_path, ColumnType::Symbol)?;
        let symbol_map = SymbolMap::open(dir, name)?;

        Ok(Self { column, symbol_map })
    }

    /// Append a symbol string. The string is encoded to an i32 ID (added to
    /// the symbol map if new) and written to the column.
    pub fn append_symbol(&mut self, symbol: &str) -> Result<i32> {
        let id = self.symbol_map.get_or_add(symbol)?;
        self.column.append_i32(id)?;
        Ok(id)
    }

    /// Append a null symbol value.
    pub fn append_null(&mut self) -> Result<()> {
        self.column.append_i32(SYMBOL_NULL)
    }

    /// Append a raw symbol ID (must be a valid ID or SYMBOL_NULL).
    pub fn append_id(&mut self, id: i32) -> Result<()> {
        self.column.append_i32(id)
    }

    /// Number of rows written.
    pub fn row_count(&self) -> u64 {
        self.column.row_count()
    }

    /// Access the underlying symbol map (e.g., for building bitmap indexes).
    pub fn symbol_map(&self) -> &SymbolMap {
        &self.symbol_map
    }

    /// Access the underlying symbol map mutably.
    pub fn symbol_map_mut(&mut self) -> &mut SymbolMap {
        &mut self.symbol_map
    }

    /// Flush column data and symbol map to disk.
    pub fn flush(&self) -> Result<()> {
        self.column.flush()?;
        self.symbol_map.flush()
    }
}

/// Reader for a symbol-encoded column. Decodes i32 IDs back to strings
/// via a read-only SymbolMap.
pub struct SymbolColumnReader {
    column: FixedColumnReader,
    symbol_map: SymbolMap,
}

impl SymbolColumnReader {
    /// Open an existing symbol column for reading.
    pub fn open(dir: &Path, name: &str) -> Result<Self> {
        let col_path = dir.join(format!("{}.d", name));
        let column = FixedColumnReader::open(&col_path, ColumnType::Symbol)?;
        let symbol_map = SymbolMap::open(dir, name)?;

        Ok(Self { column, symbol_map })
    }

    /// Read the symbol string at the given row. Returns `None` for null values.
    pub fn read_symbol(&self, row: u64) -> Option<&str> {
        let id = self.column.read_i32(row);
        if id == SYMBOL_NULL {
            return None;
        }
        self.symbol_map.get_symbol(id)
    }

    /// Read the raw i32 symbol ID at the given row.
    pub fn read_id(&self, row: u64) -> i32 {
        self.column.read_i32(row)
    }

    /// Number of rows in the column.
    pub fn row_count(&self) -> u64 {
        self.column.row_count()
    }

    /// Access the underlying symbol map.
    pub fn symbol_map(&self) -> &SymbolMap {
        &self.symbol_map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_and_read_symbols() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "ticker").unwrap();
            w.append_symbol("BTC/USD").unwrap();
            w.append_symbol("ETH/USD").unwrap();
            w.append_symbol("BTC/USD").unwrap();
            w.append_symbol("SOL/USDT").unwrap();
            w.flush().unwrap();
            assert_eq!(w.row_count(), 4);
        }

        let r = SymbolColumnReader::open(dir.path(), "ticker").unwrap();
        assert_eq!(r.row_count(), 4);
        assert_eq!(r.read_symbol(0), Some("BTC/USD"));
        assert_eq!(r.read_symbol(1), Some("ETH/USD"));
        assert_eq!(r.read_symbol(2), Some("BTC/USD"));
        assert_eq!(r.read_symbol(3), Some("SOL/USDT"));

        // Check IDs: first two should get 0 and 1, duplicate gets 0.
        assert_eq!(r.read_id(0), 0);
        assert_eq!(r.read_id(1), 1);
        assert_eq!(r.read_id(2), 0); // duplicate "BTC/USD"
        assert_eq!(r.read_id(3), 2);
    }

    #[test]
    fn null_symbols() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "nullable").unwrap();
            w.append_symbol("A").unwrap();
            w.append_null().unwrap();
            w.append_symbol("B").unwrap();
            w.append_null().unwrap();
            w.flush().unwrap();
        }

        let r = SymbolColumnReader::open(dir.path(), "nullable").unwrap();
        assert_eq!(r.read_symbol(0), Some("A"));
        assert_eq!(r.read_symbol(1), None);
        assert_eq!(r.read_symbol(2), Some("B"));
        assert_eq!(r.read_symbol(3), None);

        assert_eq!(r.read_id(1), SYMBOL_NULL);
        assert_eq!(r.read_id(3), SYMBOL_NULL);
    }

    #[test]
    fn symbol_map_accessible() {
        let dir = tempdir().unwrap();
        let mut w = SymbolColumnWriter::open(dir.path(), "access").unwrap();
        w.append_symbol("X").unwrap();
        w.append_symbol("Y").unwrap();

        let sm = w.symbol_map();
        assert_eq!(sm.len(), 2);
        assert_eq!(sm.get_id("X"), Some(0));
        assert_eq!(sm.get_id("Y"), Some(1));
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "rt").unwrap();
            for i in 0..100 {
                w.append_symbol(&format!("SYM_{}", i % 10)).unwrap();
            }
            w.flush().unwrap();
        }

        let r = SymbolColumnReader::open(dir.path(), "rt").unwrap();
        assert_eq!(r.row_count(), 100);
        assert_eq!(r.symbol_map().len(), 10);

        for i in 0..100u64 {
            let expected = format!("SYM_{}", i % 10);
            assert_eq!(r.read_symbol(i), Some(expected.as_str()));
        }
    }

    #[test]
    fn append_id_directly() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "direct").unwrap();
            // Pre-populate symbol map.
            let id_a = w.symbol_map_mut().get_or_add("A").unwrap();
            let id_b = w.symbol_map_mut().get_or_add("B").unwrap();
            // Write IDs directly.
            w.append_id(id_a).unwrap();
            w.append_id(id_b).unwrap();
            w.append_id(id_a).unwrap();
            w.append_id(SYMBOL_NULL).unwrap();
            w.flush().unwrap();
        }

        let r = SymbolColumnReader::open(dir.path(), "direct").unwrap();
        assert_eq!(r.read_symbol(0), Some("A"));
        assert_eq!(r.read_symbol(1), Some("B"));
        assert_eq!(r.read_symbol(2), Some("A"));
        assert_eq!(r.read_symbol(3), None);
    }
}
