//! Symbol map: bidirectional String <-> i32 mapping for dictionary encoding.
//!
//! Stores symbol strings (e.g., "BTC/USD", "ETH/USD") and assigns each a
//! unique i32 ID starting at 0. Provides O(1) lookup in both directions
//! using xxHash3 for hashing.
//!
//! Persistence uses two files:
//! - `.c` (chars): concatenated symbol strings, each prefixed with a u32 length.
//! - `.o` (offsets): array of u64 offsets into the chars file, one per symbol ID.
//!
//! The in-memory hash map is rebuilt on load from the persisted data.

use crate::mmap::MmapFile;
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::hash::xxh3_64;
use std::collections::HashMap;
use std::path::Path;

/// Null symbol ID, analogous to SQL NULL for symbol columns.
pub const SYMBOL_NULL: i32 = -1;

/// Bidirectional String <-> i32 symbol map with disk persistence.
pub struct SymbolMap {
    /// chars file: length-prefixed UTF-8 strings.
    chars: MmapFile,
    /// offsets file: array of u64 offsets into chars.
    offsets: MmapFile,
    /// string -> id lookup (xxhash-based key in the HashMap).
    str_to_id: HashMap<u64, Vec<(i32, String)>>,
    /// id -> string lookup.
    id_to_str: Vec<String>,
    /// Number of symbols.
    count: i32,
}

impl SymbolMap {
    /// Open or create a symbol map at `{dir}/{name}.c` and `{dir}/{name}.o`.
    pub fn open(dir: &Path, name: &str) -> Result<Self> {
        let chars_path = dir.join(format!("{}.c", name));
        let offsets_path = dir.join(format!("{}.o", name));

        let chars = MmapFile::open(&chars_path, 64 * 1024)?;
        let offsets = MmapFile::open(&offsets_path, 4096)?;

        let symbol_count = (offsets.len() / 8) as i32;

        let mut str_to_id: HashMap<u64, Vec<(i32, String)>> = HashMap::new();
        let mut id_to_str: Vec<String> = Vec::with_capacity(symbol_count as usize);

        // Rebuild in-memory index from persisted data.
        for id in 0..symbol_count {
            let off_pos = id as u64 * 8;
            let char_offset = u64::from_le_bytes(offsets.read_at(off_pos, 8).try_into().unwrap());

            let len_bytes = chars.read_at(char_offset, 4);
            let str_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            let str_bytes = chars.read_at(char_offset + 4, str_len);
            let s = std::str::from_utf8(str_bytes)
                .map_err(|e| {
                    ExchangeDbError::Corruption(format!("invalid UTF-8 in symbol map: {}", e))
                })?
                .to_string();

            let hash = xxh3_64(s.as_bytes());
            str_to_id.entry(hash).or_default().push((id, s.clone()));
            id_to_str.push(s);
        }

        Ok(Self {
            chars,
            offsets,
            str_to_id,
            id_to_str,
            count: symbol_count,
        })
    }

    /// Look up the i32 ID for a symbol string. Returns `None` if not found.
    pub fn get_id(&self, symbol: &str) -> Option<i32> {
        let hash = xxh3_64(symbol.as_bytes());
        if let Some(entries) = self.str_to_id.get(&hash) {
            for (id, s) in entries {
                if s == symbol {
                    return Some(*id);
                }
            }
        }
        None
    }

    /// Look up the string for an i32 ID. Returns `None` if out of range.
    pub fn get_symbol(&self, id: i32) -> Option<&str> {
        if id < 0 || id >= self.count {
            return None;
        }
        Some(&self.id_to_str[id as usize])
    }

    /// Get ID for a symbol, adding it if not yet present. Returns the i32 ID.
    pub fn get_or_add(&mut self, symbol: &str) -> Result<i32> {
        if let Some(id) = self.get_id(symbol) {
            return Ok(id);
        }
        self.add(symbol)
    }

    /// Add a new symbol string. Returns the newly assigned i32 ID.
    /// Returns an error if the symbol already exists.
    pub fn add(&mut self, symbol: &str) -> Result<i32> {
        if self.get_id(symbol).is_some() {
            return Err(ExchangeDbError::Corruption(format!(
                "symbol '{}' already exists in symbol map",
                symbol
            )));
        }

        let id = self.count;
        let bytes = symbol.as_bytes();

        // Write to chars file: u32 length prefix + UTF-8 bytes.
        let char_offset = self.chars.len();
        self.chars.append(&(bytes.len() as u32).to_le_bytes())?;
        self.chars.append(bytes)?;

        // Write offset to offsets file.
        self.offsets.append(&char_offset.to_le_bytes())?;

        // Update in-memory structures.
        let hash = xxh3_64(bytes);
        self.str_to_id
            .entry(hash)
            .or_default()
            .push((id, symbol.to_string()));
        self.id_to_str.push(symbol.to_string());
        self.count += 1;

        Ok(id)
    }

    /// Number of symbols in the map.
    pub fn len(&self) -> i32 {
        self.count
    }

    /// True if the map contains no symbols.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Flush all data to disk.
    pub fn flush(&self) -> Result<()> {
        self.chars.flush()?;
        self.offsets.flush()
    }

    /// Return an iterator over all (id, symbol) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (i32, &str)> {
        self.id_to_str
            .iter()
            .enumerate()
            .map(|(id, s)| (id as i32, s.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn add_and_lookup() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();

        assert_eq!(sm.add("BTC/USD").unwrap(), 0);
        assert_eq!(sm.add("ETH/USD").unwrap(), 1);
        assert_eq!(sm.add("SOL/USDT").unwrap(), 2);

        assert_eq!(sm.get_id("BTC/USD"), Some(0));
        assert_eq!(sm.get_id("ETH/USD"), Some(1));
        assert_eq!(sm.get_id("SOL/USDT"), Some(2));
        assert_eq!(sm.get_id("DOGE/USD"), None);

        assert_eq!(sm.get_symbol(0), Some("BTC/USD"));
        assert_eq!(sm.get_symbol(1), Some("ETH/USD"));
        assert_eq!(sm.get_symbol(2), Some("SOL/USDT"));
        assert_eq!(sm.get_symbol(3), None);
        assert_eq!(sm.get_symbol(-1), None);

        assert_eq!(sm.len(), 3);
    }

    #[test]
    fn get_or_add() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();

        assert_eq!(sm.get_or_add("BTC/USD").unwrap(), 0);
        assert_eq!(sm.get_or_add("ETH/USD").unwrap(), 1);
        // Should return existing ID.
        assert_eq!(sm.get_or_add("BTC/USD").unwrap(), 0);
        assert_eq!(sm.len(), 2);
    }

    #[test]
    fn duplicate_add_errors() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("BTC/USD").unwrap();
        assert!(sm.add("BTC/USD").is_err());
    }

    #[test]
    fn persistence_reload() {
        let dir = tempdir().unwrap();
        {
            let mut sm = SymbolMap::open(dir.path(), "persist").unwrap();
            sm.add("BTC/USD").unwrap();
            sm.add("ETH/USD").unwrap();
            sm.add("SOL/USDT").unwrap();
            sm.flush().unwrap();
        }

        // Reopen and verify all symbols are intact.
        let sm = SymbolMap::open(dir.path(), "persist").unwrap();
        assert_eq!(sm.len(), 3);
        assert_eq!(sm.get_id("BTC/USD"), Some(0));
        assert_eq!(sm.get_id("ETH/USD"), Some(1));
        assert_eq!(sm.get_id("SOL/USDT"), Some(2));
        assert_eq!(sm.get_symbol(0), Some("BTC/USD"));
        assert_eq!(sm.get_symbol(1), Some("ETH/USD"));
        assert_eq!(sm.get_symbol(2), Some("SOL/USDT"));
    }

    #[test]
    fn reopen_and_add_more() {
        let dir = tempdir().unwrap();
        {
            let mut sm = SymbolMap::open(dir.path(), "grow").unwrap();
            sm.add("A").unwrap();
            sm.add("B").unwrap();
            sm.flush().unwrap();
        }
        {
            let mut sm = SymbolMap::open(dir.path(), "grow").unwrap();
            assert_eq!(sm.len(), 2);
            sm.add("C").unwrap();
            sm.flush().unwrap();
        }

        let sm = SymbolMap::open(dir.path(), "grow").unwrap();
        assert_eq!(sm.len(), 3);
        assert_eq!(sm.get_id("A"), Some(0));
        assert_eq!(sm.get_id("B"), Some(1));
        assert_eq!(sm.get_id("C"), Some(2));
    }

    #[test]
    fn iter_symbols() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "iter").unwrap();
        sm.add("X").unwrap();
        sm.add("Y").unwrap();
        sm.add("Z").unwrap();

        let items: Vec<(i32, &str)> = sm.iter().collect();
        assert_eq!(items, vec![(0, "X"), (1, "Y"), (2, "Z")]);
    }

    #[test]
    fn empty_symbol() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "empty").unwrap();
        assert_eq!(sm.add("").unwrap(), 0);
        assert_eq!(sm.get_id(""), Some(0));
        assert_eq!(sm.get_symbol(0), Some(""));
    }

    #[test]
    fn many_symbols() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "many").unwrap();
        for i in 0..500 {
            let sym = format!("SYM_{}", i);
            assert_eq!(sm.add(&sym).unwrap(), i);
        }
        assert_eq!(sm.len(), 500);
        for i in 0..500 {
            let sym = format!("SYM_{}", i);
            assert_eq!(sm.get_id(&sym), Some(i));
            assert_eq!(sm.get_symbol(i), Some(sym.as_str()));
        }
    }
}
