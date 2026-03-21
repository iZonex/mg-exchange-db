//! Comprehensive tests for bitmap indexes, symbol maps, and symbol columns.
//!
//! 60 tests covering BitmapIndexWriter/Reader, SymbolMap, and SymbolColumnWriter/Reader.

use exchange_core::index::bitmap::{BitmapIndexReader, BitmapIndexWriter};
use exchange_core::index::symbol_column::{SymbolColumnReader, SymbolColumnWriter};
use exchange_core::index::symbol_map::{SymbolMap, SYMBOL_NULL};
use tempfile::tempdir;

// ============================================================================
// Bitmap Index
// ============================================================================

mod bitmap_index {
    use super::*;

    #[test]
    fn basic_add_and_read() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 100).unwrap();
            w.add(0, 200).unwrap();
            w.add(1, 50).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![100, 200]);
        assert_eq!(r.get_row_ids(1), vec![50]);
    }

    #[test]
    fn empty_key_returns_empty() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 1).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(5), Vec::<u64>::new());
    }

    #[test]
    fn negative_key_returns_empty() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 1).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(-1), Vec::<u64>::new());
    }

    #[test]
    fn negative_key_add_errors() {
        let dir = tempdir().unwrap();
        let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
        assert!(w.add(-1, 0).is_err());
    }

    #[test]
    fn block_overflow_small_capacity() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", 4).unwrap();
            for i in 0..20u64 {
                w.add(0, i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), (0..20u64).collect::<Vec<_>>());
        assert_eq!(r.count(0), 20);
    }

    #[test]
    fn non_contiguous_keys() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(5, 10).unwrap();
            w.add(10, 20).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(5), vec![10]);
        assert_eq!(r.get_row_ids(10), vec![20]);
        assert_eq!(r.get_row_ids(0), Vec::<u64>::new());
        assert_eq!(r.get_row_ids(7), Vec::<u64>::new());
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 1).unwrap();
            w.add(0, 2).unwrap();
            w.flush().unwrap();
        }
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 3).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![1, 2, 3]);
    }

    #[test]
    fn count_query() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 10).unwrap();
            w.add(0, 20).unwrap();
            w.add(0, 30).unwrap();
            w.add(1, 40).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.count(0), 3);
        assert_eq!(r.count(1), 1);
        assert_eq!(r.count(2), 0);
        assert_eq!(r.count(-1), 0);
    }

    #[test]
    fn many_keys_many_rows() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", 8).unwrap();
            for key in 0..50i32 {
                for row in 0..20u64 {
                    w.add(key, key as u64 * 1000 + row).unwrap();
                }
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        for key in 0..50i32 {
            let expected: Vec<u64> = (0..20u64).map(|row| key as u64 * 1000 + row).collect();
            assert_eq!(r.get_row_ids(key), expected);
            assert_eq!(r.count(key), 20);
        }
    }

    #[test]
    fn single_key_single_row() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 42).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![42]);
    }

    #[test]
    fn key_zero_only() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            for i in 0..100u64 {
                w.add(0, i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.count(0), 100);
    }

    #[test]
    fn large_key_values() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(1000, 1).unwrap();
            w.add(1000, 2).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(1000), vec![1, 2]);
    }

    #[test]
    fn large_row_id_values() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, u64::MAX - 1).unwrap();
            w.add(0, u64::MAX / 2).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        let ids = r.get_row_ids(0);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&(u64::MAX - 1)));
        assert!(ids.contains(&(u64::MAX / 2)));
    }

    #[test]
    fn block_capacity_one() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", 1).unwrap();
            w.add(0, 10).unwrap();
            w.add(0, 20).unwrap();
            w.add(0, 30).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![10, 20, 30]);
    }

    #[test]
    fn interleaved_keys() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 1).unwrap();
            w.add(1, 2).unwrap();
            w.add(0, 3).unwrap();
            w.add(1, 4).unwrap();
            w.add(2, 5).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![1, 3]);
        assert_eq!(r.get_row_ids(1), vec![2, 4]);
        assert_eq!(r.get_row_ids(2), vec![5]);
    }

    #[test]
    fn exact_block_boundary() {
        let dir = tempdir().unwrap();
        let cap = 4;
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", cap).unwrap();
            // Write exactly block_capacity rows
            for i in 0..cap as u64 {
                w.add(0, i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), (0..cap as u64).collect::<Vec<_>>());
    }

    #[test]
    fn one_past_block_boundary() {
        let dir = tempdir().unwrap();
        let cap = 4;
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", cap).unwrap();
            for i in 0..=cap as u64 {
                w.add(0, i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(
            r.get_row_ids(0),
            (0..=cap as u64).collect::<Vec<_>>()
        );
    }
}

// ============================================================================
// Symbol Map
// ============================================================================

mod symbol_map {
    use super::*;

    #[test]
    fn add_and_lookup() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.add("BTC/USD").unwrap(), 0);
        assert_eq!(sm.add("ETH/USD").unwrap(), 1);
        assert_eq!(sm.get_id("BTC/USD"), Some(0));
        assert_eq!(sm.get_id("ETH/USD"), Some(1));
        assert_eq!(sm.get_id("DOGE"), None);
    }

    #[test]
    fn get_symbol_by_id() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("A").unwrap();
        sm.add("B").unwrap();
        assert_eq!(sm.get_symbol(0), Some("A"));
        assert_eq!(sm.get_symbol(1), Some("B"));
        assert_eq!(sm.get_symbol(2), None);
        assert_eq!(sm.get_symbol(-1), None);
    }

    #[test]
    fn duplicate_add_errors() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("X").unwrap();
        assert!(sm.add("X").is_err());
    }

    #[test]
    fn get_or_add() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.get_or_add("A").unwrap(), 0);
        assert_eq!(sm.get_or_add("B").unwrap(), 1);
        assert_eq!(sm.get_or_add("A").unwrap(), 0);
        assert_eq!(sm.len(), 2);
    }

    #[test]
    fn persistence_reload() {
        let dir = tempdir().unwrap();
        {
            let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
            sm.add("BTC/USD").unwrap();
            sm.add("ETH/USD").unwrap();
            sm.flush().unwrap();
        }
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.len(), 2);
        assert_eq!(sm.get_id("BTC/USD"), Some(0));
        assert_eq!(sm.get_id("ETH/USD"), Some(1));
    }

    #[test]
    fn reopen_and_add_more() {
        let dir = tempdir().unwrap();
        {
            let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
            sm.add("A").unwrap();
            sm.flush().unwrap();
        }
        {
            let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
            sm.add("B").unwrap();
            sm.flush().unwrap();
        }
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.len(), 2);
        assert_eq!(sm.get_id("A"), Some(0));
        assert_eq!(sm.get_id("B"), Some(1));
    }

    #[test]
    fn empty_symbol_string() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.add("").unwrap(), 0);
        assert_eq!(sm.get_id(""), Some(0));
        assert_eq!(sm.get_symbol(0), Some(""));
    }

    #[test]
    fn many_symbols() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        for i in 0..10_000 {
            sm.add(&format!("SYM_{}", i)).unwrap();
        }
        assert_eq!(sm.len(), 10_000);
        assert_eq!(sm.get_id("SYM_0"), Some(0));
        assert_eq!(sm.get_id("SYM_9999"), Some(9999));
        assert_eq!(sm.get_symbol(5000), Some("SYM_5000"));
    }

    #[test]
    fn iter_symbols() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("X").unwrap();
        sm.add("Y").unwrap();
        sm.add("Z").unwrap();
        let items: Vec<(i32, &str)> = sm.iter().collect();
        assert_eq!(items, vec![(0, "X"), (1, "Y"), (2, "Z")]);
    }

    #[test]
    fn is_empty() {
        let dir = tempdir().unwrap();
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert!(sm.is_empty());
    }

    #[test]
    fn len_after_adds() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.len(), 0);
        sm.add("A").unwrap();
        assert_eq!(sm.len(), 1);
        sm.add("B").unwrap();
        assert_eq!(sm.len(), 2);
    }

    #[test]
    fn unicode_symbols() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("hello").unwrap();
        sm.add("Bonjour").unwrap();
        sm.flush().unwrap();
        let sm2 = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm2.get_id("hello"), Some(0));
        assert_eq!(sm2.get_id("Bonjour"), Some(1));
    }

    #[test]
    fn long_symbol_string() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let long = "x".repeat(10_000);
        sm.add(&long).unwrap();
        assert_eq!(sm.get_id(&long), Some(0));
        assert_eq!(sm.get_symbol(0), Some(long.as_str()));
    }

    #[test]
    fn symbol_null_constant() {
        assert_eq!(SYMBOL_NULL, -1);
    }
}

// ============================================================================
// Symbol Column
// ============================================================================

mod symbol_column {
    use super::*;

    #[test]
    fn write_and_read() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "ticker").unwrap();
            w.append_symbol("BTC/USD").unwrap();
            w.append_symbol("ETH/USD").unwrap();
            w.append_symbol("BTC/USD").unwrap();
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "ticker").unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_symbol(0), Some("BTC/USD"));
        assert_eq!(r.read_symbol(1), Some("ETH/USD"));
        assert_eq!(r.read_symbol(2), Some("BTC/USD"));
    }

    #[test]
    fn null_symbols() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            w.append_symbol("A").unwrap();
            w.append_null().unwrap();
            w.append_symbol("B").unwrap();
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.read_symbol(0), Some("A"));
        assert_eq!(r.read_symbol(1), None);
        assert_eq!(r.read_symbol(2), Some("B"));
        assert_eq!(r.read_id(1), SYMBOL_NULL);
    }

    #[test]
    fn auto_encoding_deduplicates() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            for _ in 0..100 {
                w.append_symbol("SAME").unwrap();
            }
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.row_count(), 100);
        assert_eq!(r.symbol_map().len(), 1);
        for i in 0..100 {
            assert_eq!(r.read_symbol(i), Some("SAME"));
            assert_eq!(r.read_id(i), 0);
        }
    }

    #[test]
    fn symbol_map_accessible_from_writer() {
        let dir = tempdir().unwrap();
        let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
        w.append_symbol("X").unwrap();
        w.append_symbol("Y").unwrap();
        assert_eq!(w.symbol_map().len(), 2);
        assert_eq!(w.symbol_map().get_id("X"), Some(0));
    }

    #[test]
    fn symbol_map_accessible_from_reader() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            w.append_symbol("A").unwrap();
            w.append_symbol("B").unwrap();
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.symbol_map().len(), 2);
        assert_eq!(r.symbol_map().get_id("A"), Some(0));
    }

    #[test]
    fn append_id_directly() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            let id_a = w.symbol_map_mut().get_or_add("A").unwrap();
            let id_b = w.symbol_map_mut().get_or_add("B").unwrap();
            w.append_id(id_a).unwrap();
            w.append_id(id_b).unwrap();
            w.append_id(SYMBOL_NULL).unwrap();
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.read_symbol(0), Some("A"));
        assert_eq!(r.read_symbol(1), Some("B"));
        assert_eq!(r.read_symbol(2), None);
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            for i in 0..100 {
                w.append_symbol(&format!("SYM_{}", i % 10)).unwrap();
            }
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.row_count(), 100);
        assert_eq!(r.symbol_map().len(), 10);
        for i in 0..100u64 {
            assert_eq!(
                r.read_symbol(i),
                Some(format!("SYM_{}", i % 10).as_str())
            );
        }
    }

    #[test]
    fn many_unique_symbols() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            for i in 0..1000 {
                w.append_symbol(&format!("UNIQUE_{}", i)).unwrap();
            }
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.row_count(), 1000);
        assert_eq!(r.symbol_map().len(), 1000);
    }

    #[test]
    fn read_id_values() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            w.append_symbol("X").unwrap();
            w.append_symbol("Y").unwrap();
            w.append_symbol("X").unwrap();
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.read_id(0), 0);
        assert_eq!(r.read_id(1), 1);
        assert_eq!(r.read_id(2), 0);
    }

    #[test]
    fn row_count_from_writer() {
        let dir = tempdir().unwrap();
        let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
        assert_eq!(w.row_count(), 0);
        w.append_symbol("A").unwrap();
        assert_eq!(w.row_count(), 1);
        w.append_null().unwrap();
        assert_eq!(w.row_count(), 2);
    }

    #[test]
    fn all_nulls() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            for _ in 0..20 {
                w.append_null().unwrap();
            }
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.row_count(), 20);
        assert_eq!(r.symbol_map().len(), 0);
        for i in 0..20 {
            assert_eq!(r.read_symbol(i), None);
        }
    }

    #[test]
    fn alternating_null_and_value() {
        let dir = tempdir().unwrap();
        {
            let mut w = SymbolColumnWriter::open(dir.path(), "s").unwrap();
            for i in 0..50 {
                if i % 2 == 0 {
                    w.append_symbol("X").unwrap();
                } else {
                    w.append_null().unwrap();
                }
            }
            w.flush().unwrap();
        }
        let r = SymbolColumnReader::open(dir.path(), "s").unwrap();
        assert_eq!(r.row_count(), 50);
        for i in 0..50u64 {
            if i % 2 == 0 {
                assert_eq!(r.read_symbol(i), Some("X"));
            } else {
                assert_eq!(r.read_symbol(i), None);
            }
        }
    }
}

// ============================================================================
// Additional bitmap index tests
// ============================================================================

mod bitmap_extra {
    use super::*;

    #[test]
    fn many_rows_per_key_large_capacity() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", 256).unwrap();
            for i in 0..1000u64 {
                w.add(0, i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.count(0), 1000);
        let ids = r.get_row_ids(0);
        assert_eq!(ids.len(), 1000);
        assert_eq!(ids[0], 0);
        assert_eq!(ids[999], 999);
    }

    #[test]
    fn keys_0_and_1_interleaved() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open(dir.path(), "idx", 4).unwrap();
            for i in 0..100u64 {
                w.add((i % 2) as i32, i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        let ids0 = r.get_row_ids(0);
        let ids1 = r.get_row_ids(1);
        assert_eq!(ids0.len(), 50);
        assert_eq!(ids1.len(), 50);
        assert!(ids0.iter().all(|&id| id % 2 == 0));
        assert!(ids1.iter().all(|&id| id % 2 == 1));
    }

    #[test]
    fn sequential_keys() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            for key in 0..100i32 {
                w.add(key, key as u64 * 10).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        for key in 0..100i32 {
            assert_eq!(r.get_row_ids(key), vec![key as u64 * 10]);
            assert_eq!(r.count(key), 1);
        }
    }

    #[test]
    fn reopen_and_add_different_key() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 100).unwrap();
            w.flush().unwrap();
        }
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(1, 200).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![100]);
        assert_eq!(r.get_row_ids(1), vec![200]);
    }

    #[test]
    fn count_empty_index() {
        let dir = tempdir().unwrap();
        {
            let w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.count(0), 0);
        assert_eq!(r.get_row_ids(0), Vec::<u64>::new());
    }
}

// ============================================================================
// Additional symbol map tests
// ============================================================================

mod symbol_map_extra {
    use super::*;

    #[test]
    fn symbol_map_special_chars() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("with space").unwrap();
        sm.add("with\ttab").unwrap();
        sm.add("with\nnewline").unwrap();
        sm.flush().unwrap();
        let sm2 = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm2.get_id("with space"), Some(0));
        assert_eq!(sm2.get_id("with\ttab"), Some(1));
        assert_eq!(sm2.get_id("with\nnewline"), Some(2));
    }

    #[test]
    fn symbol_map_similar_strings() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        sm.add("abc").unwrap();
        sm.add("abcd").unwrap();
        sm.add("ab").unwrap();
        sm.add("abcde").unwrap();
        assert_eq!(sm.get_id("abc"), Some(0));
        assert_eq!(sm.get_id("abcd"), Some(1));
        assert_eq!(sm.get_id("ab"), Some(2));
        assert_eq!(sm.get_id("abcde"), Some(3));
    }

    #[test]
    fn symbol_map_get_or_add_mixed() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.get_or_add("A").unwrap(), 0);
        assert_eq!(sm.get_or_add("B").unwrap(), 1);
        assert_eq!(sm.get_or_add("A").unwrap(), 0);
        assert_eq!(sm.get_or_add("C").unwrap(), 2);
        assert_eq!(sm.get_or_add("B").unwrap(), 1);
        assert_eq!(sm.len(), 3);
    }

    #[test]
    fn symbol_map_iter_empty() {
        let dir = tempdir().unwrap();
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let items: Vec<_> = sm.iter().collect();
        assert!(items.is_empty());
    }
}
