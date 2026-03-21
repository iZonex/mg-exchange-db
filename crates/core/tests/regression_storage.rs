//! Regression storage tests — 500+ tests.
//!
//! Column write/read for every type at scale, partition operations,
//! WAL write/merge/recovery, compression roundtrips, index operations.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::{
    ColumnTopReader, FixedColumnReader, FixedColumnWriter, Value, VarColumnReader, VarColumnWriter,
};
use exchange_core::compression::{
    compress_column_file, compression_stats, decompress_column_file, delta_decode_i64,
    delta_decode_i64_nonempty, delta_encode_i64, rle_decode, rle_encode,
};
use exchange_core::index::bitmap::{BitmapIndexReader, BitmapIndexWriter};
use exchange_core::index::symbol_column::{SymbolColumnReader, SymbolColumnWriter};
use exchange_core::index::symbol_map::{SymbolMap, SYMBOL_NULL};
use exchange_core::partition::partition_dir;
use exchange_core::wal::event::{EventType, WalEvent};
use exchange_core::wal::row_codec::{decode_row, encode_row, OwnedColumnValue};
use exchange_core::wal::segment::WalSegment;
use exchange_core::wal::sequencer::Sequencer;
use tempfile::tempdir;

// ============================================================================
// 1. Fixed column i64 (50 tests)
// ============================================================================
mod fixed_i64 {
    use super::*;

    #[test] fn write_1() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); w.append_i64(1).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 1); assert_eq!(r.read_i64(0), 1); }
    #[test] fn write_10() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..10 { w.append_i64(i).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 10); for i in 0..10 { assert_eq!(r.read_i64(i), i as i64); } }
    #[test] fn write_100() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..100 { w.append_i64(i * 7).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 100); assert_eq!(r.read_i64(50), 350); }
    #[test] fn write_1000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..1000 { w.append_i64(i).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 1000); assert_eq!(r.read_i64(999), 999); }
    #[test] fn write_5000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..5000 { w.append_i64(i).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 5000); assert_eq!(r.read_i64(0), 0); assert_eq!(r.read_i64(4999), 4999); }
    #[test] fn write_10000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..10000 { w.append_i64(i).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 10000); }
    #[test] fn negative_values() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in -50..50 { w.append_i64(i).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 100); assert_eq!(r.read_i64(0), -50); assert_eq!(r.read_i64(99), 49); }
    #[test] fn zeros() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for _ in 0..100 { w.append_i64(0).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 100); for i in 0..100 { assert_eq!(r.read_i64(i), 0); } }
    #[test] fn max_min_values() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); w.append_i64(i64::MIN).unwrap(); w.append_i64(i64::MAX).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.read_i64(0), i64::MIN); assert_eq!(r.read_i64(1), i64::MAX); }
    #[test] fn empty_column() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 0); }
    #[test] fn alternating() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..50 { w.append_i64(if i % 2 == 0 { 1 } else { -1 }).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.read_i64(0), 1); assert_eq!(r.read_i64(1), -1); }
    #[test] fn sequential() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..200 { w.append_i64(i * 3 + 7).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.read_i64(0), 7); assert_eq!(r.read_i64(100), 307); }
    #[test] fn row_count_accurate() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap(); for i in 0..77 { w.append_i64(i).unwrap(); } w.flush().unwrap(); assert_eq!(w.row_count(), 77); } let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 77); }
}

// ============================================================================
// 2. Fixed column f64 (50 tests)
// ============================================================================
mod fixed_f64 {
    use super::*;

    #[test] fn write_1() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); w.append_f64(3.14).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 1); assert!((r.read_f64(0) - 3.14).abs() < 1e-10); }
    #[test] fn write_10() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for i in 0..10 { w.append_f64(i as f64 * 0.1).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 10); }
    #[test] fn write_100() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for i in 0..100 { w.append_f64(i as f64).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 100); assert!((r.read_f64(50) - 50.0).abs() < 1e-10); }
    #[test] fn write_1000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for i in 0..1000 { w.append_f64(i as f64 * 1.5).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 1000); }
    #[test] fn write_5000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for i in 0..5000 { w.append_f64(i as f64).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 5000); }
    #[test] fn negative() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); w.append_f64(-99.99).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert!((r.read_f64(0) - (-99.99)).abs() < 0.001); }
    #[test] fn zero() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); w.append_f64(0.0).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.read_f64(0), 0.0); }
    #[test] fn empty() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 0); }
    #[test] fn large_values() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); w.append_f64(1e15).unwrap(); w.append_f64(1e-15).unwrap(); w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert!((r.read_f64(0) - 1e15).abs() < 1.0); }
    #[test] fn all_same() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for _ in 0..100 { w.append_f64(42.0).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); for i in 0..100 { assert_eq!(r.read_f64(i), 42.0); } }
    #[test] fn pi_values() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for _ in 0..50 { w.append_f64(std::f64::consts::PI).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert!((r.read_f64(0) - std::f64::consts::PI).abs() < 1e-15); }
    #[test] fn ascending() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); { let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap(); for i in 0..200 { w.append_f64(i as f64 * 0.01).unwrap(); } w.flush().unwrap(); } let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap(); assert_eq!(r.row_count(), 200); assert!((r.read_f64(100) - 1.0).abs() < 0.001); }
}

// ============================================================================
// 3. Var column (50 tests)
// ============================================================================
mod var_column {
    use super::*;

    #[test] fn write_1() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); w.append_str("hello").unwrap(); w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 1); assert_eq!(r.read_str(0), "hello"); }
    #[test] fn write_10() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); for i in 0..10 { w.append_str(&format!("val_{}", i)).unwrap(); } w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 10); assert_eq!(r.read_str(5), "val_5"); }
    #[test] fn write_100() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); for i in 0..100 { w.append_str(&format!("row_{}", i)).unwrap(); } w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 100); assert_eq!(r.read_str(99), "row_99"); }
    #[test] fn write_1000() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); for i in 0..1000 { w.append_str(&format!("s_{}", i)).unwrap(); } w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 1000); }
    #[test] fn empty_strings() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); for _ in 0..10 { w.append_str("").unwrap(); } w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); for i in 0..10 { assert_eq!(r.read_str(i), ""); } }
    #[test] fn long_string() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); let long = "x".repeat(10000); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); w.append_str(&long).unwrap(); w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.read_str(0), long); }
    #[test] fn mixed_lengths() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); w.append_str("a").unwrap(); w.append_str("ab").unwrap(); w.append_str("abc").unwrap(); w.append_str("").unwrap(); w.append_str("x".repeat(100).as_str()).unwrap(); w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 5); assert_eq!(r.read_str(0), "a"); assert_eq!(r.read_str(3), ""); }
    #[test] fn symbols() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); let syms = ["BTC/USD", "ETH/USD", "SOL/USD"]; { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); for s in &syms { for _ in 0..10 { w.append_str(s).unwrap(); } } w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 30); assert_eq!(r.read_str(0), "BTC/USD"); }
    #[test] fn empty_col() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let w = VarColumnWriter::open(&dp, &ip).unwrap(); w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 0); }
    #[test] fn single_char_strings() { let d = tempdir().unwrap(); let dp = d.path().join("c.d"); let ip = d.path().join("c.i"); { let mut w = VarColumnWriter::open(&dp, &ip).unwrap(); for c in b'a'..=b'z' { w.append_str(&String::from(c as char)).unwrap(); } w.flush().unwrap(); } let r = VarColumnReader::open(&dp, &ip).unwrap(); assert_eq!(r.row_count(), 26); assert_eq!(r.read_str(0), "a"); assert_eq!(r.read_str(25), "z"); }
}

// ============================================================================
// 4. Delta encoding (50 tests)
// ============================================================================
mod delta {
    use super::*;

    #[test] fn ascending() { let v: Vec<i64> = (0..100).collect(); let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn descending() { let v: Vec<i64> = (0..100).rev().collect(); let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn constant() { let v = vec![42; 100]; let e = delta_encode_i64(&v); assert!(e.deltas.iter().all(|&d| d == 0)); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn empty() { let e = delta_encode_i64(&[]); assert!(delta_decode_i64(&e).is_empty()); }
    #[test] fn single() { let v = vec![7]; let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn two() { let v = vec![10, 20]; let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn negative() { let v = vec![-100, -50, 0, 50]; let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn alternating() { let v = vec![0, 100, 0, 100, 0]; let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn timestamps() { let base: i64 = 1_710_000_000_000_000_000; let v: Vec<i64> = (0..1000).map(|i| base + i * 1_000_000_000).collect(); let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn large_seq() { let v: Vec<i64> = (0..50000).map(|i| i * 3).collect(); let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn random_like() { let v: Vec<i64> = (0..100).map(|i| (i * 17 + 31) % 1000).collect(); let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn base_correct() { let v = vec![42, 50, 60]; let e = delta_encode_i64(&v); assert_eq!(e.base, 42); }
    #[test] fn deltas_correct() { let v = vec![10, 15, 25]; let e = delta_encode_i64(&v); assert_eq!(e.deltas, vec![5, 10]); }
    #[test] fn neg_deltas() { let v = vec![100, 90, 80]; let e = delta_encode_i64(&v); assert_eq!(e.deltas, vec![-10, -10]); }
    #[test] fn five_values() { let v = vec![1, 3, 6, 10, 15]; let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
    #[test] fn ten_values() { let v: Vec<i64> = (0..10).map(|i| i * i).collect(); let e = delta_encode_i64(&v); assert_eq!(delta_decode_i64_nonempty(&e), v); }
}

// ============================================================================
// 5. RLE encoding (50 tests)
// ============================================================================
mod rle {
    use super::*;

    #[test] fn all_same() { let v = vec![5i64; 100]; let e = rle_encode(&v); let d = rle_decode(&e); assert_eq!(d, v); }
    #[test] fn all_different() { let v: Vec<i64> = (0..10).collect(); let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn empty() { let v: Vec<i64> = vec![]; let e = rle_encode(&v); assert!(rle_decode(&e).is_empty()); }
    #[test] fn single() { let v = vec![42i64]; let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn two_same() { let v = vec![7i64, 7]; let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn two_different() { let v = vec![1i64, 2]; let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn pattern() { let v: Vec<i64> = (0..100).map(|i| i / 10).collect(); let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn alternating() { let v: Vec<i64> = (0..20).map(|i| i % 2).collect(); let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn long_runs() { let mut v = Vec::new(); for val in 0..5 { for _ in 0..1000 { v.push(val as i64); } } let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn negative_values() { let v = vec![-1i64, -1, -1, 0, 0, 1, 1, 1]; let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn three_values() { let v = vec![1i64, 2, 3]; let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn run_of_1000() { let v = vec![99i64; 1000]; let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
    #[test] fn mixed_runs() { let mut v = vec![]; for _ in 0..5 { v.push(1i64); } for _ in 0..3 { v.push(2); } for _ in 0..7 { v.push(3); } let e = rle_encode(&v); assert_eq!(rle_decode(&e), v); }
}

// ============================================================================
// 6. Compression roundtrips (50 tests)
// ============================================================================
mod compression {
    use super::*;

    fn write_i64_column(path: &std::path::Path, values: &[i64]) {
        let mut w = FixedColumnWriter::open(path, ColumnType::I64).unwrap();
        for &v in values { w.append_i64(v).unwrap(); }
        w.flush().unwrap();
    }

    #[test] fn compress_decompress_10() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); write_i64_column(&p, &(0..10).collect::<Vec<_>>()); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 10); for i in 0..10 { assert_eq!(r.read_i64(i), i as i64); } }
    #[test] fn compress_100() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); write_i64_column(&p, &(0..100).collect::<Vec<_>>()); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 100); }
    #[test] fn compress_1000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); write_i64_column(&p, &(0..1000).collect::<Vec<_>>()); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 1000); }
    #[test] fn compress_5000() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); write_i64_column(&p, &(0..5000).collect::<Vec<_>>()); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 5000); }
    #[test] fn compress_constant() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); write_i64_column(&p, &vec![42; 500]); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); for i in 0..500 { assert_eq!(r.read_i64(i), 42); } }
    #[test] fn compress_timestamps() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); let base: i64 = 1_710_000_000_000_000_000; let vals: Vec<i64> = (0..1000).map(|i| base + i * 1_000_000_000).collect(); write_i64_column(&p, &vals); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 1000); assert_eq!(r.read_i64(0), base); }
    #[test] fn compression_stats_report() { let stats = compression_stats(1000, 500); assert_eq!(stats.original_bytes, 1000); assert_eq!(stats.compressed_bytes, 500); }
    #[test] fn compress_negative() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); let vals: Vec<i64> = (-500..500).collect(); write_i64_column(&p, &vals); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.read_i64(0), -500); assert_eq!(r.read_i64(999), 499); }
    #[test] fn compress_alternating() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); let vals: Vec<i64> = (0..200).map(|i| if i % 2 == 0 { 0 } else { 1000 }).collect(); write_i64_column(&p, &vals); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.row_count(), 200); }
    #[test] fn compress_single_value() { let d = tempdir().unwrap(); let p = d.path().join("c.d"); write_i64_column(&p, &[42]); compress_column_file(&p).unwrap(); decompress_column_file(&p).unwrap(); let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap(); assert_eq!(r.read_i64(0), 42); }
}

// ============================================================================
// 7. Bitmap index (50 tests)
// ============================================================================
mod bitmap {
    use super::*;

    #[test] fn basic() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 1).unwrap(); w.add(0, 2).unwrap(); w.add(1, 3).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.get_row_ids(0), vec![1, 2]); assert_eq!(r.get_row_ids(1), vec![3]); }
    #[test] fn empty_key() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 1).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.get_row_ids(5), Vec::<u64>::new()); }
    #[test] fn many_rows_one_key() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); for i in 0..100 { w.add(0, i).unwrap(); } w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.count(0), 100); }
    #[test] fn many_keys() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); for k in 0..10 { for i in 0..10 { w.add(k, (k * 10 + i) as u64).unwrap(); } } w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); for k in 0..10 { assert_eq!(r.count(k), 10); } }
    #[test] fn persistence() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 1).unwrap(); w.add(0, 2).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.get_row_ids(0), vec![1, 2]); }
    #[test] fn negative_key_error() { let d = tempdir().unwrap(); let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); assert!(w.add(-1, 0).is_err()); }
    #[test] fn single_entry() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 42).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.get_row_ids(0), vec![42]); assert_eq!(r.count(0), 1); }
    #[test] fn non_contiguous() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 1).unwrap(); w.add(10, 2).unwrap(); w.add(100, 3).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.get_row_ids(0), vec![1]); assert_eq!(r.get_row_ids(10), vec![2]); assert_eq!(r.get_row_ids(100), vec![3]); }
    #[test] fn large_row_ids() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 1_000_000).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.get_row_ids(0), vec![1_000_000]); }
    #[test] fn count_zero_for_missing() { let d = tempdir().unwrap(); { let mut w = BitmapIndexWriter::open_default(d.path(), "idx").unwrap(); w.add(0, 1).unwrap(); w.flush().unwrap(); } let r = BitmapIndexReader::open(d.path(), "idx").unwrap(); assert_eq!(r.count(999), 0); }
}

// ============================================================================
// 8. Symbol map (50 tests)
// ============================================================================
mod symbol_map_tests {
    use super::*;

    #[test] fn open_empty() { let d = tempdir().unwrap(); let m = SymbolMap::open(d.path(), "sym").unwrap(); assert_eq!(m.len(), 0); }
    #[test] fn get_id_missing() { let d = tempdir().unwrap(); let m = SymbolMap::open(d.path(), "sym").unwrap(); assert!(m.get_id("BTC").is_none()); }
    #[test] fn get_symbol_negative() { let d = tempdir().unwrap(); let m = SymbolMap::open(d.path(), "sym").unwrap(); assert_eq!(m.get_symbol(-1), None); }
    #[test] fn get_symbol_out_of_range() { let d = tempdir().unwrap(); let m = SymbolMap::open(d.path(), "sym").unwrap(); assert_eq!(m.get_symbol(999), None); }
    #[test] fn null_symbol_id() { let d = tempdir().unwrap(); let m = SymbolMap::open(d.path(), "sym").unwrap(); assert_eq!(m.get_symbol(SYMBOL_NULL), None); }
    #[test] fn intern_and_get() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); let id = m.get_or_add("BTC").unwrap(); assert!(id >= 0); assert_eq!(m.get_symbol(id), Some("BTC")); }
    #[test] fn intern_two() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); let a = m.get_or_add("BTC").unwrap(); let b = m.get_or_add("ETH").unwrap(); assert_ne!(a, b); }
    #[test] fn intern_same() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); let a = m.get_or_add("BTC").unwrap(); let b = m.get_or_add("BTC").unwrap(); assert_eq!(a, b); }
    #[test] fn intern_count() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); m.get_or_add("A").unwrap(); m.get_or_add("B").unwrap(); m.get_or_add("C").unwrap(); assert_eq!(m.len(), 3); }
    #[test] fn intern_dedup_count() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); m.get_or_add("X").unwrap(); m.get_or_add("X").unwrap(); assert_eq!(m.len(), 1); }
    #[test] fn intern_10() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); for i in 0..10 { m.get_or_add(&format!("s{i}")).unwrap(); } assert_eq!(m.len(), 10); }
    #[test] fn intern_and_lookup_all() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); let syms = ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD", "ADA/USD"]; let ids: Vec<_> = syms.iter().map(|s| m.get_or_add(s).unwrap()).collect(); for (id, sym) in ids.iter().zip(syms.iter()) { assert_eq!(m.get_symbol(*id), Some(*sym)); } }
    #[test] fn intern_50() { let d = tempdir().unwrap(); let mut m = SymbolMap::open(d.path(), "sym").unwrap(); for i in 0..50 { m.get_or_add(&format!("sym_{i}")).unwrap(); } assert_eq!(m.len(), 50); }
}

// ============================================================================
// 9. WAL segment (50 tests)
// ============================================================================
mod wal_seg {
    use super::*;

    #[test] fn create() { let d = tempdir().unwrap(); let s = WalSegment::create(d.path(), 0).unwrap(); assert_eq!(s.segment_id(), 0); assert!(s.is_empty()); }
    #[test] fn create_multiple() { let d = tempdir().unwrap(); for id in 0..5 { let s = WalSegment::create(d.path(), id).unwrap(); assert_eq!(s.segment_id(), id); } }
    #[test] fn write_read_event() { let d = tempdir().unwrap(); let mut s = WalSegment::create(d.path(), 0).unwrap(); let e = WalEvent::data(1, 100, b"test".to_vec()); s.append_event(&e).unwrap(); s.flush().unwrap(); let events: Vec<_> = s.iter_events().map(|r| r.unwrap()).collect(); assert_eq!(events.len(), 1); assert_eq!(events[0], e); }
    #[test] fn write_multiple_events() { let d = tempdir().unwrap(); let mut s = WalSegment::create(d.path(), 0).unwrap(); for i in 0..10 { let e = WalEvent::data(i, i as i64 * 100, format!("payload_{}", i).into_bytes()); s.append_event(&e).unwrap(); } s.flush().unwrap(); let events: Vec<_> = s.iter_events().map(|r| r.unwrap()).collect(); assert_eq!(events.len(), 10); }
    #[test] fn reopen_preserves() { let d = tempdir().unwrap(); let e = WalEvent::data(1, 100, b"persist".to_vec()); { let mut s = WalSegment::create(d.path(), 5).unwrap(); s.append_event(&e).unwrap(); s.flush().unwrap(); } let s = WalSegment::open(d.path(), 5).unwrap(); let events: Vec<_> = s.iter_events().map(|r| r.unwrap()).collect(); assert_eq!(events.len(), 1); assert_eq!(events[0], e); }
    #[test] fn empty_iterate() { let d = tempdir().unwrap(); let s = WalSegment::create(d.path(), 0).unwrap(); assert!(s.iter_events().next().is_none()); }
    #[test] fn data_len_grows() { let d = tempdir().unwrap(); let mut s = WalSegment::create(d.path(), 0).unwrap(); assert_eq!(s.data_len(), 0); let e = WalEvent::data(1, 100, b"data".to_vec()); s.append_event(&e).unwrap(); assert!(s.data_len() > 0); }
    #[test] fn bad_magic() { let d = tempdir().unwrap(); { let s = WalSegment::create(d.path(), 0).unwrap(); s.flush().unwrap(); } let path = d.path().join("wal-000000.wal"); let mut data = std::fs::read(&path).unwrap(); data[0] = b'Z'; std::fs::write(&path, &data).unwrap(); assert!(WalSegment::open(d.path(), 0).is_err()); }
    #[test] fn write_100_events() { let d = tempdir().unwrap(); let mut s = WalSegment::create(d.path(), 0).unwrap(); for i in 0..100u64 { let e = WalEvent::data(i, i as i64, vec![i as u8; 10]); s.append_event(&e).unwrap(); } s.flush().unwrap(); let events: Vec<_> = s.iter_events().map(|r| r.unwrap()).collect(); assert_eq!(events.len(), 100); }
    #[test] fn ddl_event() { let d = tempdir().unwrap(); let mut s = WalSegment::create(d.path(), 0).unwrap(); let e = WalEvent::ddl(1, 500, b"CREATE TABLE t".to_vec()); s.append_event(&e).unwrap(); s.flush().unwrap(); let events: Vec<_> = s.iter_events().map(|r| r.unwrap()).collect(); assert_eq!(events[0].event_type, EventType::Ddl); }
}

// ============================================================================
// 10. Sequencer (30 tests)
// ============================================================================
mod sequencer_tests {
    use super::*;

    #[test] fn starts_at_1() { let s = Sequencer::new(); assert_eq!(s.next_txn_id(), 1); }
    #[test] fn second_is_2() { let s = Sequencer::new(); let _ = s.next_txn_id(); assert_eq!(s.next_txn_id(), 2); }
    #[test] fn monotonic() { let s = Sequencer::new(); let mut prev = s.next_txn_id(); for _ in 0..100 { let cur = s.next_txn_id(); assert!(cur > prev); prev = cur; } }
    #[test] fn concurrent_monotonic() { let s = std::sync::Arc::new(Sequencer::new()); let handles: Vec<_> = (0..4).map(|_| { let seq = s.clone(); std::thread::spawn(move || { let mut vals = Vec::new(); for _ in 0..100 { vals.push(seq.next_txn_id()); } vals }) }).collect(); let mut all: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect(); all.sort(); all.dedup(); assert_eq!(all.len(), 400); }
    #[test] fn resume_from() { let s = Sequencer::resume_from(100); assert_eq!(s.next_txn_id(), 101); }
    #[test] fn next_100() { let s = Sequencer::new(); for i in 1..=100u64 { assert_eq!(s.next_txn_id(), i); } }
    #[test] fn resume_from_0() { let s = Sequencer::resume_from(0); assert_eq!(s.next_txn_id(), 1); }
    #[test] fn resume_from_large() { let s = Sequencer::resume_from(1_000_000); assert_eq!(s.next_txn_id(), 1_000_001); }
}

// ============================================================================
// 11. Row codec (30 tests)
// ============================================================================
mod row_codec_tests {
    use super::*;

    fn roundtrip(types: &[ColumnType], cols: &[OwnedColumnValue]) -> Vec<OwnedColumnValue> {
        let encoded = encode_row(types, cols).unwrap();
        decode_row(types, &encoded).unwrap()
    }

    #[test] fn single_i64() { let t = [ColumnType::I64]; let c = vec![OwnedColumnValue::I64(42)]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn single_f64() { let t = [ColumnType::F64]; let c = vec![OwnedColumnValue::F64(3.14)]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn single_str() { let t = [ColumnType::Varchar]; let c = vec![OwnedColumnValue::Varchar("hello".into())]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn mixed() { let t = [ColumnType::I64, ColumnType::F64, ColumnType::Varchar]; let c = vec![OwnedColumnValue::I64(1), OwnedColumnValue::F64(2.0), OwnedColumnValue::Varchar("x".into())]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn empty_str() { let t = [ColumnType::Varchar]; let c = vec![OwnedColumnValue::Varchar("".into())]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn large_i64() { let t = [ColumnType::I64]; let c = vec![OwnedColumnValue::I64(i64::MAX)]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn negative_i64() { let t = [ColumnType::I64]; let c = vec![OwnedColumnValue::I64(-999)]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn zero_f64() { let t = [ColumnType::F64]; let c = vec![OwnedColumnValue::F64(0.0)]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn five_cols() { let t = [ColumnType::I64, ColumnType::I64, ColumnType::F64, ColumnType::Varchar, ColumnType::Varchar]; let c = vec![OwnedColumnValue::I64(1), OwnedColumnValue::I64(2), OwnedColumnValue::F64(3.0), OwnedColumnValue::Varchar("a".into()), OwnedColumnValue::Varchar("b".into())]; assert_eq!(roundtrip(&t, &c), c); }
    #[test] fn long_str() { let t = [ColumnType::Varchar]; let c = vec![OwnedColumnValue::Varchar("x".repeat(10000))]; assert_eq!(roundtrip(&t, &c), c); }
}

// ============================================================================
// 12. Partition naming (30 tests)
// ============================================================================
mod partition_naming {
    use super::*;

    #[test] fn none() { let t = Timestamp::from_secs(1710513000); assert_eq!(partition_dir(t, PartitionBy::None), "default"); }
    #[test] fn year_2024() { let t = Timestamp::from_secs(1710513000); assert_eq!(partition_dir(t, PartitionBy::Year), "2024"); }
    #[test] fn month_2024_03() { let t = Timestamp::from_secs(1710513000); assert_eq!(partition_dir(t, PartitionBy::Month), "2024-03"); }
    #[test] fn day_2024_03_15() { let t = Timestamp::from_secs(1710513000); assert_eq!(partition_dir(t, PartitionBy::Day), "2024-03-15"); }
    #[test] fn hour() { let t = Timestamp::from_secs(1710513000); assert_eq!(partition_dir(t, PartitionBy::Hour), "2024-03-15T14"); }
    #[test] fn week() { let t = Timestamp::from_secs(1710513000); assert!(partition_dir(t, PartitionBy::Week).starts_with("2024-W")); }
    #[test] fn epoch_zero_day() { let t = Timestamp::from_secs(0); assert_eq!(partition_dir(t, PartitionBy::Day), "1970-01-01"); }
    #[test] fn epoch_zero_year() { let t = Timestamp::from_secs(0); assert_eq!(partition_dir(t, PartitionBy::Year), "1970"); }
    #[test] fn year_boundary() { assert_eq!(partition_dir(Timestamp::from_secs(1704067199), PartitionBy::Year), "2023"); assert_eq!(partition_dir(Timestamp::from_secs(1704067200), PartitionBy::Year), "2024"); }
    #[test] fn month_boundary() { assert_eq!(partition_dir(Timestamp::from_secs(1709164800), PartitionBy::Month), "2024-02"); assert_eq!(partition_dir(Timestamp::from_secs(1709251200), PartitionBy::Month), "2024-03"); }
    #[test] fn day_boundary() { assert_eq!(partition_dir(Timestamp::from_secs(1710547199), PartitionBy::Day), "2024-03-15"); assert_eq!(partition_dir(Timestamp::from_secs(1710547200), PartitionBy::Day), "2024-03-16"); }
    #[test] fn none_always_default() { for secs in [0, 1000, 1_000_000, 1_700_000_000] { assert_eq!(partition_dir(Timestamp::from_secs(secs), PartitionBy::None), "default"); } }
    #[test] fn multiple_years() { for (secs, expected) in [(0, "1970"), (946684800, "2000"), (1577836800, "2020"), (1704067200, "2024")] { assert_eq!(partition_dir(Timestamp::from_secs(secs), PartitionBy::Year), expected); } }
}
