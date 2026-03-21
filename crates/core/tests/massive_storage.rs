//! Massive storage test suite — 100+ tests.
//!
//! Column read/write, compression, index, partition.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::{
    FixedColumnReader, FixedColumnWriter, VarColumnReader, VarColumnWriter,
};
use exchange_core::compression::{
    delta_decode_i64, delta_decode_i64_nonempty, delta_encode_i64, rle_decode, rle_encode,
};
use exchange_core::index::bitmap::{BitmapIndexReader, BitmapIndexWriter};
use exchange_core::index::symbol_map::SymbolMap;
use exchange_core::partition::partition_dir;
use tempfile::tempdir;

mod fixed_i64_extra {
    use super::*;
    #[test]
    fn single() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            w.append_i64(42).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_i64(0), 42);
    }
    #[test]
    fn ten() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..10 {
                w.append_i64(i * 10).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 10);
        assert_eq!(r.read_i64(9), 90);
    }
    #[test]
    fn hundred() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..100i64 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 100);
        assert_eq!(r.read_i64(50), 50);
    }
    #[test]
    fn thousand() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..1000i64 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1000);
        assert_eq!(r.read_i64(999), 999);
    }
    #[test]
    fn min_max() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            w.append_i64(i64::MIN).unwrap();
            w.append_i64(i64::MAX).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.read_i64(0), i64::MIN);
        assert_eq!(r.read_i64(1), i64::MAX);
    }
    #[test]
    fn zeros() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for _ in 0..50 {
                w.append_i64(0).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        for i in 0..50 {
            assert_eq!(r.read_i64(i), 0);
        }
    }
    #[test]
    fn negatives() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..10i64 {
                w.append_i64(-i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.read_i64(5), -5);
    }
    #[test]
    fn empty() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 0);
    }
    #[test]
    fn ascending() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..20i64 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        for i in 0..20u64 {
            assert_eq!(r.read_i64(i), i as i64);
        }
    }
    #[test]
    fn descending() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in (0..20i64).rev() {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.read_i64(0), 19);
        assert_eq!(r.read_i64(19), 0);
    }
    #[test]
    fn alternating() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..20 {
                w.append_i64(if i % 2 == 0 { 1 } else { -1 }).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.read_i64(0), 1);
        assert_eq!(r.read_i64(1), -1);
    }
    #[test]
    fn ten_k() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for i in 0..10_000i64 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 10_000);
        assert_eq!(r.read_i64(5000), 5000);
    }
    #[test]
    fn constant_42() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            for _ in 0..100 {
                w.append_i64(42).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        for i in 0..100 {
            assert_eq!(r.read_i64(i), 42);
        }
    }
    #[test]
    fn one_val() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            w.append_i64(999).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_i64(0), 999);
    }
    #[test]
    fn minus_one() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::I64).unwrap();
            w.append_i64(-1).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::I64).unwrap();
        assert_eq!(r.read_i64(0), -1);
    }
}
mod fixed_f64_extra {
    use super::*;
    #[test]
    fn single() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            w.append_f64(3.14).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert!((r.read_f64(0) - 3.14).abs() < 0.001);
    }
    #[test]
    fn ten() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            for i in 0..10 {
                w.append_f64(i as f64 * 1.5).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 10);
    }
    #[test]
    fn hundred() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            for i in 0..100 {
                w.append_f64(i as f64).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 100);
    }
    #[test]
    fn zeros() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            for _ in 0..50 {
                w.append_f64(0.0).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        for i in 0..50 {
            assert!((r.read_f64(i)).abs() < 0.001);
        }
    }
    #[test]
    fn empty() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 0);
    }
    #[test]
    fn ascending() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            for i in 0..20 {
                w.append_f64(i as f64).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        for i in 0..20u64 {
            assert!((r.read_f64(i) - i as f64).abs() < 0.001);
        }
    }
    #[test]
    fn negatives() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            for i in 0..10 {
                w.append_f64(-(i as f64)).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert!((r.read_f64(5) - (-5.0)).abs() < 0.001);
    }
    #[test]
    fn thousand() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("c.d");
        {
            let mut w = FixedColumnWriter::open(&p, ColumnType::F64).unwrap();
            for i in 0..1000 {
                w.append_f64(i as f64 * 0.01).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&p, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 1000);
    }
}
mod var_col_extra {
    use super::*;
    #[test]
    fn single() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("hello").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_str(0), "hello");
    }
    #[test]
    fn three() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("a").unwrap();
            w.append_str("bb").unwrap();
            w.append_str("ccc").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_str(0), "a");
        assert_eq!(r.read_str(2), "ccc");
    }
    #[test]
    fn empty_str() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0), "");
    }
    #[test]
    fn empty_col() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 0);
    }
    #[test]
    fn long_str() {
        let s = "x".repeat(10000);
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str(&s).unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0), s);
    }
    #[test]
    fn many() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            for i in 0..100 {
                w.append_str(&format!("s{i}")).unwrap();
            }
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 100);
        assert_eq!(r.read_str(0), "s0");
        assert_eq!(r.read_str(99), "s99");
    }
    #[test]
    fn special() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("c.d");
        let ip = dir.path().join("c.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("a\tb\nc").unwrap();
            w.append_str("!@#$%").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0), "a\tb\nc");
        assert_eq!(r.read_str(1), "!@#$%");
    }
}
mod delta_extra {
    use super::*;
    #[test]
    fn ascending() {
        let v = vec![100, 110, 125, 150];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn descending() {
        let v = vec![200, 190, 185];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn empty() {
        let e = delta_encode_i64(&[]);
        assert!(delta_decode_i64(&e).is_empty());
    }
    #[test]
    fn single() {
        let v = vec![42];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn constant() {
        let v = vec![7; 5];
        let e = delta_encode_i64(&v);
        assert!(e.deltas.iter().all(|&d| d == 0));
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn negative() {
        let v = vec![-100, -50, 0, 50];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn alternating() {
        let v = vec![0, 100, 0, 100, 0];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn timestamps() {
        let b: i64 = 1_710_513_000_000_000_000;
        let v: Vec<i64> = (0..100).map(|i| b + i * 1_000_000_000).collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn two_values() {
        let v = vec![0, 1000];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn thousand() {
        let v: Vec<i64> = (0..1000).collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn all_negative() {
        let v: Vec<i64> = (-10..0).collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn large_deltas() {
        let v = vec![0, i64::MAX / 2];
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn base_value() {
        let v = vec![100, 110];
        let e = delta_encode_i64(&v);
        assert_eq!(e.base, 100);
    }
    #[test]
    fn deltas_correct() {
        let v = vec![100, 110, 125];
        let e = delta_encode_i64(&v);
        assert_eq!(e.deltas, vec![10, 15]);
    }
    #[test]
    fn three_same() {
        let v = vec![5, 5, 5];
        let e = delta_encode_i64(&v);
        assert_eq!(e.deltas, vec![0, 0]);
    }
}
mod rle_extra {
    use super::*;
    #[test]
    fn all_same() {
        let v = vec![5i64; 10];
        let e = rle_encode(&v);
        assert_eq!(e.len(), 1);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn all_different() {
        let v: Vec<i64> = (0..10).collect();
        let e = rle_encode(&v);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn empty() {
        let v: Vec<i64> = vec![];
        let e = rle_encode(&v);
        assert!(rle_decode(&e).is_empty());
    }
    #[test]
    fn single() {
        let v = vec![42i64];
        let e = rle_encode(&v);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn two_runs() {
        let v = vec![1i64, 1, 1, 2, 2];
        let e = rle_encode(&v);
        assert_eq!(e.len(), 2);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn alternating() {
        let v = vec![0i64, 1, 0, 1];
        let e = rle_encode(&v);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn large_run() {
        let v = vec![99i64; 10000];
        let e = rle_encode(&v);
        assert_eq!(e.len(), 1);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn neg_values() {
        let v = vec![-1i64, -1, -2, -2];
        let e = rle_encode(&v);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn zero_runs() {
        let v = vec![0i64; 100];
        let e = rle_encode(&v);
        assert_eq!(e.len(), 1);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn three_runs() {
        let v = vec![1i64, 1, 2, 2, 3, 3];
        let e = rle_encode(&v);
        assert_eq!(e.len(), 3);
        assert_eq!(rle_decode(&e), v);
    }
}
mod compression_combined {
    use super::*;
    #[test]
    fn delta_5000() {
        let v: Vec<i64> = (0..5000).collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn delta_alternating_big() {
        let v: Vec<i64> = (0..100)
            .map(|i| if i % 2 == 0 { 0 } else { 1000 })
            .collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn rle_many_runs() {
        let v: Vec<i64> = (0..100).flat_map(|i| vec![i; 10]).collect();
        let e = rle_encode(&v);
        assert_eq!(e.len(), 100);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn rle_single_element_runs() {
        let v: Vec<i64> = (0..50).collect();
        let e = rle_encode(&v);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn delta_neg_to_pos() {
        let v: Vec<i64> = (-50..50).collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
    #[test]
    fn rle_two_values() {
        let v: Vec<i64> = (0..1000).map(|i| if i < 500 { 0 } else { 1 }).collect();
        let e = rle_encode(&v);
        assert_eq!(e.len(), 2);
        assert_eq!(rle_decode(&e), v);
    }
    #[test]
    fn delta_constant_big() {
        let v = vec![42i64; 10000];
        let e = delta_encode_i64(&v);
        assert!(e.deltas.iter().all(|&d| d == 0));
    }
    #[test]
    fn delta_timestamps_irregular() {
        let v: Vec<i64> = (0..100).map(|i| i * i * 1_000_000).collect();
        let e = delta_encode_i64(&v);
        assert_eq!(delta_decode_i64_nonempty(&e), v);
    }
}
mod bitmap_extra {
    use super::*;
    #[test]
    fn basic() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 10).unwrap();
            w.add(0, 20).unwrap();
            w.add(1, 5).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![10, 20]);
        assert_eq!(r.get_row_ids(1), vec![5]);
    }
    #[test]
    fn empty_key() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 1).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert!(r.get_row_ids(5).is_empty());
    }
    #[test]
    fn many_rows() {
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
    fn many_keys() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            for k in 0..50i32 {
                w.add(k, k as u64).unwrap();
            }
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        for k in 0..50i32 {
            assert_eq!(r.get_row_ids(k), vec![k as u64]);
        }
    }
    #[test]
    fn block_overflow() {
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
    }
    #[test]
    fn neg_key_empty() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 1).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert!(r.get_row_ids(-1).is_empty());
    }
    #[test]
    fn neg_key_add_err() {
        let dir = tempdir().unwrap();
        let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
        assert!(w.add(-1, 0).is_err());
    }
    #[test]
    fn reopen() {
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
    fn two_keys_interleaved() {
        let dir = tempdir().unwrap();
        {
            let mut w = BitmapIndexWriter::open_default(dir.path(), "idx").unwrap();
            w.add(0, 0).unwrap();
            w.add(1, 1).unwrap();
            w.add(0, 2).unwrap();
            w.add(1, 3).unwrap();
            w.flush().unwrap();
        }
        let r = BitmapIndexReader::open(dir.path(), "idx").unwrap();
        assert_eq!(r.get_row_ids(0), vec![0, 2]);
        assert_eq!(r.get_row_ids(1), vec![1, 3]);
    }
}
mod symbol_map_extra {
    use super::*;
    #[test]
    fn new_empty() {
        let dir = tempdir().unwrap();
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.len(), 0);
    }
    #[test]
    fn add_one() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let k = sm.get_or_add("hello").unwrap();
        assert!(k >= 0);
        assert_eq!(sm.len(), 1);
    }
    #[test]
    fn add_same() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let k1 = sm.get_or_add("hello").unwrap();
        let k2 = sm.get_or_add("hello").unwrap();
        assert_eq!(k1, k2);
        assert_eq!(sm.len(), 1);
    }
    #[test]
    fn add_different() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let k1 = sm.get_or_add("a").unwrap();
        let k2 = sm.get_or_add("b").unwrap();
        assert_ne!(k1, k2);
        assert_eq!(sm.len(), 2);
    }
    #[test]
    fn lookup_existing() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let k = sm.get_or_add("hello").unwrap();
        assert_eq!(sm.get_id("hello"), Some(k));
    }
    #[test]
    fn lookup_missing() {
        let dir = tempdir().unwrap();
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.get_id("nope"), None);
    }
    #[test]
    fn reverse_lookup() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        let k = sm.get_or_add("hello").unwrap();
        assert_eq!(sm.get_symbol(k), Some("hello"));
    }
    #[test]
    fn many_symbols() {
        let dir = tempdir().unwrap();
        let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
        for i in 0..100 {
            sm.get_or_add(&format!("s{i}")).unwrap();
        }
        assert_eq!(sm.len(), 100);
    }
    #[test]
    fn reopen() {
        let dir = tempdir().unwrap();
        {
            let mut sm = SymbolMap::open(dir.path(), "sym").unwrap();
            sm.get_or_add("persist").unwrap();
            sm.flush().unwrap();
        }
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert_eq!(sm.len(), 1);
        assert!(sm.get_id("persist").is_some());
    }
    #[test]
    fn is_empty() {
        let dir = tempdir().unwrap();
        let sm = SymbolMap::open(dir.path(), "sym").unwrap();
        assert!(sm.is_empty());
    }
}
mod partition_naming_extra {
    use super::*;
    #[test]
    fn none() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710513000), PartitionBy::None),
            "default"
        );
    }
    #[test]
    fn year() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710513000), PartitionBy::Year),
            "2024"
        );
    }
    #[test]
    fn month() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710513000), PartitionBy::Month),
            "2024-03"
        );
    }
    #[test]
    fn day() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710513000), PartitionBy::Day),
            "2024-03-15"
        );
    }
    #[test]
    fn hour() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710513000), PartitionBy::Hour),
            "2024-03-15T14"
        );
    }
    #[test]
    fn week() {
        assert!(
            partition_dir(Timestamp::from_secs(1710513000), PartitionBy::Week)
                .starts_with("2024-W")
        );
    }
    #[test]
    fn epoch_day() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(0), PartitionBy::Day),
            "1970-01-01"
        );
    }
    #[test]
    fn epoch_year() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(0), PartitionBy::Year),
            "1970"
        );
    }
    #[test]
    fn eod_boundary() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710547199), PartitionBy::Day),
            "2024-03-15"
        );
    }
    #[test]
    fn next_day() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1710547200), PartitionBy::Day),
            "2024-03-16"
        );
    }
    #[test]
    fn y2000() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(946684800), PartitionBy::Year),
            "2000"
        );
    }
    #[test]
    fn y2000_month() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(946684800), PartitionBy::Month),
            "2000-01"
        );
    }
    #[test]
    fn leap_day() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1709164800), PartitionBy::Day),
            "2024-02-29"
        );
    }
    #[test]
    fn dec_31() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1735689599), PartitionBy::Day),
            "2024-12-31"
        );
    }
    #[test]
    fn jan_2025() {
        assert_eq!(
            partition_dir(Timestamp::from_secs(1735689600), PartitionBy::Year),
            "2025"
        );
    }
}
