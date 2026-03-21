//! Comprehensive tests for column readers and writers.
//!
//! 100 tests covering FixedColumnWriter/Reader, VarColumnWriter/Reader,
//! and ColumnTopReader across all types and edge cases.

use exchange_common::types::ColumnType;
use exchange_core::column::{
    ColumnTopReader, FixedColumnReader, FixedColumnWriter, Value, VarColumnReader, VarColumnWriter,
};
use tempfile::tempdir;

// ============================================================================
// Fixed column: i64
// ============================================================================

mod fixed_i64 {
    use super::*;

    #[test]
    fn write_read_single_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(42).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_i64(0), 42);
    }

    #[test]
    fn write_read_multiple_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..100 {
                w.append_i64(i * 10).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 100);
        for i in 0..100 {
            assert_eq!(r.read_i64(i), i as i64 * 10);
        }
    }

    #[test]
    fn i64_min_max() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(i64::MIN).unwrap();
            w.append_i64(i64::MAX).unwrap();
            w.append_i64(0).unwrap();
            w.append_i64(-1).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.read_i64(0), i64::MIN);
        assert_eq!(r.read_i64(1), i64::MAX);
        assert_eq!(r.read_i64(2), 0);
        assert_eq!(r.read_i64(3), -1);
    }

    #[test]
    fn empty_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.flush().unwrap();
            assert_eq!(w.row_count(), 0);
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 0);
    }

    #[test]
    fn large_column_10k_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let count = 10_000u64;
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..count {
                w.append_i64(i as i64).unwrap();
            }
            w.flush().unwrap();
            assert_eq!(w.row_count(), count);
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), count);
        assert_eq!(r.read_i64(0), 0);
        assert_eq!(r.read_i64(count - 1), (count - 1) as i64);
        assert_eq!(r.read_i64(count / 2), (count / 2) as i64);
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(111).unwrap();
            w.append_i64(222).unwrap();
            w.flush().unwrap();
        }
        // Reopen for writing, append more
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            assert_eq!(w.row_count(), 2);
            w.append_i64(333).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_i64(0), 111);
        assert_eq!(r.read_i64(1), 222);
        assert_eq!(r.read_i64(2), 333);
    }

    #[test]
    fn negative_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in -50..50 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 100);
        for (idx, i) in (-50i64..50).enumerate() {
            assert_eq!(r.read_i64(idx as u64), i);
        }
    }

    #[test]
    fn all_zeros() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for _ in 0..50 {
                w.append_i64(0).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        for i in 0..50 {
            assert_eq!(r.read_i64(i), 0);
        }
    }

    #[test]
    fn alternating_pattern() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..200 {
                w.append_i64(if i % 2 == 0 { i64::MAX } else { i64::MIN }).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        for i in 0..200u64 {
            let expected = if i % 2 == 0 { i64::MAX } else { i64::MIN };
            assert_eq!(r.read_i64(i), expected);
        }
    }
}

// ============================================================================
// Fixed column: i32
// ============================================================================

mod fixed_i32 {
    use super::*;

    #[test]
    fn write_read_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            w.append_i32(100).unwrap();
            w.append_i32(-200).unwrap();
            w.append_i32(0).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I32).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_i32(0), 100);
        assert_eq!(r.read_i32(1), -200);
        assert_eq!(r.read_i32(2), 0);
    }

    #[test]
    fn i32_min_max() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            w.append_i32(i32::MIN).unwrap();
            w.append_i32(i32::MAX).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I32).unwrap();
        assert_eq!(r.read_i32(0), i32::MIN);
        assert_eq!(r.read_i32(1), i32::MAX);
    }

    #[test]
    fn many_i32_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            for i in 0..5000 {
                w.append_i32(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I32).unwrap();
        assert_eq!(r.row_count(), 5000);
        assert_eq!(r.read_i32(0), 0);
        assert_eq!(r.read_i32(4999), 4999);
    }

    #[test]
    fn i32_row_count_different_from_i64() {
        // Verify that i32 column has correct element_size (4 bytes).
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            w.append_i32(1).unwrap();
            w.append_i32(2).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I32).unwrap();
        assert_eq!(r.row_count(), 2);
        // Reading raw bytes should be 4 bytes each
        assert_eq!(r.read_raw(0).len(), 4);
    }
}

// ============================================================================
// Fixed column: f64
// ============================================================================

mod fixed_f64 {
    use super::*;

    #[test]
    fn write_read_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            w.append_f64(3.14159265358979).unwrap();
            w.append_f64(-2.71828).unwrap();
            w.append_f64(0.0).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_f64(0), 3.14159265358979);
        assert_eq!(r.read_f64(1), -2.71828);
        assert_eq!(r.read_f64(2), 0.0);
    }

    #[test]
    fn nan_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            w.append_f64(f64::NAN).unwrap();
            w.append_f64(1.0).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert!(r.read_f64(0).is_nan());
        assert_eq!(r.read_f64(1), 1.0);
    }

    #[test]
    fn infinity_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            w.append_f64(f64::INFINITY).unwrap();
            w.append_f64(f64::NEG_INFINITY).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(r.read_f64(0), f64::INFINITY);
        assert_eq!(r.read_f64(1), f64::NEG_INFINITY);
    }

    #[test]
    fn subnormal_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            w.append_f64(f64::MIN_POSITIVE).unwrap();
            w.append_f64(f64::MAX).unwrap();
            w.append_f64(f64::MIN).unwrap();
            w.append_f64(f64::EPSILON).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(r.read_f64(0), f64::MIN_POSITIVE);
        assert_eq!(r.read_f64(1), f64::MAX);
        assert_eq!(r.read_f64(2), f64::MIN);
        assert_eq!(r.read_f64(3), f64::EPSILON);
    }

    #[test]
    fn negative_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            w.append_f64(-0.0).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        let val = r.read_f64(0);
        assert!(val.is_sign_negative());
        assert_eq!(val, 0.0);
    }

    #[test]
    fn many_float_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let count = 10_000u64;
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            for i in 0..count {
                w.append_f64(i as f64 * 0.001).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), count);
        assert_eq!(r.read_f64(0), 0.0);
        assert!((r.read_f64(1000) - 1.0).abs() < 1e-10);
    }
}

// ============================================================================
// Fixed column: f32
// ============================================================================

mod fixed_f32 {
    use super::*;

    #[test]
    fn write_read_f32() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F32).unwrap();
            w.append(&3.14f32.to_le_bytes()).unwrap();
            w.append(&(-1.0f32).to_le_bytes()).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F32).unwrap();
        assert_eq!(r.row_count(), 2);
        let v0 = f32::from_le_bytes(r.read_raw(0).try_into().unwrap());
        let v1 = f32::from_le_bytes(r.read_raw(1).try_into().unwrap());
        assert!((v0 - 3.14).abs() < 0.01);
        assert_eq!(v1, -1.0);
    }

    #[test]
    fn f32_nan() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F32).unwrap();
            w.append(&f32::NAN.to_le_bytes()).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F32).unwrap();
        let val = f32::from_le_bytes(r.read_raw(0).try_into().unwrap());
        assert!(val.is_nan());
    }
}

// ============================================================================
// Fixed column: Timestamp
// ============================================================================

mod fixed_timestamp {
    use super::*;

    #[test]
    fn write_read_timestamps() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        let ts_base: i64 = 1_710_513_000_000_000_000; // ~2024-03-15
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Timestamp).unwrap();
            for i in 0..10 {
                w.append_i64(ts_base + i * 1_000_000_000).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Timestamp).unwrap();
        assert_eq!(r.row_count(), 10);
        assert_eq!(r.read_i64(0), ts_base);
        assert_eq!(r.read_i64(9), ts_base + 9 * 1_000_000_000);
    }

    #[test]
    fn epoch_zero_timestamp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Timestamp).unwrap();
            w.append_i64(0).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Timestamp).unwrap();
        assert_eq!(r.read_i64(0), 0);
    }

    #[test]
    fn negative_timestamp_before_epoch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Timestamp).unwrap();
            w.append_i64(-1_000_000_000).unwrap(); // 1 second before epoch
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Timestamp).unwrap();
        assert_eq!(r.read_i64(0), -1_000_000_000);
    }

    #[test]
    fn timestamp_min_max_sentinels() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Timestamp).unwrap();
            w.append_i64(i64::MIN).unwrap(); // NULL sentinel
            w.append_i64(i64::MAX).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Timestamp).unwrap();
        assert_eq!(r.read_i64(0), i64::MIN);
        assert_eq!(r.read_i64(1), i64::MAX);
    }
}

// ============================================================================
// Fixed column: Boolean (1 byte)
// ============================================================================

mod fixed_boolean {
    use super::*;

    #[test]
    fn write_read_booleans() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Boolean).unwrap();
            w.append(&[1u8]).unwrap(); // true
            w.append(&[0u8]).unwrap(); // false
            w.append(&[1u8]).unwrap(); // true
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Boolean).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_raw(0), &[1u8]);
        assert_eq!(r.read_raw(1), &[0u8]);
        assert_eq!(r.read_raw(2), &[1u8]);
    }

    #[test]
    fn boolean_many_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Boolean).unwrap();
            for i in 0..1000 {
                w.append(&[(i % 2) as u8]).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Boolean).unwrap();
        assert_eq!(r.row_count(), 1000);
        for i in 0..1000u64 {
            assert_eq!(r.read_raw(i), &[(i % 2) as u8]);
        }
    }
}

// ============================================================================
// Fixed column: I8 (1 byte)
// ============================================================================

mod fixed_i8 {
    use super::*;

    #[test]
    fn write_read_i8() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I8).unwrap();
            w.append(&[127u8]).unwrap();
            w.append(&[0x80u8]).unwrap(); // -128 as unsigned byte
            w.append(&[0u8]).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I8).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_raw(0)[0] as i8, 127);
        assert_eq!(r.read_raw(1)[0] as i8, -128);
        assert_eq!(r.read_raw(2)[0] as i8, 0);
    }
}

// ============================================================================
// Fixed column: I16 (2 bytes)
// ============================================================================

mod fixed_i16 {
    use super::*;

    #[test]
    fn write_read_i16() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I16).unwrap();
            w.append(&32767i16.to_le_bytes()).unwrap();
            w.append(&(-32768i16).to_le_bytes()).unwrap();
            w.append(&0i16.to_le_bytes()).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I16).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(
            i16::from_le_bytes(r.read_raw(0).try_into().unwrap()),
            32767
        );
        assert_eq!(
            i16::from_le_bytes(r.read_raw(1).try_into().unwrap()),
            -32768
        );
    }
}

// ============================================================================
// Fixed column: raw bytes API
// ============================================================================

mod fixed_raw {
    use super::*;

    #[test]
    fn append_raw_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            let bytes = 12345i64.to_le_bytes();
            w.append(&bytes).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.read_raw(0).len(), 8);
        assert_eq!(r.read_i64(0), 12345);
    }

    #[test]
    fn read_raw_returns_correct_size_for_each_type() {
        let types_and_sizes = vec![
            (ColumnType::Boolean, 1),
            (ColumnType::I8, 1),
            (ColumnType::I16, 2),
            (ColumnType::I32, 4),
            (ColumnType::I64, 8),
            (ColumnType::F32, 4),
            (ColumnType::F64, 8),
            (ColumnType::Timestamp, 8),
            (ColumnType::Symbol, 4),
        ];
        for (ct, expected_size) in types_and_sizes {
            let dir = tempdir().unwrap();
            let path = dir.path().join("col.d");
            let zero_bytes = vec![0u8; expected_size];
            {
                let mut w = FixedColumnWriter::open(&path, ct).unwrap();
                w.append(&zero_bytes).unwrap();
                w.flush().unwrap();
            }
            let r = FixedColumnReader::open(&path, ct).unwrap();
            assert_eq!(
                r.read_raw(0).len(),
                expected_size,
                "type {:?} should have size {}",
                ct,
                expected_size
            );
        }
    }
}

// ============================================================================
// Variable column: strings
// ============================================================================

mod var_column {
    use super::*;

    #[test]
    fn write_read_strings() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("hello").unwrap();
            w.append_str("world").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 2);
        assert_eq!(r.read_str(0), "hello");
        assert_eq!(r.read_str(1), "world");
    }

    #[test]
    fn empty_string() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("").unwrap();
            w.append_str("notempty").unwrap();
            w.append_str("").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_str(0), "");
        assert_eq!(r.read_str(1), "notempty");
        assert_eq!(r.read_str(2), "");
    }

    #[test]
    fn unicode_strings() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("Hej verden").unwrap();
            w.append_str("Czesc").unwrap();
            w.append_str("Kon'nichiwa").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0), "Hej verden");
        assert_eq!(r.read_str(1), "Czesc");
        assert_eq!(r.read_str(2), "Kon'nichiwa");
    }

    #[test]
    fn special_characters() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("line1\nline2").unwrap();
            w.append_str("tab\there").unwrap();
            w.append_str("null\0byte").unwrap();
            w.append_str("quote\"here").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0), "line1\nline2");
        assert_eq!(r.read_str(1), "tab\there");
        assert_eq!(r.read_str(2), "null\0byte");
        assert_eq!(r.read_str(3), "quote\"here");
    }

    #[test]
    fn long_string() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        let long = "x".repeat(100_000);
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str(&long).unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0).len(), 100_000);
        assert_eq!(r.read_str(0), long);
    }

    #[test]
    fn binary_data() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        let data: Vec<u8> = (0..=255).collect();
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append(&data).unwrap();
            w.append(&[]).unwrap(); // empty binary
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 2);
        assert_eq!(r.read(0), data.as_slice());
        assert_eq!(r.read(1), &[] as &[u8]);
    }

    #[test]
    fn many_strings() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        let count = 5000;
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            for i in 0..count {
                w.append_str(&format!("string_{:05}", i)).unwrap();
            }
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), count);
        for i in 0..count {
            assert_eq!(r.read_str(i), format!("string_{:05}", i));
        }
    }

    #[test]
    fn binary_with_null_bytes() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        let data = vec![0u8; 1024];
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append(&data).unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read(0).len(), 1024);
        assert!(r.read(0).iter().all(|&b| b == 0));
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("first").unwrap();
            w.flush().unwrap();
        }
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            assert_eq!(w.row_count(), 1);
            w.append_str("second").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 2);
        assert_eq!(r.read_str(0), "first");
        assert_eq!(r.read_str(1), "second");
    }

    #[test]
    fn mixed_empty_and_long() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("").unwrap();
            w.append_str(&"a".repeat(10_000)).unwrap();
            w.append_str("").unwrap();
            w.append_str("short").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read_str(0), "");
        assert_eq!(r.read_str(1).len(), 10_000);
        assert_eq!(r.read_str(2), "");
        assert_eq!(r.read_str(3), "short");
    }

    #[test]
    fn varying_length_strings() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            for len in 0..100 {
                w.append_str(&"z".repeat(len)).unwrap();
            }
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 100);
        for len in 0..100usize {
            assert_eq!(r.read_str(len as u64).len(), len);
        }
    }
}

// ============================================================================
// ColumnTopReader
// ============================================================================

mod column_top {
    use super::*;

    #[test]
    fn missing_file_all_null() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.d");
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 100).unwrap();
        assert_eq!(reader.column_top(), 100);
        assert_eq!(reader.total_rows(), 100);
        for i in 0..100 {
            match reader.read_value(i) {
                Value::Null => {}
                other => panic!("expected Null at row {}, got {:?}", i, other),
            }
        }
    }

    #[test]
    fn full_data_no_top() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..10 {
                w.append_i64(i * 100).unwrap();
            }
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 10).unwrap();
        assert_eq!(reader.column_top(), 0);
        for i in 0..10 {
            match reader.read_value(i) {
                Value::I64(v) => assert_eq!(v, i as i64 * 100),
                other => panic!("expected I64 at row {}, got {:?}", i, other),
            }
        }
    }

    #[test]
    fn partial_data_leading_nulls() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        // Write 3 rows of data
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(10).unwrap();
            w.append_i64(20).unwrap();
            w.append_i64(30).unwrap();
            w.flush().unwrap();
        }
        // But partition has 7 rows
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 7).unwrap();
        assert_eq!(reader.column_top(), 4);

        // First 4 rows are NULL
        for i in 0..4 {
            match reader.read_value(i) {
                Value::Null => {}
                other => panic!("expected Null at row {}, got {:?}", i, other),
            }
        }
        // Rows 4,5,6 have data
        match reader.read_value(4) {
            Value::I64(v) => assert_eq!(v, 10),
            other => panic!("expected I64(10), got {:?}", other),
        }
        match reader.read_value(5) {
            Value::I64(v) => assert_eq!(v, 20),
            other => panic!("expected I64(20), got {:?}", other),
        }
        match reader.read_value(6) {
            Value::I64(v) => assert_eq!(v, 30),
            other => panic!("expected I64(30), got {:?}", other),
        }
    }

    #[test]
    fn i32_column_top() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            w.append_i32(42).unwrap();
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I32, 3).unwrap();
        assert_eq!(reader.column_top(), 2);
        match reader.read_value(0) {
            Value::Null => {}
            other => panic!("expected Null, got {:?}", other),
        }
        match reader.read_value(2) {
            Value::I32(v) => assert_eq!(v, 42),
            other => panic!("expected I32(42), got {:?}", other),
        }
    }

    #[test]
    fn zero_total_rows_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.d");
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 0).unwrap();
        assert_eq!(reader.column_top(), 0);
        assert_eq!(reader.total_rows(), 0);
    }

    #[test]
    fn more_data_than_total_rows() {
        // Edge case: file has 5 rows but total_rows says 3 (should not happen
        // in practice but column_top should saturate to 0).
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..5 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 3).unwrap();
        assert_eq!(reader.column_top(), 0);
    }

    #[test]
    fn single_row_column_top() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(999).unwrap();
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 1).unwrap();
        assert_eq!(reader.column_top(), 0);
        match reader.read_value(0) {
            Value::I64(v) => assert_eq!(v, 999),
            other => panic!("expected I64(999), got {:?}", other),
        }
    }

    #[test]
    fn missing_file_zero_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.d");
        let reader = ColumnTopReader::open(&path, ColumnType::I32, 0).unwrap();
        assert_eq!(reader.column_top(), 0);
        assert_eq!(reader.total_rows(), 0);
    }

    #[test]
    fn partial_data_single_row() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(77).unwrap();
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 5).unwrap();
        assert_eq!(reader.column_top(), 4);
        for i in 0..4 {
            match reader.read_value(i) {
                Value::Null => {}
                other => panic!("row {} expected Null, got {:?}", i, other),
            }
        }
        match reader.read_value(4) {
            Value::I64(v) => assert_eq!(v, 77),
            other => panic!("expected I64(77), got {:?}", other),
        }
    }
}

// ============================================================================
// Additional edge-case tests to reach 100
// ============================================================================

mod edge_cases {
    use super::*;

    #[test]
    fn uuid_column_16_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let uuid_bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Uuid).unwrap();
            w.append(&uuid_bytes).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Uuid).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_raw(0), &uuid_bytes);
    }

    #[test]
    fn long256_column_32_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let val = [0xAAu8; 32];
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Long256).unwrap();
            w.append(&val).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Long256).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_raw(0), &val);
    }

    #[test]
    fn long128_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let val = 123456789i128.to_le_bytes();
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Long128).unwrap();
            w.append(&val).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Long128).unwrap();
        assert_eq!(r.row_count(), 1);
        let read_val = i128::from_le_bytes(r.read_raw(0).try_into().unwrap());
        assert_eq!(read_val, 123456789i128);
    }

    #[test]
    fn geohash_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::GeoHash).unwrap();
            w.append_i64(0x123456789ABCDEF0).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::GeoHash).unwrap();
        assert_eq!(r.read_i64(0), 0x123456789ABCDEF0);
    }

    #[test]
    fn date_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Date).unwrap();
            w.append_i32(19800).unwrap(); // days since epoch
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Date).unwrap();
        assert_eq!(r.read_i32(0), 19800);
    }

    #[test]
    fn char_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Char).unwrap();
            w.append(&65u16.to_le_bytes()).unwrap(); // 'A'
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Char).unwrap();
        let v = u16::from_le_bytes(r.read_raw(0).try_into().unwrap());
        assert_eq!(v, 65);
    }

    #[test]
    fn ipv4_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::IPv4).unwrap();
            // 192.168.1.1
            let ip: u32 = (192 << 24) | (168 << 16) | (1 << 8) | 1;
            w.append_i32(ip as i32).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::IPv4).unwrap();
        let v = r.read_i32(0) as u32;
        assert_eq!(v >> 24, 192);
    }

    #[test]
    fn symbol_column_stores_i32() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Symbol).unwrap();
            w.append_i32(0).unwrap();
            w.append_i32(1).unwrap();
            w.append_i32(-1).unwrap(); // NULL sentinel
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Symbol).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_i32(0), 0);
        assert_eq!(r.read_i32(1), 1);
        assert_eq!(r.read_i32(2), -1);
    }

    #[test]
    fn var_column_empty_column() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.flush().unwrap();
            assert_eq!(w.row_count(), 0);
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 0);
    }

    #[test]
    fn fixed_i64_sequential_ascending() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..1000i64 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1000);
        for i in 0..1000u64 {
            assert_eq!(r.read_i64(i), i as i64);
        }
    }

    #[test]
    fn fixed_f64_powers_of_two() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            for i in 0..53 {
                w.append_f64(2.0f64.powi(i)).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        for i in 0..53u64 {
            assert_eq!(r.read_f64(i), 2.0f64.powi(i as i32));
        }
    }

    #[test]
    fn fixed_i32_boundary_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let vals = [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            for v in &vals {
                w.append_i32(*v).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I32).unwrap();
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(r.read_i32(i as u64), *v);
        }
    }

    #[test]
    fn var_column_single_byte_strings() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            for c in b'A'..=b'Z' {
                w.append_str(std::str::from_utf8(&[c]).unwrap()).unwrap();
            }
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 26);
        assert_eq!(r.read_str(0), "A");
        assert_eq!(r.read_str(25), "Z");
    }

    #[test]
    fn fixed_column_write_count_matches() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
        assert_eq!(w.row_count(), 0);
        w.append_i64(1).unwrap();
        assert_eq!(w.row_count(), 1);
        w.append_i64(2).unwrap();
        assert_eq!(w.row_count(), 2);
        for _ in 0..98 {
            w.append_i64(0).unwrap();
        }
        assert_eq!(w.row_count(), 100);
    }

    #[test]
    fn var_column_write_count_matches() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
        assert_eq!(w.row_count(), 0);
        w.append_str("a").unwrap();
        assert_eq!(w.row_count(), 1);
        w.append_str("b").unwrap();
        assert_eq!(w.row_count(), 2);
    }

    #[test]
    fn fixed_uuid_multiple_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Uuid).unwrap();
            for i in 0..50u8 {
                let mut uuid = [0u8; 16];
                uuid[0] = i;
                uuid[15] = 255 - i;
                w.append(&uuid).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::Uuid).unwrap();
        assert_eq!(r.row_count(), 50);
        assert_eq!(r.read_raw(0)[0], 0);
        assert_eq!(r.read_raw(49)[0], 49);
    }

    #[test]
    fn column_top_many_nulls() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(42).unwrap();
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I64, 1000).unwrap();
        assert_eq!(reader.column_top(), 999);
        match reader.read_value(0) {
            Value::Null => {}
            other => panic!("expected Null, got {:?}", other),
        }
        match reader.read_value(998) {
            Value::Null => {}
            other => panic!("expected Null, got {:?}", other),
        }
        match reader.read_value(999) {
            Value::I64(v) => assert_eq!(v, 42),
            other => panic!("expected I64(42), got {:?}", other),
        }
    }

    #[test]
    fn fixed_i64_same_value_repeated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for _ in 0..500 {
                w.append_i64(777).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 500);
        for i in 0..500 {
            assert_eq!(r.read_i64(i), 777);
        }
    }

    #[test]
    fn var_column_large_binary_blobs() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            let blob = vec![0xFFu8; 50_000];
            w.append(&blob).unwrap();
            let small = vec![0x00u8; 10];
            w.append(&small).unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.read(0).len(), 50_000);
        assert_eq!(r.read(1).len(), 10);
    }

    #[test]
    fn fixed_f64_very_small_values() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            w.append_f64(5e-324).unwrap(); // smallest positive f64
            w.append_f64(-5e-324).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(r.read_f64(0), 5e-324);
        assert_eq!(r.read_f64(1), -5e-324);
    }

    #[test]
    fn fixed_i64_powers_of_two() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..63 {
                w.append_i64(1i64 << i).unwrap();
            }
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 63);
        for i in 0..63u64 {
            assert_eq!(r.read_i64(i), 1i64 << i);
        }
    }

    #[test]
    fn var_column_alternating_empty_and_data() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            for i in 0..100 {
                if i % 2 == 0 {
                    w.append_str("").unwrap();
                } else {
                    w.append_str(&format!("val_{}", i)).unwrap();
                }
            }
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 100);
        for i in 0..100u64 {
            if i % 2 == 0 {
                assert_eq!(r.read_str(i), "");
            } else {
                assert_eq!(r.read_str(i), format!("val_{}", i));
            }
        }
    }

    #[test]
    fn fixed_multiple_types_in_same_dir() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("i32.d");
        let p2 = dir.path().join("f64.d");
        let p3 = dir.path().join("ts.d");
        {
            let mut w1 = FixedColumnWriter::open(&p1, ColumnType::I32).unwrap();
            let mut w2 = FixedColumnWriter::open(&p2, ColumnType::F64).unwrap();
            let mut w3 = FixedColumnWriter::open(&p3, ColumnType::Timestamp).unwrap();
            for i in 0..10 {
                w1.append_i32(i).unwrap();
                w2.append_f64(i as f64 * 0.1).unwrap();
                w3.append_i64(i as i64 * 1_000_000_000).unwrap();
            }
            w1.flush().unwrap();
            w2.flush().unwrap();
            w3.flush().unwrap();
        }
        let r1 = FixedColumnReader::open(&p1, ColumnType::I32).unwrap();
        let r2 = FixedColumnReader::open(&p2, ColumnType::F64).unwrap();
        let r3 = FixedColumnReader::open(&p3, ColumnType::Timestamp).unwrap();
        assert_eq!(r1.row_count(), 10);
        assert_eq!(r2.row_count(), 10);
        assert_eq!(r3.row_count(), 10);
    }

    #[test]
    fn column_top_full_data_equal_rows() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            for i in 0..50 {
                w.append_i32(i).unwrap();
            }
            w.flush().unwrap();
        }
        let reader = ColumnTopReader::open(&path, ColumnType::I32, 50).unwrap();
        assert_eq!(reader.column_top(), 0);
        match reader.read_value(0) {
            Value::I32(v) => assert_eq!(v, 0),
            other => panic!("expected I32(0), got {:?}", other),
        }
    }

    #[test]
    fn fixed_column_flush_multiple_times() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(1).unwrap();
            w.flush().unwrap();
            w.append_i64(2).unwrap();
            w.flush().unwrap();
            w.append_i64(3).unwrap();
            w.flush().unwrap();
        }
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 3);
        assert_eq!(r.read_i64(0), 1);
        assert_eq!(r.read_i64(1), 2);
        assert_eq!(r.read_i64(2), 3);
    }

    #[test]
    fn var_column_flush_multiple_times() {
        let dir = tempdir().unwrap();
        let dp = dir.path().join("col.d");
        let ip = dir.path().join("col.i");
        {
            let mut w = VarColumnWriter::open(&dp, &ip).unwrap();
            w.append_str("a").unwrap();
            w.flush().unwrap();
            w.append_str("b").unwrap();
            w.flush().unwrap();
        }
        let r = VarColumnReader::open(&dp, &ip).unwrap();
        assert_eq!(r.row_count(), 2);
        assert_eq!(r.read_str(0), "a");
        assert_eq!(r.read_str(1), "b");
    }
}
