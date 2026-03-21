//! Comprehensive tests for compression: delta encoding, RLE, and LZ4.
//!
//! 50 tests covering all compression algorithms and edge cases.

use exchange_core::compression::{
    compress_column_file, compression_stats, decompress_column_file, delta_decode_i64,
    delta_decode_i64_nonempty, delta_encode_i64, rle_decode, rle_encode,
};
use tempfile::tempdir;

// ============================================================================
// Delta Encoding
// ============================================================================

mod delta_encoding {
    use super::*;

    #[test]
    fn monotonic_ascending() {
        let values = vec![100, 110, 125, 150, 200];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 100);
        assert_eq!(encoded.deltas, vec![10, 15, 25, 50]);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn monotonic_descending() {
        let values = vec![200, 190, 185, 180];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.deltas, vec![-10, -5, -5]);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn empty_input() {
        let encoded = delta_encode_i64(&[]);
        assert_eq!(encoded.base, 0);
        assert!(encoded.deltas.is_empty());
        assert!(delta_decode_i64(&encoded).is_empty());
    }

    #[test]
    fn single_value() {
        let values = vec![42];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 42);
        assert!(encoded.deltas.is_empty());
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn constant_values() {
        let values = vec![7, 7, 7, 7, 7];
        let encoded = delta_encode_i64(&values);
        assert!(encoded.deltas.iter().all(|&d| d == 0));
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn negative_values() {
        let values = vec![-100, -50, 0, 50, 100];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, -100);
        assert_eq!(encoded.deltas, vec![50, 50, 50, 50]);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn alternating_values() {
        let values = vec![0, 100, 0, 100, 0];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.deltas, vec![100, -100, 100, -100]);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn realistic_nanosecond_timestamps() {
        let base_ns: i64 = 1_710_513_000_000_000_000;
        let values: Vec<i64> = (0..1000).map(|i| base_ns + i * 1_000_000_000).collect();
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, base_ns);
        assert!(encoded.deltas.iter().all(|&d| d == 1_000_000_000));
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn large_sequence() {
        let values: Vec<i64> = (0..100_000).map(|i| i * 7).collect();
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn two_values() {
        let values = vec![10, 20];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 10);
        assert_eq!(encoded.deltas, vec![10]);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn extreme_values() {
        let values = vec![i64::MIN + 1, 0, i64::MAX];
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn all_zeros() {
        let values = vec![0, 0, 0, 0, 0];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 0);
        assert_eq!(encoded.deltas, vec![0, 0, 0, 0]);
        // Note: delta_decode_i64 returns empty for base=0, empty deltas
        // but here deltas are non-empty
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn non_monotonic_random_like() {
        let values = vec![5, -3, 10, 2, -8, 0, 7];
        let encoded = delta_encode_i64(&values);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn large_deltas() {
        let values = vec![0, i64::MAX / 2, i64::MAX / 2 + 1000];
        let encoded = delta_encode_i64(&values);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }
}

// ============================================================================
// Run-Length Encoding
// ============================================================================

mod rle {
    use super::*;

    #[test]
    fn basic_encode_decode() {
        let values = vec!["BTC", "BTC", "BTC", "ETH", "ETH", "SOL"];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![("BTC", 3), ("ETH", 2), ("SOL", 1)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn empty_input() {
        let values: Vec<i32> = vec![];
        let encoded = rle_encode(&values);
        assert!(encoded.is_empty());
        assert!(rle_decode(&encoded).is_empty());
    }

    #[test]
    fn single_value() {
        let values = vec![42];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(42, 1)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn all_unique() {
        let values = vec![1, 2, 3, 4, 5];
        let encoded = rle_encode(&values);
        assert_eq!(encoded.len(), 5);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn all_same() {
        let values = vec![7; 1000];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(7, 1000)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn single_value_repeated_many_times() {
        let values = vec![99; 100_000];
        let encoded = rle_encode(&values);
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0], (99, 100_000));
        assert_eq!(rle_decode(&encoded).len(), 100_000);
    }

    #[test]
    fn alternating_two_values() {
        let values: Vec<i32> = (0..100).map(|i| if i % 2 == 0 { 0 } else { 1 }).collect();
        let encoded = rle_encode(&values);
        assert_eq!(encoded.len(), 100); // no compression for alternating
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn mixed_runs() {
        let values = vec![0, 0, 0, 1, 1, 2, 0, 0];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(0, 3), (1, 2), (2, 1), (0, 2)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn i32_symbol_ids() {
        let mut values = Vec::new();
        for _ in 0..50 {
            values.push(0);
        }
        for _ in 0..30 {
            values.push(1);
        }
        for _ in 0..20 {
            values.push(2);
        }
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(0, 50), (1, 30), (2, 20)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn string_values() {
        let values = vec!["buy".to_string(), "buy".to_string(), "sell".to_string()];
        let encoded = rle_encode(&values);
        assert_eq!(encoded.len(), 2);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn bool_values() {
        let values = vec![true, true, false, false, false, true];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(true, 2), (false, 3), (true, 1)]);
        assert_eq!(rle_decode(&encoded), values);
    }
}

// ============================================================================
// LZ4 Compression
// ============================================================================

mod lz4 {
    use super::*;

    #[test]
    fn compress_decompress_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        let data: Vec<u8> = (0..1000u64).flat_map(|i| i.to_le_bytes()).collect();
        std::fs::write(&path, &data).unwrap();

        let compressed_size = compress_column_file(&path).unwrap();
        assert!(compressed_size > 0);
        assert!(!path.exists());
        assert!(dir.path().join("price.d.lz4").exists());

        let decompressed_size = decompress_column_file(&path).unwrap();
        assert_eq!(decompressed_size, data.len() as u64);
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), data);
    }

    #[test]
    fn compress_small_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tiny.d");
        std::fs::write(&path, &[42u8]).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 1);
        assert_eq!(std::fs::read(&path).unwrap(), vec![42u8]);
    }

    #[test]
    fn compress_repeated_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("repeated.d");
        let data = vec![0xABu8; 100_000];
        std::fs::write(&path, &data).unwrap();
        let compressed = compress_column_file(&path).unwrap();
        // Repeated data should compress well
        assert!(compressed < data.len() as u64);
        let decompressed = decompress_column_file(&path).unwrap();
        assert_eq!(decompressed, data.len() as u64);
    }

    #[test]
    fn decompress_via_lz4_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("col.d");
        std::fs::write(&path, &[1, 2, 3, 4, 5]).unwrap();
        compress_column_file(&path).unwrap();
        let lz4_path = dir.path().join("col.d.lz4");
        let size = decompress_column_file(&lz4_path).unwrap();
        assert_eq!(size, 5);
    }

    #[test]
    fn corrupted_magic_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.d.lz4");
        std::fs::write(&path, b"BADDxxxxxxxx").unwrap();
        assert!(decompress_column_file(&path).is_err());
    }

    #[test]
    fn file_too_short_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("short.d.lz4");
        std::fs::write(&path, b"LZ4").unwrap();
        assert!(decompress_column_file(&path).is_err());
    }

    #[test]
    fn large_data_10mb() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("large.d");
        let data: Vec<u8> = (0..10_000_000u32).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, data.len() as u64);
        assert_eq!(std::fs::read(&path).unwrap(), data);
    }

    #[test]
    fn empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.d");
        std::fs::write(&path, &[]).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn compress_different_column_types() {
        // Simulate f64 data
        let dir = tempdir().unwrap();
        let path = dir.path().join("f64.d");
        let data: Vec<u8> = (0..500)
            .flat_map(|i| (i as f64 * 0.001).to_le_bytes())
            .collect();
        std::fs::write(&path, &data).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[test]
    fn compression_stats_basic() {
        let stats = compression_stats(1000, 400);
        assert_eq!(stats.original_bytes, 1000);
        assert_eq!(stats.compressed_bytes, 400);
        assert!((stats.ratio - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn compression_stats_zero_original() {
        let stats = compression_stats(0, 0);
        assert!((stats.ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compression_stats_equal() {
        let stats = compression_stats(100, 100);
        assert!((stats.ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compression_stats_larger_compressed() {
        let stats = compression_stats(10, 20);
        assert!((stats.ratio - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn compress_100_bytes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("hundred.d");
        std::fs::write(&path, &[42u8; 100]).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 100);
    }

    #[test]
    fn compress_1kb() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("kb.d");
        let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        std::fs::write(&path, &data).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 1024);
        assert_eq!(std::fs::read(&path).unwrap(), data);
    }

    #[test]
    fn compress_all_zeros() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("zeros.d");
        std::fs::write(&path, &vec![0u8; 10_000]).unwrap();
        let compressed = compress_column_file(&path).unwrap();
        assert!(compressed < 10_000);
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 10_000);
    }

    #[test]
    fn compress_all_ff() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ff.d");
        std::fs::write(&path, &vec![0xFFu8; 5000]).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 5000);
    }
}

// ============================================================================
// Additional delta encoding and RLE tests
// ============================================================================

mod delta_extra {
    use super::*;

    #[test]
    fn delta_large_negative_base() {
        let values = vec![-1_000_000_000, -999_999_999, -999_999_998];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, -1_000_000_000);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn delta_single_zero() {
        let encoded = delta_encode_i64(&[0]);
        // base=0, empty deltas: delta_decode_i64 would return empty,
        // but delta_decode_i64_nonempty returns [0].
        assert_eq!(delta_decode_i64_nonempty(&encoded), vec![0]);
    }

    #[test]
    fn delta_microsecond_timestamps() {
        let base: i64 = 1_710_513_000_000_000;
        let values: Vec<i64> = (0..100).map(|i| base + i * 1_000).collect();
        let encoded = delta_encode_i64(&values);
        assert!(encoded.deltas.iter().all(|&d| d == 1_000));
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn delta_irregular_intervals() {
        let values = vec![0, 1, 3, 6, 10, 15, 21, 28, 36, 45];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.deltas, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(delta_decode_i64_nonempty(&encoded), values);
    }

    #[test]
    fn delta_encode_preserves_length() {
        for len in [2, 5, 10, 100, 1000] {
            let values: Vec<i64> = (0..len).collect();
            let encoded = delta_encode_i64(&values);
            assert_eq!(encoded.deltas.len(), len as usize - 1);
            assert_eq!(delta_decode_i64_nonempty(&encoded).len(), len as usize);
        }
    }
}

mod rle_extra {
    use super::*;

    #[test]
    fn rle_two_elements_same() {
        let values = vec![5, 5];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(5, 2)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn rle_two_elements_different() {
        let values = vec![5, 6];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(5, 1), (6, 1)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn rle_run_at_end() {
        let values = vec![1, 2, 3, 3, 3];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(1, 1), (2, 1), (3, 3)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn rle_run_at_start() {
        let values = vec![1, 1, 1, 2, 3];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(1, 3), (2, 1), (3, 1)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn rle_negative_i32() {
        let values = vec![-1, -1, 0, 0, 1, 1];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(-1, 2), (0, 2), (1, 2)]);
        assert_eq!(rle_decode(&encoded), values);
    }

    #[test]
    fn rle_u8_values() {
        let values: Vec<u8> = vec![0, 0, 255, 255, 255, 128];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(0, 2), (255, 3), (128, 1)]);
        assert_eq!(rle_decode(&encoded), values);
    }
}
