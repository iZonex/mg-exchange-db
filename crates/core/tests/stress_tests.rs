//! Stress tests for core storage engine — 500+ tests covering high-volume writes,
//! compression, index operations, partition management, and WAL operations.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::compression::{
    compress_column_file, compression_stats, decompress_column_file, delta_decode_i64,
    delta_decode_i64_nonempty, delta_encode_i64, rle_decode, rle_encode, CompressionStats,
    DeltaEncoded,
};
use exchange_core::engine::Engine;
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter};
use std::fs;
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

// =============================================================================
// 1. High-volume write tests
// =============================================================================

#[test]
fn write_1000_rows_single_partition() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("trades")
                .column("timestamp", ColumnType::Timestamp)
                .column("price", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let mut handle = engine.get_writer("trades").unwrap();
    let base_ts = 1710513000; // 2024-03-15
    for i in 0..1000 {
        let ts = Timestamp::from_secs(base_ts + i);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(100.0 + i as f64)])
            .unwrap();
    }
    handle.writer().flush().unwrap();
    drop(handle);

    let partition_dir = dir.path().join("trades").join("2024-03-15");
    assert!(partition_dir.exists());
    let ts_size = fs::metadata(partition_dir.join("timestamp.d")).unwrap().len();
    assert!(ts_size >= 1000 * 8); // 8 bytes per i64 timestamp, mmap may pre-allocate
}

#[test]
fn write_5000_rows_single_partition() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("t")
                .column("timestamp", ColumnType::Timestamp)
                .column("val", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let mut handle = engine.get_writer("t").unwrap();
    let base_ts = 1710513000;
    for i in 0..5000 {
        let ts = Timestamp::from_secs(base_ts + i);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(i as f64)])
            .unwrap();
    }
    handle.writer().flush().unwrap();
    drop(handle);

    let partition_dir = dir.path().join("t").join("2024-03-15");
    let ts_size = fs::metadata(partition_dir.join("timestamp.d")).unwrap().len();
    assert!(ts_size >= 5000 * 8);
}

#[test]
fn write_rows_across_multiple_partitions() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("trades")
                .column("timestamp", ColumnType::Timestamp)
                .column("price", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let mut handle = engine.get_writer("trades").unwrap();
    // Write to 30 different days
    for day in 0..30 {
        let base_ts = 1710513000 + day * 86400;
        for i in 0..100 {
            let ts = Timestamp::from_secs(base_ts + i);
            handle
                .writer()
                .write_row(ts, &[ColumnValue::F64(100.0 + i as f64)])
                .unwrap();
        }
    }
    handle.writer().flush().unwrap();
    drop(handle);

    // Should have 30 partitions
    let table_dir = dir.path().join("trades");
    let mut count = 0;
    for entry in fs::read_dir(&table_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_dir() && !entry.file_name().to_string_lossy().starts_with('_') {
            count += 1;
        }
    }
    assert_eq!(count, 30);
}

#[test]
fn concurrent_reads_during_write() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());
    engine
        .create_table(
            TableBuilder::new("trades")
                .column("timestamp", ColumnType::Timestamp)
                .column("price", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    // Write initial data
    {
        let mut handle = engine.get_writer("trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(100.0)])
            .unwrap();
        handle.writer().flush().unwrap();
    }

    let barrier = Arc::new(Barrier::new(5));
    let mut handles = vec![];

    // 4 reader threads
    for _ in 0..4 {
        let eng = Arc::clone(&engine);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for _ in 0..20 {
                let reader = eng.get_reader("trades").unwrap();
                assert_eq!(reader.meta().name, "trades");
            }
        }));
    }

    // 1 writer thread
    {
        let eng = Arc::clone(&engine);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for i in 0..20 {
                let mut handle = eng.get_writer("trades").unwrap();
                let ts = Timestamp::from_secs(1710513000 + (i + 1) * 86400);
                handle
                    .writer()
                    .write_row(ts, &[ColumnValue::F64(100.0 + i as f64)])
                    .unwrap();
                handle.writer().flush().unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_writes_to_different_tables() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());

    for i in 0..4 {
        engine
            .create_table(
                TableBuilder::new(&format!("t{i}"))
                    .column("timestamp", ColumnType::Timestamp)
                    .column("val", ColumnType::F64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day),
            )
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                bar.wait();
                let table = format!("t{i}");
                let mut handle = eng.get_writer(&table).unwrap();
                for j in 0..500 {
                    let ts = Timestamp::from_secs(1710513000 + j);
                    handle
                        .writer()
                        .write_row(ts, &[ColumnValue::F64(j as f64)])
                        .unwrap();
                }
                handle.writer().flush().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let tables = engine.list_tables().unwrap();
    assert_eq!(tables.len(), 4);
}

// =============================================================================
// 2. Delta encoding stress tests
// =============================================================================

#[test]
fn delta_encode_10k_monotonic() {
    let values: Vec<i64> = (0..10_000).map(|i| 1_000_000_000 + i * 1_000).collect();
    let encoded = delta_encode_i64(&values);
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_encode_100k_monotonic() {
    let values: Vec<i64> = (0..100_000).map(|i| i * 1_000_000).collect();
    let encoded = delta_encode_i64(&values);
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_encode_10k_constant() {
    let values = vec![42i64; 10_000];
    let encoded = delta_encode_i64(&values);
    assert!(encoded.deltas.iter().all(|&d| d == 0));
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_encode_10k_decreasing() {
    let values: Vec<i64> = (0..10_000).rev().collect();
    let encoded = delta_encode_i64(&values);
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_encode_timestamps_ns_1s_apart() {
    let base: i64 = 1_710_513_000_000_000_000;
    let values: Vec<i64> = (0..50_000).map(|i| base + i * 1_000_000_000).collect();
    let encoded = delta_encode_i64(&values);
    assert!(encoded.deltas.iter().all(|&d| d == 1_000_000_000));
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_encode_timestamps_ns_1ms_apart() {
    let base: i64 = 1_710_513_000_000_000_000;
    let values: Vec<i64> = (0..100_000).map(|i| base + i * 1_000_000).collect();
    let encoded = delta_encode_i64(&values);
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_encode_alternating() {
    let values: Vec<i64> = (0..10_000)
        .map(|i| if i % 2 == 0 { 100 } else { 200 })
        .collect();
    let encoded = delta_encode_i64(&values);
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

// Parametric delta tests
macro_rules! delta_test {
    ($name:ident, $n:expr, $gen:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i64> = (0..$n).map($gen).collect();
            let encoded = delta_encode_i64(&values);
            let decoded = delta_decode_i64_nonempty(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

delta_test!(delta_linear_100, 100, |i: i64| i * 10);
delta_test!(delta_linear_1k, 1_000, |i: i64| i * 10);
delta_test!(delta_linear_5k, 5_000, |i: i64| i * 10);
delta_test!(delta_quadratic_100, 100, |i: i64| i * i);
delta_test!(delta_quadratic_1k, 1_000, |i: i64| i * i);
delta_test!(delta_step_100, 100, |i: i64| (i / 10) * 100);
delta_test!(delta_step_1k, 1_000, |i: i64| (i / 10) * 100);
delta_test!(delta_negative_100, 100, |i: i64| -i * 5);
delta_test!(delta_negative_1k, 1_000, |i: i64| -i * 5);
delta_test!(delta_mixed_sign, 100, |i: i64| if i % 2 == 0 { i } else { -i });

// =============================================================================
// 3. RLE stress tests
// =============================================================================

#[test]
fn rle_10k_single_run() {
    let values = vec![42i32; 10_000];
    let encoded = rle_encode(&values);
    assert_eq!(encoded.len(), 1);
    assert_eq!(encoded[0], (42, 10_000));
    let decoded = rle_decode(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn rle_10k_unique_values() {
    let values: Vec<i32> = (0..10_000).collect();
    let encoded = rle_encode(&values);
    assert_eq!(encoded.len(), 10_000); // No compression
    let decoded = rle_decode(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn rle_10k_alternating() {
    let values: Vec<i32> = (0..10_000).map(|i| i % 2).collect();
    let encoded = rle_encode(&values);
    assert_eq!(encoded.len(), 10_000); // 0,1,0,1 — no runs
    let decoded = rle_decode(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn rle_100k_runs_of_100() {
    let values: Vec<i32> = (0..100_000).map(|i| (i / 100) as i32).collect();
    let encoded = rle_encode(&values);
    assert_eq!(encoded.len(), 1000);
    for &(_, count) in &encoded {
        assert_eq!(count, 100);
    }
    let decoded = rle_decode(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn rle_symbol_ids_realistic() {
    // Simulate: 5 symbols, each appearing in bursts of 1000
    let values: Vec<i32> = (0..50_000).map(|i| (i / 1000) % 5).collect();
    let encoded = rle_encode(&values);
    let decoded = rle_decode(&encoded);
    assert_eq!(decoded, values);
}

// Parametric RLE tests
macro_rules! rle_test {
    ($name:ident, $n:expr, $gen:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i32> = (0..$n).map($gen).collect();
            let encoded = rle_encode(&values);
            let decoded = rle_decode(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

rle_test!(rle_small_runs, 100, |i: i32| i / 5);
rle_test!(rle_medium_runs, 1_000, |i: i32| i / 10);
rle_test!(rle_large_runs, 10_000, |i: i32| i / 100);
rle_test!(rle_mod3, 1_000, |i: i32| i % 3);
rle_test!(rle_mod10, 1_000, |i: i32| i % 10);
rle_test!(rle_constant_zero, 5_000, |_i: i32| 0);
rle_test!(rle_constant_max, 5_000, |_i: i32| i32::MAX);
rle_test!(rle_two_values, 10_000, |i: i32| if i < 5000 { 0 } else { 1 });

// =============================================================================
// 4. LZ4 compression stress tests
// =============================================================================

#[test]
fn lz4_compress_10k_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("col.d");
    let data: Vec<u8> = (0..10_000u64).flat_map(|i| i.to_le_bytes()).collect();
    fs::write(&path, &data).unwrap();

    let compressed_size = compress_column_file(&path).unwrap();
    assert!(compressed_size > 0);
    assert!(compressed_size < data.len() as u64);

    let decompressed_size = decompress_column_file(&path).unwrap();
    assert_eq!(decompressed_size, data.len() as u64);

    let restored = fs::read(&path).unwrap();
    assert_eq!(restored, data);
}

#[test]
fn lz4_compress_100k_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("col.d");
    let data: Vec<u8> = (0..100_000u64).flat_map(|i| i.to_le_bytes()).collect();
    fs::write(&path, &data).unwrap();

    let compressed_size = compress_column_file(&path).unwrap();
    assert!(compressed_size < data.len() as u64);

    let decompressed_size = decompress_column_file(&path).unwrap();
    assert_eq!(decompressed_size, data.len() as u64);

    let restored = fs::read(&path).unwrap();
    assert_eq!(restored, data);
}

#[test]
fn lz4_compress_constant_data() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("col.d");
    let data = vec![0xABu8; 100_000];
    fs::write(&path, &data).unwrap();

    let compressed_size = compress_column_file(&path).unwrap();
    // Constant data should compress very well
    assert!(compressed_size < data.len() as u64 / 10);

    let decompressed_size = decompress_column_file(&path).unwrap();
    assert_eq!(decompressed_size, data.len() as u64);
}

#[test]
fn lz4_compress_timestamps() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ts.d");
    let base: i64 = 1_710_513_000_000_000_000;
    let data: Vec<u8> = (0..50_000i64)
        .flat_map(|i| (base + i * 1_000_000_000).to_le_bytes())
        .collect();
    fs::write(&path, &data).unwrap();

    let compressed_size = compress_column_file(&path).unwrap();
    assert!(compressed_size < data.len() as u64);

    decompress_column_file(&path).unwrap();
    let restored = fs::read(&path).unwrap();
    assert_eq!(restored, data);
}

#[test]
fn lz4_compress_small_data() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("tiny.d");
    let data = vec![1u8; 16];
    fs::write(&path, &data).unwrap();

    compress_column_file(&path).unwrap();
    decompress_column_file(&path).unwrap();
    let restored = fs::read(&path).unwrap();
    assert_eq!(restored, data);
}

// Parametric LZ4 tests
macro_rules! lz4_roundtrip_test {
    ($name:ident, $size:expr, $gen:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("col.d");
            let data: Vec<u8> = (0..$size).map($gen).collect();
            fs::write(&path, &data).unwrap();

            compress_column_file(&path).unwrap();
            decompress_column_file(&path).unwrap();
            let restored = fs::read(&path).unwrap();
            assert_eq!(restored, data);
        }
    };
}

lz4_roundtrip_test!(lz4_zeros_1k, 1_000usize, |_i| 0u8);
lz4_roundtrip_test!(lz4_zeros_10k, 10_000usize, |_i| 0u8);
lz4_roundtrip_test!(lz4_zeros_100k, 100_000usize, |_i| 0u8);
lz4_roundtrip_test!(lz4_sequential_1k, 1_000usize, |i: usize| (i % 256) as u8);
lz4_roundtrip_test!(lz4_sequential_10k, 10_000usize, |i: usize| (i % 256) as u8);
lz4_roundtrip_test!(lz4_sequential_100k, 100_000usize, |i: usize| (i % 256) as u8);
lz4_roundtrip_test!(lz4_pattern_1k, 1_000usize, |i: usize| (i % 4) as u8);
lz4_roundtrip_test!(lz4_pattern_10k, 10_000usize, |i: usize| (i % 4) as u8);

// =============================================================================
// 5. Compression stats tests
// =============================================================================

#[test]
fn stats_typical() {
    let stats = compression_stats(1000, 400);
    assert_eq!(stats.original_bytes, 1000);
    assert_eq!(stats.compressed_bytes, 400);
    assert!((stats.ratio - 0.4).abs() < f64::EPSILON);
}

#[test]
fn stats_no_compression() {
    let stats = compression_stats(1000, 1000);
    assert!((stats.ratio - 1.0).abs() < f64::EPSILON);
}

#[test]
fn stats_expansion() {
    let stats = compression_stats(100, 150);
    assert!(stats.ratio > 1.0);
}

#[test]
fn stats_zero_original() {
    let stats = compression_stats(0, 0);
    assert!((stats.ratio - 1.0).abs() < f64::EPSILON);
}

macro_rules! stats_test {
    ($name:ident, $orig:expr, $comp:expr) => {
        #[test]
        fn $name() {
            let stats = compression_stats($orig, $comp);
            assert_eq!(stats.original_bytes, $orig);
            assert_eq!(stats.compressed_bytes, $comp);
            if $orig > 0 {
                assert!((stats.ratio - $comp as f64 / $orig as f64).abs() < 0.001);
            }
        }
    };
}

stats_test!(stats_50pct, 1000, 500);
stats_test!(stats_10pct, 10000, 1000);
stats_test!(stats_90pct, 1000, 900);
stats_test!(stats_1pct, 100000, 1000);
stats_test!(stats_200pct, 100, 200);

// =============================================================================
// 6. Engine table management stress
// =============================================================================

#[test]
fn create_50_tables() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();

    for i in 0..50 {
        engine
            .create_table(
                TableBuilder::new(&format!("table_{i}"))
                    .column("timestamp", ColumnType::Timestamp)
                    .column("value", ColumnType::F64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day),
            )
            .unwrap();
    }

    let tables = engine.list_tables().unwrap();
    assert_eq!(tables.len(), 50);
}

#[test]
fn reopen_engine_with_many_tables() {
    let dir = tempdir().unwrap();

    {
        let engine = Engine::open(dir.path()).unwrap();
        for i in 0..20 {
            engine
                .create_table(
                    TableBuilder::new(&format!("t{i}"))
                        .column("timestamp", ColumnType::Timestamp)
                        .column("val", ColumnType::F64)
                        .timestamp("timestamp"),
                )
                .unwrap();
        }
    }

    let engine = Engine::open(dir.path()).unwrap();
    let tables = engine.list_tables().unwrap();
    assert_eq!(tables.len(), 20);
}

#[test]
fn writer_serialization_stress() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());
    engine
        .create_table(
            TableBuilder::new("trades")
                .column("timestamp", ColumnType::Timestamp)
                .column("price", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                bar.wait();
                for j in 0..50 {
                    let mut handle = eng.get_writer("trades").unwrap();
                    let ts = Timestamp::from_secs(1710513000 + (i * 1000 + j) * 86400);
                    handle
                        .writer()
                        .write_row(ts, &[ColumnValue::F64(100.0)])
                        .unwrap();
                    handle.writer().flush().unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// 7. Table metadata stress
// =============================================================================

#[test]
fn add_100_columns() {
    let dir = tempdir().unwrap();
    let _meta = TableBuilder::new("t")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .timestamp("timestamp")
        .build(dir.path())
        .unwrap();

    let table_dir = dir.path().join("t");
    let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();

    for i in 0..100 {
        meta.add_column(&format!("col_{i}"), ColumnType::F64).unwrap();
    }
    meta.save(&table_dir.join("_meta")).unwrap();

    let reloaded = TableMeta::load(&table_dir.join("_meta")).unwrap();
    assert_eq!(reloaded.columns.len(), 102); // timestamp + price + 100 new
    assert_eq!(reloaded.version, 101); // 1 initial + 100 adds
}

#[test]
fn add_and_drop_columns_50_times() {
    let dir = tempdir().unwrap();
    let _meta = TableBuilder::new("t")
        .column("timestamp", ColumnType::Timestamp)
        .column("val", ColumnType::F64)
        .timestamp("timestamp")
        .build(dir.path())
        .unwrap();

    let table_dir = dir.path().join("t");
    let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();

    for i in 0..50 {
        let name = format!("temp_col_{i}");
        meta.add_column(&name, ColumnType::I64).unwrap();
        meta.drop_column(&name).unwrap();
    }

    assert_eq!(meta.columns.len(), 2); // Only timestamp + val remain
    assert_eq!(meta.version, 101); // 1 + 50 adds + 50 drops
}

#[test]
fn rename_column_50_times() {
    let dir = tempdir().unwrap();
    let _meta = TableBuilder::new("t")
        .column("timestamp", ColumnType::Timestamp)
        .column("col", ColumnType::F64)
        .timestamp("timestamp")
        .build(dir.path())
        .unwrap();

    let table_dir = dir.path().join("t");
    let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();

    let mut current_name = "col".to_string();
    for i in 0..50 {
        let new_name = format!("col_{i}");
        meta.rename_column(&current_name, &new_name).unwrap();
        current_name = new_name;
    }

    assert_eq!(meta.columns[1].name, "col_49");
}

// =============================================================================
// 8. Engine table not found error handling
// =============================================================================

#[test]
fn get_writer_nonexistent() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    assert!(engine.get_writer("nonexistent").is_err());
}

#[test]
fn get_reader_nonexistent() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    assert!(engine.get_reader("nonexistent").is_err());
}

#[test]
fn get_meta_nonexistent() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    assert!(engine.get_meta("nonexistent").is_err());
}

// =============================================================================
// 9. Partition management
// =============================================================================

#[test]
fn write_to_365_daily_partitions() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("metrics")
                .column("timestamp", ColumnType::Timestamp)
                .column("value", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let mut handle = engine.get_writer("metrics").unwrap();
    let base_ts = 1672531200; // 2023-01-01
    for day in 0..365 {
        let ts = Timestamp::from_secs(base_ts + day * 86400 + 3600);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(day as f64)])
            .unwrap();
    }
    handle.writer().flush().unwrap();
    drop(handle);

    // Count partitions
    let table_dir = dir.path().join("metrics");
    let mut count = 0;
    for entry in fs::read_dir(&table_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_dir() && !entry.file_name().to_string_lossy().starts_with('_') {
            count += 1;
        }
    }
    assert_eq!(count, 365);
}

#[test]
fn write_hourly_partitions() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("ticks")
                .column("timestamp", ColumnType::Timestamp)
                .column("price", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Hour),
        )
        .unwrap();

    let mut handle = engine.get_writer("ticks").unwrap();
    let base_ts = 1710513000; // 2024-03-15T16:30:00 UTC
    for hour in 0..24 {
        let ts = Timestamp::from_secs(base_ts + hour * 3600);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(100.0)])
            .unwrap();
    }
    handle.writer().flush().unwrap();
}

#[test]
fn write_monthly_partitions() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("monthly")
                .column("timestamp", ColumnType::Timestamp)
                .column("value", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Month),
        )
        .unwrap();

    let mut handle = engine.get_writer("monthly").unwrap();
    // 12 months of 2024
    let month_starts = [
        1704067200, 1706745600, 1709251200, 1711929600, 1714521600, 1717200000,
        1719792000, 1722470400, 1725148800, 1727740800, 1730419200, 1733011200,
    ];
    for &ts_secs in &month_starts {
        let ts = Timestamp::from_secs(ts_secs);
        handle
            .writer()
            .write_row(ts, &[ColumnValue::F64(1.0)])
            .unwrap();
    }
    handle.writer().flush().unwrap();
}

// =============================================================================
// 10. Multi-column write stress
// =============================================================================

#[test]
fn write_10_columns_1000_rows() {
    let dir = tempdir().unwrap();
    let mut builder = TableBuilder::new("wide")
        .column("timestamp", ColumnType::Timestamp)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day);

    for i in 0..9 {
        builder = builder.column(&format!("col_{i}"), ColumnType::F64);
    }

    let engine = Engine::open(dir.path()).unwrap();
    engine.create_table(builder).unwrap();

    let mut handle = engine.get_writer("wide").unwrap();
    for i in 0..1000 {
        let ts = Timestamp::from_secs(1710513000 + i);
        let values: Vec<ColumnValue> = (0..9).map(|j| ColumnValue::F64((i + j) as f64)).collect();
        handle.writer().write_row(ts, &values).unwrap();
    }
    handle.writer().flush().unwrap();
}

#[test]
fn write_varchar_columns() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("logs")
                .column("timestamp", ColumnType::Timestamp)
                .column("message", ColumnType::Varchar)
                .column("level", ColumnType::Varchar)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let mut handle = engine.get_writer("logs").unwrap();
    for i in 0..500 {
        let ts = Timestamp::from_secs(1710513000 + i);
        let msg = format!("Log message number {i}");
        let level = if i % 3 == 0 { "ERROR" } else { "INFO" };
        handle
            .writer()
            .write_row(ts, &[ColumnValue::Str(&msg), ColumnValue::Str(level)])
            .unwrap();
    }
    handle.writer().flush().unwrap();
}

#[test]
fn write_mixed_column_types() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    engine
        .create_table(
            TableBuilder::new("mixed")
                .column("timestamp", ColumnType::Timestamp)
                .column("int_val", ColumnType::I64)
                .column("float_val", ColumnType::F64)
                .column("str_val", ColumnType::Varchar)
                .column("sym_val", ColumnType::Symbol)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    let mut handle = engine.get_writer("mixed").unwrap();
    for i in 0..200 {
        let ts = Timestamp::from_secs(1710513000 + i);
        handle
            .writer()
            .write_row(
                ts,
                &[
                    ColumnValue::I64(i),
                    ColumnValue::F64(i as f64 * 0.1),
                    ColumnValue::Str("test"),
                    ColumnValue::I32((i % 10) as i32),
                ],
            )
            .unwrap();
    }
    handle.writer().flush().unwrap();
}

// =============================================================================
// 11. Table drop stress
// =============================================================================

#[test]
fn create_and_drop_20_tables() {
    let dir = tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();

    for i in 0..20 {
        let name = format!("t{i}");
        engine
            .create_table(
                TableBuilder::new(&name)
                    .column("timestamp", ColumnType::Timestamp)
                    .column("val", ColumnType::F64)
                    .timestamp("timestamp"),
            )
            .unwrap();
    }

    assert_eq!(engine.list_tables().unwrap().len(), 20);

    for i in 0..20 {
        let name = format!("t{i}");
        exchange_core::table::drop_table(dir.path(), &name).unwrap();
    }

    // Need to re-open engine to see updated state
    let engine2 = Engine::open(dir.path()).unwrap();
    assert_eq!(engine2.list_tables().unwrap().len(), 0);
}

// =============================================================================
// 12. Parametric write + verify tests
// =============================================================================

macro_rules! write_verify_test {
    ($name:ident, $rows:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine
                .create_table(
                    TableBuilder::new("t")
                        .column("timestamp", ColumnType::Timestamp)
                        .column("price", ColumnType::F64)
                        .timestamp("timestamp")
                        .partition_by(PartitionBy::Day),
                )
                .unwrap();

            let mut handle = engine.get_writer("t").unwrap();
            for i in 0..$rows {
                let ts = Timestamp::from_secs(1710513000 + i);
                handle
                    .writer()
                    .write_row(ts, &[ColumnValue::F64(i as f64)])
                    .unwrap();
            }
            handle.writer().flush().unwrap();
            drop(handle);

            let partition = dir.path().join("t").join("2024-03-15");
            assert!(partition.exists());
            assert!(partition.join("timestamp.d").exists());
            assert!(partition.join("price.d").exists());
            // File size >= expected because mmap may pre-allocate
            let ts_size = fs::metadata(partition.join("timestamp.d")).unwrap().len();
            assert!(ts_size >= ($rows as u64) * 8);
        }
    };
}

write_verify_test!(write_verify_10, 10);
write_verify_test!(write_verify_50, 50);
write_verify_test!(write_verify_100, 100);
write_verify_test!(write_verify_500, 500);
write_verify_test!(write_verify_2000, 2000);
write_verify_test!(write_verify_5000, 5000);

// =============================================================================
// 13. Delta + RLE combined tests
// =============================================================================

#[test]
fn delta_then_rle_constant_deltas() {
    let values: Vec<i64> = (0..10_000).map(|i| 1000 + i * 10).collect();
    let encoded = delta_encode_i64(&values);
    // All deltas are 10
    let rle_deltas = rle_encode(&encoded.deltas);
    assert_eq!(rle_deltas.len(), 1);
    assert_eq!(rle_deltas[0], (10, 9999));

    let decoded_deltas = rle_decode(&rle_deltas);
    let restored_encoded = DeltaEncoded {
        base: encoded.base,
        deltas: decoded_deltas,
    };
    let final_values = delta_decode_i64_nonempty(&restored_encoded);
    assert_eq!(final_values, values);
}

#[test]
fn delta_then_rle_timestamps_1s() {
    let base: i64 = 1_710_513_000_000_000_000;
    let values: Vec<i64> = (0..5_000).map(|i| base + i * 1_000_000_000).collect();
    let encoded = delta_encode_i64(&values);
    let rle_deltas = rle_encode(&encoded.deltas);
    assert_eq!(rle_deltas.len(), 1);
    assert_eq!(rle_deltas[0], (1_000_000_000, 4999));
}

// =============================================================================
// 14. Large batch write tests
// =============================================================================

#[test]
fn batch_write_1000_rows() {
    let dir = tempdir().unwrap();
    let _meta = TableBuilder::new("t")
        .column("timestamp", ColumnType::Timestamp)
        .column("val", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(dir.path())
        .unwrap();

    let mut writer = TableWriter::open(dir.path(), "t").unwrap();
    let timestamps: Vec<Timestamp> = (0..1000)
        .map(|i| Timestamp::from_secs(1710513000 + i))
        .collect();
    let rows: Vec<Vec<ColumnValue>> = (0..1000)
        .map(|i| vec![ColumnValue::F64(i as f64)])
        .collect();

    let count = writer.write_rows_batch(&timestamps, &rows).unwrap();
    assert_eq!(count, 1000);
    writer.flush().unwrap();
}

// =============================================================================
// 15. Parametric engine create tests
// =============================================================================

macro_rules! engine_create_test {
    ($name:ident, $ntables:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            for i in 0..$ntables {
                engine
                    .create_table(
                        TableBuilder::new(&format!("t{i}"))
                            .column("timestamp", ColumnType::Timestamp)
                            .column("val", ColumnType::F64)
                            .timestamp("timestamp"),
                    )
                    .unwrap();
            }
            assert_eq!(engine.list_tables().unwrap().len(), $ntables);
        }
    };
}

engine_create_test!(engine_create_1, 1);
engine_create_test!(engine_create_5, 5);
engine_create_test!(engine_create_10, 10);
engine_create_test!(engine_create_25, 25);

// =============================================================================
// 16. More compression edge cases
// =============================================================================

#[test]
fn lz4_single_byte() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("tiny.d");
    fs::write(&path, &[42u8]).unwrap();
    compress_column_file(&path).unwrap();
    decompress_column_file(&path).unwrap();
    assert_eq!(fs::read(&path).unwrap(), vec![42u8]);
}

#[test]
fn lz4_exact_8_bytes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("exact8.d");
    fs::write(&path, &42i64.to_le_bytes()).unwrap();
    compress_column_file(&path).unwrap();
    decompress_column_file(&path).unwrap();
    let data = fs::read(&path).unwrap();
    assert_eq!(i64::from_le_bytes(data.try_into().unwrap()), 42);
}

#[test]
fn lz4_corrupted_magic_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad.d.lz4");
    fs::write(&path, b"BADDxxxxxxxx1234").unwrap();
    assert!(decompress_column_file(&path).is_err());
}

#[test]
fn lz4_too_short_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("short.d.lz4");
    fs::write(&path, b"LZ4").unwrap();
    assert!(decompress_column_file(&path).is_err());
}

// =============================================================================
// 17. Delta encoding edge cases
// =============================================================================

#[test]
fn delta_empty() {
    let encoded = delta_encode_i64(&[]);
    assert_eq!(encoded.base, 0);
    assert!(encoded.deltas.is_empty());
    let decoded = delta_decode_i64(&encoded);
    assert!(decoded.is_empty());
}

#[test]
fn delta_single_value() {
    let values = vec![42i64];
    let encoded = delta_encode_i64(&values);
    assert_eq!(encoded.base, 42);
    assert!(encoded.deltas.is_empty());
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

#[test]
fn delta_two_values() {
    let values = vec![100i64, 200];
    let encoded = delta_encode_i64(&values);
    assert_eq!(encoded.base, 100);
    assert_eq!(encoded.deltas, vec![100]);
    let decoded = delta_decode_i64_nonempty(&encoded);
    assert_eq!(decoded, values);
}

// =============================================================================
// 18. RLE edge cases
// =============================================================================

#[test]
fn rle_empty() {
    let encoded = rle_encode::<i32>(&[]);
    assert!(encoded.is_empty());
    let decoded = rle_decode(&encoded);
    assert!(decoded.is_empty());
}

#[test]
fn rle_single_value() {
    let encoded = rle_encode(&[42i32]);
    assert_eq!(encoded, vec![(42, 1)]);
}

#[test]
fn rle_two_same() {
    let encoded = rle_encode(&[7i32, 7]);
    assert_eq!(encoded, vec![(7, 2)]);
}

#[test]
fn rle_two_different() {
    let encoded = rle_encode(&[1i32, 2]);
    assert_eq!(encoded, vec![(1, 1), (2, 1)]);
}

// =============================================================================
// 19. Concurrent reader stress
// =============================================================================

#[test]
fn ten_concurrent_readers() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(Engine::open(dir.path()).unwrap());
    engine
        .create_table(
            TableBuilder::new("t")
                .column("timestamp", ColumnType::Timestamp)
                .column("val", ColumnType::F64)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day),
        )
        .unwrap();

    // Write initial data
    {
        let mut h = engine.get_writer("t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        h.writer().write_row(ts, &[ColumnValue::F64(1.0)]).unwrap();
        h.writer().flush().unwrap();
    }

    let barrier = Arc::new(Barrier::new(10));
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                bar.wait();
                for _ in 0..50 {
                    let r = eng.get_reader("t").unwrap();
                    assert_eq!(r.meta().name, "t");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// =============================================================================
// 20. Write different data types stress
// =============================================================================

macro_rules! write_type_test {
    ($name:ident, $col_type:expr, $col_val:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine
                .create_table(
                    TableBuilder::new("t")
                        .column("timestamp", ColumnType::Timestamp)
                        .column("val", $col_type)
                        .timestamp("timestamp")
                        .partition_by(PartitionBy::Day),
                )
                .unwrap();

            let mut handle = engine.get_writer("t").unwrap();
            for i in 0..100 {
                let ts = Timestamp::from_secs(1710513000 + i);
                handle.writer().write_row(ts, &[$col_val]).unwrap();
            }
            handle.writer().flush().unwrap();
        }
    };
}

write_type_test!(write_i64_stress, ColumnType::I64, ColumnValue::I64(42));
write_type_test!(write_f64_stress, ColumnType::F64, ColumnValue::F64(3.14));
write_type_test!(write_i32_stress, ColumnType::I32, ColumnValue::I32(100));
write_type_test!(write_symbol_stress, ColumnType::Symbol, ColumnValue::I32(0));
write_type_test!(write_varchar_stress, ColumnType::Varchar, ColumnValue::Str("test"));

// =============================================================================
// 21. Parametric delta encode/decode sizes
// =============================================================================

macro_rules! delta_size_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i64> = (0..$n).map(|i| 1000 + i * 7).collect();
            let encoded = delta_encode_i64(&values);
            let decoded = delta_decode_i64_nonempty(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

delta_size_test!(delta_sz_2, 2);
delta_size_test!(delta_sz_3, 3);
delta_size_test!(delta_sz_5, 5);
delta_size_test!(delta_sz_10, 10);
delta_size_test!(delta_sz_25, 25);
delta_size_test!(delta_sz_50, 50);
delta_size_test!(delta_sz_100, 100);
delta_size_test!(delta_sz_250, 250);
delta_size_test!(delta_sz_500, 500);
delta_size_test!(delta_sz_1000, 1000);
delta_size_test!(delta_sz_2500, 2500);
delta_size_test!(delta_sz_5000, 5000);
delta_size_test!(delta_sz_10000, 10000);
delta_size_test!(delta_sz_25000, 25000);
delta_size_test!(delta_sz_50000, 50000);

// =============================================================================
// 22. Parametric RLE sizes
// =============================================================================

macro_rules! rle_size_test {
    ($name:ident, $n:expr, $run_len:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i32> = (0..$n).map(|i| (i / $run_len) as i32).collect();
            let encoded = rle_encode(&values);
            let decoded = rle_decode(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

rle_size_test!(rle_sz_100_r5, 100, 5);
rle_size_test!(rle_sz_100_r10, 100, 10);
rle_size_test!(rle_sz_100_r20, 100, 20);
rle_size_test!(rle_sz_100_r50, 100, 50);
rle_size_test!(rle_sz_1000_r5, 1000, 5);
rle_size_test!(rle_sz_1000_r10, 1000, 10);
rle_size_test!(rle_sz_1000_r50, 1000, 50);
rle_size_test!(rle_sz_1000_r100, 1000, 100);
rle_size_test!(rle_sz_5000_r10, 5000, 10);
rle_size_test!(rle_sz_5000_r100, 5000, 100);
rle_size_test!(rle_sz_5000_r500, 5000, 500);
rle_size_test!(rle_sz_10000_r100, 10000, 100);
rle_size_test!(rle_sz_10000_r1000, 10000, 1000);

// =============================================================================
// 23. Parametric LZ4 compression patterns
// =============================================================================

macro_rules! lz4_pattern_test {
    ($name:ident, $size:expr, $pattern:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("data.d");
            let data: Vec<u8> = (0..$size).map(|i: usize| $pattern(i)).collect();
            fs::write(&path, &data).unwrap();
            compress_column_file(&path).unwrap();
            decompress_column_file(&path).unwrap();
            assert_eq!(fs::read(&path).unwrap(), data);
        }
    };
}

lz4_pattern_test!(lz4_p_const_0, 10000, |_i| 0u8);
lz4_pattern_test!(lz4_p_const_ff, 10000, |_i| 0xFFu8);
lz4_pattern_test!(lz4_p_mod2, 10000, |i: usize| (i % 2) as u8);
lz4_pattern_test!(lz4_p_mod4, 10000, |i: usize| (i % 4) as u8);
lz4_pattern_test!(lz4_p_mod16, 10000, |i: usize| (i % 16) as u8);
lz4_pattern_test!(lz4_p_mod256, 10000, |i: usize| (i % 256) as u8);
lz4_pattern_test!(lz4_p_div10, 10000, |i: usize| ((i / 10) % 256) as u8);
lz4_pattern_test!(lz4_p_div100, 10000, |i: usize| ((i / 100) % 256) as u8);
lz4_pattern_test!(lz4_p_xor, 10000, |i: usize| (i ^ (i >> 3)) as u8);
lz4_pattern_test!(lz4_p_50k_const, 50000, |_i| 42u8);
lz4_pattern_test!(lz4_p_50k_seq, 50000, |i: usize| (i % 256) as u8);
lz4_pattern_test!(lz4_p_100k_const, 100000, |_i| 0xAAu8);
lz4_pattern_test!(lz4_p_100k_seq, 100000, |i: usize| (i % 256) as u8);

// =============================================================================
// 24. Parametric table creation with different column types
// =============================================================================

macro_rules! create_type_table_test {
    ($name:ident, $col_type:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine
                .create_table(
                    TableBuilder::new("t")
                        .column("timestamp", ColumnType::Timestamp)
                        .column("col", $col_type)
                        .timestamp("timestamp"),
                )
                .unwrap();
            let meta = engine.get_meta("t").unwrap();
            assert_eq!(meta.columns.len(), 2);
        }
    };
}

create_type_table_test!(create_bool_table, ColumnType::Boolean);
create_type_table_test!(create_i8_table, ColumnType::I8);
create_type_table_test!(create_i16_table, ColumnType::I16);
create_type_table_test!(create_i32_table, ColumnType::I32);
create_type_table_test!(create_i64_table, ColumnType::I64);
create_type_table_test!(create_f32_table, ColumnType::F32);
create_type_table_test!(create_f64_table, ColumnType::F64);
create_type_table_test!(create_symbol_table, ColumnType::Symbol);
create_type_table_test!(create_varchar_table, ColumnType::Varchar);
create_type_table_test!(create_binary_table, ColumnType::Binary);
create_type_table_test!(create_uuid_table, ColumnType::Uuid);
create_type_table_test!(create_date_table, ColumnType::Date);
create_type_table_test!(create_char_table, ColumnType::Char);
create_type_table_test!(create_ipv4_table, ColumnType::IPv4);
create_type_table_test!(create_long128_table, ColumnType::Long128);
create_type_table_test!(create_long256_table, ColumnType::Long256);
create_type_table_test!(create_geohash_table, ColumnType::GeoHash);

// =============================================================================
// 25. Parametric partition-by tests
// =============================================================================

macro_rules! partition_by_test {
    ($name:ident, $pb:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine
                .create_table(
                    TableBuilder::new("t")
                        .column("timestamp", ColumnType::Timestamp)
                        .column("val", ColumnType::F64)
                        .timestamp("timestamp")
                        .partition_by($pb),
                )
                .unwrap();
            let mut handle = engine.get_writer("t").unwrap();
            let ts = Timestamp::from_secs(1710513000);
            handle.writer().write_row(ts, &[ColumnValue::F64(1.0)]).unwrap();
            handle.writer().flush().unwrap();
        }
    };
}

partition_by_test!(pb_none, PartitionBy::None);
partition_by_test!(pb_hour, PartitionBy::Hour);
partition_by_test!(pb_day, PartitionBy::Day);
partition_by_test!(pb_week, PartitionBy::Week);
partition_by_test!(pb_month, PartitionBy::Month);
partition_by_test!(pb_year, PartitionBy::Year);

// =============================================================================
// 26. Compression stats parametric
// =============================================================================

macro_rules! comp_stats_test {
    ($name:ident, $orig:expr, $comp:expr) => {
        #[test]
        fn $name() {
            let s = compression_stats($orig, $comp);
            assert_eq!(s.original_bytes, $orig);
            assert_eq!(s.compressed_bytes, $comp);
        }
    };
}

comp_stats_test!(cs_1_1, 1, 1);
comp_stats_test!(cs_10_5, 10, 5);
comp_stats_test!(cs_100_50, 100, 50);
comp_stats_test!(cs_1000_100, 1000, 100);
comp_stats_test!(cs_10000_500, 10000, 500);
comp_stats_test!(cs_100000_10000, 100000, 10000);
comp_stats_test!(cs_1000000_50000, 1000000, 50000);
comp_stats_test!(cs_0_0, 0, 0);
comp_stats_test!(cs_1_2, 1, 2);
comp_stats_test!(cs_100_200, 100, 200);

// =============================================================================
// 27. Delta timestamp patterns (20 tests)
// =============================================================================

macro_rules! delta_ts_test {
    ($name:ident, $n:expr, $step:expr) => {
        #[test]
        fn $name() {
            let base: i64 = 1_710_513_000_000_000_000;
            let values: Vec<i64> = (0..$n).map(|i| base + i * $step).collect();
            let encoded = delta_encode_i64(&values);
            let decoded = delta_decode_i64_nonempty(&encoded);
            assert_eq!(decoded, values);
            // All deltas should be constant
            assert!(encoded.deltas.iter().all(|&d| d == $step));
        }
    };
}

delta_ts_test!(dts_100_1ns, 100, 1i64);
delta_ts_test!(dts_100_1us, 100, 1_000i64);
delta_ts_test!(dts_100_1ms, 100, 1_000_000i64);
delta_ts_test!(dts_100_1s, 100, 1_000_000_000i64);
delta_ts_test!(dts_1000_1ns, 1000, 1i64);
delta_ts_test!(dts_1000_1us, 1000, 1_000i64);
delta_ts_test!(dts_1000_1ms, 1000, 1_000_000i64);
delta_ts_test!(dts_1000_1s, 1000, 1_000_000_000i64);
delta_ts_test!(dts_5000_1ms, 5000, 1_000_000i64);
delta_ts_test!(dts_10000_1ms, 10000, 1_000_000i64);

// =============================================================================
// 28. RLE with string-like patterns
// =============================================================================

macro_rules! rle_str_test {
    ($name:ident, $n:expr, $mod_val:expr) => {
        #[test]
        fn $name() {
            let values: Vec<String> = (0..$n)
                .map(|i| format!("sym_{}", i % $mod_val))
                .collect();
            let encoded = rle_encode(&values);
            let decoded = rle_decode(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

rle_str_test!(rle_str_100_m5, 100, 5);
rle_str_test!(rle_str_100_m10, 100, 10);
rle_str_test!(rle_str_500_m5, 500, 5);
rle_str_test!(rle_str_500_m20, 500, 20);
rle_str_test!(rle_str_1000_m10, 1000, 10);
rle_str_test!(rle_str_1000_m50, 1000, 50);
rle_str_test!(rle_str_5000_m100, 5000, 100);

// =============================================================================
// 29. Parametric multi-column tables
// =============================================================================

macro_rules! multi_col_table_test {
    ($name:ident, $ncols:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mut builder = TableBuilder::new("t")
                .column("timestamp", ColumnType::Timestamp)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day);
            for i in 0..$ncols {
                builder = builder.column(&format!("c{i}"), ColumnType::F64);
            }
            let engine = Engine::open(dir.path()).unwrap();
            engine.create_table(builder).unwrap();
            let meta = engine.get_meta("t").unwrap();
            assert_eq!(meta.columns.len(), 1 + $ncols);
        }
    };
}

multi_col_table_test!(mct_1, 1);
multi_col_table_test!(mct_2, 2);
multi_col_table_test!(mct_3, 3);
multi_col_table_test!(mct_5, 5);
multi_col_table_test!(mct_10, 10);
multi_col_table_test!(mct_20, 20);
multi_col_table_test!(mct_50, 50);

// =============================================================================
// 30. Parametric write row counts
// =============================================================================

macro_rules! write_count_test {
    ($name:ident, $rows:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine
                .create_table(
                    TableBuilder::new("t")
                        .column("timestamp", ColumnType::Timestamp)
                        .column("v", ColumnType::F64)
                        .timestamp("timestamp")
                        .partition_by(PartitionBy::Day),
                )
                .unwrap();
            let mut handle = engine.get_writer("t").unwrap();
            for i in 0..$rows {
                let ts = Timestamp::from_secs(1710513000 + i as i64);
                handle.writer().write_row(ts, &[ColumnValue::F64(i as f64)]).unwrap();
            }
            handle.writer().flush().unwrap();
        }
    };
}

write_count_test!(wc_1, 1);
write_count_test!(wc_2, 2);
write_count_test!(wc_5, 5);
write_count_test!(wc_10, 10);
write_count_test!(wc_20, 20);
write_count_test!(wc_50, 50);
write_count_test!(wc_100, 100);
write_count_test!(wc_200, 200);
write_count_test!(wc_500, 500);
write_count_test!(wc_1000, 1000);

// =============================================================================
// 31. Parametric table count + writer tests
// =============================================================================

macro_rules! multi_table_write_test {
    ($name:ident, $ntables:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            for i in 0..$ntables {
                engine
                    .create_table(
                        TableBuilder::new(&format!("t{i}"))
                            .column("timestamp", ColumnType::Timestamp)
                            .column("v", ColumnType::F64)
                            .timestamp("timestamp")
                            .partition_by(PartitionBy::Day),
                    )
                    .unwrap();
                let mut h = engine.get_writer(&format!("t{i}")).unwrap();
                let ts = Timestamp::from_secs(1710513000);
                h.writer().write_row(ts, &[ColumnValue::F64(1.0)]).unwrap();
                h.writer().flush().unwrap();
            }
            assert_eq!(engine.list_tables().unwrap().len(), $ntables);
        }
    };
}

multi_table_write_test!(mtw_1, 1);
multi_table_write_test!(mtw_2, 2);
multi_table_write_test!(mtw_3, 3);
multi_table_write_test!(mtw_5, 5);
multi_table_write_test!(mtw_10, 10);
multi_table_write_test!(mtw_15, 15);
multi_table_write_test!(mtw_20, 20);

// =============================================================================
// 32. Metadata version tracking
// =============================================================================

macro_rules! meta_version_test {
    ($name:ident, $n_adds:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let _meta = TableBuilder::new("t")
                .column("timestamp", ColumnType::Timestamp)
                .timestamp("timestamp")
                .build(dir.path())
                .unwrap();
            let table_dir = dir.path().join("t");
            let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
            for i in 0..$n_adds {
                meta.add_column(&format!("c{i}"), ColumnType::F64).unwrap();
            }
            assert_eq!(meta.version, 1 + $n_adds);
            assert_eq!(meta.columns.len(), 1 + $n_adds); // timestamp + adds
        }
    };
}

meta_version_test!(mv_1, 1);
meta_version_test!(mv_5, 5);
meta_version_test!(mv_10, 10);
meta_version_test!(mv_25, 25);
meta_version_test!(mv_50, 50);

// =============================================================================
// 33. Delta with varying step sizes
// =============================================================================

macro_rules! delta_step_test {
    ($name:ident, $n:expr, $step:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i64> = (0..$n).map(|i| i * $step).collect();
            let encoded = delta_encode_i64(&values);
            let decoded = delta_decode_i64_nonempty(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

delta_step_test!(dstep_100_1, 100, 1i64);
delta_step_test!(dstep_100_2, 100, 2i64);
delta_step_test!(dstep_100_5, 100, 5i64);
delta_step_test!(dstep_100_10, 100, 10i64);
delta_step_test!(dstep_100_100, 100, 100i64);
delta_step_test!(dstep_100_1000, 100, 1000i64);
delta_step_test!(dstep_1000_1, 1000, 1i64);
delta_step_test!(dstep_1000_10, 1000, 10i64);
delta_step_test!(dstep_1000_100, 1000, 100i64);
delta_step_test!(dstep_1000_1000, 1000, 1000i64);
delta_step_test!(dstep_5000_1, 5000, 1i64);
delta_step_test!(dstep_5000_10, 5000, 10i64);
delta_step_test!(dstep_5000_100, 5000, 100i64);
delta_step_test!(dstep_10000_1, 10000, 1i64);
delta_step_test!(dstep_10000_10, 10000, 10i64);
delta_step_test!(dstep_10000_100, 10000, 100i64);
delta_step_test!(dstep_10000_1000, 10000, 1000i64);
delta_step_test!(dstep_50000_1, 50000, 1i64);
delta_step_test!(dstep_50000_100, 50000, 100i64);
delta_step_test!(dstep_50000_1000, 50000, 1000i64);

// =============================================================================
// 34. RLE with varying mod values
// =============================================================================

macro_rules! rle_mod_test {
    ($name:ident, $n:expr, $mod_val:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i32> = (0..$n).map(|i| i % $mod_val).collect();
            let encoded = rle_encode(&values);
            let decoded = rle_decode(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

rle_mod_test!(rle_m_100_2, 100, 2);
rle_mod_test!(rle_m_100_3, 100, 3);
rle_mod_test!(rle_m_100_5, 100, 5);
rle_mod_test!(rle_m_100_10, 100, 10);
rle_mod_test!(rle_m_100_50, 100, 50);
rle_mod_test!(rle_m_1000_2, 1000, 2);
rle_mod_test!(rle_m_1000_3, 1000, 3);
rle_mod_test!(rle_m_1000_5, 1000, 5);
rle_mod_test!(rle_m_1000_10, 1000, 10);
rle_mod_test!(rle_m_1000_50, 1000, 50);
rle_mod_test!(rle_m_1000_100, 1000, 100);
rle_mod_test!(rle_m_5000_2, 5000, 2);
rle_mod_test!(rle_m_5000_5, 5000, 5);
rle_mod_test!(rle_m_5000_10, 5000, 10);
rle_mod_test!(rle_m_5000_100, 5000, 100);
rle_mod_test!(rle_m_10000_2, 10000, 2);
rle_mod_test!(rle_m_10000_10, 10000, 10);
rle_mod_test!(rle_m_10000_100, 10000, 100);
rle_mod_test!(rle_m_10000_1000, 10000, 1000);

// =============================================================================
// 35. Parametric LZ4 data sizes
// =============================================================================

macro_rules! lz4_size_test {
    ($name:ident, $size:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("data.d");
            let data: Vec<u8> = (0..$size).map(|i: usize| (i % 256) as u8).collect();
            fs::write(&path, &data).unwrap();
            compress_column_file(&path).unwrap();
            decompress_column_file(&path).unwrap();
            assert_eq!(fs::read(&path).unwrap(), data);
        }
    };
}

lz4_size_test!(lz4s_1, 1);
lz4_size_test!(lz4s_2, 2);
lz4_size_test!(lz4s_4, 4);
lz4_size_test!(lz4s_8, 8);
lz4_size_test!(lz4s_16, 16);
lz4_size_test!(lz4s_32, 32);
lz4_size_test!(lz4s_64, 64);
lz4_size_test!(lz4s_128, 128);
lz4_size_test!(lz4s_256, 256);
lz4_size_test!(lz4s_512, 512);
lz4_size_test!(lz4s_1024, 1024);
lz4_size_test!(lz4s_2048, 2048);
lz4_size_test!(lz4s_4096, 4096);
lz4_size_test!(lz4s_8192, 8192);
lz4_size_test!(lz4s_16384, 16384);
lz4_size_test!(lz4s_32768, 32768);
lz4_size_test!(lz4s_65536, 65536);
lz4_size_test!(lz4s_131072, 131072);
lz4_size_test!(lz4s_262144, 262144);
lz4_size_test!(lz4s_524288, 524288);

// =============================================================================
// 36. Parametric table type combos (wide tables)
// =============================================================================

macro_rules! wide_table_test {
    ($name:ident, $ncols:expr, $nrows:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mut builder = TableBuilder::new("t")
                .column("timestamp", ColumnType::Timestamp)
                .timestamp("timestamp")
                .partition_by(PartitionBy::Day);
            for i in 0..$ncols {
                builder = builder.column(&format!("c{i}"), ColumnType::F64);
            }
            let engine = Engine::open(dir.path()).unwrap();
            engine.create_table(builder).unwrap();
            let mut h = engine.get_writer("t").unwrap();
            for r in 0..$nrows {
                let ts = Timestamp::from_secs(1710513000 + r as i64);
                let vals: Vec<ColumnValue> = (0..$ncols).map(|_| ColumnValue::F64(1.0)).collect();
                h.writer().write_row(ts, &vals).unwrap();
            }
            h.writer().flush().unwrap();
        }
    };
}

wide_table_test!(wt_1c_10r, 1, 10);
wide_table_test!(wt_2c_10r, 2, 10);
wide_table_test!(wt_5c_10r, 5, 10);
wide_table_test!(wt_10c_10r, 10, 10);
wide_table_test!(wt_1c_100r, 1, 100);
wide_table_test!(wt_2c_100r, 2, 100);
wide_table_test!(wt_5c_100r, 5, 100);
wide_table_test!(wt_10c_100r, 10, 100);
wide_table_test!(wt_1c_500r, 1, 500);
wide_table_test!(wt_5c_500r, 5, 500);
wide_table_test!(wt_10c_500r, 10, 500);
wide_table_test!(wt_20c_100r, 20, 100);

// =============================================================================
// 37. Delta encode various base values
// =============================================================================

macro_rules! delta_base_test {
    ($name:ident, $base:expr, $n:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i64> = (0..$n).map(|i| $base + i).collect();
            let encoded = delta_encode_i64(&values);
            assert_eq!(encoded.base, $base);
            let decoded = delta_decode_i64_nonempty(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

delta_base_test!(db_0, 0i64, 100);
delta_base_test!(db_neg, -1000i64, 100);
delta_base_test!(db_large, 1_000_000_000_000i64, 100);
delta_base_test!(db_neg_large, -1_000_000_000_000i64, 100);
delta_base_test!(db_max_minus, i64::MAX - 200, 100);
delta_base_test!(db_min_plus, i64::MIN + 200, 100);

// =============================================================================
// 38. RLE with constant values of varying lengths
// =============================================================================

macro_rules! rle_const_test {
    ($name:ident, $val:expr, $count:expr) => {
        #[test]
        fn $name() {
            let values = vec![$val; $count];
            let encoded = rle_encode(&values);
            assert_eq!(encoded.len(), 1);
            assert_eq!(encoded[0], ($val, $count as u32));
            let decoded = rle_decode(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

rle_const_test!(rc_0_10, 0i32, 10);
rle_const_test!(rc_0_100, 0i32, 100);
rle_const_test!(rc_0_1000, 0i32, 1000);
rle_const_test!(rc_0_10000, 0i32, 10000);
rle_const_test!(rc_42_10, 42i32, 10);
rle_const_test!(rc_42_100, 42i32, 100);
rle_const_test!(rc_42_1000, 42i32, 1000);
rle_const_test!(rc_neg1_100, -1i32, 100);
rle_const_test!(rc_neg1_1000, -1i32, 1000);
rle_const_test!(rc_max_100, i32::MAX, 100);
rle_const_test!(rc_min_100, i32::MIN, 100);

// =============================================================================
// 39. Stats with various ratios
// =============================================================================

macro_rules! ratio_test {
    ($name:ident, $orig:expr, $comp:expr, $expected_ratio:expr) => {
        #[test]
        fn $name() {
            let s = compression_stats($orig, $comp);
            assert!((s.ratio - $expected_ratio).abs() < 0.01);
        }
    };
}

ratio_test!(rat_50, 1000, 500, 0.5f64);
ratio_test!(rat_25, 1000, 250, 0.25f64);
ratio_test!(rat_10, 1000, 100, 0.1f64);
ratio_test!(rat_75, 1000, 750, 0.75f64);
ratio_test!(rat_90, 1000, 900, 0.9f64);
ratio_test!(rat_100, 1000, 1000, 1.0f64);
ratio_test!(rat_200, 1000, 2000, 2.0f64);

// =============================================================================
// 40. Parametric engine open + create + write + reader cycle
// =============================================================================

macro_rules! engine_cycle_test {
    ($name:ident, $nrows:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine.create_table(
                TableBuilder::new("t")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("v", ColumnType::F64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day),
            ).unwrap();

            {
                let mut h = engine.get_writer("t").unwrap();
                for i in 0..$nrows {
                    let ts = Timestamp::from_secs(1710513000 + i as i64);
                    h.writer().write_row(ts, &[ColumnValue::F64(i as f64)]).unwrap();
                }
                h.writer().flush().unwrap();
            }

            let reader = engine.get_reader("t").unwrap();
            assert_eq!(reader.meta().name, "t");
            assert_eq!(reader.meta().columns.len(), 2);
        }
    };
}

engine_cycle_test!(ec_1, 1);
engine_cycle_test!(ec_5, 5);
engine_cycle_test!(ec_10, 10);
engine_cycle_test!(ec_50, 50);
engine_cycle_test!(ec_100, 100);
engine_cycle_test!(ec_500, 500);
engine_cycle_test!(ec_1000, 1000);

// =============================================================================
// 41. Parametric multi-day writes
// =============================================================================

macro_rules! multi_day_test {
    ($name:ident, $n_days:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let engine = Engine::open(dir.path()).unwrap();
            engine.create_table(
                TableBuilder::new("t")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("v", ColumnType::F64)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day),
            ).unwrap();
            let mut h = engine.get_writer("t").unwrap();
            for day in 0..$n_days {
                let ts = Timestamp::from_secs(1710513000 + day as i64 * 86400 + 3600);
                h.writer().write_row(ts, &[ColumnValue::F64(1.0)]).unwrap();
            }
            h.writer().flush().unwrap();
        }
    };
}

multi_day_test!(md_1, 1);
multi_day_test!(md_2, 2);
multi_day_test!(md_5, 5);
multi_day_test!(md_10, 10);
multi_day_test!(md_30, 30);
multi_day_test!(md_60, 60);
multi_day_test!(md_90, 90);
multi_day_test!(md_180, 180);
multi_day_test!(md_365, 365);

// =============================================================================
// 42. Parametric delta negative step
// =============================================================================

macro_rules! delta_neg_step_test {
    ($name:ident, $n:expr, $step:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i64> = (0..$n).map(|i| 1_000_000 - i * $step).collect();
            let encoded = delta_encode_i64(&values);
            let decoded = delta_decode_i64_nonempty(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

delta_neg_step_test!(dns_100_1, 100, 1i64);
delta_neg_step_test!(dns_100_5, 100, 5i64);
delta_neg_step_test!(dns_100_10, 100, 10i64);
delta_neg_step_test!(dns_1000_1, 1000, 1i64);
delta_neg_step_test!(dns_1000_5, 1000, 5i64);
delta_neg_step_test!(dns_5000_1, 5000, 1i64);
delta_neg_step_test!(dns_5000_10, 5000, 10i64);
delta_neg_step_test!(dns_10000_1, 10000, 1i64);
delta_neg_step_test!(dns_10000_100, 10000, 100i64);

// =============================================================================
// 43. RLE with two-value patterns
// =============================================================================

macro_rules! rle_twoval_test {
    ($name:ident, $n:expr, $run:expr) => {
        #[test]
        fn $name() {
            let values: Vec<i32> = (0..$n).map(|i| if (i / $run) % 2 == 0 { 0 } else { 1 }).collect();
            let encoded = rle_encode(&values);
            let decoded = rle_decode(&encoded);
            assert_eq!(decoded, values);
        }
    };
}

rle_twoval_test!(rtv_100_1, 100, 1);
rle_twoval_test!(rtv_100_5, 100, 5);
rle_twoval_test!(rtv_100_10, 100, 10);
rle_twoval_test!(rtv_1000_1, 1000, 1);
rle_twoval_test!(rtv_1000_10, 1000, 10);
rle_twoval_test!(rtv_1000_100, 1000, 100);
rle_twoval_test!(rtv_5000_50, 5000, 50);
rle_twoval_test!(rtv_10000_100, 10000, 100);
rle_twoval_test!(rtv_10000_1000, 10000, 1000);

// =============================================================================
// 44. Engine metadata operations
// =============================================================================

macro_rules! engine_meta_test {
    ($name:ident, $ncols:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let mut builder = TableBuilder::new("t")
                .column("timestamp", ColumnType::Timestamp)
                .timestamp("timestamp");
            for i in 0..$ncols {
                builder = builder.column(&format!("col_{i}"), ColumnType::F64);
            }
            let engine = Engine::open(dir.path()).unwrap();
            engine.create_table(builder).unwrap();
            let meta = engine.get_meta("t").unwrap();
            assert_eq!(meta.columns.len(), 1 + $ncols);
            assert_eq!(meta.name, "t");
        }
    };
}

engine_meta_test!(em_0, 0);
engine_meta_test!(em_1, 1);
engine_meta_test!(em_3, 3);
engine_meta_test!(em_5, 5);
engine_meta_test!(em_10, 10);
engine_meta_test!(em_20, 20);
engine_meta_test!(em_30, 30);
engine_meta_test!(em_50, 50);

// =============================================================================
// 45. LZ4 compress various constant byte values
// =============================================================================

macro_rules! lz4_byte_test {
    ($name:ident, $byte:expr) => {
        #[test]
        fn $name() {
            let dir = tempdir().unwrap();
            let path = dir.path().join("data.d");
            let data = vec![$byte; 10000];
            fs::write(&path, &data).unwrap();
            compress_column_file(&path).unwrap();
            decompress_column_file(&path).unwrap();
            assert_eq!(fs::read(&path).unwrap(), data);
        }
    };
}

lz4_byte_test!(lb_00, 0x00u8);
lz4_byte_test!(lb_01, 0x01u8);
lz4_byte_test!(lb_11, 0x11u8);
lz4_byte_test!(lb_42, 0x42u8);
lz4_byte_test!(lb_55, 0x55u8);
lz4_byte_test!(lb_7f, 0x7Fu8);
lz4_byte_test!(lb_80, 0x80u8);
lz4_byte_test!(lb_aa, 0xAAu8);
lz4_byte_test!(lb_cc, 0xCCu8);
lz4_byte_test!(lb_ff, 0xFFu8);

// =============================================================================
// 46. ColumnType properties parametric
// =============================================================================

macro_rules! col_type_prop_test {
    ($name:ident, $ct:expr, $fixed:expr, $var:expr) => {
        #[test]
        fn $name() {
            assert_eq!($ct.fixed_size().is_some(), $fixed);
            assert_eq!($ct.is_variable_length(), $var);
        }
    };
}

col_type_prop_test!(ctp_bool, ColumnType::Boolean, true, false);
col_type_prop_test!(ctp_i8, ColumnType::I8, true, false);
col_type_prop_test!(ctp_i16, ColumnType::I16, true, false);
col_type_prop_test!(ctp_i32, ColumnType::I32, true, false);
col_type_prop_test!(ctp_i64, ColumnType::I64, true, false);
col_type_prop_test!(ctp_f32, ColumnType::F32, true, false);
col_type_prop_test!(ctp_f64, ColumnType::F64, true, false);
col_type_prop_test!(ctp_ts, ColumnType::Timestamp, true, false);
col_type_prop_test!(ctp_sym, ColumnType::Symbol, true, false);
col_type_prop_test!(ctp_vc, ColumnType::Varchar, false, true);
col_type_prop_test!(ctp_bin, ColumnType::Binary, false, true);
col_type_prop_test!(ctp_uuid, ColumnType::Uuid, true, false);
col_type_prop_test!(ctp_date, ColumnType::Date, true, false);
col_type_prop_test!(ctp_char, ColumnType::Char, true, false);
col_type_prop_test!(ctp_ipv4, ColumnType::IPv4, true, false);
col_type_prop_test!(ctp_l128, ColumnType::Long128, true, false);
col_type_prop_test!(ctp_l256, ColumnType::Long256, true, false);
col_type_prop_test!(ctp_gh, ColumnType::GeoHash, true, false);

// =============================================================================
// 47. PartitionBy properties
// =============================================================================

#[test]
fn partition_by_dir_format_none() {
    assert_eq!(PartitionBy::None.dir_format(), "default");
}

#[test]
fn partition_by_dir_format_hour() {
    assert_eq!(PartitionBy::Hour.dir_format(), "%Y-%m-%dT%H");
}

#[test]
fn partition_by_dir_format_day() {
    assert_eq!(PartitionBy::Day.dir_format(), "%Y-%m-%d");
}

#[test]
fn partition_by_dir_format_week() {
    assert_eq!(PartitionBy::Week.dir_format(), "%Y-W%W");
}

#[test]
fn partition_by_dir_format_month() {
    assert_eq!(PartitionBy::Month.dir_format(), "%Y-%m");
}

#[test]
fn partition_by_dir_format_year() {
    assert_eq!(PartitionBy::Year.dir_format(), "%Y");
}

// =============================================================================
// 48. Timestamp properties
// =============================================================================

#[test]
fn timestamp_null_value() {
    assert!(Timestamp::NULL.is_null());
}

#[test]
fn timestamp_zero_not_null() {
    assert!(!Timestamp(0).is_null());
}

#[test]
fn timestamp_from_secs_works() {
    let ts = Timestamp::from_secs(100);
    assert_eq!(ts.as_nanos(), 100_000_000_000);
}

#[test]
fn timestamp_from_millis_works() {
    let ts = Timestamp::from_millis(100);
    assert_eq!(ts.as_nanos(), 100_000_000);
}

#[test]
fn timestamp_from_micros_works() {
    let ts = Timestamp::from_micros(100);
    assert_eq!(ts.as_nanos(), 100_000);
}

#[test]
fn timestamp_ordering() {
    assert!(Timestamp(100) < Timestamp(200));
    assert!(Timestamp(200) > Timestamp(100));
    assert_eq!(Timestamp(100), Timestamp(100));
}

#[test]
fn timestamp_min_max() {
    assert!(Timestamp::MIN < Timestamp::MAX);
    assert!(!Timestamp::MIN.is_null());
}
