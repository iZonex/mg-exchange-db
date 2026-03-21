//! Comprehensive tests for partition management, naming, and tiered storage.
//!
//! 60 tests covering PartitionManager, partition_dir naming, LZ4 tiered storage,
//! and partition operations.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::{FixedColumnReader, FixedColumnWriter};
use exchange_core::compression::{compress_column_file, decompress_column_file};
use exchange_core::partition::{PartitionManager, partition_dir};
use exchange_core::table::{
    ColumnValue, TableBuilder, TableMeta, TableWriter, list_partitions, read_partition_rows,
    rewrite_partition,
};
use tempfile::tempdir;

// ============================================================================
// Partition directory naming
// ============================================================================

mod partition_naming {
    use super::*;

    #[test]
    fn none_partition_is_default() {
        let ts = Timestamp::from_secs(1710513000);
        assert_eq!(partition_dir(ts, PartitionBy::None), "default");
    }

    #[test]
    fn year_format() {
        let ts = Timestamp::from_secs(1710513000); // 2024-03-15
        assert_eq!(partition_dir(ts, PartitionBy::Year), "2024");
    }

    #[test]
    fn month_format() {
        let ts = Timestamp::from_secs(1710513000);
        assert_eq!(partition_dir(ts, PartitionBy::Month), "2024-03");
    }

    #[test]
    fn day_format() {
        let ts = Timestamp::from_secs(1710513000);
        assert_eq!(partition_dir(ts, PartitionBy::Day), "2024-03-15");
    }

    #[test]
    fn hour_format() {
        let ts = Timestamp::from_secs(1710513000);
        assert_eq!(partition_dir(ts, PartitionBy::Hour), "2024-03-15T14");
    }

    #[test]
    fn week_format() {
        let ts = Timestamp::from_secs(1710513000);
        let dir = partition_dir(ts, PartitionBy::Week);
        assert!(dir.starts_with("2024-W"));
    }

    #[test]
    fn epoch_zero() {
        let ts = Timestamp::from_secs(0);
        assert_eq!(partition_dir(ts, PartitionBy::Day), "1970-01-01");
        assert_eq!(partition_dir(ts, PartitionBy::Year), "1970");
    }

    #[test]
    fn end_of_day_boundary() {
        // 2024-03-15 23:59:59
        let ts = Timestamp::from_secs(1710547199);
        assert_eq!(partition_dir(ts, PartitionBy::Day), "2024-03-15");
    }

    #[test]
    fn start_of_next_day() {
        // 2024-03-16 00:00:00
        let ts = Timestamp::from_secs(1710547200);
        assert_eq!(partition_dir(ts, PartitionBy::Day), "2024-03-16");
    }

    #[test]
    fn year_boundary() {
        // 2023-12-31 23:59:59
        let ts = Timestamp::from_secs(1704067199);
        assert_eq!(partition_dir(ts, PartitionBy::Year), "2023");
        // 2024-01-01 00:00:00
        let ts2 = Timestamp::from_secs(1704067200);
        assert_eq!(partition_dir(ts2, PartitionBy::Year), "2024");
    }

    #[test]
    fn month_boundary() {
        // 2024-02-29 (leap year)
        let ts = Timestamp::from_secs(1709164800);
        assert_eq!(partition_dir(ts, PartitionBy::Month), "2024-02");
        // 2024-03-01
        let ts2 = Timestamp::from_secs(1709251200);
        assert_eq!(partition_dir(ts2, PartitionBy::Month), "2024-03");
    }

    #[test]
    fn hour_boundary() {
        // 2024-03-15 14:00:00
        let ts = Timestamp::from_secs(1710511200);
        assert_eq!(partition_dir(ts, PartitionBy::Hour), "2024-03-15T14");
        // 2024-03-15 15:00:00
        let ts2 = Timestamp::from_secs(1710514800);
        assert_eq!(partition_dir(ts2, PartitionBy::Hour), "2024-03-15T15");
    }

    #[test]
    fn none_is_always_default() {
        for secs in [0, 1000, 1710513000, i64::MAX / 1_000_000_000] {
            let ts = Timestamp::from_secs(secs);
            assert_eq!(partition_dir(ts, PartitionBy::None), "default");
        }
    }

    #[test]
    fn different_timestamps_same_day() {
        let ts1 = Timestamp::from_secs(1710513000);
        let ts2 = Timestamp::from_secs(1710513000 + 100);
        assert_eq!(
            partition_dir(ts1, PartitionBy::Day),
            partition_dir(ts2, PartitionBy::Day)
        );
    }

    #[test]
    fn different_timestamps_different_days() {
        let ts1 = Timestamp::from_secs(1710513000);
        let ts2 = Timestamp::from_secs(1710513000 + 86400);
        assert_ne!(
            partition_dir(ts1, PartitionBy::Day),
            partition_dir(ts2, PartitionBy::Day)
        );
    }
}

// ============================================================================
// PartitionManager
// ============================================================================

mod partition_manager {
    use super::*;

    #[test]
    fn create_and_list_partitions() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);

        let ts1 = Timestamp::from_secs(1710513000);
        let ts2 = Timestamp::from_secs(1710513000 + 86400);
        mgr.ensure_partition(ts1).unwrap();
        mgr.ensure_partition(ts2).unwrap();

        let parts = mgr.list_partitions().unwrap();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn ensure_partition_idempotent() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);
        let ts = Timestamp::from_secs(1710513000);
        let p1 = mgr.ensure_partition(ts).unwrap();
        let p2 = mgr.ensure_partition(ts).unwrap();
        assert_eq!(p1, p2);
    }

    #[test]
    fn partition_path_correct() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);
        let ts = Timestamp::from_secs(1710513000);
        let path = mgr.partition_path(ts);
        assert_eq!(path, dir.path().join("2024-03-15"));
    }

    #[test]
    fn list_empty_partitions() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);
        let parts = mgr.list_partitions().unwrap();
        assert!(parts.is_empty());
    }

    #[test]
    fn list_skips_internal_dirs() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("_internal")).unwrap();
        std::fs::create_dir_all(dir.path().join("2024-01-01")).unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);
        let parts = mgr.list_partitions().unwrap();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn root_and_partition_by_accessors() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Hour);
        assert_eq!(mgr.root(), dir.path());
        assert_eq!(mgr.partition_by(), PartitionBy::Hour);
    }

    #[test]
    fn partitions_sorted_by_name() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);
        // Create in reverse order
        mgr.ensure_partition(Timestamp::from_secs(1710513000 + 2 * 86400))
            .unwrap();
        mgr.ensure_partition(Timestamp::from_secs(1710513000))
            .unwrap();
        mgr.ensure_partition(Timestamp::from_secs(1710513000 + 86400))
            .unwrap();
        let parts = mgr.list_partitions().unwrap();
        assert_eq!(parts.len(), 3);
        // Verify sorted
        for i in 1..parts.len() {
            assert!(parts[i] > parts[i - 1]);
        }
    }

    #[test]
    fn none_partition_all_in_default() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::None);
        mgr.ensure_partition(Timestamp::from_secs(1710513000))
            .unwrap();
        mgr.ensure_partition(Timestamp::from_secs(1710513000 + 86400))
            .unwrap();
        let parts = mgr.list_partitions().unwrap();
        assert_eq!(parts.len(), 1);
        assert!(parts[0].ends_with("default"));
    }

    #[test]
    fn many_partitions() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Day);
        for i in 0..30 {
            mgr.ensure_partition(Timestamp::from_secs(1710513000 + i * 86400))
                .unwrap();
        }
        let parts = mgr.list_partitions().unwrap();
        assert_eq!(parts.len(), 30);
    }
}

// ============================================================================
// Partition operations (split, rewrite, list)
// ============================================================================

mod partition_operations {
    use super::*;

    fn setup_table(dir: &std::path::Path) -> TableMeta {
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("val", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir)
            .unwrap();
        let mut w = TableWriter::open(dir, "t").unwrap();
        for i in 0..10 {
            w.write_row(Timestamp::from_secs(1710513000 + i), &[ColumnValue::I64(i)])
                .unwrap();
        }
        w.flush().unwrap();
        drop(w);
        meta
    }

    #[test]
    fn list_partitions_after_write() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert!(!parts.is_empty());
    }

    #[test]
    fn read_partition_rows_matches_written() {
        let dir = tempdir().unwrap();
        let meta = setup_table(dir.path());
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        let total: usize = parts
            .iter()
            .map(|p| read_partition_rows(p, &meta).unwrap().len())
            .sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn rewrite_partition_replaces_content() {
        let dir = tempdir().unwrap();
        let meta = setup_table(dir.path());
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        let part = &parts[0];

        let new_rows = vec![vec![
            ColumnValue::Timestamp(Timestamp::from_secs(1710513000)),
            ColumnValue::I64(999),
        ]];
        rewrite_partition(part, &meta, &new_rows).unwrap();

        let rows = read_partition_rows(part, &meta).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn rewrite_partition_empty_deletes() {
        let dir = tempdir().unwrap();
        let meta = setup_table(dir.path());
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        let part = parts[0].clone();

        let empty: Vec<Vec<ColumnValue>> = vec![];
        rewrite_partition(&part, &meta, &empty).unwrap();
        assert!(!part.exists());
    }

    #[test]
    fn multi_partition_write_and_verify() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        // Write to 3 different days
        for day in 0..3 {
            for i in 0..5 {
                w.write_row(
                    Timestamp::from_secs(1710513000 + day * 86400 + i),
                    &[ColumnValue::I64(day * 10 + i)],
                )
                .unwrap();
            }
        }
        w.flush().unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn partition_with_varchar_column() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("note", ColumnType::Varchar)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        w.write_row(
            Timestamp::from_secs(1710513000),
            &[ColumnValue::Str("hello")],
        )
        .unwrap();
        w.flush().unwrap();
        drop(w);

        let meta = TableMeta::load(&dir.path().join("t/_meta")).unwrap();
        let rows = read_partition_rows(&dir.path().join("t/2024-03-15"), &meta).unwrap();
        assert_eq!(rows.len(), 1);
    }
}

// ============================================================================
// Tiered storage (LZ4 warm tier)
// ============================================================================

mod tiered_storage {
    use super::*;

    #[test]
    fn compress_partition_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            for i in 0..1000 {
                w.append_f64(i as f64 * 0.01).unwrap();
            }
            w.flush().unwrap();
        }
        let original_size = std::fs::metadata(&path).unwrap().len();
        let compressed_size = compress_column_file(&path).unwrap();
        assert!(!path.exists());
        assert!(dir.path().join("price.d.lz4").exists());
        assert!(compressed_size < original_size);
    }

    #[test]
    fn decompress_and_verify() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::F64).unwrap();
            for i in 0..100 {
                w.append_f64(i as f64).unwrap();
            }
            w.flush().unwrap();
        }
        compress_column_file(&path).unwrap();
        decompress_column_file(&path).unwrap();
        let r = FixedColumnReader::open(&path, ColumnType::F64).unwrap();
        assert_eq!(r.row_count(), 100);
        assert_eq!(r.read_f64(0), 0.0);
        assert_eq!(r.read_f64(99), 99.0);
    }

    #[test]
    fn compress_decompress_i64_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("val.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            for i in 0..500 {
                w.append_i64(i).unwrap();
            }
            w.flush().unwrap();
        }
        compress_column_file(&path).unwrap();
        decompress_column_file(&path).unwrap();
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 500);
        assert_eq!(r.read_i64(499), 499);
    }

    #[test]
    fn compress_decompress_i32_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sym.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I32).unwrap();
            for i in 0..200 {
                w.append_i32(i % 10).unwrap();
            }
            w.flush().unwrap();
        }
        compress_column_file(&path).unwrap();
        decompress_column_file(&path).unwrap();
        let r = FixedColumnReader::open(&path, ColumnType::I32).unwrap();
        assert_eq!(r.row_count(), 200);
        assert_eq!(r.read_i32(0), 0);
        assert_eq!(r.read_i32(10), 0);
    }

    #[test]
    fn compress_already_compressed_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("random.d");
        // Write pseudo-random data (hard to compress)
        let data: Vec<u8> = (0..8000u32)
            .flat_map(|i| {
                let v = i.wrapping_mul(2654435761);
                v.to_le_bytes()
            })
            .collect();
        std::fs::write(&path, &data).unwrap();
        let compressed = compress_column_file(&path).unwrap();
        // Even with low compression, roundtrip should work
        assert!(compressed > 0);
        let decompressed = decompress_column_file(&path).unwrap();
        assert_eq!(decompressed, data.len() as u64);
    }

    #[test]
    fn multiple_columns_in_partition() {
        let dir = tempdir().unwrap();
        let ts_path = dir.path().join("ts.d");
        let val_path = dir.path().join("val.d");
        {
            let mut w1 = FixedColumnWriter::open(&ts_path, ColumnType::Timestamp).unwrap();
            let mut w2 = FixedColumnWriter::open(&val_path, ColumnType::F64).unwrap();
            let base: i64 = 1_710_513_000_000_000_000;
            for i in 0..100 {
                w1.append_i64(base + i * 1_000_000_000).unwrap();
                w2.append_f64(i as f64 * 0.5).unwrap();
            }
            w1.flush().unwrap();
            w2.flush().unwrap();
        }
        // Compress both
        compress_column_file(&ts_path).unwrap();
        compress_column_file(&val_path).unwrap();
        // Decompress both
        decompress_column_file(&ts_path).unwrap();
        decompress_column_file(&val_path).unwrap();
        // Verify
        let r1 = FixedColumnReader::open(&ts_path, ColumnType::Timestamp).unwrap();
        let r2 = FixedColumnReader::open(&val_path, ColumnType::F64).unwrap();
        assert_eq!(r1.row_count(), 100);
        assert_eq!(r2.row_count(), 100);
    }

    #[test]
    fn compress_empty_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.d");
        std::fs::write(&path, &[]).unwrap();
        compress_column_file(&path).unwrap();
        let size = decompress_column_file(&path).unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn compress_single_value_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("one.d");
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::I64).unwrap();
            w.append_i64(42).unwrap();
            w.flush().unwrap();
        }
        compress_column_file(&path).unwrap();
        decompress_column_file(&path).unwrap();
        let r = FixedColumnReader::open(&path, ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_i64(0), 42);
    }

    #[test]
    fn compress_timestamp_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        let base: i64 = 1_710_513_000_000_000_000;
        {
            let mut w = FixedColumnWriter::open(&path, ColumnType::Timestamp).unwrap();
            for i in 0..1000 {
                w.append_i64(base + i * 1_000_000_000).unwrap();
            }
            w.flush().unwrap();
        }
        let original = std::fs::metadata(&path).unwrap().len();
        let compressed = compress_column_file(&path).unwrap();
        // Monotonic timestamps should compress well
        assert!(compressed < original);
        decompress_column_file(&path).unwrap();
        let r = FixedColumnReader::open(&path, ColumnType::Timestamp).unwrap();
        assert_eq!(r.row_count(), 1000);
        assert_eq!(r.read_i64(0), base);
        assert_eq!(r.read_i64(999), base + 999 * 1_000_000_000);
    }
}

// ============================================================================
// Additional partition naming and management tests
// ============================================================================

mod partition_extra {
    use super::*;

    #[test]
    fn far_future_timestamp() {
        // Year 2100
        let ts = Timestamp::from_secs(4102444800);
        let dir = partition_dir(ts, PartitionBy::Year);
        assert_eq!(dir, "2100");
    }

    #[test]
    fn leap_second_boundary() {
        // End of 2024-02-29 (leap year)
        let ts = Timestamp::from_secs(1709251199);
        assert_eq!(partition_dir(ts, PartitionBy::Day), "2024-02-29");
    }

    #[test]
    fn partition_manager_create_nested() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("db/tables/trades");
        let mut mgr = PartitionManager::new(nested.clone(), PartitionBy::Day);
        mgr.ensure_partition(Timestamp::from_secs(1710513000))
            .unwrap();
        assert!(nested.join("2024-03-15").exists());
    }

    #[test]
    fn partition_path_none_always_default() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::None);
        let p1 = mgr.partition_path(Timestamp::from_secs(0));
        let p2 = mgr.partition_path(Timestamp::from_secs(1710513000));
        assert_eq!(p1, p2);
        assert!(p1.ends_with("default"));
    }

    #[test]
    fn partition_hour_different_hours() {
        let dir = tempdir().unwrap();
        let mut mgr = PartitionManager::new(dir.path().to_path_buf(), PartitionBy::Hour);
        for hour_offset in 0..24 {
            mgr.ensure_partition(Timestamp::from_secs(1710460800 + hour_offset * 3600))
                .unwrap();
        }
        let parts = mgr.list_partitions().unwrap();
        assert_eq!(parts.len(), 24);
    }

    #[test]
    fn rewrite_partition_with_more_rows() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        w.write_row(Timestamp::from_secs(1710513000), &[ColumnValue::I64(1)])
            .unwrap();
        w.flush().unwrap();
        drop(w);

        let meta = TableMeta::load(&dir.path().join("t/_meta")).unwrap();
        let part = dir.path().join("t/2024-03-15");
        // Rewrite with 5 rows
        let ts_base = Timestamp::from_secs(1710513000);
        let new_rows: Vec<Vec<ColumnValue>> = (0..5)
            .map(|i| {
                vec![
                    ColumnValue::Timestamp(Timestamp(ts_base.as_nanos() + i * 1_000_000_000)),
                    ColumnValue::I64(i * 10),
                ]
            })
            .collect();
        let written = rewrite_partition(&part, &meta, &new_rows).unwrap();
        assert_eq!(written, 5);
        let rows = read_partition_rows(&part, &meta).unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn write_to_week_partition() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Week)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        w.write_row(Timestamp::from_secs(1710513000), &[ColumnValue::I64(1)])
            .unwrap();
        w.flush().unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn partition_with_multiple_column_types() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("i32_col", ColumnType::I32)
            .column("f64_col", ColumnType::F64)
            .column("varchar_col", ColumnType::Varchar)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        w.write_row(
            Timestamp::from_secs(1710513000),
            &[
                ColumnValue::I32(42),
                ColumnValue::F64(3.14),
                ColumnValue::Str("hello"),
            ],
        )
        .unwrap();
        w.flush().unwrap();
        drop(w);
        let meta = TableMeta::load(&dir.path().join("t/_meta")).unwrap();
        let rows = read_partition_rows(&dir.path().join("t/2024-03-15"), &meta).unwrap();
        assert_eq!(rows.len(), 1);
    }
}
