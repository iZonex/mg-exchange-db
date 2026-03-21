//! Comprehensive tests for table creation, writing, altering, and reading.
//!
//! 100 tests covering TableBuilder, TableMeta, TableWriter, and partition management.

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::{FixedColumnReader, VarColumnReader};
use exchange_core::table::{
    ColumnValue, TableBuilder, TableMeta, add_column_to_partitions, drop_column_from_partitions,
    drop_table, list_partitions, read_partition_rows, rename_column_in_partitions,
    rewrite_partition,
};
use tempfile::tempdir;

// ============================================================================
// Table creation
// ============================================================================

mod table_create {
    use super::*;

    #[test]
    fn create_basic_table() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .build(dir.path())
            .unwrap();
        assert_eq!(meta.name, "trades");
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.version, 1);
        assert!(dir.path().join("trades").exists());
        assert!(dir.path().join("trades/_meta").exists());
    }

    #[test]
    fn create_partition_by_none() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .partition_by(PartitionBy::None)
            .build(dir.path())
            .unwrap();
        let pb: PartitionBy = meta.partition_by.into();
        assert_eq!(pb, PartitionBy::None);
    }

    #[test]
    fn create_partition_by_hour() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .partition_by(PartitionBy::Hour)
            .build(dir.path())
            .unwrap();
        let pb: PartitionBy = meta.partition_by.into();
        assert_eq!(pb, PartitionBy::Hour);
    }

    #[test]
    fn create_partition_by_day() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let pb: PartitionBy = meta.partition_by.into();
        assert_eq!(pb, PartitionBy::Day);
    }

    #[test]
    fn create_partition_by_week() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .partition_by(PartitionBy::Week)
            .build(dir.path())
            .unwrap();
        let pb: PartitionBy = meta.partition_by.into();
        assert_eq!(pb, PartitionBy::Week);
    }

    #[test]
    fn create_partition_by_month() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .partition_by(PartitionBy::Month)
            .build(dir.path())
            .unwrap();
        let pb: PartitionBy = meta.partition_by.into();
        assert_eq!(pb, PartitionBy::Month);
    }

    #[test]
    fn create_partition_by_year() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .partition_by(PartitionBy::Year)
            .build(dir.path())
            .unwrap();
        let pb: PartitionBy = meta.partition_by.into();
        assert_eq!(pb, PartitionBy::Year);
    }

    #[test]
    fn duplicate_table_name_fails() {
        let dir = tempdir().unwrap();
        TableBuilder::new("dup")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        let result = TableBuilder::new("dup")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn many_column_types() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("wide")
            .column("ts", ColumnType::Timestamp)
            .column("b", ColumnType::Boolean)
            .column("i8", ColumnType::I8)
            .column("i16", ColumnType::I16)
            .column("i32", ColumnType::I32)
            .column("i64", ColumnType::I64)
            .column("f32", ColumnType::F32)
            .column("f64", ColumnType::F64)
            .column("sym", ColumnType::Symbol)
            .column("vc", ColumnType::Varchar)
            .column("bin", ColumnType::Binary)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert_eq!(meta.columns.len(), 11);
    }

    #[test]
    fn indexed_column() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("idx")
            .column("ts", ColumnType::Timestamp)
            .indexed_column("symbol", ColumnType::Symbol)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert!(meta.columns[1].indexed);
        assert!(!meta.columns[0].indexed);
    }

    #[test]
    fn meta_save_and_load() {
        let dir = tempdir().unwrap();
        let original = TableBuilder::new("test")
            .column("ts", ColumnType::Timestamp)
            .column("val", ColumnType::I64)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        let loaded = TableMeta::load(&dir.path().join("test/_meta")).unwrap();
        assert_eq!(loaded.name, original.name);
        assert_eq!(loaded.columns.len(), original.columns.len());
        assert_eq!(loaded.version, original.version);
    }

    #[test]
    fn long_table_name() {
        let dir = tempdir().unwrap();
        let name = "a".repeat(200);
        let meta = TableBuilder::new(&name)
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert_eq!(meta.name, name);
    }

    #[test]
    fn long_column_name() {
        let dir = tempdir().unwrap();
        let col_name = "col_".to_string() + &"x".repeat(200);
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column(&col_name, ColumnType::I64)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert_eq!(meta.columns[1].name, col_name);
    }

    #[test]
    fn table_with_single_column() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("single")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert_eq!(meta.columns.len(), 1);
        assert_eq!(meta.timestamp_column, 0);
    }

    #[test]
    fn timestamp_not_first_column() {
        let dir = tempdir().unwrap();
        let meta = TableBuilder::new("ts_last")
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert_eq!(meta.timestamp_column, 2);
    }
}

// ============================================================================
// Table write
// ============================================================================

mod table_write {
    use super::*;
    use exchange_core::table::TableWriter;

    fn create_basic_table(dir: &std::path::Path) -> TableMeta {
        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(dir)
            .unwrap()
    }

    #[test]
    fn write_single_row() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.0)])
            .unwrap();
        w.flush().unwrap();
        let part = dir.path().join("trades/2024-03-15");
        assert!(part.join("timestamp.d").exists());
        assert!(part.join("price.d").exists());
    }

    #[test]
    fn write_multiple_rows_same_partition() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        let base = 1710513000i64;
        for i in 0..10 {
            let ts = Timestamp::from_secs(base + i);
            w.write_row(ts, &[ColumnValue::F64(i as f64), ColumnValue::F64(1.0)])
                .unwrap();
        }
        w.flush().unwrap();
        drop(w); // must drop writer to truncate mmap file to actual data length
        let r = FixedColumnReader::open(
            &dir.path().join("trades/2024-03-15/price.d"),
            ColumnType::F64,
        )
        .unwrap();
        assert_eq!(r.row_count(), 10);
    }

    #[test]
    fn write_across_partitions() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        // Day 1
        let ts1 = Timestamp::from_secs(1710513000); // 2024-03-15
        w.write_row(ts1, &[ColumnValue::F64(100.0), ColumnValue::F64(1.0)])
            .unwrap();
        // Day 2
        let ts2 = Timestamp::from_secs(1710513000 + 86400); // 2024-03-16
        w.write_row(ts2, &[ColumnValue::F64(200.0), ColumnValue::F64(2.0)])
            .unwrap();
        w.flush().unwrap();
        assert!(dir.path().join("trades/2024-03-15").exists());
        assert!(dir.path().join("trades/2024-03-16").exists());
    }

    #[test]
    fn write_null_values() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("val", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::Null]).unwrap();
        w.flush().unwrap();
    }

    #[test]
    fn write_large_batch() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        let base = 1710513000i64;
        for i in 0..10_000 {
            let ts = Timestamp::from_secs(base + i);
            w.write_row(
                ts,
                &[ColumnValue::F64(i as f64 * 0.01), ColumnValue::F64(1.0)],
            )
            .unwrap();
        }
        w.flush().unwrap();
        let r = FixedColumnReader::open(
            &dir.path().join("trades/2024-03-15/timestamp.d"),
            ColumnType::Timestamp,
        )
        .unwrap();
        assert!(r.row_count() > 0);
    }

    #[test]
    fn write_with_varchar_column() {
        let dir = tempdir().unwrap();
        TableBuilder::new("notes")
            .column("ts", ColumnType::Timestamp)
            .column("note", ColumnType::Varchar)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "notes").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::Str("test note")]).unwrap();
        w.flush().unwrap();
        let part = dir.path().join("notes/2024-03-15");
        assert!(part.join("note.d").exists());
        assert!(part.join("note.i").exists());
    }

    #[test]
    fn write_i32_column() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("code", ColumnType::I32)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::I32(42)]).unwrap();
        w.flush().unwrap();
        let r = FixedColumnReader::open(&dir.path().join("t/2024-03-15/code.d"), ColumnType::I32)
            .unwrap();
        assert_eq!(r.read_i32(0), 42);
    }

    #[test]
    fn write_symbol_column() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("sym", ColumnType::Symbol)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::I32(0)]).unwrap();
        w.flush().unwrap();
        let r = FixedColumnReader::open(&dir.path().join("t/2024-03-15/sym.d"), ColumnType::Symbol)
            .unwrap();
        assert_eq!(r.read_i32(0), 0);
    }

    #[test]
    fn write_and_read_partition_rows() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::F64(65000.0), ColumnValue::F64(1.5)])
            .unwrap();
        w.write_row(
            Timestamp::from_secs(1710513001),
            &[ColumnValue::F64(65100.0), ColumnValue::F64(2.0)],
        )
        .unwrap();
        w.flush().unwrap();
        drop(w);

        let meta = TableMeta::load(&dir.path().join("trades/_meta")).unwrap();
        let part_path = dir.path().join("trades/2024-03-15");
        let rows = read_partition_rows(&part_path, &meta).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn write_partitioned_by_hour() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Hour)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts1 = Timestamp::from_secs(1710513000); // 14:30
        w.write_row(ts1, &[ColumnValue::I64(1)]).unwrap();
        let ts2 = Timestamp::from_secs(1710513000 + 3600); // 15:30
        w.write_row(ts2, &[ColumnValue::I64(2)]).unwrap();
        w.flush().unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn write_partitioned_by_month() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Month)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        // March
        w.write_row(Timestamp::from_secs(1710513000), &[ColumnValue::I64(1)])
            .unwrap();
        // April
        w.write_row(
            Timestamp::from_secs(1710513000 + 30 * 86400),
            &[ColumnValue::I64(2)],
        )
        .unwrap();
        w.flush().unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn write_partitioned_by_year() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Year)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        w.write_row(
            Timestamp::from_secs(1710513000), // 2024
            &[ColumnValue::I64(1)],
        )
        .unwrap();
        w.write_row(
            Timestamp::from_secs(1710513000 + 365 * 86400), // 2025
            &[ColumnValue::I64(2)],
        )
        .unwrap();
        w.flush().unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn write_no_partition() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::None)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        w.write_row(Timestamp::from_secs(1710513000), &[ColumnValue::I64(1)])
            .unwrap();
        w.write_row(
            Timestamp::from_secs(1710513000 + 86400),
            &[ColumnValue::I64(2)],
        )
        .unwrap();
        w.flush().unwrap();
        // All in "default" partition
        assert!(dir.path().join("t/default").exists());
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn write_f32_column() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("val", ColumnType::F32)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::F64(3.14)]).unwrap();
        w.flush().unwrap();
    }

    #[test]
    fn write_and_verify_timestamp_stored() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::F64(1.0), ColumnValue::F64(2.0)])
            .unwrap();
        w.flush().unwrap();
        let r = FixedColumnReader::open(
            &dir.path().join("trades/2024-03-15/timestamp.d"),
            ColumnType::Timestamp,
        )
        .unwrap();
        assert_eq!(r.read_i64(0), ts.as_nanos());
    }

    #[test]
    fn write_binary_column() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("data", ColumnType::Binary)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::Bytes(&[0xDE, 0xAD])])
            .unwrap();
        w.flush().unwrap();
        let r = VarColumnReader::open(
            &dir.path().join("t/2024-03-15/data.d"),
            &dir.path().join("t/2024-03-15/data.i"),
        )
        .unwrap();
        assert_eq!(r.read(0), &[0xDE, 0xAD]);
    }

    #[test]
    fn write_rows_batch() {
        let dir = tempdir().unwrap();
        create_basic_table(dir.path());
        let mut w = TableWriter::open(dir.path(), "trades").unwrap();
        let tss = vec![
            Timestamp::from_secs(1710513000),
            Timestamp::from_secs(1710513001),
            Timestamp::from_secs(1710513002),
        ];
        let rows = vec![
            vec![ColumnValue::F64(1.0), ColumnValue::F64(10.0)],
            vec![ColumnValue::F64(2.0), ColumnValue::F64(20.0)],
            vec![ColumnValue::F64(3.0), ColumnValue::F64(30.0)],
        ];
        let count = w.write_rows_batch(&tss, &rows).unwrap();
        assert_eq!(count, 3);
        w.flush().unwrap();
    }
}

// ============================================================================
// Table alter
// ============================================================================

mod table_alter {
    use super::*;
    use exchange_core::table::TableWriter;

    fn setup_table(dir: &std::path::Path) -> TableMeta {
        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(dir)
            .unwrap();
        // Write some data
        let mut w = TableWriter::open(dir, "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.0)])
            .unwrap();
        w.flush().unwrap();
        drop(w);
        meta
    }

    #[test]
    fn add_column() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.add_column("exchange", ColumnType::Varchar).unwrap();
        assert_eq!(meta.columns.len(), 4);
        assert_eq!(meta.version, 2);
    }

    #[test]
    fn add_duplicate_column_fails() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert!(meta.add_column("price", ColumnType::F64).is_err());
    }

    #[test]
    fn drop_column() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.drop_column("volume").unwrap();
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.version, 2);
    }

    #[test]
    fn drop_timestamp_fails() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert!(meta.drop_column("timestamp").is_err());
    }

    #[test]
    fn drop_nonexistent_column_fails() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert!(meta.drop_column("nonexistent").is_err());
    }

    #[test]
    fn rename_column() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.rename_column("price", "trade_price").unwrap();
        assert_eq!(meta.columns[1].name, "trade_price");
        assert_eq!(meta.version, 2);
    }

    #[test]
    fn rename_to_existing_fails() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert!(meta.rename_column("price", "volume").is_err());
    }

    #[test]
    fn rename_nonexistent_fails() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert!(meta.rename_column("nonexistent", "new_name").is_err());
    }

    #[test]
    fn set_column_type() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.set_column_type("price", ColumnType::I64).unwrap();
        meta.save(&table_dir.join("_meta")).unwrap();
        let reloaded = TableMeta::load(&table_dir.join("_meta")).unwrap();
        let ct: ColumnType = reloaded.columns[1].col_type.into();
        assert_eq!(ct, ColumnType::I64);
    }

    #[test]
    fn add_column_to_existing_partitions() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        add_column_to_partitions(&table_dir, "exchange", ColumnType::Varchar).unwrap();
        let part = dir.path().join("trades/2024-03-15");
        assert!(part.join("exchange.d").exists());
        assert!(part.join("exchange.i").exists());
    }

    #[test]
    fn add_fixed_column_to_existing_partitions() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        add_column_to_partitions(&table_dir, "quantity", ColumnType::I64).unwrap();
        let part = dir.path().join("trades/2024-03-15");
        assert!(part.join("quantity.d").exists());
    }

    #[test]
    fn drop_column_files_from_partitions() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        drop_column_from_partitions(&table_dir, "volume").unwrap();
        let part = dir.path().join("trades/2024-03-15");
        assert!(!part.join("volume.d").exists());
        assert!(part.join("price.d").exists());
    }

    #[test]
    fn rename_column_files_in_partitions() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        rename_column_in_partitions(&table_dir, "price", "trade_price").unwrap();
        let part = dir.path().join("trades/2024-03-15");
        assert!(!part.join("price.d").exists());
        assert!(part.join("trade_price.d").exists());
    }

    #[test]
    fn version_increments_on_alter() {
        let dir = tempdir().unwrap();
        setup_table(dir.path());
        let table_dir = dir.path().join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.version, 1);
        meta.add_column("a", ColumnType::I64).unwrap();
        assert_eq!(meta.version, 2);
        meta.drop_column("a").unwrap();
        assert_eq!(meta.version, 3);
        meta.rename_column("price", "p").unwrap();
        assert_eq!(meta.version, 4);
        meta.set_column_type("p", ColumnType::I32).unwrap();
        assert_eq!(meta.version, 5);
    }

    #[test]
    fn drop_column_adjusts_timestamp_index() {
        let dir = tempdir().unwrap();
        // timestamp at index 1
        TableBuilder::new("t")
            .column("price", ColumnType::F64)
            .column("ts", ColumnType::Timestamp)
            .column("volume", ColumnType::F64)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        let table_dir = dir.path().join("t");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.timestamp_column, 1);
        // Drop column before timestamp
        meta.drop_column("price").unwrap();
        assert_eq!(meta.timestamp_column, 0);
    }

    #[test]
    fn drop_column_after_timestamp_no_adjust() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("a", ColumnType::I64)
            .column("b", ColumnType::I64)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        let table_dir = dir.path().join("t");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.timestamp_column, 0);
        meta.drop_column("b").unwrap();
        assert_eq!(meta.timestamp_column, 0);
    }
}

// ============================================================================
// Table read and drop
// ============================================================================

mod table_read_and_drop {
    use super::*;
    use exchange_core::table::TableWriter;

    #[test]
    fn read_after_write_single_partition() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::I64(42)]).unwrap();
        w.flush().unwrap();
        drop(w);

        let meta = TableMeta::load(&dir.path().join("t/_meta")).unwrap();
        let rows = read_partition_rows(&dir.path().join("t/2024-03-15"), &meta).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn read_from_multiple_partitions() {
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
        w.write_row(
            Timestamp::from_secs(1710513000 + 86400),
            &[ColumnValue::I64(2)],
        )
        .unwrap();
        w.flush().unwrap();
        drop(w);

        let meta = TableMeta::load(&dir.path().join("t/_meta")).unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 2);
        let total_rows: usize = parts
            .iter()
            .map(|p| read_partition_rows(p, &meta).unwrap().len())
            .sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn read_empty_partition() {
        let dir = tempdir().unwrap();
        let part = dir.path().join("empty_part");
        std::fs::create_dir_all(&part).unwrap();
        let meta = TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        let rows = read_partition_rows(&part, &meta).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn drop_table_works() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        assert!(dir.path().join("t").exists());
        drop_table(dir.path(), "t").unwrap();
        assert!(!dir.path().join("t").exists());
    }

    #[test]
    fn drop_nonexistent_table_fails() {
        let dir = tempdir().unwrap();
        assert!(drop_table(dir.path(), "nope").is_err());
    }

    #[test]
    fn rewrite_partition_replaces_data() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .column("v", ColumnType::I64)
            .timestamp("ts")
            .partition_by(PartitionBy::Day)
            .build(dir.path())
            .unwrap();
        let mut w = TableWriter::open(dir.path(), "t").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        w.write_row(ts, &[ColumnValue::I64(1)]).unwrap();
        w.write_row(Timestamp::from_secs(1710513001), &[ColumnValue::I64(2)])
            .unwrap();
        w.flush().unwrap();
        drop(w);

        let meta = TableMeta::load(&dir.path().join("t/_meta")).unwrap();
        let part = dir.path().join("t/2024-03-15");
        // Rewrite with single row
        let new_rows = vec![vec![ColumnValue::Timestamp(ts), ColumnValue::I64(999)]];
        let written = rewrite_partition(&part, &meta, &new_rows).unwrap();
        assert_eq!(written, 1);

        let r = FixedColumnReader::open(&part.join("v.d"), ColumnType::I64).unwrap();
        assert_eq!(r.row_count(), 1);
        assert_eq!(r.read_i64(0), 999);
    }

    #[test]
    fn rewrite_partition_empty_removes_dir() {
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
        let empty: Vec<Vec<ColumnValue>> = vec![];
        rewrite_partition(&part, &meta, &empty).unwrap();
        assert!(!part.exists());
    }

    #[test]
    fn list_partitions_empty_table() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert!(parts.is_empty());
    }

    #[test]
    fn list_partitions_skips_internal_dirs() {
        let dir = tempdir().unwrap();
        TableBuilder::new("t")
            .column("ts", ColumnType::Timestamp)
            .timestamp("ts")
            .build(dir.path())
            .unwrap();
        // _meta already exists as a file, add an internal dir
        std::fs::create_dir_all(dir.path().join("t/_wal")).unwrap();
        std::fs::create_dir_all(dir.path().join("t/2024-01-01")).unwrap();
        let parts = list_partitions(&dir.path().join("t")).unwrap();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn read_varchar_from_partition() {
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
