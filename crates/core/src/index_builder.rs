//! Online index rebuild: reconstruct bitmap indexes for symbol columns.

use crate::column::FixedColumnReader;
use crate::index::bitmap::BitmapIndexWriter;
use crate::table::TableMeta;
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::ColumnType;
use std::path::Path;
use std::time::Instant;

/// Statistics from an index rebuild operation.
#[derive(Debug)]
pub struct IndexBuildStats {
    pub rows_indexed: u64,
    pub partitions_processed: u32,
    pub duration_ms: u64,
}

/// Rebuild the bitmap index for a given column across all partitions.
///
/// The column must be a fixed-width type (typically `Symbol`, which is `i32`).
/// Any existing index files (`.k`, `.v`) for this column are replaced.
///
/// This can run while the table is being read, but not while it is being
/// written (the caller must ensure exclusive write access).
pub fn rebuild_index(
    table_dir: &Path,
    column_name: &str,
    meta: &TableMeta,
) -> Result<IndexBuildStats> {
    let start = Instant::now();

    // Find the column definition.
    let col_def = meta
        .columns
        .iter()
        .find(|c| c.name == column_name)
        .ok_or_else(|| {
            ExchangeDbError::ColumnNotFound(column_name.to_string(), meta.name.clone())
        })?;

    let col_type: ColumnType = col_def.col_type.into();
    if col_type.is_variable_length() {
        return Err(ExchangeDbError::Corruption(
            "cannot build bitmap index on variable-length column".into(),
        ));
    }

    let partitions = crate::table::list_partitions(table_dir)?;

    let mut total_rows = 0u64;
    let mut partitions_processed = 0u32;

    for partition_path in &partitions {
        if !partition_path.is_dir() {
            continue;
        }

        let data_path = partition_path.join(format!("{}.d", column_name));
        if !data_path.exists() {
            continue;
        }

        // Remove existing index files.
        let key_path = partition_path.join(format!("{}.k", column_name));
        let val_path = partition_path.join(format!("{}.v", column_name));
        if key_path.exists() {
            std::fs::remove_file(&key_path)?;
        }
        if val_path.exists() {
            std::fs::remove_file(&val_path)?;
        }

        // Open column reader.
        let reader = FixedColumnReader::open(&data_path, col_type)?;
        let row_count = reader.row_count();

        if row_count == 0 {
            continue;
        }

        // Build new index.
        let mut index_writer = BitmapIndexWriter::open_default(partition_path, column_name)?;

        for row in 0..row_count {
            let key = reader.read_i32(row);
            if key >= 0 {
                index_writer.add(key, row)?;
            }
        }

        index_writer.flush()?;
        total_rows += row_count;
        partitions_processed += 1;
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(IndexBuildStats {
        rows_indexed: total_rows,
        partitions_processed,
        duration_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::bitmap::BitmapIndexReader;
    use crate::table::{ColumnValue, TableBuilder, TableWriter};
    use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
    use tempfile::tempdir;

    #[test]
    fn rebuild_index_produces_valid_index() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .indexed_column("symbol", ColumnType::Symbol)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Write some rows with different symbol IDs.
        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let base_ts = 1710513000i64;
        // symbol=0, symbol=1, symbol=0, symbol=2
        for (i, sym) in [0i32, 1, 0, 2].iter().enumerate() {
            writer
                .write_row(
                    Timestamp::from_secs(base_ts + i as i64),
                    &[ColumnValue::I32(*sym), ColumnValue::F64(100.0 + i as f64)],
                )
                .unwrap();
        }
        writer.flush().unwrap();
        drop(writer);

        let table_dir = db_root.join("trades");

        // Rebuild index.
        let stats = rebuild_index(&table_dir, "symbol", &meta).unwrap();
        assert_eq!(stats.rows_indexed, 4);
        assert_eq!(stats.partitions_processed, 1);

        // Verify the index.
        let part_path = table_dir.join("2024-03-15");
        let reader = BitmapIndexReader::open(&part_path, "symbol").unwrap();

        let rows_sym0 = reader.get_row_ids(0);
        assert_eq!(rows_sym0.len(), 2); // rows 0 and 2
        assert!(rows_sym0.contains(&0));
        assert!(rows_sym0.contains(&2));

        let rows_sym1 = reader.get_row_ids(1);
        assert_eq!(rows_sym1.len(), 1);
        assert!(rows_sym1.contains(&1));

        let rows_sym2 = reader.get_row_ids(2);
        assert_eq!(rows_sym2.len(), 1);
        assert!(rows_sym2.contains(&3));
    }

    #[test]
    fn rebuild_index_nonexistent_column_fails() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        let table_dir = db_root.join("trades");
        let result = rebuild_index(&table_dir, "nonexistent", &meta);
        assert!(result.is_err());
    }

    #[test]
    fn rebuild_index_replaces_existing() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .indexed_column("symbol", ColumnType::Symbol)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        writer
            .write_row(
                Timestamp::from_secs(1710513000),
                &[ColumnValue::I32(5), ColumnValue::F64(100.0)],
            )
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let table_dir = db_root.join("trades");

        // Build index twice — second run should replace the first.
        rebuild_index(&table_dir, "symbol", &meta).unwrap();
        let stats = rebuild_index(&table_dir, "symbol", &meta).unwrap();
        assert_eq!(stats.rows_indexed, 1);

        // Verify the index is still correct.
        let part_path = table_dir.join("2024-03-15");
        let reader = BitmapIndexReader::open(&part_path, "symbol").unwrap();
        let rows = reader.get_row_ids(5);
        assert_eq!(rows, vec![0]);
    }
}
