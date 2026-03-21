//! Row-by-row cursor over query results for streaming large result sets.
//!
//! Instead of materializing all rows in memory, reads partition by partition
//! on demand.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use std::path::PathBuf;

use crate::column::{FixedColumnReader, VarColumnReader};
use crate::table::{ColumnValue, TableMeta};

/// Row-by-row cursor over query results for streaming large result sets.
/// Instead of materializing all rows in memory, reads partition by partition.
pub struct TableCursor {
    meta: TableMeta,
    partitions: Vec<PathBuf>,
    current_partition_idx: usize,
    current_row: u64,
    partition_row_count: u64,
    columns_to_read: Vec<usize>,
    /// Lazily opened column readers for the current partition.
    fixed_readers: Vec<(usize, FixedColumnReader, ColumnType)>,
    var_readers: Vec<(usize, VarColumnReader)>,
}

impl TableCursor {
    /// Create a new cursor over all columns of a table.
    pub fn new(meta: TableMeta, partitions: Vec<PathBuf>) -> Self {
        let columns_to_read: Vec<usize> = (0..meta.columns.len()).collect();
        Self {
            meta,
            partitions,
            current_partition_idx: 0,
            current_row: 0,
            partition_row_count: 0,
            columns_to_read,
            fixed_readers: Vec::new(),
            var_readers: Vec::new(),
        }
    }

    /// Create a new cursor reading only specific column indices.
    pub fn with_columns(
        meta: TableMeta,
        partitions: Vec<PathBuf>,
        columns: Vec<usize>,
    ) -> Self {
        Self {
            meta,
            partitions,
            current_partition_idx: 0,
            current_row: 0,
            partition_row_count: 0,
            columns_to_read: columns,
            fixed_readers: Vec::new(),
            var_readers: Vec::new(),
        }
    }

    /// Open column readers for the partition at `current_partition_idx`.
    /// Returns `Ok(true)` if the partition was opened, `Ok(false)` if there
    /// are no more partitions.
    fn open_next_partition(&mut self) -> Result<bool> {
        if self.current_partition_idx >= self.partitions.len() {
            return Ok(false);
        }

        let partition_path = &self.partitions[self.current_partition_idx];

        self.fixed_readers.clear();
        self.var_readers.clear();

        for &col_idx in &self.columns_to_read {
            if col_idx >= self.meta.columns.len() {
                continue;
            }
            let col_def = &self.meta.columns[col_idx];
            let col_type: ColumnType = col_def.col_type.into();

            if col_type.is_variable_length() {
                let data_path = partition_path.join(format!("{}.d", col_def.name));
                let index_path = partition_path.join(format!("{}.i", col_def.name));
                if data_path.exists() && index_path.exists() {
                    let reader = VarColumnReader::open(&data_path, &index_path)?;
                    self.var_readers.push((col_idx, reader));
                }
            } else {
                let data_path = partition_path.join(format!("{}.d", col_def.name));
                if data_path.exists() {
                    let reader = FixedColumnReader::open(&data_path, col_type)?;
                    self.fixed_readers.push((col_idx, reader, col_type));
                }
            }
        }

        // Determine row count from the first available reader.
        self.partition_row_count = if let Some((_, reader, _)) = self.fixed_readers.first() {
            reader.row_count()
        } else if let Some((_, reader)) = self.var_readers.first() {
            reader.row_count()
        } else {
            0
        };

        self.current_row = 0;
        Ok(true)
    }

    /// Read one row from the current partition at `current_row`.
    fn read_current_row(&self) -> Result<Vec<ColumnValue<'static>>> {
        let mut row = vec![ColumnValue::Null; self.meta.columns.len()];

        for (col_idx, reader, col_type) in &self.fixed_readers {
            let val = match col_type {
                ColumnType::Timestamp => {
                    use exchange_common::types::Timestamp;
                    ColumnValue::Timestamp(Timestamp(reader.read_i64(self.current_row)))
                }
                ColumnType::I64 => ColumnValue::I64(reader.read_i64(self.current_row)),
                ColumnType::F64 => {
                    let v = reader.read_f64(self.current_row);
                    if v.is_nan() { ColumnValue::Null } else { ColumnValue::F64(v) }
                }
                ColumnType::I32 | ColumnType::Symbol => {
                    ColumnValue::I32(reader.read_i32(self.current_row))
                }
                ColumnType::F32 => ColumnValue::F64(
                    f32::from_le_bytes(
                        reader
                            .read_raw(self.current_row)
                            .try_into()
                            .unwrap(),
                    ) as f64,
                ),
                _ => ColumnValue::I64(reader.read_i64(self.current_row)),
            };
            row[*col_idx] = val;
        }

        for (col_idx, reader) in &self.var_readers {
            let s = reader.read_str(self.current_row).to_string();
            if s == "\0" {
                row[*col_idx] = ColumnValue::Null;
            } else {
                row[*col_idx] = ColumnValue::Str(Box::leak(s.into_boxed_str()));
            }
        }

        Ok(row)
    }

    /// Total number of partitions in this cursor.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Index of the current partition being read.
    pub fn current_partition_index(&self) -> usize {
        self.current_partition_idx
    }
}

impl Iterator for TableCursor {
    type Item = Result<Vec<ColumnValue<'static>>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have rows left in the current partition, read one.
            if self.current_row < self.partition_row_count {
                let row = self.read_current_row();
                self.current_row += 1;
                return Some(row);
            }

            // If we haven't opened the first partition yet, or need the next one.
            if self.current_partition_idx >= self.partitions.len() {
                return None;
            }

            // If partition_row_count is 0 and current_row is 0, we need to open.
            // If current_row == partition_row_count > 0, move to next.
            if self.partition_row_count > 0 {
                // Current partition exhausted, advance.
                self.current_partition_idx += 1;
            }

            match self.open_next_partition() {
                Ok(true) => {
                    // Skip empty partitions
                    if self.partition_row_count == 0 {
                        self.current_partition_idx += 1;
                        continue;
                    }
                }
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnValue, TableBuilder, TableWriter};
    use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
    use tempfile::tempdir;

    #[test]
    fn cursor_empty_table() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let partitions = crate::table::list_partitions(&db_root.join("trades")).unwrap();
        let cursor = TableCursor::new(meta, partitions);
        let rows: Vec<_> = cursor.collect();
        assert!(rows.is_empty());
    }

    #[test]
    fn cursor_single_partition() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        writer
            .write_row(ts, &[ColumnValue::F64(100.0)])
            .unwrap();
        writer
            .write_row(ts, &[ColumnValue::F64(200.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let partitions = crate::table::list_partitions(&db_root.join("trades")).unwrap();
        let cursor = TableCursor::new(meta, partitions);
        let rows: Vec<_> = cursor.collect();
        assert_eq!(rows.len(), 2);

        // Verify values
        let row0 = rows[0].as_ref().unwrap();
        match &row0[1] {
            ColumnValue::F64(v) => assert_eq!(*v, 100.0),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn cursor_across_partitions() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();

        // Day 1: 2024-03-15
        let ts1 = Timestamp::from_secs(1710513000);
        writer.write_row(ts1, &[ColumnValue::F64(100.0)]).unwrap();
        writer.write_row(ts1, &[ColumnValue::F64(101.0)]).unwrap();

        // Day 2: 2024-03-16
        let ts2 = Timestamp::from_secs(1710513000 + 86400);
        writer.write_row(ts2, &[ColumnValue::F64(200.0)]).unwrap();

        // Day 3: 2024-03-17
        let ts3 = Timestamp::from_secs(1710513000 + 2 * 86400);
        writer.write_row(ts3, &[ColumnValue::F64(300.0)]).unwrap();
        writer.write_row(ts3, &[ColumnValue::F64(301.0)]).unwrap();
        writer.write_row(ts3, &[ColumnValue::F64(302.0)]).unwrap();

        writer.flush().unwrap();
        drop(writer);

        let partitions = crate::table::list_partitions(&db_root.join("trades")).unwrap();
        assert_eq!(partitions.len(), 3);

        let cursor = TableCursor::new(meta, partitions);
        let rows: Vec<_> = cursor.collect();
        assert_eq!(rows.len(), 6); // 2 + 1 + 3

        // Verify last row
        let last = rows[5].as_ref().unwrap();
        match &last[1] {
            ColumnValue::F64(v) => assert_eq!(*v, 302.0),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn cursor_with_column_projection() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        writer
            .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(5.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let partitions = crate::table::list_partitions(&db_root.join("trades")).unwrap();

        // Only read column 1 (price)
        let cursor = TableCursor::with_columns(meta, partitions, vec![1]);
        let rows: Vec<_> = cursor.collect();
        assert_eq!(rows.len(), 1);

        let row = rows[0].as_ref().unwrap();
        // Column 1 should be populated
        match &row[1] {
            ColumnValue::F64(v) => assert_eq!(*v, 100.0),
            other => panic!("expected F64, got {other:?}"),
        }
        // Column 2 should be Null (not read)
        assert!(matches!(row[2], ColumnValue::Null));
    }
}
