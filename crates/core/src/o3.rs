//! Out-of-order (O3) data handling.
//!
//! When rows arrive with timestamps earlier than the partition's current
//! maximum, they are "out of order". This module provides:
//!
//! - [`O3SortBuffer`] — collects out-of-order rows, sorts them by timestamp.
//! - [`O3MergeStrategy`] — enum selecting how to merge O3 data.
//! - [`merge_sorted_into_partition`] — reads existing partition data, merges
//!   with new sorted rows, and rewrites the partition.

use std::collections::HashMap;
use std::path::Path;

use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::ColumnType;

use crate::column::{FixedColumnReader, FixedColumnWriter, VarColumnReader, VarColumnWriter};
use crate::table::ColumnDef;
use crate::wal::row_codec::OwnedColumnValue;

/// Strategy for merging out-of-order data into a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum O3MergeStrategy {
    /// Sort the new rows, then merge with existing data in a streaming fashion.
    Sort,
    /// Read the entire partition plus new rows, sort everything, rewrite.
    Rewrite,
}

/// A buffer that collects rows arriving out of timestamp order and can sort
/// them before writing.
pub struct O3SortBuffer {
    /// Index of the timestamp column in each row.
    ts_col_idx: usize,
    /// Accumulated rows.
    rows: Vec<Vec<OwnedColumnValue>>,
}

impl O3SortBuffer {
    /// Create a new sort buffer.
    ///
    /// `ts_col_idx` is the index of the designated timestamp column within
    /// each row's value vector.
    pub fn new(ts_col_idx: usize) -> Self {
        Self {
            ts_col_idx,
            rows: Vec::new(),
        }
    }

    /// Add a row to the buffer.
    pub fn push(&mut self, row: Vec<OwnedColumnValue>) {
        self.rows.push(row);
    }

    /// Number of buffered rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Sort all buffered rows by the designated timestamp column (ascending).
    pub fn sort(&mut self) {
        let idx = self.ts_col_idx;
        self.rows.sort_by(|a, b| {
            let ta = extract_ts(&a[idx]);
            let tb = extract_ts(&b[idx]);
            ta.cmp(&tb)
        });
    }

    /// Consume the buffer and return the (sorted) rows.
    pub fn into_sorted_rows(mut self) -> Vec<Vec<OwnedColumnValue>> {
        self.sort();
        self.rows
    }

    /// Return a reference to the buffered rows (unsorted).
    pub fn rows(&self) -> &[Vec<OwnedColumnValue>] {
        &self.rows
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.rows.clear();
    }
}

/// Merge new sorted rows into an existing partition using the **Rewrite**
/// strategy:
///
/// 1. Read all existing rows from the partition's column files.
/// 2. Concatenate with `sorted_rows`.
/// 3. Sort everything by the timestamp column.
/// 4. Rewrite all column files from scratch.
///
/// Returns the total number of rows after the merge.
pub fn merge_sorted_into_partition(
    partition_path: &Path,
    sorted_rows: Vec<Vec<OwnedColumnValue>>,
    column_defs: &[ColumnDef],
    ts_col_idx: usize,
) -> Result<u64> {
    let column_types: Vec<ColumnType> = column_defs.iter().map(|c| c.col_type.into()).collect();

    // Step 1: Read existing rows from column files.
    let existing_rows = read_partition_rows(partition_path, column_defs, &column_types)?;

    // Step 2: Concatenate.
    let mut all_rows = existing_rows;
    all_rows.extend(sorted_rows);

    // Step 3: Sort by timestamp.
    all_rows.sort_by(|a, b| {
        let ta = extract_ts(&a[ts_col_idx]);
        let tb = extract_ts(&b[ts_col_idx]);
        ta.cmp(&tb)
    });

    let total_rows = all_rows.len() as u64;

    // Step 4: Rewrite column files.
    // First, remove existing column files so we start fresh.
    for (i, col_def) in column_defs.iter().enumerate() {
        let ct = column_types[i];
        let data_path = partition_path.join(format!("{}.d", col_def.name));
        if data_path.exists() {
            std::fs::remove_file(&data_path)?;
        }
        if ct.is_variable_length() {
            let index_path = partition_path.join(format!("{}.i", col_def.name));
            if index_path.exists() {
                std::fs::remove_file(&index_path)?;
            }
        }
    }

    // Open writers and write all rows.
    let mut fixed_writers: HashMap<usize, FixedColumnWriter> = HashMap::new();
    let mut var_writers: HashMap<usize, VarColumnWriter> = HashMap::new();

    for (i, col_def) in column_defs.iter().enumerate() {
        let ct = column_types[i];
        if ct.is_variable_length() {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            let index_path = partition_path.join(format!("{}.i", col_def.name));
            var_writers.insert(i, VarColumnWriter::open(&data_path, &index_path)?);
        } else {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            fixed_writers.insert(i, FixedColumnWriter::open(&data_path, ct)?);
        }
    }

    for row in &all_rows {
        for (i, _col_def) in column_defs.iter().enumerate() {
            let ct = column_types[i];
            let val = &row[i];

            if ct.is_variable_length() {
                let w = var_writers.get_mut(&i).unwrap();
                match val {
                    OwnedColumnValue::Varchar(s) => w.append_str(s)?,
                    OwnedColumnValue::Binary(b) => w.append(b)?,
                    _ => w.append(b"")?,
                }
            } else {
                let w = fixed_writers.get_mut(&i).unwrap();
                write_fixed_value(w, ct, val)?;
            }
        }
    }

    for w in fixed_writers.values() {
        w.flush()?;
    }
    for w in var_writers.values() {
        w.flush()?;
    }

    Ok(total_rows)
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Extract a timestamp (as i64 nanos) from a column value, for sorting.
fn extract_ts(val: &OwnedColumnValue) -> i64 {
    match val {
        OwnedColumnValue::Timestamp(n) => *n,
        OwnedColumnValue::I64(n) => *n,
        _ => 0,
    }
}

/// Read all rows from a partition's column files.
fn read_partition_rows(
    partition_path: &Path,
    column_defs: &[ColumnDef],
    column_types: &[ColumnType],
) -> Result<Vec<Vec<OwnedColumnValue>>> {
    if !partition_path.exists() {
        return Ok(Vec::new());
    }

    // Determine row count from the first column file.
    let first_data = partition_path.join(format!("{}.d", column_defs[0].name));
    if !first_data.exists() {
        return Ok(Vec::new());
    }

    let row_count = if column_types[0].is_variable_length() {
        let index_path = partition_path.join(format!("{}.i", column_defs[0].name));
        if !index_path.exists() {
            return Ok(Vec::new());
        }
        let r = VarColumnReader::open(&first_data, &index_path)?;
        r.row_count()
    } else {
        let r = FixedColumnReader::open(&first_data, column_types[0])?;
        r.row_count()
    };

    if row_count == 0 {
        return Ok(Vec::new());
    }

    // Open readers for all columns.
    let mut fixed_readers: HashMap<usize, FixedColumnReader> = HashMap::new();
    let mut var_readers: HashMap<usize, VarColumnReader> = HashMap::new();

    for (i, col_def) in column_defs.iter().enumerate() {
        let ct = column_types[i];
        if ct.is_variable_length() {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            let index_path = partition_path.join(format!("{}.i", col_def.name));
            var_readers.insert(i, VarColumnReader::open(&data_path, &index_path)?);
        } else {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            fixed_readers.insert(i, FixedColumnReader::open(&data_path, ct)?);
        }
    }

    // Read rows column by column.
    let mut rows: Vec<Vec<OwnedColumnValue>> = Vec::with_capacity(row_count as usize);
    for _ in 0..row_count {
        rows.push(Vec::with_capacity(column_defs.len()));
    }

    for (i, _col_def) in column_defs.iter().enumerate() {
        let ct = column_types[i];

        if ct.is_variable_length() {
            let reader = var_readers.get(&i).unwrap();
            for row_idx in 0..row_count {
                let bytes = reader.read(row_idx);
                let val = match ct {
                    ColumnType::Varchar => {
                        let s = String::from_utf8(bytes.to_vec())
                            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
                        OwnedColumnValue::Varchar(s)
                    }
                    _ => OwnedColumnValue::Binary(bytes.to_vec()),
                };
                rows[row_idx as usize].push(val);
            }
        } else {
            let reader = fixed_readers.get(&i).unwrap();
            for row_idx in 0..row_count {
                let raw = reader.read_raw(row_idx);
                let val = read_fixed_value(ct, raw);
                rows[row_idx as usize].push(val);
            }
        }
    }

    Ok(rows)
}

/// Convert raw bytes from a FixedColumnReader into an OwnedColumnValue.
fn read_fixed_value(ct: ColumnType, raw: &[u8]) -> OwnedColumnValue {
    match ct {
        ColumnType::Boolean => OwnedColumnValue::Boolean(raw[0] != 0),
        ColumnType::I8 => OwnedColumnValue::I8(raw[0] as i8),
        ColumnType::I16 => OwnedColumnValue::I16(i16::from_le_bytes(raw.try_into().unwrap())),
        ColumnType::I32 => OwnedColumnValue::I32(i32::from_le_bytes(raw.try_into().unwrap())),
        ColumnType::I64 => OwnedColumnValue::I64(i64::from_le_bytes(raw.try_into().unwrap())),
        ColumnType::F32 => OwnedColumnValue::F32(f32::from_le_bytes(raw.try_into().unwrap())),
        ColumnType::F64 => OwnedColumnValue::F64(f64::from_le_bytes(raw.try_into().unwrap())),
        ColumnType::Timestamp => {
            OwnedColumnValue::Timestamp(i64::from_le_bytes(raw.try_into().unwrap()))
        }
        ColumnType::Symbol => OwnedColumnValue::Symbol(i32::from_le_bytes(raw.try_into().unwrap())),
        ColumnType::Uuid => {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(raw);
            OwnedColumnValue::Uuid(arr)
        }
        _ => OwnedColumnValue::Null,
    }
}

/// Write a single fixed-width value into a FixedColumnWriter.
fn write_fixed_value(
    w: &mut FixedColumnWriter,
    ct: ColumnType,
    val: &OwnedColumnValue,
) -> Result<()> {
    match (ct, val) {
        (ColumnType::Boolean, OwnedColumnValue::Boolean(v)) => {
            w.append(&[if *v { 1 } else { 0 }])
        }
        (ColumnType::I8, OwnedColumnValue::I8(v)) => w.append(&[*v as u8]),
        (ColumnType::I16, OwnedColumnValue::I16(v)) => w.append(&v.to_le_bytes()),
        (ColumnType::I32, OwnedColumnValue::I32(v)) => w.append_i32(*v),
        (ColumnType::I64, OwnedColumnValue::I64(v)) => w.append_i64(*v),
        (ColumnType::F32, OwnedColumnValue::F32(v)) => w.append(&v.to_le_bytes()),
        (ColumnType::F64, OwnedColumnValue::F64(v)) => w.append_f64(*v),
        (ColumnType::F64, OwnedColumnValue::Null) => w.append_f64(f64::NAN),
        (ColumnType::Timestamp, OwnedColumnValue::Timestamp(v)) => w.append_i64(*v),
        (ColumnType::Symbol, OwnedColumnValue::Symbol(v)) => w.append_i32(*v),
        (ColumnType::Uuid, OwnedColumnValue::Uuid(v)) => w.append(v),
        _ => {
            let size = ct.fixed_size().unwrap_or(8);
            let zeroes = vec![0u8; size];
            w.append(&zeroes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, ColumnTypeSerializable};
    use crate::column::FixedColumnReader;
    use tempfile::tempdir;

    #[test]
    fn sort_buffer_basic() {
        let mut buf = O3SortBuffer::new(0);
        assert!(buf.is_empty());

        buf.push(vec![
            OwnedColumnValue::Timestamp(300),
            OwnedColumnValue::I64(3),
        ]);
        buf.push(vec![
            OwnedColumnValue::Timestamp(100),
            OwnedColumnValue::I64(1),
        ]);
        buf.push(vec![
            OwnedColumnValue::Timestamp(200),
            OwnedColumnValue::I64(2),
        ]);

        assert_eq!(buf.len(), 3);

        let sorted = buf.into_sorted_rows();
        assert_eq!(sorted[0][0], OwnedColumnValue::Timestamp(100));
        assert_eq!(sorted[1][0], OwnedColumnValue::Timestamp(200));
        assert_eq!(sorted[2][0], OwnedColumnValue::Timestamp(300));
    }

    #[test]
    fn sort_buffer_already_sorted() {
        let mut buf = O3SortBuffer::new(0);
        buf.push(vec![OwnedColumnValue::Timestamp(1)]);
        buf.push(vec![OwnedColumnValue::Timestamp(2)]);
        buf.push(vec![OwnedColumnValue::Timestamp(3)]);

        let sorted = buf.into_sorted_rows();
        assert_eq!(sorted[0][0], OwnedColumnValue::Timestamp(1));
        assert_eq!(sorted[2][0], OwnedColumnValue::Timestamp(3));
    }

    #[test]
    fn sort_buffer_empty() {
        let buf = O3SortBuffer::new(0);
        let sorted = buf.into_sorted_rows();
        assert!(sorted.is_empty());
    }

    #[test]
    fn sort_buffer_single_row() {
        let mut buf = O3SortBuffer::new(0);
        buf.push(vec![OwnedColumnValue::Timestamp(42)]);
        let sorted = buf.into_sorted_rows();
        assert_eq!(sorted.len(), 1);
    }

    #[test]
    fn sort_buffer_clear() {
        let mut buf = O3SortBuffer::new(0);
        buf.push(vec![OwnedColumnValue::Timestamp(1)]);
        buf.push(vec![OwnedColumnValue::Timestamp(2)]);
        buf.clear();
        assert!(buf.is_empty());
    }

    #[test]
    fn merge_into_empty_partition() {
        let dir = tempdir().unwrap();
        let part_path = dir.path().join("2024-01-01");
        std::fs::create_dir_all(&part_path).unwrap();

        let col_defs = vec![
            ColumnDef {
                name: "ts".into(),
                col_type: ColumnTypeSerializable::Timestamp,
                indexed: false,
            },
            ColumnDef {
                name: "val".into(),
                col_type: ColumnTypeSerializable::I64,
                indexed: false,
            },
        ];

        let new_rows = vec![
            vec![OwnedColumnValue::Timestamp(200), OwnedColumnValue::I64(20)],
            vec![OwnedColumnValue::Timestamp(100), OwnedColumnValue::I64(10)],
        ];

        let total = merge_sorted_into_partition(&part_path, new_rows, &col_defs, 0).unwrap();
        assert_eq!(total, 2);

        // Verify sorted order.
        let ts_reader =
            FixedColumnReader::open(&part_path.join("ts.d"), ColumnType::Timestamp).unwrap();
        assert_eq!(ts_reader.row_count(), 2);
        assert_eq!(ts_reader.read_i64(0), 100);
        assert_eq!(ts_reader.read_i64(1), 200);

        let val_reader =
            FixedColumnReader::open(&part_path.join("val.d"), ColumnType::I64).unwrap();
        assert_eq!(val_reader.read_i64(0), 10);
        assert_eq!(val_reader.read_i64(1), 20);
    }

    #[test]
    fn merge_into_existing_partition() {
        let dir = tempdir().unwrap();
        let part_path = dir.path().join("2024-01-01");
        std::fs::create_dir_all(&part_path).unwrap();

        let col_defs = vec![
            ColumnDef {
                name: "ts".into(),
                col_type: ColumnTypeSerializable::Timestamp,
                indexed: false,
            },
            ColumnDef {
                name: "val".into(),
                col_type: ColumnTypeSerializable::I64,
                indexed: false,
            },
        ];

        // Write initial data: timestamps 100, 300.
        {
            let mut ts_w =
                FixedColumnWriter::open(&part_path.join("ts.d"), ColumnType::Timestamp).unwrap();
            let mut val_w =
                FixedColumnWriter::open(&part_path.join("val.d"), ColumnType::I64).unwrap();

            ts_w.append_i64(100).unwrap();
            val_w.append_i64(10).unwrap();

            ts_w.append_i64(300).unwrap();
            val_w.append_i64(30).unwrap();

            ts_w.flush().unwrap();
            val_w.flush().unwrap();
        }

        // Merge new O3 row with timestamp 200 (out of order relative to 300).
        let new_rows = vec![
            vec![OwnedColumnValue::Timestamp(200), OwnedColumnValue::I64(20)],
        ];

        let total = merge_sorted_into_partition(&part_path, new_rows, &col_defs, 0).unwrap();
        assert_eq!(total, 3);

        // Verify sorted order: 100, 200, 300.
        let ts_reader =
            FixedColumnReader::open(&part_path.join("ts.d"), ColumnType::Timestamp).unwrap();
        assert_eq!(ts_reader.row_count(), 3);
        assert_eq!(ts_reader.read_i64(0), 100);
        assert_eq!(ts_reader.read_i64(1), 200);
        assert_eq!(ts_reader.read_i64(2), 300);

        let val_reader =
            FixedColumnReader::open(&part_path.join("val.d"), ColumnType::I64).unwrap();
        assert_eq!(val_reader.read_i64(0), 10);
        assert_eq!(val_reader.read_i64(1), 20);
        assert_eq!(val_reader.read_i64(2), 30);
    }

    #[test]
    fn merge_with_varchar_columns() {
        let dir = tempdir().unwrap();
        let part_path = dir.path().join("2024-01-01");
        std::fs::create_dir_all(&part_path).unwrap();

        let col_defs = vec![
            ColumnDef {
                name: "ts".into(),
                col_type: ColumnTypeSerializable::Timestamp,
                indexed: false,
            },
            ColumnDef {
                name: "name".into(),
                col_type: ColumnTypeSerializable::Varchar,
                indexed: false,
            },
        ];

        // Write initial data.
        {
            let mut ts_w =
                FixedColumnWriter::open(&part_path.join("ts.d"), ColumnType::Timestamp).unwrap();
            let mut name_w = VarColumnWriter::open(
                &part_path.join("name.d"),
                &part_path.join("name.i"),
            )
            .unwrap();

            ts_w.append_i64(100).unwrap();
            name_w.append_str("alpha").unwrap();

            ts_w.append_i64(300).unwrap();
            name_w.append_str("gamma").unwrap();

            ts_w.flush().unwrap();
            name_w.flush().unwrap();
        }

        // Merge a row with timestamp 200.
        let new_rows = vec![vec![
            OwnedColumnValue::Timestamp(200),
            OwnedColumnValue::Varchar("beta".into()),
        ]];

        let total = merge_sorted_into_partition(&part_path, new_rows, &col_defs, 0).unwrap();
        assert_eq!(total, 3);

        // Verify order.
        let ts_reader =
            FixedColumnReader::open(&part_path.join("ts.d"), ColumnType::Timestamp).unwrap();
        assert_eq!(ts_reader.read_i64(0), 100);
        assert_eq!(ts_reader.read_i64(1), 200);
        assert_eq!(ts_reader.read_i64(2), 300);

        let name_reader = VarColumnReader::open(
            &part_path.join("name.d"),
            &part_path.join("name.i"),
        )
        .unwrap();
        assert_eq!(name_reader.read_str(0), "alpha");
        assert_eq!(name_reader.read_str(1), "beta");
        assert_eq!(name_reader.read_str(2), "gamma");
    }
}
