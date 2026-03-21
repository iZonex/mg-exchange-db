//! Batch-oriented scan and filter pipeline.
//!
//! Processes column data in fixed-size batches (default 8192 rows) for
//! maximum throughput. Avoids per-row overhead by operating on contiguous
//! slices.

use std::path::Path;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use exchange_core::mmap::MmapReadOnly;

use crate::batch::RecordBatch;
use crate::compiled_filter::FilterFn;
use crate::plan::Value;

/// Default batch size in rows.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// Process a column scan in batches for maximum throughput.
///
/// 1. Memory-maps all column files in `partition_path`.
/// 2. Processes rows in batches of `batch_size`.
/// 3. Applies the compiled filter (if any) per batch.
/// 4. Produces `RecordBatch` output.
///
/// Column files are expected at `{partition_path}/{column_name}.d`.
/// Each file contains tightly packed little-endian values (8 bytes for
/// numeric types, variable for strings).
pub fn batch_scan_and_filter(
    partition_path: &Path,
    columns: &[(String, ColumnType)],
    filter: Option<&FilterFn>,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    let batch_size = if batch_size == 0 {
        DEFAULT_BATCH_SIZE
    } else {
        batch_size
    };

    // Mmap all column files.
    let mmaps: Vec<Option<MmapReadOnly>> = columns
        .iter()
        .map(|(name, _)| {
            let path = partition_path.join(format!("{name}.d"));
            MmapReadOnly::open(&path).ok()
        })
        .collect();

    // Determine total row count from the first successfully mapped numeric column.
    let total_rows = columns
        .iter()
        .zip(mmaps.iter())
        .find_map(|((_, ct), mmap)| {
            let elem_size = element_size(*ct)?;
            mmap.as_ref().map(|m| m.len() as usize / elem_size)
        })
        .unwrap_or(0);

    let mut batches = Vec::new();
    let mut offset = 0;

    while offset < total_rows {
        let batch_end = (offset + batch_size).min(total_rows);
        let batch_len = batch_end - offset;

        let schema: Vec<(String, ColumnType)> = columns.to_vec();
        let mut batch = RecordBatch::new(schema);

        for row in offset..batch_end {
            let row_values: Vec<Value> = columns
                .iter()
                .enumerate()
                .map(|(col_idx, (_, ct))| read_value(&mmaps[col_idx], row, *ct))
                .collect();

            let passes = match filter {
                Some(f) => f(&row_values),
                None => true,
            };

            if passes {
                batch.append_row(&row_values);
            }
        }

        if batch.row_count() > 0 {
            batches.push(batch);
        }

        offset += batch_len;
    }

    Ok(batches)
}

/// Return the byte size of a single element for a given column type,
/// or `None` for variable-length types.
fn element_size(ct: ColumnType) -> Option<usize> {
    match ct {
        ColumnType::I64 | ColumnType::F64 | ColumnType::Timestamp => Some(8),
        ColumnType::I32 | ColumnType::F32 | ColumnType::Date => Some(4),
        ColumnType::I16 => Some(2),
        ColumnType::I8 | ColumnType::Boolean => Some(1),
        _ => None,
    }
}

/// Read a single `Value` from a memory-mapped column file at `row` index.
fn read_value(mmap: &Option<MmapReadOnly>, row: usize, ct: ColumnType) -> Value {
    let mmap = match mmap {
        Some(m) => m,
        None => return Value::Null,
    };

    match ct {
        ColumnType::I64 | ColumnType::Symbol => {
            let offset = row * 8;
            if offset + 8 > mmap.len() as usize {
                return Value::Null;
            }
            let bytes: [u8; 8] = mmap.read_at(offset as u64, 8).try_into().unwrap();
            Value::I64(i64::from_le_bytes(bytes))
        }
        ColumnType::F64 => {
            let offset = row * 8;
            if offset + 8 > mmap.len() as usize {
                return Value::Null;
            }
            let bytes: [u8; 8] = mmap.read_at(offset as u64, 8).try_into().unwrap();
            let v = f64::from_le_bytes(bytes);
            if v.is_nan() {
                Value::Null
            } else {
                Value::F64(v)
            }
        }
        ColumnType::Timestamp | ColumnType::Date => {
            let offset = row * 8;
            if offset + 8 > mmap.len() as usize {
                return Value::Null;
            }
            let bytes: [u8; 8] = mmap.read_at(offset as u64, 8).try_into().unwrap();
            Value::Timestamp(i64::from_le_bytes(bytes))
        }
        ColumnType::I32 => {
            let offset = row * 4;
            if offset + 4 > mmap.len() as usize {
                return Value::Null;
            }
            let bytes: [u8; 4] = mmap.read_at(offset as u64, 4).try_into().unwrap();
            Value::I64(i32::from_le_bytes(bytes) as i64)
        }
        ColumnType::F32 => {
            let offset = row * 4;
            if offset + 4 > mmap.len() as usize {
                return Value::Null;
            }
            let bytes: [u8; 4] = mmap.read_at(offset as u64, 4).try_into().unwrap();
            Value::F64(f32::from_le_bytes(bytes) as f64)
        }
        ColumnType::Boolean => {
            let offset = row;
            if offset >= mmap.len() as usize {
                return Value::Null;
            }
            let b = mmap.read_at(offset as u64, 1)[0];
            Value::I64(if b != 0 { 1 } else { 0 })
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn write_i64_column(dir: &Path, name: &str, values: &[i64]) {
        let path = dir.join(format!("{name}.d"));
        let mut data = Vec::new();
        for v in values {
            data.extend_from_slice(&v.to_le_bytes());
        }
        std::fs::write(&path, &data).unwrap();
    }

    fn write_f64_column(dir: &Path, name: &str, values: &[f64]) {
        let path = dir.join(format!("{name}.d"));
        let mut data = Vec::new();
        for v in values {
            data.extend_from_slice(&v.to_le_bytes());
        }
        std::fs::write(&path, &data).unwrap();
    }

    #[test]
    fn batch_scan_no_filter() {
        let dir = tempdir().unwrap();
        let part = dir.path();

        write_i64_column(part, "id", &[1, 2, 3, 4, 5]);
        write_f64_column(part, "price", &[100.0, 200.0, 300.0, 400.0, 500.0]);

        let columns = vec![
            ("id".into(), ColumnType::I64),
            ("price".into(), ColumnType::F64),
        ];

        let batches = batch_scan_and_filter(part, &columns, None, 3).unwrap();

        // 5 rows, batch_size=3 -> 2 batches
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].row_count(), 3);
        assert_eq!(batches[1].row_count(), 2);

        // Verify values.
        assert_eq!(batches[0].get_value(0, 0), Value::I64(1));
        assert_eq!(batches[0].get_value(0, 1), Value::F64(100.0));
        assert_eq!(batches[1].get_value(1, 0), Value::I64(5));
    }

    #[test]
    fn batch_scan_with_filter() {
        let dir = tempdir().unwrap();
        let part = dir.path();

        write_i64_column(part, "id", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        write_f64_column(
            part,
            "price",
            &[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
        );

        let columns = vec![
            ("id".into(), ColumnType::I64),
            ("price".into(), ColumnType::F64),
        ];

        let column_indices: HashMap<String, usize> = [("id".into(), 0), ("price".into(), 1)].into();

        let filter = crate::compiled_filter::compile_filter(
            &crate::plan::Filter::Gt("price".into(), Value::F64(50.0)),
            &column_indices,
        );

        let batches = batch_scan_and_filter(part, &columns, Some(&filter), 100).unwrap();

        // Rows with price > 50: 60, 70, 80, 90, 100 = 5 rows
        let total_rows: usize = batches.iter().map(|b| b.row_count()).sum();
        assert_eq!(total_rows, 5);

        // First passing row should have id=6, price=60.0
        assert_eq!(batches[0].get_value(0, 0), Value::I64(6));
        assert_eq!(batches[0].get_value(0, 1), Value::F64(60.0));
    }

    #[test]
    fn batch_scan_empty_partition() {
        let dir = tempdir().unwrap();
        let part = dir.path();

        // No files on disk.
        let columns = vec![("id".into(), ColumnType::I64)];
        let batches = batch_scan_and_filter(part, &columns, None, 100).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_scan_all_filtered_out() {
        let dir = tempdir().unwrap();
        let part = dir.path();

        write_f64_column(part, "price", &[1.0, 2.0, 3.0]);

        let columns = vec![("price".into(), ColumnType::F64)];
        let column_indices: HashMap<String, usize> = [("price".into(), 0)].into();

        let filter = crate::compiled_filter::compile_filter(
            &crate::plan::Filter::Gt("price".into(), Value::F64(1000.0)),
            &column_indices,
        );

        let batches = batch_scan_and_filter(part, &columns, Some(&filter), 100).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_scan_default_batch_size() {
        let dir = tempdir().unwrap();
        let part = dir.path();

        // Just a small column; verify that batch_size=0 uses the default.
        write_i64_column(part, "x", &[1, 2, 3]);
        let columns = vec![("x".into(), ColumnType::I64)];
        let batches = batch_scan_and_filter(part, &columns, None, 0).unwrap();
        // All 3 rows fit in one default-size batch.
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_count(), 3);
    }
}
