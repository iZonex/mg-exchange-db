//! Columnar (column-at-a-time) execution for aggregate queries.
//!
//! Instead of materialising rows and iterating them one-by-one, this module
//! memory-maps the column file directly, reinterprets the bytes as a typed
//! slice, and invokes the SIMD-accelerated routines from `exchange_core::simd`.
//!
//! This path is used when the query is a simple aggregate without a WHERE
//! clause: `SELECT count(*), sum(x), avg(x), min(x), max(x) FROM table`.

use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::ColumnType;
use exchange_core::mmap::{self, MmapReadOnly};
use exchange_core::simd;
use std::path::Path;

use crate::plan::{AggregateKind, Value};

/// Execute a single aggregate function directly on a column file.
///
/// The column file is memory-mapped read-only, cast to a typed slice,
/// and processed via SIMD helpers. For `COUNT(*)` the element count is
/// derived from the file size without reading any data.
pub fn columnar_aggregate(
    column_path: &Path,
    col_type: ColumnType,
    agg_kind: AggregateKind,
) -> Result<Value> {
    let element_size = col_type.fixed_size().ok_or_else(|| {
        ExchangeDbError::Corruption(format!(
            "columnar_aggregate requires a fixed-width column type, got {:?}",
            col_type
        ))
    })?;

    // For COUNT(*), we only need the file size.
    if matches!(agg_kind, AggregateKind::Count) {
        let file_len = std::fs::metadata(column_path)
            .map(|m| m.len())
            .unwrap_or(0);
        let count = file_len / element_size as u64;
        return Ok(Value::I64(count as i64));
    }

    let ro = MmapReadOnly::open(column_path)?;
    let data = ro.as_slice();

    // Advise sequential access for the scan.
    mmap::advise_sequential(ro.inner_mmap());

    let result = match col_type {
        ColumnType::F64 => {
            let values = bytes_as_f64(data);
            aggregate_f64(values, agg_kind)
        }
        ColumnType::I64 | ColumnType::Timestamp => {
            let values = bytes_as_i64(data);
            aggregate_i64(values, agg_kind)
        }
        ColumnType::F32 => {
            // Promote f32 to f64 for aggregation.
            let f32_values = bytes_as_f32(data);
            let promoted: Vec<f64> = f32_values.iter().map(|&v| v as f64).collect();
            aggregate_f64(&promoted, agg_kind)
        }
        ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
            // Promote i32 to i64.
            let i32_values = bytes_as_i32(data);
            let promoted: Vec<i64> = i32_values.iter().map(|&v| v as i64).collect();
            aggregate_i64(&promoted, agg_kind)
        }
        _ => Err(ExchangeDbError::Corruption(format!(
            "columnar_aggregate not supported for {:?}",
            col_type
        ))),
    };

    // Advise that we're done with this region.
    mmap::advise_dontneed(ro.inner_mmap());

    result
}

/// Aggregate over multiple partition column files, combining results.
///
/// When `table_dir` is provided, `TieredPartitionReader` is used so that
/// warm (LZ4-compressed) and cold (XPQT) partitions are transparently
/// decompressed/converted before aggregation.
pub fn columnar_aggregate_partitions(
    partition_dirs: &[std::path::PathBuf],
    col_name: &str,
    col_type: ColumnType,
    agg_kind: AggregateKind,
) -> Result<Value> {
    columnar_aggregate_partitions_tiered(partition_dirs, col_name, col_type, agg_kind, None)
}

/// Tiered-aware version of [`columnar_aggregate_partitions`].
pub fn columnar_aggregate_partitions_tiered(
    partition_dirs: &[std::path::PathBuf],
    col_name: &str,
    col_type: ColumnType,
    agg_kind: AggregateKind,
    table_dir: Option<&std::path::Path>,
) -> Result<Value> {
    use exchange_core::tiered::TieredPartitionReader;

    let mut combined_count: i64 = 0;
    let mut combined_sum_f64: f64 = 0.0;
    let mut combined_sum_i64: i64 = 0;
    let mut combined_min_f64 = f64::INFINITY;
    let mut combined_max_f64 = f64::NEG_INFINITY;
    let mut combined_min_i64 = i64::MAX;
    let mut combined_max_i64 = i64::MIN;
    let mut has_float = matches!(col_type, ColumnType::F64 | ColumnType::F32);
    let mut any_data = false;

    // Hold tiered readers alive so their temp dirs persist for the duration
    // of the aggregation loop.
    let mut _tiered_readers: Vec<TieredPartitionReader> = Vec::new();

    for partition_dir in partition_dirs {
        // Resolve the native path using TieredPartitionReader when possible.
        let native_path_buf;
        let native_path: &std::path::Path = if let Some(td) = table_dir {
            if let Ok(reader) = TieredPartitionReader::open(partition_dir, td) {
                native_path_buf = reader.native_path().to_path_buf();
                _tiered_readers.push(reader);
                &native_path_buf
            } else {
                partition_dir.as_path()
            }
        } else {
            partition_dir.as_path()
        };

        let col_path = native_path.join(format!("{col_name}.d"));
        if !col_path.exists() {
            continue;
        }

        match agg_kind {
            AggregateKind::Count => {
                if let Ok(Value::I64(c)) =
                    columnar_aggregate(&col_path, col_type, AggregateKind::Count)
                {
                    combined_count += c;
                    any_data = true;
                }
            }
            AggregateKind::Sum | AggregateKind::Avg => {
                // For AVG we need both sum and count across partitions.
                if let Ok(Value::I64(c)) =
                    columnar_aggregate(&col_path, col_type, AggregateKind::Count)
                {
                    combined_count += c;
                }
                match columnar_aggregate(&col_path, col_type, AggregateKind::Sum) {
                    Ok(Value::F64(s)) => {
                        combined_sum_f64 += s;
                        has_float = true;
                        any_data = true;
                    }
                    Ok(Value::I64(s)) => {
                        combined_sum_i64 += s;
                        any_data = true;
                    }
                    _ => {}
                }
            }
            AggregateKind::Min => match columnar_aggregate(&col_path, col_type, AggregateKind::Min)
            {
                Ok(Value::F64(v)) => {
                    if v < combined_min_f64 {
                        combined_min_f64 = v;
                    }
                    any_data = true;
                }
                Ok(Value::I64(v)) => {
                    if v < combined_min_i64 {
                        combined_min_i64 = v;
                    }
                    any_data = true;
                }
                _ => {}
            },
            AggregateKind::Max => match columnar_aggregate(&col_path, col_type, AggregateKind::Max)
            {
                Ok(Value::F64(v)) => {
                    if v > combined_max_f64 {
                        combined_max_f64 = v;
                    }
                    any_data = true;
                }
                Ok(Value::I64(v)) => {
                    if v > combined_max_i64 {
                        combined_max_i64 = v;
                    }
                    any_data = true;
                }
                _ => {}
            },
            _ => {
                // For non-simple aggregates (first, last, stddev, etc.),
                // fall back — the caller should use the row-based path.
                return Err(ExchangeDbError::Corruption(format!(
                    "columnar path does not support {:?}",
                    agg_kind
                )));
            }
        }
    }

    if !any_data {
        return match agg_kind {
            AggregateKind::Count => Ok(Value::I64(0)),
            _ => Ok(Value::Null),
        };
    }

    match agg_kind {
        AggregateKind::Count => Ok(Value::I64(combined_count)),
        AggregateKind::Sum => {
            if has_float {
                Ok(Value::F64(combined_sum_f64 + combined_sum_i64 as f64))
            } else {
                Ok(Value::I64(combined_sum_i64))
            }
        }
        AggregateKind::Avg => {
            if combined_count == 0 {
                Ok(Value::Null)
            } else {
                let total = if has_float {
                    combined_sum_f64 + combined_sum_i64 as f64
                } else {
                    combined_sum_i64 as f64
                };
                Ok(Value::F64(total / combined_count as f64))
            }
        }
        AggregateKind::Min => {
            if has_float {
                Ok(Value::F64(combined_min_f64))
            } else {
                Ok(Value::I64(combined_min_i64))
            }
        }
        AggregateKind::Max => {
            if has_float {
                Ok(Value::F64(combined_max_f64))
            } else {
                Ok(Value::I64(combined_max_i64))
            }
        }
        _ => Ok(Value::Null),
    }
}

/// Check whether a SELECT query can use the columnar fast path.
///
/// Returns `true` if:
/// - All select columns are simple aggregates (sum/avg/min/max/count)
/// - There is no WHERE clause, GROUP BY, SAMPLE BY, etc.
pub fn can_use_columnar_path(
    columns: &[crate::plan::SelectColumn],
    has_filter: bool,
    has_group_by: bool,
    has_sample_by: bool,
) -> bool {
    if has_filter || has_group_by || has_sample_by {
        return false;
    }

    columns.iter().all(|c| match c {
        crate::plan::SelectColumn::Aggregate { function, .. } => matches!(
            function,
            AggregateKind::Sum
                | AggregateKind::Avg
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::Count
        ),
        _ => false,
    })
}

// ── filter scan ────────────────────────────────────────────────────────────

/// Memory-map a column, apply a SIMD-accelerated filter, and return matching
/// row indices.
///
/// Supports the common comparison filters on numeric columns. For
/// unsupported filter types or non-numeric columns, falls back to returning
/// all indices (no filtering).
pub fn columnar_filter_scan(
    column_path: &Path,
    col_type: ColumnType,
    filter: &crate::plan::Filter,
) -> Result<Vec<u32>> {
    let ro = MmapReadOnly::open(column_path)?;
    let data = ro.as_slice();
    mmap::advise_sequential(ro.inner_mmap());

    let result = match col_type {
        ColumnType::F64 => {
            let values = bytes_as_f64(data);
            filter_scan_f64(values, filter)
        }
        ColumnType::I64 | ColumnType::Timestamp => {
            let values = bytes_as_i64(data);
            filter_scan_i64(values, filter)
        }
        ColumnType::F32 => {
            let f32_values = bytes_as_f32(data);
            let promoted: Vec<f64> = f32_values.iter().map(|&v| v as f64).collect();
            filter_scan_f64(&promoted, filter)
        }
        ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
            let i32_values = bytes_as_i32(data);
            let promoted: Vec<i64> = i32_values.iter().map(|&v| v as i64).collect();
            filter_scan_i64(&promoted, filter)
        }
        _ => {
            // Unsupported type: return all indices.
            let element_size = col_type.fixed_size().unwrap_or(8);
            let count = data.len() / element_size;
            Ok((0..count as u32).collect())
        }
    };

    mmap::advise_dontneed(ro.inner_mmap());
    result
}

fn filter_scan_f64(values: &[f64], filter: &crate::plan::Filter) -> Result<Vec<u32>> {
    use crate::plan::Filter;
    match filter {
        Filter::Gt(_, Value::F64(threshold)) => Ok(simd::filter_gt_f64(values, *threshold)),
        Filter::Gt(_, Value::I64(threshold)) => Ok(simd::filter_gt_f64(values, *threshold as f64)),
        Filter::Gte(_, Value::F64(threshold)) => {
            // gte: use gt on (threshold - epsilon) to include equality.
            // More precise: iterate and check.
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v >= *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Gte(_, Value::I64(threshold)) => {
            let t = *threshold as f64;
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v >= t {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Lt(_, Value::F64(threshold)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v < *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Lt(_, Value::I64(threshold)) => {
            let t = *threshold as f64;
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v < t {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Lte(_, Value::F64(threshold)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v <= *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Eq(_, Value::F64(expected)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v == *expected {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        _ => {
            // Unsupported filter: return all indices.
            Ok((0..values.len() as u32).collect())
        }
    }
}

fn filter_scan_i64(values: &[i64], filter: &crate::plan::Filter) -> Result<Vec<u32>> {
    use crate::plan::Filter;
    match filter {
        Filter::Eq(_, Value::I64(expected)) => Ok(simd::filter_eq_i64(values, *expected)),
        Filter::Gt(_, Value::I64(threshold)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v > *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Gte(_, Value::I64(threshold)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v >= *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Lt(_, Value::I64(threshold)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v < *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Lte(_, Value::I64(threshold)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v <= *threshold {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        Filter::Between(_, Value::I64(low), Value::I64(high)) => {
            let mut result = Vec::new();
            for (i, &v) in values.iter().enumerate() {
                if v >= *low && v <= *high {
                    result.push(i as u32);
                }
            }
            Ok(result)
        }
        _ => {
            Ok((0..values.len() as u32).collect())
        }
    }
}

// ── columnar project ───────────────────────────────────────────────────────

/// Given a set of row indices, read only the requested columns and return
/// the projected values.
///
/// Each column is memory-mapped independently. Only the rows at the given
/// indices are materialised, which is much faster than scanning the full
/// column when the selectivity is low.
pub fn columnar_project(
    partition_dir: &Path,
    meta: &exchange_core::table::TableMeta,
    row_indices: &[u32],
    column_names: &[String],
) -> Result<Vec<Vec<Value>>> {
    use exchange_core::column::{FixedColumnReader, VarColumnReader};

    if row_indices.is_empty() || column_names.is_empty() {
        return Ok(Vec::new());
    }

    // Open readers for all requested columns.
    let mut readers: Vec<ColumnProjectReader> = Vec::with_capacity(column_names.len());

    for col_name in column_names {
        let col_def = meta
            .columns
            .iter()
            .find(|c| c.name == *col_name)
            .ok_or_else(|| {
                ExchangeDbError::Corruption(format!(
                    "columnar_project: column not found: {col_name}"
                ))
            })?;

        let col_type: ColumnType = col_def.col_type.into();

        if col_type.is_variable_length() {
            let data_path = partition_dir.join(format!("{col_name}.d"));
            let index_path = partition_dir.join(format!("{col_name}.i"));
            if data_path.exists() && index_path.exists() {
                let reader = VarColumnReader::open(&data_path, &index_path)?;
                readers.push(ColumnProjectReader::Var(reader, col_type));
            } else {
                readers.push(ColumnProjectReader::Missing);
            }
        } else {
            let data_path = partition_dir.join(format!("{col_name}.d"));
            if data_path.exists() {
                let reader = FixedColumnReader::open(&data_path, col_type)?;
                readers.push(ColumnProjectReader::Fixed(reader, col_type));
            } else {
                readers.push(ColumnProjectReader::Missing);
            }
        }
    }

    // Read each row at the specified indices.
    let mut rows = Vec::with_capacity(row_indices.len());
    for &idx in row_indices {
        let mut row = Vec::with_capacity(readers.len());
        for reader in &readers {
            row.push(reader.read_value(idx as u64));
        }
        rows.push(row);
    }

    Ok(rows)
}

#[allow(dead_code)]
enum ColumnProjectReader {
    Fixed(exchange_core::column::FixedColumnReader, ColumnType),
    Var(exchange_core::column::VarColumnReader, ColumnType),
    Missing,
}

impl ColumnProjectReader {
    fn read_value(&self, row: u64) -> Value {
        match self {
            ColumnProjectReader::Fixed(r, ct) => match ct {
                ColumnType::I64 => Value::I64(r.read_i64(row)),
                ColumnType::F64 => {
                    let v = r.read_f64(row);
                    if v.is_nan() { Value::Null } else { Value::F64(v) }
                }
                ColumnType::I32 | ColumnType::Symbol => Value::I64(r.read_i32(row) as i64),
                ColumnType::Timestamp => Value::Timestamp(r.read_i64(row)),
                ColumnType::F32 => {
                    Value::F64(f32::from_le_bytes(r.read_raw(row).try_into().unwrap()) as f64)
                }
                _ => Value::Null,
            },
            ColumnProjectReader::Var(r, _) => {
                let s = r.read_str(row);
                if s == "\0" { Value::Null } else { Value::Str(s.to_string()) }
            }
            ColumnProjectReader::Missing => Value::Null,
        }
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

fn bytes_as_f64(data: &[u8]) -> &[f64] {
    let count = data.len() / 8;
    // SAFETY: f64 has alignment 8, but mmap data may not be aligned.
    // Since we read from column files that we wrote as f64, the data is
    // 8-byte aligned at the start of the mmap region.
    //
    // On platforms where unaligned access is fine (x86-64, aarch64),
    // this is safe. For strict-alignment platforms we would need to copy.
    assert!(data.as_ptr() as usize % std::mem::align_of::<f64>() == 0 || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f64, count) }
}

fn bytes_as_i64(data: &[u8]) -> &[i64] {
    let count = data.len() / 8;
    assert!(data.as_ptr() as usize % std::mem::align_of::<i64>() == 0 || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const i64, count) }
}

fn bytes_as_f32(data: &[u8]) -> &[f32] {
    let count = data.len() / 4;
    assert!(data.as_ptr() as usize % std::mem::align_of::<f32>() == 0 || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f32, count) }
}

fn bytes_as_i32(data: &[u8]) -> &[i32] {
    let count = data.len() / 4;
    assert!(data.as_ptr() as usize % std::mem::align_of::<i32>() == 0 || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const i32, count) }
}

fn aggregate_f64(values: &[f64], agg_kind: AggregateKind) -> Result<Value> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    match agg_kind {
        AggregateKind::Sum => Ok(Value::F64(simd::sum_f64(values))),
        AggregateKind::Min => Ok(Value::F64(simd::min_f64(values))),
        AggregateKind::Max => Ok(Value::F64(simd::max_f64(values))),
        AggregateKind::Count => Ok(Value::I64(simd::count_non_null_f64(values) as i64)),
        AggregateKind::Avg => {
            let count = simd::count_non_null_f64(values);
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::F64(simd::sum_f64(values) / count as f64))
            }
        }
        _ => Err(ExchangeDbError::Corruption(format!(
            "aggregate_f64 does not support {:?}",
            agg_kind
        ))),
    }
}

fn aggregate_i64(values: &[i64], agg_kind: AggregateKind) -> Result<Value> {
    if values.is_empty() {
        return Ok(Value::Null);
    }
    match agg_kind {
        AggregateKind::Sum => Ok(Value::I64(simd::sum_i64(values))),
        AggregateKind::Min => Ok(Value::I64(simd::min_i64(values))),
        AggregateKind::Max => Ok(Value::I64(simd::max_i64(values))),
        AggregateKind::Count => Ok(Value::I64(values.len() as i64)),
        AggregateKind::Avg => {
            let count = values.len();
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::F64(simd::sum_i64(values) as f64 / count as f64))
            }
        }
        _ => Err(ExchangeDbError::Corruption(format!(
            "aggregate_i64 does not support {:?}",
            agg_kind
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_core::mmap::MmapFile;
    use tempfile::tempdir;

    fn write_f64_column(path: &Path, values: &[f64]) {
        let mut mf = MmapFile::open(path, 4096).unwrap();
        for &v in values {
            mf.append(&v.to_le_bytes()).unwrap();
        }
        mf.flush().unwrap();
    }

    fn write_i64_column(path: &Path, values: &[i64]) {
        let mut mf = MmapFile::open(path, 4096).unwrap();
        for &v in values {
            mf.append(&v.to_le_bytes()).unwrap();
        }
        mf.flush().unwrap();
    }

    #[test]
    fn columnar_sum_f64() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        write_f64_column(&path, &[100.0, 200.0, 300.0]);

        let result = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Sum).unwrap();
        assert_eq!(result, Value::F64(600.0));
    }

    #[test]
    fn columnar_min_max_f64() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        write_f64_column(&path, &[50.0, 10.0, 90.0, 30.0]);

        let min = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Min).unwrap();
        let max = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Max).unwrap();
        assert_eq!(min, Value::F64(10.0));
        assert_eq!(max, Value::F64(90.0));
    }

    #[test]
    fn columnar_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        write_i64_column(&path, &[1, 2, 3, 4, 5]);

        let result = columnar_aggregate(&path, ColumnType::I64, AggregateKind::Count).unwrap();
        assert_eq!(result, Value::I64(5));
    }

    #[test]
    fn columnar_avg_f64() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        write_f64_column(&path, &[10.0, 20.0, 30.0]);

        let result = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Avg).unwrap();
        assert_eq!(result, Value::F64(20.0));
    }

    #[test]
    fn columnar_sum_i64() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("qty.d");
        write_i64_column(&path, &[10, 20, 30]);

        let result = columnar_aggregate(&path, ColumnType::I64, AggregateKind::Sum).unwrap();
        assert_eq!(result, Value::I64(60));
    }

    #[test]
    fn columnar_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.d");
        write_f64_column(&path, &[]);

        let result = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Sum).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn columnar_aggregate_matches_row_based() {
        // Verify that columnar aggregate produces the same result as
        // iterating row-by-row with the standard Sum aggregate function.
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.d");

        let values: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.1).collect();
        write_f64_column(&path, &values);

        // Columnar result.
        let col_sum = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Sum).unwrap();
        let col_min = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Min).unwrap();
        let col_max = columnar_aggregate(&path, ColumnType::F64, AggregateKind::Max).unwrap();

        // Scalar reference.
        let scalar_sum: f64 = values.iter().sum();
        let scalar_min: f64 = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let scalar_max: f64 = values
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);

        match col_sum {
            Value::F64(v) => assert!((v - scalar_sum).abs() < 1e-6),
            other => panic!("expected F64, got {:?}", other),
        }
        assert_eq!(col_min, Value::F64(scalar_min));
        assert_eq!(col_max, Value::F64(scalar_max));
    }

    #[test]
    fn can_use_columnar_path_simple_aggs() {
        use crate::plan::SelectColumn;

        let cols = vec![
            SelectColumn::Aggregate {
                function: AggregateKind::Count,
                column: "*".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
            SelectColumn::Aggregate {
                function: AggregateKind::Sum,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
            SelectColumn::Aggregate {
                function: AggregateKind::Avg,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];

        assert!(can_use_columnar_path(&cols, false, false, false));
        // With a filter — not eligible.
        assert!(!can_use_columnar_path(&cols, true, false, false));
        // With GROUP BY — not eligible.
        assert!(!can_use_columnar_path(&cols, false, true, false));
    }

    #[test]
    fn can_use_columnar_path_with_non_simple_agg() {
        use crate::plan::SelectColumn;

        let cols = vec![SelectColumn::Aggregate {
            function: AggregateKind::StdDev,
            column: "price".into(),
            alias: None,
            filter: None,
            within_group_order: None,
            arg_expr: None,
        }];
        // StdDev is not in the simple set.
        assert!(!can_use_columnar_path(&cols, false, false, false));
    }

    #[test]
    fn can_use_columnar_path_with_plain_column() {
        use crate::plan::SelectColumn;

        let cols = vec![
            SelectColumn::Name("price".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Sum,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        // A bare column name alongside aggregates — not eligible.
        assert!(!can_use_columnar_path(&cols, false, false, false));
    }

    #[test]
    fn columnar_aggregate_partitions_sum() {
        let dir = tempdir().unwrap();

        // Create two "partition" directories.
        let p1 = dir.path().join("2024-01-01");
        let p2 = dir.path().join("2024-01-02");
        std::fs::create_dir_all(&p1).unwrap();
        std::fs::create_dir_all(&p2).unwrap();

        write_f64_column(&p1.join("price.d"), &[10.0, 20.0]);
        write_f64_column(&p2.join("price.d"), &[30.0, 40.0]);

        let result = columnar_aggregate_partitions(
            &[p1, p2],
            "price",
            ColumnType::F64,
            AggregateKind::Sum,
        )
        .unwrap();

        assert_eq!(result, Value::F64(100.0));
    }

    #[test]
    fn columnar_aggregate_partitions_count() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("p1");
        let p2 = dir.path().join("p2");
        std::fs::create_dir_all(&p1).unwrap();
        std::fs::create_dir_all(&p2).unwrap();

        write_i64_column(&p1.join("ts.d"), &[1, 2, 3]);
        write_i64_column(&p2.join("ts.d"), &[4, 5]);

        let result = columnar_aggregate_partitions(
            &[p1, p2],
            "ts",
            ColumnType::I64,
            AggregateKind::Count,
        )
        .unwrap();

        assert_eq!(result, Value::I64(5));
    }

    #[test]
    fn columnar_aggregate_partitions_avg() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("p1");
        let p2 = dir.path().join("p2");
        std::fs::create_dir_all(&p1).unwrap();
        std::fs::create_dir_all(&p2).unwrap();

        write_f64_column(&p1.join("price.d"), &[10.0, 20.0]);
        write_f64_column(&p2.join("price.d"), &[30.0, 40.0]);

        let result = columnar_aggregate_partitions(
            &[p1, p2],
            "price",
            ColumnType::F64,
            AggregateKind::Avg,
        )
        .unwrap();

        assert_eq!(result, Value::F64(25.0));
    }

    // ── columnar_filter_scan tests ────────────────────────────────────

    #[test]
    fn filter_scan_f64_gt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        write_f64_column(&path, &[1.0, 5.0, 3.0, 7.0, 2.0, 8.0]);

        let filter = crate::plan::Filter::Gt("price".into(), Value::F64(4.0));
        let indices = columnar_filter_scan(&path, ColumnType::F64, &filter).unwrap();
        assert_eq!(indices, vec![1, 3, 5]);
    }

    #[test]
    fn filter_scan_f64_gte() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");
        write_f64_column(&path, &[1.0, 5.0, 3.0, 7.0]);

        let filter = crate::plan::Filter::Gte("price".into(), Value::F64(5.0));
        let indices = columnar_filter_scan(&path, ColumnType::F64, &filter).unwrap();
        assert_eq!(indices, vec![1, 3]);
    }

    #[test]
    fn filter_scan_i64_eq() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("id.d");
        write_i64_column(&path, &[1, 2, 3, 2, 4, 2]);

        let filter = crate::plan::Filter::Eq("id".into(), Value::I64(2));
        let indices = columnar_filter_scan(&path, ColumnType::I64, &filter).unwrap();
        assert_eq!(indices, vec![1, 3, 5]);
    }

    #[test]
    fn filter_scan_i64_between() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("id.d");
        write_i64_column(&path, &[1, 5, 3, 7, 2, 8]);

        let filter = crate::plan::Filter::Between("id".into(), Value::I64(3), Value::I64(7));
        let indices = columnar_filter_scan(&path, ColumnType::I64, &filter).unwrap();
        assert_eq!(indices, vec![1, 2, 3]);
    }

    // ── columnar_project tests ────────────────────────────────────────

    #[test]
    fn project_selected_rows() {
        use exchange_core::table::{ColumnDef, ColumnTypeSerializable, TableMeta};

        let dir = tempdir().unwrap();
        let part_dir = dir.path().join("p1");
        std::fs::create_dir_all(&part_dir).unwrap();

        write_f64_column(&part_dir.join("price.d"), &[10.0, 20.0, 30.0, 40.0, 50.0]);
        write_i64_column(&part_dir.join("volume.d"), &[100, 200, 300, 400, 500]);

        let meta = TableMeta {
            name: "test".to_string(),
            columns: vec![
                ColumnDef {
                    name: "price".to_string(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
                ColumnDef {
                    name: "volume".to_string(),
                    col_type: ColumnTypeSerializable::I64,
                    indexed: false,
                },
            ],
            timestamp_column: 0,
            partition_by: exchange_common::types::PartitionBy::None.into(),
            version: 0,
        };

        let indices = vec![1, 3];
        let col_names = vec!["price".to_string(), "volume".to_string()];
        let rows = columnar_project(&part_dir, &meta, &indices, &col_names).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Value::F64(20.0), Value::I64(200)]);
        assert_eq!(rows[1], vec![Value::F64(40.0), Value::I64(400)]);
    }

    #[test]
    fn project_empty_indices() {
        use exchange_core::table::{ColumnDef, ColumnTypeSerializable, TableMeta};

        let dir = tempdir().unwrap();
        let part_dir = dir.path().join("p1");
        std::fs::create_dir_all(&part_dir).unwrap();
        write_f64_column(&part_dir.join("price.d"), &[10.0, 20.0]);

        let meta = TableMeta {
            name: "test".to_string(),
            columns: vec![ColumnDef {
                name: "price".to_string(),
                col_type: ColumnTypeSerializable::F64,
                indexed: false,
            }],
            timestamp_column: 0,
            partition_by: exchange_common::types::PartitionBy::None.into(),
            version: 0,
        };

        let rows = columnar_project(&part_dir, &meta, &[], &["price".to_string()]).unwrap();
        assert!(rows.is_empty());
    }
}
