//! Parallel partition scanning for SELECT queries.
//!
//! Uses `rayon` to scan multiple partitions concurrently, then merges
//! results in partition order.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use exchange_core::column::{FixedColumnReader, VarColumnReader};
use exchange_core::table::TableMeta;
use exchange_core::tiered::TieredPartitionReader;
use std::path::Path;

use crate::plan::{CompareOp, Filter, PlanExpr, Value};

/// The minimum number of partitions that triggers parallel scanning.
/// With fewer partitions the overhead of thread-pool dispatch is not
/// worthwhile, so we fall back to sequential iteration.
const PARALLEL_THRESHOLD: usize = 2;

/// Column reader -- mirrors the private enum in `executor.rs`.
#[allow(dead_code)]
enum ColReader {
    Fixed(FixedColumnReader, ColumnType),
    Var(VarColumnReader, ColumnType),
}

/// Read a single cell from a column reader.
fn read_value(reader: &ColReader, row: u64) -> Value {
    match reader {
        ColReader::Fixed(r, ct) => match ct {
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
            ColumnType::I16 => {
                Value::I64(i16::from_le_bytes(r.read_raw(row).try_into().unwrap()) as i64)
            }
            ColumnType::I8 => Value::I64(r.read_raw(row)[0] as i8 as i64),
            ColumnType::Boolean => Value::I64(if r.read_raw(row)[0] != 0 { 1 } else { 0 }),
            ColumnType::Uuid => {
                let bytes = r.read_raw(row);
                Value::Str(hex_encode(bytes))
            }
            _ => Value::Null,
        },
        ColReader::Var(r, _) => {
            let s = r.read_str(row);
            if s == "\0" { Value::Null } else { Value::Str(s.to_string()) }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ── filter helpers (duplicated from executor because they are private) ──

fn collect_filter_columns(filter: &Filter, meta: &TableMeta) -> Vec<usize> {
    let mut indices = Vec::new();
    match filter {
        Filter::Eq(col, _)
        | Filter::NotEq(col, _)
        | Filter::Gt(col, _)
        | Filter::Lt(col, _)
        | Filter::Gte(col, _)
        | Filter::Lte(col, _)
        | Filter::IsNull(col)
        | Filter::IsNotNull(col)
        | Filter::Like(col, _)
        | Filter::NotLike(col, _)
        | Filter::ILike(col, _) => {
            if let Some(idx) = meta.columns.iter().position(|c| c.name == *col) {
                indices.push(idx);
            }
        }
        Filter::Between(col, _, _)
        | Filter::BetweenSymmetric(col, _, _)
        | Filter::In(col, _)
        | Filter::NotIn(col, _) => {
            if let Some(idx) = meta.columns.iter().position(|c| c.name == *col) {
                indices.push(idx);
            }
        }
        Filter::And(parts) | Filter::Or(parts) => {
            for p in parts {
                indices.extend(collect_filter_columns(p, meta));
            }
        }
        Filter::Subquery { column, .. } | Filter::InSubquery { column, .. } => {
            if let Some(idx) = meta.columns.iter().position(|c| c.name == *column) {
                indices.push(idx);
            }
        }
        Filter::Exists { .. } => {}
        Filter::Not(inner) => {
            indices.extend(collect_filter_columns(inner, meta));
        }
        Filter::Expression { left, right, .. } => {
            let mut cols = Vec::new();
            left.collect_columns(&mut cols);
            right.collect_columns(&mut cols);
            for col_name in &cols {
                if let Some(idx) = meta.columns.iter().position(|c| c.name == *col_name) {
                    indices.push(idx);
                }
            }
        }
        Filter::All { column, .. } | Filter::Any { column, .. } => {
            if let Some(idx) = meta.columns.iter().position(|c| c.name == *column) {
                indices.push(idx);
            }
        }
    }
    indices
}

pub fn evaluate_filter(filter: &Filter, values: &[(usize, Value)], meta: &TableMeta) -> bool {
    match filter {
        Filter::Eq(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| v.eq_coerce(expected)).unwrap_or(false)
        }
        Filter::NotEq(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| !v.eq_coerce(expected)).unwrap_or(true)
        }
        Filter::Gt(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater)).unwrap_or(false)
        }
        Filter::Lt(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less)).unwrap_or(false)
        }
        Filter::Gte(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| matches!(v.cmp_coerce(expected), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))).unwrap_or(false)
        }
        Filter::Lte(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| matches!(v.cmp_coerce(expected), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))).unwrap_or(false)
        }
        Filter::Between(col, low, high)
        | Filter::BetweenSymmetric(col, low, high) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| matches!(v.cmp_coerce(low), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                      && matches!(v.cmp_coerce(high), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)))
                .unwrap_or(false)
        }
        Filter::And(parts) => parts.iter().all(|p| evaluate_filter(p, values, meta)),
        Filter::Or(parts) => parts.iter().any(|p| evaluate_filter(p, values, meta)),
        Filter::IsNull(col) => {
            let val = get_filter_value(col, values, meta);
            matches!(val.as_ref(), None | Some(Value::Null))
        }
        Filter::IsNotNull(col) => {
            let val = get_filter_value(col, values, meta);
            matches!(val.as_ref(), Some(v) if *v != Value::Null)
        }
        Filter::In(col, list) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| list.iter().any(|item| v.eq_coerce(item))).unwrap_or(false)
        }
        Filter::NotIn(col, list) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| !list.iter().any(|item| v.eq_coerce(item))).unwrap_or(true)
        }
        Filter::Like(col, pattern) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| {
                if let Value::Str(s) = v {
                    crate::executor::like_match(s, pattern, false)
                } else {
                    false
                }
            }).unwrap_or(false)
        }
        Filter::NotLike(col, pattern) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| {
                if let Value::Str(s) = v {
                    !crate::executor::like_match(s, pattern, false)
                } else {
                    true
                }
            }).unwrap_or(true)
        }
        Filter::ILike(col, pattern) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref().map(|v| {
                if let Value::Str(s) = v {
                    crate::executor::like_match(s, pattern, true)
                } else {
                    false
                }
            }).unwrap_or(false)
        }
        Filter::Not(inner) => !evaluate_filter(inner, values, meta),
        Filter::Subquery { .. } | Filter::InSubquery { .. } | Filter::Exists { .. }
        | Filter::All { .. } | Filter::Any { .. } => {
            false
        }
        Filter::Expression { left, op, right } => {
            let lv = eval_plan_expr_parallel(left, values, meta);
            let rv = eval_plan_expr_parallel(right, values, meta);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(lv.cmp_coerce(&rv), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
                CompareOp::Lte => matches!(lv.cmp_coerce(&rv), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
            }
        }
    }
}

fn eval_plan_expr_parallel(expr: &PlanExpr, values: &[(usize, Value)], meta: &TableMeta) -> Value {
    match expr {
        PlanExpr::Column(name) => { let idx = meta.columns.iter().position(|c| c.name == *name); if let Some(idx) = idx { values.iter().find(|(i, _)| *i == idx).map(|(_, v)| v.clone()).unwrap_or(Value::Null) } else { Value::Null } },
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => crate::executor::apply_binary_op(&eval_plan_expr_parallel(left, values, meta), *op, &eval_plan_expr_parallel(right, values, meta)),
        PlanExpr::UnaryOp { op, expr } => crate::executor::apply_unary_op(*op, &eval_plan_expr_parallel(expr, values, meta)),
        PlanExpr::Function { name, args } => {
            let func_args: Vec<Value> = args.iter().map(|a| eval_plan_expr_parallel(a, values, meta)).collect();
            crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null)
        }
    }
}

/// Public wrapper for `eval_plan_expr_parallel` used by vector_groupby.
pub fn eval_plan_expr_parallel_pub(expr: &PlanExpr, values: &[(usize, Value)], meta: &TableMeta) -> Value {
    eval_plan_expr_parallel(expr, values, meta)
}

#[inline]
fn get_filter_value(col: &str, values: &[(usize, Value)], meta: &TableMeta) -> Option<Value> {
    let idx = meta.columns.iter().position(|c| c.name == col)?;
    // Values are ordered by column index (from `all_indices` which is sorted),
    // so binary search is possible, but the list is typically small (< 10 cols)
    // so a linear scan with early exit is fast enough.
    values
        .iter()
        .find(|(i, _)| *i == idx)
        .map(|(_, v)| v.clone())
}

// ── core scanning logic ─────────────────────────────────────────────

/// Scan a single partition directory and return matching rows.
///
/// Uses `TieredPartitionReader` to transparently handle hot, warm, and
/// cold partitions. The `table_dir` parameter is needed so the reader can
/// locate table metadata and cold storage files.
fn scan_partition(
    partition_path: &Path,
    meta: &TableMeta,
    all_indices: &[usize],
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    table_dir: Option<&Path>,
) -> Result<Vec<Vec<Value>>> {
    scan_partition_limited(partition_path, meta, all_indices, selected_cols, filter, table_dir, None)
}

fn scan_partition_limited(
    partition_path: &Path,
    meta: &TableMeta,
    all_indices: &[usize],
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    table_dir: Option<&Path>,
    row_limit: Option<usize>,
) -> Result<Vec<Vec<Value>>> {
    // Use TieredPartitionReader to get the native path for reading.
    // If table_dir is provided we can do tiered reads; otherwise fall back
    // to reading partition_path directly (backwards-compatible).
    let tiered_reader = if let Some(td) = table_dir {
        TieredPartitionReader::open(partition_path, td).ok()
    } else {
        None
    };

    let native_path = tiered_reader
        .as_ref()
        .map(|r| r.native_path())
        .unwrap_or(partition_path);

    let mut readers: Vec<(usize, ColReader)> = Vec::new();

    for &col_idx in all_indices {
        let col_def = &meta.columns[col_idx];
        let col_type: ColumnType = col_def.col_type.into();

        if col_type.is_variable_length() {
            let data_path = native_path.join(format!("{}.d", col_def.name));
            let index_path = native_path.join(format!("{}.i", col_def.name));
            if data_path.exists() && index_path.exists() {
                let reader = VarColumnReader::open(&data_path, &index_path)?;
                readers.push((col_idx, ColReader::Var(reader, col_type)));
            }
        } else {
            let data_path = native_path.join(format!("{}.d", col_def.name));
            if data_path.exists() {
                let reader = FixedColumnReader::open(&data_path, col_type)?;
                readers.push((col_idx, ColReader::Fixed(reader, col_type)));
            }
        }
    }

    if readers.is_empty() {
        return Ok(Vec::new());
    }

    let row_count = match &readers[0].1 {
        ColReader::Fixed(r, _) => r.row_count(),
        ColReader::Var(r, _) => r.row_count(),
    };

    // Build a lookup table: col_idx -> position in `readers` vec.
    // This replaces the O(n) linear search per column per row with O(1) indexing.
    let max_col_idx = readers.iter().map(|(idx, _)| *idx).max().unwrap_or(0);
    let mut col_to_reader: Vec<Option<usize>> = vec![None; max_col_idx + 1];
    for (pos, (col_idx, _)) in readers.iter().enumerate() {
        col_to_reader[*col_idx] = Some(pos);
    }

    // Pre-compute selected column positions in reader array for fast result construction.
    let selected_reader_positions: Vec<Option<usize>> = selected_cols
        .iter()
        .map(|(idx, _)| col_to_reader.get(*idx).copied().flatten())
        .collect();

    let num_readers = readers.len();
    let num_selected = selected_cols.len();

    // Pre-allocate the all_values vec once, reuse per row.
    let mut all_values: Vec<(usize, Value)> = Vec::with_capacity(num_readers);
    let row_count_usize = row_count as usize;
    let scan_limit = row_limit.unwrap_or(row_count_usize);
    let mut rows = Vec::with_capacity(scan_limit.min(row_count_usize));

    for row_idx in 0..row_count {
        if rows.len() >= scan_limit {
            break;
        }
        all_values.clear();
        for (col_idx, reader) in &readers {
            let val = read_value(reader, row_idx);
            all_values.push((*col_idx, val));
        }

        if let Some(f) = filter
            && !evaluate_filter(f, &all_values, meta) {
                continue;
            }

        // Build result row using pre-computed positions (O(1) per column).
        let mut row = Vec::with_capacity(num_selected);
        for pos in &selected_reader_positions {
            match pos {
                Some(p) => row.push(all_values[*p].1.clone()),
                None => row.push(Value::Null),
            }
        }

        rows.push(row);
    }

    Ok(rows)
}

/// Returns `true` if the partition list should be scanned in parallel.
pub fn should_use_parallel(partition_count: usize) -> bool {
    partition_count >= PARALLEL_THRESHOLD
}

/// Scan partitions of a table in parallel using rayon and return all
/// matching rows merged in partition order.
///
/// This function is a drop-in replacement for the sequential
/// `scan_table` used by `executor.rs`.
pub fn parallel_scan_partitions(
    table_dir: &Path,
    meta: &TableMeta,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
) -> Result<Vec<Vec<Value>>> {
    // Discover partitions across all tiers using the core listing function
    // which includes hot/warm directories and cold XPQT files from _cold/.
    let partitions = exchange_core::table::list_partitions(table_dir)?;

    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    // Compute the full set of column indices we need (selected + filter).
    let filter_col_indices = if let Some(f) = filter {
        collect_filter_columns(f, meta)
    } else {
        Vec::new()
    };

    let mut all_indices: Vec<usize> = selected_cols.iter().map(|(i, _)| *i).collect();
    for idx in &filter_col_indices {
        if !all_indices.contains(idx) {
            all_indices.push(*idx);
        }
    }
    all_indices.sort();
    all_indices.dedup();

    if !should_use_parallel(partitions.len()) {
        // Fall back to sequential scanning for few partitions.
        let mut rows = Vec::new();
        for p in &partitions {
            rows.extend(scan_partition(p, meta, &all_indices, selected_cols, filter, Some(table_dir))?);
        }
        return Ok(rows);
    }

    // Parallel scan: each partition produces its own Vec<Vec<Value>>.
    use rayon::prelude::*;

    let results: Vec<Result<Vec<Vec<Value>>>> = partitions
        .par_iter()
        .map(|p| scan_partition(p, meta, &all_indices, selected_cols, filter, Some(table_dir)))
        .collect();

    // Merge in partition order, propagating the first error.
    let mut merged = Vec::new();
    for r in results {
        merged.extend(r?);
    }

    Ok(merged)
}

/// Scan a pre-selected list of partition directories (from the optimizer's
/// pruned set) with an optional early-termination row limit.
///
/// When `row_limit` is `Some(n)`, scanning stops as soon as at least `n`
/// rows have been collected (sequential path only — the parallel path
/// collects all then truncates).
pub fn parallel_scan_partitions_pruned(
    partitions: &[std::path::PathBuf],
    meta: &TableMeta,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    row_limit: Option<u64>,
) -> Result<Vec<Vec<Value>>> {
    parallel_scan_partitions_pruned_tiered(partitions, meta, selected_cols, filter, row_limit, None)
}

/// Like `parallel_scan_partitions_pruned` but with an optional `table_dir`
/// for tiered storage transparency.
pub fn parallel_scan_partitions_pruned_tiered(
    partitions: &[std::path::PathBuf],
    meta: &TableMeta,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    row_limit: Option<u64>,
    table_dir: Option<&Path>,
) -> Result<Vec<Vec<Value>>> {
    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    // Compute the full set of column indices we need (selected + filter).
    let filter_col_indices = if let Some(f) = filter {
        collect_filter_columns(f, meta)
    } else {
        Vec::new()
    };

    let mut all_indices: Vec<usize> = selected_cols.iter().map(|(i, _)| *i).collect();
    for idx in &filter_col_indices {
        if !all_indices.contains(idx) {
            all_indices.push(*idx);
        }
    }
    all_indices.sort();
    all_indices.dedup();

    let limit = row_limit.unwrap_or(u64::MAX) as usize;

    if !should_use_parallel(partitions.len()) || limit < 1000 {
        // Sequential scan with early termination — also used for small limits
        // to avoid scanning all partitions in parallel unnecessarily.
        let mut rows = Vec::new();
        for p in partitions {
            let remaining = limit.saturating_sub(rows.len());
            rows.extend(scan_partition_limited(p, meta, &all_indices, selected_cols, filter, table_dir, Some(remaining))?);
            if rows.len() >= limit {
                rows.truncate(limit);
                break;
            }
        }
        return Ok(rows);
    }

    // Parallel scan.
    use rayon::prelude::*;

    let results: Vec<Result<Vec<Vec<Value>>>> = partitions
        .par_iter()
        .map(|p| scan_partition(p, meta, &all_indices, selected_cols, filter, table_dir))
        .collect();

    let mut merged = Vec::new();
    for r in results {
        merged.extend(r?);
        if merged.len() >= limit {
            merged.truncate(limit);
            break;
        }
    }

    Ok(merged)
}

/// Build a rayon thread pool with the given number of threads and use
/// it to run `parallel_scan_partitions`. If `num_threads` is `None`,
/// rayon's default (number of CPUs) is used.
pub fn parallel_scan_partitions_with_threads(
    table_dir: &Path,
    meta: &TableMeta,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    num_threads: Option<usize>,
) -> Result<Vec<Vec<Value>>> {
    match num_threads {
        Some(n) => {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(n)
                .build()
                .map_err(|e| {
                    exchange_common::error::ExchangeDbError::Query(format!(
                        "failed to build rayon pool: {e}"
                    ))
                })?;
            pool.install(|| parallel_scan_partitions(table_dir, meta, selected_cols, filter))
        }
        None => parallel_scan_partitions(table_dir, meta, selected_cols, filter),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_common::types::Timestamp;
    use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Helper: create a table with DAY partitioning and insert rows
    /// spanning multiple days so we get multiple partitions.
    fn setup_multi_partition_table(dir: &Path) -> (PathBuf, TableMeta) {
        let table_name = "test_parallel";

        TableBuilder::new(table_name)
            .column("timestamp", exchange_common::types::ColumnType::Timestamp)
            .column("value", exchange_common::types::ColumnType::F64)
            .column("tag", exchange_common::types::ColumnType::Varchar)
            .timestamp("timestamp")
            .partition_by(exchange_common::types::PartitionBy::Day)
            .build(dir)
            .expect("failed to create table");

        let mut writer = TableWriter::open(dir, table_name).expect("failed to open writer");
        let meta = writer.meta().clone();

        // Day 1: 2024-01-01 (3 rows)
        let day1_base = 1704067200_000_000_000i64; // 2024-01-01 00:00:00 UTC
        for i in 0..3 {
            let ts = Timestamp(day1_base + i * 1_000_000_000);
            writer
                .write_row(ts, &[ColumnValue::F64(10.0 + i as f64), ColumnValue::Str("a")])
                .expect("write failed");
        }

        // Day 2: 2024-01-02 (2 rows)
        let day2_base = day1_base + 86400_000_000_000;
        for i in 0..2 {
            let ts = Timestamp(day2_base + i * 1_000_000_000);
            writer
                .write_row(ts, &[ColumnValue::F64(20.0 + i as f64), ColumnValue::Str("b")])
                .expect("write failed");
        }

        // Day 3: 2024-01-03 (4 rows)
        let day3_base = day1_base + 2 * 86400_000_000_000;
        for i in 0..4 {
            let ts = Timestamp(day3_base + i * 1_000_000_000);
            writer
                .write_row(ts, &[ColumnValue::F64(30.0 + i as f64), ColumnValue::Str("c")])
                .expect("write failed");
        }

        writer.flush().expect("flush failed");
        // Explicitly drop the writer so MmapFile::drop truncates files
        // to their actual data length before we try to read them.
        drop(writer);

        let table_dir = dir.join(table_name);
        (table_dir, meta)
    }

    #[test]
    fn parallel_scan_returns_all_rows() {
        let tmp = TempDir::new().unwrap();
        let (table_dir, meta) = setup_multi_partition_table(tmp.path());

        // Select all columns.
        let selected: Vec<(usize, String)> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let rows =
            parallel_scan_partitions(&table_dir, &meta, &selected, None).expect("scan failed");

        // 3 + 2 + 4 = 9 total rows
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn parallel_scan_matches_sequential() {
        let tmp = TempDir::new().unwrap();
        let (table_dir, meta) = setup_multi_partition_table(tmp.path());

        let selected: Vec<(usize, String)> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        // Sequential scan (uses the same function but forces sequential
        // path by scanning with threshold check bypassed).
        let mut partitions: Vec<_> = std::fs::read_dir(&table_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| e.path())
            .collect();
        partitions.sort();

        let filter_col_indices: Vec<usize> = Vec::new();
        let mut all_indices: Vec<usize> = selected.iter().map(|(i, _)| *i).collect();
        for idx in &filter_col_indices {
            if !all_indices.contains(idx) {
                all_indices.push(*idx);
            }
        }
        all_indices.sort();
        all_indices.dedup();

        let mut sequential_rows = Vec::new();
        for p in &partitions {
            sequential_rows.extend(
                scan_partition(p, &meta, &all_indices, &selected, None, None).expect("seq scan failed"),
            );
        }

        // Parallel scan.
        let parallel_rows =
            parallel_scan_partitions(&table_dir, &meta, &selected, None).expect("par scan failed");

        assert_eq!(sequential_rows.len(), parallel_rows.len());
        assert_eq!(sequential_rows, parallel_rows);
    }

    #[test]
    fn parallel_scan_with_filter() {
        let tmp = TempDir::new().unwrap();
        let (table_dir, meta) = setup_multi_partition_table(tmp.path());

        let selected: Vec<(usize, String)> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        // Filter: value >= 20.0
        let filter = Filter::Gte("value".to_string(), Value::F64(20.0));

        let rows = parallel_scan_partitions(&table_dir, &meta, &selected, Some(&filter))
            .expect("scan failed");

        // Day 2 has 2 rows (20.0, 21.0), Day 3 has 4 rows (30..33) = 6
        assert_eq!(rows.len(), 6);

        // All values should be >= 20.0
        let val_idx = meta.columns.iter().position(|c| c.name == "value").unwrap();
        for row in &rows {
            match &row[val_idx] {
                Value::F64(v) => assert!(*v >= 20.0, "expected >= 20.0, got {v}"),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn parallel_scan_with_custom_threads() {
        let tmp = TempDir::new().unwrap();
        let (table_dir, meta) = setup_multi_partition_table(tmp.path());

        let selected: Vec<(usize, String)> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        let rows =
            parallel_scan_partitions_with_threads(&table_dir, &meta, &selected, None, Some(2))
                .expect("scan failed");

        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn parallel_scan_single_partition_falls_back_sequential() {
        let tmp = TempDir::new().unwrap();
        let table_name = "test_single";

        TableBuilder::new(table_name)
            .column("timestamp", exchange_common::types::ColumnType::Timestamp)
            .column("value", exchange_common::types::ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(exchange_common::types::PartitionBy::None)
            .build(tmp.path())
            .expect("create table failed");

        let mut writer = TableWriter::open(tmp.path(), table_name).expect("open writer failed");
        let meta = writer.meta().clone();

        for i in 0..5 {
            let ts = Timestamp(1_000_000_000 + i * 1_000_000_000);
            writer
                .write_row(ts, &[ColumnValue::F64(i as f64)])
                .expect("write failed");
        }
        writer.flush().expect("flush failed");
        drop(writer);

        let table_dir = tmp.path().join(table_name);
        let selected: Vec<(usize, String)> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (i, c.name.clone()))
            .collect();

        // Only 1 partition (PartitionBy::None -> "default"), so
        // should_use_parallel returns false.
        let rows =
            parallel_scan_partitions(&table_dir, &meta, &selected, None).expect("scan failed");
        assert_eq!(rows.len(), 5);
    }
}
