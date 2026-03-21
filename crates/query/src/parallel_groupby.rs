//! Parallel GROUP BY: each partition is aggregated independently via
//! `rayon::par_iter`, then partial results are merged.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use exchange_common::error::Result;
use exchange_core::table::TableMeta;
use rayon::prelude::*;

use crate::plan::{AggregateKind, Filter, QueryResult, SelectColumn, Value};

// ── Re-use types from vector_groupby ───────────────────────────────────────

/// A serialised group key suitable for hashing.
#[derive(Clone, Eq, PartialEq, Hash)]
struct GroupKey(Vec<u8>);

/// Per-aggregate-column accumulator for a single group.
#[derive(Clone, Debug)]
struct AggregateState {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    first: Option<f64>,
    last: Option<f64>,
    has_value: bool,
    i_sum: i64,
    has_float: bool,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
            has_value: false,
            i_sum: 0,
            has_float: false,
        }
    }

    /// Merge another partition's partial state into this one.
    fn merge(&mut self, other: &AggregateState) {
        if !other.has_value {
            return;
        }
        self.sum += other.sum;
        self.i_sum = self.i_sum.wrapping_add(other.i_sum);
        self.count += other.count;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        // FIRST: keep the first from the earliest partition that has data.
        if self.first.is_none() {
            self.first = other.first;
        }
        // LAST: always take the later partition's last.
        if other.last.is_some() {
            self.last = other.last;
        }
        self.has_value = true;
        if other.has_float {
            self.has_float = true;
        }
    }

    fn result(&self, kind: AggregateKind) -> Value {
        if !self.has_value && kind != AggregateKind::Count {
            return Value::Null;
        }
        match kind {
            AggregateKind::Sum => {
                if self.has_float {
                    Value::F64(self.sum)
                } else {
                    Value::I64(self.i_sum)
                }
            }
            AggregateKind::Count => Value::I64(self.count as i64),
            AggregateKind::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::F64(self.sum / self.count as f64)
                }
            }
            AggregateKind::Min => {
                if self.has_float {
                    Value::F64(self.min)
                } else {
                    Value::I64(self.min as i64)
                }
            }
            AggregateKind::Max => {
                if self.has_float {
                    Value::F64(self.max)
                } else {
                    Value::I64(self.max as i64)
                }
            }
            AggregateKind::First => {
                if let Some(v) = self.first {
                    if self.has_float { Value::F64(v) } else { Value::I64(v as i64) }
                } else {
                    Value::Null
                }
            }
            AggregateKind::Last => {
                if let Some(v) = self.last {
                    if self.has_float { Value::F64(v) } else { Value::I64(v as i64) }
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }
}

/// Partial result from a single partition: group key -> (key values, agg states).
type PartialResult = HashMap<GroupKey, (Vec<Value>, Vec<AggregateState>)>;

/// Parallel GROUP BY: each partition is aggregated independently,
/// then partial results are merged.
///
/// Uses `rayon::par_iter` to aggregate each partition in parallel.
/// Merge rules:
/// - SUM: add partial sums
/// - COUNT: add partial counts
/// - AVG: total_sum / total_count
/// - MIN: min of partial mins
/// - MAX: max of partial maxes
/// - FIRST: first from earliest partition
/// - LAST: last from latest partition
#[allow(clippy::too_many_arguments)]
pub fn parallel_group_by(
    db_root: &Path,
    table_name: &str,
    meta: &TableMeta,
    partitions: &[PathBuf],
    group_columns: &[String],
    aggregates: &[(AggregateKind, String)],
    filter: &Option<Filter>,
    select_cols: &[SelectColumn],
) -> Result<QueryResult> {
    if partitions.is_empty() {
        return build_empty_result(select_cols);
    }

    let table_dir = db_root.join(table_name);

    // Process each partition in parallel using vector_group_by internally.
    // Each partition produces a partial HashMap.
    let partial_results: Vec<Result<PartialResult>> = partitions
        .par_iter()
        .map(|partition_dir| {
            aggregate_single_partition(partition_dir, meta, group_columns, aggregates, filter, &table_dir)
        })
        .collect();

    // Merge partial results in partition order (important for FIRST/LAST).
    let mut global: PartialResult = HashMap::new();

    for partial in partial_results {
        let partial = partial?;
        for (key, (key_values, agg_states)) in partial {
            let entry = global
                .entry(key)
                .or_insert_with(|| (key_values, vec![AggregateState::new(); aggregates.len()]));
            for (i, state) in agg_states.iter().enumerate() {
                entry.1[i].merge(state);
            }
        }
    }

    // Build result.
    build_result(select_cols, group_columns, aggregates, &global)
}

/// Process a single partition and return a partial result map.
fn aggregate_single_partition(
    partition_dir: &Path,
    meta: &TableMeta,
    group_columns: &[String],
    aggregates: &[(AggregateKind, String)],
    filter: &Option<Filter>,
    table_dir: &Path,
) -> Result<PartialResult> {
    use exchange_core::tiered::TieredPartitionReader;

    // Use TieredPartitionReader for transparent warm/cold access.
    let tiered_reader = TieredPartitionReader::open(partition_dir, table_dir).ok();
    let native_path = tiered_reader
        .as_ref()
        .map(|r| r.native_path())
        .unwrap_or(partition_dir);

    if !native_path.exists() {
        return Ok(HashMap::new());
    }

    // Load group columns.
    let group_data: Vec<ColData> = group_columns
        .iter()
        .map(|col_name| load_column_data(native_path, meta, col_name))
        .collect::<Result<Vec<_>>>()?;

    if group_data.is_empty() {
        return Ok(HashMap::new());
    }

    let row_count = col_data_len(&group_data[0]);
    if row_count == 0 {
        return Ok(HashMap::new());
    }

    // Load aggregate input columns.
    let agg_data: Vec<Option<ColData>> = aggregates
        .iter()
        .map(|(_, col_name)| {
            if col_name == "*" {
                Ok(None)
            } else {
                load_column_data(native_path, meta, col_name).map(Some)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    // Apply filter.
    let row_mask: Option<Vec<bool>> = if let Some(f) = filter {
        Some(crate::vector_groupby::compute_filter_mask_pub(native_path, meta, f, row_count)?)
    } else {
        None
    };

    let mut partial: PartialResult = HashMap::new();

    for row_idx in 0..row_count {
        if let Some(ref mask) = row_mask
            && !mask[row_idx] {
                continue;
            }

        let mut key = GroupKey(Vec::new());
        let mut key_values: Vec<Value> = Vec::with_capacity(group_columns.len());

        for gd in &group_data {
            match gd {
                ColData::F64(vals) => {
                    let v = vals[row_idx];
                    key.0.extend_from_slice(&v.to_bits().to_le_bytes());
                    key_values.push(Value::F64(v));
                }
                ColData::I64(vals) => {
                    let v = vals[row_idx];
                    key.0.extend_from_slice(&v.to_le_bytes());
                    key_values.push(Value::I64(v));
                }
                ColData::I32(vals) => {
                    let v = vals[row_idx];
                    key.0.extend_from_slice(&v.to_le_bytes());
                    key_values.push(Value::I64(v as i64));
                }
                ColData::Str(vals) => {
                    let v = &vals[row_idx];
                    key.0.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    key.0.extend_from_slice(v.as_bytes());
                    key_values.push(Value::Str(v.clone()));
                }
            }
        }

        let entry = partial
            .entry(key)
            .or_insert_with(|| (key_values, vec![AggregateState::new(); aggregates.len()]));

        for (agg_idx, (_, _)) in aggregates.iter().enumerate() {
            if let Some(Some(ad)) = agg_data.get(agg_idx) {
                match ad {
                    ColData::F64(vals) => {
                        let v = vals[row_idx];
                        if !v.is_nan() {
                            entry.1[agg_idx].sum += v;
                            entry.1[agg_idx].count += 1;
                            if v < entry.1[agg_idx].min { entry.1[agg_idx].min = v; }
                            if v > entry.1[agg_idx].max { entry.1[agg_idx].max = v; }
                            if entry.1[agg_idx].first.is_none() { entry.1[agg_idx].first = Some(v); }
                            entry.1[agg_idx].last = Some(v);
                            entry.1[agg_idx].has_value = true;
                            entry.1[agg_idx].has_float = true;
                        }
                    }
                    ColData::I64(vals) => {
                        let v = vals[row_idx];
                        entry.1[agg_idx].i_sum = entry.1[agg_idx].i_sum.wrapping_add(v);
                        entry.1[agg_idx].sum += v as f64;
                        entry.1[agg_idx].count += 1;
                        let vf = v as f64;
                        if vf < entry.1[agg_idx].min { entry.1[agg_idx].min = vf; }
                        if vf > entry.1[agg_idx].max { entry.1[agg_idx].max = vf; }
                        if entry.1[agg_idx].first.is_none() { entry.1[agg_idx].first = Some(vf); }
                        entry.1[agg_idx].last = Some(vf);
                        entry.1[agg_idx].has_value = true;
                    }
                    ColData::I32(vals) => {
                        let v = vals[row_idx] as i64;
                        entry.1[agg_idx].i_sum = entry.1[agg_idx].i_sum.wrapping_add(v);
                        entry.1[agg_idx].sum += v as f64;
                        entry.1[agg_idx].count += 1;
                        let vf = v as f64;
                        if vf < entry.1[agg_idx].min { entry.1[agg_idx].min = vf; }
                        if vf > entry.1[agg_idx].max { entry.1[agg_idx].max = vf; }
                        if entry.1[agg_idx].first.is_none() { entry.1[agg_idx].first = Some(vf); }
                        entry.1[agg_idx].last = Some(vf);
                        entry.1[agg_idx].has_value = true;
                    }
                    ColData::Str(_) => {
                        entry.1[agg_idx].count += 1;
                        entry.1[agg_idx].has_value = true;
                    }
                }
            } else {
                // count(*)
                entry.1[agg_idx].count += 1;
                entry.1[agg_idx].has_value = true;
            }
        }
    }

    Ok(partial)
}

// ── Column data loading ────────────────────────────────────────────────────

enum ColData {
    F64(Vec<f64>),
    I64(Vec<i64>),
    I32(Vec<i32>),
    Str(Vec<String>),
}

fn load_column_data(partition_dir: &Path, meta: &TableMeta, col_name: &str) -> Result<ColData> {
    use exchange_common::error::ExchangeDbError;
    use exchange_common::types::ColumnType;
    use exchange_core::column::VarColumnReader;
    use exchange_core::mmap::{self, MmapReadOnly};

    let col_def = meta
        .columns
        .iter()
        .find(|c| c.name == col_name)
        .ok_or_else(|| ExchangeDbError::Query(format!("column not found: {col_name}")))?;

    let col_type: ColumnType = col_def.col_type.into();
    let data_path = partition_dir.join(format!("{}.d", col_name));

    if !data_path.exists() {
        return Ok(match col_type {
            ColumnType::F64 | ColumnType::F32 => ColData::F64(Vec::new()),
            ColumnType::Varchar | ColumnType::Binary => ColData::Str(Vec::new()),
            _ => ColData::I64(Vec::new()),
        });
    }

    if col_type.is_variable_length() {
        let index_path = partition_dir.join(format!("{}.i", col_name));
        if !index_path.exists() {
            return Ok(ColData::Str(Vec::new()));
        }
        let reader = VarColumnReader::open(&data_path, &index_path)?;
        let count = reader.row_count();
        let mut strings = Vec::with_capacity(count as usize);
        for i in 0..count {
            strings.push(reader.read_str(i).to_string());
        }
        return Ok(ColData::Str(strings));
    }

    let ro = MmapReadOnly::open(&data_path)?;
    let data = ro.as_slice();
    mmap::advise_sequential(ro.inner_mmap());

    let result = match col_type {
        ColumnType::F64 => {
            let vals = bytes_as_f64(data);
            ColData::F64(vals.to_vec())
        }
        ColumnType::I64 | ColumnType::Timestamp => {
            let vals = bytes_as_i64(data);
            ColData::I64(vals.to_vec())
        }
        ColumnType::F32 => {
            let vals = bytes_as_f32(data);
            ColData::F64(vals.iter().map(|&v| v as f64).collect())
        }
        ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
            let vals = bytes_as_i32(data);
            ColData::I32(vals.to_vec())
        }
        _ => {
            let element_size = col_type.fixed_size().unwrap_or(8);
            let count = data.len() / element_size;
            let mut vals = Vec::with_capacity(count);
            for i in 0..count {
                let offset = i * element_size;
                let mut buf = [0u8; 8];
                let slice = &data[offset..offset + element_size];
                buf[..element_size].copy_from_slice(slice);
                vals.push(i64::from_le_bytes(buf));
            }
            ColData::I64(vals)
        }
    };

    mmap::advise_dontneed(ro.inner_mmap());
    Ok(result)
}

fn col_data_len(data: &ColData) -> usize {
    match data {
        ColData::F64(v) => v.len(),
        ColData::I64(v) => v.len(),
        ColData::I32(v) => v.len(),
        ColData::Str(v) => v.len(),
    }
}

// ── Byte-slice casts ───────────────────────────────────────────────────────

fn bytes_as_f64(data: &[u8]) -> &[f64] {
    let count = data.len() / 8;
    assert!((data.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f64>()) || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f64, count) }
}

fn bytes_as_i64(data: &[u8]) -> &[i64] {
    let count = data.len() / 8;
    assert!((data.as_ptr() as usize).is_multiple_of(std::mem::align_of::<i64>()) || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const i64, count) }
}

fn bytes_as_f32(data: &[u8]) -> &[f32] {
    let count = data.len() / 4;
    assert!((data.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()) || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const f32, count) }
}

fn bytes_as_i32(data: &[u8]) -> &[i32] {
    let count = data.len() / 4;
    assert!((data.as_ptr() as usize).is_multiple_of(std::mem::align_of::<i32>()) || count == 0);
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const i32, count) }
}

// ── Result builders ────────────────────────────────────────────────────────

fn build_empty_result(select_cols: &[SelectColumn]) -> Result<QueryResult> {
    let col_names = select_cols
        .iter()
        .map(|c| match c {
            SelectColumn::Name(n) => n.clone(),
            SelectColumn::Aggregate { function, column, alias, .. } => {
                if let Some(a) = alias {
                    a.clone()
                } else {
                    let func_name = format!("{function:?}").to_ascii_lowercase();
                    format!("{func_name}({column})")
                }
            }
            _ => "?".to_string(),
        })
        .collect();

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: Vec::new(),
    })
}

fn build_result(
    select_cols: &[SelectColumn],
    group_columns: &[String],
    _aggregates: &[(AggregateKind, String)],
    global: &PartialResult,
) -> Result<QueryResult> {
    let mut col_names = Vec::new();
    for col in select_cols {
        match col {
            SelectColumn::Name(n) => col_names.push(n.clone()),
            SelectColumn::Aggregate { function, column, alias, .. } => {
                if let Some(a) = alias {
                    col_names.push(a.clone());
                } else {
                    let func_name = format!("{function:?}").to_ascii_lowercase();
                    col_names.push(format!("{func_name}({column})"));
                }
            }
            _ => col_names.push("?".to_string()),
        }
    }

    let mut rows = Vec::with_capacity(global.len());
    for (key_values, agg_states) in global.values() {
        let mut row = Vec::with_capacity(select_cols.len());
        let mut agg_idx = 0;

        for col in select_cols {
            match col {
                SelectColumn::Name(name) => {
                    if let Some(gb_idx) = group_columns.iter().position(|gb| gb == name) {
                        row.push(key_values[gb_idx].clone());
                    } else {
                        row.push(Value::Null);
                    }
                }
                SelectColumn::Aggregate { function, .. } => {
                    row.push(agg_states[agg_idx].result(*function));
                    agg_idx += 1;
                }
                _ => row.push(Value::Null),
            }
        }

        rows.push(row);
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_core::mmap::MmapFile;
    use exchange_core::table::{ColumnDef, ColumnTypeSerializable};
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

    fn write_i32_column(path: &Path, values: &[i32]) {
        let mut mf = MmapFile::open(path, 4096).unwrap();
        for &v in values {
            mf.append(&v.to_le_bytes()).unwrap();
        }
        mf.flush().unwrap();
    }

    fn setup_partition(dir: &Path, name: &str, symbols: &[i32], prices: &[f64], volumes: &[i64]) -> PathBuf {
        let part_dir = dir.join(name);
        std::fs::create_dir_all(&part_dir).unwrap();
        write_i32_column(&part_dir.join("symbol.d"), symbols);
        write_f64_column(&part_dir.join("price.d"), prices);
        write_i64_column(&part_dir.join("volume.d"), volumes);
        part_dir
    }

    fn make_meta() -> TableMeta {
        TableMeta {
            name: "test".to_string(),
            columns: vec![
                ColumnDef {
                    name: "symbol".to_string(),
                    col_type: ColumnTypeSerializable::I32,
                    indexed: false,
                },
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
        }
    }

    #[test]
    fn parallel_groupby_matches_sequential() {
        let dir = tempdir().unwrap();
        let meta = make_meta();

        // Create 10 partitions.
        let mut partitions = Vec::new();
        for i in 0..10 {
            let symbols: Vec<i32> = (0..100).map(|j| (j % 5) as i32).collect();
            let prices: Vec<f64> = (0..100).map(|j| (i * 100 + j) as f64 * 0.1).collect();
            let volumes: Vec<i64> = (0..100).map(|j| (i * 100 + j) as i64).collect();
            partitions.push(setup_partition(dir.path(), &format!("p{i}"), &symbols, &prices, &volumes));
        }

        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate { function: AggregateKind::Sum, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Count, column: "*".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Min, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Max, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Avg, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
        ];

        let aggregates = vec![
            (AggregateKind::Sum, "price".to_string()),
            (AggregateKind::Count, "*".to_string()),
            (AggregateKind::Min, "price".to_string()),
            (AggregateKind::Max, "price".to_string()),
            (AggregateKind::Avg, "price".to_string()),
        ];

        let group_columns = vec!["symbol".to_string()];

        // Parallel result.
        let par_result = parallel_group_by(
            dir.path(), "test", &meta, &partitions,
            &group_columns, &aggregates, &None, &select_cols,
        ).unwrap();

        // Sequential result (using vector_group_by which processes sequentially).
        let seq_result = crate::vector_groupby::vector_group_by(
            dir.path(), "test", &meta, &partitions,
            &group_columns, &aggregates, &None, &select_cols,
        ).unwrap();

        if let (QueryResult::Rows { rows: par_rows, .. }, QueryResult::Rows { rows: seq_rows, .. }) = (&par_result, &seq_result) {
            assert_eq!(par_rows.len(), seq_rows.len(), "group count mismatch");

            let mut par_sorted = par_rows.clone();
            let mut seq_sorted = seq_rows.clone();
            par_sorted.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
            seq_sorted.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());

            for (p, s) in par_sorted.iter().zip(seq_sorted.iter()) {
                // Group key.
                assert_eq!(p[0], s[0], "group key mismatch");
                // SUM.
                match (&p[1], &s[1]) {
                    (Value::F64(a), Value::F64(b)) => assert!((a - b).abs() < 1e-6, "sum mismatch: {a} vs {b}"),
                    _ => assert_eq!(p[1], s[1]),
                }
                // COUNT.
                assert_eq!(p[2], s[2], "count mismatch");
                // MIN.
                assert_eq!(p[3], s[3], "min mismatch");
                // MAX.
                assert_eq!(p[4], s[4], "max mismatch");
                // AVG.
                match (&p[5], &s[5]) {
                    (Value::F64(a), Value::F64(b)) => assert!((a - b).abs() < 1e-6, "avg mismatch: {a} vs {b}"),
                    _ => assert_eq!(p[5], s[5]),
                }
            }
        } else {
            panic!("expected Rows results");
        }
    }

    #[test]
    fn parallel_groupby_with_filter() {
        let dir = tempdir().unwrap();
        let meta = make_meta();

        let p1 = setup_partition(dir.path(), "p1", &[1, 2, 1], &[10.0, 20.0, 30.0], &[100, 200, 300]);
        let p2 = setup_partition(dir.path(), "p2", &[1, 2, 1], &[40.0, 50.0, 60.0], &[400, 500, 600]);

        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate { function: AggregateKind::Sum, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
        ];
        let aggregates = vec![(AggregateKind::Sum, "price".to_string())];
        let filter = Some(Filter::Gt("price".to_string(), Value::F64(25.0)));

        let result = parallel_group_by(
            dir.path(), "test", &meta, &[p1, p2],
            &["symbol".to_string()], &aggregates, &filter, &select_cols,
        ).unwrap();

        if let QueryResult::Rows { rows, .. } = &result {
            let mut sorted = rows.clone();
            sorted.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());

            // symbol=1: prices > 25 are 30, 40, 60 -> sum=130
            assert_eq!(sorted[0][0], Value::I64(1));
            assert_eq!(sorted[0][1], Value::F64(130.0));

            // symbol=2: prices > 25 are 50 -> sum=50
            assert_eq!(sorted[1][0], Value::I64(2));
            assert_eq!(sorted[1][1], Value::F64(50.0));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn parallel_groupby_empty() {
        let dir = tempdir().unwrap();
        let meta = make_meta();

        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate { function: AggregateKind::Count, column: "*".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
        ];
        let aggregates = vec![(AggregateKind::Count, "*".to_string())];

        let result = parallel_group_by(
            dir.path(), "test", &meta, &[],
            &["symbol".to_string()], &aggregates, &None, &select_cols,
        ).unwrap();

        if let QueryResult::Rows { rows, .. } = &result {
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn parallel_groupby_correct_sum_count_avg_min_max() {
        let dir = tempdir().unwrap();
        let meta = make_meta();

        // One group only, spread across 3 partitions.
        let p1 = setup_partition(dir.path(), "p1", &[1, 1], &[10.0, 20.0], &[100, 200]);
        let p2 = setup_partition(dir.path(), "p2", &[1, 1], &[30.0, 40.0], &[300, 400]);
        let p3 = setup_partition(dir.path(), "p3", &[1], &[50.0], &[500]);

        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate { function: AggregateKind::Sum, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Count, column: "*".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Avg, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Min, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
            SelectColumn::Aggregate { function: AggregateKind::Max, column: "price".into(), alias: None, filter: None, within_group_order: None, arg_expr: None },
        ];

        let aggregates = vec![
            (AggregateKind::Sum, "price".to_string()),
            (AggregateKind::Count, "*".to_string()),
            (AggregateKind::Avg, "price".to_string()),
            (AggregateKind::Min, "price".to_string()),
            (AggregateKind::Max, "price".to_string()),
        ];

        let result = parallel_group_by(
            dir.path(), "test", &meta, &[p1, p2, p3],
            &["symbol".to_string()], &aggregates, &None, &select_cols,
        ).unwrap();

        if let QueryResult::Rows { rows, .. } = &result {
            assert_eq!(rows.len(), 1);
            let row = &rows[0];

            // sum = 10+20+30+40+50 = 150
            assert_eq!(row[1], Value::F64(150.0));
            // count = 5
            assert_eq!(row[2], Value::I64(5));
            // avg = 150/5 = 30
            assert_eq!(row[3], Value::F64(30.0));
            // min = 10
            assert_eq!(row[4], Value::F64(10.0));
            // max = 50
            assert_eq!(row[5], Value::F64(50.0));
        } else {
            panic!("expected Rows result");
        }
    }
}
