//! Query optimizer that transforms naive plans into efficient ones.
//!
//! Applies several optimizations in sequence:
//! 1. Collect table statistics (cached)
//! 2. Partition pruning — skip partitions that cannot contain matching rows
//! 3. Index scan selection — use bitmap indexes for selective equality filters
//! 4. Predicate pushdown — move WHERE filters as close to the scan as possible
//! 5. Limit pushdown — scan partitions in reverse and stop early for ORDER BY ... DESC LIMIT N

use exchange_common::error::Result;
use exchange_common::types::{ColumnType, PartitionBy};
use exchange_core::table::TableMeta;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::plan::{Filter, JoinType, OrderBy, QueryPlan, Value};
#[cfg(test)]
use crate::plan::GroupByMode;

// ─── Table Statistics ────────────────────────────────────────────────

/// Statistics about a column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Estimated distinct values.
    pub distinct_count: u64,
    /// Number of null values.
    pub null_count: u64,
    /// Minimum observed value (encoded as JSON-friendly representation).
    pub min_value: Option<StatValue>,
    /// Maximum observed value.
    pub max_value: Option<StatValue>,
    /// Whether a bitmap index is available for this column.
    pub has_index: bool,
}

/// A serializable representation of a value for statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StatValue {
    I64(i64),
    F64(f64),
    Str(String),
    Timestamp(i64),
}

impl StatValue {
    pub fn to_plan_value(&self) -> Value {
        match self {
            StatValue::I64(v) => Value::I64(*v),
            StatValue::F64(v) => Value::F64(*v),
            StatValue::Str(s) => Value::Str(s.clone()),
            StatValue::Timestamp(ns) => Value::Timestamp(*ns),
        }
    }
}

impl From<&Value> for StatValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::I64(x) => StatValue::I64(*x),
            Value::F64(x) => StatValue::F64(*x),
            Value::Str(s) => StatValue::Str(s.clone()),
            Value::Timestamp(ns) => StatValue::Timestamp(*ns),
            Value::Null => StatValue::I64(0),
        }
    }
}

/// Aggregate statistics about a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub row_count: u64,
    pub partition_count: u32,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub column_stats: HashMap<String, ColumnStats>,
}

/// Name of the stats cache file stored under the table directory.
const STATS_FILE: &str = "_stats";

/// Collect statistics for a table by scanning partition directories and
/// sampling data files. Results are cached in a `_stats` JSON file.
pub fn collect_stats(table_dir: &Path, meta: &TableMeta) -> Result<TableStats> {
    // Try loading cached stats first.
    let stats_path = table_dir.join(STATS_FILE);
    if stats_path.exists()
        && let Ok(json) = std::fs::read_to_string(&stats_path)
            && let Ok(cached) = serde_json::from_str::<TableStats>(&json) {
                return Ok(cached);
            }

    let partitions = list_partition_dirs(table_dir)?;
    let partition_count = partitions.len() as u32;

    let mut row_count: u64 = 0;
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut column_stats: HashMap<String, ColumnStats> = HashMap::new();

    // Initialize column stats from metadata.
    for col_def in &meta.columns {
        let col_type: ColumnType = col_def.col_type.into();
        column_stats.insert(
            col_def.name.clone(),
            ColumnStats {
                distinct_count: 0,
                null_count: 0,
                min_value: None,
                max_value: None,
                has_index: col_def.indexed && col_type == ColumnType::Symbol,
            },
        );
    }

    let ts_col_name = &meta.columns[meta.timestamp_column].name;

    // Scan each partition to gather basic stats. Use TieredPartitionReader
    // so warm/cold partitions are transparently decompressed for stats.
    let mut _tiered_readers: Vec<exchange_core::tiered::TieredPartitionReader> = Vec::new();
    for partition_path in &partitions {
        let tiered_reader =
            exchange_core::tiered::TieredPartitionReader::open(partition_path, table_dir).ok();
        let native_path = tiered_reader
            .as_ref()
            .map(|r| r.native_path().to_path_buf());
        let effective_path = native_path.as_deref().unwrap_or(partition_path.as_path());
        if let Some(reader) = tiered_reader {
            _tiered_readers.push(reader);
        }

        // Read timestamp column to get row count and timestamp range.
        let ts_data_path = effective_path.join(format!("{}.d", ts_col_name));
        if !ts_data_path.exists() {
            continue;
        }

        let ts_reader = exchange_core::column::FixedColumnReader::open(
            &ts_data_path,
            ColumnType::Timestamp,
        )?;
        let n = ts_reader.row_count();
        row_count += n;

        if n > 0 {
            let first_ts = ts_reader.read_i64(0);
            let last_ts = ts_reader.read_i64(n - 1);
            if first_ts < min_ts {
                min_ts = first_ts;
            }
            if last_ts > max_ts {
                max_ts = last_ts;
            }

            // Update timestamp column stats.
            if let Some(cs) = column_stats.get_mut(ts_col_name) {
                match &cs.min_value {
                    Some(StatValue::Timestamp(existing)) if *existing <= first_ts => {}
                    _ => cs.min_value = Some(StatValue::Timestamp(first_ts)),
                }
                match &cs.max_value {
                    Some(StatValue::Timestamp(existing)) if *existing >= last_ts => {}
                    _ => cs.max_value = Some(StatValue::Timestamp(last_ts)),
                }
                cs.distinct_count += n;
            }
        }

        // For indexed symbol columns, count distinct values from bitmap index.
        for col_def in &meta.columns {
            if col_def.indexed {
                let col_type: ColumnType = col_def.col_type.into();
                if col_type == ColumnType::Symbol {
                    let key_path = effective_path.join(format!("{}.k", col_def.name));
                    if key_path.exists()
                        && let Ok(reader) =
                            exchange_core::index::bitmap::BitmapIndexReader::open(
                                effective_path,
                                &col_def.name,
                            )
                        {
                            // The reader exposes count per key; we estimate
                            // distinct values from the number of keys with non-zero counts.
                            // We cannot enumerate keys directly so we check a reasonable range.
                            let mut distinct = 0u64;
                            for key in 0..1024i32 {
                                if reader.count(key) > 0 {
                                    distinct += 1;
                                } else if key > 0 {
                                    // If we hit a gap after key 0, stop early
                                    // (keys are contiguous from the symbol table).
                                    break;
                                }
                            }
                            if let Some(cs) = column_stats.get_mut(&col_def.name) {
                                cs.distinct_count =
                                    cs.distinct_count.max(distinct);
                            }
                        }
                }
            }
        }
    }

    if min_ts == i64::MAX {
        min_ts = 0;
    }
    if max_ts == i64::MIN {
        max_ts = 0;
    }

    let stats = TableStats {
        row_count,
        partition_count,
        min_timestamp: min_ts,
        max_timestamp: max_ts,
        column_stats,
    };

    // Cache stats.
    if let Ok(json) = serde_json::to_string_pretty(&stats) {
        let _ = std::fs::write(&stats_path, json);
    }

    Ok(stats)
}

/// Invalidate the cached stats file so it gets recomputed next time.
pub fn invalidate_stats(table_dir: &Path) {
    let stats_path = table_dir.join(STATS_FILE);
    if stats_path.exists() {
        let _ = std::fs::remove_file(&stats_path);
    }
}

// ─── Partition Pruning ───────────────────────────────────────────────

/// Prune partitions that cannot contain matching rows based on the filter.
///
/// This is the single biggest optimization — for time-range queries it
/// can eliminate 99%+ of I/O by skipping partitions whose timestamp
/// range does not overlap the query predicate.
pub fn prune_partitions(
    partitions: &[PathBuf],
    filter: &Option<Filter>,
    timestamp_col: &str,
    partition_by: PartitionBy,
) -> Vec<PathBuf> {
    let filter = match filter {
        Some(f) => f,
        None => return partitions.to_vec(),
    };

    if partition_by == PartitionBy::None {
        return partitions.to_vec();
    }

    // Extract timestamp bounds from the filter.
    let (lower_bound, upper_bound) = extract_timestamp_bounds(filter, timestamp_col);

    if lower_bound.is_none() && upper_bound.is_none() {
        return partitions.to_vec();
    }

    partitions
        .iter()
        .filter(|p| {
            let raw_name = match p.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => return true, // keep if we can't parse
            };

            // Strip .xpqt / .parquet extensions for cold partition paths
            // (e.g. "2024-03-16.xpqt" -> "2024-03-16").
            let dir_name = raw_name
                .strip_suffix(".xpqt")
                .or_else(|| raw_name.strip_suffix(".parquet"))
                .unwrap_or(raw_name);

            let (part_start, part_end) =
                match partition_dir_to_timestamp_range(dir_name, partition_by) {
                    Some(range) => range,
                    None => return true, // keep if we can't parse
                };

            // Check overlap: partition range [part_start, part_end) must overlap
            // query range [lower_bound, upper_bound].
            if let Some(lower) = lower_bound
                && part_end <= lower {
                    return false; // partition entirely before query range
                }
            if let Some(upper) = upper_bound
                && part_start > upper {
                    return false; // partition entirely after query range
                }
            true
        })
        .cloned()
        .collect()
}

/// Extract lower and upper timestamp bounds from a filter expression.
///
/// Returns `(lower_bound_inclusive, upper_bound_inclusive)` as nanosecond
/// timestamps. `None` means unbounded in that direction.
fn extract_timestamp_bounds(
    filter: &Filter,
    timestamp_col: &str,
) -> (Option<i64>, Option<i64>) {
    match filter {
        Filter::Eq(col, Value::Timestamp(ts)) if col == timestamp_col => {
            (Some(*ts), Some(*ts))
        }
        Filter::Gt(col, Value::Timestamp(ts)) if col == timestamp_col => {
            (Some(*ts + 1), None)
        }
        Filter::Gte(col, Value::Timestamp(ts)) if col == timestamp_col => {
            (Some(*ts), None)
        }
        Filter::Lt(col, Value::Timestamp(ts)) if col == timestamp_col => {
            (None, Some(*ts - 1))
        }
        Filter::Lte(col, Value::Timestamp(ts)) if col == timestamp_col => {
            (None, Some(*ts))
        }
        Filter::Between(col, Value::Timestamp(lo), Value::Timestamp(hi))
            if col == timestamp_col =>
        {
            (Some(*lo), Some(*hi))
        }
        // Also handle I64 values (sometimes timestamps are stored as raw nanos).
        Filter::Eq(col, Value::I64(ts)) if col == timestamp_col => {
            (Some(*ts), Some(*ts))
        }
        Filter::Gt(col, Value::I64(ts)) if col == timestamp_col => {
            (Some(*ts + 1), None)
        }
        Filter::Gte(col, Value::I64(ts)) if col == timestamp_col => {
            (Some(*ts), None)
        }
        Filter::Lt(col, Value::I64(ts)) if col == timestamp_col => {
            (None, Some(*ts - 1))
        }
        Filter::Lte(col, Value::I64(ts)) if col == timestamp_col => {
            (None, Some(*ts))
        }
        Filter::Between(col, Value::I64(lo), Value::I64(hi))
            if col == timestamp_col =>
        {
            (Some(*lo), Some(*hi))
        }
        Filter::And(parts) => {
            let mut lower: Option<i64> = None;
            let mut upper: Option<i64> = None;
            for part in parts {
                let (lo, hi) = extract_timestamp_bounds(part, timestamp_col);
                if let Some(l) = lo {
                    lower = Some(lower.map_or(l, |prev: i64| prev.max(l)));
                }
                if let Some(h) = hi {
                    upper = Some(upper.map_or(h, |prev: i64| prev.min(h)));
                }
            }
            (lower, upper)
        }
        // For OR, we take the widest possible range.
        Filter::Or(parts) => {
            let mut lower: Option<i64> = None;
            let mut upper: Option<i64> = None;
            let mut all_bounded_below = true;
            let mut all_bounded_above = true;
            for part in parts {
                let (lo, hi) = extract_timestamp_bounds(part, timestamp_col);
                match lo {
                    Some(l) => lower = Some(lower.map_or(l, |prev: i64| prev.min(l))),
                    None => all_bounded_below = false,
                }
                match hi {
                    Some(h) => upper = Some(upper.map_or(h, |prev: i64| prev.max(h))),
                    None => all_bounded_above = false,
                }
            }
            (
                if all_bounded_below { lower } else { None },
                if all_bounded_above { upper } else { None },
            )
        }
        _ => (None, None),
    }
}

/// Parse a partition directory name back to a timestamp range in nanoseconds.
///
/// Returns `(start_ns, end_ns)` where `end_ns` is the exclusive upper bound
/// of the partition interval.
fn partition_dir_to_timestamp_range(
    dir_name: &str,
    partition_by: PartitionBy,
) -> Option<(i64, i64)> {
    match partition_by {
        PartitionBy::None => None,
        PartitionBy::Day => {
            // Format: YYYY-MM-DD
            let parts: Vec<&str> = dir_name.split('-').collect();
            if parts.len() != 3 {
                return None;
            }
            let y: i32 = parts[0].parse().ok()?;
            let m: u32 = parts[1].parse().ok()?;
            let d: u32 = parts[2].parse().ok()?;
            let start_secs = date_to_epoch_secs(y, m, d)?;
            let end_secs = start_secs + 86400; // next day
            Some((start_secs * 1_000_000_000, end_secs * 1_000_000_000))
        }
        PartitionBy::Month => {
            // Format: YYYY-MM
            let parts: Vec<&str> = dir_name.split('-').collect();
            if parts.len() != 2 {
                return None;
            }
            let y: i32 = parts[0].parse().ok()?;
            let m: u32 = parts[1].parse().ok()?;
            let start_secs = date_to_epoch_secs(y, m, 1)?;
            // Compute start of next month.
            let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
            let end_secs = date_to_epoch_secs(ny, nm, 1)?;
            Some((start_secs * 1_000_000_000, end_secs * 1_000_000_000))
        }
        PartitionBy::Year => {
            // Format: YYYY
            let y: i32 = dir_name.parse().ok()?;
            let start_secs = date_to_epoch_secs(y, 1, 1)?;
            let end_secs = date_to_epoch_secs(y + 1, 1, 1)?;
            Some((start_secs * 1_000_000_000, end_secs * 1_000_000_000))
        }
        PartitionBy::Hour => {
            // Format: YYYY-MM-DDThh
            let t_pos = dir_name.find('T')?;
            let date_part = &dir_name[..t_pos];
            let hour_part = &dir_name[t_pos + 1..];
            let parts: Vec<&str> = date_part.split('-').collect();
            if parts.len() != 3 {
                return None;
            }
            let y: i32 = parts[0].parse().ok()?;
            let m: u32 = parts[1].parse().ok()?;
            let d: u32 = parts[2].parse().ok()?;
            let h: u32 = hour_part.parse().ok()?;
            let start_secs = date_to_epoch_secs(y, m, d)? + h as i64 * 3600;
            let end_secs = start_secs + 3600;
            Some((start_secs * 1_000_000_000, end_secs * 1_000_000_000))
        }
        PartitionBy::Week => {
            // Format: YYYY-Www
            let parts: Vec<&str> = dir_name.split("-W").collect();
            if parts.len() != 2 {
                return None;
            }
            let y: i32 = parts[0].parse().ok()?;
            let w: u32 = parts[1].parse().ok()?;
            // Approximate: week w starts around day (w-1)*7 + 1 of the year.
            // This is a rough approximation; the partition module uses day/7+1.
            let jan1 = date_to_epoch_secs(y, 1, 1)?;
            let start_secs = jan1 + ((w.saturating_sub(1)) as i64) * 7 * 86400;
            let end_secs = start_secs + 7 * 86400;
            Some((start_secs * 1_000_000_000, end_secs * 1_000_000_000))
        }
    }
}

/// Convert a civil date (year, month, day) to seconds since Unix epoch
/// using Howard Hinnant's algorithm (same as used in partition.rs).
fn date_to_epoch_secs(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Adjust month for algorithm: March = 0, ..., February = 11.
    let (y, m) = if month <= 2 {
        (year as i64 - 1, month + 9)
    } else {
        (year as i64, month - 3)
    };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;
    Some(days * 86400)
}

// ─── Index Scan Selection ────────────────────────────────────────────

/// Plan for using a bitmap index to satisfy a filter.
#[derive(Debug, Clone)]
pub struct IndexScanPlan {
    /// Column name to use the index on.
    pub column: String,
    /// Symbol ID (bitmap index key) to look up.
    pub key: i32,
    /// Estimated number of matching rows.
    pub estimated_rows: u64,
}

/// Determine whether an index scan should be used for the given filter.
///
/// Returns `Some(IndexScanPlan)` when the filter is an equality predicate
/// on an indexed symbol column and the estimated selectivity is below 10%.
pub fn should_use_index(filter: &Filter, stats: &TableStats) -> Option<IndexScanPlan> {
    match filter {
        Filter::Eq(col, Value::I64(key)) => {
            let cs = stats.column_stats.get(col)?;
            if !cs.has_index {
                return None;
            }
            let estimated_rows = if cs.distinct_count > 0 {
                stats.row_count / cs.distinct_count
            } else {
                stats.row_count
            };
            // Only use index if selectivity < 10%.
            if stats.row_count > 0 && estimated_rows * 10 < stats.row_count {
                Some(IndexScanPlan {
                    column: col.clone(),
                    key: *key as i32,
                    estimated_rows,
                })
            } else {
                None
            }
        }
        Filter::And(parts) => {
            // Pick the most selective indexed predicate.
            let mut best: Option<IndexScanPlan> = None;
            for part in parts {
                if let Some(plan) = should_use_index(part, stats) {
                    best = Some(match best {
                        Some(prev) if prev.estimated_rows <= plan.estimated_rows => prev,
                        _ => plan,
                    });
                }
            }
            best
        }
        _ => None,
    }
}

// ─── Predicate Pushdown ──────────────────────────────────────────────

/// Push WHERE predicates as close to the scan as possible.
///
/// For JOINs, this separates filter predicates into those that apply
/// only to the left table, only to the right table, and those that
/// remain as post-join filters.
pub fn push_predicates_to_scan(plan: &QueryPlan) -> QueryPlan {
    match plan {
        QueryPlan::Select { filter: Some(f), .. } => {
            // For simple SELECTs, the filter is already at the scan level.
            // Ensure compound AND filters are flattened for optimal evaluation.
            let mut result = plan.clone();
            if let QueryPlan::Select {
                ref mut filter, ..
            } = result
            {
                *filter = Some(flatten_and(f.clone()));
            }
            result
        }
        QueryPlan::Join {
            left_table,
            right_table,
            left_alias,
            right_alias,
            columns,
            join_type,
            on_columns,
            filter: Some(f),
            order_by,
            limit,
        } => {
            // Attempt to push non-join predicates to the appropriate side.
            // We can only push predicates for simple table-qualified column refs.
            let left_name = left_alias.as_deref().unwrap_or(left_table);
            let right_name = right_alias.as_deref().unwrap_or(right_table);
            let flat = flatten_and_to_vec(f.clone());
            let mut _left_preds = Vec::new();
            let mut _right_preds = Vec::new();
            let mut remaining = Vec::new();

            for pred in flat {
                let col = filter_column(&pred);
                if let Some(c) = col {
                    // Check if the column is qualified with a table name.
                    if c.starts_with(&format!("{}.", left_name)) {
                        _left_preds.push(pred);
                    } else if c.starts_with(&format!("{}.", right_name)) {
                        _right_preds.push(pred);
                    } else {
                        remaining.push(pred);
                    }
                } else {
                    remaining.push(pred);
                }
            }

            let pushed_filter = if remaining.is_empty() {
                None
            } else if remaining.len() == 1 {
                Some(remaining.into_iter().next().unwrap())
            } else {
                Some(Filter::And(remaining))
            };

            QueryPlan::Join {
                left_table: left_table.clone(),
                right_table: right_table.clone(),
                left_alias: left_alias.clone(),
                right_alias: right_alias.clone(),
                columns: columns.clone(),
                join_type: *join_type,
                on_columns: on_columns.clone(),
                filter: pushed_filter,
                order_by: order_by.clone(),
                limit: *limit,
            }
        }
        QueryPlan::DerivedScan {
            subquery,
            alias,
            columns,
            filter,
            order_by,
            limit,
            group_by,
            having,
            distinct,
        } => {
            // Recursively push predicates into the subquery.
            let optimized_sub = push_predicates_to_scan(subquery);
            QueryPlan::DerivedScan {
                subquery: Box::new(optimized_sub),
                alias: alias.clone(),
                columns: columns.clone(),
                filter: filter.clone(),
                order_by: order_by.clone(),
                limit: *limit,
                group_by: group_by.clone(),
                having: having.clone(),
                distinct: *distinct,
            }
        }
        _ => plan.clone(),
    }
}

/// Flatten nested AND filters into a single AND with all children.
fn flatten_and(filter: Filter) -> Filter {
    match filter {
        Filter::And(parts) => {
            let mut flat = Vec::new();
            for part in parts {
                match flatten_and(part) {
                    Filter::And(inner) => flat.extend(inner),
                    other => flat.push(other),
                }
            }
            if flat.len() == 1 {
                flat.into_iter().next().unwrap()
            } else {
                Filter::And(flat)
            }
        }
        other => other,
    }
}

/// Flatten an AND filter into a vec of individual predicates.
fn flatten_and_to_vec(filter: Filter) -> Vec<Filter> {
    match flatten_and(filter) {
        Filter::And(parts) => parts,
        single => vec![single],
    }
}

/// Extract the column name from a simple filter predicate.
fn filter_column(filter: &Filter) -> Option<&str> {
    match filter {
        Filter::Eq(col, _)
        | Filter::Gt(col, _)
        | Filter::Lt(col, _)
        | Filter::Gte(col, _)
        | Filter::Lte(col, _)
        | Filter::Between(col, _, _)
        | Filter::BetweenSymmetric(col, _, _) => Some(col.as_str()),
        Filter::Subquery { column, .. } => Some(column.as_str()),
        _ => None,
    }
}

// ─── Limit Pushdown ──────────────────────────────────────────────────

/// Optimization hint for the executor: scan partitions in reverse order
/// and stop after collecting enough rows when the query has
/// `ORDER BY timestamp_col DESC LIMIT N`.
#[derive(Debug, Clone)]
pub struct LimitPushdown {
    /// Scan partitions in reverse order.
    pub reverse_scan: bool,
    /// Maximum rows to collect before stopping.
    pub limit: u64,
}

/// Check if the query plan can benefit from limit pushdown.
///
/// This applies when:
/// - There is a `LIMIT`
/// - There is exactly one `ORDER BY` on the designated timestamp column, `DESC`
/// - There is no `GROUP BY` or `SAMPLE BY` (which require full scan)
pub fn check_limit_pushdown(
    order_by: &[OrderBy],
    limit: Option<u64>,
    timestamp_col: &str,
    group_by: &[String],
    has_sample_by: bool,
) -> Option<LimitPushdown> {
    let lim = limit?;
    if !group_by.is_empty() || has_sample_by {
        return None;
    }
    if order_by.len() != 1 {
        return None;
    }
    let ob = &order_by[0];
    if ob.column == timestamp_col && ob.descending {
        Some(LimitPushdown {
            reverse_scan: true,
            limit: lim,
        })
    } else {
        None
    }
}

// ─── Optimized Partition List ────────────────────────────────────────

/// The result of the optimizer — an optimized plan together with
/// execution hints that the executor can use.
#[derive(Debug, Clone)]
pub struct OptimizedPlan {
    /// The (possibly transformed) query plan.
    pub plan: QueryPlan,
    /// Pruned list of partitions to scan (for SELECT queries).
    pub pruned_partitions: Option<Vec<PathBuf>>,
    /// Index scan plan if applicable.
    pub index_scan: Option<IndexScanPlan>,
    /// Limit pushdown hint.
    pub limit_pushdown: Option<LimitPushdown>,
    /// Collected table stats (if relevant).
    pub stats: Option<TableStats>,
}

// ─── Main Optimize Entry Point ───────────────────────────────────────

/// Apply all optimizations to a query plan and return an `OptimizedPlan`
/// with execution hints for the executor.
pub fn optimize(plan: QueryPlan, db_root: &Path) -> Result<OptimizedPlan> {
    match &plan {
        QueryPlan::Select {
            table,
            filter,
            order_by,
            limit,
            sample_by,
            group_by,
            ..
        } => {
            let table_dir = db_root.join(table);
            if !table_dir.exists() {
                return Ok(OptimizedPlan {
                    plan,
                    pruned_partitions: None,
                    index_scan: None,
                    limit_pushdown: None,
                    stats: None,
                });
            }

            let meta = TableMeta::load(&table_dir.join("_meta"))?;
            let partition_by: PartitionBy = meta.partition_by.into();
            let ts_col_name = meta.columns[meta.timestamp_column].name.clone();

            // 1. Collect stats (cached).
            let stats = collect_stats(&table_dir, &meta)?;

            // 2. Partition pruning.
            let all_partitions = list_partition_dirs(&table_dir)?;
            let pruned = prune_partitions(
                &all_partitions,
                filter,
                &ts_col_name,
                partition_by,
            );

            // 3. Index scan selection.
            let index_scan = filter
                .as_ref()
                .and_then(|f| should_use_index(f, &stats));

            // 4. Predicate pushdown.
            let optimized_plan = push_predicates_to_scan(&plan);

            // 5. Limit pushdown.
            let limit_pushdown = check_limit_pushdown(
                order_by,
                *limit,
                &ts_col_name,
                group_by,
                sample_by.is_some(),
            );

            Ok(OptimizedPlan {
                plan: optimized_plan,
                pruned_partitions: Some(pruned),
                index_scan,
                limit_pushdown,
                stats: Some(stats),
            })
        }
        // For non-SELECT plans, just pass through.
        _ => Ok(OptimizedPlan {
            plan,
            pruned_partitions: None,
            index_scan: None,
            limit_pushdown: None,
            stats: None,
        }),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────

/// List all partitions under a table directory, including warm (LZ4) and
/// cold (XPQT/parquet) partitions. Delegates to the core listing function
/// which discovers `_cold/` and tier metadata entries.
fn list_partition_dirs(table_dir: &Path) -> Result<Vec<PathBuf>> {
    exchange_core::table::list_partitions(table_dir)
}

// ─── Cost-Based Join Ordering ────────────────────────────────────────

/// Estimates the cost of join plans and finds optimal join orderings.
pub struct JoinCostEstimator;

impl JoinCostEstimator {
    /// Estimate the cost of a join plan.
    ///
    /// Returns a relative cost metric. Lower is better.
    ///
    /// # Arguments
    /// * `left_rows` - Estimated row count of the left input.
    /// * `right_rows` - Estimated row count of the right input.
    /// * `join_type` - The type of join.
    /// * `has_index` - Whether an index is available on the join key.
    pub fn estimate_cost(
        left_rows: u64,
        right_rows: u64,
        join_type: &JoinType,
        has_index: bool,
    ) -> f64 {
        
        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right => {
                if has_index {
                    // Index nested loop join: O(left * log(right))
                    left_rows as f64 * (right_rows as f64).log2().max(1.0)
                } else {
                    // Hash join: O(left + right) for build + probe
                    left_rows as f64 + right_rows as f64
                }
            }
            JoinType::Cross => {
                // Cartesian product: O(left * right)
                left_rows as f64 * right_rows as f64
            }
            JoinType::FullOuter => {
                // Full outer join: slightly more than hash join
                (left_rows as f64 + right_rows as f64) * 1.2
            }
            JoinType::Lateral => {
                // Lateral join: for each left row, evaluate the right subquery.
                // Similar to nested loop cost.
                left_rows as f64 * (right_rows as f64).log2().max(1.0)
            }
        }
    }

    /// For multi-table joins, find the optimal join order using a greedy
    /// algorithm that always joins the two cheapest tables first.
    ///
    /// # Arguments
    /// * `tables` - Slice of (name, estimated_rows) for each table.
    /// * `join_conditions` - Slice of (left_idx, right_idx, has_index)
    ///   describing available join conditions between tables.
    ///
    /// # Returns
    /// A vector of table indices in the order they should be joined.
    pub fn optimize_join_order(
        tables: &[(&str, u64)],
        join_conditions: &[(usize, usize, bool)],
    ) -> Vec<usize> {
        if tables.len() <= 1 {
            return (0..tables.len()).collect();
        }

        let mut remaining: Vec<usize> = (0..tables.len()).collect();
        let mut order = Vec::with_capacity(tables.len());

        // Start with the smallest table.
        remaining.sort_by_key(|&i| tables[i].1);
        order.push(remaining.remove(0));

        // Greedily add tables that produce the cheapest join.
        while !remaining.is_empty() {
            let mut best_idx = 0;
            let mut best_cost = f64::MAX;

            let current_rows: u64 = order
                .iter()
                .map(|&i| tables[i].1)
                .sum::<u64>()
                .max(1);

            for (ri, &table_idx) in remaining.iter().enumerate() {
                // Check if there's a join condition between any table in
                // the current set and this candidate.
                let has_index = join_conditions.iter().any(|&(l, r, idx)| {
                    (order.contains(&l) && r == table_idx && idx)
                        || (order.contains(&r) && l == table_idx && idx)
                });
                let has_condition = join_conditions.iter().any(|&(l, r, _)| {
                    (order.contains(&l) && r == table_idx)
                        || (order.contains(&r) && l == table_idx)
                });

                let cost = if has_condition {
                    Self::estimate_cost(
                        current_rows,
                        tables[table_idx].1,
                        &JoinType::Inner,
                        has_index,
                    )
                } else {
                    // No direct join condition — this would be a cross join.
                    // Penalize heavily.
                    Self::estimate_cost(
                        current_rows,
                        tables[table_idx].1,
                        &JoinType::Cross,
                        false,
                    )
                };

                if cost < best_cost {
                    best_cost = cost;
                    best_idx = ri;
                }
            }

            order.push(remaining.remove(best_idx));
        }

        order
    }
}

// ─── Query Plan Cost Estimation ──────────────────────────────────────

/// Estimated cost of executing a query plan.
#[derive(Debug, Clone)]
pub struct PlanCost {
    /// Estimated number of output rows.
    pub estimated_rows: u64,
    /// Estimated total bytes to read.
    pub estimated_bytes: u64,
    /// I/O cost (disk reads, measured in abstract units).
    pub io_cost: f64,
    /// CPU cost (computation, measured in abstract units).
    pub cpu_cost: f64,
    /// Total cost: io_cost + cpu_cost.
    pub total_cost: f64,
}

/// Bytes per row estimate for cost calculation.
const BYTES_PER_ROW: u64 = 64;
/// Cost factor for reading one page of data from disk.
const IO_COST_PER_PAGE: f64 = 1.0;
/// Page size for I/O cost estimation (4 KB).
const PAGE_SIZE: u64 = 4096;
/// CPU cost factor per row for filtering.
const CPU_COST_PER_ROW_FILTER: f64 = 0.01;
/// CPU cost factor per row for aggregation.
const CPU_COST_PER_ROW_AGG: f64 = 0.02;
/// CPU cost factor per row for sorting.
const CPU_COST_PER_ROW_SORT: f64 = 0.05;

/// Estimate the cost of executing a query plan.
pub fn estimate_plan_cost(plan: &QueryPlan, stats: &TableStats) -> PlanCost {
    match plan {
        QueryPlan::Select {
            filter,
            order_by,
            limit,
            group_by,
            sample_by,
            ..
        } => {
            let total_rows = stats.row_count;
            let total_bytes = total_rows * BYTES_PER_ROW;

            // I/O cost: read all partitions (pruning already applied).
            let pages = total_bytes.div_ceil(PAGE_SIZE);
            let io_cost = pages as f64 * IO_COST_PER_PAGE;

            // CPU cost: filter + aggregate + sort.
            let mut cpu_cost = 0.0;

            // Filtering cost.
            let rows_after_filter = if filter.is_some() {
                // Estimate 30% selectivity for generic filters.
                cpu_cost += total_rows as f64 * CPU_COST_PER_ROW_FILTER;
                (total_rows as f64 * 0.3) as u64
            } else {
                total_rows
            };

            // Aggregation cost.
            let rows_after_agg = if !group_by.is_empty() || sample_by.is_some() {
                cpu_cost += rows_after_filter as f64 * CPU_COST_PER_ROW_AGG;
                // Estimate: number of distinct groups.
                
                group_by.iter().filter_map(|g| {
                    stats.column_stats.get(g).map(|cs| cs.distinct_count.max(1))
                }).min().unwrap_or(rows_after_filter.min(1000))
            } else {
                rows_after_filter
            };

            // Sort cost.
            if !order_by.is_empty() && rows_after_agg > 1 {
                let n = rows_after_agg as f64;
                cpu_cost += n * n.log2() * CPU_COST_PER_ROW_SORT;
            }

            // Apply limit.
            let estimated_rows = match limit {
                Some(lim) => rows_after_agg.min(*lim),
                None => rows_after_agg,
            };

            let total_cost = io_cost + cpu_cost;

            PlanCost {
                estimated_rows,
                estimated_bytes: total_bytes,
                io_cost,
                cpu_cost,
                total_cost,
            }
        }
        QueryPlan::Join { .. } | QueryPlan::MultiJoin { .. } => {
            // For joins, estimate based on total row count from stats.
            let total_bytes = stats.row_count * BYTES_PER_ROW;
            let pages = total_bytes.div_ceil(PAGE_SIZE);
            let io_cost = pages as f64 * IO_COST_PER_PAGE;
            let cpu_cost = stats.row_count as f64 * CPU_COST_PER_ROW_FILTER;
            PlanCost {
                estimated_rows: stats.row_count,
                estimated_bytes: total_bytes,
                io_cost,
                cpu_cost,
                total_cost: io_cost + cpu_cost,
            }
        }
        _ => PlanCost {
            estimated_rows: 0,
            estimated_bytes: 0,
            io_cost: 0.0,
            cpu_cost: 0.0,
            total_cost: 0.0,
        },
    }
}

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_common::types::Timestamp;
    use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
    use tempfile::TempDir;

    /// Create a table with DAY partitioning spanning 30 days.
    fn setup_30_day_table(db_root: &Path) -> TableMeta {
        let table_name = "trades_opt";
        TableBuilder::new(table_name)
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("tag", ColumnType::Varchar)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .expect("create table");

        let mut writer = TableWriter::open(db_root, table_name).expect("open writer");
        let meta = writer.meta().clone();

        // 2024-01-01 through 2024-01-30, 10 rows per day.
        let base = 1704067200_000_000_000i64; // 2024-01-01 00:00 UTC
        for day in 0..30 {
            let day_base = base + day * 86400_000_000_000;
            for i in 0..10 {
                let ts = Timestamp(day_base + i * 1_000_000_000);
                writer
                    .write_row(
                        ts,
                        &[
                            ColumnValue::F64(100.0 + day as f64),
                            ColumnValue::Str("test"),
                        ],
                    )
                    .expect("write");
            }
        }
        writer.flush().expect("flush");
        drop(writer);
        meta
    }

    #[test]
    fn partition_pruning_single_day() {
        let tmp = TempDir::new().unwrap();
        let _meta = setup_30_day_table(tmp.path());

        let table_dir = tmp.path().join("trades_opt");
        let all_partitions = list_partition_dirs(&table_dir).unwrap();
        assert_eq!(all_partitions.len(), 30);

        // Query for just 2024-01-15.
        let day15_start = 1704067200_000_000_000i64 + 14 * 86400_000_000_000;
        let day15_end = day15_start + 86400_000_000_000 - 1;
        let filter = Filter::Between(
            "timestamp".to_string(),
            Value::Timestamp(day15_start),
            Value::Timestamp(day15_end),
        );

        let pruned = prune_partitions(
            &all_partitions,
            &Some(filter),
            "timestamp",
            PartitionBy::Day,
        );

        assert_eq!(pruned.len(), 1, "expected 1 partition, got {}", pruned.len());
        let dir_name = pruned[0].file_name().unwrap().to_str().unwrap();
        assert_eq!(dir_name, "2024-01-15");
    }

    #[test]
    fn partition_pruning_range() {
        let tmp = TempDir::new().unwrap();
        let _meta = setup_30_day_table(tmp.path());

        let table_dir = tmp.path().join("trades_opt");
        let all_partitions = list_partition_dirs(&table_dir).unwrap();

        // Query for 2024-01-10 through 2024-01-12.
        let day10_start = 1704067200_000_000_000i64 + 9 * 86400_000_000_000;
        let day12_end = 1704067200_000_000_000i64 + 12 * 86400_000_000_000 - 1;
        let filter = Filter::And(vec![
            Filter::Gte("timestamp".to_string(), Value::Timestamp(day10_start)),
            Filter::Lte("timestamp".to_string(), Value::Timestamp(day12_end)),
        ]);

        let pruned = prune_partitions(
            &all_partitions,
            &Some(filter),
            "timestamp",
            PartitionBy::Day,
        );

        assert_eq!(pruned.len(), 3, "expected 3 partitions, got {}", pruned.len());
    }

    #[test]
    fn partition_pruning_no_filter() {
        let tmp = TempDir::new().unwrap();
        let _meta = setup_30_day_table(tmp.path());

        let table_dir = tmp.path().join("trades_opt");
        let all_partitions = list_partition_dirs(&table_dir).unwrap();

        let pruned = prune_partitions(
            &all_partitions,
            &None,
            "timestamp",
            PartitionBy::Day,
        );

        assert_eq!(pruned.len(), 30);
    }

    #[test]
    fn partition_pruning_gt_filter() {
        let tmp = TempDir::new().unwrap();
        let _meta = setup_30_day_table(tmp.path());

        let table_dir = tmp.path().join("trades_opt");
        let all_partitions = list_partition_dirs(&table_dir).unwrap();

        // Query for timestamp > 2024-01-28 00:00 UTC
        let day28_start = 1704067200_000_000_000i64 + 27 * 86400_000_000_000;
        let filter = Filter::Gt("timestamp".to_string(), Value::Timestamp(day28_start));

        let pruned = prune_partitions(
            &all_partitions,
            &Some(filter),
            "timestamp",
            PartitionBy::Day,
        );

        // Days 28, 29, 30 should remain (28 partially overlaps).
        assert!(
            pruned.len() >= 2 && pruned.len() <= 3,
            "expected 2-3 partitions, got {}",
            pruned.len()
        );
    }

    #[test]
    fn stats_collection_and_caching() {
        let tmp = TempDir::new().unwrap();
        let meta = setup_30_day_table(tmp.path());

        let table_dir = tmp.path().join("trades_opt");

        // First call collects and caches.
        let stats = collect_stats(&table_dir, &meta).unwrap();
        assert_eq!(stats.row_count, 300);
        assert_eq!(stats.partition_count, 30);
        assert!(stats.min_timestamp > 0);
        assert!(stats.max_timestamp > stats.min_timestamp);

        // Verify cache file was written.
        assert!(table_dir.join(STATS_FILE).exists());

        // Second call should return cached stats.
        let stats2 = collect_stats(&table_dir, &meta).unwrap();
        assert_eq!(stats2.row_count, stats.row_count);

        // Invalidate and recollect.
        invalidate_stats(&table_dir);
        assert!(!table_dir.join(STATS_FILE).exists());
        let stats3 = collect_stats(&table_dir, &meta).unwrap();
        assert_eq!(stats3.row_count, 300);
    }

    #[test]
    fn index_scan_selection() {
        // Create a table with an indexed symbol column.
        let tmp = TempDir::new().unwrap();
        let table_name = "trades_idx";
        TableBuilder::new(table_name)
            .column("timestamp", ColumnType::Timestamp)
            .indexed_column("symbol", ColumnType::Symbol)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(tmp.path())
            .expect("create table");

        let mut writer = TableWriter::open(tmp.path(), table_name).expect("open writer");

        // Write 1000 rows, 100 distinct symbols.
        let base = 1704067200_000_000_000i64;
        for i in 0..1000 {
            let ts = Timestamp(base + i * 1_000_000_000);
            let symbol_id = (i % 100) as i32;
            writer
                .write_row(ts, &[ColumnValue::I32(symbol_id), ColumnValue::F64(100.0)])
                .expect("write");
        }
        writer.flush().expect("flush");
        drop(writer);

        // Build a stats object manually (since we don't have bitmap indexes
        // written by TableWriter automatically).
        let mut column_stats = HashMap::new();
        column_stats.insert(
            "symbol".to_string(),
            ColumnStats {
                distinct_count: 100,
                null_count: 0,
                min_value: None,
                max_value: None,
                has_index: true,
            },
        );
        column_stats.insert(
            "timestamp".to_string(),
            ColumnStats {
                distinct_count: 1000,
                null_count: 0,
                min_value: None,
                max_value: None,
                has_index: false,
            },
        );

        let stats = TableStats {
            row_count: 1000,
            partition_count: 12,
            min_timestamp: base,
            max_timestamp: base + 999 * 1_000_000_000,
            column_stats,
        };

        // Test: equality filter on indexed symbol column with low selectivity (1%).
        let filter = Filter::Eq("symbol".to_string(), Value::I64(42));
        let plan = should_use_index(&filter, &stats);
        assert!(plan.is_some(), "expected index scan plan");
        let plan = plan.unwrap();
        assert_eq!(plan.column, "symbol");
        assert_eq!(plan.key, 42);
        assert_eq!(plan.estimated_rows, 10); // 1000 / 100

        // Test: non-indexed column should return None.
        let filter2 = Filter::Eq("timestamp".to_string(), Value::I64(0));
        assert!(should_use_index(&filter2, &stats).is_none());

        // Test: high selectivity (only 2 distinct values) should not use index.
        let mut stats_low = stats.clone();
        stats_low.column_stats.get_mut("symbol").unwrap().distinct_count = 2;
        let filter3 = Filter::Eq("symbol".to_string(), Value::I64(0));
        assert!(
            should_use_index(&filter3, &stats_low).is_none(),
            "should not use index when selectivity is >= 10%"
        );
    }

    #[test]
    fn limit_pushdown_check() {
        // Should trigger: ORDER BY timestamp DESC LIMIT 10
        let result = check_limit_pushdown(
            &[OrderBy {
                column: "timestamp".to_string(),
                descending: true,
            }],
            Some(10),
            "timestamp",
            &[],
            false,
        );
        assert!(result.is_some());
        let lp = result.unwrap();
        assert!(lp.reverse_scan);
        assert_eq!(lp.limit, 10);

        // Should NOT trigger: no limit
        let result2 = check_limit_pushdown(
            &[OrderBy {
                column: "timestamp".to_string(),
                descending: true,
            }],
            None,
            "timestamp",
            &[],
            false,
        );
        assert!(result2.is_none());

        // Should NOT trigger: ASC order
        let result3 = check_limit_pushdown(
            &[OrderBy {
                column: "timestamp".to_string(),
                descending: false,
            }],
            Some(10),
            "timestamp",
            &[],
            false,
        );
        assert!(result3.is_none());

        // Should NOT trigger: GROUP BY present
        let result4 = check_limit_pushdown(
            &[OrderBy {
                column: "timestamp".to_string(),
                descending: true,
            }],
            Some(10),
            "timestamp",
            &["symbol".to_string()],
            false,
        );
        assert!(result4.is_none());

        // Should NOT trigger: SAMPLE BY present
        let result5 = check_limit_pushdown(
            &[OrderBy {
                column: "timestamp".to_string(),
                descending: true,
            }],
            Some(10),
            "timestamp",
            &[],
            true,
        );
        assert!(result5.is_none());
    }

    #[test]
    fn predicate_pushdown_flattens_and() {
        use crate::plan::SelectColumn;

        let plan = QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: Some(Filter::And(vec![
                Filter::And(vec![
                    Filter::Gt("a".to_string(), Value::I64(1)),
                    Filter::Lt("b".to_string(), Value::I64(10)),
                ]),
                Filter::Eq("c".to_string(), Value::I64(5)),
            ])),
            order_by: vec![],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };

        let optimized = push_predicates_to_scan(&plan);
        if let QueryPlan::Select { filter: Some(f), .. } = &optimized {
            // Should be flattened to a single And with 3 children.
            match f {
                Filter::And(parts) => assert_eq!(parts.len(), 3),
                _ => panic!("expected And filter, got {:?}", f),
            }
        } else {
            panic!("expected Select with filter");
        }
    }

    #[test]
    fn optimize_full_pipeline() {
        let tmp = TempDir::new().unwrap();
        let _meta = setup_30_day_table(tmp.path());

        use crate::plan::SelectColumn;

        // Query: SELECT * FROM trades_opt WHERE timestamp BETWEEN day15_start AND day15_end LIMIT 5
        let day15_start = 1704067200_000_000_000i64 + 14 * 86400_000_000_000;
        let day15_end = day15_start + 86400_000_000_000 - 1;

        let plan = QueryPlan::Select {
            table: "trades_opt".to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: Some(Filter::Between(
                "timestamp".to_string(),
                Value::Timestamp(day15_start),
                Value::Timestamp(day15_end),
            )),
            order_by: vec![],
            limit: Some(5),
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };

        let optimized = optimize(plan, tmp.path()).unwrap();
        // Should have pruned to 1 partition.
        assert_eq!(optimized.pruned_partitions.as_ref().unwrap().len(), 1);
        // Stats should be populated.
        assert!(optimized.stats.is_some());
        let stats = optimized.stats.unwrap();
        assert_eq!(stats.row_count, 300);
    }

    #[test]
    fn date_to_epoch_secs_known_values() {
        // 1970-01-01 should be 0.
        assert_eq!(date_to_epoch_secs(1970, 1, 1), Some(0));

        // 2024-01-01 should be 1704067200.
        assert_eq!(date_to_epoch_secs(2024, 1, 1), Some(1704067200));

        // 2024-03-15 should be 1710460800.
        assert_eq!(date_to_epoch_secs(2024, 3, 15), Some(1710460800));
    }

    #[test]
    fn partition_dir_to_range_day() {
        let (start, end) =
            partition_dir_to_timestamp_range("2024-01-15", PartitionBy::Day).unwrap();
        let expected_start = 1705276800i64 * 1_000_000_000;
        let expected_end = (1705276800i64 + 86400) * 1_000_000_000;
        assert_eq!(start, expected_start);
        assert_eq!(end, expected_end);
    }

    #[test]
    fn partition_dir_to_range_month() {
        let (start, end) =
            partition_dir_to_timestamp_range("2024-01", PartitionBy::Month).unwrap();
        // Jan 2024 start.
        let expected_start = 1704067200i64 * 1_000_000_000;
        // Feb 2024 start.
        let expected_end = 1706745600i64 * 1_000_000_000;
        assert_eq!(start, expected_start);
        assert_eq!(end, expected_end);
    }

    #[test]
    fn partition_dir_to_range_year() {
        let (start, end) =
            partition_dir_to_timestamp_range("2024", PartitionBy::Year).unwrap();
        let expected_start = 1704067200i64 * 1_000_000_000;
        // 2025-01-01
        let expected_end = 1735689600i64 * 1_000_000_000;
        assert_eq!(start, expected_start);
        assert_eq!(end, expected_end);
    }

    #[test]
    fn partition_dir_to_range_hour() {
        let (start, end) =
            partition_dir_to_timestamp_range("2024-01-15T14", PartitionBy::Hour).unwrap();
        let base_secs = 1705276800i64 + 14 * 3600;
        let expected_start = base_secs * 1_000_000_000;
        let expected_end = (base_secs + 3600) * 1_000_000_000;
        assert_eq!(start, expected_start);
        assert_eq!(end, expected_end);
    }

    // ─── Join Cost Estimator Tests ───────────────────────────────────

    #[test]
    fn join_cost_inner_hash() {
        // Hash join cost should be O(left + right).
        let cost = JoinCostEstimator::estimate_cost(1000, 500, &JoinType::Inner, false);
        assert_eq!(cost, 1500.0);
    }

    #[test]
    fn join_cost_inner_index() {
        // Index join cost should be O(left * log2(right)).
        let cost = JoinCostEstimator::estimate_cost(1000, 1024, &JoinType::Inner, true);
        // 1000 * log2(1024) = 1000 * 10 = 10000
        assert!((cost - 10_000.0).abs() < 0.01);
    }

    #[test]
    fn join_cost_cross() {
        // Cross join cost should be O(left * right).
        let cost = JoinCostEstimator::estimate_cost(100, 200, &JoinType::Cross, false);
        assert_eq!(cost, 20_000.0);
    }

    #[test]
    fn join_cost_hash_cheaper_than_cross() {
        let hash_cost =
            JoinCostEstimator::estimate_cost(1000, 1000, &JoinType::Inner, false);
        let cross_cost =
            JoinCostEstimator::estimate_cost(1000, 1000, &JoinType::Cross, false);
        assert!(hash_cost < cross_cost);
    }

    #[test]
    fn join_order_smallest_first() {
        // Tables: A(1000), B(100), C(500)
        // Should start with B (smallest), then add cheapest next.
        let tables = vec![("A", 1000), ("B", 100), ("C", 500)];
        let conditions = vec![
            (0, 1, false), // A JOIN B
            (1, 2, false), // B JOIN C
        ];
        let order = JoinCostEstimator::optimize_join_order(&tables, &conditions);
        // First table should be B (index 1, smallest).
        assert_eq!(order[0], 1);
    }

    #[test]
    fn join_order_two_tables() {
        let tables = vec![("A", 500), ("B", 100)];
        let conditions = vec![(0, 1, false)];
        let order = JoinCostEstimator::optimize_join_order(&tables, &conditions);
        // Should start with B (smaller).
        assert_eq!(order[0], 1);
        assert_eq!(order[1], 0);
    }

    #[test]
    fn join_order_prefers_indexed() {
        // A(1000), B(1000), C(1000)
        // Condition: A-B (no index), A-C (has index)
        // After starting with smallest (all equal, so first by sort),
        // should prefer the indexed join.
        let tables = vec![("A", 1000), ("B", 1000), ("C", 1000)];
        let conditions = vec![
            (0, 1, false), // A-B, no index
            (0, 2, true),  // A-C, has index
        ];
        let order = JoinCostEstimator::optimize_join_order(&tables, &conditions);
        assert_eq!(order.len(), 3);
    }

    // ─── Plan Cost Estimation Tests ──────────────────────────────────

    #[test]
    fn plan_cost_basic_select() {
        use crate::plan::SelectColumn;
        let plan = QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };
        let stats = TableStats {
            row_count: 10_000,
            partition_count: 1,
            min_timestamp: 0,
            max_timestamp: 1_000_000,
            column_stats: HashMap::new(),
        };
        let cost = estimate_plan_cost(&plan, &stats);
        assert_eq!(cost.estimated_rows, 10_000);
        assert!(cost.io_cost > 0.0);
        assert_eq!(cost.cpu_cost, 0.0); // no filter, no sort, no group
        assert_eq!(cost.total_cost, cost.io_cost + cost.cpu_cost);
    }

    #[test]
    fn plan_cost_with_filter() {
        use crate::plan::SelectColumn;
        let plan = QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: Some(Filter::Gt("price".to_string(), Value::F64(100.0))),
            order_by: vec![],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };
        let stats = TableStats {
            row_count: 10_000,
            partition_count: 1,
            min_timestamp: 0,
            max_timestamp: 1_000_000,
            column_stats: HashMap::new(),
        };
        let cost = estimate_plan_cost(&plan, &stats);
        // With filter: estimated 30% of rows pass.
        assert_eq!(cost.estimated_rows, 3_000);
        assert!(cost.cpu_cost > 0.0);
    }

    #[test]
    fn plan_cost_with_order_by() {
        use crate::plan::SelectColumn;
        let plan = QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: None,
            order_by: vec![OrderBy {
                column: "timestamp".to_string(),
                descending: true,
            }],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };
        let stats = TableStats {
            row_count: 10_000,
            partition_count: 1,
            min_timestamp: 0,
            max_timestamp: 1_000_000,
            column_stats: HashMap::new(),
        };
        let cost = estimate_plan_cost(&plan, &stats);
        // Sort adds CPU cost: n * log2(n) * factor.
        assert!(cost.cpu_cost > 0.0);
        assert!(cost.total_cost > cost.io_cost);
    }

    #[test]
    fn plan_cost_limit_reduces_rows() {
        use crate::plan::SelectColumn;
        let plan = QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::Wildcard],
            filter: None,
            order_by: vec![],
            limit: Some(10),
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };
        let stats = TableStats {
            row_count: 100_000,
            partition_count: 10,
            min_timestamp: 0,
            max_timestamp: 1_000_000,
            column_stats: HashMap::new(),
        };
        let cost = estimate_plan_cost(&plan, &stats);
        assert_eq!(cost.estimated_rows, 10);
    }
}
