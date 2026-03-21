//! Window function evaluation for SQL OVER clauses.
//!
//! Supports: row_number, rank, dense_rank, lag, lead, first_value, last_value,
//! and windowed aggregates (sum, avg, count).

use crate::plan::{OrderBy, Value};
use std::collections::HashMap;

/// Window function specification from the OVER clause.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<String>,
    pub order_by: Vec<OrderBy>,
    pub frame: Option<WindowFrame>,
}

/// Window frame specification (ROWS/RANGE BETWEEN ... AND ...).
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrame {
    Rows { start: FrameBound, end: FrameBound },
    /// RANGE frame: value-based windowing (e.g. RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW).
    /// The bounds are expressed in the same unit as the ORDER BY column (nanoseconds for timestamps).
    Range { start: FrameBound, end: FrameBound },
}

/// Boundary for a window frame.
#[derive(Debug, Clone, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// A window function call with its OVER specification.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFunction {
    /// Name of the window function (e.g. "row_number", "lag", "sum").
    pub name: String,
    /// Arguments to the function (column names or literal values).
    pub args: Vec<WindowFuncArg>,
    /// The OVER clause.
    pub over: WindowSpec,
    /// Output alias (from AS clause).
    pub alias: Option<String>,
}

impl std::fmt::Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(alias) = &self.alias {
            write!(f, "{alias}")
        } else {
            let args_str: Vec<String> = self
                .args
                .iter()
                .map(|a| match a {
                    WindowFuncArg::Column(c) => c.clone(),
                    WindowFuncArg::LiteralInt(n) => n.to_string(),
                    WindowFuncArg::LiteralFloat(v) => v.to_string(),
                    WindowFuncArg::LiteralStr(s) => format!("'{s}'"),
                    WindowFuncArg::Wildcard => "*".to_string(),
                    WindowFuncArg::Null => "NULL".to_string(),
                })
                .collect();
            write!(f, "{}({})", self.name, args_str.join(", "))
        }
    }
}

/// An argument to a window function.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFuncArg {
    /// A column reference.
    Column(String),
    /// A literal integer (e.g. offset for lag/lead).
    LiteralInt(i64),
    /// A literal float.
    LiteralFloat(f64),
    /// A literal string.
    LiteralStr(String),
    /// Wildcard `*`.
    Wildcard,
    /// NULL literal.
    Null,
}

/// Returns true if the given function name is a known window function.
pub fn is_window_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "row_number" | "rank" | "dense_rank" | "lag" | "lead"
            | "first_value" | "last_value"
            | "sum" | "avg" | "count"
    )
}

/// Apply window functions to a set of rows, returning new column names and
/// the rows with window function result columns appended.
///
/// `base_columns` is the list of column names corresponding to positions in each row.
/// `window_fns` is the list of window function calls to evaluate.
///
/// Returns the names of the appended columns and the modified rows.
pub fn apply_window_functions(
    rows: &mut Vec<Vec<Value>>,
    base_columns: &[String],
    window_fns: &[WindowFunction],
) -> Vec<String> {
    let mut new_col_names = Vec::new();

    for wf in window_fns {
        let col_name = wf
            .alias
            .clone()
            .unwrap_or_else(|| format_window_func_name(wf));
        new_col_names.push(col_name);

        let values = evaluate_window_function(rows, base_columns, wf);

        // Append the computed value to each row.
        for (i, row) in rows.iter_mut().enumerate() {
            row.push(values[i].clone());
        }
    }

    new_col_names
}

fn format_window_func_name(wf: &WindowFunction) -> String {
    let args_str: Vec<String> = wf
        .args
        .iter()
        .map(|a| match a {
            WindowFuncArg::Column(c) => c.clone(),
            WindowFuncArg::LiteralInt(n) => n.to_string(),
            WindowFuncArg::LiteralFloat(f) => f.to_string(),
            WindowFuncArg::LiteralStr(s) => format!("'{s}'"),
            WindowFuncArg::Wildcard => "*".to_string(),
            WindowFuncArg::Null => "NULL".to_string(),
        })
        .collect();
    format!("{}({})", wf.name, args_str.join(", "))
}

/// Evaluate a single window function over all rows, returning one Value per row.
fn evaluate_window_function(
    rows: &[Vec<Value>],
    base_columns: &[String],
    wf: &WindowFunction,
) -> Vec<Value> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Build partition groups: map from partition key to list of original row indices.
    let partition_col_indices: Vec<usize> = wf
        .over
        .partition_by
        .iter()
        .filter_map(|col| base_columns.iter().position(|c| c == col))
        .collect();

    // Create sorted indices within each partition.
    let order_col_specs: Vec<(usize, bool)> = wf
        .over
        .order_by
        .iter()
        .filter_map(|ob| {
            base_columns
                .iter()
                .position(|c| c == &ob.column)
                .map(|idx| (idx, ob.descending))
        })
        .collect();

    // Group row indices by partition key.
    let mut partition_map: HashMap<Vec<ValueKey>, Vec<usize>> = HashMap::new();
    let mut partition_order: Vec<Vec<ValueKey>> = Vec::new();

    for (i, row) in rows.iter().enumerate() {
        let key: Vec<ValueKey> = partition_col_indices
            .iter()
            .map(|&idx| ValueKey(row[idx].clone()))
            .collect();
        let entry = partition_map.entry(key.clone());
        use std::collections::hash_map::Entry;
        match entry {
            Entry::Vacant(e) => {
                e.insert(vec![i]);
                partition_order.push(key);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().push(i);
            }
        }
    }

    let mut result = vec![Value::Null; rows.len()];

    // For each partition, sort by ORDER BY and compute the function.
    for key in &partition_order {
        let indices = partition_map.get(key).unwrap();
        let mut sorted_indices: Vec<usize> = indices.clone();

        // Sort by the ORDER BY columns.
        sorted_indices.sort_by(|&a, &b| {
            for &(col_idx, desc) in &order_col_specs {
                let cmp = rows[a][col_idx]
                    .partial_cmp(&rows[b][col_idx])
                    .unwrap_or(std::cmp::Ordering::Equal);
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        compute_for_partition(rows, base_columns, wf, &sorted_indices, &mut result);
    }

    result
}

/// Compute window function values for a single partition (already sorted).
fn compute_for_partition(
    rows: &[Vec<Value>],
    base_columns: &[String],
    wf: &WindowFunction,
    sorted_indices: &[usize],
    result: &mut [Value],
) {
    let func = wf.name.to_ascii_lowercase();
    match func.as_str() {
        "row_number" => {
            for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
                result[orig_idx] = Value::I64((pos + 1) as i64);
            }
        }
        "rank" => {
            compute_rank_with_cols(rows, base_columns, wf, sorted_indices, result, false);
        }
        "dense_rank" => {
            compute_rank_with_cols(rows, base_columns, wf, sorted_indices, result, true);
        }
        "lag" => {
            compute_lag_lead(rows, base_columns, wf, sorted_indices, result, true);
        }
        "lead" => {
            compute_lag_lead(rows, base_columns, wf, sorted_indices, result, false);
        }
        "first_value" => {
            compute_first_last_value(rows, base_columns, wf, sorted_indices, result, true);
        }
        "last_value" => {
            compute_first_last_value(rows, base_columns, wf, sorted_indices, result, false);
        }
        "sum" => {
            compute_windowed_aggregate(rows, base_columns, wf, sorted_indices, result, AggKind::Sum);
        }
        "avg" => {
            compute_windowed_aggregate(rows, base_columns, wf, sorted_indices, result, AggKind::Avg);
        }
        "count" => {
            compute_windowed_aggregate(rows, base_columns, wf, sorted_indices, result, AggKind::Count);
        }
        _ => {
            // Unknown function: leave as Null.
        }
    }
}

/// Compute rank/dense_rank with access to base_columns.
fn compute_rank_with_cols(
    rows: &[Vec<Value>],
    base_columns: &[String],
    wf: &WindowFunction,
    sorted_indices: &[usize],
    result: &mut [Value],
    dense: bool,
) {
    if sorted_indices.is_empty() {
        return;
    }

    let order_col_indices: Vec<usize> = wf
        .over
        .order_by
        .iter()
        .filter_map(|ob| base_columns.iter().position(|c| c == &ob.column))
        .collect();

    result[sorted_indices[0]] = Value::I64(1);
    let mut dense_rank: i64 = 1;

    for i in 1..sorted_indices.len() {
        let prev_idx = sorted_indices[i - 1];
        let curr_idx = sorted_indices[i];

        let tied = order_col_indices.iter().all(|&col_idx| {
            rows[prev_idx][col_idx] == rows[curr_idx][col_idx]
        });

        if tied {
            result[curr_idx] = result[prev_idx].clone();
        } else if dense {
            dense_rank += 1;
            result[curr_idx] = Value::I64(dense_rank);
        } else {
            result[curr_idx] = Value::I64((i + 1) as i64);
        }
    }
}

/// Compute lag or lead.
fn compute_lag_lead(
    rows: &[Vec<Value>],
    base_columns: &[String],
    wf: &WindowFunction,
    sorted_indices: &[usize],
    result: &mut [Value],
    is_lag: bool,
) {
    // First arg: column name. Second arg: offset (default 1). Third arg: default value.
    let col_idx = match wf.args.first() {
        Some(WindowFuncArg::Column(c)) => base_columns.iter().position(|bc| bc == c),
        Some(WindowFuncArg::Wildcard) => Some(0),
        _ => None,
    };

    let col_idx = match col_idx {
        Some(idx) => idx,
        None => return,
    };

    let offset = match wf.args.get(1) {
        Some(WindowFuncArg::LiteralInt(n)) => *n as usize,
        None => 1,
        _ => 1,
    };

    let default_val = match wf.args.get(2) {
        Some(WindowFuncArg::LiteralInt(n)) => Value::I64(*n),
        Some(WindowFuncArg::LiteralFloat(f)) => Value::F64(*f),
        Some(WindowFuncArg::LiteralStr(s)) => Value::Str(s.clone()),
        Some(WindowFuncArg::Null) | None => Value::Null,
        Some(WindowFuncArg::Column(_)) => Value::Null,
        Some(WindowFuncArg::Wildcard) => Value::Null,
    };

    for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
        let source_pos = if is_lag {
            if pos >= offset {
                Some(pos - offset)
            } else {
                None
            }
        } else {
            let target = pos + offset;
            if target < sorted_indices.len() {
                Some(target)
            } else {
                None
            }
        };

        result[orig_idx] = match source_pos {
            Some(sp) => rows[sorted_indices[sp]][col_idx].clone(),
            None => default_val.clone(),
        };
    }
}

/// Compute first_value or last_value within the window frame.
fn compute_first_last_value(
    rows: &[Vec<Value>],
    base_columns: &[String],
    wf: &WindowFunction,
    sorted_indices: &[usize],
    result: &mut [Value],
    is_first: bool,
) {
    let col_idx = match wf.args.first() {
        Some(WindowFuncArg::Column(c)) => base_columns.iter().position(|bc| bc == c),
        Some(WindowFuncArg::Wildcard) => Some(0),
        _ => None,
    };

    let col_idx = match col_idx {
        Some(idx) => idx,
        None => return,
    };

    // Determine if we have a RANGE frame.
    let order_col_idx_for_range = if matches!(&wf.over.frame, Some(WindowFrame::Range { .. })) {
        wf.over.order_by.first().and_then(|ob| {
            base_columns.iter().position(|c| c == &ob.column)
        })
    } else {
        None
    };

    for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
        let (frame_start, frame_end) = if let (Some(WindowFrame::Range { start, end }), Some(oc_idx)) =
            (&wf.over.frame, order_col_idx_for_range)
        {
            resolve_range_frame(rows, sorted_indices, oc_idx, pos, start, end)
        } else {
            resolve_frame(&wf.over.frame, pos, sorted_indices.len(), !wf.over.order_by.is_empty())
        };

        let val = if is_first {
            rows[sorted_indices[frame_start]][col_idx].clone()
        } else {
            rows[sorted_indices[frame_end]][col_idx].clone()
        };

        result[orig_idx] = val;
    }
}

#[derive(Debug, Clone, Copy)]
enum AggKind {
    Sum,
    Avg,
    Count,
}

/// Compute a windowed aggregate (sum, avg, count) within the window frame.
fn compute_windowed_aggregate(
    rows: &[Vec<Value>],
    base_columns: &[String],
    wf: &WindowFunction,
    sorted_indices: &[usize],
    result: &mut [Value],
    kind: AggKind,
) {
    let col_idx = match wf.args.first() {
        Some(WindowFuncArg::Column(c)) => base_columns.iter().position(|bc| bc == c),
        Some(WindowFuncArg::Wildcard) => Some(0),
        _ => {
            // count(*) case - just count rows.
            if matches!(kind, AggKind::Count) {
                // Use first column as placeholder.
                Some(0)
            } else {
                None
            }
        }
    };

    let col_idx = match col_idx {
        Some(idx) => idx,
        None => return,
    };

    let is_count_star = matches!(wf.args.first(), Some(WindowFuncArg::Wildcard) | None);

    // Determine if we have a RANGE frame and need value-based windowing.
    let order_col_idx_for_range = if matches!(&wf.over.frame, Some(WindowFrame::Range { .. })) {
        wf.over.order_by.first().and_then(|ob| {
            base_columns.iter().position(|c| c == &ob.column)
        })
    } else {
        None
    };

    for (pos, &orig_idx) in sorted_indices.iter().enumerate() {
        let (frame_start, frame_end) = if let (Some(WindowFrame::Range { start, end }), Some(oc_idx)) =
            (&wf.over.frame, order_col_idx_for_range)
        {
            resolve_range_frame(rows, sorted_indices, oc_idx, pos, start, end)
        } else {
            resolve_frame(&wf.over.frame, pos, sorted_indices.len(), !wf.over.order_by.is_empty())
        };

        let mut sum_i: i64 = 0;
        let mut sum_f: f64 = 0.0;
        let mut has_float = false;
        let mut count: i64 = 0;

        for frame_pos in frame_start..=frame_end {
            let row_idx = sorted_indices[frame_pos];
            let val = &rows[row_idx][col_idx];

            if is_count_star {
                count += 1;
                continue;
            }

            match val {
                Value::I64(n) => {
                    sum_i += n;
                    count += 1;
                }
                Value::F64(f) => {
                    sum_f += f;
                    has_float = true;
                    count += 1;
                }
                Value::Timestamp(ns) => {
                    sum_i += ns;
                    count += 1;
                }
                Value::Null | Value::Str(_) => {
                    if is_count_star {
                        count += 1;
                    }
                }
            }
        }

        result[orig_idx] = match kind {
            AggKind::Sum => {
                if count == 0 && !is_count_star {
                    Value::Null
                } else if has_float {
                    Value::F64(sum_f + sum_i as f64)
                } else {
                    Value::I64(sum_i)
                }
            }
            AggKind::Avg => {
                if count == 0 {
                    Value::Null
                } else {
                    let total = if has_float {
                        sum_f + sum_i as f64
                    } else {
                        sum_i as f64
                    };
                    Value::F64(total / count as f64)
                }
            }
            AggKind::Count => Value::I64(count),
        };
    }
}

/// Resolve the frame bounds to concrete start/end positions within the partition.
fn resolve_frame(
    frame: &Option<WindowFrame>,
    current_pos: usize,
    partition_len: usize,
    has_order_by: bool,
) -> (usize, usize) {
    match frame {
        Some(WindowFrame::Rows { start, end }) => {
            resolve_rows_bounds(start, end, current_pos, partition_len)
        }
        Some(WindowFrame::Range { start, end }) => {
            // For RANGE frames without actual value-based resolution,
            // fall back to row-based semantics (the full value-based
            // resolution is handled by `resolve_range_frame`).
            resolve_rows_bounds(start, end, current_pos, partition_len)
        }
        None => {
            if has_order_by {
                // Default frame with ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
                (0, current_pos)
            } else {
                // Default frame without ORDER BY: entire partition.
                (0, partition_len.saturating_sub(1))
            }
        }
    }
}

fn resolve_rows_bounds(
    start: &FrameBound,
    end: &FrameBound,
    current_pos: usize,
    partition_len: usize,
) -> (usize, usize) {
    let s = match start {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(n) => current_pos.saturating_sub(*n as usize),
        FrameBound::CurrentRow => current_pos,
        FrameBound::Following(n) => (current_pos + *n as usize).min(partition_len - 1),
        FrameBound::UnboundedFollowing => partition_len - 1,
    };
    let e = match end {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::Preceding(n) => current_pos.saturating_sub(*n as usize),
        FrameBound::CurrentRow => current_pos,
        FrameBound::Following(n) => (current_pos + *n as usize).min(partition_len - 1),
        FrameBound::UnboundedFollowing => partition_len - 1,
    };
    (s, e)
}

/// Resolve a RANGE frame to concrete start/end positions using the actual
/// values from the ORDER BY column. The `range_value` on Preceding/Following
/// bounds is the delta in the same unit as the ORDER BY column (e.g.
/// nanoseconds for timestamps, raw numeric for numbers).
fn resolve_range_frame(
    rows: &[Vec<Value>],
    sorted_indices: &[usize],
    order_col_idx: usize,
    current_pos: usize,
    start: &FrameBound,
    end: &FrameBound,
) -> (usize, usize) {
    let current_val = value_to_f64(&rows[sorted_indices[current_pos]][order_col_idx]);

    let frame_start = match start {
        FrameBound::UnboundedPreceding => 0,
        FrameBound::CurrentRow => current_pos,
        FrameBound::Preceding(delta) => {
            let lower = current_val - *delta as f64;
            let mut s = current_pos;
            while s > 0 {
                let v = value_to_f64(&rows[sorted_indices[s - 1]][order_col_idx]);
                if v < lower { break; }
                s -= 1;
            }
            s
        }
        FrameBound::Following(delta) => {
            let lower = current_val + *delta as f64;
            let mut s = current_pos;
            while s < sorted_indices.len() - 1 {
                let v = value_to_f64(&rows[sorted_indices[s]][order_col_idx]);
                if v >= lower { break; }
                s += 1;
            }
            s
        }
        FrameBound::UnboundedFollowing => sorted_indices.len() - 1,
    };

    let frame_end = match end {
        FrameBound::UnboundedFollowing => sorted_indices.len() - 1,
        FrameBound::CurrentRow => current_pos,
        FrameBound::Following(delta) => {
            let upper = current_val + *delta as f64;
            let mut e = current_pos;
            while e < sorted_indices.len() - 1 {
                let v = value_to_f64(&rows[sorted_indices[e + 1]][order_col_idx]);
                if v > upper { break; }
                e += 1;
            }
            e
        }
        FrameBound::Preceding(delta) => {
            let upper = current_val - *delta as f64;
            let mut e = current_pos;
            while e > 0 {
                let v = value_to_f64(&rows[sorted_indices[e]][order_col_idx]);
                if v <= upper { break; }
                e -= 1;
            }
            e
        }
        FrameBound::UnboundedPreceding => 0,
    };

    (frame_start, frame_end)
}

fn value_to_f64(val: &Value) -> f64 {
    match val {
        Value::I64(n) => *n as f64,
        Value::F64(f) => *f,
        Value::Timestamp(ns) => *ns as f64,
        _ => 0.0,
    }
}

/// A wrapper around `Value` that implements `Eq` and `Hash` for partitioning.
#[derive(Debug, Clone)]
struct ValueKey(Value);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for ValueKey {}

impl std::hash::Hash for ValueKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Value::Null => {}
            Value::I64(v) => v.hash(state),
            Value::F64(v) => v.to_bits().hash(state),
            Value::Str(s) => s.hash(state),
            Value::Timestamp(ns) => ns.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rows() -> (Vec<String>, Vec<Vec<Value>>) {
        let cols = vec![
            "symbol".to_string(),
            "price".to_string(),
            "volume".to_string(),
            "timestamp".to_string(),
        ];
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0), Value::F64(10.0), Value::I64(1)],
            vec![Value::Str("BTC".into()), Value::F64(105.0), Value::F64(20.0), Value::I64(2)],
            vec![Value::Str("ETH".into()), Value::F64(50.0), Value::F64(30.0), Value::I64(1)],
            vec![Value::Str("BTC".into()), Value::F64(102.0), Value::F64(15.0), Value::I64(3)],
            vec![Value::Str("ETH".into()), Value::F64(55.0), Value::F64(25.0), Value::I64(2)],
        ];
        (cols, rows)
    }

    #[test]
    fn test_row_number_with_partition() {
        let (cols, mut rows) = make_rows();
        let wf = WindowFunction {
            name: "row_number".into(),
            args: vec![],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![OrderBy { column: "timestamp".into(), descending: false }],
                frame: None,
            },
            alias: Some("rn".into()),
        };

        let new_cols = apply_window_functions(&mut rows, &cols, &[wf]);
        assert_eq!(new_cols, vec!["rn"]);

        // BTC rows (indices 0, 1, 3 in original) should get rn 1, 2, 3
        assert_eq!(rows[0].last().unwrap(), &Value::I64(1)); // BTC ts=1
        assert_eq!(rows[1].last().unwrap(), &Value::I64(2)); // BTC ts=2
        assert_eq!(rows[3].last().unwrap(), &Value::I64(3)); // BTC ts=3

        // ETH rows (indices 2, 4) should get rn 1, 2
        assert_eq!(rows[2].last().unwrap(), &Value::I64(1)); // ETH ts=1
        assert_eq!(rows[4].last().unwrap(), &Value::I64(2)); // ETH ts=2
    }

    #[test]
    fn test_rank_with_ties() {
        let cols = vec!["name".to_string(), "score".to_string()];
        let mut rows = vec![
            vec![Value::Str("A".into()), Value::I64(100)],
            vec![Value::Str("B".into()), Value::I64(90)],
            vec![Value::Str("C".into()), Value::I64(100)],
            vec![Value::Str("D".into()), Value::I64(80)],
        ];

        let wf = WindowFunction {
            name: "rank".into(),
            args: vec![],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderBy { column: "score".into(), descending: true }],
                frame: None,
            },
            alias: Some("rnk".into()),
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // Sorted by score DESC: A(100), C(100), B(90), D(80)
        // Ranks: 1, 1, 3, 4
        let a_rank = rows[0].last().unwrap(); // A, score=100
        let b_rank = rows[1].last().unwrap(); // B, score=90
        let c_rank = rows[2].last().unwrap(); // C, score=100
        let d_rank = rows[3].last().unwrap(); // D, score=80

        assert_eq!(a_rank, &Value::I64(1));
        assert_eq!(c_rank, &Value::I64(1)); // tie with A
        assert_eq!(b_rank, &Value::I64(3)); // rank 3 (skips 2)
        assert_eq!(d_rank, &Value::I64(4));
    }

    #[test]
    fn test_dense_rank_with_ties() {
        let cols = vec!["name".to_string(), "score".to_string()];
        let mut rows = vec![
            vec![Value::Str("A".into()), Value::I64(100)],
            vec![Value::Str("B".into()), Value::I64(90)],
            vec![Value::Str("C".into()), Value::I64(100)],
            vec![Value::Str("D".into()), Value::I64(80)],
        ];

        let wf = WindowFunction {
            name: "dense_rank".into(),
            args: vec![],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderBy { column: "score".into(), descending: true }],
                frame: None,
            },
            alias: Some("drnk".into()),
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        let a_rank = rows[0].last().unwrap();
        let b_rank = rows[1].last().unwrap();
        let c_rank = rows[2].last().unwrap();
        let d_rank = rows[3].last().unwrap();

        assert_eq!(a_rank, &Value::I64(1));
        assert_eq!(c_rank, &Value::I64(1)); // tie
        assert_eq!(b_rank, &Value::I64(2)); // dense: no gap
        assert_eq!(d_rank, &Value::I64(3));
    }

    #[test]
    fn test_lag_basic() {
        let (cols, mut rows) = make_rows();
        let wf = WindowFunction {
            name: "lag".into(),
            args: vec![WindowFuncArg::Column("price".into()), WindowFuncArg::LiteralInt(1)],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![OrderBy { column: "timestamp".into(), descending: false }],
                frame: None,
            },
            alias: Some("prev_price".into()),
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // BTC partition sorted by ts: row0(ts=1), row1(ts=2), row3(ts=3)
        assert_eq!(rows[0].last().unwrap(), &Value::Null); // first in partition
        assert_eq!(rows[1].last().unwrap(), &Value::F64(100.0)); // lag from row0
        assert_eq!(rows[3].last().unwrap(), &Value::F64(105.0)); // lag from row1

        // ETH partition sorted by ts: row2(ts=1), row4(ts=2)
        assert_eq!(rows[2].last().unwrap(), &Value::Null); // first in partition
        assert_eq!(rows[4].last().unwrap(), &Value::F64(50.0)); // lag from row2
    }

    #[test]
    fn test_lag_with_default() {
        let cols = vec!["val".to_string()];
        let mut rows = vec![
            vec![Value::I64(10)],
            vec![Value::I64(20)],
        ];

        let wf = WindowFunction {
            name: "lag".into(),
            args: vec![
                WindowFuncArg::Column("val".into()),
                WindowFuncArg::LiteralInt(1),
                WindowFuncArg::LiteralInt(0),
            ],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            },
            alias: None,
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        assert_eq!(rows[0].last().unwrap(), &Value::I64(0)); // default
        assert_eq!(rows[1].last().unwrap(), &Value::I64(10));
    }

    #[test]
    fn test_lead_basic() {
        let (cols, mut rows) = make_rows();
        let wf = WindowFunction {
            name: "lead".into(),
            args: vec![WindowFuncArg::Column("price".into()), WindowFuncArg::LiteralInt(1)],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![OrderBy { column: "timestamp".into(), descending: false }],
                frame: None,
            },
            alias: Some("next_price".into()),
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // BTC partition sorted by ts: row0(ts=1,p=100), row1(ts=2,p=105), row3(ts=3,p=102)
        assert_eq!(rows[0].last().unwrap(), &Value::F64(105.0)); // lead to row1
        assert_eq!(rows[1].last().unwrap(), &Value::F64(102.0)); // lead to row3
        assert_eq!(rows[3].last().unwrap(), &Value::Null); // last in partition

        // ETH partition sorted by ts: row2(ts=1,p=50), row4(ts=2,p=55)
        assert_eq!(rows[2].last().unwrap(), &Value::F64(55.0));
        assert_eq!(rows[4].last().unwrap(), &Value::Null);
    }

    #[test]
    fn test_cumulative_sum() {
        let (cols, mut rows) = make_rows();
        let wf = WindowFunction {
            name: "sum".into(),
            args: vec![WindowFuncArg::Column("volume".into())],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![OrderBy { column: "timestamp".into(), descending: false }],
                frame: Some(WindowFrame::Rows {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::CurrentRow,
                }),
            },
            alias: Some("cumulative_vol".into()),
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // BTC partition: vol=10, 20, 15 -> cumulative: 10, 30, 45
        assert_eq!(rows[0].last().unwrap(), &Value::F64(10.0));
        assert_eq!(rows[1].last().unwrap(), &Value::F64(30.0));
        assert_eq!(rows[3].last().unwrap(), &Value::F64(45.0));

        // ETH partition: vol=30, 25 -> cumulative: 30, 55
        assert_eq!(rows[2].last().unwrap(), &Value::F64(30.0));
        assert_eq!(rows[4].last().unwrap(), &Value::F64(55.0));
    }

    #[test]
    fn test_multiple_window_functions() {
        let (cols, mut rows) = make_rows();
        let wf1 = WindowFunction {
            name: "row_number".into(),
            args: vec![],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![OrderBy { column: "timestamp".into(), descending: false }],
                frame: None,
            },
            alias: Some("rn".into()),
        };
        let wf2 = WindowFunction {
            name: "sum".into(),
            args: vec![WindowFuncArg::Column("volume".into())],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![OrderBy { column: "timestamp".into(), descending: false }],
                frame: Some(WindowFrame::Rows {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::CurrentRow,
                }),
            },
            alias: Some("cumvol".into()),
        };

        let new_cols = apply_window_functions(&mut rows, &cols, &[wf1, wf2]);
        assert_eq!(new_cols, vec!["rn", "cumvol"]);

        // Each row should have 6 values: 4 original + 2 window columns
        assert_eq!(rows[0].len(), 6);

        // BTC row0: rn=1, cumvol=10
        assert_eq!(rows[0][4], Value::I64(1));
        assert_eq!(rows[0][5], Value::F64(10.0));

        // BTC row1: rn=2, cumvol=30
        assert_eq!(rows[1][4], Value::I64(2));
        assert_eq!(rows[1][5], Value::F64(30.0));
    }

    #[test]
    fn test_first_value() {
        let cols = vec!["val".to_string()];
        let mut rows = vec![
            vec![Value::I64(10)],
            vec![Value::I64(20)],
            vec![Value::I64(30)],
        ];

        let wf = WindowFunction {
            name: "first_value".into(),
            args: vec![WindowFuncArg::Column("val".into())],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            },
            alias: None,
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // Default frame is UNBOUNDED PRECEDING to CURRENT ROW, first_value always = first
        assert_eq!(rows[0].last().unwrap(), &Value::I64(10));
        assert_eq!(rows[1].last().unwrap(), &Value::I64(10));
        assert_eq!(rows[2].last().unwrap(), &Value::I64(10));
    }

    #[test]
    fn test_last_value_with_frame() {
        let cols = vec!["val".to_string()];
        let mut rows = vec![
            vec![Value::I64(10)],
            vec![Value::I64(20)],
            vec![Value::I64(30)],
        ];

        let wf = WindowFunction {
            name: "last_value".into(),
            args: vec![WindowFuncArg::Column("val".into())],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: Some(WindowFrame::Rows {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::UnboundedFollowing,
                }),
            },
            alias: None,
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // With unbounded frame, last_value is always the last row
        assert_eq!(rows[0].last().unwrap(), &Value::I64(30));
        assert_eq!(rows[1].last().unwrap(), &Value::I64(30));
        assert_eq!(rows[2].last().unwrap(), &Value::I64(30));
    }

    #[test]
    fn test_count_window() {
        let cols = vec!["symbol".to_string(), "val".to_string()];
        let mut rows = vec![
            vec![Value::Str("A".into()), Value::I64(1)],
            vec![Value::Str("A".into()), Value::I64(2)],
            vec![Value::Str("B".into()), Value::I64(3)],
            vec![Value::Str("A".into()), Value::I64(4)],
        ];

        let wf = WindowFunction {
            name: "count".into(),
            args: vec![WindowFuncArg::Wildcard],
            over: WindowSpec {
                partition_by: vec!["symbol".into()],
                order_by: vec![],
                frame: None,
            },
            alias: Some("cnt".into()),
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // Without ORDER BY, default frame is entire partition (SQL standard).
        // A partition has 3 rows → count = 3 for all A rows.
        // B partition has 1 row → count = 1.
        assert_eq!(rows[0].last().unwrap(), &Value::I64(3)); // A
        assert_eq!(rows[1].last().unwrap(), &Value::I64(3)); // A
        assert_eq!(rows[3].last().unwrap(), &Value::I64(3)); // A
        assert_eq!(rows[2].last().unwrap(), &Value::I64(1)); // B
    }

    #[test]
    fn test_avg_window() {
        let cols = vec!["val".to_string()];
        let mut rows = vec![
            vec![Value::F64(10.0)],
            vec![Value::F64(20.0)],
            vec![Value::F64(30.0)],
        ];

        let wf = WindowFunction {
            name: "avg".into(),
            args: vec![WindowFuncArg::Column("val".into())],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: Some(WindowFrame::Rows {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::CurrentRow,
                }),
            },
            alias: None,
        };

        let _ = apply_window_functions(&mut rows, &cols, &[wf]);

        // Running average: 10, 15, 20
        assert_eq!(rows[0].last().unwrap(), &Value::F64(10.0));
        assert_eq!(rows[1].last().unwrap(), &Value::F64(15.0));
        assert_eq!(rows[2].last().unwrap(), &Value::F64(20.0));
    }
}
