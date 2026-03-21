//! Standard JOIN execution (INNER, LEFT, RIGHT, FULL OUTER, CROSS) using hash join.

use crate::executor::{apply_order_by, evaluate_filter_virtual, resolve_columns};
use crate::plan::*;
use exchange_common::error::{ExchangeDbError, Result};
use std::collections::HashMap;
use std::path::Path;

/// Execute a standard JOIN between two tables.
pub fn execute_join(
    db_root: &Path,
    left_table: &str,
    right_table: &str,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
    columns: &[JoinSelectColumn],
    join_type: JoinType,
    on_columns: &[(String, String)],
    filter: Option<&Filter>,
    order_by: &[OrderBy],
    limit: Option<u64>,
) -> Result<QueryResult> {
    let left_dir = db_root.join(left_table);
    if !left_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(left_table.to_string()));
    }
    let right_dir = db_root.join(right_table);
    if !right_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(right_table.to_string()));
    }

    let left_meta = exchange_core::table::TableMeta::load(&left_dir.join("_meta"))?;
    let right_meta = exchange_core::table::TableMeta::load(&right_dir.join("_meta"))?;

    let left_label = left_alias.unwrap_or(left_table);
    let right_label = right_alias.unwrap_or(right_table);

    // Read all columns from both sides.
    let left_resolved = resolve_columns(&left_meta, &[SelectColumn::Wildcard])?;
    let right_resolved = resolve_columns(&right_meta, &[SelectColumn::Wildcard])?;

    let left_rows =
        crate::parallel::parallel_scan_partitions(&left_dir, &left_meta, &left_resolved, None)?;
    let right_rows =
        crate::parallel::parallel_scan_partitions(&right_dir, &right_meta, &right_resolved, None)?;

    // Handle NATURAL JOIN: auto-detect common columns between the two tables.
    let resolved_on_columns: Vec<(String, String)>;
    let on_columns = if on_columns.len() == 1
        && on_columns[0].0 == "__natural__"
        && on_columns[0].1 == "__natural__"
    {
        // Find common column names between left and right tables.
        let left_names: Vec<&str> = left_resolved.iter().map(|s| s.1.as_str()).collect();
        let right_names: Vec<&str> = right_resolved.iter().map(|s| s.1.as_str()).collect();
        resolved_on_columns = left_names
            .iter()
            .filter(|n| right_names.contains(n))
            .map(|n| (n.to_string(), n.to_string()))
            .collect();
        if resolved_on_columns.is_empty() {
            return Err(ExchangeDbError::Query(
                "NATURAL JOIN found no common columns between the two tables".into(),
            ));
        }
        &resolved_on_columns
    } else {
        on_columns
    };

    // Resolve ON column positions. Column refs may be "alias.col" or just "col".
    let on_pairs: Vec<(usize, usize)> = on_columns
        .iter()
        .map(|(lc, rc)| {
            let li = resolve_on_col(lc, left_table, left_alias, &left_resolved)?;
            let ri = resolve_on_col(rc, right_table, right_alias, &right_resolved)?;
            Ok((li, ri))
        })
        .collect::<Result<Vec<_>>>()?;

    // Perform the join based on type.
    let left_col_count = left_resolved.len();
    let right_col_count = right_resolved.len();

    let result_rows: Vec<Vec<Value>> = if join_type == JoinType::Cross {
        // Cartesian product — no ON columns needed.
        let mut rows = Vec::with_capacity(left_rows.len() * right_rows.len());
        for left_row in &left_rows {
            for right_row in &right_rows {
                let mut combined = left_row.clone();
                combined.extend(right_row.iter().cloned());
                rows.push(combined);
            }
        }
        rows
    } else {
        // Hash join: build hash map on right side, probe with left.
        let mut right_map: HashMap<Vec<HashableValue>, Vec<usize>> = HashMap::new();
        for (i, row) in right_rows.iter().enumerate() {
            let key: Vec<HashableValue> = on_pairs.iter().map(|(_, ri)| HashableValue::from(&row[*ri])).collect();
            right_map.entry(key).or_default().push(i);
        }

        let mut rows: Vec<Vec<Value>> = Vec::new();
        // Track which right rows were matched (for RIGHT and FULL OUTER).
        let mut right_matched = if join_type == JoinType::Right || join_type == JoinType::FullOuter {
            vec![false; right_rows.len()]
        } else {
            Vec::new()
        };

        for left_row in &left_rows {
            let key: Vec<HashableValue> = on_pairs.iter().map(|(li, _)| HashableValue::from(&left_row[*li])).collect();
            if let Some(right_indices) = right_map.get(&key) {
                for &ri in right_indices {
                    let mut combined = left_row.clone();
                    combined.extend(right_rows[ri].iter().cloned());
                    rows.push(combined);
                    if !right_matched.is_empty() {
                        right_matched[ri] = true;
                    }
                }
            } else if join_type == JoinType::Left || join_type == JoinType::FullOuter {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat(Value::Null).take(right_col_count));
                rows.push(combined);
            }
        }

        // For RIGHT JOIN and FULL OUTER JOIN: emit unmatched right rows.
        if join_type == JoinType::Right || join_type == JoinType::FullOuter {
            for (i, matched) in right_matched.iter().enumerate() {
                if !matched {
                    let mut combined: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                    combined.extend(right_rows[i].iter().cloned());
                    rows.push(combined);
                }
            }
        }

        rows
    };

    // Build combined column names for resolution.
    let mut all_resolved: Vec<(usize, String)> = left_resolved
        .iter()
        .enumerate()
        .map(|(i, (_, n))| (i, format!("{left_label}.{n}")))
        .collect();
    let left_count = left_resolved.len();
    all_resolved.extend(
        right_resolved
            .iter()
            .enumerate()
            .map(|(i, (_, n))| (left_count + i, format!("{right_label}.{n}"))),
    );

    // Apply WHERE filter on the combined rows.
    // Build column name list including both qualified ("t.price") and bare ("price") forms
    // so that filters referencing either form can resolve correctly.
    let result_rows = if let Some(f) = filter {
        let filter_col_names: Vec<String> = all_resolved.iter().map(|(_, n)| n.clone()).collect();
        result_rows.into_iter().filter(|row| {
            evaluate_join_filter(f, row, &filter_col_names)
        }).collect()
    } else {
        result_rows
    };

    // Project output columns.
    let (col_names, mut output_rows) = project_join_columns(
        columns,
        &all_resolved,
        &result_rows,
        left_label,
        right_label,
        &left_resolved,
        &right_resolved,
    )?;

    // ORDER BY
    if !order_by.is_empty() {
        let fake_resolved: Vec<(usize, String)> = col_names
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n.clone()))
            .collect();
        apply_order_by(&mut output_rows, &fake_resolved, order_by);
    }

    // LIMIT
    if let Some(lim) = limit {
        output_rows.truncate(lim as usize);
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: output_rows,
    })
}

/// Execute a multi-table JOIN where the left side is a sub-plan (another join result).
pub fn execute_multi_join(
    db_root: &Path,
    left_plan: &QueryPlan,
    right_table: &str,
    right_alias: Option<&str>,
    columns: &[JoinSelectColumn],
    join_type: JoinType,
    on_columns: &[(String, String)],
    filter: Option<&Filter>,
    order_by: &[OrderBy],
    limit: Option<u64>,
) -> Result<QueryResult> {
    // Execute the left sub-plan to get its result.
    let left_result = crate::executor::execute(db_root, left_plan)?;
    let (left_col_names, left_rows) = match left_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => return Err(ExchangeDbError::Query("left side of multi-join did not produce rows".into())),
    };

    // Read right table.
    let right_dir = db_root.join(right_table);
    if !right_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(right_table.to_string()));
    }
    let right_meta = exchange_core::table::TableMeta::load(&right_dir.join("_meta"))?;
    let right_label = right_alias.unwrap_or(right_table);
    let right_resolved = resolve_columns(&right_meta, &[SelectColumn::Wildcard])?;
    let right_rows = crate::parallel::parallel_scan_partitions(&right_dir, &right_meta, &right_resolved, None)?;

    // Handle NATURAL JOIN: auto-detect common columns.
    let resolved_on_columns: Vec<(String, String)>;
    let on_columns = if on_columns.len() == 1
        && on_columns[0].0 == "__natural__"
        && on_columns[0].1 == "__natural__"
    {
        let right_col_names: Vec<String> = right_resolved.iter().map(|(_, name)| name.clone()).collect();
        resolved_on_columns = left_col_names
            .iter()
            .filter(|n| right_col_names.contains(n))
            .map(|n| (n.clone(), n.clone()))
            .collect();
        if resolved_on_columns.is_empty() {
            return Err(ExchangeDbError::Query(
                "NATURAL JOIN found no common columns between the two tables".into(),
            ));
        }
        &resolved_on_columns
    } else {
        on_columns
    };

    // Resolve ON column positions.
    let on_pairs: Vec<(usize, usize)> = on_columns
        .iter()
        .map(|(lc, rc)| {
            // Left column: search by name in left_col_names (may be "alias.col" or just "col").
            let li = resolve_on_col_virtual(lc, &left_col_names)?;
            // Right column: search by name in right_resolved.
            let ri = resolve_on_col(rc, right_table, right_alias, &right_resolved)?;
            Ok((li, ri))
        })
        .collect::<Result<Vec<_>>>()?;

    let left_col_count = left_col_names.len();
    let right_col_count = right_resolved.len();

    let result_rows: Vec<Vec<Value>> = if join_type == JoinType::Cross {
        let mut rows = Vec::with_capacity(left_rows.len() * right_rows.len());
        for left_row in &left_rows {
            for right_row in &right_rows {
                let mut combined = left_row.clone();
                combined.extend(right_row.iter().cloned());
                rows.push(combined);
            }
        }
        rows
    } else {
        let mut right_map: HashMap<Vec<HashableValue>, Vec<usize>> = HashMap::new();
        for (i, row) in right_rows.iter().enumerate() {
            let key: Vec<HashableValue> = on_pairs.iter().map(|(_, ri)| HashableValue::from(&row[*ri])).collect();
            right_map.entry(key).or_default().push(i);
        }
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut right_matched = if join_type == JoinType::Right || join_type == JoinType::FullOuter {
            vec![false; right_rows.len()]
        } else { Vec::new() };

        for left_row in &left_rows {
            let key: Vec<HashableValue> = on_pairs.iter().map(|(li, _)| HashableValue::from(&left_row[*li])).collect();
            if let Some(right_indices) = right_map.get(&key) {
                for &ri in right_indices {
                    let mut combined = left_row.clone();
                    combined.extend(right_rows[ri].iter().cloned());
                    rows.push(combined);
                    if !right_matched.is_empty() { right_matched[ri] = true; }
                }
            } else if join_type == JoinType::Left || join_type == JoinType::FullOuter {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat(Value::Null).take(right_col_count));
                rows.push(combined);
            }
        }

        if join_type == JoinType::Right || join_type == JoinType::FullOuter {
            for (i, matched) in right_matched.iter().enumerate() {
                if !matched {
                    let mut combined: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                    combined.extend(right_rows[i].iter().cloned());
                    rows.push(combined);
                }
            }
        }
        rows
    };

    // Build combined column names: left columns keep their names, right columns get "right_label.col".
    let mut all_resolved: Vec<(usize, String)> = left_col_names
        .iter()
        .enumerate()
        .map(|(i, n)| (i, n.clone()))
        .collect();
    all_resolved.extend(
        right_resolved.iter().enumerate()
            .map(|(i, (_, n))| (left_col_count + i, format!("{right_label}.{n}")))
    );

    // Apply WHERE filter on the combined rows.
    let result_rows = if let Some(f) = filter {
        let filter_col_names: Vec<String> = all_resolved.iter().map(|(_, n)| n.clone()).collect();
        result_rows.into_iter().filter(|row| {
            evaluate_join_filter(f, row, &filter_col_names)
        }).collect()
    } else {
        result_rows
    };

    // For project_join_columns we need left_resolved and right_resolved.
    // Since left came from a sub-query, its "resolved" is just column names.
    let _left_resolved_fake: Vec<(usize, String)> = left_col_names.iter().enumerate().map(|(i, n)| (i, n.clone())).collect();
    let _left_label = ""; // multi-join left has no single label

    let (col_names, mut output_rows) = project_join_columns_multi(
        columns, &all_resolved, &result_rows, &left_col_names, right_label, &right_resolved,
    )?;

    if !order_by.is_empty() {
        let fake_resolved: Vec<(usize, String)> = col_names.iter().enumerate().map(|(i, n)| (i, n.clone())).collect();
        apply_order_by(&mut output_rows, &fake_resolved, order_by);
    }
    if let Some(lim) = limit {
        output_rows.truncate(lim as usize);
    }
    Ok(QueryResult::Rows { columns: col_names, rows: output_rows })
}

/// Resolve a column reference in a virtual result set (from a sub-query).
fn resolve_on_col_virtual(col_ref: &str, col_names: &[String]) -> Result<usize> {
    // Try exact match first.
    if let Some(i) = col_names.iter().position(|n| n == col_ref) {
        return Ok(i);
    }
    // Try suffix match: "alias.col" matches "...alias.col" or just "col".
    if let Some(dot_pos) = col_ref.find('.') {
        let col_name = &col_ref[dot_pos + 1..];
        if let Some(i) = col_names.iter().position(|n| n.ends_with(&format!(".{col_name}")) || n == col_name) {
            return Ok(i);
        }
    }
    Err(ExchangeDbError::Query(format!("multi-join column not found: {col_ref}")))
}

/// Project output columns for multi-join results.
fn project_join_columns_multi(
    columns: &[JoinSelectColumn],
    all_resolved: &[(usize, String)],
    rows: &[Vec<Value>],
    left_col_names: &[String],
    right_label: &str,
    right_resolved: &[(usize, String)],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut output_indices: Vec<usize> = Vec::new();
    let mut output_names: Vec<String> = Vec::new();
    let left_count = left_col_names.len();

    for col in columns {
        match col {
            JoinSelectColumn::Wildcard => {
                for (i, (_, name)) in all_resolved.iter().enumerate() {
                    output_indices.push(i);
                    output_names.push(name.clone());
                }
            }
            JoinSelectColumn::QualifiedWildcard(table) => {
                if table == right_label {
                    for i in 0..right_resolved.len() {
                        let idx = left_count + i;
                        output_indices.push(idx);
                        output_names.push(all_resolved[idx].1.clone());
                    }
                } else {
                    // Search left columns for matching prefix.
                    for (i, name) in left_col_names.iter().enumerate() {
                        if name.starts_with(&format!("{table}.")) || name == table {
                            output_indices.push(i);
                            output_names.push(name.clone());
                        }
                    }
                }
            }
            JoinSelectColumn::Qualified(table, col_name) => {
                let prefixed = format!("{table}.{col_name}");
                if let Some(i) = all_resolved.iter().position(|(_, n)| *n == prefixed) {
                    output_indices.push(i);
                    output_names.push(col_name.clone());
                } else {
                    return Err(ExchangeDbError::Query(format!("column {table}.{col_name} not found in multi-join result")));
                }
            }
            JoinSelectColumn::Unqualified(col_name) => {
                if let Some(i) = all_resolved.iter().position(|(_, n)| n.ends_with(&format!(".{col_name}")) || n == col_name) {
                    output_indices.push(i);
                    output_names.push(col_name.clone());
                } else {
                    return Err(ExchangeDbError::Query(format!("column {col_name} not found in multi-join result")));
                }
            }
            JoinSelectColumn::QualifiedAlias(table, col_name, alias) => {
                let prefixed = if table.is_empty() { col_name.clone() } else { format!("{table}.{col_name}") };
                if let Some(i) = all_resolved.iter().position(|(_, n)| *n == prefixed || n.ends_with(&format!(".{col_name}"))) {
                    output_indices.push(i);
                    output_names.push(alias.clone());
                } else {
                    return Err(ExchangeDbError::Query(format!("column {prefixed} not found in multi-join result")));
                }
            }
            JoinSelectColumn::Expression { .. } | JoinSelectColumn::Aggregate { .. } | JoinSelectColumn::CaseWhen { .. } => {
                output_indices.push(0); // placeholder
                output_names.push("expr".to_string());
            }
        }
    }

    let output_rows: Vec<Vec<Value>> = rows.iter().map(|row| {
        output_indices.iter().map(|&i| row.get(i).cloned().unwrap_or(Value::Null)).collect()
    }).collect();

    Ok((output_names, output_rows))
}

/// Resolve a column reference that may be "alias.col" or just "col" to an
/// index in the resolved column list.
fn resolve_on_col(
    col_ref: &str,
    table_name: &str,
    table_alias: Option<&str>,
    resolved: &[(usize, String)],
) -> Result<usize> {
    if let Some(dot_pos) = col_ref.find('.') {
        let prefix = &col_ref[..dot_pos];
        let col_name = &col_ref[dot_pos + 1..];
        if prefix == table_name || table_alias.map(|a| a == prefix).unwrap_or(false) {
            resolved
                .iter()
                .position(|(_, n)| n == col_name)
                .ok_or_else(|| ExchangeDbError::Query(format!("JOIN column not found: {col_ref}")))
        } else {
            Err(ExchangeDbError::Query(format!(
                "JOIN column prefix does not match table: {col_ref}"
            )))
        }
    } else {
        resolved
            .iter()
            .position(|(_, n)| n == col_ref)
            .ok_or_else(|| ExchangeDbError::Query(format!("JOIN column not found: {col_ref}")))
    }
}

/// A hashable wrapper for Value used in join keys.
///
/// Numeric types (I64, F64, Timestamp) are normalized to a common
/// representation so that cross-type joins (e.g. INT vs DOUBLE) work
/// correctly when the values are numerically equal.
#[derive(Debug, Clone)]
enum HashableValue {
    Null,
    /// Numeric value stored as f64 bits for cross-type coercion.
    /// Both I64 and F64 values are stored here so they hash/compare equally.
    Numeric(u64),
    Str(String),
    Timestamp(i64),
}

impl From<&Value> for HashableValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => HashableValue::Null,
            Value::I64(n) => HashableValue::Numeric((*n as f64).to_bits()),
            Value::F64(f) => HashableValue::Numeric(f.to_bits()),
            Value::Str(s) => HashableValue::Str(s.clone()),
            Value::Timestamp(ns) => HashableValue::Timestamp(*ns),
        }
    }
}

impl PartialEq for HashableValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (HashableValue::Null, HashableValue::Null) => true,
            (HashableValue::Numeric(a), HashableValue::Numeric(b)) => a == b,
            (HashableValue::Str(a), HashableValue::Str(b)) => a == b,
            (HashableValue::Timestamp(a), HashableValue::Timestamp(b)) => a == b,
            // Cross-type: Timestamp vs Numeric (both are numeric-like).
            (HashableValue::Timestamp(a), HashableValue::Numeric(b)) => {
                (*a as f64).to_bits() == *b
            }
            (HashableValue::Numeric(a), HashableValue::Timestamp(b)) => {
                *a == (*b as f64).to_bits()
            }
            _ => false,
        }
    }
}

impl Eq for HashableValue {}

impl std::hash::Hash for HashableValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            HashableValue::Null => {
                0u8.hash(state);
            }
            HashableValue::Numeric(v) => {
                1u8.hash(state);
                v.hash(state);
            }
            HashableValue::Str(s) => {
                2u8.hash(state);
                s.hash(state);
            }
            HashableValue::Timestamp(ns) => {
                // Hash timestamps as Numeric so they can match I64/F64 keys.
                1u8.hash(state);
                (*ns as f64).to_bits().hash(state);
            }
        }
    }
}

/// Evaluate a filter against a combined join row. Column names in the row are
/// qualified ("alias.col"), but the filter may reference bare names ("col").
/// We try exact match first, then fall back to suffix match (".col").
fn evaluate_join_filter(filter: &Filter, row: &[Value], col_names: &[String]) -> bool {
    // Use evaluate_filter_virtual but with a wrapper for PlanExpr-based filters
    // that does suffix matching. For simple column-based filters, also do suffix matching.
    match filter {
        Filter::Eq(col, expected) => {
            get_join_value(col, row, col_names).as_ref().map(|v| v.eq_coerce(expected)).unwrap_or(false)
        }
        Filter::Gt(col, expected) => {
            get_join_value(col, row, col_names).as_ref().map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater)).unwrap_or(false)
        }
        Filter::Lt(col, expected) => {
            get_join_value(col, row, col_names).as_ref().map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less)).unwrap_or(false)
        }
        Filter::Gte(col, expected) => {
            get_join_value(col, row, col_names).as_ref().map(|v| matches!(v.cmp_coerce(expected), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))).unwrap_or(false)
        }
        Filter::Lte(col, expected) => {
            get_join_value(col, row, col_names).as_ref().map(|v| matches!(v.cmp_coerce(expected), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))).unwrap_or(false)
        }
        Filter::And(parts) => parts.iter().all(|p| evaluate_join_filter(p, row, col_names)),
        Filter::Or(parts) => parts.iter().any(|p| evaluate_join_filter(p, row, col_names)),
        Filter::IsNull(col) => {
            matches!(get_join_value(col, row, col_names).as_ref(), None | Some(Value::Null))
        }
        Filter::IsNotNull(col) => {
            matches!(get_join_value(col, row, col_names).as_ref(), Some(v) if *v != Value::Null)
        }
        Filter::In(col, list) => {
            get_join_value(col, row, col_names).as_ref().map(|v| list.iter().any(|item| v.eq_coerce(item))).unwrap_or(false)
        }
        Filter::NotIn(col, list) => {
            get_join_value(col, row, col_names).as_ref().map(|v| !list.iter().any(|item| v.eq_coerce(item))).unwrap_or(true)
        }
        Filter::Between(col, low, high) => {
            get_join_value(col, row, col_names)
                .as_ref()
                .map(|v| matches!(v.cmp_coerce(low), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                      && matches!(v.cmp_coerce(high), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)))
                .unwrap_or(false)
        }
        Filter::Like(col, pattern) => {
            get_join_value(col, row, col_names).as_ref().map(|v| if let Value::Str(s) = v { crate::executor::like_match(s, pattern, false) } else { false }).unwrap_or(false)
        }
        Filter::Expression { left, op, right } => {
            let lv = evaluate_plan_expr_join(left, row, col_names);
            let rv = evaluate_plan_expr_join(right, row, col_names);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(lv.cmp_coerce(&rv), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
                CompareOp::Lte => matches!(lv.cmp_coerce(&rv), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
            }
        }
        _ => evaluate_filter_virtual(filter, row, col_names),
    }
}

/// Look up a column value in a join row, trying exact match first then suffix match.
fn get_join_value(col: &str, row: &[Value], col_names: &[String]) -> Option<Value> {
    // Exact match.
    if let Some(idx) = col_names.iter().position(|n| n == col) {
        return row.get(idx).cloned();
    }
    // Suffix match: "price" matches "t.price".
    let suffix = format!(".{col}");
    if let Some(idx) = col_names.iter().position(|n| n.ends_with(&suffix)) {
        return row.get(idx).cloned();
    }
    None
}

/// Evaluate a PlanExpr against a join row with suffix-matching column lookups.
fn evaluate_plan_expr_join(expr: &PlanExpr, row: &[Value], col_names: &[String]) -> Value {
    match expr {
        PlanExpr::Column(name) => get_join_value(name, row, col_names).unwrap_or(Value::Null),
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => crate::executor::apply_binary_op(
            &evaluate_plan_expr_join(left, row, col_names),
            *op,
            &evaluate_plan_expr_join(right, row, col_names),
        ),
        PlanExpr::UnaryOp { op, expr } => crate::executor::apply_unary_op(*op, &evaluate_plan_expr_join(expr, row, col_names)),
        PlanExpr::Function { name, args } => {
            let func_args: Vec<Value> = args.iter().map(|a| evaluate_plan_expr_join(a, row, col_names)).collect();
            crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null)
        }
    }
}

fn project_join_columns(
    columns: &[JoinSelectColumn],
    all_resolved: &[(usize, String)],
    rows: &[Vec<Value>],
    left_label: &str,
    right_label: &str,
    left_resolved: &[(usize, String)],
    right_resolved: &[(usize, String)],
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    // Each output column is either an index into the combined row or a computed expression.
    enum OutputCol {
        Index(usize),
        Expr(PlanExpr),
    }
    let mut output_cols: Vec<OutputCol> = Vec::new();
    let mut output_names: Vec<String> = Vec::new();
    let left_count = left_resolved.len();
    let col_names: Vec<String> = all_resolved.iter().map(|(_, n)| n.clone()).collect();

    for col in columns {
        match col {
            JoinSelectColumn::Wildcard => {
                for (i, (_, name)) in all_resolved.iter().enumerate() {
                    output_cols.push(OutputCol::Index(i));
                    output_names.push(name.clone());
                }
            }
            JoinSelectColumn::QualifiedWildcard(table) => {
                if table == left_label {
                    for i in 0..left_count {
                        output_cols.push(OutputCol::Index(i));
                        output_names.push(all_resolved[i].1.clone());
                    }
                } else if table == right_label {
                    for i in 0..right_resolved.len() {
                        let idx = left_count + i;
                        output_cols.push(OutputCol::Index(idx));
                        output_names.push(all_resolved[idx].1.clone());
                    }
                }
            }
            JoinSelectColumn::Qualified(table, col_name) => {
                let prefixed = format!("{table}.{col_name}");
                if let Some(i) = all_resolved.iter().position(|(_, n)| *n == prefixed) {
                    output_cols.push(OutputCol::Index(i));
                    output_names.push(col_name.clone());
                } else {
                    return Err(ExchangeDbError::Query(format!(
                        "column {table}.{col_name} not found in join result"
                    )));
                }
            }
            JoinSelectColumn::QualifiedAlias(table, col_name, alias) => {
                if table.is_empty() {
                    // Unqualified with alias
                    if let Some(i) = all_resolved.iter().position(|(_, n)| n.ends_with(&format!(".{col_name}"))) {
                        output_cols.push(OutputCol::Index(i));
                    } else {
                        return Err(ExchangeDbError::Query(format!("column {col_name} not found in join result")));
                    }
                } else {
                    let prefixed = format!("{table}.{col_name}");
                    if let Some(i) = all_resolved.iter().position(|(_, n)| *n == prefixed) {
                        output_cols.push(OutputCol::Index(i));
                    } else {
                        return Err(ExchangeDbError::Query(format!("column {table}.{col_name} not found in join result")));
                    }
                }
                output_names.push(alias.clone());
            }
            JoinSelectColumn::Unqualified(col_name) => {
                if let Some(i) = all_resolved
                    .iter()
                    .position(|(_, n)| n.ends_with(&format!(".{col_name}")))
                {
                    output_cols.push(OutputCol::Index(i));
                    output_names.push(col_name.clone());
                } else {
                    return Err(ExchangeDbError::Query(format!(
                        "column {col_name} not found in join result"
                    )));
                }
            }
            JoinSelectColumn::Expression { expr, alias } => {
                output_cols.push(OutputCol::Expr(expr.clone()));
                output_names.push(alias.clone().unwrap_or_else(|| "expr".to_string()));
            }
            JoinSelectColumn::Aggregate { function, column, alias, .. } => {
                // Aggregates in JOIN are handled during GROUP BY/HAVING; pass through as placeholder.
                let func_name = format!("{function:?}").to_ascii_lowercase();
                output_names.push(alias.clone().unwrap_or_else(|| format!("{func_name}({column})")));
                output_cols.push(OutputCol::Index(0)); // placeholder
            }
            JoinSelectColumn::CaseWhen { alias, .. } => {
                output_names.push(alias.clone().unwrap_or_else(|| "case".to_string()));
                output_cols.push(OutputCol::Index(0)); // placeholder
            }
        }
    }

    let output_rows: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            output_cols
                .iter()
                .map(|oc| match oc {
                    OutputCol::Index(i) => row.get(*i).cloned().unwrap_or(Value::Null),
                    OutputCol::Expr(expr) => crate::executor::evaluate_plan_expr_by_name(expr, row, &col_names),
                })
                .collect()
        })
        .collect();

    Ok((output_names, output_rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_inner_join(
        left: &[Vec<Value>],
        right: &[Vec<Value>],
        left_keys: &[usize],
        right_keys: &[usize],
    ) -> Vec<Vec<Value>> {
        let mut right_map: HashMap<Vec<HashableValue>, Vec<usize>> = HashMap::new();
        for (i, row) in right.iter().enumerate() {
            let key: Vec<HashableValue> = right_keys.iter().map(|&k| HashableValue::from(&row[k])).collect();
            right_map.entry(key).or_default().push(i);
        }
        let mut result = Vec::new();
        for left_row in left {
            let key: Vec<HashableValue> = left_keys.iter().map(|&k| HashableValue::from(&left_row[k])).collect();
            if let Some(indices) = right_map.get(&key) {
                for &ri in indices {
                    let mut combined = left_row.clone();
                    combined.extend(right[ri].iter().cloned());
                    result.push(combined);
                }
            }
        }
        result
    }

    fn hash_left_join(
        left: &[Vec<Value>],
        right: &[Vec<Value>],
        left_keys: &[usize],
        right_keys: &[usize],
        right_col_count: usize,
    ) -> Vec<Vec<Value>> {
        let mut right_map: HashMap<Vec<HashableValue>, Vec<usize>> = HashMap::new();
        for (i, row) in right.iter().enumerate() {
            let key: Vec<HashableValue> = right_keys.iter().map(|&k| HashableValue::from(&row[k])).collect();
            right_map.entry(key).or_default().push(i);
        }
        let mut result = Vec::new();
        for left_row in left {
            let key: Vec<HashableValue> = left_keys.iter().map(|&k| HashableValue::from(&left_row[k])).collect();
            if let Some(indices) = right_map.get(&key) {
                for &ri in indices {
                    let mut combined = left_row.clone();
                    combined.extend(right[ri].iter().cloned());
                    result.push(combined);
                }
            } else {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat(Value::Null).take(right_col_count));
                result.push(combined);
            }
        }
        result
    }

    #[test]
    fn test_inner_join() {
        let left = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("ETH".into()), Value::F64(50.0)],
            vec![Value::Str("SOL".into()), Value::F64(25.0)],
        ];
        let right = vec![
            vec![Value::Str("BTC".into()), Value::Str("Bitcoin".into())],
            vec![Value::Str("ETH".into()), Value::Str("Ethereum".into())],
        ];

        let result = hash_inner_join(&left, &right, &[0], &[0]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], Value::Str("BTC".into()));
        assert_eq!(result[0][3], Value::Str("Bitcoin".into()));
        assert_eq!(result[1][0], Value::Str("ETH".into()));
        assert_eq!(result[1][3], Value::Str("Ethereum".into()));
    }

    #[test]
    fn test_left_join() {
        let left = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("ETH".into()), Value::F64(50.0)],
            vec![Value::Str("SOL".into()), Value::F64(25.0)],
        ];
        let right = vec![
            vec![Value::Str("BTC".into()), Value::Str("Bitcoin".into())],
            vec![Value::Str("ETH".into()), Value::Str("Ethereum".into())],
        ];

        let result = hash_left_join(&left, &right, &[0], &[0], 2);
        assert_eq!(result.len(), 3);
        // SOL row has NULLs for right side
        assert_eq!(result[2][0], Value::Str("SOL".into()));
        assert_eq!(result[2][1], Value::F64(25.0));
        assert_eq!(result[2][2], Value::Null);
        assert_eq!(result[2][3], Value::Null);
    }

    // --- Helper: hash-based RIGHT JOIN ---
    fn hash_right_join(
        left: &[Vec<Value>],
        right: &[Vec<Value>],
        left_keys: &[usize],
        right_keys: &[usize],
        left_col_count: usize,
    ) -> Vec<Vec<Value>> {
        let mut right_map: HashMap<Vec<HashableValue>, Vec<usize>> = HashMap::new();
        for (i, row) in right.iter().enumerate() {
            let key: Vec<HashableValue> = right_keys.iter().map(|&k| HashableValue::from(&row[k])).collect();
            right_map.entry(key).or_default().push(i);
        }
        let mut result = Vec::new();
        let mut right_matched = vec![false; right.len()];
        for left_row in left {
            let key: Vec<HashableValue> = left_keys.iter().map(|&k| HashableValue::from(&left_row[k])).collect();
            if let Some(indices) = right_map.get(&key) {
                for &ri in indices {
                    let mut combined = left_row.clone();
                    combined.extend(right[ri].iter().cloned());
                    result.push(combined);
                    right_matched[ri] = true;
                }
            }
        }
        // Emit unmatched right rows with NULLs on left side.
        for (i, matched) in right_matched.iter().enumerate() {
            if !matched {
                let mut combined: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                combined.extend(right[i].iter().cloned());
                result.push(combined);
            }
        }
        result
    }

    // --- Helper: hash-based FULL OUTER JOIN ---
    fn hash_full_outer_join(
        left: &[Vec<Value>],
        right: &[Vec<Value>],
        left_keys: &[usize],
        right_keys: &[usize],
        left_col_count: usize,
        right_col_count: usize,
    ) -> Vec<Vec<Value>> {
        let mut right_map: HashMap<Vec<HashableValue>, Vec<usize>> = HashMap::new();
        for (i, row) in right.iter().enumerate() {
            let key: Vec<HashableValue> = right_keys.iter().map(|&k| HashableValue::from(&row[k])).collect();
            right_map.entry(key).or_default().push(i);
        }
        let mut result = Vec::new();
        let mut right_matched = vec![false; right.len()];
        for left_row in left {
            let key: Vec<HashableValue> = left_keys.iter().map(|&k| HashableValue::from(&left_row[k])).collect();
            if let Some(indices) = right_map.get(&key) {
                for &ri in indices {
                    let mut combined = left_row.clone();
                    combined.extend(right[ri].iter().cloned());
                    result.push(combined);
                    right_matched[ri] = true;
                }
            } else {
                let mut combined = left_row.clone();
                combined.extend(std::iter::repeat(Value::Null).take(right_col_count));
                result.push(combined);
            }
        }
        for (i, matched) in right_matched.iter().enumerate() {
            if !matched {
                let mut combined: Vec<Value> = std::iter::repeat(Value::Null).take(left_col_count).collect();
                combined.extend(right[i].iter().cloned());
                result.push(combined);
            }
        }
        result
    }

    // --- Helper: CROSS JOIN (cartesian product) ---
    fn cross_join(left: &[Vec<Value>], right: &[Vec<Value>]) -> Vec<Vec<Value>> {
        let mut result = Vec::new();
        for left_row in left {
            for right_row in right {
                let mut combined = left_row.clone();
                combined.extend(right_row.iter().cloned());
                result.push(combined);
            }
        }
        result
    }

    #[test]
    fn test_right_join() {
        // Left: trades with symbol and price.
        let left = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("ETH".into()), Value::F64(50.0)],
        ];
        // Right: markets with symbol and name (includes XRP which is not in left).
        let right = vec![
            vec![Value::Str("BTC".into()), Value::Str("Bitcoin".into())],
            vec![Value::Str("ETH".into()), Value::Str("Ethereum".into())],
            vec![Value::Str("XRP".into()), Value::Str("Ripple".into())],
        ];

        let result = hash_right_join(&left, &right, &[0], &[0], 2);
        assert_eq!(result.len(), 3);
        // BTC and ETH rows match.
        assert_eq!(result[0][0], Value::Str("BTC".into()));
        assert_eq!(result[0][3], Value::Str("Bitcoin".into()));
        assert_eq!(result[1][0], Value::Str("ETH".into()));
        assert_eq!(result[1][3], Value::Str("Ethereum".into()));
        // XRP has NULLs on the left side.
        assert_eq!(result[2][0], Value::Null);
        assert_eq!(result[2][1], Value::Null);
        assert_eq!(result[2][2], Value::Str("XRP".into()));
        assert_eq!(result[2][3], Value::Str("Ripple".into()));
    }

    #[test]
    fn test_full_outer_join() {
        // Left: trades (BTC, SOL).
        let left = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("SOL".into()), Value::F64(25.0)],
        ];
        // Right: markets (BTC, XRP).
        let right = vec![
            vec![Value::Str("BTC".into()), Value::Str("Bitcoin".into())],
            vec![Value::Str("XRP".into()), Value::Str("Ripple".into())],
        ];

        let result = hash_full_outer_join(&left, &right, &[0], &[0], 2, 2);
        assert_eq!(result.len(), 3);
        // BTC matches on both sides.
        assert_eq!(result[0][0], Value::Str("BTC".into()));
        assert_eq!(result[0][3], Value::Str("Bitcoin".into()));
        // SOL has no match on right side.
        assert_eq!(result[1][0], Value::Str("SOL".into()));
        assert_eq!(result[1][2], Value::Null);
        assert_eq!(result[1][3], Value::Null);
        // XRP has no match on left side.
        assert_eq!(result[2][0], Value::Null);
        assert_eq!(result[2][1], Value::Null);
        assert_eq!(result[2][2], Value::Str("XRP".into()));
        assert_eq!(result[2][3], Value::Str("Ripple".into()));
    }

    #[test]
    fn test_cross_join() {
        let left = vec![
            vec![Value::Str("1m".into())],
            vec![Value::Str("5m".into())],
        ];
        let right = vec![
            vec![Value::Str("BTC".into())],
            vec![Value::Str("ETH".into())],
            vec![Value::Str("SOL".into())],
        ];

        let result = cross_join(&left, &right);
        // Cross join produces left_count * right_count rows.
        assert_eq!(result.len(), 2 * 3);
        assert_eq!(result.len(), 6);
        // First left row combined with each right row.
        assert_eq!(result[0][0], Value::Str("1m".into()));
        assert_eq!(result[0][1], Value::Str("BTC".into()));
        assert_eq!(result[1][0], Value::Str("1m".into()));
        assert_eq!(result[1][1], Value::Str("ETH".into()));
        assert_eq!(result[2][0], Value::Str("1m".into()));
        assert_eq!(result[2][1], Value::Str("SOL".into()));
        // Second left row.
        assert_eq!(result[3][0], Value::Str("5m".into()));
        assert_eq!(result[3][1], Value::Str("BTC".into()));
    }
}
