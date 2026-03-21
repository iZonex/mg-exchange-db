//! Filter cursor — wraps another cursor and applies a predicate.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::{CompareOp, Filter, PlanExpr, Value};
use crate::record_cursor::RecordCursor;

/// Filter cursor that evaluates a `Filter` against each row and only
/// emits rows that pass the predicate.
///
/// Uses `LinearFilter` from `compiled_filter` to avoid per-row schema
/// lookups and recursive filter evaluation.
pub struct FilterCursor {
    source: Box<dyn RecordCursor>,
    filter: Filter,
    /// Pre-compiled linear filter (lazily initialized on first batch).
    compiled: Option<crate::compiled_filter::LinearFilter>,
}

impl FilterCursor {
    pub fn new(source: Box<dyn RecordCursor>, filter: Filter) -> Self {
        Self { source, filter, compiled: None }
    }

    /// Build or retrieve the compiled filter from the schema.
    fn get_compiled(&mut self, schema: &[(String, ColumnType)]) -> &crate::compiled_filter::LinearFilter {
        if self.compiled.is_none() {
            let mut col_indices = std::collections::HashMap::new();
            for (i, (name, _)) in schema.iter().enumerate() {
                col_indices.insert(name.clone(), i);
            }
            self.compiled = Some(crate::compiled_filter::build_linear_filter(&self.filter, &col_indices));
        }
        self.compiled.as_ref().unwrap()
    }
}

impl RecordCursor for FilterCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();

        // Compile the filter once (uses fast-path typed ops, no schema lookups per row).
        let _compiled = self.get_compiled(&schema);

        let mut result = RecordBatch::new(schema.clone());

        while result.row_count() < max_rows {
            let batch = self.source.next_batch(max_rows)?;
            match batch {
                None => break,
                Some(b) => {
                    let compiled = self.compiled.as_ref().unwrap();
                    for r in 0..b.row_count() {
                        let row: Vec<Value> = (0..b.columns.len())
                            .map(|c| b.get_value(r, c))
                            .collect();
                        if compiled.evaluate(&row) {
                            result.append_row(&row);
                            if result.row_count() >= max_rows {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if result.row_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

// ── filter evaluation (kept for reference / fallback) ─────────────────

#[allow(dead_code)]
fn get_col_value<'a>(col: &str, row: &'a [Value], schema: &[(String, ColumnType)]) -> Option<&'a Value> {
    schema
        .iter()
        .position(|(name, _)| name == col)
        .and_then(|idx| row.get(idx))
}

#[allow(dead_code)]
fn evaluate_filter(filter: &Filter, row: &[Value], schema: &[(String, ColumnType)]) -> bool {
    match filter {
        Filter::Eq(col, expected) => {
            get_col_value(col, row, schema)
                .map(|v| v.eq_coerce(expected))
                .unwrap_or(false)
        }
        Filter::NotEq(col, expected) => {
            get_col_value(col, row, schema)
                .map(|v| !v.eq_coerce(expected))
                .unwrap_or(true)
        }
        Filter::Gt(col, expected) => {
            get_col_value(col, row, schema)
                .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater))
                .unwrap_or(false)
        }
        Filter::Lt(col, expected) => {
            get_col_value(col, row, schema)
                .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less))
                .unwrap_or(false)
        }
        Filter::Gte(col, expected) => {
            get_col_value(col, row, schema)
                .map(|v| matches!(v.cmp_coerce(expected), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)))
                .unwrap_or(false)
        }
        Filter::Lte(col, expected) => {
            get_col_value(col, row, schema)
                .map(|v| matches!(v.cmp_coerce(expected), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)))
                .unwrap_or(false)
        }
        Filter::Between(col, low, high) => {
            get_col_value(col, row, schema)
                .map(|v| {
                    matches!(v.cmp_coerce(low), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                        && matches!(v.cmp_coerce(high), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
                })
                .unwrap_or(false)
        }
        Filter::And(parts) => parts.iter().all(|p| evaluate_filter(p, row, schema)),
        Filter::Or(parts) => parts.iter().any(|p| evaluate_filter(p, row, schema)),
        Filter::IsNull(col) => {
            match get_col_value(col, row, schema) {
                None | Some(Value::Null) => true,
                _ => false,
            }
        }
        Filter::IsNotNull(col) => {
            matches!(get_col_value(col, row, schema), Some(v) if *v != Value::Null)
        }
        Filter::In(col, list) => {
            get_col_value(col, row, schema)
                .map(|v| list.iter().any(|item| v.eq_coerce(item)))
                .unwrap_or(false)
        }
        Filter::NotIn(col, list) => {
            get_col_value(col, row, schema)
                .map(|v| !list.iter().any(|item| v.eq_coerce(item)))
                .unwrap_or(true)
        }
        Filter::Like(col, pattern) => {
            get_col_value(col, row, schema)
                .map(|v| {
                    if let Value::Str(s) = v {
                        crate::executor::like_match(s, pattern, false)
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
        }
        Filter::NotLike(col, pattern) => {
            get_col_value(col, row, schema)
                .map(|v| {
                    if let Value::Str(s) = v {
                        !crate::executor::like_match(s, pattern, false)
                    } else {
                        true
                    }
                })
                .unwrap_or(true)
        }
        Filter::ILike(col, pattern) => {
            get_col_value(col, row, schema)
                .map(|v| {
                    if let Value::Str(s) = v {
                        crate::executor::like_match(s, pattern, true)
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
        }
        Filter::Not(inner) => !evaluate_filter(inner, row, schema),
        Filter::Expression { left, op, right } => {
            let lv = eval_expr(left, row, schema);
            let rv = eval_expr(right, row, schema);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(lv.cmp_coerce(&rv), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
                CompareOp::Lte => matches!(lv.cmp_coerce(&rv), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
            }
        }
        Filter::BetweenSymmetric(col, low, high) => {
            get_col_value(col, row, schema)
                .map(|v| {
                    let ge_low_le_high = matches!(v.cmp_coerce(low), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                        && matches!(v.cmp_coerce(high), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                    let ge_high_le_low = matches!(v.cmp_coerce(high), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                        && matches!(v.cmp_coerce(low), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                    ge_low_le_high || ge_high_le_low
                })
                .unwrap_or(false)
        }
        // Subquery filters and ALL/ANY are not supported in cursor mode.
        Filter::Subquery { .. } | Filter::InSubquery { .. } | Filter::Exists { .. }
        | Filter::All { .. } | Filter::Any { .. } => false,
    }
}

#[allow(dead_code)]
fn eval_expr(expr: &PlanExpr, row: &[Value], schema: &[(String, ColumnType)]) -> Value {
    match expr {
        PlanExpr::Column(name) => {
            get_col_value(name, row, schema).cloned().unwrap_or(Value::Null)
        }
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => {
            crate::executor::apply_binary_op(
                &eval_expr(left, row, schema),
                *op,
                &eval_expr(right, row, schema),
            )
        }
        PlanExpr::UnaryOp { op, expr } => {
            crate::executor::apply_unary_op(*op, &eval_expr(expr, row, schema))
        }
        PlanExpr::Function { .. } => Value::Null,
    }
}
