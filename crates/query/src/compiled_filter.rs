//! Compiled filter engine.
//!
//! Instead of interpreting `Filter` trees recursively at runtime, this module
//! compiles them into an enum-based evaluator where column indices are baked
//! in, all variant dispatch uses static `match` (no `Box<dyn Fn>` indirection),
//! and string comparisons for column names are eliminated.
//!
//! ## LinearFilter (stack-machine bytecode)
//!
//! The `LinearFilter` is a flat array of `FilterOp` instructions that avoids
//! both recursive `evaluate()` calls and the overhead of `cmp_coerce` for the
//! common case where values share the same type. It uses a small stack
//! (SmallVec-backed) to evaluate the filter in a single linear pass.
//!
//! For numeric comparisons, `LinearFilter` emits specialized `CmpF64` / `CmpI64`
//! ops that compare raw primitives directly, falling back to `cmp_coerce` only
//! when the row value and constant are different types.

use std::collections::HashMap;

use crate::plan::{Filter, Value};

/// A compiled row-level filter closure (legacy API, now wraps `LinearFilter`).
/// Takes a row (slice of `Value`s ordered by column index) and returns `true`
/// if the row passes the filter.
pub type FilterFn = Box<dyn Fn(&[Value]) -> bool + Send + Sync>;

/// Enum-based compiled filter that avoids dynamic dispatch.
///
/// Each variant stores column indices (resolved at compile time from column
/// names) and comparison values. The `evaluate` method is `#[inline]` and
/// uses a simple `match`, allowing the compiler to optimize and inline the
/// hot path.
#[derive(Clone)]
pub enum CompiledFilter {
    Eq(usize, Value),
    NotEq(usize, Value),
    Gt(usize, Value),
    Lt(usize, Value),
    Gte(usize, Value),
    Lte(usize, Value),
    Between(usize, Value, Value),
    BetweenSymmetric(usize, Value, Value),
    And(Vec<CompiledFilter>),
    Or(Vec<CompiledFilter>),
    IsNull(usize),
    IsNotNull(usize),
    In(usize, Vec<Value>),
    NotIn(usize, Vec<Value>),
    Like(usize, regex::Regex),
    NotLike(usize, regex::Regex),
    ILike(usize, regex::Regex),
    /// Fallback for filters that cannot be compiled (subqueries, etc.).
    AlwaysTrue,
}

impl CompiledFilter {
    /// Evaluate this filter against a row.
    #[inline]
    pub fn evaluate(&self, row: &[Value]) -> bool {
        match self {
            Self::Eq(idx, val) => row[*idx].eq_coerce(val),
            Self::NotEq(idx, val) => !row[*idx].eq_coerce(val),
            Self::Gt(idx, val) => {
                matches!(row[*idx].cmp_coerce(val), Some(std::cmp::Ordering::Greater))
            }
            Self::Lt(idx, val) => {
                matches!(row[*idx].cmp_coerce(val), Some(std::cmp::Ordering::Less))
            }
            Self::Gte(idx, val) => {
                matches!(
                    row[*idx].cmp_coerce(val),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            }
            Self::Lte(idx, val) => {
                matches!(
                    row[*idx].cmp_coerce(val),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            }
            Self::Between(idx, lo, hi) => {
                matches!(
                    row[*idx].cmp_coerce(lo),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    row[*idx].cmp_coerce(hi),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            }
            Self::BetweenSymmetric(idx, lo, hi) => {
                let fwd = matches!(row[*idx].cmp_coerce(lo), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(row[*idx].cmp_coerce(hi), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                let rev = matches!(row[*idx].cmp_coerce(hi), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(row[*idx].cmp_coerce(lo), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                fwd || rev
            }
            Self::And(filters) => filters.iter().all(|f| f.evaluate(row)),
            Self::Or(filters) => filters.iter().any(|f| f.evaluate(row)),
            Self::IsNull(idx) => row[*idx] == Value::Null,
            Self::IsNotNull(idx) => row[*idx] != Value::Null,
            Self::In(idx, vals) => vals.iter().any(|v| row[*idx].eq_coerce(v)),
            Self::NotIn(idx, vals) => !vals.iter().any(|v| row[*idx].eq_coerce(v)),
            Self::Like(idx, regex) => {
                if let Value::Str(s) = &row[*idx] { regex.is_match(s) } else { false }
            }
            Self::NotLike(idx, regex) => {
                if let Value::Str(s) = &row[*idx] { !regex.is_match(s) } else { true }
            }
            Self::ILike(idx, regex) => {
                if let Value::Str(s) = &row[*idx] { regex.is_match(s) } else { false }
            }
            Self::AlwaysTrue => true,
        }
    }
}

// ---------------------------------------------------------------------------
// LinearFilter: stack-machine bytecode filter
// ---------------------------------------------------------------------------

/// A single operation in the linear filter bytecode.
///
/// The evaluator walks the ops array once, using a small bool-stack for
/// intermediate results. Leaf ops (comparisons, null checks) push a bool;
/// `And` / `Or` pop N bools and push the combined result.
#[derive(Clone)]
pub enum FilterOp {
    // --- Specialized fast-path ops (no cmp_coerce overhead) ---
    /// Compare row[col] > f64 constant, push bool.
    GtF64(usize, f64),
    /// Compare row[col] < f64 constant, push bool.
    LtF64(usize, f64),
    /// Compare row[col] >= f64 constant, push bool.
    GteF64(usize, f64),
    /// Compare row[col] <= f64 constant, push bool.
    LteF64(usize, f64),
    /// Compare row[col] == f64 constant, push bool.
    EqF64(usize, f64),
    /// Compare row[col] != f64 constant, push bool.
    NotEqF64(usize, f64),
    /// Compare row[col] > i64 constant, push bool.
    GtI64(usize, i64),
    /// Compare row[col] < i64 constant, push bool.
    LtI64(usize, i64),
    /// Compare row[col] >= i64 constant, push bool.
    GteI64(usize, i64),
    /// Compare row[col] <= i64 constant, push bool.
    LteI64(usize, i64),
    /// Compare row[col] == i64 constant, push bool.
    EqI64(usize, i64),
    /// Compare row[col] != i64 constant, push bool.
    NotEqI64(usize, i64),

    // --- Generic ops (use cmp_coerce for cross-type) ---
    /// Generic equality: row[col] == val.
    Eq(usize, Value),
    /// Generic not-equal: row[col] != val.
    NotEq(usize, Value),
    /// Generic greater-than: row[col] > val.
    Gt(usize, Value),
    /// Generic less-than: row[col] < val.
    Lt(usize, Value),
    /// Generic greater-or-equal: row[col] >= val.
    Gte(usize, Value),
    /// Generic less-or-equal: row[col] <= val.
    Lte(usize, Value),

    /// Between: row[col] >= lo AND row[col] <= hi. Push bool.
    Between(usize, Value, Value),
    /// Symmetric between. Push bool.
    BetweenSymmetric(usize, Value, Value),

    /// Is NULL check.
    IsNull(usize),
    /// Is NOT NULL check.
    IsNotNull(usize),

    /// IN list check.
    In(usize, Vec<Value>),
    /// NOT IN list check.
    NotIn(usize, Vec<Value>),

    /// LIKE regex match.
    Like(usize, regex::Regex),
    /// NOT LIKE regex match.
    NotLike(usize, regex::Regex),
    /// ILIKE regex match.
    ILike(usize, regex::Regex),

    /// Always true (fallback).
    AlwaysTrue,

    /// Pop N bools from stack, push true if ALL are true.
    And(usize),
    /// Pop N bools from stack, push true if ANY is true.
    Or(usize),
}

/// A flattened, linear filter that evaluates without recursion.
///
/// The ops are laid out in post-order: leaf comparisons first, then the
/// combining `And`/`Or` ops. This is cache-friendly and avoids pointer-chasing.
#[derive(Clone)]
pub struct LinearFilter {
    ops: Vec<FilterOp>,
}

impl LinearFilter {
    /// Evaluate this filter against a row. Returns `true` if the row passes.
    #[inline]
    pub fn evaluate(&self, row: &[Value]) -> bool {
        // For filters with a single op (very common: simple WHERE col > val),
        // skip the stack entirely.
        if self.ops.len() == 1 {
            return Self::eval_leaf(&self.ops[0], row);
        }

        let mut stack = Vec::<bool>::with_capacity(8);

        for op in &self.ops {
            match op {
                FilterOp::And(n) => {
                    let start = stack.len() - n;
                    let result = stack[start..].iter().all(|&b| b);
                    stack.truncate(start);
                    stack.push(result);
                }
                FilterOp::Or(n) => {
                    let start = stack.len() - n;
                    let result = stack[start..].iter().any(|&b| b);
                    stack.truncate(start);
                    stack.push(result);
                }
                _ => {
                    stack.push(Self::eval_leaf(op, row));
                }
            }
        }

        stack.last().copied().unwrap_or(true)
    }

    /// Evaluate a single leaf operation.
    #[inline(always)]
    fn eval_leaf(op: &FilterOp, row: &[Value]) -> bool {
        match op {
            // Fast-path f64 comparisons: extract the f64 directly, no cmp_coerce.
            FilterOp::GtF64(idx, val) => match &row[*idx] {
                Value::F64(v) => *v > *val,
                Value::I64(v) => (*v as f64) > *val,
                _ => false,
            },
            FilterOp::LtF64(idx, val) => match &row[*idx] {
                Value::F64(v) => *v < *val,
                Value::I64(v) => (*v as f64) < *val,
                _ => false,
            },
            FilterOp::GteF64(idx, val) => match &row[*idx] {
                Value::F64(v) => *v >= *val,
                Value::I64(v) => (*v as f64) >= *val,
                _ => false,
            },
            FilterOp::LteF64(idx, val) => match &row[*idx] {
                Value::F64(v) => *v <= *val,
                Value::I64(v) => (*v as f64) <= *val,
                _ => false,
            },
            FilterOp::EqF64(idx, val) => match &row[*idx] {
                Value::F64(v) => *v == *val,
                Value::I64(v) => (*v as f64) == *val,
                _ => false,
            },
            FilterOp::NotEqF64(idx, val) => match &row[*idx] {
                Value::F64(v) => *v != *val,
                Value::I64(v) => (*v as f64) != *val,
                _ => true,
            },
            // Fast-path i64 comparisons.
            FilterOp::GtI64(idx, val) => match &row[*idx] {
                Value::I64(v) => *v > *val,
                Value::F64(v) => *v > (*val as f64),
                _ => false,
            },
            FilterOp::LtI64(idx, val) => match &row[*idx] {
                Value::I64(v) => *v < *val,
                Value::F64(v) => *v < (*val as f64),
                _ => false,
            },
            FilterOp::GteI64(idx, val) => match &row[*idx] {
                Value::I64(v) => *v >= *val,
                Value::F64(v) => *v >= (*val as f64),
                _ => false,
            },
            FilterOp::LteI64(idx, val) => match &row[*idx] {
                Value::I64(v) => *v <= *val,
                Value::F64(v) => *v <= (*val as f64),
                _ => false,
            },
            FilterOp::EqI64(idx, val) => match &row[*idx] {
                Value::I64(v) => *v == *val,
                Value::F64(v) => *v == (*val as f64),
                _ => false,
            },
            FilterOp::NotEqI64(idx, val) => match &row[*idx] {
                Value::I64(v) => *v != *val,
                Value::F64(v) => *v != (*val as f64),
                _ => true,
            },
            // Generic ops using cmp_coerce.
            FilterOp::Eq(idx, val) => row[*idx].eq_coerce(val),
            FilterOp::NotEq(idx, val) => !row[*idx].eq_coerce(val),
            FilterOp::Gt(idx, val) => {
                matches!(row[*idx].cmp_coerce(val), Some(std::cmp::Ordering::Greater))
            }
            FilterOp::Lt(idx, val) => {
                matches!(row[*idx].cmp_coerce(val), Some(std::cmp::Ordering::Less))
            }
            FilterOp::Gte(idx, val) => {
                matches!(
                    row[*idx].cmp_coerce(val),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            }
            FilterOp::Lte(idx, val) => {
                matches!(
                    row[*idx].cmp_coerce(val),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            }
            FilterOp::Between(idx, lo, hi) => {
                matches!(
                    row[*idx].cmp_coerce(lo),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    row[*idx].cmp_coerce(hi),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            }
            FilterOp::BetweenSymmetric(idx, lo, hi) => {
                let fwd = matches!(row[*idx].cmp_coerce(lo), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(row[*idx].cmp_coerce(hi), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                let rev = matches!(row[*idx].cmp_coerce(hi), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(row[*idx].cmp_coerce(lo), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                fwd || rev
            }
            FilterOp::IsNull(idx) => row[*idx] == Value::Null,
            FilterOp::IsNotNull(idx) => row[*idx] != Value::Null,
            FilterOp::In(idx, vals) => vals.iter().any(|v| row[*idx].eq_coerce(v)),
            FilterOp::NotIn(idx, vals) => !vals.iter().any(|v| row[*idx].eq_coerce(v)),
            FilterOp::Like(idx, regex) => {
                if let Value::Str(s) = &row[*idx] { regex.is_match(s) } else { false }
            }
            FilterOp::NotLike(idx, regex) => {
                if let Value::Str(s) = &row[*idx] { !regex.is_match(s) } else { true }
            }
            FilterOp::ILike(idx, regex) => {
                if let Value::Str(s) = &row[*idx] { regex.is_match(s) } else { false }
            }
            FilterOp::AlwaysTrue => true,
            // And/Or are handled in the main loop, not here.
            FilterOp::And(_) | FilterOp::Or(_) => unreachable!(),
        }
    }
}

/// Build a `LinearFilter` from a `Filter` tree.
///
/// The filter tree is flattened into post-order bytecode. Numeric comparisons
/// are specialized to avoid `cmp_coerce` overhead when the constant is f64 or
/// i64.
pub fn build_linear_filter(filter: &Filter, column_indices: &HashMap<String, usize>) -> LinearFilter {
    let mut ops = Vec::new();
    emit_ops(filter, column_indices, &mut ops);
    LinearFilter { ops }
}

/// Recursively emit ops in post-order (children before parent).
fn emit_ops(filter: &Filter, column_indices: &HashMap<String, usize>, ops: &mut Vec<FilterOp>) {
    match filter {
        Filter::Gt(col, val) => {
            let idx = column_indices[col];
            ops.push(match val {
                Value::F64(v) => FilterOp::GtF64(idx, *v),
                Value::I64(v) => FilterOp::GtI64(idx, *v),
                _ => FilterOp::Gt(idx, val.clone()),
            });
        }
        Filter::Lt(col, val) => {
            let idx = column_indices[col];
            ops.push(match val {
                Value::F64(v) => FilterOp::LtF64(idx, *v),
                Value::I64(v) => FilterOp::LtI64(idx, *v),
                _ => FilterOp::Lt(idx, val.clone()),
            });
        }
        Filter::Gte(col, val) => {
            let idx = column_indices[col];
            ops.push(match val {
                Value::F64(v) => FilterOp::GteF64(idx, *v),
                Value::I64(v) => FilterOp::GteI64(idx, *v),
                _ => FilterOp::Gte(idx, val.clone()),
            });
        }
        Filter::Lte(col, val) => {
            let idx = column_indices[col];
            ops.push(match val {
                Value::F64(v) => FilterOp::LteF64(idx, *v),
                Value::I64(v) => FilterOp::LteI64(idx, *v),
                _ => FilterOp::Lte(idx, val.clone()),
            });
        }
        Filter::Eq(col, val) => {
            let idx = column_indices[col];
            ops.push(match val {
                Value::F64(v) => FilterOp::EqF64(idx, *v),
                Value::I64(v) => FilterOp::EqI64(idx, *v),
                _ => FilterOp::Eq(idx, val.clone()),
            });
        }
        Filter::NotEq(col, val) => {
            let idx = column_indices[col];
            ops.push(match val {
                Value::F64(v) => FilterOp::NotEqF64(idx, *v),
                Value::I64(v) => FilterOp::NotEqI64(idx, *v),
                _ => FilterOp::NotEq(idx, val.clone()),
            });
        }
        Filter::Between(col, lo, hi) => {
            let idx = column_indices[col];
            ops.push(FilterOp::Between(idx, lo.clone(), hi.clone()));
        }
        Filter::BetweenSymmetric(col, lo, hi) => {
            let idx = column_indices[col];
            ops.push(FilterOp::BetweenSymmetric(idx, lo.clone(), hi.clone()));
        }
        Filter::And(filters) => {
            let n = filters.len();
            for f in filters {
                emit_ops(f, column_indices, ops);
            }
            ops.push(FilterOp::And(n));
        }
        Filter::Or(filters) => {
            let n = filters.len();
            for f in filters {
                emit_ops(f, column_indices, ops);
            }
            ops.push(FilterOp::Or(n));
        }
        Filter::IsNull(col) => ops.push(FilterOp::IsNull(column_indices[col])),
        Filter::IsNotNull(col) => ops.push(FilterOp::IsNotNull(column_indices[col])),
        Filter::In(col, vals) => ops.push(FilterOp::In(column_indices[col], vals.clone())),
        Filter::NotIn(col, vals) => ops.push(FilterOp::NotIn(column_indices[col], vals.clone())),
        Filter::Like(col, pattern) => ops.push(FilterOp::Like(column_indices[col], like_to_regex(pattern, true))),
        Filter::NotLike(col, pattern) => ops.push(FilterOp::NotLike(column_indices[col], like_to_regex(pattern, true))),
        Filter::ILike(col, pattern) => ops.push(FilterOp::ILike(column_indices[col], like_to_regex(pattern, false))),
        _ => ops.push(FilterOp::AlwaysTrue),
    }
}

// ---------------------------------------------------------------------------
// Legacy API (CompiledFilter)
// ---------------------------------------------------------------------------

/// Build a `CompiledFilter` from a `Filter` tree.
///
/// `column_indices` maps column names to their positional index in the row
/// slice that the resulting evaluator will receive.
pub fn build_compiled_filter(filter: &Filter, column_indices: &HashMap<String, usize>) -> CompiledFilter {
    match filter {
        Filter::Eq(col, val) => CompiledFilter::Eq(column_indices[col], val.clone()),
        Filter::NotEq(col, val) => CompiledFilter::NotEq(column_indices[col], val.clone()),
        Filter::Gt(col, val) => CompiledFilter::Gt(column_indices[col], val.clone()),
        Filter::Lt(col, val) => CompiledFilter::Lt(column_indices[col], val.clone()),
        Filter::Gte(col, val) => CompiledFilter::Gte(column_indices[col], val.clone()),
        Filter::Lte(col, val) => CompiledFilter::Lte(column_indices[col], val.clone()),
        Filter::Between(col, lo, hi) => CompiledFilter::Between(column_indices[col], lo.clone(), hi.clone()),
        Filter::BetweenSymmetric(col, lo, hi) => CompiledFilter::BetweenSymmetric(column_indices[col], lo.clone(), hi.clone()),
        Filter::And(filters) => CompiledFilter::And(filters.iter().map(|f| build_compiled_filter(f, column_indices)).collect()),
        Filter::Or(filters) => CompiledFilter::Or(filters.iter().map(|f| build_compiled_filter(f, column_indices)).collect()),
        Filter::IsNull(col) => CompiledFilter::IsNull(column_indices[col]),
        Filter::IsNotNull(col) => CompiledFilter::IsNotNull(column_indices[col]),
        Filter::In(col, vals) => CompiledFilter::In(column_indices[col], vals.clone()),
        Filter::NotIn(col, vals) => CompiledFilter::NotIn(column_indices[col], vals.clone()),
        Filter::Like(col, pattern) => CompiledFilter::Like(column_indices[col], like_to_regex(pattern, true)),
        Filter::NotLike(col, pattern) => CompiledFilter::NotLike(column_indices[col], like_to_regex(pattern, true)),
        Filter::ILike(col, pattern) => CompiledFilter::ILike(column_indices[col], like_to_regex(pattern, false)),
        _ => CompiledFilter::AlwaysTrue,
    }
}

/// Compile a `Filter` tree into a single efficient closure.
///
/// `column_indices` maps column names to their positional index in the row
/// slice that the resulting closure will receive.
///
/// This now builds a `LinearFilter` internally and wraps it in a closure for
/// backward compatibility. For best performance, prefer `build_linear_filter`
/// + `LinearFilter::evaluate` directly.
pub fn compile_filter(filter: &Filter, column_indices: &HashMap<String, usize>) -> FilterFn {
    let linear = build_linear_filter(filter, column_indices);
    Box::new(move |row: &[Value]| linear.evaluate(row))
}

/// Compile a filter for columnar execution.
///
/// The closure takes a slice of column byte slices (one per column in index order)
/// and a row index, returning `true` if the row passes the filter.
/// Column data is interpreted as raw bytes (little-endian f64/i64 for numeric columns).
pub fn compile_columnar_filter(
    filter: &Filter,
    column_indices: &HashMap<String, usize>,
) -> Box<dyn Fn(&[&[u8]], usize) -> bool + Send + Sync> {
    match filter {
        Filter::Eq(col, val) => {
            let idx = column_indices[col];
            let val = val.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                row_val.eq_coerce(&val)
            })
        }
        Filter::NotEq(col, val) => {
            let idx = column_indices[col];
            let val = val.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                !row_val.eq_coerce(&val)
            })
        }
        Filter::Gt(col, val) => {
            let idx = column_indices[col];
            let val = val.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                matches!(row_val.cmp_coerce(&val), Some(std::cmp::Ordering::Greater))
            })
        }
        Filter::Lt(col, val) => {
            let idx = column_indices[col];
            let val = val.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                matches!(row_val.cmp_coerce(&val), Some(std::cmp::Ordering::Less))
            })
        }
        Filter::Gte(col, val) => {
            let idx = column_indices[col];
            let val = val.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                matches!(
                    row_val.cmp_coerce(&val),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            })
        }
        Filter::Lte(col, val) => {
            let idx = column_indices[col];
            let val = val.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                matches!(
                    row_val.cmp_coerce(&val),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
        }
        Filter::Between(col, lo, hi) => {
            let idx = column_indices[col];
            let lo = lo.clone();
            let hi = hi.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                let ge = matches!(
                    row_val.cmp_coerce(&lo),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                );
                let le = matches!(
                    row_val.cmp_coerce(&hi),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                ge && le
            })
        }
        Filter::BetweenSymmetric(col, lo, hi) => {
            let idx = column_indices[col];
            let lo = lo.clone();
            let hi = hi.clone();
            Box::new(move |cols: &[&[u8]], row: usize| {
                let row_val = read_value_from_column(cols[idx], row);
                let fwd = matches!(row_val.cmp_coerce(&lo), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(row_val.cmp_coerce(&hi), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                let rev = matches!(row_val.cmp_coerce(&hi), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    && matches!(row_val.cmp_coerce(&lo), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
                fwd || rev
            })
        }
        Filter::And(filters) => {
            let compiled: Vec<Box<dyn Fn(&[&[u8]], usize) -> bool + Send + Sync>> = filters
                .iter()
                .map(|f| compile_columnar_filter(f, column_indices))
                .collect();
            Box::new(move |cols: &[&[u8]], row: usize| compiled.iter().all(|f| f(cols, row)))
        }
        Filter::Or(filters) => {
            let compiled: Vec<Box<dyn Fn(&[&[u8]], usize) -> bool + Send + Sync>> = filters
                .iter()
                .map(|f| compile_columnar_filter(f, column_indices))
                .collect();
            Box::new(move |cols: &[&[u8]], row: usize| compiled.iter().any(|f| f(cols, row)))
        }
        Filter::IsNull(col) => {
            let idx = column_indices[col];
            // In columnar storage, we check for NaN (f64) as null sentinel.
            Box::new(move |cols: &[&[u8]], row: usize| {
                let v = read_value_from_column(cols[idx], row);
                v == Value::Null
            })
        }
        Filter::IsNotNull(col) => {
            let idx = column_indices[col];
            Box::new(move |cols: &[&[u8]], row: usize| {
                let v = read_value_from_column(cols[idx], row);
                v != Value::Null
            })
        }
        _ => Box::new(|_cols: &[&[u8]], _row: usize| true),
    }
}

/// Read a `Value` from a raw column byte slice at a given row index.
///
/// Assumes 8-byte little-endian values (f64 or i64). The caller is responsible
/// for ensuring the column data has the right layout.
#[inline]
fn read_value_from_column(col_data: &[u8], row: usize) -> Value {
    let offset = row * 8;
    if offset + 8 > col_data.len() {
        return Value::Null;
    }
    let bytes: [u8; 8] = col_data[offset..offset + 8].try_into().unwrap();
    // Interpret as f64 by default; the comparison functions handle coercion.
    let v = f64::from_le_bytes(bytes);
    if v.is_nan() {
        Value::Null
    } else {
        Value::F64(v)
    }
}

/// Convert a SQL LIKE pattern into a compiled `Regex`.
///
/// `case_sensitive` controls whether the regex is case-sensitive.
fn like_to_regex(pattern: &str, case_sensitive: bool) -> regex::Regex {
    let mut re = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => re.push_str(".*"),
            '_' => re.push('.'),
            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|'
            | '\\' => {
                re.push('\\');
                re.push(ch);
            }
            _ => re.push(ch),
        }
    }
    re.push('$');
    if case_sensitive {
        regex::Regex::new(&re).unwrap()
    } else {
        regex::Regex::new(&format!("(?i){re}")).unwrap()
    }
}

/// Evaluate a `Filter` tree recursively against a row (the interpreted path).
/// Used in tests to verify that the compiled filter produces the same results.
pub fn interpret_filter(
    filter: &Filter,
    row: &[Value],
    column_indices: &HashMap<String, usize>,
) -> bool {
    match filter {
        Filter::Eq(col, val) => row[column_indices[col]].eq_coerce(val),
        Filter::NotEq(col, val) => !row[column_indices[col]].eq_coerce(val),
        Filter::Gt(col, val) => matches!(
            row[column_indices[col]].cmp_coerce(val),
            Some(std::cmp::Ordering::Greater)
        ),
        Filter::Lt(col, val) => matches!(
            row[column_indices[col]].cmp_coerce(val),
            Some(std::cmp::Ordering::Less)
        ),
        Filter::Gte(col, val) => matches!(
            row[column_indices[col]].cmp_coerce(val),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        Filter::Lte(col, val) => matches!(
            row[column_indices[col]].cmp_coerce(val),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        Filter::Between(col, lo, hi) => {
            let v = &row[column_indices[col]];
            let ge = matches!(
                v.cmp_coerce(lo),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            );
            let le = matches!(
                v.cmp_coerce(hi),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            );
            ge && le
        }
        Filter::BetweenSymmetric(col, lo, hi) => {
            let v = &row[column_indices[col]];
            let fwd = matches!(v.cmp_coerce(lo), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                && matches!(v.cmp_coerce(hi), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
            let rev = matches!(v.cmp_coerce(hi), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                && matches!(v.cmp_coerce(lo), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal));
            fwd || rev
        }
        Filter::And(fs) => fs.iter().all(|f| interpret_filter(f, row, column_indices)),
        Filter::Or(fs) => fs.iter().any(|f| interpret_filter(f, row, column_indices)),
        Filter::IsNull(col) => row[column_indices[col]] == Value::Null,
        Filter::IsNotNull(col) => row[column_indices[col]] != Value::Null,
        Filter::In(col, vals) => vals.iter().any(|v| row[column_indices[col]].eq_coerce(v)),
        Filter::NotIn(col, vals) => !vals.iter().any(|v| row[column_indices[col]].eq_coerce(v)),
        Filter::Like(col, pattern) => {
            let regex = like_to_regex(pattern, true);
            if let Value::Str(s) = &row[column_indices[col]] {
                regex.is_match(s)
            } else {
                false
            }
        }
        Filter::NotLike(col, pattern) => {
            let regex = like_to_regex(pattern, true);
            if let Value::Str(s) = &row[column_indices[col]] {
                !regex.is_match(s)
            } else {
                true
            }
        }
        Filter::ILike(col, pattern) => {
            let regex = like_to_regex(pattern, false);
            if let Value::Str(s) = &row[column_indices[col]] {
                regex.is_match(s)
            } else {
                false
            }
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_indices(names: &[&str]) -> HashMap<String, usize> {
        names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.to_string(), i))
            .collect()
    }

    #[test]
    fn compiled_matches_interpreted_10k() {
        let indices = make_indices(&["price", "volume", "symbol"]);

        let filter = Filter::And(vec![
            Filter::Gt("price".into(), Value::F64(100.0)),
            Filter::Lte("volume".into(), Value::I64(5000)),
        ]);

        let compiled = compile_filter(&filter, &indices);

        for i in 0..10_000 {
            let price = Value::F64((i % 300) as f64);
            let volume = Value::I64((i * 7) % 10_000);
            let symbol = Value::Str(format!("SYM{}", i % 5));
            let row = vec![price, volume, symbol];

            let compiled_result = compiled(&row);
            let interp_result = interpret_filter(&filter, &row, &indices);
            assert_eq!(
                compiled_result, interp_result,
                "mismatch at row {i}: compiled={compiled_result}, interpreted={interp_result}"
            );
        }
    }

    #[test]
    fn compiled_handles_and_or_nested() {
        let indices = make_indices(&["a", "b", "c"]);

        // (a > 5 AND b < 10) OR (c == 42)
        let filter = Filter::Or(vec![
            Filter::And(vec![
                Filter::Gt("a".into(), Value::I64(5)),
                Filter::Lt("b".into(), Value::I64(10)),
            ]),
            Filter::Eq("c".into(), Value::I64(42)),
        ]);

        let compiled = compile_filter(&filter, &indices);

        // a=10, b=5, c=0 -> (10>5 AND 5<10)=true OR false => true
        assert!(compiled(&[Value::I64(10), Value::I64(5), Value::I64(0)]));

        // a=1, b=20, c=42 -> false OR (42==42)=true => true
        assert!(compiled(&[Value::I64(1), Value::I64(20), Value::I64(42)]));

        // a=1, b=20, c=0 -> false OR false => false
        assert!(!compiled(&[Value::I64(1), Value::I64(20), Value::I64(0)]));

        // Verify against interpreter
        for a in 0..20 {
            for b in 0..20 {
                for c in [0, 42, 100] {
                    let row = vec![Value::I64(a), Value::I64(b), Value::I64(c)];
                    assert_eq!(
                        compiled(&row),
                        interpret_filter(&filter, &row, &indices),
                        "mismatch at a={a}, b={b}, c={c}"
                    );
                }
            }
        }
    }

    #[test]
    fn compiled_eq() {
        let indices = make_indices(&["x"]);
        let f = compile_filter(&Filter::Eq("x".into(), Value::I64(42)), &indices);
        assert!(f(&[Value::I64(42)]));
        assert!(!f(&[Value::I64(41)]));
        // Cross-type coercion
        assert!(f(&[Value::F64(42.0)]));
    }

    #[test]
    fn compiled_between() {
        let indices = make_indices(&["x"]);
        let f = compile_filter(
            &Filter::Between("x".into(), Value::I64(10), Value::I64(20)),
            &indices,
        );
        assert!(!f(&[Value::I64(9)]));
        assert!(f(&[Value::I64(10)]));
        assert!(f(&[Value::I64(15)]));
        assert!(f(&[Value::I64(20)]));
        assert!(!f(&[Value::I64(21)]));
    }

    #[test]
    fn compiled_is_null() {
        let indices = make_indices(&["x"]);
        let f = compile_filter(&Filter::IsNull("x".into()), &indices);
        assert!(f(&[Value::Null]));
        assert!(!f(&[Value::I64(1)]));

        let f2 = compile_filter(&Filter::IsNotNull("x".into()), &indices);
        assert!(!f2(&[Value::Null]));
        assert!(f2(&[Value::I64(1)]));
    }

    #[test]
    fn compiled_in_list() {
        let indices = make_indices(&["x"]);
        let vals = vec![Value::I64(1), Value::I64(3), Value::I64(5)];
        let f = compile_filter(&Filter::In("x".into(), vals), &indices);
        assert!(f(&[Value::I64(1)]));
        assert!(!f(&[Value::I64(2)]));
        assert!(f(&[Value::I64(5)]));
    }

    #[test]
    fn compiled_like() {
        let indices = make_indices(&["name"]);
        let f = compile_filter(&Filter::Like("name".into(), "BTC%".into()), &indices);
        assert!(f(&[Value::Str("BTCUSD".into())]));
        assert!(!f(&[Value::Str("ETHUSD".into())]));
    }

    #[test]
    fn compiled_ilike() {
        let indices = make_indices(&["name"]);
        let f = compile_filter(&Filter::ILike("name".into(), "btc%".into()), &indices);
        assert!(f(&[Value::Str("BTCUSD".into())]));
        assert!(f(&[Value::Str("btcusd".into())]));
    }

    #[test]
    fn columnar_filter_basic() {
        let indices = make_indices(&["price"]);
        let filter = Filter::Gt("price".into(), Value::F64(100.0));
        let compiled = compile_columnar_filter(&filter, &indices);

        // Build a column of 3 f64 values: 50.0, 150.0, 99.0
        let mut col_data = Vec::new();
        col_data.extend_from_slice(&50.0f64.to_le_bytes());
        col_data.extend_from_slice(&150.0f64.to_le_bytes());
        col_data.extend_from_slice(&99.0f64.to_le_bytes());

        let cols: Vec<&[u8]> = vec![&col_data];
        assert!(!compiled(&cols, 0)); // 50 > 100 = false
        assert!(compiled(&cols, 1)); // 150 > 100 = true
        assert!(!compiled(&cols, 2)); // 99 > 100 = false
    }

    // --- LinearFilter-specific tests ---

    #[test]
    fn linear_filter_simple_gt_f64() {
        let indices = make_indices(&["price"]);
        let filter = Filter::Gt("price".into(), Value::F64(100.0));
        let linear = build_linear_filter(&filter, &indices);

        assert!(linear.evaluate(&[Value::F64(150.0)]));
        assert!(!linear.evaluate(&[Value::F64(50.0)]));
        assert!(!linear.evaluate(&[Value::F64(100.0)]));
        // Cross-type: I64 vs F64 constant
        assert!(linear.evaluate(&[Value::I64(200)]));
        assert!(!linear.evaluate(&[Value::I64(50)]));
    }

    #[test]
    fn linear_filter_simple_eq_i64() {
        let indices = make_indices(&["side"]);
        let filter = Filter::Eq("side".into(), Value::I64(0));
        let linear = build_linear_filter(&filter, &indices);

        assert!(linear.evaluate(&[Value::I64(0)]));
        assert!(!linear.evaluate(&[Value::I64(1)]));
        // Cross-type coercion
        assert!(linear.evaluate(&[Value::F64(0.0)]));
    }

    #[test]
    fn linear_filter_and_complex() {
        let indices = make_indices(&["price", "volume", "side"]);

        // price > 50100 AND side = 0
        let filter = Filter::And(vec![
            Filter::Gt("price".into(), Value::F64(50100.0)),
            Filter::Eq("side".into(), Value::I64(0)),
        ]);

        let linear = build_linear_filter(&filter, &indices);

        // Both true
        assert!(linear.evaluate(&[Value::F64(51000.0), Value::F64(1.0), Value::I64(0)]));
        // price fails
        assert!(!linear.evaluate(&[Value::F64(50000.0), Value::F64(1.0), Value::I64(0)]));
        // side fails
        assert!(!linear.evaluate(&[Value::F64(51000.0), Value::F64(1.0), Value::I64(1)]));
        // Both fail
        assert!(!linear.evaluate(&[Value::F64(100.0), Value::F64(1.0), Value::I64(1)]));
    }

    #[test]
    fn linear_filter_or_nested() {
        let indices = make_indices(&["a", "b", "c"]);

        // (a > 5 AND b < 10) OR (c == 42)
        let filter = Filter::Or(vec![
            Filter::And(vec![
                Filter::Gt("a".into(), Value::I64(5)),
                Filter::Lt("b".into(), Value::I64(10)),
            ]),
            Filter::Eq("c".into(), Value::I64(42)),
        ]);

        let linear = build_linear_filter(&filter, &indices);

        assert!(linear.evaluate(&[Value::I64(10), Value::I64(5), Value::I64(0)]));
        assert!(linear.evaluate(&[Value::I64(1), Value::I64(20), Value::I64(42)]));
        assert!(!linear.evaluate(&[Value::I64(1), Value::I64(20), Value::I64(0)]));

        // Exhaustive check against interpreter
        for a in 0..20 {
            for b in 0..20 {
                for c in [0, 42, 100] {
                    let row = vec![Value::I64(a), Value::I64(b), Value::I64(c)];
                    assert_eq!(
                        linear.evaluate(&row),
                        interpret_filter(&filter, &row, &indices),
                        "mismatch at a={a}, b={b}, c={c}"
                    );
                }
            }
        }
    }

    #[test]
    fn linear_matches_compiled_10k() {
        let indices = make_indices(&["price", "volume", "symbol"]);

        let filter = Filter::And(vec![
            Filter::Gt("price".into(), Value::F64(100.0)),
            Filter::Lte("volume".into(), Value::I64(5000)),
        ]);

        let linear = build_linear_filter(&filter, &indices);
        let compiled = build_compiled_filter(&filter, &indices);

        for i in 0..10_000 {
            let price = Value::F64((i % 300) as f64);
            let volume = Value::I64((i * 7) % 10_000);
            let symbol = Value::Str(format!("SYM{}", i % 5));
            let row = vec![price, volume, symbol];

            assert_eq!(
                linear.evaluate(&row),
                compiled.evaluate(&row),
                "mismatch at row {i}"
            );
        }
    }
}
