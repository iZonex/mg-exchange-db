//! Executes query plans against on-disk table data.
//!
//! Supports arithmetic expressions in SELECT and WHERE, INSERT INTO ... SELECT,
//! and TRUNCATE TABLE.

fn evaluate_plan_expr(
    expr: &PlanExpr,
    values: &[(usize, Value)],
    meta: &exchange_core::table::TableMeta,
) -> Value {
    match expr {
        PlanExpr::Column(name) => {
            if let Some(idx) = meta.columns.iter().position(|c| c.name == *name) {
                values
                    .iter()
                    .find(|(i, _)| *i == idx)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => apply_binary_op(
            &evaluate_plan_expr(left, values, meta),
            *op,
            &evaluate_plan_expr(right, values, meta),
        ),
        PlanExpr::UnaryOp { op, expr } => {
            apply_unary_op(*op, &evaluate_plan_expr(expr, values, meta))
        }
        PlanExpr::Function { name, args } => {
            let func_args: Vec<Value> = args
                .iter()
                .map(|a| evaluate_plan_expr(a, values, meta))
                .collect();
            crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null)
        }
    }
}

pub(crate) fn evaluate_plan_expr_by_name(
    expr: &PlanExpr,
    row: &[Value],
    col_names: &[String],
) -> Value {
    match expr {
        PlanExpr::Column(name) => {
            // Exact match first, then suffix match for qualified column names (e.g., "t.price").
            let idx = col_names.iter().position(|n| n == name).or_else(|| {
                let suffix = format!(".{name}");
                col_names.iter().position(|n| n.ends_with(&suffix))
            });
            idx.and_then(|i| row.get(i).cloned()).unwrap_or(Value::Null)
        }
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => apply_binary_op(
            &evaluate_plan_expr_by_name(left, row, col_names),
            *op,
            &evaluate_plan_expr_by_name(right, row, col_names),
        ),
        PlanExpr::UnaryOp { op, expr } => {
            apply_unary_op(*op, &evaluate_plan_expr_by_name(expr, row, col_names))
        }
        PlanExpr::Function { name, args } => {
            let func_args: Vec<Value> = args
                .iter()
                .map(|a| evaluate_plan_expr_by_name(a, row, col_names))
                .collect();
            crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null)
        }
    }
}

pub(crate) fn apply_binary_op(lv: &Value, op: BinaryOp, rv: &Value) -> Value {
    match op {
        BinaryOp::Add => match (lv, rv) {
            (Value::I64(a), Value::I64(b)) => Value::I64(a.wrapping_add(*b)),
            (Value::F64(a), Value::F64(b)) => Value::F64(a + b),
            (Value::I64(a), Value::F64(b)) => Value::F64(*a as f64 + b),
            (Value::F64(a), Value::I64(b)) => Value::F64(a + *b as f64),
            _ => Value::Null,
        },
        BinaryOp::Sub => match (lv, rv) {
            (Value::I64(a), Value::I64(b)) => Value::I64(a.wrapping_sub(*b)),
            (Value::F64(a), Value::F64(b)) => Value::F64(a - b),
            (Value::I64(a), Value::F64(b)) => Value::F64(*a as f64 - b),
            (Value::F64(a), Value::I64(b)) => Value::F64(a - *b as f64),
            _ => Value::Null,
        },
        BinaryOp::Mul => match (lv, rv) {
            (Value::I64(a), Value::I64(b)) => Value::I64(a.wrapping_mul(*b)),
            (Value::F64(a), Value::F64(b)) => Value::F64(a * b),
            (Value::I64(a), Value::F64(b)) => Value::F64(*a as f64 * b),
            (Value::F64(a), Value::I64(b)) => Value::F64(a * *b as f64),
            _ => Value::Null,
        },
        BinaryOp::Div => match (lv, rv) {
            (Value::I64(a), Value::I64(b)) if *b != 0 => Value::I64(a / b),
            (Value::F64(a), Value::F64(b)) if *b != 0.0 => Value::F64(a / b),
            (Value::I64(a), Value::F64(b)) if *b != 0.0 => Value::F64(*a as f64 / b),
            (Value::F64(a), Value::I64(b)) if *b != 0 => Value::F64(a / *b as f64),
            _ => Value::Null,
        },
        BinaryOp::Mod => match (lv, rv) {
            (Value::I64(a), Value::I64(b)) if *b != 0 => Value::I64(a % b),
            (Value::F64(a), Value::F64(b)) if *b != 0.0 => Value::F64(a % b),
            _ => Value::Null,
        },
        BinaryOp::Concat => {
            let ls = match lv {
                Value::Str(s) => s.clone(),
                Value::I64(n) => n.to_string(),
                Value::F64(n) => n.to_string(),
                Value::Null => return Value::Null,
                other => format!("{other}"),
            };
            let rs = match rv {
                Value::Str(s) => s.clone(),
                Value::I64(n) => n.to_string(),
                Value::F64(n) => n.to_string(),
                Value::Null => return Value::Null,
                other => format!("{other}"),
            };
            Value::Str(format!("{ls}{rs}"))
        }
        BinaryOp::Gt => {
            if lv.cmp_coerce(rv) == Some(std::cmp::Ordering::Greater) {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::Lt => {
            if lv.cmp_coerce(rv) == Some(std::cmp::Ordering::Less) {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::Gte => {
            if matches!(
                lv.cmp_coerce(rv),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ) {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::Lte => {
            if matches!(
                lv.cmp_coerce(rv),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::Eq => {
            if lv.eq_coerce(rv) {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::NotEq => {
            if !lv.eq_coerce(rv) {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::And => {
            let lb = matches!(lv, Value::I64(v) if *v != 0);
            let rb = matches!(rv, Value::I64(v) if *v != 0);
            if lb && rb {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
        BinaryOp::Or => {
            let lb = matches!(lv, Value::I64(v) if *v != 0);
            let rb = matches!(rv, Value::I64(v) if *v != 0);
            if lb || rb {
                Value::I64(1)
            } else {
                Value::I64(0)
            }
        }
    }
}

pub(crate) fn apply_unary_op(op: UnaryOp, val: &Value) -> Value {
    match op {
        UnaryOp::Neg => match val {
            Value::I64(n) => Value::I64(-n),
            Value::F64(n) => Value::F64(-n),
            _ => Value::Null,
        },
        UnaryOp::Not => match val {
            Value::I64(0) => Value::I64(1),
            Value::I64(_) => Value::I64(0),
            _ => Value::Null,
        },
    }
}

/// Evaluate a `PlanExpr` without any row context (for literal expressions only).
fn evaluate_plan_expr_standalone(expr: &PlanExpr) -> Value {
    match expr {
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => apply_binary_op(
            &evaluate_plan_expr_standalone(left),
            *op,
            &evaluate_plan_expr_standalone(right),
        ),
        PlanExpr::UnaryOp { op, expr } => apply_unary_op(*op, &evaluate_plan_expr_standalone(expr)),
        PlanExpr::Function { name, args } => {
            let func_args: Vec<Value> = args.iter().map(evaluate_plan_expr_standalone).collect();
            crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null)
        }
        PlanExpr::Column(_) => Value::Null,
    }
}

use crate::functions::{self, AggregateFunction};
use crate::plan::*;
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter};
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

/// Execute a `QueryPlan` with full RBAC and resource limit enforcement.
///
/// Permission checks are performed before execution:
/// - SELECT / JOIN / ASOF JOIN / EXPLAIN / COPY TO: read permission
/// - INSERT / UPDATE / DELETE / COPY FROM: write permission
/// - CREATE TABLE / DROP TABLE / ADD/DROP/RENAME/SET COLUMN / VACUUM / MAT VIEWS: DDL permission
/// - CREATE/DROP USER/ROLE, GRANT/REVOKE: admin permission
///
/// Resource limits (concurrent queries) are enforced via the context's
/// resource manager, if configured.
pub fn execute_with_context(
    ctx: &crate::context::ExecutionContext,
    plan: &QueryPlan,
) -> Result<QueryResult> {
    // ── RBAC checks ──────────────────────────────────────────────
    check_plan_permissions(ctx, plan)?;

    // ── Resource admission ───────────────────────────────────────
    let token = ctx.admit_query()?;

    // ── MVCC snapshot ────────────────────────────────────────────
    let _snapshot_guard = ctx.begin_snapshot();

    // ── RLS filter injection ─────────────────────────────────────
    let plan_with_rls = inject_rls_filter(ctx, plan);
    let plan = plan_with_rls.as_ref().unwrap_or(plan);

    let t0 = std::time::Instant::now();
    let result = if ctx.use_cursor_engine {
        crate::cursor_executor::execute_via_cursors(&ctx.db_root, plan)
    } else if ctx.use_wal {
        execute_with_wal_and_repl(&ctx.db_root, plan, ctx.replication_manager.clone())
    } else {
        execute(&ctx.db_root, plan)
    };
    let t_inner = t0.elapsed();
    tracing::debug!(
        engine_us = t_inner.as_micros(),
        cursor = ctx.use_cursor_engine,
        wal = ctx.use_wal,
        "inner execute"
    );

    // ── MVCC commit for write operations ─────────────────────────
    // After a successful write, record the new row counts in the MVCC
    // manager so subsequent snapshots reflect the committed data.
    if let Ok(ref qr) = result
        && let Some(ref mvcc) = ctx.mvcc
    {
        let affected = match qr {
            QueryResult::Ok { affected_rows } => *affected_rows,
            _ => 0,
        };
        if affected > 0 {
            let table_name = extract_write_table(plan);
            if let Some(tbl) = table_name {
                // We record the affected count as a delta; the MVCC layer
                // uses cumulative counts, but callers that wire this into
                // production should pass the *total* row count after the
                // write.  Here we use affected_rows as a reasonable proxy
                // that bumps the version and makes the write visible.
                let current = mvcc.current_version();
                mvcc.commit_write(&[(&tbl, current + affected)]);
            }
        }
    }

    // ── Audit logging ────────────────────────────────────────────
    if let Some(ref audit) = ctx.audit_log {
        let (action, table) = audit_action_for_plan(plan);
        let audit_result = match &result {
            Ok(_) => exchange_core::audit::AuditResult::Success,
            Err(e) => exchange_core::audit::AuditResult::Error(e.to_string()),
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let _ = audit.log(exchange_core::audit::AuditEntry {
            timestamp: now,
            user: ctx.current_user.clone().unwrap_or_default(),
            action,
            table,
            query: ctx.sql_text.clone(),
            result: audit_result,
            client_ip: None,
        });
    }

    // Release the query token on completion (success or failure).
    if let Some(t) = token {
        ctx.release_query(t);
    }

    // ── Improved error messages ──────────────────────────────────
    // Attach the original SQL text to query errors for context.
    match result {
        Err(ExchangeDbError::Query(detail)) if ctx.sql_text.is_some() => {
            Err(ExchangeDbError::QueryDetailed {
                detail,
                sql: ctx.sql_text.clone().unwrap(),
            })
        }
        Err(ExchangeDbError::ColumnNotFound(col, table)) if ctx.sql_text.is_some() => {
            Err(ExchangeDbError::QueryDetailed {
                detail: format!("column '{}' not found in table '{}'", col, table),
                sql: ctx.sql_text.clone().unwrap(),
            })
        }
        other => other,
    }
}

/// Extract the table name from a write plan (INSERT/UPDATE/DELETE).
fn extract_write_table(plan: &QueryPlan) -> Option<String> {
    match plan {
        QueryPlan::Insert { table, .. }
        | QueryPlan::Update { table, .. }
        | QueryPlan::Delete { table, .. }
        | QueryPlan::InsertSelect {
            target_table: table,
            ..
        }
        | QueryPlan::InsertOnConflict { table, .. } => Some(table.clone()),
        QueryPlan::CreateTableAs { name, .. } => Some(name.clone()),
        _ => None,
    }
}

/// Map a query plan to the appropriate audit action and table name.
fn audit_action_for_plan(plan: &QueryPlan) -> (exchange_core::audit::AuditAction, Option<String>) {
    use exchange_core::audit::AuditAction;
    match plan {
        QueryPlan::Select { table, .. } => (AuditAction::Query, Some(table.clone())),
        QueryPlan::Join { left_table, .. } => (AuditAction::Query, Some(left_table.clone())),
        QueryPlan::AsofJoin { left_table, .. } => (AuditAction::Query, Some(left_table.clone())),
        QueryPlan::Insert { table, .. } => (AuditAction::Insert, Some(table.clone())),
        QueryPlan::InsertSelect { target_table, .. } => {
            (AuditAction::Insert, Some(target_table.clone()))
        }
        QueryPlan::InsertOnConflict { table, .. } => (AuditAction::Insert, Some(table.clone())),
        QueryPlan::Update { table, .. } => (AuditAction::Update, Some(table.clone())),
        QueryPlan::Delete { table, .. } => (AuditAction::Delete, Some(table.clone())),
        QueryPlan::CreateTable { name, .. } => (AuditAction::CreateTable, Some(name.clone())),
        QueryPlan::CreateTableAs { name, .. } => (AuditAction::CreateTable, Some(name.clone())),
        QueryPlan::DropTable { table, .. } => (AuditAction::DropTable, Some(table.clone())),
        QueryPlan::AddColumn { table, .. }
        | QueryPlan::DropColumn { table, .. }
        | QueryPlan::RenameColumn { table, .. }
        | QueryPlan::SetColumnType { table, .. } => (AuditAction::AlterTable, Some(table.clone())),
        QueryPlan::CreateUser { username, .. } => (AuditAction::CreateUser, Some(username.clone())),
        QueryPlan::DropUser { username, .. } => (AuditAction::DropUser, Some(username.clone())),
        QueryPlan::Grant { target, .. } => (AuditAction::Grant, Some(target.clone())),
        QueryPlan::Revoke { target, .. } => (AuditAction::Revoke, Some(target.clone())),
        QueryPlan::Vacuum { table } => (AuditAction::Vacuum, Some(table.clone())),
        _ => (AuditAction::Query, None),
    }
}

/// Inject RLS filter into a plan if policies exist for the current user.
///
/// Supports SELECT, DELETE, UPDATE, and JOIN plans. For joins, the RLS
/// filter is applied to the left table's filter (the right table filter
/// would require a separate pass in a full implementation).
fn inject_rls_filter(
    ctx: &crate::context::ExecutionContext,
    plan: &QueryPlan,
) -> Option<QueryPlan> {
    match plan {
        QueryPlan::Select { table, filter, .. } => {
            let rls_filter = ctx.get_rls_filter(table)?;
            let combined = match filter {
                Some(existing) => Filter::And(vec![existing.clone(), rls_filter]),
                None => rls_filter,
            };
            let mut new_plan = plan.clone();
            if let QueryPlan::Select {
                filter: ref mut f, ..
            } = new_plan
            {
                *f = Some(combined);
            }
            Some(new_plan)
        }
        QueryPlan::Delete { table, filter } => {
            let rls_filter = ctx.get_rls_filter(table)?;
            let combined = match filter {
                Some(existing) => Filter::And(vec![existing.clone(), rls_filter]),
                None => rls_filter,
            };
            Some(QueryPlan::Delete {
                table: table.clone(),
                filter: Some(combined),
            })
        }
        QueryPlan::Update {
            table,
            assignments,
            filter,
        } => {
            let rls_filter = ctx.get_rls_filter(table)?;
            let combined = match filter {
                Some(existing) => Filter::And(vec![existing.clone(), rls_filter]),
                None => rls_filter,
            };
            Some(QueryPlan::Update {
                table: table.clone(),
                assignments: assignments.clone(),
                filter: Some(combined),
            })
        }
        QueryPlan::Join {
            left_table, filter, ..
        } => {
            let rls_filter = ctx.get_rls_filter(left_table)?;
            let combined = match filter {
                Some(existing) => Filter::And(vec![existing.clone(), rls_filter]),
                None => rls_filter,
            };
            let mut new_plan = plan.clone();
            if let QueryPlan::Join {
                filter: ref mut f, ..
            } = new_plan
            {
                *f = Some(combined);
            }
            Some(new_plan)
        }
        _ => None,
    }
}

/// Verify that the execution context has the required permissions for the plan.
fn check_plan_permissions(ctx: &crate::context::ExecutionContext, plan: &QueryPlan) -> Result<()> {
    match plan {
        // ── Read operations ──────────────────────────────────────
        QueryPlan::Select { table, .. } => ctx.check_read(table),
        QueryPlan::Join {
            left_table,
            right_table,
            ..
        } => {
            ctx.check_read(left_table)?;
            ctx.check_read(right_table)
        }
        QueryPlan::AsofJoin {
            left_table,
            right_table,
            ..
        } => {
            ctx.check_read(left_table)?;
            ctx.check_read(right_table)
        }
        QueryPlan::Explain { query } => check_plan_permissions(ctx, query),
        QueryPlan::ExplainAnalyze { query } => check_plan_permissions(ctx, query),
        QueryPlan::CopyTo { table, .. } => ctx.check_read(table),
        QueryPlan::SetOperation { left, right, .. } => {
            check_plan_permissions(ctx, left)?;
            check_plan_permissions(ctx, right)
        }
        QueryPlan::WithCte { body, .. } => check_plan_permissions(ctx, body),
        QueryPlan::DerivedScan { subquery, .. } => check_plan_permissions(ctx, subquery),

        // ── Write operations ─────────────────────────────────────
        QueryPlan::Insert { table, .. } => ctx.check_write(table),
        QueryPlan::Update { table, .. } => ctx.check_write(table),
        QueryPlan::Delete { table, .. } => ctx.check_write(table),
        QueryPlan::CopyFrom { table, .. } => ctx.check_write(table),

        // ── DDL operations ───────────────────────────────────────
        QueryPlan::CreateTable { .. } => ctx.check_ddl(),
        QueryPlan::DropTable { .. } => ctx.check_ddl(),
        QueryPlan::AddColumn { .. } => ctx.check_ddl(),
        QueryPlan::DropColumn { .. } => ctx.check_ddl(),
        QueryPlan::RenameColumn { .. } => ctx.check_ddl(),
        QueryPlan::SetColumnType { .. } => ctx.check_ddl(),
        QueryPlan::Vacuum { .. } => ctx.check_ddl(),
        QueryPlan::DetachPartition { .. } => ctx.check_ddl(),
        QueryPlan::AttachPartition { .. } => ctx.check_ddl(),
        QueryPlan::SquashPartitions { .. } => ctx.check_ddl(),
        QueryPlan::CreateMatView { .. } => ctx.check_ddl(),
        QueryPlan::RefreshMatView { .. } => ctx.check_ddl(),
        QueryPlan::DropMatView { .. } => ctx.check_ddl(),

        // ── Admin operations ─────────────────────────────────────
        QueryPlan::CreateUser { .. } => ctx.check_admin(),
        QueryPlan::DropUser { .. } => ctx.check_admin(),
        QueryPlan::CreateRole { .. } => ctx.check_admin(),
        QueryPlan::DropRole { .. } => ctx.check_admin(),
        QueryPlan::Grant { .. } => ctx.check_admin(),
        QueryPlan::Revoke { .. } => ctx.check_admin(),

        // ── No-op / compatibility operations ─────────────────────
        QueryPlan::Begin | QueryPlan::Commit | QueryPlan::Rollback => Ok(()),
        QueryPlan::Set { .. } | QueryPlan::Show { .. } => Ok(()),
        QueryPlan::ShowTables
        | QueryPlan::ShowColumns { .. }
        | QueryPlan::ShowCreateTable { .. } => Ok(()),
        QueryPlan::LongSequence { .. } => Ok(()),
        QueryPlan::GenerateSeries { .. } => Ok(()),
        QueryPlan::ReadParquet { .. } => Ok(()),
        QueryPlan::InsertSelect { target_table, .. } => ctx.check_write(target_table),
        QueryPlan::TruncateTable { .. } => ctx.check_ddl(),
        QueryPlan::MultiJoin { .. } => Ok(()),
        QueryPlan::Pivot { .. }
        | QueryPlan::Merge { .. }
        | QueryPlan::InsertOnConflict { .. }
        | QueryPlan::Values { .. }
        | QueryPlan::LateralJoin { .. } => Ok(()),
        QueryPlan::CreateIndex { .. }
        | QueryPlan::DropIndex { .. }
        | QueryPlan::RenameTable { .. } => ctx.check_ddl(),
        QueryPlan::CreateSequence { .. } | QueryPlan::DropSequence { .. } => ctx.check_ddl(),
        QueryPlan::SequenceOp { .. } => Ok(()),
        QueryPlan::CreateProcedure { .. } | QueryPlan::DropProcedure { .. } => ctx.check_ddl(),
        QueryPlan::CallProcedure { .. } => Ok(()),
        QueryPlan::CreateDownsampling { .. } => ctx.check_ddl(),
        QueryPlan::CreateView { .. } | QueryPlan::DropView { .. } => ctx.check_ddl(),
        QueryPlan::CreateTrigger { .. } | QueryPlan::DropTrigger { .. } => ctx.check_ddl(),
        QueryPlan::CommentOn { .. } => ctx.check_ddl(),
        QueryPlan::CreateTableAs { .. } => ctx.check_ddl(),
        QueryPlan::ReadCsv { .. } => Ok(()),
    }
}

/// Execute a `QueryPlan` against the database rooted at `db_root`.
///
/// For SELECT queries, the optimizer is invoked first to prune partitions,
/// select index scans, push predicates down, and enable limit pushdown.
///
/// This is a convenience wrapper that creates an anonymous context with
/// no security or resource limits. Use [`execute_with_context`] for
/// full RBAC and resource enforcement.
pub fn execute(db_root: &Path, plan: &QueryPlan) -> Result<QueryResult> {
    match plan {
        QueryPlan::CreateTable {
            name,
            columns,
            partition_by,
            timestamp_col,
            if_not_exists,
        } => {
            if *if_not_exists {
                let table_dir = db_root.join(name);
                if table_dir.exists() {
                    return Ok(QueryResult::Ok { affected_rows: 0 });
                }
            }
            execute_create_table(
                db_root,
                name,
                columns,
                partition_by.as_deref(),
                timestamp_col.as_deref(),
            )
        }
        QueryPlan::Insert {
            table,
            columns,
            values,
            upsert,
        } => {
            let reordered = reorder_insert_values(db_root, table, columns, values)?;
            validate_check_constraints(db_root, table, &reordered)?;
            validate_unique_constraints(db_root, table, &reordered)?;
            validate_foreign_key_constraints(db_root, table, &reordered)?;
            let result = execute_insert(db_root, table, &reordered, *upsert, false)?;
            fire_triggers_after_insert(db_root, table);
            Ok(result)
        }
        QueryPlan::Select { .. } => {
            // Check if the table is actually a view and redirect.
            if let QueryPlan::Select {
                table,
                columns,
                filter,
                order_by,
                limit,
                offset,
                ..
            } = plan
                && let Some(view_plan) = resolve_view(
                    db_root,
                    table,
                    columns,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                )
            {
                return execute(db_root, &view_plan);
            }
            // Skip optimizer for simple queries (no filter = no partition pruning needed,
            // no ORDER BY = no sort optimization needed). This saves ~100µs per query.
            let can_skip_optimizer = if let QueryPlan::Select {
                filter, order_by, ..
            } = plan
            {
                filter.is_none() && order_by.is_empty()
            } else {
                false
            };
            let optimized;
            let (opt_plan, pruned, limit_pd) = if can_skip_optimizer {
                // Build a trivial limit pushdown directly.
                let lp = if let QueryPlan::Select { limit: Some(l), .. } = plan {
                    Some(crate::optimizer::LimitPushdown {
                        limit: *l,
                        reverse_scan: false,
                    })
                } else {
                    None
                };
                (plan, None, lp)
            } else {
                optimized = crate::optimizer::optimize(plan.clone(), db_root)?;
                (
                    &optimized.plan,
                    optimized.pruned_partitions.as_deref(),
                    optimized.limit_pushdown,
                )
            };
            match opt_plan {
                QueryPlan::Select {
                    table,
                    columns,
                    filter,
                    order_by,
                    limit,
                    offset,
                    sample_by,
                    latest_on,
                    group_by,
                    group_by_mode,
                    having,
                    distinct,
                    distinct_on,
                } => execute_select_with_hints(
                    db_root,
                    table,
                    columns,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    sample_by.as_ref(),
                    latest_on.as_ref(),
                    group_by,
                    group_by_mode,
                    having.as_ref(),
                    *distinct,
                    distinct_on,
                    pruned,
                    limit_pd.as_ref(),
                ),
                _ => unreachable!("optimizer returned non-Select for Select input"),
            }
        }
        QueryPlan::Join {
            left_table,
            right_table,
            left_alias,
            right_alias,
            columns,
            join_type,
            on_columns,
            filter,
            order_by,
            limit,
        } => crate::join::execute_join(
            db_root,
            left_table,
            right_table,
            left_alias.as_deref(),
            right_alias.as_deref(),
            columns,
            *join_type,
            on_columns,
            filter.as_ref(),
            order_by,
            *limit,
        ),
        QueryPlan::MultiJoin {
            left,
            right_table,
            right_alias,
            columns,
            join_type,
            on_columns,
            filter,
            order_by,
            limit,
        } => crate::join::execute_multi_join(
            db_root,
            left,
            right_table,
            right_alias.as_deref(),
            columns,
            *join_type,
            on_columns,
            filter.as_ref(),
            order_by,
            *limit,
        ),
        QueryPlan::AsofJoin {
            left_table,
            right_table,
            left_columns,
            right_columns,
            on_columns,
            filter,
            order_by,
            limit,
        } => execute_asof_join(
            db_root,
            left_table,
            right_table,
            left_columns,
            right_columns,
            on_columns,
            filter.as_ref(),
            order_by,
            *limit,
        ),
        QueryPlan::AddColumn {
            table,
            column_name,
            column_type,
        } => execute_add_column(db_root, table, column_name, column_type),
        QueryPlan::DropColumn { table, column_name } => {
            execute_drop_column(db_root, table, column_name)
        }
        QueryPlan::RenameColumn {
            table,
            old_name,
            new_name,
        } => execute_rename_column(db_root, table, old_name, new_name),
        QueryPlan::SetColumnType {
            table,
            column_name,
            new_type,
        } => execute_set_column_type(db_root, table, column_name, new_type),
        QueryPlan::DropTable { table, if_exists } => {
            if *if_exists {
                let table_dir = db_root.join(table);
                if !table_dir.exists() {
                    return Ok(QueryResult::Ok { affected_rows: 0 });
                }
            }
            execute_drop_table(db_root, table)
        }
        QueryPlan::Delete { table, filter } => execute_delete(db_root, table, filter.as_ref()),
        QueryPlan::Update {
            table,
            assignments,
            filter,
        } => execute_update(db_root, table, assignments, filter.as_ref()),
        QueryPlan::SetOperation {
            op,
            left,
            right,
            all,
            limit,
        } => execute_set_operation(db_root, *op, left, right, *all, *limit, &HashMap::new()),
        QueryPlan::WithCte { ctes, body } => execute_with_cte(db_root, ctes, body),
        QueryPlan::DerivedScan {
            subquery,
            alias: _,
            columns,
            filter,
            order_by,
            limit,
            group_by,
            having,
            distinct,
        } => execute_derived_scan(
            db_root,
            subquery,
            columns,
            filter.as_ref(),
            order_by,
            *limit,
            group_by,
            having.as_ref(),
            *distinct,
            &HashMap::new(),
        ),
        QueryPlan::CopyTo {
            table,
            path,
            options,
        } => execute_copy_to(db_root, table, path, options),
        QueryPlan::CopyFrom {
            table,
            path,
            options,
        } => execute_copy_from(db_root, table, path, options),
        QueryPlan::Explain { query } => execute_explain(db_root, query),
        QueryPlan::ExplainAnalyze { query } => execute_explain_analyze(db_root, query),
        QueryPlan::Vacuum { table } => execute_vacuum(db_root, table),
        QueryPlan::CreateMatView { name, source_sql } => {
            execute_create_mat_view(db_root, name, source_sql)
        }
        QueryPlan::RefreshMatView { name } => execute_refresh_mat_view(db_root, name),
        QueryPlan::DropMatView { name } => execute_drop_mat_view(db_root, name),
        QueryPlan::CreateUser { username, password } => {
            execute_create_user(db_root, username, password)
        }
        QueryPlan::DropUser { username } => execute_drop_user(db_root, username),
        QueryPlan::CreateRole { name } => execute_create_role(db_root, name),
        QueryPlan::DropRole { name } => execute_drop_role(db_root, name),
        QueryPlan::Grant { permission, target } => execute_grant(db_root, permission, target),
        QueryPlan::Revoke { permission, target } => execute_revoke(db_root, permission, target),
        QueryPlan::ShowTables => execute_show_tables(db_root),
        QueryPlan::ShowColumns { table } => execute_show_columns(db_root, table),
        QueryPlan::ShowCreateTable { table } => execute_show_create_table(db_root, table),
        QueryPlan::LongSequence { count, columns } => execute_long_sequence(*count, columns),
        QueryPlan::GenerateSeries {
            start,
            stop,
            step,
            columns,
            is_timestamp,
        } => execute_generate_series(*start, *stop, *step, columns, *is_timestamp),
        QueryPlan::ReadParquet { path, columns } => execute_read_parquet(path, columns),
        QueryPlan::Begin | QueryPlan::Commit | QueryPlan::Rollback => {
            Ok(QueryResult::Ok { affected_rows: 0 })
        }
        QueryPlan::Set { .. } => Ok(QueryResult::Ok { affected_rows: 0 }),
        QueryPlan::Show { name } => execute_show_variable(name),
        QueryPlan::InsertSelect {
            target_table,
            columns: _,
            source,
        } => {
            let source_result = execute(db_root, source)?;
            match source_result {
                QueryResult::Rows { rows, .. } => {
                    execute_insert(db_root, target_table, &rows, false, false)
                }
                QueryResult::Ok { .. } => Err(ExchangeDbError::Query(
                    "INSERT ... SELECT source returned no rows".into(),
                )),
            }
        }
        QueryPlan::TruncateTable { table } => {
            let table_dir = db_root.join(table);
            if !table_dir.exists() {
                return Err(ExchangeDbError::TableNotFound(table.to_string()));
            }
            if let Ok(entries) = std::fs::read_dir(&table_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    if entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                        && !name_str.starts_with('_')
                        && name_str != "wal"
                    {
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                }
            }
            Ok(QueryResult::Ok { affected_rows: 0 })
        }
        QueryPlan::DetachPartition { table, partition } => {
            execute_detach_partition(db_root, table, partition)
        }
        QueryPlan::AttachPartition { table, partition } => {
            execute_attach_partition(db_root, table, partition)
        }
        QueryPlan::SquashPartitions {
            table,
            partition1,
            partition2,
        } => execute_squash_partitions(db_root, table, partition1, partition2),
        QueryPlan::Pivot {
            source,
            aggregate,
            agg_column,
            pivot_col,
            values,
        } => execute_pivot(db_root, source, *aggregate, agg_column, pivot_col, values),
        QueryPlan::Merge {
            target_table,
            source_table,
            on_column,
            when_clauses,
        } => execute_merge(db_root, target_table, source_table, on_column, when_clauses),
        QueryPlan::InsertOnConflict {
            table,
            columns,
            values,
            on_conflict,
        } => execute_insert_on_conflict(db_root, table, columns, values, on_conflict),
        QueryPlan::Values { column_names, rows } => Ok(QueryResult::Rows {
            columns: column_names.clone(),
            rows: rows.clone(),
        }),
        QueryPlan::LateralJoin { .. } => Err(ExchangeDbError::Query(
            "LATERAL JOIN not yet implemented".into(),
        )),
        QueryPlan::CreateIndex {
            name,
            table,
            columns,
        } => execute_create_index(db_root, name, table, columns),
        QueryPlan::DropIndex { name } => execute_drop_index(db_root, name),
        QueryPlan::RenameTable { old_name, new_name } => {
            execute_rename_table(db_root, old_name, new_name)
        }
        QueryPlan::CreateSequence {
            name,
            start,
            increment,
        } => crate::sequence::create_sequence(db_root, name, *start, *increment),
        QueryPlan::DropSequence { name } => crate::sequence::drop_sequence(db_root, name),
        QueryPlan::SequenceOp { op } => crate::sequence::execute_sequence_op(db_root, op),
        QueryPlan::CreateProcedure { name, body } => execute_create_procedure(db_root, name, body),
        QueryPlan::DropProcedure { name } => execute_drop_procedure(db_root, name),
        QueryPlan::CallProcedure { name } => execute_call_procedure(db_root, name),
        QueryPlan::CreateDownsampling {
            source_table,
            target_name,
            interval_secs,
            columns,
        } => {
            execute_create_downsampling(db_root, source_table, target_name, *interval_secs, columns)
        }
        QueryPlan::CreateView { name, sql } => execute_create_view(db_root, name, sql),
        QueryPlan::DropView { name } => execute_drop_view(db_root, name),
        QueryPlan::CreateTrigger {
            name,
            table,
            procedure,
        } => execute_create_trigger(db_root, name, table, procedure),
        QueryPlan::DropTrigger { name, table } => execute_drop_trigger(db_root, name, table),
        QueryPlan::CommentOn {
            object_type,
            object_name,
            table_name,
            comment,
        } => execute_comment_on(
            db_root,
            object_type,
            object_name,
            table_name.as_deref(),
            comment,
        ),
        QueryPlan::CreateTableAs {
            name,
            source,
            partition_by,
        } => execute_create_table_as(db_root, name, source, partition_by.as_deref()),
        QueryPlan::ReadCsv { path, columns } => execute_read_csv(path, columns),
    }
}

/// Execute SHOW <variable> — return the variable value for client compatibility.
fn execute_show_variable(name: &str) -> Result<QueryResult> {
    let lower = name.to_ascii_lowercase();
    let value = match lower.as_str() {
        "server_version" => "0.1.0".to_string(),
        "server_encoding" => "UTF8".to_string(),
        "client_encoding" => "UTF8".to_string(),
        "standard_conforming_strings" => "on".to_string(),
        "datestyle" => "ISO".to_string(),
        "timezone" | "time zone" => "UTC".to_string(),
        "transaction_isolation" | "transaction isolation" => "read committed".to_string(),
        "integer_datetimes" => "on".to_string(),
        "intervalstyle" => "postgres".to_string(),
        "is_superuser" => "on".to_string(),
        "session_authorization" => "exchangedb".to_string(),
        "max_identifier_length" => "63".to_string(),
        _ => format!("unknown variable: {name}"),
    };
    Ok(QueryResult::Rows {
        columns: vec![name.to_string()],
        rows: vec![vec![Value::Str(value)]],
    })
}

/// Execute SHOW TABLES.
fn execute_show_tables(db_root: &Path) -> Result<QueryResult> {
    let mut names = Vec::new();
    if let Ok(entries) = std::fs::read_dir(db_root) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let meta_path = entry.path().join("_meta");
                if meta_path.exists()
                    && let Some(name) = entry.file_name().to_str()
                {
                    names.push(name.to_string());
                }
            }
        }
    }
    names.sort();
    let rows: Vec<Vec<Value>> = names.into_iter().map(|n| vec![Value::Str(n)]).collect();
    Ok(QueryResult::Rows {
        columns: vec!["table_name".to_string()],
        rows,
    })
}

/// Execute SHOW COLUMNS FROM <table>.
fn execute_show_columns(db_root: &Path, table: &str) -> Result<QueryResult> {
    let meta = TableMeta::load(&db_root.join(table).join("_meta"))?;
    let comments = load_comments(db_root);
    let rows: Vec<Vec<Value>> = meta
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let key = format!("{}.{}", table, c.name);
            let comment = comments
                .columns
                .get(&key)
                .map(|s| Value::Str(s.clone()))
                .unwrap_or(Value::Null);
            vec![
                Value::Str(c.name.clone()),
                Value::Str(format!("{:?}", c.col_type)),
                Value::I64(i as i64 + 1),
                comment,
            ]
        })
        .collect();
    Ok(QueryResult::Rows {
        columns: vec![
            "column_name".to_string(),
            "data_type".to_string(),
            "ordinal_position".to_string(),
            "comment".to_string(),
        ],
        rows,
    })
}

/// Execute SHOW CREATE TABLE <table>.
fn execute_show_create_table(db_root: &Path, table: &str) -> Result<QueryResult> {
    let meta = TableMeta::load(&db_root.join(table).join("_meta"))?;
    let cols: Vec<String> = meta
        .columns
        .iter()
        .map(|c| format!("  {} {:?}", c.name, c.col_type))
        .collect();
    let ddl = format!("CREATE TABLE {} (\n{}\n)", table, cols.join(",\n"));
    Ok(QueryResult::Rows {
        columns: vec!["create_table".to_string()],
        rows: vec![vec![Value::Str(ddl)]],
    })
}

/// Execute SELECT ... FROM long_sequence(N).
///
/// Generates N rows where column `x` contains 1..N. Scalar functions
/// (including `rnd_*` functions) in the select list are evaluated per row.
fn execute_long_sequence(count: u64, columns: &[SelectColumn]) -> Result<QueryResult> {
    let mut col_names = Vec::new();
    let mut rows = Vec::with_capacity(count as usize);

    for col in columns {
        match col {
            SelectColumn::Name(n) => col_names.push(n.clone()),
            SelectColumn::Wildcard => col_names.push("x".to_string()),
            SelectColumn::ScalarFunction { name, .. } => col_names.push(name.clone()),
            SelectColumn::Aggregate {
                function,
                column,
                alias,
                ..
            } => {
                col_names.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| format!("{:?}({})", function, column).to_lowercase()),
                );
            }
            _ => col_names.push("?".to_string()),
        }
    }
    if col_names.is_empty() {
        col_names.push("x".to_string());
    }

    for i in 1..=count {
        let x_val = Value::I64(i as i64);
        let mut row = Vec::with_capacity(col_names.len());

        if columns.is_empty() {
            row.push(x_val);
        } else {
            for col in columns {
                match col {
                    SelectColumn::Name(_) | SelectColumn::Wildcard => {
                        row.push(x_val.clone());
                    }
                    SelectColumn::ScalarFunction { name, args } => {
                        let resolved_args: Vec<Value> = args
                            .iter()
                            .map(|a| match a {
                                SelectColumnArg::Column(_) => x_val.clone(),
                                SelectColumnArg::Literal(v) => v.clone(),
                            })
                            .collect();
                        match crate::scalar::evaluate_scalar(name, &resolved_args) {
                            Ok(v) => row.push(v),
                            Err(e) => return Err(ExchangeDbError::Query(e)),
                        }
                    }
                    _ => row.push(x_val.clone()),
                }
            }
        }

        rows.push(row);
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows,
    })
}

/// Execute a `generate_series(start, stop, step)` with integer or timestamp values.
fn execute_generate_series(
    start: i64,
    stop: i64,
    step: i64,
    columns: &[SelectColumn],
    is_timestamp: bool,
) -> Result<QueryResult> {
    if step == 0 {
        return Err(ExchangeDbError::Query(
            "generate_series step cannot be 0".into(),
        ));
    }

    let mut col_names = Vec::new();
    for col in columns {
        match col {
            SelectColumn::Name(n) => col_names.push(n.clone()),
            SelectColumn::Wildcard => col_names.push("generate_series".to_string()),
            SelectColumn::ScalarFunction { name, .. } => col_names.push(name.clone()),
            SelectColumn::Aggregate {
                function,
                column,
                alias,
                ..
            } => {
                col_names.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| format!("{:?}({})", function, column).to_lowercase()),
                );
            }
            _ => col_names.push("?".to_string()),
        }
    }
    if col_names.is_empty() {
        col_names.push("generate_series".to_string());
    }

    let mut rows = Vec::new();
    let mut current = start;

    let going_up = step > 0;
    loop {
        if going_up && current > stop {
            break;
        }
        if !going_up && current < stop {
            break;
        }

        let x_val = if is_timestamp {
            Value::Timestamp(current)
        } else {
            Value::I64(current)
        };

        let mut row = Vec::with_capacity(col_names.len());
        if columns.is_empty() {
            row.push(x_val.clone());
        } else {
            for col in columns {
                match col {
                    SelectColumn::Name(_) | SelectColumn::Wildcard => {
                        row.push(x_val.clone());
                    }
                    SelectColumn::ScalarFunction { name, args } => {
                        let resolved_args: Vec<Value> = args
                            .iter()
                            .map(|a| match a {
                                SelectColumnArg::Column(_) => x_val.clone(),
                                SelectColumnArg::Literal(v) => v.clone(),
                            })
                            .collect();
                        match crate::scalar::evaluate_scalar(name, &resolved_args) {
                            Ok(v) => row.push(v),
                            Err(e) => return Err(ExchangeDbError::Query(e)),
                        }
                    }
                    _ => row.push(x_val.clone()),
                }
            }
        }
        rows.push(row);
        current = current.wrapping_add(step);
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows,
    })
}

/// Execute a `read_parquet('/path')` table-valued function.
fn execute_read_parquet(path: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    use exchange_core::parquet::reader::{ParquetReader, RowValue};

    let reader = ParquetReader::open(path)?;
    let pq_meta = reader.metadata();

    // Determine which columns to select.
    let is_wildcard =
        columns.is_empty() || columns.iter().any(|c| matches!(c, SelectColumn::Wildcard));

    let col_names: Vec<String> = if is_wildcard {
        pq_meta.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        columns
            .iter()
            .filter_map(|c| match c {
                SelectColumn::Name(n) => Some(n.clone()),
                _ => None,
            })
            .collect()
    };

    let pq_rows = if is_wildcard {
        reader.read_all()?
    } else {
        reader.read_columns(&col_names)?
    };

    // Convert RowValue to Value.
    let rows: Vec<Vec<Value>> = pq_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    RowValue::I64(n) => Value::I64(n),
                    RowValue::F64(n) => Value::F64(n),
                    RowValue::Str(s) => Value::Str(s),
                    RowValue::Timestamp(ns) => Value::Timestamp(ns),
                    RowValue::Bytes(_) => Value::Null,
                    RowValue::Null => Value::Null,
                })
                .collect()
        })
        .collect();

    Ok(QueryResult::Rows {
        columns: col_names,
        rows,
    })
}

/// Execute a `CREATE TABLE ... AS SELECT` statement.
///
/// Runs the source query, infers the schema from the result columns, creates
/// the table, and inserts all rows.
fn execute_create_table_as(
    db_root: &Path,
    name: &str,
    source: &QueryPlan,
    partition_by: Option<&str>,
) -> Result<QueryResult> {
    // Execute the source query to get the data.
    let source_result = execute(db_root, source)?;
    match source_result {
        QueryResult::Rows {
            columns: col_names,
            rows,
        } => {
            // Infer column types from the first row of data (or default to VARCHAR).
            let col_defs: Vec<PlanColumnDef> = col_names
                .iter()
                .enumerate()
                .map(|(i, col_name)| {
                    let type_name = if let Some(first_row) = rows.first() {
                        match first_row.get(i) {
                            Some(Value::I64(_)) => "BIGINT",
                            Some(Value::F64(_)) => "DOUBLE",
                            Some(Value::Timestamp(_)) => "TIMESTAMP",
                            Some(Value::Str(_)) => "VARCHAR",
                            Some(Value::Null) | None => "VARCHAR",
                        }
                    } else {
                        "VARCHAR"
                    };
                    PlanColumnDef {
                        name: col_name.clone(),
                        type_name: type_name.to_string(),
                        check: None,
                        unique: false,
                        references: None,
                    }
                })
                .collect();

            // Detect timestamp column for automatic designation.
            let has_timestamp = col_defs.iter().any(|c| c.type_name == "TIMESTAMP");

            // If no timestamp column exists, add a synthetic one since every
            // ExchangeDB table requires a designated timestamp column.
            let mut final_col_defs = col_defs;
            let needs_synthetic_ts = !has_timestamp;
            if needs_synthetic_ts {
                final_col_defs.insert(
                    0,
                    PlanColumnDef {
                        name: "_ctas_timestamp".to_string(),
                        type_name: "TIMESTAMP".to_string(),
                        check: None,
                        unique: false,
                        references: None,
                    },
                );
            }

            let timestamp_col = final_col_defs
                .iter()
                .find(|c| c.type_name == "TIMESTAMP")
                .map(|c| c.name.clone());

            // Create the table.
            execute_create_table(
                db_root,
                name,
                &final_col_defs,
                partition_by,
                timestamp_col.as_deref(),
            )?;

            // Insert all rows, prepending a synthetic timestamp if needed.
            let row_count = rows.len() as u64;
            if !rows.is_empty() {
                let insert_rows: Vec<Vec<Value>> = if needs_synthetic_ts {
                    rows.iter()
                        .enumerate()
                        .map(|(i, row)| {
                            let mut new_row =
                                vec![Value::Timestamp(Timestamp::now().as_nanos() + i as i64)];
                            new_row.extend(row.iter().cloned());
                            new_row
                        })
                        .collect()
                } else {
                    rows
                };
                execute_insert(db_root, name, &insert_rows, false, false)?;
            }

            Ok(QueryResult::Ok {
                affected_rows: row_count,
            })
        }
        QueryResult::Ok { .. } => Err(ExchangeDbError::Query(
            "CREATE TABLE AS SELECT source did not produce rows".into(),
        )),
    }
}

/// Execute a `read_csv('/path')` table-valued function.
///
/// Reads a CSV file, infers column types from the first data row, and returns
/// all rows as query results. The first line is treated as a header row.
fn execute_read_csv(path: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let file = std::fs::File::open(path).map_err(|e| {
        ExchangeDbError::Query(format!("cannot open CSV file '{}': {e}", path.display()))
    })?;
    let reader = std::io::BufReader::new(file);
    let mut lines = reader.lines();

    // Read header line.
    let header_line = lines
        .next()
        .ok_or_else(|| ExchangeDbError::Query("CSV file is empty".into()))?
        .map_err(|e| ExchangeDbError::Query(format!("error reading CSV header: {e}")))?;

    let all_col_names: Vec<String> = header_line
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect();

    // Determine which columns to return.
    let is_wildcard =
        columns.is_empty() || columns.iter().any(|c| matches!(c, SelectColumn::Wildcard));

    let selected_indices: Vec<usize> = if is_wildcard {
        (0..all_col_names.len()).collect()
    } else {
        columns
            .iter()
            .filter_map(|c| match c {
                SelectColumn::Name(n) => all_col_names.iter().position(|cn| cn == n),
                _ => None,
            })
            .collect()
    };

    let result_col_names: Vec<String> = selected_indices
        .iter()
        .map(|&i| all_col_names[i].clone())
        .collect();

    // Read data rows.
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for line_result in lines {
        let line = line_result
            .map_err(|e| ExchangeDbError::Query(format!("error reading CSV line: {e}")))?;
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(',').collect();
        let row: Vec<Value> = selected_indices
            .iter()
            .map(|&i| {
                let field = fields
                    .get(i)
                    .map(|s| s.trim().trim_matches('"'))
                    .unwrap_or("");
                if field.is_empty() || field.eq_ignore_ascii_case("null") {
                    Value::Null
                } else if let Ok(n) = field.parse::<i64>() {
                    Value::I64(n)
                } else if let Ok(n) = field.parse::<f64>() {
                    Value::F64(n)
                } else {
                    Value::Str(field.to_string())
                }
            })
            .collect();
        rows.push(row);
    }

    Ok(QueryResult::Rows {
        columns: result_col_names,
        rows,
    })
}

/// Execute a `QueryPlan` with WAL-backed writes for INSERT operations.
///
/// Identical to [`execute`] except that INSERT statements use `WalTableWriter`
/// for crash-safe durability. All other plan types delegate to `execute`.
pub fn execute_with_wal(db_root: &Path, plan: &QueryPlan) -> Result<QueryResult> {
    execute_with_wal_and_repl(db_root, plan, None)
}

/// Execute a `QueryPlan` with WAL-backed writes and optional replication.
///
/// When `repl_mgr` is `Some`, the WAL writer ships committed segments to
/// replicas after each INSERT commit.
pub fn execute_with_wal_and_repl(
    db_root: &Path,
    plan: &QueryPlan,
    repl_mgr: Option<std::sync::Arc<exchange_core::replication::ReplicationManager>>,
) -> Result<QueryResult> {
    match plan {
        QueryPlan::Insert {
            table,
            columns,
            values,
            upsert,
        } => {
            let reordered = reorder_insert_values(db_root, table, columns, values)?;
            validate_check_constraints(db_root, table, &reordered)?;
            validate_unique_constraints(db_root, table, &reordered)?;
            validate_foreign_key_constraints(db_root, table, &reordered)?;
            let result =
                execute_insert_with_repl(db_root, table, &reordered, *upsert, true, repl_mgr)?;
            fire_triggers_after_insert(db_root, table);
            Ok(result)
        }
        other => execute(db_root, other),
    }
}

fn type_name_to_column_type(name: &str) -> Result<ColumnType> {
    match name.to_ascii_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Ok(ColumnType::Boolean),
        "TINYINT" | "I8" | "INT8" => Ok(ColumnType::I8),
        "SMALLINT" | "I16" | "INT16" => Ok(ColumnType::I16),
        "INT" | "INTEGER" | "I32" | "INT32" => Ok(ColumnType::I32),
        "BIGINT" | "I64" | "INT64" | "LONG" => Ok(ColumnType::I64),
        "FLOAT" | "REAL" | "F32" => Ok(ColumnType::F32),
        "DOUBLE" | "DOUBLE PRECISION" | "F64" => Ok(ColumnType::F64),
        "TIMESTAMP" => Ok(ColumnType::Timestamp),
        "SYMBOL" => Ok(ColumnType::Symbol),
        "VARCHAR" | "STRING" | "TEXT" | "CHARACTER VARYING" => Ok(ColumnType::Varchar),
        "BINARY" | "BLOB" | "BYTEA" => Ok(ColumnType::Binary),
        "UUID" => Ok(ColumnType::Uuid),
        "DATE" => Ok(ColumnType::Date),
        "CHAR" => Ok(ColumnType::Char),
        "IPV4" => Ok(ColumnType::IPv4),
        "IPV6" => Ok(ColumnType::IPv6),
        "LONG128" | "INT128" => Ok(ColumnType::Long128),
        "LONG256" | "INT256" => Ok(ColumnType::Long256),
        "GEOHASH" => Ok(ColumnType::GeoHash),
        "TIMESTAMP_MICRO" | "TIMESTAMPMICRO" => Ok(ColumnType::TimestampMicro),
        "TIMESTAMP_MILLI" | "TIMESTAMPMILLI" => Ok(ColumnType::TimestampMilli),
        "INTERVAL" => Ok(ColumnType::Interval),
        "DECIMAL8" => Ok(ColumnType::Decimal8),
        "DECIMAL16" => Ok(ColumnType::Decimal16),
        "DECIMAL32" => Ok(ColumnType::Decimal32),
        "DECIMAL64" => Ok(ColumnType::Decimal64),
        "DECIMAL128" | "DECIMAL" => Ok(ColumnType::Decimal128),
        s if s.starts_with("DECIMAL(") && s.ends_with(')') => {
            // DECIMAL(precision, scale) — maps to Decimal128 for up to 38 digits
            Ok(ColumnType::Decimal128)
        }
        "DECIMAL256" => Ok(ColumnType::Decimal256),
        "GEOBYTE" => Ok(ColumnType::GeoByte),
        "GEOSHORT" => Ok(ColumnType::GeoShort),
        "GEOINT" => Ok(ColumnType::GeoInt),
        "ARRAY" => Ok(ColumnType::Array),
        "CURSOR" => Ok(ColumnType::Cursor),
        "RECORD" => Ok(ColumnType::Record),
        "REGCLASS" => Ok(ColumnType::RegClass),
        "REGPROCEDURE" => Ok(ColumnType::RegProcedure),
        "ARRAYSTRING" | "TEXT[]" => Ok(ColumnType::ArrayString),
        "NULL" => Ok(ColumnType::Null),
        "VARARG" => Ok(ColumnType::VarArg),
        "PARAMETER" => Ok(ColumnType::Parameter),
        "VARCHAR_SLICE" | "VARCHARSLICE" => Ok(ColumnType::VarcharSlice),
        other => Err(ExchangeDbError::Query(format!(
            "unknown column type: {other}"
        ))),
    }
}

fn partition_by_from_str(s: &str) -> Result<PartitionBy> {
    match s.to_ascii_uppercase().as_str() {
        "NONE" => Ok(PartitionBy::None),
        "HOUR" => Ok(PartitionBy::Hour),
        "DAY" => Ok(PartitionBy::Day),
        "WEEK" => Ok(PartitionBy::Week),
        "MONTH" => Ok(PartitionBy::Month),
        "YEAR" => Ok(PartitionBy::Year),
        other => Err(ExchangeDbError::Query(format!(
            "unknown partition scheme: {other}"
        ))),
    }
}

fn execute_create_table(
    db_root: &Path,
    name: &str,
    columns: &[PlanColumnDef],
    partition_by: Option<&str>,
    timestamp_col: Option<&str>,
) -> Result<QueryResult> {
    if columns.is_empty() {
        return Err(ExchangeDbError::Query(
            "CREATE TABLE requires at least one column".into(),
        ));
    }

    let mut builder = TableBuilder::new(name);

    for col in columns {
        let ct = type_name_to_column_type(&col.type_name)?;
        builder = builder.column(&col.name, ct);
    }

    if let Some(ts_col) = timestamp_col {
        builder = builder.timestamp(ts_col);
    }

    if let Some(pb) = partition_by {
        builder = builder.partition_by(partition_by_from_str(pb)?);
    }

    builder.build(db_root)?;

    // Save CHECK constraints if any columns have them.
    let checks: Vec<CheckConstraint> = columns
        .iter()
        .filter_map(|c| {
            c.check.as_ref().map(|_expr| CheckConstraint {
                column: c.name.clone(),
                expr_sql: plan_expr_to_sql(_expr),
            })
        })
        .collect();
    if !checks.is_empty() {
        let constraints_path = db_root.join(name).join("_checks.json");
        let data = serde_json::to_string_pretty(&checks).unwrap();
        let _ = std::fs::write(&constraints_path, data);
    }

    // Save UNIQUE constraints if any columns have them.
    let uniques: Vec<UniqueConstraint> = columns
        .iter()
        .filter(|c| c.unique)
        .map(|c| UniqueConstraint {
            columns: vec![c.name.clone()],
        })
        .collect();
    if !uniques.is_empty() {
        let path = db_root.join(name).join("_unique.json");
        let data = serde_json::to_string_pretty(&uniques).unwrap();
        let _ = std::fs::write(&path, data);
    }

    // Save FOREIGN KEY constraints if any columns have them.
    let fks: Vec<ForeignKeyConstraint> = columns
        .iter()
        .filter_map(|c| {
            c.references
                .as_ref()
                .map(|(ref_table, ref_col)| ForeignKeyConstraint {
                    column: c.name.clone(),
                    ref_table: ref_table.clone(),
                    ref_column: ref_col.clone(),
                })
        })
        .collect();
    if !fks.is_empty() {
        let path = db_root.join(name).join("_fkeys.json");
        let data = serde_json::to_string_pretty(&fks).unwrap();
        let _ = std::fs::write(&path, data);
    }

    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn plan_expr_to_sql(expr: &PlanExpr) -> String {
    match expr {
        PlanExpr::Column(name) => name.clone(),
        PlanExpr::Literal(Value::I64(n)) => n.to_string(),
        PlanExpr::Literal(Value::F64(n)) => n.to_string(),
        PlanExpr::Literal(Value::Str(s)) => format!("'{}'", s),
        PlanExpr::Literal(Value::Null) => "NULL".to_string(),
        PlanExpr::Literal(Value::Timestamp(ns)) => ns.to_string(),
        PlanExpr::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Mod => "%",
                BinaryOp::Gt => ">",
                BinaryOp::Lt => "<",
                BinaryOp::Gte => ">=",
                BinaryOp::Lte => "<=",
                BinaryOp::Eq => "=",
                BinaryOp::NotEq => "!=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Concat => "||",
            };
            format!(
                "({} {} {})",
                plan_expr_to_sql(left),
                op_str,
                plan_expr_to_sql(right)
            )
        }
        PlanExpr::UnaryOp { op, expr } => {
            let op_str = match op {
                UnaryOp::Neg => "-",
                UnaryOp::Not => "NOT ",
            };
            format!("{}{}", op_str, plan_expr_to_sql(expr))
        }
        PlanExpr::Function { name, args } => {
            let arg_strs: Vec<String> = args.iter().map(plan_expr_to_sql).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
    }
}

/// Reorder and validate INSERT values to match the table schema.
///
/// Handles: column reordering, partial column inserts (fills missing with NULL),
/// column count validation, and nonexistent column detection.
fn reorder_insert_values(
    db_root: &Path,
    table: &str,
    columns: &[String],
    values: &[Vec<Value>],
) -> Result<Vec<Vec<Value>>> {
    // If no columns specified, pass through as-is (assumes schema order).
    if columns.is_empty() {
        return Ok(values.to_vec());
    }

    // Load table metadata to get schema column order.
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    let meta_path = table_dir.join("_meta");
    let meta = exchange_core::table::TableMeta::load(&meta_path)?;

    let schema_names: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();

    // Validate that all specified columns exist.
    for col in columns {
        if !schema_names.iter().any(|s| s.eq_ignore_ascii_case(col)) {
            return Err(ExchangeDbError::Query(format!(
                "column '{}' does not exist in table '{}'",
                col, table
            )));
        }
    }

    // Validate value count matches column count for each row.
    for row in values {
        if row.len() != columns.len() {
            return Err(ExchangeDbError::Query(format!(
                "INSERT has {} columns but {} values",
                columns.len(),
                row.len()
            )));
        }
    }

    // Build a mapping from specified column name to schema index.
    let col_to_schema_idx: Vec<usize> = columns
        .iter()
        .map(|c| {
            schema_names
                .iter()
                .position(|s| s.eq_ignore_ascii_case(c))
                .unwrap() // safe: validated above
        })
        .collect();

    // Check if columns are already in schema order and complete.
    let is_identity = columns.len() == schema_names.len()
        && col_to_schema_idx
            .iter()
            .enumerate()
            .all(|(i, &idx)| idx == i);
    if is_identity {
        return Ok(values.to_vec());
    }

    // Reorder values to match schema order, filling missing columns with NULL.
    let mut result = Vec::with_capacity(values.len());
    for row in values {
        let mut new_row = vec![Value::Null; schema_names.len()];
        for (val_idx, &schema_idx) in col_to_schema_idx.iter().enumerate() {
            new_row[schema_idx] = row[val_idx].clone();
        }
        result.push(new_row);
    }

    Ok(result)
}

fn execute_insert(
    db_root: &Path,
    table: &str,
    values: &[Vec<Value>],
    upsert: bool,
    use_wal: bool,
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta;
    let count: u64;

    if use_wal && !upsert {
        // WAL path: all rows are written to WAL as a single batch event.
        // If any row fails validation, nothing is written (atomic).
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, table, config)?;
        meta = writer.meta().clone();
        let ts_idx = meta.timestamp_column;

        // Phase 1: validate and collect all rows first (no side effects).
        let mut prepared_rows: Vec<(Timestamp, Vec<OwnedColumnValue>)> =
            Vec::with_capacity(values.len());
        for row in values {
            let ts = if ts_idx < row.len() {
                value_to_timestamp(&row[ts_idx])?
            } else {
                Timestamp::now()
            };

            let owned_values: Vec<OwnedColumnValue> = row
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    if i == ts_idx {
                        OwnedColumnValue::Timestamp(ts.as_nanos())
                    } else {
                        plan_value_to_owned(v)
                    }
                })
                .collect();

            prepared_rows.push((ts, owned_values));
        }

        // Phase 2: write all rows to WAL as a single batch (all-or-nothing).
        count = prepared_rows.len() as u64;
        for (ts, owned_values) in prepared_rows {
            writer.write_row(ts, owned_values)?;
        }

        writer.commit()?;
    } else {
        // Direct path (or upsert which needs dedup support not yet in WAL writer).
        //
        // Atomicity: collect all parsed rows first, then write them all.
        // If any row fails parsing/validation, nothing is written.

        let tmp_meta = TableMeta::load(&table_dir.join("_meta"))?;
        let ts_idx = tmp_meta.timestamp_column;

        // Phase 1: validate and collect all rows first (no side effects).
        struct PreparedRow {
            ts: Timestamp,
            col_values: Vec<Value>,
        }

        let mut prepared: Vec<PreparedRow> = Vec::with_capacity(values.len());
        for row in values {
            let ts = if ts_idx < row.len() {
                value_to_timestamp(&row[ts_idx])?
            } else {
                Timestamp::now()
            };
            prepared.push(PreparedRow {
                ts,
                col_values: row.clone(),
            });
        }

        // Phase 2: write all rows atomically.
        // Scope the writer so it is dropped (releasing file handles) before the
        // dedup pass needs to read the same partition files.
        {
            let mut writer = TableWriter::open(db_root, table)?;
            meta = writer.meta().clone();

            if upsert {
                let ts_col_name = meta.columns[meta.timestamp_column].name.clone();
                let dedup_cfg = exchange_core::dedup::DedupConfig::new(vec![ts_col_name]);
                writer.set_dedup_config(dedup_cfg);
            }

            for prep in &prepared {
                let col_values: Vec<ColumnValue<'_>> = prep
                    .col_values
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != ts_idx)
                    .map(|(_, v)| plan_value_to_column_value(v))
                    .collect();

                writer.write_row(prep.ts, &col_values)?;
            }

            writer.flush()?;
        }

        count = prepared.len() as u64;

        // For upsert, deduplicate existing data in affected partitions.
        if upsert {
            let table_dir = db_root.join(table);
            let partitions = exchange_core::table::list_partitions(&table_dir)?;
            for partition_path in &partitions {
                let rows = exchange_core::table::read_partition_rows_tiered(
                    partition_path,
                    &meta,
                    &table_dir,
                )?;
                if rows.is_empty() {
                    continue;
                }

                let ts_col_idx = meta.timestamp_column;
                let key_indices = vec![ts_col_idx];
                let unique_indices = exchange_core::dedup::unique_row_indices(&rows, &key_indices);
                if unique_indices.len() < rows.len() {
                    let unique_rows: Vec<Vec<ColumnValue<'_>>> = unique_indices
                        .iter()
                        .map(|&i| rows[i].iter().map(|v| v.borrow_column_value()).collect())
                        .collect();
                    exchange_core::table::rewrite_partition(partition_path, &meta, &unique_rows)?;
                }
            }
        }
    }

    // Invalidate mmap cache so subsequent reads see the new data.
    exchange_core::mmap::invalidate_mmap_cache(&table_dir);
    if let Some(reg) = crate::table_registry::global() {
        reg.invalidate(table);
    }

    Ok(QueryResult::Ok {
        affected_rows: count,
    })
}

/// Like `execute_insert` but wires an optional replication manager into the
/// WAL writer so that committed segments are shipped to replicas.
fn execute_insert_with_repl(
    db_root: &Path,
    table: &str,
    values: &[Vec<Value>],
    upsert: bool,
    use_wal: bool,
    repl_mgr: Option<std::sync::Arc<exchange_core::replication::ReplicationManager>>,
) -> Result<QueryResult> {
    if !use_wal || upsert || repl_mgr.is_none() {
        // Fall back to the existing path when no replication or no WAL.
        return execute_insert(db_root, table, values, upsert, use_wal);
    }

    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table, config)?;

    // Wire the replication manager so WAL segments are shipped on commit.
    if let Some(mgr) = repl_mgr {
        writer.set_replication_manager(mgr);
    }

    let meta = writer.meta().clone();
    let ts_idx = meta.timestamp_column;
    let mut count = 0u64;

    let col_count = meta.columns.len();

    for row in values {
        // The row may have all columns (len == col_count) or may be
        // missing the designated timestamp column (len == col_count - 1).
        // In the short-row case we auto-generate the timestamp and
        // insert it at the correct position so encode_row sees the
        // full column set.
        let ts = if row.len() == col_count && ts_idx < row.len() {
            // Full row — extract timestamp from the designated column.
            match value_to_timestamp(&row[ts_idx]) {
                Ok(t) => t,
                Err(_) => Timestamp::now(),
            }
        } else {
            Timestamp::now()
        };

        let owned_values: Vec<OwnedColumnValue> = if row.len() == col_count {
            // Row already has all columns — map 1:1.
            row.iter()
                .enumerate()
                .map(|(i, v)| {
                    if i == ts_idx {
                        OwnedColumnValue::Timestamp(ts.as_nanos())
                    } else {
                        plan_value_to_owned(v)
                    }
                })
                .collect()
        } else {
            // Short row (timestamp omitted) — rebuild with timestamp inserted.
            let mut vals = Vec::with_capacity(col_count);
            let mut src = 0usize;
            for i in 0..col_count {
                if i == ts_idx {
                    vals.push(OwnedColumnValue::Timestamp(ts.as_nanos()));
                } else {
                    if src < row.len() {
                        vals.push(plan_value_to_owned(&row[src]));
                        src += 1;
                    } else {
                        vals.push(OwnedColumnValue::Null);
                    }
                }
            }
            vals
        };

        writer.write_row(ts, owned_values)?;
        count += 1;
    }

    writer.commit()?;

    Ok(QueryResult::Ok {
        affected_rows: count,
    })
}

/// Convert a plan `Value` to an `OwnedColumnValue` for WAL writing.
fn plan_value_to_owned(v: &Value) -> OwnedColumnValue {
    match v {
        Value::I64(n) => OwnedColumnValue::I64(*n),
        Value::F64(n) => OwnedColumnValue::F64(*n),
        Value::Timestamp(ns) => OwnedColumnValue::Timestamp(*ns),
        Value::Str(s) => OwnedColumnValue::Varchar(s.clone()),
        Value::Null => OwnedColumnValue::Null,
    }
}

fn value_to_timestamp(v: &Value) -> Result<Timestamp> {
    match v {
        Value::I64(ns) => Ok(Timestamp(*ns)),
        Value::Timestamp(ns) => Ok(Timestamp(*ns)),
        Value::F64(f) => Ok(Timestamp(*f as i64)),
        _ => Err(ExchangeDbError::Query(
            "cannot convert value to timestamp".into(),
        )),
    }
}

fn plan_value_to_column_value(v: &Value) -> ColumnValue<'_> {
    match v {
        Value::I64(n) => ColumnValue::I64(*n),
        Value::F64(n) => ColumnValue::F64(*n),
        Value::Timestamp(ns) => ColumnValue::Timestamp(Timestamp(*ns)),
        Value::Str(s) => ColumnValue::Str(s.as_str()),
        Value::Null => ColumnValue::Null,
    }
}

fn execute_add_column(
    db_root: &Path,
    table: &str,
    column_name: &str,
    column_type_name: &str,
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let col_type = type_name_to_column_type(column_type_name)?;
    let meta_path = table_dir.join("_meta");
    let mut meta = TableMeta::load(&meta_path)?;

    meta.add_column(column_name, col_type)?;
    meta.save(&meta_path)?;

    // Fill NULL values in existing partitions.
    exchange_core::table::add_column_to_partitions(&table_dir, column_name, col_type)?;

    // Record in column version file.
    let cv_path = table_dir.join("_cv");
    let mut cv = exchange_core::column_version::ColumnVersionFile::load(&cv_path)?;
    cv.record_add(meta.version, column_name, &cv_path)?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_column(db_root: &Path, table: &str, column_name: &str) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta_path = table_dir.join("_meta");
    let mut meta = TableMeta::load(&meta_path)?;
    meta.drop_column(column_name)?;
    meta.save(&meta_path)?;

    // Delete column files from partitions.
    exchange_core::table::drop_column_from_partitions(&table_dir, column_name)?;

    // Record in column version file.
    let cv_path = table_dir.join("_cv");
    let mut cv = exchange_core::column_version::ColumnVersionFile::load(&cv_path)?;
    cv.record_drop(meta.version, column_name, &cv_path)?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_rename_column(
    db_root: &Path,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta_path = table_dir.join("_meta");
    let mut meta = TableMeta::load(&meta_path)?;
    meta.rename_column(old_name, new_name)?;
    meta.save(&meta_path)?;

    // Rename column files in partitions.
    exchange_core::table::rename_column_in_partitions(&table_dir, old_name, new_name)?;

    // Record in column version file.
    let cv_path = table_dir.join("_cv");
    let mut cv = exchange_core::column_version::ColumnVersionFile::load(&cv_path)?;
    cv.record_rename(meta.version, new_name, old_name, &cv_path)?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_set_column_type(
    db_root: &Path,
    table: &str,
    column_name: &str,
    new_type_name: &str,
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let new_col_type = type_name_to_column_type(new_type_name)?;
    let meta_path = table_dir.join("_meta");
    let mut meta = TableMeta::load(&meta_path)?;

    // Find old type name for version tracking.
    let old_col = meta
        .columns
        .iter()
        .find(|c| c.name == column_name)
        .ok_or_else(|| {
            ExchangeDbError::ColumnNotFound(column_name.to_string(), table.to_string())
        })?;
    let old_type: ColumnType = old_col.col_type.into();
    let old_type_name = format!("{old_type:?}");

    meta.set_column_type(column_name, new_col_type)?;
    meta.save(&meta_path)?;

    // Record in column version file.
    let cv_path = table_dir.join("_cv");
    let mut cv = exchange_core::column_version::ColumnVersionFile::load(&cv_path)?;
    cv.record_type_change(meta.version, column_name, &old_type_name, &cv_path)?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_delete(db_root: &Path, table: &str, filter: Option<&Filter>) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta = TableMeta::load(&table_dir.join("_meta"))?;
    let partitions = exchange_core::table::list_partitions(&table_dir)?;

    let mut deleted_count = 0u64;

    for partition_path in &partitions {
        let rows =
            exchange_core::table::read_partition_rows_tiered(partition_path, &meta, &table_dir)?;
        if rows.is_empty() {
            continue;
        }

        let original_count = rows.len();

        // Keep rows that do NOT match the filter.
        let kept_rows: Vec<Vec<ColumnValue<'_>>> = if let Some(f) = filter {
            rows.iter()
                .filter(|row| {
                    let values: Vec<(usize, Value)> = row
                        .iter()
                        .enumerate()
                        .map(|(i, cv)| (i, column_value_to_plan_value(cv, &meta, i)))
                        .collect();
                    !evaluate_filter(f, &values, &meta)
                })
                .map(|row| row.iter().map(|v| v.borrow_column_value()).collect())
                .collect()
        } else {
            // No filter means delete all rows.
            Vec::new()
        };

        let removed = original_count - kept_rows.len();
        if removed > 0 {
            exchange_core::table::rewrite_partition(partition_path, &meta, &kept_rows)?;
            deleted_count += removed as u64;
        }
    }

    // Invalidate caches after data modification.
    exchange_core::mmap::invalidate_mmap_cache(&table_dir);
    if let Some(reg) = crate::table_registry::global() {
        reg.invalidate(table);
    }

    Ok(QueryResult::Ok {
        affected_rows: deleted_count,
    })
}

fn execute_update(
    db_root: &Path,
    table: &str,
    assignments: &[(String, PlanExpr)],
    filter: Option<&Filter>,
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta = TableMeta::load(&table_dir.join("_meta"))?;
    let partitions = exchange_core::table::list_partitions(&table_dir)?;

    // Resolve assignment column indices.
    let assignment_indices: Vec<(usize, &PlanExpr)> = assignments
        .iter()
        .map(|(col_name, expr)| {
            let idx = meta
                .columns
                .iter()
                .position(|c| c.name == *col_name)
                .ok_or_else(|| {
                    ExchangeDbError::ColumnNotFound(col_name.clone(), table.to_string())
                });
            idx.map(|i| (i, expr))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut updated_count = 0u64;

    for partition_path in &partitions {
        let rows =
            exchange_core::table::read_partition_rows_tiered(partition_path, &meta, &table_dir)?;
        if rows.is_empty() {
            continue;
        }

        let mut modified = false;
        let mut new_rows: Vec<Vec<ColumnValue<'static>>> = Vec::with_capacity(rows.len());

        for row in &rows {
            let values: Vec<(usize, Value)> = row
                .iter()
                .enumerate()
                .map(|(i, cv)| (i, column_value_to_plan_value(cv, &meta, i)))
                .collect();

            let matches = if let Some(f) = filter {
                evaluate_filter(f, &values, &meta)
            } else {
                true
            };

            if matches {
                // Apply assignments to this row, evaluating expressions.
                let mut new_row = row.clone();
                for &(col_idx, expr) in &assignment_indices {
                    let val = evaluate_plan_expr(expr, &values, &meta);
                    new_row[col_idx] = plan_value_to_static_column_value(&val, &meta, col_idx);
                }
                new_rows.push(new_row);
                modified = true;
                updated_count += 1;
            } else {
                new_rows.push(row.clone());
            }
        }

        if modified {
            let borrowed_rows: Vec<Vec<ColumnValue<'_>>> = new_rows
                .iter()
                .map(|row| row.iter().map(|v| v.borrow_column_value()).collect())
                .collect();
            exchange_core::table::rewrite_partition(partition_path, &meta, &borrowed_rows)?;
        }
    }

    // Invalidate caches after data modification.
    exchange_core::mmap::invalidate_mmap_cache(&table_dir);
    if let Some(reg) = crate::table_registry::global() {
        reg.invalidate(table);
    }

    Ok(QueryResult::Ok {
        affected_rows: updated_count,
    })
}

/// Convert a ColumnValue back to a plan Value (for use in filter evaluation).
fn column_value_to_plan_value(cv: &ColumnValue<'_>, _meta: &TableMeta, _col_idx: usize) -> Value {
    match cv {
        ColumnValue::I64(v) => Value::I64(*v),
        ColumnValue::F64(v) if v.is_nan() => Value::Null,
        ColumnValue::F64(v) => Value::F64(*v),
        ColumnValue::I32(v) => Value::I64(*v as i64),
        ColumnValue::Timestamp(t) => Value::Timestamp(t.as_nanos()),
        ColumnValue::Str(s) => Value::Str(s.to_string()),
        ColumnValue::Bytes(_) => Value::Null,
        ColumnValue::Null => Value::Null,
    }
}

/// Convert a plan Value to a static ColumnValue for mutation.
fn plan_value_to_static_column_value(
    v: &Value,
    meta: &TableMeta,
    col_idx: usize,
) -> ColumnValue<'static> {
    let col_type: ColumnType = meta.columns[col_idx].col_type.into();
    match v {
        Value::I64(n) => match col_type {
            ColumnType::Timestamp => ColumnValue::Timestamp(Timestamp(*n)),
            ColumnType::I32 | ColumnType::Symbol => ColumnValue::I32(*n as i32),
            ColumnType::F64 => ColumnValue::F64(*n as f64),
            ColumnType::F32 => ColumnValue::F64(*n as f64),
            _ => ColumnValue::I64(*n),
        },
        Value::F64(n) => match col_type {
            ColumnType::I64 => ColumnValue::I64(*n as i64),
            ColumnType::I32 | ColumnType::Symbol => ColumnValue::I32(*n as i32),
            _ => ColumnValue::F64(*n),
        },
        Value::Timestamp(ns) => ColumnValue::Timestamp(Timestamp(*ns)),
        Value::Str(s) => ColumnValue::Str(Box::leak(s.clone().into_boxed_str())),
        Value::Null => ColumnValue::Null,
    }
}

fn execute_drop_table(db_root: &Path, table: &str) -> Result<QueryResult> {
    exchange_core::table::drop_table(db_root, table)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

#[allow(clippy::too_many_arguments)]
fn execute_select(
    db_root: &Path,
    table: &str,
    columns: &[SelectColumn],
    filter: Option<&Filter>,
    order_by: &[OrderBy],
    limit: Option<u64>,
    offset: Option<u64>,
    sample_by: Option<&SampleBy>,
    latest_on: Option<&LatestOn>,
    group_by: &[String],
    having: Option<&Filter>,
    distinct: bool,
) -> Result<QueryResult> {
    // Intercept catalog / information_schema queries.
    if crate::catalog::is_catalog_query(table) {
        return crate::catalog::execute_catalog_query(db_root, table, columns);
    }

    // Handle SELECT with no FROM for system functions (version(), etc.)
    if table == "__no_table__" && crate::catalog::is_system_function_query(columns) {
        return crate::catalog::execute_system_function(columns);
    }

    // Handle SELECT with no FROM for arbitrary expressions (SELECT 1, SELECT 1+1, etc.)
    if table == "__no_table__" {
        let mut col_names = Vec::new();
        let mut row = Vec::new();
        for col in columns.iter() {
            match col {
                SelectColumn::Expression { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| "?column?".to_string());
                    col_names.push(name);
                    let val = evaluate_plan_expr_standalone(expr);
                    row.push(val);
                }
                SelectColumn::ScalarFunction { name, args } => {
                    col_names.push(name.clone());
                    let func_args: Vec<Value> = args
                        .iter()
                        .map(|a| match a {
                            SelectColumnArg::Column(c) => Value::Str(c.clone()),
                            SelectColumnArg::Literal(v) => v.clone(),
                        })
                        .collect();
                    let val =
                        crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null);
                    row.push(val);
                }
                SelectColumn::Name(name) => {
                    col_names.push(name.clone());
                    row.push(Value::Null);
                }
                _ => {
                    col_names.push("?column?".to_string());
                    row.push(Value::Null);
                }
            }
        }
        return Ok(QueryResult::Rows {
            columns: col_names,
            rows: vec![row],
        });
    }

    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta = TableMeta::load(&table_dir.join("_meta"))?;

    // Resolve any subquery filters before scanning.
    let resolved_filter;
    let filter = if let Some(f) = filter {
        if has_subquery_filters(f) {
            resolved_filter = Some(resolve_subquery_filters(db_root, f, &HashMap::new())?);
            resolved_filter.as_ref()
        } else {
            Some(f)
        }
    } else {
        None
    };

    // ── Columnar fast path ──────────────────────────────────────────────
    // For simple aggregate queries (SELECT sum(x), count(*), ... FROM t)
    // without WHERE, GROUP BY, or SAMPLE BY, skip row materialisation
    // and operate directly on the column files via SIMD.
    let has_aggregates_early = columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Aggregate { .. }));
    let has_agg_filter = columns.iter().any(|c| {
        matches!(
            c,
            SelectColumn::Aggregate {
                filter: Some(_),
                ..
            }
        )
    });
    if has_aggregates_early
        && !has_agg_filter
        && filter.is_none()
        && group_by.is_empty()
        && sample_by.is_none()
        && latest_on.is_none()
        && order_by.is_empty()
        && !distinct
        && crate::columnar::can_use_columnar_path(columns, false, false, false)
    {
        let partition_dirs = exchange_core::table::list_partitions(&table_dir)?;
        let mut result_row = Vec::with_capacity(columns.len());
        let mut col_names = Vec::with_capacity(columns.len());

        let mut columnar_ok = true;
        for col in columns {
            if let SelectColumn::Aggregate {
                function,
                column,
                alias,
                ..
            } = col
            {
                let agg_col_name = alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}({})", function, column).to_lowercase());
                col_names.push(agg_col_name);

                let col_def = meta
                    .columns
                    .iter()
                    .find(|c| c.name == *column || column == "*");
                let col_type: ColumnType = if let Some(cd) = col_def {
                    cd.col_type.into()
                } else if column == "*" {
                    meta.columns[meta.timestamp_column].col_type.into()
                } else {
                    columnar_ok = false;
                    break;
                };

                let col_file_name = if column == "*" {
                    &meta.columns[meta.timestamp_column].name
                } else {
                    column
                };

                match crate::columnar::columnar_aggregate_partitions_tiered(
                    &partition_dirs,
                    col_file_name,
                    col_type,
                    *function,
                    Some(&table_dir),
                ) {
                    Ok(v) => result_row.push(v),
                    Err(_) => {
                        columnar_ok = false;
                        break;
                    }
                }
            }
        }

        if columnar_ok {
            return Ok(QueryResult::Rows {
                columns: col_names,
                rows: vec![result_row],
            });
        }
        // If columnar path failed, fall through to the regular path.
    }

    // For GROUP BY, ensure group-by columns are included in the scan.
    let mut scan_columns = columns.to_vec();
    for gb_col in group_by {
        let already_present = scan_columns.iter().any(|c| match c {
            SelectColumn::Name(n) => n == gb_col,
            SelectColumn::Wildcard => true,
            SelectColumn::Aggregate { column, .. } => column == gb_col,
            SelectColumn::ScalarFunction { .. } => false,
            SelectColumn::WindowFunction(_) | SelectColumn::ScalarSubquery { .. } => false,
            SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
            SelectColumn::Expression { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
        });
        if !already_present {
            // Don't add as a table column if it's an alias for a CASE WHEN or Expression.
            let is_alias = columns.iter().any(|c| match c {
                SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
                SelectColumn::Expression { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
                _ => false,
            });
            if !is_alias {
                scan_columns.push(SelectColumn::Name(gb_col.clone()));
            }
        }
    }

    // For aggregate FILTER clauses, ensure the columns referenced in the
    // filter are included in the scan.
    for col in columns {
        if let SelectColumn::Aggregate {
            filter: Some(flt), ..
        } = col
        {
            let filter_cols = collect_filter_column_names(flt);
            for fc in filter_cols {
                let already = scan_columns.iter().any(|c| match c {
                    SelectColumn::Name(n) => n == &fc,
                    SelectColumn::Wildcard => true,
                    _ => false,
                });
                if !already {
                    scan_columns.push(SelectColumn::Name(fc));
                }
            }
        }
    }

    // For SAMPLE BY, ensure the timestamp column is included in the scan
    // so that rows can be bucketed by time.
    if sample_by.is_some() {
        let ts_col_name = &meta.columns[meta.timestamp_column].name;
        let already_present = scan_columns.iter().any(|c| match c {
            SelectColumn::Name(n) => n == ts_col_name,
            SelectColumn::Wildcard => true,
            _ => false,
        });
        if !already_present {
            scan_columns.push(SelectColumn::Name(ts_col_name.clone()));
        }
    }

    // For LATEST ON, ensure the timestamp and partition columns are included
    // in the scan so that latest-row selection can work even when the user
    // selects only a subset of columns.
    let mut extra_latest_cols: Vec<String> = Vec::new();
    if let Some(lo) = latest_on {
        for col_name in [&lo.timestamp_col, &lo.partition_col] {
            let already_present = scan_columns.iter().any(|c| match c {
                SelectColumn::Name(n) => n == col_name,
                SelectColumn::Wildcard => true,
                _ => false,
            });
            if !already_present {
                scan_columns.push(SelectColumn::Name(col_name.clone()));
                extra_latest_cols.push(col_name.clone());
            }
        }
    }

    // Determine which columns to read.
    let selected_cols = resolve_columns(&meta, &scan_columns)?;

    // Scan all partitions – use parallel scan when there are enough
    // partitions to benefit from concurrency.
    let mut all_rows =
        crate::parallel::parallel_scan_partitions(&table_dir, &meta, &selected_cols, filter)?;

    // If LATEST ON is requested, keep only the most recent row per partition.
    if let Some(lo) = latest_on {
        let ts_idx = selected_cols
            .iter()
            .position(|(_, name)| name == &lo.timestamp_col)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(lo.timestamp_col.clone(), table.to_string())
            })?;
        let part_idx = selected_cols
            .iter()
            .position(|(_, name)| name == &lo.partition_col)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(lo.partition_col.clone(), table.to_string())
            })?;
        all_rows = crate::latest::latest_on(&all_rows, ts_idx, part_idx);

        // Remove extra columns that were added only for LATEST ON processing.
        if !extra_latest_cols.is_empty() {
            let keep_count = selected_cols.len() - extra_latest_cols.len();
            for row in &mut all_rows {
                row.truncate(keep_count);
            }
        }
    }

    // If SAMPLE BY is requested, perform time-bucketed aggregation.
    if let Some(sb) = sample_by {
        all_rows = apply_sample_by(&meta, columns, &selected_cols, all_rows, sb)?;
    }

    // GROUP BY with aggregation
    let has_aggregates = columns.iter().any(|c| match c {
        SelectColumn::Aggregate { .. } => true,
        SelectColumn::Expression { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    });
    if !group_by.is_empty() {
        all_rows = apply_group_by(columns, &selected_cols, &all_rows, group_by, having)?;
    } else if has_aggregates && sample_by.is_none() {
        // If aggregate functions without GROUP BY or SAMPLE BY, collapse into single row.
        all_rows = apply_aggregates(columns, &selected_cols, &all_rows)?;
    }

    // Check if we have scalar functions (CaseWhen/Expression) that need evaluation.
    let has_scalar_fns = columns.iter().any(|c| {
        matches!(
            c,
            SelectColumn::ScalarFunction { .. }
                | SelectColumn::CaseWhen { .. }
                | SelectColumn::Expression { .. }
        )
    });
    // When we have scalar functions AND DISTINCT, defer DISTINCT until after evaluation.
    let deferred_distinct =
        distinct && has_scalar_fns && !has_aggregates && group_by.is_empty() && sample_by.is_none();

    // DISTINCT (applied before ORDER BY so ORDER BY sorts the distinct rows)
    if distinct && !deferred_distinct {
        all_rows = apply_distinct(all_rows);
    }

    // ORDER BY — use TopK heap when LIMIT is small, full sort otherwise.
    if !order_by.is_empty() {
        let resolved_cols = if !group_by.is_empty() || has_aggregates || sample_by.is_some() {
            let result_cols = result_column_names(columns, &selected_cols);
            result_cols.into_iter().enumerate().collect::<Vec<_>>()
        } else {
            selected_cols.clone()
        };

        // TopK optimization: when LIMIT is small (<=1000), use a bounded
        // binary heap instead of sorting all rows. O(n log K) vs O(n log n).
        let effective_k = limit.map(|l| l as usize + offset.unwrap_or(0) as usize);
        if let Some(k) = effective_k {
            if k <= 1000 && k < all_rows.len() {
                all_rows = apply_topk(&all_rows, &resolved_cols, order_by, k);
            } else {
                apply_order_by(&mut all_rows, &resolved_cols, order_by);
            }
        } else {
            apply_order_by(&mut all_rows, &resolved_cols, order_by);
        }
    }

    // OFFSET (applied before LIMIT per SQL standard)
    if let Some(off) = offset {
        let off = off as usize;
        if off >= all_rows.len() {
            all_rows.clear();
        } else {
            all_rows.drain(..off);
        }
    }

    // Apply scalar functions: transform rows to produce the final select list.
    if has_scalar_fns && !has_aggregates && group_by.is_empty() && sample_by.is_none() {
        all_rows = apply_scalar_functions(columns, &selected_cols, all_rows)?;
    }

    // Apply deferred DISTINCT after scalar functions have been evaluated.
    if deferred_distinct {
        all_rows = apply_distinct(all_rows);
    }

    // Apply window functions (computed BEFORE DISTINCT/ORDER BY/LIMIT per SQL standard).
    let window_fns: Vec<crate::window::WindowFunction> = columns
        .iter()
        .filter_map(|c| match c {
            SelectColumn::WindowFunction(wf) => Some(wf.clone()),
            _ => None,
        })
        .collect();

    if !window_fns.is_empty() {
        return execute_window_projection(
            &mut all_rows,
            columns,
            &selected_cols,
            &window_fns,
            order_by,
            limit,
            offset,
            distinct,
        );
    }

    // LIMIT (non-window path)
    if let Some(lim) = limit {
        all_rows.truncate(lim as usize);
    }

    let col_names = result_column_names(columns, &selected_cols);

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: all_rows,
    })
}

/// Shared helper: compute window functions, project, then apply ORDER BY / OFFSET / LIMIT.
#[allow(clippy::too_many_arguments)]
fn execute_window_projection(
    all_rows: &mut [Vec<Value>],
    columns: &[SelectColumn],
    selected_cols: &[(usize, String)],
    window_fns: &[crate::window::WindowFunction],
    order_by: &[OrderBy],
    limit: Option<u64>,
    offset: Option<u64>,
    _distinct: bool,
) -> Result<QueryResult> {
    let base_col_names: Vec<String> = selected_cols.iter().map(|(_, n)| n.clone()).collect();
    let extra_window_col_names =
        crate::window::apply_window_functions(all_rows, &base_col_names, window_fns);

    // If no explicit ORDER BY, sort by the first window function's ORDER BY
    // (before projection, since these columns are still in the base layout).
    if order_by.is_empty() {
        let first_wf_order = &window_fns[0].over.order_by;
        if !first_wf_order.is_empty() {
            apply_order_by(all_rows, selected_cols, first_wf_order);
        }
    }

    // Build final column names and projection indices.
    let num_base = selected_cols.len();
    let mut col_names = Vec::new();
    let mut output_indices: Vec<usize> = Vec::new();
    let mut win_idx = 0;
    for col in columns {
        match col {
            SelectColumn::WindowFunction(_) => {
                col_names.push(extra_window_col_names[win_idx].clone());
                output_indices.push(num_base + win_idx);
                win_idx += 1;
            }
            SelectColumn::Wildcard => {
                for (i, (_, n)) in selected_cols.iter().enumerate() {
                    col_names.push(n.clone());
                    output_indices.push(i);
                }
            }
            SelectColumn::Name(n) => {
                col_names.push(n.clone());
                if let Some(pos) = selected_cols.iter().position(|(_, cn)| cn == n) {
                    output_indices.push(pos);
                }
            }
            SelectColumn::Aggregate {
                function,
                column,
                alias,
                ..
            } => {
                let cl = alias.clone().unwrap_or_else(|| {
                    let fn_name = format!("{function:?}").to_ascii_lowercase();
                    format!("{fn_name}({column})")
                });
                col_names.push(cl);
                if let Some(pos) = selected_cols.iter().position(|(_, cn)| cn == column) {
                    output_indices.push(pos);
                }
            }
            SelectColumn::ScalarFunction { name, .. } => {
                col_names.push(name.clone());
            }
            SelectColumn::CaseWhen { alias, .. } => {
                col_names.push(alias.clone().unwrap_or_else(|| "case".to_string()));
            }
            SelectColumn::Expression { alias, .. } => {
                col_names.push(alias.clone().unwrap_or_else(|| "expr".to_string()));
            }
            SelectColumn::ScalarSubquery { alias, .. } => {
                col_names.push(alias.clone().unwrap_or_else(|| "subquery".to_string()));
            }
        }
    }

    // Project rows to output columns.
    let mut projected: Vec<Vec<Value>> = all_rows
        .iter()
        .map(|row| output_indices.iter().map(|&i| row[i].clone()).collect())
        .collect();

    // Apply explicit ORDER BY on projected columns (after projection so aliases are available).
    if !order_by.is_empty() {
        let proj_resolved: Vec<(usize, String)> = col_names
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n.clone()))
            .collect();
        apply_order_by(&mut projected, &proj_resolved, order_by);
    }

    // OFFSET then LIMIT.
    if let Some(off) = offset {
        let off = off as usize;
        if off >= projected.len() {
            projected.clear();
        } else {
            projected.drain(..off);
        }
    }
    if let Some(lim) = limit {
        projected.truncate(lim as usize);
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: projected,
    })
}

/// Like `execute_select` but accepts optimizer hints for pruned partitions
/// and limit pushdown.
/// Scan rows directly from pre-opened table handles (zero file-open overhead).
fn scan_from_open_table(
    open_table: &std::sync::Arc<crate::table_registry::OpenTable>,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    row_limit: Option<u64>,
    meta: &TableMeta,
) -> Result<Vec<Vec<Value>>> {
    let limit = row_limit.unwrap_or(u64::MAX) as usize;
    let mut rows = Vec::with_capacity(limit.min(1024));

    for part in &open_table.partitions {
        if rows.len() >= limit {
            break;
        }
        let remaining = limit - rows.len();
        let scan_count = (part.row_count as usize).min(remaining);

        // Build column index map: selected col index -> position in part.columns
        let col_positions: Vec<Option<usize>> = selected_cols
            .iter()
            .map(|(_, name)| part.columns.iter().position(|(cn, _)| cn == name))
            .collect();

        for row_idx in 0..scan_count {
            // Read all values for filter evaluation if needed.
            if let Some(f) = filter {
                let filter_values: Vec<(usize, Value)> = part
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, (_, col))| {
                        let meta_idx = meta
                            .columns
                            .iter()
                            .position(|c| c.name == part.columns[i].0)
                            .unwrap_or(i);
                        (meta_idx, read_open_column_value(col, row_idx as u64))
                    })
                    .collect();
                if !crate::parallel::evaluate_filter(f, &filter_values, meta) {
                    continue;
                }
            }

            let mut row = Vec::with_capacity(selected_cols.len());
            for pos in &col_positions {
                match pos {
                    Some(p) => {
                        row.push(read_open_column_value(&part.columns[*p].1, row_idx as u64))
                    }
                    None => row.push(Value::Null),
                }
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

/// Read a value from an OpenColumn.
#[inline]
fn read_open_column_value(col: &crate::table_registry::OpenColumn, row: u64) -> Value {
    use crate::table_registry::OpenColumn;
    match col {
        OpenColumn::Fixed(reader, ct) => match ct {
            ColumnType::I64 | ColumnType::Timestamp => Value::I64(reader.read_i64(row)),
            ColumnType::F64 => Value::F64(reader.read_f64(row)),
            ColumnType::F32 => {
                let raw = reader.read_raw(row);
                Value::F64(f32::from_le_bytes(raw.try_into().unwrap_or_default()) as f64)
            }
            ColumnType::I32 | ColumnType::Symbol => Value::I64(reader.read_i32(row) as i64),
            ColumnType::I16 => {
                let raw = reader.read_raw(row);
                Value::I64(i16::from_le_bytes(raw.try_into().unwrap_or_default()) as i64)
            }
            ColumnType::I8 | ColumnType::Boolean => {
                let raw = reader.read_raw(row);
                Value::I64(raw[0] as i8 as i64)
            }
            _ => Value::I64(reader.read_i64(row)),
        },
        OpenColumn::Var(reader, _) => {
            let bytes = reader.read(row);
            if bytes.is_empty() {
                Value::Null
            } else {
                Value::Str(String::from_utf8_lossy(bytes).to_string())
            }
        }
    }
}

/// Fallback: scan from disk with file-based readers.
fn scan_from_disk(
    table_dir: &Path,
    meta: &TableMeta,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
    pruned_partitions: Option<&[PathBuf]>,
    limit_pushdown: Option<&crate::optimizer::LimitPushdown>,
) -> Result<Vec<Vec<Value>>> {
    let mut partitions_list = if let Some(pp) = pruned_partitions {
        pp.to_vec()
    } else {
        exchange_core::table::list_partitions(table_dir)?
    };
    if let Some(lp) = limit_pushdown
        && lp.reverse_scan
    {
        partitions_list.reverse();
    }
    crate::parallel::parallel_scan_partitions_pruned_tiered(
        &partitions_list,
        meta,
        selected_cols,
        filter,
        limit_pushdown.map(|lp| lp.limit),
        Some(table_dir),
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_select_with_hints(
    db_root: &Path,
    table: &str,
    columns: &[SelectColumn],
    filter: Option<&Filter>,
    order_by: &[OrderBy],
    limit: Option<u64>,
    offset: Option<u64>,
    sample_by: Option<&SampleBy>,
    latest_on: Option<&LatestOn>,
    group_by: &[String],
    group_by_mode: &GroupByMode,
    having: Option<&Filter>,
    distinct: bool,
    distinct_on: &[String],
    pruned_partitions: Option<&[PathBuf]>,
    limit_pushdown: Option<&crate::optimizer::LimitPushdown>,
) -> Result<QueryResult> {
    // Intercept catalog / information_schema queries.
    if crate::catalog::is_catalog_query(table) {
        return crate::catalog::execute_catalog_query(db_root, table, columns);
    }

    // Handle SELECT with no FROM for system functions (version(), current_database(), etc.)
    if table == "__no_table__" && crate::catalog::is_system_function_query(columns) {
        return crate::catalog::execute_system_function(columns);
    }

    // Handle SELECT with no FROM for arbitrary expressions (SELECT 1, SELECT 1+1, etc.)
    if table == "__no_table__" {
        let mut col_names = Vec::new();
        let mut row = Vec::new();
        for col in columns.iter() {
            match col {
                SelectColumn::Expression { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| "?column?".to_string());
                    col_names.push(name);
                    let val = evaluate_plan_expr_standalone(expr);
                    row.push(val);
                }
                SelectColumn::ScalarFunction { name, args } => {
                    col_names.push(name.clone());
                    let func_args: Vec<Value> = args
                        .iter()
                        .map(|a| match a {
                            SelectColumnArg::Column(c) => Value::Str(c.clone()),
                            SelectColumnArg::Literal(v) => v.clone(),
                        })
                        .collect();
                    let val =
                        crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null);
                    row.push(val);
                }
                SelectColumn::Name(name) => {
                    col_names.push(name.clone());
                    row.push(Value::Null);
                }
                _ => {
                    col_names.push("?column?".to_string());
                    row.push(Value::Null);
                }
            }
        }
        return Ok(QueryResult::Rows {
            columns: col_names,
            rows: vec![row],
        });
    }

    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }

    let meta = TableMeta::load(&table_dir.join("_meta"))?;

    // Resolve any subquery filters before scanning.
    let resolved_filter;
    let filter = if let Some(f) = filter {
        if has_subquery_filters(f) {
            resolved_filter = Some(resolve_subquery_filters(db_root, f, &HashMap::new())?);
            resolved_filter.as_ref()
        } else {
            Some(f)
        }
    } else {
        None
    };

    // ── Vectorized / Parallel GROUP BY fast path ──────────────────────
    // When the SELECT has a GROUP BY with only simple aggregates and
    // no complex expressions/joins/SAMPLE BY/LATEST ON, we can bypass
    // row materialisation and process columns directly.
    let has_agg_filter_hints = columns.iter().any(|c| {
        matches!(
            c,
            SelectColumn::Aggregate {
                filter: Some(_),
                ..
            }
        )
    });
    if !group_by.is_empty()
        && sample_by.is_none()
        && latest_on.is_none()
        && having.is_none()
        && !has_agg_filter_hints
        && crate::vector_groupby::can_use_vector_groupby(columns, group_by, false, false)
    {
        // Extract aggregates from select columns.
        let aggregates: Vec<(AggregateKind, String)> = columns
            .iter()
            .filter_map(|c| match c {
                SelectColumn::Aggregate {
                    function, column, ..
                } => Some((*function, column.clone())),
                _ => None,
            })
            .collect();

        // Determine partitions to scan.
        let partitions = if let Some(pp) = pruned_partitions {
            pp.to_vec()
        } else {
            exchange_core::table::list_partitions(&table_dir)?
        };

        let owned_filter = filter.cloned();

        if partitions.len() > 1 {
            // Use parallel GROUP BY across multiple partitions.
            let mut result = crate::parallel_groupby::parallel_group_by(
                db_root,
                table,
                &meta,
                &partitions,
                group_by,
                &aggregates,
                &owned_filter,
                columns,
            )?;

            // Apply ORDER BY / LIMIT / DISTINCT on the result.
            apply_post_groupby(&mut result, order_by, limit, distinct);
            return Ok(result);
        } else if !partitions.is_empty() {
            // Single partition: use vectorized GROUP BY.
            let mut result = crate::vector_groupby::vector_group_by(
                db_root,
                table,
                &meta,
                &partitions,
                group_by,
                &aggregates,
                &owned_filter,
                columns,
            )?;

            apply_post_groupby(&mut result, order_by, limit, distinct);
            return Ok(result);
        }
    }

    // For GROUP BY, ensure group-by columns are included in the scan.
    let mut scan_columns = columns.to_vec();
    for gb_col in group_by {
        let already_present = scan_columns.iter().any(|c| match c {
            SelectColumn::Name(n) => n == gb_col,
            SelectColumn::Wildcard => true,
            SelectColumn::Aggregate { column, .. } => column == gb_col,
            SelectColumn::ScalarFunction { .. } => false,
            SelectColumn::WindowFunction(_) | SelectColumn::ScalarSubquery { .. } => false,
            SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
            SelectColumn::Expression { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
        });
        if !already_present {
            // Don't add as a table column if it's an alias for a CASE WHEN or Expression.
            let is_alias = columns.iter().any(|c| match c {
                SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
                SelectColumn::Expression { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
                _ => false,
            });
            if !is_alias {
                scan_columns.push(SelectColumn::Name(gb_col.clone()));
            }
        }
    }

    // For aggregate FILTER clauses, ensure filter columns are in the scan.
    for col in columns {
        if let SelectColumn::Aggregate {
            filter: Some(flt), ..
        } = col
        {
            let filter_cols = collect_filter_column_names(flt);
            for fc in filter_cols {
                let already = scan_columns.iter().any(|c| match c {
                    SelectColumn::Name(n) => n == &fc,
                    SelectColumn::Wildcard => true,
                    _ => false,
                });
                if !already {
                    scan_columns.push(SelectColumn::Name(fc));
                }
            }
        }
    }

    // For ORDER BY, ensure order-by columns are included in the scan.
    // Skip columns that are window function aliases (they don't exist in the table schema).
    let window_aliases: Vec<String> = columns
        .iter()
        .filter_map(|c| match c {
            SelectColumn::WindowFunction(wf) => wf.alias.clone().or_else(|| Some(wf.to_string())),
            _ => None,
        })
        .collect();
    // Collect expression/case-when aliases that ORDER BY might reference.
    let expr_aliases: Vec<String> = columns
        .iter()
        .filter_map(|c| match c {
            SelectColumn::Expression { alias: Some(a), .. } => Some(a.clone()),
            SelectColumn::CaseWhen { alias: Some(a), .. } => Some(a.clone()),
            _ => None,
        })
        .collect();
    let mut extra_order_cols = Vec::new();
    for ob in order_by {
        if window_aliases.iter().any(|a| a == &ob.column) {
            continue; // Window function alias; will be resolved after projection.
        }
        let already_present = scan_columns.iter().any(|c| match c {
            SelectColumn::Name(n) => n == &ob.column,
            SelectColumn::Wildcard => true,
            SelectColumn::Aggregate {
                function,
                column,
                alias,
                ..
            } => {
                if column == &ob.column || alias.as_deref() == Some(&ob.column) {
                    true
                } else {
                    // Also match the generated aggregate name, e.g. "sum(d)"
                    let func_name = format!("{function:?}").to_ascii_lowercase();
                    let full_name = format!("{func_name}({column})");
                    full_name == ob.column
                }
            }
            SelectColumn::Expression { alias, expr, .. } => {
                alias.as_deref() == Some(&ob.column) || format!("{expr:?}") == ob.column
            }
            SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(&ob.column),
            SelectColumn::ScalarSubquery { alias, .. } => alias.as_deref() == Some(&ob.column),
            _ => false,
        });
        if !already_present {
            // Check if the ORDER BY column is an expression-based string (not a simple identifier).
            // If it contains operators or spaces, it's an expression that should match a SELECT
            // alias rather than a table column. Skip adding as a scan column.
            let is_expression = ob.column.contains(' ')
                || ob.column.contains('*')
                || ob.column.contains('+')
                || ob.column.contains('-')
                || ob.column.contains('/');
            // Also skip if it matches an expression/case-when alias.
            let is_alias = expr_aliases.iter().any(|a| a == &ob.column);
            if !is_expression && !is_alias {
                scan_columns.push(SelectColumn::Name(ob.column.clone()));
                extra_order_cols.push(ob.column.clone());
            }
        }
    }

    // For SAMPLE BY, ensure the timestamp column is included in the scan.
    if sample_by.is_some() {
        let ts_col_name = &meta.columns[meta.timestamp_column].name;
        let already_present = scan_columns.iter().any(|c| match c {
            SelectColumn::Name(n) => n == ts_col_name,
            SelectColumn::Wildcard => true,
            _ => false,
        });
        if !already_present {
            scan_columns.push(SelectColumn::Name(ts_col_name.clone()));
        }
    }

    // For LATEST ON, ensure the timestamp and partition columns are included
    // in the scan so that latest-row selection can work even when the user
    // selects only a subset of columns.
    let mut extra_latest_cols2: Vec<String> = Vec::new();
    if let Some(lo) = latest_on {
        for col_name in [&lo.timestamp_col, &lo.partition_col] {
            let already_present = scan_columns.iter().any(|c| match c {
                SelectColumn::Name(n) => n == col_name,
                SelectColumn::Wildcard => true,
                _ => false,
            });
            if !already_present {
                scan_columns.push(SelectColumn::Name(col_name.clone()));
                extra_latest_cols2.push(col_name.clone());
            }
        }
    }

    // Determine which columns to read.
    let selected_cols = resolve_columns(&meta, &scan_columns)?;

    // Try the hot table registry first for zero-overhead scanning.
    // Falls back to standard file-based scan if registry is not initialized.
    let mut all_rows = if let Some(registry) = crate::table_registry::global() {
        if let Ok(open_table) = registry.get(table) {
            scan_from_open_table(
                &open_table,
                &selected_cols,
                filter,
                limit_pushdown.map(|lp| lp.limit),
                &meta,
            )?
        } else {
            scan_from_disk(
                &table_dir,
                &meta,
                &selected_cols,
                filter,
                pruned_partitions,
                limit_pushdown,
            )?
        }
    } else {
        scan_from_disk(
            &table_dir,
            &meta,
            &selected_cols,
            filter,
            pruned_partitions,
            limit_pushdown,
        )?
    };

    // If LATEST ON is requested, keep only the most recent row per partition.
    if let Some(lo) = latest_on {
        let ts_idx = selected_cols
            .iter()
            .position(|(_, name)| name == &lo.timestamp_col)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(lo.timestamp_col.clone(), table.to_string())
            })?;
        let part_idx = selected_cols
            .iter()
            .position(|(_, name)| name == &lo.partition_col)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(lo.partition_col.clone(), table.to_string())
            })?;
        all_rows = crate::latest::latest_on(&all_rows, ts_idx, part_idx);

        // Remove extra columns that were added only for LATEST ON processing.
        if !extra_latest_cols2.is_empty() {
            let keep_count = selected_cols.len() - extra_latest_cols2.len();
            for row in &mut all_rows {
                row.truncate(keep_count);
            }
        }
    }

    // If SAMPLE BY is requested, perform time-bucketed aggregation.
    if let Some(sb) = sample_by {
        all_rows = apply_sample_by(&meta, columns, &selected_cols, all_rows, sb)?;
    }

    // GROUP BY with aggregation (hints path)
    let has_aggregates = columns.iter().any(|c| match c {
        SelectColumn::Aggregate { .. } => true,
        SelectColumn::Expression { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    });
    if !group_by.is_empty() {
        match group_by_mode {
            GroupByMode::Normal => {
                all_rows = apply_group_by(columns, &selected_cols, &all_rows, group_by, having)?;
            }
            GroupByMode::GroupingSets(sets) => {
                all_rows = apply_grouping_sets(
                    columns,
                    &selected_cols,
                    &all_rows,
                    group_by,
                    sets,
                    having,
                )?;
            }
            GroupByMode::Rollup(cols) => {
                let sets = expand_rollup(cols);
                all_rows = apply_grouping_sets(
                    columns,
                    &selected_cols,
                    &all_rows,
                    group_by,
                    &sets,
                    having,
                )?;
            }
            GroupByMode::Cube(cols) => {
                let sets = expand_cube(cols);
                all_rows = apply_grouping_sets(
                    columns,
                    &selected_cols,
                    &all_rows,
                    group_by,
                    &sets,
                    having,
                )?;
            }
        }
    } else if has_aggregates && sample_by.is_none() {
        all_rows = apply_aggregates(columns, &selected_cols, &all_rows)?;
    }

    // Check if we have scalar functions (CaseWhen/Expression) that need evaluation.
    let has_scalar_fns2 = columns.iter().any(|c| {
        matches!(
            c,
            SelectColumn::ScalarFunction { .. }
                | SelectColumn::CaseWhen { .. }
                | SelectColumn::Expression { .. }
        )
    });
    let deferred_distinct2 = distinct
        && has_scalar_fns2
        && !has_aggregates
        && group_by.is_empty()
        && sample_by.is_none();

    // DISTINCT (applied before ORDER BY so ORDER BY sorts the distinct rows)
    if distinct && !deferred_distinct2 {
        all_rows = apply_distinct(all_rows);
    }

    // ORDER BY — use TopK heap when LIMIT is small, full sort otherwise.
    if !order_by.is_empty() {
        let resolved_cols = if !group_by.is_empty() || has_aggregates || sample_by.is_some() {
            let result_cols = result_column_names(columns, &selected_cols);
            result_cols.into_iter().enumerate().collect::<Vec<_>>()
        } else {
            selected_cols.clone()
        };

        let effective_k = limit.map(|l| l as usize + offset.unwrap_or(0) as usize);
        if let Some(k) = effective_k {
            if k <= 1000 && k < all_rows.len() {
                all_rows = apply_topk(&all_rows, &resolved_cols, order_by, k);
            } else {
                apply_order_by(&mut all_rows, &resolved_cols, order_by);
            }
        } else {
            apply_order_by(&mut all_rows, &resolved_cols, order_by);
        }
    }

    // DISTINCT ON: after ORDER BY, keep only the first row per distinct-on key.
    if !distinct_on.is_empty() {
        let col_indices: Vec<usize> = distinct_on
            .iter()
            .filter_map(|name| selected_cols.iter().position(|(_, n)| n == name))
            .collect();
        if !col_indices.is_empty() {
            let mut seen = HashSet::new();
            let mut filtered = Vec::new();
            for row in all_rows {
                let key: Vec<ValueKey> = col_indices
                    .iter()
                    .map(|&i| ValueKey(row.get(i).cloned().unwrap_or(Value::Null)))
                    .collect();
                if seen.insert(key) {
                    filtered.push(row);
                }
            }
            all_rows = filtered;
        }
    }

    // Apply window functions (computed BEFORE LIMIT per SQL standard).
    let window_fns: Vec<crate::window::WindowFunction> = columns
        .iter()
        .filter_map(|c| match c {
            SelectColumn::WindowFunction(wf) => Some(wf.clone()),
            _ => None,
        })
        .collect();

    if !window_fns.is_empty() {
        return execute_window_projection(
            &mut all_rows,
            columns,
            &selected_cols,
            &window_fns,
            order_by,
            limit,
            offset,
            distinct,
        );
    }

    // Remove extra ORDER BY columns that were added for sorting but not
    // part of the original SELECT.
    if !extra_order_cols.is_empty() && group_by.is_empty() && !has_aggregates {
        let original_count = selected_cols.len() - extra_order_cols.len();
        for row in &mut all_rows {
            row.truncate(original_count);
        }
    }

    // OFFSET
    if let Some(off) = offset {
        if (off as usize) < all_rows.len() {
            all_rows = all_rows.split_off(off as usize);
        } else {
            all_rows.clear();
        }
    }

    // Apply scalar functions.
    if has_scalar_fns2 && !has_aggregates && group_by.is_empty() && sample_by.is_none() {
        all_rows = apply_scalar_functions(columns, &selected_cols, all_rows)?;
    }

    // Apply deferred DISTINCT after scalar functions have been evaluated.
    if deferred_distinct2 {
        all_rows = apply_distinct(all_rows);
    }

    // LIMIT (non-window path)
    if let Some(lim) = limit {
        all_rows.truncate(lim as usize);
    }

    let col_names = result_column_names(columns, &selected_cols);

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: all_rows,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_asof_join(
    db_root: &Path,
    left_table: &str,
    right_table: &str,
    left_columns: &[SelectColumn],
    right_columns: &[SelectColumn],
    on_columns: &[(String, String)],
    _filter: Option<&Filter>,
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

    let left_meta = TableMeta::load(&left_dir.join("_meta"))?;
    let right_meta = TableMeta::load(&right_dir.join("_meta"))?;

    // Always read ALL columns for the left side so that timestamp and ON columns
    // are available for the ASOF join matching logic.
    let left_resolved = resolve_columns(&left_meta, &[SelectColumn::Wildcard])?;
    let mut left_rows = scan_table(&left_dir, &left_meta, &left_resolved, None)?;

    // Sort left rows by timestamp.
    let left_ts_col = left_meta.timestamp_column;
    let left_ts_pos = left_resolved
        .iter()
        .position(|(idx, _)| *idx == left_ts_col)
        .unwrap_or(0);
    left_rows.sort_by(|a, b| {
        a[left_ts_pos]
            .partial_cmp(&b[left_ts_pos])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // For the right side, read all columns.
    let right_all_cols: Vec<SelectColumn> = vec![SelectColumn::Wildcard];
    let right_resolved = resolve_columns(&right_meta, &right_all_cols)?;
    let mut right_rows = scan_table(&right_dir, &right_meta, &right_resolved, None)?;

    // Sort right rows by timestamp.
    let right_ts_col = right_meta.timestamp_column;
    let right_ts_pos = right_resolved
        .iter()
        .position(|(idx, _)| *idx == right_ts_col)
        .unwrap_or(0);
    right_rows.sort_by(|a, b| {
        a[right_ts_pos]
            .partial_cmp(&b[right_ts_pos])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Resolve ON columns to indices in the resolved columns.
    let on_col_pairs: Vec<(usize, usize)> = on_columns
        .iter()
        .filter_map(|(left_name, right_name)| {
            let left_idx = left_resolved.iter().position(|(_, n)| n == left_name)?;
            let right_idx = right_resolved.iter().position(|(_, n)| n == right_name)?;
            Some((left_idx, right_idx))
        })
        .collect();

    // Determine which right columns to output (those explicitly selected, or all
    // non-join/non-ts columns if wildcard).
    let right_output_cols: Vec<usize> = if right_columns.is_empty() {
        // No right columns explicitly selected; don't output any.
        Vec::new()
    } else if right_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Wildcard))
    {
        // All right columns except those used for joining and timestamp.
        let on_right_indices: Vec<usize> = on_col_pairs.iter().map(|(_, ri)| *ri).collect();
        (0..right_resolved.len())
            .filter(|i| *i != right_ts_pos && !on_right_indices.contains(i))
            .collect()
    } else {
        right_columns
            .iter()
            .filter_map(|c| match c {
                SelectColumn::Name(name) => right_resolved.iter().position(|(_, n)| n == name),
                _ => None,
            })
            .collect()
    };

    // Perform the ASOF JOIN.
    let result_rows = crate::asof::asof_join(
        &left_rows,
        &right_rows,
        left_ts_pos,
        right_ts_pos,
        &on_col_pairs,
        &right_output_cols,
    );

    // Build full column names for the combined result.
    let mut all_col_names: Vec<String> = left_resolved.iter().map(|(_, n)| n.clone()).collect();
    for &idx in &right_output_cols {
        all_col_names.push(right_resolved[idx].1.clone());
    }

    // Project output to only the requested columns (from left_columns and right_columns).
    // If left_columns contains Wildcard, include all left columns; otherwise select by name.
    // Similarly for right_columns.
    let left_has_wildcard = left_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Wildcard));
    let left_has_aggregates = left_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Aggregate { .. }));

    // If there are aggregates, wrap in a virtual query execution.
    if left_has_aggregates {
        // Execute aggregation over the combined result.
        return execute_select_from_virtual(
            &all_col_names,
            &result_rows,
            left_columns,
            None,
            order_by,
            limit,
            &[],
            None,
            false,
            &HashMap::new(),
        );
    }

    let mut output_indices: Vec<usize> = Vec::new();
    let mut col_names: Vec<String> = Vec::new();
    let left_count = left_resolved.len();

    if left_has_wildcard {
        // Include all left columns.
        for (i, name) in all_col_names.iter().enumerate().take(left_count) {
            output_indices.push(i);
            col_names.push(name.clone());
        }
    } else {
        // Include only the explicitly requested left columns.
        for lc in left_columns {
            if let SelectColumn::Name(name) = lc
                && let Some(i) = left_resolved.iter().position(|(_, n)| n == name)
            {
                output_indices.push(i);
                col_names.push(name.clone());
            }
        }
    }

    let right_has_wildcard = right_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Wildcard));
    if right_has_wildcard {
        for (i, &col_idx) in right_output_cols.iter().enumerate() {
            output_indices.push(left_count + i);
            col_names.push(right_resolved[col_idx].1.clone());
        }
    } else {
        for rc in right_columns {
            if let SelectColumn::Name(name) = rc
                && let Some(i) = right_output_cols
                    .iter()
                    .position(|&idx| right_resolved[idx].1 == *name)
            {
                output_indices.push(left_count + i);
                col_names.push(name.clone());
            }
        }
    }

    // Apply projection.
    let mut result_rows: Vec<Vec<Value>> = result_rows
        .into_iter()
        .map(|row| {
            output_indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    // ORDER BY
    if !order_by.is_empty() {
        let combined_cols: Vec<(usize, String)> = col_names
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n.clone()))
            .collect();
        apply_order_by(&mut result_rows, &combined_cols, order_by);
    }

    // LIMIT
    if let Some(lim) = limit {
        result_rows.truncate(lim as usize);
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: result_rows,
    })
}

/// Resolve SelectColumns to indices in the table metadata.
pub(crate) fn resolve_columns(
    meta: &TableMeta,
    columns: &[SelectColumn],
) -> Result<Vec<(usize, String)>> {
    let mut result = Vec::new();
    for col in columns {
        match col {
            SelectColumn::Wildcard => {
                for (i, c) in meta.columns.iter().enumerate() {
                    result.push((i, c.name.clone()));
                }
            }
            SelectColumn::Name(name) => {
                let idx = meta
                    .columns
                    .iter()
                    .position(|c| c.name == *name)
                    .ok_or_else(|| {
                        ExchangeDbError::ColumnNotFound(name.clone(), meta.name.clone())
                    })?;
                result.push((idx, name.clone()));
            }
            SelectColumn::Aggregate {
                column, arg_expr, ..
            } => {
                if column == "*" {
                    // count(*) doesn't need a specific column; use first column.
                    result.push((0, meta.columns[0].name.clone()));
                } else {
                    let idx = meta
                        .columns
                        .iter()
                        .position(|c| c.name == *column)
                        .ok_or_else(|| {
                            ExchangeDbError::ColumnNotFound(column.clone(), meta.name.clone())
                        })?;
                    result.push((idx, column.clone()));
                }
                // Also resolve any additional columns referenced by arg_expr.
                if let Some(expr) = arg_expr {
                    let refs = collect_plan_expr_column_names(expr);
                    for ref_col in refs {
                        let already = result.iter().any(|(_, n)| n == &ref_col);
                        if !already
                            && let Some(idx) = meta.columns.iter().position(|c| c.name == ref_col)
                        {
                            result.push((idx, ref_col));
                        }
                    }
                }
            }
            SelectColumn::ScalarFunction { args, .. } => {
                // For scalar functions, resolve each column argument so
                // the underlying data is scanned.
                for arg in args {
                    if let SelectColumnArg::Column(name) = arg {
                        let already = result.iter().any(|(_, n)| n == name);
                        if !already {
                            let idx = meta
                                .columns
                                .iter()
                                .position(|c| c.name == *name)
                                .ok_or_else(|| {
                                    ExchangeDbError::ColumnNotFound(name.clone(), meta.name.clone())
                                })?;
                            result.push((idx, name.clone()));
                        }
                    }
                }
            }
            SelectColumn::WindowFunction(wf) => {
                // Resolve all columns referenced by the window function:
                // arguments, partition_by, and order_by.
                let mut needed_cols: Vec<String> = Vec::new();
                let mut has_wildcard = false;
                for arg in &wf.args {
                    match arg {
                        crate::window::WindowFuncArg::Column(c) => needed_cols.push(c.clone()),
                        crate::window::WindowFuncArg::Wildcard => has_wildcard = true,
                        _ => {}
                    }
                }
                // For count(*) OVER (...), ensure at least one column is scanned
                // so rows are materialized.
                if has_wildcard && needed_cols.is_empty() && result.is_empty() {
                    needed_cols.push(meta.columns[0].name.clone());
                }
                for pb_col in &wf.over.partition_by {
                    needed_cols.push(pb_col.clone());
                }
                for ob in &wf.over.order_by {
                    needed_cols.push(ob.column.clone());
                }
                for col_name in &needed_cols {
                    let already = result.iter().any(|(_, n)| n == col_name);
                    if !already {
                        let idx = meta
                            .columns
                            .iter()
                            .position(|c| c.name == *col_name)
                            .ok_or_else(|| {
                                ExchangeDbError::ColumnNotFound(col_name.clone(), meta.name.clone())
                            })?;
                        result.push((idx, col_name.clone()));
                    }
                }
            }
            SelectColumn::CaseWhen {
                conditions,
                expr_conditions,
                expr_else,
                ..
            } => {
                // Resolve columns referenced in CASE WHEN conditions.
                for (filter, _) in conditions {
                    collect_filter_column_refs(filter, meta, &mut result)?;
                }
                // Also resolve columns referenced in expression results.
                if let Some(econds) = expr_conditions {
                    for (_, expr) in econds {
                        let refs = collect_plan_expr_column_names(expr);
                        for col_name in refs {
                            let already = result.iter().any(|(_, n)| n == &col_name);
                            if !already
                                && let Some(idx) =
                                    meta.columns.iter().position(|c| c.name == col_name)
                            {
                                result.push((idx, col_name));
                            }
                        }
                    }
                }
                if let Some(expr) = expr_else {
                    let refs = collect_plan_expr_column_names(expr);
                    for col_name in refs {
                        let already = result.iter().any(|(_, n)| n == &col_name);
                        if !already
                            && let Some(idx) = meta.columns.iter().position(|c| c.name == col_name)
                        {
                            result.push((idx, col_name));
                        }
                    }
                }
            }
            SelectColumn::Expression { expr, .. } => {
                // Resolve columns referenced in expressions.
                let mut cols = Vec::new();
                expr.collect_columns(&mut cols);
                for col_name in &cols {
                    let already = result.iter().any(|(_, n)| n == col_name);
                    if !already
                        && let Some(idx) = meta.columns.iter().position(|c| c.name == *col_name)
                    {
                        result.push((idx, col_name.clone()));
                    }
                }
                // If the expression is constant (no column refs), ensure at least
                // one column is scanned so rows are materialized.
                if cols.is_empty() && result.is_empty() {
                    result.push((0, meta.columns[0].name.clone()));
                }
            }
            SelectColumn::ScalarSubquery { .. } => {
                // Scalar subqueries don't reference columns from the main table.
                // Ensure at least one column is scanned so rows are materialized.
                if result.is_empty() {
                    result.push((0, meta.columns[0].name.clone()));
                }
            }
        }
    }
    Ok(result)
}

/// Helper to resolve column names referenced in a Filter to indices in table metadata.
fn collect_filter_column_refs(
    filter: &Filter,
    meta: &TableMeta,
    result: &mut Vec<(usize, String)>,
) -> Result<()> {
    match filter {
        Filter::Eq(col, _)
        | Filter::Gt(col, _)
        | Filter::Lt(col, _)
        | Filter::Gte(col, _)
        | Filter::Lte(col, _)
        | Filter::IsNull(col)
        | Filter::IsNotNull(col)
        | Filter::Like(col, _)
        | Filter::NotLike(col, _)
        | Filter::ILike(col, _) => {
            let already = result.iter().any(|(_, n)| n == col);
            if !already && let Some(idx) = meta.columns.iter().position(|c| c.name == *col) {
                result.push((idx, col.clone()));
            }
        }
        Filter::Between(col, _, _)
        | Filter::BetweenSymmetric(col, _, _)
        | Filter::In(col, _)
        | Filter::NotIn(col, _) => {
            let already = result.iter().any(|(_, n)| n == col);
            if !already && let Some(idx) = meta.columns.iter().position(|c| c.name == *col) {
                result.push((idx, col.clone()));
            }
        }
        Filter::And(parts) | Filter::Or(parts) => {
            for p in parts {
                collect_filter_column_refs(p, meta, result)?;
            }
        }
        Filter::Not(inner) => {
            collect_filter_column_refs(inner, meta, result)?;
        }
        Filter::NotEq(col, _) => {
            let already = result.iter().any(|(_, n)| n == col);
            if !already && let Some(idx) = meta.columns.iter().position(|c| c.name == *col) {
                result.push((idx, col.clone()));
            }
        }
        Filter::Expression { left, right, .. } => {
            // Collect columns from both sides of the expression.
            let mut cols = Vec::new();
            left.collect_columns(&mut cols);
            right.collect_columns(&mut cols);
            for col_name in cols {
                let already = result.iter().any(|(_, n)| n == &col_name);
                if !already && let Some(idx) = meta.columns.iter().position(|c| c.name == col_name)
                {
                    result.push((idx, col_name));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn result_column_names(select_cols: &[SelectColumn], resolved: &[(usize, String)]) -> Vec<String> {
    // If any select column is a Wildcard, just use all resolved column names directly.
    if select_cols
        .iter()
        .any(|c| matches!(c, SelectColumn::Wildcard))
    {
        return resolved.iter().map(|(_, n)| n.clone()).collect();
    }

    select_cols
        .iter()
        .map(|col| match col {
            SelectColumn::Wildcard => unreachable!(),
            SelectColumn::Name(n) => n.clone(),
            SelectColumn::Aggregate {
                function,
                column,
                alias,
                ..
            } => {
                if let Some(a) = alias {
                    a.clone()
                } else {
                    let func_name = format!("{function:?}").to_ascii_lowercase();
                    format!("{func_name}({column})")
                }
            }
            SelectColumn::ScalarFunction { name, args } => {
                let arg_strs: Vec<String> = args
                    .iter()
                    .map(|a| match a {
                        SelectColumnArg::Column(c) => c.clone(),
                        SelectColumnArg::Literal(v) => format!("{v}"),
                    })
                    .collect();
                format!("{name}({})", arg_strs.join(", "))
            }
            SelectColumn::WindowFunction(wf) => wf.alias.clone().unwrap_or_else(|| wf.name.clone()),
            SelectColumn::CaseWhen { alias, .. } => {
                alias.clone().unwrap_or_else(|| "case".to_string())
            }
            SelectColumn::Expression { alias, .. } => {
                alias.clone().unwrap_or_else(|| "expr".to_string())
            }
            SelectColumn::ScalarSubquery { alias, .. } => {
                alias.clone().unwrap_or_else(|| "subquery".to_string())
            }
        })
        .collect()
}

/// Scan all partitions of a table and return rows (each row has values
/// corresponding to `selected_cols`).
///
/// Uses `list_partitions` (which discovers hot, warm, and cold partitions)
/// and `TieredPartitionReader` to transparently decompress/convert before
/// reading column files.
fn scan_table(
    table_dir: &Path,
    meta: &TableMeta,
    selected_cols: &[(usize, String)],
    filter: Option<&Filter>,
) -> Result<Vec<Vec<Value>>> {
    use exchange_core::column::{FixedColumnReader, VarColumnReader};
    use exchange_core::tiered::TieredPartitionReader;

    let mut rows = Vec::new();

    // Discover ALL partitions including warm (.d.lz4) and cold (.xpqt).
    let partitions = exchange_core::table::list_partitions(table_dir)?;

    // Determine the full set of column indices we need to read
    // (selected + any referenced in filter).
    let filter_col_indices = if let Some(f) = filter {
        collect_filter_columns(f, meta)
    } else {
        Vec::new()
    };

    // Union of selected + filter columns (by index, deduplicated).
    let mut all_indices: Vec<usize> = selected_cols.iter().map(|(i, _)| *i).collect();
    for idx in &filter_col_indices {
        if !all_indices.contains(idx) {
            all_indices.push(*idx);
        }
    }
    all_indices.sort();
    all_indices.dedup();

    for partition_path in &partitions {
        // Use TieredPartitionReader to get the native path for reading.
        // This transparently handles warm (LZ4) and cold (XPQT) partitions.
        let tiered_reader = TieredPartitionReader::open(partition_path, table_dir).ok();
        let native_path = tiered_reader
            .as_ref()
            .map(|r| r.native_path())
            .unwrap_or(partition_path.as_path());

        // Open column readers for all needed columns.
        let mut readers: Vec<(usize, ColumnReader)> = Vec::new();

        for &col_idx in &all_indices {
            let col_def = &meta.columns[col_idx];
            let col_type: ColumnType = col_def.col_type.into();

            if col_type.is_variable_length() {
                let data_path = native_path.join(format!("{}.d", col_def.name));
                let index_path = native_path.join(format!("{}.i", col_def.name));
                if data_path.exists() && index_path.exists() {
                    let reader = VarColumnReader::open(&data_path, &index_path)?;
                    readers.push((col_idx, ColumnReader::Var(reader, col_type)));
                }
            } else {
                let data_path = native_path.join(format!("{}.d", col_def.name));
                if data_path.exists() {
                    let reader = FixedColumnReader::open(&data_path, col_type)?;
                    readers.push((col_idx, ColumnReader::Fixed(reader, col_type)));
                }
            }
        }

        if readers.is_empty() {
            continue;
        }

        // Determine row count from the first reader.
        let row_count = match &readers[0].1 {
            ColumnReader::Fixed(r, _) => r.row_count(),
            ColumnReader::Var(r, _) => r.row_count(),
        };

        for row_idx in 0..row_count {
            // Read all needed values for this row.
            let mut all_values: Vec<(usize, Value)> = Vec::new();
            for (col_idx, reader) in &readers {
                let val = read_value(reader, row_idx);
                all_values.push((*col_idx, val));
            }

            // Apply filter.
            if let Some(f) = filter
                && !evaluate_filter(f, &all_values, meta)
            {
                continue;
            }

            // Extract only selected columns in order.
            let row: Vec<Value> = selected_cols
                .iter()
                .map(|(idx, _)| {
                    all_values
                        .iter()
                        .find(|(i, _)| i == idx)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(Value::Null)
                })
                .collect();

            rows.push(row);
        }
    }

    Ok(rows)
}

enum ColumnReader {
    Fixed(exchange_core::column::FixedColumnReader, ColumnType),
    Var(exchange_core::column::VarColumnReader, ColumnType),
}

fn read_value(reader: &ColumnReader, row: u64) -> Value {
    match reader {
        ColumnReader::Fixed(r, ct) => match ct {
            ColumnType::I64 => Value::I64(r.read_i64(row)),
            ColumnType::F64 => {
                let v = r.read_f64(row);
                if v.is_nan() {
                    Value::Null
                } else {
                    Value::F64(v)
                }
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
                // Return UUID as hex string.
                let bytes = r.read_raw(row);
                Value::Str(hex::encode(bytes))
            }
            _ => Value::Null,
        },
        ColumnReader::Var(r, _ct) => {
            let s = r.read_str(row);
            if s == "\0" {
                Value::Null
            } else {
                Value::Str(s.to_string())
            }
        }
    }
}

fn collect_filter_columns(filter: &Filter, meta: &TableMeta) -> Vec<usize> {
    let mut indices = Vec::new();
    match filter {
        Filter::Eq(col, _)
        | Filter::Gt(col, _)
        | Filter::Lt(col, _)
        | Filter::Gte(col, _)
        | Filter::Lte(col, _)
        | Filter::IsNull(col)
        | Filter::IsNotNull(col)
        | Filter::Like(col, _)
        | Filter::NotLike(col, _)
        | Filter::ILike(col, _)
        | Filter::NotEq(col, _) => {
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

fn evaluate_filter(filter: &Filter, values: &[(usize, Value)], meta: &TableMeta) -> bool {
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
            val.as_ref()
                .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater))
                .unwrap_or(false)
        }
        Filter::Lt(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less))
                .unwrap_or(false)
        }
        Filter::Gte(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    matches!(
                        v.cmp_coerce(expected),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    )
                })
                .unwrap_or(false)
        }
        Filter::Lte(col, expected) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    matches!(
                        v.cmp_coerce(expected),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    )
                })
                .unwrap_or(false)
        }
        Filter::Between(col, low, high) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    matches!(
                        v.cmp_coerce(low),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ) && matches!(
                        v.cmp_coerce(high),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    )
                })
                .unwrap_or(false)
        }
        Filter::BetweenSymmetric(col, low, high) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    let fwd = matches!(
                        v.cmp_coerce(low),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ) && matches!(
                        v.cmp_coerce(high),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    );
                    let rev = matches!(
                        v.cmp_coerce(high),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ) && matches!(
                        v.cmp_coerce(low),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    );
                    fwd || rev
                })
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
            val.as_ref()
                .map(|v| list.iter().any(|item| v.eq_coerce(item)))
                .unwrap_or(false)
        }
        Filter::NotIn(col, list) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| !list.iter().any(|item| v.eq_coerce(item)))
                .unwrap_or(true)
        }
        Filter::Like(col, pattern) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    if let Value::Str(s) = v {
                        like_match(s, pattern, false)
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
        }
        Filter::NotLike(col, pattern) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    if let Value::Str(s) = v {
                        !like_match(s, pattern, false)
                    } else {
                        true
                    }
                })
                .unwrap_or(true)
        }
        Filter::ILike(col, pattern) => {
            let val = get_filter_value(col, values, meta);
            val.as_ref()
                .map(|v| {
                    if let Value::Str(s) = v {
                        like_match(s, pattern, true)
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
        }
        Filter::Subquery { .. } | Filter::InSubquery { .. } | Filter::Exists { .. } => {
            // Subquery filters should have been resolved before reaching here.
            false
        }
        Filter::Not(inner) => !evaluate_filter(inner, values, meta),
        Filter::Expression { left, op, right } => {
            let lv = evaluate_plan_expr(left, values, meta);
            let rv = evaluate_plan_expr(right, values, meta);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                CompareOp::Lte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
            }
        }
        // ALL/ANY subquery filters not yet evaluated in scan-level filter.
        Filter::All { .. } | Filter::Any { .. } => false,
    }
}

/// Match a string against a SQL LIKE pattern.
/// `%` matches any sequence of characters, `_` matches any single character.
/// If `case_insensitive` is true, comparison is done case-insensitively.
pub(crate) fn like_match(s: &str, pattern: &str, case_insensitive: bool) -> bool {
    let s = if case_insensitive {
        s.to_ascii_lowercase()
    } else {
        s.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_ascii_lowercase()
    } else {
        pattern.to_string()
    };
    like_match_impl(s.as_bytes(), pattern.as_bytes())
}

fn like_match_impl(s: &[u8], p: &[u8]) -> bool {
    // Sentinel bytes used by the planner's ESCAPE pre-processing:
    //   0x01 = literal underscore (was `<esc>_`)
    //   0x02 = literal percent    (was `<esc>%`)
    let mut si = 0;
    let mut pi = 0;
    let mut star_p = usize::MAX;
    let mut star_s = 0;

    while si < s.len() {
        if pi < p.len() && p[pi] == 0x01 {
            // Escaped underscore — match literal '_' only.
            if s[si] == b'_' {
                si += 1;
                pi += 1;
            } else if star_p != usize::MAX {
                star_s += 1;
                si = star_s;
                pi = star_p + 1;
            } else {
                return false;
            }
        } else if pi < p.len() && p[pi] == 0x02 {
            // Escaped percent — match literal '%' only.
            if s[si] == b'%' {
                si += 1;
                pi += 1;
            } else if star_p != usize::MAX {
                star_s += 1;
                si = star_s;
                pi = star_p + 1;
            } else {
                return false;
            }
        } else if pi < p.len() && p[pi] == b'_' {
            // _ matches any single character
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == b'%' {
            // % matches any sequence; remember position for backtracking
            star_p = pi;
            star_s = si;
            pi += 1;
        } else if pi < p.len() && s[si] == p[pi] {
            si += 1;
            pi += 1;
        } else if star_p != usize::MAX {
            // Backtrack: advance the match position after the last %
            star_s += 1;
            si = star_s;
            pi = star_p + 1;
        } else {
            return false;
        }
    }
    // Consume trailing %
    while pi < p.len() && p[pi] == b'%' {
        pi += 1;
    }
    pi == p.len()
}

fn get_filter_value(col: &str, values: &[(usize, Value)], meta: &TableMeta) -> Option<Value> {
    let idx = meta.columns.iter().position(|c| c.name == col)?;
    values
        .iter()
        .find(|(i, _)| *i == idx)
        .map(|(_, v)| v.clone())
}

/// Apply ORDER BY, LIMIT, and DISTINCT to a vectorized group-by result.
fn apply_post_groupby(
    result: &mut QueryResult,
    order_by: &[OrderBy],
    limit: Option<u64>,
    distinct: bool,
) {
    if let QueryResult::Rows { rows, columns } = result {
        if !order_by.is_empty() {
            let fake_resolved: Vec<(usize, String)> = columns
                .iter()
                .enumerate()
                .map(|(i, n)| (i, n.clone()))
                .collect();
            apply_order_by(rows, &fake_resolved, order_by);
        }
        if let Some(lim) = limit {
            rows.truncate(lim as usize);
        }
        if distinct {
            *rows = apply_distinct(std::mem::take(rows));
        }
    }
}

pub(crate) fn apply_order_by(
    rows: &mut [Vec<Value>],
    selected_cols: &[(usize, String)],
    order_by: &[OrderBy],
) {
    rows.sort_by(|a, b| {
        for ob in order_by {
            let col_pos = selected_cols.iter().position(|(_, n)| n == &ob.column);
            if let Some(pos) = col_pos {
                let cmp = a[pos]
                    .partial_cmp(&b[pos])
                    .unwrap_or(std::cmp::Ordering::Equal);
                let cmp = if ob.descending { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// TopK optimization for ORDER BY + LIMIT: maintain a bounded BinaryHeap of
/// size K instead of sorting all N rows. O(N log K) instead of O(N log N).
///
/// Returns the top K rows in the correct output order.
fn apply_topk(
    rows: &[Vec<Value>],
    selected_cols: &[(usize, String)],
    order_by: &[OrderBy],
    k: usize,
) -> Vec<Vec<Value>> {
    use std::cmp::Ordering as O;
    use std::collections::BinaryHeap;

    // Resolve ORDER BY column positions once.
    let col_positions: Vec<(usize, bool)> = order_by
        .iter()
        .filter_map(|ob| {
            selected_cols
                .iter()
                .position(|(_, n)| n == &ob.column)
                .map(|pos| (pos, ob.descending))
        })
        .collect();

    /// A wrapper that implements Ord for the BinaryHeap. The ordering is
    /// *reversed* relative to the desired output so the heap evicts the
    /// "worst" element (the one that would appear last in the result).
    struct HeapEntry {
        row: Vec<Value>,
        col_positions: Vec<(usize, bool)>,
    }

    impl HeapEntry {
        fn cmp_key(&self, other: &Self) -> O {
            for &(pos, desc) in &self.col_positions {
                let cmp = self.row[pos]
                    .partial_cmp(&other.row[pos])
                    .unwrap_or(O::Equal);
                // For ASC: keep smallest, evict largest -> natural max-heap order
                // For DESC: keep largest, evict smallest -> reverse
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != O::Equal {
                    return cmp;
                }
            }
            O::Equal
        }
    }

    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.cmp_key(other) == O::Equal
        }
    }
    impl Eq for HeapEntry {}
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<O> {
            Some(self.cmp(other))
        }
    }
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> O {
            self.cmp_key(other)
        }
    }

    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);

    for row in rows {
        heap.push(HeapEntry {
            row: row.clone(),
            col_positions: col_positions.clone(),
        });
        if heap.len() > k {
            heap.pop();
        }
    }

    // Extract and sort in the correct output order.
    let mut result: Vec<Vec<Value>> = heap.into_iter().map(|e| e.row).collect();
    result.sort_by(|a, b| {
        for &(pos, desc) in &col_positions {
            let cmp = a[pos].partial_cmp(&b[pos]).unwrap_or(O::Equal);
            let cmp = if desc { cmp.reverse() } else { cmp };
            if cmp != O::Equal {
                return cmp;
            }
        }
        O::Equal
    });
    result
}

/// Collect all column name references from a PlanExpr.
fn collect_plan_expr_column_names(expr: &PlanExpr) -> Vec<String> {
    let mut refs = Vec::new();
    collect_plan_expr_cols_recursive(expr, &mut refs);
    refs
}

fn collect_plan_expr_cols_recursive(expr: &PlanExpr, out: &mut Vec<String>) {
    match expr {
        PlanExpr::Column(name) => {
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
        PlanExpr::Literal(_) => {}
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_plan_expr_cols_recursive(left, out);
            collect_plan_expr_cols_recursive(right, out);
        }
        PlanExpr::UnaryOp { expr, .. } => {
            collect_plan_expr_cols_recursive(expr, out);
        }
        PlanExpr::Function { args, .. } => {
            for a in args {
                collect_plan_expr_cols_recursive(a, out);
            }
        }
    }
}

/// Check if a PlanExpr contains any aggregate function calls.
fn expr_has_aggregate(expr: &PlanExpr) -> bool {
    match expr {
        PlanExpr::Function { name, args } => {
            if AggregateKind::from_name(name).is_some() {
                return true;
            }
            args.iter().any(expr_has_aggregate)
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        PlanExpr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        PlanExpr::Column(_) | PlanExpr::Literal(_) => false,
    }
}

/// Evaluate a PlanExpr that may contain aggregate function calls.
/// `agg_results` maps aggregate function signatures to their computed values.
fn evaluate_expr_with_aggregates(
    expr: &PlanExpr,
    agg_results: &HashMap<String, Value>,
    row: &[Value],
    col_names: &[String],
) -> Value {
    match expr {
        PlanExpr::Function { name, args } => {
            if let Some(kind) = AggregateKind::from_name(name) {
                // This is an aggregate function — look up the result.
                let key = format!(
                    "{:?}({})",
                    kind,
                    args.iter()
                        .map(|a| format!("{a:?}"))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                return agg_results.get(&key).cloned().unwrap_or(Value::Null);
            }
            // Scalar function: evaluate arguments recursively.
            let func_args: Vec<Value> = args
                .iter()
                .map(|a| evaluate_expr_with_aggregates(a, agg_results, row, col_names))
                .collect();
            crate::scalar::evaluate_scalar(name, &func_args).unwrap_or(Value::Null)
        }
        PlanExpr::Column(name) => col_names
            .iter()
            .position(|n| n == name)
            .and_then(|i| row.get(i).cloned())
            .unwrap_or(Value::Null),
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => {
            let lv = evaluate_expr_with_aggregates(left, agg_results, row, col_names);
            let rv = evaluate_expr_with_aggregates(right, agg_results, row, col_names);
            apply_binary_op(&lv, *op, &rv)
        }
        PlanExpr::UnaryOp { op, expr } => {
            let v = evaluate_expr_with_aggregates(expr, agg_results, row, col_names);
            apply_unary_op(*op, &v)
        }
    }
}

/// Collect all aggregate functions from a PlanExpr and create aggregate function instances.
fn collect_aggregates_from_expr(
    expr: &PlanExpr,
    agg_map: &mut HashMap<String, (AggregateKind, String, Box<dyn AggregateFunction>)>,
) {
    match expr {
        PlanExpr::Function { name, args } => {
            if let Some(kind) = AggregateKind::from_name(name) {
                // Extract column name from the first argument.
                let col_name = match args.first() {
                    Some(PlanExpr::Column(c)) => c.clone(),
                    _ => "*".to_string(),
                };
                let key = format!(
                    "{:?}({})",
                    kind,
                    args.iter()
                        .map(|a| format!("{a:?}"))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                agg_map
                    .entry(key)
                    .or_insert_with(|| (kind, col_name, functions::create_aggregate(kind)));
            } else {
                for a in args {
                    collect_aggregates_from_expr(a, agg_map);
                }
            }
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_aggregates_from_expr(left, agg_map);
            collect_aggregates_from_expr(right, agg_map);
        }
        PlanExpr::UnaryOp { expr, .. } => collect_aggregates_from_expr(expr, agg_map),
        PlanExpr::Column(_) | PlanExpr::Literal(_) => {}
    }
}

fn apply_aggregates(
    select_cols: &[SelectColumn],
    resolved: &[(usize, String)],
    rows: &[Vec<Value>],
) -> Result<Vec<Vec<Value>>> {
    // Check if any Expression columns contain aggregate function calls.
    let has_expr_aggs = select_cols.iter().any(|c| match c {
        SelectColumn::Expression { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    });

    if has_expr_aggs {
        // Collect all aggregate functions from both direct Aggregates and Expressions.
        let mut agg_map: HashMap<String, (AggregateKind, String, Box<dyn AggregateFunction>)> =
            HashMap::new();

        // Direct aggregate columns.
        for col in select_cols {
            match col {
                SelectColumn::Aggregate {
                    function, column, ..
                } => {
                    let key = format!("{:?}(Column({:?}))", function, column);
                    agg_map.entry(key).or_insert_with(|| {
                        (
                            *function,
                            column.clone(),
                            functions::create_aggregate(*function),
                        )
                    });
                }
                SelectColumn::Expression { expr, .. } => {
                    collect_aggregates_from_expr(expr, &mut agg_map);
                }
                _ => {}
            }
        }

        // Map column names to positions for feeding values.
        let col_positions: HashMap<String, usize> = resolved
            .iter()
            .map(|(_, name)| {
                (
                    name.clone(),
                    resolved.iter().position(|(_, n)| n == name).unwrap(),
                )
            })
            .collect();

        // Feed all rows to the aggregate functions.
        for row in rows {
            for (_, (_, col_name, func)) in agg_map.iter_mut() {
                if col_name == "*" {
                    func.add(&Value::I64(1));
                } else if let Some(&pos) = col_positions.get(col_name.as_str())
                    && pos < row.len()
                {
                    func.add(&row[pos]);
                }
            }
        }

        // Collect results.
        let agg_results: HashMap<String, Value> = agg_map
            .iter()
            .map(|(key, (_, _, func))| (key.clone(), func.result()))
            .collect();

        // Build result row.
        let col_names: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
        let empty_row: Vec<Value> = vec![Value::Null; col_names.len()];

        let result_row: Vec<Value> = select_cols
            .iter()
            .enumerate()
            .map(|(i, col)| match col {
                SelectColumn::Aggregate {
                    function, column, ..
                } => {
                    let key = format!("{:?}(Column({:?}))", function, column);
                    agg_results.get(&key).cloned().unwrap_or(Value::Null)
                }
                SelectColumn::Expression { expr, .. } => {
                    evaluate_expr_with_aggregates(expr, &agg_results, &empty_row, &col_names)
                }
                _ => {
                    if let Some(first_row) = rows.first() {
                        if i < first_row.len() {
                            first_row[i].clone()
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
            })
            .collect();

        return Ok(vec![result_row]);
    }

    // Standard path: only direct Aggregate columns.
    let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = select_cols
        .iter()
        .map(|c| match c {
            SelectColumn::Aggregate { function, .. } => {
                Some(functions::create_aggregate(*function))
            }
            _ => None,
        })
        .collect();

    let col_names_for_filter: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
    for row in rows {
        for (i, func) in agg_funcs.iter_mut().enumerate() {
            if let Some(f) = func {
                // Check FILTER (WHERE ...) clause if present.
                if let SelectColumn::Aggregate {
                    filter: Some(flt), ..
                } = &select_cols[i]
                    && !evaluate_filter_virtual(flt, row, &col_names_for_filter)
                {
                    continue;
                }
                // Use arg_expr if present.
                if let SelectColumn::Aggregate {
                    arg_expr: Some(expr),
                    ..
                } = &select_cols[i]
                {
                    let val = evaluate_plan_expr_by_name(expr, row, &col_names_for_filter);
                    f.add(&val);
                } else if let SelectColumn::Aggregate { column, .. } = &select_cols[i] {
                    if column == "*" {
                        f.add(&Value::I64(1));
                    } else if let Some(pos) = resolved.iter().position(|(_, n)| n == column) {
                        if pos < row.len() {
                            f.add(&row[pos]);
                        }
                    } else if i < row.len() {
                        f.add(&row[i]);
                    }
                }
            }
        }
    }

    let result_row: Vec<Value> = agg_funcs
        .iter()
        .enumerate()
        .map(|(i, func)| {
            if let Some(f) = func {
                f.result()
            } else if let Some(first_row) = rows.first() {
                if i < first_row.len() {
                    first_row[i].clone()
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        })
        .collect();

    Ok(vec![result_row])
}

fn apply_sample_by(
    meta: &TableMeta,
    select_cols: &[SelectColumn],
    resolved: &[(usize, String)],
    rows: Vec<Vec<Value>>,
    sample_by: &SampleBy,
) -> Result<Vec<Vec<Value>>> {
    let interval_ns = sample_by.interval.as_nanos() as i64;
    let ts_col_idx = meta.timestamp_column;
    let ts_resolved_pos = resolved.iter().position(|(idx, _)| *idx == ts_col_idx);
    if ts_resolved_pos.is_none() && rows.is_empty() {
        return Ok(Vec::new());
    }

    let first_ts = rows
        .first()
        .and_then(|row| {
            ts_resolved_pos.and_then(|pos| match &row[pos] {
                Value::Timestamp(ns) | Value::I64(ns) => Some(*ns),
                _ => None,
            })
        })
        .unwrap_or(0);
    let align_offset = if sample_by.align == AlignMode::Calendar {
        0i64
    } else {
        first_ts % interval_ns
    };
    let bucket_fn =
        |ts_ns: i64| -> i64 { ((ts_ns - align_offset) / interval_ns) * interval_ns + align_offset };

    let mut bucket_map: Vec<(i64, Vec<Vec<Value>>)> = Vec::new();
    for row in &rows {
        let ts_ns = ts_resolved_pos
            .and_then(|pos| match &row[pos] {
                Value::Timestamp(ns) | Value::I64(ns) => Some(*ns),
                _ => None,
            })
            .unwrap_or(0);
        let bucket = bucket_fn(ts_ns);
        if let Some(last) = bucket_map.last_mut()
            && last.0 == bucket
        {
            last.1.push(row.clone());
            continue;
        }
        bucket_map.push((bucket, vec![row.clone()]));
    }

    let aggregate_bucket = |bucket_ts: i64, bucket_rows: &[Vec<Value>]| -> Vec<Value> {
        let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = select_cols
            .iter()
            .map(|c| match c {
                SelectColumn::Aggregate { function, .. } => {
                    Some(functions::create_aggregate(*function))
                }
                _ => None,
            })
            .collect();
        for row in bucket_rows {
            for (i, func) in agg_funcs.iter_mut().enumerate() {
                if let Some(f) = func
                    && i < row.len()
                {
                    f.add(&row[i]);
                }
            }
        }
        select_cols
            .iter()
            .enumerate()
            .map(|(i, col)| match col {
                SelectColumn::Aggregate { .. } => agg_funcs[i]
                    .as_ref()
                    .map(|f| f.result())
                    .unwrap_or(Value::Null),
                SelectColumn::Name(name) => {
                    if let Some(pos) = resolved.iter().position(|(_, n)| n == name) {
                        if resolved[pos].0 == ts_col_idx {
                            Value::Timestamp(bucket_ts)
                        } else {
                            bucket_rows
                                .first()
                                .and_then(|r| r.get(pos).cloned())
                                .unwrap_or(Value::Null)
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            })
            .collect()
    };
    let empty_row = |bucket_ts: i64| -> Vec<Value> {
        select_cols
            .iter()
            .map(|col| match col {
                SelectColumn::Name(name) => {
                    if let Some(pos) = resolved.iter().position(|(_, n)| n == name) {
                        if resolved[pos].0 == ts_col_idx {
                            Value::Timestamp(bucket_ts)
                        } else {
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            })
            .collect()
    };

    if matches!(sample_by.fill, FillMode::None) {
        return Ok(bucket_map
            .iter()
            .map(|(bts, brows)| aggregate_bucket(*bts, brows))
            .collect());
    }
    if bucket_map.is_empty() {
        return Ok(Vec::new());
    }

    let min_bucket = bucket_map.first().unwrap().0;
    let max_bucket = bucket_map.last().unwrap().0;
    let mut known: std::collections::BTreeMap<i64, Vec<Value>> = std::collections::BTreeMap::new();
    for (bts, brows) in &bucket_map {
        known.insert(*bts, aggregate_bucket(*bts, brows));
    }
    let mut all_buckets = Vec::new();
    {
        let mut t = min_bucket;
        while t <= max_bucket {
            all_buckets.push(t);
            t += interval_ns;
        }
    }
    let mut result = Vec::with_capacity(all_buckets.len());

    match &sample_by.fill {
        FillMode::Null => {
            for &bts in &all_buckets {
                result.push(known.get(&bts).cloned().unwrap_or_else(|| empty_row(bts)));
            }
        }
        FillMode::Prev => {
            let mut last_known: Option<Vec<Value>> = None;
            for &bts in &all_buckets {
                if let Some(row) = known.get(&bts) {
                    last_known = Some(row.clone());
                    result.push(row.clone());
                } else if let Some(prev) = &last_known {
                    let mut filled = prev.clone();
                    for (i, col) in select_cols.iter().enumerate() {
                        if let SelectColumn::Name(name) = col
                            && let Some(pos) = resolved.iter().position(|(_, n)| n == name)
                            && resolved[pos].0 == ts_col_idx
                        {
                            filled[i] = Value::Timestamp(bts);
                        }
                    }
                    result.push(filled);
                } else {
                    result.push(empty_row(bts));
                }
            }
        }
        FillMode::Value(fill_val) => {
            for &bts in &all_buckets {
                if let Some(row) = known.get(&bts) {
                    result.push(row.clone());
                } else {
                    let mut filled = empty_row(bts);
                    for (i, col) in select_cols.iter().enumerate() {
                        if matches!(col, SelectColumn::Aggregate { .. }) {
                            filled[i] = fill_val.clone();
                        }
                    }
                    result.push(filled);
                }
            }
        }
        FillMode::Linear => {
            let known_indices: Vec<usize> = all_buckets
                .iter()
                .enumerate()
                .filter(|(_, bts)| known.contains_key(bts))
                .map(|(idx, _)| idx)
                .collect();
            let mut pre_result: Vec<Option<Vec<Value>>> = all_buckets
                .iter()
                .map(|bts| known.get(bts).cloned())
                .collect();
            for idx in 0..all_buckets.len() {
                if pre_result[idx].is_some() {
                    continue;
                }
                let bts = all_buckets[idx];
                let prev_ki = known_indices.iter().rev().find(|&&ki| ki < idx);
                let next_ki = known_indices.iter().find(|&&ki| ki > idx);
                let mut row = empty_row(bts);
                if let (Some(&pi), Some(&ni)) = (prev_ki, next_ki) {
                    let prev_row = pre_result[pi].as_ref().unwrap();
                    let next_row = pre_result[ni].as_ref().unwrap();
                    let frac = (idx - pi) as f64 / (ni - pi) as f64;
                    for (i, col) in select_cols.iter().enumerate() {
                        if matches!(col, SelectColumn::Aggregate { .. }) {
                            row[i] = sample_by_interpolate(&prev_row[i], &next_row[i], frac);
                        }
                    }
                } else if let Some(&pi) = prev_ki {
                    let prev_row = pre_result[pi].as_ref().unwrap();
                    for (i, col) in select_cols.iter().enumerate() {
                        if matches!(col, SelectColumn::Aggregate { .. }) {
                            row[i] = prev_row[i].clone();
                        }
                    }
                }
                pre_result[idx] = Some(row);
            }
            result = pre_result.into_iter().flatten().collect();
        }
        FillMode::None => unreachable!(),
    }
    Ok(result)
}

fn sample_by_interpolate(a: &Value, b: &Value, frac: f64) -> Value {
    match (a, b) {
        (Value::F64(va), Value::F64(vb)) => Value::F64(va + (vb - va) * frac),
        (Value::I64(va), Value::I64(vb)) => {
            Value::I64((*va as f64 + (*vb - *va) as f64 * frac) as i64)
        }
        (Value::I64(va), Value::F64(vb)) => Value::F64(*va as f64 + (vb - *va as f64) * frac),
        (Value::F64(va), Value::I64(vb)) => Value::F64(va + (*vb as f64 - va) * frac),
        _ => a.clone(),
    }
}

/// Evaluate a CASE WHEN SelectColumn against a row.
fn evaluate_case_when_select_col(
    col: &SelectColumn,
    row: &[Value],
    resolved: &[(usize, String)],
) -> Value {
    match col {
        SelectColumn::CaseWhen {
            conditions,
            else_value,
            expr_conditions,
            expr_else,
            ..
        } => {
            let col_names: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
            if let Some(econds) = expr_conditions {
                let mut result = expr_else
                    .as_ref()
                    .map(|e| evaluate_plan_expr_by_name(e, row, &col_names))
                    .or_else(|| else_value.clone())
                    .unwrap_or(Value::Null);
                for (filter, expr) in econds {
                    if evaluate_case_filter(filter, row, resolved) {
                        result = evaluate_plan_expr_by_name(expr, row, &col_names);
                        break;
                    }
                }
                result
            } else {
                let mut result = else_value.clone().unwrap_or(Value::Null);
                for (filter, val) in conditions {
                    if evaluate_case_filter(filter, row, resolved) {
                        result = val.clone();
                        break;
                    }
                }
                result
            }
        }
        _ => Value::Null,
    }
}

/// A group-by key source: either a resolved column position or a select column
/// index (for CASE WHEN / Expression aliases used in GROUP BY).
enum GroupBySource {
    /// Index into the resolved columns (row position).
    Resolved(usize),
    /// Index into the select_cols array (for CASE WHEN / Expression aliases).
    SelectCol(usize),
}

/// GROUP BY execution: group rows by key columns, apply aggregates per group.
fn apply_group_by(
    select_cols: &[SelectColumn],
    resolved: &[(usize, String)],
    rows: &[Vec<Value>],
    group_by: &[String],
    having: Option<&Filter>,
) -> Result<Vec<Vec<Value>>> {
    // Find the source of each group-by column: either in resolved columns
    // or as a CASE WHEN / Expression alias in select_cols.
    let col_names_for_eval: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
    let gb_sources: Vec<GroupBySource> = group_by
        .iter()
        .map(|gb_col| {
            // Try resolved columns first.
            if let Some(pos) = resolved.iter().position(|(_, name)| name == gb_col) {
                return Ok(GroupBySource::Resolved(pos));
            }
            // Try CASE WHEN or Expression aliases in select_cols.
            for (i, sc) in select_cols.iter().enumerate() {
                match sc {
                    SelectColumn::CaseWhen { alias, .. } => {
                        if alias.as_deref() == Some(gb_col) {
                            return Ok(GroupBySource::SelectCol(i));
                        }
                    }
                    SelectColumn::Expression { alias, .. } => {
                        if alias.as_deref() == Some(gb_col) {
                            return Ok(GroupBySource::SelectCol(i));
                        }
                    }
                    _ => {}
                }
            }
            Err(ExchangeDbError::Query(format!(
                "GROUP BY column not found: {gb_col}"
            )))
        })
        .collect::<Result<Vec<_>>>()?;

    // Helper to compute the group key value for a given source and row.
    let compute_key_value = |src: &GroupBySource, row: &[Value]| -> Value {
        match src {
            GroupBySource::Resolved(pos) => row[*pos].clone(),
            GroupBySource::SelectCol(idx) => match &select_cols[*idx] {
                SelectColumn::CaseWhen { .. } => {
                    evaluate_case_when_select_col(&select_cols[*idx], row, resolved)
                }
                SelectColumn::Expression { expr, .. } => {
                    evaluate_plan_expr_by_name(expr, row, &col_names_for_eval)
                }
                _ => Value::Null,
            },
        }
    };

    // Group rows by key. We use a Vec to preserve insertion order while using
    // a HashMap for lookup.
    let mut group_map: HashMap<Vec<ValueKey>, usize> = HashMap::new();
    let mut groups: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();

    for row in rows {
        let key: Vec<ValueKey> = gb_sources
            .iter()
            .map(|src| ValueKey(compute_key_value(src, row)))
            .collect();

        if let Some(&idx) = group_map.get(&key) {
            groups[idx].1.push(row.clone());
        } else {
            let key_values: Vec<Value> = gb_sources
                .iter()
                .map(|src| compute_key_value(src, row))
                .collect();
            let idx = groups.len();
            group_map.insert(key, idx);
            groups.push((key_values, vec![row.clone()]));
        }
    }

    // For each group, compute aggregates and build the result row.
    let mut result = Vec::new();
    for (key_values, group_rows) in &groups {
        // Initialize aggregate functions for each aggregate column.
        let mut agg_funcs: Vec<Option<Box<dyn AggregateFunction>>> = select_cols
            .iter()
            .map(|c| match c {
                SelectColumn::Aggregate { function, .. } => {
                    Some(functions::create_aggregate(*function))
                }
                _ => None,
            })
            .collect();

        // Feed rows into aggregates.
        let col_names_for_filter: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
        for row in group_rows {
            for (i, func) in agg_funcs.iter_mut().enumerate() {
                if let Some(f) = func {
                    let (col_name, agg_filter, arg_expr) = match &select_cols[i] {
                        SelectColumn::Aggregate {
                            column,
                            filter,
                            arg_expr,
                            ..
                        } => (column, filter.as_deref(), arg_expr.as_ref()),
                        _ => continue,
                    };

                    // Check FILTER (WHERE ...) clause if present.
                    if let Some(flt) = agg_filter
                        && !evaluate_filter_virtual(flt, row, &col_names_for_filter)
                    {
                        continue;
                    }

                    if let Some(expr) = arg_expr {
                        // Evaluate expression and feed result to aggregate.
                        let val = evaluate_plan_expr_by_name(expr, row, &col_names_for_filter);
                        f.add(&val);
                    } else if col_name == "*" {
                        f.add(&Value::I64(1));
                    } else if let Some(pos) = resolved.iter().position(|(_, n)| n == col_name)
                        && pos < row.len()
                    {
                        f.add(&row[pos]);
                    }
                }
            }
        }

        // Build the result row: for each select column, either use the group key
        // or the aggregate result.
        let result_row: Vec<Value> = select_cols
            .iter()
            .enumerate()
            .map(|(i, col)| match col {
                SelectColumn::Aggregate { .. } => {
                    if let Some(f) = &agg_funcs[i] {
                        f.result()
                    } else {
                        Value::Null
                    }
                }
                SelectColumn::Name(name) => {
                    // Check if this is a group-by column.
                    if let Some(gb_idx) = group_by.iter().position(|gb| gb == name) {
                        key_values[gb_idx].clone()
                    } else if let Some(pos) = resolved.iter().position(|(_, n)| n == name) {
                        // Non-grouped, non-aggregated column: return first value.
                        group_rows
                            .first()
                            .and_then(|r| r.get(pos))
                            .cloned()
                            .unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                SelectColumn::Wildcard => Value::Null,
                SelectColumn::Expression { expr, alias, .. } => {
                    // Check if this expression is a group-by key (by alias).
                    if let Some(a) = alias
                        && let Some(gb_idx) = group_by.iter().position(|gb| gb == a)
                    {
                        return key_values[gb_idx].clone();
                    }
                    // Evaluate the expression against the first row of the group.
                    let col_names: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
                    evaluate_plan_expr_by_name(
                        expr,
                        group_rows.first().unwrap_or(&vec![]),
                        &col_names,
                    )
                }
                SelectColumn::CaseWhen { alias, .. } => {
                    // Check if this CASE WHEN is a group-by key (by alias).
                    if let Some(a) = alias
                        && let Some(gb_idx) = group_by.iter().position(|gb| gb == a)
                    {
                        return key_values[gb_idx].clone();
                    }
                    // Evaluate CASE WHEN against the first row of the group.
                    evaluate_case_when_select_col(
                        col,
                        group_rows.first().unwrap_or(&vec![]),
                        resolved,
                    )
                }
                SelectColumn::ScalarFunction { .. }
                | SelectColumn::WindowFunction(_)
                | SelectColumn::ScalarSubquery { .. } => Value::Null,
            })
            .collect();

        // Apply HAVING filter if present.
        if let Some(having_filter) = having
            && !evaluate_having(having_filter, select_cols, &result_row)
        {
            continue;
        }

        result.push(result_row);
    }

    Ok(result)
}

/// Collect all column names referenced in a Filter (as Strings).
fn collect_filter_column_names(filter: &Filter) -> Vec<String> {
    let mut cols = Vec::new();
    match filter {
        Filter::Eq(c, _)
        | Filter::NotEq(c, _)
        | Filter::Gt(c, _)
        | Filter::Lt(c, _)
        | Filter::Gte(c, _)
        | Filter::Lte(c, _)
        | Filter::IsNull(c)
        | Filter::IsNotNull(c)
        | Filter::Like(c, _)
        | Filter::NotLike(c, _)
        | Filter::ILike(c, _) => {
            cols.push(c.clone());
        }
        Filter::Between(c, _, _) | Filter::BetweenSymmetric(c, _, _) => {
            cols.push(c.clone());
        }
        Filter::In(c, _) | Filter::NotIn(c, _) => {
            cols.push(c.clone());
        }
        Filter::And(parts) | Filter::Or(parts) => {
            for p in parts {
                cols.extend(collect_filter_column_names(p));
            }
        }
        Filter::Not(inner) => {
            cols.extend(collect_filter_column_names(inner));
        }
        Filter::Expression { left, right, .. } => {
            left.collect_columns(&mut cols);
            right.collect_columns(&mut cols);
        }
        _ => {}
    }
    cols
}

/// Expand ROLLUP(a, b, c) into GROUPING SETS ((a,b,c), (a,b), (a), ()).
fn expand_rollup(cols: &[String]) -> Vec<Vec<String>> {
    let mut sets = Vec::new();
    for i in (0..=cols.len()).rev() {
        sets.push(cols[..i].to_vec());
    }
    sets
}

/// Expand CUBE(a, b) into all possible subsets: ((a,b), (a), (b), ()).
fn expand_cube(cols: &[String]) -> Vec<Vec<String>> {
    let n = cols.len();
    let mut sets = Vec::new();
    // Iterate from all bits set to none (so full set comes first).
    for mask in (0..(1u64 << n)).rev() {
        let mut set = Vec::new();
        for (i, col) in cols.iter().enumerate() {
            if mask & (1 << i) != 0 {
                set.push(col.clone());
            }
        }
        sets.push(set);
    }
    sets
}

/// Apply GROUPING SETS: run GROUP BY for each set, UNION ALL results.
/// Columns not in the current grouping set get NULL.
fn apply_grouping_sets(
    select_cols: &[SelectColumn],
    resolved: &[(usize, String)],
    rows: &[Vec<Value>],
    all_group_cols: &[String],
    sets: &[Vec<String>],
    having: Option<&Filter>,
) -> Result<Vec<Vec<Value>>> {
    let mut combined_result = Vec::new();

    for set in sets {
        if set.is_empty() {
            // Empty grouping set: aggregate all rows into a single row.
            let set_rows = apply_group_by_with_nulls(
                select_cols,
                resolved,
                rows,
                &[],
                all_group_cols,
                having,
            )?;
            combined_result.extend(set_rows);
        } else {
            let set_rows = apply_group_by_with_nulls(
                select_cols,
                resolved,
                rows,
                set,
                all_group_cols,
                having,
            )?;
            combined_result.extend(set_rows);
        }
    }

    Ok(combined_result)
}

/// Like `apply_group_by` but sets non-grouped columns to NULL.
fn apply_group_by_with_nulls(
    select_cols: &[SelectColumn],
    resolved: &[(usize, String)],
    rows: &[Vec<Value>],
    active_group_cols: &[String],
    all_group_cols: &[String],
    having: Option<&Filter>,
) -> Result<Vec<Vec<Value>>> {
    if active_group_cols.is_empty() {
        // No grouping: aggregate everything into one row.
        let mut result_row = apply_aggregates(select_cols, resolved, rows)?;
        // Set all group-by columns to NULL.
        if let Some(row) = result_row.first_mut() {
            for (i, col) in select_cols.iter().enumerate() {
                if let SelectColumn::Name(name) = col
                    && all_group_cols.contains(name)
                {
                    row[i] = Value::Null;
                }
            }
        }
        return Ok(result_row);
    }

    let mut grouped = apply_group_by(select_cols, resolved, rows, active_group_cols, having)?;

    // For group-by columns not in active_group_cols, set to NULL.
    for row in &mut grouped {
        for (i, col) in select_cols.iter().enumerate() {
            if let SelectColumn::Name(name) = col
                && all_group_cols.contains(name)
                && !active_group_cols.contains(name)
            {
                row[i] = Value::Null;
            }
        }
    }

    Ok(grouped)
}

/// Evaluate a HAVING filter against a result row. The filter's column names
/// are aggregate function expressions like "count(*)" or "sum(volume)".
fn evaluate_having(filter: &Filter, select_cols: &[SelectColumn], result_row: &[Value]) -> bool {
    match filter {
        Filter::Eq(col, expected) => get_having_value(col, select_cols, result_row)
            .map(|v| v.eq_coerce(expected))
            .unwrap_or(false),
        Filter::NotEq(col, expected) => get_having_value(col, select_cols, result_row)
            .map(|v| !v.eq_coerce(expected))
            .unwrap_or(false),
        Filter::Gt(col, expected) => get_having_value(col, select_cols, result_row)
            .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater))
            .unwrap_or(false),
        Filter::Lt(col, expected) => get_having_value(col, select_cols, result_row)
            .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less))
            .unwrap_or(false),
        Filter::Gte(col, expected) => get_having_value(col, select_cols, result_row)
            .map(|v| {
                matches!(
                    v.cmp_coerce(expected),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::Lte(col, expected) => get_having_value(col, select_cols, result_row)
            .map(|v| {
                matches!(
                    v.cmp_coerce(expected),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::And(parts) => parts
            .iter()
            .all(|p| evaluate_having(p, select_cols, result_row)),
        Filter::Or(parts) => parts
            .iter()
            .any(|p| evaluate_having(p, select_cols, result_row)),
        Filter::Not(inner) => !evaluate_having(inner, select_cols, result_row),
        Filter::Expression { left, op, right } => {
            let lv = evaluate_having_plan_expr(left, select_cols, result_row);
            let rv = evaluate_having_plan_expr(right, select_cols, result_row);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                CompareOp::Lte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
            }
        }
        _ => true, // For unhandled filters, don't exclude the row
    }
}

/// Evaluate a PlanExpr in the context of a HAVING clause, where Column references
/// are aggregate function names or aliases that map to positions in the result row.
fn evaluate_having_plan_expr(
    expr: &PlanExpr,
    select_cols: &[SelectColumn],
    result_row: &[Value],
) -> Value {
    match expr {
        PlanExpr::Column(name) => {
            get_having_value(name, select_cols, result_row).unwrap_or(Value::Null)
        }
        PlanExpr::Literal(v) => v.clone(),
        PlanExpr::BinaryOp { left, op, right } => {
            let lv = evaluate_having_plan_expr(left, select_cols, result_row);
            let rv = evaluate_having_plan_expr(right, select_cols, result_row);
            crate::executor::apply_binary_op(&lv, *op, &rv)
        }
        PlanExpr::UnaryOp { op, expr: inner } => {
            let v = evaluate_having_plan_expr(inner, select_cols, result_row);
            crate::executor::apply_unary_op(*op, &v)
        }
        PlanExpr::Function { .. } => Value::Null,
    }
}

/// Get the value for a HAVING column reference. The column name may be an
/// aggregate expression like "count(*)" which maps to the corresponding
/// position in the result row.
fn get_having_value(
    col: &str,
    select_cols: &[SelectColumn],
    result_row: &[Value],
) -> Option<Value> {
    // First try to match against result column names (aggregate expressions).
    for (i, sc) in select_cols.iter().enumerate() {
        match sc {
            SelectColumn::Aggregate {
                function,
                column,
                alias,
                arg_expr,
                ..
            } => {
                let func_name = format!("{function:?}").to_ascii_lowercase();
                let func_expr = format!("{func_name}({column})");
                // Match either the alias or the function expression form.
                if alias.as_deref() == Some(col) || func_expr == col {
                    return result_row.get(i).cloned();
                }
                // Also match against the full expression form when arg_expr is present.
                // E.g., "sum(d + 1.0)" should match Aggregate { function: Sum, column: "d", arg_expr: Some(...) }
                if arg_expr.is_some() {
                    // Try matching the HAVING column "func(expr_string)" against
                    // the function name prefix.
                    if col.starts_with(&format!("{func_name}(")) && col.ends_with(')') {
                        return result_row.get(i).cloned();
                    }
                }
            }
            SelectColumn::Name(n) => {
                if n == col {
                    return result_row.get(i).cloned();
                }
            }
            SelectColumn::Expression { alias: Some(a), .. } => {
                if a == col {
                    return result_row.get(i).cloned();
                }
            }
            SelectColumn::CaseWhen { alias: Some(a), .. } => {
                if a == col {
                    return result_row.get(i).cloned();
                }
            }
            _ => continue,
        }
    }
    None
}

/// Deduplicate rows for DISTINCT.
fn apply_distinct(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        let key: Vec<ValueKey> = row.iter().map(|v| ValueKey(v.clone())).collect();
        if seen.insert(key) {
            result.push(row);
        }
    }
    result
}

/// Apply scalar functions to transform rows according to the select list.
/// Each output row has one value per select column. For `Name` columns the
/// value is looked up by position; for `ScalarFunction` columns the function
/// is evaluated using the referenced column values from the row.
fn apply_scalar_functions(
    select_cols: &[SelectColumn],
    resolved: &[(usize, String)],
    rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut new_row = Vec::with_capacity(select_cols.len());
        for col in select_cols {
            match col {
                SelectColumn::Name(name) => {
                    // Find position in resolved columns.
                    let pos = resolved.iter().position(|(_, n)| n == name);
                    new_row.push(pos.and_then(|p| row.get(p).cloned()).unwrap_or(Value::Null));
                }
                SelectColumn::ScalarFunction { name, args } => {
                    // Resolve each argument to a Value.
                    let func_args: Vec<Value> = args
                        .iter()
                        .map(|a| match a {
                            SelectColumnArg::Column(col_name) => {
                                let pos = resolved.iter().position(|(_, n)| n == col_name);
                                pos.and_then(|p| row.get(p).cloned()).unwrap_or(Value::Null)
                            }
                            SelectColumnArg::Literal(v) => v.clone(),
                        })
                        .collect();
                    let result = crate::scalar::evaluate_scalar(name, &func_args)
                        .map_err(ExchangeDbError::Query)?;
                    new_row.push(result);
                }
                SelectColumn::Wildcard => {
                    // Should not happen in mixed scalar mode, but handle gracefully.
                    new_row.extend(row.iter().cloned());
                }
                SelectColumn::CaseWhen {
                    conditions,
                    else_value,
                    expr_conditions,
                    expr_else,
                    ..
                } => {
                    let col_names: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
                    if let Some(econds) = expr_conditions {
                        // Expression-based CASE WHEN
                        let mut result = expr_else
                            .as_ref()
                            .map(|e| evaluate_plan_expr_by_name(e, row, &col_names))
                            .or_else(|| else_value.clone())
                            .unwrap_or(Value::Null);
                        for (filter, expr) in econds {
                            if evaluate_case_filter(filter, row, resolved) {
                                result = evaluate_plan_expr_by_name(expr, row, &col_names);
                                break;
                            }
                        }
                        new_row.push(result);
                    } else {
                        let mut result = else_value.clone().unwrap_or(Value::Null);
                        for (filter, val) in conditions {
                            if evaluate_case_filter(filter, row, resolved) {
                                result = val.clone();
                                break;
                            }
                        }
                        new_row.push(result);
                    }
                }
                SelectColumn::Expression { expr, .. } => {
                    // Evaluate the expression against the current row.
                    let col_names: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
                    let val = evaluate_plan_expr_by_name(expr, row, &col_names);
                    new_row.push(val);
                }
                SelectColumn::Aggregate { .. }
                | SelectColumn::WindowFunction(_)
                | SelectColumn::ScalarSubquery { .. } => {
                    // Aggregates/window/subquery are not handled here; pass through.
                    new_row.push(Value::Null);
                }
            }
        }
        out.push(new_row);
    }
    Ok(out)
}

/// Evaluate a filter condition for CASE WHEN against an in-memory row.
fn evaluate_case_filter(filter: &Filter, row: &[Value], resolved: &[(usize, String)]) -> bool {
    match filter {
        Filter::Eq(col, expected) => get_resolved_value(col, row, resolved)
            .map(|v| v.eq_coerce(expected))
            .unwrap_or(false),
        Filter::Gt(col, expected) => get_resolved_value(col, row, resolved)
            .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater))
            .unwrap_or(false),
        Filter::Lt(col, expected) => get_resolved_value(col, row, resolved)
            .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less))
            .unwrap_or(false),
        Filter::Gte(col, expected) => get_resolved_value(col, row, resolved)
            .map(|v| {
                matches!(
                    v.cmp_coerce(expected),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::Lte(col, expected) => get_resolved_value(col, row, resolved)
            .map(|v| {
                matches!(
                    v.cmp_coerce(expected),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::Between(col, low, high) => get_resolved_value(col, row, resolved)
            .map(|v| {
                matches!(
                    v.cmp_coerce(low),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    v.cmp_coerce(high),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::IsNull(col) => {
            let val = get_resolved_value(col, row, resolved);
            matches!(val, None | Some(Value::Null))
        }
        Filter::IsNotNull(col) => {
            let val = get_resolved_value(col, row, resolved);
            matches!(val, Some(v) if v != Value::Null)
        }
        Filter::In(col, list) => get_resolved_value(col, row, resolved)
            .map(|v| list.iter().any(|item| v.eq_coerce(item)))
            .unwrap_or(false),
        Filter::NotIn(col, list) => get_resolved_value(col, row, resolved)
            .map(|v| !list.iter().any(|item| v.eq_coerce(item)))
            .unwrap_or(true),
        Filter::Like(col, pattern) => get_resolved_value(col, row, resolved)
            .map(|v| {
                if let Value::Str(s) = &v {
                    like_match(s, pattern, false)
                } else {
                    false
                }
            })
            .unwrap_or(false),
        Filter::NotLike(col, pattern) => get_resolved_value(col, row, resolved)
            .map(|v| {
                if let Value::Str(s) = &v {
                    !like_match(s, pattern, false)
                } else {
                    true
                }
            })
            .unwrap_or(true),
        Filter::ILike(col, pattern) => get_resolved_value(col, row, resolved)
            .map(|v| {
                if let Value::Str(s) = &v {
                    like_match(s, pattern, true)
                } else {
                    false
                }
            })
            .unwrap_or(false),
        Filter::NotEq(col, expected) => get_resolved_value(col, row, resolved)
            .map(|v| !v.eq_coerce(expected))
            .unwrap_or(true),
        Filter::BetweenSymmetric(col, low, high) => get_resolved_value(col, row, resolved)
            .map(|v| {
                let fwd = matches!(
                    v.cmp_coerce(low),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    v.cmp_coerce(high),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                let rev = matches!(
                    v.cmp_coerce(high),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    v.cmp_coerce(low),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                fwd || rev
            })
            .unwrap_or(false),
        Filter::Not(inner) => !evaluate_case_filter(inner, row, resolved),
        Filter::And(parts) => parts.iter().all(|p| evaluate_case_filter(p, row, resolved)),
        Filter::Or(parts) => parts.iter().any(|p| evaluate_case_filter(p, row, resolved)),
        Filter::Expression { left, op, right } => {
            let col_names: Vec<String> = resolved.iter().map(|(_, n)| n.clone()).collect();
            let lv = evaluate_plan_expr_by_name(left, row, &col_names);
            let rv = evaluate_plan_expr_by_name(right, row, &col_names);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                CompareOp::Lte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
            }
        }
        Filter::Subquery { .. }
        | Filter::InSubquery { .. }
        | Filter::Exists { .. }
        | Filter::All { .. }
        | Filter::Any { .. } => false,
    }
}

fn get_resolved_value(col: &str, row: &[Value], resolved: &[(usize, String)]) -> Option<Value> {
    resolved
        .iter()
        .position(|(_, n)| n == col)
        .and_then(|p| row.get(p).cloned())
}

/// Type alias for CTE storage: column names + rows.
type CteStore = HashMap<String, (Vec<String>, Vec<Vec<Value>>)>;

/// Execute a query plan with CTE context available.
fn execute_with_cte_context(
    db_root: &Path,
    plan: &QueryPlan,
    cte_store: &CteStore,
) -> Result<QueryResult> {
    match plan {
        QueryPlan::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            offset,
            sample_by,
            latest_on,
            group_by,
            group_by_mode: _,
            having,
            distinct,
            distinct_on: _,
        } => {
            // Handle SELECT with no FROM (e.g. SELECT (SELECT count(*) FROM cte))
            if table == "__no_table__" {
                let has_subqueries = columns
                    .iter()
                    .any(|c| matches!(c, SelectColumn::ScalarSubquery { .. }));
                if has_subqueries {
                    let mut col_names = Vec::new();
                    let mut row = Vec::new();
                    for col in columns.iter() {
                        if let SelectColumn::ScalarSubquery { subquery, alias } = col {
                            col_names.push(alias.clone().unwrap_or_else(|| "subquery".to_string()));
                            let sub_result =
                                execute_with_cte_context(db_root, subquery, cte_store)?;
                            let val = match sub_result {
                                QueryResult::Rows { rows, .. }
                                    if !rows.is_empty() && !rows[0].is_empty() =>
                                {
                                    rows[0][0].clone()
                                }
                                _ => Value::Null,
                            };
                            row.push(val);
                        }
                    }
                    return Ok(QueryResult::Rows {
                        columns: col_names,
                        rows: vec![row],
                    });
                }
                // Handle SELECT with literal expressions and no FROM clause
                // (e.g., base case of recursive CTE: SELECT 1 AS n).
                let has_expressions = columns
                    .iter()
                    .any(|c| matches!(c, SelectColumn::Expression { .. }));
                if has_expressions {
                    let mut col_names = Vec::new();
                    let mut row = Vec::new();
                    for col in columns.iter() {
                        match col {
                            SelectColumn::Expression { expr, alias } => {
                                let name = alias.clone().unwrap_or_else(|| "?column?".to_string());
                                col_names.push(name);
                                let val = evaluate_plan_expr_standalone(expr);
                                row.push(val);
                            }
                            SelectColumn::Name(name) => {
                                col_names.push(name.clone());
                                row.push(Value::Null);
                            }
                            _ => {
                                col_names.push("?column?".to_string());
                                row.push(Value::Null);
                            }
                        }
                    }
                    return Ok(QueryResult::Rows {
                        columns: col_names,
                        rows: vec![row],
                    });
                }
                // For non-subquery no-table queries (system functions, etc.)
                return execute_select(
                    db_root,
                    table,
                    columns,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    *offset,
                    sample_by.as_ref(),
                    latest_on.as_ref(),
                    group_by,
                    having.as_ref(),
                    *distinct,
                );
            }
            // Check if the table is a CTE.
            if let Some((cte_cols, cte_rows)) = cte_store.get(table.as_str()) {
                return execute_select_from_virtual(
                    cte_cols,
                    cte_rows,
                    columns,
                    filter.as_ref(),
                    order_by,
                    *limit,
                    group_by,
                    having.as_ref(),
                    *distinct,
                    cte_store,
                );
            }
            execute_select(
                db_root,
                table,
                columns,
                filter.as_ref(),
                order_by,
                *limit,
                *offset,
                sample_by.as_ref(),
                latest_on.as_ref(),
                group_by,
                having.as_ref(),
                *distinct,
            )
        }
        QueryPlan::SetOperation {
            op,
            left,
            right,
            all,
            limit,
        } => execute_set_operation(db_root, *op, left, right, *all, *limit, cte_store),
        QueryPlan::WithCte { ctes, body } => {
            // Nested CTEs: first materialize them into the store, then execute body.
            let mut new_store = cte_store.clone();
            for cte in ctes {
                let result = execute_with_cte_context(db_root, &cte.query, &new_store)?;
                if let QueryResult::Rows { columns, rows } = result {
                    new_store.insert(cte.name.clone(), (columns, rows));
                }
            }
            execute_with_cte_context(db_root, body, &new_store)
        }
        QueryPlan::DerivedScan {
            subquery,
            alias: _,
            columns,
            filter,
            order_by,
            limit,
            group_by,
            having,
            distinct,
        } => execute_derived_scan(
            db_root,
            subquery,
            columns,
            filter.as_ref(),
            order_by,
            *limit,
            group_by,
            having.as_ref(),
            *distinct,
            cte_store,
        ),
        // For JOINs, materialize any CTE tables as temporary on-disk tables.
        QueryPlan::Join {
            left_table,
            right_table,
            ..
        } => {
            let cte_tables =
                materialize_cte_tables(db_root, cte_store, &[left_table, right_table])?;
            let result = execute(db_root, plan);
            // Clean up materialized CTE tables.
            for name in &cte_tables {
                let _ = exchange_core::table::drop_table(db_root, name);
            }
            result
        }
        // For other plan types, delegate to the standard executor.
        other => execute(db_root, other),
    }
}

/// Materialize CTE results as temporary on-disk tables for JOIN execution.
fn materialize_cte_tables(
    db_root: &Path,
    cte_store: &CteStore,
    table_names: &[&String],
) -> Result<Vec<String>> {
    let mut materialized = Vec::new();
    for name in table_names {
        if let Some((col_names, rows)) = cte_store.get(name.as_str()) {
            // Only materialize if the table doesn't already exist on disk.
            let table_dir = db_root.join(name.as_str());
            if table_dir.exists() {
                continue;
            }
            // Infer column types from data and create a temporary table.
            let col_defs = infer_column_defs(col_names, rows);
            let mut builder = exchange_core::table::TableBuilder::new(name.as_str());
            // Find or create a timestamp column.
            let has_timestamp = col_defs.iter().any(|d| {
                let ct: exchange_common::types::ColumnType = d.col_type.into();
                ct == exchange_common::types::ColumnType::Timestamp
            });
            for def in &col_defs {
                let ct: exchange_common::types::ColumnType = def.col_type.into();
                builder = builder.column(&def.name, ct);
            }
            if !has_timestamp {
                builder = builder.column("_cte_ts", exchange_common::types::ColumnType::Timestamp);
                builder = builder.timestamp("_cte_ts");
            } else {
                // Use the first timestamp column.
                if let Some(ts_def) = col_defs.iter().find(|d| {
                    let ct: exchange_common::types::ColumnType = d.col_type.into();
                    ct == exchange_common::types::ColumnType::Timestamp
                }) {
                    builder = builder.timestamp(&ts_def.name);
                }
            }
            builder = builder.partition_by(exchange_common::types::PartitionBy::None);
            builder.build(db_root)?;

            // Write rows.
            if !rows.is_empty() {
                let mut writer = exchange_core::table::TableWriter::open(db_root, name.as_str())?;
                for (i, row) in rows.iter().enumerate() {
                    let ts =
                        exchange_common::types::Timestamp(1_710_460_800_000_000_000 + i as i64);
                    let col_values: Vec<exchange_core::table::ColumnValue<'_>> =
                        row.iter().map(|v| plan_value_to_column_value(v)).collect();
                    writer.write_row(ts, &col_values)?;
                }
                writer.flush()?;
            }
            materialized.push(name.to_string());
        }
    }
    Ok(materialized)
}

/// Execute a WITH CTE plan, including recursive CTE support.
fn execute_with_cte(
    db_root: &Path,
    ctes: &[CteDefinition],
    body: &QueryPlan,
) -> Result<QueryResult> {
    let mut cte_store: CteStore = HashMap::new();

    // Materialize each CTE in order.
    for cte in ctes {
        if cte.recursive {
            // Recursive CTE: the query is a UNION ALL of base case and recursive step.
            // Execute by iterating until no new rows are produced.
            let result = execute_recursive_cte(db_root, &cte.name, &cte.query, &cte_store)?;
            if let QueryResult::Rows { columns, rows } = result {
                cte_store.insert(cte.name.clone(), (columns, rows));
            } else {
                return Err(ExchangeDbError::Query(format!(
                    "recursive CTE '{}' did not produce rows",
                    cte.name
                )));
            }
        } else {
            let result = execute_with_cte_context(db_root, &cte.query, &cte_store)?;
            if let QueryResult::Rows { columns, rows } = result {
                cte_store.insert(cte.name.clone(), (columns, rows));
            } else {
                return Err(ExchangeDbError::Query(format!(
                    "CTE '{}' did not produce rows",
                    cte.name
                )));
            }
        }
    }

    // Execute the main query body with CTEs available.
    let mut result = execute_with_cte_context(db_root, body, &cte_store)?;

    // Post-process: evaluate scalar subqueries in SELECT columns.
    if let QueryPlan::Select { columns, .. } = body {
        let has_scalar_subqueries = columns
            .iter()
            .any(|c| matches!(c, SelectColumn::ScalarSubquery { .. }));
        if has_scalar_subqueries {
            result = evaluate_scalar_subqueries_in_result(db_root, &result, columns, &cte_store)?;
        }
    }

    Ok(result)
}

/// Execute a recursive CTE by iterating until fixpoint.
///
/// A recursive CTE has the form:
///   WITH RECURSIVE name AS (base_case UNION ALL recursive_step)
/// We execute the base case first, then repeatedly execute the recursive step
/// with the previous iteration's results as the CTE contents, collecting all
/// rows. Stops when no new rows are produced or max iterations reached.
fn execute_recursive_cte(
    db_root: &Path,
    cte_name: &str,
    query: &QueryPlan,
    parent_cte_store: &CteStore,
) -> Result<QueryResult> {
    const MAX_ITERATIONS: usize = 1000;

    // The query plan should be a SetOperation (UNION ALL) with left = base, right = recursive.
    let (base_plan, recursive_plan, _all) = match query {
        QueryPlan::SetOperation {
            op: SetOp::Union,
            left,
            right,
            all,
            ..
        } => (left.as_ref(), right.as_ref(), *all),
        _ => {
            // If not a union, just execute normally (non-recursive usage).
            return execute_with_cte_context(db_root, query, parent_cte_store);
        }
    };

    // Execute base case.
    let base_result = execute_with_cte_context(db_root, base_plan, parent_cte_store)?;
    let (col_names, base_rows) = match base_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "recursive CTE base case did not produce rows".into(),
            ));
        }
    };

    let mut all_rows = base_rows.clone();
    let mut working_rows = base_rows;

    for _iteration in 0..MAX_ITERATIONS {
        if working_rows.is_empty() {
            break;
        }

        // Set up CTE store with current working rows.
        let mut cte_store = parent_cte_store.clone();
        cte_store.insert(cte_name.to_string(), (col_names.clone(), working_rows));

        // Execute recursive step.
        let step_result = execute_with_cte_context(db_root, recursive_plan, &cte_store)?;
        let new_rows = match step_result {
            QueryResult::Rows { rows, .. } => rows,
            _ => break,
        };

        if new_rows.is_empty() {
            break;
        }

        all_rows.extend(new_rows.clone());
        working_rows = new_rows;
    }

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: all_rows,
    })
}

/// Evaluate scalar subqueries in SELECT column results.
fn evaluate_scalar_subqueries_in_result(
    db_root: &Path,
    result: &QueryResult,
    columns: &[SelectColumn],
    cte_store: &CteStore,
) -> Result<QueryResult> {
    let (col_names, rows) = match result {
        QueryResult::Rows {
            columns: cn,
            rows: r,
        } => (cn.clone(), r.clone()),
        other => return Ok(other.clone()),
    };

    // Evaluate each scalar subquery once.
    let mut subquery_values: Vec<Option<Value>> = Vec::new();
    for col in columns {
        if let SelectColumn::ScalarSubquery { subquery, .. } = col {
            let sub_result = execute_with_cte_context(db_root, subquery, cte_store)?;
            let val = match sub_result {
                QueryResult::Rows { rows, .. } => {
                    if !rows.is_empty() && !rows[0].is_empty() {
                        rows[0][0].clone()
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };
            subquery_values.push(Some(val));
        } else {
            subquery_values.push(None);
        }
    }

    // Build new column names and rows with subquery values substituted.
    let mut new_col_names = Vec::new();
    let mut new_rows: Vec<Vec<Value>> = if rows.is_empty() {
        vec![Vec::new()]
    } else {
        rows.clone()
    };

    let mut orig_col_idx = 0;
    for (i, col) in columns.iter().enumerate() {
        match col {
            SelectColumn::ScalarSubquery { alias, .. } => {
                new_col_names.push(alias.clone().unwrap_or_else(|| "subquery".to_string()));
                let val = subquery_values[i].clone().unwrap_or(Value::Null);
                for row in &mut new_rows {
                    // Replace or insert the scalar subquery value.
                    if orig_col_idx < row.len() {
                        row[orig_col_idx] = val.clone();
                    } else {
                        row.push(val.clone());
                    }
                }
                orig_col_idx += 1;
            }
            _ => {
                if orig_col_idx < col_names.len() {
                    new_col_names.push(col_names[orig_col_idx].clone());
                }
                orig_col_idx += 1;
            }
        }
    }

    Ok(QueryResult::Rows {
        columns: new_col_names,
        rows: new_rows,
    })
}

/// Execute a SELECT against virtual (in-memory) rows, such as CTE results or subquery results.
#[allow(clippy::too_many_arguments)]
fn execute_select_from_virtual(
    source_cols: &[String],
    source_rows: &[Vec<Value>],
    columns: &[SelectColumn],
    filter: Option<&Filter>,
    order_by: &[OrderBy],
    limit: Option<u64>,
    group_by: &[String],
    having: Option<&Filter>,
    distinct: bool,
    _cte_store: &CteStore,
) -> Result<QueryResult> {
    // Build a pseudo "resolved" mapping from source columns.
    let _resolved: Vec<(usize, String)> = source_cols
        .iter()
        .enumerate()
        .map(|(i, name)| (i, name.clone()))
        .collect();

    // For GROUP BY, ensure group-by columns are included in the scan.
    let mut scan_columns = columns.to_vec();
    for gb_col in group_by {
        let already_present = scan_columns.iter().any(|c| match c {
            SelectColumn::Name(n) => n == gb_col,
            SelectColumn::Wildcard => true,
            SelectColumn::Aggregate { column, .. } => column == gb_col,
            SelectColumn::ScalarFunction { .. } => false,
            SelectColumn::WindowFunction(_) | SelectColumn::ScalarSubquery { .. } => false,
            SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
            SelectColumn::Expression { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
        });
        if !already_present {
            // Don't add as a table column if it's an alias for a CASE WHEN or Expression.
            let is_alias = columns.iter().any(|c| match c {
                SelectColumn::CaseWhen { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
                SelectColumn::Expression { alias, .. } => alias.as_deref() == Some(gb_col.as_str()),
                _ => false,
            });
            if !is_alias {
                scan_columns.push(SelectColumn::Name(gb_col.clone()));
            }
        }
    }

    // Determine which columns to scan. For wildcard, use all.
    let selected_cols = resolve_virtual_columns(source_cols, &scan_columns)?;

    // Check if any column is an Expression that needs evaluation.
    let has_expressions = scan_columns
        .iter()
        .any(|c| matches!(c, SelectColumn::Expression { .. }));

    // Filter rows.
    let mut all_rows: Vec<Vec<Value>> = source_rows
        .iter()
        .filter(|row| {
            if let Some(f) = filter {
                evaluate_filter_virtual(f, row, source_cols)
            } else {
                true
            }
        })
        .map(|row| {
            if has_expressions {
                // Project with expression evaluation.
                let mut result_row = Vec::new();
                for col in &scan_columns {
                    match col {
                        SelectColumn::Expression { expr, .. } => {
                            let val = evaluate_plan_expr_by_name(expr, row, source_cols);
                            result_row.push(val);
                        }
                        SelectColumn::Wildcard => {
                            for (i, _) in source_cols.iter().enumerate() {
                                result_row.push(row.get(i).cloned().unwrap_or(Value::Null));
                            }
                        }
                        SelectColumn::Name(name) => {
                            if let Some(idx) = source_cols.iter().position(|n| n == name) {
                                result_row.push(row.get(idx).cloned().unwrap_or(Value::Null));
                            } else {
                                result_row.push(Value::Null);
                            }
                        }
                        SelectColumn::Aggregate { column, .. } => {
                            // For aggregates, project the underlying column.
                            if column == "*" {
                                if !source_cols.is_empty() {
                                    result_row.push(row.first().cloned().unwrap_or(Value::Null));
                                }
                            } else if let Some(idx) = source_cols.iter().position(|n| n == column) {
                                result_row.push(row.get(idx).cloned().unwrap_or(Value::Null));
                            }
                        }
                        _ => {
                            // Fallback: use the selected_cols indices.
                        }
                    }
                }
                if result_row.is_empty() {
                    selected_cols
                        .iter()
                        .map(|&(idx, _)| row.get(idx).cloned().unwrap_or(Value::Null))
                        .collect()
                } else {
                    result_row
                }
            } else {
                // Standard projection by column index.
                selected_cols
                    .iter()
                    .map(|&(idx, _)| row.get(idx).cloned().unwrap_or(Value::Null))
                    .collect()
            }
        })
        .collect();

    // GROUP BY with aggregation.
    let has_aggregates = columns.iter().any(|c| match c {
        SelectColumn::Aggregate { .. } => true,
        SelectColumn::Expression { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    });
    if !group_by.is_empty() {
        all_rows = apply_group_by(columns, &selected_cols, &all_rows, group_by, having)?;
    } else if has_aggregates {
        all_rows = apply_aggregates(columns, &selected_cols, &all_rows)?;
    }

    // ORDER BY.
    if !order_by.is_empty() {
        if !group_by.is_empty() || has_aggregates {
            let result_cols = result_column_names(columns, &selected_cols);
            let fake_resolved: Vec<(usize, String)> = result_cols.into_iter().enumerate().collect();
            apply_order_by(&mut all_rows, &fake_resolved, order_by);
        } else {
            apply_order_by(&mut all_rows, &selected_cols, order_by);
        }
    }

    // LIMIT.
    if let Some(lim) = limit {
        all_rows.truncate(lim as usize);
    }

    // DISTINCT.
    if distinct {
        all_rows = apply_distinct(all_rows);
    }

    let col_names = result_column_names(columns, &selected_cols);

    Ok(QueryResult::Rows {
        columns: col_names,
        rows: all_rows,
    })
}

/// Find a column in source columns by name, trying exact match first then suffix match.
fn find_virtual_col(source_cols: &[String], name: &str) -> Option<(usize, String)> {
    // Exact match.
    if let Some(idx) = source_cols.iter().position(|n| n == name) {
        return Some((idx, name.to_string()));
    }
    // Suffix match: "symbol" matches "o.symbol".
    let suffix = format!(".{name}");
    if let Some(idx) = source_cols.iter().position(|n| n.ends_with(&suffix)) {
        return Some((idx, name.to_string()));
    }
    None
}

/// Resolve columns for a virtual (in-memory) source.
fn resolve_virtual_columns(
    source_cols: &[String],
    columns: &[SelectColumn],
) -> Result<Vec<(usize, String)>> {
    let mut result = Vec::new();
    for col in columns {
        match col {
            SelectColumn::Wildcard => {
                for (i, name) in source_cols.iter().enumerate() {
                    result.push((i, name.clone()));
                }
            }
            SelectColumn::Name(name) => {
                let (idx, resolved_name) =
                    find_virtual_col(source_cols, name).ok_or_else(|| {
                        ExchangeDbError::Query(format!(
                            "column '{}' not found in virtual source",
                            name
                        ))
                    })?;
                result.push((idx, resolved_name));
            }
            SelectColumn::Aggregate {
                column, arg_expr, ..
            } => {
                if column == "*" {
                    if !source_cols.is_empty() {
                        result.push((0, source_cols[0].clone()));
                    }
                } else {
                    let (idx, resolved_name) =
                        find_virtual_col(source_cols, column).ok_or_else(|| {
                            ExchangeDbError::Query(format!(
                                "column '{}' not found in virtual source",
                                column
                            ))
                        })?;
                    result.push((idx, resolved_name));
                }
                // Also resolve columns referenced by arg_expr (e.g., sum(f.price * f.filled)).
                if let Some(expr) = arg_expr {
                    let mut cols = Vec::new();
                    expr.collect_columns(&mut cols);
                    for col_name in &cols {
                        let already = result.iter().any(|(_, n)| n == col_name);
                        if !already
                            && let Some((idx, resolved)) = find_virtual_col(source_cols, col_name)
                        {
                            result.push((idx, resolved));
                        }
                    }
                }
            }
            SelectColumn::ScalarFunction { args, .. } => {
                for arg in args {
                    if let SelectColumnArg::Column(name) = arg {
                        let already = result.iter().any(|(_, n)| n == name);
                        if !already {
                            let idx =
                                source_cols.iter().position(|n| n == name).ok_or_else(|| {
                                    ExchangeDbError::Query(format!(
                                        "column '{}' not found in virtual source",
                                        name
                                    ))
                                })?;
                            result.push((idx, name.clone()));
                        }
                    }
                }
            }
            SelectColumn::WindowFunction(_) | SelectColumn::ScalarSubquery { .. } => {
                // Not handling window functions / scalar subqueries for virtual sources for now.
            }
            SelectColumn::CaseWhen { conditions, .. } => {
                // Resolve columns referenced in CASE WHEN conditions for virtual sources.
                for (filter, _) in conditions {
                    collect_virtual_filter_cols(filter, source_cols, &mut result);
                }
            }
            SelectColumn::Expression { expr, .. } => {
                let mut cols = Vec::new();
                expr.collect_columns(&mut cols);
                for col_name in &cols {
                    let already = result.iter().any(|(_, n)| n == col_name);
                    if !already && let Some(idx) = source_cols.iter().position(|n| n == col_name) {
                        result.push((idx, col_name.clone()));
                    }
                }
            }
        }
    }
    // Ensure group-by columns are included.
    Ok(result)
}

/// Collect column references from a filter for virtual sources.
fn collect_virtual_filter_cols(
    filter: &Filter,
    source_cols: &[String],
    result: &mut Vec<(usize, String)>,
) {
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
        | Filter::ILike(col, _)
        | Filter::Between(col, _, _)
        | Filter::BetweenSymmetric(col, _, _)
        | Filter::In(col, _)
        | Filter::NotIn(col, _) => {
            let already = result.iter().any(|(_, n)| n == col);
            if !already && let Some(idx) = source_cols.iter().position(|n| n == col) {
                result.push((idx, col.clone()));
            }
        }
        Filter::And(parts) | Filter::Or(parts) => {
            for p in parts {
                collect_virtual_filter_cols(p, source_cols, result);
            }
        }
        Filter::Not(inner) => {
            collect_virtual_filter_cols(inner, source_cols, result);
        }
        _ => {}
    }
}

/// Evaluate a filter against a virtual row using column names instead of TableMeta.
pub(crate) fn evaluate_filter_virtual(
    filter: &Filter,
    row: &[Value],
    col_names: &[String],
) -> bool {
    match filter {
        Filter::Eq(col, expected) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| v.eq_coerce(expected))
            .unwrap_or(false),
        Filter::NotEq(col, expected) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| !v.eq_coerce(expected))
            .unwrap_or(true),
        Filter::Gt(col, expected) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Greater))
            .unwrap_or(false),
        Filter::Lt(col, expected) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| v.cmp_coerce(expected) == Some(std::cmp::Ordering::Less))
            .unwrap_or(false),
        Filter::Gte(col, expected) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| {
                matches!(
                    v.cmp_coerce(expected),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::Lte(col, expected) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| {
                matches!(
                    v.cmp_coerce(expected),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::Between(col, low, high) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| {
                matches!(
                    v.cmp_coerce(low),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    v.cmp_coerce(high),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                )
            })
            .unwrap_or(false),
        Filter::BetweenSymmetric(col, low, high) => get_virtual_value(col, row, col_names)
            .as_ref()
            .map(|v| {
                let fwd = matches!(
                    v.cmp_coerce(low),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    v.cmp_coerce(high),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                let rev = matches!(
                    v.cmp_coerce(high),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ) && matches!(
                    v.cmp_coerce(low),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                );
                fwd || rev
            })
            .unwrap_or(false),
        Filter::And(parts) => parts
            .iter()
            .all(|p| evaluate_filter_virtual(p, row, col_names)),
        Filter::Or(parts) => parts
            .iter()
            .any(|p| evaluate_filter_virtual(p, row, col_names)),
        Filter::IsNull(col) => {
            let val = get_virtual_value(col, row, col_names);
            matches!(val.as_ref(), None | Some(Value::Null))
        }
        Filter::IsNotNull(col) => {
            let val = get_virtual_value(col, row, col_names);
            matches!(val.as_ref(), Some(v) if *v != Value::Null)
        }
        Filter::In(col, list) => {
            let val = get_virtual_value(col, row, col_names);
            val.as_ref()
                .map(|v| list.iter().any(|item| v.eq_coerce(item)))
                .unwrap_or(false)
        }
        Filter::NotIn(col, list) => {
            let val = get_virtual_value(col, row, col_names);
            val.as_ref()
                .map(|v| !list.iter().any(|item| v.eq_coerce(item)))
                .unwrap_or(true)
        }
        Filter::Like(col, pattern) => {
            let val = get_virtual_value(col, row, col_names);
            val.as_ref()
                .map(|v| {
                    if let Value::Str(s) = v {
                        like_match(s, pattern, false)
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
        }
        Filter::NotLike(col, pattern) => {
            let val = get_virtual_value(col, row, col_names);
            val.as_ref()
                .map(|v| {
                    if let Value::Str(s) = v {
                        !like_match(s, pattern, false)
                    } else {
                        true
                    }
                })
                .unwrap_or(true)
        }
        Filter::ILike(col, pattern) => {
            let val = get_virtual_value(col, row, col_names);
            val.as_ref()
                .map(|v| {
                    if let Value::Str(s) = v {
                        like_match(s, pattern, true)
                    } else {
                        false
                    }
                })
                .unwrap_or(false)
        }
        Filter::Not(inner) => !evaluate_filter_virtual(inner, row, col_names),
        Filter::Subquery { .. } | Filter::InSubquery { .. } | Filter::Exists { .. } => false,
        Filter::Expression { left, op, right } => {
            let lv = evaluate_plan_expr_by_name(left, row, col_names);
            let rv = evaluate_plan_expr_by_name(right, row, col_names);
            match op {
                CompareOp::Eq => lv.eq_coerce(&rv),
                CompareOp::NotEq => !lv.eq_coerce(&rv),
                CompareOp::Gt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Greater),
                CompareOp::Lt => lv.cmp_coerce(&rv) == Some(std::cmp::Ordering::Less),
                CompareOp::Gte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                CompareOp::Lte => matches!(
                    lv.cmp_coerce(&rv),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
            }
        }
        Filter::All { .. } | Filter::Any { .. } => false,
    }
}

fn get_virtual_value(col: &str, row: &[Value], col_names: &[String]) -> Option<Value> {
    // Exact match first.
    if let Some(idx) = col_names.iter().position(|n| n == col) {
        return row.get(idx).cloned();
    }
    // Suffix match: "price" matches "t.price".
    let suffix = format!(".{col}");
    col_names
        .iter()
        .position(|n| n.ends_with(&suffix))
        .and_then(|idx| row.get(idx).cloned())
}

/// Execute a set operation (UNION / INTERSECT / EXCEPT).
fn execute_set_operation(
    db_root: &Path,
    op: SetOp,
    left: &QueryPlan,
    right: &QueryPlan,
    all: bool,
    limit: Option<u64>,
    cte_store: &CteStore,
) -> Result<QueryResult> {
    let left_result = execute_with_cte_context(db_root, left, cte_store)?;
    let right_result = execute_with_cte_context(db_root, right, cte_store)?;

    let (left_cols, left_rows) = match left_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "set operation requires SELECT queries".into(),
            ));
        }
    };

    let (right_cols, right_rows) = match right_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "set operation requires SELECT queries".into(),
            ));
        }
    };

    // Validate column count matches.
    if left_cols.len() != right_cols.len() {
        return Err(ExchangeDbError::Query(format!(
            "set operation column count mismatch: left has {}, right has {}",
            left_cols.len(),
            right_cols.len()
        )));
    }

    let result_rows = match op {
        SetOp::Union => {
            let mut combined = left_rows;
            combined.extend(right_rows);
            if all {
                combined
            } else {
                apply_distinct(combined)
            }
        }
        SetOp::Intersect => {
            // Keep rows from left that also appear in right.
            let right_set: HashSet<Vec<ValueKey>> = right_rows
                .iter()
                .map(|r| r.iter().map(|v| ValueKey(v.clone())).collect())
                .collect();
            let mut result: Vec<Vec<Value>> = left_rows
                .into_iter()
                .filter(|row| {
                    let key: Vec<ValueKey> = row.iter().map(|v| ValueKey(v.clone())).collect();
                    right_set.contains(&key)
                })
                .collect();
            if !all {
                result = apply_distinct(result);
            }
            result
        }
        SetOp::Except => {
            // Keep rows from left that do NOT appear in right.
            let right_set: HashSet<Vec<ValueKey>> = right_rows
                .iter()
                .map(|r| r.iter().map(|v| ValueKey(v.clone())).collect())
                .collect();
            let mut result: Vec<Vec<Value>> = left_rows
                .into_iter()
                .filter(|row| {
                    let key: Vec<ValueKey> = row.iter().map(|v| ValueKey(v.clone())).collect();
                    !right_set.contains(&key)
                })
                .collect();
            if !all {
                result = apply_distinct(result);
            }
            result
        }
    };

    let final_rows = if let Some(n) = limit {
        result_rows.into_iter().take(n as usize).collect()
    } else {
        result_rows
    };

    Ok(QueryResult::Rows {
        columns: left_cols,
        rows: final_rows,
    })
}

/// Execute a derived scan (subquery in FROM).
#[allow(clippy::too_many_arguments)]
fn execute_derived_scan(
    db_root: &Path,
    subquery: &QueryPlan,
    columns: &[SelectColumn],
    filter: Option<&Filter>,
    order_by: &[OrderBy],
    limit: Option<u64>,
    group_by: &[String],
    having: Option<&Filter>,
    distinct: bool,
    cte_store: &CteStore,
) -> Result<QueryResult> {
    // Execute the subquery.
    let sub_result = execute_with_cte_context(db_root, subquery, cte_store)?;
    let (sub_cols, sub_rows) = match sub_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "derived table must produce rows".into(),
            ));
        }
    };

    // Now execute the outer query against the subquery results.
    // First, resolve any subquery filters by executing them.
    let resolved_filter = if let Some(f) = filter {
        Some(resolve_subquery_filters(db_root, f, cte_store)?)
    } else {
        None
    };

    execute_select_from_virtual(
        &sub_cols,
        &sub_rows,
        columns,
        resolved_filter.as_ref(),
        order_by,
        limit,
        group_by,
        having,
        distinct,
        cte_store,
    )
}

/// Check if a filter tree contains any subquery filters (including IN/EXISTS).
fn has_subquery_filters(filter: &Filter) -> bool {
    match filter {
        Filter::Subquery { .. }
        | Filter::InSubquery { .. }
        | Filter::Exists { .. }
        | Filter::All { .. }
        | Filter::Any { .. } => true,
        Filter::And(parts) | Filter::Or(parts) => parts.iter().any(has_subquery_filters),
        _ => false,
    }
}

/// Resolve subquery filters by executing the subqueries and replacing them with
/// concrete values.
fn resolve_subquery_filters(
    db_root: &Path,
    filter: &Filter,
    cte_store: &CteStore,
) -> Result<Filter> {
    match filter {
        Filter::Subquery {
            column,
            op,
            subquery,
        } => {
            let result = execute_with_cte_context(db_root, subquery, cte_store)?;
            let scalar_value = match result {
                QueryResult::Rows { rows, .. } => {
                    if rows.len() != 1 || rows[0].len() != 1 {
                        return Err(ExchangeDbError::Query(
                            "scalar subquery must return exactly one row with one column".into(),
                        ));
                    }
                    rows[0][0].clone()
                }
                _ => {
                    return Err(ExchangeDbError::Query(
                        "scalar subquery did not produce rows".into(),
                    ));
                }
            };
            match op {
                CompareOp::Eq => Ok(Filter::Eq(column.clone(), scalar_value)),
                CompareOp::Gt => Ok(Filter::Gt(column.clone(), scalar_value)),
                CompareOp::Lt => Ok(Filter::Lt(column.clone(), scalar_value)),
                CompareOp::NotEq => Ok(Filter::NotEq(column.clone(), scalar_value)),
                CompareOp::Gte => Ok(Filter::Gte(column.clone(), scalar_value)),
                CompareOp::Lte => Ok(Filter::Lte(column.clone(), scalar_value)),
            }
        }
        Filter::And(parts) => {
            let resolved: Vec<Filter> = parts
                .iter()
                .map(|p| resolve_subquery_filters(db_root, p, cte_store))
                .collect::<Result<Vec<_>>>()?;
            Ok(Filter::And(resolved))
        }
        Filter::Or(parts) => {
            let resolved: Vec<Filter> = parts
                .iter()
                .map(|p| resolve_subquery_filters(db_root, p, cte_store))
                .collect::<Result<Vec<_>>>()?;
            Ok(Filter::Or(resolved))
        }
        Filter::InSubquery {
            column,
            subquery,
            negated,
        } => {
            let result = execute_with_cte_context(db_root, subquery, cte_store)?;
            let values = match result {
                QueryResult::Rows { rows, .. } => rows
                    .into_iter()
                    .filter_map(|row| row.into_iter().next())
                    .collect::<Vec<Value>>(),
                _ => {
                    return Err(ExchangeDbError::Query(
                        "IN subquery did not produce rows".into(),
                    ));
                }
            };
            if *negated {
                Ok(Filter::NotIn(column.clone(), values))
            } else {
                Ok(Filter::In(column.clone(), values))
            }
        }
        Filter::Exists { subquery, negated } => {
            let result = execute_with_cte_context(db_root, subquery, cte_store)?;
            let has_rows = matches!(result, QueryResult::Rows { ref rows, .. } if !rows.is_empty());
            let passes = if *negated { !has_rows } else { has_rows };
            if passes {
                // Trivially true: empty AND evaluates to true.
                Ok(Filter::And(Vec::new()))
            } else {
                // Trivially false: match on a non-existent column.
                Ok(Filter::Eq(
                    "__nonexistent_exists_check__".to_string(),
                    Value::Null,
                ))
            }
        }
        Filter::All {
            column,
            op,
            subquery,
        } => {
            let result = execute_with_cte_context(db_root, subquery, cte_store)?;
            let values = match result {
                QueryResult::Rows { rows, .. } => rows
                    .into_iter()
                    .filter_map(|row| row.into_iter().next())
                    .collect::<Vec<Value>>(),
                _ => {
                    return Err(ExchangeDbError::Query(
                        "ALL subquery did not produce rows".into(),
                    ));
                }
            };
            // Resolve ALL: column op ALL(...) -> AND of column op each value
            if values.is_empty() {
                // ALL with empty set is TRUE.
                Ok(Filter::And(Vec::new()))
            } else {
                let filters: Vec<Filter> = values
                    .into_iter()
                    .map(|v| match op {
                        CompareOp::Eq => Filter::Eq(column.clone(), v),
                        CompareOp::NotEq => Filter::NotEq(column.clone(), v),
                        CompareOp::Gt => Filter::Gt(column.clone(), v),
                        CompareOp::Lt => Filter::Lt(column.clone(), v),
                        CompareOp::Gte => Filter::Gte(column.clone(), v),
                        CompareOp::Lte => Filter::Lte(column.clone(), v),
                    })
                    .collect();
                Ok(Filter::And(filters))
            }
        }
        Filter::Any {
            column,
            op,
            subquery,
        } => {
            let result = execute_with_cte_context(db_root, subquery, cte_store)?;
            let values = match result {
                QueryResult::Rows { rows, .. } => rows
                    .into_iter()
                    .filter_map(|row| row.into_iter().next())
                    .collect::<Vec<Value>>(),
                _ => {
                    return Err(ExchangeDbError::Query(
                        "ANY subquery did not produce rows".into(),
                    ));
                }
            };
            // Resolve ANY: column op ANY(...) -> OR of column op each value
            if values.is_empty() {
                // ANY with empty set is FALSE.
                Ok(Filter::Eq(
                    "__nonexistent_any_check__".to_string(),
                    Value::Null,
                ))
            } else if matches!(op, CompareOp::Eq) {
                // Optimized: column = ANY(...) -> column IN (...)
                Ok(Filter::In(column.clone(), values))
            } else {
                let filters: Vec<Filter> = values
                    .into_iter()
                    .map(|v| match op {
                        CompareOp::Eq => Filter::Eq(column.clone(), v),
                        CompareOp::NotEq => Filter::NotEq(column.clone(), v),
                        CompareOp::Gt => Filter::Gt(column.clone(), v),
                        CompareOp::Lt => Filter::Lt(column.clone(), v),
                        CompareOp::Gte => Filter::Gte(column.clone(), v),
                        CompareOp::Lte => Filter::Lte(column.clone(), v),
                    })
                    .collect();
                Ok(Filter::Or(filters))
            }
        }
        Filter::Not(inner) => {
            if has_subquery_filters(inner) {
                let resolved = resolve_subquery_filters(db_root, inner, cte_store)?;
                Ok(Filter::Not(Box::new(resolved)))
            } else {
                Ok(filter.clone())
            }
        }
        // Non-subquery filters pass through unchanged.
        other => Ok(other.clone()),
    }
}

/// A wrapper around `Value` that implements `Eq` and `Hash` for use as
/// HashMap/HashSet keys. F64 is compared by its bit representation.
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

// hex encoding helper (avoid pulling in the `hex` crate for just this).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
fn execute_copy_to(
    db_root: &Path,
    table: &str,
    path: &PathBuf,
    options: &CopyOptions,
) -> Result<QueryResult> {
    // Parquet format: use the PAR1XCHG writer.
    if options.format == CopyFormat::Parquet {
        return execute_copy_to_parquet(db_root, table, path);
    }

    let select_plan = QueryPlan::Select {
        table: table.to_string(),
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
    let result = execute(db_root, &select_plan)?;
    let (columns, rows) = match result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "COPY TO requires a table with rows".into(),
            ));
        }
    };
    let delim = options.delimiter;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(path)?;
    let mut wtr = std::io::BufWriter::new(file);
    if options.header {
        let hdr: Vec<&str> = columns.iter().map(|c| c.as_str()).collect();
        writeln!(wtr, "{}", hdr.join(&delim.to_string())).map_err(ExchangeDbError::Io)?;
    }
    let mut count = 0u64;
    for row in &rows {
        let fields: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => String::new(),
                Value::I64(n) => n.to_string(),
                Value::F64(n) => n.to_string(),
                Value::Str(s) => s.clone(),
                Value::Timestamp(ns) => ns.to_string(),
            })
            .collect();
        writeln!(wtr, "{}", fields.join(&delim.to_string())).map_err(ExchangeDbError::Io)?;
        count += 1;
    }
    wtr.flush().map_err(ExchangeDbError::Io)?;
    Ok(QueryResult::Ok {
        affected_rows: count,
    })
}
fn execute_copy_to_parquet(db_root: &Path, table: &str, path: &Path) -> Result<QueryResult> {
    use exchange_core::parquet::reader::RowValue;
    use exchange_core::parquet::writer::{ParquetColumn, ParquetType, ParquetWriter};

    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    let meta = TableMeta::load(&table_dir.join("_meta"))?;

    // Build schema.
    let schema: Vec<ParquetColumn> = meta
        .columns
        .iter()
        .map(|c| {
            let ct: ColumnType = c.col_type.into();
            ParquetColumn {
                name: c.name.clone(),
                parquet_type: ParquetType::from_column_type(ct),
                col_type: ct,
            }
        })
        .collect();

    // Read all data via SELECT *.
    let select_plan = QueryPlan::Select {
        table: table.to_string(),
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
    let result = execute(db_root, &select_plan)?;
    let (columns, rows) = match result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "COPY TO requires a table with rows".into(),
            ));
        }
    };

    // Convert Value rows to RowValue rows.
    let pq_rows: Vec<Vec<RowValue>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    Value::I64(n) => RowValue::I64(*n),
                    Value::F64(n) => RowValue::F64(*n),
                    Value::Str(s) => RowValue::Str(s.clone()),
                    Value::Timestamp(ns) => RowValue::Timestamp(*ns),
                    Value::Null => RowValue::Null,
                })
                .collect()
        })
        .collect();

    let writer = ParquetWriter::new(path, schema);
    let stats = writer.write_rows(&columns, &pq_rows)?;

    Ok(QueryResult::Ok {
        affected_rows: stats.rows_written,
    })
}
fn execute_copy_from(
    db_root: &Path,
    table: &str,
    path: &PathBuf,
    options: &CopyOptions,
) -> Result<QueryResult> {
    // Parquet format: use the PAR1XCHG reader.
    if options.format == CopyFormat::Parquet {
        return execute_copy_from_parquet(db_root, table, path);
    }

    let file = std::fs::File::open(path)?;
    let rdr = std::io::BufReader::new(file);
    let delim = options.delimiter;
    let mut lines_it = rdr.lines();
    let csv_cols: Vec<String> = if options.header {
        let header = lines_it
            .next()
            .ok_or_else(|| ExchangeDbError::Query("COPY FROM: empty file".into()))?
            .map_err(ExchangeDbError::Io)?;
        header.split(delim).map(|s| s.trim().to_string()).collect()
    } else {
        Vec::new()
    };
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        if csv_cols.is_empty() {
            return Err(ExchangeDbError::Query(
                "COPY FROM: table does not exist and CSV has no header".into(),
            ));
        }
        let mut builder = TableBuilder::new(table);
        builder = builder.column(&csv_cols[0], ColumnType::Timestamp);
        for col in &csv_cols[1..] {
            builder = builder.column(col, ColumnType::Varchar);
        }
        builder = builder.timestamp(&csv_cols[0]);
        builder.build(db_root)?;
    }
    let meta = TableMeta::load(&table_dir.join("_meta"))?;
    let mut values: Vec<Vec<Value>> = Vec::new();
    for lr in lines_it {
        let line = lr.map_err(ExchangeDbError::Io)?;
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(delim).collect();
        let mut row = Vec::with_capacity(meta.columns.len());
        for (i, cd) in meta.columns.iter().enumerate() {
            let f = if i < fields.len() {
                fields[i].trim()
            } else {
                ""
            };
            let ct: ColumnType = cd.col_type.into();
            row.push(parse_csv_fld(f, ct));
        }
        values.push(row);
    }
    execute(
        db_root,
        &QueryPlan::Insert {
            table: table.to_string(),
            columns: meta.columns.iter().map(|c| c.name.clone()).collect(),
            values,
            upsert: false,
        },
    )
}
fn execute_copy_from_parquet(db_root: &Path, table: &str, path: &Path) -> Result<QueryResult> {
    use exchange_core::parquet::reader::{ParquetReader, RowValue};

    let reader = ParquetReader::open(path)?;
    let pq_rows = reader.read_all()?;

    // Convert RowValue rows to Value rows for INSERT.
    let col_names: Vec<String> = reader
        .metadata()
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let values: Vec<Vec<Value>> = pq_rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    RowValue::I64(n) => Value::I64(*n),
                    RowValue::F64(n) => Value::F64(*n),
                    RowValue::Str(s) => Value::Str(s.clone()),
                    RowValue::Timestamp(ns) => Value::Timestamp(*ns),
                    RowValue::Bytes(_) => Value::Null,
                    RowValue::Null => Value::Null,
                })
                .collect()
        })
        .collect();

    // If the table does not exist, create it from parquet schema.
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        let mut builder = TableBuilder::new(table);
        for cm in &reader.metadata().columns {
            builder = builder.column(&cm.name, cm.col_type);
        }
        // Try to set the first timestamp column as the designated timestamp.
        if let Some(ts_col) = reader
            .metadata()
            .columns
            .iter()
            .find(|c| c.col_type == ColumnType::Timestamp)
        {
            builder = builder.timestamp(&ts_col.name);
        }
        builder.build(db_root)?;
    }

    execute(
        db_root,
        &QueryPlan::Insert {
            table: table.to_string(),
            columns: col_names,
            values,
            upsert: false,
        },
    )
}
fn parse_csv_fld(field: &str, col_type: ColumnType) -> Value {
    if field.is_empty() {
        return Value::Null;
    }
    match col_type {
        ColumnType::Timestamp => field
            .parse::<i64>()
            .map(Value::Timestamp)
            .unwrap_or(Value::Str(field.to_string())),
        ColumnType::I64 | ColumnType::I32 | ColumnType::I16 | ColumnType::I8 => {
            field.parse::<i64>().map(Value::I64).unwrap_or(Value::Null)
        }
        ColumnType::F64 | ColumnType::F32 => {
            field.parse::<f64>().map(Value::F64).unwrap_or(Value::Null)
        }
        ColumnType::Boolean => match field.to_ascii_lowercase().as_str() {
            "true" | "1" => Value::I64(1),
            _ => Value::I64(0),
        },
        _ => Value::Str(field.to_string()),
    }
}
fn execute_explain(_db_root: &Path, query: &QueryPlan) -> Result<QueryResult> {
    Ok(QueryResult::Rows {
        columns: vec!["plan".to_string()],
        rows: vec![vec![Value::Str(fmt_plan(query))]],
    })
}
fn execute_explain_analyze(db_root: &Path, query: &QueryPlan) -> Result<QueryResult> {
    use crate::profiler::{ProfilingStep, QueryProfiler};
    use std::time::Instant;

    let mut profiler = QueryProfiler::new();

    // Record optimization step.
    let opt_start = Instant::now();
    let plan_desc = fmt_plan(query);
    let opt_duration = opt_start.elapsed();
    profiler.add_step(ProfilingStep {
        name: "Plan".to_string(),
        duration: opt_duration,
        rows_in: 0,
        rows_out: 0,
        bytes_scanned: 0,
        details: plan_desc,
    });

    // Execute the actual query with timing.
    let exec_start = Instant::now();
    let result = execute(db_root, query)?;
    let exec_duration = exec_start.elapsed();

    let (rows_out, result_desc) = match &result {
        QueryResult::Rows { columns, rows } => (
            rows.len() as u64,
            format!("{} columns, {} rows", columns.len(), rows.len()),
        ),
        QueryResult::Ok { affected_rows } => {
            (*affected_rows, format!("{} rows affected", affected_rows))
        }
    };

    profiler.add_step(ProfilingStep {
        name: "Execute".to_string(),
        duration: exec_duration,
        rows_in: 0,
        rows_out,
        bytes_scanned: 0,
        details: result_desc,
    });

    let report = profiler.format_report();

    Ok(QueryResult::Rows {
        columns: vec!["plan".to_string()],
        rows: vec![vec![Value::Str(report)]],
    })
}
fn fmt_plan(plan: &QueryPlan) -> String {
    match plan {
        QueryPlan::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            sample_by,
            group_by,
            ..
        } => {
            let mut p = vec![format!("SELECT on table: {table}")];
            let cs: Vec<String> = columns
                .iter()
                .map(|c| match c {
                    SelectColumn::Wildcard => "*".into(),
                    SelectColumn::Name(n) => n.clone(),
                    SelectColumn::Aggregate {
                        function,
                        column,
                        alias,
                        ..
                    } => {
                        if let Some(a) = alias {
                            a.clone()
                        } else {
                            format!("{function:?}({column})")
                        }
                    }
                    SelectColumn::ScalarFunction { name, .. } => name.clone(),
                    SelectColumn::WindowFunction(wf) => wf.name.clone(),
                    SelectColumn::CaseWhen { alias, .. } => {
                        alias.clone().unwrap_or_else(|| "case".to_string())
                    }
                    SelectColumn::Expression { alias, .. } => {
                        alias.clone().unwrap_or_else(|| "expr".to_string())
                    }
                    SelectColumn::ScalarSubquery { alias, .. } => {
                        alias.clone().unwrap_or_else(|| "subquery".to_string())
                    }
                })
                .collect();
            p.push(format!("  columns: [{}]", cs.join(", ")));

            if let Some(f) = filter {
                p.push(format!("  filter: {f:?}"));
            }

            if !order_by.is_empty() {
                let ob: Vec<String> = order_by
                    .iter()
                    .map(|o| {
                        if o.descending {
                            format!("{} DESC", o.column)
                        } else {
                            format!("{} ASC", o.column)
                        }
                    })
                    .collect();
                p.push(format!("  order_by: [{}]", ob.join(", ")));
            }

            if let Some(l) = limit {
                p.push(format!("  limit: {l}"));
            }

            if let Some(sb) = sample_by {
                p.push(format!("  sample_by: {:?}", sb.interval));
            }

            if !group_by.is_empty() {
                p.push(format!("  group_by: [{}]", group_by.join(", ")));
            }
            p.join("\n")
        }
        QueryPlan::Insert { table, values, .. } => {
            format!("INSERT into table: {table}, rows: {}", values.len())
        }
        QueryPlan::CreateTable { name, columns, .. } => {
            format!("CREATE TABLE {name} with {} columns", columns.len())
        }
        QueryPlan::Join {
            left_table,
            right_table,
            join_type,
            ..
        } => format!("{join_type:?} JOIN {left_table} x {right_table}"),
        QueryPlan::MultiJoin {
            left,
            right_table,
            join_type,
            ..
        } => format!("{join_type:?} JOIN ({}) x {right_table}", fmt_plan(left)),
        QueryPlan::AsofJoin {
            left_table,
            right_table,
            ..
        } => format!("ASOF JOIN {left_table} x {right_table}"),
        QueryPlan::Delete { table, filter, .. } => {
            format!("DELETE from {table}, filter: {filter:?}")
        }
        QueryPlan::Update { table, filter, .. } => format!("UPDATE {table}, filter: {filter:?}"),
        QueryPlan::Vacuum { table } => format!("VACUUM {table}"),
        QueryPlan::DetachPartition { table, partition } => {
            format!("ALTER TABLE {table} DETACH PARTITION '{partition}'")
        }
        QueryPlan::AttachPartition { table, partition } => {
            format!("ALTER TABLE {table} ATTACH PARTITION '{partition}'")
        }
        QueryPlan::SquashPartitions {
            table,
            partition1,
            partition2,
        } => format!("ALTER TABLE {table} SQUASH PARTITIONS '{partition1}', '{partition2}'"),
        QueryPlan::CopyTo { table, path, .. } => format!("COPY {table} TO {}", path.display()),
        QueryPlan::CopyFrom { table, path, .. } => format!("COPY {table} FROM {}", path.display()),
        QueryPlan::ReadParquet { path, .. } => format!("READ_PARQUET({})", path.display()),
        QueryPlan::ReadCsv { path, .. } => format!("READ_CSV({})", path.display()),
        QueryPlan::CreateTableAs { name, source, .. } => {
            format!("CREATE TABLE {name} AS\n{}", fmt_plan(source))
        }
        QueryPlan::Explain { query } => format!("EXPLAIN\n{}", fmt_plan(query)),
        QueryPlan::ExplainAnalyze { query } => format!("EXPLAIN ANALYZE\n{}", fmt_plan(query)),
        other => format!("{other:?}"),
    }
}
fn execute_vacuum(db_root: &Path, table: &str) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    let meta = TableMeta::load(&table_dir.join("_meta"))?;
    let job = exchange_core::vacuum::VacuumJob::new(table_dir, meta);
    let stats = job.run()?;
    Ok(QueryResult::Rows {
        columns: vec![
            "wal_segments_removed".into(),
            "empty_partitions_removed".into(),
            "orphan_files_removed".into(),
            "bytes_freed".into(),
        ],
        rows: vec![vec![
            Value::I64(stats.wal_segments_removed as i64),
            Value::I64(stats.empty_partitions_removed as i64),
            Value::I64(stats.orphan_files_removed as i64),
            Value::I64(stats.bytes_freed as i64),
        ]],
    })
}

fn execute_detach_partition(db_root: &Path, table: &str, partition: &str) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    exchange_core::partition_mgmt::detach_partition(&table_dir, partition)?;
    Ok(QueryResult::Ok { affected_rows: 1 })
}

fn execute_attach_partition(db_root: &Path, table: &str, partition: &str) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    exchange_core::partition_mgmt::attach_partition(&table_dir, partition)?;
    Ok(QueryResult::Ok { affected_rows: 1 })
}

fn execute_squash_partitions(
    db_root: &Path,
    table: &str,
    p1: &str,
    p2: &str,
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    let meta = TableMeta::load(&table_dir.join("_meta"))?;
    let merged = exchange_core::partition_mgmt::squash_partitions(&table_dir, p1, p2, &meta)?;
    Ok(QueryResult::Rows {
        columns: vec!["merged_partition".into()],
        rows: vec![vec![Value::Str(merged)]],
    })
}
// execute_pivot, execute_merge, execute_insert_on_conflict: see full implementations below.

/// Execute CREATE MATERIALIZED VIEW: parse and run the defining query,
/// store the results as a regular table, and save the matview metadata.
fn execute_create_mat_view(db_root: &Path, name: &str, source_sql: &str) -> Result<QueryResult> {
    use exchange_core::matview::MatViewMeta;
    use exchange_core::table::{ColumnDef, ColumnTypeSerializable};

    let mv_table_dir = db_root.join(name);
    if mv_table_dir.exists() {
        return Err(ExchangeDbError::TableAlreadyExists(name.to_string()));
    }

    // Parse and execute the defining query to get the result set.
    let plan = crate::planner::plan_query(source_sql)?;
    let result = execute(db_root, &plan)?;

    let (col_names, rows) = match result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "materialized view query must return rows".into(),
            ));
        }
    };

    // Extract the source table name from the SQL (simple heuristic: first FROM <table>).
    let source_table = extract_source_table(source_sql).unwrap_or_default();

    // Infer column types from the result data.
    let col_defs = infer_column_defs(&col_names, &rows);

    // Build the backing table.
    let mut builder = exchange_core::table::TableBuilder::new(name);

    // Add user columns first.
    for def in &col_defs {
        let ct: ColumnType = def.col_type.into();
        builder = builder.column(&def.name, ct);
    }

    // Add a timestamp column at the end (internal, for partitioning).
    builder = builder.column("_mv_timestamp", ColumnType::Timestamp);

    builder = builder
        .timestamp("_mv_timestamp")
        .partition_by(PartitionBy::None);

    builder.build(db_root)?;

    // Write the result rows.
    write_matview_rows(db_root, name, &col_defs, &rows)?;

    // Save matview metadata.
    let mut all_defs = col_defs.clone();
    all_defs.push(ColumnDef {
        name: "_mv_timestamp".to_string(),
        col_type: ColumnTypeSerializable::Timestamp,
        indexed: false,
    });

    let mv_meta = MatViewMeta {
        name: name.to_string(),
        source_table,
        query_sql: source_sql.to_string(),
        columns: all_defs,
        last_refresh: Some(Timestamp::now().as_nanos()),
        auto_refresh: false,
        version: 1,
    };
    mv_meta.save(&mv_table_dir.join("_matview"))?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Execute REFRESH MATERIALIZED VIEW: reload metadata, re-execute the
/// defining query, and rewrite the backing table data.
fn execute_refresh_mat_view(db_root: &Path, name: &str) -> Result<QueryResult> {
    use exchange_core::matview::MatViewMeta;

    let mv_table_dir = db_root.join(name);
    if !mv_table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(name.to_string()));
    }

    let matview_path = mv_table_dir.join("_matview");
    if !matview_path.exists() {
        return Err(ExchangeDbError::Query(format!(
            "'{name}' is not a materialized view"
        )));
    }

    let mut mv_meta = MatViewMeta::load(&matview_path)?;
    let source_sql = mv_meta.query_sql.clone();

    // Re-execute the defining query.
    let plan = crate::planner::plan_query(&source_sql)?;
    let result = execute(db_root, &plan)?;

    let (col_names, rows) = match result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "materialized view query must return rows".into(),
            ));
        }
    };

    // Delete all existing partitions.
    let partitions = exchange_core::table::list_partitions(&mv_table_dir)?;
    for p in &partitions {
        if p.exists() {
            std::fs::remove_dir_all(p)?;
        }
    }

    // Re-infer column defs from the new results.
    let col_defs = infer_column_defs(&col_names, &rows);

    // Write the new rows.
    write_matview_rows(db_root, name, &col_defs, &rows)?;

    // Update metadata.
    mv_meta.last_refresh = Some(Timestamp::now().as_nanos());
    mv_meta.version += 1;
    mv_meta.save(&matview_path)?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Execute DROP MATERIALIZED VIEW: delete the backing table directory and metadata.
fn execute_drop_mat_view(db_root: &Path, name: &str) -> Result<QueryResult> {
    let mv_table_dir = db_root.join(name);
    if !mv_table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(name.to_string()));
    }

    let matview_path = mv_table_dir.join("_matview");
    if !matview_path.exists() {
        return Err(ExchangeDbError::Query(format!(
            "'{name}' is not a materialized view"
        )));
    }

    std::fs::remove_dir_all(&mv_table_dir)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Infer column definitions from query result data.
fn infer_column_defs(
    col_names: &[String],
    rows: &[Vec<Value>],
) -> Vec<exchange_core::table::ColumnDef> {
    use exchange_core::table::{ColumnDef, ColumnTypeSerializable};

    col_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            // Look at the first non-null value to determine the type.
            let ct = rows
                .iter()
                .find_map(|row| {
                    if i < row.len() {
                        match &row[i] {
                            Value::I64(_) => Some(ColumnTypeSerializable::I64),
                            Value::F64(_) => Some(ColumnTypeSerializable::F64),
                            Value::Str(_) => Some(ColumnTypeSerializable::Varchar),
                            Value::Timestamp(_) => Some(ColumnTypeSerializable::Timestamp),
                            Value::Null => None,
                        }
                    } else {
                        None
                    }
                })
                .unwrap_or(ColumnTypeSerializable::F64);

            ColumnDef {
                name: name.clone(),
                col_type: ct,
                indexed: false,
            }
        })
        .collect()
}

/// Write materialized view result rows into the backing table.
fn write_matview_rows(
    db_root: &Path,
    table_name: &str,
    _col_defs: &[exchange_core::table::ColumnDef],
    rows: &[Vec<Value>],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut writer = TableWriter::open(db_root, table_name)?;
    let ts = Timestamp::now();

    for (row_idx, row) in rows.iter().enumerate() {
        // Use a synthetic timestamp offset by row index to ensure unique timestamps.
        let row_ts = Timestamp(ts.as_nanos() + row_idx as i64);

        let col_values: Vec<ColumnValue<'_>> =
            row.iter().map(|v| plan_value_to_column_value(v)).collect();

        writer.write_row(row_ts, &col_values)?;
    }

    writer.flush()?;
    Ok(())
}

/// Extract the source table name from a SQL query (simple heuristic).
fn extract_source_table(sql: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let from_pos = upper.find("FROM ")?;
    let rest = sql[from_pos + 5..].trim_start();
    let end = rest
        .find(|c: char| c.is_whitespace() || c == ';' || c == ')')
        .unwrap_or(rest.len());
    let table = rest[..end].trim().to_string();
    if table.is_empty() { None } else { Some(table) }
}

// ── RBAC execution ──────────────────────────────────────────────────

fn execute_create_user(db_root: &Path, username: &str, password: &str) -> Result<QueryResult> {
    use exchange_core::rbac::{RbacStore, User, hash_password};

    let store = RbacStore::open(db_root)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let user = User {
        username: username.to_string(),
        password_hash: hash_password(password),
        roles: vec![],
        enabled: true,
        created_at: now,
    };
    store.create_user(&user)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_user(db_root: &Path, username: &str) -> Result<QueryResult> {
    use exchange_core::rbac::RbacStore;

    let store = RbacStore::open(db_root)?;
    store.delete_user(username)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_create_role(db_root: &Path, name: &str) -> Result<QueryResult> {
    use exchange_core::rbac::{RbacStore, Role};

    let store = RbacStore::open(db_root)?;
    let role = Role {
        name: name.to_string(),
        permissions: vec![],
    };
    store.create_role(&role)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_role(db_root: &Path, name: &str) -> Result<QueryResult> {
    use exchange_core::rbac::RbacStore;

    let store = RbacStore::open(db_root)?;
    store.delete_role(name)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_grant(
    db_root: &Path,
    permission: &GrantPermission,
    target: &str,
) -> Result<QueryResult> {
    use exchange_core::rbac::RbacStore;

    let store = RbacStore::open(db_root)?;

    // If it's a role assignment (GRANT <role> TO <user>), add the role to the user.
    if let GrantPermission::Role { role_name } = permission {
        // Verify the role exists.
        if store.get_role(role_name)?.is_none() {
            return Err(ExchangeDbError::Query(format!(
                "role '{role_name}' not found"
            )));
        }
        let mut user = store
            .get_user(target)?
            .ok_or_else(|| ExchangeDbError::Query(format!("user '{target}' not found")))?;
        if !user.roles.contains(role_name) {
            user.roles.push(role_name.clone());
            store.update_user(&user)?;
        }
        return Ok(QueryResult::Ok { affected_rows: 0 });
    }

    // Otherwise it's a permission grant on a role.
    let perms = grant_std_permission_to_permissions(permission);
    let mut role = store
        .get_role(target)?
        .ok_or_else(|| ExchangeDbError::Query(format!("role '{target}' not found")))?;
    for perm in perms {
        if !role.permissions.contains(&perm) {
            role.permissions.push(perm);
        }
    }
    store.update_role(&role)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_revoke(
    db_root: &Path,
    permission: &GrantPermission,
    target: &str,
) -> Result<QueryResult> {
    use exchange_core::rbac::RbacStore;

    let store = RbacStore::open(db_root)?;

    // If it's a role revocation (REVOKE <role> FROM <user>), remove the role.
    if let GrantPermission::Role { role_name } = permission {
        let mut user = store
            .get_user(target)?
            .ok_or_else(|| ExchangeDbError::Query(format!("user '{target}' not found")))?;
        user.roles.retain(|r| r != role_name);
        store.update_user(&user)?;
        return Ok(QueryResult::Ok { affected_rows: 0 });
    }

    // Otherwise revoke a permission from a role.
    let perms = grant_std_permission_to_permissions(permission);
    let mut role = store
        .get_role(target)?
        .ok_or_else(|| ExchangeDbError::Query(format!("role '{target}' not found")))?;
    for perm in &perms {
        role.permissions.retain(|p| p != perm);
    }
    store.update_role(&role)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Convert a `GrantPermission` (plan-level) to a `Permission` (core-level).
fn grant_permission_to_permission(gp: &GrantPermission) -> exchange_core::rbac::Permission {
    use exchange_core::rbac::Permission;
    match gp {
        GrantPermission::Read { table } => Permission::Read {
            table: table.clone(),
        },
        GrantPermission::Write { table } => Permission::Write {
            table: table.clone(),
        },
        GrantPermission::DDL => Permission::DDL,
        GrantPermission::Admin => Permission::Admin,
        GrantPermission::System => Permission::System,
        GrantPermission::ColumnRead { table, columns } => Permission::ColumnRead {
            table: table.clone(),
            columns: columns.clone(),
        },
        GrantPermission::Role { .. } => {
            unreachable!("Role grants are handled separately")
        }
        GrantPermission::Select { table } => Permission::Read {
            table: Some(table.clone()),
        },
        GrantPermission::Insert { table }
        | GrantPermission::Update { table }
        | GrantPermission::Delete { table } => Permission::Write {
            table: Some(table.clone()),
        },
        GrantPermission::All { table } => Permission::Write {
            table: Some(table.clone()),
        },
    }
}

/// Execute a PIVOT query: rotate rows into columns.
///
/// 1. Execute the source query to get all rows.
/// 2. Group by all non-pivot, non-aggregate columns.
/// 3. For each group, compute the aggregate for each pivot value.
/// 4. Output one row per group with pivot values as separate columns.
fn execute_pivot(
    db_root: &Path,
    source: &QueryPlan,
    aggregate: AggregateKind,
    agg_column: &str,
    pivot_col: &str,
    values: &[PivotValue],
) -> Result<QueryResult> {
    let source_result = execute(db_root, source)?;
    let (src_cols, src_rows) = match source_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "PIVOT source did not return rows".into(),
            ));
        }
    };

    let pivot_col_idx = src_cols
        .iter()
        .position(|c| c == pivot_col)
        .ok_or_else(|| ExchangeDbError::Query(format!("PIVOT column '{pivot_col}' not found")))?;
    let agg_col_idx = src_cols
        .iter()
        .position(|c| c == agg_column)
        .ok_or_else(|| {
            ExchangeDbError::Query(format!("aggregate column '{agg_column}' not found"))
        })?;

    // Group-by columns: all columns except pivot_col and agg_column.
    let group_col_idxs: Vec<usize> = (0..src_cols.len())
        .filter(|i| *i != pivot_col_idx && *i != agg_col_idx)
        .collect();

    // Build result columns: group columns + one per pivot value.
    let mut result_cols: Vec<String> = group_col_idxs
        .iter()
        .map(|i| src_cols[*i].clone())
        .collect();
    for pv in values {
        result_cols.push(pv.alias.clone());
    }

    // Group rows.
    let mut groups: HashMap<Vec<String>, Vec<Vec<Value>>> = HashMap::new();
    for row in &src_rows {
        let key: Vec<String> = group_col_idxs.iter().map(|i| row[*i].to_string()).collect();
        groups.entry(key).or_default().push(row.clone());
    }

    let mut result_rows = Vec::new();
    for (key_strs, rows) in &groups {
        let mut result_row: Vec<Value> = group_col_idxs
            .iter()
            .enumerate()
            .map(|(ki, gi)| {
                // Re-extract the original value from the first row.
                if let Some(first) = rows.first() {
                    first[*gi].clone()
                } else {
                    Value::Str(key_strs[ki].clone())
                }
            })
            .collect();

        for pv in values {
            // Filter rows matching this pivot value.
            let pv_str = match &pv.value {
                Value::Str(s) => s.clone(),
                other => other.to_string().trim_matches('\'').to_string(),
            };
            let matching: Vec<&Value> = rows
                .iter()
                .filter(|r| match &r[pivot_col_idx] {
                    Value::Str(s) => *s == pv_str,
                    other => other.to_string().trim_matches('\'') == pv_str.as_str(),
                })
                .map(|r| &r[agg_col_idx])
                .collect();

            let agg_val = compute_simple_aggregate(aggregate, &matching);
            result_row.push(agg_val);
        }
        result_rows.push(result_row);
    }

    Ok(QueryResult::Rows {
        columns: result_cols,
        rows: result_rows,
    })
}

/// Compute a simple aggregate over a list of values.
fn compute_simple_aggregate(kind: AggregateKind, values: &[&Value]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    match kind {
        AggregateKind::Avg
        | AggregateKind::AvgDouble
        | AggregateKind::AvgFloat
        | AggregateKind::AvgInt
        | AggregateKind::AvgLong => {
            let mut sum = 0.0f64;
            let mut count = 0usize;
            for v in values {
                match v {
                    Value::F64(f) => {
                        sum += f;
                        count += 1;
                    }
                    Value::I64(i) => {
                        sum += *i as f64;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Value::F64(sum / count as f64)
            } else {
                Value::Null
            }
        }
        AggregateKind::Sum | AggregateKind::SumDouble | AggregateKind::SumFloat => {
            let mut sum = 0.0f64;
            for v in values {
                match v {
                    Value::F64(f) => sum += f,
                    Value::I64(i) => sum += *i as f64,
                    _ => {}
                }
            }
            Value::F64(sum)
        }
        AggregateKind::Min | AggregateKind::MinDouble | AggregateKind::MinFloat => values
            .iter()
            .filter(|v| !matches!(v, Value::Null))
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| (*v).clone())
            .unwrap_or(Value::Null),
        AggregateKind::Max | AggregateKind::MaxDouble | AggregateKind::MaxFloat => values
            .iter()
            .filter(|v| !matches!(v, Value::Null))
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|v| (*v).clone())
            .unwrap_or(Value::Null),
        AggregateKind::Count => Value::I64(values.len() as i64),
        AggregateKind::First => values.first().map(|v| (*v).clone()).unwrap_or(Value::Null),
        AggregateKind::Last => values.last().map(|v| (*v).clone()).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Execute a MERGE statement.
fn execute_merge(
    db_root: &Path,
    target_table: &str,
    source_table: &str,
    on_column: &(String, String),
    when_clauses: &[MergeWhen],
) -> Result<QueryResult> {
    // Read target table.
    let target_result = execute(
        db_root,
        &QueryPlan::Select {
            table: target_table.to_string(),
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
        },
    )?;
    let (target_cols, target_rows) = match target_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "MERGE target did not return rows".into(),
            ));
        }
    };

    // Read source table.
    let source_result = execute(
        db_root,
        &QueryPlan::Select {
            table: source_table.to_string(),
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
        },
    )?;
    let (source_cols, source_rows) = match source_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "MERGE source did not return rows".into(),
            ));
        }
    };

    let target_key_idx = target_cols
        .iter()
        .position(|c| c == &on_column.0)
        .ok_or_else(|| {
            ExchangeDbError::Query(format!("column '{}' not found in target", on_column.0))
        })?;
    let source_key_idx = source_cols
        .iter()
        .position(|c| c == &on_column.1)
        .ok_or_else(|| {
            ExchangeDbError::Query(format!("column '{}' not found in source", on_column.1))
        })?;

    // Build a set of target keys.
    let target_keys: HashSet<String> = target_rows
        .iter()
        .map(|r| r[target_key_idx].to_string())
        .collect();

    let mut affected = 0u64;

    for source_row in &source_rows {
        let key = source_row[source_key_idx].to_string();
        if target_keys.contains(&key) {
            // MATCHED.
            for clause in when_clauses {
                match clause {
                    MergeWhen::MatchedUpdate { assignments } => {
                        let filter =
                            Filter::Eq(on_column.0.clone(), source_row[source_key_idx].clone());
                        let plan_assignments: Vec<(String, PlanExpr)> = assignments
                            .iter()
                            .map(|(col, expr)| {
                                let resolved = match expr {
                                    PlanExpr::Column(name) => {
                                        if let Some(idx) =
                                            source_cols.iter().position(|c| c == name)
                                        {
                                            PlanExpr::Literal(source_row[idx].clone())
                                        } else {
                                            expr.clone()
                                        }
                                    }
                                    other => other.clone(),
                                };
                                (col.clone(), resolved)
                            })
                            .collect();
                        execute(
                            db_root,
                            &QueryPlan::Update {
                                table: target_table.to_string(),
                                assignments: plan_assignments,
                                filter: Some(filter),
                            },
                        )?;
                        affected += 1;
                    }
                    MergeWhen::MatchedDelete => {
                        let filter =
                            Filter::Eq(on_column.0.clone(), source_row[source_key_idx].clone());
                        execute(
                            db_root,
                            &QueryPlan::Delete {
                                table: target_table.to_string(),
                                filter: Some(filter),
                            },
                        )?;
                        affected += 1;
                    }
                    MergeWhen::NotMatchedInsert { .. } => {}
                }
            }
        } else {
            // NOT MATCHED.
            for clause in when_clauses {
                if let MergeWhen::NotMatchedInsert { values } = clause {
                    let row_values: Vec<Value> = values
                        .iter()
                        .map(|expr| match expr {
                            PlanExpr::Column(name) => {
                                if let Some(idx) = source_cols.iter().position(|c| c == name) {
                                    source_row[idx].clone()
                                } else {
                                    Value::Null
                                }
                            }
                            PlanExpr::Literal(v) => v.clone(),
                            _ => Value::Null,
                        })
                        .collect();
                    execute(
                        db_root,
                        &QueryPlan::Insert {
                            table: target_table.to_string(),
                            columns: vec![],
                            values: vec![row_values],
                            upsert: false,
                        },
                    )?;
                    affected += 1;
                }
            }
        }
    }

    Ok(QueryResult::Ok {
        affected_rows: affected,
    })
}

/// Execute INSERT ... ON CONFLICT.
fn execute_insert_on_conflict(
    db_root: &Path,
    table: &str,
    columns: &[String],
    values: &[Vec<Value>],
    on_conflict: &OnConflictClause,
) -> Result<QueryResult> {
    let reordered = reorder_insert_values(db_root, table, columns, values)?;
    let meta = TableMeta::load(&db_root.join(table).join("_meta"))?;
    let mut affected = 0u64;

    for row in &reordered {
        // Check if a conflicting row exists.
        let has_conflict = if !on_conflict.columns.is_empty() {
            let col_name = &on_conflict.columns[0];
            if let Some(col_idx) = meta.columns.iter().position(|c| c.name == *col_name) {
                if let Some(val) = row.get(col_idx) {
                    let check_result = execute(
                        db_root,
                        &QueryPlan::Select {
                            table: table.to_string(),
                            columns: vec![SelectColumn::Wildcard],
                            filter: Some(Filter::Eq(col_name.clone(), val.clone())),
                            order_by: vec![],
                            limit: Some(1),
                            offset: None,
                            sample_by: None,
                            latest_on: None,
                            group_by: vec![],
                            group_by_mode: GroupByMode::Normal,
                            having: None,
                            distinct: false,
                            distinct_on: vec![],
                        },
                    )?;
                    match check_result {
                        QueryResult::Rows { rows, .. } => !rows.is_empty(),
                        _ => false,
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if has_conflict {
            match &on_conflict.action {
                OnConflictAction::DoNothing => {
                    // Skip.
                }
                OnConflictAction::DoUpdate { assignments } => {
                    let col_name = &on_conflict.columns[0];
                    let col_idx = meta
                        .columns
                        .iter()
                        .position(|c| c.name == *col_name)
                        .unwrap();
                    let filter = Filter::Eq(col_name.clone(), row[col_idx].clone());

                    let resolved_assignments: Vec<(String, PlanExpr)> = assignments
                        .iter()
                        .map(|(col, expr)| {
                            let resolved = resolve_excluded_refs(expr, &meta, row);
                            (col.clone(), resolved)
                        })
                        .collect();

                    execute(
                        db_root,
                        &QueryPlan::Update {
                            table: table.to_string(),
                            assignments: resolved_assignments,
                            filter: Some(filter),
                        },
                    )?;
                    affected += 1;
                }
            }
        } else {
            execute_insert(db_root, table, std::slice::from_ref(row), false, false)?;
            affected += 1;
        }
    }

    Ok(QueryResult::Ok {
        affected_rows: affected,
    })
}

/// Replace EXCLUDED.col references with literal values from the insert row.
fn resolve_excluded_refs(expr: &PlanExpr, meta: &TableMeta, row: &[Value]) -> PlanExpr {
    match expr {
        PlanExpr::Column(name) => {
            if let Some(idx) = meta.columns.iter().position(|c| c.name == *name)
                && let Some(val) = row.get(idx)
            {
                return PlanExpr::Literal(val.clone());
            }
            expr.clone()
        }
        PlanExpr::BinaryOp { left, op, right } => PlanExpr::BinaryOp {
            left: Box::new(resolve_excluded_refs(left, meta, row)),
            op: *op,
            right: Box::new(resolve_excluded_refs(right, meta, row)),
        },
        PlanExpr::UnaryOp { op, expr: inner } => PlanExpr::UnaryOp {
            op: *op,
            expr: Box::new(resolve_excluded_refs(inner, meta, row)),
        },
        PlanExpr::Function { name, args } => PlanExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_excluded_refs(a, meta, row))
                .collect(),
        },
        PlanExpr::Literal(_) => expr.clone(),
    }
}

/// Execute a LATERAL JOIN: for each row in the left table, execute the
/// subquery with the left row's values available as correlated references,
/// then combine all results.
#[allow(dead_code)]
fn execute_lateral_join(
    db_root: &Path,
    left_table: &str,
    subquery: &QueryPlan,
    subquery_alias: &str,
    order_by: &[OrderBy],
    limit: Option<u64>,
) -> Result<QueryResult> {
    // Read all rows from the left table.
    let left_result = execute(
        db_root,
        &QueryPlan::Select {
            table: left_table.to_string(),
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
        },
    )?;

    let (left_cols, left_rows) = match left_result {
        QueryResult::Rows { columns, rows } => (columns, rows),
        _ => {
            return Err(ExchangeDbError::Query(
                "LATERAL: left table returned no rows".into(),
            ));
        }
    };

    let mut result_cols: Option<Vec<String>> = None;
    let mut all_rows = Vec::new();

    for left_row in &left_rows {
        // Execute the subquery for this left row.
        // The subquery typically has a WHERE that references the left table's columns.
        // We substitute column references in the subquery's filter by binding values.
        let sub_result = execute_lateral_subquery(db_root, subquery, &left_cols, left_row)?;

        let (sub_cols, sub_rows) = match sub_result {
            QueryResult::Rows { columns, rows } => (columns, rows),
            _ => continue,
        };

        if result_cols.is_none() {
            let mut combined_cols = left_cols.clone();
            for sc in &sub_cols {
                combined_cols.push(format!("{}.{}", subquery_alias, sc));
            }
            result_cols = Some(combined_cols);
        }

        if sub_rows.is_empty() {
            // LEFT lateral semantics: emit left row with NULLs for subquery cols.
            let mut combined = left_row.clone();
            for _ in &sub_cols {
                combined.push(Value::Null);
            }
            all_rows.push(combined);
        } else {
            for sub_row in &sub_rows {
                let mut combined = left_row.clone();
                combined.extend(sub_row.iter().cloned());
                all_rows.push(combined);
            }
        }
    }

    let final_cols = result_cols.unwrap_or_else(|| left_cols.clone());

    // Apply ORDER BY.
    if !order_by.is_empty() {
        let resolved: Vec<(usize, String)> = final_cols
            .iter()
            .enumerate()
            .map(|(i, n)| (i, n.clone()))
            .collect();
        apply_order_by(&mut all_rows, &resolved, order_by);
    }

    // Apply LIMIT.
    if let Some(lim) = limit {
        all_rows.truncate(lim as usize);
    }

    Ok(QueryResult::Rows {
        columns: final_cols,
        rows: all_rows,
    })
}

/// Execute a lateral subquery by substituting correlated column references.
#[allow(dead_code)]
fn execute_lateral_subquery(
    db_root: &Path,
    subquery: &QueryPlan,
    left_cols: &[String],
    left_row: &[Value],
) -> Result<QueryResult> {
    // For the lateral subquery, we substitute filter column references
    // that match left table columns with the current left row values.
    match subquery {
        QueryPlan::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            offset,
            sample_by,
            latest_on,
            group_by,
            group_by_mode,
            having,
            distinct,
            distinct_on,
        } => {
            let resolved_filter = filter
                .as_ref()
                .map(|f| substitute_lateral_filter(f, left_cols, left_row));
            execute(
                db_root,
                &QueryPlan::Select {
                    table: table.clone(),
                    columns: columns.clone(),
                    filter: resolved_filter,
                    order_by: order_by.clone(),
                    limit: *limit,
                    offset: *offset,
                    sample_by: sample_by.clone(),
                    latest_on: latest_on.clone(),
                    group_by: group_by.clone(),
                    group_by_mode: group_by_mode.clone(),
                    having: having.clone(),
                    distinct: *distinct,
                    distinct_on: distinct_on.clone(),
                },
            )
        }
        other => execute(db_root, other),
    }
}

/// Substitute column references in a filter with values from the left row.
#[allow(dead_code)]
fn substitute_lateral_filter(filter: &Filter, left_cols: &[String], left_row: &[Value]) -> Filter {
    match filter {
        Filter::Eq(col, val) => {
            if let Some(new_val) = resolve_lateral_value(val, left_cols, left_row) {
                Filter::Eq(col.clone(), new_val)
            } else {
                filter.clone()
            }
        }
        Filter::And(parts) => Filter::And(
            parts
                .iter()
                .map(|p| substitute_lateral_filter(p, left_cols, left_row))
                .collect(),
        ),
        Filter::Or(parts) => Filter::Or(
            parts
                .iter()
                .map(|p| substitute_lateral_filter(p, left_cols, left_row))
                .collect(),
        ),
        Filter::Expression { left, op, right } => Filter::Expression {
            left: substitute_lateral_expr(left, left_cols, left_row),
            op: *op,
            right: substitute_lateral_expr(right, left_cols, left_row),
        },
        other => other.clone(),
    }
}

#[allow(dead_code)]
fn substitute_lateral_expr(expr: &PlanExpr, left_cols: &[String], left_row: &[Value]) -> PlanExpr {
    match expr {
        PlanExpr::Column(name) => {
            // Check if this column is from the left table (qualified or unqualified).
            let bare = name.split('.').next_back().unwrap_or(name);
            if let Some(idx) = left_cols.iter().position(|c| c == bare || c == name) {
                PlanExpr::Literal(left_row[idx].clone())
            } else {
                expr.clone()
            }
        }
        PlanExpr::BinaryOp { left, op, right } => PlanExpr::BinaryOp {
            left: Box::new(substitute_lateral_expr(left, left_cols, left_row)),
            op: *op,
            right: Box::new(substitute_lateral_expr(right, left_cols, left_row)),
        },
        other => other.clone(),
    }
}

#[allow(dead_code)]
fn resolve_lateral_value(
    _val: &Value,
    _left_cols: &[String],
    _left_row: &[Value],
) -> Option<Value> {
    // Values are already resolved literals; no substitution needed.
    // Column references in LATERAL WHERE clauses become Expression filters.
    None
}

fn execute_create_index(
    db_root: &Path,
    name: &str,
    table: &str,
    columns: &[String],
) -> Result<QueryResult> {
    let table_dir = db_root.join(table);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table.to_string()));
    }
    let meta = TableMeta::load(&table_dir.join("_meta"))?;
    for col in columns {
        if !meta.columns.iter().any(|c| c.name == *col) {
            return Err(ExchangeDbError::Query(format!(
                "column '{}' not found in table '{}'",
                col, table
            )));
        }
    }
    let index_dir = db_root.join("_indexes");
    let _ = std::fs::create_dir_all(&index_dir);
    let index_meta = serde_json::json!({ "name": name, "table": table, "columns": columns });
    let meta_path = index_dir.join(format!("{}.json", name));
    std::fs::write(
        &meta_path,
        serde_json::to_string_pretty(&index_meta).unwrap(),
    )
    .map_err(ExchangeDbError::Io)?;
    for col_name in columns {
        if meta.columns.iter().any(|c| c.name == *col_name) {
            let _ = exchange_core::index_builder::rebuild_index(&table_dir, col_name, &meta);
        }
    }
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_index(db_root: &Path, name: &str) -> Result<QueryResult> {
    let index_dir = db_root.join("_indexes");
    let meta_path = index_dir.join(format!("{}.json", name));
    if meta_path.exists() {
        let _ = std::fs::remove_file(&meta_path);
    }
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_rename_table(db_root: &Path, old_name: &str, new_name: &str) -> Result<QueryResult> {
    let old_dir = db_root.join(old_name);
    if !old_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(old_name.to_string()));
    }
    let new_dir = db_root.join(new_name);
    if new_dir.exists() {
        return Err(ExchangeDbError::Query(format!(
            "table '{}' already exists",
            new_name
        )));
    }
    std::fs::rename(&old_dir, &new_dir).map_err(ExchangeDbError::Io)?;
    let meta_path = new_dir.join("_meta");
    if meta_path.exists()
        && let Ok(mut meta) = TableMeta::load(&meta_path)
    {
        meta.name = new_name.to_string();
        let _ = meta.save(&meta_path);
    }
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn validate_check_constraints(db_root: &Path, table: &str, values: &[Vec<Value>]) -> Result<()> {
    let constraints_path = db_root.join(table).join("_checks.json");
    if !constraints_path.exists() {
        return Ok(());
    }
    let data = std::fs::read_to_string(&constraints_path).map_err(ExchangeDbError::Io)?;
    let constraints: Vec<CheckConstraint> = serde_json::from_str(&data)
        .map_err(|e| ExchangeDbError::Query(format!("invalid check constraints: {e}")))?;
    if constraints.is_empty() {
        return Ok(());
    }
    let meta_path = db_root.join(table).join("_meta");
    let meta = TableMeta::load(&meta_path)?;
    let col_names: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();

    // Parse each constraint expression once as a WHERE filter.
    let mut parsed_filters: Vec<(String, Option<Filter>)> = Vec::new();
    for constraint in &constraints {
        let filter = crate::planner::plan_query(&format!(
            "SELECT * FROM __dummy__ WHERE {}",
            constraint.expr_sql
        ))
        .ok()
        .and_then(|plan| {
            if let QueryPlan::Select { filter, .. } = plan {
                filter
            } else {
                None
            }
        });
        parsed_filters.push((constraint.column.clone(), filter));
    }

    for (row_idx, row) in values.iter().enumerate() {
        for (col_name, filter) in &parsed_filters {
            if let Some(f) = filter
                && !evaluate_filter_virtual(f, row, &col_names)
            {
                return Err(ExchangeDbError::Query(format!(
                    "CHECK constraint on column '{}' violated by row {}",
                    col_name, row_idx
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Stored Procedures
// ---------------------------------------------------------------------------

/// Stored procedure storage: simple file-based approach.
/// Each procedure is stored as a file in `db_root/__procedures__/<name>.sql`.
fn procedures_dir(db_root: &Path) -> PathBuf {
    db_root.join("__procedures__")
}

fn execute_create_procedure(db_root: &Path, name: &str, body: &str) -> Result<QueryResult> {
    let dir = procedures_dir(db_root);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{name}.sql"));
    std::fs::write(&path, body)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_procedure(db_root: &Path, name: &str) -> Result<QueryResult> {
    let path = procedures_dir(db_root).join(format!("{name}.sql"));
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_call_procedure(db_root: &Path, name: &str) -> Result<QueryResult> {
    let path = procedures_dir(db_root).join(format!("{name}.sql"));
    if !path.exists() {
        return Err(ExchangeDbError::Query(format!(
            "procedure '{name}' not found"
        )));
    }
    let body = std::fs::read_to_string(&path)?;
    // Execute each statement in the body sequentially.
    let mut last_result = QueryResult::Ok { affected_rows: 0 };
    for stmt_str in body.split(';') {
        let trimmed = stmt_str.trim();
        if trimmed.is_empty() {
            continue;
        }
        let plan = crate::planner::plan_query(trimmed)?;
        last_result = execute(db_root, &plan)?;
    }
    Ok(last_result)
}

// ---------------------------------------------------------------------------
// CREATE DOWNSAMPLING integration
// ---------------------------------------------------------------------------

fn execute_create_downsampling(
    db_root: &Path,
    source_table: &str,
    target_name: &str,
    interval_secs: u64,
    columns: &[(String, String, String)],
) -> Result<QueryResult> {
    use exchange_core::downsampling::*;

    let ds_columns: Vec<DownsampleColumn> = columns
        .iter()
        .map(|(func, src, alias)| match func.as_str() {
            "first" => DownsampleColumn::First {
                source: src.clone(),
                alias: alias.clone(),
            },
            "last" => DownsampleColumn::Last {
                source: src.clone(),
                alias: alias.clone(),
            },
            "min" => DownsampleColumn::Min {
                source: src.clone(),
                alias: alias.clone(),
            },
            "max" => DownsampleColumn::Max {
                source: src.clone(),
                alias: alias.clone(),
            },
            "sum" => DownsampleColumn::Sum {
                source: src.clone(),
                alias: alias.clone(),
            },
            "avg" => DownsampleColumn::Avg {
                source: src.clone(),
                alias: alias.clone(),
            },
            "count" => DownsampleColumn::Count {
                alias: alias.clone(),
            },
            _ => DownsampleColumn::Sum {
                source: src.clone(),
                alias: alias.clone(),
            },
        })
        .collect();

    let interval = DownsampleInterval {
        name: target_name.to_string(),
        interval: std::time::Duration::from_secs(interval_secs),
        columns: ds_columns,
        partition_by: vec![],
    };

    let _config = DownsamplingConfig {
        source_table: source_table.to_string(),
        intervals: vec![interval],
        auto_refresh: true,
        refresh_lag: std::time::Duration::from_secs(0),
    };

    // Store the config as a JSON file for the background scheduler to pick up.
    let ds_dir = db_root.join("__downsampling__");
    std::fs::create_dir_all(&ds_dir)?;
    let config_path = ds_dir.join(format!("{target_name}.json"));
    let config_json = serde_json::json!({
        "source_table": source_table,
        "target_name": target_name,
        "interval_secs": interval_secs,
        "columns": columns.iter().map(|(f, s, a)| {
            serde_json::json!({"function": f, "source": s, "alias": a})
        }).collect::<Vec<_>>(),
    });
    std::fs::write(&config_path, config_json.to_string())?;

    Ok(QueryResult::Ok { affected_rows: 0 })
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

/// Directory for storing view definitions.
fn views_dir(db_root: &Path) -> PathBuf {
    db_root.join("_views")
}

fn execute_create_view(db_root: &Path, name: &str, sql: &str) -> Result<QueryResult> {
    let dir = views_dir(db_root);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{name}.json"));
    if path.exists() {
        return Err(ExchangeDbError::Query(format!(
            "view '{name}' already exists"
        )));
    }
    let data = serde_json::json!({ "name": name, "sql": sql });
    std::fs::write(&path, data.to_string())?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_view(db_root: &Path, name: &str) -> Result<QueryResult> {
    let path = views_dir(db_root).join(format!("{name}.json"));
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Try to resolve a SELECT from a view. If the table name corresponds to a
/// stored view definition, parse and plan the view SQL, then execute it with
/// the outer filters merged in.
fn resolve_view(
    db_root: &Path,
    table: &str,
    _columns: &[SelectColumn],
    outer_filter: Option<&Filter>,
    _order_by: &[OrderBy],
    _limit: Option<u64>,
    _offset: Option<u64>,
) -> Option<QueryPlan> {
    let table_dir = db_root.join(table);
    if table_dir.exists() {
        return None; // Real table, not a view.
    }
    let view_path = views_dir(db_root).join(format!("{table}.json"));
    if !view_path.exists() {
        return None;
    }
    let data = std::fs::read_to_string(&view_path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&data).ok()?;
    let view_sql = json.get("sql")?.as_str()?;

    // Plan the view's inner query.
    let mut inner_plan = crate::planner::plan_query(view_sql).ok()?;

    // Merge outer filter into the inner plan if present.
    if let Some(outer_f) = outer_filter
        && let QueryPlan::Select { ref mut filter, .. } = inner_plan
    {
        *filter = Some(match filter.take() {
            Some(existing) => Filter::And(vec![existing, outer_f.clone()]),
            None => outer_f.clone(),
        });
    }

    Some(inner_plan)
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

/// Directory for storing trigger definitions for a given table.
fn triggers_dir(db_root: &Path, table: &str) -> PathBuf {
    db_root.join("_triggers").join(table)
}

fn execute_create_trigger(
    db_root: &Path,
    name: &str,
    table: &str,
    procedure: &str,
) -> Result<QueryResult> {
    let dir = triggers_dir(db_root, table);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{name}.json"));
    let data = serde_json::json!({ "name": name, "table": table, "procedure": procedure });
    std::fs::write(&path, data.to_string())?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

fn execute_drop_trigger(db_root: &Path, name: &str, table: &str) -> Result<QueryResult> {
    let path = triggers_dir(db_root, table).join(format!("{name}.json"));
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Fire AFTER INSERT triggers for a table. Errors are silently ignored since
/// trigger execution is best-effort in this simple implementation.
fn fire_triggers_after_insert(db_root: &Path, table: &str) {
    let dir = triggers_dir(db_root, table);
    if !dir.exists() {
        return;
    }
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Ok(data) = std::fs::read_to_string(&path)
            && let Ok(json) = serde_json::from_str::<serde_json::Value>(&data)
            && let Some(proc_name) = json.get("procedure").and_then(|v| v.as_str())
        {
            // Call the stored procedure (best-effort).
            let _ = execute_call_procedure(db_root, proc_name);
        }
    }
}

// ---------------------------------------------------------------------------
// UNIQUE constraint validation
// ---------------------------------------------------------------------------

fn validate_unique_constraints(db_root: &Path, table: &str, values: &[Vec<Value>]) -> Result<()> {
    let unique_path = db_root.join(table).join("_unique.json");
    if !unique_path.exists() {
        return Ok(());
    }
    let data = std::fs::read_to_string(&unique_path).map_err(ExchangeDbError::Io)?;
    let constraints: Vec<UniqueConstraint> = serde_json::from_str(&data)
        .map_err(|e| ExchangeDbError::Query(format!("invalid unique constraints: {e}")))?;
    if constraints.is_empty() {
        return Ok(());
    }

    let meta_path = db_root.join(table).join("_meta");
    let meta = TableMeta::load(&meta_path)?;
    let col_names: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();

    // Read all existing rows.
    let table_dir = db_root.join(table);
    let partitions = exchange_core::table::list_partitions(&table_dir)?;
    let mut existing_rows: Vec<Vec<Value>> = Vec::new();
    for partition_path in &partitions {
        let rows =
            exchange_core::table::read_partition_rows_tiered(partition_path, &meta, &table_dir)?;
        for row in rows {
            let vals: Vec<Value> = row.iter().map(|v| cv_to_plan_value(v)).collect();
            existing_rows.push(vals);
        }
    }

    for constraint in &constraints {
        let col_indices: Vec<usize> = constraint
            .columns
            .iter()
            .filter_map(|cn| col_names.iter().position(|n| n.eq_ignore_ascii_case(cn)))
            .collect();
        if col_indices.is_empty() {
            continue;
        }

        // Collect existing values for the unique columns.
        let mut seen: Vec<Vec<Value>> = existing_rows
            .iter()
            .map(|row| {
                col_indices
                    .iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();

        // Check new rows against existing + previous new rows.
        for new_row in values {
            let key: Vec<Value> = col_indices
                .iter()
                .map(|&i| new_row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            // NULL values are not considered duplicates.
            if key.iter().any(|v| matches!(v, Value::Null)) {
                seen.push(key);
                continue;
            }
            if seen.iter().any(|existing_key| existing_key == &key) {
                return Err(ExchangeDbError::Query(format!(
                    "UNIQUE constraint violation: duplicate value on column(s) {:?}",
                    constraint.columns
                )));
            }
            seen.push(key);
        }
    }
    Ok(())
}

/// Convert a ColumnValue back to a plan `Value` (simple, no meta needed).
fn cv_to_plan_value(cv: &ColumnValue<'_>) -> Value {
    match cv {
        ColumnValue::I64(v) => Value::I64(*v),
        ColumnValue::F64(v) => Value::F64(*v),
        ColumnValue::Str(s) => Value::Str(s.to_string()),
        ColumnValue::Timestamp(ts) => Value::Timestamp(ts.as_nanos()),
        ColumnValue::Null => Value::Null,
        ColumnValue::I32(v) => Value::I64(*v as i64),
        ColumnValue::Bytes(_) => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// FOREIGN KEY constraint validation
// ---------------------------------------------------------------------------

fn validate_foreign_key_constraints(
    db_root: &Path,
    table: &str,
    values: &[Vec<Value>],
) -> Result<()> {
    let fk_path = db_root.join(table).join("_fkeys.json");
    if !fk_path.exists() {
        return Ok(());
    }
    let data = std::fs::read_to_string(&fk_path).map_err(ExchangeDbError::Io)?;
    let constraints: Vec<ForeignKeyConstraint> = serde_json::from_str(&data)
        .map_err(|e| ExchangeDbError::Query(format!("invalid foreign key constraints: {e}")))?;
    if constraints.is_empty() {
        return Ok(());
    }

    let meta_path = db_root.join(table).join("_meta");
    let meta = TableMeta::load(&meta_path)?;
    let col_names: Vec<String> = meta.columns.iter().map(|c| c.name.clone()).collect();

    for fk in &constraints {
        let col_idx = col_names
            .iter()
            .position(|n| n.eq_ignore_ascii_case(&fk.column));
        let col_idx = match col_idx {
            Some(i) => i,
            None => continue,
        };

        // Load parent table data.
        let parent_dir = db_root.join(&fk.ref_table);
        if !parent_dir.exists() {
            return Err(ExchangeDbError::Query(format!(
                "foreign key references table '{}' which does not exist",
                fk.ref_table
            )));
        }
        let parent_meta = TableMeta::load(&parent_dir.join("_meta"))?;
        let parent_col_names: Vec<String> =
            parent_meta.columns.iter().map(|c| c.name.clone()).collect();
        let ref_col_idx = parent_col_names
            .iter()
            .position(|n| n.eq_ignore_ascii_case(&fk.ref_column))
            .ok_or_else(|| {
                ExchangeDbError::Query(format!(
                    "foreign key column '{}' not found in table '{}'",
                    fk.ref_column, fk.ref_table
                ))
            })?;

        let parent_partitions = exchange_core::table::list_partitions(&parent_dir)?;
        let mut parent_values: HashSet<String> = HashSet::new();
        for pp in &parent_partitions {
            let rows =
                exchange_core::table::read_partition_rows_tiered(pp, &parent_meta, &parent_dir)?;
            for row in rows {
                if let Some(v) = row.get(ref_col_idx) {
                    parent_values.insert(format!("{:?}", v));
                }
            }
        }

        // Check each new row.
        for new_row in values {
            let val = new_row.get(col_idx).cloned().unwrap_or(Value::Null);
            if matches!(&val, Value::Null) {
                continue; // NULL is allowed in FK.
            }
            let val_key = match &val {
                Value::I64(v) => format!("I64({v})"),
                Value::F64(v) => format!("F64({v})"),
                Value::Str(s) => format!("Str(\"{s}\")"),
                Value::Timestamp(ns) => format!("Timestamp({ns})"),
                Value::Null => continue,
            };
            if !parent_values.contains(&val_key) {
                return Err(ExchangeDbError::Query(format!(
                    "foreign key constraint violation: value {} in column '{}' not found in {}.{}",
                    val, fk.column, fk.ref_table, fk.ref_column
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// COMMENT ON
// ---------------------------------------------------------------------------

/// Stored comments: a simple JSON map in `db_root/_comments.json`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
struct CommentsStore {
    /// Table-level comments: table_name -> comment.
    #[serde(default)]
    tables: HashMap<String, String>,
    /// Column-level comments: "table.column" -> comment.
    #[serde(default)]
    columns: HashMap<String, String>,
}

fn comments_path(db_root: &Path) -> PathBuf {
    db_root.join("_comments.json")
}

fn load_comments(db_root: &Path) -> CommentsStore {
    let path = comments_path(db_root);
    if !path.exists() {
        return CommentsStore::default();
    }
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
        .unwrap_or_default()
}

fn save_comments(db_root: &Path, store: &CommentsStore) -> Result<()> {
    let path = comments_path(db_root);
    let data = serde_json::to_string_pretty(store)
        .map_err(|e| ExchangeDbError::Query(format!("failed to serialize comments: {e}")))?;
    std::fs::write(&path, data)?;
    Ok(())
}

fn execute_comment_on(
    db_root: &Path,
    object_type: &CommentObjectType,
    object_name: &str,
    table_name: Option<&str>,
    comment: &str,
) -> Result<QueryResult> {
    let mut store = load_comments(db_root);
    match object_type {
        CommentObjectType::Table => {
            store
                .tables
                .insert(object_name.to_string(), comment.to_string());
        }
        CommentObjectType::Column => {
            let table = table_name.unwrap_or("");
            let key = format!("{table}.{object_name}");
            store.columns.insert(key, comment.to_string());
        }
    }
    save_comments(db_root, &store)?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

// ---------------------------------------------------------------------------
// Standard GRANT syntax mapping
// ---------------------------------------------------------------------------

/// Extended grant_permission_to_permission that handles SQL-standard privileges.
fn grant_std_permission_to_permissions(
    gp: &GrantPermission,
) -> Vec<exchange_core::rbac::Permission> {
    use exchange_core::rbac::Permission;
    match gp {
        GrantPermission::Select { table } => vec![Permission::Read {
            table: Some(table.clone()),
        }],
        GrantPermission::Insert { table }
        | GrantPermission::Update { table }
        | GrantPermission::Delete { table } => {
            vec![Permission::Write {
                table: Some(table.clone()),
            }]
        }
        GrantPermission::All { table } => vec![
            Permission::Read {
                table: Some(table.clone()),
            },
            Permission::Write {
                table: Some(table.clone()),
            },
        ],
        other => vec![grant_permission_to_permission(other)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_by_single_key() {
        // Simulate: SELECT symbol, sum(price) FROM ... GROUP BY symbol
        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Sum,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "symbol".into()), (1, "price".into())];
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("ETH".into()), Value::F64(50.0)],
            vec![Value::Str("BTC".into()), Value::F64(200.0)],
            vec![Value::Str("ETH".into()), Value::F64(75.0)],
        ];
        let group_by = vec!["symbol".to_string()];

        let result = apply_group_by(&select_cols, &resolved, &rows, &group_by, None).unwrap();
        assert_eq!(result.len(), 2);

        // Find BTC group
        let btc = result
            .iter()
            .find(|r| r[0] == Value::Str("BTC".into()))
            .unwrap();
        assert_eq!(btc[1], Value::F64(300.0));

        // Find ETH group
        let eth = result
            .iter()
            .find(|r| r[0] == Value::Str("ETH".into()))
            .unwrap();
        assert_eq!(eth[1], Value::F64(125.0));
    }

    #[test]
    fn test_group_by_multiple_keys() {
        // SELECT exchange, symbol, count(*) FROM ... GROUP BY exchange, symbol
        let select_cols = vec![
            SelectColumn::Name("exchange".into()),
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Count,
                column: "*".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![
            (0, "exchange".into()),
            (1, "symbol".into()),
            (2, "price".into()), // placeholder for count(*)
        ];
        let rows = vec![
            vec![
                Value::Str("NYSE".into()),
                Value::Str("BTC".into()),
                Value::F64(100.0),
            ],
            vec![
                Value::Str("NYSE".into()),
                Value::Str("BTC".into()),
                Value::F64(200.0),
            ],
            vec![
                Value::Str("NASDAQ".into()),
                Value::Str("BTC".into()),
                Value::F64(150.0),
            ],
            vec![
                Value::Str("NYSE".into()),
                Value::Str("ETH".into()),
                Value::F64(50.0),
            ],
        ];
        let group_by = vec!["exchange".to_string(), "symbol".to_string()];

        let result = apply_group_by(&select_cols, &resolved, &rows, &group_by, None).unwrap();
        assert_eq!(result.len(), 3); // NYSE/BTC, NASDAQ/BTC, NYSE/ETH

        let nyse_btc = result
            .iter()
            .find(|r| r[0] == Value::Str("NYSE".into()) && r[1] == Value::Str("BTC".into()))
            .unwrap();
        assert_eq!(nyse_btc[2], Value::I64(2));
    }

    #[test]
    fn test_group_by_with_having() {
        // SELECT symbol, count(*) FROM ... GROUP BY symbol HAVING count(*) > 2
        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Count,
                column: "*".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "symbol".into()), (1, "price".into())];
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("BTC".into()), Value::F64(200.0)],
            vec![Value::Str("BTC".into()), Value::F64(300.0)],
            vec![Value::Str("ETH".into()), Value::F64(50.0)],
            vec![Value::Str("ETH".into()), Value::F64(75.0)],
        ];
        let group_by = vec!["symbol".to_string()];
        let having = Filter::Gt("count(*)".into(), Value::I64(2));

        let result =
            apply_group_by(&select_cols, &resolved, &rows, &group_by, Some(&having)).unwrap();
        assert_eq!(result.len(), 1); // Only BTC has count > 2
        assert_eq!(result[0][0], Value::Str("BTC".into()));
        assert_eq!(result[0][1], Value::I64(3));
    }

    #[test]
    fn test_group_by_with_avg() {
        // SELECT symbol, avg(price) FROM ... GROUP BY symbol
        let select_cols = vec![
            SelectColumn::Name("symbol".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Avg,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "symbol".into()), (1, "price".into())];
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("BTC".into()), Value::F64(200.0)],
            vec![Value::Str("ETH".into()), Value::F64(50.0)],
        ];
        let group_by = vec!["symbol".to_string()];

        let result = apply_group_by(&select_cols, &resolved, &rows, &group_by, None).unwrap();
        let btc = result
            .iter()
            .find(|r| r[0] == Value::Str("BTC".into()))
            .unwrap();
        assert_eq!(btc[1], Value::F64(150.0));

        let eth = result
            .iter()
            .find(|r| r[0] == Value::Str("ETH".into()))
            .unwrap();
        assert_eq!(eth[1], Value::F64(50.0));
    }

    #[test]
    fn test_distinct_basic() {
        let rows = vec![
            vec![Value::Str("BTC".into())],
            vec![Value::Str("ETH".into())],
            vec![Value::Str("BTC".into())],
            vec![Value::Str("SOL".into())],
            vec![Value::Str("ETH".into())],
        ];
        let result = apply_distinct(rows);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::Str("BTC".into()));
        assert_eq!(result[1][0], Value::Str("ETH".into()));
        assert_eq!(result[2][0], Value::Str("SOL".into()));
    }

    #[test]
    fn test_distinct_multi_column() {
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::F64(100.0)],
            vec![Value::Str("BTC".into()), Value::F64(200.0)],
            vec![Value::Str("BTC".into()), Value::F64(100.0)], // duplicate
        ];
        let result = apply_distinct(rows);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_distinct_preserves_order() {
        let rows = vec![
            vec![Value::I64(3)],
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(1)],
            vec![Value::I64(3)],
        ];
        let result = apply_distinct(rows);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::I64(3));
        assert_eq!(result[1][0], Value::I64(1));
        assert_eq!(result[2][0], Value::I64(2));
    }

    /// Helper: create a test table and insert some rows, return the db_root path.
    fn setup_test_table(dir: &std::path::Path) -> std::path::PathBuf {
        let db_root = dir.to_path_buf();

        // CREATE TABLE
        let create_plan = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        )
        .unwrap();
        execute(&db_root, &create_plan).unwrap();

        // INSERT rows
        let insert_plan = crate::planner::plan_query(
            "INSERT INTO trades VALUES \
             (1000000000, 'BTC/USD', 65000.0, 1.5), \
             (2000000000, 'ETH/USD', 3000.0, 10.0), \
             (3000000000, 'BTC/USD', 64000.0, 2.0), \
             (4000000000, 'ETH/USD', 3100.0, 5.0), \
             (5000000000, 'SOL/USD', 100.0, 100.0)",
        )
        .unwrap();
        execute(&db_root, &insert_plan).unwrap();

        db_root
    }

    #[test]
    fn test_delete_with_filter() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Delete rows where symbol = 'BTC/USD'
        let plan =
            crate::planner::plan_query("DELETE FROM trades WHERE symbol = 'BTC/USD'").unwrap();
        let result = execute(&db_root, &plan).unwrap();

        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 2),
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify remaining rows
        let select = crate::planner::plan_query("SELECT * FROM trades ORDER BY timestamp").unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                // Should have ETH/USD, ETH/USD, SOL/USD
                assert_eq!(rows[0][1], Value::Str("ETH/USD".into()));
                assert_eq!(rows[1][1], Value::Str("ETH/USD".into()));
                assert_eq!(rows[2][1], Value::Str("SOL/USD".into()));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_delete_all() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Delete all rows (no filter)
        let plan = crate::planner::plan_query("DELETE FROM trades").unwrap();
        let result = execute(&db_root, &plan).unwrap();

        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 5),
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify empty
        let select = crate::planner::plan_query("SELECT * FROM trades").unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_update_single_column() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Update price to 99999 for all rows
        let plan = crate::planner::plan_query("UPDATE trades SET price = 99999.0").unwrap();
        let result = execute(&db_root, &plan).unwrap();

        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 5),
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify all prices changed
        let select = crate::planner::plan_query("SELECT price FROM trades").unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 5);
                for row in &rows {
                    assert_eq!(row[0], Value::F64(99999.0));
                }
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_update_with_filter() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Update price to 70000 only for BTC/USD
        let plan = crate::planner::plan_query(
            "UPDATE trades SET price = 70000.0 WHERE symbol = 'BTC/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();

        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 2),
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify BTC prices changed, others unchanged
        let select =
            crate::planner::plan_query("SELECT symbol, price FROM trades ORDER BY timestamp")
                .unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 5);
                // Row 0: BTC/USD -> 70000
                assert_eq!(rows[0][0], Value::Str("BTC/USD".into()));
                assert_eq!(rows[0][1], Value::F64(70000.0));
                // Row 1: ETH/USD -> unchanged (3000)
                assert_eq!(rows[1][0], Value::Str("ETH/USD".into()));
                assert_eq!(rows[1][1], Value::F64(3000.0));
                // Row 2: BTC/USD -> 70000
                assert_eq!(rows[2][0], Value::Str("BTC/USD".into()));
                assert_eq!(rows[2][1], Value::F64(70000.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_insert_or_replace() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        // Create table
        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
        )
        .unwrap();
        execute(&db_root, &create).unwrap();

        // Insert initial row
        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES (1000000000, 'BTC/USD', 65000.0)",
        )
        .unwrap();
        execute(&db_root, &ins).unwrap();

        // Insert or replace with same timestamp
        let upsert = crate::planner::plan_query(
            "INSERT OR REPLACE INTO trades VALUES (1000000000, 'BTC/USD', 70000.0)",
        )
        .unwrap();
        execute(&db_root, &upsert).unwrap();

        // Should have only 1 row with the updated value
        let select = crate::planner::plan_query("SELECT * FROM trades").unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][2], Value::F64(70000.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_create_matview_from_sample_by() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        ).unwrap();
        execute(&db_root, &create).unwrap();

        // Bucket 1: 0s, 30s  Bucket 2: 60s, 90s
        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES \
             (0, 'BTC/USD', 100.0, 1.0), \
             (30000000000, 'BTC/USD', 200.0, 2.0), \
             (60000000000, 'BTC/USD', 300.0, 3.0), \
             (90000000000, 'BTC/USD', 400.0, 4.0)",
        )
        .unwrap();
        execute(&db_root, &ins).unwrap();

        let cmv = crate::planner::plan_query(
            "CREATE MATERIALIZED VIEW ohlcv_1m AS \
             SELECT symbol, first(price) as open, max(price) as high, \
                    min(price) as low, last(price) as close, \
                    sum(volume) as total_vol \
             FROM trades SAMPLE BY 1m",
        )
        .unwrap();
        let result = execute(&db_root, &cmv).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));

        assert!(db_root.join("ohlcv_1m").exists());
        assert!(db_root.join("ohlcv_1m").join("_matview").exists());

        let select =
            crate::planner::plan_query("SELECT open, high, low, close, total_vol FROM ohlcv_1m")
                .unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert!(columns.contains(&"open".to_string()));
                assert!(columns.contains(&"close".to_string()));
                assert_eq!(rows.len(), 2);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_query_matview() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        ).unwrap();
        execute(&db_root, &create).unwrap();

        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES \
             (1000000000, 'BTC/USD', 100.0, 1.0), \
             (2000000000, 'ETH/USD', 50.0, 2.0), \
             (3000000000, 'BTC/USD', 200.0, 3.0)",
        )
        .unwrap();
        execute(&db_root, &ins).unwrap();

        let cmv = crate::planner::plan_query(
            "CREATE MATERIALIZED VIEW trade_summary AS \
             SELECT symbol, sum(volume) as total_vol, avg(price) as avg_price \
             FROM trades GROUP BY symbol",
        )
        .unwrap();
        execute(&db_root, &cmv).unwrap();

        let select = crate::planner::plan_query(
            "SELECT symbol, total_vol FROM trade_summary WHERE symbol = 'BTC/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("BTC/USD".into()));
                assert_eq!(rows[0][1], Value::F64(4.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_refresh_matview() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        ).unwrap();
        execute(&db_root, &create).unwrap();

        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES \
             (1000000000, 'BTC/USD', 100.0, 1.0), \
             (2000000000, 'ETH/USD', 50.0, 2.0)",
        )
        .unwrap();
        execute(&db_root, &ins).unwrap();

        let cmv = crate::planner::plan_query(
            "CREATE MATERIALIZED VIEW vol_summary AS \
             SELECT symbol, sum(volume) as total_vol FROM trades GROUP BY symbol",
        )
        .unwrap();
        execute(&db_root, &cmv).unwrap();

        let ins2 = crate::planner::plan_query(
            "INSERT INTO trades VALUES (3000000000, 'BTC/USD', 150.0, 5.0)",
        )
        .unwrap();
        execute(&db_root, &ins2).unwrap();

        let select = crate::planner::plan_query(
            "SELECT total_vol FROM vol_summary WHERE symbol = 'BTC/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &select).unwrap();
        match &result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::F64(1.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }

        let refresh = crate::planner::plan_query("REFRESH MATERIALIZED VIEW vol_summary").unwrap();
        let result = execute(&db_root, &refresh).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));

        let select2 = crate::planner::plan_query(
            "SELECT total_vol FROM vol_summary WHERE symbol = 'BTC/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &select2).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::F64(6.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_drop_matview() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        ).unwrap();
        execute(&db_root, &create).unwrap();

        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES (1000000000, 'BTC/USD', 100.0, 1.0)",
        )
        .unwrap();
        execute(&db_root, &ins).unwrap();

        let cmv = crate::planner::plan_query(
            "CREATE MATERIALIZED VIEW test_mv AS \
             SELECT symbol, sum(volume) as total_vol FROM trades GROUP BY symbol",
        )
        .unwrap();
        execute(&db_root, &cmv).unwrap();
        assert!(db_root.join("test_mv").exists());

        let drop = crate::planner::plan_query("DROP MATERIALIZED VIEW test_mv").unwrap();
        let result = execute(&db_root, &drop).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));

        assert!(!db_root.join("test_mv").exists());

        let select = crate::planner::plan_query("SELECT * FROM test_mv").unwrap();
        let result = execute(&db_root, &select);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_matview_from_group_by() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        ).unwrap();
        execute(&db_root, &create).unwrap();

        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES \
             (1000000000, 'BTC/USD', 100.0, 1.0), \
             (2000000000, 'BTC/USD', 200.0, 2.0), \
             (3000000000, 'ETH/USD', 50.0, 3.0), \
             (4000000000, 'ETH/USD', 60.0, 4.0), \
             (5000000000, 'SOL/USD', 10.0, 10.0)",
        )
        .unwrap();
        execute(&db_root, &ins).unwrap();

        let cmv = crate::planner::plan_query(
            "CREATE MATERIALIZED VIEW symbol_stats AS \
             SELECT symbol, count(price) as trade_count, max(price) as max_price, \
                    min(price) as min_price, sum(volume) as total_vol \
             FROM trades GROUP BY symbol",
        )
        .unwrap();
        let result = execute(&db_root, &cmv).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));

        let matview_meta = exchange_core::matview::MatViewMeta::load(
            &db_root.join("symbol_stats").join("_matview"),
        )
        .unwrap();
        assert_eq!(matview_meta.name, "symbol_stats");
        assert_eq!(matview_meta.source_table, "trades");
        assert!(matview_meta.last_refresh.is_some());

        let select = crate::planner::plan_query(
            "SELECT symbol, trade_count, max_price, total_vol FROM symbol_stats ORDER BY symbol",
        )
        .unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // =====================================================================
    // CTE (Common Table Expression) tests
    // =====================================================================

    #[test]
    fn test_simple_cte() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        let plan = crate::planner::plan_query(
            "WITH high_volume AS (SELECT symbol, price, volume FROM trades WHERE volume > 5) \
             SELECT symbol, price FROM high_volume ORDER BY price",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["symbol", "price"]);
                // volume > 5: ETH/USD (10.0), ETH/USD (5.0), SOL/USD (100.0)
                // ETH/USD vol=5.0 is NOT > 5, so just ETH/USD(10.0) and SOL/USD(100.0)
                assert_eq!(rows.len(), 2);
                // Ordered by price: SOL/USD (100.0), ETH/USD (3000.0)
                assert_eq!(rows[0][0], Value::Str("SOL/USD".into()));
                assert_eq!(rows[0][1], Value::F64(100.0));
                assert_eq!(rows[1][0], Value::Str("ETH/USD".into()));
                assert_eq!(rows[1][1], Value::F64(3000.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_cte_referenced_multiple_times() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Use a CTE and reference it in a query with aggregate
        let plan = crate::planner::plan_query(
            "WITH btc_trades AS (SELECT symbol, price FROM trades WHERE symbol = 'BTC/USD') \
             SELECT symbol, avg(price) FROM btc_trades GROUP BY symbol",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("BTC/USD".into()));
                // avg of 65000.0 and 64000.0 = 64500.0
                assert_eq!(rows[0][1], Value::F64(64500.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // =====================================================================
    // UNION / INTERSECT / EXCEPT tests
    // =====================================================================

    #[test]
    fn test_union_all() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        let plan = crate::planner::plan_query(
            "SELECT symbol FROM trades WHERE symbol = 'BTC/USD' \
             UNION ALL \
             SELECT symbol FROM trades WHERE symbol = 'ETH/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // 2 BTC/USD + 2 ETH/USD = 4
                assert_eq!(rows.len(), 4);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_union_deduplicated() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        let plan = crate::planner::plan_query(
            "SELECT symbol FROM trades WHERE symbol = 'BTC/USD' \
             UNION \
             SELECT symbol FROM trades WHERE symbol = 'ETH/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Deduplicated: BTC/USD, ETH/USD = 2
                assert_eq!(rows.len(), 2);
                let symbols: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
                assert!(symbols.contains(&&Value::Str("BTC/USD".into())));
                assert!(symbols.contains(&&Value::Str("ETH/USD".into())));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_intersect() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Intersect between all symbols and those with price > 1000
        // All symbols: BTC/USD, ETH/USD, SOL/USD
        // Symbols with price > 1000: BTC/USD, ETH/USD
        let plan = crate::planner::plan_query(
            "SELECT symbol FROM trades \
             INTERSECT \
             SELECT symbol FROM trades WHERE price > 1000",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // BTC/USD and ETH/USD appear in both (deduplicated)
                assert_eq!(rows.len(), 2);
                let symbols: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
                assert!(symbols.contains(&&Value::Str("BTC/USD".into())));
                assert!(symbols.contains(&&Value::Str("ETH/USD".into())));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_except() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // All symbols EXCEPT those with price > 1000
        let plan = crate::planner::plan_query(
            "SELECT symbol FROM trades \
             EXCEPT \
             SELECT symbol FROM trades WHERE price > 1000",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // SOL/USD is the only one with price <= 1000
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Str("SOL/USD".into()));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // =====================================================================
    // Subquery tests
    // =====================================================================

    #[test]
    fn test_scalar_subquery_in_where() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Select rows where price > average price
        let plan = crate::planner::plan_query(
            "SELECT symbol, price FROM trades WHERE price > (SELECT avg(price) FROM trades) ORDER BY price",
        ).unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // avg price = (65000 + 3000 + 64000 + 3100 + 100) / 5 = 27040
                // Rows with price > 27040: BTC/USD (65000), BTC/USD (64000)
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::F64(64000.0));
                assert_eq!(rows[1][1], Value::F64(65000.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_derived_table_in_from() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());

        // Select from a subquery in FROM
        let plan = crate::planner::plan_query(
            "SELECT symbol, avg_price FROM \
             (SELECT symbol, avg(price) as avg_price FROM trades GROUP BY symbol) sub \
             WHERE avg_price > 1000 ORDER BY avg_price",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["symbol", "avg_price"]);
                // BTC/USD avg = 64500, ETH/USD avg = 3050, SOL/USD avg = 100
                // avg_price > 1000: ETH/USD (3050), BTC/USD (64500)
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Str("ETH/USD".into()));
                assert_eq!(rows[0][1], Value::F64(3050.0));
                assert_eq!(rows[1][0], Value::Str("BTC/USD".into()));
                assert_eq!(rows[1][1], Value::F64(64500.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_copy_to_from_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        // Create and populate a table.
        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        let insert = crate::planner::plan_query(
            "INSERT INTO trades VALUES (1000000000, 'BTC/USD', 65000.0), (2000000000, 'ETH/USD', 3100.0)",
        ).unwrap();
        execute(db_root, &insert).unwrap();

        // COPY TO
        let csv_path = dir.path().join("trades.csv");
        let copy_to = QueryPlan::CopyTo {
            table: "trades".to_string(),
            path: csv_path.clone(),
            options: CopyOptions::default(),
        };
        let result = execute(db_root, &copy_to).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 2),
            other => panic!("expected Ok, got {other:?}"),
        }
        assert!(csv_path.exists());

        // Drop original table.
        let drop_plan = crate::planner::plan_query("DROP TABLE trades").unwrap();
        execute(db_root, &drop_plan).unwrap();

        // COPY FROM into new table (auto-create).
        let copy_from = QueryPlan::CopyFrom {
            table: "trades".to_string(),
            path: csv_path,
            options: CopyOptions::default(),
        };
        let result = execute(db_root, &copy_from).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 2),
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify the data.
        let select = crate::planner::plan_query("SELECT * FROM trades").unwrap();
        let result = execute(db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(rows.len(), 2);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_explain_output_format() {
        let db_root = tempfile::tempdir().unwrap();
        let db = db_root.path();

        // Create a table so the planner can reference it.
        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, price DOUBLE, symbol VARCHAR)",
        )
        .unwrap();
        execute(db, &create).unwrap();

        let plan = crate::planner::plan_query(
            "EXPLAIN SELECT * FROM trades WHERE price > 100 ORDER BY timestamp",
        )
        .unwrap();

        // Verify it's an Explain variant.
        match &plan {
            QueryPlan::Explain { .. } => {}
            other => panic!("expected Explain, got {other:?}"),
        }

        let result = execute(db, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["plan".to_string()]);
                assert!(!rows.is_empty());
                let plan_text = match &rows[0][0] {
                    Value::Str(s) => s.clone(),
                    other => panic!("expected Str, got {other:?}"),
                };
                assert!(plan_text.contains("SELECT on table: trades"));
                assert!(plan_text.contains("columns:"));
                assert!(plan_text.contains("filter:"));
                assert!(plan_text.contains("order_by:"));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_vacuum_basic() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        // Create and populate a table.
        let create =
            crate::planner::plan_query("CREATE TABLE trades (timestamp TIMESTAMP, price DOUBLE)")
                .unwrap();
        execute(db_root, &create).unwrap();

        let insert =
            crate::planner::plan_query("INSERT INTO trades VALUES (1000000000, 65000.0)").unwrap();
        execute(db_root, &insert).unwrap();

        // Create a fake applied WAL segment.
        let wal_dir = db_root.join("trades").join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::write(wal_dir.join("wal-000000.applied"), b"test_data").unwrap();

        // Run VACUUM via planner.
        let vacuum_plan = crate::planner::plan_query("VACUUM trades").unwrap();
        match &vacuum_plan {
            QueryPlan::Vacuum { table } => assert_eq!(table, "trades"),
            other => panic!("expected Vacuum, got {other:?}"),
        }

        let result = execute(db_root, &vacuum_plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns[0], "wal_segments_removed");
                assert_eq!(rows[0][0], Value::I64(1)); // one applied segment removed
            }
            other => panic!("expected Rows, got {other:?}"),
        }

        // Verify the applied segment was removed.
        assert!(!wal_dir.join("wal-000000.applied").exists());
    }

    // ── LIKE pattern matching tests ────────────────────────────────

    #[test]
    fn test_like_match_percent() {
        assert!(like_match("BTC/USD", "BTC%", false));
        assert!(like_match("BTC/USD", "%USD", false));
        assert!(like_match("BTC/USD", "%TC%", false));
        assert!(!like_match("ETH/USD", "BTC%", false));
        assert!(like_match("", "%", false));
        assert!(!like_match("abc", "a%d", false));
    }

    #[test]
    fn test_like_match_underscore() {
        assert!(like_match("BTC/USD", "BTC_USD", false));
        assert!(!like_match("BTC/USD", "BTC__USD", false));
        assert!(like_match("abc", "a_c", false));
        assert!(!like_match("abcd", "a_c", false));
    }

    #[test]
    fn test_like_match_case_insensitive() {
        assert!(like_match("BTC/USD", "%usd", true));
        assert!(like_match("btc/usd", "BTC%", true));
        assert!(!like_match("BTC/USD", "%usd", false));
    }

    // ── IS NULL / IS NOT NULL executor tests ───────────────────────

    #[test]
    fn test_evaluate_case_filter_is_null() {
        let resolved = vec![(0, "symbol".to_string()), (1, "price".to_string())];

        // Row with null symbol
        let row_null = vec![Value::Null, Value::F64(100.0)];
        assert!(evaluate_case_filter(
            &Filter::IsNull("symbol".into()),
            &row_null,
            &resolved
        ));
        assert!(!evaluate_case_filter(
            &Filter::IsNotNull("symbol".into()),
            &row_null,
            &resolved
        ));

        // Row with non-null symbol
        let row_present = vec![Value::Str("BTC".into()), Value::F64(100.0)];
        assert!(!evaluate_case_filter(
            &Filter::IsNull("symbol".into()),
            &row_present,
            &resolved
        ));
        assert!(evaluate_case_filter(
            &Filter::IsNotNull("symbol".into()),
            &row_present,
            &resolved
        ));
    }

    // ── IN / NOT IN executor tests ─────────────────────────────────

    #[test]
    fn test_evaluate_case_filter_in() {
        let resolved = vec![(0, "symbol".to_string())];
        let row = vec![Value::Str("BTC".into())];
        let list = vec![Value::Str("BTC".into()), Value::Str("ETH".into())];

        assert!(evaluate_case_filter(
            &Filter::In("symbol".into(), list.clone()),
            &row,
            &resolved
        ));
        assert!(!evaluate_case_filter(
            &Filter::NotIn("symbol".into(), list.clone()),
            &row,
            &resolved
        ));

        let row2 = vec![Value::Str("SOL".into())];
        assert!(!evaluate_case_filter(
            &Filter::In("symbol".into(), list.clone()),
            &row2,
            &resolved
        ));
        assert!(evaluate_case_filter(
            &Filter::NotIn("symbol".into(), list),
            &row2,
            &resolved
        ));
    }

    #[test]
    fn test_evaluate_case_filter_in_numeric() {
        let resolved = vec![(0, "price".to_string())];
        let row = vec![Value::I64(100)];
        let list = vec![Value::I64(100), Value::I64(200), Value::I64(300)];

        assert!(evaluate_case_filter(
            &Filter::In("price".into(), list.clone()),
            &row,
            &resolved
        ));

        let row2 = vec![Value::I64(150)];
        assert!(!evaluate_case_filter(
            &Filter::In("price".into(), list),
            &row2,
            &resolved
        ));
    }

    // ── CASE WHEN executor tests ───────────────────────────────────

    #[test]
    fn test_apply_scalar_case_when() {
        let select_cols = vec![SelectColumn::CaseWhen {
            conditions: vec![
                (
                    Filter::Gt("price".into(), Value::F64(100.0)),
                    Value::Str("high".into()),
                ),
                (
                    Filter::Gt("price".into(), Value::F64(50.0)),
                    Value::Str("mid".into()),
                ),
            ],
            else_value: Some(Value::Str("low".into())),
            alias: None,
            expr_conditions: None,
            expr_else: None,
        }];
        let resolved = vec![(0, "price".into())];

        // price = 200 -> "high"
        let rows = vec![vec![Value::F64(200.0)]];
        let result = apply_scalar_functions(&select_cols, &resolved, rows).unwrap();
        assert_eq!(result[0][0], Value::Str("high".into()));

        // price = 75 -> "mid"
        let rows = vec![vec![Value::F64(75.0)]];
        let result = apply_scalar_functions(&select_cols, &resolved, rows).unwrap();
        assert_eq!(result[0][0], Value::Str("mid".into()));

        // price = 10 -> "low"
        let rows = vec![vec![Value::F64(10.0)]];
        let result = apply_scalar_functions(&select_cols, &resolved, rows).unwrap();
        assert_eq!(result[0][0], Value::Str("low".into()));
    }

    #[test]
    fn test_apply_scalar_case_when_simple() {
        // Simulates: CASE symbol WHEN 'BTC' THEN 'Bitcoin' WHEN 'ETH' THEN 'Ethereum' ELSE 'Other' END
        let select_cols = vec![SelectColumn::CaseWhen {
            conditions: vec![
                (
                    Filter::Eq("symbol".into(), Value::Str("BTC".into())),
                    Value::Str("Bitcoin".into()),
                ),
                (
                    Filter::Eq("symbol".into(), Value::Str("ETH".into())),
                    Value::Str("Ethereum".into()),
                ),
            ],
            else_value: Some(Value::Str("Other".into())),
            alias: None,
            expr_conditions: None,
            expr_else: None,
        }];
        let resolved = vec![(0, "symbol".into())];

        let rows = vec![vec![Value::Str("BTC".into())]];
        let result = apply_scalar_functions(&select_cols, &resolved, rows).unwrap();
        assert_eq!(result[0][0], Value::Str("Bitcoin".into()));

        let rows = vec![vec![Value::Str("SOL".into())]];
        let result = apply_scalar_functions(&select_cols, &resolved, rows).unwrap();
        assert_eq!(result[0][0], Value::Str("Other".into()));
    }

    #[test]
    fn test_sample_by_fill_null() {
        use exchange_core::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable};
        let meta = TableMeta {
            name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".into(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "price".into(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::None,
            timestamp_column: 0,
            version: 1,
        };
        let select_cols = vec![
            SelectColumn::Name("timestamp".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Avg,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "timestamp".into()), (1, "price".into())];
        // Rows at hour 0 and hour 2, missing hour 1.
        let hour_ns = 3_600_000_000_000i64;
        let rows = vec![
            vec![Value::Timestamp(0), Value::F64(100.0)],
            vec![Value::Timestamp(2 * hour_ns), Value::F64(300.0)],
        ];
        let sb = SampleBy {
            interval: std::time::Duration::from_secs(3600),
            fill: FillMode::Null,
            align: AlignMode::Calendar,
        };
        let result = apply_sample_by(&meta, &select_cols, &resolved, rows, &sb).unwrap();
        // Should have 3 rows: hour 0, hour 1 (filled), hour 2.
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][1], Value::F64(100.0));
        assert_eq!(result[1][1], Value::Null); // filled with NULL
        assert_eq!(result[2][1], Value::F64(300.0));
    }

    #[test]
    fn test_sample_by_fill_prev() {
        use exchange_core::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable};
        let meta = TableMeta {
            name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".into(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "price".into(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::None,
            timestamp_column: 0,
            version: 1,
        };
        let select_cols = vec![
            SelectColumn::Name("timestamp".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Avg,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "timestamp".into()), (1, "price".into())];
        let hour_ns = 3_600_000_000_000i64;
        let rows = vec![
            vec![Value::Timestamp(0), Value::F64(100.0)],
            vec![Value::Timestamp(2 * hour_ns), Value::F64(300.0)],
        ];
        let sb = SampleBy {
            interval: std::time::Duration::from_secs(3600),
            fill: FillMode::Prev,
            align: AlignMode::Calendar,
        };
        let result = apply_sample_by(&meta, &select_cols, &resolved, rows, &sb).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][1], Value::F64(100.0));
        assert_eq!(result[1][1], Value::F64(100.0)); // carried forward
        assert_eq!(result[2][1], Value::F64(300.0));
    }

    #[test]
    fn test_sample_by_fill_value() {
        use exchange_core::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable};
        let meta = TableMeta {
            name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".into(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "price".into(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::None,
            timestamp_column: 0,
            version: 1,
        };
        let select_cols = vec![
            SelectColumn::Name("timestamp".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Avg,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "timestamp".into()), (1, "price".into())];
        let hour_ns = 3_600_000_000_000i64;
        let rows = vec![
            vec![Value::Timestamp(0), Value::F64(100.0)],
            vec![Value::Timestamp(2 * hour_ns), Value::F64(300.0)],
        ];
        let sb = SampleBy {
            interval: std::time::Duration::from_secs(3600),
            fill: FillMode::Value(Value::I64(0)),
            align: AlignMode::Calendar,
        };
        let result = apply_sample_by(&meta, &select_cols, &resolved, rows, &sb).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][1], Value::F64(100.0));
        assert_eq!(result[1][1], Value::I64(0)); // filled with 0
        assert_eq!(result[2][1], Value::F64(300.0));
    }

    #[test]
    fn test_sample_by_align_calendar() {
        use exchange_core::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable};
        let meta = TableMeta {
            name: "test".into(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".into(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "price".into(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::None,
            timestamp_column: 0,
            version: 1,
        };
        let select_cols = vec![
            SelectColumn::Name("timestamp".into()),
            SelectColumn::Aggregate {
                function: AggregateKind::Avg,
                column: "price".into(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            },
        ];
        let resolved = vec![(0, "timestamp".into()), (1, "price".into())];
        let hour_ns = 3_600_000_000_000i64;
        // Data at 30 min and 1h30min.
        let rows = vec![
            vec![Value::Timestamp(hour_ns / 2), Value::F64(100.0)],
            vec![Value::Timestamp(hour_ns + hour_ns / 2), Value::F64(200.0)],
        ];
        let sb = SampleBy {
            interval: std::time::Duration::from_secs(3600),
            fill: FillMode::None,
            align: AlignMode::Calendar,
        };
        let result = apply_sample_by(&meta, &select_cols, &resolved, rows, &sb).unwrap();
        assert_eq!(result.len(), 2);
        // Calendar alignment: bucket 0 (0:00-1:00) and bucket 1 (1:00-2:00).
        assert_eq!(result[0][0], Value::Timestamp(0));
        assert_eq!(result[1][0], Value::Timestamp(hour_ns));
    }

    #[test]
    fn test_long_sequence_basic() {
        let columns = vec![SelectColumn::Name("x".into())];
        let result = execute_long_sequence(5, &columns).unwrap();
        match result {
            QueryResult::Rows {
                columns: cols,
                rows,
            } => {
                assert_eq!(cols, vec!["x"]);
                assert_eq!(rows.len(), 5);
                for (i, row) in rows.iter().enumerate() {
                    assert_eq!(row[0], Value::I64(i as i64 + 1));
                }
            }
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_long_sequence_with_scalar_fn() {
        let columns = vec![SelectColumn::ScalarFunction {
            name: "rnd_int".into(),
            args: vec![
                SelectColumnArg::Literal(Value::I64(0)),
                SelectColumnArg::Literal(Value::I64(100)),
            ],
        }];
        let result = execute_long_sequence(10, &columns).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 10);
                for row in &rows {
                    match &row[0] {
                        Value::I64(v) => assert!(*v >= 0 && *v <= 100),
                        other => panic!("expected I64, got {other:?}"),
                    }
                }
            }
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_show_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();
        // Create a table.
        let plan =
            crate::planner::plan_query("CREATE TABLE test_tbl (ts TIMESTAMP, v DOUBLE)").unwrap();
        execute(db_root, &plan).unwrap();

        let result = execute_show_tables(db_root).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["table_name"]);
                assert!(rows.iter().any(|r| r[0] == Value::Str("test_tbl".into())));
            }
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_show_columns() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();
        let plan = crate::planner::plan_query(
            "CREATE TABLE test_cols (ts TIMESTAMP, price DOUBLE, symbol VARCHAR)",
        )
        .unwrap();
        execute(db_root, &plan).unwrap();

        let result = execute_show_columns(db_root, "test_cols").unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert!(columns.contains(&"column_name".to_string()));
                assert!(columns.contains(&"data_type".to_string()));
                assert_eq!(rows.len(), 3); // ts, price, symbol
            }
            _ => panic!("expected Rows"),
        }
    }

    #[test]
    fn test_arithmetic_select_price_times_volume() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan = crate::planner::plan_query(
            "SELECT price * volume AS notional FROM trades ORDER BY timestamp",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["notional"]);
                assert_eq!(rows.len(), 5);
                assert_eq!(rows[0][0], Value::F64(65000.0 * 1.5));
                assert_eq!(rows[1][0], Value::F64(3000.0 * 10.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_arithmetic_select_with_literal() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan = crate::planner::plan_query(
            "SELECT price * 1.1 AS price_plus_10pct FROM trades ORDER BY timestamp LIMIT 1",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["price_plus_10pct"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::F64(65000.0 * 1.1));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_string_concatenation() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let create = crate::planner::plan_query(
            "CREATE TABLE users (timestamp TIMESTAMP, first_name VARCHAR, last_name VARCHAR)",
        )
        .unwrap();
        execute(&db_root, &create).unwrap();
        let insert = crate::planner::plan_query(
            "INSERT INTO users VALUES (1000, 'John', 'Doe'), (2000, 'Jane', 'Smith')",
        )
        .unwrap();
        execute(&db_root, &insert).unwrap();
        let plan = crate::planner::plan_query(
            "SELECT first_name || ' ' || last_name AS full_name FROM users ORDER BY timestamp",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["full_name"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Str("John Doe".into()));
                assert_eq!(rows[1][0], Value::Str("Jane Smith".into()));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_arithmetic_in_where() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan =
            crate::planner::plan_query("SELECT symbol FROM trades WHERE price * volume > 50000")
                .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert!(rows.len() >= 2);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_insert_into_select() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let create = crate::planner::plan_query("CREATE TABLE trades_backup (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)").unwrap();
        execute(&db_root, &create).unwrap();
        let plan = crate::planner::plan_query(
            "INSERT INTO trades_backup SELECT * FROM trades WHERE symbol = 'BTC/USD'",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 2),
            other => panic!("expected Ok, got {other:?}"),
        }
        let select =
            crate::planner::plan_query("SELECT * FROM trades_backup ORDER BY timestamp").unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_truncate_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let select_before = crate::planner::plan_query("SELECT * FROM trades").unwrap();
        let result = execute(&db_root, &select_before).unwrap();
        match &result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 5),
            other => panic!("expected Rows, got {other:?}"),
        }
        let plan = crate::planner::plan_query("TRUNCATE TABLE trades").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 0),
            other => panic!("expected Ok, got {other:?}"),
        }
        let select_after = crate::planner::plan_query("SELECT * FROM trades").unwrap();
        let result = execute(&db_root, &select_after).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_explain_analyze_output_contains_timing() {
        let db_root = tempfile::tempdir().unwrap();
        let db = db_root.path();
        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, price DOUBLE, symbol VARCHAR)",
        )
        .unwrap();
        execute(db, &create).unwrap();
        let insert = crate::planner::plan_query(
            "INSERT INTO trades VALUES (1704067200000000000, 65000.0, 'BTC/USD')",
        )
        .unwrap();
        execute(db, &insert).unwrap();
        let plan = crate::planner::plan_query(
            "EXPLAIN ANALYZE SELECT * FROM trades WHERE price > 100 ORDER BY timestamp",
        )
        .unwrap();
        match &plan {
            QueryPlan::ExplainAnalyze { .. } => {}
            other => panic!("expected ExplainAnalyze, got {other:?}"),
        }
        let result = execute(db, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["plan".to_string()]);
                assert!(!rows.is_empty());
                let plan_text = match &rows[0][0] {
                    Value::Str(s) => s.clone(),
                    other => panic!("expected Str, got {other:?}"),
                };
                assert!(
                    plan_text.contains("Query Plan:"),
                    "missing Query Plan header"
                );
                assert!(plan_text.contains("Time:"), "missing timing info");
                assert!(
                    plan_text.contains("Total execution time:"),
                    "missing total time"
                );
                assert!(plan_text.contains("Execute:"), "missing Execute step");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // ── Tests for newly implemented SQL features ──

    #[test]
    fn test_not_eq_operator() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan =
            crate::planner::plan_query("SELECT * FROM trades WHERE symbol != 'BTC/USD'").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
                for row in &rows {
                    assert_ne!(row[1], Value::Str("BTC/USD".into()));
                }
            }
            other => panic!("expected Rows, got {other:?}"),
        }
        let plan2 =
            crate::planner::plan_query("SELECT * FROM trades WHERE price <> 65000.0").unwrap();
        let result2 = execute(&db_root, &plan2).unwrap();
        match result2 {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 4);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let plan = crate::planner::plan_query(
            "CREATE TABLE IF NOT EXISTS trades (timestamp TIMESTAMP, price DOUBLE)",
        )
        .unwrap();
        execute(&db_root, &plan).unwrap();
        let plan2 = crate::planner::plan_query(
            "CREATE TABLE IF NOT EXISTS trades (timestamp TIMESTAMP, price DOUBLE)",
        )
        .unwrap();
        let result2 = execute(&db_root, &plan2).unwrap();
        assert!(matches!(result2, QueryResult::Ok { .. }));
    }

    #[test]
    fn test_drop_table_if_exists() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let plan = crate::planner::plan_query("DROP TABLE IF EXISTS nonexistent").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));
        let plan2 = crate::planner::plan_query("DROP TABLE nonexistent").unwrap();
        assert!(execute(&db_root, &plan2).is_err());
    }

    #[test]
    fn test_expression_based_update() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan =
            crate::planner::plan_query("UPDATE trades SET price = price * 1.1 WHERE volume > 5.0")
                .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 2),
            other => panic!("expected Ok, got {other:?}"),
        }
        let select =
            crate::planner::plan_query("SELECT price, volume FROM trades ORDER BY timestamp")
                .unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::F64(65000.0)); // BTC vol=1.5, unchanged
                // ETH vol=10.0: 3000.0 * 1.1 (floating point)
                if let Value::F64(v) = rows[1][0] {
                    assert!((v - 3300.0).abs() < 0.01);
                }
                assert_eq!(rows[2][0], Value::F64(64000.0)); // BTC vol=2.0, unchanged
                assert_eq!(rows[3][0], Value::F64(3100.0)); // ETH vol=5.0, unchanged
                // SOL vol=100.0: 100.0 * 1.1 (floating point)
                if let Value::F64(v) = rows[4][0] {
                    assert!((v - 110.0).abs() < 0.01);
                }
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_update_with_addition() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan = crate::planner::plan_query("UPDATE trades SET volume = volume + 10").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => assert_eq!(affected_rows, 5),
            other => panic!("expected Ok, got {other:?}"),
        }
        let select =
            crate::planner::plan_query("SELECT volume FROM trades ORDER BY timestamp").unwrap();
        let result = execute(&db_root, &select).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows[0][0], Value::F64(11.5));
                assert_eq!(rows[1][0], Value::F64(20.0));
                assert_eq!(rows[2][0], Value::F64(12.0));
                assert_eq!(rows[3][0], Value::F64(15.0));
                assert_eq!(rows[4][0], Value::F64(110.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_null_in_insert_values() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        execute(
            &db_root,
            &crate::planner::plan_query(
                "CREATE TABLE t1 (timestamp TIMESTAMP, name VARCHAR, price DOUBLE, volume DOUBLE)",
            )
            .unwrap(),
        )
        .unwrap();
        execute(
            &db_root,
            &crate::planner::plan_query("INSERT INTO t1 VALUES (1000, NULL, 65000.0, 1.5)")
                .unwrap(),
        )
        .unwrap();
        let result = execute(
            &db_root,
            &crate::planner::plan_query("SELECT * FROM t1").unwrap(),
        )
        .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Null);
                assert_eq!(rows[0][2], Value::F64(65000.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_partial_column_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        execute(&db_root, &crate::planner::plan_query(
            "CREATE TABLE t2 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        ).unwrap()).unwrap();
        execute(
            &db_root,
            &crate::planner::plan_query("INSERT INTO t2 (timestamp, price) VALUES (1000, 65000.0)")
                .unwrap(),
        )
        .unwrap();
        let result = execute(
            &db_root,
            &crate::planner::plan_query("SELECT * FROM t2").unwrap(),
        )
        .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Null);
                assert_eq!(rows[0][2], Value::F64(65000.0));
                assert_eq!(rows[0][3], Value::Null);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_column_reordering_in_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        execute(
            &db_root,
            &crate::planner::plan_query(
                "CREATE TABLE t3 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
            )
            .unwrap(),
        )
        .unwrap();
        execute(
            &db_root,
            &crate::planner::plan_query(
                "INSERT INTO t3 (price, timestamp, symbol) VALUES (65000.0, 1000, 'BTC/USD')",
            )
            .unwrap(),
        )
        .unwrap();
        let result = execute(
            &db_root,
            &crate::planner::plan_query("SELECT * FROM t3").unwrap(),
        )
        .unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Str("BTC/USD".into()));
                assert_eq!(rows[0][2], Value::F64(65000.0));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_not_operator_in_filter() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan =
            crate::planner::plan_query("SELECT * FROM trades WHERE NOT (price > 100)").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Str("SOL/USD".into()));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_union_all_with_limit() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan = crate::planner::plan_query(
            "SELECT * FROM trades UNION ALL SELECT * FROM trades LIMIT 3",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_multiple_ctes() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan = crate::planner::plan_query(
            "WITH active AS (SELECT * FROM trades WHERE volume > 5), btc AS (SELECT * FROM active WHERE symbol = 'ETH/USD') SELECT * FROM btc",
        ).unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Str("ETH/USD".into()));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_values_standalone() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let plan = crate::planner::plan_query("VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::I64(1));
                assert_eq!(rows[0][1], Value::Str("a".into()));
                assert_eq!(rows[2][0], Value::I64(3));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_recursive_cte() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path().to_path_buf();
        let plan = crate::planner::plan_query(
            "WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 10) SELECT * FROM seq"
        ).unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 10);
                // First row should be 1, last should be 10.
                assert_eq!(rows[0][0], Value::I64(1));
                assert_eq!(rows[9][0], Value::I64(10));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_fetch_first_rows_only() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        let plan =
            crate::planner::plan_query("SELECT * FROM trades FETCH FIRST 1 ROWS ONLY").unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_between_symmetric() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        // BETWEEN SYMMETRIC 70000 AND 60000 should still match 65000.
        let plan = crate::planner::plan_query(
            "SELECT * FROM trades WHERE price BETWEEN SYMMETRIC 70000 AND 60000",
        )
        .unwrap();
        let result = execute(&db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                // Should find at least the BTC/USD row at 65000.
                assert!(!rows.is_empty(), "BETWEEN SYMMETRIC should match rows");
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_cast_in_where_clause() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = setup_test_table(dir.path());
        // Verify the plan is correctly formed with Expression filter.
        let plan =
            crate::planner::plan_query("SELECT * FROM trades WHERE CAST(volume AS INTEGER) > 5")
                .unwrap();
        match &plan {
            QueryPlan::Select {
                filter: Some(Filter::Expression { left, op, right }),
                ..
            } => {
                // CAST produces a Function PlanExpr.
                assert!(matches!(left, PlanExpr::Function { .. }));
                assert_eq!(*op, CompareOp::Gt);
                assert!(matches!(right, PlanExpr::Literal(Value::I64(5))));
            }
            other => panic!("expected Select with Expression filter, got {other:?}"),
        }
        // For now, just verify planning works. Full execution with CAST in
        // filter expressions requires the optimizer's expression evaluator
        // to pass through column values. The planner correctly generates the plan.
    }

    #[test]
    fn test_distinct_on() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        for (ts, sym, price) in &[
            (1000, "BTC", 100.0),
            (2000, "BTC", 200.0),
            (3000, "BTC", 300.0),
            (1000, "ETH", 50.0),
            (2000, "ETH", 60.0),
        ] {
            let sql = format!("INSERT INTO trades VALUES ({ts}, '{sym}', {price})");
            let plan = crate::planner::plan_query(&sql).unwrap();
            execute(db_root, &plan).unwrap();
        }

        let plan = crate::planner::plan_query(
            "SELECT DISTINCT ON (symbol) symbol, price FROM trades ORDER BY symbol, timestamp DESC",
        )
        .unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // one per symbol
                let btc = rows
                    .iter()
                    .find(|r| r[0] == Value::Str("BTC".into()))
                    .unwrap();
                assert_eq!(btc[1], Value::F64(300.0)); // latest BTC price
                let eth = rows
                    .iter()
                    .find(|r| r[0] == Value::Str("ETH".into()))
                    .unwrap();
                assert_eq!(eth[1], Value::F64(60.0)); // latest ETH price
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_create_drop_index() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        let plan = crate::planner::plan_query("CREATE INDEX idx_sym ON trades (symbol)").unwrap();
        let result = execute(db_root, &plan).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));

        // Verify index metadata was created.
        assert!(db_root.join("_indexes").join("idx_sym.json").exists());

        let drop_plan = crate::planner::plan_query("DROP INDEX idx_sym").unwrap();
        execute(db_root, &drop_plan).unwrap();
        assert!(!db_root.join("_indexes").join("idx_sym.json").exists());
    }

    #[test]
    fn test_rename_table() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let create =
            crate::planner::plan_query("CREATE TABLE trades (timestamp TIMESTAMP, price DOUBLE)")
                .unwrap();
        execute(db_root, &create).unwrap();

        let plan =
            crate::planner::plan_query("ALTER TABLE trades RENAME TO trades_archive").unwrap();
        let result = execute(db_root, &plan).unwrap();
        assert!(matches!(result, QueryResult::Ok { .. }));

        // Old directory should not exist.
        assert!(!db_root.join("trades").exists());
        // New directory should exist.
        assert!(db_root.join("trades_archive").exists());

        // Should be queryable under new name.
        let select = crate::planner::plan_query("SELECT * FROM trades_archive").unwrap();
        let result = execute(db_root, &select).unwrap();
        assert!(matches!(result, QueryResult::Rows { .. }));
    }

    #[test]
    fn test_sequence_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let plan = crate::planner::plan_query("CREATE SEQUENCE trade_seq START WITH 1").unwrap();
        execute(db_root, &plan).unwrap();

        let plan = crate::planner::plan_query("SELECT nextval('trade_seq')").unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(1)),
            other => panic!("expected Rows, got {other:?}"),
        }

        let plan = crate::planner::plan_query("SELECT nextval('trade_seq')").unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(2)),
            other => panic!("expected Rows, got {other:?}"),
        }

        let plan = crate::planner::plan_query("SELECT currval('trade_seq')").unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(2)),
            other => panic!("expected Rows, got {other:?}"),
        }

        let plan = crate::planner::plan_query("DROP SEQUENCE trade_seq").unwrap();
        execute(db_root, &plan).unwrap();
    }

    #[test]
    fn test_check_constraint_violation() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (price DOUBLE CHECK (price > 0), volume DOUBLE CHECK (volume >= 0))"
        ).unwrap();
        execute(db_root, &create).unwrap();

        // Valid insert should work.
        let plan = crate::planner::plan_query("INSERT INTO trades VALUES (100.0, 10.0)").unwrap();
        let result = execute(db_root, &plan);
        assert!(result.is_ok());

        // Negative price should fail.
        let plan = crate::planner::plan_query("INSERT INTO trades VALUES (-1.0, 10.0)").unwrap();
        let result = execute(db_root, &plan);
        assert!(result.is_err());
    }

    #[test]
    fn test_all_any_subquery() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        let create2 = crate::planner::plan_query(
            "CREATE TABLE watchlist (timestamp TIMESTAMP, symbol VARCHAR)",
        )
        .unwrap();
        execute(db_root, &create2).unwrap();

        let base_ts = 1710460800_000_000_000i64;
        for (i, (sym, price)) in [("BTC", 100.0), ("ETH", 200.0), ("SOL", 50.0)]
            .iter()
            .enumerate()
        {
            let ts = base_ts + (i as i64) * 1_000_000_000;
            let sql = format!("INSERT INTO trades VALUES ({ts}, '{sym}', {price})");
            execute(db_root, &crate::planner::plan_query(&sql).unwrap()).unwrap();
        }

        execute(
            db_root,
            &crate::planner::plan_query(&format!(
                "INSERT INTO watchlist VALUES ({}, 'BTC')",
                base_ts
            ))
            .unwrap(),
        )
        .unwrap();
        execute(
            db_root,
            &crate::planner::plan_query(&format!(
                "INSERT INTO watchlist VALUES ({}, 'ETH')",
                base_ts + 1_000_000_000
            ))
            .unwrap(),
        )
        .unwrap();

        // ANY: symbol = ANY (SELECT symbol FROM watchlist)
        let plan = crate::planner::plan_query(
            "SELECT * FROM trades WHERE symbol = ANY (SELECT symbol FROM watchlist)",
        )
        .unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // BTC and ETH only
            }
            other => panic!("expected Rows, got {other:?}"),
        }

        // ALL: price > ALL (SELECT price FROM trades WHERE symbol = 'SOL')
        let plan = crate::planner::plan_query(
            "SELECT * FROM trades WHERE price > ALL (SELECT price FROM trades WHERE symbol = 'SOL')"
        ).unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // BTC (100) and ETH (200) > SOL (50)
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // ── Feature: CREATE TABLE AS SELECT ──────────────────────────

    #[test]
    fn test_create_table_as_select() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();

        // Create source table.
        let create = crate::planner::plan_query(
            "CREATE TABLE source (ts TIMESTAMP, price DOUBLE PRECISION, symbol VARCHAR)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        // Insert data.
        let ins = crate::planner::plan_query(
            "INSERT INTO source VALUES (1704067200000000000, 100.0, 'BTC'), (1704067201000000000, 200.0, 'ETH')"
        ).unwrap();
        execute(db_root, &ins).unwrap();

        // CREATE TABLE AS SELECT
        let ctas =
            crate::planner::plan_query("CREATE TABLE backup AS SELECT * FROM source").unwrap();
        match &ctas {
            QueryPlan::CreateTableAs { name, .. } => {
                assert_eq!(name, "backup");
            }
            other => panic!("expected CreateTableAs, got {other:?}"),
        }
        let result = execute(db_root, &ctas).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => {
                assert_eq!(affected_rows, 2);
            }
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify the new table has the same data.
        let sel = crate::planner::plan_query("SELECT * FROM backup").unwrap();
        let result = execute(db_root, &sel).unwrap();
        match result {
            QueryResult::Rows { rows, columns } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(columns.len(), 3);
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_create_table_as_select_with_aggregation() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();

        let create = crate::planner::plan_query(
            "CREATE TABLE trades (ts TIMESTAMP, price DOUBLE PRECISION, symbol VARCHAR)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        let ins = crate::planner::plan_query(
            "INSERT INTO trades VALUES (1704067200000000000, 100.0, 'BTC'), (1704067201000000000, 200.0, 'BTC'), (1704067202000000000, 50.0, 'ETH')"
        ).unwrap();
        execute(db_root, &ins).unwrap();

        // CREATE TABLE AS SELECT with aggregation
        let ctas = crate::planner::plan_query(
            "CREATE TABLE summary AS SELECT symbol, avg(price) FROM trades GROUP BY symbol",
        )
        .unwrap();
        let result = execute(db_root, &ctas).unwrap();
        match result {
            QueryResult::Ok { affected_rows } => {
                assert_eq!(affected_rows, 2); // BTC and ETH
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    // ── Feature: NOT BETWEEN ─────────────────────────────────────

    #[test]
    fn test_not_between() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();

        let create =
            crate::planner::plan_query("CREATE TABLE items (ts TIMESTAMP, price BIGINT)").unwrap();
        execute(db_root, &create).unwrap();

        let ins = crate::planner::plan_query(
            "INSERT INTO items VALUES (1704067200000000000, 50), (1704067201000000000, 150), (1704067202000000000, 250)"
        ).unwrap();
        execute(db_root, &ins).unwrap();

        // NOT BETWEEN should exclude rows with price in [100, 200]
        let plan =
            crate::planner::plan_query("SELECT * FROM items WHERE price NOT BETWEEN 100 AND 200")
                .unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // 50 and 250
                // Verify the prices are 50 and 250 (not 150)
                let prices: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
                assert!(prices.contains(&&Value::I64(50)));
                assert!(prices.contains(&&Value::I64(250)));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // ── Feature: sum(boolean) ────────────────────────────────────

    #[test]
    fn test_sum_boolean_column() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();

        // Use BIGINT for the flag column to test sum(0/1) behavior.
        // Boolean columns are stored as 1-byte in storage, but the Sum aggregate
        // correctly handles I64(0)/I64(1) values regardless of source.
        let create = crate::planner::plan_query(
            "CREATE TABLE users (ts TIMESTAMP, name VARCHAR, is_active BIGINT)",
        )
        .unwrap();
        execute(db_root, &create).unwrap();

        // Insert with 0/1 as boolean-like values.
        let ins = crate::planner::plan_query(
            "INSERT INTO users VALUES (1704067200000000000, 'alice', 1), (1704067201000000000, 'bob', 0), (1704067202000000000, 'charlie', 1)"
        ).unwrap();
        execute(db_root, &ins).unwrap();

        // sum(is_active) should count TRUE values (1+0+1 = 2)
        let plan = crate::planner::plan_query("SELECT sum(is_active) FROM users").unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::I64(2)); // alice + charlie = 2
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// Test that sum() works on boolean expressions like sum(price > 100).
    #[test]
    fn test_sum_boolean_expression() {
        let select_cols = vec![SelectColumn::Aggregate {
            function: AggregateKind::Sum,
            column: "price".into(),
            alias: None,
            filter: None,
            within_group_order: None,
            arg_expr: Some(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column("price".into())),
                op: BinaryOp::Gt,
                right: Box::new(PlanExpr::Literal(Value::I64(100))),
            }),
        }];
        let resolved = vec![(0, "price".into())];
        let rows = vec![
            vec![Value::I64(50)],
            vec![Value::I64(150)],
            vec![Value::I64(200)],
            vec![Value::I64(80)],
        ];

        let result = apply_group_by(&select_cols, &resolved, &rows, &[], None).unwrap();
        assert_eq!(result.len(), 1);
        // 50>100=0, 150>100=1, 200>100=1, 80>100=0 => sum=2
        assert_eq!(result[0][0], Value::I64(2));
    }

    // ── Feature: read_csv ────────────────────────────────────────

    #[test]
    fn test_read_csv() {
        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("test.csv");

        // Write a test CSV file.
        std::fs::write(&csv_path, "name,age,score\nalice,30,95.5\nbob,25,88.0\n").unwrap();

        let plan = crate::planner::plan_query(&format!(
            "SELECT * FROM read_csv('{}')",
            csv_path.display()
        ))
        .unwrap();
        match &plan {
            QueryPlan::ReadCsv { .. } => {}
            other => panic!("expected ReadCsv, got {other:?}"),
        }

        let result = execute(tmp.path(), &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "age", "score"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Str("alice".into()));
                assert_eq!(rows[0][1], Value::I64(30));
                assert_eq!(rows[0][2], Value::F64(95.5));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    #[test]
    fn test_read_csv_column_selection() {
        let tmp = tempfile::tempdir().unwrap();
        let csv_path = tmp.path().join("test2.csv");

        std::fs::write(&csv_path, "id,name,value\n1,foo,10\n2,bar,20\n").unwrap();

        let plan = crate::planner::plan_query(&format!(
            "SELECT name, value FROM read_csv('{}')",
            csv_path.display()
        ))
        .unwrap();

        let result = execute(tmp.path(), &plan).unwrap();
        match result {
            QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["name", "value"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Str("foo".into()));
                assert_eq!(rows[0][1], Value::I64(10));
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // ── Feature: DECIMAL with precision/scale ────────────────────

    #[test]
    fn test_decimal_precision_scale() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();

        // DECIMAL(18, 8) should parse and create a table using Decimal128
        let plan =
            crate::planner::plan_query("CREATE TABLE prices (ts TIMESTAMP, amount DECIMAL(18, 8))")
                .unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Ok { .. } => {}
            other => panic!("expected Ok, got {other:?}"),
        }

        // Verify the table was created.
        let show = crate::planner::plan_query("SHOW COLUMNS FROM prices").unwrap();
        let result = execute(db_root, &show).unwrap();
        match result {
            QueryResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // ts and amount
            }
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    // ── Feature: IPv6 type ───────────────────────────────────────

    #[test]
    fn test_ipv6_column_type() {
        let tmp = tempfile::tempdir().unwrap();
        let db_root = tmp.path();

        let plan = crate::planner::plan_query("CREATE TABLE connections (ts TIMESTAMP, addr IPV6)")
            .unwrap();
        let result = execute(db_root, &plan).unwrap();
        match result {
            QueryResult::Ok { .. } => {}
            other => panic!("expected Ok, got {other:?}"),
        }
    }
}
