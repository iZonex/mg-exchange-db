//! Converts parsed SQL (sqlparser AST) into our internal `QueryPlan`.

use crate::parser::{parse_duration, parse_sql, AsofJoinInfo, LatestOnInfo, MergeInfo, PartitionCommand, PivotInfo, RbacCommand, ShowCommand};
use crate::plan::*;
use exchange_common::error::{ExchangeDbError, Result};
use sqlparser::ast::{self, AssignmentTarget, CopyTarget, CopyOption, Expr, FromTable, JoinConstraint, JoinOperator, SelectItem, SetExpr, SetOperator, Statement, TableFactor};
use std::path::PathBuf;

/// Plan a SQL string into a `QueryPlan`.
pub fn plan_query(sql: &str) -> Result<QueryPlan> {
    let parsed = parse_sql(sql)?;

    // Handle RBAC commands (pre-processed by parser).
    if let Some(rbac) = parsed.rbac_command {
        return match rbac {
            RbacCommand::CreateUser { username, password } => {
                Ok(QueryPlan::CreateUser { username, password })
            }
            RbacCommand::DropUser { username } => Ok(QueryPlan::DropUser { username }),
            RbacCommand::CreateRole { name } => Ok(QueryPlan::CreateRole { name }),
            RbacCommand::DropRole { name } => Ok(QueryPlan::DropRole { name }),
            RbacCommand::Grant { permission, target } => {
                Ok(QueryPlan::Grant { permission, target })
            }
            RbacCommand::Revoke { permission, target } => {
                Ok(QueryPlan::Revoke { permission, target })
            }
        };
    }

    // Handle partition commands (pre-processed by parser).
    if let Some(cmd) = parsed.partition_command {
        return match cmd {
            PartitionCommand::Detach { table, partition } => {
                Ok(QueryPlan::DetachPartition { table, partition })
            }
            PartitionCommand::Attach { table, partition } => {
                Ok(QueryPlan::AttachPartition { table, partition })
            }
            PartitionCommand::Squash { table, partition1, partition2 } => {
                Ok(QueryPlan::SquashPartitions { table, partition1, partition2 })
            }
        };
    }

    // Handle VACUUM (pre-processed by parser, no AST statements).
    if let Some(table) = parsed.vacuum_table {
        return Ok(QueryPlan::Vacuum { table });
    }

    // Handle CREATE MATERIALIZED VIEW (pre-processed by parser).
    if let Some((name, source_sql)) = parsed.create_matview {
        return Ok(QueryPlan::CreateMatView { name, source_sql });
    }

    // Handle REFRESH MATERIALIZED VIEW (pre-processed by parser).
    if let Some(name) = parsed.refresh_matview {
        return Ok(QueryPlan::RefreshMatView { name });
    }

    // Handle DROP MATERIALIZED VIEW (pre-processed by parser).
    if let Some(name) = parsed.drop_matview {
        return Ok(QueryPlan::DropMatView { name });
    }

    // Handle SHOW commands (pre-processed by parser).
    if let Some(show) = parsed.show_command {
        return match show {
            ShowCommand::ShowTables => Ok(QueryPlan::ShowTables),
            ShowCommand::ShowColumns { table } => Ok(QueryPlan::ShowColumns { table }),
            ShowCommand::ShowCreateTable { table } => Ok(QueryPlan::ShowCreateTable { table }),
        };
    }

    // Handle MERGE (pre-processed by parser).
    if let Some(merge) = parsed.merge_command {
        return plan_merge(&merge);
    }

    // Handle stored procedure commands (pre-processed by parser).
    if let Some((name, body)) = parsed.create_procedure {
        return Ok(QueryPlan::CreateProcedure { name, body });
    }
    if let Some(name) = parsed.drop_procedure {
        return Ok(QueryPlan::DropProcedure { name });
    }
    if let Some(name) = parsed.call_procedure {
        return Ok(QueryPlan::CallProcedure { name });
    }

    // Handle CREATE DOWNSAMPLING (pre-processed by parser).
    if let Some(info) = parsed.create_downsampling {
        return Ok(QueryPlan::CreateDownsampling {
            source_table: info.source_table,
            target_name: info.target_name,
            interval_secs: info.interval_secs,
            columns: info.columns,
        });
    }

    // Handle CREATE VIEW / DROP VIEW (pre-processed by parser).
    if let Some((name, sql)) = parsed.create_view {
        return Ok(QueryPlan::CreateView { name, sql });
    }
    if let Some(name) = parsed.drop_view {
        return Ok(QueryPlan::DropView { name });
    }

    // Handle CREATE TRIGGER / DROP TRIGGER (pre-processed by parser).
    if let Some((name, table, procedure)) = parsed.create_trigger {
        return Ok(QueryPlan::CreateTrigger { name, table, procedure });
    }
    if let Some((name, table)) = parsed.drop_trigger {
        return Ok(QueryPlan::DropTrigger { name, table });
    }

    // Handle COMMENT ON (pre-processed by parser).
    if let Some((obj_type, obj_name, table_name, comment)) = parsed.comment_on {
        let object_type = if obj_type == "TABLE" {
            crate::plan::CommentObjectType::Table
        } else {
            crate::plan::CommentObjectType::Column
        };
        return Ok(QueryPlan::CommentOn { object_type, object_name: obj_name, table_name, comment });
    }

    if parsed.statements.len() != 1 {
        return Err(ExchangeDbError::Query(
            "expected exactly one SQL statement".into(),
        ));
    }

    let stmt = &parsed.statements[0];
    match stmt {
        Statement::CreateTable(ct) => plan_create_table(ct, parsed.designated_timestamp.as_deref(), parsed.partition_by_clause.as_deref()),
        Statement::Insert(ins) => plan_insert(ins),
        Statement::Query(q) => {
            if let Some(pivot_info) = parsed.pivot_info {
                return plan_pivot(q, &pivot_info);
            }
            if let Some(asof) = parsed.asof_join {
                plan_asof_join(q, &asof)
            } else {
                let body_plan = plan_select(q, parsed.sample_by_raw.as_deref(), parsed.sample_by_fill.as_deref(), parsed.sample_by_align_calendar, parsed.latest_on.as_ref())?;
                // Wrap with CTEs if present.
                if let Some(with) = &q.with {
                    let is_recursive = with.recursive;
                    let mut ctes = Vec::new();
                    for cte in &with.cte_tables {
                        let cte_name = cte.alias.name.value.clone();
                        let cte_plan = plan_select(&cte.query, None, None, false, None)?;
                        ctes.push(CteDefinition {
                            name: cte_name,
                            query: Box::new(cte_plan),
                            recursive: is_recursive,
                        });
                    }
                    Ok(QueryPlan::WithCte {
                        ctes,
                        body: Box::new(body_plan),
                    })
                } else {
                    Ok(body_plan)
                }
            }
        }
        Statement::AlterTable {
            name,
            if_exists: _,
            only: _,
            operations,
            ..
        } => plan_alter_table(name, operations),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade: _,
            ..
        } => plan_drop(object_type, names, *if_exists),
        Statement::Delete(del) => plan_delete(del),
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => plan_update(table, assignments, selection),
        Statement::Explain { statement, analyze, .. } => {
            let inner_sql = statement.to_string();
            let inner_plan = plan_query(&inner_sql)?;
            if *analyze {
                Ok(QueryPlan::ExplainAnalyze {
                    query: Box::new(inner_plan),
                })
            } else {
                Ok(QueryPlan::Explain {
                    query: Box::new(inner_plan),
                })
            }
        }
        Statement::Copy {
            source,
            to,
            target,
            options,
            ..
        } => plan_copy(source, *to, target, options),
        Statement::StartTransaction { .. } => Ok(QueryPlan::Begin),
        Statement::Commit { .. } => Ok(QueryPlan::Commit),
        Statement::Rollback { .. } => Ok(QueryPlan::Rollback),
        Statement::SetVariable { variables, value, .. } => {
            let name = variables.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(".");
            let val = value.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ");
            Ok(QueryPlan::Set { name, value: val })
        }
        Statement::ShowVariable { variable } => {
            let name = variable.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(".");
            Ok(QueryPlan::Show { name })
        }
        Statement::Truncate { table_names, .. } => {
            if table_names.is_empty() {
                return Err(ExchangeDbError::Query("TRUNCATE requires a table name".into()));
            }
            let table = table_names[0].name.to_string();
            Ok(QueryPlan::TruncateTable { table })
        }
        Statement::CreateIndex(ci) => {
            let name = ci.name.as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unnamed_index".to_string());
            let table = ci.table_name.to_string();
            let columns: Vec<String> = ci.columns.iter().map(|c| {
                c.expr.to_string()
            }).collect();
            Ok(QueryPlan::CreateIndex { name, table, columns })
        }
        Statement::CreateSequence { name, sequence_options, .. } => {
            let seq_name = name.to_string();
            let mut start = 1i64;
            let mut increment = 1i64;
            for opt in sequence_options {
                match opt {
                    ast::SequenceOptions::StartWith(expr, _) => {
                        if let Ok(v) = expr_to_value(expr) {
                            if let Value::I64(n) = v {
                                start = n;
                            }
                        }
                    }
                    ast::SequenceOptions::IncrementBy(expr, _) => {
                        if let Ok(v) = expr_to_value(expr) {
                            if let Value::I64(n) = v {
                                increment = n;
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(QueryPlan::CreateSequence { name: seq_name, start, increment })
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported statement: {other}"
        ))),
    }
}

fn plan_copy(
    source: &ast::CopySource,
    to: bool,
    target: &CopyTarget,
    options: &[CopyOption],
) -> Result<QueryPlan> {
    let table = match source {
        ast::CopySource::Table { table_name, .. } => table_name.to_string(),
        ast::CopySource::Query(_) => {
            return Err(ExchangeDbError::Query(
                "COPY from query is not supported, use a table name".into(),
            ))
        }
    };

    let path = match target {
        CopyTarget::File { filename } => PathBuf::from(filename),
        _ => {
            return Err(ExchangeDbError::Query(
                "COPY target must be a file path".into(),
            ))
        }
    };

    let mut copy_opts = CopyOptions::default();

    for opt in options {
        match opt {
            CopyOption::Header(b) => copy_opts.header = *b,
            CopyOption::Delimiter(c) => copy_opts.delimiter = *c,
            CopyOption::Format(ident) => {
                match ident.value.to_ascii_uppercase().as_str() {
                    "CSV" => copy_opts.format = CopyFormat::Csv,
                    "TSV" => {
                        copy_opts.format = CopyFormat::Tsv;
                        copy_opts.delimiter = '\t';
                    }
                    "PARQUET" => {
                        copy_opts.format = CopyFormat::Parquet;
                    }
                    other => {
                        return Err(ExchangeDbError::Query(format!(
                            "unsupported COPY format: {other}"
                        )))
                    }
                }
            }
            _ => {} // ignore unknown options
        }
    }

    // Auto-detect parquet format from file extension.
    if copy_opts.format != CopyFormat::Parquet {
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            if ext.eq_ignore_ascii_case("parquet") {
                copy_opts.format = CopyFormat::Parquet;
            }
        }
    }

    if to {
        Ok(QueryPlan::CopyTo { table, path, options: copy_opts })
    } else {
        Ok(QueryPlan::CopyFrom { table, path, options: copy_opts })
    }
}

fn plan_create_table(ct: &ast::CreateTable, designated_timestamp: Option<&str>, partition_by_clause: Option<&str>) -> Result<QueryPlan> {
    let name = ct.name.to_string();

    // Handle CREATE TABLE ... AS SELECT
    if let Some(ref query) = ct.query {
        let source_plan = plan_select(query, None, None, false, None)?;
        return Ok(QueryPlan::CreateTableAs {
            name,
            source: Box::new(source_plan),
            partition_by: partition_by_clause.map(|s| s.to_string()),
        });
    }

    let columns: Vec<PlanColumnDef> = ct
        .columns
        .iter()
        .map(|c| {
            // Look for CHECK constraint in column options.
            let check = c.options.iter().find_map(|opt| {
                if let ast::ColumnOption::Check(expr) = &opt.option {
                    sql_expr_to_plan_expr(expr).ok()
                } else {
                    None
                }
            });
            // Look for UNIQUE constraint.
            let unique = c.options.iter().any(|opt| {
                matches!(&opt.option, ast::ColumnOption::Unique { .. })
            });
            // Look for REFERENCES (foreign key) constraint.
            let references = c.options.iter().find_map(|opt| {
                if let ast::ColumnOption::ForeignKey { foreign_table, referred_columns, .. } = &opt.option {
                    let ref_table = foreign_table.to_string();
                    let ref_col = referred_columns.first().map(|c| c.value.clone()).unwrap_or_default();
                    Some((ref_table, ref_col))
                } else {
                    None
                }
            });
            PlanColumnDef {
                name: c.name.value.clone(),
                type_name: c.data_type.to_string().to_ascii_uppercase(),
                check,
                unique,
                references,
            }
        })
        .collect();

    // Use the explicit TIMESTAMP(col) designation if provided; otherwise
    // fall back to auto-detecting the first TIMESTAMP-typed column.
    let timestamp_col = if let Some(ts_col) = designated_timestamp {
        Some(ts_col.to_string())
    } else {
        columns
            .iter()
            .find(|c| c.type_name == "TIMESTAMP")
            .map(|c| c.name.clone())
    };

    Ok(QueryPlan::CreateTable {
        name,
        columns,
        partition_by: partition_by_clause.map(|s| s.to_string()),
        timestamp_col,
        if_not_exists: ct.if_not_exists,
    })
}

fn plan_alter_table(
    name: &ast::ObjectName,
    operations: &[ast::AlterTableOperation],
) -> Result<QueryPlan> {
    let table = name.to_string();

    if operations.len() != 1 {
        return Err(ExchangeDbError::Query(
            "expected exactly one ALTER TABLE operation".into(),
        ));
    }

    match &operations[0] {
        ast::AlterTableOperation::AddColumn { column_def, .. } => {
            let column_name = column_def.name.value.clone();
            let column_type = column_def.data_type.to_string().to_ascii_uppercase();
            Ok(QueryPlan::AddColumn {
                table,
                column_name,
                column_type,
            })
        }
        ast::AlterTableOperation::DropColumn {
            column_name,
            ..
        } => Ok(QueryPlan::DropColumn {
            table,
            column_name: column_name.value.clone(),
        }),
        ast::AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => Ok(QueryPlan::RenameColumn {
            table,
            old_name: old_column_name.value.clone(),
            new_name: new_column_name.value.clone(),
        }),
        ast::AlterTableOperation::AlterColumn {
            column_name,
            op,
        } => {
            match op {
                ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                    Ok(QueryPlan::SetColumnType {
                        table,
                        column_name: column_name.value.clone(),
                        new_type: data_type.to_string().to_ascii_uppercase(),
                    })
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported ALTER COLUMN operation: {other}"
                ))),
            }
        }
        ast::AlterTableOperation::RenameTable { table_name } => {
            Ok(QueryPlan::RenameTable {
                old_name: table,
                new_name: table_name.to_string(),
            })
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported ALTER TABLE operation: {other}"
        ))),
    }
}

fn plan_drop(
    object_type: &ast::ObjectType,
    names: &[ast::ObjectName],
    if_exists: bool,
) -> Result<QueryPlan> {
    match object_type {
        ast::ObjectType::Table => {
            if names.len() != 1 {
                return Err(ExchangeDbError::Query(
                    "DROP TABLE expects exactly one table name".into(),
                ));
            }
            Ok(QueryPlan::DropTable {
                table: names[0].to_string(),
                if_exists,
            })
        }
        ast::ObjectType::Index => {
            if names.len() != 1 {
                return Err(ExchangeDbError::Query(
                    "DROP INDEX expects exactly one index name".into(),
                ));
            }
            Ok(QueryPlan::DropIndex {
                name: names[0].to_string(),
            })
        }
        ast::ObjectType::Sequence => {
            if names.len() != 1 {
                return Err(ExchangeDbError::Query(
                    "DROP SEQUENCE expects exactly one sequence name".into(),
                ));
            }
            Ok(QueryPlan::DropSequence {
                name: names[0].to_string(),
            })
        }
        ast::ObjectType::View => {
            if names.len() != 1 {
                return Err(ExchangeDbError::Query(
                    "DROP VIEW expects exactly one view name".into(),
                ));
            }
            Ok(QueryPlan::DropView {
                name: names[0].to_string(),
            })
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported DROP object type: {other}"
        ))),
    }
}

fn plan_insert(ins: &ast::Insert) -> Result<QueryPlan> {
    let table = ins.table.to_string();

    let columns: Vec<String> = ins
        .columns
        .iter()
        .map(|c| c.value.clone())
        .collect();

    let source = ins
        .source
        .as_ref()
        .ok_or_else(|| ExchangeDbError::Query("INSERT without body".into()))?;

    match source.body.as_ref() {
        SetExpr::Values(vals) => {
            let mut rows = Vec::new();
            for row in &vals.rows {
                let mut row_vals = Vec::new();
                for expr in row {
                    row_vals.push(expr_to_value(expr)?);
                }
                rows.push(row_vals);
            }

            // Check for ON CONFLICT clause (PostgreSQL-style upsert).
            if let Some(ast::OnInsert::OnConflict(on_conflict)) = &ins.on {
                let conflict_cols = match &on_conflict.conflict_target {
                    Some(ast::ConflictTarget::Columns(cols)) => {
                        cols.iter().map(|c| c.value.clone()).collect()
                    }
                    Some(ast::ConflictTarget::OnConstraint(_)) => {
                        return Err(ExchangeDbError::Query(
                            "ON CONFLICT ON CONSTRAINT is not supported".into(),
                        ))
                    }
                    None => Vec::new(),
                };
                let action = match &on_conflict.action {
                    ast::OnConflictAction::DoNothing => OnConflictAction::DoNothing,
                    ast::OnConflictAction::DoUpdate(do_update) => {
                        let mut assignments = Vec::new();
                        for assign in &do_update.assignments {
                            let col_name = match &assign.target {
                                AssignmentTarget::ColumnName(name) => name.to_string(),
                                AssignmentTarget::Tuple(_) => {
                                    return Err(ExchangeDbError::Query(
                                        "tuple assignments not supported in ON CONFLICT".into(),
                                    ))
                                }
                            };
                            let value_expr = sql_expr_to_plan_expr(&assign.value)?;
                            assignments.push((col_name, value_expr));
                        }
                        OnConflictAction::DoUpdate { assignments }
                    }
                };
                return Ok(QueryPlan::InsertOnConflict {
                    table,
                    columns,
                    values: rows,
                    on_conflict: OnConflictClause {
                        columns: conflict_cols,
                        action,
                    },
                });
            }

            // Detect INSERT OR REPLACE (upsert).
            let upsert = matches!(ins.or, Some(ast::SqliteOnConflict::Replace));

            Ok(QueryPlan::Insert {
                table,
                columns,
                values: rows,
                upsert,
            })
        }
        SetExpr::Select(_) | SetExpr::SetOperation { .. } => {
            // INSERT INTO ... SELECT ...
            let select_plan = plan_select(source, None, None, false, None)?;
            Ok(QueryPlan::InsertSelect {
                target_table: table,
                columns,
                source: Box::new(select_plan),
            })
        }
        other => {
            Err(ExchangeDbError::Query(format!(
                "unsupported INSERT source: {other}"
            )))
        }
    }
}

fn plan_delete(del: &ast::Delete) -> Result<QueryPlan> {
    // Extract the table name from the FROM clause.
    let table = match &del.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if tables.is_empty() {
                return Err(ExchangeDbError::Query("DELETE requires a FROM clause".into()));
            }
            match &tables[0].relation {
                TableFactor::Table { name, .. } => name.to_string(),
                other => {
                    return Err(ExchangeDbError::Query(format!(
                        "unsupported DELETE FROM clause: {other}"
                    )))
                }
            }
        }
    };

    let filter = if let Some(selection) = &del.selection {
        Some(expr_to_filter(selection)?)
    } else {
        None
    };

    Ok(QueryPlan::Delete { table, filter })
}

fn plan_update(
    table_with_joins: &ast::TableWithJoins,
    assignments: &[ast::Assignment],
    selection: &Option<Expr>,
) -> Result<QueryPlan> {
    let table = match &table_with_joins.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        other => {
            return Err(ExchangeDbError::Query(format!(
                "unsupported UPDATE table: {other}"
            )))
        }
    };

    let mut plan_assignments = Vec::new();
    for assign in assignments {
        let col_name = match &assign.target {
            AssignmentTarget::ColumnName(name) => name.to_string(),
            AssignmentTarget::Tuple(_) => {
                return Err(ExchangeDbError::Query(
                    "tuple assignments are not supported in UPDATE".into(),
                ))
            }
        };
        let value_expr = sql_expr_to_plan_expr(&assign.value)?;
        plan_assignments.push((col_name, value_expr));
    }

    let filter = if let Some(sel) = selection {
        Some(expr_to_filter(sel)?)
    } else {
        None
    };

    Ok(QueryPlan::Update {
        table,
        assignments: plan_assignments,
        filter,
    })
}

fn plan_asof_join(query: &ast::Query, asof: &AsofJoinInfo) -> Result<QueryPlan> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(ExchangeDbError::Query(
                "only simple SELECT queries are supported for ASOF JOIN".into(),
            ))
        }
    };

    // Table name (left table).
    let left_table = if select.from.is_empty() {
        return Err(ExchangeDbError::Query("ASOF JOIN requires a FROM clause".into()));
    } else {
        match &select.from[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            other => {
                return Err(ExchangeDbError::Query(format!(
                    "unsupported FROM clause: {other}"
                )))
            }
        }
    };

    let right_table = asof.right_table.clone();
    let left_alias = asof.left_alias.as_deref();
    let right_alias = asof.right_alias.as_deref();

    // Partition columns into left and right based on alias prefixes.
    let mut left_columns = Vec::new();
    let mut right_columns = Vec::new();

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                left_columns.push(SelectColumn::Wildcard);
            }
            SelectItem::QualifiedWildcard(name, _) => {
                let prefix = name.to_string();
                if Some(prefix.as_str()) == left_alias || prefix == left_table {
                    left_columns.push(SelectColumn::Wildcard);
                } else if Some(prefix.as_str()) == right_alias || prefix == right_table {
                    right_columns.push(SelectColumn::Wildcard);
                } else {
                    // Default to left.
                    left_columns.push(SelectColumn::Wildcard);
                }
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                match expr {
                    Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                        let prefix = &parts[0].value;
                        let col_name = parts[1].value.clone();
                        if Some(prefix.as_str()) == left_alias || prefix == &left_table {
                            left_columns.push(SelectColumn::Name(col_name));
                        } else if Some(prefix.as_str()) == right_alias || prefix == &right_table {
                            right_columns.push(SelectColumn::Name(col_name));
                        } else {
                            left_columns.push(SelectColumn::Name(col_name));
                        }
                    }
                    _ => {
                        left_columns.push(select_expr_to_column(expr, None)?);
                    }
                }
            }
        }
    }

    // WHERE filter
    let filter = if let Some(selection) = &select.selection {
        Some(expr_to_filter(selection)?)
    } else {
        None
    };

    // ORDER BY
    let order_by = query
        .order_by
        .as_ref()
        .map(|ob| match &ob.kind {
            ast::OrderByKind::Expressions(exprs) => exprs
                .iter()
                .map(|o| {
                    let column = match &o.expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        Expr::CompoundIdentifier(parts) => {
                            parts.last().map(|p| p.value.clone()).unwrap_or_default()
                        }
                        other => other.to_string(),
                    };
                    let descending = o.options.asc == Some(false);
                    Ok(OrderBy { column, descending })
                })
                .collect::<Result<Vec<_>>>(),
            ast::OrderByKind::All(_) => {
                Err(ExchangeDbError::Query("ORDER BY ALL is not supported".into()))
            }
        })
        .transpose()?
        .unwrap_or_default();

    // LIMIT
    let limit = if let Some(limit_expr) = &query.limit {
        match expr_to_value(limit_expr)? {
            Value::I64(n) if n >= 0 => Some(n as u64),
            _ => None,
        }
    } else {
        None
    };

    Ok(QueryPlan::AsofJoin {
        left_table,
        right_table,
        left_columns,
        right_columns,
        on_columns: asof.on_columns.clone(),
        filter,
        order_by,
        limit,
    })
}

fn plan_set_expr(set_expr: &SetExpr) -> Result<QueryPlan> {
    match set_expr {
        SetExpr::SetOperation { left, right, set_quantifier, op } => {
            let set_op = match op {
                SetOperator::Union => SetOp::Union,
                SetOperator::Intersect => SetOp::Intersect,
                SetOperator::Except | SetOperator::Minus => SetOp::Except,
            };
            let all = matches!(set_quantifier, ast::SetQuantifier::All);
            let left_plan = plan_set_expr(left)?;
            let right_plan = plan_set_expr(right)?;
            Ok(QueryPlan::SetOperation {
                op: set_op,
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                all,
                limit: None,
            })
        }
        SetExpr::Select(s) => {
            // Plan a simple SELECT from a SetExpr::Select (without ORDER BY/LIMIT from Query).
            plan_select_inner(s, None, None, false, None, &[], None, None)
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported set expression: {other}"
        ))),
    }
}

fn plan_select(query: &ast::Query, sample_by_raw: Option<&str>, sample_by_fill: Option<&str>, sample_by_align_calendar: bool, latest_on_info: Option<&LatestOnInfo>) -> Result<QueryPlan> {
    // Handle set operations (UNION / INTERSECT / EXCEPT).
    match query.body.as_ref() {
        SetExpr::SetOperation { .. } => {
            let mut plan = plan_set_expr(query.body.as_ref())?;
            // Apply outer LIMIT if present.
            let outer_limit = extract_limit(query)?;
            if let QueryPlan::SetOperation { ref mut limit, .. } = plan {
                *limit = outer_limit;
            }
            return Ok(plan);
        }
        SetExpr::Values(vals) => {
            // Standalone VALUES expression: VALUES (1, 'a'), (2, 'b')
            let mut rows = Vec::new();
            for row in &vals.rows {
                let mut row_vals = Vec::new();
                for expr in row {
                    row_vals.push(expr_to_value(expr)?);
                }
                rows.push(row_vals);
            }
            // Generate default column names: column1, column2, ...
            let num_cols = rows.first().map(|r| r.len()).unwrap_or(0);
            let column_names: Vec<String> = (1..=num_cols)
                .map(|i| format!("column{i}"))
                .collect();
            return Ok(QueryPlan::Values { column_names, rows });
        }
        SetExpr::Select(_) => { /* handled below */ }
        SetExpr::Query(inner_query) => {
            // Parenthesized subquery: (SELECT ... UNION ALL SELECT ...) LIMIT n
            let mut plan = plan_select(inner_query, sample_by_raw, sample_by_fill, sample_by_align_calendar, latest_on_info)?;
            // Apply outer LIMIT if present.
            let outer_limit = extract_limit(query)?;
            if let Some(lim) = outer_limit {
                if let QueryPlan::SetOperation { ref mut limit, .. } = plan {
                    *limit = Some(lim);
                }
            }
            return Ok(plan);
        }
        _ => {
            return Err(ExchangeDbError::Query(
                "only simple SELECT queries are supported".into(),
            ))
        }
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => unreachable!(),
    };

    let order_by = extract_order_by(query)?;
    let limit = extract_limit(query)?;
    let offset = extract_offset(query)?;

    plan_select_inner(select, sample_by_raw, sample_by_fill, sample_by_align_calendar, latest_on_info, &order_by, limit, offset)
}

/// Inner helper for planning a SELECT: takes the parsed Select node and
/// ORDER BY / LIMIT / OFFSET already extracted from the outer Query.
fn plan_select_inner(
    select: &ast::Select,
    sample_by_raw: Option<&str>,
    sample_by_fill: Option<&str>,
    sample_by_align_calendar: bool,
    latest_on_info: Option<&LatestOnInfo>,
    order_by: &[OrderBy],
    limit: Option<u64>,
    offset: Option<u64>,
) -> Result<QueryPlan> {
    // Check for sequence functions: nextval('seq'), currval('seq'), setval('seq', N).
    if select.projection.len() == 1 {
        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
            let fname = func.name.to_string().to_ascii_lowercase();
            if matches!(fname.as_str(), "nextval" | "currval" | "setval") {
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    let seq_name = match arg_list.args.first() {
                        Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))) => {
                            expr_to_value(expr).ok().and_then(|v| match v {
                                Value::Str(s) => Some(s),
                                _ => None,
                            })
                        }
                        _ => None,
                    };
                    if let Some(name) = seq_name {
                        let op = match fname.as_str() {
                            "nextval" => SequenceOpKind::NextVal(name),
                            "currval" => SequenceOpKind::CurrVal(name),
                            "setval" => {
                                let val = arg_list.args.get(1).and_then(|a| match a {
                                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                                        expr_to_value(expr).ok().and_then(|v| match v {
                                            Value::I64(n) => Some(n),
                                            _ => None,
                                        })
                                    }
                                    _ => None,
                                }).unwrap_or(1);
                                SequenceOpKind::SetVal(name, val)
                            }
                            _ => unreachable!(),
                        };
                        return Ok(QueryPlan::SequenceOp { op });
                    }
                }
            }
        }
    }

    // Check for derived table (subquery in FROM).
    if !select.from.is_empty() {
        if let TableFactor::Derived { subquery, alias, .. } = &select.from[0].relation {
            let sub_plan = plan_select(subquery, None, None, false, None)?;
            let alias_name = alias.as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| "subquery".to_string());

            // Parse the outer query's columns, filter, etc.
            let columns = select
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::Wildcard(_) => Ok(SelectColumn::Wildcard),
                    SelectItem::UnnamedExpr(expr) => select_expr_to_column_with_alias(expr, None),
                    SelectItem::ExprWithAlias { expr, alias } => {
                        select_expr_to_column_with_alias(expr, Some(alias.value.clone()))
                    }
                    other => Err(ExchangeDbError::Query(format!(
                        "unsupported projection: {other}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;

            let filter = if let Some(selection) = &select.selection {
                Some(expr_to_filter(selection)?)
            } else {
                None
            };

            let group_by = extract_group_by(select)?;
            let having = if let Some(having_expr) = &select.having {
                Some(having_expr_to_filter(having_expr)?)
            } else {
                None
            };
            let distinct = select.distinct.is_some();

            return Ok(QueryPlan::DerivedScan {
                subquery: Box::new(sub_plan),
                alias: alias_name,
                columns,
                filter,
                order_by: order_by.to_vec(),
                limit,
                group_by,
                having,
                distinct,
            });
        }
    }

    // Check for table-valued functions: long_sequence(N), generate_series(start, stop, step).
    if !select.from.is_empty() {
        if let Some(ls_plan) = try_plan_table_function(&select.from[0].relation, select, order_by, limit)? {
            return Ok(ls_plan);
        }
    }

    // Table name
    let (table, left_alias) = if select.from.is_empty() {
        // Allow SELECT with no FROM for system function calls like
        // SELECT version(), SELECT current_database(), etc.
        ("__no_table__".to_string(), None)
    } else {
        match &select.from[0].relation {
            TableFactor::Table { name, alias, .. } => {
                let tbl = name.to_string();
                let al = alias.as_ref().map(|a| a.name.value.clone());
                (tbl, al)
            }
            other => {
                return Err(ExchangeDbError::Query(format!(
                    "unsupported FROM clause: {other}"
                )))
            }
        }
    };

    // Check for LATERAL JOIN: SELECT ... FROM table t, LATERAL (SELECT ...) l
    if select.from.len() == 2 {
        if let TableFactor::Derived { lateral: true, subquery, alias } = &select.from[1].relation {
            let sub_plan = plan_select(subquery, None, None, false, None)?;
            let sub_alias = alias.as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| "lateral".to_string());
            let columns = extract_join_select_columns_helper(&select.projection)?;
            let filter = if let Some(selection) = &select.selection {
                Some(expr_to_filter(selection)?)
            } else {
                None
            };
            return Ok(QueryPlan::LateralJoin {
                left_table: table.clone(),
                left_alias: left_alias.clone(),
                subquery: Box::new(sub_plan),
                subquery_alias: sub_alias,
                columns,
                filter,
                order_by: order_by.to_vec(),
                limit,
            });
        }
    }

    // Implicit cross join: SELECT * FROM a, b  (multiple FROM entries).
    if select.from.len() > 1 {
        let columns = extract_join_select_columns_helper(&select.projection)?;
        let filter = if let Some(selection) = &select.selection {
            Some(expr_to_filter(selection)?)
        } else {
            None
        };
        // Build cross joins from left to right.
        let (second_table, second_alias) = match &select.from[1].relation {
            TableFactor::Table { name, alias, .. } => {
                (name.to_string(), alias.as_ref().map(|a| a.name.value.clone()))
            }
            TableFactor::Derived { .. } => {
                return Err(ExchangeDbError::Query("non-LATERAL derived tables in FROM must be in the first position".into()));
            }
            other => return Err(ExchangeDbError::Query(format!("unsupported FROM clause: {other}"))),
        };
        let is_last = select.from.len() == 2;
        let mut current_plan = QueryPlan::Join {
            left_table: table.clone(),
            right_table: second_table,
            left_alias: left_alias.clone(),
            right_alias: second_alias,
            columns: if is_last { columns.clone() } else { vec![JoinSelectColumn::Wildcard] },
            join_type: JoinType::Cross,
            on_columns: Vec::new(),
            filter: if is_last { filter.clone() } else { None },
            order_by: if is_last { order_by.to_vec() } else { Vec::new() },
            limit: if is_last { limit } else { None },
        };
        for i in 2..select.from.len() {
            let (rt, ra) = match &select.from[i].relation {
                TableFactor::Table { name, alias, .. } => {
                    (name.to_string(), alias.as_ref().map(|a| a.name.value.clone()))
                }
                other => return Err(ExchangeDbError::Query(format!("unsupported FROM clause: {other}"))),
            };
            let is_last_i = i == select.from.len() - 1;
            current_plan = QueryPlan::MultiJoin {
                left: Box::new(current_plan),
                right_table: rt,
                right_alias: ra,
                columns: if is_last_i { columns.clone() } else { vec![JoinSelectColumn::Wildcard] },
                join_type: JoinType::Cross,
                on_columns: Vec::new(),
                filter: if is_last_i { filter.clone() } else { None },
                order_by: if is_last_i { order_by.to_vec() } else { Vec::new() },
                limit: if is_last_i { limit } else { None },
            };
        }
        return Ok(current_plan);
    }

    // Check for standard JOINs - need to reconstruct a query for the helper.
    if !select.from.is_empty() && !select.from[0].joins.is_empty() {
        // Check if the query has aggregates, GROUP BY, HAVING, or DISTINCT.
        // If so, wrap the JOIN in a DerivedScan to handle post-join processing.
        let has_aggregates = join_projection_has_aggregates(&select.projection);
        let group_by_exprs = extract_group_by_for_join(select)?;
        let has_group_by = !group_by_exprs.is_empty();
        let has_having = select.having.is_some();
        let has_distinct = select.distinct.is_some();

        if has_aggregates || has_group_by || has_having || has_distinct {
            // Build the JOIN plan with SELECT * (wildcard), no order/limit for the inner join.
            // Keep the filter on the inner join for WHERE conditions.
            let join_plan = plan_standard_join_inner_impl(
                select, &table, left_alias.as_deref(),
                &[], None, true,
            )?;

            // Convert projections to SelectColumns, resolving compound identifiers
            // (e.g., "o.symbol" -> "symbol", "f.filled_qty" -> "filled_qty").
            let columns = select.projection.iter().map(|item| match item {
                SelectItem::Wildcard(_) => Ok(SelectColumn::Wildcard),
                SelectItem::UnnamedExpr(expr) => join_select_expr_to_column(expr, None),
                SelectItem::ExprWithAlias { expr, alias } => {
                    join_select_expr_to_column(expr, Some(alias.value.clone()))
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported projection: {other}"
                ))),
            }).collect::<Result<Vec<_>>>()?;

            // Parse WHERE filter (for post-join filtering in the DerivedScan).
            let filter = if let Some(selection) = &select.selection {
                Some(expr_to_filter(selection)?)
            } else {
                None
            };

            // Parse HAVING.
            let having = if let Some(having_expr) = &select.having {
                Some(having_expr_to_filter(having_expr)?)
            } else {
                None
            };

            return Ok(QueryPlan::DerivedScan {
                subquery: Box::new(join_plan),
                alias: "join_result".to_string(),
                columns,
                filter,
                order_by: order_by.to_vec(),
                limit,
                group_by: group_by_exprs,
                having,
                distinct: has_distinct,
            });
        }

        return plan_standard_join_inner(select, &table, left_alias.as_deref(), order_by, limit);
    }

    // Columns
    let columns = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) => Ok(SelectColumn::Wildcard),
            SelectItem::UnnamedExpr(expr) => select_expr_to_column_with_alias(expr, None),
            SelectItem::ExprWithAlias { expr, alias } => {
                select_expr_to_column_with_alias(expr, Some(alias.value.clone()))
            }
            other => Err(ExchangeDbError::Query(format!(
                "unsupported projection: {other}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    // WHERE filter
    let filter = if let Some(selection) = &select.selection {
        Some(expr_to_filter(selection)?)
    } else {
        None
    };

    // SAMPLE BY
    let sample_by = if let Some(raw) = sample_by_raw {
        let interval = parse_duration(raw)?;
        let fill = match sample_by_fill {
            Some(f) => {
                let f_upper = f.to_ascii_uppercase();
                match f_upper.as_str() {
                    "NONE" => FillMode::None,
                    "NULL" => FillMode::Null,
                    "PREV" => FillMode::Prev,
                    "LINEAR" => FillMode::Linear,
                    _ => {
                        // Try parsing as a numeric constant.
                        if let Ok(n) = f.parse::<i64>() {
                            FillMode::Value(Value::I64(n))
                        } else if let Ok(n) = f.parse::<f64>() {
                            FillMode::Value(Value::F64(n))
                        } else {
                            FillMode::Value(Value::Str(f.to_string()))
                        }
                    }
                }
            }
            None => FillMode::None,
        };
        let align = if sample_by_align_calendar {
            AlignMode::Calendar
        } else {
            AlignMode::FirstObservation
        };
        Some(SampleBy { interval, fill, align })
    } else {
        None
    };

    // LATEST ON
    let latest_on = latest_on_info.map(|info| LatestOn {
        timestamp_col: info.timestamp_col.clone(),
        partition_col: info.partition_col.clone(),
    });

    // GROUP BY
    let group_by = extract_group_by(select)?;
    let group_by_mode = extract_group_by_mode(select);

    // HAVING
    let having = if let Some(having_expr) = &select.having {
        Some(having_expr_to_filter(having_expr)?)
    } else {
        None
    };

    // DISTINCT / DISTINCT ON
    let (distinct, distinct_on) = match &select.distinct {
        Some(ast::Distinct::Distinct) => (true, Vec::new()),
        Some(ast::Distinct::On(exprs)) => {
            let cols: Vec<String> = exprs
                .iter()
                .map(|e| expr_to_col_name(e).unwrap_or_else(|_| e.to_string()))
                .collect();
            (false, cols)
        }
        None => (false, Vec::new()),
    };

    Ok(QueryPlan::Select {
        table,
        columns,
        filter,
        order_by: order_by.to_vec(),
        limit,
        offset,
        sample_by,
        latest_on,
        group_by,
        group_by_mode,
        having,
        distinct,
        distinct_on,
    })
}

fn extract_order_by(query: &ast::Query) -> Result<Vec<OrderBy>> {
    // Build a mapping of expression strings to their aliases from the SELECT list.
    let select = query.body.as_select().map(|s| &s.projection);
    let expr_alias_map: Vec<(String, String)> = select
        .map(|proj| {
            proj.iter().filter_map(|item| {
                if let SelectItem::ExprWithAlias { expr, alias } = item {
                    Some((expr.to_string(), alias.value.clone()))
                } else {
                    None
                }
            }).collect()
        })
        .unwrap_or_default();

    query
        .order_by
        .as_ref()
        .map(|ob| {
            match &ob.kind {
                ast::OrderByKind::Expressions(exprs) => {
                    exprs.iter()
                        .map(|o| {
                            let column = match &o.expr {
                                Expr::Identifier(ident) => ident.value.clone(),
                                Expr::CompoundIdentifier(parts) => {
                                    parts.last().map(|p| p.value.clone()).unwrap_or_default()
                                }
                                other => {
                                    let expr_str = other.to_string();
                                    // Check if this expression matches a SELECT alias.
                                    if let Some((_, alias)) = expr_alias_map.iter().find(|(e, _)| *e == expr_str) {
                                        alias.clone()
                                    } else {
                                        expr_str
                                    }
                                }
                            };
                            let descending = o.options.asc == Some(false);
                            Ok(OrderBy { column, descending })
                        })
                        .collect::<Result<Vec<_>>>()
                }
                ast::OrderByKind::All(_) => {
                    Err(ExchangeDbError::Query("ORDER BY ALL is not supported".into()))
                }
            }
        })
        .transpose()
        .map(|v| v.unwrap_or_default())
}

fn extract_limit(query: &ast::Query) -> Result<Option<u64>> {
    if let Some(limit_expr) = &query.limit {
        match expr_to_value(limit_expr)? {
            Value::I64(n) if n >= 0 => Ok(Some(n as u64)),
            _ => Ok(None),
        }
    } else if let Some(ref fetch) = query.fetch {
        // FETCH FIRST N ROWS ONLY — SQL standard equivalent of LIMIT.
        if let Some(ref qty) = fetch.quantity {
            match expr_to_value(qty)? {
                Value::I64(n) if n >= 0 => Ok(Some(n as u64)),
                _ => Ok(None),
            }
        } else {
            // FETCH FIRST ROWS ONLY without a count means LIMIT 1.
            Ok(Some(1))
        }
    } else {
        Ok(None)
    }
}

fn extract_offset(query: &ast::Query) -> Result<Option<u64>> {
    if let Some(offset) = &query.offset {
        match expr_to_value(&offset.value)? {
            Value::I64(n) if n >= 0 => Ok(Some(n as u64)),
            _ => Ok(None),
        }
    } else {
        Ok(None)
    }
}

fn extract_group_by(select: &ast::Select) -> Result<Vec<String>> {
    match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().map(|e| match e {
                Expr::Identifier(ident) => Ok(ident.value.clone()),
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported GROUP BY expression: {other}"
                ))),
            }).collect()
        }
        ast::GroupByExpr::All(_) => {
            Err(ExchangeDbError::Query("GROUP BY ALL is not supported".into()))
        }
    }
}

/// Extract advanced GROUP BY mode (GROUPING SETS / ROLLUP / CUBE) from modifiers.
fn extract_group_by_mode(select: &ast::Select) -> GroupByMode {
    let modifiers = match &select.group_by {
        ast::GroupByExpr::Expressions(_, mods) => mods,
        ast::GroupByExpr::All(mods) => mods,
    };
    if modifiers.is_empty() {
        return GroupByMode::Normal;
    }
    for modifier in modifiers {
        match modifier {
            ast::GroupByWithModifier::Rollup => {
                // The GROUP BY expressions ARE the rollup columns.
                if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    let cols: Vec<String> = exprs.iter().filter_map(|e| match e {
                        Expr::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    }).collect();
                    return GroupByMode::Rollup(cols);
                }
            }
            ast::GroupByWithModifier::Cube => {
                if let ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                    let cols: Vec<String> = exprs.iter().filter_map(|e| match e {
                        Expr::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    }).collect();
                    return GroupByMode::Cube(cols);
                }
            }
            ast::GroupByWithModifier::GroupingSets(_expr) => {
                // The modifier contains a nested expression with the sets.
                // sqlparser represents GROUPING SETS as a Tuple of Tuples.
                if let ast::GroupByWithModifier::GroupingSets(expr) = modifier {
                    let sets = parse_grouping_sets_expr(expr);
                    return GroupByMode::GroupingSets(sets);
                }
            }
            _ => {}
        }
    }
    GroupByMode::Normal
}

/// Parse a GROUPING SETS expression into a Vec<Vec<String>>.
fn parse_grouping_sets_expr(expr: &Expr) -> Vec<Vec<String>> {
    match expr {
        Expr::Tuple(items) => {
            items.iter().map(|item| {
                match item {
                    Expr::Tuple(inner) => {
                        inner.iter().filter_map(|e| match e {
                            Expr::Identifier(ident) => Some(ident.value.clone()),
                            _ => None,
                        }).collect()
                    }
                    Expr::Identifier(ident) => vec![ident.value.clone()],
                    _ => vec![],
                }
            }).collect()
        }
        _ => vec![],
    }
}

/// Convert a HAVING expression to a Filter. HAVING may reference aggregate
/// functions like `count(*) > 10`, so we need special handling.
fn having_expr_to_filter(expr: &Expr) -> Result<Filter> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                ast::BinaryOperator::And => {
                    let l = having_expr_to_filter(left)?;
                    let r = having_expr_to_filter(right)?;
                    let mut parts = Vec::new();
                    match l {
                        Filter::And(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    match r {
                        Filter::And(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    Ok(Filter::And(parts))
                }
                ast::BinaryOperator::Or => {
                    let l = having_expr_to_filter(left)?;
                    let r = having_expr_to_filter(right)?;
                    let mut parts = Vec::new();
                    match l {
                        Filter::Or(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    match r {
                        Filter::Or(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    Ok(Filter::Or(parts))
                }
                ast::BinaryOperator::Gt | ast::BinaryOperator::Lt
                | ast::BinaryOperator::GtEq | ast::BinaryOperator::LtEq
                | ast::BinaryOperator::Eq | ast::BinaryOperator::NotEq => {
                    // The left side may be an aggregate function like count(*)
                    // or a complex expression like max(d) - min(d).
                    if let Ok(col) = having_expr_to_col_name(left) {
                        let val = expr_to_value(right)?;
                        match op {
                            ast::BinaryOperator::Gt => Ok(Filter::Gt(col, val)),
                            ast::BinaryOperator::Lt => Ok(Filter::Lt(col, val)),
                            ast::BinaryOperator::GtEq => Ok(Filter::Gte(col, val)),
                            ast::BinaryOperator::LtEq => Ok(Filter::Lte(col, val)),
                            ast::BinaryOperator::Eq => Ok(Filter::Eq(col, val)),
                            ast::BinaryOperator::NotEq => Ok(Filter::NotEq(col, val)),
                            _ => unreachable!(),
                        }
                    } else {
                        // Fall back to Expression filter for complex HAVING.
                        let left_expr = having_expr_to_plan_expr(left)?;
                        let right_expr = having_expr_to_plan_expr(right)?;
                        let cmp_op = match op {
                            ast::BinaryOperator::Gt => CompareOp::Gt,
                            ast::BinaryOperator::Lt => CompareOp::Lt,
                            ast::BinaryOperator::GtEq => CompareOp::Gte,
                            ast::BinaryOperator::LtEq => CompareOp::Lte,
                            ast::BinaryOperator::Eq => CompareOp::Eq,
                            ast::BinaryOperator::NotEq => CompareOp::NotEq,
                            _ => unreachable!(),
                        };
                        Ok(Filter::Expression {
                            left: left_expr,
                            op: cmp_op,
                            right: right_expr,
                        })
                    }
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported binary operator in HAVING: {other}"
                ))),
            }
        }
        Expr::Nested(inner) => having_expr_to_filter(inner),
        other => Err(ExchangeDbError::Query(format!(
            "unsupported HAVING expression: {other}"
        ))),
    }
}

/// Convert a HAVING left-hand expression to a column name string.
/// Supports both plain identifiers and aggregate function calls
/// (which get normalized to e.g. "count(*)").
fn having_expr_to_col_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            // "o.sym" -> "sym"
            Ok(parts[1].value.clone())
        }
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_ascii_lowercase();
            let arg_str = match &func.args {
                ast::FunctionArguments::List(arg_list) => {
                    if arg_list.args.len() == 1 {
                        match &arg_list.args[0] {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => "*".to_string(),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            )) => ident.value.clone(),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::CompoundIdentifier(parts),
                            )) if parts.len() == 2 => {
                                // "f.oid" -> "oid"
                                parts[1].value.clone()
                            }
                            other => other.to_string(),
                        }
                    } else {
                        "?".to_string()
                    }
                }
                _ => "?".to_string(),
            };
            Ok(format!("{func_name}({arg_str})"))
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported HAVING expression for column name: {other}"
        ))),
    }
}

/// Convert a HAVING expression to a PlanExpr where aggregate function calls
/// are represented as Column references (e.g., sum(d) -> Column("sum(d)")).
fn having_expr_to_plan_expr(expr: &Expr) -> Result<PlanExpr> {
    match expr {
        Expr::Identifier(ident) => Ok(PlanExpr::Column(ident.value.clone())),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Ok(PlanExpr::Column(parts[1].value.clone()))
        }
        Expr::Function(_) => {
            // Convert aggregate function to a column name reference.
            let col_name = having_expr_to_col_name(expr)?;
            Ok(PlanExpr::Column(col_name))
        }
        Expr::Value(v) => {
            let val = sql_value_to_value(v)?;
            Ok(PlanExpr::Literal(val))
        }
        Expr::BinaryOp { left, op, right } => {
            let plan_op = match op {
                ast::BinaryOperator::Plus => BinaryOp::Add,
                ast::BinaryOperator::Minus => BinaryOp::Sub,
                ast::BinaryOperator::Multiply => BinaryOp::Mul,
                ast::BinaryOperator::Divide => BinaryOp::Div,
                ast::BinaryOperator::Modulo => BinaryOp::Mod,
                other => {
                    return Err(ExchangeDbError::Query(format!(
                        "unsupported binary operator in HAVING expression: {other}"
                    )))
                }
            };
            Ok(PlanExpr::BinaryOp {
                left: Box::new(having_expr_to_plan_expr(left)?),
                op: plan_op,
                right: Box::new(having_expr_to_plan_expr(right)?),
            })
        }
        Expr::Nested(inner) => having_expr_to_plan_expr(inner),
        other => Err(ExchangeDbError::Query(format!(
            "unsupported HAVING expression: {other}"
        ))),
    }
}

#[allow(dead_code)]
fn plan_standard_join(
    query: &ast::Query,
    select: &ast::Select,
    left_table: &str,
    left_alias: Option<&str>,
) -> Result<QueryPlan> {
    let order_by = extract_order_by(query)?;
    let limit = extract_limit(query)?;
    plan_standard_join_inner_impl(select, left_table, left_alias, &order_by, limit, false)
}

fn plan_standard_join_inner(
    select: &ast::Select,
    left_table: &str,
    left_alias: Option<&str>,
    order_by: &[OrderBy],
    limit: Option<u64>,
) -> Result<QueryPlan> {
    plan_standard_join_inner_impl(select, left_table, left_alias, order_by, limit, false)
}

/// When `force_wildcard` is true, the JOIN plan uses SELECT * regardless of
/// the actual projection (used when wrapping in DerivedScan for aggregation).
fn plan_standard_join_inner_impl(
    select: &ast::Select,
    left_table: &str,
    left_alias: Option<&str>,
    order_by: &[OrderBy],
    limit: Option<u64>,
    force_wildcard: bool,
) -> Result<QueryPlan> {
    let joins = &select.from[0].joins;

    // Multi-table JOIN support: chain joins via nested MultiJoin plans.
    if joins.len() > 1 {
        let columns = if force_wildcard {
            vec![JoinSelectColumn::Wildcard]
        } else {
            extract_join_select_columns_helper(&select.projection)?
        };
        let filter = if let Some(selection) = &select.selection {
            Some(expr_to_filter(selection)?)
        } else {
            None
        };
        // Build first join (no filter/order/limit for intermediate joins).
        let mut current_plan = build_single_join_plan_helper(
            left_table, left_alias, &joins[0],
            &[JoinSelectColumn::Wildcard], None, &[], None,
        )?;
        for i in 1..joins.len() {
            let jn = &joins[i];
            let is_last = i == joins.len() - 1;
            let (rt, ra) = match &jn.relation {
                TableFactor::Table { name, alias, .. } => (name.to_string(), alias.as_ref().map(|a| a.name.value.clone())),
                other => return Err(ExchangeDbError::Query(format!("unsupported JOIN table: {other}"))),
            };
            let (jt, oc) = extract_join_type_and_on_helper(jn)?;
            current_plan = QueryPlan::MultiJoin {
                left: Box::new(current_plan),
                right_table: rt, right_alias: ra,
                columns: if is_last { columns.clone() } else { vec![JoinSelectColumn::Wildcard] },
                join_type: jt, on_columns: oc,
                filter: if is_last { filter.clone() } else { None },
                order_by: if is_last { order_by.to_vec() } else { Vec::new() },
                limit: if is_last { limit } else { None },
            };
        }
        return Ok(current_plan);
    }

    let join = &joins[0];

    // Determine right table and alias
    let (right_table, right_alias) = match &join.relation {
        TableFactor::Table { name, alias, .. } => {
            let tbl = name.to_string();
            let al = alias.as_ref().map(|a| a.name.value.clone());
            (tbl, al)
        }
        other => {
            return Err(ExchangeDbError::Query(format!(
                "unsupported JOIN table: {other}"
            )))
        }
    };

    // Determine join type
    let join_type = match &join.join_operator {
        JoinOperator::Inner(constraint)
        | JoinOperator::Join(constraint) => {
            (JoinType::Inner, constraint)
        }
        JoinOperator::LeftOuter(constraint)
        | JoinOperator::Left(constraint) => {
            (JoinType::Left, constraint)
        }
        JoinOperator::RightOuter(constraint)
        | JoinOperator::Right(constraint) => {
            (JoinType::Right, constraint)
        }
        JoinOperator::FullOuter(constraint) => {
            (JoinType::FullOuter, constraint)
        }
        JoinOperator::CrossJoin => {
            return {
                // CROSS JOIN: no ON columns needed
                let columns = if force_wildcard {
                    vec![JoinSelectColumn::Wildcard]
                } else {
                    extract_join_select_columns_helper(&select.projection)?
                };
                let filter = if let Some(selection) = &select.selection {
                    Some(expr_to_filter(selection)?)
                } else {
                    None
                };
                Ok(QueryPlan::Join {
                    left_table: left_table.to_string(),
                    right_table,
                    left_alias: left_alias.map(|s| s.to_string()),
                    right_alias,
                    columns,
                    join_type: JoinType::Cross,
                    on_columns: Vec::new(),
                    filter,
                    order_by: order_by.to_vec(),
                    limit,
                })
            };
        }
        other => {
            return Err(ExchangeDbError::Query(format!(
                "unsupported JOIN type: {other:?}"
            )))
        }
    };

    // Extract ON columns
    let on_columns = match join_type.1 {
        JoinConstraint::On(expr) => extract_join_on_columns(expr)?,
        JoinConstraint::Using(cols) => {
            // USING(col1, col2) -> ON left.col1 = right.col1 AND left.col2 = right.col2
            cols.iter().map(|c| {
                let col_name = c.to_string();
                (col_name.clone(), col_name)
            }).collect()
        }
        JoinConstraint::Natural => {
            // NATURAL JOIN: empty on_columns signals the executor to auto-detect
            // common columns between the two tables.
            Vec::new()
        }
        JoinConstraint::None => Vec::new(),
        #[allow(unreachable_patterns)]
        other => {
            return Err(ExchangeDbError::Query(format!(
                "unsupported JOIN constraint: {other:?}"
            )))
        }
    };

    // For NATURAL JOIN, we store a special marker as the first on_columns entry
    // so the executor can detect and resolve common columns.
    let is_natural = matches!(join_type.1, JoinConstraint::Natural);
    let on_columns = if is_natural {
        // Use a sentinel value to signal natural join to executor
        vec![("__natural__".to_string(), "__natural__".to_string())]
    } else {
        on_columns
    };

    // Extract columns
    let columns = if force_wildcard {
        vec![JoinSelectColumn::Wildcard]
    } else {
        extract_join_select_columns_helper(&select.projection)?
    };

    // WHERE filter
    let filter = if let Some(selection) = &select.selection {
        Some(expr_to_filter(selection)?)
    } else {
        None
    };

    Ok(QueryPlan::Join {
        left_table: left_table.to_string(),
        right_table,
        left_alias: left_alias.map(|s| s.to_string()),
        right_alias,
        columns,
        join_type: join_type.0,
        on_columns,
        filter,
        order_by: order_by.to_vec(),
        limit,
    })
}

/// Extract equality column pairs from a JOIN ON expression.
fn extract_join_on_columns(expr: &Expr) -> Result<Vec<(String, String)>> {
    match expr {
        Expr::BinaryOp { left, op: ast::BinaryOperator::Eq, right } => {
            let left_col = extract_join_col_name(left)?;
            let right_col = extract_join_col_name(right)?;
            Ok(vec![(left_col, right_col)])
        }
        Expr::BinaryOp { left, op: ast::BinaryOperator::And, right } => {
            let mut cols = extract_join_on_columns(left)?;
            cols.extend(extract_join_on_columns(right)?);
            Ok(cols)
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported JOIN ON expression: {other}"
        ))),
    }
}

/// Extract a column name from a JOIN ON expression, stripping table prefix.
fn extract_join_col_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            // Return "alias.column" format so we can resolve which table it belongs to
            Ok(format!("{}.{}", parts[0].value, parts[1].value))
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported JOIN column expression: {other}"
        ))),
    }
}

/// Extract join select columns from a SQL projection list.
#[allow(dead_code)]
fn extract_join_select_columns(projection: &[SelectItem]) -> Result<Vec<JoinSelectColumn>> {
    extract_join_select_columns_helper(projection)
}

// Helper: extract join select columns for multi-table join support.
fn extract_join_select_columns_helper(projection: &[SelectItem]) -> Result<Vec<JoinSelectColumn>> {
    projection.iter().map(|item| match item {
        SelectItem::Wildcard(_) => Ok(JoinSelectColumn::Wildcard),
        SelectItem::QualifiedWildcard(name, _) => Ok(JoinSelectColumn::QualifiedWildcard(name.to_string())),
        SelectItem::ExprWithAlias { expr, alias } => {
            let alias_str = alias.value.clone();
            match expr {
                Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    Ok(JoinSelectColumn::QualifiedAlias(parts[0].value.clone(), parts[1].value.clone(), alias_str))
                }
                Expr::Identifier(ident) => {
                    Ok(JoinSelectColumn::QualifiedAlias(String::new(), ident.value.clone(), alias_str))
                }
                _ => {
                    // Expression or CASE WHEN with alias
                    let plan_expr = sql_expr_to_plan_expr(expr)?;
                    Ok(JoinSelectColumn::Expression { expr: plan_expr, alias: Some(alias_str) })
                }
            }
        }
        SelectItem::UnnamedExpr(expr) => match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(JoinSelectColumn::Qualified(parts[0].value.clone(), parts[1].value.clone())),
            Expr::Identifier(ident) => Ok(JoinSelectColumn::Unqualified(ident.value.clone())),
            Expr::Function(func) => {
                let func_name = func.name.to_string();
                if let Some(kind) = AggregateKind::from_name(&func_name) {
                    // Aggregate function in JOIN select
                    let (col_name, arg_expr) = match &func.args {
                        ast::FunctionArguments::List(arg_list) if !arg_list.args.is_empty() => {
                            match &arg_list.args[0] {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::Identifier(ident))) => (ident.value.clone(), None),
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                                    if parts.len() == 2 { (format!("{}.{}", parts[0].value, parts[1].value), None) } else { ("*".to_string(), None) }
                                }
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => ("*".to_string(), None),
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                                    // Complex expression (e.g. f.price * f.filled)
                                    let plan_expr = sql_expr_to_plan_expr(expr)?;
                                    ("*".to_string(), Some(plan_expr))
                                }
                                _ => ("*".to_string(), None),
                            }
                        }
                        _ => ("*".to_string(), None),
                    };
                    Ok(JoinSelectColumn::Aggregate { function: kind, column: col_name, alias: None, arg_expr })
                } else {
                    // Treat as expression
                    let plan_expr = sql_expr_to_plan_expr(expr)?;
                    Ok(JoinSelectColumn::Expression { expr: plan_expr, alias: None })
                }
            }
            Expr::BinaryOp { .. } | Expr::UnaryOp { .. } => {
                let plan_expr = sql_expr_to_plan_expr(expr)?;
                Ok(JoinSelectColumn::Expression { expr: plan_expr, alias: None })
            }
            Expr::Case { .. } => {
                let plan_expr = sql_expr_to_plan_expr(expr)?;
                Ok(JoinSelectColumn::Expression { expr: plan_expr, alias: None })
            }
            other => Err(ExchangeDbError::Query(format!("unsupported projection in JOIN: {other}"))),
        },
    }).collect::<Result<Vec<_>>>()
}

// Helper: extract join type and ON columns from a join AST node.
fn extract_join_type_and_on_helper(join: &ast::Join) -> Result<(JoinType, Vec<(String, String)>)> {
    let (jt, constraint): (JoinType, Option<&JoinConstraint>) = match &join.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => (JoinType::Inner, Some(c)),
        JoinOperator::LeftOuter(c) | JoinOperator::Left(c) => (JoinType::Left, Some(c)),
        JoinOperator::RightOuter(c) | JoinOperator::Right(c) => (JoinType::Right, Some(c)),
        JoinOperator::FullOuter(c) => (JoinType::FullOuter, Some(c)),
        JoinOperator::CrossJoin => (JoinType::Cross, None),
        other => return Err(ExchangeDbError::Query(format!("unsupported JOIN type: {other:?}"))),
    };
    let on_columns = if let Some(c) = constraint {
        match c {
            JoinConstraint::On(expr) => extract_join_on_columns(expr)?,
            JoinConstraint::Using(cols) => {
                cols.iter().map(|c| {
                    let col_name = c.to_string();
                    (col_name.clone(), col_name)
                }).collect()
            }
            JoinConstraint::Natural => {
                vec![("__natural__".to_string(), "__natural__".to_string())]
            }
            JoinConstraint::None => Vec::new(),
            #[allow(unreachable_patterns)]
            other => return Err(ExchangeDbError::Query(format!("unsupported JOIN constraint: {other:?}"))),
        }
    } else { Vec::new() };
    Ok((jt, on_columns))
}

// Helper: build a single Join plan.
fn build_single_join_plan_helper(left_table: &str, left_alias: Option<&str>, join: &ast::Join, columns: &[JoinSelectColumn], filter: Option<Filter>, order_by: &[OrderBy], limit: Option<u64>) -> Result<QueryPlan> {
    let (right_table, right_alias) = match &join.relation {
        TableFactor::Table { name, alias, .. } => (name.to_string(), alias.as_ref().map(|a| a.name.value.clone())),
        other => return Err(ExchangeDbError::Query(format!("unsupported JOIN table: {other}"))),
    };
    let (join_type, on_columns) = extract_join_type_and_on_helper(join)?;
    Ok(QueryPlan::Join {
        left_table: left_table.to_string(), right_table,
        left_alias: left_alias.map(|s| s.to_string()), right_alias,
        columns: columns.to_vec(), join_type, on_columns, filter,
        order_by: order_by.to_vec(), limit,
    })
}

/// Convert a SQL expression to a `SelectColumn`, with optional alias for window functions.
fn select_expr_to_column_with_alias(expr: &Expr, alias: Option<String>) -> Result<SelectColumn> {
    // Check for window function (function with OVER clause).
    if let Expr::Function(func) = expr {
        if func.over.is_some() {
            return plan_window_function(func, alias);
        }
    }
    select_expr_to_column(expr, alias)
}

fn select_expr_to_column(expr: &Expr, alias: Option<String>) -> Result<SelectColumn> {
    match expr {
        Expr::Identifier(ident) => {
            if let Some(a) = alias {
                // Column with alias: wrap as Expression so the alias is preserved.
                Ok(SelectColumn::Expression {
                    expr: PlanExpr::Column(ident.value.clone()),
                    alias: Some(a),
                })
            } else {
                Ok(SelectColumn::Name(ident.value.clone()))
            }
        }
        Expr::Function(func) => {
            let func_name = func.name.to_string();

            // Try aggregate functions first.
            if let Some(mut kind) = AggregateKind::from_name(&func_name) {
                // Extract the single column argument.
                let args = &func.args;
                let (col_name, arg_expr_parsed) = match args {
                    ast::FunctionArguments::List(arg_list) => {
                        // Handle count(DISTINCT col) syntax.
                        if let Some(ast::DuplicateTreatment::Distinct) = arg_list.duplicate_treatment {
                            if matches!(kind, AggregateKind::Count) {
                                kind = AggregateKind::CountDistinct;
                            }
                        }
                        if arg_list.args.len() != 1 {
                            return Err(ExchangeDbError::Query(format!(
                                "{func_name} expects exactly one argument"
                            )));
                        }
                        match &arg_list.args[0] {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            )) => (ident.value.clone(), None),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => ("*".to_string(), None),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                                // Expression argument like sum(d * 2.0) or count(CASE WHEN ...)
                                let plan_expr = sql_expr_to_plan_expr(expr)?;
                                let col_refs = plan_expr_column_refs(&plan_expr);
                                let nominal_col = col_refs.into_iter().next().unwrap_or_else(|| "*".to_string());
                                (nominal_col, Some(plan_expr))
                            }
                            other => {
                                return Err(ExchangeDbError::Query(format!(
                                    "unsupported function argument: {other}"
                                )))
                            }
                        }
                    }
                    _ => {
                        return Err(ExchangeDbError::Query(format!(
                            "unsupported function arguments for {func_name}"
                        )))
                    }
                };

                // Parse FILTER (WHERE ...) clause if present.
                let agg_filter = if let Some(filter_expr) = &func.filter {
                    Some(Box::new(expr_to_filter(filter_expr)?))
                } else {
                    None
                };

                // Parse WITHIN GROUP (ORDER BY ...) clause if present.
                let within_group = if !func.within_group.is_empty() {
                    let orders = func.within_group.iter().map(|o| {
                        let col = match &o.expr {
                            Expr::Identifier(ident) => ident.value.clone(),
                            other => other.to_string(),
                        };
                        OrderBy {
                            column: col,
                            descending: o.options.asc.map(|a| !a).unwrap_or(false),
                        }
                    }).collect::<Vec<_>>();
                    Some(orders)
                } else {
                    None
                };

                return Ok(SelectColumn::Aggregate {
                    function: kind,
                    column: col_name,
                    alias,
                    filter: agg_filter,
                    within_group_order: within_group,
                    arg_expr: arg_expr_parsed,
                });
            }

            // Try scalar functions.
            if crate::scalar::evaluate_scalar(&func_name, &[]).is_ok()
                || crate::scalar::ScalarRegistry::new().get(&func_name).is_some()
            {
                // Check if any argument is an aggregate function call.
                // If so, represent as Expression with PlanExpr::Function
                // so that the executor can evaluate the aggregate first,
                // then apply the scalar.
                let has_nested_agg = has_aggregate_arg(&func.args);
                if has_nested_agg {
                    let plan_args = extract_plan_expr_args(&func.args)?;
                    return Ok(SelectColumn::Expression {
                        expr: PlanExpr::Function {
                            name: func_name.to_ascii_lowercase(),
                            args: plan_args,
                        },
                        alias,
                    });
                }

                let scalar_args = extract_scalar_args(&func.args)?;
                return Ok(SelectColumn::ScalarFunction {
                    name: func_name.to_ascii_lowercase(),
                    args: scalar_args,
                });
            }

            // System/catalog functions (version(), current_database(), etc.)
            let lower_name = func_name.to_ascii_lowercase();
            if matches!(
                lower_name.as_str(),
                "version" | "current_database" | "current_schema" | "current_schemas"
            ) {
                return Ok(SelectColumn::ScalarFunction {
                    name: lower_name,
                    args: vec![],
                });
            }

            Err(ExchangeDbError::Query(format!(
                "unknown function: {func_name}"
            )))
        }
        Expr::Case { operand, conditions, else_result } => {
            let mut case_conditions = Vec::new();
            let mut expr_conds = Vec::new();
            let mut needs_expr = false;
            for case_when in conditions.iter() {
                let filter = if let Some(op) = operand {
                    let col = expr_to_col_name(op)?;
                    let val = expr_to_value(&case_when.condition)?;
                    Filter::Eq(col, val)
                } else {
                    expr_to_filter(&case_when.condition)?
                };
                // Try literal first; fall back to expression.
                if let Ok(result_val) = expr_to_value(&case_when.result) {
                    let plan_expr = PlanExpr::Literal(result_val.clone());
                    case_conditions.push((filter.clone(), result_val));
                    expr_conds.push((filter, plan_expr));
                } else {
                    // Expression result (e.g. d * 2.0)
                    let plan_expr = sql_expr_to_plan_expr(&case_when.result)?;
                    needs_expr = true;
                    case_conditions.push((filter.clone(), Value::Null)); // placeholder
                    expr_conds.push((filter, plan_expr));
                }
            }
            let (else_val, expr_else) = if let Some(e) = else_result {
                if let Ok(v) = expr_to_value(e) {
                    (Some(v.clone()), Some(PlanExpr::Literal(v)))
                } else {
                    let plan_expr = sql_expr_to_plan_expr(e)?;
                    needs_expr = true;
                    (None, Some(plan_expr))
                }
            } else {
                (None, None)
            };
            Ok(SelectColumn::CaseWhen {
                conditions: case_conditions,
                else_value: else_val,
                alias,
                expr_conditions: if needs_expr { Some(expr_conds) } else { None },
                expr_else: if needs_expr { expr_else } else { None },
            })
        }
        Expr::Cast { expr, data_type, .. } => {
            // Convert CAST(expr AS type) to a scalar function call.
            let func_name = match data_type {
                ast::DataType::Integer(_) | ast::DataType::Int(_) | ast::DataType::BigInt(_)
                | ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => "cast_to_int",
                ast::DataType::Float(_) | ast::DataType::Double(_)
                | ast::DataType::DoublePrecision | ast::DataType::Real
                | ast::DataType::Numeric(_) | ast::DataType::Decimal(_)
                | ast::DataType::Dec(_) => "cast_to_float",
                ast::DataType::Varchar(_) | ast::DataType::Char(_)
                | ast::DataType::Text | ast::DataType::String(_) => "cast_to_str",
                ast::DataType::Timestamp(_, _) => "cast_to_timestamp",
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported CAST target type: {other}"
                ))),
            };
            let arg = match expr.as_ref() {
                Expr::Identifier(ident) => SelectColumnArg::Column(ident.value.clone()),
                other => {
                    let val = expr_to_value(other)?;
                    SelectColumnArg::Literal(val)
                }
            };
            Ok(SelectColumn::ScalarFunction {
                name: func_name.to_string(),
                args: vec![arg],
            })
        }
        Expr::Value(_) => {
            // Literal value in SELECT list (e.g. SELECT 42, 'hello', ...)
            let val = expr_to_value(expr)?;
            Ok(SelectColumn::Expression {
                expr: PlanExpr::Literal(val),
                alias,
            })
        }
        other => {
            // Try to parse as a complex expression (arithmetic, concatenation, etc.)
            if is_complex_expr(other) {
                let plan_expr = sql_expr_to_plan_expr(other)?;
                return Ok(SelectColumn::Expression { expr: plan_expr, alias });
            }
            // Scalar subquery: (SELECT count(*) FROM t)
            if let Expr::Subquery(subquery) = other {
                let sub_plan = plan_select(subquery, None, None, false, None)?;
                return Ok(SelectColumn::ScalarSubquery {
                    subquery: Box::new(sub_plan),
                    alias,
                });
            }
            Err(ExchangeDbError::Query(format!(
                "unsupported select expression: {other}"
            )))
        }
    }
}

/// Convert a sqlparser Function with an OVER clause into a `SelectColumn::WindowFunction`.
fn plan_window_function(func: &ast::Function, alias: Option<String>) -> Result<SelectColumn> {
    use crate::window::WindowFunction;

    let func_name = func.name.to_string().to_ascii_lowercase();

    // Extract function arguments.
    let args = extract_window_func_args(&func.args)?;

    // Extract the OVER clause.
    let over = match &func.over {
        Some(ast::WindowType::WindowSpec(spec)) => convert_window_spec(spec)?,
        Some(ast::WindowType::NamedWindow(_name)) => {
            return Err(ExchangeDbError::Query(
                "named windows are not supported yet".into(),
            ));
        }
        None => {
            return Err(ExchangeDbError::Query(
                "window function requires OVER clause".into(),
            ));
        }
    };

    Ok(SelectColumn::WindowFunction(WindowFunction {
        name: func_name,
        args,
        over,
        alias,
    }))
}

/// Extract arguments from a window function call.
fn extract_window_func_args(
    args: &ast::FunctionArguments,
) -> Result<Vec<crate::window::WindowFuncArg>> {
    use crate::window::WindowFuncArg;

    match args {
        ast::FunctionArguments::List(arg_list) => {
            arg_list
                .args
                .iter()
                .map(|arg| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        Expr::Identifier(ident),
                    )) => Ok(WindowFuncArg::Column(ident.value.clone())),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                        Ok(WindowFuncArg::Wildcard)
                    }
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        // Try as literal value.
                        match expr_to_value(expr) {
                            Ok(Value::I64(n)) => Ok(WindowFuncArg::LiteralInt(n)),
                            Ok(Value::F64(f)) => Ok(WindowFuncArg::LiteralFloat(f)),
                            Ok(Value::Str(s)) => Ok(WindowFuncArg::LiteralStr(s)),
                            Ok(Value::Null) => Ok(WindowFuncArg::Null),
                            _ => Err(ExchangeDbError::Query(format!(
                                "unsupported window function argument: {expr}"
                            ))),
                        }
                    }
                    other => Err(ExchangeDbError::Query(format!(
                        "unsupported window function argument: {other}"
                    ))),
                })
                .collect()
        }
        ast::FunctionArguments::None => Ok(Vec::new()),
        other => Err(ExchangeDbError::Query(format!(
            "unsupported window function arguments: {other}"
        ))),
    }
}

/// Convert a sqlparser `WindowSpec` to our internal `WindowSpec`.
fn convert_window_spec(
    spec: &ast::WindowSpec,
) -> Result<crate::window::WindowSpec> {
    use crate::window::{FrameBound, WindowFrame, WindowSpec};

    let partition_by: Vec<String> = spec
        .partition_by
        .iter()
        .map(|expr| match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            other => Err(ExchangeDbError::Query(format!(
                "unsupported PARTITION BY expression: {other}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    let order_by: Vec<OrderBy> = spec
        .order_by
        .iter()
        .map(|o| {
            let column = match &o.expr {
                Expr::Identifier(ident) => ident.value.clone(),
                other => other.to_string(),
            };
            let descending = o.options.asc == Some(false);
            Ok(OrderBy { column, descending })
        })
        .collect::<Result<Vec<_>>>()?;

    let frame = if let Some(wf) = &spec.window_frame {
        let start = convert_frame_bound(&wf.start_bound)?;
        let end = match &wf.end_bound {
            Some(eb) => convert_frame_bound(eb)?,
            None => FrameBound::CurrentRow,
        };
        Some(WindowFrame::Rows { start, end })
    } else {
        None
    };

    Ok(WindowSpec {
        partition_by,
        order_by,
        frame,
    })
}

/// Convert a sqlparser `WindowFrameBound` to our internal `FrameBound`.
fn convert_frame_bound(
    bound: &ast::WindowFrameBound,
) -> Result<crate::window::FrameBound> {
    use crate::window::FrameBound;

    match bound {
        ast::WindowFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
        ast::WindowFrameBound::Preceding(None) => Ok(FrameBound::UnboundedPreceding),
        ast::WindowFrameBound::Following(None) => Ok(FrameBound::UnboundedFollowing),
        ast::WindowFrameBound::Preceding(Some(expr)) => {
            let val = expr_to_value(expr)?;
            match val {
                Value::I64(n) => Ok(FrameBound::Preceding(n as u64)),
                _ => Err(ExchangeDbError::Query(
                    "window frame bound must be an integer".into(),
                )),
            }
        }
        ast::WindowFrameBound::Following(Some(expr)) => {
            let val = expr_to_value(expr)?;
            match val {
                Value::I64(n) => Ok(FrameBound::Following(n as u64)),
                _ => Err(ExchangeDbError::Query(
                    "window frame bound must be an integer".into(),
                )),
            }
        }
    }
}

/// Check if any function argument is an aggregate function call.
fn has_aggregate_arg(args: &ast::FunctionArguments) -> bool {
    match args {
        ast::FunctionArguments::List(arg_list) => {
            arg_list.args.iter().any(|arg| {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(Expr::Function(f))) = arg {
                    AggregateKind::from_name(&f.name.to_string()).is_some()
                } else {
                    false
                }
            })
        }
        _ => false,
    }
}

/// Extract function arguments as PlanExpr (for nested aggregate expressions).
fn extract_plan_expr_args(args: &ast::FunctionArguments) -> Result<Vec<PlanExpr>> {
    match args {
        ast::FunctionArguments::List(arg_list) => {
            arg_list.args.iter().map(|arg| {
                match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        sql_expr_to_plan_expr(expr)
                    }
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                        Ok(PlanExpr::Column("*".to_string()))
                    }
                    other => Err(ExchangeDbError::Query(format!(
                        "unsupported function argument: {other}"
                    ))),
                }
            }).collect()
        }
        ast::FunctionArguments::None => Ok(Vec::new()),
        other => Err(ExchangeDbError::Query(format!(
            "unsupported function arguments: {other}"
        ))),
    }
}

/// Extract scalar function arguments from SQL AST function arguments.
fn extract_scalar_args(args: &ast::FunctionArguments) -> Result<Vec<SelectColumnArg>> {
    match args {
        ast::FunctionArguments::List(arg_list) => {
            arg_list
                .args
                .iter()
                .map(|arg| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                        Expr::Identifier(ident),
                    )) => Ok(SelectColumnArg::Column(ident.value.clone())),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        // Try to evaluate as a literal value.
                        match expr_to_value(expr) {
                            Ok(val) => Ok(SelectColumnArg::Literal(val)),
                            Err(_) => Ok(SelectColumnArg::Column(expr.to_string())),
                        }
                    }
                    other => Err(ExchangeDbError::Query(format!(
                        "unsupported scalar function argument: {other}"
                    ))),
                })
                .collect()
        }
        ast::FunctionArguments::None => Ok(Vec::new()),
        other => Err(ExchangeDbError::Query(format!(
            "unsupported function arguments: {other}"
        ))),
    }
}

fn expr_to_filter(expr: &Expr) -> Result<Filter> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            match op {
                ast::BinaryOperator::And => {
                    let l = expr_to_filter(left)?;
                    let r = expr_to_filter(right)?;
                    // Flatten nested ANDs.
                    let mut parts = Vec::new();
                    match l {
                        Filter::And(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    match r {
                        Filter::And(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    Ok(Filter::And(parts))
                }
                ast::BinaryOperator::Or => {
                    let l = expr_to_filter(left)?;
                    let r = expr_to_filter(right)?;
                    let mut parts = Vec::new();
                    match l {
                        Filter::Or(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    match r {
                        Filter::Or(mut inner) => parts.append(&mut inner),
                        other => parts.push(other),
                    }
                    Ok(Filter::Or(parts))
                }
                ast::BinaryOperator::Eq
                | ast::BinaryOperator::NotEq
                | ast::BinaryOperator::Gt
                | ast::BinaryOperator::Lt
                | ast::BinaryOperator::GtEq
                | ast::BinaryOperator::LtEq => {
                    // Check if right side is a subquery.
                    if let Expr::Subquery(subquery) = right.as_ref() {
                        let col = expr_to_col_name(left)?;
                        let sub_plan = plan_select(subquery, None, None, false, None)?;
                        let compare_op = match op {
                            ast::BinaryOperator::Eq => CompareOp::Eq,
                            ast::BinaryOperator::NotEq => CompareOp::NotEq,
                            ast::BinaryOperator::Gt => CompareOp::Gt,
                            ast::BinaryOperator::Lt => CompareOp::Lt,
                            ast::BinaryOperator::GtEq => CompareOp::Gte,
                            ast::BinaryOperator::LtEq => CompareOp::Lte,
                            _ => unreachable!(),
                        };
                        return Ok(Filter::Subquery {
                            column: col,
                            op: compare_op,
                            subquery: Box::new(sub_plan),
                        });
                    }

                    // If either side is complex (arithmetic, etc.) or both sides
                    // are column references (column-vs-column comparison), use
                    // expression filter.
                    let right_is_col = matches!(right.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_));
                    let left_is_col = matches!(left.as_ref(), Expr::Identifier(_) | Expr::CompoundIdentifier(_));
                    if is_complex_filter_side(left) || is_complex_filter_side(right) || (left_is_col && right_is_col) {
                        let compare_op = match op {
                            ast::BinaryOperator::Eq => CompareOp::Eq,
                            ast::BinaryOperator::NotEq => CompareOp::NotEq,
                            ast::BinaryOperator::Gt => CompareOp::Gt,
                            ast::BinaryOperator::Lt => CompareOp::Lt,
                            ast::BinaryOperator::GtEq => CompareOp::Gte,
                            ast::BinaryOperator::LtEq => CompareOp::Lte,
                            _ => unreachable!(),
                        };
                        return Ok(Filter::Expression {
                            left: sql_expr_to_plan_expr(left)?,
                            op: compare_op,
                            right: sql_expr_to_plan_expr(right)?,
                        });
                    }

                    let col = expr_to_col_name(left)?;
                    let val = expr_to_value(right)?;
                    match op {
                        ast::BinaryOperator::Eq => Ok(Filter::Eq(col, val)),
                        ast::BinaryOperator::NotEq => Ok(Filter::NotEq(col, val)),
                        ast::BinaryOperator::Gt => Ok(Filter::Gt(col, val)),
                        ast::BinaryOperator::Lt => Ok(Filter::Lt(col, val)),
                        ast::BinaryOperator::GtEq => Ok(Filter::Gte(col, val)),
                        ast::BinaryOperator::LtEq => Ok(Filter::Lte(col, val)),
                        _ => unreachable!(),
                    }
                }
                // PostgreSQL regex operators: ~, ~*, !~, !~*
                ast::BinaryOperator::PGRegexMatch
                | ast::BinaryOperator::PGRegexIMatch
                | ast::BinaryOperator::PGRegexNotMatch
                | ast::BinaryOperator::PGRegexNotIMatch => {
                    // Convert regex operators to Expression filters using regexp_match function.
                    let col_expr = sql_expr_to_plan_expr(left)?;
                    let pattern_expr = sql_expr_to_plan_expr(right)?;
                    let case_insensitive = matches!(op, ast::BinaryOperator::PGRegexIMatch | ast::BinaryOperator::PGRegexNotIMatch);
                    let negated = matches!(op, ast::BinaryOperator::PGRegexNotMatch | ast::BinaryOperator::PGRegexNotIMatch);
                    let func_name = if case_insensitive { "regexp_match_ci" } else { "regexp_match" };
                    let match_expr = PlanExpr::Function {
                        name: func_name.to_string(),
                        args: vec![col_expr, pattern_expr],
                    };
                    let filter = Filter::Expression {
                        left: match_expr,
                        op: CompareOp::Eq,
                        right: PlanExpr::Literal(if negated { Value::I64(0) } else { Value::I64(1) }),
                    };
                    Ok(filter)
                }
                // PostgreSQL OPERATOR(pg_catalog.~) custom operator syntax
                ast::BinaryOperator::PGCustomBinaryOperator(parts) => {
                    // Map known custom operators to their built-in equivalents.
                    let op_name = parts.last().map(|s| s.as_str()).unwrap_or("");
                    match op_name {
                        "~" => {
                            let col_expr = sql_expr_to_plan_expr(left)?;
                            let pattern_expr = sql_expr_to_plan_expr(right)?;
                            Ok(Filter::Expression {
                                left: PlanExpr::Function {
                                    name: "regexp_match".to_string(),
                                    args: vec![col_expr, pattern_expr],
                                },
                                op: CompareOp::Eq,
                                right: PlanExpr::Literal(Value::I64(1)),
                            })
                        }
                        "~~" => {
                            // LIKE operator
                            let col = expr_to_col_name(left)?;
                            let val = expr_to_value(right)?;
                            match val {
                                Value::Str(pat) => Ok(Filter::Like(col, pat)),
                                _ => Err(ExchangeDbError::Query("LIKE pattern must be a string".into())),
                            }
                        }
                        "=" => {
                            let col = expr_to_col_name(left)?;
                            let val = expr_to_value(right)?;
                            Ok(Filter::Eq(col, val))
                        }
                        _ => Err(ExchangeDbError::Query(format!(
                            "unsupported custom operator: OPERATOR({op_name})"
                        ))),
                    }
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported binary operator: {other}"
                ))),
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let col = expr_to_col_name(expr)?;
            let low_val = expr_to_value(low)?;
            let high_val = expr_to_value(high)?;
            let between = Filter::Between(col, low_val, high_val);
            if *negated {
                Ok(Filter::Not(Box::new(between)))
            } else {
                Ok(between)
            }
        }
        Expr::Nested(inner) => expr_to_filter(inner),
        Expr::IsNull(inner) => {
            let col = expr_to_col_name(inner)?;
            Ok(Filter::IsNull(col))
        }
        Expr::IsNotNull(inner) => {
            let col = expr_to_col_name(inner)?;
            Ok(Filter::IsNotNull(col))
        }
        Expr::InList { expr, list, negated } => {
            let col = expr_to_col_name(expr)?;
            let values: Vec<Value> = list
                .iter()
                .map(|e| expr_to_value(e))
                .collect::<Result<Vec<_>>>()?;
            if *negated {
                Ok(Filter::NotIn(col, values))
            } else {
                Ok(Filter::In(col, values))
            }
        }
        Expr::Like { expr, pattern, negated, escape_char, .. } => {
            let col = expr_to_col_name(expr)?;
            let pat = match pattern.as_ref() {
                Expr::Value(v) => {
                    let val = sql_value_to_value(v)?;
                    match val {
                        Value::Str(s) => s,
                        other => return Err(ExchangeDbError::Query(format!(
                            "LIKE pattern must be a string, got: {other}"
                        ))),
                    }
                }
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported LIKE pattern expression: {other}"
                ))),
            };
            let pat = apply_like_escape(&pat, escape_char.as_deref());
            if *negated {
                Ok(Filter::NotLike(col, pat))
            } else {
                Ok(Filter::Like(col, pat))
            }
        }
        Expr::ILike { expr, pattern, negated, escape_char, .. } => {
            let col = expr_to_col_name(expr)?;
            let pat = match pattern.as_ref() {
                Expr::Value(v) => {
                    let val = sql_value_to_value(v)?;
                    match val {
                        Value::Str(s) => s,
                        other => return Err(ExchangeDbError::Query(format!(
                            "ILIKE pattern must be a string, got: {other}"
                        ))),
                    }
                }
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported ILIKE pattern expression: {other}"
                ))),
            };
            let pat = apply_like_escape(&pat, escape_char.as_deref());
            if *negated {
                // NOT ILIKE: treat as case-insensitive NOT LIKE — negate ILike
                // We don't have a dedicated NotILike, so use And with a negation workaround.
                // Simpler: just add NotLike with lowercased pattern (handled in executor).
                // Actually, let's just reuse ILike and wrap in a "not" via Or/And trick.
                // Better approach: add it as NotLike but that's case-sensitive.
                // For simplicity, we won't support NOT ILIKE for now — report error.
                Err(ExchangeDbError::Query("NOT ILIKE is not supported yet".into()))
            } else {
                Ok(Filter::ILike(col, pat))
            }
        }
        Expr::InSubquery { expr, subquery, negated } => {
            let col = expr_to_col_name(expr)?;
            let sub_plan = plan_select(subquery, None, None, false, None)?;
            Ok(Filter::InSubquery {
                column: col,
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan = plan_select(subquery, None, None, false, None)?;
            Ok(Filter::Exists {
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::UnaryOp { op: ast::UnaryOperator::Not, expr } => {
            let inner = expr_to_filter(expr)?;
            Ok(Filter::Not(Box::new(inner)))
        }
        // Standalone function call used as boolean condition (e.g. pg_table_is_visible(c.oid))
        Expr::Function(_) => {
            let func_expr = sql_expr_to_plan_expr(expr)?;
            Ok(Filter::Expression {
                left: func_expr,
                op: CompareOp::Eq,
                right: PlanExpr::Literal(Value::I64(1)),
            })
        }
        Expr::AllOp { left, compare_op, right } => {
            let col = expr_to_col_name(left)?;
            let compare = match compare_op {
                ast::BinaryOperator::Eq => CompareOp::Eq,
                ast::BinaryOperator::NotEq => CompareOp::NotEq,
                ast::BinaryOperator::Gt => CompareOp::Gt,
                ast::BinaryOperator::Lt => CompareOp::Lt,
                ast::BinaryOperator::GtEq => CompareOp::Gte,
                ast::BinaryOperator::LtEq => CompareOp::Lte,
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported ALL comparison operator: {other}"
                ))),
            };
            let subquery = match right.as_ref() {
                Expr::Subquery(q) => plan_select(q, None, None, false, None)?,
                other => return Err(ExchangeDbError::Query(format!(
                    "ALL requires a subquery, got: {other}"
                ))),
            };
            Ok(Filter::All { column: col, op: compare, subquery: Box::new(subquery) })
        }
        Expr::AnyOp { left, compare_op, right, .. } => {
            let col = expr_to_col_name(left)?;
            let compare = match compare_op {
                ast::BinaryOperator::Eq => CompareOp::Eq,
                ast::BinaryOperator::NotEq => CompareOp::NotEq,
                ast::BinaryOperator::Gt => CompareOp::Gt,
                ast::BinaryOperator::Lt => CompareOp::Lt,
                ast::BinaryOperator::GtEq => CompareOp::Gte,
                ast::BinaryOperator::LtEq => CompareOp::Lte,
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported ANY comparison operator: {other}"
                ))),
            };
            let subquery = match right.as_ref() {
                Expr::Subquery(q) => plan_select(q, None, None, false, None)?,
                other => return Err(ExchangeDbError::Query(format!(
                    "ANY requires a subquery, got: {other}"
                ))),
            };
            Ok(Filter::Any { column: col, op: compare, subquery: Box::new(subquery) })
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported filter expression: {other}"
        ))),
    }
}

/// Pre-process a LIKE pattern to handle the ESCAPE character.
///
/// When `escape_char` is specified, occurrences of `<esc>_` and `<esc>%` in the
/// pattern are replaced with sentinel bytes (`\x01` and `\x02` respectively)
/// so that `like_match_impl` treats them as literal characters rather than
/// wildcards. A doubled escape character (`<esc><esc>`) is replaced with a
/// single literal occurrence of the escape character.
fn apply_like_escape(pattern: &str, escape_char: Option<&str>) -> String {
    let esc = match escape_char {
        Some(s) if !s.is_empty() => s.chars().next().unwrap(),
        _ => return pattern.to_string(),
    };
    let chars: Vec<char> = pattern.chars().collect();
    let mut result = String::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == esc && i + 1 < chars.len() {
            match chars[i + 1] {
                '_' => {
                    result.push('\x01'); // sentinel for literal underscore
                    i += 2;
                }
                '%' => {
                    result.push('\x02'); // sentinel for literal percent
                    i += 2;
                }
                c if c == esc => {
                    result.push(esc);
                    i += 2;
                }
                _ => {
                    result.push(chars[i]);
                    i += 1;
                }
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

fn expr_to_col_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => {
            // "t.col" -> "t.col" (keep qualified for join context)
            Ok(parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join("."))
        }
        other => Err(ExchangeDbError::Query(format!(
            "expected column name, got: {other}"
        ))),
    }
}

fn expr_to_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => {
            // In sqlparser 0.55, Expr::Value wraps a Value directly.
            // Access it and convert to our internal Value type.
            sql_value_to_value(v)
        }
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => {
            let val = expr_to_value(expr)?;
            match val {
                Value::I64(n) => Ok(Value::I64(-n)),
                Value::F64(n) => Ok(Value::F64(-n)),
                _ => Err(ExchangeDbError::Query("cannot negate non-numeric value".into())),
            }
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported value expression: {other}"
        ))),
    }
}

/// Convert a sqlparser AST value to our internal Value type.
/// Accepts any type that dereferences to `ast::Value` (handles both
/// `ast::Value` directly and `ValueWithSpan` wrapper in newer sqlparser versions).
fn sql_value_to_value<V>(v: &V) -> Result<Value>
where
    V: std::fmt::Display + ?Sized,
{
    // Use Display-based conversion as a version-agnostic approach.
    // sqlparser's Value types all implement Display consistently.
    let s = v.to_string();

    // Try to detect the type from the Display output.
    if s == "NULL" {
        return Ok(Value::Null);
    }

    // Single-quoted strings: 'text'
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Ok(Value::Str(s[1..s.len() - 1].to_string()));
    }

    // Double-quoted strings: "text"
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Ok(Value::Str(s[1..s.len() - 1].to_string()));
    }

    // Numbers
    if s.contains('.') || s.contains('e') || s.contains('E') {
        if let Ok(f) = s.parse::<f64>() {
            return Ok(Value::F64(f));
        }
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(Value::I64(i));
    }

    Err(ExchangeDbError::Query(format!(
        "unsupported SQL value: {s}"
    )))
}

fn is_complex_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) => false,
        Expr::Function(func) => {
            let name = func.name.to_string();
            if AggregateKind::from_name(&name).is_some() { return false; }
            if func.over.is_some() { return false; }
            if crate::scalar::evaluate_scalar(&name, &[]).is_ok()
                || crate::scalar::ScalarRegistry::new().get(&name).is_some()
            { return false; }
            let lower = name.to_ascii_lowercase();
            if matches!(lower.as_str(), "version" | "current_database" | "current_schema" | "current_schemas") {
                return false;
            }
            true
        }
        Expr::BinaryOp { .. } | Expr::UnaryOp { .. } | Expr::Nested(_) => true,
        _ => false,
    }
}

fn sql_expr_to_plan_expr(expr: &Expr) -> Result<PlanExpr> {
    match expr {
        Expr::Identifier(ident) => Ok(PlanExpr::Column(ident.value.clone())),
        Expr::CompoundIdentifier(parts) => {
            Ok(PlanExpr::Column(parts.iter().map(|p| p.value.clone()).collect::<Vec<_>>().join(".")))
        }
        Expr::Value(v) => {
            let val = sql_value_to_value(v)?;
            Ok(PlanExpr::Literal(val))
        }
        Expr::BinaryOp { left, op, right } => {
            let plan_op = match op {
                ast::BinaryOperator::Plus => BinaryOp::Add,
                ast::BinaryOperator::Minus => BinaryOp::Sub,
                ast::BinaryOperator::Multiply => BinaryOp::Mul,
                ast::BinaryOperator::Divide => BinaryOp::Div,
                ast::BinaryOperator::Modulo => BinaryOp::Mod,
                ast::BinaryOperator::StringConcat => BinaryOp::Concat,
                ast::BinaryOperator::Gt => BinaryOp::Gt,
                ast::BinaryOperator::Lt => BinaryOp::Lt,
                ast::BinaryOperator::GtEq => BinaryOp::Gte,
                ast::BinaryOperator::LtEq => BinaryOp::Lte,
                ast::BinaryOperator::Eq => BinaryOp::Eq,
                ast::BinaryOperator::NotEq => BinaryOp::NotEq,
                ast::BinaryOperator::And => BinaryOp::And,
                ast::BinaryOperator::Or => BinaryOp::Or,
                other => {
                    return Err(ExchangeDbError::Query(format!(
                        "unsupported binary operator in expression: {other}"
                    )))
                }
            };
            Ok(PlanExpr::BinaryOp {
                left: Box::new(sql_expr_to_plan_expr(left)?),
                op: plan_op,
                right: Box::new(sql_expr_to_plan_expr(right)?),
            })
        }
        Expr::UnaryOp { op, expr } => {
            let plan_op = match op {
                ast::UnaryOperator::Minus => UnaryOp::Neg,
                ast::UnaryOperator::Not => UnaryOp::Not,
                other => {
                    return Err(ExchangeDbError::Query(format!(
                        "unsupported unary operator in expression: {other}"
                    )))
                }
            };
            Ok(PlanExpr::UnaryOp {
                op: plan_op,
                expr: Box::new(sql_expr_to_plan_expr(expr)?),
            })
        }
        Expr::Nested(inner) => sql_expr_to_plan_expr(inner),
        Expr::Cast { expr, data_type, .. } => {
            // Convert CAST(expr AS type) to a function call in expression context.
            let func_name = match data_type {
                ast::DataType::Integer(_) | ast::DataType::Int(_) | ast::DataType::BigInt(_)
                | ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) => "cast_to_int",
                ast::DataType::Float(_) | ast::DataType::Double(_)
                | ast::DataType::DoublePrecision | ast::DataType::Real
                | ast::DataType::Numeric(_) | ast::DataType::Decimal(_)
                | ast::DataType::Dec(_) => "cast_to_float",
                ast::DataType::Varchar(_) | ast::DataType::Char(_)
                | ast::DataType::Text | ast::DataType::String(_) => "cast_to_str",
                ast::DataType::Timestamp(_, _) => "cast_to_timestamp",
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported CAST target type in expression: {other}"
                ))),
            };
            let inner_expr = sql_expr_to_plan_expr(expr)?;
            Ok(PlanExpr::Function {
                name: func_name.to_string(),
                args: vec![inner_expr],
            })
        }
        Expr::CompoundFieldAccess { root, access_chain } => {
            // Handle array subscript: tags[1] -> split_part(tags, ',', 1)
            let col_name = match root.as_ref() {
                Expr::Identifier(ident) => ident.value.clone(),
                other => return Err(ExchangeDbError::Query(format!(
                    "unsupported compound field access root: {other}"
                ))),
            };
            if let Some(first_access) = access_chain.first() {
                match first_access {
                    ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => {
                        let index_expr = sql_expr_to_plan_expr(index)?;
                        // Convert to split_part(col, ',', index)
                        Ok(PlanExpr::Function {
                            name: "split_part".to_string(),
                            args: vec![
                                PlanExpr::Column(col_name),
                                PlanExpr::Literal(Value::Str(",".to_string())),
                                index_expr,
                            ],
                        })
                    }
                    _ => Err(ExchangeDbError::Query(format!(
                        "unsupported access expression on column '{col_name}'"
                    ))),
                }
            } else {
                Ok(PlanExpr::Column(col_name))
            }
        }
        Expr::Function(func) => {
            let name = func.name.to_string().to_ascii_lowercase();
            let args = match &func.args {
                ast::FunctionArguments::List(arg_list) => {
                    arg_list.args.iter().map(|arg| {
                        match arg {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                sql_expr_to_plan_expr(e)
                            }
                            other => Err(ExchangeDbError::Query(format!(
                                "unsupported function argument in expression: {other}"
                            ))),
                        }
                    }).collect::<Result<Vec<_>>>()?
                }
                ast::FunctionArguments::None => Vec::new(),
                other => {
                    return Err(ExchangeDbError::Query(format!(
                        "unsupported function arguments in expression: {other}"
                    )))
                }
            };
            Ok(PlanExpr::Function { name, args })
        }
        Expr::Case { operand: _, conditions, else_result } => {
            // Build a CASE WHEN expression using nested IIF-like functions.
            // We'll use a simple approach: evaluate condition, return THEN value or ELSE.
            // For now, handle the common "searched CASE" pattern.
            // We represent CASE WHEN as a chain: if(cond1, then1, if(cond2, then2, else))
            // Since we don't have a native CASE in PlanExpr, use a pseudo-function.
            let else_expr = if let Some(e) = else_result {
                sql_expr_to_plan_expr(e)?
            } else {
                PlanExpr::Literal(Value::Null)
            };
            // Build from inside out
            let mut result = else_expr;
            for case_when in conditions.iter().rev() {
                let then_expr = sql_expr_to_plan_expr(&case_when.result)?;
                let cond_expr = sql_expr_to_plan_expr(&case_when.condition)?;
                result = PlanExpr::Function {
                    name: "__case_when".to_string(),
                    args: vec![cond_expr, then_expr, result],
                };
            }
            Ok(result)
        }
        Expr::IsNull(expr) => {
            let inner = sql_expr_to_plan_expr(expr)?;
            Ok(PlanExpr::Function { name: "is_null".to_string(), args: vec![inner] })
        }
        Expr::IsNotNull(expr) => {
            let inner = sql_expr_to_plan_expr(expr)?;
            Ok(PlanExpr::Function { name: "is_not_null".to_string(), args: vec![inner] })
        }
        other => Err(ExchangeDbError::Query(format!(
            "unsupported expression: {other}"
        ))),
    }
}

fn is_complex_filter_side(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_) => false,
        Expr::CompoundIdentifier(_) => false,
        Expr::Value(_) => false,
        Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => {
            !matches!(expr.as_ref(), Expr::Value(_))
        }
        // CAST expressions (e.g., price::int, CAST(x AS INT)) are complex.
        Expr::Cast { .. } => true,
        // Array subscript is complex.
        Expr::CompoundFieldAccess { .. } => true,
        _ => true,
    }
}

/// Check if an expression is a timestamp or interval cast (e.g. `'2024-01-01'::timestamp`).
fn is_timestamp_or_interval_cast(expr: &Expr) -> bool {
    match expr {
        Expr::Cast { data_type, .. } => {
            let dt_str = format!("{data_type}").to_ascii_lowercase();
            dt_str.contains("timestamp") || dt_str.contains("interval")
        }
        _ => false,
    }
}

/// Parse a table function argument to i64, supporting integers, floats,
/// timestamps (as nanoseconds), and intervals (as nanoseconds).
fn table_func_arg_to_i64(expr: &Expr) -> Option<i64> {
    // Try direct value conversion first.
    if let Ok(v) = expr_to_value(expr) {
        match v {
            Value::I64(n) => return Some(n),
            Value::F64(n) => return Some(n as i64),
            Value::Timestamp(n) => return Some(n),
            _ => {}
        }
    }

    // Handle CAST expressions: '2024-01-01'::timestamp, '1 day'::interval.
    if let Expr::Cast { expr: inner, data_type, .. } = expr {
        let dt_str = format!("{data_type}").to_ascii_lowercase();

        if let Ok(inner_val) = expr_to_value(inner) {
            if let Value::Str(s) = inner_val {
                if dt_str.contains("timestamp") {
                    return parse_timestamp_str(&s);
                }
                if dt_str.contains("interval") {
                    return parse_interval_str(&s);
                }
            }
        }
    }

    None
}

/// Parse a timestamp string like "2024-01-01" or "2024-01-01T00:00:00" to
/// nanoseconds since Unix epoch.
fn parse_timestamp_str(s: &str) -> Option<i64> {
    // Try "YYYY-MM-DD" format.
    let parts: Vec<&str> = s.split('T').collect();
    let date_part = parts[0];
    let date_components: Vec<&str> = date_part.split('-').collect();
    if date_components.len() != 3 {
        return None;
    }

    let year: i64 = date_components[0].parse().ok()?;
    let month: i64 = date_components[1].parse().ok()?;
    let day: i64 = date_components[2].parse().ok()?;

    // Simple days-since-epoch calculation (sufficient for generate_series).
    // Using the algorithm from PostgreSQL's date2j.
    let mut y = year;
    let mut m = month;
    if m <= 2 {
        y -= 1;
        m += 12;
    }
    let days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * (m - 3) + 2) / 5 + day - 719469;
    let mut nanos = days * 86_400_000_000_000i64;

    // Parse optional time part.
    if parts.len() > 1 {
        let time_str = parts[1].trim_end_matches('Z');
        let time_parts: Vec<&str> = time_str.split(':').collect();
        if let Some(h) = time_parts.first().and_then(|s| s.parse::<i64>().ok()) {
            nanos += h * 3_600_000_000_000i64;
        }
        if let Some(min) = time_parts.get(1).and_then(|s| s.parse::<i64>().ok()) {
            nanos += min * 60_000_000_000i64;
        }
        if let Some(sec) = time_parts.get(2).and_then(|s| s.parse::<i64>().ok()) {
            nanos += sec * 1_000_000_000i64;
        }
    }

    Some(nanos)
}

/// Parse an interval string like "1 day", "2 hours", "30 minutes" to nanoseconds.
fn parse_interval_str(s: &str) -> Option<i64> {
    let s = s.trim().to_ascii_lowercase();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }

    let n: i64 = parts[0].parse().ok()?;
    let unit = parts[1].trim_end_matches('s'); // normalize "days" -> "day"

    let nanos_per_unit = match unit {
        "nanosecond" | "ns" => 1i64,
        "microsecond" | "us" => 1_000,
        "millisecond" | "ms" => 1_000_000,
        "second" | "sec" => 1_000_000_000,
        "minute" | "min" => 60_000_000_000,
        "hour" | "hr" => 3_600_000_000_000,
        "day" => 86_400_000_000_000,
        "week" => 7 * 86_400_000_000_000,
        "month" | "mon" => 30 * 86_400_000_000_000,
        "year" | "yr" => 365 * 86_400_000_000_000,
        _ => return None,
    };

    Some(n * nanos_per_unit)
}

/// Attempt to plan a table-valued function (`long_sequence(N)` or
/// `generate_series(start, stop, step)`).
fn try_plan_table_function(
    table_factor: &TableFactor,
    select: &ast::Select,
    _order_by: &[OrderBy],
    _limit: Option<u64>,
) -> Result<Option<QueryPlan>> {
    let (func_name, args) = match table_factor {
        TableFactor::Function { name, args, .. } => {
            (name.to_string().to_ascii_lowercase(), args.clone())
        }
        TableFactor::Table { name, args: Some(table_args), .. } => {
            (name.to_string().to_ascii_lowercase(), table_args.args.clone())
        }
        _ => return Ok(None),
    };

    // Handle read_parquet('path') table function.
    if func_name == "read_parquet" {
        let path_str = args
            .iter()
            .find_map(|arg| match arg {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                    expr_to_value(expr).ok().and_then(|v| match v {
                        Value::Str(s) => Some(s),
                        _ => None,
                    })
                }
                _ => None,
            })
            .ok_or_else(|| ExchangeDbError::Query("read_parquet requires a file path argument".into()))?;

        let columns = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::Wildcard(_) => Ok(SelectColumn::Wildcard),
                SelectItem::UnnamedExpr(expr) => select_expr_to_column_with_alias(expr, None),
                SelectItem::ExprWithAlias { expr, alias } => {
                    select_expr_to_column_with_alias(expr, Some(alias.value.clone()))
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported projection: {other}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        return Ok(Some(QueryPlan::ReadParquet {
            path: PathBuf::from(path_str),
            columns,
        }));
    }

    // Handle read_csv('path') table function.
    if func_name == "read_csv" {
        let path_str = args
            .iter()
            .find_map(|arg| match arg {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                    expr_to_value(expr).ok().and_then(|v| match v {
                        Value::Str(s) => Some(s),
                        _ => None,
                    })
                }
                _ => None,
            })
            .ok_or_else(|| ExchangeDbError::Query("read_csv requires a file path argument".into()))?;

        let columns = select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::Wildcard(_) => Ok(SelectColumn::Wildcard),
                SelectItem::UnnamedExpr(expr) => select_expr_to_column_with_alias(expr, None),
                SelectItem::ExprWithAlias { expr, alias } => {
                    select_expr_to_column_with_alias(expr, Some(alias.value.clone()))
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported projection: {other}"
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        return Ok(Some(QueryPlan::ReadCsv {
            path: PathBuf::from(path_str),
            columns,
        }));
    }

    if func_name != "long_sequence" && func_name != "generate_series" {
        return Ok(None);
    }

    // Extract argument values, supporting integers, floats, timestamps, and intervals.
    let arg_values: Vec<i64> = args
        .iter()
        .filter_map(|arg| {
            match arg {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                    table_func_arg_to_i64(expr)
                }
                _ => None,
            }
        })
        .collect();

    // Detect whether any argument is a timestamp or interval cast.
    let has_timestamp_args = args.iter().any(|arg| match arg {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => is_timestamp_or_interval_cast(expr),
        _ => false,
    });

    let columns = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::Wildcard(_) => Ok(SelectColumn::Wildcard),
            SelectItem::UnnamedExpr(expr) => select_expr_to_column_with_alias(expr, None),
            SelectItem::ExprWithAlias { expr, alias } => {
                select_expr_to_column_with_alias(expr, Some(alias.value.clone()))
            }
            other => Err(ExchangeDbError::Query(format!(
                "unsupported projection: {other}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;

    if func_name == "long_sequence" {
        if arg_values.is_empty() {
            return Err(ExchangeDbError::Query("long_sequence requires one argument".into()));
        }
        let count = arg_values[0].max(0) as u64;
        return Ok(Some(QueryPlan::LongSequence { count, columns }));
    }

    // generate_series(start, stop[, step])
    if arg_values.len() < 2 {
        return Err(ExchangeDbError::Query(
            "generate_series requires at least 2 arguments".into(),
        ));
    }
    let start = arg_values[0];
    let stop = arg_values[1];
    let step = if arg_values.len() >= 3 { arg_values[2] } else { 1 };
    if step == 0 {
        return Err(ExchangeDbError::Query(
            "generate_series step cannot be 0".into(),
        ));
    }

    Ok(Some(QueryPlan::GenerateSeries {
        start,
        stop,
        step,
        columns,
        is_timestamp: has_timestamp_args,
    }))
}

/// Check if a SQL projection list contains aggregate function calls.
fn join_projection_has_aggregates(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_is_aggregate(expr)
        }
        _ => false,
    })
}

/// Recursively check if an expression contains an aggregate function call.
fn expr_is_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => {
            let name = func.name.to_string();
            AggregateKind::from_name(&name).is_some()
        }
        _ => false,
    }
}

/// Extract GROUP BY column names for a JOIN query.
/// Handles both bare identifiers and compound identifiers (e.g., "o.symbol" -> "symbol").
fn extract_group_by_for_join(select: &ast::Select) -> Result<Vec<String>> {
    match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().map(|e| match e {
                Expr::Identifier(ident) => Ok(ident.value.clone()),
                Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                    // "alias.column" -> use just the column name for virtual source lookup.
                    Ok(parts[1].value.clone())
                }
                other => Err(ExchangeDbError::Query(format!(
                    "unsupported GROUP BY expression: {other}"
                ))),
            }).collect()
        }
        ast::GroupByExpr::All(_) => {
            Err(ExchangeDbError::Query("GROUP BY ALL is not supported".into()))
        }
    }
}

/// Convert a SQL expression from a JOIN query projection into a `SelectColumn`,
/// handling compound identifiers (stripping table alias prefixes).
fn join_select_expr_to_column(expr: &Expr, alias: Option<String>) -> Result<SelectColumn> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let col_name = parts[1].value.clone();
            if let Some(a) = alias {
                Ok(SelectColumn::Expression {
                    expr: PlanExpr::Column(col_name),
                    alias: Some(a),
                })
            } else {
                Ok(SelectColumn::Name(col_name))
            }
        }
        Expr::Function(func) => {
            let func_name = func.name.to_string();
            // Check for aggregate functions.
            if let Some(kind) = AggregateKind::from_name(&func_name) {
                let (col_name, arg_expr_parsed) = match &func.args {
                    ast::FunctionArguments::List(arg_list) => {
                        if arg_list.args.len() != 1 {
                            return Err(ExchangeDbError::Query(format!(
                                "{func_name} expects exactly one argument"
                            )));
                        }
                        match &arg_list.args[0] {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => ("*".to_string(), None),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            )) => (ident.value.clone(), None),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::CompoundIdentifier(parts),
                            )) if parts.len() == 2 => {
                                // "f.filled_qty" -> "filled_qty"
                                (parts[1].value.clone(), None)
                            }
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                                // Expression argument like sum(f.price * f.filled) or count(CASE WHEN ...)
                                let plan_expr = sql_expr_to_plan_expr(expr)?;
                                let col_refs = plan_expr_column_refs(&plan_expr);
                                let nominal_col = col_refs.into_iter().next().unwrap_or_else(|| "*".to_string());
                                (nominal_col, Some(plan_expr))
                            }
                            other => {
                                return Err(ExchangeDbError::Query(format!(
                                    "unsupported function argument: {other}"
                                )))
                            }
                        }
                    }
                    _ => {
                        return Err(ExchangeDbError::Query(format!(
                            "unsupported function arguments for {func_name}"
                        )))
                    }
                };
                return Ok(SelectColumn::Aggregate {
                    function: kind,
                    column: col_name,
                    alias,
                    filter: None,
                    within_group_order: None,
                    arg_expr: arg_expr_parsed,
                });
            }
            // Fall back to the standard column parser.
            select_expr_to_column(expr, alias)
        }
        _ => select_expr_to_column(expr, alias),
    }
}

/// Plan a PIVOT query from pre-extracted pivot metadata.
fn plan_pivot(query: &ast::Query, info: &PivotInfo) -> Result<QueryPlan> {
    let source = plan_select(query, None, None, false, None)?;
    let agg_kind = AggregateKind::from_name(&info.aggregate)
        .ok_or_else(|| ExchangeDbError::Query(format!(
            "unsupported aggregate function in PIVOT: {}", info.aggregate
        )))?;
    let values = info.values.iter().map(|(v, a)| PivotValue {
        value: Value::Str(v.clone()),
        alias: a.clone(),
    }).collect();
    Ok(QueryPlan::Pivot {
        source: Box::new(source),
        aggregate: agg_kind,
        agg_column: info.agg_column.clone(),
        pivot_col: info.pivot_col.clone(),
        values,
    })
}

/// Plan a MERGE statement from pre-extracted merge metadata.
fn plan_merge(info: &MergeInfo) -> Result<QueryPlan> {
    let mut when_clauses = Vec::new();

    if let Some(ref updates) = info.matched_update {
        let assignments: Vec<(String, PlanExpr)> = updates.iter().map(|(col, expr_str)| {
            // Try to parse the expression. If it's a qualified name like "source.price",
            // extract the column name.
            let trimmed = expr_str.trim();
            let expr = if trimmed.contains('.') {
                let parts: Vec<&str> = trimmed.splitn(2, '.').collect();
                PlanExpr::Column(parts.last().unwrap_or(&trimmed).to_string())
            } else if let Ok(val) = trimmed.parse::<i64>() {
                PlanExpr::Literal(Value::I64(val))
            } else if let Ok(val) = trimmed.parse::<f64>() {
                PlanExpr::Literal(Value::F64(val))
            } else {
                PlanExpr::Column(trimmed.to_string())
            };
            (col.clone(), expr)
        }).collect();
        when_clauses.push(MergeWhen::MatchedUpdate { assignments });
    }

    if info.matched_delete {
        when_clauses.push(MergeWhen::MatchedDelete);
    }

    if let Some(ref vals) = info.not_matched_values {
        let exprs: Vec<PlanExpr> = vals.iter().map(|v| {
            let trimmed = v.trim();
            if trimmed.contains('.') {
                let parts: Vec<&str> = trimmed.splitn(2, '.').collect();
                PlanExpr::Column(parts.last().unwrap_or(&trimmed).to_string())
            } else if let Ok(val) = trimmed.parse::<i64>() {
                PlanExpr::Literal(Value::I64(val))
            } else if let Ok(val) = trimmed.parse::<f64>() {
                PlanExpr::Literal(Value::F64(val))
            } else {
                PlanExpr::Column(trimmed.to_string())
            }
        }).collect();
        when_clauses.push(MergeWhen::NotMatchedInsert { values: exprs });
    }

    Ok(QueryPlan::Merge {
        target_table: info.target_table.clone(),
        source_table: info.source_table.clone(),
        on_column: (info.on_left.clone(), info.on_right.clone()),
        when_clauses,
    })
}

/// Collect all column name references from a PlanExpr.
fn plan_expr_column_refs(expr: &PlanExpr) -> Vec<String> {
    let mut refs = Vec::new();
    collect_plan_expr_cols(expr, &mut refs);
    refs
}

fn collect_plan_expr_cols(expr: &PlanExpr, out: &mut Vec<String>) {
    match expr {
        PlanExpr::Column(name) => { if !out.contains(name) { out.push(name.clone()); } }
        PlanExpr::Literal(_) => {}
        PlanExpr::BinaryOp { left, right, .. } => { collect_plan_expr_cols(left, out); collect_plan_expr_cols(right, out); }
        PlanExpr::UnaryOp { expr, .. } => { collect_plan_expr_cols(expr, out); }
        PlanExpr::Function { args, .. } => { for a in args { collect_plan_expr_cols(a, out); } }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn plan_create_table() {
        let plan = plan_query(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
        )
        .unwrap();

        match plan {
            QueryPlan::CreateTable {
                name,
                columns,
                timestamp_col,
                ..
            } => {
                assert_eq!(name, "trades");
                assert_eq!(columns.len(), 4);
                assert_eq!(columns[0].name, "timestamp");
                assert_eq!(columns[0].type_name, "TIMESTAMP");
                assert_eq!(columns[1].name, "symbol");
                assert_eq!(timestamp_col, Some("timestamp".to_string()));
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn plan_insert() {
        let plan =
            plan_query("INSERT INTO trades VALUES (1000000, 'BTC', 65000.0, 1.5)").unwrap();

        match plan {
            QueryPlan::Insert {
                table, values, ..
            } => {
                assert_eq!(table, "trades");
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 4);
                assert_eq!(values[0][0], Value::I64(1000000));
                assert_eq!(values[0][1], Value::Str("BTC".into()));
                assert_eq!(values[0][2], Value::F64(65000.0));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_simple() {
        let plan = plan_query("SELECT * FROM trades").unwrap();

        match plan {
            QueryPlan::Select {
                table,
                columns,
                filter,
                ..
            } => {
                assert_eq!(table, "trades");
                assert_eq!(columns, vec![SelectColumn::Wildcard]);
                assert!(filter.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_with_where() {
        let plan = plan_query("SELECT price, volume FROM trades WHERE price > 100 AND volume <= 10")
            .unwrap();

        match plan {
            QueryPlan::Select {
                columns, filter, ..
            } => {
                assert_eq!(columns.len(), 2);
                match filter.unwrap() {
                    Filter::And(parts) => {
                        assert_eq!(parts.len(), 2);
                        assert_eq!(parts[0], Filter::Gt("price".into(), Value::I64(100)));
                        assert_eq!(parts[1], Filter::Lte("volume".into(), Value::I64(10)));
                    }
                    other => panic!("expected And filter, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_with_between() {
        let plan =
            plan_query("SELECT * FROM trades WHERE price BETWEEN 100 AND 200").unwrap();

        match plan {
            QueryPlan::Select { filter, .. } => {
                assert_eq!(
                    filter.unwrap(),
                    Filter::Between("price".into(), Value::I64(100), Value::I64(200))
                );
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_with_order_limit() {
        let plan =
            plan_query("SELECT * FROM trades ORDER BY price DESC LIMIT 10").unwrap();

        match plan {
            QueryPlan::Select {
                order_by, limit, ..
            } => {
                assert_eq!(order_by.len(), 1);
                assert_eq!(order_by[0].column, "price");
                assert!(order_by[0].descending);
                assert_eq!(limit, Some(10));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_with_offset() {
        let plan =
            plan_query("SELECT * FROM trades LIMIT 5 OFFSET 10").unwrap();

        match plan {
            QueryPlan::Select {
                limit, offset, ..
            } => {
                assert_eq!(limit, Some(5));
                assert_eq!(offset, Some(10));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_offset_without_limit() {
        let plan =
            plan_query("SELECT * FROM trades OFFSET 3").unwrap();

        match plan {
            QueryPlan::Select {
                limit, offset, ..
            } => {
                assert_eq!(limit, None);
                assert_eq!(offset, Some(3));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_with_sample_by() {
        let plan = plan_query(
            "SELECT symbol, avg(price) FROM trades SAMPLE BY 1h ORDER BY timestamp",
        )
        .unwrap();

        match plan {
            QueryPlan::Select {
                columns,
                sample_by,
                order_by,
                ..
            } => {
                assert_eq!(columns.len(), 2);
                assert!(matches!(
                    &columns[1],
                    SelectColumn::Aggregate {
                        function: AggregateKind::Avg,
                        column,
                        ..
                    } if column == "price"
                ));
                assert_eq!(
                    sample_by.unwrap().interval,
                    Duration::from_secs(3600)
                );
                assert_eq!(order_by.len(), 1);
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_select_with_aggregate_functions() {
        let plan = plan_query(
            "SELECT count(*), sum(volume), min(price), max(price), first(price), last(price) FROM trades",
        )
        .unwrap();

        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 6);
                assert!(matches!(
                    &columns[0],
                    SelectColumn::Aggregate { function: AggregateKind::Count, .. }
                ));
                assert!(matches!(
                    &columns[4],
                    SelectColumn::Aggregate { function: AggregateKind::First, .. }
                ));
                assert!(matches!(
                    &columns[5],
                    SelectColumn::Aggregate { function: AggregateKind::Last, .. }
                ));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_negative_value() {
        let plan =
            plan_query("SELECT * FROM trades WHERE price > -100").unwrap();

        match plan {
            QueryPlan::Select { filter, .. } => {
                assert_eq!(
                    filter.unwrap(),
                    Filter::Gt("price".into(), Value::I64(-100))
                );
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_asof_join() {
        let plan = plan_query(
            "SELECT t.*, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON (t.symbol = q.symbol)",
        )
        .unwrap();

        match plan {
            QueryPlan::AsofJoin {
                left_table,
                right_table,
                left_columns,
                right_columns,
                on_columns,
                ..
            } => {
                assert_eq!(left_table, "trades");
                assert_eq!(right_table, "quotes");
                assert_eq!(left_columns, vec![SelectColumn::Wildcard]);
                assert_eq!(
                    right_columns,
                    vec![
                        SelectColumn::Name("bid".into()),
                        SelectColumn::Name("ask".into()),
                    ]
                );
                assert_eq!(
                    on_columns,
                    vec![("symbol".to_string(), "symbol".to_string())]
                );
            }
            other => panic!("expected AsofJoin, got {other:?}"),
        }
    }

    #[test]
    fn plan_latest_on() {
        let plan = plan_query(
            "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
        )
        .unwrap();

        match plan {
            QueryPlan::Select { latest_on, table, .. } => {
                assert_eq!(table, "trades");
                let lo = latest_on.unwrap();
                assert_eq!(lo.timestamp_col, "timestamp");
                assert_eq!(lo.partition_col, "symbol");
            }
            other => panic!("expected Select with latest_on, got {other:?}"),
        }
    }

    #[test]
    fn plan_alter_table_add_column() {
        let plan = plan_query("ALTER TABLE trades ADD COLUMN exchange VARCHAR").unwrap();
        match plan {
            QueryPlan::AddColumn {
                table,
                column_name,
                column_type,
            } => {
                assert_eq!(table, "trades");
                assert_eq!(column_name, "exchange");
                // sqlparser may normalize VARCHAR to CHARACTER VARYING
                assert!(column_type == "VARCHAR" || column_type == "CHARACTER VARYING",
                    "unexpected column_type: {column_type}");
            }
            other => panic!("expected AddColumn, got {other:?}"),
        }
    }

    #[test]
    fn plan_alter_table_drop_column() {
        let plan = plan_query("ALTER TABLE trades DROP COLUMN exchange").unwrap();
        match plan {
            QueryPlan::DropColumn {
                table,
                column_name,
            } => {
                assert_eq!(table, "trades");
                assert_eq!(column_name, "exchange");
            }
            other => panic!("expected DropColumn, got {other:?}"),
        }
    }

    #[test]
    fn plan_alter_table_rename_column() {
        let plan =
            plan_query("ALTER TABLE trades RENAME COLUMN price TO trade_price").unwrap();
        match plan {
            QueryPlan::RenameColumn {
                table,
                old_name,
                new_name,
            } => {
                assert_eq!(table, "trades");
                assert_eq!(old_name, "price");
                assert_eq!(new_name, "trade_price");
            }
            other => panic!("expected RenameColumn, got {other:?}"),
        }
    }

    #[test]
    fn plan_alter_table_set_type() {
        let plan =
            plan_query("ALTER TABLE trades ALTER COLUMN price SET DATA TYPE DOUBLE PRECISION")
                .unwrap();
        match plan {
            QueryPlan::SetColumnType {
                table,
                column_name,
                new_type,
            } => {
                assert_eq!(table, "trades");
                assert_eq!(column_name, "price");
                assert_eq!(new_type, "DOUBLE PRECISION");
            }
            other => panic!("expected SetColumnType, got {other:?}"),
        }
    }

    #[test]
    fn plan_drop_table() {
        let plan = plan_query("DROP TABLE trades").unwrap();
        match plan {
            QueryPlan::DropTable { table, if_exists } => {
                assert_eq!(table, "trades");
                assert!(!if_exists);
            }
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn plan_group_by_single_key() {
        let plan = plan_query(
            "SELECT symbol, sum(volume) FROM trades GROUP BY symbol",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { group_by, columns, .. } => {
                assert_eq!(group_by, vec!["symbol".to_string()]);
                assert_eq!(columns.len(), 2);
                assert!(matches!(&columns[0], SelectColumn::Name(n) if n == "symbol"));
                assert!(matches!(
                    &columns[1],
                    SelectColumn::Aggregate { function: AggregateKind::Sum, column, .. }
                    if column == "volume"
                ));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_group_by_multiple_keys() {
        let plan = plan_query(
            "SELECT symbol, exchange, avg(price) FROM trades GROUP BY symbol, exchange",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { group_by, columns, .. } => {
                assert_eq!(group_by, vec!["symbol".to_string(), "exchange".to_string()]);
                assert_eq!(columns.len(), 3);
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_group_by_with_having() {
        let plan = plan_query(
            "SELECT symbol, count(*) FROM trades GROUP BY symbol HAVING count(*) > 10",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { group_by, having, .. } => {
                assert_eq!(group_by, vec!["symbol".to_string()]);
                assert!(having.is_some());
                let h = having.unwrap();
                assert_eq!(h, Filter::Gt("count(*)".into(), Value::I64(10)));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_distinct() {
        let plan = plan_query("SELECT DISTINCT symbol FROM trades").unwrap();
        match plan {
            QueryPlan::Select { distinct, columns, .. } => {
                assert!(distinct);
                assert_eq!(columns.len(), 1);
                assert!(matches!(&columns[0], SelectColumn::Name(n) if n == "symbol"));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_non_distinct() {
        let plan = plan_query("SELECT symbol FROM trades").unwrap();
        match plan {
            QueryPlan::Select { distinct, .. } => {
                assert!(!distinct);
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_inner_join() {
        let plan = plan_query(
            "SELECT t.symbol, t.price, m.name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol",
        )
        .unwrap();
        match plan {
            QueryPlan::Join {
                left_table,
                right_table,
                left_alias,
                right_alias,
                columns,
                join_type,
                on_columns,
                ..
            } => {
                assert_eq!(left_table, "trades");
                assert_eq!(right_table, "markets");
                assert_eq!(left_alias, Some("t".to_string()));
                assert_eq!(right_alias, Some("m".to_string()));
                assert_eq!(join_type, JoinType::Inner);
                assert_eq!(columns.len(), 3);
                assert_eq!(on_columns.len(), 1);
                // ON columns come as "t.symbol" = "m.symbol"
                assert!(on_columns[0].0.contains("symbol"));
                assert!(on_columns[0].1.contains("symbol"));
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn plan_left_join() {
        let plan = plan_query(
            "SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol",
        )
        .unwrap();
        match plan {
            QueryPlan::Join {
                join_type,
                left_table,
                right_table,
                ..
            } => {
                assert_eq!(join_type, JoinType::Left);
                assert_eq!(left_table, "trades");
                assert_eq!(right_table, "markets");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn plan_delete_with_filter() {
        let plan = plan_query("DELETE FROM trades WHERE price < 50000").unwrap();
        match plan {
            QueryPlan::Delete { table, filter } => {
                assert_eq!(table, "trades");
                assert_eq!(filter.unwrap(), Filter::Lt("price".into(), Value::I64(50000)));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn plan_delete_all() {
        let plan = plan_query("DELETE FROM trades").unwrap();
        match plan {
            QueryPlan::Delete { table, filter } => {
                assert_eq!(table, "trades");
                assert!(filter.is_none());
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn plan_delete_compound_filter() {
        let plan = plan_query(
            "DELETE FROM trades WHERE symbol = 'BTC/USD' AND price < 50000",
        )
        .unwrap();
        match plan {
            QueryPlan::Delete { table, filter } => {
                assert_eq!(table, "trades");
                match filter.unwrap() {
                    Filter::And(parts) => {
                        assert_eq!(parts.len(), 2);
                        assert_eq!(parts[0], Filter::Eq("symbol".into(), Value::Str("BTC/USD".into())));
                        assert_eq!(parts[1], Filter::Lt("price".into(), Value::I64(50000)));
                    }
                    other => panic!("expected And filter, got {other:?}"),
                }
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn plan_update_simple() {
        let plan = plan_query("UPDATE trades SET price = 100.0 WHERE symbol = 'BTC/USD'").unwrap();
        match plan {
            QueryPlan::Update { table, assignments, filter } => {
                assert_eq!(table, "trades");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "price");
                assert_eq!(assignments[0].1, PlanExpr::Literal(Value::F64(100.0)));
                assert_eq!(filter.unwrap(), Filter::Eq("symbol".into(), Value::Str("BTC/USD".into())));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn plan_update_no_filter() {
        let plan = plan_query("UPDATE trades SET price = 0").unwrap();
        match plan {
            QueryPlan::Update { table, assignments, filter } => {
                assert_eq!(table, "trades");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "price");
                assert_eq!(assignments[0].1, PlanExpr::Literal(Value::I64(0)));
                assert!(filter.is_none());
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn plan_insert_or_replace() {
        let plan = plan_query(
            "INSERT OR REPLACE INTO trades (timestamp, symbol, price) VALUES (1710000000, 'BTC/USD', 65000.0)",
        )
        .unwrap();
        match plan {
            QueryPlan::Insert { table, columns, values, upsert } => {
                assert_eq!(table, "trades");
                assert!(upsert);
                assert_eq!(columns, vec!["timestamp", "symbol", "price"]);
                assert_eq!(values.len(), 1);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn plan_insert_normal_not_upsert() {
        let plan = plan_query(
            "INSERT INTO trades VALUES (1710000000, 'BTC/USD', 65000.0)",
        )
        .unwrap();
        match plan {
            QueryPlan::Insert { upsert, .. } => {
                assert!(!upsert);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn plan_window_row_number() {
        let plan = plan_query(
            "SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY timestamp) as rn FROM trades",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 3);
                assert!(matches!(&columns[0], SelectColumn::Name(n) if n == "symbol"));
                assert!(matches!(&columns[1], SelectColumn::Name(n) if n == "price"));
                match &columns[2] {
                    SelectColumn::WindowFunction(wf) => {
                        assert_eq!(wf.name, "row_number");
                        assert_eq!(wf.over.partition_by, vec!["symbol".to_string()]);
                        assert_eq!(wf.over.order_by.len(), 1);
                        assert_eq!(wf.over.order_by[0].column, "timestamp");
                        assert!(!wf.over.order_by[0].descending);
                        assert_eq!(wf.alias, Some("rn".to_string()));
                    }
                    other => panic!("expected WindowFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_window_lag() {
        let plan = plan_query(
            "SELECT lag(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price FROM trades",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::WindowFunction(wf) => {
                        assert_eq!(wf.name, "lag");
                        assert_eq!(wf.args.len(), 2);
                        assert_eq!(wf.alias, Some("prev_price".to_string()));
                    }
                    other => panic!("expected WindowFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_window_cumulative_sum() {
        let plan = plan_query(
            "SELECT sum(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumvol FROM trades",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::WindowFunction(wf) => {
                        assert_eq!(wf.name, "sum");
                        assert!(wf.over.frame.is_some());
                        assert_eq!(wf.alias, Some("cumvol".to_string()));
                    }
                    other => panic!("expected WindowFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_window_rank_desc() {
        let plan = plan_query(
            "SELECT rank() OVER (ORDER BY price DESC) as price_rank FROM trades",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::WindowFunction(wf) => {
                        assert_eq!(wf.name, "rank");
                        assert!(wf.over.partition_by.is_empty());
                        assert_eq!(wf.over.order_by.len(), 1);
                        assert!(wf.over.order_by[0].descending);
                        assert_eq!(wf.alias, Some("price_rank".to_string()));
                    }
                    other => panic!("expected WindowFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    // =====================================================================
    // CTE, set operation, and subquery planner tests
    // =====================================================================

    #[test]
    fn plan_simple_cte() {
        let plan = plan_query(
            "WITH active AS (SELECT * FROM trades WHERE volume > 10) \
             SELECT symbol FROM active",
        ).unwrap();
        match plan {
            QueryPlan::WithCte { ctes, body } => {
                assert_eq!(ctes.len(), 1);
                assert_eq!(ctes[0].name, "active");
                match body.as_ref() {
                    QueryPlan::Select { table, .. } => {
                        assert_eq!(table, "active");
                    }
                    other => panic!("expected Select body, got {other:?}"),
                }
            }
            other => panic!("expected WithCte, got {other:?}"),
        }
    }

    #[test]
    fn plan_union_all() {
        let plan = plan_query(
            "SELECT symbol FROM trades WHERE symbol = 'BTC' \
             UNION ALL \
             SELECT symbol FROM trades WHERE symbol = 'ETH'",
        ).unwrap();
        match plan {
            QueryPlan::SetOperation { op, all, .. } => {
                assert_eq!(op, SetOp::Union);
                assert!(all);
            }
            other => panic!("expected SetOperation, got {other:?}"),
        }
    }

    #[test]
    fn plan_union_distinct() {
        let plan = plan_query(
            "SELECT symbol FROM trades \
             UNION \
             SELECT symbol FROM trades",
        ).unwrap();
        match plan {
            QueryPlan::SetOperation { op, all, .. } => {
                assert_eq!(op, SetOp::Union);
                assert!(!all);
            }
            other => panic!("expected SetOperation, got {other:?}"),
        }
    }

    #[test]
    fn plan_intersect() {
        let plan = plan_query(
            "SELECT symbol FROM trades \
             INTERSECT \
             SELECT symbol FROM quotes",
        ).unwrap();
        match plan {
            QueryPlan::SetOperation { op, all, .. } => {
                assert_eq!(op, SetOp::Intersect);
                assert!(!all);
            }
            other => panic!("expected SetOperation, got {other:?}"),
        }
    }

    #[test]
    fn plan_except() {
        let plan = plan_query(
            "SELECT symbol FROM trades \
             EXCEPT \
             SELECT symbol FROM quotes",
        ).unwrap();
        match plan {
            QueryPlan::SetOperation { op, all, .. } => {
                assert_eq!(op, SetOp::Except);
                assert!(!all);
            }
            other => panic!("expected SetOperation, got {other:?}"),
        }
    }

    #[test]
    fn plan_scalar_subquery_in_where() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE price > (SELECT avg(price) FROM trades)",
        ).unwrap();
        match plan {
            QueryPlan::Select { filter, .. } => {
                match filter.unwrap() {
                    Filter::Subquery { column, op, subquery } => {
                        assert_eq!(column, "price");
                        assert_eq!(op, CompareOp::Gt);
                        // The subquery should be a Select with avg(price)
                        match subquery.as_ref() {
                            QueryPlan::Select { columns, .. } => {
                                assert!(matches!(&columns[0], SelectColumn::Aggregate {
                                    function: AggregateKind::Avg, ..
                                }));
                            }
                            other => panic!("expected Select subquery, got {other:?}"),
                        }
                    }
                    other => panic!("expected Subquery filter, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_derived_table() {
        let plan = plan_query(
            "SELECT symbol, avg_price FROM \
             (SELECT symbol, avg(price) as avg_price FROM trades GROUP BY symbol) sub \
             WHERE avg_price > 100",
        ).unwrap();
        match plan {
            QueryPlan::DerivedScan { alias, columns, filter, .. } => {
                assert_eq!(alias, "sub");
                assert_eq!(columns.len(), 2);
                assert!(filter.is_some());
            }
            other => panic!("expected DerivedScan, got {other:?}"),
        }
    }

    // ── IS NULL / IS NOT NULL tests ────────────────────────────────

    #[test]
    fn plan_is_null_filter() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol IS NULL").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::IsNull(col)), .. } => {
                assert_eq!(col, "symbol");
            }
            other => panic!("expected Select with IsNull filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_is_not_null_filter() {
        let plan = plan_query("SELECT * FROM trades WHERE price IS NOT NULL").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::IsNotNull(col)), .. } => {
                assert_eq!(col, "price");
            }
            other => panic!("expected Select with IsNotNull filter, got {other:?}"),
        }
    }

    // ── IN / NOT IN tests ──────────────────────────────────────────

    #[test]
    fn plan_in_string_list() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::In(col, values)), .. } => {
                assert_eq!(col, "symbol");
                assert_eq!(values, vec![Value::Str("BTC/USD".into()), Value::Str("ETH/USD".into())]);
            }
            other => panic!("expected Select with In filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_in_numeric_list() {
        let plan = plan_query("SELECT * FROM trades WHERE price IN (100, 200, 300)").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::In(col, values)), .. } => {
                assert_eq!(col, "price");
                assert_eq!(values, vec![Value::I64(100), Value::I64(200), Value::I64(300)]);
            }
            other => panic!("expected Select with In filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_not_in() {
        let plan = plan_query("SELECT * FROM trades WHERE price NOT IN (100, 200)").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::NotIn(col, values)), .. } => {
                assert_eq!(col, "price");
                assert_eq!(values, vec![Value::I64(100), Value::I64(200)]);
            }
            other => panic!("expected Select with NotIn filter, got {other:?}"),
        }
    }

    // ── LIKE / ILIKE tests ─────────────────────────────────────────

    #[test]
    fn plan_like_percent() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol LIKE 'BTC%'").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Like(col, pat)), .. } => {
                assert_eq!(col, "symbol");
                assert_eq!(pat, "BTC%");
            }
            other => panic!("expected Select with Like filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_like_underscore() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol LIKE 'BTC_USD'").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Like(col, pat)), .. } => {
                assert_eq!(col, "symbol");
                assert_eq!(pat, "BTC_USD");
            }
            other => panic!("expected Select with Like filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_not_like() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol NOT LIKE 'ETH%'").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::NotLike(col, pat)), .. } => {
                assert_eq!(col, "symbol");
                assert_eq!(pat, "ETH%");
            }
            other => panic!("expected Select with NotLike filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_ilike() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol ILIKE '%usd'").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::ILike(col, pat)), .. } => {
                assert_eq!(col, "symbol");
                assert_eq!(pat, "%usd");
            }
            other => panic!("expected Select with ILike filter, got {other:?}"),
        }
    }

    // ── CASE WHEN tests ────────────────────────────────────────────

    #[test]
    fn plan_case_when_searched() {
        let plan = plan_query(
            "SELECT CASE WHEN price > 100 THEN 'high' WHEN price > 50 THEN 'mid' ELSE 'low' END FROM trades"
        ).unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::CaseWhen { conditions, else_value, .. } => {
                        assert_eq!(conditions.len(), 2);
                        assert_eq!(conditions[0].1, Value::Str("high".into()));
                        assert_eq!(conditions[1].1, Value::Str("mid".into()));
                        assert_eq!(*else_value, Some(Value::Str("low".into())));
                    }
                    other => panic!("expected CaseWhen, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_case_when_simple() {
        let plan = plan_query(
            "SELECT CASE symbol WHEN 'BTC/USD' THEN 'Bitcoin' WHEN 'ETH/USD' THEN 'Ethereum' ELSE 'Other' END FROM trades"
        ).unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::CaseWhen { conditions, else_value, .. } => {
                        assert_eq!(conditions.len(), 2);
                        // Simple CASE is converted to Eq filters
                        assert_eq!(conditions[0].0, Filter::Eq("symbol".into(), Value::Str("BTC/USD".into())));
                        assert_eq!(conditions[0].1, Value::Str("Bitcoin".into()));
                        assert_eq!(conditions[1].0, Filter::Eq("symbol".into(), Value::Str("ETH/USD".into())));
                        assert_eq!(conditions[1].1, Value::Str("Ethereum".into()));
                        assert_eq!(*else_value, Some(Value::Str("Other".into())));
                    }
                    other => panic!("expected CaseWhen, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    // ── CAST tests ─────────────────────────────────────────────────

    #[test]
    fn plan_cast_to_integer() {
        let plan = plan_query("SELECT CAST(price AS INTEGER) FROM trades").unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::ScalarFunction { name, args } => {
                        assert_eq!(name, "cast_to_int");
                        assert_eq!(args.len(), 1);
                        assert_eq!(args[0], SelectColumnArg::Column("price".into()));
                    }
                    other => panic!("expected ScalarFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_cast_to_text() {
        let plan = plan_query("SELECT CAST(price AS TEXT) FROM trades").unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::ScalarFunction { name, args } => {
                        assert_eq!(name, "cast_to_str");
                        assert_eq!(args.len(), 1);
                    }
                    other => panic!("expected ScalarFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_cast_literal_to_timestamp() {
        let plan = plan_query("SELECT CAST('2024-01-01' AS TIMESTAMP) FROM trades").unwrap();
        match plan {
            QueryPlan::Select { columns, .. } => {
                assert_eq!(columns.len(), 1);
                match &columns[0] {
                    SelectColumn::ScalarFunction { name, args } => {
                        assert_eq!(name, "cast_to_timestamp");
                        assert_eq!(args.len(), 1);
                        assert_eq!(args[0], SelectColumnArg::Literal(Value::Str("2024-01-01".into())));
                    }
                    other => panic!("expected ScalarFunction, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn plan_sample_by_fill_null() {
        let plan = plan_query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(NULL)").unwrap();
        match plan {
            QueryPlan::Select { sample_by: Some(sb), .. } => {
                assert_eq!(sb.interval, Duration::from_secs(3600));
                assert_eq!(sb.fill, FillMode::Null);
                assert_eq!(sb.align, AlignMode::FirstObservation);
            }
            other => panic!("expected Select with SampleBy, got {other:?}"),
        }
    }

    #[test]
    fn plan_sample_by_fill_prev() {
        let plan = plan_query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(PREV)").unwrap();
        match plan {
            QueryPlan::Select { sample_by: Some(sb), .. } => {
                assert_eq!(sb.fill, FillMode::Prev);
            }
            other => panic!("expected Select with SampleBy, got {other:?}"),
        }
    }

    #[test]
    fn plan_sample_by_fill_zero() {
        let plan = plan_query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(0)").unwrap();
        match plan {
            QueryPlan::Select { sample_by: Some(sb), .. } => {
                assert_eq!(sb.fill, FillMode::Value(Value::I64(0)));
            }
            other => panic!("expected Select with SampleBy, got {other:?}"),
        }
    }

    #[test]
    fn plan_sample_by_fill_linear() {
        let plan = plan_query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(LINEAR)").unwrap();
        match plan {
            QueryPlan::Select { sample_by: Some(sb), .. } => {
                assert_eq!(sb.fill, FillMode::Linear);
            }
            other => panic!("expected Select with SampleBy, got {other:?}"),
        }
    }

    #[test]
    fn plan_sample_by_align_calendar() {
        let plan = plan_query("SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR").unwrap();
        match plan {
            QueryPlan::Select { sample_by: Some(sb), .. } => {
                assert_eq!(sb.align, AlignMode::Calendar);
            }
            other => panic!("expected Select with SampleBy, got {other:?}"),
        }
    }

    #[test]
    fn plan_show_tables() {
        let plan = plan_query("SHOW TABLES").unwrap();
        assert!(matches!(plan, QueryPlan::ShowTables));
    }

    #[test]
    fn plan_show_columns() {
        let plan = plan_query("SHOW COLUMNS FROM trades").unwrap();
        match plan {
            QueryPlan::ShowColumns { table } => assert_eq!(table, "trades"),
            other => panic!("expected ShowColumns, got {other:?}"),
        }
    }

    #[test]
    fn plan_describe() {
        let plan = plan_query("DESCRIBE trades").unwrap();
        match plan {
            QueryPlan::ShowColumns { table } => assert_eq!(table, "trades"),
            other => panic!("expected ShowColumns, got {other:?}"),
        }
    }

    #[test]
    fn plan_show_create_table() {
        let plan = plan_query("SHOW CREATE TABLE trades").unwrap();
        match plan {
            QueryPlan::ShowCreateTable { table } => assert_eq!(table, "trades"),
            other => panic!("expected ShowCreateTable, got {other:?}"),
        }
    }

    #[test]
    fn plan_right_join() {
        let plan = plan_query(
            "SELECT * FROM trades t RIGHT JOIN markets m ON t.symbol = m.symbol",
        )
        .unwrap();
        match plan {
            QueryPlan::Join {
                join_type,
                left_table,
                right_table,
                ..
            } => {
                assert_eq!(join_type, JoinType::Right);
                assert_eq!(left_table, "trades");
                assert_eq!(right_table, "markets");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn plan_full_outer_join() {
        let plan = plan_query(
            "SELECT * FROM trades t FULL OUTER JOIN markets m ON t.symbol = m.symbol",
        )
        .unwrap();
        match plan {
            QueryPlan::Join {
                join_type,
                left_table,
                right_table,
                ..
            } => {
                assert_eq!(join_type, JoinType::FullOuter);
                assert_eq!(left_table, "trades");
                assert_eq!(right_table, "markets");
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn plan_cross_join() {
        let plan = plan_query(
            "SELECT * FROM symbols CROSS JOIN timeframes",
        )
        .unwrap();
        match plan {
            QueryPlan::Join {
                join_type,
                left_table,
                right_table,
                on_columns,
                ..
            } => {
                assert_eq!(join_type, JoinType::Cross);
                assert_eq!(left_table, "symbols");
                assert_eq!(right_table, "timeframes");
                assert!(on_columns.is_empty());
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn plan_implicit_cross_join() {
        let plan = plan_query(
            "SELECT * FROM symbols, timeframes",
        )
        .unwrap();
        match plan {
            QueryPlan::Join {
                join_type,
                left_table,
                right_table,
                on_columns,
                ..
            } => {
                assert_eq!(join_type, JoinType::Cross);
                assert_eq!(left_table, "symbols");
                assert_eq!(right_table, "timeframes");
                assert!(on_columns.is_empty());
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn plan_multi_table_join() {
        let plan = plan_query(
            "SELECT t.price, m.name, e.exchange_name \
             FROM trades t \
             JOIN markets m ON t.symbol = m.symbol \
             JOIN exchanges e ON m.exchange_id = e.id",
        )
        .unwrap();
        match plan {
            QueryPlan::MultiJoin {
                left,
                right_table,
                join_type,
                ..
            } => {
                assert_eq!(right_table, "exchanges");
                assert_eq!(join_type, JoinType::Inner);
                // The left side should be a Join(trades, markets).
                match left.as_ref() {
                    QueryPlan::Join {
                        left_table,
                        right_table,
                        join_type,
                        ..
                    } => {
                        assert_eq!(left_table, "trades");
                        assert_eq!(right_table, "markets");
                        assert_eq!(join_type, &JoinType::Inner);
                    }
                    other => panic!("expected inner Join, got {other:?}"),
                }
            }
            other => panic!("expected MultiJoin, got {other:?}"),
        }
    }

    #[test]
    fn plan_in_subquery() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE symbol IN (SELECT symbol FROM watchlist)",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::InSubquery { column, negated, .. }), .. } => {
                assert_eq!(column, "symbol");
                assert!(!negated);
            }
            other => panic!("expected Select with InSubquery filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_not_in_subquery() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE symbol NOT IN (SELECT symbol FROM blacklist)",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::InSubquery { column, negated, .. }), .. } => {
                assert_eq!(column, "symbol");
                assert!(negated);
            }
            other => panic!("expected Select with InSubquery (negated) filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_exists_subquery() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE EXISTS (SELECT symbol FROM alerts WHERE symbol = 'BTC')",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Exists { negated, .. }), .. } => {
                assert!(!negated);
            }
            other => panic!("expected Select with Exists filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_not_exists_subquery() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE NOT EXISTS (SELECT symbol FROM blacklist WHERE symbol = 'BTC')",
        )
        .unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Exists { negated, .. }), .. } => {
                assert!(negated);
            }
            other => panic!("expected Select with Exists (negated) filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_not_eq_operator() {
        let plan = plan_query("SELECT * FROM trades WHERE symbol != 'BTC'").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::NotEq(col, Value::Str(v))), .. } => {
                assert_eq!(col, "symbol");
                assert_eq!(v, "BTC");
            }
            other => panic!("expected Select with NotEq filter, got {other:?}"),
        }
        let plan2 = plan_query("SELECT * FROM trades WHERE price <> 100").unwrap();
        match plan2 {
            QueryPlan::Select { filter: Some(Filter::NotEq(col, Value::I64(v))), .. } => {
                assert_eq!(col, "price");
                assert_eq!(v, 100);
            }
            other => panic!("expected Select with NotEq filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_create_table_if_not_exists() {
        let plan = plan_query("CREATE TABLE IF NOT EXISTS trades (timestamp TIMESTAMP, price DOUBLE)").unwrap();
        match plan {
            QueryPlan::CreateTable { name, if_not_exists, .. } => {
                assert_eq!(name, "trades");
                assert!(if_not_exists);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn plan_drop_table_if_exists() {
        let plan = plan_query("DROP TABLE IF EXISTS trades").unwrap();
        match plan {
            QueryPlan::DropTable { table, if_exists } => {
                assert_eq!(table, "trades");
                assert!(if_exists);
            }
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn plan_not_operator() {
        let plan = plan_query("SELECT * FROM trades WHERE NOT (price > 100)").unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Not(inner)), .. } => {
                match *inner {
                    Filter::Gt(col, Value::I64(100)) => assert_eq!(col, "price"),
                    other => panic!("expected Gt inside Not, got {other:?}"),
                }
            }
            other => panic!("expected Select with Not filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_expression_update() {
        let plan = plan_query("UPDATE trades SET price = price * 1.1 WHERE volume > 100").unwrap();
        match plan {
            QueryPlan::Update { table, assignments, filter } => {
                assert_eq!(table, "trades");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].0, "price");
                match &assignments[0].1 {
                    PlanExpr::BinaryOp { op, .. } => assert_eq!(*op, BinaryOp::Mul),
                    other => panic!("expected BinaryOp expression, got {other:?}"),
                }
                assert!(filter.is_some());
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn plan_union_all_with_limit() {
        let plan = plan_query("SELECT * FROM t1 UNION ALL SELECT * FROM t2 LIMIT 10").unwrap();
        match plan {
            QueryPlan::SetOperation { op, all, limit, .. } => {
                assert_eq!(op, SetOp::Union);
                assert!(all);
                assert_eq!(limit, Some(10));
            }
            other => panic!("expected SetOperation, got {other:?}"),
        }
    }

    #[test]
    fn plan_null_in_insert() {
        let plan = plan_query("INSERT INTO t1 VALUES (1000, NULL, 65000.0, 1.5)").unwrap();
        match plan {
            QueryPlan::Insert { values, .. } => {
                assert_eq!(values[0][1], Value::Null);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn plan_recursive_cte() {
        let plan = plan_query(
            "WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM seq WHERE n < 100) SELECT * FROM seq"
        ).unwrap();
        match plan {
            QueryPlan::WithCte { ctes, .. } => {
                assert_eq!(ctes.len(), 1);
                assert_eq!(ctes[0].name, "seq");
                assert!(ctes[0].recursive);
            }
            other => panic!("expected WithCte, got {other:?}"),
        }
    }

    #[test]
    fn plan_merge() {
        let plan = plan_query(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET price = source.price WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.price)"
        ).unwrap();
        match plan {
            QueryPlan::Merge { target_table, source_table, on_column, when_clauses } => {
                assert_eq!(target_table, "target");
                assert_eq!(source_table, "source");
                assert_eq!(on_column.0, "id");
                assert_eq!(on_column.1, "id");
                assert!(when_clauses.len() >= 2);
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn plan_on_conflict_do_nothing() {
        let plan = plan_query(
            "INSERT INTO trades (symbol, price) VALUES ('BTC/USD', 65000) ON CONFLICT (symbol) DO NOTHING"
        ).unwrap();
        match plan {
            QueryPlan::InsertOnConflict { table, on_conflict, .. } => {
                assert_eq!(table, "trades");
                assert_eq!(on_conflict.columns, vec!["symbol".to_string()]);
                assert!(matches!(on_conflict.action, OnConflictAction::DoNothing));
            }
            other => panic!("expected InsertOnConflict, got {other:?}"),
        }
    }

    #[test]
    fn plan_on_conflict_do_update() {
        let plan = plan_query(
            "INSERT INTO trades (symbol, price) VALUES ('BTC/USD', 65000) ON CONFLICT (symbol) DO UPDATE SET price = 70000"
        ).unwrap();
        match plan {
            QueryPlan::InsertOnConflict { on_conflict, .. } => {
                match on_conflict.action {
                    OnConflictAction::DoUpdate { assignments } => {
                        assert_eq!(assignments.len(), 1);
                        assert_eq!(assignments[0].0, "price");
                    }
                    other => panic!("expected DoUpdate, got {other:?}"),
                }
            }
            other => panic!("expected InsertOnConflict, got {other:?}"),
        }
    }

    #[test]
    fn plan_cast_in_where() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE CAST(price AS INTEGER) > 100"
        ).unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Expression { .. }), .. } => {
                // CAST in WHERE should produce an Expression filter.
            }
            other => panic!("expected Select with Expression filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_values_standalone() {
        let plan = plan_query("VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        match plan {
            QueryPlan::Values { column_names, rows } => {
                assert_eq!(column_names.len(), 2);
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0][0], Value::I64(1));
                assert_eq!(rows[0][1], Value::Str("a".into()));
            }
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn plan_fetch_first() {
        let plan = plan_query(
            "SELECT * FROM trades FETCH FIRST 10 ROWS ONLY"
        ).unwrap();
        match plan {
            QueryPlan::Select { limit, .. } => {
                assert_eq!(limit, Some(10));
            }
            other => panic!("expected Select with limit 10, got {other:?}"),
        }
    }

    #[test]
    fn plan_fetch_first_with_offset() {
        let plan = plan_query(
            "SELECT * FROM trades OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"
        ).unwrap();
        match plan {
            QueryPlan::Select { limit, offset, .. } => {
                assert_eq!(limit, Some(10));
                assert_eq!(offset, Some(5));
            }
            other => panic!("expected Select with limit/offset, got {other:?}"),
        }
    }

    #[test]
    fn plan_between_symmetric() {
        // BETWEEN SYMMETRIC 200 AND 100 should be rewritten to work even when low > high.
        let plan = plan_query(
            "SELECT * FROM trades WHERE price BETWEEN SYMMETRIC 200 AND 100"
        ).unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Or(parts)), .. } => {
                // Should have two BETWEEN clauses ORed together.
                assert_eq!(parts.len(), 2);
            }
            other => panic!("expected Select with Or filter (two BETWEENs), got {other:?}"),
        }
    }

    #[test]
    fn plan_distinct_on() {
        let plan = plan_query(
            "SELECT DISTINCT ON (symbol) symbol, price, timestamp FROM trades ORDER BY symbol, timestamp DESC"
        ).unwrap();
        match plan {
            QueryPlan::Select { distinct, distinct_on, columns, .. } => {
                assert!(!distinct);
                assert_eq!(distinct_on, vec!["symbol".to_string()]);
                assert_eq!(columns.len(), 3);
            }
            other => panic!("expected Select with distinct_on, got {other:?}"),
        }
    }

    #[test]
    fn plan_create_index() {
        let plan = plan_query("CREATE INDEX idx_trades_symbol ON trades (symbol)").unwrap();
        match plan {
            QueryPlan::CreateIndex { name, table, columns } => {
                assert_eq!(name, "idx_trades_symbol");
                assert_eq!(table, "trades");
                assert_eq!(columns, vec!["symbol".to_string()]);
            }
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn plan_drop_index() {
        let plan = plan_query("DROP INDEX idx_trades_symbol").unwrap();
        match plan {
            QueryPlan::DropIndex { name } => {
                assert_eq!(name, "idx_trades_symbol");
            }
            other => panic!("expected DropIndex, got {other:?}"),
        }
    }

    #[test]
    fn plan_alter_table_rename() {
        let plan = plan_query("ALTER TABLE trades RENAME TO trades_archive").unwrap();
        match plan {
            QueryPlan::RenameTable { old_name, new_name } => {
                assert_eq!(old_name, "trades");
                assert_eq!(new_name, "trades_archive");
            }
            other => panic!("expected RenameTable, got {other:?}"),
        }
    }

    #[test]
    fn plan_create_sequence() {
        let plan = plan_query("CREATE SEQUENCE trade_seq START WITH 1").unwrap();
        match plan {
            QueryPlan::CreateSequence { name, start, increment } => {
                assert_eq!(name, "trade_seq");
                assert_eq!(start, 1);
                assert_eq!(increment, 1); // default
            }
            other => panic!("expected CreateSequence, got {other:?}"),
        }
    }

    #[test]
    fn plan_create_sequence_with_options() {
        // sqlparser may or may not handle INCREMENT BY depending on the dialect.
        // Test the minimal form that the GenericDialect supports.
        let plan = plan_query("CREATE SEQUENCE trade_seq").unwrap();
        match plan {
            QueryPlan::CreateSequence { name, .. } => {
                assert_eq!(name, "trade_seq");
            }
            other => panic!("expected CreateSequence, got {other:?}"),
        }
    }

    #[test]
    fn plan_drop_sequence() {
        let plan = plan_query("DROP SEQUENCE trade_seq").unwrap();
        match plan {
            QueryPlan::DropSequence { name } => {
                assert_eq!(name, "trade_seq");
            }
            other => panic!("expected DropSequence, got {other:?}"),
        }
    }

    #[test]
    fn plan_nextval() {
        let plan = plan_query("SELECT nextval('trade_seq')").unwrap();
        match plan {
            QueryPlan::SequenceOp { op: SequenceOpKind::NextVal(name) } => {
                assert_eq!(name, "trade_seq");
            }
            other => panic!("expected SequenceOp NextVal, got {other:?}"),
        }
    }

    #[test]
    fn plan_currval() {
        let plan = plan_query("SELECT currval('trade_seq')").unwrap();
        match plan {
            QueryPlan::SequenceOp { op: SequenceOpKind::CurrVal(name) } => {
                assert_eq!(name, "trade_seq");
            }
            other => panic!("expected SequenceOp CurrVal, got {other:?}"),
        }
    }

    #[test]
    fn plan_check_constraint() {
        let plan = plan_query(
            "CREATE TABLE trades (price DOUBLE CHECK (price > 0), volume DOUBLE CHECK (volume >= 0))"
        ).unwrap();
        match plan {
            QueryPlan::CreateTable { columns, .. } => {
                assert!(columns[0].check.is_some());
                assert!(columns[1].check.is_some());
            }
            other => panic!("expected CreateTable with check constraints, got {other:?}"),
        }
    }

    #[test]
    fn plan_all_subquery() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE price > ALL (SELECT avg_price FROM benchmarks)"
        ).unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::All { column, op, .. }), .. } => {
                assert_eq!(column, "price");
                assert_eq!(op, CompareOp::Gt);
            }
            other => panic!("expected Select with All filter, got {other:?}"),
        }
    }

    #[test]
    fn plan_any_subquery() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE symbol = ANY (SELECT symbol FROM watchlist)"
        ).unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Any { column, op, .. }), .. } => {
                assert_eq!(column, "symbol");
                assert_eq!(op, CompareOp::Eq);
            }
            other => panic!("expected Select with Any filter, got {other:?}"),
        }
    }

    // ── Feature: CREATE TABLE AS SELECT ──────────────────────────

    #[test]
    fn plan_create_table_as_select() {
        let plan = plan_query(
            "CREATE TABLE backup AS SELECT * FROM trades WHERE price > 100"
        ).unwrap();
        match plan {
            QueryPlan::CreateTableAs { name, source, partition_by } => {
                assert_eq!(name, "backup");
                assert!(partition_by.is_none());
                // Source should be a Select plan
                match *source {
                    QueryPlan::Select { table, .. } => {
                        assert_eq!(table, "trades");
                    }
                    other => panic!("expected Select source, got {other:?}"),
                }
            }
            other => panic!("expected CreateTableAs, got {other:?}"),
        }
    }

    #[test]
    fn plan_create_table_as_select_with_agg() {
        let plan = plan_query(
            "CREATE TABLE summary AS SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol"
        ).unwrap();
        match plan {
            QueryPlan::CreateTableAs { name, .. } => {
                assert_eq!(name, "summary");
            }
            other => panic!("expected CreateTableAs, got {other:?}"),
        }
    }

    // ── Feature: NOT BETWEEN ─────────────────────────────────────

    #[test]
    fn plan_not_between() {
        let plan = plan_query(
            "SELECT * FROM trades WHERE price NOT BETWEEN 100 AND 200"
        ).unwrap();
        match plan {
            QueryPlan::Select { filter: Some(Filter::Not(inner)), .. } => {
                match *inner {
                    Filter::Between(col, Value::I64(100), Value::I64(200)) => {
                        assert_eq!(col, "price");
                    }
                    other => panic!("expected Between inside Not, got {other:?}"),
                }
            }
            other => panic!("expected Select with Not(Between), got {other:?}"),
        }
    }

    // ── Feature: read_csv ────────────────────────────────────────

    #[test]
    fn plan_read_csv() {
        let plan = plan_query(
            "SELECT * FROM read_csv('/path/to/data.csv')"
        ).unwrap();
        match plan {
            QueryPlan::ReadCsv { path, .. } => {
                assert_eq!(path.to_string_lossy(), "/path/to/data.csv");
            }
            other => panic!("expected ReadCsv, got {other:?}"),
        }
    }

    #[test]
    fn plan_read_csv_columns() {
        let plan = plan_query(
            "SELECT name, value FROM read_csv('/data.csv')"
        ).unwrap();
        match plan {
            QueryPlan::ReadCsv { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert!(matches!(&columns[0], SelectColumn::Name(n) if n == "name"));
                assert!(matches!(&columns[1], SelectColumn::Name(n) if n == "value"));
            }
            other => panic!("expected ReadCsv, got {other:?}"),
        }
    }

    // ── Feature: DECIMAL with precision/scale ────────────────────

    #[test]
    fn plan_decimal_with_precision() {
        let plan = plan_query(
            "CREATE TABLE prices (ts TIMESTAMP, amount DECIMAL(18, 8))"
        ).unwrap();
        match plan {
            QueryPlan::CreateTable { columns, .. } => {
                assert_eq!(columns.len(), 2);
                // The DECIMAL(18, 8) should be parsed as type containing "DECIMAL"
                let amount_col = &columns[1];
                assert_eq!(amount_col.name, "amount");
                assert!(amount_col.type_name.contains("DECIMAL"), "type_name={}", amount_col.type_name);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }
}
