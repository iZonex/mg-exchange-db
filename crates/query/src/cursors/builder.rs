//! Cursor pipeline builder — constructs a cursor tree from a `QueryPlan`.

use std::path::Path;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use exchange_core::table::TableMeta;

use super::except::ExceptCursor;
use super::intersect::IntersectCursor;
use crate::cursors::*;
use crate::plan::{AggregateKind, QueryPlan, QueryResult, SelectColumn, SetOp, Value};
use crate::record_cursor::RecordCursor;
use crate::window::WindowFunction;

/// Build a cursor pipeline from a `QueryPlan`.
///
/// Supports `QueryPlan::Select`, `QueryPlan::Join`, and `QueryPlan::SetOperation`.
/// Returns a layered cursor chain appropriate for the plan type.
pub fn build_cursor(db_root: &Path, plan: &QueryPlan) -> Result<Box<dyn RecordCursor>> {
    match plan {
        QueryPlan::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            offset,
            group_by,
            sample_by,
            latest_on,
            distinct,
            ..
        } => {
            let table_dir = db_root.join(table);
            let meta_path = table_dir.join("_meta");
            let meta = TableMeta::load(&meta_path)?;

            // Determine which columns we need to read from disk.
            let all_columns: Vec<(usize, String, ColumnType)> = meta
                .columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let ct: ColumnType = col.col_type.into();
                    (i, col.name.clone(), ct)
                })
                .collect();

            let partitions = exchange_core::table::list_partitions(&table_dir)?;

            // 1. Scan cursor — read all columns from disk.
            let cursor: Box<dyn RecordCursor> =
                Box::new(ScanCursor::new(partitions, all_columns.clone()));

            // 2. Filter cursor.
            let cursor: Box<dyn RecordCursor> = if let Some(f) = filter {
                Box::new(FilterCursor::new(cursor, f.clone()))
            } else {
                cursor
            };

            // 2b. LATEST ON cursor.
            let cursor: Box<dyn RecordCursor> = if let Some(lo) = latest_on {
                let source_schema = cursor.schema().to_vec();
                let ts_col = source_schema
                    .iter()
                    .position(|(n, _)| n == &lo.timestamp_col)
                    .unwrap_or(0);
                let part_col = source_schema
                    .iter()
                    .position(|(n, _)| n == &lo.partition_col)
                    .unwrap_or(0);
                Box::new(LatestByCursor::new(cursor, vec![part_col], ts_col))
            } else {
                cursor
            };

            // 3. Aggregate cursor (GROUP BY).
            let has_aggregates = columns
                .iter()
                .any(|c| matches!(c, SelectColumn::Aggregate { .. }));
            let cursor: Box<dyn RecordCursor> = if !group_by.is_empty() || has_aggregates {
                let aggs: Vec<(AggregateKind, String)> = columns
                    .iter()
                    .filter_map(|c| match c {
                        SelectColumn::Aggregate {
                            function, column, ..
                        } => Some((*function, column.clone())),
                        _ => None,
                    })
                    .collect();
                Box::new(AggregateCursor::new(cursor, group_by.clone(), aggs))
            } else {
                cursor
            };

            // 3b. SAMPLE BY cursor (time-bucketed aggregation).
            let cursor: Box<dyn RecordCursor> = if let Some(sb) = sample_by {
                let source_schema = cursor.schema().to_vec();

                // Find timestamp column (first Timestamp column).
                let ts_col = source_schema
                    .iter()
                    .position(|(_, ct)| *ct == ColumnType::Timestamp)
                    .unwrap_or(0);

                // Build aggregate list from columns.
                let aggs: Vec<(AggregateKind, usize)> = columns
                    .iter()
                    .filter_map(|c| match c {
                        SelectColumn::Aggregate {
                            function, column, ..
                        } => {
                            let col_idx = source_schema
                                .iter()
                                .position(|(n, _)| n == column)
                                .unwrap_or(0);
                            Some((*function, col_idx))
                        }
                        _ => None,
                    })
                    .collect();

                let interval_nanos = sb.interval.as_nanos() as i64;

                Box::new(SampleByCursor::new(
                    cursor,
                    interval_nanos,
                    ts_col,
                    aggs,
                    sb.fill.clone(),
                ))
            } else {
                cursor
            };

            // 4. Window function cursor.
            let window_fns: Vec<WindowFunction> = columns
                .iter()
                .filter_map(|c| match c {
                    SelectColumn::WindowFunction(wf) => Some(wf.clone()),
                    _ => None,
                })
                .collect();
            let cursor: Box<dyn RecordCursor> = if !window_fns.is_empty() {
                Box::new(WindowCursor::new(cursor, window_fns))
            } else {
                cursor
            };

            // 5. Project cursor — select only the requested columns.
            let cursor: Box<dyn RecordCursor> = if !has_aggregates && !group_by.is_empty() {
                // After aggregation the schema already matches, skip projection.
                cursor
            } else if !has_aggregates {
                // Determine which columns to project.
                let is_wildcard = columns.iter().any(|c| matches!(c, SelectColumn::Wildcard));
                if is_wildcard {
                    cursor
                } else {
                    let source_schema: Vec<(String, ColumnType)> = cursor.schema().to_vec();
                    let indices: Vec<usize> = columns
                        .iter()
                        .filter_map(|c| match c {
                            SelectColumn::Name(name) => {
                                source_schema.iter().position(|(n, _)| n == name)
                            }
                            _ => None,
                        })
                        .collect();
                    if indices.is_empty() {
                        cursor
                    } else {
                        Box::new(ProjectCursor::new(cursor, indices))
                    }
                }
            } else {
                cursor
            };

            // 5b. DISTINCT cursor.
            let cursor: Box<dyn RecordCursor> = if *distinct {
                Box::new(DistinctCursor::new(cursor))
            } else {
                cursor
            };

            // 6. Sort cursor — use TopKCursor for ORDER BY + LIMIT with small K.
            let cursor: Box<dyn RecordCursor> = if !order_by.is_empty() {
                if let Some(lim) = limit {
                    let k = *lim as usize + offset.unwrap_or(0) as usize;
                    if k <= 1000 {
                        Box::new(TopKCursor::new(cursor, k, order_by.clone()))
                    } else {
                        Box::new(SortCursor::new(cursor, order_by.clone()))
                    }
                } else {
                    Box::new(SortCursor::new(cursor, order_by.clone()))
                }
            } else {
                cursor
            };

            // 7. Limit/offset cursor.
            let off = offset.unwrap_or(0);
            let cursor: Box<dyn RecordCursor> = if limit.is_some() || off > 0 {
                let lim = limit.unwrap_or(u64::MAX);
                Box::new(LimitCursor::new(cursor, lim, off))
            } else {
                cursor
            };

            Ok(cursor)
        }

        QueryPlan::Join {
            left_table,
            right_table,
            columns: _,
            join_type,
            on_columns,
            filter,
            order_by,
            limit,
            ..
        } => {
            // Build scan cursors for both tables.
            let left_cursor = build_table_scan(db_root, left_table)?;
            let right_cursor = build_table_scan(db_root, right_table)?;

            let left_schema = left_cursor.schema().to_vec();
            let right_schema = right_cursor.schema().to_vec();

            // Resolve join key column indices.
            let left_key_cols: Vec<usize> = on_columns
                .iter()
                .filter_map(|(lcol, _)| left_schema.iter().position(|(n, _)| n == lcol))
                .collect();
            let right_key_cols: Vec<usize> = on_columns
                .iter()
                .filter_map(|(_, rcol)| right_schema.iter().position(|(n, _)| n == rcol))
                .collect();

            let cursor: Box<dyn RecordCursor> = Box::new(HashJoinCursor::new(
                left_cursor,
                right_cursor,
                left_key_cols,
                right_key_cols,
                *join_type,
            ));

            // Apply filter if present.
            let cursor: Box<dyn RecordCursor> = if let Some(f) = filter {
                Box::new(FilterCursor::new(cursor, f.clone()))
            } else {
                cursor
            };

            // Sort.
            let cursor: Box<dyn RecordCursor> = if !order_by.is_empty() {
                Box::new(SortCursor::new(cursor, order_by.clone()))
            } else {
                cursor
            };

            // Limit.
            let cursor: Box<dyn RecordCursor> = if let Some(lim) = limit {
                Box::new(LimitCursor::new(cursor, *lim, 0))
            } else {
                cursor
            };

            Ok(cursor)
        }

        QueryPlan::SetOperation {
            op,
            left,
            right,
            all,
            limit: _,
        } => {
            let left_cursor = build_cursor(db_root, left)?;
            let right_cursor = build_cursor(db_root, right)?;

            match op {
                SetOp::Union => {
                    if *all {
                        Ok(Box::new(UnionCursor::new(vec![left_cursor, right_cursor])))
                    } else {
                        Ok(Box::new(UnionDistinctCursor::new(vec![
                            left_cursor,
                            right_cursor,
                        ])))
                    }
                }
                SetOp::Intersect => Ok(Box::new(IntersectCursor::new(left_cursor, right_cursor))),
                SetOp::Except => Ok(Box::new(ExceptCursor::new(left_cursor, right_cursor))),
            }
        }

        QueryPlan::AsofJoin {
            left_table,
            right_table,
            left_columns: _,
            right_columns: _,
            on_columns,
            filter,
            order_by,
            limit,
        } => {
            let left_cursor = build_table_scan(db_root, left_table)?;
            let right_cursor = build_table_scan(db_root, right_table)?;

            let left_schema = left_cursor.schema().to_vec();
            let right_schema = right_cursor.schema().to_vec();

            // Find timestamp columns (first Timestamp column in each side).
            let left_ts = left_schema
                .iter()
                .position(|(_, ct)| *ct == ColumnType::Timestamp)
                .unwrap_or(0);
            let right_ts = right_schema
                .iter()
                .position(|(_, ct)| *ct == ColumnType::Timestamp)
                .unwrap_or(0);

            // Resolve partition key columns.
            let left_key_cols: Vec<usize> = on_columns
                .iter()
                .filter_map(|(lcol, _)| left_schema.iter().position(|(n, _)| n == lcol))
                .collect();
            let right_key_cols: Vec<usize> = on_columns
                .iter()
                .filter_map(|(_, rcol)| right_schema.iter().position(|(n, _)| n == rcol))
                .collect();

            let cursor: Box<dyn RecordCursor> = Box::new(AsofJoinCursor::new(
                left_cursor,
                right_cursor,
                left_ts,
                right_ts,
                left_key_cols,
                right_key_cols,
            ));

            let cursor: Box<dyn RecordCursor> = if let Some(f) = filter {
                Box::new(FilterCursor::new(cursor, f.clone()))
            } else {
                cursor
            };

            let cursor: Box<dyn RecordCursor> = if !order_by.is_empty() {
                Box::new(SortCursor::new(cursor, order_by.clone()))
            } else {
                cursor
            };

            let cursor: Box<dyn RecordCursor> = if let Some(lim) = limit {
                Box::new(LimitCursor::new(cursor, *lim, 0))
            } else {
                cursor
            };

            Ok(cursor)
        }

        QueryPlan::WithCte { ctes, body } => {
            // Materialize each CTE into a MemoryCursor, then build the body.
            // For simplicity, CTEs are not yet referenced by name in the body
            // scan; we just build the body plan directly since the planner
            // has already inlined CTE references.
            let _ = ctes; // CTEs are inlined by the planner.
            build_cursor(db_root, body)
        }

        QueryPlan::DerivedScan {
            subquery,
            alias: _,
            columns,
            filter,
            order_by,
            limit,
            group_by,
            having: _,
            distinct,
        } => {
            // Build the inner subquery cursor.
            let cursor = build_cursor(db_root, subquery)?;

            // Wrap in MemoryCursor to materialize the subquery result.
            let inner_schema = cursor.schema().to_vec();
            let mut mem_rows = Vec::new();
            let mut inner = cursor;
            loop {
                match inner.next_batch(8192)? {
                    None => break,
                    Some(batch) => mem_rows.extend(batch.to_rows()),
                }
            }
            let cursor: Box<dyn RecordCursor> =
                Box::new(MemoryCursor::from_rows(inner_schema, &mem_rows));

            // Filter.
            let cursor: Box<dyn RecordCursor> = if let Some(f) = filter {
                Box::new(FilterCursor::new(cursor, f.clone()))
            } else {
                cursor
            };

            // Aggregate.
            let has_aggregates = columns
                .iter()
                .any(|c| matches!(c, SelectColumn::Aggregate { .. }));
            let cursor: Box<dyn RecordCursor> = if !group_by.is_empty() || has_aggregates {
                let aggs: Vec<(AggregateKind, String)> = columns
                    .iter()
                    .filter_map(|c| match c {
                        SelectColumn::Aggregate {
                            function, column, ..
                        } => Some((*function, column.clone())),
                        _ => None,
                    })
                    .collect();
                Box::new(AggregateCursor::new(cursor, group_by.clone(), aggs))
            } else {
                cursor
            };

            // Distinct.
            let cursor: Box<dyn RecordCursor> = if *distinct {
                Box::new(DistinctCursor::new(cursor))
            } else {
                cursor
            };

            // Sort.
            let cursor: Box<dyn RecordCursor> = if !order_by.is_empty() {
                Box::new(SortCursor::new(cursor, order_by.clone()))
            } else {
                cursor
            };

            // Limit.
            let cursor: Box<dyn RecordCursor> = if let Some(lim) = limit {
                Box::new(LimitCursor::new(cursor, *lim, 0))
            } else {
                cursor
            };

            Ok(cursor)
        }

        QueryPlan::LateralJoin {
            left_table,
            left_alias: _,
            subquery,
            subquery_alias: _,
            columns: _,
            filter,
            order_by,
            limit,
        } => {
            let left_cursor = build_table_scan(db_root, left_table)?;
            // Build the right (subquery) cursor.
            let right_cursor = build_cursor(db_root, subquery)?;

            // Use NestedLoopJoinCursor with a predicate that always matches
            // (the lateral correlation is handled by the planner inlining refs).
            let always_true: super::nested_loop_join::JoinPredicate =
                Box::new(|_left: &[Value], _right: &[Value]| true);
            let cursor: Box<dyn RecordCursor> = Box::new(NestedLoopJoinCursor::new(
                left_cursor,
                right_cursor,
                always_true,
            ));

            let cursor: Box<dyn RecordCursor> = if let Some(f) = filter {
                Box::new(FilterCursor::new(cursor, f.clone()))
            } else {
                cursor
            };

            let cursor: Box<dyn RecordCursor> = if !order_by.is_empty() {
                Box::new(SortCursor::new(cursor, order_by.clone()))
            } else {
                cursor
            };

            let cursor: Box<dyn RecordCursor> = if let Some(lim) = limit {
                Box::new(LimitCursor::new(cursor, *lim, 0))
            } else {
                cursor
            };

            Ok(cursor)
        }

        QueryPlan::Pivot {
            source,
            aggregate,
            agg_column,
            pivot_col,
            values,
        } => {
            let source_cursor = build_cursor(db_root, source)?;
            let source_schema = source_cursor.schema().to_vec();

            let group_col = source_schema
                .iter()
                .position(|(n, _)| n != agg_column && n != pivot_col)
                .unwrap_or(0);
            let pivot_col_idx = source_schema
                .iter()
                .position(|(n, _)| n == pivot_col)
                .unwrap_or(0);

            let _ = aggregate;

            let pivot_values: Vec<String> = values.iter().map(|pv| pv.alias.clone()).collect();

            Ok(Box::new(PivotedAggregateCursor::new(
                source_cursor,
                group_col,
                pivot_col_idx,
                pivot_values,
            )))
        }

        QueryPlan::Values { column_names, rows } => {
            let schema: Vec<(String, ColumnType)> = column_names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let ct = rows
                        .first()
                        .and_then(|r| r.get(i))
                        .map(|v| match v {
                            Value::I64(_) => ColumnType::I64,
                            Value::F64(_) => ColumnType::F64,
                            Value::Str(_) => ColumnType::Varchar,
                            Value::Timestamp(_) => ColumnType::Timestamp,
                            Value::Null => ColumnType::I64,
                        })
                        .unwrap_or(ColumnType::I64);
                    (name.clone(), ct)
                })
                .collect();
            Ok(Box::new(ValuesCursor::new(schema, rows.clone())))
        }

        QueryPlan::LongSequence { count, columns: _ } => {
            Ok(Box::new(GenerateSeriesCursor::new_i64(1, *count as i64, 1)))
        }

        QueryPlan::GenerateSeries {
            start,
            stop,
            step,
            columns: _,
            is_timestamp,
        } => {
            if *is_timestamp {
                Ok(Box::new(GenerateSeriesCursor::new_timestamp(
                    *start, *stop, *step,
                )))
            } else {
                Ok(Box::new(GenerateSeriesCursor::new_i64(
                    *start, *stop, *step,
                )))
            }
        }

        QueryPlan::ReadParquet { path, columns: _ } => {
            // Read parquet into memory, then serve via MemoryCursor.
            // For now, return an empty cursor if the file doesn't exist.
            let schema = vec![("data".to_string(), ColumnType::Varchar)];
            let rows: Vec<Vec<Value>> = if path.exists() {
                // Placeholder: actual parquet reading is done by the executor.
                Vec::new()
            } else {
                Vec::new()
            };
            Ok(Box::new(MemoryCursor::from_rows(schema, &rows)))
        }

        QueryPlan::MultiJoin {
            left,
            right_table,
            right_alias: _,
            columns: _,
            join_type,
            on_columns,
            filter,
            order_by,
            limit,
        } => {
            // Build the left side recursively (could be another Join/MultiJoin).
            let left_cursor = build_cursor(db_root, left)?;
            let right_cursor = build_table_scan(db_root, right_table)?;

            let left_schema = left_cursor.schema().to_vec();
            let right_schema = right_cursor.schema().to_vec();

            let left_key_cols: Vec<usize> = on_columns
                .iter()
                .filter_map(|(lcol, _)| {
                    left_schema
                        .iter()
                        .position(|(n, _)| n == lcol || n.ends_with(&format!(".{lcol}")))
                })
                .collect();
            let right_key_cols: Vec<usize> = on_columns
                .iter()
                .filter_map(|(_, rcol)| right_schema.iter().position(|(n, _)| n == rcol))
                .collect();

            let cursor: Box<dyn RecordCursor> = Box::new(HashJoinCursor::new(
                left_cursor,
                right_cursor,
                left_key_cols,
                right_key_cols,
                *join_type,
            ));

            let cursor: Box<dyn RecordCursor> = if let Some(f) = filter {
                Box::new(FilterCursor::new(cursor, f.clone()))
            } else {
                cursor
            };

            let cursor: Box<dyn RecordCursor> = if !order_by.is_empty() {
                Box::new(SortCursor::new(cursor, order_by.clone()))
            } else {
                cursor
            };

            let cursor: Box<dyn RecordCursor> = if let Some(lim) = limit {
                Box::new(LimitCursor::new(cursor, *lim, 0))
            } else {
                cursor
            };

            Ok(cursor)
        }

        QueryPlan::Explain { query } => {
            // Build an explain cursor that describes the plan without executing.
            let description = format!("{:#?}", query);
            let schema = vec![("QUERY PLAN".to_string(), ColumnType::Varchar)];
            let rows = vec![vec![Value::Str(description)]];
            Ok(Box::new(MemoryCursor::from_rows(schema, &rows)))
        }

        // DML/DDL plans: execute via the regular executor and wrap results
        // in a MemoryCursor. This allows the cursor pipeline to handle every
        // plan type.
        _ => {
            let result = crate::executor::execute(db_root, plan)?;
            match result {
                QueryResult::Rows { columns, rows } => {
                    if rows.is_empty() {
                        let schema: Vec<(String, ColumnType)> = columns
                            .iter()
                            .map(|c| (c.clone(), ColumnType::Varchar))
                            .collect();
                        Ok(Box::new(MemoryCursor::from_rows(schema, &[])))
                    } else {
                        let col_names: Vec<String> = columns;
                        let batch = crate::batch::RecordBatch::from_rows(&col_names, &rows);
                        Ok(Box::new(MemoryCursor::new(batch)))
                    }
                }
                QueryResult::Ok { affected_rows } => {
                    let schema = vec![("affected_rows".to_string(), ColumnType::I64)];
                    let rows = vec![vec![Value::I64(affected_rows as i64)]];
                    Ok(Box::new(MemoryCursor::from_rows(schema, &rows)))
                }
            }
        }
    }
}

/// Build a full table scan cursor for the given table.
fn build_table_scan(db_root: &Path, table: &str) -> Result<Box<dyn RecordCursor>> {
    let table_dir = db_root.join(table);
    let meta_path = table_dir.join("_meta");
    let meta = TableMeta::load(&meta_path)?;

    let all_columns: Vec<(usize, String, ColumnType)> = meta
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let ct: ColumnType = col.col_type.into();
            (i, col.name.clone(), ct)
        })
        .collect();

    let partitions = exchange_core::table::list_partitions(&table_dir)?;
    Ok(Box::new(ScanCursor::new(partitions, all_columns)))
}
