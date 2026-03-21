//! Adaptive query execution strategy selection.
//!
//! Decides at runtime whether to use vectorized, row-at-a-time, columnar,
//! or parallel execution based on data size and query characteristics.

use crate::optimizer::TableStats;
use crate::plan::{AggregateKind, QueryPlan, SelectColumn};

/// Execution strategy chosen by the adaptive executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    /// Small data or complex expressions — process one row at a time.
    RowAtATime,
    /// Large data with simple aggregates — process in batches.
    Vectorized,
    /// Pure aggregate queries (no GROUP BY) — columnar scan.
    Columnar,
    /// Complex queries needing streaming — cursor-based pipeline.
    CursorPipeline,
    /// Multi-partition with rayon — parallel scan and merge.
    Parallel,
}

/// Thresholds for strategy selection.
const SMALL_TABLE_ROWS: u64 = 10_000;
const MEDIUM_TABLE_ROWS: u64 = 100_000;
const PARALLEL_PARTITION_THRESHOLD: u32 = 2;

/// Decides at runtime which execution strategy to use based on the query
/// plan and table statistics.
pub fn choose_execution_strategy(
    plan: &QueryPlan,
    stats: &TableStats,
) -> ExecutionStrategy {
    match plan {
        QueryPlan::Select {
            columns,
            group_by,
            sample_by,
            filter: _,
         latest_on,
            ..
        } => {
            let has_aggregates = columns.iter().any(|c| matches!(c, SelectColumn::Aggregate { .. }));
            let has_window_functions = columns.iter().any(|c| matches!(c, SelectColumn::WindowFunction(_)));
            let has_group_by = !group_by.is_empty();
            let has_sample_by = sample_by.is_some();
            let has_latest_on = latest_on.is_some();
            let has_complex_exprs = columns.iter().any(|c| {
                matches!(c, SelectColumn::CaseWhen { .. } | SelectColumn::ScalarSubquery { .. })
            });

            // Small tables: row-at-a-time is fastest (no overhead).
            if stats.row_count < SMALL_TABLE_ROWS {
                return ExecutionStrategy::RowAtATime;
            }

            // Window functions or LATEST ON with complex expressions: cursor pipeline.
            if has_window_functions || (has_latest_on && has_complex_exprs) {
                return ExecutionStrategy::CursorPipeline;
            }

            // Pure aggregates without GROUP BY: columnar scan.
            if has_aggregates && !has_group_by && !has_sample_by && !has_complex_exprs {
                let all_simple_aggs = columns.iter().all(|c| match c {
                    SelectColumn::Aggregate { function, .. } => matches!(
                        function,
                        AggregateKind::Sum
                            | AggregateKind::Count
                            | AggregateKind::Min
                            | AggregateKind::Max
                            | AggregateKind::Avg
                    ),
                    SelectColumn::Name(_) | SelectColumn::Wildcard => true,
                    _ => false,
                });
                if all_simple_aggs {
                    return ExecutionStrategy::Columnar;
                }
            }

            // Large tables with multiple partitions: parallel.
            if stats.row_count >= MEDIUM_TABLE_ROWS
                && stats.partition_count >= PARALLEL_PARTITION_THRESHOLD
            {
                return ExecutionStrategy::Parallel;
            }

            // Medium-large tables with simple operations: vectorized.
            if stats.row_count >= SMALL_TABLE_ROWS && !has_complex_exprs {
                return ExecutionStrategy::Vectorized;
            }

            // Default fallback.
            ExecutionStrategy::RowAtATime
        }
        // Joins: prefer parallel for large datasets.
        QueryPlan::Join { .. } | QueryPlan::MultiJoin { .. } | QueryPlan::LateralJoin { .. } => {
            if stats.row_count >= MEDIUM_TABLE_ROWS {
                ExecutionStrategy::Parallel
            } else {
                ExecutionStrategy::RowAtATime
            }
        }
        // Everything else: row-at-a-time.
        _ => ExecutionStrategy::RowAtATime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::TableStats;
    use crate::plan::*;
    use std::collections::HashMap;

    fn make_stats(row_count: u64, partition_count: u32) -> TableStats {
        TableStats {
            row_count,
            partition_count,
            min_timestamp: 0,
            max_timestamp: 1_000_000,
            column_stats: HashMap::new(),
        }
    }

    fn simple_select() -> QueryPlan {
        QueryPlan::Select {
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
        }
    }

    fn agg_select() -> QueryPlan {
        QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::Aggregate {
                function: AggregateKind::Sum,
                column: "price".to_string(),
                alias: None,
                filter: None,
                within_group_order: None,
                arg_expr: None,
            }],
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
        }
    }

    #[test]
    fn small_table_uses_row_at_a_time() {
        let plan = simple_select();
        let stats = make_stats(100, 1);
        assert_eq!(
            choose_execution_strategy(&plan, &stats),
            ExecutionStrategy::RowAtATime
        );
    }

    #[test]
    fn large_multi_partition_uses_parallel() {
        let plan = simple_select();
        let stats = make_stats(500_000, 10);
        assert_eq!(
            choose_execution_strategy(&plan, &stats),
            ExecutionStrategy::Parallel
        );
    }

    #[test]
    fn pure_aggregate_uses_columnar() {
        let plan = agg_select();
        let stats = make_stats(50_000, 1);
        assert_eq!(
            choose_execution_strategy(&plan, &stats),
            ExecutionStrategy::Columnar
        );
    }

    #[test]
    fn medium_table_simple_query_uses_vectorized() {
        let plan = simple_select();
        let stats = make_stats(50_000, 1);
        assert_eq!(
            choose_execution_strategy(&plan, &stats),
            ExecutionStrategy::Vectorized
        );
    }

    #[test]
    fn window_function_uses_cursor_pipeline() {
        let plan = QueryPlan::Select {
            table: "t".to_string(),
            columns: vec![SelectColumn::WindowFunction(
                crate::window::WindowFunction {
                    name: "row_number".to_string(),
                    args: vec![],
                    over: crate::window::WindowSpec {
                        partition_by: vec![],
                        order_by: vec![],
                        frame: None,
                    },
                    alias: None,
                },
            )],
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
        let stats = make_stats(50_000, 1);
        assert_eq!(
            choose_execution_strategy(&plan, &stats),
            ExecutionStrategy::CursorPipeline
        );
    }

    #[test]
    fn join_large_uses_parallel() {
        let plan = QueryPlan::Join {
            left_table: "a".to_string(),
            right_table: "b".to_string(),
            left_alias: None,
            right_alias: None,
            columns: vec![JoinSelectColumn::Wildcard],
            join_type: JoinType::Inner,
            on_columns: vec![],
            filter: None,
            order_by: vec![],
            limit: None,
        };
        let stats = make_stats(500_000, 5);
        assert_eq!(
            choose_execution_strategy(&plan, &stats),
            ExecutionStrategy::Parallel
        );
    }
}
