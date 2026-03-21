//! Cursor-based query execution engine.
//!
//! Provides an alternative execution path that uses the cursor pipeline
//! to stream results instead of materializing everything at once.

use std::path::Path;

use exchange_common::error::Result;

use crate::cursors::build_cursor;
use crate::plan::{QueryPlan, QueryResult, Value};
use crate::record_cursor::RecordCursor;

/// Execute a query plan using the cursor-based execution engine.
///
/// Returns a cursor that can be iterated for streaming results. The caller
/// pulls batches on demand, allowing backpressure and early termination.
pub fn execute_cursor(db_root: &Path, plan: &QueryPlan) -> Result<Box<dyn RecordCursor>> {
    build_cursor(db_root, plan)
}

/// Convenience: execute a cursor and materialize all results into a `QueryResult`.
///
/// This is useful when the caller wants the full result set at once (e.g.,
/// for compatibility with the existing `execute` API).
pub fn execute_cursor_all(db_root: &Path, plan: &QueryPlan) -> Result<QueryResult> {
    let mut cursor = execute_cursor(db_root, plan)?;
    let schema = cursor.schema().to_vec();
    let columns: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();

    let mut rows: Vec<Vec<Value>> = Vec::new();

    loop {
        match cursor.next_batch(1024)? {
            None => break,
            Some(batch) => {
                for r in 0..batch.row_count() {
                    let row: Vec<Value> = (0..batch.columns.len())
                        .map(|c| batch.get_value(r, c))
                        .collect();
                    rows.push(row);
                }
            }
        }
    }

    Ok(QueryResult::Rows { columns, rows })
}

/// Execute a query plan via the cursor pipeline, streaming batches of 8192 rows.
///
/// This is the primary entry point for the cursor-based execution engine.
/// It builds a cursor tree from the plan, drains all batches, and returns
/// the results as a `QueryResult`.
pub fn execute_via_cursors(db_root: &Path, plan: &QueryPlan) -> Result<QueryResult> {
    let mut cursor = build_cursor(db_root, plan)?;
    let schema = cursor.schema().to_vec();
    let columns: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();

    let mut all_rows: Vec<Vec<Value>> = Vec::new();

    loop {
        match cursor.next_batch(8192)? {
            None => break,
            Some(batch) => {
                all_rows.extend(batch.to_rows());
            }
        }
    }

    Ok(QueryResult::Rows { columns, rows: all_rows })
}

/// Configuration flag for enabling cursor-based execution.
///
/// When `use_cursor_engine` is true, queries are routed through the
/// cursor pipeline instead of the traditional executor.
#[derive(Default)]
pub struct CursorEngineConfig {
    pub use_cursor_engine: bool,
}


/// Execute a query plan, routing through the cursor engine when configured.
///
/// If `config.use_cursor_engine` is true, uses the cursor pipeline.
/// Otherwise, falls back to the regular `execute` function.
pub fn execute_with_engine(
    db_root: &Path,
    plan: &QueryPlan,
    config: &CursorEngineConfig,
) -> Result<QueryResult> {
    if config.use_cursor_engine {
        execute_via_cursors(db_root, plan)
    } else {
        crate::executor::execute(db_root, plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::RecordBatch;
    use crate::cursors::memory::MemoryCursor;
    use crate::cursors::filter::FilterCursor;
    use crate::cursors::sort::SortCursor;
    use crate::cursors::limit::LimitCursor;
    use crate::cursors::topk::TopKCursor;
    use crate::cursors::union::UnionCursor;
    use crate::cursors::hash_join::HashJoinCursor;
    use crate::cursors::merge_sort::MergeSortCursor;
    use crate::cursors::sample_by::SampleByCursor;
    use crate::plan::{AggregateKind, FillMode, JoinType, OrderBy};
    use exchange_common::types::ColumnType;

    /// Helper to drain all rows from a cursor.
    fn drain_cursor(cursor: &mut dyn RecordCursor) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        loop {
            match cursor.next_batch(1024).unwrap() {
                None => break,
                Some(batch) => {
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..batch.columns.len())
                            .map(|c| batch.get_value(r, c))
                            .collect();
                        rows.push(row);
                    }
                }
            }
        }
        rows
    }

    #[test]
    fn cursor_executor_sort_vs_topk_same_results() {
        // Verify that TopKCursor and SortCursor+LimitCursor produce the same results.
        let schema = vec![
            ("id".to_string(), ColumnType::I64),
            ("val".to_string(), ColumnType::F64),
        ];
        let rows: Vec<Vec<Value>> = (0..100)
            .map(|i| vec![Value::I64(i), Value::F64((100 - i) as f64)])
            .collect();

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: false,
        }];

        // Path 1: Sort + Limit.
        let source1 = MemoryCursor::from_rows(schema.clone(), &rows);
        let sorted = SortCursor::new(Box::new(source1), order_by.clone());
        let mut limited = LimitCursor::new(Box::new(sorted), 10, 0);
        let result1 = drain_cursor(&mut limited);

        // Path 2: TopK.
        let source2 = MemoryCursor::from_rows(schema.clone(), &rows);
        let mut topk = TopKCursor::new(Box::new(source2), 10, order_by);
        // TopK already returns exactly 10 rows, but we also need limit for offset=0.
        let mut topk_limited = LimitCursor::new(Box::new(topk), 10, 0);
        let result2 = drain_cursor(&mut topk_limited);

        assert_eq!(result1.len(), 10);
        assert_eq!(result2.len(), 10);

        // Both should return the same rows (sorted by val ASC, first 10).
        for i in 0..10 {
            assert_eq!(result1[i], result2[i]);
        }
    }

    #[test]
    fn cursor_pipeline_filter_sort_limit() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..50).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);

        // Filter: val >= 10
        let filter = crate::plan::Filter::Gte("val".to_string(), Value::I64(10));
        let filtered = FilterCursor::new(Box::new(source), filter);

        // Sort descending.
        let sorted = SortCursor::new(
            Box::new(filtered),
            vec![OrderBy {
                column: "val".to_string(),
                descending: true,
            }],
        );

        // Limit 5.
        let mut limited = LimitCursor::new(Box::new(sorted), 5, 0);
        let result = drain_cursor(&mut limited);

        assert_eq!(result.len(), 5);
        assert_eq!(result[0][0], Value::I64(49));
        assert_eq!(result[1][0], Value::I64(48));
        assert_eq!(result[2][0], Value::I64(47));
        assert_eq!(result[3][0], Value::I64(46));
        assert_eq!(result[4][0], Value::I64(45));
    }

    #[test]
    fn hash_join_produces_correct_results() {
        let left_schema = vec![
            ("id".to_string(), ColumnType::I64),
            ("name".to_string(), ColumnType::Varchar),
        ];
        let right_schema = vec![
            ("user_id".to_string(), ColumnType::I64),
            ("score".to_string(), ColumnType::F64),
        ];

        let left_rows = vec![
            vec![Value::I64(1), Value::Str("Alice".into())],
            vec![Value::I64(2), Value::Str("Bob".into())],
            vec![Value::I64(3), Value::Str("Carol".into())],
        ];
        let right_rows = vec![
            vec![Value::I64(1), Value::F64(100.0)],
            vec![Value::I64(3), Value::F64(300.0)],
        ];

        let left = MemoryCursor::from_rows(left_schema, &left_rows);
        let right = MemoryCursor::from_rows(right_schema, &right_rows);

        let mut cursor = HashJoinCursor::new(
            Box::new(left),
            Box::new(right),
            vec![0],
            vec![0],
            JoinType::Inner,
        );

        let result = drain_cursor(&mut cursor);
        assert_eq!(result.len(), 2);

        // Alice (id=1) matched score=100.
        let alice: Vec<_> = result
            .iter()
            .filter(|r| r[1] == Value::Str("Alice".into()))
            .collect();
        assert_eq!(alice.len(), 1);
        assert_eq!(alice[0][3], Value::F64(100.0));

        // Carol (id=3) matched score=300.
        let carol: Vec<_> = result
            .iter()
            .filter(|r| r[1] == Value::Str("Carol".into()))
            .collect();
        assert_eq!(carol.len(), 1);
        assert_eq!(carol[0][3], Value::F64(300.0));
    }

    #[test]
    fn merge_sort_merges_three_sorted_streams() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(
            schema.clone(),
            &[vec![Value::I64(1)], vec![Value::I64(4)], vec![Value::I64(7)]],
        );
        let s2 = MemoryCursor::from_rows(
            schema.clone(),
            &[vec![Value::I64(2)], vec![Value::I64(5)], vec![Value::I64(8)]],
        );
        let s3 = MemoryCursor::from_rows(
            schema.clone(),
            &[vec![Value::I64(3)], vec![Value::I64(6)], vec![Value::I64(9)]],
        );

        let mut cursor = MergeSortCursor::new(
            vec![Box::new(s1), Box::new(s2), Box::new(s3)],
            vec![OrderBy {
                column: "val".to_string(),
                descending: false,
            }],
        );

        let result = drain_cursor(&mut cursor);
        let values: Vec<i64> = result
            .iter()
            .map(|r| match r[0] {
                Value::I64(v) => v,
                _ => panic!("expected I64"),
            })
            .collect();

        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn topk_gets_top_10_from_10000() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..10000).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = TopKCursor::new(
            Box::new(source),
            10,
            vec![OrderBy {
                column: "val".to_string(),
                descending: true,
            }],
        );

        let result = drain_cursor(&mut cursor);
        assert_eq!(result.len(), 10);

        let values: Vec<i64> = result
            .iter()
            .map(|r| match r[0] {
                Value::I64(v) => v,
                _ => panic!("expected I64"),
            })
            .collect();

        assert_eq!(values, vec![9999, 9998, 9997, 9996, 9995, 9994, 9993, 9992, 9991, 9990]);
    }

    #[test]
    fn sample_by_buckets_correctly() {
        let schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("price".to_string(), ColumnType::F64),
        ];
        let rows = vec![
            vec![Value::Timestamp(10), Value::F64(1.0)],
            vec![Value::Timestamp(20), Value::F64(3.0)],
            vec![Value::Timestamp(110), Value::F64(5.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = SampleByCursor::new(
            Box::new(source),
            100,
            0,
            vec![(AggregateKind::Avg, 1)],
            FillMode::None,
        );

        let result = drain_cursor(&mut cursor);
        assert_eq!(result.len(), 2);

        // Bucket [0..100): avg of 1.0 and 3.0 = 2.0
        assert_eq!(result[0][0], Value::Timestamp(0));
        assert_eq!(result[0][1], Value::F64(2.0));

        // Bucket [100..200): avg of 5.0 = 5.0
        assert_eq!(result[1][0], Value::Timestamp(100));
        assert_eq!(result[1][1], Value::F64(5.0));
    }

    #[test]
    fn union_cursor_concatenates_two_cursors() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(
            schema.clone(),
            &[vec![Value::I64(1)], vec![Value::I64(2)]],
        );
        let s2 = MemoryCursor::from_rows(
            schema.clone(),
            &[vec![Value::I64(3)], vec![Value::I64(4)]],
        );

        let mut cursor = UnionCursor::new(vec![Box::new(s1), Box::new(s2)]);
        let result = drain_cursor(&mut cursor);

        let values: Vec<i64> = result
            .iter()
            .map(|r| match r[0] {
                Value::I64(v) => v,
                _ => panic!("expected I64"),
            })
            .collect();

        assert_eq!(values, vec![1, 2, 3, 4]);
    }
}
