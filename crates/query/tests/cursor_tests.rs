//! Integration tests for the cursor-based execution framework.

use exchange_common::types::{ColumnType, PartitionBy};
use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
use exchange_query::batch::RecordBatch;
use exchange_query::cursors::*;
use exchange_query::plan::{AggregateKind, Filter, OrderBy, QueryPlan, SelectColumn, Value};
use exchange_query::record_cursor::RecordCursor;
use tempfile::TempDir;

use std::path::Path;

/// Helper: create a table with DAY partitioning and insert rows
/// spanning multiple days so we get multiple partitions.
fn setup_trades_table(db_root: &Path) -> Vec<(usize, String, ColumnType)> {
    use exchange_common::types::Timestamp;

    TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    let mut writer = TableWriter::open(db_root, "trades").unwrap();

    // Day 1: 2024-01-01 (3 rows)
    let day1_base = 1_704_067_200_000_000_000i64;
    for i in 0..3 {
        let ts = Timestamp(day1_base + i * 1_000_000_000);
        writer
            .write_row(
                ts,
                &[
                    ColumnValue::F64(100.0 + i as f64),
                    ColumnValue::F64(10.0 + i as f64),
                ],
            )
            .unwrap();
    }

    // Day 2: 2024-01-02 (2 rows)
    let day2_base = day1_base + 86_400_000_000_000;
    for i in 0..2 {
        let ts = Timestamp(day2_base + i * 1_000_000_000);
        writer
            .write_row(
                ts,
                &[
                    ColumnValue::F64(200.0 + i as f64),
                    ColumnValue::F64(20.0 + i as f64),
                ],
            )
            .unwrap();
    }

    // Day 3: 2024-01-03 (4 rows)
    let day3_base = day1_base + 2 * 86_400_000_000_000;
    for i in 0..4 {
        let ts = Timestamp(day3_base + i * 1_000_000_000);
        writer
            .write_row(
                ts,
                &[
                    ColumnValue::F64(300.0 + i as f64),
                    ColumnValue::F64(30.0 + i as f64),
                ],
            )
            .unwrap();
    }

    writer.flush().unwrap();
    drop(writer);

    vec![
        (0, "timestamp".to_string(), ColumnType::Timestamp),
        (1, "price".to_string(), ColumnType::F64),
        (2, "volume".to_string(), ColumnType::F64),
    ]
}

/// Drain a cursor into a Vec of rows.
fn drain_cursor(cursor: &mut dyn RecordCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    loop {
        match cursor.next_batch(1024).unwrap() {
            None => break,
            Some(batch) => {
                rows.extend(batch.to_rows());
            }
        }
    }
    rows
}

// ── RecordBatch tests ────────────────────────────────────────────────

#[test]
fn record_batch_roundtrip() {
    let cols = vec!["id".to_string(), "name".to_string(), "price".to_string()];
    let rows = vec![
        vec![Value::I64(1), Value::Str("BTC".into()), Value::F64(50000.0)],
        vec![Value::I64(2), Value::Str("ETH".into()), Value::F64(3000.0)],
        vec![Value::I64(3), Value::Str("SOL".into()), Value::F64(100.0)],
    ];

    let batch = RecordBatch::from_rows(&cols, &rows);
    assert_eq!(batch.row_count(), 3);
    assert_eq!(batch.to_rows(), rows);
}

#[test]
fn record_batch_slice_and_concat() {
    let cols = vec!["x".to_string()];
    let rows: Vec<Vec<Value>> = (0..5).map(|i| vec![Value::I64(i)]).collect();
    let batch = RecordBatch::from_rows(&cols, &rows);

    let s1 = batch.slice(0, 2);
    let s2 = batch.slice(2, 3);
    assert_eq!(s1.row_count(), 2);
    assert_eq!(s2.row_count(), 3);

    let merged = RecordBatch::concat(&[&s1, &s2]);
    assert_eq!(merged.row_count(), 5);
    assert_eq!(merged.to_rows(), rows);
}

// ── ScanCursor tests ─────────────────────────────────────────────────

#[test]
fn scan_cursor_reads_all_rows_from_multi_partition_table() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let mut cursor = ScanCursor::new(partitions, columns);
    let rows = drain_cursor(&mut cursor);

    // 3 + 2 + 4 = 9 rows
    assert_eq!(rows.len(), 9);
}

#[test]
fn scan_cursor_respects_batch_size() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let mut cursor = ScanCursor::new(partitions, columns);

    // Request only 2 rows at a time.
    let batch1 = cursor.next_batch(2).unwrap().unwrap();
    assert!(batch1.row_count() <= 2);

    // Drain the rest.
    let mut total = batch1.row_count();
    loop {
        match cursor.next_batch(2).unwrap() {
            None => break,
            Some(b) => {
                assert!(b.row_count() <= 2);
                total += b.row_count();
            }
        }
    }
    assert_eq!(total, 9);
}

// ── FilterCursor tests ──────────────────────────────────────────────

#[test]
fn filter_cursor_filters_correctly() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let scan = Box::new(ScanCursor::new(partitions, columns));

    // Filter: price >= 200.0
    let filter = Filter::Gte("price".to_string(), Value::F64(200.0));
    let mut cursor = FilterCursor::new(scan, filter);
    let rows = drain_cursor(&mut cursor);

    // Day 2: 200.0, 201.0 (2 rows) + Day 3: 300..303 (4 rows) = 6
    assert_eq!(rows.len(), 6);

    // All prices should be >= 200.0
    for row in &rows {
        match &row[1] {
            Value::F64(v) => assert!(*v >= 200.0, "expected >= 200.0, got {v}"),
            other => panic!("expected F64, got {other:?}"),
        }
    }
}

#[test]
fn filter_cursor_with_and() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let scan = Box::new(ScanCursor::new(partitions, columns));

    // price >= 200.0 AND price < 301.0
    let filter = Filter::And(vec![
        Filter::Gte("price".to_string(), Value::F64(200.0)),
        Filter::Lt("price".to_string(), Value::F64(301.0)),
    ]);
    let mut cursor = FilterCursor::new(scan, filter);
    let rows = drain_cursor(&mut cursor);

    // 200.0, 201.0, 300.0 = 3 rows
    assert_eq!(rows.len(), 3);
}

// ── LimitCursor tests ───────────────────────────────────────────────

#[test]
fn limit_cursor_respects_limit() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let scan = Box::new(ScanCursor::new(partitions, columns));
    let mut cursor = LimitCursor::new(scan, 3, 0);
    let rows = drain_cursor(&mut cursor);

    assert_eq!(rows.len(), 3);
}

#[test]
fn limit_cursor_respects_offset() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let scan = Box::new(ScanCursor::new(partitions, columns));
    // Skip 7, take 5 => only 2 rows available (9 - 7 = 2)
    let mut cursor = LimitCursor::new(scan, 5, 7);
    let rows = drain_cursor(&mut cursor);

    assert_eq!(rows.len(), 2);
}

#[test]
fn limit_cursor_offset_and_limit() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let scan = Box::new(ScanCursor::new(partitions, columns));
    // Skip 2, take 3
    let mut cursor = LimitCursor::new(scan, 3, 2);
    let rows = drain_cursor(&mut cursor);

    assert_eq!(rows.len(), 3);
    // First row after skipping 2 should be the 3rd original row (price=102.0)
    match &rows[0][1] {
        Value::F64(v) => assert_eq!(*v, 102.0),
        other => panic!("expected F64, got {other:?}"),
    }
}

// ── SortCursor tests ────────────────────────────────────────────────

#[test]
fn sort_cursor_sorts_correctly() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    let scan = Box::new(ScanCursor::new(partitions, columns));

    // Sort by price descending.
    let order_by = vec![OrderBy {
        column: "price".to_string(),
        descending: true,
    }];

    let mut cursor = SortCursor::new(scan, order_by);
    let rows = drain_cursor(&mut cursor);

    assert_eq!(rows.len(), 9);

    // Verify descending order.
    for i in 1..rows.len() {
        let prev = match &rows[i - 1][1] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        };
        let curr = match &rows[i][1] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        };
        assert!(prev >= curr, "expected descending: {prev} >= {curr}");
    }
}

#[test]
fn sort_cursor_ascending() {
    let schema = vec![
        ("name".to_string(), ColumnType::Varchar),
        ("score".to_string(), ColumnType::I64),
    ];
    let batch = {
        let mut b = RecordBatch::new(schema.clone());
        b.append_row(&[Value::Str("Charlie".into()), Value::I64(50)]);
        b.append_row(&[Value::Str("Alice".into()), Value::I64(90)]);
        b.append_row(&[Value::Str("Bob".into()), Value::I64(70)]);
        b
    };

    let mem = Box::new(MemoryCursor::new(batch));
    let order_by = vec![OrderBy {
        column: "score".to_string(),
        descending: false,
    }];

    let mut cursor = SortCursor::new(mem, order_by);
    let rows = drain_cursor(&mut cursor);

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::I64(50));
    assert_eq!(rows[1][1], Value::I64(70));
    assert_eq!(rows[2][1], Value::I64(90));
}

// ── Pipeline tests ──────────────────────────────────────────────────

#[test]
fn pipeline_scan_filter_sort_limit() {
    let tmp = TempDir::new().unwrap();
    let columns = setup_trades_table(tmp.path());

    let table_dir = tmp.path().join("trades");
    let partitions = exchange_core::table::list_partitions(&table_dir).unwrap();

    // Scan -> Filter(price >= 200) -> Sort(price DESC) -> Limit(3)
    let scan = Box::new(ScanCursor::new(partitions, columns));
    let filter = Filter::Gte("price".to_string(), Value::F64(200.0));
    let filtered = Box::new(FilterCursor::new(scan, filter));
    let sorted = Box::new(SortCursor::new(
        filtered,
        vec![OrderBy {
            column: "price".to_string(),
            descending: true,
        }],
    ));
    let mut limited = LimitCursor::new(sorted, 3, 0);

    let rows = drain_cursor(&mut limited);
    assert_eq!(rows.len(), 3);

    // Highest 3 prices >= 200: 303, 302, 301
    let prices: Vec<f64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        })
        .collect();

    assert_eq!(prices, vec![303.0, 302.0, 301.0]);
}

// ── MemoryCursor tests ──────────────────────────────────────────────

#[test]
fn memory_cursor_works() {
    let schema = vec![
        ("a".to_string(), ColumnType::I64),
        ("b".to_string(), ColumnType::Varchar),
    ];
    let rows = vec![
        vec![Value::I64(1), Value::Str("hello".into())],
        vec![Value::I64(2), Value::Str("world".into())],
    ];

    let mut cursor = MemoryCursor::from_rows(schema, &rows);

    assert_eq!(cursor.estimated_rows(), Some(2));

    let batch = cursor.next_batch(100).unwrap().unwrap();
    assert_eq!(batch.row_count(), 2);
    assert_eq!(batch.get_value(0, 0), Value::I64(1));
    assert_eq!(batch.get_value(1, 1), Value::Str("world".into()));

    // Exhausted.
    assert!(cursor.next_batch(100).unwrap().is_none());
}

#[test]
fn memory_cursor_batching() {
    let schema = vec![("x".to_string(), ColumnType::I64)];
    let rows: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::I64(i)]).collect();

    let mut cursor = MemoryCursor::from_rows(schema, &rows);

    let b1 = cursor.next_batch(3).unwrap().unwrap();
    assert_eq!(b1.row_count(), 3);

    let b2 = cursor.next_batch(3).unwrap().unwrap();
    assert_eq!(b2.row_count(), 3);

    let b3 = cursor.next_batch(3).unwrap().unwrap();
    assert_eq!(b3.row_count(), 3);

    let b4 = cursor.next_batch(3).unwrap().unwrap();
    assert_eq!(b4.row_count(), 1);

    assert!(cursor.next_batch(3).unwrap().is_none());
}

// ── EmptyCursor tests ───────────────────────────────────────────────

#[test]
fn empty_cursor_returns_none() {
    let schema = vec![("a".to_string(), ColumnType::I64)];
    let mut cursor = EmptyCursor::new(schema.clone());

    assert!(cursor.next_batch(100).unwrap().is_none());
    assert_eq!(cursor.estimated_rows(), Some(0));
    assert_eq!(cursor.schema(), &schema[..]);
}

// ── ProjectCursor tests ─────────────────────────────────────────────

#[test]
fn project_cursor_selects_columns() {
    let schema = vec![
        ("a".to_string(), ColumnType::I64),
        ("b".to_string(), ColumnType::F64),
        ("c".to_string(), ColumnType::Varchar),
    ];
    let rows = vec![
        vec![Value::I64(1), Value::F64(2.0), Value::Str("x".into())],
        vec![Value::I64(3), Value::F64(4.0), Value::Str("y".into())],
    ];
    let mem = Box::new(MemoryCursor::from_rows(schema, &rows));

    // Project columns a and c (indices 0 and 2).
    let mut cursor = ProjectCursor::new(mem, vec![0, 2]);

    let result_schema = cursor.schema();
    assert_eq!(result_schema.len(), 2);
    assert_eq!(result_schema[0].0, "a");
    assert_eq!(result_schema[1].0, "c");

    let result = drain_cursor(&mut cursor);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], vec![Value::I64(1), Value::Str("x".into())]);
    assert_eq!(result[1], vec![Value::I64(3), Value::Str("y".into())]);
}

// ── AggregateCursor tests ───────────────────────────────────────────

#[test]
fn aggregate_cursor_no_group_by() {
    let schema = vec![
        ("name".to_string(), ColumnType::Varchar),
        ("price".to_string(), ColumnType::F64),
    ];
    let rows = vec![
        vec![Value::Str("a".into()), Value::F64(10.0)],
        vec![Value::Str("b".into()), Value::F64(20.0)],
        vec![Value::Str("c".into()), Value::F64(30.0)],
    ];
    let mem = Box::new(MemoryCursor::from_rows(schema, &rows));

    let aggregates = vec![
        (AggregateKind::Sum, "price".to_string()),
        (AggregateKind::Count, "price".to_string()),
        (AggregateKind::Avg, "price".to_string()),
    ];

    let mut cursor = AggregateCursor::new(mem, vec![], aggregates);
    let result = drain_cursor(&mut cursor);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], Value::F64(60.0)); // sum
    assert_eq!(result[0][1], Value::I64(3)); // count
    assert_eq!(result[0][2], Value::F64(20.0)); // avg
}

#[test]
fn aggregate_cursor_with_group_by() {
    let schema = vec![
        ("category".to_string(), ColumnType::Varchar),
        ("value".to_string(), ColumnType::F64),
    ];
    let rows = vec![
        vec![Value::Str("A".into()), Value::F64(10.0)],
        vec![Value::Str("B".into()), Value::F64(20.0)],
        vec![Value::Str("A".into()), Value::F64(30.0)],
        vec![Value::Str("B".into()), Value::F64(40.0)],
        vec![Value::Str("A".into()), Value::F64(50.0)],
    ];
    let mem = Box::new(MemoryCursor::from_rows(schema, &rows));

    let aggregates = vec![
        (AggregateKind::Sum, "value".to_string()),
        (AggregateKind::Count, "value".to_string()),
    ];

    let mut cursor = AggregateCursor::new(mem, vec!["category".to_string()], aggregates);
    let result = drain_cursor(&mut cursor);

    assert_eq!(result.len(), 2);

    // Group A: sum=90, count=3
    assert_eq!(result[0][0], Value::Str("A".into()));
    assert_eq!(result[0][1], Value::F64(90.0));
    assert_eq!(result[0][2], Value::I64(3));

    // Group B: sum=60, count=2
    assert_eq!(result[1][0], Value::Str("B".into()));
    assert_eq!(result[1][1], Value::F64(60.0));
    assert_eq!(result[1][2], Value::I64(2));
}

// ── build_cursor integration tests ──────────────────────────────────

#[test]
fn build_cursor_select_all() {
    let tmp = TempDir::new().unwrap();
    let _columns = setup_trades_table(tmp.path());

    let plan = QueryPlan::Select {
        table: "trades".to_string(),
        columns: vec![SelectColumn::Wildcard],
        filter: None,
        order_by: vec![],
        limit: None,
        offset: None,
        sample_by: None,
        latest_on: None,
        group_by: vec![],
        group_by_mode: exchange_query::plan::GroupByMode::Normal,
        having: None,
        distinct: false,
        distinct_on: vec![],
    };

    let mut cursor = exchange_query::cursors::build_cursor(tmp.path(), &plan).unwrap();
    let rows = drain_cursor(cursor.as_mut());
    assert_eq!(rows.len(), 9);
}

#[test]
fn build_cursor_with_filter_and_limit() {
    let tmp = TempDir::new().unwrap();
    let _columns = setup_trades_table(tmp.path());

    let plan = QueryPlan::Select {
        table: "trades".to_string(),
        columns: vec![SelectColumn::Wildcard],
        filter: Some(Filter::Gte("price".to_string(), Value::F64(200.0))),
        order_by: vec![OrderBy {
            column: "price".to_string(),
            descending: true,
        }],
        limit: Some(2),
        offset: None,
        sample_by: None,
        latest_on: None,
        group_by: vec![],
        group_by_mode: exchange_query::plan::GroupByMode::Normal,
        having: None,
        distinct: false,
        distinct_on: vec![],
    };

    let mut cursor = exchange_query::cursors::build_cursor(tmp.path(), &plan).unwrap();
    let rows = drain_cursor(cursor.as_mut());
    assert_eq!(rows.len(), 2);

    // Descending order, top 2: 303, 302
    let prices: Vec<f64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        })
        .collect();
    assert_eq!(prices, vec![303.0, 302.0]);
}
