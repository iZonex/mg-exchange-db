//! Memory bounds and spill-to-disk tests for ExchangeDB query engine.
//!
//! Verifies that query execution respects memory limits, the memory tracker
//! correctly rejects over-budget allocations, and the external sort spill-to-disk
//! mechanism produces correct results under memory pressure.

use std::sync::Arc;

use exchange_query::memory::QueryMemoryTracker;
use exchange_query::plan::{OrderBy, Value};
use exchange_query::spill::ExternalSort;
use exchange_query::test_utils::TestDb;

// ===========================================================================
// QueryMemoryTracker tests
// ===========================================================================

/// Allocation within limit succeeds.
#[test]
fn memory_tracker_within_limit() {
    let tracker = QueryMemoryTracker::new(1024 * 1024, 1);
    tracker.try_allocate(512 * 1024).unwrap();
    assert_eq!(tracker.used(), 512 * 1024);
    assert_eq!(tracker.remaining(), 512 * 1024);
}

/// Allocation exceeding limit returns error.
#[test]
fn memory_tracker_exceeds_limit() {
    let tracker = QueryMemoryTracker::new(1024, 1);
    tracker.try_allocate(800).unwrap();
    let result = tracker.try_allocate(300);
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("memory limit exceeded"));
    // Should not have changed.
    assert_eq!(tracker.used(), 800);
}

/// Exact limit allocation succeeds.
#[test]
fn memory_tracker_exact_limit() {
    let tracker = QueryMemoryTracker::new(1000, 1);
    tracker.try_allocate(1000).unwrap();
    assert_eq!(tracker.used(), 1000);
    assert_eq!(tracker.remaining(), 0);
}

/// Zero limit rejects everything.
#[test]
fn memory_tracker_zero_limit() {
    let tracker = QueryMemoryTracker::new(0, 1);
    assert!(tracker.try_allocate(1).is_err());
}

/// Release frees budget.
#[test]
fn memory_tracker_release_frees_budget() {
    let tracker = QueryMemoryTracker::new(1000, 1);
    tracker.try_allocate(800).unwrap();
    assert!(tracker.try_allocate(300).is_err());

    tracker.release(500);
    assert_eq!(tracker.used(), 300);
    assert_eq!(tracker.remaining(), 700);

    tracker.try_allocate(300).unwrap();
    assert_eq!(tracker.used(), 600);
}

/// Multiple small allocations accumulate.
#[test]
fn memory_tracker_many_small_allocations() {
    let tracker = QueryMemoryTracker::new(10_000, 1);
    for _ in 0..100 {
        tracker.try_allocate(100).unwrap();
    }
    assert_eq!(tracker.used(), 10_000);
    assert!(tracker.try_allocate(1).is_err());
}

/// Allocate and release cycles.
#[test]
fn memory_tracker_alloc_release_cycles() {
    let tracker = QueryMemoryTracker::new(1000, 1);
    for _ in 0..100 {
        tracker.try_allocate(500).unwrap();
        tracker.release(500);
    }
    assert_eq!(tracker.used(), 0);
    assert_eq!(tracker.remaining(), 1000);
}

/// Concurrent allocations via Arc.
#[test]
fn memory_tracker_concurrent_allocations() {
    let tracker = Arc::new(QueryMemoryTracker::new(100_000, 1));
    let mut handles = vec![];

    for _ in 0..10 {
        let tracker = tracker.clone();
        handles.push(std::thread::spawn(move || {
            let mut success = 0;
            for _ in 0..1000 {
                if tracker.try_allocate(10).is_ok() {
                    success += 1;
                }
            }
            success
        }));
    }

    let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    // 100_000 / 10 = 10_000 possible allocations.
    assert_eq!(total, 10_000);
    assert_eq!(tracker.used(), 100_000);
}

/// Query ID is preserved.
#[test]
fn memory_tracker_query_id() {
    let tracker = QueryMemoryTracker::new(1024, 42);
    assert_eq!(tracker.query_id(), 42);
}

/// Limit accessor.
#[test]
fn memory_tracker_limit_accessor() {
    let tracker = QueryMemoryTracker::new(5000, 1);
    assert_eq!(tracker.limit(), 5000);
}

// ===========================================================================
// ExternalSort / spill-to-disk tests
// ===========================================================================

/// Small dataset sorts in-memory (no spill).
#[test]
fn sort_small_no_spill() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "id".to_string(),
        descending: false,
    }];
    let col_names = vec!["id".to_string(), "name".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        1024 * 1024,
        order_by,
        col_names,
    );

    let mut rows = vec![
        vec![Value::I64(3), Value::Str("c".into())],
        vec![Value::I64(1), Value::Str("a".into())],
        vec![Value::I64(2), Value::Str("b".into())],
    ];
    sorter.add_rows(&mut rows).unwrap();
    assert_eq!(sorter.run_count(), 0, "should not spill with large budget");

    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0][0], Value::I64(1));
    assert_eq!(result[1][0], Value::I64(2));
    assert_eq!(result[2][0], Value::I64(3));
}

/// Sort spills to disk with small memory limit.
#[test]
fn sort_spills_to_disk() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "id".to_string(),
        descending: false,
    }];
    let col_names = vec!["id".to_string(), "value".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        200, // Very small
        order_by,
        col_names,
    );

    let mut rows: Vec<Vec<Value>> = (0..1000)
        .rev()
        .map(|i| vec![Value::I64(i), Value::Str(format!("val_{i}"))])
        .collect();
    sorter.add_rows(&mut rows).unwrap();

    assert!(sorter.run_count() > 1, "should have spilled to multiple runs");

    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 1000);
    for i in 0..1000i64 {
        assert_eq!(result[i as usize][0], Value::I64(i));
    }
}

/// Sort 10K rows with very small budget.
#[test]
fn sort_10k_rows_small_budget() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "id".to_string(),
        descending: false,
    }];
    let col_names = vec!["id".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        512,
        order_by,
        col_names,
    );

    let mut rows: Vec<Vec<Value>> = (0..10_000)
        .rev()
        .map(|i| vec![Value::I64(i)])
        .collect();
    sorter.add_rows(&mut rows).unwrap();

    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 10_000);
    for i in 0..10_000i64 {
        assert_eq!(result[i as usize][0], Value::I64(i));
    }
}

/// Descending sort with spill.
#[test]
fn sort_descending_with_spill() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "id".to_string(),
        descending: true,
    }];
    let col_names = vec!["id".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        200,
        order_by,
        col_names,
    );

    let mut rows: Vec<Vec<Value>> = (0..500).map(|i| vec![Value::I64(i)]).collect();
    sorter.add_rows(&mut rows).unwrap();

    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 500);
    assert_eq!(result[0][0], Value::I64(499));
    assert_eq!(result[499][0], Value::I64(0));
}

/// Multi-column sort with spill.
#[test]
fn sort_multi_column_with_spill() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![
        OrderBy {
            column: "group".to_string(),
            descending: false,
        },
        OrderBy {
            column: "value".to_string(),
            descending: true,
        },
    ];
    let col_names = vec!["group".to_string(), "value".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        300,
        order_by,
        col_names,
    );

    let mut rows: Vec<Vec<Value>> = (0..200)
        .map(|i| {
            vec![
                Value::Str(format!("g{}", i % 5)),
                Value::I64(i),
            ]
        })
        .collect();

    // Reference sort.
    let mut expected = rows.clone();
    expected.sort_by(|a, b| {
        let cmp1 = a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal);
        if cmp1 != std::cmp::Ordering::Equal {
            return cmp1;
        }
        b[1].partial_cmp(&a[1]).unwrap_or(std::cmp::Ordering::Equal)
    });

    sorter.add_rows(&mut rows).unwrap();
    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), expected.len());
    for (i, (got, exp)) in result.iter().zip(expected.iter()).enumerate() {
        assert_eq!(got, exp, "mismatch at row {i}");
    }
}

/// Empty sort produces empty result.
#[test]
fn sort_empty() {
    let temp = tempfile::tempdir().unwrap();
    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        1024,
        vec![],
        vec![],
    );
    let mut rows: Vec<Vec<Value>> = vec![];
    sorter.add_rows(&mut rows).unwrap();
    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert!(result.is_empty());
}

/// Single-row sort.
#[test]
fn sort_single_row() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "id".to_string(),
        descending: false,
    }];
    let col_names = vec!["id".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        1024,
        order_by,
        col_names,
    );

    let mut rows = vec![vec![Value::I64(42)]];
    sorter.add_rows(&mut rows).unwrap();
    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], Value::I64(42));
}

/// Sort with NULL values.
#[test]
fn sort_with_nulls() {
    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "id".to_string(),
        descending: false,
    }];
    let col_names = vec!["id".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        200,
        order_by,
        col_names,
    );

    let mut rows = vec![
        vec![Value::I64(3)],
        vec![Value::Null],
        vec![Value::I64(1)],
        vec![Value::Null],
        vec![Value::I64(2)],
    ];
    sorter.add_rows(&mut rows).unwrap();
    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 5);
    // NULLs sort before numeric values in the default ordering.
}

/// Serialization roundtrip for all Value types.
#[test]
fn value_serialization_roundtrip() {
    let row = vec![
        Value::Null,
        Value::I64(i64::MAX),
        Value::I64(i64::MIN),
        Value::F64(std::f64::consts::PI),
        Value::F64(0.0),
        Value::Str("hello world".into()),
        Value::Str("".into()),
        Value::Timestamp(1_710_513_000_000_000_000),
        Value::Timestamp(0),
    ];

    let temp = tempfile::tempdir().unwrap();
    let order_by = vec![OrderBy {
        column: "a".to_string(),
        descending: false,
    }];
    let col_names = vec!["a".to_string()];

    let mut sorter = ExternalSort::new(
        temp.path().to_path_buf(),
        1, // Force spill to test serialization
        order_by,
        col_names,
    );

    let mut rows = vec![row.clone()];
    sorter.add_rows(&mut rows).unwrap();
    let result = sorter.finish().unwrap().collect_rows().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], row);
}

// ===========================================================================
// SQL-level memory and boundary tests via TestDb
// ===========================================================================

/// Large query returns results.
#[test]
fn large_query_returns_results() {
    let db = TestDb::with_trades(1000);
    let (_, rows) = db.query("SELECT * FROM trades ORDER BY timestamp");
    assert_eq!(rows.len(), 1000);
}

/// GROUP BY with many groups.
#[test]
fn group_by_many_groups() {
    let db = TestDb::with_trades(300);
    let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
    assert_eq!(rows.len(), 3); // BTC/USD, ETH/USD, SOL/USD
}

/// ORDER BY on large result set.
#[test]
fn order_by_large_result() {
    let db = TestDb::with_trades(500);
    let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC");
    assert_eq!(rows.len(), 500);
    // Verify descending order.
    for i in 0..rows.len() - 1 {
        let a = match &rows[i][0] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        };
        let b = match &rows[i + 1][0] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        };
        assert!(a >= b, "not sorted descending at index {i}: {a} < {b}");
    }
}

/// COUNT(*) on large table.
#[test]
fn count_large_table() {
    let db = TestDb::with_trades(1000);
    let val = db.query_scalar("SELECT count(*) FROM trades");
    assert_eq!(val, Value::I64(1000));
}

/// SUM on large table.
#[test]
fn sum_large_table() {
    let db = TestDb::with_trades(100);
    let val = db.query_scalar("SELECT sum(price) FROM trades");
    match val {
        Value::F64(v) => assert!(v > 0.0),
        _ => panic!("expected F64"),
    }
}

/// AVG on large table.
#[test]
fn avg_large_table() {
    let db = TestDb::with_trades(100);
    let val = db.query_scalar("SELECT avg(price) FROM trades");
    match val {
        Value::F64(v) => assert!(v > 0.0),
        _ => panic!("expected F64"),
    }
}

/// MIN/MAX on large table.
#[test]
fn min_max_large_table() {
    let db = TestDb::with_trades(100);
    let min = db.query_scalar("SELECT min(price) FROM trades");
    let max = db.query_scalar("SELECT max(price) FROM trades");
    match (&min, &max) {
        (Value::F64(a), Value::F64(b)) => assert!(a < b),
        _ => panic!("expected F64"),
    }
}

/// LIMIT restricts output.
#[test]
fn limit_restricts_output() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query("SELECT * FROM trades LIMIT 10");
    assert_eq!(rows.len(), 10);
}

/// OFFSET skips rows.
#[test]
fn offset_skips_rows() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 90");
    assert_eq!(rows.len(), 10);
}

/// Multiple aggregates in one query.
#[test]
fn multiple_aggregates() {
    let db = TestDb::with_trades(100);
    let (cols, rows) = db.query(
        "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades",
    );
    assert_eq!(cols.len(), 5);
    assert_eq!(rows.len(), 1);
}

/// WHERE + ORDER BY + LIMIT combo.
#[test]
fn where_order_limit_combo() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query(
        "SELECT price FROM trades WHERE symbol = 'BTC/USD' ORDER BY price DESC LIMIT 5",
    );
    assert!(rows.len() <= 5);
    // Verify descending.
    for i in 0..rows.len().saturating_sub(1) {
        if let (Value::F64(a), Value::F64(b)) = (&rows[i][0], &rows[i + 1][0]) {
            assert!(a >= b);
        }
    }
}

/// GROUP BY + HAVING filters groups.
#[test]
fn group_by_having() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query(
        "SELECT symbol, count(*) FROM trades GROUP BY symbol HAVING count(*) > 0",
    );
    assert!(!rows.is_empty());
    // All groups should have count > 0.
    for row in &rows {
        if let Value::I64(cnt) = &row[1] {
            assert!(*cnt > 0);
        }
    }
}

/// DISTINCT removes duplicates.
#[test]
fn distinct_removes_duplicates() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
    assert_eq!(rows.len(), 3); // 3 symbols
}
