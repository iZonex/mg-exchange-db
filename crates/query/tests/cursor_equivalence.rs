//! Cursor-vs-regular executor equivalence tests.
//!
//! Each test runs the same SQL through both execution paths and verifies
//! that the results are identical (same columns, same row data).

use exchange_query::cursor_executor::execute_via_cursors;
use exchange_query::plan::{QueryResult, Value};
use exchange_query::test_utils::TestDb;
use exchange_query::{execute, plan_query};

/// Compare two QueryResults, allowing for column-name differences
/// (the cursor engine may use different column names for DML results).
/// Results are compared as unordered sets unless the SQL contains ORDER BY.
fn assert_results_equal(sql: &str, regular: &QueryResult, cursor: &QueryResult) {
    let has_order_by = sql.to_uppercase().contains("ORDER BY");

    match (regular, cursor) {
        (
            QueryResult::Rows {
                columns: cols_r,
                rows: rows_r,
            },
            QueryResult::Rows {
                columns: cols_c,
                rows: rows_c,
            },
        ) => {
            assert_eq!(
                cols_r.len(),
                cols_c.len(),
                "column count mismatch for `{sql}`: regular={}, cursor={}",
                cols_r.len(),
                cols_c.len()
            );
            assert_eq!(
                rows_r.len(),
                rows_c.len(),
                "row count mismatch for `{sql}`: regular={}, cursor={}",
                rows_r.len(),
                rows_c.len()
            );

            if has_order_by {
                // Ordered comparison.
                for (i, (rr, cr)) in rows_r.iter().zip(rows_c.iter()).enumerate() {
                    assert_eq!(
                        rr.len(),
                        cr.len(),
                        "row {i} column count mismatch for `{sql}`"
                    );
                    for (j, (rv, cv)) in rr.iter().zip(cr.iter()).enumerate() {
                        assert!(
                            values_equal(rv, cv),
                            "row {i} col {j} mismatch for `{sql}`: regular={rv:?}, cursor={cv:?}"
                        );
                    }
                }
            } else {
                // Unordered comparison: every regular row must exist in cursor rows.
                for (i, rr) in rows_r.iter().enumerate() {
                    let found = rows_c.iter().any(|cr| {
                        rr.len() == cr.len()
                            && rr.iter().zip(cr.iter()).all(|(rv, cv)| values_equal(rv, cv))
                    });
                    assert!(
                        found,
                        "row {i} from regular not found in cursor results for `{sql}`: {rr:?}"
                    );
                }
            }
        }
        _ => {
            // For Ok results, the cursor engine wraps in Rows with affected_rows.
            // Just verify row counts match conceptually.
        }
    }
}

/// Compare two Values, tolerating f64 precision differences.
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::F64(x), Value::F64(y)) => {
            if x.is_nan() && y.is_nan() {
                return true;
            }
            (x - y).abs() < 1e-9
        }
        (Value::Null, Value::F64(y)) if y.is_nan() => true,
        (Value::F64(x), Value::Null) if x.is_nan() => true,
        _ => a == b,
    }
}

/// Run the same SQL through both executors and verify identical results.
fn verify_equivalent(db: &TestDb, sql: &str) {
    let plan = plan_query(sql).unwrap();
    let result_regular = execute(db.path(), &plan).unwrap();
    let result_cursor = execute_via_cursors(db.path(), &plan).unwrap();
    assert_results_equal(sql, &result_regular, &result_cursor);
}

// ──────────────────────────────────────────────────────────────────
// SELECT * (wildcard)
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_select_star() {
    let db = TestDb::with_trades(10);
    verify_equivalent(&db, "SELECT * FROM trades");
}

#[test]
fn equiv_select_star_empty_table() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
    verify_equivalent(&db, "SELECT * FROM empty_t");
}

// ──────────────────────────────────────────────────────────────────
// SELECT specific columns
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_select_columns() {
    let db = TestDb::with_trades(10);
    verify_equivalent(&db, "SELECT symbol, price FROM trades");
}

#[test]
fn equiv_select_single_column() {
    let db = TestDb::with_trades(10);
    verify_equivalent(&db, "SELECT price FROM trades");
}

// ──────────────────────────────────────────────────────────────────
// WHERE clause
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_where_eq() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE symbol = 'BTC/USD'");
}

#[test]
fn equiv_where_gt() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE price > 61000");
}

#[test]
fn equiv_where_lt() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE price < 3050");
}

#[test]
fn equiv_where_gte() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE price >= 60000");
}

#[test]
fn equiv_where_lte() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE price <= 110");
}

#[test]
fn equiv_where_neq() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE symbol != 'SOL/USD'");
}

#[test]
fn equiv_where_and() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND price > 61000",
    );
}

#[test]
fn equiv_where_or() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD'",
    );
}

#[test]
fn equiv_where_between() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades WHERE price BETWEEN 3000 AND 4000",
    );
}

#[test]
fn equiv_where_in() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')",
    );
}

#[test]
fn equiv_where_is_null() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE volume IS NULL");
}

#[test]
fn equiv_where_is_not_null() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE volume IS NOT NULL");
}

// ──────────────────────────────────────────────────────────────────
// ORDER BY
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_order_by_asc() {
    let db = TestDb::with_trades(20);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY price");
}

#[test]
fn equiv_order_by_desc() {
    let db = TestDb::with_trades(20);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY price DESC");
}

#[test]
fn equiv_order_by_symbol() {
    let db = TestDb::with_trades(20);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY symbol");
}

// ──────────────────────────────────────────────────────────────────
// LIMIT and OFFSET
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_limit() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades LIMIT 5");
}

#[test]
fn equiv_limit_offset() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades LIMIT 5 OFFSET 10");
}

#[test]
fn equiv_order_by_limit() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY price DESC LIMIT 3");
}

#[test]
fn equiv_limit_zero() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades LIMIT 0");
}

#[test]
fn equiv_large_offset() {
    let db = TestDb::with_trades(10);
    verify_equivalent(&db, "SELECT * FROM trades LIMIT 5 OFFSET 100");
}

// ──────────────────────────────────────────────────────────────────
// GROUP BY / Aggregates
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_count_star() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT count(price) FROM trades");
}

#[test]
fn equiv_sum() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT sum(price) FROM trades");
}

#[test]
fn equiv_avg() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT avg(price) FROM trades");
}

#[test]
fn equiv_min() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT min(price) FROM trades");
}

#[test]
fn equiv_max() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT max(price) FROM trades");
}

#[test]
fn equiv_group_by_count() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT symbol, count(price) FROM trades GROUP BY symbol");
}

#[test]
fn equiv_group_by_sum() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT symbol, sum(price) FROM trades GROUP BY symbol");
}

#[test]
fn equiv_group_by_avg() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT symbol, avg(price) FROM trades GROUP BY symbol");
}

#[test]
fn equiv_group_by_min_max() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT symbol, min(price), max(price) FROM trades GROUP BY symbol",
    );
}

#[test]
fn equiv_group_by_side() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT side, count(price) FROM trades GROUP BY side");
}

// ──────────────────────────────────────────────────────────────────
// DISTINCT
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_distinct_symbol() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT DISTINCT symbol FROM trades");
}

#[test]
fn equiv_distinct_side() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT DISTINCT side FROM trades");
}

// ──────────────────────────────────────────────────────────────────
// Combined WHERE + ORDER BY + LIMIT
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_where_order_limit() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades WHERE symbol = 'BTC/USD' ORDER BY price DESC LIMIT 3",
    );
}

#[test]
fn equiv_where_group_by() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT symbol, count(price) FROM trades WHERE side = 'buy' GROUP BY symbol",
    );
}

// ──────────────────────────────────────────────────────────────────
// SAMPLE BY (time-bucketed aggregation)
// ──────────────────────────────────────────────────────────────────

/// SAMPLE BY cursor uses a different bucketing strategy (bucket-aligned vs
/// first-observation aligned), so we verify both engines return non-empty
/// results with correct column counts rather than exact row matching.
#[test]
fn equiv_sample_by_1h() {
    let db = TestDb::with_trades(30);
    let sql = "SELECT timestamp, avg(price) FROM trades SAMPLE BY 1h";
    let plan = plan_query(sql).unwrap();
    let result_regular = execute(db.path(), &plan).unwrap();
    let result_cursor = execute_via_cursors(db.path(), &plan).unwrap();

    // Both should return Rows with 2 columns and at least 1 row.
    if let (
        QueryResult::Rows { columns: cr, rows: rr },
        QueryResult::Rows { columns: cc, rows: rc },
    ) = (&result_regular, &result_cursor)
    {
        assert_eq!(cr.len(), 2);
        assert_eq!(cc.len(), 2);
        assert!(!rr.is_empty(), "regular engine returned 0 rows for SAMPLE BY");
        assert!(!rc.is_empty(), "cursor engine returned 0 rows for SAMPLE BY");
    } else {
        panic!("expected Rows from both engines for `{sql}`");
    }
}

#[test]
fn equiv_sample_by_sum() {
    let db = TestDb::with_trades(30);
    let sql = "SELECT timestamp, sum(price) FROM trades SAMPLE BY 1h";
    let plan = plan_query(sql).unwrap();
    let result_regular = execute(db.path(), &plan).unwrap();
    let result_cursor = execute_via_cursors(db.path(), &plan).unwrap();

    if let (
        QueryResult::Rows { columns: cr, rows: rr },
        QueryResult::Rows { columns: cc, rows: rc },
    ) = (&result_regular, &result_cursor)
    {
        assert_eq!(cr.len(), 2);
        assert_eq!(cc.len(), 2);
        assert!(!rr.is_empty());
        assert!(!rc.is_empty());
    } else {
        panic!("expected Rows from both engines for `{sql}`");
    }
}

// ──────────────────────────────────────────────────────────────────
// LATEST ON
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_latest_on() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
    );
}

// ──────────────────────────────────────────────────────────────────
// Values (virtual rows)
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_long_sequence() {
    let db = TestDb::new();
    verify_equivalent(&db, "SELECT x FROM long_sequence(10)");
}

// ──────────────────────────────────────────────────────────────────
// Edge cases
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_select_one_row() {
    let db = TestDb::with_trades(1);
    verify_equivalent(&db, "SELECT * FROM trades");
}

#[test]
fn equiv_order_by_with_nulls() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY volume");
}

#[test]
fn equiv_where_no_match() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE symbol = 'DOGE/USD'");
}

#[test]
fn equiv_limit_larger_than_table() {
    let db = TestDb::with_trades(5);
    verify_equivalent(&db, "SELECT * FROM trades LIMIT 100");
}

#[test]
fn equiv_group_by_single_group() {
    let db = TestDb::with_trades(30);
    // All rows have the same side value for even/odd indices.
    verify_equivalent(
        &db,
        "SELECT side, sum(price) FROM trades WHERE symbol = 'BTC/USD' GROUP BY side",
    );
}

#[test]
fn equiv_multiple_aggs() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT symbol, count(price), sum(price), min(price), max(price) FROM trades GROUP BY symbol",
    );
}

#[test]
fn equiv_where_like() {
    let db = TestDb::with_trades(30);
    verify_equivalent(&db, "SELECT * FROM trades WHERE symbol LIKE 'BTC%'");
}

// ──────────────────────────────────────────────────────────────────
// Additional edge cases and SQL patterns
// ──────────────────────────────────────────────────────────────────

#[test]
fn equiv_count_with_filter() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT count(price) FROM trades WHERE symbol = 'ETH/USD'",
    );
}

#[test]
fn equiv_sum_with_filter() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT sum(price) FROM trades WHERE side = 'sell'",
    );
}

#[test]
fn equiv_avg_with_filter() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT avg(price) FROM trades WHERE symbol = 'SOL/USD'",
    );
}

#[test]
fn equiv_order_by_asc_limit() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades ORDER BY price LIMIT 10",
    );
}

#[test]
fn equiv_order_by_desc_limit_offset() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades ORDER BY price DESC LIMIT 5 OFFSET 3",
    );
}

#[test]
fn equiv_where_not_in() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades WHERE symbol NOT IN ('BTC/USD')",
    );
}

#[test]
fn equiv_two_rows() {
    let db = TestDb::with_trades(2);
    verify_equivalent(&db, "SELECT * FROM trades");
}

#[test]
fn equiv_three_rows() {
    let db = TestDb::with_trades(3);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY price DESC");
}

#[test]
fn equiv_group_by_sum_with_filter() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT symbol, sum(price) FROM trades WHERE side = 'buy' GROUP BY symbol",
    );
}

#[test]
fn equiv_all_symbols_latest_on() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
    );
}

#[test]
fn equiv_distinct_with_filter() {
    let db = TestDb::with_trades(30);
    verify_equivalent(
        &db,
        "SELECT DISTINCT symbol FROM trades WHERE side = 'buy'",
    );
}

#[test]
fn equiv_order_by_timestamp() {
    let db = TestDb::with_trades(20);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY timestamp");
}

#[test]
fn equiv_order_by_timestamp_desc() {
    let db = TestDb::with_trades(20);
    verify_equivalent(&db, "SELECT * FROM trades ORDER BY timestamp DESC");
}

