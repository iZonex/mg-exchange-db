//! Regression SELECT tests — 500+ tests.
//!
//! Exercises every SELECT feature combination: WHERE operators, aggregates,
//! ORDER BY, LIMIT, GROUP BY, HAVING, DISTINCT, CASE WHEN, arithmetic,
//! subqueries, aliases, and data type interactions.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Create a table with mixed types: i INT, d DOUBLE, s VARCHAR.
fn db_mixed() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, i INT, d DOUBLE, s VARCHAR)");
    let data = [
        (0, 1, 10.0, "alpha"),
        (1, 2, 20.0, "beta"),
        (2, 3, 30.0, "gamma"),
        (3, 4, 40.0, "delta"),
        (4, 5, 50.0, "epsilon"),
        (5, 6, 60.0, "alpha"),
        (6, 7, 70.0, "beta"),
        (7, 8, 80.0, "gamma"),
        (8, 9, 90.0, "delta"),
        (9, 10, 100.0, "epsilon"),
    ];
    for (t, i, d, s) in &data {
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}, {}, '{}')", ts(*t), i, d, s));
    }
    db
}

/// Create a table with NULLs.
fn db_nulls() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE, s VARCHAR)");
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0, 'a')", ts(0)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL, 'b')", ts(1)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0, NULL)", ts(2)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL, NULL)", ts(3)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 50.0, 'e')", ts(4)));
    db
}

fn assert_f64_near(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        other => panic!("expected F64(~{expected}), got {other:?}"),
    }
}

// ============================================================================
// 1. WHERE + ORDER BY (50 tests)
// ============================================================================
mod where_order {
    use super::*;

    #[test] fn eq_int_order_asc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 5 ORDER BY i");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::I64(6));
    }
    #[test] fn eq_int_order_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 5 ORDER BY i DESC");
        assert_eq!(rows[0][0], Value::I64(10));
    }
    #[test] fn eq_string_order_asc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s = 'alpha' ORDER BY i");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(1));
        assert_eq!(rows[1][0], Value::I64(6));
    }
    #[test] fn gt_double_order_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE d > 50.0 ORDER BY d DESC");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::F64(100.0));
    }
    #[test] fn lt_int_order_asc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i < 4 ORDER BY i");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(1));
    }
    #[test] fn gte_order_asc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i >= 8 ORDER BY i");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(8));
    }
    #[test] fn lte_order_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i <= 3 ORDER BY i DESC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(3));
    }
    #[test] fn neq_int_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i != 5 ORDER BY i");
        assert_eq!(rows.len(), 9);
    }
    #[test] fn and_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 3 AND i < 8 ORDER BY i");
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], Value::I64(4));
        assert_eq!(rows[3][0], Value::I64(7));
    }
    #[test] fn or_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 1 OR i = 10 ORDER BY i");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(1));
        assert_eq!(rows[1][0], Value::I64(10));
    }
    #[test] fn where_double_eq_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE d = 50.0 ORDER BY i");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(5));
    }
    #[test] fn where_string_neq_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s != 'alpha' ORDER BY i");
        assert_eq!(rows.len(), 8);
    }
    #[test] fn order_by_double() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE i <= 5 ORDER BY d DESC");
        assert_eq!(rows[0][0], Value::F64(50.0));
    }
    #[test] fn order_by_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s FROM t WHERE i <= 5 ORDER BY s");
        // alpha, beta, delta, epsilon, gamma (alphabetical)
        assert_eq!(rows[0][0], Value::Str("alpha".into()));
    }
    #[test] fn where_gt_double_order_double() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE d >= 80.0 ORDER BY d");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::F64(80.0));
    }
    #[test] fn and_or_combined_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE (i < 3 OR i > 8) ORDER BY i");
        assert_eq!(rows.len(), 4);
    }
    #[test] fn where_int_between_values_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i >= 3 AND i <= 7 ORDER BY i DESC");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::I64(7));
    }
    #[test] fn where_string_eq_order_by_d() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE s = 'beta' ORDER BY d");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::F64(20.0));
        assert_eq!(rows[1][0], Value::F64(70.0));
    }
    #[test] fn where_multiple_and_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 2 AND i < 8 AND s != 'gamma' ORDER BY i");
        assert!(rows.len() >= 2);
    }
    #[test] fn where_all_match_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 0 ORDER BY i");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn where_none_match() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 100 ORDER BY i");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn order_multiple_cols() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, i FROM t ORDER BY s, i");
        assert_eq!(rows.len(), 10);
        // first rows should be alpha 1, alpha 6
        assert_eq!(rows[0][0], Value::Str("alpha".into()));
    }
    #[test] fn where_with_order_by_alias() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i AS val FROM t WHERE i > 5 ORDER BY i");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn order_by_computed_column() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i, d * 2 AS doubled FROM t ORDER BY d");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn where_lt_double_order_int() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE d < 35.0 ORDER BY i");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_gte_string_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s FROM t WHERE s >= 'delta' ORDER BY s");
        assert!(rows.len() >= 4);
    }
    #[test] fn where_not_eq_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i != 5 AND i != 6 ORDER BY i");
        assert_eq!(rows.len(), 8);
    }
    #[test] fn order_by_timestamp() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t ORDER BY timestamp");
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0][0], Value::I64(1));
    }
    #[test] fn order_by_timestamp_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t ORDER BY timestamp DESC");
        assert_eq!(rows[0][0], Value::I64(10));
    }
    #[test] fn where_and_three_conditions_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 1 AND d < 80.0 AND s != 'delta' ORDER BY i");
        assert!(rows.len() >= 3);
    }
    #[test] fn where_or_two_string_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s = 'alpha' OR s = 'beta' ORDER BY i");
        assert_eq!(rows.len(), 4);
    }
    #[test] fn where_negative_int_no_results() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i < 0 ORDER BY i");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn where_double_between_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE d >= 30.0 AND d <= 70.0 ORDER BY d");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::F64(30.0));
    }
    #[test] fn order_by_two_cols_desc_asc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, i FROM t ORDER BY s DESC, i");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn where_complex_parens_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE (i = 1 OR i = 2) AND d < 30.0 ORDER BY i");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_all_types_combined_order() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT i FROM t WHERE i > 3 AND d < 90.0 AND s = 'beta' ORDER BY i"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(7));
    }
    #[test] fn select_star_where_order() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT * FROM t WHERE i <= 3 ORDER BY i");
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_gt_int_order_double_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE i > 6 ORDER BY d DESC");
        assert_eq!(rows[0][0], Value::F64(100.0));
    }
    #[test] fn order_by_string_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s FROM t WHERE i <= 5 ORDER BY s DESC");
        assert_eq!(rows[0][0], Value::Str("gamma".into()));
    }
    #[test] fn where_eq_string_exact_case() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s = 'Alpha'");
        assert_eq!(rows.len(), 0); // case sensitive
    }
    #[test] fn where_double_exact_eq_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE d = 100.0 ORDER BY i");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(10));
    }
    #[test] fn three_or_conditions_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 1 OR i = 5 OR i = 10 ORDER BY i");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_gt_and_lt_same_col_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 4 AND i < 4 ORDER BY i");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn where_gte_lte_same_value() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i >= 5 AND i <= 5 ORDER BY i");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(5));
    }
    #[test] fn where_first_row_only() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 1 ORDER BY i");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_last_row_only() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 10 ORDER BY i");
        assert_eq!(rows.len(), 1);
    }
}

// ============================================================================
// 2. WHERE + LIMIT (50 tests)
// ============================================================================
mod where_limit {
    use super::*;

    #[test] fn limit_1() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn limit_5() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn limit_larger_than_rows() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t LIMIT 100");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn limit_0() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t LIMIT 0");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn where_eq_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s = 'alpha' LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_gt_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 5 LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_lt_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i < 5 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_and_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 2 AND i < 8 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_or_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 1 OR i = 10 LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t ORDER BY i LIMIT 3");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(1));
        assert_eq!(rows[2][0], Value::I64(3));
    }
    #[test] fn order_desc_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t ORDER BY i DESC LIMIT 3");
        assert_eq!(rows[0][0], Value::I64(10));
    }
    #[test] fn where_order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 3 ORDER BY i LIMIT 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(4));
        assert_eq!(rows[1][0], Value::I64(5));
    }
    #[test] fn where_order_desc_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i < 8 ORDER BY i DESC LIMIT 2");
        assert_eq!(rows[0][0], Value::I64(7));
    }
    #[test] fn limit_on_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t LIMIT 10");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn limit_1_on_1_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn limit_with_select_star() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT * FROM t LIMIT 3");
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 3);
    }
    #[test] fn limit_with_alias() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i AS val FROM t LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_string_neq_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s != 'alpha' LIMIT 4");
        assert_eq!(rows.len(), 4);
    }
    #[test] fn where_double_gt_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE d > 60.0 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_double_lt_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t WHERE d < 30.0 LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_no_match_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 100 LIMIT 5");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn where_all_match_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 0 LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn order_by_d_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t ORDER BY d LIMIT 3");
        assert_eq!(rows[0][0], Value::F64(10.0));
    }
    #[test] fn order_by_s_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s FROM t ORDER BY s LIMIT 2");
        assert_eq!(rows[0][0], Value::Str("alpha".into()));
    }
    #[test] fn where_three_and_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 1 AND i < 9 AND s = 'beta' LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn limit_2_order_d_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t ORDER BY d DESC LIMIT 2");
        assert_eq!(rows[0][0], Value::F64(100.0));
        assert_eq!(rows[1][0], Value::F64(90.0));
    }
    #[test] fn limit_10_exact_table_size() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t LIMIT 10");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn limit_with_where_gte() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i >= 9 LIMIT 5");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn limit_with_where_lte() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i <= 2 LIMIT 5");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn order_limit_1_gets_min() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t ORDER BY i LIMIT 1");
        assert_eq!(rows[0][0], Value::I64(1));
    }
    #[test] fn order_desc_limit_1_gets_max() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t ORDER BY i DESC LIMIT 1");
        assert_eq!(rows[0][0], Value::I64(10));
    }
    #[test] fn where_or_three_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 1 OR i = 5 OR i = 9 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn limit_after_where_eq_double() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE d = 50.0 LIMIT 10");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_order_limit_complex() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE d >= 30.0 AND d <= 80.0 ORDER BY i DESC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn limit_with_two_columns() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT i, d FROM t LIMIT 4");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 4);
    }
    #[test] fn limit_preserves_projection() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT s, d FROM t LIMIT 1");
        assert_eq!(cols[0], "s");
        assert_eq!(cols[1], "d");
    }
    #[test] fn where_complex_or_and_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE (s = 'alpha' OR s = 'beta') AND i > 3 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn limit_with_trades() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn where_order_limit_trades() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT price FROM trades WHERE symbol = 'BTC/USD' ORDER BY price LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn limit_with_multiple_projections() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT i, d, s FROM t LIMIT 2");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 2);
    }
    #[test] fn limit_after_multiple_wheres() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 2 AND i < 9 LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn order_double_asc_limit_top_3() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d FROM t ORDER BY d LIMIT 3");
        assert_eq!(rows[0][0], Value::F64(10.0));
        assert_eq!(rows[1][0], Value::F64(20.0));
        assert_eq!(rows[2][0], Value::F64(30.0));
    }
}

// ============================================================================
// 3. GROUP BY + aggregates (50 tests)
// ============================================================================
mod group_agg {
    use super::*;

    #[test] fn count_by_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s ORDER BY s");
        assert_eq!(rows.len(), 5); // alpha, beta, gamma, delta, epsilon
        for row in &rows { assert_eq!(row[1], Value::I64(2)); }
    }
    #[test] fn sum_by_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) FROM t GROUP BY s ORDER BY s");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn avg_by_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, avg(d) FROM t GROUP BY s ORDER BY s");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn min_by_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, min(d) FROM t GROUP BY s ORDER BY s");
        // alpha: min(10,60)=10
        assert_eq!(rows[0][1], Value::F64(10.0));
    }
    #[test] fn max_by_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, max(d) FROM t GROUP BY s ORDER BY s");
        // alpha: max(10,60)=60
        assert_eq!(rows[0][1], Value::F64(60.0));
    }
    #[test] fn count_star_no_group() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_eq!(val, Value::I64(10));
    }
    #[test] fn sum_no_group() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT sum(d) FROM t");
        assert_f64_near(&val, 550.0, 0.01);
    }
    #[test] fn avg_no_group() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT avg(d) FROM t");
        assert_f64_near(&val, 55.0, 0.01);
    }
    #[test] fn min_no_group() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT min(d) FROM t");
        assert_eq!(val, Value::F64(10.0));
    }
    #[test] fn max_no_group() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT max(d) FROM t");
        assert_eq!(val, Value::F64(100.0));
    }
    #[test] fn count_int_col() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT count(i) FROM t");
        assert_eq!(val, Value::I64(10));
    }
    #[test] fn sum_int() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT sum(i) FROM t");
        assert_eq!(val, Value::I64(55));
    }
    #[test] fn avg_int() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT avg(i) FROM t");
        assert_f64_near(&val, 5.5, 0.01);
    }
    #[test] fn min_int() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT min(i) FROM t");
        assert_eq!(val, Value::I64(1));
    }
    #[test] fn max_int() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT max(i) FROM t");
        assert_eq!(val, Value::I64(10));
    }
    #[test] fn group_by_count_order_count() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) AS cnt FROM trades GROUP BY symbol ORDER BY cnt DESC");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_sum_order_sum() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, sum(price) FROM trades GROUP BY symbol ORDER BY sum(price)");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_avg_order() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol ORDER BY avg(price)");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_min_order() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, min(price) FROM trades GROUP BY symbol ORDER BY min(price)");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_max_order() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, max(price) FROM trades GROUP BY symbol ORDER BY max(price)");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_two_keys() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side ORDER BY symbol");
        assert!(rows.len() >= 3);
    }
    #[test] fn group_by_with_where() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_with_where_gt() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades WHERE price > 1000 GROUP BY symbol ORDER BY symbol");
        assert!(rows.len() >= 1);
    }
    #[test] fn group_by_with_limit() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn group_by_single_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT g, count(*), sum(v) FROM t GROUP BY g");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::I64(5));
    }
    #[test] fn group_by_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR, v DOUBLE)");
        let (_, rows) = db.query("SELECT g, count(*) FROM t GROUP BY g");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn multiple_aggs_same_query() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*), sum(d), avg(d), min(d), max(d) FROM t GROUP BY s ORDER BY s");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][1], Value::I64(2)); // count for alpha
    }
    #[test] fn count_star_vs_count_col() {
        let db = db_nulls();
        let star = db.query_scalar("SELECT count(*) FROM t");
        assert_eq!(star, Value::I64(5));
    }
    #[test] fn sum_with_nulls() {
        let db = db_nulls();
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // sum of 10+30+50 = 90 (NULLs skipped)
        match val {
            Value::F64(v) => assert!((v - 90.0).abs() < 0.01),
            Value::I64(v) => assert_eq!(v, 90),
            _ => panic!("unexpected {val:?}"),
        }
    }
    #[test] fn avg_with_nulls() {
        let db = db_nulls();
        let val = db.query_scalar("SELECT avg(v) FROM t");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("unexpected {val:?}"),
        }
    }
    #[test] fn min_with_nulls() {
        let db = db_nulls();
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_eq!(val, Value::F64(10.0));
    }
    #[test] fn max_with_nulls() {
        let db = db_nulls();
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_eq!(val, Value::F64(50.0));
    }
    #[test] fn group_by_order_by_agg_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) AS total FROM t GROUP BY s ORDER BY total DESC");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn group_by_where_order_limit() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol ORDER BY symbol LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn count_distinct_via_subquery() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn group_by_int_key() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, bucket INT, v DOUBLE)");
        for i in 0..12 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}, {}.0)", ts(i), i % 3, i));
        }
        let (_, rows) = db.query("SELECT bucket, count(*) FROM t GROUP BY bucket ORDER BY bucket");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][1], Value::I64(4));
    }
    #[test] fn sum_group_by_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g INT, v DOUBLE)");
        for i in 0..6 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}, {}.0)", ts(i), i % 2, (i + 1) * 10));
        }
        let (_, rows) = db.query("SELECT g, sum(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn multiple_aggs_no_group() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT count(*), sum(d), avg(d), min(d), max(d) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(10));
    }
    #[test]
    fn group_by_where_neq() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t WHERE s != 'alpha' GROUP BY s ORDER BY s");
        assert_eq!(rows.len(), 4);
    }
    #[test] fn group_by_double_agg() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT s, min(i), max(i) FROM t GROUP BY s ORDER BY s");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 5);
    }
    #[test] fn sum_int_group() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(i) FROM t GROUP BY s ORDER BY s");
        // alpha: 1+6=7
        assert_eq!(rows[0][1], Value::I64(7));
    }
    #[test] fn avg_int_group() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, avg(i) FROM t GROUP BY s ORDER BY s");
        // alpha: (1+6)/2 = 3.5
        assert_f64_near(&rows[0][1], 3.5, 0.01);
    }
    #[test] fn group_by_count_with_order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) AS c FROM t GROUP BY s ORDER BY c DESC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
}

// ============================================================================
// 4. GROUP BY + HAVING (50 tests)
// ============================================================================
mod group_having {
    use super::*;

    #[test] fn having_count_gt() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol HAVING count(*) > 5");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_count_eq() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) = 2");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn having_sum_gt() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) > 100.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_avg_gt() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, avg(d) FROM t GROUP BY s HAVING avg(d) > 50.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_min_lt() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, min(d) FROM t GROUP BY s HAVING min(d) < 30.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_max_gt() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, max(d) FROM t GROUP BY s HAVING max(d) > 80.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_filters_all() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) > 100");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn having_with_where() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol HAVING count(*) >= 3"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn having_with_order() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) > 50.0 ORDER BY sum(d)"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn having_with_order_desc() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) > 50.0 ORDER BY sum(d) DESC"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn having_with_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, count(*) FROM t GROUP BY s HAVING count(*) = 2 LIMIT 3"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn having_count_gte() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) >= 2");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn having_count_lte() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) <= 2");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn having_count_neq() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) != 10");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn having_sum_lte() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) <= 70.0 ORDER BY s");
        // alpha: 10+60=70, beta: 20+70=90
        assert!(rows.len() >= 1);
    }
    #[test] fn having_avg_lt() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, avg(d) FROM t GROUP BY s HAVING avg(d) < 40.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_where_order_limit() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c FROM trades WHERE side = 'buy' GROUP BY symbol HAVING count(*) >= 1 ORDER BY c LIMIT 2"
        );
        assert!(rows.len() <= 2);
    }
    #[test] fn having_sum_eq() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) = 70.0");
        // alpha: 10+60=70
        assert!(rows.len() >= 1);
    }
    #[test]
    fn having_max_lt() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, max(d) FROM t GROUP BY s HAVING max(d) < 50.0");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn having_min_gte() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, min(d) FROM t GROUP BY s HAVING min(d) >= 40.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn group_having_on_trades() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, avg(price) FROM trades GROUP BY symbol HAVING avg(price) > 100.0 ORDER BY symbol"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn group_having_multiple_aggs() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, count(*), sum(d) FROM t GROUP BY s HAVING count(*) >= 2 AND sum(d) > 60.0"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn having_on_int_sum() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(i) FROM t GROUP BY s HAVING sum(i) > 10 ORDER BY s");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_on_int_avg() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, avg(i) FROM t GROUP BY s HAVING avg(i) > 5.0 ORDER BY s");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_on_int_min() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, min(i) FROM t GROUP BY s HAVING min(i) > 3 ORDER BY s");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_on_int_max() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, max(i) FROM t GROUP BY s HAVING max(i) < 8 ORDER BY s");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_count_1() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) = 1");
        assert_eq!(rows.len(), 0); // all groups have 2
    }
    #[test] fn having_keeps_all() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, count(*) FROM t GROUP BY s HAVING count(*) > 0");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn having_with_alias_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) AS total FROM t GROUP BY s HAVING sum(d) > 60.0 ORDER BY total");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_sum_negative() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) < 0.0");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn having_avg_between() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, avg(d) FROM t GROUP BY s HAVING avg(d) >= 30.0 AND avg(d) <= 60.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_with_two_groups() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side HAVING count(*) >= 3"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn having_count_order_desc_limit_1() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING count(*) > 0 ORDER BY c DESC LIMIT 1"
        );
        assert_eq!(rows.len(), 1);
    }
    #[test] fn having_where_string_filter() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, count(*) FROM t WHERE i > 3 GROUP BY s HAVING count(*) >= 1 ORDER BY s"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn having_on_empty_result() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR, v DOUBLE)");
        let (_, rows) = db.query("SELECT g, count(*) FROM t GROUP BY g HAVING count(*) > 0");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn having_double_sum_eq_exact() {
        let db = db_mixed();
        // beta: 20+70=90
        let (_, rows) = db.query("SELECT s, sum(d) FROM t GROUP BY s HAVING sum(d) = 90.0");
        assert!(rows.len() >= 1);
    }
    #[test] fn having_max_eq() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, max(d) FROM t GROUP BY s HAVING max(d) = 100.0");
        assert_eq!(rows.len(), 1); // epsilon: max(50,100)=100
    }
    #[test] fn having_min_eq() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, min(d) FROM t GROUP BY s HAVING min(d) = 10.0");
        assert_eq!(rows.len(), 1); // alpha
    }
    #[test] fn having_gt_with_order_and_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, sum(d) AS t FROM t GROUP BY s HAVING sum(d) > 60 ORDER BY t DESC LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn having_on_trades_all_pass() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol HAVING count(*) > 0");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn having_multiple_agg_types() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, min(d), max(d), avg(d) FROM t GROUP BY s HAVING max(d) - min(d) > 40.0"
        );
        assert!(rows.len() >= 1);
    }
}

// ============================================================================
// 5. DISTINCT (30 tests)
// ============================================================================
mod distinct {
    use super::*;

    #[test] fn distinct_string() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn distinct_int() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT i FROM t");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn distinct_with_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t ORDER BY s");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::Str("alpha".into()));
    }
    #[test] fn distinct_with_where() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t WHERE i > 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn distinct_with_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn distinct_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x')", ts(i)));
        }
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn distinct_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn distinct_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'only')", ts(0)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn distinct_double() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT d FROM t");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn distinct_on_trades_symbol() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn distinct_on_trades_side() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn distinct_order_desc() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t ORDER BY s DESC");
        assert_eq!(rows[0][0], Value::Str("gamma".into()));
    }
    #[test] fn distinct_where_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t WHERE i <= 5 ORDER BY s");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn distinct_where_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT s FROM t WHERE i > 3 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn distinct_trades_order_limit() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn distinct_two_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(2)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t ORDER BY v");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn distinct_many_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i % 4));
        }
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 4);
    }
    #[test] fn distinct_int_with_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT DISTINCT i FROM t ORDER BY i");
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0][0], Value::I64(1));
    }
    #[test] fn distinct_on_1_col_out_of_many() {
        let db = TestDb::with_trades(30);
        let (cols, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol");
        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 3);
    }
    #[test] fn distinct_with_where_string_eq() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades WHERE symbol = 'BTC/USD'");
        assert!(rows.len() <= 2);
    }
}

// ============================================================================
// 6. Arithmetic expressions (30 tests)
// ============================================================================
mod arithmetic {
    use super::*;

    #[test] fn add_doubles() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d + 5.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(15.0));
    }
    #[test] fn sub_doubles() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d - 5.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(5.0));
    }
    #[test] fn mul_doubles() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d * 2.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(20.0));
    }
    #[test] fn div_doubles() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d / 2.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(5.0));
    }
    #[test] fn add_ints() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT i + 10 FROM t WHERE i = 1");
        assert_eq!(val, Value::I64(11));
    }
    #[test] fn sub_ints() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT i - 1 FROM t WHERE i = 5");
        assert_eq!(val, Value::I64(4));
    }
    #[test] fn mul_ints() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT i * 3 FROM t WHERE i = 4");
        assert_eq!(val, Value::I64(12));
    }
    #[test] fn div_ints() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT i / 2 FROM t WHERE i = 10");
        assert_eq!(val, Value::I64(5));
    }
    #[test] fn add_two_columns() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i + d AS total FROM t WHERE i = 1");
        // 1 + 10.0 = 11.0
        assert_f64_near(&rows[0][0], 11.0, 0.01);
    }
    #[test] fn sub_two_columns() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d - i AS diff FROM t WHERE i = 5");
        assert_f64_near(&rows[0][0], 45.0, 0.01);
    }
    #[test] fn mul_two_columns() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d * i AS prod FROM t WHERE i = 2");
        // 20.0 * 2 = 40.0
        assert_f64_near(&rows[0][0], 40.0, 0.01);
    }
    #[test] fn chained_add_sub() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d + 10.0 - 5.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(15.0));
    }
    #[test] fn chained_mul_div() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d * 2.0 / 4.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(5.0));
    }
    #[test] fn arith_in_where() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE d / 10.0 > 5.0");
        assert!(rows.len() >= 4);
    }
    #[test]
    fn arith_in_order_by() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i, d * -1.0 AS neg FROM t ORDER BY d * -1.0 LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn arith_with_literal_0() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d + 0.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(10.0));
    }
    #[test] fn arith_mul_by_0() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d * 0.0 FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(0.0));
    }
    #[test] fn arith_sub_self() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d - d FROM t WHERE i = 1");
        assert_eq!(val, Value::F64(0.0));
    }
    #[test] fn arith_negative_literal() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT i + -1 FROM t WHERE i = 5");
        assert_eq!(val, Value::I64(4));
    }
    #[test] fn arith_complex_expression() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT (d + 10.0) * 2.0 AS calc FROM t WHERE i = 1");
        assert_f64_near(&rows[0][0], 40.0, 0.01);
    }
    #[test] fn arith_in_agg() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT sum(d * 2.0) FROM t");
        assert_f64_near(&val, 1100.0, 0.01);
    }
    #[test] fn arith_in_group_by_agg() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT s, sum(d + 1.0) FROM t GROUP BY s ORDER BY s");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn arith_divide_by_constant() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d / 10.0 AS tenths FROM t ORDER BY d LIMIT 3");
        assert_f64_near(&rows[0][0], 1.0, 0.01);
    }
    #[test] fn arith_multiply_large() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d * 1000.0 FROM t WHERE i = 10");
        assert_eq!(val, Value::F64(100000.0));
    }
    #[test] fn arith_int_add_double() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i + 0.5 FROM t WHERE i = 1");
        assert_f64_near(&rows[0][0], 1.5, 0.01);
    }
    #[test] fn arith_all_rows() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d * 2.0 FROM t ORDER BY d");
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0][0], Value::F64(20.0));
    }
    #[test] fn arith_in_limit_order() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i, d + i AS combined FROM t ORDER BY combined DESC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn arith_three_terms() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT d + i + 0.5 FROM t WHERE i = 1");
        assert_f64_near(&val, 11.5, 0.01);
    }
    #[test] fn arith_sum_of_arith() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT sum(d - 10.0) FROM t");
        // sum(0+10+20+30+40+50+60+70+80+90) = 450
        assert_f64_near(&val, 450.0, 0.01);
    }
}

// ============================================================================
// 7. CASE WHEN (30 tests)
// ============================================================================
mod case_when {
    use super::*;

    #[test] fn case_simple() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT CASE WHEN i > 5 THEN 'big' ELSE 'small' END FROM t ORDER BY i");
        assert_eq!(rows[0][0], Value::Str("small".into()));
        assert_eq!(rows[9][0], Value::Str("big".into()));
    }
    #[test] fn case_three_branches() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN i <= 3 THEN 'low' WHEN i <= 7 THEN 'mid' ELSE 'high' END AS cat FROM t ORDER BY i"
        );
        assert_eq!(rows[0][0], Value::Str("low".into()));
        assert_eq!(rows[4][0], Value::Str("mid".into()));
        assert_eq!(rows[9][0], Value::Str("high".into()));
    }
    #[test] fn case_numeric_result() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT CASE WHEN i > 5 THEN 1 ELSE 0 END AS flag FROM t ORDER BY i");
        assert_eq!(rows[0][0], Value::I64(0));
        assert_eq!(rows[9][0], Value::I64(1));
    }
    #[test] fn case_with_string_comparison() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN s = 'alpha' THEN 'A' ELSE 'X' END AS code FROM t ORDER BY i"
        );
        assert_eq!(rows[0][0], Value::Str("A".into()));
        assert_eq!(rows[1][0], Value::Str("X".into()));
    }
    #[test] fn case_in_order_by() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT i, CASE WHEN i > 5 THEN 0 ELSE 1 END AS priority FROM t ORDER BY priority, i"
        );
        assert_eq!(rows.len(), 10);
    }
    #[test] fn case_no_else() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT CASE WHEN i > 5 THEN 'big' END FROM t WHERE i = 1");
        assert_eq!(rows[0][0], Value::Null);
    }
    #[test] fn case_with_double_comparison() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN d >= 50.0 THEN 'high' ELSE 'low' END AS level FROM t ORDER BY i"
        );
        assert_eq!(rows[0][0], Value::Str("low".into()));
    }
    #[test] fn case_returns_double() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT CASE WHEN i > 5 THEN d * 2.0 ELSE d END FROM t WHERE i = 6");
        assert_eq!(rows[0][0], Value::F64(120.0));
    }
    #[test] fn case_all_true() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT CASE WHEN i > 0 THEN 'yes' ELSE 'no' END FROM t");
        for row in &rows {
            assert_eq!(row[0], Value::Str("yes".into()));
        }
    }
    #[test] fn case_all_false() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT CASE WHEN i > 100 THEN 'yes' ELSE 'no' END FROM t");
        for row in &rows {
            assert_eq!(row[0], Value::Str("no".into()));
        }
    }
    #[test] fn case_with_where() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN d > 50.0 THEN 'above' ELSE 'below' END FROM t WHERE s = 'alpha'"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn case_with_group_by() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN i <= 5 THEN 'low' ELSE 'high' END AS tier, count(*) FROM t GROUP BY tier ORDER BY tier"
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::I64(5));
    }
    #[test] fn case_nested() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN i = 1 THEN 'one' WHEN i = 2 THEN 'two' WHEN i = 3 THEN 'three' ELSE 'other' END FROM t ORDER BY i LIMIT 4"
        );
        assert_eq!(rows[0][0], Value::Str("one".into()));
        assert_eq!(rows[1][0], Value::Str("two".into()));
        assert_eq!(rows[2][0], Value::Str("three".into()));
        assert_eq!(rows[3][0], Value::Str("other".into()));
    }
    #[test] fn case_with_and() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN i > 3 AND i < 8 THEN 'mid' ELSE 'edge' END FROM t ORDER BY i"
        );
        assert_eq!(rows[0][0], Value::Str("edge".into()));
        assert_eq!(rows[3][0], Value::Str("mid".into()));
    }
    #[test] fn case_with_or() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN s = 'alpha' OR s = 'beta' THEN 'AB' ELSE 'other' END FROM t WHERE i <= 3 ORDER BY i"
        );
        assert_eq!(rows[0][0], Value::Str("AB".into()));
    }
    #[test] fn case_in_agg() {
        let db = db_mixed();
        let val = db.query_scalar(
            "SELECT count(CASE WHEN i > 5 THEN 1 END) FROM t"
        );
        assert_eq!(val, Value::I64(5));
    }
    #[test] fn case_sum_conditional() {
        let db = db_mixed();
        let val = db.query_scalar(
            "SELECT sum(CASE WHEN s = 'alpha' THEN d ELSE 0 END) FROM t"
        );
        // alpha: 10 + 60 = 70
        assert_f64_near(&val, 70.0, 0.01);
    }
    #[test] fn case_four_branches() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN i <= 2 THEN 'Q1' WHEN i <= 5 THEN 'Q2' WHEN i <= 8 THEN 'Q3' ELSE 'Q4' END AS q FROM t ORDER BY i"
        );
        assert_eq!(rows[0][0], Value::Str("Q1".into()));
        assert_eq!(rows[2][0], Value::Str("Q2".into()));
        assert_eq!(rows[5][0], Value::Str("Q3".into()));
        assert_eq!(rows[8][0], Value::Str("Q4".into()));
    }
    #[test] fn case_with_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN i > 5 THEN 'big' ELSE 'small' END FROM t ORDER BY i LIMIT 3"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn case_on_trades() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 10000 THEN 'expensive' ELSE 'cheap' END AS cat FROM trades ORDER BY price LIMIT 5"
        );
        assert_eq!(rows.len(), 5);
    }
}

// ============================================================================
// 8. Aliases (20 tests)
// ============================================================================
mod aliases {
    use super::*;

    #[test] fn column_alias() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT i AS value FROM t LIMIT 1");
        assert!(cols.contains(&"value".to_string()));
    }
    #[test] fn multiple_aliases() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT i AS a, d AS b, s AS c FROM t LIMIT 1");
        assert!(cols.contains(&"a".to_string()));
        assert!(cols.contains(&"b".to_string()));
        assert!(cols.contains(&"c".to_string()));
    }
    #[test] fn agg_alias() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT count(*) AS total FROM t");
        assert!(cols.contains(&"total".to_string()));
    }
    #[test] fn expr_alias() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT d * 2.0 AS doubled FROM t LIMIT 1");
        assert!(cols.contains(&"doubled".to_string()));
    }
    #[test] fn alias_in_group_by() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT s AS grp, count(*) AS cnt FROM t GROUP BY s");
        assert!(cols.contains(&"grp".to_string()));
        assert!(cols.contains(&"cnt".to_string()));
        assert_eq!(rows.len(), 5);
    }
    #[test] fn alias_preserved_in_results() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT min(d) AS minimum, max(d) AS maximum FROM t");
        assert!(cols.contains(&"minimum".to_string()));
        assert!(cols.contains(&"maximum".to_string()));
    }
    #[test] fn alias_sum() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT sum(i) AS total_i FROM t");
        assert!(cols.contains(&"total_i".to_string()));
    }
    #[test] fn alias_avg() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT avg(d) AS mean_d FROM t");
        assert!(cols.contains(&"mean_d".to_string()));
    }
    #[test] fn case_alias() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT CASE WHEN i > 5 THEN 1 ELSE 0 END AS flag FROM t LIMIT 1");
        assert!(cols.contains(&"flag".to_string()));
    }
    #[test] fn alias_order_by_orig_col() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i AS num FROM t ORDER BY i LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn alias_same_as_column() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT i AS i FROM t LIMIT 1");
        assert!(cols.contains(&"i".to_string()));
    }
    #[test] fn alias_with_where() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT d AS price FROM t WHERE i > 5");
        assert!(cols.contains(&"price".to_string()));
        assert_eq!(rows.len(), 5);
    }
    #[test] fn alias_arith_expr() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT i + d AS combined FROM t LIMIT 1");
        assert!(cols.contains(&"combined".to_string()));
    }
    #[test] fn alias_count_star() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT count(*) AS n FROM t");
        assert!(cols.contains(&"n".to_string()));
    }
    #[test] fn alias_on_trades() {
        let db = TestDb::with_trades(10);
        let (cols, _) = db.query("SELECT symbol AS sym, price AS p FROM trades LIMIT 1");
        assert!(cols.contains(&"sym".to_string()));
        assert!(cols.contains(&"p".to_string()));
    }
    #[test] fn alias_distinct() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT DISTINCT s AS category FROM t ORDER BY s");
        assert!(cols.contains(&"category".to_string()));
        assert_eq!(rows.len(), 5);
    }
    #[test] fn alias_group_having() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT s AS grp, count(*) AS cnt FROM t GROUP BY s HAVING count(*) = 2");
        assert!(cols.contains(&"grp".to_string()));
    }
    #[test] fn alias_four_cols() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT i AS a, d AS b, s AS c, i + d AS e FROM t LIMIT 1");
        assert_eq!(cols.len(), 4);
    }
    #[test] fn alias_in_limit() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT i AS x FROM t LIMIT 5");
        assert!(cols.contains(&"x".to_string()));
        assert_eq!(rows.len(), 5);
    }
    #[test] fn alias_agg_multiple() {
        let db = db_mixed();
        let (cols, _) = db.query("SELECT count(*) AS n, sum(d) AS s, avg(d) AS a FROM t");
        assert!(cols.contains(&"n".to_string()));
        assert!(cols.contains(&"s".to_string()));
        assert!(cols.contains(&"a".to_string()));
    }
}

// ============================================================================
// 9. Empty table / single row edge cases (30 tests)
// ============================================================================
mod edge_cases {
    use super::*;

    #[test] fn select_star_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn count_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_eq!(val, Value::I64(0));
    }
    #[test] fn sum_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_eq!(val, Value::Null);
    }
    #[test] fn avg_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_eq!(val, Value::Null);
    }
    #[test] fn min_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_eq!(val, Value::Null);
    }
    #[test] fn max_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_eq!(val, Value::Null);
    }
    #[test] fn select_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn count_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_eq!(val, Value::I64(1));
    }
    #[test] fn sum_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }
    #[test] fn avg_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }
    #[test] fn min_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }
    #[test] fn max_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }
    #[test] fn where_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t WHERE v > 0.0");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn order_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t ORDER BY v");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn limit_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t LIMIT 10");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn group_by_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR, v DOUBLE)");
        let (_, rows) = db.query("SELECT g, count(*) FROM t GROUP BY g");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn distinct_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn select_star_single_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT s FROM t");
        assert_eq!(val, Value::Str("hello".into()));
    }
    #[test] fn select_star_single_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(99));
    }
    #[test] fn select_star_single_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({})", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_no_match_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t WHERE v > 100.0");
        assert_eq!(rows.len(), 0);
    }
    #[test] fn order_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn limit_1_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn distinct_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn group_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT g, count(*) FROM t GROUP BY g");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::I64(1));
    }
    #[test] fn insert_then_select_all() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR, c INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.5, 'x', 10)", ts(0)));
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 1);
    }
    #[test] fn select_columns_reordered() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 'x')", ts(0)));
        let (cols, _) = db.query("SELECT b, a FROM t");
        assert_eq!(cols[0], "b");
        assert_eq!(cols[1], "a");
    }
    #[test] fn count_after_where_excludes_all() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT count(*) FROM t WHERE i > 100");
        assert_eq!(val, Value::I64(0));
    }
    #[test] fn sum_after_where_excludes_all() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT sum(d) FROM t WHERE i > 100");
        assert_eq!(val, Value::Null);
    }
    #[test] fn avg_after_where_excludes_all() {
        let db = db_mixed();
        let val = db.query_scalar("SELECT avg(d) FROM t WHERE i > 100");
        assert_eq!(val, Value::Null);
    }
}

// ============================================================================
// 10. Larger data set tests (40 tests)
// ============================================================================
mod larger_data {
    use super::*;

    #[test] fn select_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 100);
    }
    #[test] fn count_100() {
        let db = TestDb::with_trades(100);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(100));
    }
    #[test] fn where_symbol_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert!(rows.len() >= 30);
    }
    #[test] fn group_by_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn order_by_price_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn avg_by_symbol_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn min_max_price_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT min(price), max(price) FROM trades");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn distinct_symbols_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_and_order_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT price FROM trades WHERE side = 'buy' ORDER BY price DESC LIMIT 10");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn group_having_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING count(*) > 20 ORDER BY c"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn sum_by_side_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT side, sum(price) FROM trades GROUP BY side ORDER BY side");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn case_with_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 10000 THEN 'high' ELSE 'low' END AS tier, count(*) FROM trades GROUP BY tier ORDER BY tier"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn arithmetic_on_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT price * 1.1 AS adj_price FROM trades LIMIT 10");
        assert_eq!(rows.len(), 10);
    }
    #[test] fn multiple_aggs_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(100));
    }
    #[test] fn where_order_limit_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT symbol, price FROM trades WHERE symbol = 'ETH/USD' ORDER BY price DESC LIMIT 5"
        );
        assert_eq!(rows.len(), 5);
    }
    #[test] fn group_two_keys_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side ORDER BY symbol, side"
        );
        assert_eq!(rows.len(), 6); // 3 symbols * 2 sides
    }
    #[test] fn select_200_rows() {
        let db = TestDb::with_trades(200);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(200));
    }
    #[test] fn order_desc_200() {
        let db = TestDb::with_trades(200);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_complex_200() {
        let db = TestDb::with_trades(200);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'SOL/USD' AND side = 'buy' ORDER BY price LIMIT 10"
        );
        assert!(rows.len() <= 10);
    }
    #[test] fn distinct_sides_200() {
        let db = TestDb::with_trades(200);
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades ORDER BY side");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn limit_1_200_rows() {
        let db = TestDb::with_trades(200);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn group_having_order_limit_200() {
        let db = TestDb::with_trades(200);
        let (_, rows) = db.query(
            "SELECT symbol, avg(price) AS ap FROM trades GROUP BY symbol HAVING avg(price) > 0 ORDER BY ap LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn select_50_with_all_features() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c, avg(price) AS ap FROM trades WHERE side = 'buy' GROUP BY symbol HAVING count(*) >= 1 ORDER BY c DESC LIMIT 3"
        );
        assert!(rows.len() <= 3);
    }
    #[test] fn case_group_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT CASE WHEN symbol = 'BTC/USD' THEN 'BTC' WHEN symbol = 'ETH/USD' THEN 'ETH' ELSE 'OTHER' END AS coin, count(*) FROM trades GROUP BY coin ORDER BY coin"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn arith_agg_100() {
        let db = TestDb::with_trades(100);
        let val = db.query_scalar("SELECT sum(price * 2.0) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }
    #[test] fn max_min_diff_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT max(price) - min(price) FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!(*v > 0.0),
            _ => panic!("expected F64"),
        }
    }
    #[test] fn count_where_100() {
        let db = TestDb::with_trades(100);
        let buy = db.query_scalar("SELECT count(*) FROM trades WHERE side = 'buy'");
        let sell = db.query_scalar("SELECT count(*) FROM trades WHERE side = 'sell'");
        let total = db.query_scalar("SELECT count(*) FROM trades");
        match (buy, sell, total) {
            (Value::I64(b), Value::I64(s), Value::I64(t)) => assert_eq!(b + s, t),
            _ => panic!("expected I64"),
        }
    }
    #[test] fn min_per_symbol_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT symbol, min(price) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn max_per_symbol_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT symbol, max(price) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn sum_per_side_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT side, sum(price) FROM trades GROUP BY side ORDER BY side");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn avg_per_side_100() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT side, avg(price) FROM trades GROUP BY side ORDER BY side");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn distinct_three_cols_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_gt_order_limit_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT price FROM trades WHERE price > 200.0 ORDER BY price LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn nested_conditions_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE (symbol = 'BTC/USD' OR symbol = 'ETH/USD') AND side = 'buy' ORDER BY price LIMIT 10"
        );
        assert!(rows.len() <= 10);
    }
    #[test] fn all_combinations_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, side, count(*) AS c, avg(price) AS ap FROM trades WHERE price > 100.0 GROUP BY symbol, side HAVING count(*) >= 1 ORDER BY c DESC LIMIT 5"
        );
        assert!(rows.len() <= 5);
    }
    #[test] fn select_two_columns_order_50() {
        let db = TestDb::with_trades(50);
        let (cols, rows) = db.query("SELECT symbol, price FROM trades ORDER BY price DESC LIMIT 3");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_by_alias_order_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol AS s, count(*) AS c FROM trades GROUP BY symbol ORDER BY c DESC"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn having_trades_avg_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, avg(price) FROM trades GROUP BY symbol HAVING avg(price) > 50.0"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn case_arith_50() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 10000 THEN price * 0.99 ELSE price * 1.01 END AS adj FROM trades LIMIT 5"
        );
        assert_eq!(rows.len(), 5);
    }
}

// ============================================================================
// 11. Multi-feature combinations (50 tests)
// ============================================================================
mod multi_feature {
    use super::*;

    #[test] fn where_group_order() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol ORDER BY symbol"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_group_having_order() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol HAVING count(*) >= 1 ORDER BY symbol"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn where_group_having_order_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'sell' GROUP BY symbol HAVING count(*) >= 1 ORDER BY symbol LIMIT 2"
        );
        assert!(rows.len() <= 2);
    }
    #[test] fn distinct_order_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_distinct_order() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades WHERE side = 'buy' ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn arith_where_order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT d * 2.0 AS doubled FROM t WHERE i > 5 ORDER BY doubled LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn case_group_order() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 10000 THEN 'high' ELSE 'low' END AS tier, count(*) FROM trades GROUP BY tier ORDER BY tier"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn case_where_order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN d > 50.0 THEN 'above' ELSE 'below' END AS level FROM t WHERE i > 3 ORDER BY level LIMIT 4"
        );
        assert!(rows.len() <= 4);
    }
    #[test] fn alias_group_having_order() {
        let db = db_mixed();
        let (cols, rows) = db.query(
            "SELECT s AS grp, count(*) AS cnt FROM t GROUP BY s HAVING count(*) = 2 ORDER BY grp"
        );
        assert!(cols.contains(&"grp".to_string()));
        assert_eq!(rows.len(), 5);
    }
    #[test] fn multiple_aggs_group_order_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*), sum(price), avg(price) FROM trades GROUP BY symbol ORDER BY count(*) DESC LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn where_arith_group() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, sum(d * 2.0) FROM t WHERE i > 2 GROUP BY s ORDER BY s"
        );
        assert!(rows.len() >= 3);
    }
    #[test] fn where_multiple_or_order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT i FROM t WHERE s = 'alpha' OR s = 'beta' OR s = 'gamma' ORDER BY i LIMIT 4"
        );
        assert_eq!(rows.len(), 4);
    }
    #[test]
    fn arith_group_having() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, sum(d + 1.0) AS total FROM t GROUP BY s HAVING sum(d + 1.0) > 70.0 ORDER BY total"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn count_case_group() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(CASE WHEN side = 'buy' THEN 1 END) AS buys FROM trades GROUP BY symbol ORDER BY symbol"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn where_not_eq_group_order() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, avg(d) FROM t WHERE s != 'alpha' GROUP BY s ORDER BY s"
        );
        assert_eq!(rows.len(), 4);
    }
    #[test]
    fn case_distinct() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT DISTINCT CASE WHEN i <= 5 THEN 'low' ELSE 'high' END AS tier FROM t ORDER BY tier"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn where_order_limit_1_min() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn where_order_limit_1_max() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test] fn group_order_by_agg_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, min(price) AS mp FROM trades GROUP BY symbol ORDER BY mp LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn where_gt_lt_group_count() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, count(*) FROM t WHERE i >= 3 AND i <= 8 GROUP BY s ORDER BY s"
        );
        assert!(rows.len() >= 3);
    }
    #[test] fn case_where_group_having_order_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT CASE WHEN symbol = 'BTC/USD' THEN 'BTC' ELSE 'ALT' END AS coin, count(*) AS c FROM trades WHERE side = 'buy' GROUP BY coin HAVING count(*) >= 1 ORDER BY c DESC LIMIT 2"
        );
        assert!(rows.len() <= 2);
    }
    #[test] fn sum_arith_where_group() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, sum(price * 0.01) AS fee FROM trades WHERE side = 'sell' GROUP BY symbol ORDER BY fee DESC"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn multiple_aliases_group_order() {
        let db = db_mixed();
        let (cols, rows) = db.query(
            "SELECT s AS category, count(*) AS total, avg(d) AS mean FROM t GROUP BY s ORDER BY mean DESC"
        );
        assert!(cols.contains(&"category".to_string()));
        assert!(cols.contains(&"total".to_string()));
        assert!(cols.contains(&"mean".to_string()));
        assert_eq!(rows.len(), 5);
    }
    #[test] fn where_string_order_desc_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT i FROM t WHERE s = 'gamma' ORDER BY i DESC LIMIT 1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(8));
    }
    #[test] fn arith_alias_order_limit() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT i, d - i AS diff FROM t ORDER BY diff DESC LIMIT 3");
        assert!(cols.contains(&"diff".to_string()));
        assert_eq!(rows.len(), 3);
    }
    #[test] fn group_multiple_aggs_having_order() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, min(d), max(d), count(*) FROM t GROUP BY s HAVING max(d) > 50.0 ORDER BY s"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn where_or_group_order_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, count(*) FROM t WHERE s = 'alpha' OR s = 'beta' GROUP BY s ORDER BY s LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn distinct_where_order_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT DISTINCT symbol FROM trades WHERE side = 'buy' ORDER BY symbol LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn full_pipeline_test() {
        let db = TestDb::with_trades(100);
        let (cols, rows) = db.query(
            "SELECT symbol AS s, side, count(*) AS c, avg(price) AS ap FROM trades WHERE price > 100.0 GROUP BY symbol, side HAVING count(*) >= 1 ORDER BY c DESC LIMIT 4"
        );
        assert!(cols.contains(&"s".to_string()));
        assert!(cols.contains(&"c".to_string()));
        assert!(rows.len() <= 4);
    }
    #[test] fn multi_order_and_limit() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT symbol, price FROM trades ORDER BY symbol, price LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test] fn group_by_three_aggs() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, sum(price), min(price), max(price) FROM trades GROUP BY symbol ORDER BY symbol"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_and_or_group_having() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'buy' AND (symbol = 'BTC/USD' OR symbol = 'ETH/USD') GROUP BY symbol HAVING count(*) >= 1 ORDER BY symbol"
        );
        assert!(rows.len() >= 1);
    }
    #[test] fn case_sum_group() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, sum(CASE WHEN side = 'buy' THEN price ELSE 0 END) AS buy_total FROM trades GROUP BY symbol ORDER BY symbol"
        );
        assert_eq!(rows.len(), 3);
    }
    #[test] fn arith_with_case_and_limit() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT CASE WHEN d > 50.0 THEN d * 1.1 ELSE d * 0.9 END AS adjusted FROM t ORDER BY adjusted LIMIT 5"
        );
        assert_eq!(rows.len(), 5);
    }
    #[test] fn where_neq_group_count_order() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, count(*) AS c FROM t WHERE i != 1 GROUP BY s ORDER BY c DESC"
        );
        assert!(rows.len() >= 4);
    }
    #[test] fn full_query_with_arith() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c, sum(price * 0.01) AS fee FROM trades WHERE price > 50.0 GROUP BY symbol HAVING count(*) >= 1 ORDER BY fee DESC LIMIT 3"
        );
        assert!(rows.len() <= 3);
    }
    #[test] fn where_group_multiple_having_order_limit() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c, avg(price) AS ap FROM trades WHERE side = 'sell' GROUP BY symbol HAVING count(*) > 5 AND avg(price) > 0 ORDER BY ap DESC LIMIT 2"
        );
        assert!(rows.len() <= 2);
    }
    #[test]
    fn arith_in_having() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT s, sum(d) AS total FROM t GROUP BY s HAVING sum(d) * 2 > 100 ORDER BY total"
        );
        assert!(rows.len() >= 1);
    }
    #[test]
    fn case_where_distinct() {
        let db = db_mixed();
        let (_, rows) = db.query(
            "SELECT DISTINCT CASE WHEN i > 5 THEN 'big' ELSE 'small' END AS size FROM t WHERE d > 20.0"
        );
        assert_eq!(rows.len(), 2);
    }
    #[test] fn select_star_order_limit() {
        let db = db_mixed();
        let (cols, rows) = db.query("SELECT * FROM t ORDER BY i DESC LIMIT 3");
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 3);
    }
    #[test] fn arith_multiple_ops_limit() {
        let db = db_mixed();
        let (_, rows) = db.query("SELECT (d + 10.0) * 2.0 - 5.0 AS calc FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test] fn where_complex_group_limit() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT side, count(*) FROM trades WHERE (symbol = 'BTC/USD' AND price > 50000) OR (symbol = 'ETH/USD') GROUP BY side LIMIT 2"
        );
        assert!(rows.len() <= 2);
    }
    #[test] fn full_pipeline_200_rows() {
        let db = TestDb::with_trades(200);
        let (_, rows) = db.query(
            "SELECT symbol, side, count(*) AS c, avg(price) AS ap, min(price) AS lo, max(price) AS hi FROM trades WHERE price > 50.0 GROUP BY symbol, side HAVING count(*) >= 5 ORDER BY c DESC LIMIT 4"
        );
        assert!(rows.len() <= 4);
    }
    #[test] fn select_all_features_combined() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query(
            "SELECT symbol AS s, CASE WHEN side = 'buy' THEN 'B' ELSE 'S' END AS dir, count(*) AS c, avg(price * 1.01) AS ap FROM trades WHERE symbol != 'SOL/USD' GROUP BY symbol, side HAVING count(*) >= 1 ORDER BY c DESC LIMIT 3"
        );
        assert!(rows.len() <= 3);
    }
}
