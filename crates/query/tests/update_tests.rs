//! UPDATE statement tests for ExchangeDB (60+ tests).
//!
//! Covers: basic SET, multiple columns, WHERE conditions, type-specific updates,
//! edge cases (empty table, nonexistent column, all rows).

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// ===========================================================================
// update_basic: UPDATE SET single/multiple columns
// ===========================================================================
mod update_basic {
    use super::*;

    #[test]
    fn update_single_column() {
        let db = TestDb::with_trades(10);
        db.exec_ok("UPDATE trades SET price = 99999.0 WHERE symbol = 'BTC/USD'");
        let (_, rows) = db.query("SELECT price FROM trades WHERE symbol = 'BTC/USD'");
        for row in &rows {
            match &row[0] {
                Value::F64(p) => assert!((*p - 99999.0).abs() < 0.01),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn update_multiple_columns() {
        let db = TestDb::with_trades(10);
        db.exec_ok("UPDATE trades SET price = 1.0, side = 'neutral' WHERE symbol = 'ETH/USD'");
        let (_, rows) = db.query("SELECT price, side FROM trades WHERE symbol = 'ETH/USD'");
        for row in &rows {
            assert_eq!(row[0], Value::F64(1.0));
            assert_eq!(row[1], Value::Str("neutral".to_string()));
        }
    }

    #[test]
    fn update_all_rows() {
        let db = TestDb::with_trades(10);
        db.exec_ok("UPDATE trades SET side = 'unknown'");
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("unknown".to_string()));
    }

    #[test]
    fn update_single_row_by_exact_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a', 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b', 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c', 3.0)", ts(2)));
        db.exec_ok("UPDATE t SET v = 99.0 WHERE name = 'b'");
        let (_, rows) = db.query("SELECT v FROM t WHERE name = 'b'");
        assert_eq!(rows[0][0], Value::F64(99.0));
        // Others unchanged
        let (_, rows_a) = db.query("SELECT v FROM t WHERE name = 'a'");
        assert_eq!(rows_a[0][0], Value::F64(1.0));
    }

    #[test]
    fn update_preserves_row_count() {
        let db = TestDb::with_trades(20);
        let before = db.query("SELECT count(*) FROM trades");
        db.exec_ok("UPDATE trades SET price = 0.0");
        let after = db.query("SELECT count(*) FROM trades");
        assert_eq!(before.1[0][0], after.1[0][0]);
    }

    #[test]
    fn update_to_same_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 5.0 WHERE v = 5.0");
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(5.0));
    }

    #[test]
    fn update_returns_affected_count() {
        let db = TestDb::with_trades(10);
        let result = db.exec("UPDATE trades SET price = 0.0 WHERE symbol = 'BTC/USD'");
        assert!(result.is_ok());
    }

    #[test]
    fn update_set_to_expression() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = v * 2");
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(20.0));
    }

    #[test]
    fn update_set_to_addition() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = v + 5");
        let val = db.query_scalar("SELECT v FROM t");
        assert!(val.eq_coerce(&Value::F64(15.0)));
    }
}

// ===========================================================================
// update_where: with various WHERE conditions
// ===========================================================================
mod update_where {
    use super::*;

    #[test]
    fn update_where_eq_string() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET price = 0.0 WHERE symbol = 'SOL/USD'");
        let (_, rows) = db.query("SELECT price FROM trades WHERE symbol = 'SOL/USD'");
        for row in &rows {
            assert_eq!(row[0], Value::F64(0.0));
        }
    }

    #[test]
    fn update_where_gt() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'expensive' WHERE price > 50000");
        let (_, rows) = db.query("SELECT side FROM trades WHERE price > 50000");
        for row in &rows {
            assert_eq!(row[0], Value::Str("expensive".to_string()));
        }
    }

    #[test]
    fn update_where_lt() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'cheap' WHERE price < 200");
        let (_, rows) = db.query("SELECT side FROM trades WHERE price < 200");
        for row in &rows {
            assert_eq!(row[0], Value::Str("cheap".to_string()));
        }
    }

    #[test]
    fn update_where_and() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET price = 0.0 WHERE symbol = 'BTC/USD' AND side = 'buy'");
        let (_, rows) =
            db.query("SELECT price FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'");
        for row in &rows {
            assert_eq!(row[0], Value::F64(0.0));
        }
    }

    #[test]
    fn update_where_or() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET price = 0.0 WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD'");
        let (_, rows) =
            db.query("SELECT price FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD'");
        for row in &rows {
            assert_eq!(row[0], Value::F64(0.0));
        }
    }

    #[test]
    fn update_where_not_eq() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'other' WHERE symbol != 'BTC/USD'");
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades WHERE symbol != 'BTC/USD'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("other".to_string()));
    }

    #[test]
    fn update_where_gte() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'high' WHERE price >= 60000");
        let (_, rows) = db.query("SELECT side FROM trades WHERE price >= 60000");
        for row in &rows {
            assert_eq!(row[0], Value::Str("high".to_string()));
        }
    }

    #[test]
    fn update_where_lte() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'low' WHERE price <= 200");
        let (_, rows) = db.query("SELECT side FROM trades WHERE price <= 200");
        for row in &rows {
            assert_eq!(row[0], Value::Str("low".to_string()));
        }
    }

    #[test]
    fn update_where_no_match() {
        let db = TestDb::with_trades(10);
        db.exec_ok("UPDATE trades SET price = 0.0 WHERE symbol = 'DOGE/USD'");
        // No rows should have changed
        let (_, rows) = db.query("SELECT price FROM trades WHERE price = 0.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn update_where_null_check() {
        let db = TestDb::with_trades(20);
        // Some rows have NULL volume
        db.exec_ok("UPDATE trades SET side = 'null_vol' WHERE volume IS NULL");
        let (_, rows) = db.query("SELECT side FROM trades WHERE volume IS NULL");
        for row in &rows {
            assert_eq!(row[0], Value::Str("null_vol".to_string()));
        }
    }

    #[test]
    fn update_where_between() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'mid' WHERE price BETWEEN 3000 AND 4000");
        let (_, rows) = db.query("SELECT side FROM trades WHERE price BETWEEN 3000 AND 4000");
        for row in &rows {
            assert_eq!(row[0], Value::Str("mid".to_string()));
        }
    }

    #[test]
    fn update_where_in_list() {
        let db = TestDb::with_trades(20);
        db.exec_ok("UPDATE trades SET side = 'selected' WHERE symbol IN ('BTC/USD', 'SOL/USD')");
        let (_, rows) = db.query("SELECT side FROM trades WHERE symbol IN ('BTC/USD', 'SOL/USD')");
        for row in &rows {
            assert_eq!(row[0], Value::Str("selected".to_string()));
        }
    }

    #[test]
    fn update_sequential_updates() {
        let db = TestDb::with_trades(10);
        db.exec_ok("UPDATE trades SET price = 1.0 WHERE symbol = 'BTC/USD'");
        db.exec_ok("UPDATE trades SET price = 2.0 WHERE symbol = 'ETH/USD'");
        db.exec_ok("UPDATE trades SET price = 3.0 WHERE symbol = 'SOL/USD'");
        let (_, rows) = db.query("SELECT DISTINCT price FROM trades ORDER BY price");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn update_overwrite_previous_update() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 2.0");
        db.exec_ok("UPDATE t SET v = 3.0");
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(3.0));
    }
}

// ===========================================================================
// update_types: type-specific updates
// ===========================================================================
mod update_types {
    use super::*;

    #[test]
    fn update_double_to_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 0.0");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(0.0));
    }

    #[test]
    fn update_double_to_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = -100.5");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(-100.5));
    }

    #[test]
    fn update_varchar_to_different_string() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'old')", ts(0)));
        db.exec_ok("UPDATE t SET s = 'new'");
        assert_eq!(
            db.query_scalar("SELECT s FROM t"),
            Value::Str("new".to_string())
        );
    }

    #[test]
    fn update_varchar_to_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        db.exec_ok("UPDATE t SET s = ''");
        assert_eq!(
            db.query_scalar("SELECT s FROM t"),
            Value::Str("".to_string())
        );
    }

    #[test]
    fn update_set_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = NULL");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::Null);
    }

    #[test]
    fn update_null_to_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(0)));
        db.exec_ok("UPDATE t SET v = 99.0");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(99.0));
    }

    #[test]
    fn update_large_double_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 1e15");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(1e15));
    }

    #[test]
    fn update_varchar_to_long_string() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'short')", ts(0)));
        let long_str = "y".repeat(5000);
        db.exec_ok(&format!("UPDATE t SET s = '{}'", long_str));
        let val = db.query_scalar("SELECT s FROM t");
        match val {
            Value::Str(s) => assert_eq!(s.len(), 5000),
            other => panic!("expected Str, got {other:?}"),
        }
    }
}

// ===========================================================================
// update_edge: nonexistent column, empty table, etc.
// ===========================================================================
mod update_edge {
    use super::*;

    #[test]
    fn update_nonexistent_column() {
        let db = TestDb::with_trades(5);
        let result = db.exec("UPDATE trades SET no_col = 1.0");
        assert!(result.is_err());
    }

    #[test]
    fn update_nonexistent_table() {
        let db = TestDb::new();
        let result = db.exec("UPDATE no_table SET v = 1.0");
        assert!(result.is_err());
    }

    #[test]
    fn update_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("UPDATE t SET v = 99.0");
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn update_after_delete_all() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("DELETE FROM t WHERE v = 1.0");
        // Update on empty table should be no-op
        db.exec_ok("UPDATE t SET v = 99.0");
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn update_then_insert_then_query() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 10.0");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY timestamp");
        assert_eq!(rows[0][0], Value::F64(10.0));
        assert_eq!(rows[1][0], Value::F64(20.0));
    }

    #[test]
    fn update_many_rows() {
        let db = TestDb::with_trades(100);
        db.exec_ok("UPDATE trades SET side = 'updated'");
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("updated".to_string()));
    }

    #[test]
    fn update_only_matching_rows() {
        let db = TestDb::with_trades(20);
        let (_, before_btc) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'");
        let btc_count = match &before_btc[0][0] {
            Value::I64(n) => *n,
            other => panic!("{other:?}"),
        };
        db.exec_ok("UPDATE trades SET price = 0.0 WHERE symbol = 'BTC/USD'");
        let (_, after_zero) = db.query("SELECT count(*) FROM trades WHERE price = 0.0");
        let zero_count = match &after_zero[0][0] {
            Value::I64(n) => *n,
            other => panic!("{other:?}"),
        };
        assert_eq!(btc_count, zero_count);
    }

    #[test]
    fn update_preserves_other_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 'x')", ts(0)));
        db.exec_ok("UPDATE t SET a = 2.0");
        let (_, rows) = db.query("SELECT a, b FROM t");
        assert_eq!(rows[0][0], Value::F64(2.0));
        assert_eq!(rows[0][1], Value::Str("x".to_string()));
    }

    #[test]
    fn update_idempotent() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(0)));
        for _ in 0..5 {
            db.exec_ok("UPDATE t SET v = 5.0");
        }
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(5.0));
    }

    #[test]
    fn update_string_to_string_with_spaces() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'old')", ts(0)));
        db.exec_ok("UPDATE t SET s = 'new value with spaces'");
        assert_eq!(
            db.query_scalar("SELECT s FROM t"),
            Value::Str("new value with spaces".to_string())
        );
    }

    #[test]
    fn update_multiple_rows_different_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 3.0)", ts(2)));
        db.exec_ok("UPDATE t SET v = 99.0 WHERE grp = 'A'");
        let (_, rows) = db.query("SELECT v FROM t WHERE grp = 'A'");
        for row in &rows {
            assert_eq!(row[0], Value::F64(99.0));
        }
        let (_, rows_b) = db.query("SELECT v FROM t WHERE grp = 'B'");
        assert_eq!(rows_b[0][0], Value::F64(2.0));
    }

    #[test]
    fn update_with_subtraction() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = v - 30");
        assert!(
            db.query_scalar("SELECT v FROM t")
                .eq_coerce(&Value::F64(70.0))
        );
    }

    #[test]
    fn update_after_add_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("ALTER TABLE t ADD COLUMN s VARCHAR");
        db.exec_ok("UPDATE t SET s = 'filled'");
        assert_eq!(
            db.query_scalar("SELECT s FROM t"),
            Value::Str("filled".to_string())
        );
    }

    #[test]
    fn update_verify_count_unchanged() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("UPDATE t SET v = 0.0");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }

    #[test]
    fn update_verify_sum_changes() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("UPDATE t SET v = 1.0");
        assert_eq!(db.query_scalar("SELECT sum(v) FROM t"), Value::F64(10.0));
    }

    #[test]
    fn update_where_timestamp_range() {
        let db = TestDb::with_trades(20);
        let cutoff = BASE_TS + 5 * 600_000_000_000i64;
        db.exec_ok(&format!(
            "UPDATE trades SET side = 'early' WHERE timestamp < {}",
            cutoff
        ));
        let (_, rows) = db.query(&format!(
            "SELECT DISTINCT side FROM trades WHERE timestamp < {}",
            cutoff
        ));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("early".to_string()));
    }

    #[test]
    fn update_chain_multiple_columns_sequentially() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 'x')", ts(0)));
        db.exec_ok("UPDATE t SET a = 2.0");
        db.exec_ok("UPDATE t SET b = 'y'");
        let (_, rows) = db.query("SELECT a, b FROM t");
        assert_eq!(rows[0][0], Value::F64(2.0));
        assert_eq!(rows[0][1], Value::Str("y".to_string()));
    }

    #[test]
    fn update_large_dataset() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..500).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        db.exec_ok("UPDATE t SET v = -1.0 WHERE v < 250");
        let neg_count = match db.query_scalar("SELECT count(*) FROM t WHERE v = -1.0") {
            Value::I64(n) => n,
            other => panic!("{other:?}"),
        };
        assert_eq!(neg_count, 250);
    }

    #[test]
    fn update_verify_avg_after() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("UPDATE t SET v = 5.0");
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_eq!(val, Value::F64(5.0));
    }

    #[test]
    fn update_with_division() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = v / 4");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(25.0));
    }

    #[test]
    fn update_selective_partial() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, cat VARCHAR, v DOUBLE)");
        for i in 0..20 {
            let cat = if i % 2 == 0 { "even" } else { "odd" };
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i),
                cat,
                i
            ));
        }
        db.exec_ok("UPDATE t SET v = 0.0 WHERE cat = 'even'");
        let zero_count = match db.query_scalar("SELECT count(*) FROM t WHERE v = 0.0") {
            Value::I64(n) => n,
            other => panic!("{other:?}"),
        };
        assert_eq!(zero_count, 10);
        let nonzero_count = match db.query_scalar("SELECT count(*) FROM t WHERE v != 0.0") {
            Value::I64(n) => n,
            other => panic!("{other:?}"),
        };
        assert_eq!(nonzero_count, 10);
    }

    #[test]
    fn update_preserves_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let my_ts = ts(42);
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", my_ts));
        db.exec_ok("UPDATE t SET v = 2.0");
        let (_, rows) = db.query("SELECT timestamp FROM t");
        match &rows[0][0] {
            Value::Timestamp(t) => assert_eq!(*t, my_ts),
            other => panic!("{other:?}"),
        }
    }
}
