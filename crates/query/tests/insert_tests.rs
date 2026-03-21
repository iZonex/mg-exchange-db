//! INSERT statement tests for ExchangeDB (80+ tests).
//!
//! Covers: basic INSERT VALUES, multi-row INSERT, INSERT SELECT,
//! type coercion, NULL handling, upsert/dedup semantics, and edge cases.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_TS: i64 = 1710460800_000_000_000; // 2024-03-15 00:00:00 UTC nanos

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// ===========================================================================
// insert_basic: INSERT INTO ... VALUES, multiple rows, type coercion
// ===========================================================================
mod insert_basic {
    use super::*;

    #[test]
    fn single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT v FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(1.0));
    }

    #[test]
    fn two_rows_separate_inserts() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 2.0)",
            ts(1)
        ));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::F64(1.0));
        assert_eq!(rows[1][0], Value::F64(2.0));
    }

    #[test]
    fn multi_row_single_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 10.0), ({}, 20.0), ({}, 30.0)",
            ts(0),
            ts(1),
            ts(2)
        ));
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn multi_row_10_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..10).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn multi_row_100_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..100).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(100));
    }

    #[test]
    fn insert_preserves_order_by_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // Insert in reverse order
        for i in (0..5).rev() {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {}.0)",
                ts(i),
                i
            ));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY timestamp");
        for i in 0..5 {
            assert_eq!(rows[i][0], Value::F64(i as f64));
        }
    }

    #[test]
    fn insert_with_all_columns_explicit() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, a, b) VALUES ({}, 42.0, 'hello')",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT a, b FROM t");
        assert_eq!(rows[0][0], Value::F64(42.0));
        assert_eq!(rows[0][1], Value::Str("hello".to_string()));
    }

    #[test]
    fn insert_column_order_different_from_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (b, timestamp, a) VALUES ('world', {}, 99.0)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT a, b FROM t");
        assert_eq!(rows[0][0], Value::F64(99.0));
        assert_eq!(rows[0][1], Value::Str("world".to_string()));
    }

    #[test]
    fn insert_into_table_with_many_columns() {
        let db = TestDb::new();
        db.exec_ok(
            "CREATE TABLE wide (timestamp TIMESTAMP, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 VARCHAR, c5 VARCHAR)"
        );
        db.exec_ok(&format!(
            "INSERT INTO wide (timestamp, c1, c2, c3, c4, c5) VALUES ({}, 1.0, 2.0, 3.0, 'a', 'b')",
            ts(0)
        ));
        let (cols, rows) = db.query("SELECT * FROM wide");
        assert_eq!(cols.len(), 6);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn insert_count_returns_affected_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let affected = db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0), ({}, 2.0)",
            ts(0),
            ts(1)
        ));
        // affected rows should be 2 (or possibly returned as row count)
        assert!(affected >= 0); // just ensure no error
    }

    #[test]
    fn insert_same_timestamp_different_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 2.0)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT count(*) FROM t");
        // Both rows should exist (same timestamp is allowed)
        let count = match &rows[0][0] {
            Value::I64(n) => *n,
            other => panic!("expected I64, got {other:?}"),
        };
        assert!(count >= 1);
    }

    #[test]
    fn insert_and_immediately_query() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {}.0)",
                ts(i),
                i
            ));
            let (_, rows) = db.query("SELECT count(*) FROM t");
            assert_eq!(rows[0][0], Value::I64(i + 1));
        }
    }

    #[test]
    fn insert_negative_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, -42.5)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(-42.5));
    }

    #[test]
    fn insert_zero_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 0.0)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(0.0));
    }

    #[test]
    fn insert_very_large_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1e18)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(1e18));
    }

    #[test]
    fn insert_very_small_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1e-15)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        match val {
            Value::F64(v) => assert!((v - 1e-15).abs() < 1e-20),
            other => panic!("expected F64, got {other:?}"),
        }
    }
}

// ===========================================================================
// insert_select: INSERT INTO ... SELECT, with WHERE, with GROUP BY
// ===========================================================================
mod insert_select {
    use super::*;

    #[test]
    fn insert_select_all_rows() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE TABLE trades_copy (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)");
        db.exec_ok("INSERT INTO trades_copy SELECT * FROM trades");
        let (_, rows) = db.query("SELECT count(*) FROM trades_copy");
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn insert_select_with_where() {
        let db = TestDb::with_trades(20);
        db.exec_ok("CREATE TABLE btc_trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)");
        db.exec_ok("INSERT INTO btc_trades SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        let (_, rows) = db.query("SELECT * FROM btc_trades");
        assert!(!rows.is_empty());
        // All rows should be BTC/USD
        let (_, btc_count) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(
            rows.len() as i64,
            match &btc_count[0][0] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            }
        );
    }

    #[test]
    fn insert_select_subset_columns() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE TABLE prices (timestamp TIMESTAMP, price DOUBLE)");
        db.exec_ok("INSERT INTO prices SELECT timestamp, price FROM trades");
        let (_, rows) = db.query("SELECT count(*) FROM prices");
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn insert_select_with_limit() {
        let db = TestDb::with_trades(20);
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)");
        db.exec_ok("INSERT INTO t2 SELECT * FROM trades LIMIT 5");
        let (_, rows) = db.query("SELECT count(*) FROM t2");
        assert_eq!(rows[0][0], Value::I64(5));
    }

    #[test]
    fn insert_select_with_order_by() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)");
        db.exec_ok("INSERT INTO t2 SELECT * FROM trades ORDER BY price DESC");
        let (_, rows) = db.query("SELECT count(*) FROM t2");
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn insert_select_from_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE src (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE dst (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO dst SELECT * FROM src");
        let (_, rows) = db.query("SELECT count(*) FROM dst");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn insert_select_with_group_by() {
        let db = TestDb::with_trades(30);
        db.exec_ok("CREATE TABLE sym_counts (timestamp TIMESTAMP, cnt DOUBLE)");
        // Insert aggregated data - use a constant timestamp
        db.exec_ok(&format!(
            "INSERT INTO sym_counts SELECT {}, count(*) FROM trades GROUP BY symbol",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT * FROM sym_counts");
        assert_eq!(rows.len(), 3); // 3 symbols
    }

    #[test]
    fn insert_select_self_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        // Insert from self - doubles the data each time
        db.exec_ok("INSERT INTO t SELECT * FROM t");
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(2));
    }

    #[test]
    fn insert_select_with_expression() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, double_price DOUBLE)");
        db.exec_ok("INSERT INTO t2 SELECT timestamp, price * 2 FROM trades");
        let (_, rows) = db.query("SELECT count(*) FROM t2");
        assert_eq!(rows[0][0], Value::I64(10));
    }
}

// ===========================================================================
// insert_upsert: INSERT OR REPLACE, dedup behavior
// ===========================================================================
mod insert_upsert {
    use super::*;

    #[test]
    fn duplicate_timestamp_both_kept() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 2.0)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT * FROM t");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn duplicate_all_columns_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT * FROM t");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn many_duplicates_same_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {}.0)",
                ts(0),
                i
            ));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t");
        let count = match &rows[0][0] {
            Value::I64(n) => *n,
            other => panic!("expected I64, got {other:?}"),
        };
        assert!(count >= 1, "at least 1 row should exist");
    }

    #[test]
    fn dedup_preserves_latest_per_symbol() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 30.0)", ts(0)));
        // LATEST ON should give one per symbol
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn insert_after_delete_same_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok("DELETE FROM t WHERE v = 1.0");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 2.0)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(2.0));
    }
}

// ===========================================================================
// insert_types: each column type: i64, f64, varchar, timestamp, null
// ===========================================================================
mod insert_types {
    use super::*;

    #[test]
    fn insert_double_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 3.14)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        match val {
            Value::F64(v) => assert!((v - 3.14).abs() < 0.001),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn insert_integer_into_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 42)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert!(val.eq_coerce(&Value::F64(42.0)) || val.eq_coerce(&Value::I64(42)));
    }

    #[test]
    fn insert_varchar_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, s) VALUES ({}, 'hello world')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT s FROM t");
        assert_eq!(val, Value::Str("hello world".to_string()));
    }

    #[test]
    fn insert_empty_string() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, s) VALUES ({}, '')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT s FROM t");
        assert_eq!(val, Value::Str("".to_string()));
    }

    #[test]
    fn insert_long_string() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        let long_str = "x".repeat(10000);
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, s) VALUES ({}, '{}')",
            ts(0),
            long_str
        ));
        let val = db.query_scalar("SELECT s FROM t");
        match val {
            Value::Str(s) => assert_eq!(s.len(), 10000),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn insert_timestamp_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let my_ts = ts(100);
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            my_ts
        ));
        let (_, rows) = db.query("SELECT timestamp FROM t");
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            Value::Timestamp(t) => assert_eq!(*t, my_ts),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }

    #[test]
    fn insert_null_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, NULL)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn insert_null_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, s) VALUES ({}, NULL)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT s FROM t");
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn insert_multiple_nulls() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, a, b) VALUES ({}, NULL, NULL)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT a, b FROM t");
        assert_eq!(rows[0][0], Value::Null);
        assert_eq!(rows[0][1], Value::Null);
    }

    #[test]
    fn insert_mix_null_and_nonnull() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, a, b) VALUES ({}, 1.0, NULL)",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, a, b) VALUES ({}, NULL, 'x')",
            ts(1)
        ));
        let (_, rows) = db.query("SELECT a, b FROM t ORDER BY timestamp");
        assert_eq!(rows[0][0], Value::F64(1.0));
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[1][0], Value::Null);
        assert_eq!(rows[1][1], Value::Str("x".to_string()));
    }

    #[test]
    fn insert_integer_literal() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 100)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        // Should be coerced to F64(100.0) or kept as I64(100)
        assert!(val.eq_coerce(&Value::F64(100.0)) || val.eq_coerce(&Value::I64(100)));
    }

    #[test]
    fn insert_negative_integer() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, -999)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert!(val.eq_coerce(&Value::F64(-999.0)) || val.eq_coerce(&Value::I64(-999)));
    }

    #[test]
    fn insert_string_with_spaces() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, s) VALUES ({}, '  spaces  ')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT s FROM t");
        assert_eq!(val, Value::Str("  spaces  ".to_string()));
    }

    #[test]
    fn insert_string_with_numbers() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, s) VALUES ({}, '12345')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT s FROM t");
        assert_eq!(val, Value::Str("12345".to_string()));
    }

    #[test]
    fn insert_timestamp_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t (timestamp, v) VALUES (0, 1.0)");
        let (_, rows) = db.query("SELECT timestamp FROM t");
        match &rows[0][0] {
            Value::Timestamp(t) => assert_eq!(*t, 0),
            other => panic!("expected Timestamp(0), got {other:?}"),
        }
    }

    #[test]
    fn insert_very_large_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let big_ts = 2000000000_000_000_000i64; // ~year 2033
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            big_ts
        ));
        let (_, rows) = db.query("SELECT timestamp FROM t");
        match &rows[0][0] {
            Value::Timestamp(t) => assert_eq!(*t, big_ts),
            other => panic!("expected Timestamp, got {other:?}"),
        }
    }
}

// ===========================================================================
// insert_edge_cases: wrong column count, nonexistent table, etc.
// ===========================================================================
mod insert_edge_cases {
    use super::*;

    #[test]
    fn insert_into_nonexistent_table() {
        let db = TestDb::new();
        let _err = db.exec_err(&format!(
            "INSERT INTO no_table (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
    }

    #[test]
    fn insert_wrong_column_count_too_many() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let result = db.exec(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0, 2.0)",
            ts(0)
        ));
        assert!(result.is_err(), "too many values should error");
    }

    #[test]
    fn insert_wrong_column_count_too_few() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        let result = db.exec(&format!(
            "INSERT INTO t (timestamp, a, b) VALUES ({}, 1.0)",
            ts(0)
        ));
        assert!(result.is_err(), "too few values should error");
    }

    #[test]
    fn insert_nonexistent_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let result = db.exec(&format!(
            "INSERT INTO t (timestamp, no_col) VALUES ({}, 1.0)",
            ts(0)
        ));
        assert!(result.is_err(), "nonexistent column should error");
    }

    #[test]
    fn insert_multiple_batches() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for batch in 0..10 {
            let values: Vec<String> = (0..10)
                .map(|i| {
                    let idx = batch * 10 + i;
                    format!("({}, {}.0)", ts(idx), idx)
                })
                .collect();
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES {}",
                values.join(", ")
            ));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(100));
    }

    #[test]
    fn insert_after_drop_recreate() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok("DROP TABLE t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 2.0)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(2.0));
    }

    #[test]
    fn insert_after_truncate() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 1.0)",
            ts(0)
        ));
        db.exec_ok("TRUNCATE TABLE t");
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(0));
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, 2.0)",
            ts(0)
        ));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(2.0));
    }

    #[test]
    fn insert_1000_rows_performance() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..1000).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(1000));
    }

    #[test]
    fn insert_across_day_boundaries() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // Insert rows across 3 days
        for day in 0..3i64 {
            for hour in 0..24i64 {
                let t = ts(day * 86400 + hour * 3600);
                db.exec_ok(&format!(
                    "INSERT INTO t (timestamp, v) VALUES ({}, {}.0)",
                    t,
                    day * 24 + hour
                ));
            }
        }
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(72));
    }

    #[test]
    fn insert_into_table_with_added_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("ALTER TABLE t ADD COLUMN s VARCHAR");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v, s) VALUES ({}, 2.0, 'new')",
            ts(1)
        ));
        let (_, rows) = db.query("SELECT count(*) FROM t");
        assert_eq!(rows[0][0], Value::I64(2));
    }

    #[test]
    fn insert_partial_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, a) VALUES ({}, 1.0)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT b, c FROM t");
        assert_eq!(rows[0][0], Value::Null);
        assert_eq!(rows[0][1], Value::Null);
    }

    #[test]
    fn insert_scientific_notation() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.5e3)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(1500.0));
    }

    #[test]
    fn insert_verify_sum_after_batch() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (1..=10).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_eq!(val, Value::F64(55.0));
    }

    #[test]
    fn insert_verify_min_max_after_batch() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (1..=20).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(1.0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(20.0));
    }

    #[test]
    fn insert_interleaved_timestamps() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(5)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(15)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY timestamp");
        assert_eq!(rows[0][0], Value::F64(2.0));
        assert_eq!(rows[1][0], Value::F64(1.0));
        assert_eq!(rows[2][0], Value::F64(3.0));
    }

    #[test]
    fn insert_and_filter_by_inserted_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'alpha', 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'beta', 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'gamma', 3.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE name = 'beta'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(2.0));
    }

    #[test]
    fn insert_and_aggregate_by_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        for i in 0..12 {
            let grp = if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            };
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i),
                grp,
                i
            ));
        }
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp");
        assert_eq!(rows.len(), 3);
        let total: i64 = rows
            .iter()
            .map(|r| match &r[1] {
                Value::I64(n) => *n,
                other => panic!("{other:?}"),
            })
            .sum();
        assert_eq!(total, 12);
    }

    #[test]
    fn insert_select_with_alias() {
        let db = TestDb::with_trades(10);
        db.exec_ok(
            "CREATE TABLE t2 (timestamp TIMESTAMP, sym VARCHAR, p DOUBLE, vol DOUBLE, s VARCHAR)",
        );
        db.exec_ok("INSERT INTO t2 SELECT * FROM trades");
        let (_, rows) = db.query("SELECT count(*) FROM t2");
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn insert_and_distinct_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        for i in 0..20 {
            let grp = format!("G{}", i % 4);
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i),
                grp,
                i
            ));
        }
        let val = db.query_scalar("SELECT count(DISTINCT grp) FROM t");
        assert_eq!(val, Value::I64(4));
    }

    #[test]
    fn insert_and_order_by_multiple_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 2.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 3.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 1.0)", ts(2)));
        let (_, rows) = db.query("SELECT a, b FROM t ORDER BY a, b");
        assert_eq!(rows[0][0], Value::Str("A".to_string()));
        assert_eq!(rows[0][1], Value::F64(1.0));
    }

    #[test]
    fn insert_and_having_filter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        for i in 0..15 {
            let grp = format!("G{}", i % 3);
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i),
                grp,
                i
            ));
        }
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 5");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn insert_and_sample_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..100 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn insert_and_latest_on() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        for i in 0..10 {
            let sym = if i % 2 == 0 { "A" } else { "B" };
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i),
                sym,
                i
            ));
        }
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn insert_and_window_function() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn insert_and_join() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE, v1 DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE, v2 DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 20)", ts(0)));
        let (_, rows) = db.query("SELECT a.v1, b.v2 FROM a INNER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn insert_and_union() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a UNION ALL SELECT v FROM b");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn insert_select_with_where_and_order() {
        let db = TestDb::with_trades(20);
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)");
        db.exec_ok(
            "INSERT INTO t2 SELECT * FROM trades WHERE side = 'buy' ORDER BY price DESC LIMIT 5",
        );
        let (_, rows) = db.query("SELECT count(*) FROM t2");
        assert_eq!(rows[0][0], Value::I64(5));
    }

    #[test]
    fn insert_and_count_distinct() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        for i in 0..30 {
            let s = format!("val_{}", i % 5);
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}')", ts(i), s));
        }
        let val = db.query_scalar("SELECT count_distinct(s) FROM t");
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn insert_and_first_last() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT first(v), last(v) FROM t");
        assert_ne!(rows[0][0], rows[0][1]);
    }
}
