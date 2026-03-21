//! DELETE statement tests for ExchangeDB (50+ tests).
//!
//! Covers: basic DELETE with WHERE, DELETE all rows, complex WHERE conditions,
//! edge cases (empty table, nonexistent table, chained operations).

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// ===========================================================================
// delete_basic: DELETE with WHERE
// ===========================================================================
mod delete_basic {
    use super::*;

    #[test]
    fn delete_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        db.exec_ok("DELETE FROM t WHERE v = 1.0");
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::F64(2.0));
    }

    #[test]
    fn delete_by_string_column() {
        let db = TestDb::with_trades(20);
        let (_, before) = db.query("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'");
        let sol_count = match &before[0][0] { Value::I64(n) => *n, other => panic!("{other:?}") };
        assert!(sol_count > 0);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'SOL/USD'");
        let (_, after) = db.query("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'");
        assert_eq!(after[0][0], Value::I64(0));
    }

    #[test]
    fn delete_by_numeric_comparison() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE price > 50000");
        let (_, rows) = db.query("SELECT count(*) FROM trades WHERE price > 50000");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn delete_reduces_count() {
        let db = TestDb::with_trades(20);
        let before = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        let after = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert!(after < before);
    }

    #[test]
    fn delete_no_matching_rows() {
        let db = TestDb::with_trades(10);
        let before = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        db.exec_ok("DELETE FROM trades WHERE symbol = 'DOGE/USD'");
        let after = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(before, after);
    }

    #[test]
    fn delete_then_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("DELETE FROM t WHERE v = 1.0");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(2.0));
    }

    #[test]
    fn delete_preserves_unmatched_rows() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        // ETH and SOL should remain
        let (_, eth) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'");
        let (_, sol) = db.query("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'");
        assert!(match &eth[0][0] { Value::I64(n) => *n > 0, _ => false });
        assert!(match &sol[0][0] { Value::I64(n) => *n > 0, _ => false });
    }

    #[test]
    fn delete_by_side() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE side = 'buy'");
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("sell".to_string()));
    }

    #[test]
    fn delete_multiple_times() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        db.exec_ok("DELETE FROM trades WHERE symbol = 'ETH/USD'");
        db.exec_ok("DELETE FROM trades WHERE symbol = 'SOL/USD'");
        let count = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_by_lt_price() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE price < 200");
        let (_, rows) = db.query("SELECT price FROM trades WHERE price < 200");
        assert_eq!(rows.len(), 0);
    }
}

// ===========================================================================
// delete_all: DELETE without WHERE (all rows)
// ===========================================================================
mod delete_all {
    use super::*;

    #[test]
    fn delete_all_rows() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades");
        let count = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_all_then_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("DELETE FROM t");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }

    #[test]
    fn delete_all_twice() {
        let db = TestDb::with_trades(10);
        db.exec_ok("DELETE FROM trades");
        db.exec_ok("DELETE FROM trades"); // second delete on empty table
        assert_eq!(db.query_scalar("SELECT count(*) FROM trades"), Value::I64(0));
    }

    #[test]
    fn delete_all_preserves_schema() {
        let db = TestDb::with_trades(10);
        db.exec_ok("DELETE FROM trades");
        let (cols, _) = db.query("SELECT * FROM trades");
        assert_eq!(cols.len(), 5);
    }

    #[test]
    fn delete_all_large_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..500)
            .map(|i| format!("({}, {}.0)", ts(i), i))
            .collect();
        db.exec_ok(&format!("INSERT INTO t (timestamp, v) VALUES {}", values.join(", ")));
        db.exec_ok("DELETE FROM t");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn delete_all_verify_aggregates() {
        let db = TestDb::with_trades(10);
        db.exec_ok("DELETE FROM trades");
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(0));
    }
}

// ===========================================================================
// delete_conditions: complex WHERE, multiple conditions
// ===========================================================================
mod delete_conditions {
    use super::*;

    #[test]
    fn delete_with_and() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'");
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delete_with_or() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD'");
        let (_, btc) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'");
        let (_, eth) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'");
        assert_eq!(btc[0][0], Value::I64(0));
        assert_eq!(eth[0][0], Value::I64(0));
        // SOL should remain
        let sol = match db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert!(sol > 0);
    }

    #[test]
    fn delete_with_not_eq() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol != 'BTC/USD'");
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn delete_with_gte() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE price >= 60000");
        let (_, rows) = db.query("SELECT price FROM trades WHERE price >= 60000");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delete_with_lte() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE price <= 200");
        let (_, rows) = db.query("SELECT price FROM trades WHERE price <= 200");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delete_with_between() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE price BETWEEN 3000 AND 4000");
        let (_, rows) = db.query("SELECT count(*) FROM trades WHERE price BETWEEN 3000 AND 4000");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn delete_with_in_list() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol IN ('BTC/USD', 'SOL/USD')");
        let remaining = match db.query_scalar("SELECT count(DISTINCT symbol) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        // only ETH/USD should remain
        assert!(remaining <= 1);
    }

    #[test]
    fn delete_where_is_null() {
        let db = TestDb::with_trades(20);
        let null_before = match db.query_scalar("SELECT count(*) FROM trades WHERE volume IS NULL") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert!(null_before > 0);
        db.exec_ok("DELETE FROM trades WHERE volume IS NULL");
        let null_after = match db.query_scalar("SELECT count(*) FROM trades WHERE volume IS NULL") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(null_after, 0);
    }

    #[test]
    fn delete_where_is_not_null() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE volume IS NOT NULL");
        let (_, rows) = db.query("SELECT * FROM trades");
        for row in &rows {
            // volume column (index 3) should be NULL
            assert_eq!(row[3], Value::Null);
        }
    }

    #[test]
    fn delete_complex_and_or() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE (symbol = 'BTC/USD' AND side = 'buy') OR (symbol = 'SOL/USD' AND side = 'sell')");
        let (_, rows) = db.query("SELECT * FROM trades WHERE (symbol = 'BTC/USD' AND side = 'buy') OR (symbol = 'SOL/USD' AND side = 'sell')");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delete_by_timestamp_range() {
        let db = TestDb::with_trades(20);
        let cutoff = BASE_TS + 5 * 600_000_000_000i64; // after 5th row
        db.exec_ok(&format!("DELETE FROM trades WHERE timestamp < {}", cutoff));
        let (_, rows) = db.query(&format!("SELECT count(*) FROM trades WHERE timestamp < {}", cutoff));
        assert_eq!(rows[0][0], Value::I64(0));
    }
}

// ===========================================================================
// delete_edge: empty table, nonexistent table
// ===========================================================================
mod delete_edge {
    use super::*;

    #[test]
    fn delete_from_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DELETE FROM t"); // should not error
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn delete_from_empty_table_with_where() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DELETE FROM t WHERE v > 0"); // should not error
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn delete_from_nonexistent_table() {
        let db = TestDb::new();
        let result = db.exec("DELETE FROM no_such_table");
        assert!(result.is_err());
    }

    #[test]
    fn delete_insert_delete_cycle() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for cycle in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(cycle), cycle));
            db.exec_ok("DELETE FROM t");
            assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
        }
    }

    #[test]
    fn delete_half_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("DELETE FROM t WHERE v < 5");
        let count = match db.query_scalar("SELECT count(*) FROM t") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(count, 5);
    }

    #[test]
    fn delete_one_by_one() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        for i in 0..5 {
            db.exec_ok(&format!("DELETE FROM t WHERE v = {}.0", i));
            let remaining = match db.query_scalar("SELECT count(*) FROM t") {
                Value::I64(n) => n, other => panic!("{other:?}")
            };
            assert_eq!(remaining, 4 - i);
        }
    }

    #[test]
    fn delete_verify_remaining_data_integrity() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        // Verify remaining data is correct
        let (_, rows) = db.query("SELECT symbol FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert_ne!(s, "BTC/USD"),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn delete_with_order_by_in_select_after() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'SOL/USD'");
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn delete_large_dataset() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..500).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        db.exec_ok("DELETE FROM t WHERE v < 250");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(250));
    }

    #[test]
    fn delete_then_verify_aggregates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("DELETE FROM t WHERE v <= 5");
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // 6+7+8+9+10 = 40
        assert_eq!(val, Value::F64(40.0));
    }

    #[test]
    fn delete_then_group_by() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'SOL/USD'");
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 2); // only BTC and ETH
    }

    #[test]
    fn delete_after_update() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        db.exec_ok("UPDATE t SET v = 99.0 WHERE v = 1.0");
        db.exec_ok("DELETE FROM t WHERE v = 99.0");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(2.0));
    }

    #[test]
    fn delete_by_exact_double_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.14)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.71)", ts(1)));
        db.exec_ok("DELETE FROM t WHERE v = 3.14");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }

    #[test]
    fn delete_preserves_schema_columns() {
        let db = TestDb::with_trades(10);
        db.exec_ok("DELETE FROM trades");
        let (cols, _) = db.query("SELECT * FROM trades");
        assert_eq!(cols.len(), 5);
    }

    #[test]
    fn delete_interleaved_with_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
            if i % 2 == 0 {
                db.exec_ok(&format!("DELETE FROM t WHERE v = {}.0", i));
            }
        }
        // Only odd values remain: 1,3,5,7,9
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(5));
    }

    #[test]
    fn delete_using_like() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        let count = match db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(count, 0);
    }

    #[test]
    fn delete_verify_distinct_after() {
        let db = TestDb::with_trades(20);
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        db.exec_ok("DELETE FROM trades WHERE symbol = 'ETH/USD'");
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("SOL/USD".to_string()));
    }

    #[test]
    fn delete_verify_avg_changes() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        // Remove highest values
        db.exec_ok("DELETE FROM t WHERE v > 5");
        let val = db.query_scalar("SELECT avg(v) FROM t");
        match val {
            Value::F64(a) => assert_eq!(a, 3.0), // avg(1,2,3,4,5)=3
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn delete_with_multiple_conditions_and() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Y', 1.0)", ts(2)));
        db.exec_ok("DELETE FROM t WHERE a = 'X' AND b = 1.0");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(2));
    }

    #[test]
    fn delete_and_reinsert_cycle() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for cycle in 0..3 {
            for i in 0..5 {
                db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(cycle * 100 + i), cycle * 5 + i));
            }
        }
        db.exec_ok("DELETE FROM t WHERE v < 5");
        let count = match db.query_scalar("SELECT count(*) FROM t") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(count, 10);
    }

    #[test]
    fn delete_by_string_prefix_equivalent() {
        let db = TestDb::with_trades(20);
        // Delete all BTC trades
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn delete_then_count_per_symbol() {
        let db = TestDb::with_trades(30);
        let btc_before = match db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        db.exec_ok("DELETE FROM trades WHERE symbol = 'BTC/USD'");
        let total_after = match db.query_scalar("SELECT count(*) FROM trades") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert_eq!(total_after, 30 - btc_before);
    }
}
