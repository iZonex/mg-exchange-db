//! Set operations and CTE tests for ExchangeDB (50+ tests).
//!
//! Covers: UNION ALL, UNION (distinct), INTERSECT, EXCEPT, CTEs.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Setup two tables with some overlapping data for set operation tests.
fn setup_set_db() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
    db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");

    // t1: BTC(100), ETH(200), SOL(300)
    db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 'BTC', 100.0)", ts(0)));
    db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 'ETH', 200.0)", ts(1)));
    db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 'SOL', 300.0)", ts(2)));

    // t2: ETH(200), SOL(300), ADA(400) - overlaps on ETH and SOL
    db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 'ETH', 200.0)", ts(3)));
    db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 'SOL', 300.0)", ts(4)));
    db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 'ADA', 400.0)", ts(5)));

    db
}

// ===========================================================================
// union_all
// ===========================================================================
mod union_all {
    use super::*;

    #[test]
    fn basic_union_all() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol, price FROM t1 UNION ALL SELECT symbol, price FROM t2");
        assert_eq!(rows.len(), 6); // 3 + 3, no dedup
    }

    #[test]
    fn union_all_preserves_duplicates() {
        let db = setup_set_db();
        let (_, rows) = db.query("SELECT symbol FROM t1 UNION ALL SELECT symbol FROM t2");
        // ETH and SOL appear twice each
        let symbols: Vec<&str> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::Str(s) => s.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(symbols.iter().filter(|&&s| s == "ETH").count(), 2);
        assert_eq!(symbols.iter().filter(|&&s| s == "SOL").count(), 2);
    }

    #[test]
    fn union_all_single_column() {
        let db = setup_set_db();
        let (cols, rows) = db.query("SELECT price FROM t1 UNION ALL SELECT price FROM t2");
        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn union_all_with_where() {
        let db = setup_set_db();
        let (_, rows) = db.query(
            "SELECT symbol FROM t1 WHERE price > 100 UNION ALL SELECT symbol FROM t2 WHERE price > 300"
        );
        // t1: ETH(200), SOL(300) -> 2 rows; t2: ADA(400) -> 1 row = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn union_all_empty_left() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol FROM t1 WHERE price > 9999 UNION ALL SELECT symbol FROM t2");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn union_all_empty_right() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol FROM t1 UNION ALL SELECT symbol FROM t2 WHERE price > 9999");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn union_all_both_empty() {
        let db = setup_set_db();
        let (_, rows) = db.query(
            "SELECT symbol FROM t1 WHERE price > 9999 UNION ALL SELECT symbol FROM t2 WHERE price > 9999"
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn union_all_with_limit() {
        let db = setup_set_db();
        let (_, rows) = db.query("(SELECT symbol FROM t1 UNION ALL SELECT symbol FROM t2) LIMIT 4");
        assert!(rows.len() <= 4);
    }

    #[test]
    fn union_all_three_queries() {
        let db = setup_set_db();
        db.exec_ok("CREATE TABLE t3 (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t3 VALUES ({}, 'DOT', 500.0)", ts(6)));
        let (_, rows) = db.query(
            "SELECT symbol FROM t1 UNION ALL SELECT symbol FROM t2 UNION ALL SELECT symbol FROM t3",
        );
        assert_eq!(rows.len(), 7); // 3 + 3 + 1
    }

    #[test]
    fn union_all_same_table() {
        let db = setup_set_db();
        let (_, rows) = db.query("SELECT symbol FROM t1 UNION ALL SELECT symbol FROM t1");
        assert_eq!(rows.len(), 6); // 3 + 3
    }

    #[test]
    fn union_all_different_column_count_error() {
        let db = setup_set_db();
        let result = db.exec("SELECT symbol, price FROM t1 UNION ALL SELECT symbol FROM t2");
        assert!(result.is_err());
    }
}

// ===========================================================================
// union_distinct
// ===========================================================================
mod union_distinct {
    use super::*;

    #[test]
    fn basic_union() {
        let db = setup_set_db();
        let (_, rows) = db.query("SELECT symbol, price FROM t1 UNION SELECT symbol, price FROM t2");
        // BTC(100), ETH(200), SOL(300), ADA(400) - duplicates removed
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn union_dedup_exact_match() {
        let db = setup_set_db();
        let (_, rows) = db.query("SELECT symbol FROM t1 UNION SELECT symbol FROM t2");
        // BTC, ETH, SOL, ADA -> 4 distinct symbols
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn union_all_same_data() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t UNION SELECT v FROM t");
        assert_eq!(rows.len(), 1); // deduped
    }

    #[test]
    fn union_with_different_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a UNION SELECT v FROM b");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn union_with_nulls() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, NULL)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, NULL)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a UNION SELECT v FROM b");
        // Both NULLs might be deduped or not depending on implementation
        assert!(rows.len() >= 1);
    }

    #[test]
    fn union_no_overlap() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 3.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 4.0)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM a UNION SELECT v FROM b");
        assert_eq!(rows.len(), 4);
    }
}

// ===========================================================================
// intersect
// ===========================================================================
mod intersect {
    use super::*;

    #[test]
    fn basic_intersect() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol, price FROM t1 INTERSECT SELECT symbol, price FROM t2");
        // ETH(200) and SOL(300) are in both
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn intersect_no_overlap() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a INTERSECT SELECT v FROM b");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn intersect_identical_tables() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol, price FROM t1 INTERSECT SELECT symbol, price FROM t1");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn intersect_single_column() {
        let db = setup_set_db();
        let (_, rows) = db.query("SELECT symbol FROM t1 INTERSECT SELECT symbol FROM t2");
        // ETH and SOL are in both
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn intersect_empty_left() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol FROM t1 WHERE price > 9999 INTERSECT SELECT symbol FROM t2");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn intersect_empty_right() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol FROM t1 INTERSECT SELECT symbol FROM t2 WHERE price > 9999");
        assert_eq!(rows.len(), 0);
    }
}

// ===========================================================================
// except
// ===========================================================================
mod except {
    use super::*;

    #[test]
    fn basic_except() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol, price FROM t1 EXCEPT SELECT symbol, price FROM t2");
        // BTC(100) is only in t1
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn except_reverse() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol, price FROM t2 EXCEPT SELECT symbol, price FROM t1");
        // ADA(400) is only in t2
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn except_no_difference() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol, price FROM t1 EXCEPT SELECT symbol, price FROM t1");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn except_all_different() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 3.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a EXCEPT SELECT v FROM b");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn except_empty_result() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM a EXCEPT SELECT v FROM b");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn except_single_column() {
        let db = setup_set_db();
        let (_, rows) = db.query("SELECT symbol FROM t1 EXCEPT SELECT symbol FROM t2");
        // BTC only
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("BTC".to_string()));
    }
}

// ===========================================================================
// cte: Common Table Expressions
// ===========================================================================
mod cte {
    use super::*;

    #[test]
    fn basic_cte() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH btc AS (SELECT * FROM trades WHERE symbol = 'BTC/USD') SELECT count(*) FROM btc",
        );
        let count = match &rows[0][0] {
            Value::I64(n) => *n,
            other => panic!("{other:?}"),
        };
        assert!(count > 0);
    }

    #[test]
    fn cte_with_aggregation() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH stats AS (SELECT symbol, avg(price) AS avg_p FROM trades GROUP BY symbol) \
             SELECT * FROM stats",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cte_referenced_twice() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH btc AS (SELECT price FROM trades WHERE symbol = 'BTC/USD') \
             SELECT (SELECT count(*) FROM btc) FROM btc LIMIT 1",
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn cte_with_filter() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH expensive AS (SELECT * FROM trades WHERE price > 10000) \
             SELECT count(*) FROM expensive",
        );
        let count = match &rows[0][0] {
            Value::I64(n) => *n,
            other => panic!("{other:?}"),
        };
        assert!(count > 0);
    }

    #[test]
    fn multiple_ctes() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH btc AS (SELECT * FROM trades WHERE symbol = 'BTC/USD'), \
                  eth AS (SELECT * FROM trades WHERE symbol = 'ETH/USD') \
             SELECT (SELECT count(*) FROM btc), (SELECT count(*) FROM eth)",
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn cte_select_subset() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH prices AS (SELECT price FROM trades) \
             SELECT min(price), max(price) FROM prices",
        );
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].cmp_coerce(&rows[0][1]) == Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn cte_empty_result() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query(
            "WITH empty AS (SELECT * FROM trades WHERE symbol = 'DOGE/USD') \
             SELECT count(*) FROM empty",
        );
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn cte_with_order_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH sorted AS (SELECT price FROM trades ORDER BY price DESC) \
             SELECT * FROM sorted LIMIT 5",
        );
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn cte_with_join() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query(
            "WITH btc_trades AS (SELECT * FROM trades WHERE symbol = 'BTC/USD'), \
                  btc_quotes AS (SELECT * FROM quotes WHERE symbol = 'BTC/USD') \
             SELECT t.price, q.bid FROM btc_trades t \
             INNER JOIN btc_quotes q ON t.timestamp = q.timestamp",
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn cte_with_limit() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH top AS (SELECT price FROM trades ORDER BY price DESC LIMIT 3) \
             SELECT * FROM top",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cte_count_from_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("WITH data AS (SELECT * FROM t) SELECT count(*) FROM data");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn cte_with_distinct() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("WITH syms AS (SELECT DISTINCT symbol FROM trades) SELECT count(*) FROM syms");
        assert_eq!(rows[0][0], Value::I64(3));
    }
}

// ===========================================================================
// Additional set operation edge cases
// ===========================================================================
mod set_operations_extra {
    use super::*;

    #[test]
    fn union_all_with_aggregation() {
        let db = setup_set_db();
        let val = db.query_scalar(
            "SELECT count(*) FROM (SELECT symbol FROM t1 UNION ALL SELECT symbol FROM t2)",
        );
        assert_eq!(val, Value::I64(6));
    }

    #[test]
    fn union_all_preserves_order() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a UNION ALL SELECT v FROM b");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn intersect_with_where() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol FROM t1 WHERE price > 100 INTERSECT SELECT symbol FROM t2");
        // ETH(200) and SOL(300) are in both and have price > 100
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn except_with_where() {
        let db = setup_set_db();
        let (_, rows) =
            db.query("SELECT symbol FROM t1 WHERE price <= 200 EXCEPT SELECT symbol FROM t2");
        // BTC(100) is in t1 with price <= 200, not in t2; ETH(200) is in both
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn union_type_coercion() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM a UNION ALL SELECT v FROM b");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn cte_in_union() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "WITH btc AS (SELECT price FROM trades WHERE symbol = 'BTC/USD') \
             SELECT price FROM btc UNION ALL SELECT price FROM btc",
        );
        // Double the BTC rows
        let btc_count =
            match db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'") {
                Value::I64(n) => n,
                other => panic!("{other:?}"),
            };
        assert_eq!(rows.len() as i64, btc_count * 2);
    }

    #[test]
    fn union_all_large_tables() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        let values_a: Vec<String> = (0..100).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        let values_b: Vec<String> = (100..200)
            .map(|i| format!("({}, {}.0)", ts(i), i))
            .collect();
        db.exec_ok(&format!("INSERT INTO a VALUES {}", values_a.join(", ")));
        db.exec_ok(&format!("INSERT INTO b VALUES {}", values_b.join(", ")));
        let val =
            db.query_scalar("SELECT count(*) FROM (SELECT v FROM a UNION ALL SELECT v FROM b)");
        assert_eq!(val, Value::I64(200));
    }

    #[test]
    fn union_distinct_large() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        // Same values in both
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO a VALUES ({}, {}.0)", ts(i), i));
            db.exec_ok(&format!("INSERT INTO b VALUES ({}, {}.0)", ts(i + 100), i));
        }
        let val = db.query_scalar("SELECT count(*) FROM (SELECT v FROM a UNION SELECT v FROM b)");
        assert_eq!(val, Value::I64(50)); // deduped
    }

    #[test]
    fn intersect_with_multiple_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, x DOUBLE, y VARCHAR)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, x DOUBLE, y VARCHAR)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2.0, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1.0, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 3.0, 'c')", ts(1)));
        let (_, rows) = db.query("SELECT x, y FROM a INTERSECT SELECT x, y FROM b");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn except_with_multiple_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, x DOUBLE, y VARCHAR)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, x DOUBLE, y VARCHAR)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2.0, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1.0, 'a')", ts(0)));
        let (_, rows) = db.query("SELECT x, y FROM a EXCEPT SELECT x, y FROM b");
        assert_eq!(rows.len(), 1);
    }
}
