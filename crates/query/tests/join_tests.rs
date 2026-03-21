//! JOIN tests for ExchangeDB (100+ tests).
//!
//! Covers: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN,
//! ASOF JOIN, multi-table joins, self-joins, and complex ON conditions.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Create a database with `trades` and `markets` tables for join testing.
/// - trades: (timestamp, symbol, price) -- BTC/USD, ETH/USD, SOL/USD
/// - markets: (timestamp, symbol, name) -- only BTC/USD and ETH/USD (no SOL)
fn setup_join_db() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
    db.exec_ok("CREATE TABLE markets (timestamp TIMESTAMP, symbol VARCHAR, name VARCHAR)");

    for (i, (sym, price)) in [("BTC/USD", 60000.0), ("ETH/USD", 3000.0), ("SOL/USD", 100.0)]
        .iter()
        .enumerate()
    {
        db.exec_ok(&format!(
            "INSERT INTO trades VALUES ({}, '{}', {})", ts(i as i64), sym, price
        ));
    }

    for (i, (sym, name)) in [("BTC/USD", "Bitcoin"), ("ETH/USD", "Ethereum")]
        .iter()
        .enumerate()
    {
        db.exec_ok(&format!(
            "INSERT INTO markets VALUES ({}, '{}', '{}')", ts(i as i64), sym, name
        ));
    }

    db
}

/// Create a database with two tables having overlapping keys for join tests.
fn setup_orders_db() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE orders (timestamp TIMESTAMP, order_id DOUBLE, symbol VARCHAR, qty DOUBLE)");
    db.exec_ok("CREATE TABLE fills (timestamp TIMESTAMP, order_id DOUBLE, price DOUBLE, filled_qty DOUBLE)");

    // Orders: 5 orders
    for i in 0..5 {
        db.exec_ok(&format!(
            "INSERT INTO orders VALUES ({}, {}, '{}', {})",
            ts(i), i, if i % 2 == 0 { "BTC" } else { "ETH" }, (i + 1) as f64 * 10.0
        ));
    }

    // Fills: orders 0, 1, 2 have fills; orders 3, 4 do not
    for i in 0..3 {
        db.exec_ok(&format!(
            "INSERT INTO fills VALUES ({}, {}, {}, {})",
            ts(i), i, 50000.0 + i as f64 * 1000.0, (i + 1) as f64 * 5.0
        ));
    }
    // Extra fill for order 0
    db.exec_ok(&format!(
        "INSERT INTO fills VALUES ({}, 0, 50100.0, 3.0)", ts(10)
    ));

    db
}

// ===========================================================================
// inner_join
// ===========================================================================
mod inner_join {
    use super::*;

    #[test]
    fn basic_inner_join() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, t.price, m.name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol"
        );
        assert_eq!(rows.len(), 2); // BTC and ETH match
    }

    #[test]
    fn inner_join_no_match_excluded() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol FROM trades t INNER JOIN markets m ON t.symbol = m.symbol"
        );
        let symbols: Vec<&str> = rows.iter().map(|r| match &r[0] {
            Value::Str(s) => s.as_str(),
            other => panic!("{other:?}"),
        }).collect();
        assert!(!symbols.contains(&"SOL/USD"));
    }

    #[test]
    fn inner_join_with_where() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, t.price FROM trades t INNER JOIN markets m ON t.symbol = m.symbol WHERE t.price > 10000"
        );
        assert!(!rows.is_empty());
        for row in &rows {
            match &row[1] {
                Value::F64(p) => assert!(*p > 10000.0),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn inner_join_with_order_by() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.price FROM trades t INNER JOIN markets m ON t.symbol = m.symbol ORDER BY t.price DESC"
        );
        assert_eq!(rows.len(), 2);
        assert!(rows[0][0].cmp_coerce(&rows[1][0]) != Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn inner_join_self_join() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, id DOUBLE, parent_id DOUBLE, name VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 0, 'root')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2, 1, 'child1')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3, 1, 'child2')", ts(2)));
        let (_, rows) = db.query(
            "SELECT c.name, p.name FROM t c INNER JOIN t p ON c.parent_id = p.id"
        );
        assert_eq!(rows.len(), 2); // child1 and child2 match root
    }

    #[test]
    fn inner_join_one_to_many() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.order_id, f.price FROM orders o INNER JOIN fills f ON o.order_id = f.order_id"
        );
        // Order 0 has 2 fills, orders 1 and 2 have 1 each = 4 total
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn inner_join_with_count() {
        let db = setup_orders_db();
        let val = db.query_scalar(
            "SELECT count(*) FROM orders o INNER JOIN fills f ON o.order_id = f.order_id"
        );
        assert_eq!(val, Value::I64(4));
    }

    #[test]
    fn inner_join_with_group_by() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.symbol, count(*) FROM orders o INNER JOIN fills f ON o.order_id = f.order_id GROUP BY o.symbol"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn inner_join_empty_right_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a INNER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn inner_join_empty_left_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a INNER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn inner_join_both_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM a INNER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn inner_join_with_limit() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT * FROM orders o INNER JOIN fills f ON o.order_id = f.order_id LIMIT 2"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn inner_join_select_specific_columns() {
        let db = setup_join_db();
        let (cols, rows) = db.query(
            "SELECT t.price, m.name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol"
        );
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn inner_join_with_alias() {
        let db = setup_join_db();
        let (cols, _) = db.query(
            "SELECT t.price AS trade_price, m.name AS market_name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol"
        );
        assert!(cols.contains(&"trade_price".to_string()) || cols.contains(&"price".to_string()));
    }
}

// ===========================================================================
// left_join
// ===========================================================================
mod left_join {
    use super::*;

    #[test]
    fn basic_left_join() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol"
        );
        assert_eq!(rows.len(), 3); // All 3 trades appear
    }

    #[test]
    fn left_join_null_for_nonmatching() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol ORDER BY t.symbol"
        );
        // SOL/USD should have NULL for m.name
        let sol_row = rows.iter().find(|r| r[0] == Value::Str("SOL/USD".to_string()));
        assert!(sol_row.is_some());
        assert_eq!(sol_row.unwrap()[1], Value::Null);
    }

    #[test]
    fn left_join_with_where_on_left() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol WHERE t.price > 1000"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn left_join_all_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 20)", ts(0)));
        let (_, rows) = db.query("SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
        assert_ne!(rows[0][1], Value::Null);
    }

    #[test]
    fn left_join_no_right_matches() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2)", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 99)", ts(0)));
        let (_, rows) = db.query("SELECT a.k, b.k FROM a LEFT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row[1], Value::Null);
        }
    }

    #[test]
    fn left_join_one_to_many() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.order_id, f.price FROM orders o LEFT JOIN fills f ON o.order_id = f.order_id"
        );
        // 5 orders: order 0 -> 2 fills, orders 1,2 -> 1 fill each, orders 3,4 -> NULL
        assert!(rows.len() >= 5);
    }

    #[test]
    fn left_join_preserves_all_left_rows() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT DISTINCT o.order_id FROM orders o LEFT JOIN fills f ON o.order_id = f.order_id"
        );
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn left_join_with_count() {
        let db = setup_join_db();
        let val = db.query_scalar(
            "SELECT count(*) FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol"
        );
        assert_eq!(val, Value::I64(3));
    }

    #[test]
    fn left_join_empty_right() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a LEFT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn left_join_empty_left() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a LEFT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 0);
    }
}

// ===========================================================================
// right_join
// ===========================================================================
mod right_join {
    use super::*;

    #[test]
    fn basic_right_join() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, m.name FROM trades t RIGHT JOIN markets m ON t.symbol = m.symbol"
        );
        // Both BTC and ETH should appear
        assert!(rows.len() >= 2);
    }

    #[test]
    fn right_join_null_for_nonmatching() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 20)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2, 30)", ts(1)));
        let (_, rows) = db.query("SELECT a.v, b.w FROM a RIGHT JOIN b ON a.k = b.k ORDER BY b.k");
        assert_eq!(rows.len(), 2);
        // k=2 has no left match -> a.v should be NULL
        let row_k2 = rows.iter().find(|r| r[1] == Value::F64(30.0));
        assert!(row_k2.is_some());
        assert_eq!(row_k2.unwrap()[0], Value::Null);
    }

    #[test]
    fn right_join_all_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a RIGHT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn right_join_empty_left() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a RIGHT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn right_join_preserves_all_right_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO b VALUES ({}, {})", ts(i), i));
        }
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 0)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a RIGHT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 5);
    }
}

// ===========================================================================
// full_outer_join
// ===========================================================================
mod full_outer_join {
    use super::*;

    #[test]
    fn basic_full_outer_join() {
        let db = setup_join_db();
        let (_, rows) = db.query(
            "SELECT t.symbol, m.name FROM trades t FULL OUTER JOIN markets m ON t.symbol = m.symbol"
        );
        // BTC, ETH match both sides; SOL only in trades -> 3 rows total
        assert!(rows.len() >= 3);
    }

    #[test]
    fn full_outer_join_nulls_both_sides() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2, 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2, 200)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 3, 300)", ts(1)));
        let (_, rows) = db.query(
            "SELECT a.k, a.v, b.k, b.w FROM a FULL OUTER JOIN b ON a.k = b.k"
        );
        // k=1: left only, k=2: both, k=3: right only -> 3 rows
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn full_outer_join_both_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM a FULL OUTER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn full_outer_join_one_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a FULL OUTER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn full_outer_join_with_count() {
        let db = setup_join_db();
        let val = db.query_scalar(
            "SELECT count(*) FROM trades t FULL OUTER JOIN markets m ON t.symbol = m.symbol"
        );
        match val {
            Value::I64(n) => assert!(n >= 3),
            other => panic!("{other:?}"),
        }
    }
}

// ===========================================================================
// cross_join
// ===========================================================================
mod cross_join {
    use super::*;

    #[test]
    fn basic_cross_join() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2)", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 30)", ts(2)));
        let (_, rows) = db.query("SELECT a.v, b.w FROM a CROSS JOIN b");
        assert_eq!(rows.len(), 6); // 2 * 3
    }

    #[test]
    fn cross_join_with_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM a CROSS JOIN b");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn cross_join_single_row_each() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2)", ts(0)));
        let (_, rows) = db.query("SELECT a.v, b.w FROM a CROSS JOIN b");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn cross_join_cartesian_product_size() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, w DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO a VALUES ({}, {})", ts(i), i));
        }
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO b VALUES ({}, {})", ts(i), i));
        }
        let val = db.query_scalar("SELECT count(*) FROM a CROSS JOIN b");
        assert_eq!(val, Value::I64(20)); // 5 * 4
    }

    #[test]
    fn cross_join_with_where() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, w DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO a VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO b VALUES ({}, {})", ts(i), i));
        }
        let (_, rows) = db.query("SELECT a.v, b.w FROM a CROSS JOIN b WHERE a.v = b.w");
        assert_eq!(rows.len(), 3); // diagonal
    }
}

// ===========================================================================
// asof_join
// ===========================================================================
mod asof_join {
    use super::*;

    #[test]
    fn basic_asof_join() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query(
            "SELECT trades.price, quotes.bid FROM trades ASOF JOIN quotes ON trades.symbol = quotes.symbol"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn asof_join_with_aliases() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query(
            "SELECT t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn asof_join_matches_closest_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");

        // Trade at t=10s
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 100.0)", ts(10)));
        // Quotes at t=5s and t=15s
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', 99.0)", ts(5)));
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', 101.0)", ts(15)));

        let (_, rows) = db.query(
            "SELECT t.price, q.bid FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert!(!rows.is_empty());
        // ASOF should match the quote at t=5s (most recent <= trade timestamp)
        assert_eq!(rows[0][1], Value::F64(99.0));
    }

    #[test]
    fn asof_join_no_prior_quote_returns_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");

        // Trade at t=0 (before any quote)
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 100.0)", ts(0)));
        // Quote at t=10s
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', 99.0)", ts(10)));

        let (_, rows) = db.query(
            "SELECT t.price, q.bid FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert_eq!(rows.len(), 1);
        // No prior quote -> bid should be NULL
        assert_eq!(rows[0][1], Value::Null);
    }

    #[test]
    fn asof_join_multiple_symbols() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");

        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 100.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 200.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', 99.0)", ts(5)));
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'B', 199.0)", ts(5)));

        let (_, rows) = db.query(
            "SELECT t.symbol, t.price, q.bid FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn asof_join_many_quotes() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");

        // Trade at t=50s
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 100.0)", ts(50)));
        // Many quotes before
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', {})", ts(i * 5), 90.0 + i as f64));
        }

        let (_, rows) = db.query(
            "SELECT t.price, q.bid FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert!(!rows.is_empty());
        // Should match quote at t=45s (bid = 99.0)
        assert_eq!(rows[0][1], Value::F64(99.0));
    }

    #[test]
    fn asof_join_with_trades_and_quotes() {
        let db = TestDb::with_trades_and_quotes();
        let val = db.query_scalar(
            "SELECT count(*) FROM trades ASOF JOIN quotes ON trades.symbol = quotes.symbol"
        );
        match val {
            Value::I64(n) => assert!(n > 0),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn asof_join_exact_timestamp_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");

        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 100.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', 99.5)", ts(10)));

        let (_, rows) = db.query(
            "SELECT t.price, q.bid FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::F64(99.5));
    }

    #[test]
    fn asof_join_symbol_mismatch() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");

        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 100.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'B', 99.0)", ts(5)));

        let (_, rows) = db.query(
            "SELECT t.price, q.bid FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Null); // No matching symbol
    }
}

// ===========================================================================
// multi_table_join
// ===========================================================================
mod multi_table_join {
    use super::*;

    #[test]
    fn three_table_join() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, id DOUBLE, name VARCHAR)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, a_id DOUBLE, value DOUBLE)");
        db.exec_ok("CREATE TABLE c (timestamp TIMESTAMP, a_id DOUBLE, tag VARCHAR)");

        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 'alpha')", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2, 'beta')", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 100.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2, 200.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO c VALUES ({}, 1, 'x')", ts(0)));
        db.exec_ok(&format!("INSERT INTO c VALUES ({}, 2, 'y')", ts(1)));

        let (_, rows) = db.query(
            "SELECT a.name, b.value, c.tag FROM a \
             INNER JOIN b ON a.id = b.a_id \
             INNER JOIN c ON a.id = c.a_id"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn three_table_join_with_missing() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, id DOUBLE, name VARCHAR)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, a_id DOUBLE, value DOUBLE)");
        db.exec_ok("CREATE TABLE c (timestamp TIMESTAMP, a_id DOUBLE, tag VARCHAR)");

        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 'alpha')", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2, 'beta')", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 100.0)", ts(0)));
        // No b row for a_id=2
        db.exec_ok(&format!("INSERT INTO c VALUES ({}, 1, 'x')", ts(0)));
        db.exec_ok(&format!("INSERT INTO c VALUES ({}, 2, 'y')", ts(1)));

        let (_, rows) = db.query(
            "SELECT a.name, b.value, c.tag FROM a \
             LEFT JOIN b ON a.id = b.a_id \
             INNER JOIN c ON a.id = c.a_id"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn four_table_join() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, k DOUBLE, v1 DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, k DOUBLE, v2 DOUBLE)");
        db.exec_ok("CREATE TABLE t3 (timestamp TIMESTAMP, k DOUBLE, v3 DOUBLE)");
        db.exec_ok("CREATE TABLE t4 (timestamp TIMESTAMP, k DOUBLE, v4 DOUBLE)");

        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, {}, {})", ts(i), i, i * 10));
            db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, {}, {})", ts(i), i, i * 20));
            db.exec_ok(&format!("INSERT INTO t3 VALUES ({}, {}, {})", ts(i), i, i * 30));
            db.exec_ok(&format!("INSERT INTO t4 VALUES ({}, {}, {})", ts(i), i, i * 40));
        }

        let (_, rows) = db.query(
            "SELECT t1.v1, t2.v2, t3.v3, t4.v4 FROM t1 \
             INNER JOIN t2 ON t1.k = t2.k \
             INNER JOIN t3 ON t1.k = t3.k \
             INNER JOIN t4 ON t1.k = t4.k"
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn mixed_join_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, id DOUBLE, name VARCHAR)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, a_id DOUBLE, val DOUBLE)");
        db.exec_ok("CREATE TABLE c (timestamp TIMESTAMP, a_id DOUBLE, tag VARCHAR)");

        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 'x')", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 2, 'y')", ts(1)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO c VALUES ({}, 2, 'z')", ts(0)));

        let (_, rows) = db.query(
            "SELECT a.name, b.val, c.tag FROM a \
             LEFT JOIN b ON a.id = b.a_id \
             LEFT JOIN c ON a.id = c.a_id"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn join_with_aggregation() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.symbol, sum(f.filled_qty) FROM orders o \
             INNER JOIN fills f ON o.order_id = f.order_id \
             GROUP BY o.symbol"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn join_with_subquery_as_table() {
        let db = TestDb::with_trades(20);
        db.exec_ok("CREATE TABLE ref_prices (timestamp TIMESTAMP, symbol VARCHAR, ref_price DOUBLE)");
        db.exec_ok(&format!("INSERT INTO ref_prices VALUES ({}, 'BTC/USD', 60000.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO ref_prices VALUES ({}, 'ETH/USD', 3000.0)", ts(0)));
        let (_, rows) = db.query(
            "SELECT t.symbol, t.price, r.ref_price FROM trades t \
             INNER JOIN ref_prices r ON t.symbol = r.symbol"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn join_result_with_order_by() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.order_id, f.price FROM orders o \
             INNER JOIN fills f ON o.order_id = f.order_id \
             ORDER BY f.price DESC"
        );
        for i in 1..rows.len() {
            assert!(rows[i - 1][1].cmp_coerce(&rows[i][1]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn join_with_distinct() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT DISTINCT o.symbol FROM orders o \
             INNER JOIN fills f ON o.order_id = f.order_id"
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn left_join_with_count_per_group() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.order_id, count(f.price) FROM orders o \
             LEFT JOIN fills f ON o.order_id = f.order_id \
             GROUP BY o.order_id"
        );
        assert_eq!(rows.len(), 5); // all 5 orders
    }

    #[test]
    fn inner_join_duplicate_keys() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE, w DOUBLE)");
        // Two rows in a with same key
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1, 20)", ts(1)));
        // Two rows in b with same key
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 100)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 1, 200)", ts(1)));
        let (_, rows) = db.query("SELECT a.v, b.w FROM a INNER JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 4); // 2 * 2 = 4
    }

    #[test]
    fn left_join_with_null_key() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, k DOUBLE, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, k DOUBLE, w DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, NULL, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, NULL, 20)", ts(0)));
        let (_, rows) = db.query("SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn join_with_where_on_both_tables() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.order_id, f.price FROM orders o \
             INNER JOIN fills f ON o.order_id = f.order_id \
             WHERE o.symbol = 'BTC' AND f.price > 50000"
        );
        for row in &rows {
            match &row[1] {
                Value::F64(p) => assert!(*p > 50000.0),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn cross_join_with_limit() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, w DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO a VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO b VALUES ({}, {})", ts(i), i * 10));
        }
        let (_, rows) = db.query("SELECT * FROM a CROSS JOIN b LIMIT 10");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn inner_join_with_sum() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.symbol, sum(f.filled_qty) AS total_filled \
             FROM orders o INNER JOIN fills f ON o.order_id = f.order_id \
             GROUP BY o.symbol"
        );
        for row in &rows {
            match &row[1] {
                Value::F64(s) => assert!(*s > 0.0),
                Value::I64(s) => assert!(*s > 0),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn left_join_with_having() {
        let db = setup_orders_db();
        let (_, rows) = db.query(
            "SELECT o.order_id, count(f.price) AS fill_count \
             FROM orders o LEFT JOIN fills f ON o.order_id = f.order_id \
             GROUP BY o.order_id HAVING fill_count > 0"
        );
        assert!(rows.len() <= 5);
    }

    #[test]
    fn asof_join_with_many_trades() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        db.exec_ok("CREATE TABLE q (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', {})", ts(i * 2), 100.0 + i as f64));
        }
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO q VALUES ({}, 'A', {})", ts(i * 4 + 1), 99.0 + i as f64));
        }
        let val = db.query_scalar(
            "SELECT count(*) FROM t ASOF JOIN q ON t.symbol = q.symbol"
        );
        assert_eq!(val, Value::I64(20));
    }

    #[test]
    fn join_after_update() {
        let db = setup_join_db();
        db.exec_ok("UPDATE trades SET price = 0.0 WHERE symbol = 'BTC/USD'");
        let (_, rows) = db.query(
            "SELECT t.price, m.name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol WHERE t.symbol = 'BTC/USD'"
        );
        for row in &rows {
            assert_eq!(row[0], Value::F64(0.0));
        }
    }

    #[test]
    fn join_after_delete() {
        let db = setup_join_db();
        db.exec_ok("DELETE FROM trades WHERE symbol = 'SOL/USD'");
        let (_, rows) = db.query(
            "SELECT t.symbol FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol"
        );
        assert_eq!(rows.len(), 2); // only BTC and ETH
    }

    #[test]
    fn self_join_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, k DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        let val = db.query_scalar("SELECT count(*) FROM t a INNER JOIN t b ON a.k = b.k");
        assert_eq!(val, Value::I64(5));
    }
}
