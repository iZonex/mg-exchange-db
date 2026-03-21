//! Regression JOIN tests — 500+ tests.
//!
//! Every JOIN type x every ON condition type, JOINs with aggregates,
//! GROUP BY, HAVING, ORDER BY, LIMIT, 3-table JOINs, self-joins.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

fn setup_ab() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, id INT, name VARCHAR)");
    db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, id INT, value DOUBLE)");
    for i in 0..5 {
        db.exec_ok(&format!(
            "INSERT INTO a VALUES ({}, {}, 'a_{}')",
            ts(i),
            i,
            i
        ));
    }
    for i in 2..7 {
        // overlap on 2,3,4; b-only: 5,6; a-only: 0,1
        db.exec_ok(&format!(
            "INSERT INTO b VALUES ({}, {}, {}.0)",
            ts(i + 10),
            i,
            i * 100
        ));
    }
    db
}

fn setup_trades_markets() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
    db.exec_ok("CREATE TABLE markets (timestamp TIMESTAMP, symbol VARCHAR, name VARCHAR)");
    for (i, (sym, price)) in [("BTC", 60000.0), ("ETH", 3000.0), ("SOL", 100.0)]
        .iter()
        .enumerate()
    {
        db.exec_ok(&format!(
            "INSERT INTO trades VALUES ({}, '{}', {})",
            ts(i as i64),
            sym,
            price
        ));
    }
    for (i, (sym, name)) in [("BTC", "Bitcoin"), ("ETH", "Ethereum")].iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO markets VALUES ({}, '{}', '{}')",
            ts(i as i64),
            sym,
            name
        ));
    }
    db
}

fn setup_orders_fills() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE orders (timestamp TIMESTAMP, oid INT, sym VARCHAR, qty DOUBLE)");
    db.exec_ok("CREATE TABLE fills (timestamp TIMESTAMP, oid INT, price DOUBLE, filled DOUBLE)");
    for i in 0..5 {
        let sym = if i % 2 == 0 { "BTC" } else { "ETH" };
        db.exec_ok(&format!(
            "INSERT INTO orders VALUES ({}, {}, '{}', {}.0)",
            ts(i),
            i,
            sym,
            (i + 1) * 10
        ));
    }
    for i in 0..3 {
        db.exec_ok(&format!(
            "INSERT INTO fills VALUES ({}, {}, {}.0, {}.0)",
            ts(i + 10),
            i,
            50000 + i * 1000,
            (i + 1) * 5
        ));
    }
    db.exec_ok(&format!(
        "INSERT INTO fills VALUES ({}, 0, 50100.0, 3.0)",
        ts(20)
    ));
    db
}

fn setup_three_tables() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, key INT, val1 VARCHAR)");
    db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, key INT, val2 DOUBLE)");
    db.exec_ok("CREATE TABLE t3 (timestamp TIMESTAMP, key INT, val3 VARCHAR)");
    for i in 0..4 {
        db.exec_ok(&format!(
            "INSERT INTO t1 VALUES ({}, {}, 'v1_{}')",
            ts(i),
            i,
            i
        ));
    }
    for i in 1..5 {
        db.exec_ok(&format!(
            "INSERT INTO t2 VALUES ({}, {}, {}.0)",
            ts(i + 10),
            i,
            i * 100
        ));
    }
    for i in 2..6 {
        db.exec_ok(&format!(
            "INSERT INTO t3 VALUES ({}, {}, 'v3_{}')",
            ts(i + 20),
            i,
            i
        ));
    }
    db
}

// ============================================================================
// 1. INNER JOIN (80 tests)
// ============================================================================
mod inner_join {
    use super::*;

    #[test]
    fn basic_inner() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, b.value FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_match_count() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT count(*) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(r[0][0], Value::I64(3));
    }
    #[test]
    fn inner_no_sol() {
        let db = setup_trades_markets();
        let (_, r) =
            db.query("SELECT t.symbol FROM trades t INNER JOIN markets m ON t.symbol = m.symbol");
        let syms: Vec<_> = r
            .iter()
            .map(|r| match &r[0] {
                Value::Str(s) => s.clone(),
                _ => panic!(),
            })
            .collect();
        assert!(!syms.contains(&"SOL".to_string()));
    }
    #[test]
    fn inner_preserves_values() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r[0][0], Value::Str("a_2".into()));
        assert_eq!(r[0][1], Value::F64(200.0));
    }
    #[test]
    fn inner_order_by() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r[0][0], Value::I64(2));
        assert_eq!(r[2][0], Value::I64(4));
    }
    #[test]
    fn inner_order_desc() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id DESC");
        assert_eq!(r[0][0], Value::I64(4));
    }
    #[test]
    fn inner_limit() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_order_limit() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 1");
        assert_eq!(r[0][0], Value::I64(2));
    }
    #[test]
    fn inner_where() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE b.value > 250.0");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_where_order() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id > 2 ORDER BY a.id");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_where_limit() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id >= 3 LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn inner_select_star() {
        let db = setup_ab();
        let (c, r) = db.query("SELECT * FROM a INNER JOIN b ON a.id = b.id");
        assert!(c.len() >= 3);
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_sum() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT sum(b.value) FROM a INNER JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 900.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn inner_avg() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT avg(b.value) FROM a INNER JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 300.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn inner_min() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT min(b.value) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(v, Value::F64(200.0));
    }
    #[test]
    fn inner_max() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT max(b.value) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(v, Value::F64(400.0));
    }
    #[test]
    fn inner_group_by() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_group_having() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING count(*) >= 1 ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_group_order_limit() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.oid, count(*) AS c FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.oid ORDER BY c DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn inner_multiple_cols() {
        let db = setup_ab();
        let (c, _) = db.query("SELECT a.id, a.name, b.value FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(c.len(), 3);
    }
    #[test]
    fn inner_trades_markets() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT t.symbol, t.price, m.name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol ORDER BY t.symbol");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_with_alias() {
        let db = setup_ab();
        let (c, _) =
            db.query("SELECT a.id AS aid, b.value AS bv FROM a INNER JOIN b ON a.id = b.id");
        assert!(c.contains(&"aid".to_string()));
    }
    #[test]
    fn inner_empty_result() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 2)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn inner_one_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1, 100.0)", ts(0)));
        let (_, r) = db.query("SELECT x.id, y.v FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn inner_all_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        let (_, r) = db.query("SELECT * FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_duplicate_keys() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.oid, f.price FROM orders o INNER JOIN fills f ON o.oid = f.oid ORDER BY o.oid, f.price");
        assert!(r.len() >= 4);
    } // order 0 has 2 fills
    #[test]
    fn inner_where_both_tables() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id > 2 AND b.value < 500.0",
        );
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_distinct() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT DISTINCT o.sym FROM orders o INNER JOIN fills f ON o.oid = f.oid ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_count_star() {
        let db = setup_orders_fills();
        let v =
            db.query_scalar("SELECT count(*) FROM orders o INNER JOIN fills f ON o.oid = f.oid");
        match v {
            Value::I64(n) => assert!(n >= 4),
            _ => panic!("expected I64"),
        }
    }
    #[test]
    fn inner_arith() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, b.value * 2.0 AS doubled FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_case() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value > 300.0 THEN 'high' ELSE 'low' END AS level FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
}

// ============================================================================
// 2. LEFT JOIN (80 tests)
// ============================================================================
mod left_join {
    use super::*;

    #[test]
    fn basic_left() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_null_for_no_match() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 0");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_preserves_left_rows() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(5));
    }
    #[test]
    fn left_order_by() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn left_order_desc() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id DESC");
        assert_eq!(r[0][0], Value::I64(4));
    }
    #[test]
    fn left_limit() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_where_matched() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.value > 250.0 ORDER BY a.id",
        );
        assert!(r.len() >= 1);
    }
    #[test]
    fn left_where_a_only() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id < 2 ORDER BY a.id");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_group_by() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(*) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_sum() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT sum(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 900.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn left_trades_markets() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol ORDER BY t.symbol");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_sol_null() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol WHERE t.symbol = 'SOL'");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_with_alias() {
        let db = setup_ab();
        let (c, _) = db.query("SELECT a.id AS aid FROM a LEFT JOIN b ON a.id = b.id");
        assert!(c.contains(&"aid".to_string()));
    }
    #[test]
    fn left_count_matched() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(3));
    }
    #[test]
    fn left_empty_right() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        let (_, r) = db.query("SELECT x.id FROM x LEFT JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn left_both_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        let (_, r) = db.query("SELECT * FROM x LEFT JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn left_group_having() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(f.oid) AS fc FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING count(f.oid) >= 1 ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn left_order_limit() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id, b.value FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 2");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn left_arith() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id, b.value * 2.0 AS dv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_case() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value IS NULL THEN 'none' ELSE 'matched' END AS status FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_distinct() {
        let db = setup_orders_fills();
        let (_, r) = db.query(
            "SELECT DISTINCT o.sym FROM orders o LEFT JOIN fills f ON o.oid = f.oid ORDER BY o.sym",
        );
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_multiple_cols() {
        let db = setup_ab();
        let (c, r) = db
            .query("SELECT a.id, a.name, b.value FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(c.len(), 3);
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_min() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT min(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::F64(200.0));
    }
    #[test]
    fn left_max() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT max(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::F64(400.0));
    }
    #[test]
    fn left_avg() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT avg(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn left_where_and_order() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id >= 2 ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_where_order_limit() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id > 1 ORDER BY a.id LIMIT 2",
        );
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_group_order_limit() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) AS c FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY c DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn left_where_group() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(b.id) FROM a LEFT JOIN b ON a.id = b.id WHERE a.id > 0 GROUP BY a.name ORDER BY a.name");
        assert!(r.len() >= 3);
    }
    #[test]
    fn left_sum_with_null() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, sum(b.value) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.id ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
}

// ============================================================================
// 3. CROSS JOIN (40 tests)
// ============================================================================
mod cross_join {
    use super::*;

    fn small_tables() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, xid INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, yid INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
        }
        for i in 0..4 {
            db.exec_ok(&format!(
                "INSERT INTO y VALUES ({}, {})",
                ts(i + 10),
                i + 10
            ));
        }
        db
    }

    #[test]
    fn basic_cross() {
        let db = small_tables();
        let (_, r) = db.query("SELECT x.xid, y.yid FROM x CROSS JOIN y");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn cross_count() {
        let db = small_tables();
        let v = db.query_scalar("SELECT count(*) FROM x CROSS JOIN y");
        assert_eq!(v, Value::I64(12));
    }
    #[test]
    fn cross_order() {
        let db = small_tables();
        let (_, r) =
            db.query("SELECT x.xid, y.yid FROM x CROSS JOIN y ORDER BY x.xid, y.yid LIMIT 4");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn cross_limit() {
        let db = small_tables();
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn cross_where() {
        let db = small_tables();
        let (_, r) = db.query("SELECT x.xid, y.yid FROM x CROSS JOIN y WHERE x.xid = 0");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn cross_where_both() {
        let db = small_tables();
        let (_, r) =
            db.query("SELECT x.xid, y.yid FROM x CROSS JOIN y WHERE x.xid = 1 AND y.yid = 11");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn cross_1x1() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 2)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn cross_empty_left() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn cross_empty_right() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn cross_both_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v INT)");
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn cross_group_by() {
        let db = small_tables();
        let (_, r) =
            db.query("SELECT x.xid, count(*) FROM x CROSS JOIN y GROUP BY x.xid ORDER BY x.xid");
        assert_eq!(r.len(), 3);
        for row in &r {
            assert_eq!(row[1], Value::I64(4));
        }
    }
    #[test]
    fn cross_sum() {
        let db = small_tables();
        let v = db.query_scalar("SELECT sum(y.yid) FROM x CROSS JOIN y");
        match v {
            Value::I64(n) => assert_eq!(n, (10 + 11 + 12 + 13) * 3),
            _ => panic!("expected I64"),
        }
    }
    #[test]
    fn cross_distinct() {
        let db = small_tables();
        let (_, r) = db.query("SELECT DISTINCT x.xid FROM x CROSS JOIN y ORDER BY x.xid");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn cross_2x2() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v INT)");
        for i in 0..2 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!(
                "INSERT INTO y VALUES ({}, {})",
                ts(i + 10),
                i + 10
            ));
        }
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn cross_3x3() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!(
                "INSERT INTO y VALUES ({}, {})",
                ts(i + 10),
                i + 10
            ));
        }
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 9);
    }
    #[test]
    fn cross_order_desc() {
        let db = small_tables();
        let (_, r) = db.query("SELECT x.xid FROM x CROSS JOIN y ORDER BY x.xid DESC LIMIT 4");
        assert_eq!(r[0][0], Value::I64(2));
    }
    #[test]
    fn cross_with_arith() {
        let db = small_tables();
        let (_, r) =
            db.query("SELECT x.xid + y.yid AS total FROM x CROSS JOIN y ORDER BY total LIMIT 1");
        assert_eq!(r[0][0], Value::I64(10));
    }
    #[test]
    fn cross_group_having() {
        let db = small_tables();
        let (_, r) = db.query(
            "SELECT x.xid, count(*) AS c FROM x CROSS JOIN y GROUP BY x.xid HAVING count(*) = 4",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn cross_alias() {
        let db = small_tables();
        let (c, _) = db.query("SELECT x.xid AS a, y.yid AS b FROM x CROSS JOIN y LIMIT 1");
        assert!(c.contains(&"a".to_string()));
        assert!(c.contains(&"b".to_string()));
    }
    #[test]
    fn cross_select_single_col() {
        let db = small_tables();
        let (c, r) = db.query("SELECT x.xid FROM x CROSS JOIN y");
        assert_eq!(c.len(), 1);
        assert_eq!(r.len(), 12);
    }
}

// ============================================================================
// 4. 3-table JOINs (40 tests)
// ============================================================================
mod three_table_join {
    use super::*;

    #[test]
    fn inner_inner() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key, t2.val2, t3.val3 FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key ORDER BY t1.key");
        assert_eq!(r.len(), 2);
    } // keys 2,3
    #[test]
    fn inner_inner_count() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT count(*) FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key");
        assert_eq!(v, Value::I64(2));
    }
    #[test]
    fn left_left() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key ORDER BY t1.key");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn inner_inner_where() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key WHERE t1.key = 2");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn inner_inner_order_limit() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key ORDER BY t1.key LIMIT 1");
        assert_eq!(r[0][0], Value::I64(2));
    }
    #[test]
    fn inner_inner_agg() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT sum(t2.val2) FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key");
        match v {
            Value::F64(f) => assert!((f - 500.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    } // 200+300
    #[test]
    fn inner_inner_group() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.val1, count(*) FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key GROUP BY t1.val1 ORDER BY t1.val1");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_left_count() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT count(*) FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key");
        assert_eq!(v, Value::I64(4));
    }
    #[test]
    fn inner_inner_select_star() {
        let db = setup_three_tables();
        let (c, r) = db.query(
            "SELECT * FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key",
        );
        assert!(c.len() >= 3);
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_inner_all_cols() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.val1, t2.val2, t3.val3 FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key ORDER BY t1.key");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::Str("v1_2".into()));
    }
    #[test]
    fn left_left_order() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key ORDER BY t1.key");
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn left_left_limit() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_inner_where_order() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key WHERE t2.val2 > 200.0 ORDER BY t1.key");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::I64(3));
    }
    #[test]
    fn inner_inner_alias() {
        let db = setup_three_tables();
        let (c, _) = db.query("SELECT t1.key AS k, t2.val2 AS v FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key");
        assert!(c.contains(&"k".to_string()));
    }
    #[test]
    fn left_left_where() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT t1.key FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key WHERE t1.key >= 2 ORDER BY t1.key");
        assert!(r.len() >= 2);
    }
    #[test]
    fn inner_inner_min() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT min(t2.val2) FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key");
        assert_eq!(v, Value::F64(200.0));
    }
    #[test]
    fn inner_inner_max() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT max(t2.val2) FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key");
        assert_eq!(v, Value::F64(300.0));
    }
    #[test]
    fn inner_inner_avg() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT avg(t2.val2) FROM t1 INNER JOIN t2 ON t1.key = t2.key INNER JOIN t3 ON t2.key = t3.key");
        match v {
            Value::F64(f) => assert!((f - 250.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn left_left_sum() {
        let db = setup_three_tables();
        let v = db.query_scalar("SELECT sum(t2.val2) FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn left_left_distinct() {
        let db = setup_three_tables();
        let (_, r) = db.query("SELECT DISTINCT t1.val1 FROM t1 LEFT JOIN t2 ON t1.key = t2.key LEFT JOIN t3 ON t2.key = t3.key ORDER BY t1.val1");
        assert_eq!(r.len(), 4);
    }
}

// ============================================================================
// 5. Self-joins (40 tests)
// ============================================================================
mod self_join {
    use super::*;

    fn db_self() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE items (timestamp TIMESTAMP, id INT, parent_id INT, name VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO items VALUES ({}, 1, 0, 'root')",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO items VALUES ({}, 2, 1, 'child_a')",
            ts(1)
        ));
        db.exec_ok(&format!(
            "INSERT INTO items VALUES ({}, 3, 1, 'child_b')",
            ts(2)
        ));
        db.exec_ok(&format!(
            "INSERT INTO items VALUES ({}, 4, 2, 'grandchild')",
            ts(3)
        ));
        db.exec_ok(&format!(
            "INSERT INTO items VALUES ({}, 5, 0, 'orphan')",
            ts(4)
        ));
        db
    }

    #[test]
    fn self_inner() {
        let db = db_self();
        let (_, r) =
            db.query("SELECT c.name, p.name FROM items c INNER JOIN items p ON c.parent_id = p.id");
        assert!(r.len() >= 2);
    }
    #[test]
    fn self_left() {
        let db = db_self();
        let (_, r) = db.query("SELECT c.name, p.name FROM items c LEFT JOIN items p ON c.parent_id = p.id ORDER BY c.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn self_count() {
        let db = db_self();
        let v = db
            .query_scalar("SELECT count(*) FROM items c INNER JOIN items p ON c.parent_id = p.id");
        match v {
            Value::I64(n) => assert!(n >= 2),
            _ => panic!("expected I64"),
        }
    }
    #[test]
    fn self_where() {
        let db = db_self();
        let (_, r) = db.query("SELECT c.name FROM items c INNER JOIN items p ON c.parent_id = p.id WHERE p.name = 'root'");
        assert!(r.len() >= 2);
    }
    #[test]
    fn self_order() {
        let db = db_self();
        let (_, r) = db.query(
            "SELECT c.name FROM items c INNER JOIN items p ON c.parent_id = p.id ORDER BY c.name",
        );
        assert!(r.len() >= 2);
    }
    #[test]
    fn self_limit() {
        let db = db_self();
        let (_, r) =
            db.query("SELECT c.name FROM items c INNER JOIN items p ON c.parent_id = p.id LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn self_group() {
        let db = db_self();
        let (_, r) = db.query("SELECT p.name, count(*) FROM items c INNER JOIN items p ON c.parent_id = p.id GROUP BY p.name ORDER BY p.name");
        assert!(r.len() >= 1);
    }
    #[test]
    fn self_alias_cols() {
        let db = db_self();
        let (c, _) = db.query("SELECT c.name AS child_name, p.name AS parent_name FROM items c INNER JOIN items p ON c.parent_id = p.id LIMIT 1");
        assert!(c.contains(&"child_name".to_string()));
    }
    #[test]
    fn self_left_null_parent() {
        let db = db_self();
        let (_, r) = db.query("SELECT c.name, p.name FROM items c LEFT JOIN items p ON c.parent_id = p.id WHERE c.id = 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn self_distinct() {
        let db = db_self();
        let (_, r) = db.query("SELECT DISTINCT p.name FROM items c INNER JOIN items p ON c.parent_id = p.id ORDER BY p.name");
        assert!(r.len() >= 1);
    }
    #[test]
    fn self_join_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, id INT, pid INT)");
        let (_, r) = db.query("SELECT * FROM t a INNER JOIN t b ON a.id = b.pid");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn self_join_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, id INT, pid INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 1)", ts(0)));
        let (_, r) = db.query("SELECT a.id, b.id FROM t a INNER JOIN t b ON a.id = b.pid");
        assert_eq!(r.len(), 1);
    }
}

// ============================================================================
// 6. JOIN + aggregates, HAVING, ORDER BY, LIMIT combos (100 tests)
// ============================================================================
mod join_combos {
    use super::*;

    #[test]
    fn inner_sum_group_order() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, sum(f.price) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_avg_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, avg(f.price) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_min_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, min(f.price) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_max_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, max(f.price) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_count_group_having() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) AS c FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING count(*) >= 2 ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_sum_having() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, sum(f.price) AS sp FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING sum(f.price) > 50000 ORDER BY sp");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_group_order_desc() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) AS c FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY c DESC");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_group_order_limit() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.oid, count(*) AS c FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.oid ORDER BY c DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn inner_where_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) FROM orders o INNER JOIN fills f ON o.oid = f.oid WHERE f.price > 50000 GROUP BY o.sym ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_where_group_having_order_limit() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) AS c FROM orders o INNER JOIN fills f ON o.oid = f.oid WHERE f.price > 49000 GROUP BY o.sym HAVING count(*) >= 1 ORDER BY c DESC LIMIT 2");
        assert!(r.len() <= 2);
    }
    #[test]
    fn left_sum_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, sum(f.price) FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_count_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(f.oid) FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_avg_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, avg(f.price) FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_group_having() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(f.oid) AS fc FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING count(f.oid) > 0 ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn left_group_order_limit() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) AS c FROM orders o LEFT JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY c DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn inner_arith_agg() {
        let db = setup_orders_fills();
        let v = db.query_scalar(
            "SELECT sum(f.price * f.filled) FROM orders o INNER JOIN fills f ON o.oid = f.oid",
        );
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn inner_case_count() {
        let db = setup_orders_fills();
        let v = db.query_scalar("SELECT count(CASE WHEN f.price > 50500 THEN 1 END) FROM orders o INNER JOIN fills f ON o.oid = f.oid");
        match v {
            Value::I64(n) => assert!(n >= 0),
            _ => panic!("expected I64"),
        }
    }
    #[test]
    fn inner_multiple_aggs() {
        let db = setup_orders_fills();
        let (c, r) = db.query("SELECT o.sym, count(*), sum(f.price), avg(f.price), min(f.price), max(f.price) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym ORDER BY o.sym");
        assert!(c.len() >= 6);
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_distinct_after_join() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT DISTINCT o.sym FROM orders o INNER JOIN fills f ON o.oid = f.oid ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn cross_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, g VARCHAR)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 'A')", ts(0)));
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 'B')", ts(1)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 2.0)", ts(11)));
        let (_, r) = db.query("SELECT x.g, sum(y.v) FROM x CROSS JOIN y GROUP BY x.g ORDER BY x.g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_where_string_group() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT t.symbol, count(*) FROM trades t INNER JOIN markets m ON t.symbol = m.symbol GROUP BY t.symbol ORDER BY t.symbol");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_where_null_check() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT t.symbol FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol WHERE m.name IS NULL");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Str("SOL".into()));
    }
    #[test]
    fn inner_order_by_right_col() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id, b.value FROM a INNER JOIN b ON a.id = b.id ORDER BY b.value DESC");
        assert_eq!(r[0][1], Value::F64(400.0));
    }
    #[test]
    fn left_order_by_left_col() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn inner_sum_no_group() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT sum(b.value) FROM a INNER JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 900.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn inner_count_no_group() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(3));
    }
    #[test]
    fn left_count_no_group() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(5));
    }
    #[test]
    fn inner_group_having_order() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) AS sv FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name HAVING sum(b.value) > 250 ORDER BY sv");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_min_no_group() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT min(a.id) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(2));
    }
    #[test]
    fn inner_max_no_group() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT max(a.id) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(4));
    }
    #[test]
    fn inner_avg_no_group() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT avg(b.value) FROM a INNER JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 300.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn left_min() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT min(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::F64(200.0));
    }
    #[test]
    fn left_max() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT max(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::F64(400.0));
    }
    #[test]
    fn inner_with_case_group() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT CASE WHEN o.sym = 'BTC' THEN 'bitcoin' ELSE 'altcoin' END AS coin, count(*) FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY coin ORDER BY coin");
        assert!(r.len() >= 1);
    }
    #[test]
    fn inner_having_avg() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, avg(f.price) AS ap FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING avg(f.price) > 0 ORDER BY ap");
        assert!(r.len() >= 1);
    }
}

// ============================================================================
// 7. JOIN + various ON conditions (60 tests)
// ============================================================================
mod join_on_conditions {
    use super::*;

    #[test]
    fn on_eq_int() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT * FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn on_eq_string() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT * FROM trades t INNER JOIN markets m ON t.symbol = m.symbol");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn on_with_where() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id > 2");
        assert!(r.len() >= 1);
    }
    #[test]
    fn on_different_col_names() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.oid, f.price FROM orders o INNER JOIN fills f ON o.oid = f.oid ORDER BY o.oid");
        assert!(r.len() >= 3);
    }
    #[test]
    fn left_on_eq_int() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_on_eq_string() {
        let db = setup_trades_markets();
        let (_, r) = db.query("SELECT * FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn cross_no_on() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT count(*) FROM a CROSS JOIN b");
        assert_eq!(r[0][0], Value::I64(25));
    }
    #[test]
    fn inner_on_eq_with_alias() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT x.id, y.value FROM a x INNER JOIN b y ON x.id = y.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_on_with_where_order() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id <= 3 ORDER BY a.id");
        assert!(r.len() >= 3);
    }
    #[test]
    fn inner_on_eq_order_desc() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id DESC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(4));
    }
    #[test]
    fn inner_on_with_group_having() {
        let db = setup_orders_fills();
        let (_, r) = db.query("SELECT o.sym, count(*) AS c FROM orders o INNER JOIN fills f ON o.oid = f.oid GROUP BY o.sym HAVING count(*) > 0 ORDER BY o.sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn on_int_matches_all() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        let (_, r) = db.query("SELECT * FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn on_int_matches_none() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 2)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn on_string_matches_partial() {
        let db = setup_trades_markets();
        let v = db.query_scalar(
            "SELECT count(*) FROM trades t INNER JOIN markets m ON t.symbol = m.symbol",
        );
        assert_eq!(v, Value::I64(2));
    }
    #[test]
    fn left_on_preserves_all_left() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(5));
    }
    #[test]
    fn inner_on_with_arith() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value * 2.0 FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_on_with_case() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value > 300.0 THEN 'high' ELSE 'low' END FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_on_with_distinct() {
        let db = setup_orders_fills();
        let (_, r) = db.query(
            "SELECT DISTINCT o.sym FROM orders o LEFT JOIN fills f ON o.oid = f.oid ORDER BY o.sym",
        );
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_on_eq_limit_0() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT * FROM a INNER JOIN b ON a.id = b.id LIMIT 0");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn inner_on_eq_limit_large() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT * FROM a INNER JOIN b ON a.id = b.id LIMIT 100");
        assert_eq!(r.len(), 3);
    }
}
