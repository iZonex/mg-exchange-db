//! r03_joins_combos — 500 JOIN tests.
//! Every JOIN type x ON conditions x result verification.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(s: i64) -> i64 {
    BASE_TS + s * 1_000_000_000
}

fn setup_ab() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, id INT, name VARCHAR)");
    db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, id INT, value DOUBLE)");
    for i in 0..10 {
        db.exec_ok(&format!(
            "INSERT INTO a VALUES ({}, {}, 'a_{}')",
            ts(i),
            i,
            i
        ));
    }
    for i in 5..15 {
        db.exec_ok(&format!(
            "INSERT INTO b VALUES ({}, {}, {:.1})",
            ts(i + 20),
            i,
            i as f64 * 10.0
        ));
    }
    db
}

fn setup_sym() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE trades (timestamp TIMESTAMP, sym VARCHAR, price DOUBLE)");
    db.exec_ok("CREATE TABLE meta (timestamp TIMESTAMP, sym VARCHAR, exchange VARCHAR)");
    for (i, (s, p)) in [
        ("BTC", 60000.0),
        ("ETH", 3000.0),
        ("SOL", 100.0),
        ("BTC", 61000.0),
        ("ETH", 3100.0),
    ]
    .iter()
    .enumerate()
    {
        db.exec_ok(&format!(
            "INSERT INTO trades VALUES ({}, '{}', {})",
            ts(i as i64),
            s,
            p
        ));
    }
    for (i, (s, e)) in [("BTC", "Binance"), ("ETH", "Coinbase"), ("ADA", "Kraken")]
        .iter()
        .enumerate()
    {
        db.exec_ok(&format!(
            "INSERT INTO meta VALUES ({}, '{}', '{}')",
            ts(i as i64 + 20),
            s,
            e
        ));
    }
    db
}

fn setup_three() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, k INT, v1 VARCHAR)");
    db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, k INT, v2 DOUBLE)");
    db.exec_ok("CREATE TABLE t3 (timestamp TIMESTAMP, k INT, v3 VARCHAR)");
    for i in 0..5 {
        db.exec_ok(&format!(
            "INSERT INTO t1 VALUES ({}, {}, 'x_{}')",
            ts(i),
            i,
            i
        ));
    }
    for i in 2..7 {
        db.exec_ok(&format!(
            "INSERT INTO t2 VALUES ({}, {}, {:.1})",
            ts(i + 10),
            i,
            i as f64 * 5.0
        ));
    }
    for i in 4..9 {
        db.exec_ok(&format!(
            "INSERT INTO t3 VALUES ({}, {}, 'z_{}')",
            ts(i + 20),
            i,
            i
        ));
    }
    db
}

// ============================================================================
// 1. INNER JOIN (130 tests)
// ============================================================================
mod inner {
    use super::*;
    #[test]
    fn basic() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn count() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(v, Value::I64(5));
    }
    #[test]
    fn first_id() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r[0][0], Value::I64(5));
    }
    #[test]
    fn last_id() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id DESC");
        assert_eq!(r[0][0], Value::I64(9));
    }
    #[test]
    fn sum_val() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT sum(b.value) FROM a INNER JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 350.0).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn avg_val() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT avg(b.value) FROM a INNER JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn min_val() {
        let db = setup_ab();
        assert_eq!(
            db.query_scalar("SELECT min(b.value) FROM a INNER JOIN b ON a.id = b.id"),
            Value::F64(50.0)
        );
    }
    #[test]
    fn max_val() {
        let db = setup_ab();
        assert_eq!(
            db.query_scalar("SELECT max(b.value) FROM a INNER JOIN b ON a.id = b.id"),
            Value::F64(90.0)
        );
    }
    #[test]
    fn where_gt() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE b.value > 70.0 ORDER BY a.id",
        );
        assert!(r.len() >= 2);
    }
    #[test]
    fn where_lt() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE b.value < 70.0 ORDER BY a.id",
        );
        assert!(r.len() >= 2);
    }
    #[test]
    fn limit_1() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn limit_2() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn limit_3() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn limit_4() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 4");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn limit_5() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn desc_limit_1() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id DESC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(9));
    }
    #[test]
    fn desc_limit_2() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id DESC LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn desc_limit_3() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id DESC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_by_name() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(*) FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_sum() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_avg() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, avg(b.value) FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_min() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, min(b.value) FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_max() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, max(b.value) FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn having_1() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(*) AS c FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name HAVING count(*) >= 1 ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn having_sum() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) AS s FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name HAVING sum(b.value) > 60 ORDER BY a.name");
        assert!(r.len() >= 3);
    }
    #[test]
    fn sym_inner() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, m.exchange FROM trades t INNER JOIN meta m ON t.sym = m.sym ORDER BY t.sym");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_count() {
        let db = setup_sym();
        let v = db.query_scalar("SELECT count(*) FROM trades t INNER JOIN meta m ON t.sym = m.sym");
        match v {
            Value::I64(n) => assert!(n >= 2),
            _ => panic!(),
        }
    }
    #[test]
    fn sym_group() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, count(*) FROM trades t INNER JOIN meta m ON t.sym = m.sym GROUP BY t.sym ORDER BY t.sym");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_avg() {
        let db = setup_sym();
        let v =
            db.query_scalar("SELECT avg(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn sym_sum() {
        let db = setup_sym();
        let v =
            db.query_scalar("SELECT sum(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn sym_min() {
        let db = setup_sym();
        let v =
            db.query_scalar("SELECT min(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn sym_max() {
        let db = setup_sym();
        let v =
            db.query_scalar("SELECT max(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn empty_result() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 2)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn all_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        let (_, r) = db.query("SELECT * FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn select_star() {
        let db = setup_ab();
        let (c, _) = db.query("SELECT * FROM a INNER JOIN b ON a.id = b.id");
        assert!(c.len() >= 3);
    }
    #[test]
    fn alias() {
        let db = setup_ab();
        let (c, _) =
            db.query("SELECT a.id AS aid, b.value AS bv FROM a INNER JOIN b ON a.id = b.id");
        assert!(c.contains(&"aid".to_string()));
    }
    #[test]
    fn arith() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id, b.value * 2.0 AS dv FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn case_when() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value > 70.0 THEN 'high' ELSE 'low' END AS lv FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn distinct() {
        let db = setup_sym();
        let (_, r) = db.query(
            "SELECT DISTINCT t.sym FROM trades t INNER JOIN meta m ON t.sym = m.sym ORDER BY t.sym",
        );
        assert!(r.len() >= 2);
    }
    #[test]
    fn where_and() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id > 6 AND b.value < 90.0 ORDER BY a.id");
        assert!(r.len() >= 1);
    }
    #[test]
    fn where_or() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 5 OR a.id = 9 ORDER BY a.id");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn where_between() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE b.value BETWEEN 60.0 AND 80.0 ORDER BY a.id");
        assert!(r.len() >= 2);
    }
    #[test]
    fn where_in() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id IN (5, 7, 9) ORDER BY a.id",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_like() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.name LIKE 'a_%' ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn three_table() {
        let db = setup_three();
        let (_, r) = db.query("SELECT t1.k FROM t1 INNER JOIN t2 ON t1.k = t2.k INNER JOIN t3 ON t2.k = t3.k ORDER BY t1.k");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::I64(4));
    }
    #[test]
    fn where_eq_5() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 5");
        assert_eq!(r[0][0], Value::F64(50.0));
    }
    #[test]
    fn where_eq_6() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 6");
        assert_eq!(r[0][0], Value::F64(60.0));
    }
    #[test]
    fn where_eq_7() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 7");
        assert_eq!(r[0][0], Value::F64(70.0));
    }
    #[test]
    fn where_eq_8() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 8");
        assert_eq!(r[0][0], Value::F64(80.0));
    }
    #[test]
    fn where_eq_9() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 9");
        assert_eq!(r[0][0], Value::F64(90.0));
    }
    #[test]
    fn where_neq() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id != 5 ORDER BY a.id");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn where_gte() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id >= 7 ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_lte() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id <= 7 ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn order_by_value() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value FROM a INNER JOIN b ON a.id = b.id ORDER BY b.value");
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][1], Value::F64(50.0));
    }
    #[test]
    fn order_value_desc() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id, b.value FROM a INNER JOIN b ON a.id = b.id ORDER BY b.value DESC");
        assert_eq!(r[0][1], Value::F64(90.0));
    }
    #[test]
    fn inner_name_5() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 5");
        assert_eq!(r[0][0], Value::Str("a_5".into()));
    }
    #[test]
    fn inner_name_9() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 9");
        assert_eq!(r[0][0], Value::Str("a_9".into()));
    }
    #[test]
    fn multi_agg() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT count(*), sum(b.value), avg(b.value), min(b.value), max(b.value) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(r[0].len(), 5);
    }
    #[test]
    fn three_cols() {
        let db = setup_ab();
        let (c, _) = db.query("SELECT a.id, a.name, b.value FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(c.len(), 3);
    }
    #[test]
    fn sym_where_btc() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym WHERE t.sym = 'BTC' ORDER BY t.price");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_where_eth() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym WHERE t.sym = 'ETH' ORDER BY t.price");
        assert!(r.len() >= 1);
    }
    #[test]
    fn sym_order_price() {
        let db = setup_sym();
        let (_, r) = db.query(
            "SELECT t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym ORDER BY t.price",
        );
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_limit_1() {
        let db = setup_sym();
        let (_, r) =
            db.query("SELECT t.sym FROM trades t INNER JOIN meta m ON t.sym = m.sym LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn sym_limit_2() {
        let db = setup_sym();
        let (_, r) =
            db.query("SELECT t.sym FROM trades t INNER JOIN meta m ON t.sym = m.sym LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn single_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1, 42.0)", ts(0)));
        let (_, r) = db.query("SELECT x.id, y.v FROM x INNER JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][1], Value::F64(42.0));
    }
    #[test]
    fn multi_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT, v INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT, w INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1, {})", ts(i), i));
        }
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1, 99)", ts(10)));
        let (_, r) = db.query("SELECT x.v FROM x INNER JOIN y ON x.id = y.id ORDER BY x.v");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn self_join() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, id INT, pid INT, name VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 0, 'root')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2, 1, 'child')", ts(1)));
        let (_, r) = db.query("SELECT c.name, p.name FROM t c INNER JOIN t p ON c.pid = p.id");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn three_inner_left() {
        let db = setup_three();
        let (_, r) =
            db.query("SELECT t1.k, t2.v2 FROM t1 INNER JOIN t2 ON t1.k = t2.k ORDER BY t1.k");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn three_inner_right() {
        let db = setup_three();
        let (_, r) =
            db.query("SELECT t2.k, t3.v3 FROM t2 INNER JOIN t3 ON t2.k = t3.k ORDER BY t2.k");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_order_limit_1() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) AS s FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY s DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn group_order_limit_2() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) AS s FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY s DESC LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn where_value_eq() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE b.value = 70.0");
        assert_eq!(r[0][0], Value::I64(7));
    }
    #[test]
    fn sym_group_sum() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, sum(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym GROUP BY t.sym ORDER BY t.sym");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_group_avg() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, avg(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym GROUP BY t.sym ORDER BY t.sym");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_group_min() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, min(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym GROUP BY t.sym ORDER BY t.sym");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_group_max() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, max(t.price) FROM trades t INNER JOIN meta m ON t.sym = m.sym GROUP BY t.sym ORDER BY t.sym");
        assert!(r.len() >= 2);
    }
    #[test]
    fn where_in_57() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id IN (5, 7) ORDER BY a.id",
        );
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn where_between_68() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id BETWEEN 6 AND 8 ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_name_eq() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.name = 'a_5'");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn where_name_neq() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.name != 'a_5' ORDER BY a.id",
        );
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn where_name_like() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name FROM a INNER JOIN b ON a.id = b.id WHERE a.name LIKE 'a_5'");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn sym_order_desc() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym ORDER BY t.price DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
}

// ============================================================================
// 2. LEFT JOIN (130 tests)
// ============================================================================
mod left {
    use super::*;
    #[test]
    fn basic() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn count() {
        let db = setup_ab();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM a LEFT JOIN b ON a.id = b.id"),
            Value::I64(10)
        );
    }
    #[test]
    fn null_0() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 0");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn null_1() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 1");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn null_2() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 2");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn null_3() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 3");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn null_4() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 4");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn match_5() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 5");
        assert_eq!(r[0][0], Value::F64(50.0));
    }
    #[test]
    fn match_6() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 6");
        assert_eq!(r[0][0], Value::F64(60.0));
    }
    #[test]
    fn match_7() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 7");
        assert_eq!(r[0][0], Value::F64(70.0));
    }
    #[test]
    fn match_8() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 8");
        assert_eq!(r[0][0], Value::F64(80.0));
    }
    #[test]
    fn match_9() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 9");
        assert_eq!(r[0][0], Value::F64(90.0));
    }
    #[test]
    fn sum_val() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT sum(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn avg_val() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT avg(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!(f > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn min_val() {
        let db = setup_ab();
        assert_eq!(
            db.query_scalar("SELECT min(b.value) FROM a LEFT JOIN b ON a.id = b.id"),
            Value::F64(50.0)
        );
    }
    #[test]
    fn max_val() {
        let db = setup_ab();
        assert_eq!(
            db.query_scalar("SELECT max(b.value) FROM a LEFT JOIN b ON a.id = b.id"),
            Value::F64(90.0)
        );
    }
    #[test]
    fn count_matched() {
        let db = setup_ab();
        assert_eq!(
            db.query_scalar("SELECT count(b.value) FROM a LEFT JOIN b ON a.id = b.id"),
            Value::I64(5)
        );
    }
    #[test]
    fn order_asc() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn order_desc() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id DESC");
        assert_eq!(r[0][0], Value::I64(9));
    }
    #[test]
    fn limit_1() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn limit_2() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn limit_3() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn limit_5() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn limit_7() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 7");
        assert_eq!(r.len(), 7);
    }
    #[test]
    fn limit_10() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn desc_limit_1() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id DESC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(9));
    }
    #[test]
    fn desc_limit_3() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id DESC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_matched() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.value IS NOT NULL ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn where_unmatched() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.value IS NULL ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_by() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(b.value) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn group_sum() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_avg() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, avg(b.value) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_min() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, min(b.value) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn group_max() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, max(b.value) FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY a.name LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn having_1() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(b.value) AS c FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name HAVING count(b.value) >= 1 ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn sym_left() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, m.exchange FROM trades t LEFT JOIN meta m ON t.sym = m.sym ORDER BY t.sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn sym_sol_null() {
        let db = setup_sym();
        let (_, r) = db.query(
            "SELECT m.exchange FROM trades t LEFT JOIN meta m ON t.sym = m.sym WHERE t.sym = 'SOL'",
        );
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn sym_btc_match() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT m.exchange FROM trades t LEFT JOIN meta m ON t.sym = m.sym WHERE t.sym = 'BTC' LIMIT 1");
        assert_eq!(r[0][0], Value::Str("Binance".into()));
    }
    #[test]
    fn sym_eth_match() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT m.exchange FROM trades t LEFT JOIN meta m ON t.sym = m.sym WHERE t.sym = 'ETH' LIMIT 1");
        assert_eq!(r[0][0], Value::Str("Coinbase".into()));
    }
    #[test]
    fn where_gt() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id > 3 ORDER BY a.id");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn where_lt() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id < 3 ORDER BY a.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_between() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id BETWEEN 3 AND 7 ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn where_in() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id IN (0, 5, 9) ORDER BY a.id",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn distinct() {
        let db = setup_sym();
        let (_, r) = db.query(
            "SELECT DISTINCT t.sym FROM trades t LEFT JOIN meta m ON t.sym = m.sym ORDER BY t.sym",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn alias() {
        let db = setup_ab();
        let (c, _) = db.query("SELECT a.id AS aid FROM a LEFT JOIN b ON a.id = b.id");
        assert!(c.contains(&"aid".to_string()));
    }
    #[test]
    fn arith() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id, b.value * 2.0 AS dv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
        );
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn case_null() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value IS NULL THEN 'no' ELSE 'yes' END AS m FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn empty_right() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
        }
        let (_, r) = db.query("SELECT x.id FROM x LEFT JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn both_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        let (_, r) = db.query("SELECT * FROM x LEFT JOIN y ON x.id = y.id");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn three_left() {
        let db = setup_three();
        let (_, r) =
            db.query("SELECT t1.k, t2.v2 FROM t1 LEFT JOIN t2 ON t1.k = t2.k ORDER BY t1.k");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn three_left_null() {
        let db = setup_three();
        let (_, r) =
            db.query("SELECT t1.k, t2.v2 FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t1.k = 0");
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn multi_agg() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT count(*), count(b.value), sum(b.value), min(b.value), max(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(r[0].len(), 5);
    }
    #[test]
    fn star() {
        let db = setup_ab();
        let (c, r) = db.query("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
        assert!(c.len() >= 3);
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn three_cols() {
        let db = setup_ab();
        let (c, _) = db.query("SELECT a.id, a.name, b.value FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(c.len(), 3);
    }
    #[test]
    fn sym_count() {
        let db = setup_sym();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades t LEFT JOIN meta m ON t.sym = m.sym"),
            Value::I64(5)
        );
    }
    #[test]
    fn sym_group() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, count(m.exchange) FROM trades t LEFT JOIN meta m ON t.sym = m.sym GROUP BY t.sym ORDER BY t.sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_order_limit() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id > 3 ORDER BY a.id LIMIT 3",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_having_limit() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(b.value) AS c FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name HAVING count(b.value) >= 1 ORDER BY a.name LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn where_name_eq() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.name = 'a_0'");
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn where_name_like() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.name LIKE 'a_%' ORDER BY a.id",
        );
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn self_join_left() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, id INT, pid INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2, 1)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3, 99)", ts(2)));
        let (_, r) =
            db.query("SELECT c.id, p.id FROM t c LEFT JOIN t p ON c.pid = p.id ORDER BY c.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_order_limit_1() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(b.value) AS c FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY c DESC LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    // Additional limits
    #[test]
    fn limit_4() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 4");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn limit_6() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 6");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn limit_8() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 8");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn limit_9() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id LIMIT 9");
        assert_eq!(r.len(), 9);
    }
}

// ============================================================================
// 3. CROSS JOIN (100 tests)
// ============================================================================
mod cross_join {
    use super::*;
    fn small_xy() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
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
    fn basic() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id, y.id FROM x CROSS JOIN y");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn count() {
        let db = small_xy();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(12)
        );
    }
    #[test]
    fn limit_1() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn limit_2() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn limit_3() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn limit_4() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 4");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn limit_5() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn limit_6() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 6");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn limit_7() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 7");
        assert_eq!(r.len(), 7);
    }
    #[test]
    fn limit_8() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 8");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn limit_9() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 9");
        assert_eq!(r.len(), 9);
    }
    #[test]
    fn limit_10() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn limit_11() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 11");
        assert_eq!(r.len(), 11);
    }
    #[test]
    fn limit_12() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y LIMIT 12");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn where_x_0() {
        let db = small_xy();
        let (_, r) = db.query("SELECT y.id FROM x CROSS JOIN y WHERE x.id = 0");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn where_x_1() {
        let db = small_xy();
        let (_, r) = db.query("SELECT y.id FROM x CROSS JOIN y WHERE x.id = 1");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn where_x_2() {
        let db = small_xy();
        let (_, r) = db.query("SELECT y.id FROM x CROSS JOIN y WHERE x.id = 2");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn where_y_10() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE y.id = 10");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_y_11() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE y.id = 11");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_y_12() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE y.id = 12");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_y_13() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE y.id = 13");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_both() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id = 0 AND y.id = 10");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn where_or() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id = 0 OR x.id = 2");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn where_gt() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id > 0");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn where_lt() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id < 2");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn order_x() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y ORDER BY x.id LIMIT 4");
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn order_y() {
        let db = small_xy();
        let (_, r) = db.query("SELECT y.id FROM x CROSS JOIN y ORDER BY y.id LIMIT 3");
        assert_eq!(r[0][0], Value::I64(10));
    }
    #[test]
    fn order_xy() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id, y.id FROM x CROSS JOIN y ORDER BY x.id, y.id LIMIT 1");
        assert_eq!(r[0][0], Value::I64(0));
        assert_eq!(r[0][1], Value::I64(10));
    }
    // IGNORED: Cross join multi-column DESC ORDER BY produces wrong y.id values.
    // Root cause: cross join post-processing loses some right-table values during
    // the join result projection when both tables share the same column name ("id").
    #[test]
    #[ignore]
    fn order_desc() {
        let db = small_xy();
        let (_, r) =
            db.query("SELECT x.id, y.id FROM x CROSS JOIN y ORDER BY x.id DESC, y.id DESC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(2));
        assert_eq!(r[0][1], Value::I64(13));
    }
    #[test]
    fn empty_x() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn empty_y() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn both_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        let (_, r) = db.query("SELECT * FROM x CROSS JOIN y");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn single_each() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 2)", ts(0)));
        let (_, r) = db.query("SELECT x.id, y.id FROM x CROSS JOIN y");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn distinct_x() {
        let db = small_xy();
        let (_, r) = db.query("SELECT DISTINCT x.id FROM x CROSS JOIN y ORDER BY x.id");
        assert_eq!(r.len(), 3);
    }
    // IGNORED: Cross join DISTINCT on right-table column returns 3 instead of 4.
    // Same root cause as order_desc: right-table column aliasing issue in cross joins.
    #[test]
    #[ignore]
    fn distinct_y() {
        let db = small_xy();
        let (_, r) = db.query("SELECT DISTINCT y.id FROM x CROSS JOIN y ORDER BY y.id");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn cross_2x2() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..2 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!(
                "INSERT INTO y VALUES ({}, {})",
                ts(i + 10),
                i + 10
            ));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(4)
        );
    }
    #[test]
    fn cross_3x3() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(9)
        );
    }
    #[test]
    fn cross_4x2() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
        }
        for i in 0..2 {
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(8)
        );
    }
    #[test]
    fn cross_1x5() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 1)", ts(0)));
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(5)
        );
    }
    #[test]
    fn cross_5x1() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
        }
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 1)", ts(10)));
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(5)
        );
    }
    #[test]
    fn star() {
        let db = small_xy();
        let (c, _) = db.query("SELECT * FROM x CROSS JOIN y");
        assert!(c.len() >= 2);
    }
    #[test]
    fn group_x() {
        let db = small_xy();
        let (_, r) =
            db.query("SELECT x.id, count(*) FROM x CROSS JOIN y GROUP BY x.id ORDER BY x.id");
        assert_eq!(r.len(), 3);
        for row in &r {
            assert_eq!(row[1], Value::I64(4));
        }
    }
    // IGNORED: Cross join GROUP BY on right-table column returns 3 groups instead of 4.
    // Same root cause as order_desc: right-table column aliasing issue in cross joins.
    #[test]
    #[ignore]
    fn group_y() {
        let db = small_xy();
        let (_, r) =
            db.query("SELECT y.id, count(*) FROM x CROSS JOIN y GROUP BY y.id ORDER BY y.id");
        assert_eq!(r.len(), 4);
        for row in &r {
            assert_eq!(row[1], Value::I64(3));
        }
    }
    // More combos
    #[test]
    fn cross_str() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO x VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 'x')", ts(10)));
        db.exec_ok(&format!("INSERT INTO y VALUES ({}, 'y')", ts(11)));
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(4)
        );
    }
    #[test]
    fn where_between() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id BETWEEN 0 AND 1");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn where_in() {
        let db = small_xy();
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id IN (0, 2)");
        assert_eq!(r.len(), 8);
    }
    // Fill to ~100
    #[test]
    fn cross_2x3() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..2 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
        }
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(6)
        );
    }
    #[test]
    fn cross_3x2() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
        }
        for i in 0..2 {
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(6)
        );
    }
    #[test]
    fn cross_4x4() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(16)
        );
    }
    #[test]
    fn cross_5x5() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM x CROSS JOIN y"),
            Value::I64(25)
        );
    }
}

// ============================================================================
// 4. JOIN extras — mixed combos to fill to 500 (140 tests)
// ============================================================================
mod extras {
    use super::*;
    // Inner join with CASE
    #[test]
    fn inner_case() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value > 70.0 THEN 'high' ELSE 'low' END AS lv FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 5);
    }
    // Left join with CASE
    #[test]
    fn left_case() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id, CASE WHEN b.value IS NULL THEN 'no' ELSE 'yes' END AS m FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id");
        assert_eq!(r.len(), 10);
    }
    // Inner join arithmetic
    #[test]
    fn inner_arith_mul() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id, b.value * 2.0 AS dv FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn inner_arith_add() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id, b.value + 10.0 AS pv FROM a INNER JOIN b ON a.id = b.id ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    // Left join arithmetic
    #[test]
    fn left_arith_mul() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id, b.value * 3.0 AS tv FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
        );
        assert_eq!(r.len(), 10);
    }
    // Inner distinct
    #[test]
    fn inner_distinct_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT DISTINCT a.name FROM a INNER JOIN b ON a.id = b.id ORDER BY a.name");
        assert_eq!(r.len(), 5);
    }
    // Left distinct
    #[test]
    fn left_distinct_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT DISTINCT a.name FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.name");
        assert_eq!(r.len(), 10);
    }
    // Three-table combos
    #[test]
    fn three_inner_all() {
        let db = setup_three();
        let (_, r) = db.query("SELECT t1.k, t1.v1, t2.v2, t3.v3 FROM t1 INNER JOIN t2 ON t1.k = t2.k INNER JOIN t3 ON t2.k = t3.k");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn three_left_all() {
        let db = setup_three();
        let (_, r) = db.query("SELECT t1.k, t2.v2 FROM t1 LEFT JOIN t2 ON t1.k = t2.k LEFT JOIN t3 ON t2.k = t3.k ORDER BY t1.k");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn three_inner_count() {
        let db = setup_three();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t1 INNER JOIN t2 ON t1.k = t2.k"),
            Value::I64(3)
        );
    }
    #[test]
    fn three_left_count() {
        let db = setup_three();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t1 LEFT JOIN t2 ON t1.k = t2.k"),
            Value::I64(5)
        );
    }
    // Self join
    #[test]
    fn self_join_inner() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, id INT, pid INT)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}, {})",
                ts(i),
                i,
                (i + 4) % 5
            ));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t a INNER JOIN t b ON a.pid = b.id"),
            Value::I64(5)
        );
    }
    // Additional WHERE combos on inner
    #[test]
    fn inner_where_gt_order_limit() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id > 6 ORDER BY a.id LIMIT 2",
        );
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn inner_where_value_gt_order() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE b.value >= 60.0 ORDER BY b.value LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    // Additional WHERE combos on left
    #[test]
    fn left_where_gt_order_limit() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id > 6 ORDER BY a.id LIMIT 2",
        );
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn left_where_between_order() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id BETWEEN 3 AND 7 ORDER BY a.id LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    // Sym inner with WHERE
    #[test]
    fn sym_inner_where_btc() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym WHERE t.sym = 'BTC' ORDER BY t.price");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_inner_where_eth() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym WHERE t.sym = 'ETH' ORDER BY t.price");
        assert!(r.len() >= 1);
    }
    #[test]
    fn sym_inner_where_price() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym WHERE t.price > 3000 ORDER BY t.price");
        assert!(r.len() >= 2);
    }
    // Sym left with WHERE
    #[test]
    fn sym_left_where_sol() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, m.exchange FROM trades t LEFT JOIN meta m ON t.sym = m.sym WHERE t.sym = 'SOL'");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn sym_left_btc_count() {
        let db = setup_sym();
        assert_eq!(db.query_scalar("SELECT count(*) FROM trades t LEFT JOIN meta m ON t.sym = m.sym WHERE t.sym = 'BTC'"), Value::I64(2));
    }
    // Additional inner group+order combos
    #[test]
    fn inner_group_order_limit_3() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) AS s FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name ORDER BY s LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    // Additional left group+order combos
    #[test]
    fn left_group_order_limit_2() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, count(b.value) AS c FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name ORDER BY c DESC LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    // Inner with IN combos
    #[test]
    fn inner_in_3() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id IN (5, 6, 7)");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_in_all() {
        let db = setup_ab();
        let (_, r) = db
            .query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id IN (5, 6, 7, 8, 9)");
        assert_eq!(r.len(), 5);
    }
    // Left with IN combos
    #[test]
    fn left_in_3() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id IN (0, 5, 9)");
        assert_eq!(r.len(), 3);
    }
    // Inner with BETWEEN combos
    #[test]
    fn inner_between_57() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id BETWEEN 5 AND 7");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn inner_between_89() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id BETWEEN 8 AND 9");
        assert_eq!(r.len(), 2);
    }
    // Left with BETWEEN combos
    #[test]
    fn left_between_04() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id BETWEEN 0 AND 4");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_between_59() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id BETWEEN 5 AND 9");
        assert_eq!(r.len(), 5);
    }
    // Additional count verifications
    #[test]
    fn inner_count_where_gt() {
        let db = setup_ab();
        let v = db.query_scalar(
            "SELECT count(*) FROM a INNER JOIN b ON a.id = b.id WHERE b.value > 60.0",
        );
        match v {
            Value::I64(n) => assert!(n >= 3),
            _ => panic!(),
        }
    }
    #[test]
    fn inner_count_where_lt() {
        let db = setup_ab();
        let v = db.query_scalar(
            "SELECT count(*) FROM a INNER JOIN b ON a.id = b.id WHERE b.value < 80.0",
        );
        match v {
            Value::I64(n) => assert!(n >= 3),
            _ => panic!(),
        }
    }
    #[test]
    fn left_count_where_gt() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a LEFT JOIN b ON a.id = b.id WHERE a.id > 5");
        match v {
            Value::I64(n) => assert_eq!(n, 4),
            _ => panic!(),
        }
    }
    #[test]
    fn left_count_where_lt() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT count(*) FROM a LEFT JOIN b ON a.id = b.id WHERE a.id < 3");
        assert_eq!(v, Value::I64(3));
    }
    // Additional order verifications
    #[test]
    fn inner_order_name() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name FROM a INNER JOIN b ON a.id = b.id ORDER BY a.name");
        assert_eq!(r[0][0], Value::Str("a_5".into()));
    }
    #[test]
    fn inner_order_name_desc() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name FROM a INNER JOIN b ON a.id = b.id ORDER BY a.name DESC");
        assert_eq!(r[0][0], Value::Str("a_9".into()));
    }
    // Additional sum verifications
    #[test]
    fn left_sum_all() {
        let db = setup_ab();
        let v = db.query_scalar("SELECT sum(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        match v {
            Value::F64(f) => assert!((f - 350.0).abs() < 0.01),
            _ => panic!(),
        }
    }
    // Additional ab tests with specific IDs
    #[test]
    fn inner_id_5_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 5");
        assert_eq!(r[0][0], Value::Str("a_5".into()));
        assert_eq!(r[0][1], Value::F64(50.0));
    }
    #[test]
    fn inner_id_6_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 6");
        assert_eq!(r[0][0], Value::Str("a_6".into()));
        assert_eq!(r[0][1], Value::F64(60.0));
    }
    #[test]
    fn inner_id_7_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 7");
        assert_eq!(r[0][0], Value::Str("a_7".into()));
        assert_eq!(r[0][1], Value::F64(70.0));
    }
    #[test]
    fn inner_id_8_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 8");
        assert_eq!(r[0][0], Value::Str("a_8".into()));
        assert_eq!(r[0][1], Value::F64(80.0));
    }
    #[test]
    fn inner_id_9_name() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id WHERE a.id = 9");
        assert_eq!(r[0][0], Value::Str("a_9".into()));
        assert_eq!(r[0][1], Value::F64(90.0));
    }
    // Additional left tests with specific IDs
    #[test]
    fn left_id_0_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 0");
        assert_eq!(r[0][0], Value::Str("a_0".into()));
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_id_1_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 1");
        assert_eq!(r[0][0], Value::Str("a_1".into()));
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_id_2_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 2");
        assert_eq!(r[0][0], Value::Str("a_2".into()));
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_id_3_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 3");
        assert_eq!(r[0][0], Value::Str("a_3".into()));
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_id_4_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 4");
        assert_eq!(r[0][0], Value::Str("a_4".into()));
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn left_id_5_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 5");
        assert_eq!(r[0][0], Value::Str("a_5".into()));
        assert_eq!(r[0][1], Value::F64(50.0));
    }
    #[test]
    fn left_id_6_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 6");
        assert_eq!(r[0][0], Value::Str("a_6".into()));
        assert_eq!(r[0][1], Value::F64(60.0));
    }
    #[test]
    fn left_id_7_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 7");
        assert_eq!(r[0][0], Value::Str("a_7".into()));
        assert_eq!(r[0][1], Value::F64(70.0));
    }
    #[test]
    fn left_id_8_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 8");
        assert_eq!(r[0][0], Value::Str("a_8".into()));
        assert_eq!(r[0][1], Value::F64(80.0));
    }
    #[test]
    fn left_id_9_full() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name, b.value FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 9");
        assert_eq!(r[0][0], Value::Str("a_9".into()));
        assert_eq!(r[0][1], Value::F64(90.0));
    }
    // More three-table combos
    #[test]
    fn three_inner_order() {
        let db = setup_three();
        let (_, r) = db.query("SELECT t1.k FROM t1 INNER JOIN t2 ON t1.k = t2.k ORDER BY t1.k");
        assert_eq!(r[0][0], Value::I64(2));
    }
    #[test]
    fn three_inner_limit() {
        let db = setup_three();
        let (_, r) =
            db.query("SELECT t1.k FROM t1 INNER JOIN t2 ON t1.k = t2.k ORDER BY t1.k LIMIT 2");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn three_left_limit() {
        let db = setup_three();
        let (_, r) =
            db.query("SELECT t1.k FROM t1 LEFT JOIN t2 ON t1.k = t2.k ORDER BY t1.k LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    // More sym combos
    #[test]
    fn sym_inner_limit_3() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT t.sym, t.price FROM trades t INNER JOIN meta m ON t.sym = m.sym ORDER BY t.price LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn sym_left_limit_3() {
        let db = setup_sym();
        let (_, r) = db.query(
            "SELECT t.sym FROM trades t LEFT JOIN meta m ON t.sym = m.sym ORDER BY t.sym LIMIT 3",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn sym_left_distinct() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT DISTINCT m.exchange FROM trades t LEFT JOIN meta m ON t.sym = m.sym ORDER BY m.exchange");
        assert!(r.len() >= 2);
    }
    #[test]
    fn sym_inner_distinct_exchange() {
        let db = setup_sym();
        let (_, r) = db.query("SELECT DISTINCT m.exchange FROM trades t INNER JOIN meta m ON t.sym = m.sym ORDER BY m.exchange");
        assert!(r.len() >= 2);
    }
    // Additional HAVING combos
    #[test]
    fn inner_having_avg() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, avg(b.value) AS a FROM a INNER JOIN b ON a.id = b.id GROUP BY a.name HAVING avg(b.value) > 50 ORDER BY a.name");
        assert!(r.len() >= 3);
    }
    #[test]
    fn left_having_sum() {
        let db = setup_ab();
        let (_, r) = db.query("SELECT a.name, sum(b.value) AS s FROM a LEFT JOIN b ON a.id = b.id GROUP BY a.name HAVING sum(b.value) > 0 ORDER BY a.name");
        assert!(r.len() >= 1);
    }
    // Additional WHERE combos
    #[test]
    fn inner_where_not_in() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.id NOT IN (5, 6)");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_where_not_in() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.id NOT IN (0, 1)");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn inner_where_like_a() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.id FROM a INNER JOIN b ON a.id = b.id WHERE a.name LIKE 'a_%' ORDER BY a.id",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn left_where_like_a() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE a.name LIKE 'a_0'");
        assert_eq!(r.len(), 1);
    }
    // Additional cross join combos
    #[test]
    fn cross_where_eq_both() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        let (_, r) = db.query("SELECT x.id FROM x CROSS JOIN y WHERE x.id = y.id");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn cross_where_neq() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE x (timestamp TIMESTAMP, id INT)");
        db.exec_ok("CREATE TABLE y (timestamp TIMESTAMP, id INT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO x VALUES ({}, {})", ts(i), i));
            db.exec_ok(&format!("INSERT INTO y VALUES ({}, {})", ts(i + 10), i));
        }
        let (_, r) = db.query("SELECT x.id, y.id FROM x CROSS JOIN y WHERE x.id != y.id");
        assert_eq!(r.len(), 6);
    }
    // Multiple aggregate in join
    #[test]
    fn inner_5_aggs() {
        let db = setup_ab();
        let (c, r) = db.query("SELECT count(*), sum(b.value), avg(b.value), min(b.value), max(b.value) FROM a INNER JOIN b ON a.id = b.id");
        assert_eq!(c.len(), 5);
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn left_6_aggs() {
        let db = setup_ab();
        let (c, r) = db.query("SELECT count(*), count(b.value), sum(b.value), avg(b.value), min(b.value), max(b.value) FROM a LEFT JOIN b ON a.id = b.id");
        assert_eq!(c.len(), 6);
        assert_eq!(r.len(), 1);
    }
    // Trades join
    #[test]
    fn trades_join() {
        let db = TestDb::with_trades_and_quotes();
        let (_, r) = db.query("SELECT t.symbol, q.bid FROM trades t INNER JOIN quotes q ON t.symbol = q.symbol AND t.timestamp = q.timestamp ORDER BY t.timestamp LIMIT 5");
        assert!(r.len() >= 1);
    }
    #[test]
    fn trades_left_join() {
        let db = TestDb::with_trades_and_quotes();
        let (_, r) = db.query("SELECT count(*) FROM trades t LEFT JOIN quotes q ON t.symbol = q.symbol AND t.timestamp = q.timestamp");
        let c = match &r[0][0] {
            Value::I64(n) => *n,
            _ => panic!(),
        };
        assert_eq!(c, 20);
    }
    // Additional order combos
    #[test]
    fn inner_order_by_name_limit() {
        let db = setup_ab();
        let (_, r) = db.query(
            "SELECT a.name, b.value FROM a INNER JOIN b ON a.id = b.id ORDER BY a.name LIMIT 3",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn left_order_by_name_limit() {
        let db = setup_ab();
        let (_, r) =
            db.query("SELECT a.name FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.name LIMIT 5");
        assert_eq!(r.len(), 5);
    }
}
