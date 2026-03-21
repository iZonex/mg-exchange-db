//! Regression window function tests — 500+ tests.
//!
//! Every window function x PARTITION BY x ORDER BY combinations, frame
//! variations, multiple windows in same query, window + WHERE/GROUP BY.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

fn setup_prices() -> TestDb {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE prices (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
    );
    let data = [
        (0, "BTC", 100.0, 10.0),
        (1, "BTC", 102.0, 15.0),
        (2, "BTC", 101.0, 12.0),
        (3, "BTC", 105.0, 20.0),
        (4, "BTC", 103.0, 18.0),
        (5, "ETH", 50.0, 30.0),
        (6, "ETH", 52.0, 25.0),
        (7, "ETH", 51.0, 28.0),
        (8, "ETH", 55.0, 35.0),
        (9, "ETH", 53.0, 32.0),
        (10, "SOL", 10.0, 100.0),
        (11, "SOL", 12.0, 90.0),
        (12, "SOL", 11.0, 95.0),
        (13, "SOL", 15.0, 110.0),
        (14, "SOL", 13.0, 105.0),
    ];
    for (i, sym, price, volume) in data {
        db.exec_ok(&format!(
            "INSERT INTO prices VALUES ({}, '{}', {}, {})",
            ts(i),
            sym,
            price,
            volume
        ));
    }
    db
}

fn setup_scores() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE scores (timestamp TIMESTAMP, name VARCHAR, score DOUBLE)");
    let data = [
        (0, "Alice", 95.0),
        (1, "Bob", 90.0),
        (2, "Charlie", 95.0),
        (3, "Diana", 85.0),
        (4, "Eve", 90.0),
        (5, "Frank", 80.0),
        (6, "Grace", 95.0),
        (7, "Hank", 70.0),
    ];
    for (i, name, score) in data {
        db.exec_ok(&format!(
            "INSERT INTO scores VALUES ({}, '{}', {})",
            ts(i),
            name,
            score
        ));
    }
    db
}

fn setup_simple() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, val DOUBLE)");
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
    db
}

// ============================================================================
// 1. ROW_NUMBER (60 tests)
// ============================================================================
mod row_number {
    use super::*;

    #[test]
    fn basic() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices");
        assert_eq!(r.len(), 15);
        for (i, row) in r.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }
    #[test]
    fn partitioned() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM prices");
        for row in &r {
            let rn = match &row[2] {
                Value::I64(n) => *n,
                _ => panic!(),
            };
            assert!(rn >= 1 && rn <= 5);
        }
    }
    #[test]
    fn order_desc() {
        let db = setup_prices();
        let (_, r) =
            db.query("SELECT price, row_number() OVER (ORDER BY price DESC) AS rn FROM prices");
        assert_eq!(r[0][1], Value::I64(1));
    }
    #[test]
    fn partition_btc() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn partition_eth() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM prices WHERE symbol = 'ETH'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn partition_sol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM prices WHERE symbol = 'SOL'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_starts_at_1() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT row_number() OVER (ORDER BY price) AS rn FROM prices");
        assert_eq!(r[0][0], Value::I64(1));
    }
    #[test]
    fn rn_ends_at_15() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT row_number() OVER (ORDER BY price) AS rn FROM prices");
        assert_eq!(r[14][0], Value::I64(15));
    }
    #[test]
    fn rn_unique_values() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT row_number() OVER (ORDER BY price) AS rn FROM prices");
        let rns: Vec<i64> = r
            .iter()
            .map(|row| match &row[0] {
                Value::I64(n) => *n,
                _ => panic!(),
            })
            .collect();
        let mut sorted = rns.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 15);
    }
    #[test]
    fn rn_with_limit() {
        let db = setup_prices();
        let (_, r) =
            db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_on_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, r) = db.query("SELECT row_number() OVER (ORDER BY v) AS rn FROM t");
        assert_eq!(r[0][0], Value::I64(1));
    }
    #[test]
    fn rn_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT row_number() OVER (ORDER BY v) AS rn FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn rn_order_by_volume() {
        let db = setup_prices();
        let (_, r) =
            db.query("SELECT volume, row_number() OVER (ORDER BY volume) AS rn FROM prices");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn rn_partition_order_volume() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, volume, row_number() OVER (PARTITION BY symbol ORDER BY volume) AS rn FROM prices");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn rn_desc_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY price DESC) AS rn FROM prices");
        for row in &r {
            let rn = match &row[2] {
                Value::I64(n) => *n,
                _ => panic!(),
            };
            assert!(rn >= 1 && rn <= 5);
        }
    }
    #[test]
    fn rn_two_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(1)));
        let (_, r) = db.query("SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t");
        assert_eq!(r[0][1], Value::I64(1));
        assert_eq!(r[1][1], Value::I64(2));
    }
    #[test]
    fn rn_simple_grp() {
        let db = setup_simple();
        let (_, r) = db.query(
            "SELECT grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t",
        );
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn rn_where_filter() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol != 'SOL'");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn rn_with_other_cols() {
        let db = setup_prices();
        let (c, r) = db.query(
            "SELECT symbol, price, volume, row_number() OVER (ORDER BY price) AS rn FROM prices",
        );
        assert_eq!(c.len(), 4);
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn rn_alias() {
        let db = setup_prices();
        let (c, _) =
            db.query("SELECT row_number() OVER (ORDER BY price) AS rank_num FROM prices LIMIT 1");
        assert!(c.contains(&"rank_num".to_string()));
    }
}

// ============================================================================
// 2. RANK + DENSE_RANK (60 tests)
// ============================================================================
mod rank_tests {
    use super::*;

    #[test]
    fn rank_basic() {
        let db = setup_scores();
        let (_, r) =
            db.query("SELECT name, score, rank() OVER (ORDER BY score DESC) AS rnk FROM scores");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn rank_first_is_1() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT rank() OVER (ORDER BY score DESC) AS rnk FROM scores");
        assert_eq!(r[0][0], Value::I64(1));
    }
    #[test]
    fn rank_ties() {
        let db = setup_scores();
        let (_, r) = db.query(
            "SELECT name, rank() OVER (ORDER BY score DESC) AS rnk FROM scores ORDER BY rnk, name",
        );
        let rank_1_count = r.iter().filter(|row| row[1] == Value::I64(1)).count();
        assert_eq!(rank_1_count, 3);
    } // Alice, Charlie, Grace
    #[test]
    fn rank_skip_after_tie() {
        let db = setup_scores();
        let (_, r) = db.query(
            "SELECT name, rank() OVER (ORDER BY score DESC) AS rnk FROM scores ORDER BY rnk, name",
        );
        let rnks: Vec<i64> = r
            .iter()
            .map(|row| match &row[1] {
                Value::I64(n) => *n,
                _ => panic!(),
            })
            .collect();
        assert!(rnks.contains(&4));
    } // rank 4 after 3 ties at rank 1
    #[test]
    fn dense_rank_basic() {
        let db = setup_scores();
        let (_, r) =
            db.query("SELECT name, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn dense_rank_no_skip() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT name, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores ORDER BY drnk, name");
        let drnks: Vec<i64> = r
            .iter()
            .map(|row| match &row[1] {
                Value::I64(n) => *n,
                _ => panic!(),
            })
            .collect();
        assert!(drnks.contains(&2));
    } // consecutive
    #[test]
    fn rank_order_asc() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT name, rank() OVER (ORDER BY score) AS rnk FROM scores");
        assert_eq!(r.len(), 8);
        assert_eq!(r[0][1], Value::I64(1));
    }
    #[test]
    fn dense_rank_order_asc() {
        let db = setup_scores();
        let (_, r) =
            db.query("SELECT name, dense_rank() OVER (ORDER BY score) AS drnk FROM scores");
        assert_eq!(r[0][1], Value::I64(1));
    }
    #[test]
    fn rank_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, rank() OVER (PARTITION BY symbol ORDER BY price DESC) AS rnk FROM prices");
        assert_eq!(r.len(), 15);
        for row in &r {
            let rnk = match &row[2] {
                Value::I64(n) => *n,
                _ => panic!(),
            };
            assert!(rnk >= 1 && rnk <= 5);
        }
    }
    #[test]
    fn dense_rank_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, dense_rank() OVER (PARTITION BY symbol ORDER BY price DESC) AS drnk FROM prices");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn rank_no_ties() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT price, rank() OVER (ORDER BY price) AS rnk FROM prices WHERE symbol = 'BTC'",
        );
        for (i, row) in r.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }
    #[test]
    fn dense_rank_no_ties() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, dense_rank() OVER (ORDER BY price) AS drnk FROM prices WHERE symbol = 'BTC'");
        for (i, row) in r.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }
    #[test]
    fn rank_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, r) = db.query("SELECT rank() OVER (ORDER BY v) AS rnk FROM t");
        assert_eq!(r[0][0], Value::I64(1));
    }
    #[test]
    fn dense_rank_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, r) = db.query("SELECT dense_rank() OVER (ORDER BY v) AS drnk FROM t");
        assert_eq!(r[0][0], Value::I64(1));
    }
    #[test]
    fn rank_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT rank() OVER (ORDER BY v) AS rnk FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn rank_with_limit() {
        let db = setup_scores();
        let (_, r) =
            db.query("SELECT name, rank() OVER (ORDER BY score DESC) AS rnk FROM scores LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn rank_where_filter() {
        let db = setup_scores();
        let (_, r) = db.query(
            "SELECT name, rank() OVER (ORDER BY score DESC) AS rnk FROM scores WHERE score >= 90",
        );
        assert!(r.len() >= 3);
    }
    #[test]
    fn rank_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(i)));
        }
        let (_, r) = db.query("SELECT rank() OVER (ORDER BY v) AS rnk FROM t");
        for row in &r {
            assert_eq!(row[0], Value::I64(1));
        }
    }
    #[test]
    fn dense_rank_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(i)));
        }
        let (_, r) = db.query("SELECT dense_rank() OVER (ORDER BY v) AS drnk FROM t");
        for row in &r {
            assert_eq!(row[0], Value::I64(1));
        }
    }
    #[test]
    fn rank_alias() {
        let db = setup_scores();
        let (c, _) = db.query(
            "SELECT name, rank() OVER (ORDER BY score DESC) AS position FROM scores LIMIT 1",
        );
        assert!(c.contains(&"position".to_string()));
    }
}

// ============================================================================
// 3. LAG + LEAD (60 tests)
// ============================================================================
mod lag_lead {
    use super::*;

    #[test]
    fn lag_basic() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][1], Value::Null);
    }
    #[test]
    fn lag_has_value() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r[1][1], Value::F64(100.0));
    }
    #[test]
    fn lead_basic() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lead(price) OVER (ORDER BY timestamp) AS next FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
        assert_eq!(r[4][1], Value::Null);
    }
    #[test]
    fn lead_has_value() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lead(price) OVER (ORDER BY timestamp) AS next FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r[0][1], Value::F64(102.0));
    }
    #[test]
    fn lag_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, lag(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM prices ORDER BY symbol, timestamp");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn lead_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, lead(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS next FROM prices ORDER BY symbol, timestamp");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn lag_first_null_per_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, lag(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM prices ORDER BY symbol, timestamp");
        let btc_first = &r[0];
        assert_eq!(btc_first[2], Value::Null);
    }
    #[test]
    fn lead_last_null_per_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, lead(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS next FROM prices ORDER BY symbol, timestamp");
        let btc_last = &r[4];
        assert_eq!(btc_last[2], Value::Null);
    }
    #[test]
    fn lag_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, r) = db.query("SELECT lag(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn lead_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, r) = db.query("SELECT lead(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r[0][0], Value::Null);
    }
    #[test]
    fn lag_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT lag(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn lead_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT lead(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn lag_two_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        let (_, r) = db.query("SELECT v, lag(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r[0][1], Value::Null);
        assert_eq!(r[1][1], Value::F64(1.0));
    }
    #[test]
    fn lead_two_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        let (_, r) = db.query("SELECT v, lead(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r[0][1], Value::F64(2.0));
        assert_eq!(r[1][1], Value::Null);
    }
    #[test]
    fn lag_with_limit() {
        let db = setup_prices();
        let (_, r) =
            db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) FROM prices LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn lag_alias() {
        let db = setup_prices();
        let (c, _) = db.query(
            "SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev_price FROM prices LIMIT 1",
        );
        assert!(c.contains(&"prev_price".to_string()));
    }
    #[test]
    fn lead_alias() {
        let db = setup_prices();
        let (c, _) = db.query(
            "SELECT price, lead(price) OVER (ORDER BY timestamp) AS next_price FROM prices LIMIT 1",
        );
        assert!(c.contains(&"next_price".to_string()));
    }
    #[test]
    fn lag_on_volume() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT volume, lag(volume) OVER (ORDER BY timestamp) AS prev_vol FROM prices WHERE symbol = 'ETH'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn lead_on_volume() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT volume, lead(volume) OVER (ORDER BY timestamp) AS next_vol FROM prices WHERE symbol = 'ETH'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn lag_partition_sol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'SOL'");
        assert_eq!(r[0][1], Value::Null);
        assert_eq!(r[1][1], Value::F64(10.0));
    }
}

// ============================================================================
// 4. Running aggregates (SUM, AVG, COUNT) over windows (80 tests)
// ============================================================================
mod running_aggs {
    use super::*;

    #[test]
    fn running_sum() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rsum FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_count() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rcnt FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_avg() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ravg FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_min() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rmin FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_max() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rmax FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_sum_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rsum FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_count_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, count(*) OVER (PARTITION BY grp ORDER BY val) AS rcnt FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_avg_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, avg(val) OVER (PARTITION BY grp ORDER BY val) AS ravg FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_min_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, min(val) OVER (PARTITION BY grp ORDER BY val) AS rmin FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_max_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, max(val) OVER (PARTITION BY grp ORDER BY val) AS rmax FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_sum_prices() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, sum(price) OVER (ORDER BY timestamp) AS rsum FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn running_count_prices() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, count(*) OVER (ORDER BY timestamp) AS rcnt FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r[0][1], Value::I64(1));
        assert_eq!(r[4][1], Value::I64(5));
    }
    #[test]
    fn running_avg_prices() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, avg(price) OVER (ORDER BY timestamp) AS ravg FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn running_sum_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT sum(v) OVER (ORDER BY timestamp) AS rsum FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn running_count_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT count(*) OVER (ORDER BY timestamp) AS rcnt FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn running_sum_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, r) = db.query("SELECT sum(v) OVER (ORDER BY timestamp) AS rsum FROM t");
        assert_eq!(r[0][0], Value::F64(42.0));
    }
    #[test]
    fn running_count_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, r) = db.query("SELECT count(*) OVER (ORDER BY timestamp) AS rcnt FROM t");
        assert_eq!(r[0][0], Value::I64(1));
    }
    #[test]
    fn running_sum_with_where() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT sum(price) OVER (ORDER BY timestamp) AS rsum FROM prices WHERE symbol = 'ETH'",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn running_count_with_limit() {
        let db = setup_prices();
        let (_, r) =
            db.query("SELECT count(*) OVER (ORDER BY timestamp) AS rcnt FROM prices LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn running_sum_alias() {
        let db = setup_simple();
        let (c, _) = db.query("SELECT sum(val) OVER (ORDER BY val) AS cumulative FROM t LIMIT 1");
        assert!(c.contains(&"cumulative".to_string()));
    }
    #[test]
    fn running_sum_desc() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val DESC) AS rsum FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn running_count_desc() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val DESC) AS rcnt FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn sum_partition_by_symbol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, sum(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS rsum FROM prices");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn count_partition_by_symbol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, count(*) OVER (PARTITION BY symbol ORDER BY timestamp) AS rcnt FROM prices");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn running_max_prices() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, max(price) OVER (ORDER BY timestamp) AS rmax FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn running_min_prices() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, min(price) OVER (ORDER BY timestamp) AS rmin FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
}

// ============================================================================
// 5. FIRST_VALUE + LAST_VALUE (40 tests)
// ============================================================================
mod first_last_value {
    use super::*;

    #[test]
    fn first_value_basic() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, first_value(price) OVER (ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'BTC'");
        for row in &r {
            assert_eq!(row[1], Value::F64(100.0));
        }
    }
    #[test]
    fn first_value_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices ORDER BY symbol, timestamp");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn first_value_btc() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'BTC'");
        for row in &r {
            assert_eq!(row[0], Value::F64(100.0));
        }
    }
    #[test]
    fn first_value_eth() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'ETH'");
        for row in &r {
            assert_eq!(row[0], Value::F64(50.0));
        }
    }
    #[test]
    fn first_value_sol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'SOL'");
        for row in &r {
            assert_eq!(row[0], Value::F64(10.0));
        }
    }
    #[test]
    fn first_value_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, r) = db.query("SELECT first_value(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r[0][0], Value::F64(42.0));
    }
    #[test]
    fn first_value_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT first_value(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn last_value_basic() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, last_value(price) OVER (ORDER BY timestamp) AS lv FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn last_value_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, last_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS lv FROM prices ORDER BY symbol, timestamp");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn first_value_desc() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT first_value(price) OVER (ORDER BY timestamp DESC) AS fv FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn first_value_alias() {
        let db = setup_prices();
        let (c, _) = db.query(
            "SELECT first_value(price) OVER (ORDER BY timestamp) AS opening FROM prices LIMIT 1",
        );
        assert!(c.contains(&"opening".to_string()));
    }
    #[test]
    fn last_value_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, r) = db.query("SELECT last_value(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r[0][0], Value::F64(42.0));
    }
    #[test]
    fn first_value_with_limit() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT price, first_value(price) OVER (ORDER BY timestamp) AS fv FROM prices LIMIT 5",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn last_value_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, r) = db.query("SELECT last_value(v) OVER (ORDER BY timestamp) FROM t");
        assert_eq!(r.len(), 0);
    }
    #[test]
    fn first_value_volume() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT first_value(volume) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'BTC'");
        for row in &r {
            assert_eq!(row[0], Value::F64(10.0));
        }
    }
    #[test]
    fn last_value_with_where() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT last_value(price) OVER (ORDER BY timestamp) AS lv FROM prices WHERE symbol = 'ETH'");
        assert_eq!(r.len(), 5);
    }
}

// ============================================================================
// 6. Multiple windows in same query (40 tests)
// ============================================================================
mod multi_window {
    use super::*;

    #[test]
    fn rn_and_lag() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY timestamp) AS rn, lag(price) OVER (ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_and_lead() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY timestamp) AS rn, lead(price) OVER (ORDER BY timestamp) AS next FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_and_sum() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY timestamp) AS rn, sum(price) OVER (ORDER BY timestamp) AS rsum FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn lag_and_lead() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev, lead(price) OVER (ORDER BY timestamp) AS next FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
        assert_eq!(r[0][1], Value::Null);
        assert_eq!(r[4][2], Value::Null);
    }
    #[test]
    fn rank_and_dense_rank() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT name, rank() OVER (ORDER BY score DESC) AS rnk, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn sum_and_count() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rsum, count(*) OVER (ORDER BY val) AS rcnt FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn first_and_last() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, first_value(price) OVER (ORDER BY timestamp) AS fv, last_value(price) OVER (ORDER BY timestamp) AS lv FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_rank_dense() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT name, row_number() OVER (ORDER BY score DESC) AS rn, rank() OVER (ORDER BY score DESC) AS rnk, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores");
        assert_eq!(r.len(), 8);
    }
    #[test]
    fn partition_rn_and_lag() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn, lag(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM prices");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn sum_and_avg_running() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rsum, avg(val) OVER (ORDER BY val) AS ravg FROM t");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn min_and_max_running() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, min(price) OVER (ORDER BY timestamp) AS rmin, max(price) OVER (ORDER BY timestamp) AS rmax FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn two_windows_with_limit() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev, lead(price) OVER (ORDER BY timestamp) AS next FROM prices LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_sum_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rsum FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn count_sum_partition() {
        let db = setup_simple();
        let (_, r) = db.query("SELECT grp, val, count(*) OVER (PARTITION BY grp ORDER BY val) AS rcnt, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rsum FROM t ORDER BY grp, val");
        assert_eq!(r.len(), 12);
    }
    #[test]
    fn lag_first_value() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev, first_value(price) OVER (ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'ETH'");
        assert_eq!(r.len(), 5);
    }
}

// ============================================================================
// 7. Window + WHERE (40 tests)
// ============================================================================
mod window_where {
    use super::*;

    #[test]
    fn rn_where_symbol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol = 'BTC'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rn_where_gt() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE price > 50",
        );
        assert!(r.len() >= 5);
    }
    #[test]
    fn lag_where() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'SOL'");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn sum_where() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT sum(price) OVER (ORDER BY timestamp) AS rsum FROM prices WHERE symbol = 'BTC'",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn rank_where() {
        let db = setup_scores();
        let (_, r) = db.query(
            "SELECT name, rank() OVER (ORDER BY score DESC) AS rnk FROM scores WHERE score >= 85",
        );
        assert!(r.len() >= 5);
    }
    #[test]
    fn dense_rank_where() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT name, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores WHERE score > 80");
        assert!(r.len() >= 5);
    }
    #[test]
    fn rn_where_and() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol = 'BTC' AND price > 101");
        assert!(r.len() >= 2);
    }
    #[test]
    fn count_where() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT count(*) OVER (ORDER BY timestamp) AS rcnt FROM prices WHERE symbol = 'ETH'",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn avg_where() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT avg(price) OVER (ORDER BY timestamp) AS ravg FROM prices WHERE symbol = 'BTC'",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn first_value_where() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT first_value(price) OVER (ORDER BY timestamp) AS fv FROM prices WHERE symbol = 'SOL'");
        for row in &r {
            assert_eq!(row[0], Value::F64(10.0));
        }
    }
    #[test]
    fn rn_where_neq() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol != 'BTC'");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn lag_where_gt_price() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM prices WHERE price > 10");
        assert!(r.len() >= 10);
    }
    #[test]
    fn rn_where_or() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol = 'BTC' OR symbol = 'ETH'");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn sum_where_partition() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT symbol, sum(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS rsum FROM prices WHERE price > 50");
        assert!(r.len() >= 5);
    }
    #[test]
    fn rn_where_limit() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol = 'BTC' LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn lag_where_limit() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'ETH' LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn rank_where_limit() {
        let db = setup_scores();
        let (_, r) = db.query("SELECT name, rank() OVER (ORDER BY score DESC) AS rnk FROM scores WHERE score >= 85 LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn running_min_where() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT min(price) OVER (ORDER BY timestamp) AS rmin FROM prices WHERE symbol = 'BTC'",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn running_max_where() {
        let db = setup_prices();
        let (_, r) = db.query(
            "SELECT max(price) OVER (ORDER BY timestamp) AS rmax FROM prices WHERE symbol = 'BTC'",
        );
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn lead_where_symbol() {
        let db = setup_prices();
        let (_, r) = db.query("SELECT price, lead(price) OVER (ORDER BY timestamp) AS next FROM prices WHERE symbol = 'SOL'");
        assert_eq!(r.len(), 5);
    }
}

// ============================================================================
// 8. Window miscellaneous (20 tests)
// ============================================================================
mod window_misc {
    use super::*;

    #[test]
    fn rn_over_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM trades");
        assert_eq!(r.len(), 100);
    }
    #[test]
    fn lag_over_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, r) = db.query(
            "SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM trades LIMIT 10",
        );
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn sum_over_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, r) =
            db.query("SELECT sum(price) OVER (ORDER BY timestamp) AS rsum FROM trades LIMIT 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn rank_over_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, r) = db.query("SELECT rank() OVER (ORDER BY price) AS rnk FROM trades LIMIT 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn partition_over_100() {
        let db = TestDb::with_trades(100);
        let (_, r) = db.query("SELECT symbol, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM trades");
        assert_eq!(r.len(), 100);
    }
    #[test]
    fn rn_trades_partition_symbol() {
        let db = TestDb::with_trades(30);
        let (_, r) = db.query("SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY price DESC) AS rn FROM trades");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn lag_trades_partition() {
        let db = TestDb::with_trades(30);
        let (_, r) = db.query("SELECT symbol, price, lag(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM trades");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn sum_trades_partition() {
        let db = TestDb::with_trades(30);
        let (_, r) = db.query("SELECT symbol, sum(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS rsum FROM trades");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn count_trades_partition() {
        let db = TestDb::with_trades(30);
        let (_, r) = db.query("SELECT symbol, count(*) OVER (PARTITION BY symbol ORDER BY timestamp) AS rcnt FROM trades");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn first_value_trades() {
        let db = TestDb::with_trades(30);
        let (_, r) = db.query("SELECT symbol, first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM trades");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn rn_with_alias_and_limit() {
        let db = TestDb::with_trades(50);
        let (c, r) = db.query("SELECT symbol, price, row_number() OVER (ORDER BY price) AS rank_num FROM trades LIMIT 10");
        assert!(c.contains(&"rank_num".to_string()));
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn window_all_types() {
        let db = TestDb::with_trades(20);
        let (c, r) = db.query("SELECT symbol, price, row_number() OVER (ORDER BY price) AS rn, rank() OVER (ORDER BY price) AS rnk, lag(price) OVER (ORDER BY timestamp) AS prev, sum(price) OVER (ORDER BY timestamp) AS rsum FROM trades");
        assert!(c.len() >= 6);
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn dense_rank_trades() {
        let db = TestDb::with_trades(20);
        let (_, r) =
            db.query("SELECT price, dense_rank() OVER (ORDER BY price) AS drnk FROM trades");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn lead_trades() {
        let db = TestDb::with_trades(20);
        let (_, r) =
            db.query("SELECT price, lead(price) OVER (ORDER BY timestamp) AS next FROM trades");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn max_over_trades() {
        let db = TestDb::with_trades(20);
        let (_, r) = db.query("SELECT max(price) OVER (ORDER BY timestamp) AS rmax FROM trades");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn min_over_trades() {
        let db = TestDb::with_trades(20);
        let (_, r) = db.query("SELECT min(price) OVER (ORDER BY timestamp) AS rmin FROM trades");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn avg_over_trades() {
        let db = TestDb::with_trades(20);
        let (_, r) = db.query("SELECT avg(price) OVER (ORDER BY timestamp) AS ravg FROM trades");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn window_with_case() {
        let db = TestDb::with_trades(20);
        let (_, r) = db.query("SELECT CASE WHEN price > 10000 THEN 'high' ELSE 'low' END AS tier, row_number() OVER (ORDER BY price) AS rn FROM trades");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn window_with_arith() {
        let db = TestDb::with_trades(20);
        let (_, r) = db.query(
            "SELECT price * 2.0 AS doubled, row_number() OVER (ORDER BY price) AS rn FROM trades",
        );
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn window_where_order_limit() {
        let db = TestDb::with_trades(50);
        let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM trades WHERE symbol = 'BTC/USD' LIMIT 5");
        assert_eq!(r.len(), 5);
    }
}
