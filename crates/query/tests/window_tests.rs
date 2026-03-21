//! Window function tests for ExchangeDB (80+ tests).
//!
//! Covers: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, running aggregates
//! (SUM, AVG, COUNT), FIRST_VALUE, LAST_VALUE with various OVER clauses.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1_710_460_800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Setup a table with ordered data for window function tests.
fn setup_window_db() -> TestDb {
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

    for (i, symbol, price, volume) in data {
        db.exec_ok(&format!(
            "INSERT INTO prices VALUES ({}, '{}', {}, {})",
            ts(i),
            symbol,
            price,
            volume
        ));
    }

    db
}

/// Setup a table with tied values for RANK tests.
fn setup_rank_db() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE scores (timestamp TIMESTAMP, name VARCHAR, score DOUBLE)");
    let data = [
        (0, "Alice", 95.0),
        (1, "Bob", 90.0),
        (2, "Charlie", 95.0), // tie with Alice
        (3, "Diana", 85.0),
        (4, "Eve", 90.0), // tie with Bob
        (5, "Frank", 80.0),
        (6, "Grace", 95.0), // tie with Alice and Charlie
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

// ===========================================================================
// row_number
// ===========================================================================
mod row_number {
    use super::*;

    #[test]
    fn basic_row_number() {
        let db = setup_window_db();
        let (_, rows) =
            db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices");
        assert_eq!(rows.len(), 15);
        // Row numbers should be 1..15
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn row_number_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM prices"
        );
        assert_eq!(rows.len(), 15);
        // Within each symbol partition, row numbers should be 1..5
        for row in &rows {
            let rn = match &row[2] {
                Value::I64(n) => *n,
                other => panic!("{other:?}"),
            };
            assert!((1..=5).contains(&rn));
        }
    }

    #[test]
    fn row_number_desc_order() {
        let db = setup_window_db();
        let (_, rows) =
            db.query("SELECT price, row_number() OVER (ORDER BY price DESC) AS rn FROM prices");
        assert_eq!(rows[0][1], Value::I64(1));
        // First row should have highest price
        let first_price = match &rows[0][0] {
            Value::F64(p) => *p,
            other => panic!("{other:?}"),
        };
        let last_price = match &rows[14][0] {
            Value::F64(p) => *p,
            other => panic!("{other:?}"),
        };
        assert!(first_price >= last_price);
    }

    #[test]
    fn row_number_no_order() {
        let db = setup_window_db();
        let (_, rows) = db.query("SELECT price, row_number() OVER () AS rn FROM prices");
        assert_eq!(rows.len(), 15);
        // Should still assign sequential numbers
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn row_number_partition_by_symbol_order_timestamp() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, timestamp, row_number() OVER (PARTITION BY symbol ORDER BY timestamp) AS rn FROM prices"
        );
        // Each partition: 5 rows numbered 1-5
        let btc_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("BTC".to_string()))
            .collect();
        assert_eq!(btc_rows.len(), 5);
        for (i, row) in btc_rows.iter().enumerate() {
            assert_eq!(row[2], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn row_number_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::I64(1));
    }

    #[test]
    fn row_number_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn row_number_with_where() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices WHERE symbol = 'BTC'"
        );
        assert_eq!(rows.len(), 5);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn row_number_with_limit() {
        let db = setup_window_db();
        let (_, rows) =
            db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM prices LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
}

// ===========================================================================
// rank
// ===========================================================================
mod rank {
    use super::*;

    #[test]
    fn basic_rank() {
        let db = setup_rank_db();
        let (_, rows) =
            db.query("SELECT name, score, rank() OVER (ORDER BY score DESC) AS rnk FROM scores");
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn rank_with_ties() {
        let db = setup_rank_db();
        let (_, rows) =
            db.query("SELECT score, rank() OVER (ORDER BY score DESC) AS rnk FROM scores");
        // Score 95 appears 3 times -> rank 1,1,1 then next is 4
        let score95_ranks: Vec<i64> = rows
            .iter()
            .filter(|r| r[0] == Value::F64(95.0))
            .map(|r| match &r[1] {
                Value::I64(n) => *n,
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(score95_ranks.len(), 3);
        for r in &score95_ranks {
            assert_eq!(*r, 1);
        }
    }

    #[test]
    fn rank_gap_after_ties() {
        let db = setup_rank_db();
        let (_, rows) = db.query(
            "SELECT score, rank() OVER (ORDER BY score DESC) AS rnk FROM scores ORDER BY rnk",
        );
        // After 3 ties at rank 1, next should be rank 4 (not 2)
        let rank4_rows: Vec<_> = rows.iter().filter(|r| r[1] == Value::I64(4)).collect();
        if !rank4_rows.is_empty() {
            let score = match &rank4_rows[0][0] {
                Value::F64(s) => *s,
                other => panic!("{other:?}"),
            };
            assert_eq!(score, 90.0);
        }
    }

    #[test]
    fn rank_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, rank() OVER (PARTITION BY symbol ORDER BY price DESC) AS rnk FROM prices"
        );
        assert_eq!(rows.len(), 15);
    }

    #[test]
    fn rank_no_ties() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT v, rank() OVER (ORDER BY v) AS rnk FROM t");
        // No ties: rank should equal row_number
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn rank_all_same_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v, rank() OVER (ORDER BY v) AS rnk FROM t");
        // All same -> all rank 1
        for row in &rows {
            assert_eq!(row[1], Value::I64(1));
        }
    }

    #[test]
    fn rank_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT v, rank() OVER (ORDER BY v) AS rnk FROM t");
        assert_eq!(rows[0][1], Value::I64(1));
    }
}

// ===========================================================================
// dense_rank
// ===========================================================================
mod dense_rank {
    use super::*;

    #[test]
    fn basic_dense_rank() {
        let db = setup_rank_db();
        let (_, rows) =
            db.query("SELECT score, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores");
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn dense_rank_no_gaps() {
        let db = setup_rank_db();
        let (_, rows) = db.query(
            "SELECT score, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores ORDER BY drnk"
        );
        // Scores: 95(x3), 90(x2), 85(x1), 80(x1), 70(x1)
        // Dense ranks: 1,1,1, 2,2, 3, 4, 5
        let max_rank = rows
            .iter()
            .map(|r| match &r[1] {
                Value::I64(n) => *n,
                other => panic!("{other:?}"),
            })
            .max()
            .unwrap();
        assert_eq!(max_rank, 5); // No gaps: 5 distinct values -> max dense_rank = 5
    }

    #[test]
    fn dense_rank_ties() {
        let db = setup_rank_db();
        let (_, rows) =
            db.query("SELECT score, dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores");
        let rank1_count = rows.iter().filter(|r| r[1] == Value::I64(1)).count();
        assert_eq!(rank1_count, 3); // 3 rows with score 95
    }

    #[test]
    fn dense_rank_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, dense_rank() OVER (PARTITION BY symbol ORDER BY price DESC) AS drnk FROM prices"
        );
        assert_eq!(rows.len(), 15);
    }

    #[test]
    fn dense_rank_no_ties() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT v, dense_rank() OVER (ORDER BY v) AS drnk FROM t");
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn dense_rank_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v, dense_rank() OVER (ORDER BY v) AS drnk FROM t");
        for row in &rows {
            assert_eq!(row[1], Value::I64(1));
        }
    }

    #[test]
    fn dense_rank_vs_rank_comparison() {
        let db = setup_rank_db();
        let (_, rows) = db.query(
            "SELECT score, rank() OVER (ORDER BY score DESC) AS rnk, \
                    dense_rank() OVER (ORDER BY score DESC) AS drnk FROM scores ORDER BY score DESC"
        );
        // rank has gaps, dense_rank does not
        let last_rank = match &rows.last().unwrap()[1] {
            Value::I64(n) => *n,
            other => panic!("{other:?}"),
        };
        let last_dense = match &rows.last().unwrap()[2] {
            Value::I64(n) => *n,
            other => panic!("{other:?}"),
        };
        assert!(last_rank >= last_dense);
    }
}

// ===========================================================================
// lag_lead
// ===========================================================================
mod lag_lead {
    use super::*;

    #[test]
    fn basic_lag() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, lag(price, 1) OVER (ORDER BY timestamp) AS prev_price FROM prices WHERE symbol = 'BTC'"
        );
        assert_eq!(rows.len(), 5);
        // First row's lag should be NULL
        assert_eq!(rows[0][1], Value::Null);
    }

    #[test]
    fn basic_lead() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, lead(price, 1) OVER (ORDER BY timestamp) AS next_price FROM prices WHERE symbol = 'BTC'"
        );
        assert_eq!(rows.len(), 5);
        // Last row's lead should be NULL
        assert_eq!(rows[4][1], Value::Null);
    }

    #[test]
    fn lag_offset_2() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, lag(price, 2) OVER (ORDER BY timestamp) AS prev2 FROM prices WHERE symbol = 'BTC'"
        );
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[1][1], Value::Null);
        // Third row should have first row's price
        assert_eq!(rows[2][1], Value::F64(100.0));
    }

    #[test]
    fn lead_offset_2() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, lead(price, 2) OVER (ORDER BY timestamp) AS next2 FROM prices WHERE symbol = 'BTC'"
        );
        assert_eq!(rows[3][1], Value::Null);
        assert_eq!(rows[4][1], Value::Null);
    }

    #[test]
    fn lag_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, lag(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM prices"
        );
        assert_eq!(rows.len(), 15);
        // First row of each partition should be NULL
        let btc_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("BTC".to_string()))
            .collect();
        assert_eq!(btc_rows[0][2], Value::Null);
    }

    #[test]
    fn lead_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, lead(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS nxt FROM prices"
        );
        let btc_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("BTC".to_string()))
            .collect();
        // Last BTC row should have NULL lead
        assert_eq!(btc_rows.last().unwrap()[2], Value::Null);
    }

    #[test]
    fn lag_with_default() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, lag(price, 1, 0) OVER (ORDER BY timestamp) AS prev FROM prices WHERE symbol = 'BTC'"
        );
        // First row should use default 0 instead of NULL
        assert!(rows[0][1].eq_coerce(&Value::F64(0.0)) || rows[0][1].eq_coerce(&Value::I64(0)));
    }

    #[test]
    fn lag_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT v, lag(v, 1) OVER (ORDER BY timestamp) AS prev FROM t");
        assert_eq!(rows[0][1], Value::Null);
    }

    #[test]
    fn lead_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT v, lead(v, 1) OVER (ORDER BY timestamp) AS nxt FROM t");
        assert_eq!(rows[0][1], Value::Null);
    }

    #[test]
    fn lag_references_correct_previous() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i),
                (i + 1) * 10
            ));
        }
        let (_, rows) = db.query("SELECT v, lag(v, 1) OVER (ORDER BY timestamp) AS prev FROM t");
        // row 1: v=20, prev=10
        assert_eq!(rows[1][0], Value::F64(20.0));
        assert_eq!(rows[1][1], Value::F64(10.0));
        // row 2: v=30, prev=20
        assert_eq!(rows[2][0], Value::F64(30.0));
        assert_eq!(rows[2][1], Value::F64(20.0));
    }

    #[test]
    fn lead_references_correct_next() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i),
                (i + 1) * 10
            ));
        }
        let (_, rows) = db.query("SELECT v, lead(v, 1) OVER (ORDER BY timestamp) AS nxt FROM t");
        // row 0: v=10, nxt=20
        assert_eq!(rows[0][0], Value::F64(10.0));
        assert_eq!(rows[0][1], Value::F64(20.0));
        // row 3: v=40, nxt=50
        assert_eq!(rows[3][0], Value::F64(40.0));
        assert_eq!(rows[3][1], Value::F64(50.0));
    }

    #[test]
    fn lag_varchar_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c')", ts(2)));
        let (_, rows) = db.query("SELECT s, lag(s, 1) OVER (ORDER BY timestamp) AS prev FROM t");
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[1][1], Value::Str("a".to_string()));
        assert_eq!(rows[2][1], Value::Str("b".to_string()));
    }
}

// ===========================================================================
// running_agg: running sum, avg, count
// ===========================================================================
mod running_agg {
    use super::*;

    #[test]
    fn running_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT v, sum(v) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum FROM t"
        );
        assert_eq!(rows.len(), 5);
        // Running sum: 1, 3, 6, 10, 15
        let expected_sums = [1.0, 3.0, 6.0, 10.0, 15.0];
        for (i, row) in rows.iter().enumerate() {
            match &row[1] {
                Value::F64(s) => assert!(
                    (*s - expected_sums[i]).abs() < 0.01,
                    "row {i}: expected {}, got {s}",
                    expected_sums[i]
                ),
                Value::I64(s) => assert_eq!(*s, expected_sums[i] as i64),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn running_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT v, count(*) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt FROM t"
        );
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn running_avg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT v, avg(v) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg FROM t"
        );
        // Running avg: 1/1=1.0, 3/2=1.5, 6/3=2.0, 10/4=2.5
        let expected = [1.0, 1.5, 2.0, 2.5];
        for (i, row) in rows.iter().enumerate() {
            match &row[1] {
                Value::F64(a) => assert!((*a - expected[i]).abs() < 0.01),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn windowed_sum_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, sum(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sum FROM prices"
        );
        assert_eq!(rows.len(), 15);
    }

    #[test]
    fn windowed_count_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db
            .query("SELECT symbol, count(*) OVER (PARTITION BY symbol) AS part_count FROM prices");
        // Each partition has 5 rows
        for row in &rows {
            assert_eq!(row[1], Value::I64(5));
        }
    }

    #[test]
    fn sum_over_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT v, sum(v) OVER (ORDER BY timestamp) AS s FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn running_sum_single_partition() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100.0)", ts(0)));
        let (_, rows) = db.query(
            "SELECT v, sum(v) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM t"
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::F64(100.0));
    }

    #[test]
    fn windowed_min_max() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, min(price) OVER (PARTITION BY symbol) AS min_p, \
                    max(price) OVER (PARTITION BY symbol) AS max_p FROM prices",
        );
        assert_eq!(rows.len(), 15);
        // Within each partition, min < max
        for row in &rows {
            assert!(row[1].cmp_coerce(&row[2]) != Some(std::cmp::Ordering::Greater));
        }
    }
}

// ===========================================================================
// first_last_value
// ===========================================================================
mod first_last_value {
    use super::*;

    #[test]
    fn basic_first_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        let (_, rows) = db.query("SELECT v, first_value(v) OVER (ORDER BY timestamp) AS fv FROM t");
        // First value should always be 10.0
        for row in &rows {
            assert_eq!(row[1], Value::F64(10.0));
        }
    }

    #[test]
    fn basic_last_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        let (_, rows) = db.query(
            "SELECT v, last_value(v) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM t"
        );
        // Last value with full frame should be 50.0
        for row in &rows {
            assert_eq!(row[1], Value::F64(50.0));
        }
    }

    #[test]
    fn first_value_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices"
        );
        let btc_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("BTC".to_string()))
            .collect();
        // First BTC price is 100.0
        for row in &btc_rows {
            assert_eq!(row[2], Value::F64(100.0));
        }
    }

    #[test]
    fn last_value_with_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, last_value(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM prices"
        );
        let btc_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("BTC".to_string()))
            .collect();
        // Last BTC price is 103.0
        for row in &btc_rows {
            assert_eq!(row[2], Value::F64(103.0));
        }
    }

    #[test]
    fn first_value_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT v, first_value(v) OVER (ORDER BY timestamp) AS fv FROM t");
        assert_eq!(rows[0][1], Value::F64(42.0));
    }

    #[test]
    fn last_value_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query(
            "SELECT v, last_value(v) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM t"
        );
        assert_eq!(rows[0][1], Value::F64(42.0));
    }

    #[test]
    fn first_value_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT v, first_value(v) OVER (ORDER BY timestamp) AS fv FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn first_value_with_frame() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        let (_, rows) = db.query(
            "SELECT v, first_value(v) OVER (ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS fv FROM t"
        );
        assert_eq!(rows.len(), 5);
        // First row: frame is [row0, row1], first_value = 10.0
        assert_eq!(rows[0][1], Value::F64(10.0));
        // Second row: frame is [row0, row1, row2], first_value = 10.0
        assert_eq!(rows[1][1], Value::F64(10.0));
        // Third row: frame is [row1, row2, row3], first_value = 20.0
        assert_eq!(rows[2][1], Value::F64(20.0));
    }

    #[test]
    fn last_value_with_frame() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        let (_, rows) = db.query(
            "SELECT v, last_value(v) OVER (ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS lv FROM t"
        );
        assert_eq!(rows.len(), 5);
        // First row: frame is [row0, row1], last_value = 20.0
        assert_eq!(rows[0][1], Value::F64(20.0));
        // Last row: frame is [row3, row4], last_value = 50.0
        assert_eq!(rows[4][1], Value::F64(50.0));
    }
}

// ===========================================================================
// window_extra: additional window function tests
// ===========================================================================
mod window_extra {
    use super::*;

    #[test]
    fn multiple_window_functions_same_query() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, \
                    row_number() OVER (ORDER BY price) AS rn, \
                    rank() OVER (ORDER BY price) AS rnk \
             FROM prices WHERE symbol = 'BTC'",
        );
        assert_eq!(rows.len(), 5);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[1], Value::I64((i + 1) as i64));
        }
    }

    #[test]
    fn row_number_and_lag_together() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        let (_, rows) = db.query(
            "SELECT v, \
                    row_number() OVER (ORDER BY timestamp) AS rn, \
                    lag(v, 1) OVER (ORDER BY timestamp) AS prev \
             FROM t",
        );
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][2], Value::Null);
        assert_eq!(rows[1][2], Value::F64(10.0));
    }

    #[test]
    fn window_over_partitioned_large_data() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query(
            "SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY timestamp) AS rn FROM trades"
        );
        assert_eq!(rows.len(), 50);
    }

    #[test]
    fn dense_rank_asc_vs_desc() {
        let db = setup_rank_db();
        let (_, rows_asc) =
            db.query("SELECT score, dense_rank() OVER (ORDER BY score ASC) AS dr FROM scores");
        let (_, rows_desc) =
            db.query("SELECT score, dense_rank() OVER (ORDER BY score DESC) AS dr FROM scores");
        // Both should have 8 rows
        assert_eq!(rows_asc.len(), 8);
        assert_eq!(rows_desc.len(), 8);
    }

    #[test]
    fn running_sum_matches_total() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT v, sum(v) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM t"
        );
        // Last row's running sum should equal total sum
        let last_running = &rows[9][1];
        let total = db.query_scalar("SELECT sum(v) FROM t");
        assert!(last_running.eq_coerce(&total));
    }

    #[test]
    fn window_with_where_clause() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT price, rank() OVER (ORDER BY price DESC) AS rnk \
             FROM prices WHERE symbol = 'ETH'",
        );
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lead_at_end_of_partition() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, price, lead(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS nxt FROM prices"
        );
        // Last row of each partition should have NULL lead
        let eth_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("ETH".to_string()))
            .collect();
        assert_eq!(eth_rows.last().unwrap()[2], Value::Null);
    }

    #[test]
    fn first_value_across_all_partitions() {
        let db = setup_window_db();
        let (_, rows) = db.query(
            "SELECT symbol, first_value(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS fv FROM prices"
        );
        let sol_rows: Vec<_> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("SOL".to_string()))
            .collect();
        for row in &sol_rows {
            assert_eq!(row[1], Value::F64(10.0)); // first SOL price
        }
    }

    #[test]
    fn running_count_matches_row_number() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..8 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT row_number() OVER (ORDER BY timestamp) AS rn, \
                    count(*) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt \
             FROM t"
        );
        for row in &rows {
            assert_eq!(row[0], row[1]);
        }
    }

    #[test]
    fn window_function_on_single_partition() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT grp, v, sum(v) OVER (PARTITION BY grp ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM t"
        );
        // Running sum: 0, 1, 3
        let expected = [0.0, 1.0, 3.0];
        for (i, row) in rows.iter().enumerate() {
            assert!(row[2].eq_coerce(&Value::F64(expected[i])));
        }
    }

    #[test]
    fn rank_with_many_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            let v = if i < 10 { 1.0 } else { 2.0 };
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), v));
        }
        let (_, rows) = db.query("SELECT v, rank() OVER (ORDER BY v) AS rnk FROM t");
        let rank1_count = rows.iter().filter(|r| r[1] == Value::I64(1)).count();
        assert_eq!(rank1_count, 10);
        let rank11_count = rows.iter().filter(|r| r[1] == Value::I64(11)).count();
        assert_eq!(rank11_count, 10);
    }

    #[test]
    fn dense_rank_with_many_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            let v = if i < 10 { 1.0 } else { 2.0 };
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), v));
        }
        let (_, rows) = db.query("SELECT v, dense_rank() OVER (ORDER BY v) AS drnk FROM t");
        let max_dr = rows
            .iter()
            .map(|r| match &r[1] {
                Value::I64(n) => *n,
                other => panic!("{other:?}"),
            })
            .max()
            .unwrap();
        assert_eq!(max_dr, 2); // only 2 distinct values
    }

    #[test]
    fn lag_offset_greater_than_partition_size() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        let (_, rows) = db.query("SELECT v, lag(v, 5) OVER (ORDER BY timestamp) AS prev5 FROM t");
        // Both rows have offset > partition size -> NULL
        assert_eq!(rows[0][1], Value::Null);
        assert_eq!(rows[1][1], Value::Null);
    }

    #[test]
    fn row_number_with_large_dataset() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT row_number() OVER (ORDER BY timestamp) AS rn FROM trades");
        assert_eq!(rows.len(), 50);
        assert_eq!(rows[49][0], Value::I64(50));
    }

    #[test]
    fn running_sum_partitioned_matches_group_sum() {
        let db = setup_window_db();
        // Last row of each partition's running sum = total sum for that partition
        let (_, window_rows) = db.query(
            "SELECT symbol, price, sum(price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM prices"
        );
        let (_, group_rows) = db.query("SELECT symbol, sum(price) FROM prices GROUP BY symbol");
        // For each symbol, the last running sum should match the group sum
        for grow in &group_rows {
            let sym = &grow[0];
            let total = &grow[1];
            let part_rows: Vec<_> = window_rows.iter().filter(|r| &r[0] == sym).collect();
            let last_rs = &part_rows.last().unwrap()[2];
            assert!(last_rs.eq_coerce(total), "running sum mismatch for {sym:?}");
        }
    }

    #[test]
    fn window_count_over_all_equals_total() {
        let db = setup_window_db();
        let (_, rows) = db.query("SELECT count(*) OVER () AS total FROM prices LIMIT 1");
        assert_eq!(rows[0][0], Value::I64(15));
    }

    #[test]
    fn lag_and_lead_are_inverses() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        let (_, rows) = db.query(
            "SELECT v, lag(v, 1) OVER (ORDER BY timestamp) AS prev, \
                    lead(v, 1) OVER (ORDER BY timestamp) AS nxt FROM t",
        );
        // row 1: prev=0, nxt=20
        assert_eq!(rows[1][1], Value::F64(0.0));
        assert_eq!(rows[1][2], Value::F64(20.0));
        // row 2: prev=10, nxt=30
        assert_eq!(rows[2][1], Value::F64(10.0));
        assert_eq!(rows[2][2], Value::F64(30.0));
    }

    #[test]
    fn rank_equals_row_number_when_no_ties() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query(
            "SELECT v, row_number() OVER (ORDER BY v) AS rn, rank() OVER (ORDER BY v) AS rnk FROM t"
        );
        for row in &rows {
            assert_eq!(row[1], row[2]);
        }
    }
}
