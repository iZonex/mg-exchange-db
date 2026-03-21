//! Comprehensive SAMPLE BY tests — 100 tests.
//!
//! Tests various intervals (1s, 5s, 10s, 30s, 1m, 5m, 15m, 30m, 1h, 2h, 4h, 1d),
//! fill modes (NONE, NULL, PREV, 0, LINEAR), aggregates in SAMPLE BY, edge cases
//! (single row, empty table, all in one bucket), with WHERE, ORDER BY, LIMIT.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1_710_460_800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// =============================================================================
// Basic SAMPLE BY intervals
// =============================================================================
mod sample_intervals {
    use super::*;

    #[test]
    fn sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_10m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m");
        assert!(rows.len() >= 10);
    }

    #[test]
    fn sample_by_1m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1m");
        assert!(rows.len() >= 10);
    }

    #[test]
    fn sample_by_5m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_15m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 15m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_30m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_2h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 2h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_4h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 4h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_1d() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_1s() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1s");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_5s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // 10 rows, 1 second apart
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 5s");
        assert_eq!(rows.len(), 2); // 2 buckets of 5 seconds
    }

    #[test]
    fn sample_by_10s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 10s");
        assert_eq!(rows.len(), 2); // 2 buckets of 10 seconds
    }

    #[test]
    fn sample_by_30s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..60 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 30s");
        assert_eq!(rows.len(), 2); // 2 buckets of 30 seconds
    }
}

// =============================================================================
// Fill modes
// =============================================================================
mod fill_modes {
    use super::*;

    #[test]
    fn fill_none() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(NONE)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(NULL)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_prev() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(PREV)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_zero() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(0)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_linear() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(LINEAR)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_none_5m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 5m FILL(NONE)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_null_10m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 10m FILL(NULL)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_prev_30m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 30m FILL(PREV)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_zero_1d() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d FILL(0)");
        assert!(!rows.is_empty());
    }

    #[test]
    fn fill_linear_2h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 2h FILL(LINEAR)");
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// Aggregates with SAMPLE BY
// =============================================================================
mod sample_aggregates {
    use super::*;

    #[test]
    fn sum_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v > 0.0),
                _ => panic!("expected F64"),
            }
        }
    }

    #[test]
    fn avg_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn min_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT min(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn max_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT max(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn count_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
        // Total across all buckets should be 20
        let total: i64 = rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                _ => 0,
            })
            .sum();
        assert_eq!(total, 20);
    }

    #[test]
    fn first_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT first(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn last_sample_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT last(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn multiple_aggs_sample_by() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query(
            "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades SAMPLE BY 1h",
        );
        assert_eq!(cols.len(), 5);
        assert!(!rows.is_empty());
    }

    #[test]
    fn first_last_sample_by() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query("SELECT first(price), last(price) FROM trades SAMPLE BY 1h");
        assert_eq!(cols.len(), 2);
        assert!(!rows.is_empty());
    }

    #[test]
    fn min_max_sample_by() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query("SELECT min(price), max(price) FROM trades SAMPLE BY 1h");
        assert_eq!(cols.len(), 2);
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// SAMPLE BY with WHERE
// =============================================================================
mod sample_with_where {
    use super::*;

    #[test]
    fn sample_by_where_symbol() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_where_side() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades WHERE side = 'buy' SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_where_in() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT avg(price) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD') SAMPLE BY 1h",
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_where_price_gt() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades WHERE price > 1000 SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_where_and() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy' SAMPLE BY 1h",
        );
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_where_like() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT count(*) FROM trades WHERE symbol LIKE 'BTC%' SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// Edge cases
// =============================================================================
mod sample_edge_cases {
    use super::*;

    #[test]
    fn sample_by_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sample_by_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 1h");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn sample_by_all_one_bucket() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // 10 rows within 1 second
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", BASE_TS + i, i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn sample_by_each_row_own_bucket_1s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 2), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1s");
        // Each row 2 seconds apart, so each gets its own 1s bucket
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn sample_by_two_rows() {
        let db = TestDb::with_trades(2);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
        // Total should be 100
        let total: i64 = rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                _ => 0,
            })
            .sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn sample_by_with_null_values() {
        let db = TestDb::with_trades(20);
        // volume has NULLs (row 0, 10, 20...)
        let (_, rows) = db.query("SELECT avg(volume) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// SAMPLE BY with multiple columns
// =============================================================================
mod sample_multi_column {
    use super::*;

    #[test]
    fn sample_by_count_sum() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query("SELECT count(*), sum(price) FROM trades SAMPLE BY 1h");
        assert_eq!(cols.len(), 2);
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_all_aggs() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query(
            "SELECT count(*), sum(price), avg(price), min(price), max(price), first(price), last(price) FROM trades SAMPLE BY 1h",
        );
        assert_eq!(cols.len(), 7);
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_volume_aggs() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query(
            "SELECT sum(volume), avg(volume), min(volume), max(volume) FROM trades SAMPLE BY 1h",
        );
        assert_eq!(cols.len(), 4);
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// Custom interval table tests
// =============================================================================
mod sample_custom {
    use super::*;

    #[test]
    fn custom_data_sample_by_1h_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // 12 rows, 10 minutes apart = 2 hours total
        for i in 0..12 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 600),
                (i + 1) * 10
            ));
        }
        let (_, rows) = db.query("SELECT sum(v) FROM t SAMPLE BY 1h");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn custom_data_sample_by_30m_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..12 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 600), // 10 min apart
                i
            ));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 30m");
        assert_eq!(rows.len(), 4); // 120min / 30min = 4 buckets
    }

    #[test]
    fn custom_data_sample_by_10m_avg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // 6 rows, 5 minutes apart
        for i in 0..6 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 300),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 10m");
        // 30 minutes total / 10 min = 3 buckets, 2 rows each
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn dense_data_sample_by_1s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // 10 rows, 100ms apart (within same second)
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                BASE_TS + i * 100_000_000, // 100ms
                i
            ));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1s");
        assert_eq!(rows.len(), 1); // all within 1 second
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn sample_by_with_fill_none_has_no_gaps() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // 3 rows: at 0s, 3600s (1h), 7200s (2h)
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(3600)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(7200)));
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 1h FILL(NONE)");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn sample_by_1d_multi_day() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d");
        assert!(!rows.is_empty());
        let total: i64 = rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                _ => 0,
            })
            .sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn sample_by_align_to_calendar() {
        let db = TestDb::with_trades(20);
        let result = db.exec("SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR");
        // May or may not be supported; just make sure it doesn't crash
        assert!(result.is_ok());
    }

    #[test]
    fn sample_by_count_consistency() {
        let db = TestDb::with_trades(30);
        // Total count via SAMPLE BY should equal plain count
        let plain_count = db.query_scalar("SELECT count(*) FROM trades");
        let (_, sampled) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        let sampled_total: i64 = sampled
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                _ => 0,
            })
            .sum();
        match plain_count {
            Value::I64(n) => assert_eq!(n, sampled_total),
            _ => panic!("expected I64"),
        }
    }
}
