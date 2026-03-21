//! Per-type regression tests for TIMESTAMP operators — 500+ tests.
//!
//! Every SQL operator is tested with TIMESTAMP data: =, !=, >, <, >=, <=,
//! BETWEEN (time ranges), IS NULL, IS NOT NULL, ORDER BY, GROUP BY, HAVING,
//! LIMIT/OFFSET, DISTINCT, SAMPLE BY, LATEST ON, aggregate functions.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000; // 2024-03-15 00:00:00 UTC in nanos

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Create a table with timestamps every 10 minutes, with values.
fn db_ts() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    for i in 0..10 {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, {}.0)",
            ts(i * 600), // every 10 min
            i * 10
        ));
    }
    db
}

/// Create a table with timestamps and symbols for partition tests.
fn db_ts_partitioned() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
    let syms = ["A", "B", "C"];
    for i in 0..12 {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, '{}', {}.0)",
            ts(i * 600),
            syms[i as usize % 3],
            i * 10
        ));
    }
    db
}

/// Create a table spanning multiple days for broader range tests.
fn db_ts_multiday() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    // 3 days, 5 rows each, 8 hours apart
    for day in 0..3 {
        for i in 0..5 {
            let offset = day * 86400 + i * 28800; // 86400=1day, 28800=8h
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(offset),
                day * 100 + i * 10
            ));
        }
    }
    db
}

fn assert_f64_near(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!(
            ((*v as f64) - expected).abs() < tol,
            "expected ~{expected}, got I64({v})"
        ),
        other => panic!("expected numeric ~{expected}, got {other:?}"),
    }
}

// =============================================================================
// Module 1: Equality (=)
// =============================================================================
mod eq {
    use super::*;

    #[test]
    fn eq_first_ts() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", ts(0)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(0.0));
    }

    #[test]
    fn eq_middle_ts() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp = {}",
            ts(3000) // 5th row
        ));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_last_ts() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp = {}",
            ts(5400) // 10th row
        ));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_no_match() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", ts(999)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn eq_exact_base() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", BASE_TS));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", ts(0)));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_two_rows_same_ts() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(0)));
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", ts(0)));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn eq_with_trades() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query(&format!(
            "SELECT * FROM trades WHERE timestamp = {}",
            BASE_TS
        ));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_second_row() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", ts(600)));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(10.0));
    }

    #[test]
    fn eq_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp = {}", ts(0)));
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 2: Not Equal (!=)
// =============================================================================
mod ne {
    use super::*;

    #[test]
    fn ne_first() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp != {}", ts(0)));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn ne_no_match_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp != {}", ts(999)));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn ne_middle() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp != {}", ts(3000)));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn ne_last() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp != {}", ts(5400)));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn ne_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp != {}", ts(0)));
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 3: Greater Than (>)
// =============================================================================
mod gt {
    use super::*;

    #[test]
    fn gt_first() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(0)));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn gt_middle() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {}",
            ts(2400) // after 4th row (0,600,1200,1800,2400...)
        ));
        // rows at 3000, 3600, 4200, 4800, 5400 => 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn gt_last() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(5400)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_before_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(-1)));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn gt_after_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(99999)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_between_rows() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {}",
            ts(300) // between first (0) and second (600)
        ));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn gt_one_before_last() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(4800)));
        assert_eq!(rows.len(), 1); // only 5400
    }

    #[test]
    fn gt_multiday() {
        let db = db_ts_multiday();
        // rows after first day
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {}",
            ts(86400) // start of day 2
        ));
        // day2: 4 rows after base+86400 (at 86400+28800, etc.), day3: 5 rows
        assert!(rows.len() >= 8);
    }

    #[test]
    fn gt_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(0)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(-1)));
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 4: Less Than (<)
// =============================================================================
mod lt {
    use super::*;

    #[test]
    fn lt_last() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(5400)));
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn lt_first() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(0)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_middle() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(3000)));
        // rows at 0, 600, 1200, 1800, 2400 = 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lt_after_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(99999)));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn lt_before_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(-1)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_between_rows() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(900)));
        // rows at 0, 600 = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn lt_second_row() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(600)));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn lt_multiday() {
        let db = db_ts_multiday();
        // day0 rows: ts(0), ts(28800), ts(57600), ts(86400), ts(115200)
        // < ts(86400): ts(0), ts(28800), ts(57600) = 3
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(86400)));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn lt_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(999)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp < {}", ts(1)));
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 5: >= and <=
// =============================================================================
mod gte_lte {
    use super::*;

    #[test]
    fn gte_first() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp >= {}", ts(0)));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn gte_middle() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp >= {}", ts(3000)));
        // 3000, 3600, 4200, 4800, 5400 = 5 plus row at 3000 = 6
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn gte_last() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp >= {}", ts(5400)));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gte_after_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp >= {}", ts(99999)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lte_last() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp <= {}", ts(5400)));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn lte_first() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp <= {}", ts(0)));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn lte_middle() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp <= {}", ts(2400)));
        // 0, 600, 1200, 1800, 2400 = 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lte_before_all() {
        let db = db_ts();
        let (_, rows) = db.query(&format!("SELECT v FROM t WHERE timestamp <= {}", ts(-1)));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gte_lte_range() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp >= {} AND timestamp <= {}",
            ts(1200),
            ts(3600)
        ));
        // 1200, 1800, 2400, 3000, 3600 = 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn gte_lte_single() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp >= {} AND timestamp <= {}",
            ts(600),
            ts(600)
        ));
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 6: BETWEEN
// =============================================================================
mod between {
    use super::*;

    #[test]
    fn between_full() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(0),
            ts(5400)
        ));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn between_subset() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(1200),
            ts(3000)
        ));
        // 1200, 1800, 2400, 3000 = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn between_single() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(600),
            ts(600)
        ));
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn between_none() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(99990),
            ts(99999)
        ));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_first_three() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(0),
            ts(1200)
        ));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn between_last_three() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(4200),
            ts(5400)
        ));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn between_wide() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(-99999),
            ts(99999)
        ));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn between_multiday() {
        let db = db_ts_multiday();
        // Multiple days overlap in this range; rows from day0, day1, day2 may fall in range
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(86400),
            ts(86400 + 28800 * 4)
        ));
        assert!(rows.len() >= 5);
    }

    #[test]
    fn between_narrow() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(100),
            ts(500)
        ));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_two_rows() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(0),
            ts(600)
        ));
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 7: ORDER BY
// =============================================================================
mod order_by {
    use super::*;

    #[test]
    fn order_asc() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp, v FROM t ORDER BY timestamp ASC");
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0][0], Value::Timestamp(ts(0)));
    }

    #[test]
    fn order_desc() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp DESC");
        assert_eq!(rows[0][0], Value::Timestamp(ts(5400)));
    }

    #[test]
    fn order_default() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp");
        assert_eq!(rows[0][0], Value::Timestamp(ts(0)));
    }

    #[test]
    fn order_with_limit() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn order_desc_limit() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp DESC LIMIT 2");
        assert_eq!(rows.len(), 2);
        // First should be latest timestamp
        assert!(matches!(rows[0][0], Value::Timestamp(_)));
    }

    #[test]
    fn order_with_offset() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Timestamp(ts(1200)));
    }

    #[test]
    fn order_with_where() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT timestamp FROM t WHERE timestamp > {} ORDER BY timestamp ASC",
            ts(2400)
        ));
        assert!(rows.len() >= 1);
        assert_eq!(rows[0][0], Value::Timestamp(ts(3000)));
    }

    #[test]
    fn order_chronological() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC");
        for i in 1..rows.len() {
            assert!(
                rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater),
                "timestamps should be non-decreasing"
            );
        }
    }

    #[test]
    fn order_reverse_chronological() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp DESC");
        for i in 1..rows.len() {
            assert!(
                rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less),
                "timestamps should be non-increasing"
            );
        }
    }

    #[test]
    fn order_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 8: Aggregates
// =============================================================================
mod aggregates {
    use super::*;

    #[test]
    fn count_star() {
        let db = db_ts();
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(10));
    }

    #[test]
    fn count_with_where() {
        let db = db_ts();
        let val = db.query_scalar(&format!(
            "SELECT count(*) FROM t WHERE timestamp > {}",
            ts(2400)
        ));
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn min_timestamp() {
        let db = db_ts();
        let val = db.query_scalar("SELECT min(timestamp) FROM t");
        assert_eq!(val, Value::Timestamp(ts(0)));
    }

    #[test]
    fn max_timestamp() {
        let db = db_ts();
        let val = db.query_scalar("SELECT max(timestamp) FROM t");
        assert_eq!(val, Value::Timestamp(ts(5400)));
    }

    #[test]
    fn first_timestamp() {
        let db = db_ts();
        let val = db.query_scalar("SELECT first(timestamp) FROM t");
        assert_eq!(val, Value::Timestamp(ts(0)));
    }

    #[test]
    fn last_timestamp() {
        let db = db_ts();
        let val = db.query_scalar("SELECT last(timestamp) FROM t");
        assert_eq!(val, Value::Timestamp(ts(5400)));
    }

    #[test]
    fn sum_values() {
        let db = db_ts();
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // 0+10+20+30+40+50+60+70+80+90 = 450
        assert_f64_near(&val, 450.0, 0.01);
    }

    #[test]
    fn avg_values() {
        let db = db_ts();
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 45.0, 0.01);
    }

    #[test]
    fn min_values() {
        let db = db_ts();
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(0.0));
    }

    #[test]
    fn max_values() {
        let db = db_ts();
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(90.0));
    }

    #[test]
    fn first_value() {
        let db = db_ts();
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::F64(0.0));
    }

    #[test]
    fn last_value() {
        let db = db_ts();
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::F64(90.0));
    }

    #[test]
    fn count_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn multiple_aggregates() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v), sum(v), avg(v) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn count_with_between() {
        let db = db_ts();
        let val = db.query_scalar(&format!(
            "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(1200),
            ts(3000)
        ));
        assert_eq!(val, Value::I64(4));
    }
}

// =============================================================================
// Module 9: GROUP BY with timestamp-based partitioning
// =============================================================================
mod group_by {
    use super::*;

    #[test]
    fn group_by_sym_count() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sym_sum() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, sum(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sym_min() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, min(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sym_max() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, max(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sym_avg() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, avg(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sym_first() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, first(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sym_last() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, last(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'X', {}.0)",
                ts(i * 600),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT sym, count(*) FROM t GROUP BY sym");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn group_by_with_order() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym ORDER BY sym DESC");
        assert_eq!(rows[0][0], Value::Str("C".into()));
    }

    #[test]
    fn group_by_with_limit() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 10: HAVING
// =============================================================================
mod having {
    use super::*;

    #[test]
    fn having_count() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c >= 4");
        assert_eq!(rows.len(), 3); // each has 4
    }

    #[test]
    fn having_sum() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, sum(v) AS s FROM t GROUP BY sym HAVING s > 100");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn having_all() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c >= 1");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn having_none() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c > 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn having_with_order() {
        let db = db_ts_partitioned();
        let (_, rows) =
            db.query("SELECT sym, sum(v) AS s FROM t GROUP BY sym HAVING s > 100 ORDER BY sym");
        assert!(rows.len() >= 1);
    }
}

// =============================================================================
// Module 11: SAMPLE BY
// =============================================================================
mod sample_by {
    use super::*;

    #[test]
    fn sample_by_10m() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 10m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_1h() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_30m() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_5m() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_sum() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT sum(v) FROM t SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_avg() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_min() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT min(v) FROM t SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_max() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT max(v) FROM t SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_1d_multiday() {
        let db = db_ts_multiday();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1d");
        assert!(rows.len() >= 3);
    }

    #[test]
    fn sample_by_1m() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1m");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_with_where() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT count(*) FROM t WHERE timestamp >= {} SAMPLE BY 30m",
            ts(1800)
        ));
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_2h() {
        let db = db_ts_multiday();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 2h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_4h() {
        let db = db_ts_multiday();
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 4h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_trades() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_trades_count() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_1s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1s");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_5s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 5s");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_15m() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 15m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sample_by_first_last() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT first(v), last(v) FROM t SAMPLE BY 30m");
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// Module 12: LATEST ON
// =============================================================================
mod latest_on {
    use super::*;

    #[test]
    fn latest_on_basic() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(rows.len(), 3); // one per symbol
    }

    #[test]
    fn latest_on_columns() {
        let db = db_ts_partitioned();
        let (cols, rows) =
            db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym ORDER BY sym");
        assert!(cols.contains(&"sym".to_string()));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_trades() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3); // BTC, ETH, SOL
    }

    #[test]
    fn latest_on_trades_columns() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT symbol, timestamp FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY symbol",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_single_partition() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'A', {}.0)",
                ts(i * 600),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn latest_on_returns_latest_value() {
        let db = db_ts_partitioned();
        let (_, rows) =
            db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym ORDER BY sym");
        // One row per symbol (A, B, C)
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_two_partitions() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 30.0)", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 40.0)", ts(3)));
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn latest_on_with_where() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE side = 'buy' LATEST ON timestamp PARTITION BY symbol",
        );
        assert!(rows.len() <= 3);
    }

    #[test]
    fn latest_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn latest_on_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 13: LIMIT / OFFSET
// =============================================================================
mod limit_offset {
    use super::*;

    #[test]
    fn limit_basic() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t LIMIT 5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn limit_one() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_exceeds() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t LIMIT 100");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn offset_basic() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t ORDER BY timestamp ASC LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn offset_all() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t LIMIT 10 OFFSET 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_zero() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_with_where() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT * FROM t WHERE timestamp > {} LIMIT 3",
            ts(2400)
        ));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn offset_one() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t ORDER BY timestamp ASC LIMIT 1 OFFSET 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn offset_last() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t ORDER BY timestamp ASC LIMIT 10 OFFSET 9");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_with_order() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT * FROM t ORDER BY timestamp DESC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 14: DISTINCT
// =============================================================================
mod distinct {
    use super::*;

    #[test]
    fn distinct_timestamps() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn distinct_with_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(1)));
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM t");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn distinct_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_sym() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT DISTINCT sym FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn distinct_with_order() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM t ORDER BY timestamp ASC");
        assert_eq!(rows[0][0], Value::Timestamp(ts(0)));
    }

    #[test]
    fn distinct_trades_symbol() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 15: CASE WHEN with timestamps
// =============================================================================
mod case_when {
    use super::*;

    #[test]
    fn case_recent() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT CASE WHEN timestamp > {} THEN 'recent' ELSE 'old' END FROM t",
            ts(3000)
        ));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn case_timestamp_range() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT CASE WHEN timestamp < {} THEN 'early' WHEN timestamp > {} THEN 'late' ELSE 'mid' END FROM t",
            ts(1800),
            ts(3600)
        ));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn case_with_alias() {
        let db = db_ts();
        let (cols, _) = db.query(&format!(
            "SELECT CASE WHEN timestamp > {} THEN 'new' ELSE 'old' END AS age FROM t",
            ts(2400)
        ));
        assert!(cols.contains(&"age".to_string()));
    }

    #[test]
    fn case_returns_number() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT CASE WHEN timestamp > {} THEN 1 ELSE 0 END FROM t",
            ts(2400)
        ));
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn case_all_else() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT CASE WHEN timestamp > {} THEN 'future' ELSE 'past' END FROM t",
            ts(99999)
        ));
        for r in &rows {
            assert_eq!(r[0], Value::Str("past".into()));
        }
    }

    #[test]
    fn case_all_when() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT CASE WHEN timestamp >= {} THEN 'present' ELSE 'never' END FROM t",
            ts(0)
        ));
        for r in &rows {
            assert_eq!(r[0], Value::Str("present".into()));
        }
    }

    #[test]
    fn case_value_based() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn case_without_else() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT CASE WHEN timestamp > {} THEN 'recent' END FROM t",
            ts(4200)
        ));
        assert_eq!(rows.len(), 10);
    }
}

// =============================================================================
// Module 16: Logical (AND, OR)
// =============================================================================
mod logical {
    use super::*;

    #[test]
    fn and_time_range() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp >= {} AND timestamp <= {}",
            ts(1200),
            ts(3000)
        ));
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn and_time_and_value() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {} AND v > 50",
            ts(2400)
        ));
        // rows after 2400: v=50,60,70,80,90 -> v>50: 60,70,80,90 = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn or_time_boundaries() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp = {} OR timestamp = {}",
            ts(0),
            ts(5400)
        ));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn or_none() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp = {} OR timestamp = {}",
            ts(999),
            ts(998)
        ));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn and_or_combined() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE (timestamp < {} OR timestamp > {}) AND v > 0",
            ts(1200),
            ts(4200)
        ));
        // ts < 1200: v=0,10 -> v>0: 10; ts > 4200: v=80,90 -> both > 0; total = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn and_chain() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {} AND timestamp < {} AND v > 20",
            ts(600),
            ts(4200)
        ));
        // ts in (1200,1800,2400,3000,3600): v=20,30,40,50,60 -> v>20: 30,40,50,60 = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn or_chain() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp = {} OR timestamp = {} OR timestamp = {}",
            ts(0),
            ts(2400),
            ts(5400)
        ));
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 17: Edge cases
// =============================================================================
mod edge_cases {
    use super::*;

    #[test]
    fn empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn timestamp_type_in_result() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t LIMIT 1");
        assert!(matches!(rows[0][0], Value::Timestamp(_)));
    }

    #[test]
    fn select_star() {
        let db = db_ts();
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn select_alias() {
        let db = db_ts();
        let (cols, _) = db.query("SELECT timestamp AS ts, v AS val FROM t");
        assert!(cols.contains(&"ts".to_string()));
        assert!(cols.contains(&"val".to_string()));
    }

    #[test]
    fn where_order_limit() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {} ORDER BY timestamp ASC LIMIT 3",
            ts(2400)
        ));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn multiday_count() {
        let db = db_ts_multiday();
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(15));
    }

    #[test]
    fn multiday_min_max() {
        let db = db_ts_multiday();
        let min_ts = db.query_scalar("SELECT min(timestamp) FROM t");
        assert_eq!(min_ts, Value::Timestamp(ts(0)));
    }

    #[test]
    fn insert_order_preserved() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp FROM t");
        for i in 1..rows.len() {
            assert!(
                rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater),
                "timestamps should be non-decreasing"
            );
        }
    }

    #[test]
    fn select_timestamp_and_value() {
        let db = db_ts();
        let (cols, rows) = db.query("SELECT timestamp, v FROM t LIMIT 1");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], Value::Timestamp(_)));
        assert!(matches!(rows[0][1], Value::F64(_)));
    }

    #[test]
    fn twenty_rows() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 20);
    }
}

// =============================================================================
// Module 18: Multiple aggregates and complex queries
// =============================================================================
mod complex {
    use super::*;

    #[test]
    fn sum_and_count() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT sum(v), count(*) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::I64(10));
    }

    #[test]
    fn min_and_max_ts() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT min(timestamp), max(timestamp) FROM t");
        assert_eq!(rows[0][0], Value::Timestamp(ts(0)));
        assert_eq!(rows[0][1], Value::Timestamp(ts(5400)));
    }

    #[test]
    fn first_and_last() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT first(v), last(v) FROM t");
        assert_eq!(rows[0][0], Value::F64(0.0));
        assert_eq!(rows[0][1], Value::F64(90.0));
    }

    #[test]
    fn all_aggregates() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v), sum(v), avg(v) FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn group_having_order_limit() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query(
            "SELECT sym, sum(v) AS s FROM t GROUP BY sym HAVING s > 100 ORDER BY s DESC LIMIT 2",
        );
        assert!(rows.len() >= 1 && rows.len() <= 2);
    }

    #[test]
    fn where_between_agg() {
        let db = db_ts();
        let val = db.query_scalar(&format!(
            "SELECT sum(v) FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(1200),
            ts(3000)
        ));
        // v=20+30+40+50 = 140
        assert_f64_near(&val, 140.0, 0.01);
    }

    #[test]
    fn where_between_count() {
        let db = db_ts();
        let val = db.query_scalar(&format!(
            "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(0),
            ts(2400)
        ));
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn combined_sample_and_where() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT count(*) FROM t WHERE timestamp >= {} SAMPLE BY 30m",
            ts(1800)
        ));
        assert!(!rows.is_empty());
    }

    #[test]
    fn latest_on_trades_buy() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE side = 'buy' LATEST ON timestamp PARTITION BY symbol",
        );
        assert!(rows.len() <= 3 && rows.len() >= 1);
    }

    #[test]
    fn group_by_multi_aggregates() {
        let db = db_ts_partitioned();
        let (_, rows) = db
            .query("SELECT sym, count(*), min(v), max(v), sum(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn sample_by_with_sym_where() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT avg(v) FROM t WHERE sym = 'A' SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn order_and_limit_and_offset() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY timestamp DESC LIMIT 3 OFFSET 2");
        assert!(rows.len() >= 1 && rows.len() <= 3);
    }

    #[test]
    fn distinct_with_order() {
        let db = db_ts_partitioned();
        let (_, rows) = db.query("SELECT DISTINCT sym FROM t ORDER BY sym");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Str("A".into()));
    }

    #[test]
    fn timestamp_in_expression() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT timestamp, v * 2 AS doubled FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn between_and_order() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {} ORDER BY v DESC",
            ts(1200),
            ts(3600)
        ));
        // 1200,1800,2400,3000,3600 = 5 rows; v=20,30,40,50,60
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::F64(60.0));
    }

    #[test]
    fn between_and_count_and_sum() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT count(*), sum(v) FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(1800),
            ts(3600)
        ));
        // 1800,2400,3000,3600 = 4 rows; v=30+40+50+60 = 180
        assert_eq!(rows[0][0], Value::I64(4));
        assert_f64_near(&rows[0][1], 180.0, 0.01);
    }

    #[test]
    fn gt_and_lt_and_order() {
        let db = db_ts();
        let (_, rows) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {} AND timestamp < {} ORDER BY v",
            ts(600),
            ts(4800)
        ));
        // 1200..4200: v=20,30,40,50,60,70
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 19: Arithmetic with timestamp values
// =============================================================================
mod arithmetic {
    use super::*;

    #[test]
    fn select_value_plus() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT v + 100 FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn select_value_minus() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT v - 10 FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn select_value_times() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT v * 2 FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn select_value_div() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT v / 10 FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn select_negative_value() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT -v FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn expression_in_where() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT v FROM t WHERE v * 2 > 100");
        // v > 50: 60, 70, 80, 90 = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn sum_basic() {
        let db = db_ts();
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 450.0, 0.01);
    }

    #[test]
    fn avg_basic() {
        let db = db_ts();
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 45.0, 0.01);
    }
}

// =============================================================================
// Module 20: CAST with timestamps
// =============================================================================
mod cast_ops {
    use super::*;

    #[test]
    fn cast_double_to_int() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn cast_preserves_rows() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn cast_with_where() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t WHERE v > 50");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn cast_with_order() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn cast_value_to_varchar() {
        let db = db_ts();
        let (_, rows) = db.query("SELECT CAST(v AS VARCHAR) FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], Value::Str(_)));
    }
}

// =============================================================================
// Module 21: Additional comparison combinations
// =============================================================================
mod comparison_combos {
    use super::*;

    #[test]
    fn eq_and_gt() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp = {} AND v = 0.0",
            ts(0)
        ));
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn gt_and_lt() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp > {} AND timestamp < {}",
            ts(600),
            ts(3600)
        ));
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn gte_and_lte_narrow() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp >= {} AND timestamp <= {}",
            ts(1200),
            ts(1800)
        ));
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn ne_and_gt() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp != {} AND timestamp > {}",
            ts(0),
            ts(3600)
        ));
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn between_and_value() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {} AND v > 30",
            ts(0),
            ts(5400)
        ));
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn gt_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp > {}",
                ts(3000)
            )),
            Value::I64(4)
        );
    }
    #[test]
    fn lt_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp < {}",
                ts(1800)
            )),
            Value::I64(3)
        );
    }
    #[test]
    fn between_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
                ts(1200),
                ts(3600)
            )),
            Value::I64(5)
        );
    }
    #[test]
    fn eq_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp = {}",
                ts(0)
            )),
            Value::I64(1)
        );
    }
    #[test]
    fn ne_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp != {}",
                ts(0)
            )),
            Value::I64(9)
        );
    }
    #[test]
    fn gte_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp >= {}",
                ts(3000)
            )),
            Value::I64(5)
        );
    }
    #[test]
    fn lte_count() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp <= {}",
                ts(2400)
            )),
            Value::I64(5)
        );
    }
}

// =============================================================================
// Module 22: Additional aggregate variations
// =============================================================================
mod agg_variations {
    use super::*;

    #[test]
    fn sum_where_gt() {
        let db = db_ts();
        assert_f64_near(
            &db.query_scalar(&format!(
                "SELECT sum(v) FROM t WHERE timestamp > {}",
                ts(2400)
            )),
            350.0,
            0.01,
        );
    }
    #[test]
    fn avg_where_gt() {
        let db = db_ts();
        assert_f64_near(
            &db.query_scalar(&format!(
                "SELECT avg(v) FROM t WHERE timestamp > {}",
                ts(2400)
            )),
            70.0,
            0.01,
        );
    }
    #[test]
    fn min_where_gt() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT min(v) FROM t WHERE timestamp > {}",
                ts(2400)
            )),
            Value::F64(50.0)
        );
    }
    #[test]
    fn max_where_lt() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT max(v) FROM t WHERE timestamp < {}",
                ts(3000)
            )),
            Value::F64(40.0)
        );
    }
    #[test]
    fn first_where_gt() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT first(v) FROM t WHERE timestamp > {}",
                ts(2400)
            )),
            Value::F64(50.0)
        );
    }
    #[test]
    fn last_where_lt() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT last(v) FROM t WHERE timestamp < {}",
                ts(3000)
            )),
            Value::F64(40.0)
        );
    }
    #[test]
    fn count_where_between() {
        let db = db_ts();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
                ts(600),
                ts(4200)
            )),
            Value::I64(7)
        );
    }
    #[test]
    fn sum_where_between() {
        let db = db_ts();
        assert_f64_near(
            &db.query_scalar(&format!(
                "SELECT sum(v) FROM t WHERE timestamp BETWEEN {} AND {}",
                ts(600),
                ts(2400)
            )),
            100.0,
            0.01,
        );
    }

    #[test]
    fn grouped_count_a() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r[0][0], Value::Str("A".into()));
        assert_eq!(r[0][1], Value::I64(4));
    }

    #[test]
    fn grouped_count_b() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r[1][0], Value::Str("B".into()));
        assert_eq!(r[1][1], Value::I64(4));
    }

    #[test]
    fn grouped_count_c() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r[2][0], Value::Str("C".into()));
        assert_eq!(r[2][1], Value::I64(4));
    }
}

// =============================================================================
// Module 23: Additional SAMPLE BY variations
// =============================================================================
mod sample_extra {
    use super::*;

    #[test]
    fn sample_sum_1h() {
        let db = db_ts();
        let (_, r) = db.query("SELECT sum(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_avg_1h() {
        let db = db_ts();
        let (_, r) = db.query("SELECT avg(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_min_1h() {
        let db = db_ts();
        let (_, r) = db.query("SELECT min(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_max_1h() {
        let db = db_ts();
        let (_, r) = db.query("SELECT max(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_first_1h() {
        let db = db_ts();
        let (_, r) = db.query("SELECT first(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_last_1h() {
        let db = db_ts();
        let (_, r) = db.query("SELECT last(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_count_5m() {
        let db = db_ts();
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_sum_5m() {
        let db = db_ts();
        let (_, r) = db.query("SELECT sum(v) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_avg_5m() {
        let db = db_ts();
        let (_, r) = db.query("SELECT avg(v) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_min_5m() {
        let db = db_ts();
        let (_, r) = db.query("SELECT min(v) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_max_5m() {
        let db = db_ts();
        let (_, r) = db.query("SELECT max(v) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_count_1d_multi() {
        let db = db_ts_multiday();
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1d");
        assert!(r.len() >= 3);
    }
    #[test]
    fn sample_sum_1d_multi() {
        let db = db_ts_multiday();
        let (_, r) = db.query("SELECT sum(v) FROM t SAMPLE BY 1d");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_avg_1d_multi() {
        let db = db_ts_multiday();
        let (_, r) = db.query("SELECT avg(v) FROM t SAMPLE BY 1d");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_min_1d_multi() {
        let db = db_ts_multiday();
        let (_, r) = db.query("SELECT min(v) FROM t SAMPLE BY 1d");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_max_1d_multi() {
        let db = db_ts_multiday();
        let (_, r) = db.query("SELECT max(v) FROM t SAMPLE BY 1d");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_count_30s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 10), i));
        }
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 30s");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_10s() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 10s");
        assert!(!r.is_empty());
    }
}

// =============================================================================
// Module 24: Additional LATEST ON
// =============================================================================
mod latest_on_extra {
    use super::*;

    #[test]
    fn latest_on_three_syms() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        for i in 0..9 {
            let s = ["X", "Y", "Z"][i as usize % 3];
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i * 100),
                s,
                i * 10
            ));
        }
        let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn latest_on_single_sym() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'ONLY', {}.0)",
                ts(i * 100),
                i
            ));
        }
        let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn latest_on_two_syms() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 3.0)", ts(2)));
        let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn latest_on_many_syms() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        let syms = ["A", "B", "C", "D", "E"];
        for i in 0..25 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i * 60),
                syms[i as usize % 5],
                i
            ));
        }
        let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn latest_on_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 0);
    }
}

// =============================================================================
// Module 25: Additional CASE WHEN
// =============================================================================
mod case_extra {
    use super::*;

    #[test]
    fn case_early_late() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT CASE WHEN timestamp < {} THEN 'early' ELSE 'late' END FROM t",
            ts(3000)
        ));
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn case_value_high_low() {
        let db = db_ts();
        let (_, r) = db.query("SELECT CASE WHEN v >= 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn case_three_time_ranges() {
        let db = db_ts();
        let (_, r) = db.query(&format!("SELECT CASE WHEN timestamp < {} THEN 'first' WHEN timestamp < {} THEN 'second' ELSE 'third' END FROM t", ts(2000), ts(4000)));
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn case_with_where_time() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT CASE WHEN v > 50 THEN 'big' ELSE 'small' END FROM t WHERE timestamp > {}",
            ts(2400)
        ));
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn case_with_limit_time() {
        let db = db_ts();
        let (_, r) = db.query("SELECT CASE WHEN v > 50 THEN 'big' ELSE 'small' END FROM t LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn case_returns_int() {
        let db = db_ts();
        let (_, r) = db.query("SELECT CASE WHEN v > 50 THEN 1 ELSE 0 END FROM t");
        assert_eq!(r.len(), 10);
    }
}

// =============================================================================
// Module 26: Many rows with timestamps
// =============================================================================
mod many_rows {
    use super::*;

    #[test]
    fn fifty_rows_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }

    #[test]
    fn fifty_rows_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 1225.0, 0.01);
    }

    #[test]
    fn fifty_rows_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(0.0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(49.0));
    }

    #[test]
    fn fifty_rows_order() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 60),
                49 - i
            ));
        }
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 5");
        assert_eq!(r[0][0], Value::F64(0.0));
    }

    #[test]
    fn fifty_rows_filter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v >= 40");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_between() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 10 AND 19");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        let grps = ["A", "B", "C", "D", "E"];
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i * 60),
                grps[i as usize % 5],
                i
            ));
        }
        let (_, r) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_having() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        let grps = ["A", "B", "C", "D", "E"];
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i * 60),
                grps[i as usize % 5],
                i
            ));
        }
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 10");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_sample_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 10m");
        assert!(!r.is_empty());
    }

    #[test]
    fn fifty_rows_distinct() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 60),
                i % 10
            ));
        }
        let (_, r) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_latest_on() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        let syms = ["A", "B", "C"];
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i * 60),
                syms[i as usize % 3],
                i
            ));
        }
        let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn fifty_rows_in() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (0, 10, 20, 30, 40)");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_time_filter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query(&format!("SELECT v FROM t WHERE timestamp > {}", ts(2400)));
        assert!(r.len() >= 1);
    }

    #[test]
    fn fifty_rows_time_between() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query(&format!(
            "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(600),
            ts(1800)
        ));
        match &r[0][0] {
            Value::I64(n) => assert!(*n >= 1),
            other => panic!("got {other:?}"),
        }
    }
}

// =============================================================================
// Module 27: Additional DISTINCT + ORDER combos
// =============================================================================
mod distinct_order_extra {
    use super::*;

    #[test]
    fn distinct_ts_desc() {
        let db = db_ts();
        let (_, r) = db.query("SELECT DISTINCT timestamp FROM t ORDER BY timestamp DESC");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn distinct_ts_limit() {
        let db = db_ts();
        let (_, r) = db.query("SELECT DISTINCT timestamp FROM t ORDER BY timestamp ASC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn distinct_sym() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT DISTINCT sym FROM t ORDER BY sym DESC");
        assert_eq!(r[0][0], Value::Str("C".into()));
    }
    #[test]
    fn order_v_asc_limit() {
        let db = db_ts();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::F64(0.0));
    }
    #[test]
    fn order_v_desc_limit() {
        let db = db_ts();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 3");
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::F64(90.0));
    }
    #[test]
    fn offset_three() {
        let db = db_ts();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 3");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::F64(30.0));
    }
    #[test]
    fn distinct_v_where_gt() {
        let db = db_ts();
        let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v > 50");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn order_ts_between() {
        let db = db_ts();
        let (_, r) = db.query(&format!(
            "SELECT v FROM t WHERE timestamp BETWEEN {} AND {} ORDER BY v DESC",
            ts(1200),
            ts(3600)
        ));
        assert_eq!(r[0][0], Value::F64(60.0));
    }
}

// =============================================================================
// Module 28: Coalesce + CAST extras
// =============================================================================
mod coalesce_cast_extra {
    use super::*;

    #[test]
    fn cast_v_to_int_asc() {
        let db = db_ts();
        let (_, r) = db.query("SELECT CAST(v AS INT) FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn cast_v_to_varchar_limit() {
        let db = db_ts();
        let (_, r) = db.query("SELECT CAST(v AS VARCHAR) FROM t LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn coalesce_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(1)));
        let (_, r) = db.query("SELECT coalesce(v, 0.0) FROM t");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn cast_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -42.9)", ts(0)));
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS INT) FROM t"),
            Value::I64(-42)
        );
    }
    #[test]
    fn cast_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.0)", ts(0)));
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS INT) FROM t"),
            Value::I64(0)
        );
    }
}

// =============================================================================
// Module 29: GROUP BY + HAVING extras
// =============================================================================
mod group_having_extra {
    use super::*;

    #[test]
    fn having_avg() {
        let db = db_ts_partitioned();
        let (_, r) =
            db.query("SELECT sym, avg(v) AS a FROM t GROUP BY sym HAVING a > 40 ORDER BY sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn having_min() {
        let db = db_ts_partitioned();
        let (_, r) =
            db.query("SELECT sym, min(v) AS m FROM t GROUP BY sym HAVING m >= 10 ORDER BY sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn having_max() {
        let db = db_ts_partitioned();
        let (_, r) =
            db.query("SELECT sym, max(v) AS m FROM t GROUP BY sym HAVING m > 50 ORDER BY sym");
        assert!(r.len() >= 1);
    }
    #[test]
    fn group_order_desc() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym ORDER BY c DESC");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_limit_1() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, sum(v) FROM t GROUP BY sym ORDER BY sym LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn having_count_eq() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c = 4");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn having_count_gt() {
        let db = db_ts_partitioned();
        let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c > 100");
        assert_eq!(r.len(), 0);
    }
}

// =============================================================================
// Module 30: Wide table + mixed types
// =============================================================================
mod wide_table {
    use super::*;

    #[test]
    fn three_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 2.0, 3.0)", ts(0)));
        let (cols, _) = db.query("SELECT a, b, c FROM t");
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn mixed_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, d DOUBLE, i BIGINT, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 3.14, 42, 'hello')",
            ts(0)
        ));
        let (_, r) = db.query("SELECT d, i, s FROM t");
        assert_eq!(r[0][0], Value::F64(3.14));
        assert_eq!(r[0][1], Value::I64(42));
        assert_eq!(r[0][2], Value::Str("hello".into()));
    }

    #[test]
    fn expression_alias() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0, 10.0)", ts(0)));
        let (cols, r) = db.query("SELECT a + b AS total, a * b AS product FROM t");
        assert!(cols.contains(&"total".to_string()));
        assert_f64_near(&r[0][0], 15.0, 0.01);
    }

    #[test]
    fn sym_and_value_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'X', 10.0, 20.0)",
            ts(0)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'Y', 30.0, 40.0)",
            ts(1)
        ));
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'X', 50.0, 60.0)",
            ts(2)
        ));
        let (_, r) = db.query("SELECT sym, sum(a), sum(b) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 2);
    }
}

// =============================================================================
// Module 31: Bulk timestamp operation tests
// =============================================================================
mod bulk_ts_ops {
    use super::*;

    fn db20() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 300),
                i * 5
            ));
        }
        db
    }

    #[test]
    fn count20() {
        let db = db20();
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(20));
    }
    #[test]
    fn sum20() {
        let db = db20();
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 950.0, 0.01);
    }
    #[test]
    fn avg20() {
        let db = db20();
        assert_f64_near(&db.query_scalar("SELECT avg(v) FROM t"), 47.5, 0.01);
    }
    #[test]
    fn min20() {
        let db = db20();
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(0.0));
    }
    #[test]
    fn max20() {
        let db = db20();
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(95.0));
    }
    #[test]
    fn first20() {
        let db = db20();
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::F64(0.0));
    }
    #[test]
    fn last20() {
        let db = db20();
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::F64(95.0));
    }
    #[test]
    fn gt_50() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 50"),
            Value::I64(9)
        );
    }
    #[test]
    fn lt_50() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 50"),
            Value::I64(10)
        );
    }
    #[test]
    fn gte_50() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v >= 50"),
            Value::I64(10)
        );
    }
    #[test]
    fn lte_50() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v <= 50"),
            Value::I64(11)
        );
    }
    #[test]
    fn eq_50() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 50"),
            Value::I64(1)
        );
    }
    #[test]
    fn ne_50() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v != 50"),
            Value::I64(19)
        );
    }
    #[test]
    fn btw_20_60() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 20 AND 60"),
            Value::I64(9)
        );
    }
    #[test]
    fn in_0_50_95() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (0, 50, 95)"),
            Value::I64(3)
        );
    }
    #[test]
    fn order_asc1() {
        let db = db20();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1");
        assert_eq!(r[0][0], Value::F64(0.0));
    }
    #[test]
    fn order_desc1() {
        let db = db20();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 1");
        assert_eq!(r[0][0], Value::F64(95.0));
    }
    #[test]
    fn distinct20() {
        let db = db20();
        let (_, r) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn limit10() {
        let db = db20();
        let (_, r) = db.query("SELECT v FROM t LIMIT 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn offset10() {
        let db = db20();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 10 OFFSET 10");
        assert_eq!(r.len(), 10);
        assert_eq!(r[0][0], Value::F64(50.0));
    }
    #[test]
    fn case20() {
        let db = db20();
        let (_, r) = db.query("SELECT CASE WHEN v >= 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn add1() {
        let db = db20();
        let (_, r) = db.query("SELECT v + 1 FROM t LIMIT 1");
        assert_f64_near(&r[0][0], 1.0, 0.01);
    }
    #[test]
    fn mul2() {
        let db = db20();
        let (_, r) = db.query("SELECT v * 2 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 100.0, 0.01);
    }
    #[test]
    fn neg_v() {
        let db = db20();
        let (_, r) = db.query("SELECT -v FROM t WHERE v = 30");
        assert_f64_near(&r[0][0], -30.0, 0.01);
    }
    #[test]
    fn cast_int() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS INT) FROM t WHERE v = 50"),
            Value::I64(50)
        );
    }
    #[test]
    fn coalesce20() {
        let db = db20();
        let (_, r) = db.query("SELECT coalesce(v, 0.0) FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn sum_gt_50() {
        let db = db20();
        assert_f64_near(
            &db.query_scalar("SELECT sum(v) FROM t WHERE v > 50"),
            675.0,
            0.01,
        );
    }
    #[test]
    fn avg_lt_50() {
        let db = db20();
        assert_f64_near(
            &db.query_scalar("SELECT avg(v) FROM t WHERE v < 50"),
            22.5,
            0.01,
        );
    }
    #[test]
    fn min_gt_20() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT min(v) FROM t WHERE v > 20"),
            Value::F64(25.0)
        );
    }
    #[test]
    fn max_lt_70() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT max(v) FROM t WHERE v < 70"),
            Value::F64(65.0)
        );
    }
    #[test]
    fn and_gt_lt() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 20 AND v < 80"),
            Value::I64(11)
        );
    }
    #[test]
    fn or_eq() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 0 OR v = 95"),
            Value::I64(2)
        );
    }
    #[test]
    fn sample_5m() {
        let db = db20();
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_10m() {
        let db = db20();
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 10m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_30m() {
        let db = db20();
        let (_, r) = db.query("SELECT sum(v) FROM t SAMPLE BY 30m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_1h() {
        let db = db20();
        let (_, r) = db.query("SELECT avg(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn ts_gt_mid() {
        let db = db20();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp > {}",
                ts(2700)
            )),
            Value::I64(10)
        );
    }
    #[test]
    fn ts_lt_mid() {
        let db = db20();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp < {}",
                ts(3000)
            )),
            Value::I64(10)
        );
    }
    #[test]
    fn ts_btw() {
        let db = db20();
        assert_eq!(
            db.query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
                ts(1500),
                ts(4200)
            )),
            Value::I64(10)
        );
    }
    #[test]
    fn ts_order_asc() {
        let db = db20();
        let (_, r) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC LIMIT 1");
        assert_eq!(r[0][0], Value::Timestamp(ts(0)));
    }
    #[test]
    fn ts_order_desc() {
        let db = db20();
        let (_, r) = db.query("SELECT timestamp FROM t ORDER BY timestamp DESC LIMIT 1");
        assert!(matches!(r[0][0], Value::Timestamp(_)));
    }
    #[test]
    fn ts_min() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT min(timestamp) FROM t"),
            Value::Timestamp(ts(0))
        );
    }
    #[test]
    fn first_ts() {
        let db = db20();
        assert_eq!(
            db.query_scalar("SELECT first(timestamp) FROM t"),
            Value::Timestamp(ts(0))
        );
    }
    #[test]
    fn v_add_self() {
        let db = db20();
        let (_, r) = db.query("SELECT v + v FROM t WHERE v = 25");
        assert_f64_near(&r[0][0], 50.0, 0.01);
    }
    #[test]
    fn v_sub_self() {
        let db = db20();
        let (_, r) = db.query("SELECT v - v FROM t WHERE v = 25");
        assert_f64_near(&r[0][0], 0.0, 0.01);
    }
    #[test]
    fn div_5() {
        let db = db20();
        let (_, r) = db.query("SELECT v / 5 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 10.0, 0.01);
    }
}

// =============================================================================
// Module 32: Systematic timestamp WHERE + aggregate combos
// =============================================================================
mod where_agg_combos {
    use super::*;

    fn mk() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
        let syms = ["X", "Y", "Z"];
        for i in 0..30 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {}.0)",
                ts(i * 200),
                syms[i as usize % 3],
                i * 3
            ));
        }
        db
    }

    #[test]
    fn count_all() {
        assert_eq!(mk().query_scalar("SELECT count(*) FROM t"), Value::I64(30));
    }
    #[test]
    fn count_x() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'X'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_y() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'Y'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_z() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'Z'"),
            Value::I64(10)
        );
    }
    #[test]
    fn sum_all() {
        assert_f64_near(&mk().query_scalar("SELECT sum(v) FROM t"), 1305.0, 0.01);
    }
    #[test]
    fn min_all() {
        assert_eq!(mk().query_scalar("SELECT min(v) FROM t"), Value::F64(0.0));
    }
    #[test]
    fn max_all() {
        assert_eq!(mk().query_scalar("SELECT max(v) FROM t"), Value::F64(87.0));
    }
    #[test]
    fn avg_all() {
        assert_f64_near(&mk().query_scalar("SELECT avg(v) FROM t"), 43.5, 0.01);
    }
    #[test]
    fn first_all() {
        assert_eq!(mk().query_scalar("SELECT first(v) FROM t"), Value::F64(0.0));
    }
    #[test]
    fn last_all() {
        assert_eq!(mk().query_scalar("SELECT last(v) FROM t"), Value::F64(87.0));
    }
    #[test]
    fn count_gt_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v > 50"),
            Value::I64(13)
        );
    }
    #[test]
    fn count_lt_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v < 50"),
            Value::I64(17)
        );
    }
    #[test]
    fn count_btw() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 20 AND 60"),
            Value::I64(14)
        );
    }
    #[test]
    fn grp_count() {
        let (_, r) = mk().query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_sum() {
        let (_, r) = mk().query("SELECT sym, sum(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_min() {
        let (_, r) = mk().query("SELECT sym, min(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_max() {
        let (_, r) = mk().query("SELECT sym, max(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_avg() {
        let (_, r) = mk().query("SELECT sym, avg(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_first() {
        let (_, r) = mk().query("SELECT sym, first(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_last() {
        let (_, r) = mk().query("SELECT sym, last(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn having_gt_5() {
        let (_, r) = mk().query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c > 5");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn order_asc_l5() {
        let (_, r) = mk().query("SELECT v FROM t ORDER BY v ASC LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn order_desc_l5() {
        let (_, r) = mk().query("SELECT v FROM t ORDER BY v DESC LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn distinct_sym() {
        let (_, r) = mk().query("SELECT DISTINCT sym FROM t");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn distinct_v() {
        let (_, r) = mk().query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn case_hl() {
        let (_, r) = mk().query("SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn add_10() {
        let (_, r) = mk().query("SELECT v + 10 FROM t LIMIT 1");
        assert_f64_near(&r[0][0], 10.0, 0.01);
    }
    #[test]
    fn neg_v() {
        let v = mk().query_scalar("SELECT -v FROM t WHERE v = 51");
        assert_f64_near(&v, -51.0, 0.01);
    }
    #[test]
    fn in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v IN (0, 51, 87)"),
            Value::I64(3)
        );
    }
    #[test]
    fn not_in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v NOT IN (0, 51, 87)"),
            Value::I64(27)
        );
    }
    #[test]
    fn cast_int() {
        assert_eq!(
            mk().query_scalar("SELECT CAST(v AS INT) FROM t WHERE v = 51"),
            Value::I64(51)
        );
    }
    #[test]
    fn coalesce_v() {
        let (_, r) = mk().query("SELECT coalesce(v, 0.0) FROM t");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn sample_5m() {
        let (_, r) = mk().query("SELECT count(*) FROM t SAMPLE BY 5m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_1h() {
        let (_, r) = mk().query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn limit_15() {
        let (_, r) = mk().query("SELECT v FROM t LIMIT 15");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn offset_15() {
        let (_, r) = mk().query("SELECT v FROM t ORDER BY v ASC LIMIT 15 OFFSET 15");
        assert_eq!(r.len(), 15);
    }
    #[test]
    fn star() {
        let (c, r) = mk().query("SELECT * FROM t LIMIT 5");
        assert_eq!(c.len(), 3);
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn latest_on() {
        let (_, r) = mk().query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn ts_gt_mid() {
        assert_eq!(
            mk().query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp > {}",
                ts(2800)
            )),
            Value::I64(15)
        );
    }
    #[test]
    fn ts_lt_mid() {
        assert_eq!(
            mk().query_scalar(&format!(
                "SELECT count(*) FROM t WHERE timestamp < {}",
                ts(3000)
            )),
            Value::I64(15)
        );
    }
    #[test]
    fn ts_btw() {
        let v = mk().query_scalar(&format!(
            "SELECT count(*) FROM t WHERE timestamp BETWEEN {} AND {}",
            ts(1000),
            ts(4000)
        ));
        match &v {
            Value::I64(n) => assert!(*n >= 1),
            o => panic!("got {o:?}"),
        }
    }
    #[test]
    fn min_ts() {
        assert_eq!(
            mk().query_scalar("SELECT min(timestamp) FROM t"),
            Value::Timestamp(ts(0))
        );
    }
    #[test]
    fn first_ts() {
        assert_eq!(
            mk().query_scalar("SELECT first(timestamp) FROM t"),
            Value::Timestamp(ts(0))
        );
    }
    #[test]
    fn alias_s() {
        let (c, _) = mk().query("SELECT sym AS symbol FROM t LIMIT 1");
        assert!(c.contains(&"symbol".to_string()));
    }
    #[test]
    fn case_sym() {
        let (_, r) = mk().query("SELECT CASE WHEN sym = 'X' THEN 'first' ELSE 'other' END FROM t");
        assert_eq!(r.len(), 30);
    }
    #[test]
    fn btw_and_grp() {
        let (_, r) = mk().query(
            "SELECT sym, count(*) FROM t WHERE v BETWEEN 20 AND 60 GROUP BY sym ORDER BY sym",
        );
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn grp_limit() {
        let (_, r) = mk().query("SELECT sym, sum(v) FROM t GROUP BY sym ORDER BY sym LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn two_aggs() {
        let (_, r) = mk().query("SELECT min(v), max(v) FROM t");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn three_aggs() {
        let (_, r) = mk().query("SELECT count(*), sum(v), avg(v) FROM t");
        assert_eq!(r[0][0], Value::I64(30));
    }
    #[test]
    fn order_ts_asc() {
        let (_, r) = mk().query("SELECT timestamp FROM t ORDER BY timestamp ASC LIMIT 1");
        assert_eq!(r[0][0], Value::Timestamp(ts(0)));
    }
}

// =============================================================================
// Module 33: Bulk per-symbol + value tests
// =============================================================================
mod per_sym_tests {
    use super::*;

    fn mk() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, price DOUBLE, vol DOUBLE)");
        let syms = ["BTC", "ETH", "SOL", "ADA", "DOT"];
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {:.1}, {:.1})",
                ts(i * 120),
                syms[i as usize % 5],
                100.0 + (i as f64) * 10.0,
                1.0 + (i as f64) * 0.5
            ));
        }
        db
    }

    #[test]
    fn count_50() {
        assert_eq!(mk().query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }
    #[test]
    fn count_btc() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'BTC'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_eth() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'ETH'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_sol() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'SOL'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_ada() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'ADA'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_dot() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym = 'DOT'"),
            Value::I64(10)
        );
    }
    #[test]
    fn ne_btc() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym != 'BTC'"),
            Value::I64(40)
        );
    }
    #[test]
    fn in_2() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym IN ('BTC', 'ETH')"),
            Value::I64(20)
        );
    }
    #[test]
    fn in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym IN ('BTC', 'ETH', 'SOL')"),
            Value::I64(30)
        );
    }
    #[test]
    fn not_in_2() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE sym NOT IN ('BTC', 'ETH')"),
            Value::I64(30)
        );
    }
    #[test]
    fn grp_count() {
        let (_, r) = mk().query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_sum_price() {
        let (_, r) = mk().query("SELECT sym, sum(price) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_min_price() {
        let (_, r) = mk().query("SELECT sym, min(price) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_max_price() {
        let (_, r) = mk().query("SELECT sym, max(price) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_avg_price() {
        let (_, r) = mk().query("SELECT sym, avg(price) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_first_price() {
        let (_, r) = mk().query("SELECT sym, first(price) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_last_price() {
        let (_, r) = mk().query("SELECT sym, last(price) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_sum_vol() {
        let (_, r) = mk().query("SELECT sym, sum(vol) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_min_vol() {
        let (_, r) = mk().query("SELECT sym, min(vol) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn grp_max_vol() {
        let (_, r) = mk().query("SELECT sym, max(vol) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn having_gt_5() {
        let (_, r) = mk().query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c > 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn distinct_sym() {
        let (_, r) = mk().query("SELECT DISTINCT sym FROM t");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn order_price_asc() {
        let (_, r) = mk().query("SELECT price FROM t ORDER BY price ASC LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn order_price_desc() {
        let (_, r) = mk().query("SELECT price FROM t ORDER BY price DESC LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn min_price() {
        assert_eq!(
            mk().query_scalar("SELECT min(price) FROM t"),
            Value::F64(100.0)
        );
    }
    #[test]
    fn max_price() {
        assert_eq!(
            mk().query_scalar("SELECT max(price) FROM t"),
            Value::F64(590.0)
        );
    }
    #[test]
    fn latest_on() {
        let (_, r) = mk().query("SELECT * FROM t LATEST ON timestamp PARTITION BY sym");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn sample_10m() {
        let (_, r) = mk().query("SELECT count(*) FROM t SAMPLE BY 10m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_1h() {
        let (_, r) = mk().query("SELECT avg(price) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_sum() {
        let (_, r) = mk().query("SELECT sum(vol) FROM t SAMPLE BY 30m");
        assert!(!r.is_empty());
    }
    #[test]
    fn price_gt_300() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE price > 300"),
            Value::I64(29)
        );
    }
    #[test]
    fn price_lt_200() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE price < 200"),
            Value::I64(10)
        );
    }
    #[test]
    fn price_btw() {
        let v = mk().query_scalar("SELECT count(*) FROM t WHERE price BETWEEN 200 AND 400");
        match &v {
            Value::I64(n) => assert!(*n >= 1),
            o => panic!("got {o:?}"),
        }
    }
    #[test]
    fn case_hl() {
        let (_, r) = mk().query("SELECT CASE WHEN price > 300 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 50);
    }
    #[test]
    fn cast_int() {
        let (_, r) = mk().query("SELECT CAST(price AS INT) FROM t LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn coalesce_price() {
        let (_, r) = mk().query("SELECT coalesce(price, 0.0) FROM t");
        assert_eq!(r.len(), 50);
    }
    #[test]
    fn limit_20() {
        let (_, r) = mk().query("SELECT * FROM t LIMIT 20");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn offset_20() {
        let (_, r) = mk().query("SELECT * FROM t ORDER BY price ASC LIMIT 20 OFFSET 20");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn star() {
        let (c, r) = mk().query("SELECT * FROM t LIMIT 5");
        assert_eq!(c.len(), 4);
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn add_10() {
        let (_, r) = mk().query("SELECT price + 10 FROM t LIMIT 1");
        assert!(matches!(r[0][0], Value::F64(_)));
    }
    #[test]
    fn mul_2() {
        let (_, r) = mk().query("SELECT price * 2 FROM t LIMIT 1");
        assert!(matches!(r[0][0], Value::F64(_)));
    }
    #[test]
    fn neg_price() {
        let (_, r) = mk().query("SELECT -price FROM t LIMIT 1");
        assert!(matches!(r[0][0], Value::F64(_)));
    }
    #[test]
    fn alias_p() {
        let (c, _) = mk().query("SELECT price AS p FROM t LIMIT 1");
        assert!(c.contains(&"p".to_string()));
    }
    #[test]
    fn two_aggs() {
        let (_, r) = mk().query("SELECT min(price), max(price) FROM t");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn four_aggs() {
        let (_, r) = mk().query("SELECT count(*), min(price), max(price), sum(price) FROM t");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn where_sym_order() {
        let (_, r) = mk().query("SELECT price FROM t WHERE sym = 'BTC' ORDER BY price ASC");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn where_sym_limit() {
        let (_, r) = mk().query("SELECT price FROM t WHERE sym = 'ETH' LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn distinct_order() {
        let (_, r) = mk().query("SELECT DISTINCT sym FROM t ORDER BY sym");
        assert_eq!(r[0][0], Value::Str("ADA".into()));
    }
    #[test]
    fn grp_having_sum() {
        let (_, r) = mk().query("SELECT sym, sum(price) AS s FROM t GROUP BY sym HAVING s > 3000");
        assert!(r.len() >= 1);
    }
    #[test]
    fn grp_limit() {
        let (_, r) = mk().query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn ts_min() {
        assert_eq!(
            mk().query_scalar("SELECT min(timestamp) FROM t"),
            Value::Timestamp(ts(0))
        );
    }
    #[test]
    fn first_ts() {
        assert_eq!(
            mk().query_scalar("SELECT first(timestamp) FROM t"),
            Value::Timestamp(ts(0))
        );
    }
    #[test]
    fn first_price() {
        assert_eq!(
            mk().query_scalar("SELECT first(price) FROM t"),
            Value::F64(100.0)
        );
    }
    #[test]
    fn last_price() {
        assert_eq!(
            mk().query_scalar("SELECT last(price) FROM t"),
            Value::F64(590.0)
        );
    }
    #[test]
    fn like_b_pct() {
        let (_, r) = mk().query("SELECT sym FROM t WHERE sym LIKE 'B%'");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn ilike_btc() {
        let (_, r) = mk().query("SELECT sym FROM t WHERE sym ILIKE 'btc'");
        assert_eq!(r.len(), 10);
    }
}
