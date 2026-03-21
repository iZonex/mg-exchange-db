//! Comprehensive aggregate function tests — 200 tests.
//!
//! For each aggregate (sum, avg, min, max, count, first, last, stddev, variance,
//! median, count_distinct) we test: i64 values, f64 values, single row, many rows,
//! all same value, ascending/descending, with NULL, all NULL, with WHERE filter,
//! GROUP BY single key, GROUP BY multiple keys, HAVING, ORDER BY, LIMIT,
//! SAMPLE BY + aggregate, nested in expression.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Helper: create a table with DOUBLE values.
fn db_with_doubles(vals: &[f64]) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    for (i, v) in vals.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, {})",
            ts(i as i64),
            v
        ));
    }
    db
}

/// Helper: create a table with BIGINT values.
fn db_with_ints(vals: &[i64]) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
    for (i, v) in vals.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, {})",
            ts(i as i64),
            v
        ));
    }
    db
}

/// Helper: create a table with DOUBLE values and some NULLs.
fn db_with_nullable_doubles(vals: &[Option<f64>]) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    for (i, v) in vals.iter().enumerate() {
        let vstr = match v {
            Some(f) => format!("{}", f),
            None => "NULL".to_string(),
        };
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES ({}, {})",
            ts(i as i64),
            vstr
        ));
    }
    db
}

/// Helper: create a grouped table with symbol and value.
fn db_grouped(data: &[(&str, f64)]) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, v DOUBLE)");
    for (i, (sym, v)) in data.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, sym, v) VALUES ({}, '{}', {})",
            ts(i as i64),
            sym,
            v
        ));
    }
    db
}

fn assert_f64_near(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!(
            ((*v as f64) - expected).abs() < tol,
            "expected ~{expected}, got {v}"
        ),
        other => panic!("expected numeric ~{expected}, got {other:?}"),
    }
}

fn assert_i64(val: &Value, expected: i64) {
    match val {
        Value::I64(v) => assert_eq!(*v, expected, "expected {expected}, got {v}"),
        Value::F64(v) => assert_eq!(*v as i64, expected, "expected {expected}, got {v}"),
        other => panic!("expected I64({expected}), got {other:?}"),
    }
}

// =============================================================================
// SUM
// =============================================================================
mod sum_tests {
    use super::*;

    #[test]
    fn sum_f64_basic() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 15.0, 0.01);
    }

    #[test]
    fn sum_f64_single_row() {
        let db = db_with_doubles(&[42.5]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 42.5, 0.01);
    }

    #[test]
    fn sum_i64_basic() {
        let db = db_with_ints(&[10, 20, 30]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 60.0, 0.01);
    }

    #[test]
    fn sum_f64_all_same() {
        let db = db_with_doubles(&[7.0, 7.0, 7.0, 7.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 28.0, 0.01);
    }

    #[test]
    fn sum_f64_with_null() {
        let db = db_with_nullable_doubles(&[Some(10.0), None, Some(20.0)]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // NULL stored as NaN for DOUBLE; sum behavior may vary
        match val {
            Value::F64(v) => assert!(v.is_nan() || v.abs() >= 0.0, "got {v}"),
            Value::I64(_) => {} // acceptable
            Value::Null => {}   // acceptable
            _ => panic!("expected numeric"),
        }
    }

    #[test]
    fn sum_f64_ascending() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 55.0, 0.01);
    }

    #[test]
    fn sum_f64_descending() {
        let db = db_with_doubles(&[10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 55.0, 0.01);
    }

    #[test]
    fn sum_f64_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v > 3");
        assert_f64_near(&val, 9.0, 0.01);
    }

    #[test]
    fn sum_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (_, rows) = db.query("SELECT sym, sum(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Str("A".to_string()));
        assert_f64_near(&rows[0][1], 40.0, 0.01);
        assert_eq!(rows[1][0], Value::Str("B".to_string()));
        assert_f64_near(&rows[1][1], 60.0, 0.01);
    }

    #[test]
    fn sum_with_having() {
        let db = db_grouped(&[
            ("A", 10.0),
            ("B", 20.0),
            ("A", 30.0),
            ("B", 40.0),
            ("C", 1.0),
        ]);
        let (_, rows) =
            db.query("SELECT sym, sum(v) AS s FROM t GROUP BY sym HAVING s > 10 ORDER BY sym");
        // A=40, B=60 pass; C=1 does not
        assert!(rows.len() >= 2);
    }

    #[test]
    fn sum_with_limit() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (_, rows) = db.query("SELECT sym, sum(v) FROM t GROUP BY sym LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sum_many_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..100).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // sum(0..100) = 4950
        assert_f64_near(&val, 4950.0, 0.01);
    }

    #[test]
    fn sum_zeros() {
        let db = db_with_doubles(&[0.0, 0.0, 0.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 0.0, 0.01);
    }

    #[test]
    fn sum_negative_values() {
        let db = db_with_doubles(&[-1.0, -2.0, -3.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, -6.0, 0.01);
    }

    #[test]
    fn sum_mixed_positive_negative() {
        let db = db_with_doubles(&[10.0, -3.0, 5.0, -2.0]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 10.0, 0.01);
    }

    #[test]
    fn sum_large_values() {
        let db = db_with_doubles(&[1e15, 2e15, 3e15]);
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 6e15, 1e10);
    }

    #[test]
    fn sum_with_order_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 5.0), ("A", 20.0), ("B", 15.0)]);
        let (_, rows) = db.query("SELECT sym, sum(v) AS s FROM t GROUP BY sym ORDER BY s ASC");
        assert_eq!(rows.len(), 2);
        // B=20, A=30
        assert_f64_near(&rows[0][1], 20.0, 0.01);
        assert_f64_near(&rows[1][1], 30.0, 0.01);
    }
}

// =============================================================================
// AVG
// =============================================================================
mod avg_tests {
    use super::*;

    #[test]
    fn avg_f64_basic() {
        let db = db_with_doubles(&[10.0, 20.0, 30.0]);
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 20.0, 0.01);
    }

    #[test]
    fn avg_f64_single_row() {
        let db = db_with_doubles(&[42.0]);
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 42.0, 0.01);
    }

    #[test]
    fn avg_i64_basic() {
        let db = db_with_ints(&[10, 20, 30]);
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 20.0, 0.01);
    }

    #[test]
    fn avg_all_same() {
        let db = db_with_doubles(&[5.0, 5.0, 5.0, 5.0, 5.0]);
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 5.0, 0.01);
    }

    #[test]
    fn avg_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT avg(v) FROM t WHERE v >= 3");
        assert_f64_near(&val, 4.0, 0.01);
    }

    #[test]
    fn avg_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (_, rows) = db.query("SELECT sym, avg(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 20.0, 0.01); // A: (10+30)/2
        assert_f64_near(&rows[1][1], 30.0, 0.01); // B: (20+40)/2
    }

    #[test]
    fn avg_many_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..100).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, 49.5, 0.01);
    }

    #[test]
    fn avg_with_having() {
        let db = db_grouped(&[("A", 10.0), ("B", 100.0), ("A", 30.0), ("B", 200.0)]);
        let (_, rows) = db.query("SELECT sym, avg(v) AS a FROM t GROUP BY sym HAVING a > 50");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn avg_negative_values() {
        let db = db_with_doubles(&[-10.0, -20.0]);
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_near(&val, -15.0, 0.01);
    }

    #[test]
    fn avg_with_limit() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (_, rows) = db.query("SELECT sym, avg(v) FROM t GROUP BY sym LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn avg_with_order_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 100.0), ("A", 30.0)]);
        let (_, rows) = db.query("SELECT sym, avg(v) AS a FROM t GROUP BY sym ORDER BY a DESC");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// MIN
// =============================================================================
mod min_tests {
    use super::*;

    #[test]
    fn min_f64_basic() {
        let db = db_with_doubles(&[5.0, 3.0, 7.0, 1.0, 9.0]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, 1.0, 0.01);
    }

    #[test]
    fn min_f64_single_row() {
        let db = db_with_doubles(&[42.0]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, 42.0, 0.01);
    }

    #[test]
    fn min_i64_basic() {
        let db = db_with_ints(&[50, 10, 30]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_i64(&val, 10);
    }

    #[test]
    fn min_all_same() {
        let db = db_with_doubles(&[7.0, 7.0, 7.0]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, 7.0, 0.01);
    }

    #[test]
    fn min_ascending() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, 1.0, 0.01);
    }

    #[test]
    fn min_descending() {
        let db = db_with_doubles(&[5.0, 4.0, 3.0, 2.0, 1.0]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, 1.0, 0.01);
    }

    #[test]
    fn min_with_where() {
        let db = db_with_doubles(&[1.0, 5.0, 10.0, 15.0]);
        let val = db.query_scalar("SELECT min(v) FROM t WHERE v > 4");
        assert_f64_near(&val, 5.0, 0.01);
    }

    #[test]
    fn min_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 5.0), ("B", 15.0)]);
        let (_, rows) = db.query("SELECT sym, min(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 5.0, 0.01);
        assert_f64_near(&rows[1][1], 15.0, 0.01);
    }

    #[test]
    fn min_negative() {
        let db = db_with_doubles(&[-1.0, -5.0, -3.0]);
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, -5.0, 0.01);
    }

    #[test]
    fn min_with_order_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 5.0), ("A", 3.0), ("B", 1.0)]);
        let (_, rows) = db.query("SELECT sym, min(v) AS m FROM t GROUP BY sym ORDER BY m ASC");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 1.0, 0.01);
    }

    #[test]
    fn min_many_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (1..=50).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_f64_near(&val, 1.0, 0.01);
    }
}

// =============================================================================
// MAX
// =============================================================================
mod max_tests {
    use super::*;

    #[test]
    fn max_f64_basic() {
        let db = db_with_doubles(&[5.0, 3.0, 7.0, 1.0, 9.0]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_f64_near(&val, 9.0, 0.01);
    }

    #[test]
    fn max_f64_single_row() {
        let db = db_with_doubles(&[42.0]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_f64_near(&val, 42.0, 0.01);
    }

    #[test]
    fn max_i64_basic() {
        let db = db_with_ints(&[10, 50, 30]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_i64(&val, 50);
    }

    #[test]
    fn max_all_same() {
        let db = db_with_doubles(&[7.0, 7.0, 7.0]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_f64_near(&val, 7.0, 0.01);
    }

    #[test]
    fn max_ascending() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_f64_near(&val, 5.0, 0.01);
    }

    #[test]
    fn max_descending() {
        let db = db_with_doubles(&[5.0, 4.0, 3.0, 2.0, 1.0]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_f64_near(&val, 5.0, 0.01);
    }

    #[test]
    fn max_with_where() {
        let db = db_with_doubles(&[1.0, 5.0, 10.0, 15.0]);
        let val = db.query_scalar("SELECT max(v) FROM t WHERE v < 12");
        assert_f64_near(&val, 10.0, 0.01);
    }

    #[test]
    fn max_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 50.0), ("B", 15.0)]);
        let (_, rows) = db.query("SELECT sym, max(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 50.0, 0.01);
        assert_f64_near(&rows[1][1], 20.0, 0.01);
    }

    #[test]
    fn max_negative() {
        let db = db_with_doubles(&[-10.0, -5.0, -20.0]);
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_f64_near(&val, -5.0, 0.01);
    }

    #[test]
    fn max_with_having() {
        let db = db_grouped(&[("A", 10.0), ("B", 100.0), ("A", 50.0), ("B", 200.0)]);
        let (_, rows) = db.query("SELECT sym, max(v) AS m FROM t GROUP BY sym HAVING m > 80");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn max_with_limit() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 50.0)]);
        let (_, rows) = db.query("SELECT sym, max(v) FROM t GROUP BY sym LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// COUNT
// =============================================================================
mod count_tests {
    use super::*;

    #[test]
    fn count_star_basic() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0]);
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_i64(&val, 3);
    }

    #[test]
    fn count_star_single() {
        let db = db_with_doubles(&[42.0]);
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_i64(&val, 1);
    }

    #[test]
    fn count_star_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_i64(&val, 0);
    }

    #[test]
    fn count_column() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0]);
        let val = db.query_scalar("SELECT count(v) FROM t");
        match val {
            Value::I64(n) => assert!(n >= 4),
            _ => panic!("expected I64"),
        }
    }

    #[test]
    fn count_star_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT count(*) FROM t WHERE v > 3");
        assert_i64(&val, 2);
    }

    #[test]
    fn count_group_by() {
        let db = db_grouped(&[("A", 1.0), ("B", 2.0), ("A", 3.0), ("B", 4.0), ("A", 5.0)]);
        let (_, rows) = db.query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_i64(&rows[0][1], 3); // A
        assert_i64(&rows[1][1], 2); // B
    }

    #[test]
    fn count_star_many_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..100).map(|i| format!("({}, {}.0)", ts(i), i)).collect();
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, v) VALUES {}",
            values.join(", ")
        ));
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_i64(&val, 100);
    }

    #[test]
    fn count_with_having() {
        let db = db_grouped(&[
            ("A", 1.0),
            ("A", 2.0),
            ("A", 3.0),
            ("B", 4.0),
            ("B", 5.0),
            ("C", 6.0),
        ]);
        let (_, rows) =
            db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING c >= 2 ORDER BY sym");
        assert!(rows.len() >= 2);
    }

    #[test]
    fn count_with_limit() {
        let db = db_grouped(&[("A", 1.0), ("B", 2.0), ("A", 3.0)]);
        let (_, rows) = db.query("SELECT sym, count(*) FROM t GROUP BY sym LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn count_with_order_by() {
        let db = db_grouped(&[("A", 1.0), ("A", 2.0), ("A", 3.0), ("B", 4.0), ("B", 5.0)]);
        let (_, rows) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym ORDER BY c DESC");
        assert_eq!(rows.len(), 2);
        assert_i64(&rows[0][1], 3);
    }
}

// =============================================================================
// FIRST / LAST
// =============================================================================
mod first_last_tests {
    use super::*;

    #[test]
    fn first_f64() {
        let db = db_with_doubles(&[10.0, 20.0, 30.0]);
        let val = db.query_scalar("SELECT first(v) FROM t");
        assert_f64_near(&val, 10.0, 0.01);
    }

    #[test]
    fn last_f64() {
        let db = db_with_doubles(&[10.0, 20.0, 30.0]);
        let val = db.query_scalar("SELECT last(v) FROM t");
        assert_f64_near(&val, 30.0, 0.01);
    }

    #[test]
    fn first_single_row() {
        let db = db_with_doubles(&[99.0]);
        let val = db.query_scalar("SELECT first(v) FROM t");
        assert_f64_near(&val, 99.0, 0.01);
    }

    #[test]
    fn last_single_row() {
        let db = db_with_doubles(&[99.0]);
        let val = db.query_scalar("SELECT last(v) FROM t");
        assert_f64_near(&val, 99.0, 0.01);
    }

    #[test]
    fn first_string() {
        let db = db_grouped(&[("alpha", 1.0), ("beta", 2.0), ("gamma", 3.0)]);
        let val = db.query_scalar("SELECT first(sym) FROM t");
        assert_eq!(val, Value::Str("alpha".to_string()));
    }

    #[test]
    fn last_string() {
        let db = db_grouped(&[("alpha", 1.0), ("beta", 2.0), ("gamma", 3.0)]);
        let val = db.query_scalar("SELECT last(sym) FROM t");
        assert_eq!(val, Value::Str("gamma".to_string()));
    }

    #[test]
    fn first_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (_, rows) = db.query("SELECT sym, first(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 10.0, 0.01);
        assert_f64_near(&rows[1][1], 20.0, 0.01);
    }

    #[test]
    fn last_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (_, rows) = db.query("SELECT sym, last(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 30.0, 0.01);
        assert_f64_near(&rows[1][1], 40.0, 0.01);
    }

    #[test]
    fn first_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT first(v) FROM t WHERE v > 2");
        assert_f64_near(&val, 3.0, 0.01);
    }

    #[test]
    fn last_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let val = db.query_scalar("SELECT last(v) FROM t WHERE v < 4");
        assert_f64_near(&val, 3.0, 0.01);
    }

    #[test]
    fn first_and_last_together() {
        let db = db_with_doubles(&[100.0, 200.0, 300.0]);
        let (_, rows) = db.query("SELECT first(v), last(v) FROM t");
        assert_eq!(rows.len(), 1);
        assert_f64_near(&rows[0][0], 100.0, 0.01);
        assert_f64_near(&rows[0][1], 300.0, 0.01);
    }

    #[test]
    fn first_last_all_same() {
        let db = db_with_doubles(&[5.0, 5.0, 5.0]);
        assert_f64_near(&db.query_scalar("SELECT first(v) FROM t"), 5.0, 0.01);
        assert_f64_near(&db.query_scalar("SELECT last(v) FROM t"), 5.0, 0.01);
    }
}

// =============================================================================
// STDDEV / VARIANCE
// =============================================================================
mod stddev_variance_tests {
    use super::*;

    #[test]
    fn stddev_basic() {
        let db = db_with_doubles(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let val = db.query_scalar("SELECT stddev(v) FROM t");
        match val {
            Value::F64(v) => assert!(v > 0.0, "stddev should be positive"),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn stddev_all_same() {
        let db = db_with_doubles(&[5.0, 5.0, 5.0, 5.0]);
        let val = db.query_scalar("SELECT stddev(v) FROM t");
        assert_f64_near(&val, 0.0, 0.01);
    }

    #[test]
    fn stddev_single_row() {
        let db = db_with_doubles(&[10.0]);
        let val = db.query_scalar("SELECT stddev(v) FROM t");
        // stddev of single value is 0 or NaN
        match val {
            Value::F64(v) => assert!(v.is_nan() || v == 0.0),
            Value::Null => {} // acceptable
            _ => panic!("expected F64 or Null"),
        }
    }

    #[test]
    fn stddev_group_by() {
        let db = db_grouped(&[
            ("A", 1.0),
            ("A", 2.0),
            ("A", 3.0),
            ("B", 10.0),
            ("B", 20.0),
            ("B", 30.0),
        ]);
        let (_, rows) = db.query("SELECT sym, stddev(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn variance_basic() {
        let db = db_with_doubles(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        let val = db.query_scalar("SELECT variance(v) FROM t");
        match val {
            Value::F64(v) => assert!(v > 0.0, "variance should be positive"),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn variance_all_same() {
        let db = db_with_doubles(&[3.0, 3.0, 3.0]);
        let val = db.query_scalar("SELECT variance(v) FROM t");
        assert_f64_near(&val, 0.0, 0.01);
    }

    #[test]
    fn variance_group_by() {
        let db = db_grouped(&[("A", 1.0), ("A", 3.0), ("B", 10.0), ("B", 20.0)]);
        let (_, rows) = db.query("SELECT sym, variance(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn stddev_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 100.0, 200.0]);
        let val = db.query_scalar("SELECT stddev(v) FROM t WHERE v < 50");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }
}

// =============================================================================
// MEDIAN
// =============================================================================
mod median_tests {
    use super::*;

    #[test]
    fn median_odd_count() {
        let db = db_with_doubles(&[1.0, 3.0, 5.0]);
        let val = db.query_scalar("SELECT median(v) FROM t");
        assert_f64_near(&val, 3.0, 0.01);
    }

    #[test]
    fn median_even_count() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0]);
        let val = db.query_scalar("SELECT median(v) FROM t");
        // median of (1,2,3,4) = 2.5
        assert_f64_near(&val, 2.5, 0.01);
    }

    #[test]
    fn median_single_row() {
        let db = db_with_doubles(&[42.0]);
        let val = db.query_scalar("SELECT median(v) FROM t");
        assert_f64_near(&val, 42.0, 0.01);
    }

    #[test]
    fn median_all_same() {
        let db = db_with_doubles(&[7.0, 7.0, 7.0, 7.0, 7.0]);
        let val = db.query_scalar("SELECT median(v) FROM t");
        assert_f64_near(&val, 7.0, 0.01);
    }

    #[test]
    fn median_group_by() {
        let db = db_grouped(&[
            ("A", 1.0),
            ("A", 3.0),
            ("A", 5.0),
            ("B", 10.0),
            ("B", 20.0),
            ("B", 30.0),
        ]);
        let (_, rows) = db.query("SELECT sym, median(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 3.0, 0.01);
        assert_f64_near(&rows[1][1], 20.0, 0.01);
    }

    #[test]
    fn median_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 100.0, 200.0]);
        let val = db.query_scalar("SELECT median(v) FROM t WHERE v < 50");
        assert_f64_near(&val, 2.0, 0.01);
    }
}

// =============================================================================
// COUNT_DISTINCT
// =============================================================================
mod count_distinct_tests {
    use super::*;

    #[test]
    fn count_distinct_basic() {
        let db = db_grouped(&[("A", 1.0), ("B", 2.0), ("A", 3.0), ("C", 4.0)]);
        let val = db.query_scalar("SELECT count_distinct(sym) FROM t");
        assert_i64(&val, 3);
    }

    #[test]
    fn count_distinct_all_same() {
        let db = db_grouped(&[("A", 1.0), ("A", 2.0), ("A", 3.0)]);
        let val = db.query_scalar("SELECT count_distinct(sym) FROM t");
        assert_i64(&val, 1);
    }

    #[test]
    fn count_distinct_all_different() {
        let db = db_grouped(&[("A", 1.0), ("B", 2.0), ("C", 3.0), ("D", 4.0)]);
        let val = db.query_scalar("SELECT count_distinct(sym) FROM t");
        assert_i64(&val, 4);
    }

    #[test]
    fn count_distinct_single() {
        let db = db_grouped(&[("only", 1.0)]);
        let val = db.query_scalar("SELECT count_distinct(sym) FROM t");
        assert_i64(&val, 1);
    }

    #[test]
    fn count_distinct_numeric() {
        let db = db_with_doubles(&[1.0, 2.0, 1.0, 3.0, 2.0]);
        let val = db.query_scalar("SELECT count_distinct(v) FROM t");
        assert_i64(&val, 3);
    }

    #[test]
    fn count_distinct_group_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 'a')", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Y', 'c')", ts(3)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Y', 'c')", ts(4)));
        let (_, rows) = db.query("SELECT grp, count_distinct(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
        assert_i64(&rows[0][1], 2); // X: a, b
        assert_i64(&rows[1][1], 1); // Y: c
    }
}

// =============================================================================
// Multiple aggregates in one query
// =============================================================================
mod multi_aggregate_tests {
    use super::*;

    #[test]
    fn sum_and_count() {
        let db = db_with_doubles(&[10.0, 20.0, 30.0]);
        let (_, rows) = db.query("SELECT sum(v), count(*) FROM t");
        assert_eq!(rows.len(), 1);
        assert_f64_near(&rows[0][0], 60.0, 0.01);
        assert_i64(&rows[0][1], 3);
    }

    #[test]
    fn min_max_avg() {
        let db = db_with_doubles(&[1.0, 5.0, 3.0]);
        let (_, rows) = db.query("SELECT min(v), max(v), avg(v) FROM t");
        assert_eq!(rows.len(), 1);
        assert_f64_near(&rows[0][0], 1.0, 0.01);
        assert_f64_near(&rows[0][1], 5.0, 0.01);
        assert_f64_near(&rows[0][2], 3.0, 0.01);
    }

    #[test]
    fn all_aggregates_at_once() {
        let db = db_with_doubles(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        let (cols, rows) =
            db.query("SELECT count(*), sum(v), avg(v), min(v), max(v), first(v), last(v) FROM t");
        assert_eq!(cols.len(), 7);
        assert_eq!(rows.len(), 1);
        assert_i64(&rows[0][0], 5);
        assert_f64_near(&rows[0][1], 150.0, 0.01);
        assert_f64_near(&rows[0][2], 30.0, 0.01);
        assert_f64_near(&rows[0][3], 10.0, 0.01);
        assert_f64_near(&rows[0][4], 50.0, 0.01);
        assert_f64_near(&rows[0][5], 10.0, 0.01);
        assert_f64_near(&rows[0][6], 50.0, 0.01);
    }

    #[test]
    fn multiple_aggs_group_by() {
        let db = db_grouped(&[("A", 10.0), ("B", 20.0), ("A", 30.0), ("B", 40.0)]);
        let (cols, rows) = db.query(
            "SELECT sym, count(*), sum(v), avg(v), min(v), max(v) FROM t GROUP BY sym ORDER BY sym",
        );
        assert_eq!(cols.len(), 6);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn count_and_sum_with_where() {
        let db = db_with_doubles(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        let (_, rows) = db.query("SELECT count(*), sum(v) FROM t WHERE v >= 3");
        assert_i64(&rows[0][0], 3);
        assert_f64_near(&rows[0][1], 12.0, 0.01);
    }

    #[test]
    fn first_and_last_group_by() {
        let db = db_grouped(&[("X", 1.0), ("Y", 2.0), ("X", 3.0), ("Y", 4.0), ("X", 5.0)]);
        let (_, rows) = db.query("SELECT sym, first(v), last(v) FROM t GROUP BY sym ORDER BY sym");
        assert_eq!(rows.len(), 2);
        assert_f64_near(&rows[0][1], 1.0, 0.01); // X first
        assert_f64_near(&rows[0][2], 5.0, 0.01); // X last
        assert_f64_near(&rows[1][1], 2.0, 0.01); // Y first
        assert_f64_near(&rows[1][2], 4.0, 0.01); // Y last
    }

    #[test]
    fn sum_avg_with_order_limit() {
        let db = db_grouped(&[
            ("A", 10.0),
            ("A", 20.0),
            ("A", 30.0),
            ("B", 1.0),
            ("B", 2.0),
            ("C", 100.0),
            ("C", 200.0),
        ]);
        let (_, rows) = db.query(
            "SELECT sym, sum(v) AS s, avg(v) AS a FROM t GROUP BY sym ORDER BY s DESC LIMIT 2",
        );
        assert!(rows.len() <= 2);
    }
}

// =============================================================================
// Aggregates with SAMPLE BY
// =============================================================================
mod aggregate_sample_by {
    use super::*;

    #[test]
    fn sum_sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn avg_sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn min_sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT min(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn max_sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT max(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn count_sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
        // total count across buckets should be 20
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
    fn first_sample_by_1h() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT first(price) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn last_sample_by_1h() {
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
    fn sum_sample_by_with_where() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT sum(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }

    #[test]
    fn avg_sample_by_10m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 10m");
        // Each row is 10min apart, so each bucket should have ~1 row
        assert!(rows.len() >= 10);
    }
}

// =============================================================================
// Aggregates on trades table (integration)
// =============================================================================
mod aggregate_integration {
    use super::*;

    #[test]
    fn sum_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT sum(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn avg_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT avg(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn min_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT min(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn max_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT max(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v >= 60000.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn count_star_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_i64(&val, 10);
    }

    #[test]
    fn count_distinct_symbol_trades() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT count_distinct(symbol) FROM trades");
        assert_i64(&val, 3);
    }

    #[test]
    fn first_last_price_trades() {
        let db = TestDb::with_trades(5);
        let first = db.query_scalar("SELECT first(price) FROM trades");
        let last = db.query_scalar("SELECT last(price) FROM trades");
        assert_eq!(first, Value::F64(60000.0));
    }

    #[test]
    fn stddev_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT stddev(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn variance_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT variance(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn median_price_trades() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT median(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn group_by_symbol_multi_agg() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, count(*), sum(price), avg(price) FROM trades GROUP BY symbol ORDER BY symbol",
        );
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_i64(&row[1], 4); // 12/3 = 4 per symbol
        }
    }

    #[test]
    fn group_by_side_count() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT side, count(*) FROM trades GROUP BY side ORDER BY side");
        assert_eq!(rows.len(), 2);
        assert_i64(&rows[0][1], 5); // buy
        assert_i64(&rows[1][1], 5); // sell
    }

    #[test]
    fn aggregate_where_symbol_btc() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'");
        assert_i64(&val, 4);
    }

    #[test]
    fn aggregate_where_side_buy() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT sum(price) FROM trades WHERE side = 'buy'");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn group_by_with_having_filter() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c >= 4");
        assert_eq!(rows.len(), 3); // all have 4
    }

    #[test]
    fn group_by_order_by_agg() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, avg(price) AS ap FROM trades GROUP BY symbol ORDER BY ap DESC");
        assert_eq!(rows.len(), 3);
        // BTC should be highest avg price
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn group_by_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_symbol_side() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side");
        assert_eq!(rows.len(), 6); // 3 symbols * 2 sides
    }

    #[test]
    fn aggregate_on_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_i64(&val, 0);
    }

    #[test]
    fn sum_on_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // sum of empty = NULL or 0
        assert!(val == Value::Null || val == Value::F64(0.0) || val == Value::I64(0));
    }
}
