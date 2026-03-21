//! Per-type regression tests for F64/DOUBLE operators — 500+ tests.
//!
//! Every SQL operator is tested with DOUBLE data: comparison, logical,
//! arithmetic, aggregate, CAST, CASE WHEN, IN/NOT IN, BETWEEN, IS NULL,
//! ORDER BY, GROUP BY, HAVING, LIMIT/OFFSET, DISTINCT, precision edge cases.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1_710_460_800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Standard test table: -99.9, -0.001, 0.0, 0.001, 42.5, 99.9
fn db_f64() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    let vals = [-99.9, -0.001, 0.0, 0.001, 42.5, 99.9];
    for (i, val) in vals.iter().enumerate() {
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i as i64), val));
    }
    db
}

/// Nullable DOUBLE table.
fn db_f64_nullable() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.5)", ts(0)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(1)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.3)", ts(2)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(3)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 50.1)", ts(4)));
    db
}

/// Grouped DOUBLE table.
fn db_f64_grouped() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
    let data = [
        ("A", 10.0),
        ("B", 20.5),
        ("A", 30.0),
        ("B", 40.5),
        ("A", 50.0),
        ("C", 60.5),
        ("C", 70.0),
        ("B", 80.5),
    ];
    for (i, (g, v)) in data.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, '{}', {})",
            ts(i as i64),
            g,
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
    fn eq_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 42.5");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(42.5));
    }

    #[test]
    fn eq_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -99.9");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 0.0");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_no_match() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 999.9");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn eq_small_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 0.001");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_small_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -0.001");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.15)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 3.15");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn eq_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.23)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 1.23");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_large_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e15)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 1e15");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_integer_literal() {
        let db = db_f64();
        // 0.0 should match 0
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 0");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 2: Not Equal (!=)
// =============================================================================
mod ne {
    use super::*;

    #[test]
    fn ne_excludes_one() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 42.5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_excludes_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 0.0");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_no_match_all_returned() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 999.0");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn ne_all_same_excluded() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.5)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 5.5");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn ne_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != -99.9");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_small() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 0.001");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_large() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 99.9");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 10.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn ne_two_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 1.0");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn ne_preserves_count() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != -0.001");
        assert_eq!(rows.len(), 5);
    }
}

// =============================================================================
// Module 3: Greater Than (>)
// =============================================================================
mod gt {
    use super::*;

    #[test]
    fn gt_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0.0");
        // 0.001, 42.5, 99.9 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn gt_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -1.0");
        // -0.001, 0.0, 0.001, 42.5, 99.9 = 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn gt_all() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -200.0");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn gt_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 100.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_boundary() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 99.9");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_just_below() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 99.8");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gt_forty_two() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 42.5");
        assert_eq!(rows.len(), 1); // 99.9
    }

    #[test]
    fn gt_small_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0.0005");
        // 0.001, 42.5, 99.9 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn gt_large() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e15)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e10)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 1e12");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gt_minus_hundred() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -100.0");
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 4: Less Than (<)
// =============================================================================
mod lt {
    use super::*;

    #[test]
    fn lt_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 0.0");
        // -99.9, -0.001 = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn lt_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 50.0");
        // -99.9, -0.001, 0.0, 0.001, 42.5 = 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lt_all() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 999.0");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn lt_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -99.9");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_boundary() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -99.9");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_just_above() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -99.8");
        assert_eq!(rows.len(), 1); // -99.9
    }

    #[test]
    fn lt_small() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 0.001");
        // -99.9, -0.001, 0.0 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn lt_negative_threshold() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -0.0005");
        // -99.9, -0.001 = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn lt_single_row_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -5.0)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 0");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn lt_large() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e15)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 1e12");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 5: >= and <=
// =============================================================================
mod gte_lte {
    use super::*;

    #[test]
    fn gte_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 0.0");
        // 0.0, 0.001, 42.5, 99.9 = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn gte_boundary() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 99.9");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gte_all() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= -99.9");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn gte_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 100.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gte_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= -0.001");
        // -0.001, 0.0, 0.001, 42.5, 99.9 = 5
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lte_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 0.0");
        // -99.9, -0.001, 0.0 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn lte_boundary() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= -99.9");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn lte_all() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 99.9");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn lte_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= -100.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lte_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 42.5");
        // -99.9, -0.001, 0.0, 0.001, 42.5 = 5
        assert_eq!(rows.len(), 5);
    }
}

// =============================================================================
// Module 6: BETWEEN
// =============================================================================
mod between {
    use super::*;

    #[test]
    fn between_full() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -99.9 AND 99.9");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn between_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 0.0 AND 100.0");
        // 0.0, 0.001, 42.5, 99.9 = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn between_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -100.0 AND 0.0");
        // -99.9, -0.001, 0.0 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn between_single() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 42.5 AND 42.5");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn between_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 200.0 AND 300.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_narrow() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -0.002 AND 0.002");
        // -0.001, 0.0, 0.001 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn between_wide() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -1000.0 AND 1000.0");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn between_tight_no_match() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 1.0 AND 40.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_near_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -0.001 AND 0.001");
        // -0.001, 0.0, 0.001 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn between_large() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e15)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 1e14 AND 1e16");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 7: IN / NOT IN
// =============================================================================
mod in_op {
    use super::*;

    #[test]
    fn in_single() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (42.5)");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn in_multiple() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (0.0, 42.5, 99.9)");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn in_no_match() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (999.0, 888.0)");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn in_negative() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (-99.9, -0.001)");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn not_in_single() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (42.5)");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn not_in_multiple() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (0.0, 42.5)");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn not_in_no_match() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (999.0)");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn in_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (0.0)");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn in_all() {
        let db = db_f64();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v IN (-99.9, -0.001, 0.0, 0.001, 42.5, 99.9)");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn not_in_all() {
        let db = db_f64();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v NOT IN (-99.9, -0.001, 0.0, 0.001, 42.5, 99.9)");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 8: IS NULL / IS NOT NULL
// =============================================================================
mod null_ops {
    use super::*;

    #[test]
    fn is_null() {
        let db = db_f64_nullable();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert!(!rows.is_empty());
    }

    #[test]
    fn is_not_null() {
        let db = db_f64_nullable();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert!(rows.len() >= 3);
    }

    #[test]
    fn no_nulls_all_not_null() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn no_nulls_none_null() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn null_count() {
        let db = db_f64_nullable();
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn is_null_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn all_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert!(!rows.is_empty());
    }

    #[test]
    fn coalesce_replaces_null() {
        let db = db_f64_nullable();
        let (_, rows) = db.query("SELECT coalesce(v, 0.0) FROM t");
        assert_eq!(rows.len(), 5);
        for r in &rows {
            assert_ne!(r[0], Value::Null);
        }
    }

    #[test]
    fn coalesce_no_nulls() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT coalesce(v, 999.0) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn coalesce_preserves_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        let val = db.query_scalar("SELECT coalesce(v, 0.0) FROM t");
        assert_f64_near(&val, 42.5, 0.01);
    }
}

// =============================================================================
// Module 9: ORDER BY
// =============================================================================
mod order_by {
    use super::*;

    #[test]
    fn order_asc() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::F64(-99.9));
        assert_eq!(rows[5][0], Value::F64(99.9));
    }

    #[test]
    fn order_desc() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        assert_eq!(rows[0][0], Value::F64(99.9));
    }

    #[test]
    fn order_default() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v");
        assert_eq!(rows[0][0], Value::F64(-99.9));
    }

    #[test]
    fn order_with_limit() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::F64(-99.9));
    }

    #[test]
    fn order_desc_limit() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 2");
        assert_eq!(rows[0][0], Value::F64(99.9));
        assert_eq!(rows[1][0], Value::F64(42.5));
    }

    #[test]
    fn order_with_offset() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::F64(0.0));
    }

    #[test]
    fn order_with_where() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0 ORDER BY v ASC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::F64(0.001));
    }

    #[test]
    fn order_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.5)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn order_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.15)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn order_asc_negative_to_positive() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::F64(-99.9));
        assert_eq!(rows[1][0], Value::F64(-0.001));
        assert_eq!(rows[2][0], Value::F64(0.0));
        assert_eq!(rows[3][0], Value::F64(0.001));
        assert_eq!(rows[4][0], Value::F64(42.5));
        assert_eq!(rows[5][0], Value::F64(99.9));
    }
}

// =============================================================================
// Module 10: Aggregates
// =============================================================================
mod aggregates {
    use super::*;

    #[test]
    fn sum_basic() {
        let db = db_f64();
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // -99.9 + -0.001 + 0 + 0.001 + 42.5 + 99.9 = 42.5
        assert_f64_near(&val, 42.5, 0.01);
    }

    #[test]
    fn sum_positive() {
        let db = db_f64();
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v > 0");
        // 0.001 + 42.5 + 99.9 = 142.401
        assert_f64_near(&val, 142.401, 0.01);
    }

    #[test]
    fn sum_negative() {
        let db = db_f64();
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v < 0");
        // -99.9 + -0.001 = -99.901
        assert_f64_near(&val, -99.901, 0.01);
    }

    #[test]
    fn count_star() {
        let db = db_f64();
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(6));
    }

    #[test]
    fn min_basic() {
        let db = db_f64();
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(-99.9));
    }

    #[test]
    fn max_basic() {
        let db = db_f64();
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(99.9));
    }

    #[test]
    fn avg_basic() {
        let db = db_f64();
        let val = db.query_scalar("SELECT avg(v) FROM t");
        // 42.5 / 6 ≈ 7.0833
        assert_f64_near(&val, 7.0833, 0.01);
    }

    #[test]
    fn first_basic() {
        let db = db_f64();
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::F64(-99.9));
    }

    #[test]
    fn last_basic() {
        let db = db_f64();
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::F64(99.9));
    }

    #[test]
    fn sum_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.15)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 3.15, 0.01);
    }

    #[test]
    fn min_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.72)", ts(0)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(2.72));
    }

    #[test]
    fn max_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.72)", ts(0)));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(2.72));
    }

    #[test]
    fn count_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn sum_zeros() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.0)", ts(i)));
        }
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 0.0, 0.01);
    }

    #[test]
    fn avg_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(i)));
        }
        assert_f64_near(&db.query_scalar("SELECT avg(v) FROM t"), 10.0, 0.01);
    }

    #[test]
    fn min_max_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7.7)", ts(i)));
        }
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(7.7));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(7.7));
    }

    #[test]
    fn sum_with_where() {
        let db = db_f64();
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v >= 0");
        // 0 + 0.001 + 42.5 + 99.9 = 142.401
        assert_f64_near(&val, 142.401, 0.01);
    }

    #[test]
    fn count_with_where() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 0"),
            Value::I64(3)
        );
    }

    #[test]
    fn multiple_aggregates() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v), sum(v) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(6));
    }
}

// =============================================================================
// Module 11: GROUP BY
// =============================================================================
mod group_by {
    use super::*;

    #[test]
    fn group_by_count() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sum() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_min() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, min(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_max() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, max(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_avg() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, avg(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_first() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, first(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_last() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, last(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_single_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'X', {}.5)",
                ts(i),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn group_by_with_order() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp DESC");
        assert_eq!(rows[0][0], Value::Str("C".into()));
    }

    #[test]
    fn group_by_with_limit() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 12: HAVING
// =============================================================================
mod having {
    use super::*;

    #[test]
    fn having_count() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 3");
        assert_eq!(rows.len(), 2); // A(3), B(3)
    }

    #[test]
    fn having_sum() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100");
        // A: 90, B: 141.5, C: 130.5 => B and C > 100; but check actual
        assert!(rows.len() >= 2);
    }

    #[test]
    fn having_all() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 1");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn having_none() {
        let db = db_f64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c > 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn having_with_order() {
        let db = db_f64_grouped();
        let (_, rows) =
            db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100 ORDER BY grp");
        assert!(rows.len() >= 2);
    }
}

// =============================================================================
// Module 13: LIMIT / OFFSET
// =============================================================================
mod limit_offset {
    use super::*;

    #[test]
    fn limit_basic() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn limit_one() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_exceeds() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn offset_basic() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn offset_all() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 10 OFFSET 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_with_where() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn offset_one() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v LIMIT 1 OFFSET 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_zero() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 0");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 14: DISTINCT
// =============================================================================
mod distinct {
    use super::*;

    #[test]
    fn distinct_unique() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn distinct_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.2)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.1)", ts(2)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn distinct_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.15)", ts(i)));
        }
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_with_order() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT DISTINCT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::F64(-99.9));
    }

    #[test]
    fn distinct_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7.7)", ts(0)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 15: Arithmetic
// =============================================================================
mod arithmetic {
    use super::*;

    #[test]
    fn add_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v + 5.5 FROM t"), 16.0, 0.01);
    }

    #[test]
    fn sub_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v - 3.5 FROM t"), 7.0, 0.01);
    }

    #[test]
    fn mul_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v * 3.0 FROM t"), 30.0, 0.01);
    }

    #[test]
    fn div_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v / 4.0 FROM t"), 2.5, 0.01);
    }

    #[test]
    fn modulo_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v % 3.0 FROM t"), 1.0, 0.01);
    }

    #[test]
    fn add_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -5.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v + 10.0 FROM t"), 4.5, 0.01);
    }

    #[test]
    fn unary_minus() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT -v FROM t"), -42.5, 0.01);
    }

    #[test]
    fn unary_minus_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -10.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT -v FROM t"), 10.0, 0.01);
    }

    #[test]
    fn mul_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v * 0 FROM t"), 0.0, 0.01);
    }

    #[test]
    fn add_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v + 0 FROM t"), 42.5, 0.01);
    }

    #[test]
    fn mul_one() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v * 1 FROM t"), 42.5, 0.01);
    }

    #[test]
    fn two_columns_add() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.5, 20.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT a + b FROM t"), 31.0, 0.01);
    }

    #[test]
    fn two_columns_sub() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0, 10.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT a - b FROM t"), 19.5, 0.01);
    }

    #[test]
    fn two_columns_mul() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 6.0, 7.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT a * b FROM t"), 42.0, 0.01);
    }

    #[test]
    fn expression_in_where() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v * 2 > 80");
        // v > 40: 42.5, 99.9 => 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn nested_arithmetic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        assert_f64_near(
            &db.query_scalar("SELECT (v + 5.0) * 2.0 FROM t"),
            30.0,
            0.01,
        );
    }

    #[test]
    fn arithmetic_preserves_rows() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v + 1.0 FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn sub_to_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v - 10.0 FROM t"), -7.0, 0.01);
    }
}

// =============================================================================
// Module 16: CAST
// =============================================================================
mod cast_ops {
    use super::*;

    #[test]
    fn cast_double_to_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.9)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS INT) FROM t");
        assert_eq!(val, Value::I64(42));
    }

    #[test]
    fn cast_double_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert!(s.contains("42.5"), "got: {s}"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn cast_negative_to_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -3.7)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS INT) FROM t");
        assert_eq!(val, Value::I64(-3));
    }

    #[test]
    fn cast_zero_to_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.0)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS INT) FROM t");
        assert_eq!(val, Value::I64(0));
    }

    #[test]
    fn cast_preserves_rows() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn cast_large_to_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99999.99)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS INT) FROM t");
        assert_eq!(val, Value::I64(99999));
    }

    #[test]
    fn cast_with_order() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn cast_with_where() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CAST(v AS INT) FROM t WHERE v > 0");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_int_back_to_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }

    #[test]
    fn cast_zero_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.0)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert!(s.contains('0')),
            other => panic!("expected Str, got {other:?}"),
        }
    }
}

// =============================================================================
// Module 17: CASE WHEN
// =============================================================================
mod case_when {
    use super::*;

    #[test]
    fn case_positive_negative() {
        let db = db_f64();
        let (_, rows) =
            db.query("SELECT CASE WHEN v > 0 THEN 'positive' ELSE 'non_positive' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_zero_check() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CASE WHEN v = 0 THEN 'zero' ELSE 'nonzero' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_multi_branch() {
        let db = db_f64();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v < 0 THEN 'neg' WHEN v = 0 THEN 'zero' ELSE 'pos' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_returns_number() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 0 THEN 1 ELSE 0 END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_with_alias() {
        let db = db_f64();
        let (cols, _) = db.query("SELECT CASE WHEN v > 0 THEN 'yes' ELSE 'no' END AS flag FROM t");
        assert!(cols.contains(&"flag".to_string()));
    }

    #[test]
    fn case_high_low() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_without_else() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 50 THEN 'high' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_all_else() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 1000 THEN 'huge' ELSE 'normal' END FROM t");
        for r in &rows {
            assert_eq!(r[0], Value::Str("normal".into()));
        }
    }
}

// =============================================================================
// Module 18: Logical (AND, OR)
// =============================================================================
mod logical {
    use super::*;

    #[test]
    fn and_both() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0 AND v < 50");
        // 0.001, 42.5 = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn and_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 200 AND v < 300");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn or_either() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -99.9 OR v = 99.9");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn or_none() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 999.0 OR v = 888.0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn and_or_combined() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE (v > 0 AND v < 50) OR v = -99.9");
        // 0.001, 42.5, -99.9 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn and_chain() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -1 AND v < 1 AND v != 0");
        // -0.001, 0.001 = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn or_chain() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 0.0 OR v = 42.5 OR v = 99.9");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn complex_predicate() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE (v > 0 OR v < -50) AND v != 42.5");
        // v > 0 OR v < -50: {-99.9, 0.001, 42.5, 99.9} AND != 42.5: {-99.9, 0.001, 99.9} = 3
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 19: Precision edge cases
// =============================================================================
mod precision {
    use super::*;

    #[test]
    fn very_small_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e-15)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_f64_near(&val, 1e-15, 1e-18);
    }

    #[test]
    fn very_large_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e15)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v FROM t"), 1e15, 1.0);
    }

    #[test]
    fn sum_precision_small() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.1)", ts(i)));
        }
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_near(&val, 1.0, 0.01);
    }

    #[test]
    fn avg_precision() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(2)));
        assert_f64_near(&db.query_scalar("SELECT avg(v) FROM t"), 2.0, 0.01);
    }

    #[test]
    fn large_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e10)", ts(i)));
        }
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 5e10, 1.0);
    }

    #[test]
    fn negative_near_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -1e-15)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_f64_near(&val, -1e-15, 1e-18);
    }

    #[test]
    fn half_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // Use a large but representable value that the SQL parser can handle
        let v = 1e18;
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {v})", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        match val {
            Value::F64(got) => assert!((got - v).abs() / v < 1e-10),
            other => panic!("expected F64, got {other:?}"),
        }
    }
}

// =============================================================================
// Module 20: SAMPLE BY
// =============================================================================
mod sample_by {
    use super::*;

    #[test]
    fn sample_by_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.5)", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT sum(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_avg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_min() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT min(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT max(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// Module 21: Edge cases & multi-column
// =============================================================================
mod edge_cases {
    use super::*;

    #[test]
    fn empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SELECT v FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(42.0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(42.0));
    }

    #[test]
    fn twenty_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(20));
    }

    #[test]
    fn multi_column_select() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 2.0)", ts(0)));
        let (cols, _) = db.query("SELECT a, b FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn expression_alias() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        let (cols, _) = db.query("SELECT v * 2 AS doubled FROM t");
        assert!(cols.contains(&"doubled".to_string()));
    }

    #[test]
    fn where_order_limit() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 0 ORDER BY v DESC LIMIT 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::F64(99.9));
    }

    #[test]
    fn group_having_order_limit() {
        let db = db_f64_grouped();
        let (_, rows) = db.query(
            "SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100 ORDER BY s DESC LIMIT 1",
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_star() {
        let db = db_f64();
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn combined_in_and_between() {
        let db = db_f64();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v IN (0.0, 0.001, 42.5) AND v BETWEEN 0.0 AND 1.0");
        // 0.0, 0.001 = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn descending_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in (0..10).rev() {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(9 - i), i));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        for (i, row) in rows.iter().enumerate().take(10) {
            assert_eq!(row[0], Value::F64(i as f64));
        }
    }
}

// =============================================================================
// Module 22: Multiple aggregates together
// =============================================================================
mod multi_agg {
    use super::*;

    #[test]
    fn sum_and_count() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT sum(v), count(*) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::I64(6));
    }

    #[test]
    fn min_and_max() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT min(v), max(v) FROM t");
        assert_eq!(rows[0][0], Value::F64(-99.9));
        assert_eq!(rows[0][1], Value::F64(99.9));
    }

    #[test]
    fn first_and_last() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT first(v), last(v) FROM t");
        assert_eq!(rows[0][0], Value::F64(-99.9));
        assert_eq!(rows[0][1], Value::F64(99.9));
    }

    #[test]
    fn all_aggregates() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v), sum(v), avg(v) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(6));
    }

    #[test]
    fn aggregates_with_where() {
        let db = db_f64();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v) FROM t WHERE v >= 0");
        assert_eq!(rows[0][0], Value::I64(4));
        assert_eq!(rows[0][1], Value::F64(0.0));
        assert_eq!(rows[0][2], Value::F64(99.9));
    }

    #[test]
    fn group_multi_aggregates() {
        let db = db_f64_grouped();
        let (_, rows) = db
            .query("SELECT grp, count(*), min(v), max(v), sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 23: Additional comparison combinations
// =============================================================================
mod comparison_combos {
    use super::*;

    #[test]
    fn eq_and_gt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v = 42.5 AND v > 0");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn eq_and_lt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v = -99.9 AND v < 0");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn ne_and_gt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v != 0.0 AND v > 0");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn ne_and_lt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v != -0.001 AND v < 0");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn gte_and_lte() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v >= -0.001 AND v <= 0.001");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn gt_and_ne() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v > -0.01 AND v != 0.0");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn lt_and_ne() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v < 50 AND v != -99.9");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn gte_and_ne() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v >= 0 AND v != 42.5");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn lte_and_ne() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v <= 42.5 AND v != 0.0");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn gt_or_eq() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v > 42.5 OR v = -99.9");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn lt_or_eq() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v < -0.001 OR v = 99.9");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn between_and_ne() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 0 AND 100 AND v != 42.5");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn in_and_gt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (0.0, 0.001, 42.5, 99.9) AND v > 0");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn in_or_lt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (42.5, 99.9) OR v < -50");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn not_in_and_between() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v NOT IN (0.0) AND v BETWEEN -1 AND 1");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn gt_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 0"),
            Value::I64(3)
        );
    }
    #[test]
    fn lt_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 0"),
            Value::I64(2)
        );
    }
    #[test]
    fn eq_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 0.0"),
            Value::I64(1)
        );
    }
    #[test]
    fn ne_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v != 0.0"),
            Value::I64(5)
        );
    }
    #[test]
    fn gte_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v >= 0"),
            Value::I64(4)
        );
    }
    #[test]
    fn lte_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v <= 0"),
            Value::I64(3)
        );
    }
    #[test]
    fn between_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN -0.001 AND 0.001"),
            Value::I64(3)
        );
    }
    #[test]
    fn in_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (0.0, 42.5)"),
            Value::I64(2)
        );
    }
    #[test]
    fn not_in_count() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v NOT IN (0.0, 42.5)"),
            Value::I64(4)
        );
    }
}

// =============================================================================
// Module 24: Additional aggregate variations
// =============================================================================
mod agg_variations {
    use super::*;

    #[test]
    fn sum_where_eq() {
        let db = db_f64();
        assert_f64_near(
            &db.query_scalar("SELECT sum(v) FROM t WHERE v = 42.5"),
            42.5,
            0.01,
        );
    }
    #[test]
    fn min_where_gt() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT min(v) FROM t WHERE v > 0"),
            Value::F64(0.001)
        );
    }
    #[test]
    fn max_where_lt() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT max(v) FROM t WHERE v < 0"),
            Value::F64(-0.001)
        );
    }
    #[test]
    fn avg_positive() {
        let db = db_f64();
        let v = db.query_scalar("SELECT avg(v) FROM t WHERE v > 0");
        assert_f64_near(&v, 47.467, 0.01);
    }
    #[test]
    fn count_between() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 0.001 AND 99.9"),
            Value::I64(3)
        );
    }
    #[test]
    fn sum_between() {
        let db = db_f64();
        assert_f64_near(
            &db.query_scalar("SELECT sum(v) FROM t WHERE v BETWEEN 0 AND 100"),
            142.401,
            0.01,
        );
    }
    #[test]
    fn first_where_gt() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT first(v) FROM t WHERE v > 0"),
            Value::F64(0.001)
        );
    }
    #[test]
    fn last_where_lt() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT last(v) FROM t WHERE v < 0"),
            Value::F64(-0.001)
        );
    }
    #[test]
    fn count_in() {
        let db = db_f64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (-99.9, 0.0, 99.9)"),
            Value::I64(3)
        );
    }

    #[test]
    fn grouped_count_a() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[0][0], Value::Str("A".into()));
        assert_eq!(r[0][1], Value::I64(3));
    }

    #[test]
    fn grouped_count_b() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[1][0], Value::Str("B".into()));
        assert_eq!(r[1][1], Value::I64(3));
    }

    #[test]
    fn grouped_count_c() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[2][0], Value::Str("C".into()));
        assert_eq!(r[2][1], Value::I64(2));
    }

    #[test]
    fn grouped_sum_a() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp");
        assert_f64_near(&r[0][1], 90.0, 0.01);
    }

    #[test]
    fn grouped_min_a() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, min(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[0][1], Value::F64(10.0));
    }

    #[test]
    fn grouped_max_a() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, max(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[0][1], Value::F64(50.0));
    }
}

// =============================================================================
// Module 25: Additional arithmetic, CASE, DISTINCT, ORDER combos
// =============================================================================
mod extra_combos {
    use super::*;

    #[test]
    fn add_100() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v + 100 FROM t"), 100.5, 0.01);
    }
    #[test]
    fn sub_100() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 200.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v - 100 FROM t"), 100.5, 0.01);
    }
    #[test]
    fn mul_10() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v * 10 FROM t"), 55.0, 0.01);
    }
    #[test]
    fn div_4() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v / 4 FROM t"), 25.0, 0.01);
    }
    #[test]
    fn paren_expr() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT (v + 3) * 2 FROM t"), 16.0, 0.01);
    }
    #[test]
    fn sub_self() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.5)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v - v FROM t"), 0.0, 0.01);
    }
    #[test]
    fn add_self() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 21.25)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v + v FROM t"), 42.5, 0.01);
    }

    #[test]
    fn case_gt_50() {
        let db = db_f64();
        let (_, r) = db.query("SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_eq_zero() {
        let db = db_f64();
        let (_, r) = db.query("SELECT CASE WHEN v = 0 THEN 'zero' ELSE 'nonzero' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_negative_check() {
        let db = db_f64();
        let (_, r) = db.query("SELECT CASE WHEN v < 0 THEN 'neg' ELSE 'nonneg' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_three_branch() {
        let db = db_f64();
        let (_, r) = db.query(
            "SELECT CASE WHEN v < -50 THEN 'low' WHEN v < 50 THEN 'mid' ELSE 'high' END FROM t",
        );
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_with_where() {
        let db = db_f64();
        let (_, r) =
            db.query("SELECT CASE WHEN v > 0 THEN 'pos' ELSE 'neg' END FROM t WHERE v != 0");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn case_with_limit() {
        let db = db_f64();
        let (_, r) = db.query("SELECT CASE WHEN v > 0 THEN 'pos' ELSE 'neg' END FROM t LIMIT 3");
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn distinct_desc() {
        let db = db_f64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t ORDER BY v DESC");
        assert_eq!(r[0][0], Value::F64(99.9));
    }
    #[test]
    fn distinct_limit() {
        let db = db_f64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn order_limit_one_asc() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1");
        assert_eq!(r[0][0], Value::F64(-99.9));
    }
    #[test]
    fn order_limit_one_desc() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 1");
        assert_eq!(r[0][0], Value::F64(99.9));
    }
    #[test]
    fn offset_three() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 3");
        assert_eq!(r[0][0], Value::F64(0.001));
    }
    #[test]
    fn distinct_where_gt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v > 0");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn distinct_where_lt() {
        let db = db_f64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v < 0");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn order_where_between() {
        let db = db_f64();
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 0 AND 100 ORDER BY v DESC");
        assert_eq!(r[0][0], Value::F64(99.9));
    }
}

// =============================================================================
// Module 26: More GROUP BY + HAVING
// =============================================================================
mod group_having_extra {
    use super::*;

    #[test]
    fn having_avg() {
        let db = db_f64_grouped();
        let (_, r) =
            db.query("SELECT grp, avg(v) AS a FROM t GROUP BY grp HAVING a > 40 ORDER BY grp");
        assert!(!r.is_empty());
    }
    #[test]
    fn having_min() {
        let db = db_f64_grouped();
        let (_, r) =
            db.query("SELECT grp, min(v) AS m FROM t GROUP BY grp HAVING m >= 20 ORDER BY grp");
        assert!(!r.is_empty());
    }
    #[test]
    fn having_max() {
        let db = db_f64_grouped();
        let (_, r) =
            db.query("SELECT grp, max(v) AS m FROM t GROUP BY grp HAVING m > 60 ORDER BY grp");
        assert!(!r.is_empty());
    }
    #[test]
    fn group_order_desc() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY c DESC");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_limit_1() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn having_count_eq() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 2");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn having_count_gt2() {
        let db = db_f64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c > 2");
        assert_eq!(r.len(), 2);
    }
}

// =============================================================================
// Module 27: SAMPLE BY + coalesce + cast extras
// =============================================================================
mod sample_coalesce_cast_extra {
    use super::*;

    #[test]
    fn sample_1h() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.5)", ts(i * 600), i));
        }
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_30m() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 600), i));
        }
        let (_, r) = db.query("SELECT sum(v) FROM t SAMPLE BY 30m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_15m() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i * 60), i));
        }
        let (_, r) = db.query("SELECT avg(v) FROM t SAMPLE BY 15m");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_1d() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 86400),
                i * 100
            ));
        }
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1d");
        assert!(!r.is_empty());
    }
    #[test]
    fn sample_first_last() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.0)",
                ts(i * 600),
                i * 10
            ));
        }
        let (_, r) = db.query("SELECT first(v), last(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn cast_with_limit() {
        let db = db_f64();
        let (_, r) = db.query("SELECT CAST(v AS INT) FROM t LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn coalesce_default_99() {
        let db = db_f64_nullable();
        let (_, r) = db.query("SELECT coalesce(v, 99.0) FROM t");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn cast_negative_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -42.5)", ts(0)));
        match db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t") {
            Value::Str(s) => assert!(s.contains("-42.5")),
            other => panic!("got {other:?}"),
        }
    }
}

// =============================================================================
// Module 28: Wide table + many rows
// =============================================================================
mod wide_many {
    use super::*;

    #[test]
    fn three_double_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.1, 2.2, 3.3)", ts(0)));
        let (_, r) = db.query("SELECT a, b, c FROM t");
        assert_eq!(r[0][0], Value::F64(1.1));
        assert_eq!(r[0][1], Value::F64(2.2));
        assert_eq!(r[0][2], Value::F64(3.3));
    }

    #[test]
    fn sum_three_cols() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 10.0, 20.0, 30.0)",
            ts(0)
        ));
        assert_f64_near(&db.query_scalar("SELECT a + b + c FROM t"), 60.0, 0.01);
    }

    #[test]
    fn fifty_rows_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.5)", ts(i), i));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }

    #[test]
    fn fifty_rows_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 1225.0, 0.01);
    }

    #[test]
    fn fifty_rows_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(0.0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(49.0));
    }

    #[test]
    fn fifty_rows_distinct() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i % 10));
        }
        let (_, r) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_order() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), 49 - i));
        }
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 5");
        assert_eq!(r[0][0], Value::F64(0.0));
    }

    #[test]
    fn fifty_rows_filter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v >= 40");
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
                ts(i),
                grps[i as usize % 5],
                i
            ));
        }
        let (_, r) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_between() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 10 AND 19");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_in() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (0, 10, 20, 30, 40)");
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
                ts(i),
                grps[i as usize % 5],
                i
            ));
        }
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 10");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn mixed_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, d DOUBLE, i BIGINT, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 3.15, 42, 'hello')",
            ts(0)
        ));
        let (_, r) = db.query("SELECT d, i, s FROM t");
        assert_eq!(r[0][0], Value::F64(3.15));
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
        assert_f64_near(&r[0][1], 50.0, 0.01);
    }
}

// =============================================================================
// Module 29: Bulk comparison tests on 10-row table
// =============================================================================
mod bulk_comparisons {
    use super::*;

    fn db10() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i * 10));
        }
        db
    }

    #[test]
    fn gt_v0() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 0"),
            Value::I64(9)
        );
    }
    #[test]
    fn gt_v10() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 10"),
            Value::I64(8)
        );
    }
    #[test]
    fn gt_v20() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 20"),
            Value::I64(7)
        );
    }
    #[test]
    fn gt_v30() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 30"),
            Value::I64(6)
        );
    }
    #[test]
    fn gt_v40() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 40"),
            Value::I64(5)
        );
    }
    #[test]
    fn gt_v50() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 50"),
            Value::I64(4)
        );
    }
    #[test]
    fn gt_v60() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 60"),
            Value::I64(3)
        );
    }
    #[test]
    fn gt_v70() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 70"),
            Value::I64(2)
        );
    }
    #[test]
    fn gt_v80() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 80"),
            Value::I64(1)
        );
    }
    #[test]
    fn gt_v90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 90"),
            Value::I64(0)
        );
    }
    #[test]
    fn lt_v10() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 10"),
            Value::I64(1)
        );
    }
    #[test]
    fn lt_v20() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 20"),
            Value::I64(2)
        );
    }
    #[test]
    fn lt_v30() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 30"),
            Value::I64(3)
        );
    }
    #[test]
    fn lt_v40() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 40"),
            Value::I64(4)
        );
    }
    #[test]
    fn lt_v50() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 50"),
            Value::I64(5)
        );
    }
    #[test]
    fn lt_v60() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 60"),
            Value::I64(6)
        );
    }
    #[test]
    fn lt_v70() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 70"),
            Value::I64(7)
        );
    }
    #[test]
    fn lt_v80() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 80"),
            Value::I64(8)
        );
    }
    #[test]
    fn lt_v90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 90"),
            Value::I64(9)
        );
    }
    #[test]
    fn gte_v0() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v >= 0"),
            Value::I64(10)
        );
    }
    #[test]
    fn gte_v50() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v >= 50"),
            Value::I64(5)
        );
    }
    #[test]
    fn gte_v90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v >= 90"),
            Value::I64(1)
        );
    }
    #[test]
    fn lte_v0() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v <= 0"),
            Value::I64(1)
        );
    }
    #[test]
    fn lte_v50() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v <= 50"),
            Value::I64(6)
        );
    }
    #[test]
    fn lte_v90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v <= 90"),
            Value::I64(10)
        );
    }
    #[test]
    fn btw_0_90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 0 AND 90"),
            Value::I64(10)
        );
    }
    #[test]
    fn btw_20_60() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 20 AND 60"),
            Value::I64(5)
        );
    }
    #[test]
    fn in_0_50_90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (0, 50, 90)"),
            Value::I64(3)
        );
    }
    #[test]
    fn eq_v0() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 0"),
            Value::I64(1)
        );
    }
    #[test]
    fn eq_v50() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 50"),
            Value::I64(1)
        );
    }
    #[test]
    fn ne_v0() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v != 0"),
            Value::I64(9)
        );
    }
    #[test]
    fn sum10() {
        let db = db10();
        assert_f64_near(&db.query_scalar("SELECT sum(v) FROM t"), 450.0, 0.01);
    }
    #[test]
    fn avg10() {
        let db = db10();
        assert_f64_near(&db.query_scalar("SELECT avg(v) FROM t"), 45.0, 0.01);
    }
    #[test]
    fn min10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(0.0));
    }
    #[test]
    fn max10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(90.0));
    }
    #[test]
    fn first10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::F64(0.0));
    }
    #[test]
    fn last10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::F64(90.0));
    }
    #[test]
    fn order_asc1() {
        let db = db10();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1");
        assert_eq!(r[0][0], Value::F64(0.0));
    }
    #[test]
    fn order_desc1() {
        let db = db10();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 1");
        assert_eq!(r[0][0], Value::F64(90.0));
    }
    #[test]
    fn distinct10() {
        let db = db10();
        let (_, r) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn limit5() {
        let db = db10();
        let (_, r) = db.query("SELECT v FROM t LIMIT 5");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn offset5() {
        let db = db10();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 5 OFFSET 5");
        assert_eq!(r[0][0], Value::F64(50.0));
    }
    #[test]
    fn case10() {
        let db = db10();
        let (_, r) = db.query("SELECT CASE WHEN v >= 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn add1() {
        let db = db10();
        let (_, r) = db.query("SELECT v + 1 FROM t LIMIT 1");
        assert_f64_near(&r[0][0], 1.0, 0.01);
    }
    #[test]
    fn mul2() {
        let db = db10();
        let (_, r) = db.query("SELECT v * 2 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 100.0, 0.01);
    }
    #[test]
    fn neg10() {
        let db = db10();
        let (_, r) = db.query("SELECT -v FROM t WHERE v = 30");
        assert_f64_near(&r[0][0], -30.0, 0.01);
    }
    #[test]
    fn cast_int() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS INT) FROM t WHERE v = 50"),
            Value::I64(50)
        );
    }
    #[test]
    fn coalesce10() {
        let db = db10();
        let (_, r) = db.query("SELECT coalesce(v, 0.0) FROM t");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn sum_gt_50() {
        let db = db10();
        assert_f64_near(
            &db.query_scalar("SELECT sum(v) FROM t WHERE v > 50"),
            300.0,
            0.01,
        );
    }
    #[test]
    fn avg_lt_50() {
        let db = db10();
        assert_f64_near(
            &db.query_scalar("SELECT avg(v) FROM t WHERE v < 50"),
            20.0,
            0.01,
        );
    }
    #[test]
    fn count_btw() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 10 AND 80"),
            Value::I64(8)
        );
    }
    #[test]
    fn min_gt_20() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT min(v) FROM t WHERE v > 20"),
            Value::F64(30.0)
        );
    }
    #[test]
    fn max_lt_70() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT max(v) FROM t WHERE v < 70"),
            Value::F64(60.0)
        );
    }
    #[test]
    fn and_gt_lt() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 20 AND v < 80"),
            Value::I64(5)
        );
    }
    #[test]
    fn or_eq() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 0 OR v = 90"),
            Value::I64(2)
        );
    }
    #[test]
    fn sample_10m() {
        let db = db10();
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 10m");
        assert!(!r.is_empty());
    }
    #[test]
    fn div_10() {
        let db = db10();
        let (_, r) = db.query("SELECT v / 10 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 5.0, 0.01);
    }
    #[test]
    fn add_sub() {
        let db = db10();
        let (_, r) = db.query("SELECT v + 10 - 5 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 55.0, 0.01);
    }
    #[test]
    fn sub_10() {
        let db = db10();
        let (_, r) = db.query("SELECT v - 10 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 40.0, 0.01);
    }
    #[test]
    fn add_self() {
        let db = db10();
        let (_, r) = db.query("SELECT v + v FROM t WHERE v = 30");
        assert_f64_near(&r[0][0], 60.0, 0.01);
    }
    #[test]
    fn sub_self() {
        let db = db10();
        let (_, r) = db.query("SELECT v - v FROM t WHERE v = 30");
        assert_f64_near(&r[0][0], 0.0, 0.01);
    }
}

// =============================================================================
// Module 30: Systematic WHERE + aggregate combos
// =============================================================================
mod where_agg_combos {
    use super::*;

    fn mk() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE, g VARCHAR)");
        for i in 0..20 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}.5, '{}')",
                ts(i),
                i * 5,
                if i % 2 == 0 { "even" } else { "odd" }
            ));
        }
        db
    }

    #[test]
    fn count_all() {
        assert_eq!(mk().query_scalar("SELECT count(*) FROM t"), Value::I64(20));
    }
    #[test]
    fn count_even() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE g = 'even'"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_odd() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE g = 'odd'"),
            Value::I64(10)
        );
    }
    #[test]
    fn sum_all() {
        assert_f64_near(&mk().query_scalar("SELECT sum(v) FROM t"), 960.0, 1.0);
    }
    #[test]
    fn min_all() {
        assert_f64_near(&mk().query_scalar("SELECT min(v) FROM t"), 0.5, 0.01);
    }
    #[test]
    fn max_all() {
        assert_f64_near(&mk().query_scalar("SELECT max(v) FROM t"), 95.5, 0.01);
    }
    #[test]
    fn avg_all() {
        assert_f64_near(&mk().query_scalar("SELECT avg(v) FROM t"), 48.0, 0.01);
    }
    #[test]
    fn first_all() {
        assert_f64_near(&mk().query_scalar("SELECT first(v) FROM t"), 0.5, 0.01);
    }
    #[test]
    fn last_all() {
        assert_f64_near(&mk().query_scalar("SELECT last(v) FROM t"), 95.5, 0.01);
    }
    #[test]
    fn count_gt_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v > 50"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_lt_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v < 50"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_btw() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 20 AND 70"),
            Value::I64(10)
        );
    }
    #[test]
    fn grp_count() {
        let (_, r) = mk().query("SELECT g, count(*) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_sum() {
        let (_, r) = mk().query("SELECT g, sum(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_min() {
        let (_, r) = mk().query("SELECT g, min(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_max() {
        let (_, r) = mk().query("SELECT g, max(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_avg() {
        let (_, r) = mk().query("SELECT g, avg(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_first() {
        let (_, r) = mk().query("SELECT g, first(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_last() {
        let (_, r) = mk().query("SELECT g, last(v) FROM t GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn having_gt_5() {
        let (_, r) = mk().query("SELECT g, count(*) AS c FROM t GROUP BY g HAVING c > 5");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn having_sum_gt_400() {
        let (_, r) = mk().query("SELECT g, sum(v) AS s FROM t GROUP BY g HAVING s > 400");
        assert_eq!(r.len(), 2);
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
    fn distinct_g() {
        let (_, r) = mk().query("SELECT DISTINCT g FROM t");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn distinct_v() {
        let (_, r) = mk().query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn case_hl() {
        let (_, r) = mk().query("SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn add_10() {
        let (_, r) = mk().query("SELECT v + 10 FROM t LIMIT 1");
        assert_f64_near(&r[0][0], 10.5, 0.01);
    }
    #[test]
    fn neg_v() {
        let v = mk().query_scalar("SELECT -v FROM t WHERE v = 50.5");
        assert_f64_near(&v, -50.5, 0.01);
    }
    #[test]
    fn in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v IN (0.5, 50.5, 95.5)"),
            Value::I64(3)
        );
    }
    #[test]
    fn not_in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v NOT IN (0.5, 50.5, 95.5)"),
            Value::I64(17)
        );
    }
    #[test]
    fn cast_int() {
        assert_eq!(
            mk().query_scalar("SELECT CAST(v AS INT) FROM t WHERE v = 50.5"),
            Value::I64(50)
        );
    }
    #[test]
    fn coalesce_v() {
        let (_, r) = mk().query("SELECT coalesce(v, 0.0) FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn sample_1h() {
        let (_, r) = mk().query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
    #[test]
    fn limit_10() {
        let (_, r) = mk().query("SELECT v FROM t LIMIT 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn offset_10() {
        let (_, r) = mk().query("SELECT v FROM t ORDER BY v ASC LIMIT 10 OFFSET 10");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn star() {
        let (c, r) = mk().query("SELECT * FROM t LIMIT 5");
        assert_eq!(c.len(), 3);
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn where_and_order() {
        let (_, r) = mk().query("SELECT v FROM t WHERE v > 50 ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn where_and_distinct() {
        let (_, r) = mk().query("SELECT DISTINCT g FROM t WHERE v > 50");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn mul_3() {
        let v = mk().query_scalar("SELECT v * 3 FROM t WHERE v = 10.5");
        assert_f64_near(&v, 31.5, 0.01);
    }
    #[test]
    fn sub_5() {
        let v = mk().query_scalar("SELECT v - 5 FROM t WHERE v = 10.5");
        assert_f64_near(&v, 5.5, 0.01);
    }
    #[test]
    fn div_5() {
        let v = mk().query_scalar("SELECT v / 5 FROM t WHERE v = 50.5");
        assert_f64_near(&v, 10.1, 0.01);
    }
    #[test]
    fn alias_select() {
        let (c, _) = mk().query("SELECT v AS val, g AS grp FROM t LIMIT 1");
        assert!(c.contains(&"val".to_string()));
    }
    #[test]
    fn two_aggs() {
        let (_, r) = mk().query("SELECT min(v), max(v) FROM t");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn three_aggs() {
        let (_, r) = mk().query("SELECT count(*), sum(v), avg(v) FROM t");
        assert_eq!(r[0][0], Value::I64(20));
    }
    #[test]
    fn four_aggs() {
        let (_, r) = mk().query("SELECT count(*), min(v), max(v), sum(v) FROM t");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn grp_limit() {
        let (_, r) = mk().query("SELECT g, sum(v) FROM t GROUP BY g ORDER BY g LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn eq_and_order() {
        let (_, r) = mk().query("SELECT v FROM t WHERE g = 'even' ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn btw_and_grp() {
        let (_, r) =
            mk().query("SELECT g, count(*) FROM t WHERE v BETWEEN 20 AND 70 GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn case_even_odd() {
        let (_, r) = mk().query("SELECT CASE WHEN g = 'even' THEN 'E' ELSE 'O' END FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn add_sub() {
        let v = mk().query_scalar("SELECT v + 10 - 5 FROM t WHERE v = 50.5");
        assert_f64_near(&v, 55.5, 0.01);
    }
    #[test]
    fn mul_div() {
        let v = mk().query_scalar("SELECT v * 2 / 2 FROM t WHERE v = 50.5");
        assert_f64_near(&v, 50.5, 0.01);
    }
}
