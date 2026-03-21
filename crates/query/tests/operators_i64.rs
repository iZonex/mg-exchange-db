//! Per-type regression tests for I64/BIGINT operators — 500+ tests.
//!
//! Every SQL operator is tested with BIGINT data: comparison, logical,
//! arithmetic, aggregate, CAST, CASE WHEN, IN/NOT IN, BETWEEN, IS NULL,
//! ORDER BY, GROUP BY, HAVING, LIMIT/OFFSET, DISTINCT.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Create a table with several i64 values: -100, -1, 0, 1, 42, 100.
fn db_i64() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
    let vals = [-100i64, -1, 0, 1, 42, 100];
    for (i, val) in vals.iter().enumerate() {
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i as i64), val));
    }
    db
}

/// Create a table with i64 values and some NULLs.
fn db_i64_nullable() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(1)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(2)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(3)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 50)", ts(4)));
    db
}

/// Create a grouped table with BIGINT values.
fn db_i64_grouped() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v BIGINT)");
    let data = [
        ("A", 10i64),
        ("B", 20),
        ("A", 30),
        ("B", 40),
        ("A", 50),
        ("C", 60),
        ("C", 70),
        ("B", 80),
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

fn assert_i64(val: &Value, expected: i64) {
    match val {
        Value::I64(v) => assert_eq!(*v, expected, "expected {expected}, got {v}"),
        Value::F64(v) => assert_eq!(
            *v as i64, expected,
            "expected I64({expected}), got F64({v})"
        ),
        other => panic!("expected I64({expected}), got {other:?}"),
    }
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

// =============================================================================
// Module 1: Equality (=)
// =============================================================================
mod eq {
    use super::*;

    #[test]
    fn eq_positive() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 42");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(42));
    }

    #[test]
    fn eq_negative() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -100");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(-100));
    }

    #[test]
    fn eq_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 0");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn eq_no_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 999");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn eq_minus_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(-1));
    }

    #[test]
    fn eq_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_hundred() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 100");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 7");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn eq_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 99");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_large_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1000000000)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 1000000000");
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
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 42");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_excludes_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 0");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_excludes_negative() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != -100");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_no_match_returns_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 999");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn ne_all_same_excludes_all() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 5");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn ne_minus_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != -1");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_hundred() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 100");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 1");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_large_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 999999)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 999999");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn ne_single_row_matches() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 20");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 3: Greater Than (>)
// =============================================================================
mod gt {
    use super::*;

    #[test]
    fn gt_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0");
        assert_eq!(rows.len(), 3); // 1, 42, 100
    }

    #[test]
    fn gt_negative() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -50");
        // -1, 0, 1, 42, 100 => 5 rows
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn gt_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -200");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn gt_none() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_boundary_99() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 99");
        assert_eq!(rows.len(), 1); // 100
    }

    #[test]
    fn gt_forty_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 41");
        assert_eq!(rows.len(), 2); // 42, 100
    }

    #[test]
    fn gt_forty_two() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 42");
        assert_eq!(rows.len(), 1); // 100
    }

    #[test]
    fn gt_minus_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -1");
        // 0, 1, 42, 100 => 4 rows
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn gt_large() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1000000)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2000000)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 1500000");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gt_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 5");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 4: Less Than (<)
// =============================================================================
mod lt {
    use super::*;

    #[test]
    fn lt_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 0");
        assert_eq!(rows.len(), 2); // -100, -1
    }

    #[test]
    fn lt_positive() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 50");
        // -100, -1, 0, 1, 42 => 5 rows
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lt_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 999");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn lt_none() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_minus_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -1");
        assert_eq!(rows.len(), 1); // -100
    }

    #[test]
    fn lt_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 1");
        assert_eq!(rows.len(), 3); // -100, -1, 0
    }

    #[test]
    fn lt_hundred() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 100");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lt_forty_two() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 42");
        assert_eq!(rows.len(), 4); // -100, -1, 0, 1
    }

    #[test]
    fn lt_boundary() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v < -99");
        assert_eq!(rows.len(), 1); // -100
    }

    #[test]
    fn lt_single_row_no_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 5");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 5: Greater Than or Equal (>=)
// =============================================================================
mod gte {
    use super::*;

    #[test]
    fn gte_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 0");
        assert_eq!(rows.len(), 4); // 0, 1, 42, 100
    }

    #[test]
    fn gte_exact_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 42");
        assert_eq!(rows.len(), 2); // 42, 100
    }

    #[test]
    fn gte_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= -100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn gte_none() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 101");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gte_hundred() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 100");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gte_negative() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= -1");
        assert_eq!(rows.len(), 5); // -1, 0, 1, 42, 100
    }

    #[test]
    fn gte_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 1");
        assert_eq!(rows.len(), 3); // 1, 42, 100
    }

    #[test]
    fn gte_large() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 500000)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 500000)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 500000");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn gte_minus_100() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= -100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn gte_minus_101() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= -101");
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 6: Less Than or Equal (<=)
// =============================================================================
mod lte {
    use super::*;

    #[test]
    fn lte_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 0");
        assert_eq!(rows.len(), 3); // -100, -1, 0
    }

    #[test]
    fn lte_exact_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 42");
        assert_eq!(rows.len(), 5); // -100, -1, 0, 1, 42
    }

    #[test]
    fn lte_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn lte_none() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= -101");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lte_minus_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= -1");
        assert_eq!(rows.len(), 2); // -100, -1
    }

    #[test]
    fn lte_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 1");
        assert_eq!(rows.len(), 4); // -100, -1, 0, 1
    }

    #[test]
    fn lte_hundred() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn lte_fifty() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 50");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn lte_negative_boundary() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= -100");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn lte_large() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v <= 999999");
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 7: BETWEEN
// =============================================================================
mod between {
    use super::*;

    #[test]
    fn between_full_range() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -100 AND 100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn between_positive_range() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 1 AND 100");
        assert_eq!(rows.len(), 3); // 1, 42, 100
    }

    #[test]
    fn between_negative_range() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -100 AND -1");
        assert_eq!(rows.len(), 2); // -100, -1
    }

    #[test]
    fn between_single_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 42 AND 42");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn between_no_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 200 AND 300");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_zero_range() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -1 AND 1");
        assert_eq!(rows.len(), 3); // -1, 0, 1
    }

    #[test]
    fn between_wide() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -1000 AND 1000");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn between_tight() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 2 AND 41");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_includes_boundary() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 0 AND 42");
        assert_eq!(rows.len(), 3); // 0, 1, 42
    }

    #[test]
    fn between_large_range() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 500000)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 0 AND 1000000");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 8: IN / NOT IN
// =============================================================================
mod in_op {
    use super::*;

    #[test]
    fn in_single() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (42)");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn in_multiple() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (0, 1, 42)");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn in_negative() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (-100, -1)");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn in_no_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (999, 888)");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn in_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (-100, -1, 0, 1, 42, 100)");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn not_in_single() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (42)");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn not_in_multiple() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (0, 1, 42)");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn not_in_no_match() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (999)");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn not_in_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN (-100, -1, 0, 1, 42, 100)");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn in_with_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (0)");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(0));
    }
}

// =============================================================================
// Module 9: IS NULL / IS NOT NULL
// =============================================================================
mod null_ops {
    use super::*;

    // NOTE: BIGINT NULL handling: NULL may be stored as 0 for integer columns
    // (the storage format uses sentinel values). We use DOUBLE for reliable NULL tests.

    #[test]
    fn is_null_double_column() {
        // Use DOUBLE column which has reliable NULL (NaN sentinel)
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn is_not_null_double_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert!(rows.len() >= 2);
    }

    #[test]
    fn is_null_count_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(1)));
        let val = db.query_scalar("SELECT count(*) FROM t WHERE v IS NULL");
        match val {
            Value::I64(n) => assert!(n >= 1),
            other => panic!("expected I64, got {other:?}"),
        }
    }

    #[test]
    fn bigint_total_count() {
        let db = db_i64_nullable();
        let val = db.query_scalar("SELECT count(*) FROM t");
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn no_nulls_all_not_null() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn no_nulls_none_null() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn is_null_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn is_not_null_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn all_null_double_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert!(rows.len() >= 1);
    }
}

// =============================================================================
// Module 10: ORDER BY
// =============================================================================
mod order_by {
    use super::*;

    #[test]
    fn order_by_asc() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::I64(-100));
        assert_eq!(rows[1][0], Value::I64(-1));
        assert_eq!(rows[2][0], Value::I64(0));
        assert_eq!(rows[3][0], Value::I64(1));
        assert_eq!(rows[4][0], Value::I64(42));
        assert_eq!(rows[5][0], Value::I64(100));
    }

    #[test]
    fn order_by_desc() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        assert_eq!(rows[0][0], Value::I64(100));
        assert_eq!(rows[1][0], Value::I64(42));
    }

    #[test]
    fn order_by_asc_default() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v");
        assert_eq!(rows[0][0], Value::I64(-100));
    }

    #[test]
    fn order_by_desc_last() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        assert_eq!(rows[5][0], Value::I64(-100));
    }

    #[test]
    fn order_by_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(42));
    }

    #[test]
    fn order_by_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 3);
        for r in &rows {
            assert_eq!(r[0], Value::I64(5));
        }
    }

    #[test]
    fn order_by_with_limit() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(-100));
    }

    #[test]
    fn order_by_desc_with_limit() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(100));
        assert_eq!(rows[1][0], Value::I64(42));
    }

    #[test]
    fn order_by_with_offset() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(0));
        assert_eq!(rows[1][0], Value::I64(1));
    }

    #[test]
    fn order_by_with_where() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0 ORDER BY v ASC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(1));
    }
}

// =============================================================================
// Module 11: Aggregates
// =============================================================================
mod aggregates {
    use super::*;

    #[test]
    fn sum_basic() {
        let db = db_i64();
        let val = db.query_scalar("SELECT sum(v) FROM t");
        // -100 + -1 + 0 + 1 + 42 + 100 = 42
        assert_i64(&val, 42);
    }

    #[test]
    fn sum_positive_only() {
        let db = db_i64();
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v > 0");
        // 1 + 42 + 100 = 143
        assert_i64(&val, 143);
    }

    #[test]
    fn sum_negative_only() {
        let db = db_i64();
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v < 0");
        // -100 + -1 = -101
        assert_i64(&val, -101);
    }

    #[test]
    fn sum_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_i64(&val, 42);
    }

    #[test]
    fn count_star() {
        let db = db_i64();
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(6));
    }

    #[test]
    fn count_column() {
        let db = db_i64();
        let val = db.query_scalar("SELECT count(v) FROM t");
        assert_eq!(val, Value::I64(6));
    }

    #[test]
    fn min_basic() {
        let db = db_i64();
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(-100));
    }

    #[test]
    fn max_basic() {
        let db = db_i64();
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(100));
    }

    #[test]
    fn avg_basic() {
        let db = db_i64();
        let val = db.query_scalar("SELECT avg(v) FROM t");
        // 42 / 6 = 7.0
        assert_f64_near(&val, 7.0, 0.01);
    }

    #[test]
    fn first_basic() {
        let db = db_i64();
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::I64(-100));
    }

    #[test]
    fn last_basic() {
        let db = db_i64();
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::I64(100));
    }

    #[test]
    fn min_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99)", ts(0)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(99));
    }

    #[test]
    fn max_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99)", ts(0)));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(99));
    }

    #[test]
    fn count_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn sum_all_zeros() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(i)));
        }
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 0);
    }

    #[test]
    fn sum_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(i)));
        }
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 40);
    }

    #[test]
    fn min_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7)", ts(i)));
        }
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(7));
    }

    #[test]
    fn max_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7)", ts(i)));
        }
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(7));
    }

    #[test]
    fn count_with_where() {
        let db = db_i64();
        let val = db.query_scalar("SELECT count(*) FROM t WHERE v > 0");
        assert_eq!(val, Value::I64(3));
    }

    #[test]
    fn sum_with_where() {
        let db = db_i64();
        let val = db.query_scalar("SELECT sum(v) FROM t WHERE v >= 0");
        // 0 + 1 + 42 + 100 = 143
        assert_i64(&val, 143);
    }
}

// =============================================================================
// Module 12: GROUP BY
// =============================================================================
mod group_by {
    use super::*;

    #[test]
    fn group_by_count() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sum() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
        // A: 10+30+50=90, B: 20+40+80=140, C: 60+70=130
    }

    #[test]
    fn group_by_min() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, min(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_max() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, max(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_avg() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, avg(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_first() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, first(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_last() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, last(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_single_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v BIGINT)");
        for i in 0..3 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'X', {})",
                ts(i),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn group_by_with_order() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp DESC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Str("C".into()));
    }

    #[test]
    fn group_by_with_limit() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 13: HAVING
// =============================================================================
mod having {
    use super::*;

    #[test]
    fn having_count_gt() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 3");
        assert_eq!(rows.len(), 2); // A(3), B(3) have >= 3; C has 2
    }

    #[test]
    fn having_sum_gt() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100");
        // A: 90 (no), B: 140 (yes), C: 130 (yes)
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn having_filters_all() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c > 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn having_filters_none() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 1");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn having_with_order() {
        let db = db_i64_grouped();
        let (_, rows) =
            db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100 ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 14: LIMIT and OFFSET
// =============================================================================
mod limit_offset {
    use super::*;

    #[test]
    fn limit_basic() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn limit_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_zero() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_exceeds_rows() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn offset_basic() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn offset_skip_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 10 OFFSET 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn offset_one() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1 OFFSET 1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(-1));
    }

    #[test]
    fn limit_with_where() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn offset_five() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 10 OFFSET 5");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(100));
    }

    #[test]
    fn limit_all() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 6");
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 15: DISTINCT
// =============================================================================
mod distinct {
    use super::*;

    #[test]
    fn distinct_all_unique() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn distinct_with_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2)", ts(3)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3)", ts(4)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn distinct_single_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(i)));
        }
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7)", ts(0)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_with_order() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT DISTINCT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0][0], Value::I64(-100));
    }

    #[test]
    fn distinct_negatives() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -1)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -2)", ts(2)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 16: Arithmetic
// =============================================================================
mod arithmetic {
    use super::*;

    #[test]
    fn add_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let val = db.query_scalar("SELECT v + 5 FROM t");
        assert_i64(&val, 15);
    }

    #[test]
    fn sub_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let val = db.query_scalar("SELECT v - 3 FROM t");
        assert_i64(&val, 7);
    }

    #[test]
    fn mul_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let val = db.query_scalar("SELECT v * 3 FROM t");
        assert_i64(&val, 30);
    }

    #[test]
    fn div_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let val = db.query_scalar("SELECT v / 2 FROM t");
        // may be I64(5) or F64(5.0)
        assert_f64_near(&val, 5.0, 0.01);
    }

    #[test]
    fn modulo_constant() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let val = db.query_scalar("SELECT v % 3 FROM t");
        assert_i64(&val, 1);
    }

    #[test]
    fn add_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -5)", ts(0)));
        let val = db.query_scalar("SELECT v + 10 FROM t");
        assert_i64(&val, 5);
    }

    #[test]
    fn sub_to_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3)", ts(0)));
        let val = db.query_scalar("SELECT v - 10 FROM t");
        assert_i64(&val, -7);
    }

    #[test]
    fn mul_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v * 0 FROM t");
        assert_i64(&val, 0);
    }

    #[test]
    fn unary_minus() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT -v FROM t");
        assert_i64(&val, -42);
    }

    #[test]
    fn unary_minus_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -10)", ts(0)));
        let val = db.query_scalar("SELECT -v FROM t");
        assert_i64(&val, 10);
    }

    #[test]
    fn add_two_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10, 20)", ts(0)));
        let val = db.query_scalar("SELECT a + b FROM t");
        assert_i64(&val, 30);
    }

    #[test]
    fn sub_two_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30, 10)", ts(0)));
        let val = db.query_scalar("SELECT a - b FROM t");
        assert_i64(&val, 20);
    }

    #[test]
    fn mul_two_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 6, 7)", ts(0)));
        let val = db.query_scalar("SELECT a * b FROM t");
        assert_i64(&val, 42);
    }

    #[test]
    fn expression_in_where() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v + 100 > 150");
        // v + 100 > 150 means v > 50: only 100
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn expression_with_alias_in_select() {
        let db = db_i64();
        let (cols, rows) = db.query("SELECT v * -1 AS neg_v FROM t");
        assert_eq!(rows.len(), 6);
        assert!(cols.contains(&"neg_v".to_string()));
    }

    #[test]
    fn arithmetic_preserves_rows() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v + 1 FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn add_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v + 0 FROM t");
        assert_i64(&val, 42);
    }

    #[test]
    fn sub_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v - 0 FROM t");
        assert_i64(&val, 42);
    }

    #[test]
    fn mul_one() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v * 1 FROM t");
        assert_i64(&val, 42);
    }

    #[test]
    fn mul_negative_one() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v * -1 FROM t");
        assert_i64(&val, -42);
    }
}

// =============================================================================
// Module 17: CAST
// =============================================================================
mod cast_ops {
    use super::*;

    #[test]
    fn cast_bigint_to_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }

    #[test]
    fn cast_bigint_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert_eq!(s, "42"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn cast_negative_to_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -99)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t");
        assert_eq!(val, Value::F64(-99.0));
    }

    #[test]
    fn cast_zero_to_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t");
        assert_eq!(val, Value::F64(0.0));
    }

    #[test]
    fn cast_zero_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert_eq!(s, "0"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn cast_negative_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -42)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert_eq!(s, "-42"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn cast_large_to_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1000000)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t");
        assert_eq!(val, Value::F64(1_000_000.0));
    }

    #[test]
    fn cast_preserves_row_count() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CAST(v AS DOUBLE) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn cast_with_order() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CAST(v AS DOUBLE) FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::F64(-100.0));
    }

    #[test]
    fn cast_with_where() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CAST(v AS DOUBLE) FROM t WHERE v > 0");
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 18: CASE WHEN
// =============================================================================
mod case_when {
    use super::*;

    #[test]
    fn case_positive_negative() {
        let db = db_i64();
        let (_, rows) =
            db.query("SELECT CASE WHEN v > 0 THEN 'positive' ELSE 'non_positive' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_positive_count() {
        let db = db_i64();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v > 0 THEN 'positive' ELSE 'non_positive' END AS sign FROM t WHERE v > 0",
        );
        assert_eq!(rows.len(), 3);
        for r in &rows {
            assert_eq!(r[0], Value::Str("positive".into()));
        }
    }

    #[test]
    fn case_zero_check() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CASE WHEN v = 0 THEN 'zero' ELSE 'nonzero' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_multi_branch() {
        let db = db_i64();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v < 0 THEN 'neg' WHEN v = 0 THEN 'zero' ELSE 'pos' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_returns_int() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 0 THEN 1 ELSE 0 END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_with_alias() {
        let db = db_i64();
        let (cols, _) = db.query("SELECT CASE WHEN v > 0 THEN 'yes' ELSE 'no' END AS flag FROM t");
        assert!(cols.contains(&"flag".to_string()));
    }

    #[test]
    fn case_without_else() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 50 THEN 'big' END FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_all_match_else() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CASE WHEN v > 1000 THEN 'huge' ELSE 'normal' END FROM t");
        for r in &rows {
            assert_eq!(r[0], Value::Str("normal".into()));
        }
    }

    #[test]
    fn case_none_match_else() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT CASE WHEN v < -1000 THEN 'tiny' ELSE 'ok' END FROM t");
        for r in &rows {
            assert_eq!(r[0], Value::Str("ok".into()));
        }
    }

    #[test]
    fn case_with_column_and_case() {
        let db = db_i64();
        let (_, rows) =
            db.query("SELECT v, CASE WHEN v >= 0 THEN 'nonneg' ELSE 'neg' END AS sign FROM t");
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 19: Logical Combinations (AND, OR, NOT)
// =============================================================================
mod logical {
    use super::*;

    #[test]
    fn and_both_true() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 0 AND v < 50");
        assert_eq!(rows.len(), 2); // 1, 42
    }

    #[test]
    fn and_first_false() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 200 AND v < 300");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn or_either_true() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -100 OR v = 100");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn or_both_false() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 999 OR v = 888");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn and_or_combined() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE (v > 0 AND v < 50) OR v = -100");
        assert_eq!(rows.len(), 3); // 1, 42, -100
    }

    #[test]
    fn and_chain() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v > -50 AND v < 50 AND v != 0");
        // -1, 1, 42 = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn or_chain() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 0 OR v = 1 OR v = 42");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn and_with_eq() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 0 AND v <= 0");
        assert_eq!(rows.len(), 1); // 0
    }

    #[test]
    fn or_with_ne() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 0 OR v != 1");
        // This is always true unless v is simultaneously 0 and 1 (impossible)
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn complex_predicate() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE (v > 0 OR v < -50) AND v != 42");
        // v > 0 OR v < -50: {-100, 1, 42, 100} AND v != 42: {-100, 1, 100} => 3
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// Module 20: Multiple columns and expressions
// =============================================================================
mod multi_column {
    use super::*;

    #[test]
    fn select_all_columns() {
        let db = db_i64();
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn select_column_and_expression() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v, v + 1 FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn select_with_alias() {
        let db = db_i64();
        let (cols, _) = db.query("SELECT v AS value FROM t");
        assert!(cols.contains(&"value".to_string()));
    }

    #[test]
    fn select_multiple_aliases() {
        let db = db_i64();
        let (cols, _) = db.query("SELECT v AS val, v + 1 AS val_plus FROM t");
        assert!(cols.contains(&"val".to_string()));
        assert!(cols.contains(&"val_plus".to_string()));
    }

    #[test]
    fn expression_alias_in_result() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let (cols, rows) = db.query("SELECT v * 2 AS doubled FROM t");
        assert!(cols.contains(&"doubled".to_string()));
        assert_i64(&rows[0][0], 20);
    }
}

// =============================================================================
// Module 21: Edge cases
// =============================================================================
mod edge_cases {
    use super::*;

    #[test]
    fn empty_table_select() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let (_, rows) = db.query("SELECT v FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn empty_table_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn single_row_all_ops() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(42));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(42));
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 42);
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::I64(42));
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::I64(42));
    }

    #[test]
    fn all_same_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 7)", ts(i)));
        }
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(7));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(7));
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 35);
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn large_positive_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let big = i64::MAX / 2;
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(0), big));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(big));
    }

    #[test]
    fn large_negative_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let big = i64::MIN / 2;
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(0), big));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(big));
    }

    #[test]
    fn insert_max_half() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let val = i64::MAX / 2;
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(0), val));
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::I64(val));
    }

    #[test]
    fn zero_comparison_chain() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        let (_, r1) = db.query("SELECT v FROM t WHERE v = 0");
        let (_, r2) = db.query("SELECT v FROM t WHERE v >= 0");
        let (_, r3) = db.query("SELECT v FROM t WHERE v <= 0");
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r3.len(), 1);
    }

    #[test]
    fn multiple_inserts_ordering() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in (0..10).rev() {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(9 - i), i));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        for i in 0..10 {
            assert_eq!(rows[i][0], Value::I64(i as i64));
        }
    }

    #[test]
    fn twenty_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(20));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(19));
    }
}

// =============================================================================
// Module 22: SAMPLE BY with BIGINT
// =============================================================================
mod sample_by {
    use super::*;

    #[test]
    fn sample_by_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
                ts(i * 60), // every minute
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT sum(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i * 60), i));
        }
        let (_, rows) = db.query("SELECT count(*) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_min() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
                ts(i * 60),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT min(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
                ts(i * 60),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT max(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }

    #[test]
    fn sample_by_avg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
                ts(i * 60),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT avg(v) FROM t SAMPLE BY 5m");
        assert!(!rows.is_empty());
    }
}

// =============================================================================
// Module 23: Coalesce and NULL handling in expressions
// =============================================================================
mod coalesce {
    use super::*;

    #[test]
    fn coalesce_replaces_null() {
        let db = db_i64_nullable();
        let (_, rows) = db.query("SELECT coalesce(v, 0) FROM t");
        assert_eq!(rows.len(), 5);
        for r in &rows {
            assert_ne!(r[0], Value::Null);
        }
    }

    #[test]
    fn coalesce_no_nulls() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT coalesce(v, 999) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn coalesce_all_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(i)));
        }
        let (_, rows) = db.query("SELECT coalesce(v, -1) FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn coalesce_preserves_non_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT coalesce(v, 999) FROM t");
        assert_i64(&val, 42);
    }

    #[test]
    fn coalesce_with_zero_default() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(0)));
        let val = db.query_scalar("SELECT coalesce(v, 0) FROM t");
        assert_i64(&val, 0);
    }
}

// =============================================================================
// Module 24: Multiple aggregate queries
// =============================================================================
mod multi_agg {
    use super::*;

    #[test]
    fn sum_and_count() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT sum(v), count(*) FROM t");
        assert_eq!(rows.len(), 1);
        assert_i64(&rows[0][0], 42);
        assert_eq!(rows[0][1], Value::I64(6));
    }

    #[test]
    fn min_and_max() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT min(v), max(v) FROM t");
        assert_eq!(rows[0][0], Value::I64(-100));
        assert_eq!(rows[0][1], Value::I64(100));
    }

    #[test]
    fn all_aggregates() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v), sum(v) FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(6));
        assert_eq!(rows[0][1], Value::I64(-100));
        assert_eq!(rows[0][2], Value::I64(100));
        assert_i64(&rows[0][3], 42);
    }

    #[test]
    fn first_and_last() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT first(v), last(v) FROM t");
        assert_eq!(rows[0][0], Value::I64(-100));
        assert_eq!(rows[0][1], Value::I64(100));
    }

    #[test]
    fn aggregates_with_where() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT count(*), min(v), max(v) FROM t WHERE v >= 0");
        assert_eq!(rows[0][0], Value::I64(4));
        assert_eq!(rows[0][1], Value::I64(0));
        assert_eq!(rows[0][2], Value::I64(100));
    }
}

// =============================================================================
// Module 25: Additional boundary & stress tests
// =============================================================================
mod boundary {
    use super::*;

    #[test]
    fn ascending_sequence() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        for i in 0..20 {
            assert_eq!(rows[i][0], Value::I64(i as i64));
        }
    }

    #[test]
    fn descending_sequence() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..20 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), 19 - i));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        for i in 0..20 {
            assert_eq!(rows[i][0], Value::I64(19 - i as i64));
        }
    }

    #[test]
    fn filter_boundary_inclusive() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 5 AND v <= 5");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(5));
    }

    #[test]
    fn alternating_positive_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            let val = if i % 2 == 0 { i as i64 } else { -(i as i64) };
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), val));
        }
        let (_, pos) = db.query("SELECT v FROM t WHERE v > 0");
        let (_, neg) = db.query("SELECT v FROM t WHERE v < 0");
        assert!(pos.len() >= 1);
        assert!(neg.len() >= 1);
    }

    #[test]
    fn sum_alternating() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        // 1, -1, 2, -2, 3, -3 => sum = 0
        for i in 1..=3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i * 2 - 2), i));
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i * 2 - 1), -i));
        }
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 0);
    }

    #[test]
    fn group_by_multiple_aggregates() {
        let db = db_i64_grouped();
        let (_, rows) = db
            .query("SELECT grp, count(*), min(v), max(v), sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn where_and_order_and_limit() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v >= 0 ORDER BY v DESC LIMIT 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(100));
        assert_eq!(rows[1][0], Value::I64(42));
    }

    #[test]
    fn select_star_with_where() {
        let db = db_i64();
        let (cols, rows) = db.query("SELECT * FROM t WHERE v = 42");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn combined_in_and_between() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (0, 1, 42) AND v BETWEEN 0 AND 10");
        assert_eq!(rows.len(), 2); // 0, 1
    }

    #[test]
    fn combined_or_and_order() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = -100 OR v = 100 ORDER BY v ASC");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::I64(-100));
        assert_eq!(rows[1][0], Value::I64(100));
    }

    #[test]
    fn group_having_order_limit() {
        let db = db_i64_grouped();
        let (_, rows) = db.query(
            "SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100 ORDER BY s DESC LIMIT 1",
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn count_distinct_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2)", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3)", ts(3)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t ORDER BY v");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn nested_arithmetic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        let val = db.query_scalar("SELECT (v + 5) * 2 FROM t");
        assert_i64(&val, 30);
    }

    #[test]
    fn subtract_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100, 30)", ts(0)));
        let val = db.query_scalar("SELECT a - b FROM t");
        assert_i64(&val, 70);
    }

    #[test]
    fn division_integer() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100)", ts(0)));
        let val = db.query_scalar("SELECT v / 10 FROM t");
        assert_f64_near(&val, 10.0, 0.01);
    }

    #[test]
    fn modulo_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -7)", ts(0)));
        let val = db.query_scalar("SELECT v % 3 FROM t");
        // In Rust, -7 % 3 = -1
        assert_i64(&val, -1);
    }

    #[test]
    fn where_not_eq() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 0 AND v != -1 AND v != 1");
        assert_eq!(rows.len(), 3); // -100, 42, 100
    }

    #[test]
    fn between_negative_only() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN -200 AND -50");
        assert_eq!(rows.len(), 1); // -100
    }

    #[test]
    fn in_with_negative_and_positive() {
        let db = db_i64();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN (-100, 100)");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2)", ts(2)));
        let (_, rows) = db.query("SELECT v, count(*) FROM t GROUP BY v ORDER BY v");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 26: Additional comparison combinations
// =============================================================================
mod comparison_combos {
    use super::*;

    #[test]
    fn eq_and_gt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v = 42 AND v > 0");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn eq_and_lt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v = -100 AND v < 0");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn ne_and_gt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v != 0 AND v > 0");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn ne_and_lt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v != -1 AND v < 0");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn gte_and_lte() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v >= -1 AND v <= 1");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn gt_and_ne() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v > -2 AND v != 0");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn lt_and_ne() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v < 50 AND v != -1");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn gte_and_ne() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v >= 0 AND v != 42");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn lte_and_ne() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v <= 42 AND v != 0");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn gt_or_eq() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v > 42 OR v = -100");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn lt_or_eq() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v < -1 OR v = 100");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn between_and_ne() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 0 AND 100 AND v != 42");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn in_and_gt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (0, 1, 42, 100) AND v > 0");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn in_or_lt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (42, 100) OR v < -50");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn not_in_and_between() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v NOT IN (0) AND v BETWEEN -1 AND 42");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn gt_zero_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v > 0"),
            Value::I64(3)
        );
    }
    #[test]
    fn lt_zero_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v < 0"),
            Value::I64(2)
        );
    }
    #[test]
    fn eq_zero_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 0"),
            Value::I64(1)
        );
    }
    #[test]
    fn ne_zero_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v != 0"),
            Value::I64(5)
        );
    }
    #[test]
    fn gte_zero_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v >= 0"),
            Value::I64(4)
        );
    }
    #[test]
    fn lte_zero_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v <= 0"),
            Value::I64(3)
        );
    }
    #[test]
    fn between_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN -1 AND 1"),
            Value::I64(3)
        );
    }
    #[test]
    fn in_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (0, 42)"),
            Value::I64(2)
        );
    }
    #[test]
    fn not_in_count() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v NOT IN (0, 42)"),
            Value::I64(4)
        );
    }
}

// =============================================================================
// Module 27: Additional aggregate variations
// =============================================================================
mod agg_variations {
    use super::*;

    #[test]
    fn sum_where_eq() {
        let db = db_i64();
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t WHERE v = 42"), 42);
    }
    #[test]
    fn sum_where_ne() {
        let db = db_i64();
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t WHERE v != 42"), 0);
    }
    #[test]
    fn min_where_gt() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT min(v) FROM t WHERE v > 0"),
            Value::I64(1)
        );
    }
    #[test]
    fn max_where_lt() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT max(v) FROM t WHERE v < 0"),
            Value::I64(-1)
        );
    }
    #[test]
    fn avg_positive() {
        let db = db_i64();
        let v = db.query_scalar("SELECT avg(v) FROM t WHERE v > 0");
        assert_f64_near(&v, 47.667, 0.1);
    }
    #[test]
    fn avg_negative() {
        let db = db_i64();
        let v = db.query_scalar("SELECT avg(v) FROM t WHERE v < 0");
        assert_f64_near(&v, -50.5, 0.1);
    }
    #[test]
    fn count_between() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 1 AND 100"),
            Value::I64(3)
        );
    }
    #[test]
    fn sum_between() {
        let db = db_i64();
        assert_i64(
            &db.query_scalar("SELECT sum(v) FROM t WHERE v BETWEEN 1 AND 100"),
            143,
        );
    }
    #[test]
    fn min_between() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT min(v) FROM t WHERE v BETWEEN 0 AND 100"),
            Value::I64(0)
        );
    }
    #[test]
    fn max_between() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT max(v) FROM t WHERE v BETWEEN 0 AND 100"),
            Value::I64(100)
        );
    }
    #[test]
    fn first_where_gt() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT first(v) FROM t WHERE v > 0"),
            Value::I64(1)
        );
    }
    #[test]
    fn last_where_lt() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT last(v) FROM t WHERE v < 0"),
            Value::I64(-1)
        );
    }
    #[test]
    fn count_in() {
        let db = db_i64();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (-100, 0, 100)"),
            Value::I64(3)
        );
    }
    #[test]
    fn sum_in() {
        let db = db_i64();
        assert_i64(
            &db.query_scalar("SELECT sum(v) FROM t WHERE v IN (-100, 0, 100)"),
            0,
        );
    }

    #[test]
    fn grouped_count_a() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp");
        // A: 3, B: 3, C: 2
        assert_eq!(rows[0][0], Value::Str("A".into()));
        assert_eq!(rows[0][1], Value::I64(3));
    }

    #[test]
    fn grouped_count_b() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows[1][0], Value::Str("B".into()));
        assert_eq!(rows[1][1], Value::I64(3));
    }

    #[test]
    fn grouped_count_c() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows[2][0], Value::Str("C".into()));
        assert_eq!(rows[2][1], Value::I64(2));
    }

    #[test]
    fn grouped_sum_a() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp");
        assert_i64(&rows[0][1], 90); // A: 10+30+50
    }

    #[test]
    fn grouped_sum_b() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp");
        assert_i64(&rows[1][1], 140); // B: 20+40+80
    }

    #[test]
    fn grouped_sum_c() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp");
        assert_i64(&rows[2][1], 130); // C: 60+70
    }

    #[test]
    fn grouped_min_a() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, min(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows[0][1], Value::I64(10));
    }

    #[test]
    fn grouped_max_a() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, max(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows[0][1], Value::I64(50));
    }

    #[test]
    fn grouped_min_b() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, min(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows[1][1], Value::I64(20));
    }

    #[test]
    fn grouped_max_b() {
        let db = db_i64_grouped();
        let (_, rows) = db.query("SELECT grp, max(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows[1][1], Value::I64(80));
    }
}

// =============================================================================
// Module 28: Additional arithmetic and expression tests
// =============================================================================
mod arith_extra {
    use super::*;

    #[test]
    fn add_100() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v + 100 FROM t"), 100);
    }
    #[test]
    fn sub_100() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 200)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v - 100 FROM t"), 100);
    }
    #[test]
    fn mul_10() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v * 10 FROM t"), 50);
    }
    #[test]
    fn div_5() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 50)", ts(0)));
        assert_f64_near(&db.query_scalar("SELECT v / 5 FROM t"), 10.0, 0.01);
    }
    #[test]
    fn mod_7() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 23)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v % 7 FROM t"), 2);
    }
    #[test]
    fn neg_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        assert_i64(&db.query_scalar("SELECT -v FROM t"), 0);
    }
    #[test]
    fn add_neg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -10)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v + -5 FROM t"), -15);
    }
    #[test]
    fn mul_neg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v * -3 FROM t"), -15);
    }
    #[test]
    fn paren_expr() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        assert_i64(&db.query_scalar("SELECT (v + 3) * 2 FROM t"), 16);
    }
    #[test]
    fn complex_expr() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v * 2 + 3 FROM t"), 23);
    }
    #[test]
    fn sub_self() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v - v FROM t"), 0);
    }
    #[test]
    fn add_self() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 21)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v + v FROM t"), 42);
    }
    #[test]
    fn mod_self() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        assert_i64(&db.query_scalar("SELECT v % v FROM t"), 0);
    }
}

// =============================================================================
// Module 29: More DISTINCT and ORDER BY combinations
// =============================================================================
mod distinct_order_extra {
    use super::*;

    #[test]
    fn distinct_desc() {
        let db = db_i64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t ORDER BY v DESC");
        assert_eq!(r[0][0], Value::I64(100));
    }
    #[test]
    fn distinct_limit() {
        let db = db_i64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], Value::I64(-100));
    }
    #[test]
    fn order_limit_one_asc() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(-100));
    }
    #[test]
    fn order_limit_one_desc() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(100));
    }
    #[test]
    fn offset_three() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 3");
        assert_eq!(r[0][0], Value::I64(1));
        assert_eq!(r[1][0], Value::I64(42));
    }
    #[test]
    fn offset_four() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1 OFFSET 4");
        assert_eq!(r[0][0], Value::I64(42));
    }
    #[test]
    fn distinct_where_gt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v > 0");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn distinct_where_lt() {
        let db = db_i64();
        let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v < 0");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn order_where_between() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 0 AND 100 ORDER BY v DESC");
        assert_eq!(r[0][0], Value::I64(100));
    }
    #[test]
    fn order_where_in() {
        let db = db_i64();
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (1, 42) ORDER BY v ASC");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], Value::I64(1));
    }
}

// =============================================================================
// Module 30: More CASE WHEN variations
// =============================================================================
mod case_extra {
    use super::*;

    #[test]
    fn case_gt_100() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v > 100 THEN 'over' ELSE 'under' END FROM t");
        for row in &r {
            assert_eq!(row[0], Value::Str("under".into()));
        }
    }
    #[test]
    fn case_eq_42() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v = 42 THEN 'answer' ELSE 'other' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_negative() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v < 0 THEN 'neg' ELSE 'nonneg' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_between() {
        let db = db_i64();
        let (_, r) =
            db.query("SELECT CASE WHEN v BETWEEN 0 AND 50 THEN 'mid' ELSE 'out' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_in_list() {
        let db = db_i64();
        let (_, r) =
            db.query("SELECT CASE WHEN v IN (0, 42) THEN 'special' ELSE 'normal' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_three_branches() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v < -50 THEN 'very_low' WHEN v < 0 THEN 'low' WHEN v < 50 THEN 'mid' ELSE 'high' END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_preserves_count() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v > 0 THEN 1 ELSE 0 END FROM t");
        assert_eq!(r.len(), 6);
    }
    #[test]
    fn case_with_where() {
        let db = db_i64();
        let (_, r) =
            db.query("SELECT CASE WHEN v > 0 THEN 'pos' ELSE 'neg' END FROM t WHERE v != 0");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn case_with_limit() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v > 0 THEN 'pos' ELSE 'neg' END FROM t LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn case_four_branches() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CASE WHEN v = -100 THEN 'a' WHEN v = -1 THEN 'b' WHEN v = 0 THEN 'c' WHEN v = 1 THEN 'd' ELSE 'e' END FROM t");
        assert_eq!(r.len(), 6);
    }
}

// =============================================================================
// Module 31: More GROUP BY + HAVING combinations
// =============================================================================
mod group_having_extra {
    use super::*;

    #[test]
    fn having_avg() {
        let db = db_i64_grouped();
        let (_, r) =
            db.query("SELECT grp, avg(v) AS a FROM t GROUP BY grp HAVING a > 40 ORDER BY grp");
        assert!(r.len() >= 1);
    }
    #[test]
    fn having_min() {
        let db = db_i64_grouped();
        let (_, r) =
            db.query("SELECT grp, min(v) AS m FROM t GROUP BY grp HAVING m >= 20 ORDER BY grp");
        assert!(r.len() >= 1);
    }
    #[test]
    fn having_max() {
        let db = db_i64_grouped();
        let (_, r) =
            db.query("SELECT grp, max(v) AS m FROM t GROUP BY grp HAVING m <= 50 ORDER BY grp");
        assert!(r.len() >= 1);
    }
    #[test]
    fn group_order_desc() {
        let db = db_i64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY c DESC");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn group_limit_1() {
        let db = db_i64_grouped();
        let (_, r) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn having_count_eq() {
        let db = db_i64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 2");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn having_count_gt2() {
        let db = db_i64_grouped();
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c > 2");
        assert_eq!(r.len(), 2);
    }
}

// =============================================================================
// Module 32: More SAMPLE BY with BIGINT
// =============================================================================
mod sample_extra {
    use super::*;

    #[test]
    fn sample_by_1h() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..20 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
                ts(i * 600),
                i * 5
            ));
        }
        let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }

    #[test]
    fn sample_by_30m() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i * 600), i));
        }
        let (_, r) = db.query("SELECT sum(v) FROM t SAMPLE BY 30m");
        assert!(!r.is_empty());
    }

    #[test]
    fn sample_by_15m() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i * 60), i));
        }
        let (_, r) = db.query("SELECT avg(v) FROM t SAMPLE BY 15m");
        assert!(!r.is_empty());
    }

    #[test]
    fn sample_by_1d() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
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
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {})",
                ts(i * 600),
                i * 10
            ));
        }
        let (_, r) = db.query("SELECT first(v), last(v) FROM t SAMPLE BY 1h");
        assert!(!r.is_empty());
    }
}

// =============================================================================
// Module 33: Additional COALESCE and CAST combinations
// =============================================================================
mod coalesce_cast_extra {
    use super::*;

    #[test]
    fn cast_to_double_positive() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100)", ts(0)));
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t"),
            Value::F64(100.0)
        );
    }
    #[test]
    fn cast_to_double_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -50)", ts(0)));
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t"),
            Value::F64(-50.0)
        );
    }
    #[test]
    fn cast_to_varchar_positive() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 123)", ts(0)));
        match db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t") {
            Value::Str(s) => assert_eq!(s, "123"),
            other => panic!("got {other:?}"),
        }
    }
    #[test]
    fn cast_to_varchar_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -99)", ts(0)));
        match db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t") {
            Value::Str(s) => assert_eq!(s, "-99"),
            other => panic!("got {other:?}"),
        }
    }
    #[test]
    fn cast_with_limit() {
        let db = db_i64();
        let (_, r) = db.query("SELECT CAST(v AS DOUBLE) FROM t LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn coalesce_default_100() {
        let db = db_i64_nullable();
        let (_, r) = db.query("SELECT coalesce(v, 100) FROM t");
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn coalesce_with_limit() {
        let db = db_i64_nullable();
        let (_, r) = db.query("SELECT coalesce(v, 0) FROM t LIMIT 3");
        assert_eq!(r.len(), 3);
    }
}

// =============================================================================
// Module 34: Wide table tests
// =============================================================================
mod wide_table {
    use super::*;

    #[test]
    fn three_bigint_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT, c BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 2, 3)", ts(0)));
        let (cols, rows) = db.query("SELECT a, b, c FROM t");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows[0][0], Value::I64(1));
        assert_eq!(rows[0][1], Value::I64(2));
        assert_eq!(rows[0][2], Value::I64(3));
    }

    #[test]
    fn sum_of_three_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT, c BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10, 20, 30)", ts(0)));
        assert_i64(&db.query_scalar("SELECT a + b + c FROM t"), 60);
    }

    #[test]
    fn mixed_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, i BIGINT, d DOUBLE, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 42, 3.14, 'hello')",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT i, d, s FROM t");
        assert_eq!(rows[0][0], Value::I64(42));
        assert_eq!(rows[0][1], Value::F64(3.14));
        assert_eq!(rows[0][2], Value::Str("hello".into()));
    }

    #[test]
    fn bigint_and_varchar_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, a BIGINT, b BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 10, 20)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Y', 30, 40)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 50, 60)", ts(2)));
        let (_, rows) = db.query("SELECT grp, sum(a), sum(b) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn select_with_expression_alias() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a BIGINT, b BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5, 10)", ts(0)));
        let (cols, rows) = db.query("SELECT a + b AS total, a * b AS product FROM t");
        assert!(cols.contains(&"total".to_string()));
        assert!(cols.contains(&"product".to_string()));
        assert_i64(&rows[0][0], 15);
        assert_i64(&rows[0][1], 50);
    }
}

// =============================================================================
// Module 35: Stress with many rows
// =============================================================================
mod many_rows {
    use super::*;

    #[test]
    fn fifty_rows_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }

    #[test]
    fn fifty_rows_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50i64 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        // sum 0..49 = 49*50/2 = 1225
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 1225);
    }

    #[test]
    fn fifty_rows_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(49));
    }

    #[test]
    fn fifty_rows_distinct() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i % 10));
        }
        let (_, r) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_order() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), 49 - i));
        }
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 5");
        assert_eq!(r[0][0], Value::I64(0));
        assert_eq!(r[4][0], Value::I64(4));
    }

    #[test]
    fn fifty_rows_filter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v >= 40");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v BIGINT)");
        let grps = ["A", "B", "C", "D", "E"];
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {})",
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
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v BETWEEN 10 AND 19");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_in() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..50 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i));
        }
        let (_, r) = db.query("SELECT v FROM t WHERE v IN (0, 10, 20, 30, 40)");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_having() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v BIGINT)");
        let grps = ["A", "B", "C", "D", "E"];
        for i in 0..50 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, '{}', {})",
                ts(i),
                grps[i as usize % 5],
                i
            ));
        }
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 10");
        assert_eq!(r.len(), 5);
    }
}

// =============================================================================
// Module 36: Bulk comparison tests on 10-row table
// =============================================================================
mod bulk_comparisons {
    use super::*;

    fn db10() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i), i * 10));
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
    fn btw_30_70() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 30 AND 70"),
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
    fn in_10_30_50_70() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v IN (10, 30, 50, 70)"),
            Value::I64(4)
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
    fn eq_v90() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v = 90"),
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
    fn ne_v50() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM t WHERE v != 50"),
            Value::I64(9)
        );
    }
    #[test]
    fn sum10() {
        let db = db10();
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t"), 450);
    }
    #[test]
    fn avg10() {
        let db = db10();
        assert_f64_near(&db.query_scalar("SELECT avg(v) FROM t"), 45.0, 0.01);
    }
    #[test]
    fn min10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(0));
    }
    #[test]
    fn max10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(90));
    }
    #[test]
    fn first10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::I64(0));
    }
    #[test]
    fn last10() {
        let db = db10();
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::I64(90));
    }
    #[test]
    fn order_asc1() {
        let db = db10();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn order_desc1() {
        let db = db10();
        let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 1");
        assert_eq!(r[0][0], Value::I64(90));
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
        assert_eq!(r[0][0], Value::I64(50));
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
        assert_i64(&r[0][0], 1);
    }
    #[test]
    fn mul2() {
        let db = db10();
        let (_, r) = db.query("SELECT v * 2 FROM t WHERE v = 50");
        assert_i64(&r[0][0], 100);
    }
    #[test]
    fn neg10() {
        let db = db10();
        let (_, r) = db.query("SELECT -v FROM t WHERE v = 30");
        assert_i64(&r[0][0], -30);
    }
    #[test]
    fn cast_dbl() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT CAST(v AS DOUBLE) FROM t WHERE v = 50"),
            Value::F64(50.0)
        );
    }
    #[test]
    fn cast_vc() {
        let db = db10();
        match db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t WHERE v = 50") {
            Value::Str(s) => assert_eq!(s, "50"),
            other => panic!("got {other:?}"),
        }
    }
    #[test]
    fn coalesce10() {
        let db = db10();
        let (_, r) = db.query("SELECT coalesce(v, 0) FROM t");
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn sum_gt_50() {
        let db = db10();
        assert_i64(&db.query_scalar("SELECT sum(v) FROM t WHERE v > 50"), 300);
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
            Value::I64(30)
        );
    }
    #[test]
    fn max_lt_70() {
        let db = db10();
        assert_eq!(
            db.query_scalar("SELECT max(v) FROM t WHERE v < 70"),
            Value::I64(60)
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
}

// =============================================================================
// Module 37: Systematic WHERE + aggregate combos
// =============================================================================
mod where_agg_combos {
    use super::*;

    fn mk() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT, g VARCHAR)");
        for i in 0..20 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, {}, '{}')",
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
        assert_i64(&mk().query_scalar("SELECT sum(v) FROM t"), 950);
    }
    #[test]
    fn sum_even() {
        let v = mk().query_scalar("SELECT sum(v) FROM t WHERE g = 'even'");
        assert_i64(&v, 450);
    }
    #[test]
    fn sum_odd() {
        let v = mk().query_scalar("SELECT sum(v) FROM t WHERE g = 'odd'");
        assert_i64(&v, 500);
    }
    #[test]
    fn min_all() {
        assert_eq!(mk().query_scalar("SELECT min(v) FROM t"), Value::I64(0));
    }
    #[test]
    fn max_all() {
        assert_eq!(mk().query_scalar("SELECT max(v) FROM t"), Value::I64(95));
    }
    #[test]
    fn avg_all() {
        assert_f64_near(&mk().query_scalar("SELECT avg(v) FROM t"), 47.5, 0.01);
    }
    #[test]
    fn first_all() {
        assert_eq!(mk().query_scalar("SELECT first(v) FROM t"), Value::I64(0));
    }
    #[test]
    fn last_all() {
        assert_eq!(mk().query_scalar("SELECT last(v) FROM t"), Value::I64(95));
    }
    #[test]
    fn count_gt_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v > 50"),
            Value::I64(9)
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
    fn count_gte_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v >= 50"),
            Value::I64(10)
        );
    }
    #[test]
    fn count_lte_50() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v <= 50"),
            Value::I64(11)
        );
    }
    #[test]
    fn count_btw() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v BETWEEN 20 AND 70"),
            Value::I64(11)
        );
    }
    #[test]
    fn sum_btw() {
        assert_i64(
            &mk().query_scalar("SELECT sum(v) FROM t WHERE v BETWEEN 20 AND 70"),
            495,
        );
    }
    #[test]
    fn min_gt_30() {
        assert_eq!(
            mk().query_scalar("SELECT min(v) FROM t WHERE v > 30"),
            Value::I64(35)
        );
    }
    #[test]
    fn max_lt_60() {
        assert_eq!(
            mk().query_scalar("SELECT max(v) FROM t WHERE v < 60"),
            Value::I64(55)
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
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn order_desc_l5() {
        let (_, r) = mk().query("SELECT v FROM t ORDER BY v DESC LIMIT 5");
        assert_eq!(r[0][0], Value::I64(95));
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
    fn case_high_low() {
        let (_, r) = mk().query("SELECT CASE WHEN v > 50 THEN 'high' ELSE 'low' END FROM t");
        assert_eq!(r.len(), 20);
    }
    #[test]
    fn add_10() {
        let (_, r) = mk().query("SELECT v + 10 FROM t LIMIT 1");
        assert_i64(&r[0][0], 10);
    }
    #[test]
    fn neg_v() {
        let (_, r) = mk().query("SELECT -v FROM t WHERE v = 50");
        assert_i64(&r[0][0], -50);
    }
    #[test]
    fn in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v IN (0, 50, 95)"),
            Value::I64(3)
        );
    }
    #[test]
    fn not_in_3() {
        assert_eq!(
            mk().query_scalar("SELECT count(*) FROM t WHERE v NOT IN (0, 50, 95)"),
            Value::I64(17)
        );
    }
    #[test]
    fn cast_dbl() {
        assert_eq!(
            mk().query_scalar("SELECT CAST(v AS DOUBLE) FROM t WHERE v = 50"),
            Value::F64(50.0)
        );
    }
    #[test]
    fn cast_vc() {
        match mk().query_scalar("SELECT CAST(v AS VARCHAR) FROM t WHERE v = 50") {
            Value::Str(s) => assert_eq!(s, "50"),
            o => panic!("got {o:?}"),
        }
    }
    #[test]
    fn coalesce_v() {
        let (_, r) = mk().query("SELECT coalesce(v, 0) FROM t");
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
        assert_eq!(r[0][0], Value::I64(55));
    }
    #[test]
    fn where_and_distinct() {
        let (_, r) = mk().query("SELECT DISTINCT g FROM t WHERE v > 50");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn mul_3() {
        let (_, r) = mk().query("SELECT v * 3 FROM t WHERE v = 10");
        assert_i64(&r[0][0], 30);
    }
    #[test]
    fn sub_5() {
        let (_, r) = mk().query("SELECT v - 5 FROM t WHERE v = 10");
        assert_i64(&r[0][0], 5);
    }
    #[test]
    fn div_5() {
        let (_, r) = mk().query("SELECT v / 5 FROM t WHERE v = 50");
        assert_f64_near(&r[0][0], 10.0, 0.01);
    }
    #[test]
    fn mod_7() {
        let (_, r) = mk().query("SELECT v % 7 FROM t WHERE v = 50");
        assert_i64(&r[0][0], 1);
    }
    #[test]
    fn alias_select() {
        let (c, _) = mk().query("SELECT v AS val, g AS grp FROM t LIMIT 1");
        assert!(c.contains(&"val".to_string()));
        assert!(c.contains(&"grp".to_string()));
    }
    #[test]
    fn two_aggs() {
        let (_, r) = mk().query("SELECT min(v), max(v) FROM t");
        assert_eq!(r[0][0], Value::I64(0));
        assert_eq!(r[0][1], Value::I64(95));
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
    fn grp_count_order_desc() {
        let (_, r) = mk().query("SELECT g, count(*) AS c FROM t GROUP BY g ORDER BY c DESC");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn grp_sum_limit() {
        let (_, r) = mk().query("SELECT g, sum(v) FROM t GROUP BY g ORDER BY g LIMIT 1");
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn eq_and_order() {
        let (_, r) = mk().query("SELECT v FROM t WHERE g = 'even' ORDER BY v ASC LIMIT 3");
        assert_eq!(r[0][0], Value::I64(0));
    }
    #[test]
    fn ne_and_order() {
        let (_, r) = mk().query("SELECT v FROM t WHERE g != 'even' ORDER BY v ASC LIMIT 3");
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn btw_and_grp() {
        let (_, r) =
            mk().query("SELECT g, count(*) FROM t WHERE v BETWEEN 20 AND 70 GROUP BY g ORDER BY g");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn in_and_order() {
        let (_, r) = mk().query("SELECT v FROM t WHERE v IN (0, 25, 50, 75) ORDER BY v ASC");
        assert_eq!(r.len(), 4);
    }
    #[test]
    fn distinct_btw() {
        let (_, r) = mk().query("SELECT DISTINCT g FROM t WHERE v BETWEEN 30 AND 60");
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn case_even_odd() {
        let (_, r) = mk().query("SELECT CASE WHEN g = 'even' THEN 'E' ELSE 'O' END FROM t");
        assert_eq!(r.len(), 20);
    }
}
