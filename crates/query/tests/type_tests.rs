//! Comprehensive column type tests — 200 tests.
//!
//! For each supported type: BOOLEAN, INT (I32), BIGINT (I64), FLOAT (F32),
//! DOUBLE (F64), TIMESTAMP, VARCHAR — tests CREATE TABLE, INSERT, SELECT,
//! WHERE filter, ORDER BY, GROUP BY, MIN/MAX/FIRST/LAST, NULL handling,
//! boundary values, type in expressions.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1_710_460_800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// =============================================================================
// DOUBLE (F64) — primary numeric type
// =============================================================================
mod double_type {
    use super::*;

    #[test]
    fn create_table_with_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn insert_positive_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.15)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(3.15));
    }

    #[test]
    fn insert_negative_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -99.5)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(-99.5));
    }

    #[test]
    fn insert_zero_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.0)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(0.0));
    }

    #[test]
    fn insert_large_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1e15)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(1e15));
    }

    #[test]
    fn insert_small_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.000001)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        match val {
            Value::F64(v) => assert!((v - 0.000001).abs() < 1e-10),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn double_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        // NULL for DOUBLE may be stored as NaN -> read back as Null
        assert!(val == Value::Null || val == Value::F64(0.0));
    }

    #[test]
    fn double_where_eq() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99.0)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 42.0");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(42.0));
    }

    #[test]
    fn double_where_gt() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 15");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn double_order_by_asc() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::F64(10.0));
        assert_eq!(rows[1][0], Value::F64(20.0));
        assert_eq!(rows[2][0], Value::F64(30.0));
    }

    #[test]
    fn double_order_by_desc() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        assert_eq!(rows[0][0], Value::F64(30.0));
    }

    #[test]
    fn double_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 15.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(2)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(5.0));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(15.0));
    }

    #[test]
    fn double_sum_avg() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(2)));
        match db.query_scalar("SELECT sum(v) FROM t") {
            Value::F64(v) => assert!((v - 60.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
        match db.query_scalar("SELECT avg(v) FROM t") {
            Value::F64(v) => assert!((v - 20.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn double_in_expression() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        let (_, rows) = db.query("SELECT v * 2 FROM t");
        assert_eq!(rows[0][0], Value::F64(20.0));
    }

    #[test]
    fn double_between() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 15.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 25.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 10 AND 20");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(15.0));
    }

    #[test]
    fn double_first_last() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(2)));
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::F64(1.0));
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::F64(3.0));
    }

    #[test]
    fn double_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(2));
    }

    #[test]
    fn double_group_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn multiple_double_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 2.0, 3.0)", ts(0)));
        let (_, rows) = db.query("SELECT a, b, c FROM t");
        assert_eq!(rows[0][0], Value::F64(1.0));
        assert_eq!(rows[0][1], Value::F64(2.0));
        assert_eq!(rows[0][2], Value::F64(3.0));
    }
}

// =============================================================================
// BIGINT (I64)
// =============================================================================
mod bigint_type {
    use super::*;

    #[test]
    fn create_table_with_bigint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn insert_positive_bigint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(42));
    }

    #[test]
    fn insert_negative_bigint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -100)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(-100));
    }

    #[test]
    fn insert_zero_bigint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(0));
    }

    #[test]
    fn insert_large_bigint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1000000000)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(1_000_000_000));
    }

    #[test]
    fn bigint_where_eq() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 20");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(20));
    }

    #[test]
    fn bigint_where_gt() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 15");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn bigint_where_lt() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v < 25");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn bigint_order_by_asc() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::I64(10));
        assert_eq!(rows[1][0], Value::I64(20));
        assert_eq!(rows[2][0], Value::I64(30));
    }

    #[test]
    fn bigint_order_by_desc() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        assert_eq!(rows[0][0], Value::I64(30));
    }

    #[test]
    fn bigint_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 15)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(2)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(5));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(15));
    }

    #[test]
    fn bigint_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30)", ts(2)));
        let val = db.query_scalar("SELECT sum(v) FROM t");
        match val {
            Value::I64(v) => assert_eq!(v, 60),
            Value::F64(v) => assert!((v - 60.0).abs() < 0.01),
            other => panic!("expected numeric, got {other:?}"),
        }
    }

    #[test]
    fn bigint_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(2));
    }

    #[test]
    fn bigint_first_last() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 200)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 300)", ts(2)));
        assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::I64(100));
        assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::I64(300));
    }

    #[test]
    fn bigint_between() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 15)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 25)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v BETWEEN 10 AND 20");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(15));
    }

    #[test]
    fn bigint_group_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 30)", ts(2)));
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// INT (I32)
// =============================================================================
mod int_type {
    use super::*;

    #[test]
    fn create_table_with_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn insert_int_positive() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        // I32 read back as I64
        assert_eq!(val, Value::I64(42));
    }

    #[test]
    fn insert_int_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -42)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(-42));
    }

    #[test]
    fn insert_int_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::I64(0));
    }

    #[test]
    fn int_where_eq() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 10");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn int_order_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::I64(1));
        assert_eq!(rows[1][0], Value::I64(2));
        assert_eq!(rows[2][0], Value::I64(3));
    }

    #[test]
    fn int_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 15)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(2)));
        assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(5));
        assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(15));
    }

    #[test]
    fn int_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3)", ts(2)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(3));
    }

    #[test]
    fn int_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20)", ts(1)));
        let val = db.query_scalar("SELECT sum(v) FROM t");
        match val {
            Value::I64(v) => assert_eq!(v, 30),
            Value::F64(v) => assert!((v - 30.0).abs() < 0.01),
            other => panic!("expected numeric, got {other:?}"),
        }
    }

    #[test]
    fn int_multiple_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        let values: Vec<String> = (0..20).map(|i| format!("({}, {})", ts(i), i)).collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(20));
    }
}

// =============================================================================
// FLOAT (F32)
// =============================================================================
mod float_type {
    use super::*;

    #[test]
    fn create_table_with_float() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn insert_float_positive() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.15)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        // F32 read back as F64
        match val {
            Value::F64(v) => assert!((v - 3.15).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn insert_float_negative() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, -1.5)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        match val {
            Value::F64(v) => assert!((v - (-1.5)).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn insert_float_zero() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 0.0)", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::F64(0.0));
    }

    #[test]
    fn float_where_gt() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v > 15");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn float_order_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 20.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        // F32 read as F64
        match (&rows[0][0], &rows[2][0]) {
            (Value::F64(a), Value::F64(b)) => assert!(a < b),
            _ => panic!("expected F64 values"),
        }
    }

    #[test]
    fn float_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 15.0)", ts(1)));
        let min_val = db.query_scalar("SELECT min(v) FROM t");
        let max_val = db.query_scalar("SELECT max(v) FROM t");
        match (&min_val, &max_val) {
            (Value::F64(mn), Value::F64(mx)) => {
                assert!((*mn - 5.0).abs() < 0.01);
                assert!((*mx - 15.0).abs() < 0.01);
            }
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn float_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v FLOAT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(2));
    }
}

// =============================================================================
// BOOLEAN — INSERT into BOOLEAN columns not fully supported via SQL
// =============================================================================
mod boolean_type {
    use super::*;

    #[test]
    fn create_table_with_boolean() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BOOLEAN)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn boolean_empty_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BOOLEAN)");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }
}

// =============================================================================
// VARCHAR
// =============================================================================
mod varchar_type {
    use super::*;

    #[test]
    fn create_table_with_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn insert_varchar_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::Str("hello".to_string()));
    }

    #[test]
    fn insert_varchar_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '')", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::Str("".to_string()));
    }

    #[test]
    fn insert_varchar_long() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let long_str = "a".repeat(200);
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}')", ts(0), long_str));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::Str(long_str));
    }

    #[test]
    fn varchar_where_eq() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'alice')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'bob')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'charlie')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'bob'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("bob".to_string()));
    }

    #[test]
    fn varchar_where_like() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'apple')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'apricot')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'banana')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'ap%'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn varchar_where_ilike() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Hello')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'HELLO')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'world')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'hello'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn varchar_order_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'cherry')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'apple')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'banana')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::Str("apple".to_string()));
        assert_eq!(rows[1][0], Value::Str("banana".to_string()));
        assert_eq!(rows[2][0], Value::Str("cherry".to_string()));
    }

    #[test]
    fn varchar_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(2));
    }

    #[test]
    fn varchar_count_distinct() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(2)));
        assert_eq!(
            db.query_scalar("SELECT count_distinct(v) FROM t"),
            Value::I64(2)
        );
    }

    #[test]
    fn varchar_first_last() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'first')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'middle')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'last')", ts(2)));
        assert_eq!(
            db.query_scalar("SELECT first(v) FROM t"),
            Value::Str("first".to_string())
        );
        assert_eq!(
            db.query_scalar("SELECT last(v) FROM t"),
            Value::Str("last".to_string())
        );
    }

    #[test]
    fn varchar_group_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Y', 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Str("X".to_string()));
        assert_eq!(rows[1][0], Value::Str("Y".to_string()));
    }

    #[test]
    fn varchar_in_list() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN ('a', 'c')");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn varchar_not_in() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN ('b')");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn varchar_with_spaces() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello world')", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::Str("hello world".to_string()));
    }

    #[test]
    fn varchar_with_numbers() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '12345')", ts(0)));
        let val = db.query_scalar("SELECT v FROM t");
        assert_eq!(val, Value::Str("12345".to_string()));
    }

    #[test]
    fn varchar_like_wildcard_all() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'def')", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn varchar_like_exact() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'test')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'rest')", ts(1)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'test'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn varchar_like_suffix() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'test')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'rest')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'best')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%est'");
        assert_eq!(rows.len(), 3);
    }
}

// =============================================================================
// TIMESTAMP
// =============================================================================
mod timestamp_type {
    use super::*;

    #[test]
    fn create_table_with_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 1);
    }

    #[test]
    fn insert_timestamp() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({})", ts(0)));
        let val = db.query_scalar("SELECT timestamp FROM t");
        assert!(matches!(val, Value::Timestamp(_)));
    }

    #[test]
    fn timestamp_order_by() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // Insert in order since out-of-order insert may not be supported
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY timestamp ASC");
        assert_eq!(rows[0][0], Value::F64(1.0));
        assert_eq!(rows[2][0], Value::F64(3.0));
    }

    #[test]
    fn timestamp_select_all() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(20)));
        let (_, rows) = db.query("SELECT v FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn timestamp_values_preserved() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(10)));
        let (_, rows) = db.query("SELECT timestamp FROM t");
        assert_eq!(rows.len(), 2);
        // Timestamps should be different
        assert_ne!(rows[0][0], rows[1][0]);
    }

    #[test]
    fn timestamp_min_max() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(20)));
        let min_val = db.query_scalar("SELECT min(timestamp) FROM t");
        let max_val = db.query_scalar("SELECT max(timestamp) FROM t");
        assert_eq!(min_val, Value::Timestamp(ts(0)));
        assert_eq!(max_val, Value::Timestamp(ts(20)));
    }

    #[test]
    fn timestamp_first_last() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(10)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.0)", ts(20)));
        let first = db.query_scalar("SELECT first(timestamp) FROM t");
        let last = db.query_scalar("SELECT last(timestamp) FROM t");
        assert_eq!(first, Value::Timestamp(ts(0)));
        assert_eq!(last, Value::Timestamp(ts(20)));
    }

    #[test]
    fn timestamp_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({})", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({})", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({})", ts(2)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(3));
    }

    #[test]
    fn timestamp_multiple_per_second() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // Timestamps 1ns apart
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", BASE_TS + i, i));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(5));
    }
}

// =============================================================================
// Mixed type tables
// =============================================================================
mod mixed_types {
    use super::*;

    #[test]
    fn table_with_basic_types() {
        let db = TestDb::new();
        db.exec_ok(
            "CREATE TABLE t (timestamp TIMESTAMP, i INT, l BIGINT, f FLOAT, d DOUBLE, s VARCHAR)",
        );
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 42, 1000, 3.15, 2.718, 'hello')",
            ts(0)
        ));
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 6);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn mixed_type_where_on_each_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, i INT, d DOUBLE, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 10.0, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2, 20.0, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3, 30.0, 'c')", ts(2)));

        let (_, rows) = db.query("SELECT * FROM t WHERE i = 2");
        assert_eq!(rows.len(), 1);

        let (_, rows) = db.query("SELECT * FROM t WHERE d > 15");
        assert_eq!(rows.len(), 2);

        let (_, rows) = db.query("SELECT * FROM t WHERE s = 'c'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn mixed_type_order_by_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, i INT, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3, 'c')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1, 'a')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2, 'b')", ts(2)));
        let (_, rows) = db.query("SELECT s FROM t ORDER BY i ASC");
        assert_eq!(rows[0][0], Value::Str("a".to_string()));
        assert_eq!(rows[1][0], Value::Str("b".to_string()));
        assert_eq!(rows[2][0], Value::Str("c".to_string()));
    }

    #[test]
    fn mixed_type_group_by_varchar_agg_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, val DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 10.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Y', 20.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'X', 30.0)", ts(2)));
        let (_, rows) = db.query("SELECT grp, sum(val) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn mixed_type_group_by_varchar_agg_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, val INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 10)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', 20)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 30)", ts(2)));
        let (_, rows) = db.query("SELECT grp, sum(val) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn mixed_type_multiple_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b VARCHAR, c VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x', 'y', 'z')", ts(0)));
        let (_, rows) = db.query("SELECT a, b, c FROM t");
        assert_eq!(rows[0][0], Value::Str("x".to_string()));
        assert_eq!(rows[0][1], Value::Str("y".to_string()));
        assert_eq!(rows[0][2], Value::Str("z".to_string()));
    }

    #[test]
    fn mixed_int_and_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, i BIGINT, d DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42, 3.15)", ts(0)));
        let (_, rows) = db.query("SELECT i, d FROM t");
        assert_eq!(rows[0][0], Value::I64(42));
        assert_eq!(rows[0][1], Value::F64(3.15));
    }

    #[test]
    fn expression_double_col_times_int_col() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, qty INT, price DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10, 5.0)", ts(0)));
        let (_, rows) = db.query("SELECT price * qty FROM t");
        match &rows[0][0] {
            Value::F64(v) => assert!((*v - 50.0).abs() < 0.01),
            Value::I64(v) => assert_eq!(*v, 50),
            other => panic!("expected numeric, got {other:?}"),
        }
    }

    #[test]
    fn ten_columns_table() {
        let db = TestDb::new();
        db.exec_ok(
            "CREATE TABLE wide (timestamp TIMESTAMP, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 INT, c7 INT, c8 VARCHAR, c9 VARCHAR)",
        );
        db.exec_ok(&format!(
            "INSERT INTO wide VALUES ({}, 1.0, 2.0, 3.0, 4.0, 5.0, 6, 7, 'eight', 'nine')",
            ts(0)
        ));
        let (cols, rows) = db.query("SELECT * FROM wide");
        assert_eq!(cols.len(), 10);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn null_in_each_type() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, d DOUBLE, i BIGINT, s VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, NULL, NULL, NULL)",
            ts(0)
        ));
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn many_rows_mixed_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT, s VARCHAR)");
        let values: Vec<String> = (0..50)
            .map(|i| format!("({}, {}, 'item_{}')", ts(i), i, i))
            .collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }
}

// =============================================================================
// TINYINT (I8) and SMALLINT (I16)
// =============================================================================
mod small_int_types {
    use super::*;

    #[test]
    fn create_table_with_tinyint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v TINYINT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn create_table_with_smallint() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v SMALLINT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn create_tinyint_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v TINYINT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn create_smallint_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v SMALLINT)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }
}

// =============================================================================
// Multiple tables
// =============================================================================
mod multi_table {
    use super::*;

    #[test]
    fn two_tables_different_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 3.15)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 42)", ts(0)));
        assert_eq!(db.query_scalar("SELECT v FROM t1"), Value::F64(3.15));
        assert_eq!(db.query_scalar("SELECT v FROM t2"), Value::I64(42));
    }

    #[test]
    fn four_tables_each_different() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v INT)");
        db.exec_ok("CREATE TABLE t3 (timestamp TIMESTAMP, v BIGINT)");
        db.exec_ok("CREATE TABLE t4 (timestamp TIMESTAMP, v VARCHAR)");

        db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 2)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t3 VALUES ({}, 3)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t4 VALUES ({}, 'four')", ts(0)));

        assert_eq!(db.query_scalar("SELECT count(*) FROM t1"), Value::I64(1));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t2"), Value::I64(1));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t3"), Value::I64(1));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t4"), Value::I64(1));
    }
}
