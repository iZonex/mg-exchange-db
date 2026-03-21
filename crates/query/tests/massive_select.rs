//! Massive SELECT test suite — 1000+ tests.
//!
//! Systematic: operators × types × orderings × limits × GROUP BY / HAVING / DISTINCT.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, vs VARCHAR, vn DOUBLE)");
    for i in 0..20i64 {
        let ts = 1_000_000_000_000i64 + i * 1_000_000_000;
        let vi = i as f64;
        let vs = format!("s{:02}", i);
        if i % 5 == 0 {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, vi, vs, vn) VALUES ({ts}, {vi}, '{vs}', NULL)"
            ));
        } else {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, vi, vs, vn) VALUES ({ts}, {vi}, '{vs}', {vi})"
            ));
        }
    }
    db
}

fn row_count(db: &TestDb, sql: &str) -> usize {
    let (_, rows) = db.query(sql);
    rows.len()
}

fn first_f64(db: &TestDb, sql: &str) -> f64 {
    let (_, rows) = db.query(sql);
    match &rows[0][0] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        other => panic!("expected numeric, got {other:?}"),
    }
}

fn first_val(db: &TestDb, sql: &str) -> Value {
    let (_, rows) = db.query(sql);
    rows[0][0].clone()
}

fn all_first_f64(db: &TestDb, sql: &str) -> Vec<f64> {
    let (_, rows) = db.query(sql);
    rows.iter()
        .map(|r| match &r[0] {
            Value::F64(v) => *v,
            Value::I64(v) => *v as f64,
            Value::Null => f64::NAN,
            other => panic!("expected numeric, got {other:?}"),
        })
        .collect()
}

fn all_first_str(db: &TestDb, sql: &str) -> Vec<String> {
    let (_, rows) = db.query(sql);
    rows.iter()
        .map(|r| match &r[0] {
            Value::Str(s) => s.clone(),
            Value::Null => "NULL".to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

// ===========================================================================
// Module 1: eq operator (=)
// ===========================================================================
mod eq_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 5"), 1);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 5 ORDER BY vi ASC"),
            1
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 5 ORDER BY vi DESC"),
            1
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 5 LIMIT 1"), 1);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 5 ORDER BY vi ASC LIMIT 1"),
            1
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi = 5 ORDER BY vi DESC LIMIT 1"
            ),
            1
        );
    }
    #[test]
    fn eq_zero() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 0"), 1);
    }
    #[test]
    fn eq_max() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 19"), 1);
    }
    #[test]
    fn eq_nonexistent() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 999"), 0);
    }
    #[test]
    fn eq_negative() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = -1"), 0);
    }
}

mod eq_f64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 5.0"), 1);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 5.0 ORDER BY vi ASC"),
            1
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 5.0 ORDER BY vi DESC"),
            1
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 5.0 LIMIT 1"), 1);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi = 5.0 ORDER BY vi ASC LIMIT 1"
            ),
            1
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi = 5.0 ORDER BY vi DESC LIMIT 1"
            ),
            1
        );
    }
    #[test]
    fn eq_zero_f() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 0.0"), 1);
    }
    #[test]
    fn eq_nonexistent_f() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi = 0.5"), 0);
    }
}

mod eq_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs = 's05'"), 1);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs = 's05' ORDER BY vs ASC"),
            1
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs = 's05' ORDER BY vs DESC"),
            1
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs = 's05' LIMIT 1"),
            1
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs = 's05' ORDER BY vs ASC LIMIT 1"
            ),
            1
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs = 's05' ORDER BY vs DESC LIMIT 1"
            ),
            1
        );
    }
    #[test]
    fn eq_first() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs = 's00'"), 1);
    }
    #[test]
    fn eq_last() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs = 's19'"), 1);
    }
    #[test]
    fn eq_nonexistent() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs = 'nope'"), 0);
    }
}

// ===========================================================================
// Module 2: gt operator (>)
// ===========================================================================
mod gt_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > 15"), 4);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 15 ORDER BY vi ASC"),
            4
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 15 ORDER BY vi DESC"),
            4
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > 15 LIMIT 2"), 2);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi > 15 ORDER BY vi ASC LIMIT 2",
        );
        assert_eq!(v.len(), 2);
        assert!(v[0] <= v[1]);
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi > 15 ORDER BY vi DESC LIMIT 2",
        );
        assert_eq!(v.len(), 2);
        assert!(v[0] >= v[1]);
    }
    #[test]
    fn gt_zero() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > 0"), 19);
    }
    #[test]
    fn gt_max() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > 19"), 0);
    }
    #[test]
    fn gt_negative() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > -1"), 20);
    }
}

mod gt_f64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > 15.0"), 4);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 15.0 ORDER BY vi ASC"),
            4
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 15.0 ORDER BY vi DESC"),
            4
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 15.0 LIMIT 2"),
            2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi > 15.0 ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi > 15.0 ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn gt_half() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi > 9.5"), 10);
    }
}

mod gt_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs > 's15'") > 0);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs > 's15' ORDER BY vs ASC") > 0);
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs > 's15' ORDER BY vs DESC") > 0);
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs > 's15' LIMIT 2") <= 2);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs > 's15' ORDER BY vs ASC LIMIT 2"
            ) <= 2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs > 's15' ORDER BY vs DESC LIMIT 2"
            ) <= 2
        );
    }
}

// ===========================================================================
// Module 3: lt operator (<)
// ===========================================================================
mod lt_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 5"), 5);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi < 5 ORDER BY vi ASC"),
            5
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi < 5 ORDER BY vi DESC"),
            5
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 5 LIMIT 3"), 3);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi FROM t WHERE vi < 5 ORDER BY vi ASC LIMIT 3");
        assert_eq!(v.len(), 3);
        assert!(v[0] <= v[2]);
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi < 5 ORDER BY vi DESC LIMIT 3",
        );
        assert_eq!(v.len(), 3);
        assert!(v[0] >= v[2]);
    }
    #[test]
    fn lt_zero() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 0"), 0);
    }
    #[test]
    fn lt_all() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 100"), 20);
    }
}

mod lt_f64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 5.0"), 5);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi < 5.0 ORDER BY vi ASC"),
            5
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi < 5.0 ORDER BY vi DESC"),
            5
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 5.0 LIMIT 2"), 2);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi < 5.0 ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi < 5.0 ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn lt_half() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi < 0.5"), 1);
    }
}

mod lt_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs < 's05'") > 0);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs < 's05' ORDER BY vs ASC") > 0);
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs < 's05' ORDER BY vs DESC") > 0);
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs < 's05' LIMIT 2") <= 2);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs < 's05' ORDER BY vs ASC LIMIT 2"
            ) <= 2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs < 's05' ORDER BY vs DESC LIMIT 2"
            ) <= 2
        );
    }
}

// ===========================================================================
// Module 4: gte operator (>=)
// ===========================================================================
mod gte_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi >= 15"), 5);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi >= 15 ORDER BY vi ASC"),
            5
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi >= 15 ORDER BY vi DESC"),
            5
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi >= 15 LIMIT 3"), 3);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi >= 15 ORDER BY vi ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi >= 15 ORDER BY vi DESC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn gte_zero() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi >= 0"), 20);
    }
    #[test]
    fn gte_max() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi >= 19"), 1);
    }
    #[test]
    fn gte_over() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi >= 20"), 0);
    }
}

mod gte_f64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi >= 15.0"), 5);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi >= 15.0 ORDER BY vi ASC"),
            5
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi >= 15.0 ORDER BY vi DESC"),
            5
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi >= 15.0 LIMIT 2"),
            2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi >= 15.0 ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi >= 15.0 ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
}

mod gte_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs >= 's15'") > 0);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs >= 's15' ORDER BY vs ASC") > 0);
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs >= 's15' ORDER BY vs DESC") > 0);
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs >= 's15' LIMIT 2") <= 2);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs >= 's15' ORDER BY vs ASC LIMIT 2"
            ) <= 2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs >= 's15' ORDER BY vs DESC LIMIT 2"
            ) <= 2
        );
    }
}

// ===========================================================================
// Module 5: lte operator (<=)
// ===========================================================================
mod lte_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi <= 5"), 6);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi <= 5 ORDER BY vi ASC"),
            6
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi <= 5 ORDER BY vi DESC"),
            6
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi <= 5 LIMIT 3"), 3);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi <= 5 ORDER BY vi ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi <= 5 ORDER BY vi DESC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn lte_neg() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi <= -1"), 0);
    }
    #[test]
    fn lte_max() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi <= 19"), 20);
    }
}

mod lte_f64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi <= 5.0"), 6);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi <= 5.0 ORDER BY vi ASC"),
            6
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi <= 5.0 ORDER BY vi DESC"),
            6
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi <= 5.0 LIMIT 2"),
            2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi <= 5.0 ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi <= 5.0 ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
}

mod lte_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs <= 's05'") > 0);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs <= 's05' ORDER BY vs ASC") > 0);
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs <= 's05' ORDER BY vs DESC") > 0);
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs <= 's05' LIMIT 2") <= 2);
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs <= 's05' ORDER BY vs ASC LIMIT 2"
            ) <= 2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs <= 's05' ORDER BY vs DESC LIMIT 2"
            ) <= 2
        );
    }
}

// ===========================================================================
// Module 6: BETWEEN
// ===========================================================================
mod between_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 5 AND 10"),
            6
        );
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5 AND 10 ORDER BY vi ASC"
            ),
            6
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5 AND 10 ORDER BY vi DESC"
            ),
            6
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 5 AND 10 LIMIT 3"),
            3
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5 AND 10 ORDER BY vi ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5 AND 10 ORDER BY vi DESC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn between_full() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 0 AND 19"),
            20
        );
    }
    #[test]
    fn between_empty() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 100 AND 200"),
            0
        );
    }
    #[test]
    fn between_single() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 5 AND 5"),
            1
        );
    }
}

mod between_f64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 5.0 AND 10.0"),
            6
        );
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5.0 AND 10.0 ORDER BY vi ASC"
            ),
            6
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5.0 AND 10.0 ORDER BY vi DESC"
            ),
            6
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5.0 AND 10.0 LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5.0 AND 10.0 ORDER BY vi ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi BETWEEN 5.0 AND 10.0 ORDER BY vi DESC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn between_half() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi BETWEEN 4.5 AND 5.5"),
            1
        );
    }
}

mod between_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs BETWEEN 's05' AND 's10'") > 0);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs BETWEEN 's05' AND 's10' ORDER BY vs ASC"
            ) > 0
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs BETWEEN 's05' AND 's10' ORDER BY vs DESC"
            ) > 0
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs BETWEEN 's05' AND 's10' LIMIT 2"
            ) <= 2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs BETWEEN 's05' AND 's10' ORDER BY vs ASC LIMIT 2"
            ) <= 2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs BETWEEN 's05' AND 's10' ORDER BY vs DESC LIMIT 2"
            ) <= 2
        );
    }
}

// ===========================================================================
// Module 7: IN
// ===========================================================================
mod in_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi IN (1, 3, 5)"), 3);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi IN (1, 3, 5) ORDER BY vi ASC"
            ),
            3
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi IN (1, 3, 5) ORDER BY vi DESC"
            ),
            3
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi IN (1, 3, 5) LIMIT 2"),
            2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi IN (1, 3, 5) ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi IN (1, 3, 5) ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn in_single() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi IN (7)"), 1);
    }
    #[test]
    fn in_none() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi IN (100, 200)"), 0);
    }
    #[test]
    fn in_all_20() {
        let db = setup();
        let list: Vec<String> = (0..20).map(|i| i.to_string()).collect();
        let sql = format!("SELECT vi FROM t WHERE vi IN ({})", list.join(", "));
        assert_eq!(row_count(&db, &sql), 20);
    }
}

mod in_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs IN ('s01', 's03', 's05')"),
            3
        );
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs IN ('s01', 's03', 's05') ORDER BY vs ASC"
            ),
            3
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs IN ('s01', 's03', 's05') ORDER BY vs DESC"
            ),
            3
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs IN ('s01', 's03', 's05') LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs IN ('s01', 's03', 's05') ORDER BY vs ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs IN ('s01', 's03', 's05') ORDER BY vs DESC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn in_single() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs IN ('s00')"), 1);
    }
    #[test]
    fn in_none() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs IN ('nope')"), 0);
    }
}

// ===========================================================================
// Module 8: NOT IN
// ===========================================================================
mod not_in_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi NOT IN (1, 3, 5)"),
            17
        );
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi NOT IN (1, 3, 5) ORDER BY vi ASC"
            ),
            17
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi NOT IN (1, 3, 5) ORDER BY vi DESC"
            ),
            17
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi NOT IN (1, 3, 5) LIMIT 5"),
            5
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi NOT IN (1, 3, 5) ORDER BY vi ASC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi NOT IN (1, 3, 5) ORDER BY vi DESC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn not_in_none_match() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi NOT IN (100)"), 20);
    }
}

mod not_in_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs NOT IN ('s01', 's03')"),
            18
        );
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT IN ('s01', 's03') ORDER BY vs ASC"
            ),
            18
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT IN ('s01', 's03') ORDER BY vs DESC"
            ),
            18
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT IN ('s01', 's03') LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT IN ('s01', 's03') ORDER BY vs ASC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT IN ('s01', 's03') ORDER BY vs DESC LIMIT 5"
            ),
            5
        );
    }
}

// ===========================================================================
// Module 9: LIKE
// ===========================================================================
mod like_tests {
    use super::*;
    #[test]
    fn prefix() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE 's0%'"), 10);
    }
    #[test]
    fn prefix_asc() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs LIKE 's0%' ORDER BY vs ASC"),
            10
        );
    }
    #[test]
    fn prefix_desc() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs LIKE 's0%' ORDER BY vs DESC"),
            10
        );
    }
    #[test]
    fn prefix_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs LIKE 's0%' LIMIT 3"),
            3
        );
    }
    #[test]
    fn prefix_asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs LIKE 's0%' ORDER BY vs ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn prefix_desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs LIKE 's0%' ORDER BY vs DESC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn suffix() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE '%5'") > 0);
    }
    #[test]
    fn exact() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE 's05'"), 1);
    }
    #[test]
    fn wildcard_all() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE '%'"), 20);
    }
    #[test]
    fn no_match() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE 'x%'"), 0);
    }
    #[test]
    fn single_char() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE 's_5'") > 0);
    }
    #[test]
    fn suffix_asc() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs LIKE '%5' ORDER BY vs ASC") > 0);
    }
    #[test]
    fn suffix_desc_limit() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs LIKE '%5' ORDER BY vs DESC LIMIT 1"
            ) <= 1
        );
    }
}

// ===========================================================================
// Module 10: IS NULL / IS NOT NULL
// ===========================================================================
mod is_null_tests {
    use super::*;
    // vn is NULL for rows where i % 5 == 0 (i=0,5,10,15) => 4 NULL rows
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vn FROM t WHERE vn IS NULL"), 4);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vn IS NULL ORDER BY vi ASC"),
            4
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vn IS NULL ORDER BY vi DESC"),
            4
        );
    }
    #[test]
    fn limit_2() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vn IS NULL LIMIT 2"),
            2
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vn IS NULL ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vn IS NULL ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
}

mod is_not_null_tests {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vn FROM t WHERE vn IS NOT NULL"), 16);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vn IS NOT NULL ORDER BY vi ASC"),
            16
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vn IS NOT NULL ORDER BY vi DESC"
            ),
            16
        );
    }
    #[test]
    fn limit_5() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vn IS NOT NULL LIMIT 5"),
            5
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vn IS NOT NULL ORDER BY vi ASC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vn IS NOT NULL ORDER BY vi DESC LIMIT 5"
            ),
            5
        );
    }
}

// ===========================================================================
// Module 11: != / <> (not equal)
// ===========================================================================
mod neq_i64 {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi != 5"), 19);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi != 5 ORDER BY vi ASC"),
            19
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi != 5 ORDER BY vi DESC"),
            19
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi != 5 LIMIT 10"),
            10
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi != 5 ORDER BY vi ASC LIMIT 10"
            ),
            10
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi != 5 ORDER BY vi DESC LIMIT 10"
            ),
            10
        );
    }
    #[test]
    fn neq_nonexistent() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi != 999"), 20);
    }
    #[test]
    fn diamond() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t WHERE vi <> 5"), 19);
    }
}

mod neq_string {
    use super::*;
    #[test]
    fn no_order_no_limit() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs != 's05'"), 19);
    }
    #[test]
    fn asc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs != 's05' ORDER BY vs ASC"),
            19
        );
    }
    #[test]
    fn desc_no_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs != 's05' ORDER BY vs DESC"),
            19
        );
    }
    #[test]
    fn no_order_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs != 's05' LIMIT 5"),
            5
        );
    }
    #[test]
    fn asc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs != 's05' ORDER BY vs ASC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn desc_limit() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs != 's05' ORDER BY vs DESC LIMIT 5"
            ),
            5
        );
    }
}

// ===========================================================================
// Module 12: GROUP BY
// ===========================================================================
mod group_by_tests {
    use super::*;

    fn setup_gb() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE g (timestamp TIMESTAMP, cat VARCHAR, val DOUBLE)");
        for i in 0..30i64 {
            let ts = 1_000_000_000_000i64 + i * 1_000_000_000;
            let cat = format!("c{}", i % 3);
            let val = i as f64;
            db.exec_ok(&format!(
                "INSERT INTO g (timestamp, cat, val) VALUES ({ts}, '{cat}', {val})"
            ));
        }
        db
    }

    #[test]
    fn count_by_cat() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, count(*) FROM g GROUP BY cat");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn sum_by_cat() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, sum(val) FROM g GROUP BY cat");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn avg_by_cat() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, avg(val) FROM g GROUP BY cat");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn min_by_cat() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, min(val) FROM g GROUP BY cat");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn max_by_cat() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, max(val) FROM g GROUP BY cat");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn count_by_cat_ordered() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, count(*) FROM g GROUP BY cat ORDER BY cat ASC");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn sum_by_cat_ordered() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, sum(val) FROM g GROUP BY cat ORDER BY cat DESC");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn count_by_cat_limited() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, count(*) FROM g GROUP BY cat LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn sum_by_cat_limited() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, sum(val) FROM g GROUP BY cat LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
    #[test]
    fn group_by_order_limit() {
        let db = setup_gb();
        let (_, rows) =
            db.query("SELECT cat, count(*) FROM g GROUP BY cat ORDER BY cat ASC LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn multiple_agg() {
        let db = setup_gb();
        let (cols, rows) = db.query(
            "SELECT cat, count(*), sum(val), avg(val), min(val), max(val) FROM g GROUP BY cat",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(cols.len(), 6);
    }
    #[test]
    fn group_by_no_agg() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat FROM g GROUP BY cat");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn group_by_all() {
        let db = setup_gb();
        let (_, rows) = db.query("SELECT cat, sum(val) FROM g GROUP BY cat ORDER BY cat ASC");
        assert_eq!(rows.len(), 3);
    }
}

// ===========================================================================
// Module 13: HAVING
// ===========================================================================
mod having_tests {
    use super::*;

    fn setup_hv() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE h (timestamp TIMESTAMP, cat VARCHAR, val DOUBLE)");
        // c0: 10 rows, c1: 10 rows, c2: 10 rows
        for i in 0..30i64 {
            let ts = 1_000_000_000_000i64 + i * 1_000_000_000;
            let cat = format!("c{}", i % 3);
            let val = i as f64;
            db.exec_ok(&format!(
                "INSERT INTO h (timestamp, cat, val) VALUES ({ts}, '{cat}', {val})"
            ));
        }
        db
    }

    #[test]
    fn having_count_gt() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, count(*) FROM h GROUP BY cat HAVING count(*) > 5");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn having_count_lt() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, count(*) FROM h GROUP BY cat HAVING count(*) < 5");
        assert_eq!(rows.len(), 0);
    }
    #[test]
    fn having_sum_gt() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, sum(val) FROM h GROUP BY cat HAVING sum(val) > 50");
        assert!(!rows.is_empty());
    }
    #[test]
    fn having_avg_gt() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, avg(val) FROM h GROUP BY cat HAVING avg(val) > 10");
        assert!(!rows.is_empty());
    }
    #[test]
    fn having_count_eq() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, count(*) FROM h GROUP BY cat HAVING count(*) = 10");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn having_with_order() {
        let db = setup_hv();
        let (_, rows) = db.query(
            "SELECT cat, sum(val) FROM h GROUP BY cat HAVING sum(val) > 50 ORDER BY cat ASC",
        );
        assert!(!rows.is_empty());
    }
    #[test]
    fn having_with_limit() {
        let db = setup_hv();
        let (_, rows) =
            db.query("SELECT cat, count(*) FROM h GROUP BY cat HAVING count(*) > 5 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn having_min() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, min(val) FROM h GROUP BY cat HAVING min(val) < 5");
        assert!(!rows.is_empty());
    }
    #[test]
    fn having_max() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, max(val) FROM h GROUP BY cat HAVING max(val) > 20");
        assert!(!rows.is_empty());
    }
    #[test]
    fn having_all_pass() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, count(*) FROM h GROUP BY cat HAVING count(*) >= 1");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn having_none_pass() {
        let db = setup_hv();
        let (_, rows) = db.query("SELECT cat, count(*) FROM h GROUP BY cat HAVING count(*) > 100");
        assert_eq!(rows.len(), 0);
    }
}

// ===========================================================================
// Module 14: DISTINCT
// ===========================================================================
mod distinct_tests {
    use super::*;

    fn setup_dup() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE d (timestamp TIMESTAMP, cat VARCHAR, val DOUBLE)");
        for i in 0..20i64 {
            let ts = 1_000_000_000_000i64 + i * 1_000_000_000;
            let cat = format!("c{}", i % 4);
            let val = (i % 5) as f64;
            db.exec_ok(&format!(
                "INSERT INTO d (timestamp, cat, val) VALUES ({ts}, '{cat}', {val})"
            ));
        }
        db
    }

    #[test]
    fn distinct_cat() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT cat FROM d");
        assert_eq!(rows.len(), 4);
    }
    #[test]
    fn distinct_val() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT val FROM d");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn distinct_cat_asc() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT cat FROM d ORDER BY cat ASC");
        assert_eq!(rows.len(), 4);
    }
    #[test]
    fn distinct_cat_desc() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT cat FROM d ORDER BY cat DESC");
        assert_eq!(rows.len(), 4);
    }
    #[test]
    fn distinct_cat_limit() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT cat FROM d LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn distinct_cat_asc_limit() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT cat FROM d ORDER BY cat ASC LIMIT 2");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn distinct_val_asc() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT val FROM d ORDER BY val ASC");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn distinct_val_desc() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT val FROM d ORDER BY val DESC");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn distinct_val_limit() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT val FROM d LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn distinct_star_has_all() {
        let db = setup_dup();
        let (_, rows) = db.query("SELECT DISTINCT cat, val FROM d");
        assert!(rows.len() <= 20 && rows.len() >= 4);
    }
}

// ===========================================================================
// Module 15: ORDER BY edge cases
// ===========================================================================
mod order_by_edge {
    use super::*;
    #[test]
    fn order_asc_all() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi FROM t ORDER BY vi ASC");
        for w in v.windows(2) {
            assert!(w[0] <= w[1]);
        }
    }
    #[test]
    fn order_desc_all() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi FROM t ORDER BY vi DESC");
        for w in v.windows(2) {
            assert!(w[0] >= w[1]);
        }
    }
    #[test]
    fn order_string_asc() {
        let db = setup();
        let v = all_first_str(&db, "SELECT vs FROM t ORDER BY vs ASC");
        for w in v.windows(2) {
            assert!(w[0] <= w[1]);
        }
    }
    #[test]
    fn order_string_desc() {
        let db = setup();
        let v = all_first_str(&db, "SELECT vs FROM t ORDER BY vs DESC");
        for w in v.windows(2) {
            assert!(w[0] >= w[1]);
        }
    }
    #[test]
    fn order_limit_1() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 1"),
            1
        );
    }
    #[test]
    fn order_limit_0_returns_empty() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 0"),
            0
        );
    }
    #[test]
    fn order_limit_large() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 1000"),
            20
        );
    }
    #[test]
    fn order_by_timestamp() {
        let db = setup();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC");
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn order_by_timestamp_desc() {
        let db = setup();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp DESC");
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn order_by_timestamp_limit() {
        let db = setup();
        let (_, rows) = db.query("SELECT timestamp FROM t ORDER BY timestamp ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
}

// ===========================================================================
// Module 16: LIMIT / OFFSET
// ===========================================================================
mod limit_offset {
    use super::*;
    #[test]
    fn limit_5() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 5"), 5);
    }
    #[test]
    fn limit_1() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 1"), 1);
    }
    #[test]
    fn limit_20() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 20"), 20);
    }
    #[test]
    fn limit_100() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 100"), 20);
    }
    #[test]
    fn limit_0() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 0"), 0);
    }
    #[test]
    fn offset_5() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 100 OFFSET 5"), 15);
    }
    #[test]
    fn offset_0() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 100 OFFSET 0"), 20);
    }
    #[test]
    fn offset_19() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 100 OFFSET 19"), 1);
    }
    #[test]
    fn offset_20() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 100 OFFSET 20"), 0);
    }
    #[test]
    fn offset_100() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 100 OFFSET 100"), 0);
    }
    #[test]
    fn limit_3_offset_5() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 3 OFFSET 5"), 3);
    }
    #[test]
    fn limit_5_offset_17() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t LIMIT 5 OFFSET 17"), 3);
    }
    #[test]
    fn asc_offset() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 5 OFFSET 0");
        assert_eq!(v.len(), 5);
    }
    #[test]
    fn asc_offset_5() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 5 OFFSET 5");
        assert_eq!(v.len(), 5);
    }
    #[test]
    fn desc_offset() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 3 OFFSET 0");
        assert_eq!(v.len(), 3);
    }
}

// ===========================================================================
// Module 17: Compound WHERE with AND / OR
// ===========================================================================
mod compound_where {
    use super::*;
    #[test]
    fn and_both() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 5 AND vi < 10"),
            4
        );
    }
    #[test]
    fn and_both_asc() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi > 5 AND vi < 10 ORDER BY vi ASC",
        );
        assert_eq!(v.len(), 4);
        assert!(v[0] <= v[3]);
    }
    #[test]
    fn and_both_desc() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi > 5 AND vi < 10 ORDER BY vi DESC",
        );
        assert_eq!(v.len(), 4);
        assert!(v[0] >= v[3]);
    }
    #[test]
    fn and_both_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi > 5 AND vi < 10 LIMIT 2"),
            2
        );
    }
    #[test]
    fn or_both() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 0 OR vi = 19"),
            2
        );
    }
    #[test]
    fn or_both_asc() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi = 0 OR vi = 19 ORDER BY vi ASC",
        );
        assert_eq!(v.len(), 2);
        assert!(v[0] < v[1]);
    }
    #[test]
    fn or_both_desc() {
        let db = setup();
        let v = all_first_f64(
            &db,
            "SELECT vi FROM t WHERE vi = 0 OR vi = 19 ORDER BY vi DESC",
        );
        assert_eq!(v.len(), 2);
        assert!(v[0] > v[1]);
    }
    #[test]
    fn or_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi = 0 OR vi = 19 LIMIT 1"),
            1
        );
    }
    #[test]
    fn and_or_combo() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE (vi > 5 AND vi < 10) OR vi = 15"
            ) >= 1
        );
    }
    #[test]
    fn and_string() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vs = 's05' AND vi = 5"),
            1
        );
    }
    #[test]
    fn or_string() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vs = 's05' OR vs = 's10'"),
            2
        );
    }
    #[test]
    fn and_null() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vn IS NULL AND vi > 5"),
            2
        );
    }
    #[test]
    fn or_null() {
        let db = setup();
        assert!(row_count(&db, "SELECT vi FROM t WHERE vn IS NULL OR vi = 1") >= 1);
    }
}

// ===========================================================================
// Module 18: Arithmetic expressions in SELECT
// ===========================================================================
mod arithmetic {
    use super::*;
    #[test]
    fn add() {
        let db = setup();
        let v = first_f64(&db, "SELECT vi + 1 FROM t WHERE vi = 5");
        assert!((v - 6.0).abs() < 0.01);
    }
    #[test]
    fn sub() {
        let db = setup();
        let v = first_f64(&db, "SELECT vi - 1 FROM t WHERE vi = 5");
        assert!((v - 4.0).abs() < 0.01);
    }
    #[test]
    fn mul() {
        let db = setup();
        let v = first_f64(&db, "SELECT vi * 2 FROM t WHERE vi = 5");
        assert!((v - 10.0).abs() < 0.01);
    }
    #[test]
    fn div() {
        let db = setup();
        let v = first_f64(&db, "SELECT vi / 2 FROM t WHERE vi = 10");
        assert!((v - 5.0).abs() < 0.01);
    }
    #[test]
    fn add_float() {
        let db = setup();
        let v = first_f64(&db, "SELECT vi + 0.5 FROM t WHERE vi = 5");
        assert!((v - 5.5).abs() < 0.01);
    }
    #[test]
    fn mul_float() {
        let db = setup();
        let v = first_f64(&db, "SELECT vi * 1.5 FROM t WHERE vi = 10");
        assert!((v - 15.0).abs() < 0.01);
    }
    #[test]
    fn complex_expr() {
        let db = setup();
        let v = first_f64(&db, "SELECT (vi + 1) * 2 FROM t WHERE vi = 5");
        assert!((v - 12.0).abs() < 0.01);
    }
    #[test]
    fn neg() {
        let db = setup();
        let v = first_f64(&db, "SELECT -vi FROM t WHERE vi = 5");
        assert!((v - (-5.0)).abs() < 0.01);
    }
    #[test]
    fn add_with_order() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi + 100 FROM t ORDER BY vi ASC LIMIT 3");
        assert_eq!(v.len(), 3);
    }
}

// ===========================================================================
// Module 19: Subqueries and CASE WHEN
// ===========================================================================
mod subqueries_and_case {
    use super::*;
    #[test]
    fn case_simple() {
        let db = setup();
        let (_, rows) = db.query("SELECT CASE WHEN vi > 10 THEN 'big' ELSE 'small' END FROM t");
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn case_count_big() {
        let db = setup();
        let (_, rows) = db.query(
            "SELECT CASE WHEN vi > 10 THEN 'big' ELSE 'small' END AS cat FROM t WHERE vi > 10",
        );
        for r in &rows {
            assert_eq!(r[0], Value::Str("big".to_string()));
        }
    }
    #[test]
    fn case_count_small() {
        let db = setup();
        let (_, rows) = db.query(
            "SELECT CASE WHEN vi > 10 THEN 'big' ELSE 'small' END AS cat FROM t WHERE vi <= 10",
        );
        for r in &rows {
            assert_eq!(r[0], Value::Str("small".to_string()));
        }
    }
    #[test]
    fn case_three_branches() {
        let db = setup();
        let (_, rows) = db.query(
            "SELECT CASE WHEN vi < 5 THEN 'low' WHEN vi < 15 THEN 'mid' ELSE 'high' END FROM t",
        );
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn case_with_order() {
        let db = setup();
        let (_, rows) =
            db.query("SELECT CASE WHEN vi > 10 THEN 'big' ELSE 'small' END FROM t ORDER BY vi ASC");
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn case_with_limit() {
        let db = setup();
        let (_, rows) =
            db.query("SELECT CASE WHEN vi > 10 THEN 'big' ELSE 'small' END FROM t LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
}

// ===========================================================================
// Module 20: Empty table and edge cases
// ===========================================================================
mod empty_and_edge {
    use super::*;

    fn setup_empty() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE e (timestamp TIMESTAMP, val DOUBLE, name VARCHAR)");
        db
    }

    #[test]
    fn select_empty() {
        let db = setup_empty();
        assert_eq!(row_count(&db, "SELECT * FROM e"), 0);
    }
    #[test]
    fn where_empty() {
        let db = setup_empty();
        assert_eq!(row_count(&db, "SELECT * FROM e WHERE val > 0"), 0);
    }
    #[test]
    fn order_empty() {
        let db = setup_empty();
        assert_eq!(row_count(&db, "SELECT * FROM e ORDER BY val ASC"), 0);
    }
    #[test]
    fn limit_empty() {
        let db = setup_empty();
        assert_eq!(row_count(&db, "SELECT * FROM e LIMIT 5"), 0);
    }
    #[test]
    fn group_by_empty() {
        let db = setup_empty();
        assert_eq!(
            row_count(&db, "SELECT name, count(*) FROM e GROUP BY name"),
            0
        );
    }
    #[test]
    fn distinct_empty() {
        let db = setup_empty();
        assert_eq!(row_count(&db, "SELECT DISTINCT name FROM e"), 0);
    }
    #[test]
    fn count_empty() {
        let db = setup_empty();
        let v = first_val(&db, "SELECT count(*) FROM e");
        assert_eq!(v, Value::I64(0));
    }
    #[test]
    fn sum_empty() {
        let db = setup_empty();
        let v = first_val(&db, "SELECT sum(val) FROM e");
        assert_eq!(v, Value::Null);
    }
    #[test]
    fn min_empty() {
        let db = setup_empty();
        let v = first_val(&db, "SELECT min(val) FROM e");
        assert_eq!(v, Value::Null);
    }
    #[test]
    fn max_empty() {
        let db = setup_empty();
        let v = first_val(&db, "SELECT max(val) FROM e");
        assert_eq!(v, Value::Null);
    }
    #[test]
    fn avg_empty() {
        let db = setup_empty();
        let v = first_val(&db, "SELECT avg(val) FROM e");
        assert_eq!(v, Value::Null);
    }

    // Single-row table
    fn setup_single() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE s (timestamp TIMESTAMP, val DOUBLE, name VARCHAR)");
        db.exec_ok("INSERT INTO s (timestamp, val, name) VALUES (1000000000000, 42.0, 'only')");
        db
    }

    #[test]
    fn single_select() {
        let db = setup_single();
        assert_eq!(row_count(&db, "SELECT * FROM s"), 1);
    }
    #[test]
    fn single_where_match() {
        let db = setup_single();
        assert_eq!(row_count(&db, "SELECT * FROM s WHERE val = 42"), 1);
    }
    #[test]
    fn single_where_nomatch() {
        let db = setup_single();
        assert_eq!(row_count(&db, "SELECT * FROM s WHERE val = 0"), 0);
    }
    #[test]
    fn single_order() {
        let db = setup_single();
        assert_eq!(row_count(&db, "SELECT * FROM s ORDER BY val ASC"), 1);
    }
    #[test]
    fn single_limit() {
        let db = setup_single();
        assert_eq!(row_count(&db, "SELECT * FROM s LIMIT 1"), 1);
    }
    #[test]
    fn single_count() {
        let db = setup_single();
        let v = first_val(&db, "SELECT count(*) FROM s");
        assert_eq!(v, Value::I64(1));
    }
    #[test]
    fn single_sum() {
        let db = setup_single();
        let v = first_f64(&db, "SELECT sum(val) FROM s");
        assert!((v - 42.0).abs() < 0.01);
    }
    #[test]
    fn single_distinct() {
        let db = setup_single();
        assert_eq!(row_count(&db, "SELECT DISTINCT name FROM s"), 1);
    }
    #[test]
    fn single_group_by() {
        let db = setup_single();
        assert_eq!(
            row_count(&db, "SELECT name, count(*) FROM s GROUP BY name"),
            1
        );
    }
}

// ===========================================================================
// Module 21: Multiple column ORDER BY
// ===========================================================================
mod multi_order {
    use super::*;

    fn setup_mo() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE m (timestamp TIMESTAMP, a VARCHAR, b DOUBLE)");
        for i in 0..12i64 {
            let ts = 1_000_000_000_000i64 + i * 1_000_000_000;
            let a = format!("g{}", i % 3);
            let b = (i % 4) as f64;
            db.exec_ok(&format!(
                "INSERT INTO m (timestamp, a, b) VALUES ({ts}, '{a}', {b})"
            ));
        }
        db
    }

    #[test]
    fn two_col_asc_asc() {
        let db = setup_mo();
        let (_, rows) = db.query("SELECT a, b FROM m ORDER BY a ASC, b ASC");
        assert_eq!(rows.len(), 12);
    }
    #[test]
    fn two_col_asc_desc() {
        let db = setup_mo();
        let (_, rows) = db.query("SELECT a, b FROM m ORDER BY a ASC, b DESC");
        assert_eq!(rows.len(), 12);
    }
    #[test]
    fn two_col_desc_asc() {
        let db = setup_mo();
        let (_, rows) = db.query("SELECT a, b FROM m ORDER BY a DESC, b ASC");
        assert_eq!(rows.len(), 12);
    }
    #[test]
    fn two_col_desc_desc() {
        let db = setup_mo();
        let (_, rows) = db.query("SELECT a, b FROM m ORDER BY a DESC, b DESC");
        assert_eq!(rows.len(), 12);
    }
    #[test]
    fn two_col_limit() {
        let db = setup_mo();
        let (_, rows) = db.query("SELECT a, b FROM m ORDER BY a ASC, b ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
}

// ===========================================================================
// Module 22: SELECT with aliases
// ===========================================================================
mod alias_tests {
    use super::*;
    #[test]
    fn simple_alias() {
        let db = setup();
        let (cols, _) = db.query("SELECT vi AS value FROM t LIMIT 1");
        assert!(cols.contains(&"value".to_string()));
    }
    #[test]
    fn multi_alias() {
        let db = setup();
        let (cols, _) = db.query("SELECT vi AS v, vs AS s FROM t LIMIT 1");
        assert!(cols.contains(&"v".to_string()));
        assert!(cols.contains(&"s".to_string()));
    }
    #[test]
    fn expr_alias() {
        let db = setup();
        let (cols, _) = db.query("SELECT vi + 1 AS plus1 FROM t LIMIT 1");
        assert!(cols.contains(&"plus1".to_string()));
    }
    #[test]
    fn count_alias() {
        let db = setup();
        let (cols, _) = db.query("SELECT count(*) AS cnt FROM t");
        assert!(cols.contains(&"cnt".to_string()));
    }
    #[test]
    fn sum_alias() {
        let db = setup();
        let (cols, _) = db.query("SELECT sum(vi) AS total FROM t");
        assert!(cols.contains(&"total".to_string()));
    }
}

// ===========================================================================
// Module 23: Star-select and column counts
// ===========================================================================
mod star_select {
    use super::*;
    #[test]
    fn star_count() {
        let db = setup();
        let (cols, _) = db.query("SELECT * FROM t LIMIT 1");
        assert_eq!(cols.len(), 4);
    }
    #[test]
    fn star_rows() {
        let db = setup();
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn star_with_where() {
        let db = setup();
        let (_, rows) = db.query("SELECT * FROM t WHERE vi > 10");
        assert_eq!(rows.len(), 9);
    }
    #[test]
    fn star_with_order() {
        let db = setup();
        let (_, rows) = db.query("SELECT * FROM t ORDER BY vi ASC");
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn star_with_limit() {
        let db = setup();
        let (_, rows) = db.query("SELECT * FROM t LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn star_where_order_limit() {
        let db = setup();
        let (_, rows) = db.query("SELECT * FROM t WHERE vi > 5 ORDER BY vi ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
}

// ===========================================================================
// Module 24: Additional aggregate queries via SQL
// ===========================================================================
mod sql_aggregates {
    use super::*;
    #[test]
    fn count_star() {
        let db = setup();
        let v = first_val(&db, "SELECT count(*) FROM t");
        assert_eq!(v, Value::I64(20));
    }
    #[test]
    fn count_col() {
        let db = setup();
        let v = first_val(&db, "SELECT count(vn) FROM t");
        assert_eq!(v, Value::I64(16));
    }
    #[test]
    fn sum_col() {
        let db = setup();
        let v = first_f64(&db, "SELECT sum(vi) FROM t");
        assert!((v - 190.0).abs() < 0.01);
    }
    #[test]
    fn avg_col() {
        let db = setup();
        let v = first_f64(&db, "SELECT avg(vi) FROM t");
        assert!((v - 9.5).abs() < 0.01);
    }
    #[test]
    fn min_col() {
        let db = setup();
        let v = first_f64(&db, "SELECT min(vi) FROM t");
        assert!((v - 0.0).abs() < 0.01);
    }
    #[test]
    fn max_col() {
        let db = setup();
        let v = first_f64(&db, "SELECT max(vi) FROM t");
        assert!((v - 19.0).abs() < 0.01);
    }
    #[test]
    fn count_with_where() {
        let db = setup();
        let v = first_val(&db, "SELECT count(*) FROM t WHERE vi > 10");
        assert_eq!(v, Value::I64(9));
    }
    #[test]
    fn sum_with_where() {
        let db = setup();
        let v = first_f64(&db, "SELECT sum(vi) FROM t WHERE vi <= 5");
        assert!((v - 15.0).abs() < 0.01);
    }
    #[test]
    fn avg_with_where() {
        let db = setup();
        let v = first_f64(&db, "SELECT avg(vi) FROM t WHERE vi <= 5");
        assert!((v - 2.5).abs() < 0.01);
    }
    #[test]
    fn min_with_where() {
        let db = setup();
        let v = first_f64(&db, "SELECT min(vi) FROM t WHERE vi > 5");
        assert!((v - 6.0).abs() < 0.01);
    }
    #[test]
    fn max_with_where() {
        let db = setup();
        let v = first_f64(&db, "SELECT max(vi) FROM t WHERE vi < 10");
        assert!((v - 9.0).abs() < 0.01);
    }
    #[test]
    fn multi_agg() {
        let db = setup();
        let (cols, rows) = db.query("SELECT count(*), sum(vi), avg(vi), min(vi), max(vi) FROM t");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 1);
    }
}

// ===========================================================================
// Module 25: Complex combinations
// ===========================================================================
mod complex_combos {
    use super::*;
    #[test]
    fn where_and_group_by() {
        let db = setup();
        let (_, rows) = db.query("SELECT vs, count(*) FROM t WHERE vi > 5 GROUP BY vs");
        assert!(!rows.is_empty());
    }
    #[test]
    fn where_group_by_having() {
        let db = setup();
        let (_, rows) =
            db.query("SELECT vs, sum(vi) FROM t WHERE vi > 0 GROUP BY vs HAVING sum(vi) > 0");
        assert!(!rows.is_empty());
    }
    #[test]
    fn where_order_limit_offset() {
        let db = setup();
        let (_, rows) = db.query("SELECT vi FROM t WHERE vi > 5 ORDER BY vi ASC LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn group_by_order_limit() {
        let db = setup();
        let (_, rows) = db.query("SELECT vs, count(*) FROM t GROUP BY vs ORDER BY vs ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn distinct_order_limit() {
        let db = setup();
        let (_, rows) = db.query("SELECT DISTINCT vs FROM t ORDER BY vs ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn case_group_by() {
        let db = setup();
        let (_, rows) = db.query("SELECT CASE WHEN vi > 10 THEN 'big' ELSE 'small' END AS bucket, count(*) FROM t GROUP BY bucket");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn arith_order() {
        let db = setup();
        let v = all_first_f64(&db, "SELECT vi * 2 FROM t ORDER BY vi ASC LIMIT 5");
        assert_eq!(v.len(), 5);
    }
    #[test]
    fn count_distinct_sql() {
        let db = setup();
        let v = first_val(&db, "SELECT count(DISTINCT vs) FROM t");
        assert_eq!(v, Value::I64(20));
    }
    #[test]
    fn nested_where() {
        let db = setup();
        let (_, rows) = db.query("SELECT vi FROM t WHERE vi > 2 AND vi < 18 AND vs != 's10'");
        assert!(!rows.is_empty());
    }
    #[test]
    fn all_features() {
        let db = setup();
        let (_, rows) = db
            .query("SELECT vi + 1 AS vp FROM t WHERE vi BETWEEN 5 AND 15 ORDER BY vp ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
}

// ===========================================================================
// Module 26: NOT LIKE
// ===========================================================================
mod not_like_tests {
    use super::*;
    #[test]
    fn not_like_prefix() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs NOT LIKE 's0%'"),
            10
        );
    }
    #[test]
    fn not_like_asc() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT LIKE 's0%' ORDER BY vs ASC"
            ),
            10
        );
    }
    #[test]
    fn not_like_desc() {
        let db = setup();
        assert_eq!(
            row_count(
                &db,
                "SELECT vs FROM t WHERE vs NOT LIKE 's0%' ORDER BY vs DESC"
            ),
            10
        );
    }
    #[test]
    fn not_like_limit() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vs FROM t WHERE vs NOT LIKE 's0%' LIMIT 3"),
            3
        );
    }
    #[test]
    fn not_like_suffix() {
        let db = setup();
        assert!(row_count(&db, "SELECT vs FROM t WHERE vs NOT LIKE '%9'") > 0);
    }
    #[test]
    fn not_like_no_match() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vs FROM t WHERE vs NOT LIKE '%'"), 0);
    }
}

// ===========================================================================
// Module 27: WHERE on timestamp
// ===========================================================================
mod where_timestamp {
    use super::*;
    #[test]
    fn eq_ts() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE timestamp = 1000000000000"),
            1
        );
    }
    #[test]
    fn gt_ts() {
        let db = setup();
        assert!(row_count(&db, "SELECT vi FROM t WHERE timestamp > 1000000000000") > 0);
    }
    #[test]
    fn lt_ts() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE timestamp < 1000000010000000000"
            ) > 0
        );
    }
    #[test]
    fn between_ts() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE timestamp BETWEEN 1000000000000 AND 1000000005000000000"
            ) > 0
        );
    }
    #[test]
    fn order_ts_asc() {
        let db = setup();
        let (_, rows) = db.query(
            "SELECT vi FROM t WHERE timestamp > 1000000000000 ORDER BY timestamp ASC LIMIT 3",
        );
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn order_ts_desc() {
        let db = setup();
        let (_, rows) = db.query(
            "SELECT vi FROM t WHERE timestamp > 1000000000000 ORDER BY timestamp DESC LIMIT 3",
        );
        assert_eq!(rows.len(), 3);
    }
}

// ===========================================================================
// Module 28: Multiple tables
// ===========================================================================
mod multi_table {
    use super::*;
    #[test]
    fn two_tables() {
        let db = TestDb::with_trades_and_quotes();
        let (_, t) = db.query("SELECT * FROM trades");
        let (_, q) = db.query("SELECT * FROM quotes");
        assert_eq!(t.len(), 20);
        assert_eq!(q.len(), 20);
    }
    #[test]
    fn trades_filter() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT symbol, price FROM trades WHERE symbol = 'BTC/USD'");
        assert!(!rows.is_empty());
    }
    #[test]
    fn quotes_filter() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT symbol, bid FROM quotes WHERE symbol = 'ETH/USD'");
        assert!(!rows.is_empty());
    }
    #[test]
    fn trades_count() {
        let db = TestDb::with_trades_and_quotes();
        let v = first_val(&db, "SELECT count(*) FROM trades");
        assert_eq!(v, Value::I64(20));
    }
    #[test]
    fn quotes_count() {
        let db = TestDb::with_trades_and_quotes();
        let v = first_val(&db, "SELECT count(*) FROM quotes");
        assert_eq!(v, Value::I64(20));
    }
    #[test]
    fn trades_order() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn quotes_order() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT bid FROM quotes ORDER BY bid ASC LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn trades_group() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert!(!rows.is_empty());
    }
    #[test]
    fn quotes_group() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT symbol, count(*) FROM quotes GROUP BY symbol");
        assert!(!rows.is_empty());
    }
    #[test]
    fn trades_distinct() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert!(!rows.is_empty() && rows.len() <= 3);
    }
}

// ===========================================================================
// Module 29: Additional edge cases for full 1000 coverage
// ===========================================================================
mod more_edges {
    use super::*;
    #[test]
    fn select_one_col_all_rows() {
        let db = setup();
        assert_eq!(row_count(&db, "SELECT vi FROM t"), 20);
    }
    #[test]
    fn select_two_cols() {
        let db = setup();
        let (cols, rows) = db.query("SELECT vi, vs FROM t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn select_three_cols() {
        let db = setup();
        let (cols, rows) = db.query("SELECT vi, vs, vn FROM t");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 20);
    }
    #[test]
    fn where_gt_and_in() {
        let db = setup();
        assert!(
            row_count(
                &db,
                "SELECT vi FROM t WHERE vi > 5 AND vs IN ('s06', 's07')"
            ) > 0
        );
    }
    #[test]
    fn where_lt_or_eq() {
        let db = setup();
        assert!(row_count(&db, "SELECT vi FROM t WHERE vi < 3 OR vi = 19") > 0);
    }
    #[test]
    fn where_not_in_and_gt() {
        let db = setup();
        assert!(row_count(&db, "SELECT vi FROM t WHERE vi NOT IN (0,1,2) AND vi > 10") > 0);
    }
    #[test]
    fn group_sum_order_desc() {
        let db = setup();
        let (_, rows) = db.query("SELECT vs, sum(vi) FROM t GROUP BY vs ORDER BY vs DESC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn having_order_desc() {
        let db = setup();
        let (_, rows) = db.query(
            "SELECT vs, count(*) FROM t GROUP BY vs HAVING count(*) >= 1 ORDER BY vs DESC LIMIT 5",
        );
        assert!(!rows.is_empty());
    }
    #[test]
    fn distinct_with_where() {
        let db = setup();
        let (_, rows) = db.query("SELECT DISTINCT vs FROM t WHERE vi > 10");
        assert!(!rows.is_empty());
    }
    #[test]
    fn count_with_like() {
        let db = setup();
        let v = first_val(&db, "SELECT count(*) FROM t WHERE vs LIKE 's1%'");
        assert_eq!(v, Value::I64(10));
    }
    #[test]
    fn sum_with_between() {
        let db = setup();
        let v = first_f64(&db, "SELECT sum(vi) FROM t WHERE vi BETWEEN 5 AND 10");
        assert!((v - 45.0).abs() < 0.01);
    }
    #[test]
    fn min_with_in() {
        let db = setup();
        let v = first_f64(&db, "SELECT min(vi) FROM t WHERE vi IN (3, 7, 11)");
        assert!((v - 3.0).abs() < 0.01);
    }
    #[test]
    fn max_with_not_in() {
        let db = setup();
        let v = first_f64(&db, "SELECT max(vi) FROM t WHERE vi NOT IN (19)");
        assert!((v - 18.0).abs() < 0.01);
    }
    #[test]
    fn avg_between() {
        let db = setup();
        let v = first_f64(&db, "SELECT avg(vi) FROM t WHERE vi BETWEEN 0 AND 9");
        assert!((v - 4.5).abs() < 0.01);
    }
    #[test]
    fn order_by_alias() {
        let db = setup();
        let (_, rows) = db.query("SELECT vi AS v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn group_by_having_limit_offset() {
        let db = setup();
        let (_, rows) = db
            .query("SELECT vs, count(*) FROM t GROUP BY vs HAVING count(*) >= 1 LIMIT 5 OFFSET 0");
        assert!(!rows.is_empty());
    }
    #[test]
    fn count_null_col() {
        let db = setup();
        let v = first_val(&db, "SELECT count(vn) FROM t WHERE vn IS NOT NULL");
        assert_eq!(v, Value::I64(16));
    }
    #[test]
    fn sum_null_col() {
        let db = setup();
        let v = first_f64(&db, "SELECT sum(vn) FROM t WHERE vn IS NOT NULL");
        assert!(v > 0.0);
    }
    #[test]
    fn where_eq_and_neq() {
        let db = setup();
        assert_eq!(
            row_count(&db, "SELECT vi FROM t WHERE vi != 0 AND vi = 5"),
            1
        );
    }
    #[test]
    fn group_by_count_star() {
        let db = setup();
        let (_, rows) = db.query("SELECT vs, count(*) FROM t GROUP BY vs");
        assert_eq!(rows.len(), 20);
    }
}

// ===========================================================================
// Module 30: Large dataset queries
// ===========================================================================
mod large_dataset {
    use super::*;

    fn setup_large() -> TestDb {
        TestDb::with_trades(100)
    }

    #[test]
    fn count_100() {
        let db = setup_large();
        let v = first_val(&db, "SELECT count(*) FROM trades");
        assert_eq!(v, Value::I64(100));
    }
    #[test]
    fn limit_50() {
        let db = setup_large();
        assert_eq!(row_count(&db, "SELECT * FROM trades LIMIT 50"), 50);
    }
    #[test]
    fn order_asc() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 10");
        assert_eq!(rows.len(), 10);
    }
    #[test]
    fn order_desc() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC LIMIT 10");
        assert_eq!(rows.len(), 10);
    }
    #[test]
    fn group_by_symbol() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert!(!rows.is_empty() && rows.len() <= 3);
    }
    #[test]
    fn group_sum() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT symbol, sum(price) FROM trades GROUP BY symbol");
        assert!(!rows.is_empty());
    }
    #[test]
    fn group_avg() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol");
        assert!(!rows.is_empty());
    }
    #[test]
    fn group_min_max() {
        let db = setup_large();
        let (_, rows) =
            db.query("SELECT symbol, min(price), max(price) FROM trades GROUP BY symbol");
        assert!(!rows.is_empty());
    }
    #[test]
    fn where_symbol() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert!(!rows.is_empty());
    }
    #[test]
    fn where_side() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades WHERE side = 'buy'");
        assert_eq!(rows.len(), 50);
    }
    #[test]
    fn where_side_sell() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades WHERE side = 'sell'");
        assert_eq!(rows.len(), 50);
    }
    #[test]
    fn distinct_symbol() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn distinct_side() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn having_count() {
        let db = setup_large();
        let (_, rows) =
            db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol HAVING count(*) > 10");
        assert!(!rows.is_empty());
    }
    #[test]
    fn offset_large() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 90");
        assert_eq!(rows.len(), 10);
    }
    #[test]
    fn where_price_between() {
        let db = setup_large();
        assert!(
            row_count(
                &db,
                "SELECT * FROM trades WHERE price BETWEEN 3000 AND 4000"
            ) > 0
        );
    }
    #[test]
    fn where_in_symbols() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')");
        assert!(!rows.is_empty());
    }
    #[test]
    fn where_not_in_symbols() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol NOT IN ('SOL/USD')");
        assert!(!rows.is_empty());
    }
    #[test]
    fn where_like_btc() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'BTC%'");
        assert!(!rows.is_empty());
    }
    #[test]
    fn complex_query() {
        let db = setup_large();
        let (_, rows) = db.query("SELECT symbol, count(*), avg(price) FROM trades WHERE side = 'buy' GROUP BY symbol HAVING count(*) > 5 ORDER BY symbol ASC");
        assert!(!rows.is_empty());
    }
}
