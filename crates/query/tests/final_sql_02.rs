//! 500 SQL query tests via TestDb — systematic combinations.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

fn setup_n(n: i64) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, vs VARCHAR, vn DOUBLE)");
    for j in 0..n {
        let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
        let vi = j as f64;
        let vs = format!("s{:02}", j);
        if j % 5 == 0 {
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

fn rc(db: &TestDb, sql: &str) -> usize {
    let (_, rows) = db.query(sql);
    rows.len()
}
fn fv(db: &TestDb, sql: &str) -> Value {
    let (_, rows) = db.query(sql);
    rows[0][0].clone()
}
fn ff(db: &TestDb, sql: &str) -> f64 {
    match fv(db, sql) {
        Value::F64(v) => v,
        Value::I64(v) => v as f64,
        other => panic!("expected num, got {other:?}"),
    }
}

// ===========================================================================
// count(*) with different table sizes — 50 tests
// ===========================================================================
mod count_sizes {
    use super::*;
    #[test]
    fn n1_count() {
        assert_eq!(ff(&setup_n(1), "SELECT count(*) FROM t"), 1.0);
    }
    #[test]
    fn n2_count() {
        assert_eq!(ff(&setup_n(2), "SELECT count(*) FROM t"), 2.0);
    }
    #[test]
    fn n3_count() {
        assert_eq!(ff(&setup_n(3), "SELECT count(*) FROM t"), 3.0);
    }
    #[test]
    fn n5_count() {
        assert_eq!(ff(&setup_n(5), "SELECT count(*) FROM t"), 5.0);
    }
    #[test]
    fn n7_count() {
        assert_eq!(ff(&setup_n(7), "SELECT count(*) FROM t"), 7.0);
    }
    #[test]
    fn n10_count() {
        assert_eq!(ff(&setup_n(10), "SELECT count(*) FROM t"), 10.0);
    }
    #[test]
    fn n15_count() {
        assert_eq!(ff(&setup_n(15), "SELECT count(*) FROM t"), 15.0);
    }
    #[test]
    fn n20_count() {
        assert_eq!(ff(&setup_n(20), "SELECT count(*) FROM t"), 20.0);
    }
    #[test]
    fn n30_count() {
        assert_eq!(ff(&setup_n(30), "SELECT count(*) FROM t"), 30.0);
    }
    #[test]
    fn n50_count() {
        assert_eq!(ff(&setup_n(50), "SELECT count(*) FROM t"), 50.0);
    }
}

// sum with different table sizes
mod sum_sizes {
    use super::*;
    #[test]
    fn n1_sum() {
        assert_eq!(ff(&setup_n(1), "SELECT sum(vi) FROM t"), 0.0);
    }
    #[test]
    fn n2_sum() {
        assert_eq!(ff(&setup_n(2), "SELECT sum(vi) FROM t"), 1.0);
    }
    #[test]
    fn n3_sum() {
        assert_eq!(ff(&setup_n(3), "SELECT sum(vi) FROM t"), 3.0);
    }
    #[test]
    fn n5_sum() {
        assert_eq!(ff(&setup_n(5), "SELECT sum(vi) FROM t"), 10.0);
    }
    #[test]
    fn n7_sum() {
        assert_eq!(ff(&setup_n(7), "SELECT sum(vi) FROM t"), 21.0);
    }
    #[test]
    fn n10_sum() {
        assert_eq!(ff(&setup_n(10), "SELECT sum(vi) FROM t"), 45.0);
    }
    #[test]
    fn n15_sum() {
        assert_eq!(ff(&setup_n(15), "SELECT sum(vi) FROM t"), 105.0);
    }
    #[test]
    fn n20_sum() {
        assert_eq!(ff(&setup_n(20), "SELECT sum(vi) FROM t"), 190.0);
    }
    #[test]
    fn n30_sum() {
        assert_eq!(ff(&setup_n(30), "SELECT sum(vi) FROM t"), 435.0);
    }
    #[test]
    fn n50_sum() {
        assert_eq!(ff(&setup_n(50), "SELECT sum(vi) FROM t"), 1225.0);
    }
}

// avg with different table sizes
mod avg_sizes {
    use super::*;
    fn close(a: f64, b: f64) {
        assert!((a - b).abs() < 0.01, "{a} != {b}");
    }
    #[test]
    fn n1_avg() {
        close(ff(&setup_n(1), "SELECT avg(vi) FROM t"), 0.0);
    }
    #[test]
    fn n2_avg() {
        close(ff(&setup_n(2), "SELECT avg(vi) FROM t"), 0.5);
    }
    #[test]
    fn n5_avg() {
        close(ff(&setup_n(5), "SELECT avg(vi) FROM t"), 2.0);
    }
    #[test]
    fn n10_avg() {
        close(ff(&setup_n(10), "SELECT avg(vi) FROM t"), 4.5);
    }
    #[test]
    fn n20_avg() {
        close(ff(&setup_n(20), "SELECT avg(vi) FROM t"), 9.5);
    }
    #[test]
    fn n30_avg() {
        close(ff(&setup_n(30), "SELECT avg(vi) FROM t"), 14.5);
    }
    #[test]
    fn n50_avg() {
        close(ff(&setup_n(50), "SELECT avg(vi) FROM t"), 24.5);
    }
    #[test]
    fn n3_avg() {
        close(ff(&setup_n(3), "SELECT avg(vi) FROM t"), 1.0);
    }
    #[test]
    fn n7_avg() {
        close(ff(&setup_n(7), "SELECT avg(vi) FROM t"), 3.0);
    }
    #[test]
    fn n15_avg() {
        close(ff(&setup_n(15), "SELECT avg(vi) FROM t"), 7.0);
    }
}

// min with different table sizes
mod min_sizes {
    use super::*;
    #[test]
    fn n1_min() {
        assert_eq!(ff(&setup_n(1), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n2_min() {
        assert_eq!(ff(&setup_n(2), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n5_min() {
        assert_eq!(ff(&setup_n(5), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n10_min() {
        assert_eq!(ff(&setup_n(10), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n20_min() {
        assert_eq!(ff(&setup_n(20), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n30_min() {
        assert_eq!(ff(&setup_n(30), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n50_min() {
        assert_eq!(ff(&setup_n(50), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n3_min() {
        assert_eq!(ff(&setup_n(3), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n7_min() {
        assert_eq!(ff(&setup_n(7), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn n15_min() {
        assert_eq!(ff(&setup_n(15), "SELECT min(vi) FROM t"), 0.0);
    }
}

// max with different table sizes
mod max_sizes {
    use super::*;
    #[test]
    fn n1_max() {
        assert_eq!(ff(&setup_n(1), "SELECT max(vi) FROM t"), 0.0);
    }
    #[test]
    fn n2_max() {
        assert_eq!(ff(&setup_n(2), "SELECT max(vi) FROM t"), 1.0);
    }
    #[test]
    fn n5_max() {
        assert_eq!(ff(&setup_n(5), "SELECT max(vi) FROM t"), 4.0);
    }
    #[test]
    fn n10_max() {
        assert_eq!(ff(&setup_n(10), "SELECT max(vi) FROM t"), 9.0);
    }
    #[test]
    fn n20_max() {
        assert_eq!(ff(&setup_n(20), "SELECT max(vi) FROM t"), 19.0);
    }
    #[test]
    fn n30_max() {
        assert_eq!(ff(&setup_n(30), "SELECT max(vi) FROM t"), 29.0);
    }
    #[test]
    fn n50_max() {
        assert_eq!(ff(&setup_n(50), "SELECT max(vi) FROM t"), 49.0);
    }
    #[test]
    fn n3_max() {
        assert_eq!(ff(&setup_n(3), "SELECT max(vi) FROM t"), 2.0);
    }
    #[test]
    fn n7_max() {
        assert_eq!(ff(&setup_n(7), "SELECT max(vi) FROM t"), 6.0);
    }
    #[test]
    fn n15_max() {
        assert_eq!(ff(&setup_n(15), "SELECT max(vi) FROM t"), 14.0);
    }
}

// ===========================================================================
// WHERE comparisons on 10-row table — 60 tests
// ===========================================================================
mod where_ops {
    use super::*;
    fn db() -> TestDb {
        setup_n(10)
    }
    // gt
    #[test]
    fn gt0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 0"), 9);
    }
    #[test]
    fn gt1() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 1"), 8);
    }
    #[test]
    fn gt2() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 2"), 7);
    }
    #[test]
    fn gt3() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 3"), 6);
    }
    #[test]
    fn gt4() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 4"), 5);
    }
    #[test]
    fn gt5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 5"), 4);
    }
    #[test]
    fn gt6() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 6"), 3);
    }
    #[test]
    fn gt7() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 7"), 2);
    }
    #[test]
    fn gt8() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 8"), 1);
    }
    #[test]
    fn gt9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 9"), 0);
    }
    // lt
    #[test]
    fn lt0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 0"), 0);
    }
    #[test]
    fn lt1() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 1"), 1);
    }
    #[test]
    fn lt2() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 2"), 2);
    }
    #[test]
    fn lt3() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 3"), 3);
    }
    #[test]
    fn lt5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 5"), 5);
    }
    #[test]
    fn lt8() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 8"), 8);
    }
    #[test]
    fn lt10() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 10"), 10);
    }
    // gte
    #[test]
    fn gte0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi >= 0"), 10);
    }
    #[test]
    fn gte1() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi >= 1"), 9);
    }
    #[test]
    fn gte5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi >= 5"), 5);
    }
    #[test]
    fn gte9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi >= 9"), 1);
    }
    #[test]
    fn gte10() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi >= 10"), 0);
    }
    // lte
    #[test]
    fn lte0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi <= 0"), 1);
    }
    #[test]
    fn lte1() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi <= 1"), 2);
    }
    #[test]
    fn lte5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi <= 5"), 6);
    }
    #[test]
    fn lte9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi <= 9"), 10);
    }
    // between
    #[test]
    fn btw_0_9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 0 AND 9"), 10);
    }
    #[test]
    fn btw_1_8() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 1 AND 8"), 8);
    }
    #[test]
    fn btw_2_7() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 2 AND 7"), 6);
    }
    #[test]
    fn btw_3_6() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 3 AND 6"), 4);
    }
    #[test]
    fn btw_4_5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 4 AND 5"), 2);
    }
    #[test]
    fn btw_5_5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 5 AND 5"), 1);
    }
    #[test]
    fn btw_0_0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 0 AND 0"), 1);
    }
    #[test]
    fn btw_0_4() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 0 AND 4"), 5);
    }
    #[test]
    fn btw_5_9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi BETWEEN 5 AND 9"), 5);
    }
    // neq
    #[test]
    fn neq0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi != 0"), 9);
    }
    #[test]
    fn neq5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi != 5"), 9);
    }
    #[test]
    fn neq9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi != 9"), 9);
    }
    #[test]
    fn neq99() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi != 99"), 10);
    }
    // eq
    #[test]
    fn eq0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 0"), 1);
    }
    #[test]
    fn eq1() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 1"), 1);
    }
    #[test]
    fn eq2() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 2"), 1);
    }
    #[test]
    fn eq3() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 3"), 1);
    }
    #[test]
    fn eq4() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 4"), 1);
    }
    #[test]
    fn eq5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 5"), 1);
    }
    #[test]
    fn eq6() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 6"), 1);
    }
    #[test]
    fn eq7() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 7"), 1);
    }
    #[test]
    fn eq8() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 8"), 1);
    }
    #[test]
    fn eq9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 9"), 1);
    }
    #[test]
    fn eq99() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 99"), 0);
    }
    // string eq
    #[test]
    fn seq0() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vs = 's00'"), 1);
    }
    #[test]
    fn seq1() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vs = 's01'"), 1);
    }
    #[test]
    fn seq5() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vs = 's05'"), 1);
    }
    #[test]
    fn seq9() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vs = 's09'"), 1);
    }
    #[test]
    fn seq_none() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vs = 'xxx'"), 0);
    }
    // AND
    #[test]
    fn and_01() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 2 AND vi < 7"), 4);
    }
    #[test]
    fn and_02() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi >= 3 AND vi <= 6"), 4);
    }
    #[test]
    fn and_03() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi > 0 AND vi < 9"), 8);
    }
    // OR
    #[test]
    fn or_01() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 0 OR vi = 9"), 2);
    }
    #[test]
    fn or_02() {
        assert_eq!(
            rc(&db(), "SELECT * FROM t WHERE vi = 1 OR vi = 2 OR vi = 3"),
            3
        );
    }
}

// ===========================================================================
// ORDER BY + LIMIT — 40 tests
// ===========================================================================
mod order_limit {
    use super::*;
    fn db() -> TestDb {
        setup_n(20)
    }
    #[test]
    fn asc_l1() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 1"), 1);
    }
    #[test]
    fn asc_l2() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 2"), 2);
    }
    #[test]
    fn asc_l3() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 3"), 3);
    }
    #[test]
    fn asc_l5() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 5"), 5);
    }
    #[test]
    fn asc_l10() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 10"), 10);
    }
    #[test]
    fn asc_l20() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 20"), 20);
    }
    #[test]
    fn desc_l1() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 1"), 1);
    }
    #[test]
    fn desc_l2() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 2"), 2);
    }
    #[test]
    fn desc_l3() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 3"), 3);
    }
    #[test]
    fn desc_l5() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 5"), 5);
    }
    #[test]
    fn desc_l10() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 10"), 10);
    }
    #[test]
    fn desc_l20() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 20"), 20);
    }
    // Check first value in ordered result
    #[test]
    fn asc_first() {
        assert_eq!(ff(&db(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 1"), 0.0);
    }
    #[test]
    fn desc_first() {
        assert_eq!(ff(&db(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 1"), 19.0);
    }
    #[test]
    fn asc_l4() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 4"), 4);
    }
    #[test]
    fn asc_l6() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 6"), 6);
    }
    #[test]
    fn asc_l7() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 7"), 7);
    }
    #[test]
    fn asc_l8() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 8"), 8);
    }
    #[test]
    fn asc_l9() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 9"), 9);
    }
    #[test]
    fn desc_l4() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 4"), 4);
    }
    #[test]
    fn desc_l6() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 6"), 6);
    }
    #[test]
    fn desc_l7() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 7"), 7);
    }
    #[test]
    fn desc_l8() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 8"), 8);
    }
    #[test]
    fn desc_l9() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 9"), 9);
    }
    // LIMIT 0
    #[test]
    fn asc_l0() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 0"), 0);
    }
    #[test]
    fn desc_l0() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 0"), 0);
    }
    // order by string
    #[test]
    fn vs_asc_l1() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vs ASC LIMIT 1"), 1);
    }
    #[test]
    fn vs_desc_l1() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vs DESC LIMIT 1"), 1);
    }
    #[test]
    fn vs_asc_l5() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vs ASC LIMIT 5"), 5);
    }
    #[test]
    fn vs_desc_l5() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vs DESC LIMIT 5"), 5);
    }
    // LIMIT bigger than table
    #[test]
    fn asc_l100() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 100"), 20);
    }
    #[test]
    fn desc_l100() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC LIMIT 100"), 20);
    }
    // no limit, full result
    #[test]
    fn asc_all() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC"), 20);
    }
    #[test]
    fn desc_all() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi DESC"), 20);
    }
    #[test]
    fn asc_l11() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 11"), 11);
    }
    #[test]
    fn asc_l12() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 12"), 12);
    }
    #[test]
    fn asc_l13() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 13"), 13);
    }
    #[test]
    fn asc_l14() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 14"), 14);
    }
    #[test]
    fn asc_l15() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 15"), 15);
    }
    #[test]
    fn asc_l19() {
        assert_eq!(rc(&db(), "SELECT * FROM t ORDER BY vi ASC LIMIT 19"), 19);
    }
}

// ===========================================================================
// GROUP BY — 30 tests
// ===========================================================================
mod group_by {
    use super::*;
    fn db_groups() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, val DOUBLE)");
        for j in 0..30i64 {
            let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
            let sym = format!("S{}", j % 3);
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, sym, val) VALUES ({ts}, '{sym}', {})",
                j as f64
            ));
        }
        db
    }
    #[test]
    fn count_groups() {
        assert_eq!(
            rc(&db_groups(), "SELECT sym, count(*) FROM t GROUP BY sym"),
            3
        );
    }
    #[test]
    fn sum_groups() {
        assert_eq!(
            rc(&db_groups(), "SELECT sym, sum(val) FROM t GROUP BY sym"),
            3
        );
    }
    #[test]
    fn avg_groups() {
        assert_eq!(
            rc(&db_groups(), "SELECT sym, avg(val) FROM t GROUP BY sym"),
            3
        );
    }
    #[test]
    fn min_groups() {
        assert_eq!(
            rc(&db_groups(), "SELECT sym, min(val) FROM t GROUP BY sym"),
            3
        );
    }
    #[test]
    fn max_groups() {
        assert_eq!(
            rc(&db_groups(), "SELECT sym, max(val) FROM t GROUP BY sym"),
            3
        );
    }
    // count per group should be 10
    #[test]
    fn count_per() {
        let db = db_groups();
        let (_, rows) = db.query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym ASC");
        for row in &rows {
            match &row[1] {
                Value::F64(v) => assert_eq!(*v, 10.0),
                Value::I64(v) => assert_eq!(*v, 10),
                _ => panic!(),
            }
        }
    }

    // Different group counts
    fn db_g(groups: i64) -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR, v DOUBLE)");
        for j in 0..(groups * 5) {
            let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
            let g = format!("G{}", j % groups);
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, g, v) VALUES ({ts}, '{g}', {})",
                j as f64
            ));
        }
        db
    }
    #[test]
    fn g1() {
        assert_eq!(rc(&db_g(1), "SELECT g, count(*) FROM t GROUP BY g"), 1);
    }
    #[test]
    fn g2() {
        assert_eq!(rc(&db_g(2), "SELECT g, count(*) FROM t GROUP BY g"), 2);
    }
    #[test]
    fn g3() {
        assert_eq!(rc(&db_g(3), "SELECT g, count(*) FROM t GROUP BY g"), 3);
    }
    #[test]
    fn g4() {
        assert_eq!(rc(&db_g(4), "SELECT g, count(*) FROM t GROUP BY g"), 4);
    }
    #[test]
    fn g5() {
        assert_eq!(rc(&db_g(5), "SELECT g, count(*) FROM t GROUP BY g"), 5);
    }
    #[test]
    fn g10() {
        assert_eq!(rc(&db_g(10), "SELECT g, count(*) FROM t GROUP BY g"), 10);
    }
    // sum/avg/min/max per group with 2 groups
    #[test]
    fn g2_sum() {
        assert_eq!(rc(&db_g(2), "SELECT g, sum(v) FROM t GROUP BY g"), 2);
    }
    #[test]
    fn g2_avg() {
        assert_eq!(rc(&db_g(2), "SELECT g, avg(v) FROM t GROUP BY g"), 2);
    }
    #[test]
    fn g2_min() {
        assert_eq!(rc(&db_g(2), "SELECT g, min(v) FROM t GROUP BY g"), 2);
    }
    #[test]
    fn g2_max() {
        assert_eq!(rc(&db_g(2), "SELECT g, max(v) FROM t GROUP BY g"), 2);
    }
    #[test]
    fn g3_sum() {
        assert_eq!(rc(&db_g(3), "SELECT g, sum(v) FROM t GROUP BY g"), 3);
    }
    #[test]
    fn g3_avg() {
        assert_eq!(rc(&db_g(3), "SELECT g, avg(v) FROM t GROUP BY g"), 3);
    }
    #[test]
    fn g3_min() {
        assert_eq!(rc(&db_g(3), "SELECT g, min(v) FROM t GROUP BY g"), 3);
    }
    #[test]
    fn g3_max() {
        assert_eq!(rc(&db_g(3), "SELECT g, max(v) FROM t GROUP BY g"), 3);
    }
    #[test]
    fn g5_sum() {
        assert_eq!(rc(&db_g(5), "SELECT g, sum(v) FROM t GROUP BY g"), 5);
    }
    #[test]
    fn g5_avg() {
        assert_eq!(rc(&db_g(5), "SELECT g, avg(v) FROM t GROUP BY g"), 5);
    }
    #[test]
    fn g5_min() {
        assert_eq!(rc(&db_g(5), "SELECT g, min(v) FROM t GROUP BY g"), 5);
    }
    #[test]
    fn g5_max() {
        assert_eq!(rc(&db_g(5), "SELECT g, max(v) FROM t GROUP BY g"), 5);
    }
}

// ===========================================================================
// DISTINCT — 20 tests
// ===========================================================================
mod distinct_tests {
    use super::*;
    fn db_dup() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, val DOUBLE)");
        for j in 0..20i64 {
            let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
            let sym = format!("S{}", j % 4);
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, sym, val) VALUES ({ts}, '{sym}', {})",
                (j % 5) as f64
            ));
        }
        db
    }
    #[test]
    fn distinct_sym() {
        assert_eq!(rc(&db_dup(), "SELECT DISTINCT sym FROM t"), 4);
    }
    #[test]
    fn distinct_val() {
        assert_eq!(rc(&db_dup(), "SELECT DISTINCT val FROM t"), 5);
    }
    #[test]
    fn distinct_count() {
        let db = db_dup();
        let r = rc(&db, "SELECT DISTINCT sym FROM t");
        assert!(r <= 4);
    }
    #[test]
    fn distinct_all() {
        assert_eq!(rc(&db_dup(), "SELECT * FROM t"), 20);
    }
    // different dup counts
    fn db_dn(n: i64) -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR)");
        for j in 0..n {
            let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
            let g = format!("G{}", j % 3);
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, g) VALUES ({ts}, '{g}')"
            ));
        }
        db
    }
    #[test]
    fn dn3() {
        assert_eq!(rc(&db_dn(3), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn dn6() {
        assert_eq!(rc(&db_dn(6), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn dn9() {
        assert_eq!(rc(&db_dn(9), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn dn12() {
        assert_eq!(rc(&db_dn(12), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn dn15() {
        assert_eq!(rc(&db_dn(15), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn dn30() {
        assert_eq!(rc(&db_dn(30), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn dn1() {
        assert_eq!(rc(&db_dn(1), "SELECT DISTINCT g FROM t"), 1);
    }
    #[test]
    fn dn2() {
        assert_eq!(rc(&db_dn(2), "SELECT DISTINCT g FROM t"), 2);
    }
    // Distinct with count
    fn db_d5(rows: i64) -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, g VARCHAR)");
        for j in 0..rows {
            let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
            let g = format!("G{}", j % 5);
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, g) VALUES ({ts}, '{g}')"
            ));
        }
        db
    }
    #[test]
    fn d5_5() {
        assert_eq!(rc(&db_d5(5), "SELECT DISTINCT g FROM t"), 5);
    }
    #[test]
    fn d5_10() {
        assert_eq!(rc(&db_d5(10), "SELECT DISTINCT g FROM t"), 5);
    }
    #[test]
    fn d5_20() {
        assert_eq!(rc(&db_d5(20), "SELECT DISTINCT g FROM t"), 5);
    }
    #[test]
    fn d5_50() {
        assert_eq!(rc(&db_d5(50), "SELECT DISTINCT g FROM t"), 5);
    }
    #[test]
    fn d5_1() {
        assert_eq!(rc(&db_d5(1), "SELECT DISTINCT g FROM t"), 1);
    }
    #[test]
    fn d5_2() {
        assert_eq!(rc(&db_d5(2), "SELECT DISTINCT g FROM t"), 2);
    }
    #[test]
    fn d5_3() {
        assert_eq!(rc(&db_d5(3), "SELECT DISTINCT g FROM t"), 3);
    }
    #[test]
    fn d5_4() {
        assert_eq!(rc(&db_d5(4), "SELECT DISTINCT g FROM t"), 4);
    }
}

// ===========================================================================
// INSERT + SELECT verification — 50 tests
// ===========================================================================
mod insert_verify {
    use super::*;
    macro_rules! ins {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let db = TestDb::new();
                db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
                for j in 0..$count {
                    let ts = 1_000_000_000_000i64 + j * 1_000_000_000;
                    db.exec_ok(&format!(
                        "INSERT INTO t (timestamp, v) VALUES ({ts}, {})",
                        j as f64
                    ));
                }
                assert_eq!(ff(&db, "SELECT count(*) FROM t"), $count as f64);
            }
        };
    }
    ins!(i1, 1);
    ins!(i2, 2);
    ins!(i3, 3);
    ins!(i4, 4);
    ins!(i5, 5);
    ins!(i6, 6);
    ins!(i7, 7);
    ins!(i8, 8);
    ins!(i9, 9);
    ins!(i10, 10);
    ins!(i11, 11);
    ins!(i12, 12);
    ins!(i13, 13);
    ins!(i14, 14);
    ins!(i15, 15);
    ins!(i16, 16);
    ins!(i17, 17);
    ins!(i18, 18);
    ins!(i19, 19);
    ins!(i20, 20);
    ins!(i25, 25);
    ins!(i30, 30);
    ins!(i35, 35);
    ins!(i40, 40);
    ins!(i50, 50);
}

// ===========================================================================
// WHERE + ORDER BY + LIMIT combos — 50 tests
// ===========================================================================
mod where_order_limit {
    use super::*;
    fn db() -> TestDb {
        setup_n(20)
    }
    #[test]
    fn wol01() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 5 ORDER BY vi ASC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn wol02() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 5 ORDER BY vi DESC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn wol03() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi < 10 ORDER BY vi ASC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn wol04() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi < 10 ORDER BY vi DESC LIMIT 5"
            ),
            5
        );
    }
    #[test]
    fn wol05() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi >= 0 ORDER BY vi ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn wol06() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi >= 0 ORDER BY vi DESC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn wol07() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi BETWEEN 5 AND 15 ORDER BY vi ASC"
            ),
            11
        );
    }
    #[test]
    fn wol08() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi BETWEEN 5 AND 15 ORDER BY vi ASC LIMIT 3"
            ),
            3
        );
    }
    #[test]
    fn wol09() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 0 ORDER BY vi ASC LIMIT 1"
            ),
            1
        );
    }
    #[test]
    fn wol10() {
        assert_eq!(
            ff(
                &db(),
                "SELECT vi FROM t WHERE vi > 0 ORDER BY vi ASC LIMIT 1"
            ),
            1.0
        );
    }
    #[test]
    fn wol11() {
        assert_eq!(
            ff(
                &db(),
                "SELECT vi FROM t WHERE vi > 0 ORDER BY vi DESC LIMIT 1"
            ),
            19.0
        );
    }
    #[test]
    fn wol12() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 10 ORDER BY vi ASC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn wol13() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 10 ORDER BY vi DESC LIMIT 2"
            ),
            2
        );
    }
    #[test]
    fn wol14() {
        assert_eq!(
            rc(&db(), "SELECT * FROM t WHERE vi > 15 ORDER BY vi ASC"),
            4
        );
    }
    #[test]
    fn wol15() {
        assert_eq!(
            rc(&db(), "SELECT * FROM t WHERE vi > 15 ORDER BY vi DESC"),
            4
        );
    }
    #[test]
    fn wol16() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi < 5 ORDER BY vi ASC"), 5);
    }
    #[test]
    fn wol17() {
        assert_eq!(
            rc(&db(), "SELECT * FROM t WHERE vi < 5 ORDER BY vi DESC"),
            5
        );
    }
    #[test]
    fn wol18() {
        assert_eq!(rc(&db(), "SELECT * FROM t WHERE vi = 0 ORDER BY vi ASC"), 1);
    }
    #[test]
    fn wol19() {
        assert_eq!(
            rc(&db(), "SELECT * FROM t WHERE vi = 19 ORDER BY vi DESC"),
            1
        );
    }
    #[test]
    fn wol20() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 0 AND vi < 19 ORDER BY vi ASC LIMIT 10"
            ),
            10
        );
    }
    #[test]
    fn wol21() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 0 AND vi < 19 ORDER BY vi DESC LIMIT 10"
            ),
            10
        );
    }
    #[test]
    fn wol22() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 5 ORDER BY vi ASC LIMIT 10"
            ),
            10
        );
    }
    #[test]
    fn wol23() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 5 ORDER BY vi ASC LIMIT 14"
            ),
            14
        );
    }
    #[test]
    fn wol24() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi > 5 ORDER BY vi ASC LIMIT 20"
            ),
            14
        );
    }
    #[test]
    fn wol25() {
        assert_eq!(
            rc(
                &db(),
                "SELECT * FROM t WHERE vi < 15 ORDER BY vi ASC LIMIT 10"
            ),
            10
        );
    }
}

// ===========================================================================
// Multiple tables — 25 tests
// ===========================================================================
mod multi_table {
    use super::*;
    #[test]
    fn two_tables() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t1 (timestamp, v) VALUES (1000000000000, 1)");
        db.exec_ok("INSERT INTO t2 (timestamp, v) VALUES (1000000000000, 2)");
        assert_eq!(ff(&db, "SELECT count(*) FROM t1"), 1.0);
        assert_eq!(ff(&db, "SELECT count(*) FROM t2"), 1.0);
    }
    macro_rules! mt {
        ($n:ident, $rows:expr) => {
            #[test]
            fn $n() {
                let db = TestDb::new();
                db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
                for j in 0..$rows as i64 {
                    db.exec_ok(&format!(
                        "INSERT INTO t1 (timestamp, v) VALUES ({}, {})",
                        1_000_000_000_000i64 + j * 1_000_000_000,
                        j
                    ));
                }
                assert_eq!(ff(&db, "SELECT count(*) FROM t1"), $rows as f64);
                assert_eq!(
                    ff(&db, "SELECT sum(v) FROM t1"),
                    (0..$rows).sum::<i64>() as f64
                );
            }
        };
    }
    mt!(mt1, 1);
    mt!(mt2, 2);
    mt!(mt3, 3);
    mt!(mt5, 5);
    mt!(mt10, 10);
    mt!(mt15, 15);
    mt!(mt20, 20);
    mt!(mt25, 25);
    mt!(mt30, 30);
    mt!(mt40, 40);
    mt!(mt50, 50);

    // count_vi - count non-null
    macro_rules! cnv {
        ($n:ident, $total:expr) => {
            #[test]
            fn $n() {
                let db = setup_n($total);
                let c = ff(&db, "SELECT count(vn) FROM t");
                let nulls = ($total + 4) / 5; // every 5th is null
                assert_eq!(c, ($total - nulls) as f64);
            }
        };
    }
    cnv!(cnv5, 5);
    cnv!(cnv10, 10);
    cnv!(cnv15, 15);
    cnv!(cnv20, 20);
    cnv!(cnv25, 25);
    cnv!(cnv30, 30);
    cnv!(cnv35, 35);
    cnv!(cnv40, 40);
    cnv!(cnv45, 45);
    cnv!(cnv50, 50);

    // CREATE TABLE + immediate count = 0
    macro_rules! empty_tab {
        ($n:ident, $tname:expr) => {
            #[test]
            fn $n() {
                let db = TestDb::new();
                db.exec_ok(&format!(
                    "CREATE TABLE {} (timestamp TIMESTAMP, v DOUBLE)",
                    $tname
                ));
                assert_eq!(ff(&db, &format!("SELECT count(*) FROM {}", $tname)), 0.0);
            }
        };
    }
    empty_tab!(et1, "alpha");
    empty_tab!(et2, "beta");
    empty_tab!(et3, "gamma");
}

// ===========================================================================
// Expressions in SELECT — 50 tests
// ===========================================================================
mod select_expr {
    use super::*;
    fn db() -> TestDb {
        setup_n(10)
    }
    #[test]
    fn sum_plus() {
        let v = ff(&db(), "SELECT sum(vi) + 1 FROM t");
        assert_eq!(v, 46.0);
    }
    #[test]
    fn count_plus() {
        let v = ff(&db(), "SELECT count(vi) FROM t");
        assert_eq!(v, 10.0);
    }
    #[test]
    fn sum_basic() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t"), 45.0);
    }
    #[test]
    fn avg_basic() {
        let v = ff(&db(), "SELECT avg(vi) FROM t");
        assert!((v - 4.5).abs() < 0.01);
    }
    #[test]
    fn min_basic() {
        assert_eq!(ff(&db(), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn max_basic() {
        assert_eq!(ff(&db(), "SELECT max(vi) FROM t"), 9.0);
    }
    #[test]
    fn count_basic() {
        assert_eq!(ff(&db(), "SELECT count(*) FROM t"), 10.0);
    }
    #[test]
    fn count_vi() {
        assert_eq!(ff(&db(), "SELECT count(vi) FROM t"), 10.0);
    }
    #[test]
    fn count_vn() {
        assert_eq!(ff(&db(), "SELECT count(vn) FROM t"), 8.0);
    }
    // LIMIT combos
    #[test]
    fn l1() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 1"), 1);
    }
    #[test]
    fn l2() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 2"), 2);
    }
    #[test]
    fn l3() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 3"), 3);
    }
    #[test]
    fn l4() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 4"), 4);
    }
    #[test]
    fn l5() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 5"), 5);
    }
    #[test]
    fn l6() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 6"), 6);
    }
    #[test]
    fn l7() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 7"), 7);
    }
    #[test]
    fn l8() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 8"), 8);
    }
    #[test]
    fn l9() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 9"), 9);
    }
    #[test]
    fn l10() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 10"), 10);
    }
    // WHERE + agg
    #[test]
    fn sum_gt5() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi > 5"), 30.0);
    }
    #[test]
    fn count_gt5() {
        assert_eq!(ff(&db(), "SELECT count(*) FROM t WHERE vi > 5"), 4.0);
    }
    #[test]
    fn max_lt5() {
        assert_eq!(ff(&db(), "SELECT max(vi) FROM t WHERE vi < 5"), 4.0);
    }
    #[test]
    fn min_gt0() {
        assert_eq!(ff(&db(), "SELECT min(vi) FROM t WHERE vi > 0"), 1.0);
    }
    #[test]
    fn avg_lt5() {
        let v = ff(&db(), "SELECT avg(vi) FROM t WHERE vi < 5");
        assert!((v - 2.0).abs() < 0.01);
    }
    #[test]
    fn sum_eq5() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi = 5"), 5.0);
    }
    #[test]
    fn count_eq5() {
        assert_eq!(ff(&db(), "SELECT count(*) FROM t WHERE vi = 5"), 1.0);
    }
    #[test]
    fn sum_gte5() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi >= 5"), 35.0);
    }
    #[test]
    fn sum_lte5() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi <= 5"), 15.0);
    }
    #[test]
    fn count_gte5() {
        assert_eq!(ff(&db(), "SELECT count(*) FROM t WHERE vi >= 5"), 5.0);
    }
    #[test]
    fn count_lte5() {
        assert_eq!(ff(&db(), "SELECT count(*) FROM t WHERE vi <= 5"), 6.0);
    }
    #[test]
    fn sum_btw_3_7() {
        assert_eq!(
            ff(&db(), "SELECT sum(vi) FROM t WHERE vi BETWEEN 3 AND 7"),
            25.0
        );
    }
    #[test]
    fn count_btw_3_7() {
        assert_eq!(
            ff(&db(), "SELECT count(*) FROM t WHERE vi BETWEEN 3 AND 7"),
            5.0
        );
    }
    // ORDER BY + first val
    #[test]
    fn first_asc() {
        assert_eq!(ff(&db(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 1"), 0.0);
    }
    #[test]
    fn first_desc() {
        assert_eq!(ff(&db(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 1"), 9.0);
    }
    // All rows no filter
    #[test]
    fn all_rows() {
        assert_eq!(rc(&db(), "SELECT * FROM t"), 10);
    }
    #[test]
    fn no_rows() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 0"), 0);
    }
    #[test]
    fn over_limit() {
        assert_eq!(rc(&db(), "SELECT * FROM t LIMIT 100"), 10);
    }
    // Additional agg combos
    #[test]
    fn sum_gt0() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi > 0"), 45.0);
    }
    #[test]
    fn sum_gt8() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi > 8"), 9.0);
    }
    #[test]
    fn sum_lt3() {
        assert_eq!(ff(&db(), "SELECT sum(vi) FROM t WHERE vi < 3"), 3.0);
    }
    #[test]
    fn count_lt3() {
        assert_eq!(ff(&db(), "SELECT count(*) FROM t WHERE vi < 3"), 3.0);
    }
    #[test]
    fn max_all() {
        assert_eq!(ff(&db(), "SELECT max(vi) FROM t"), 9.0);
    }
    #[test]
    fn min_all() {
        assert_eq!(ff(&db(), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn sum_btw_0_9() {
        assert_eq!(
            ff(&db(), "SELECT sum(vi) FROM t WHERE vi BETWEEN 0 AND 9"),
            45.0
        );
    }
    #[test]
    fn sum_btw_1_8() {
        assert_eq!(
            ff(&db(), "SELECT sum(vi) FROM t WHERE vi BETWEEN 1 AND 8"),
            36.0
        );
    }
    #[test]
    fn sum_btw_2_7() {
        assert_eq!(
            ff(&db(), "SELECT sum(vi) FROM t WHERE vi BETWEEN 2 AND 7"),
            27.0
        );
    }
    #[test]
    fn count_btw_0_9() {
        assert_eq!(
            ff(&db(), "SELECT count(*) FROM t WHERE vi BETWEEN 0 AND 9"),
            10.0
        );
    }
}
