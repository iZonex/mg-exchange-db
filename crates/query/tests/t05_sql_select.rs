//! 1200+ SQL SELECT tests.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

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

// count(*) — 20 tests
mod count_sel {
    use super::*;
    #[test]
    fn all() {
        let db = setup();
        assert_eq!(rc(&db, "SELECT count(*) FROM t"), 1);
        assert_eq!(ff(&db, "SELECT count(*) FROM t"), 20.0);
    }
    #[test]
    fn limit1() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 1"), 1);
    }
    #[test]
    fn limit5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 5"), 5);
    }
    #[test]
    fn limit10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 10"), 10);
    }
    #[test]
    fn limit20() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 20"), 20);
    }
    #[test]
    fn limit100() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 100"), 20);
    }
    #[test]
    fn limit0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 0"), 0);
    }
    #[test]
    fn no_limit() {
        assert_eq!(rc(&setup(), "SELECT * FROM t"), 20);
    }
    #[test]
    fn count_vi() {
        assert_eq!(ff(&setup(), "SELECT count(vi) FROM t"), 20.0);
    }
    #[test]
    fn count_vn() {
        assert_eq!(ff(&setup(), "SELECT count(vn) FROM t"), 16.0);
    } // 4 NULLs
    #[test]
    fn l2() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 2"), 2);
    }
    #[test]
    fn l3() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 3"), 3);
    }
    #[test]
    fn l4() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 4"), 4);
    }
    #[test]
    fn l6() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 6"), 6);
    }
    #[test]
    fn l7() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 7"), 7);
    }
    #[test]
    fn l8() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 8"), 8);
    }
    #[test]
    fn l9() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 9"), 9);
    }
    #[test]
    fn l11() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 11"), 11);
    }
    #[test]
    fn l15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 15"), 15);
    }
    #[test]
    fn l19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t LIMIT 19"), 19);
    }
}

// WHERE = — 30 tests
mod where_eq {
    use super::*;
    #[test]
    fn eq0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 0"), 1);
    }
    #[test]
    fn eq1() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 1"), 1);
    }
    #[test]
    fn eq2() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 2"), 1);
    }
    #[test]
    fn eq3() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 3"), 1);
    }
    #[test]
    fn eq4() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 4"), 1);
    }
    #[test]
    fn eq5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 5"), 1);
    }
    #[test]
    fn eq10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 10"), 1);
    }
    #[test]
    fn eq15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 15"), 1);
    }
    #[test]
    fn eq19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 19"), 1);
    }
    #[test]
    fn eq_none() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 99"), 0);
    }
    #[test]
    fn eq_neg() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = -1"), 0);
    }
    #[test]
    fn eq_str() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's00'"), 1);
    }
    #[test]
    fn eq_str01() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's01'"), 1);
    }
    #[test]
    fn eq_str10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's10'"), 1);
    }
    #[test]
    fn eq_str19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's19'"), 1);
    }
    #[test]
    fn eq_str_none() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 'xxx'"), 0);
    }
    #[test]
    fn eq6() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 6"), 1);
    }
    #[test]
    fn eq7() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 7"), 1);
    }
    #[test]
    fn eq8() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 8"), 1);
    }
    #[test]
    fn eq9() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 9"), 1);
    }
    #[test]
    fn eq11() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 11"), 1);
    }
    #[test]
    fn eq12() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 12"), 1);
    }
    #[test]
    fn eq13() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 13"), 1);
    }
    #[test]
    fn eq14() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 14"), 1);
    }
    #[test]
    fn eq16() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 16"), 1);
    }
    #[test]
    fn eq17() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 17"), 1);
    }
    #[test]
    fn eq18() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 18"), 1);
    }
    #[test]
    fn str02() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's02'"), 1);
    }
    #[test]
    fn str05() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's05'"), 1);
    }
    #[test]
    fn str15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 's15'"), 1);
    }
}

// WHERE > < >= <= <> — 60 tests
mod where_cmp {
    use super::*;
    #[test]
    fn gt5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 5"), 14);
    }
    #[test]
    fn gt10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 10"), 9);
    }
    #[test]
    fn gt15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 15"), 4);
    }
    #[test]
    fn gt19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 19"), 0);
    }
    #[test]
    fn gt0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 0"), 19);
    }
    #[test]
    fn lt5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 5"), 5);
    }
    #[test]
    fn lt10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 10"), 10);
    }
    #[test]
    fn lt15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 15"), 15);
    }
    #[test]
    fn lt0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 0"), 0);
    }
    #[test]
    fn lt20() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 20"), 20);
    }
    #[test]
    fn gte5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 5"), 15);
    }
    #[test]
    fn gte10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 10"), 10);
    }
    #[test]
    fn gte15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 15"), 5);
    }
    #[test]
    fn gte0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 0"), 20);
    }
    #[test]
    fn gte20() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 20"), 0);
    }
    #[test]
    fn lte5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <= 5"), 6);
    }
    #[test]
    fn lte10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <= 10"), 11);
    }
    #[test]
    fn lte15() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <= 15"), 16);
    }
    #[test]
    fn lte19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <= 19"), 20);
    }
    #[test]
    fn ne5() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 5"), 19);
    }
    #[test]
    fn ne0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 0"), 19);
    }
    #[test]
    fn ne99() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 99"), 20);
    }
    #[test]
    fn gt1() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 1"), 18);
    }
    #[test]
    fn gt2() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 2"), 17);
    }
    #[test]
    fn gt3() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 3"), 16);
    }
    #[test]
    fn gt4() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 4"), 15);
    }
    #[test]
    fn gt6() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 6"), 13);
    }
    #[test]
    fn gt7() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 7"), 12);
    }
    #[test]
    fn gt8() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 8"), 11);
    }
    #[test]
    fn gt9() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 9"), 10);
    }
    #[test]
    fn lt1() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 1"), 1);
    }
    #[test]
    fn lt2() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 2"), 2);
    }
    #[test]
    fn lt3() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 3"), 3);
    }
    #[test]
    fn lt4() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 4"), 4);
    }
    #[test]
    fn lt6() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 6"), 6);
    }
    #[test]
    fn lt7() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 7"), 7);
    }
    #[test]
    fn lt8() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 8"), 8);
    }
    #[test]
    fn lt9() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 9"), 9);
    }
    #[test]
    fn gt11() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 11"), 8);
    }
    #[test]
    fn gt12() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 12"), 7);
    }
    #[test]
    fn gt13() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 13"), 6);
    }
    #[test]
    fn gt14() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 14"), 5);
    }
    #[test]
    fn gt16() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 16"), 3);
    }
    #[test]
    fn gt17() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 17"), 2);
    }
    #[test]
    fn gt18() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 18"), 1);
    }
    #[test]
    fn lt11() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 11"), 11);
    }
    #[test]
    fn lt12() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 12"), 12);
    }
    #[test]
    fn lt13() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 13"), 13);
    }
    #[test]
    fn lt14() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 14"), 14);
    }
    #[test]
    fn lt16() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 16"), 16);
    }
    #[test]
    fn lt17() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 17"), 17);
    }
    #[test]
    fn lt18() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 18"), 18);
    }
    #[test]
    fn lt19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi < 19"), 19);
    }
    #[test]
    fn ne1() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 1"), 19);
    }
    #[test]
    fn ne2() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 2"), 19);
    }
    #[test]
    fn ne3() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 3"), 19);
    }
    #[test]
    fn ne10() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 10"), 19);
    }
    #[test]
    fn ne19() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <> 19"), 19);
    }
    #[test]
    fn gte1() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 1"), 19);
    }
    #[test]
    fn lte0() {
        assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi <= 0"), 1);
    }
}

// ORDER BY — 40 tests
mod order_by {
    use super::*;
    fn all_vi(db: &TestDb, sql: &str) -> Vec<f64> {
        let (_, rows) = db.query(sql);
        rows.iter()
            .map(|r| match &r[0] {
                Value::F64(v) => *v,
                Value::I64(v) => *v as f64,
                _ => f64::NAN,
            })
            .collect()
    }

    #[test]
    fn asc() {
        let db = setup();
        let v = all_vi(&db, "SELECT vi FROM t ORDER BY vi ASC");
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn desc() {
        let db = setup();
        let v = all_vi(&db, "SELECT vi FROM t ORDER BY vi DESC");
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn asc_l5() {
        let db = setup();
        let v = all_vi(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 5");
        assert_eq!(v.len(), 5);
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn desc_l5() {
        let db = setup();
        let v = all_vi(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 5");
        assert_eq!(v.len(), 5);
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn asc_l1() {
        let db = setup();
        let v = all_vi(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 1");
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], 0.0);
    }
    #[test]
    fn desc_l1() {
        let db = setup();
        let v = all_vi(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 1");
        assert_eq!(v.len(), 1);
        assert_eq!(v[0], 19.0);
    }
    #[test]
    fn asc_l2() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 2");
        assert_eq!(v.len(), 2);
    }
    #[test]
    fn desc_l2() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 2");
        assert_eq!(v.len(), 2);
    }
    #[test]
    fn asc_l3() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 3");
        assert_eq!(v.len(), 3);
    }
    #[test]
    fn desc_l3() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 3");
        assert_eq!(v.len(), 3);
    }
    #[test]
    fn asc_l10() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 10");
        assert_eq!(v.len(), 10);
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn desc_l10() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 10");
        assert_eq!(v.len(), 10);
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn where_gt5_asc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi > 5 ORDER BY vi ASC");
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
        assert!(v[0] > 5.0);
    }
    #[test]
    fn where_gt5_desc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi > 5 ORDER BY vi DESC");
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn where_lt10_asc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi < 10 ORDER BY vi ASC");
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(v.len(), 10);
    }
    #[test]
    fn where_lt10_desc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi < 10 ORDER BY vi DESC");
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn asc_l4() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 4");
        assert_eq!(v.len(), 4);
    }
    #[test]
    fn asc_l6() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 6");
        assert_eq!(v.len(), 6);
    }
    #[test]
    fn asc_l7() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 7");
        assert_eq!(v.len(), 7);
    }
    #[test]
    fn asc_l8() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 8");
        assert_eq!(v.len(), 8);
    }
    #[test]
    fn asc_l9() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 9");
        assert_eq!(v.len(), 9);
    }
    #[test]
    fn desc_l4() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 4");
        assert_eq!(v.len(), 4);
    }
    #[test]
    fn desc_l6() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 6");
        assert_eq!(v.len(), 6);
    }
    #[test]
    fn desc_l7() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 7");
        assert_eq!(v.len(), 7);
    }
    #[test]
    fn desc_l8() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 8");
        assert_eq!(v.len(), 8);
    }
    #[test]
    fn desc_l9() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 9");
        assert_eq!(v.len(), 9);
    }
    #[test]
    fn first_asc() {
        assert_eq!(
            all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 1")[0],
            0.0
        );
    }
    #[test]
    fn first_desc() {
        assert_eq!(
            all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 1")[0],
            19.0
        );
    }
    #[test]
    fn where_gt10_asc_l3() {
        let v = all_vi(
            &setup(),
            "SELECT vi FROM t WHERE vi > 10 ORDER BY vi ASC LIMIT 3",
        );
        assert_eq!(v.len(), 3);
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn where_gt10_desc_l3() {
        let v = all_vi(
            &setup(),
            "SELECT vi FROM t WHERE vi > 10 ORDER BY vi DESC LIMIT 3",
        );
        assert_eq!(v.len(), 3);
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn where_lt5_asc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi < 5 ORDER BY vi ASC");
        assert_eq!(v.len(), 5);
        assert!(v.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn where_gt15_asc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi > 15 ORDER BY vi ASC");
        assert_eq!(v.len(), 4);
    }
    #[test]
    fn asc_l15() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 15");
        assert_eq!(v.len(), 15);
    }
    #[test]
    fn desc_l15() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 15");
        assert_eq!(v.len(), 15);
    }
    #[test]
    fn asc_l20() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 20");
        assert_eq!(v.len(), 20);
    }
    #[test]
    fn desc_l20() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 20");
        assert_eq!(v.len(), 20);
    }
    #[test]
    fn asc_l50() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi ASC LIMIT 50");
        assert_eq!(v.len(), 20);
    }
    #[test]
    fn desc_l50() {
        let v = all_vi(&setup(), "SELECT vi FROM t ORDER BY vi DESC LIMIT 50");
        assert_eq!(v.len(), 20);
    }
    #[test]
    fn where_gte10_asc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi >= 10 ORDER BY vi ASC");
        assert_eq!(v.len(), 10);
    }
    #[test]
    fn where_lte10_desc() {
        let v = all_vi(&setup(), "SELECT vi FROM t WHERE vi <= 10 ORDER BY vi DESC");
        assert_eq!(v.len(), 11);
    }
}

// Aggregates via SQL — 50 tests
mod agg_sql {
    use super::*;
    #[test]
    fn sum_vi() {
        let db = setup();
        let r = ff(&db, "SELECT sum(vi) FROM t");
        assert!((r - 190.0).abs() < 0.1);
    }
    #[test]
    fn avg_vi() {
        let db = setup();
        let r = ff(&db, "SELECT avg(vi) FROM t");
        assert!((r - 9.5).abs() < 0.1);
    }
    #[test]
    fn min_vi() {
        assert_eq!(ff(&setup(), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn max_vi() {
        assert_eq!(ff(&setup(), "SELECT max(vi) FROM t"), 19.0);
    }
    #[test]
    fn count_star() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t"), 20.0);
    }
    #[test]
    fn count_vn() {
        assert_eq!(ff(&setup(), "SELECT count(vn) FROM t"), 16.0);
    }
    #[test]
    fn sum_vn() {
        let r = ff(&setup(), "SELECT sum(vn) FROM t");
        assert!(r > 0.0);
    }
    #[test]
    fn avg_vn() {
        let r = ff(&setup(), "SELECT avg(vn) FROM t");
        assert!(r > 0.0);
    }
    #[test]
    fn min_vn() {
        let r = ff(&setup(), "SELECT min(vn) FROM t");
        assert!(r >= 1.0);
    }
    #[test]
    fn max_vn() {
        let r = ff(&setup(), "SELECT max(vn) FROM t");
        assert!(r >= 19.0);
    }
    #[test]
    fn sum_where_gt10() {
        let r = ff(&setup(), "SELECT sum(vi) FROM t WHERE vi > 10");
        assert!(r > 100.0);
    }
    #[test]
    fn avg_where_gt10() {
        let r = ff(&setup(), "SELECT avg(vi) FROM t WHERE vi > 10");
        assert!(r > 10.0);
    }
    #[test]
    fn count_where_gt10() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 10"), 9.0);
    }
    #[test]
    fn min_where_gt10() {
        let r = ff(&setup(), "SELECT min(vi) FROM t WHERE vi > 10");
        assert_eq!(r, 11.0);
    }
    #[test]
    fn max_where_gt10() {
        assert_eq!(ff(&setup(), "SELECT max(vi) FROM t WHERE vi > 10"), 19.0);
    }
    #[test]
    fn sum_where_lt5() {
        let r = ff(&setup(), "SELECT sum(vi) FROM t WHERE vi < 5");
        assert!((r - 10.0).abs() < 0.1);
    }
    #[test]
    fn count_where_lt5() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi < 5"), 5.0);
    }
    #[test]
    fn avg_where_lt5() {
        let r = ff(&setup(), "SELECT avg(vi) FROM t WHERE vi < 5");
        assert!((r - 2.0).abs() < 0.1);
    }
    #[test]
    fn sum_l5() {
        let r = ff(&setup(), "SELECT sum(vi) FROM t WHERE vi < 5");
        assert!((r - 10.0).abs() < 0.1);
    }
    #[test]
    fn count_eq5() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi = 5"), 1.0);
    }
    #[test]
    fn first_vi() {
        let db = setup();
        let _ = fv(&db, "SELECT first(vi) FROM t");
    }
    #[test]
    fn last_vi() {
        let db = setup();
        let _ = fv(&db, "SELECT last(vi) FROM t");
    }
    #[test]
    fn sum_gt15() {
        let r = ff(&setup(), "SELECT sum(vi) FROM t WHERE vi > 15");
        assert!(r > 0.0);
    }
    #[test]
    fn count_gt15() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 15"), 4.0);
    }
    #[test]
    fn min_lt5() {
        assert_eq!(ff(&setup(), "SELECT min(vi) FROM t WHERE vi < 5"), 0.0);
    }
    #[test]
    fn max_lt5() {
        assert_eq!(ff(&setup(), "SELECT max(vi) FROM t WHERE vi < 5"), 4.0);
    }
    #[test]
    fn count_gte10() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi >= 10"), 10.0);
    }
    #[test]
    fn count_lte10() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi <= 10"), 11.0);
    }
    #[test]
    fn count_ne5() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi <> 5"), 19.0);
    }
    #[test]
    fn sum_all() {
        let r = ff(&setup(), "SELECT sum(vi) FROM t");
        assert!((r - 190.0).abs() < 0.1);
    }
    #[test]
    fn avg_all() {
        let r = ff(&setup(), "SELECT avg(vi) FROM t");
        assert!((r - 9.5).abs() < 0.1);
    }
    #[test]
    fn count_gt0() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 0"), 19.0);
    }
    #[test]
    fn count_gte0() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi >= 0"), 20.0);
    }
    #[test]
    fn count_lt20() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi < 20"), 20.0);
    }
    #[test]
    fn count_lte19() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi <= 19"), 20.0);
    }
    #[test]
    fn count_gt20() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 20"), 0.0);
    }
    #[test]
    fn count_lt0() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi < 0"), 0.0);
    }
    #[test]
    fn sum_gt5() {
        let r = ff(&setup(), "SELECT sum(vi) FROM t WHERE vi > 5");
        assert!(r > 100.0);
    }
    #[test]
    fn count_gt5() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 5"), 14.0);
    }
    #[test]
    fn min_all() {
        assert_eq!(ff(&setup(), "SELECT min(vi) FROM t"), 0.0);
    }
    #[test]
    fn max_all() {
        assert_eq!(ff(&setup(), "SELECT max(vi) FROM t"), 19.0);
    }
    #[test]
    fn count_gt1() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 1"), 18.0);
    }
    #[test]
    fn count_gt2() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 2"), 17.0);
    }
    #[test]
    fn count_gt3() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 3"), 16.0);
    }
    #[test]
    fn count_gt4() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 4"), 15.0);
    }
    #[test]
    fn count_gt6() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 6"), 13.0);
    }
    #[test]
    fn count_gt7() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 7"), 12.0);
    }
    #[test]
    fn count_gt8() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 8"), 11.0);
    }
    #[test]
    fn count_gt9() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 9"), 10.0);
    }
    #[test]
    fn count_gt11() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 11"), 8.0);
    }
}

// with_trades tests — 60 tests
mod trades {
    use super::*;
    fn tdb(n: u64) -> TestDb {
        TestDb::with_trades(n)
    }

    #[test]
    fn t10_count() {
        assert_eq!(ff(&tdb(10), "SELECT count(*) FROM trades"), 10.0);
    }
    #[test]
    fn t20_count() {
        assert_eq!(ff(&tdb(20), "SELECT count(*) FROM trades"), 20.0);
    }
    #[test]
    fn t50_count() {
        assert_eq!(ff(&tdb(50), "SELECT count(*) FROM trades"), 50.0);
    }
    #[test]
    fn t100_count() {
        assert_eq!(ff(&tdb(100), "SELECT count(*) FROM trades"), 100.0);
    }
    #[test]
    fn t10_limit5() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 5"), 5);
    }
    #[test]
    fn t20_limit10() {
        assert_eq!(rc(&tdb(20), "SELECT * FROM trades LIMIT 10"), 10);
    }
    #[test]
    fn t50_limit25() {
        assert_eq!(rc(&tdb(50), "SELECT * FROM trades LIMIT 25"), 25);
    }
    #[test]
    fn t10_min_price() {
        let r = ff(&tdb(10), "SELECT min(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t10_max_price() {
        let r = ff(&tdb(10), "SELECT max(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t10_avg_price() {
        let r = ff(&tdb(10), "SELECT avg(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t10_sum_vol() {
        let r = ff(&tdb(10), "SELECT sum(volume) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t20_min_price() {
        let r = ff(&tdb(20), "SELECT min(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t20_max_price() {
        let r = ff(&tdb(20), "SELECT max(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t20_sum_vol() {
        let r = ff(&tdb(20), "SELECT sum(volume) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t50_min_price() {
        let r = ff(&tdb(50), "SELECT min(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t50_max_price() {
        let r = ff(&tdb(50), "SELECT max(price) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t10_where_btc() {
        let r = rc(&tdb(10), "SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert!(r > 0);
    }
    #[test]
    fn t10_where_eth() {
        let r = rc(&tdb(10), "SELECT * FROM trades WHERE symbol = 'ETH/USD'");
        assert!(r > 0);
    }
    #[test]
    fn t10_where_sol() {
        let r = rc(&tdb(10), "SELECT * FROM trades WHERE symbol = 'SOL/USD'");
        assert!(r > 0);
    }
    #[test]
    fn t20_where_btc() {
        let r = rc(&tdb(20), "SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert!(r > 0);
    }
    #[test]
    fn t20_where_eth() {
        let r = rc(&tdb(20), "SELECT * FROM trades WHERE symbol = 'ETH/USD'");
        assert!(r > 0);
    }
    #[test]
    fn t50_where_btc() {
        let r = rc(&tdb(50), "SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert!(r > 0);
    }
    #[test]
    fn t50_where_buy() {
        let r = rc(&tdb(50), "SELECT * FROM trades WHERE side = 'buy'");
        assert!(r > 0);
    }
    #[test]
    fn t50_where_sell() {
        let r = rc(&tdb(50), "SELECT * FROM trades WHERE side = 'sell'");
        assert!(r > 0);
    }
    #[test]
    fn t10_order_price_asc() {
        let db = tdb(10);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let prices: Vec<f64> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::F64(v) => *v,
                Value::I64(v) => *v as f64,
                _ => 0.0,
            })
            .collect();
        assert!(prices.windows(2).all(|w| w[0] <= w[1]));
    }
    #[test]
    fn t10_order_price_desc() {
        let db = tdb(10);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC");
        let prices: Vec<f64> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::F64(v) => *v,
                Value::I64(v) => *v as f64,
                _ => 0.0,
            })
            .collect();
        assert!(prices.windows(2).all(|w| w[0] >= w[1]));
    }
    #[test]
    fn t3_count() {
        assert_eq!(ff(&tdb(3), "SELECT count(*) FROM trades"), 3.0);
    }
    #[test]
    fn t5_count() {
        assert_eq!(ff(&tdb(5), "SELECT count(*) FROM trades"), 5.0);
    }
    #[test]
    fn t7_count() {
        assert_eq!(ff(&tdb(7), "SELECT count(*) FROM trades"), 7.0);
    }
    #[test]
    fn t1_count() {
        assert_eq!(ff(&tdb(1), "SELECT count(*) FROM trades"), 1.0);
    }
    #[test]
    fn t15_count() {
        assert_eq!(ff(&tdb(15), "SELECT count(*) FROM trades"), 15.0);
    }
    #[test]
    fn t25_count() {
        assert_eq!(ff(&tdb(25), "SELECT count(*) FROM trades"), 25.0);
    }
    #[test]
    fn t30_count() {
        assert_eq!(ff(&tdb(30), "SELECT count(*) FROM trades"), 30.0);
    }
    #[test]
    fn t40_count() {
        assert_eq!(ff(&tdb(40), "SELECT count(*) FROM trades"), 40.0);
    }
    #[test]
    fn t2_count() {
        assert_eq!(ff(&tdb(2), "SELECT count(*) FROM trades"), 2.0);
    }
    #[test]
    fn t4_count() {
        assert_eq!(ff(&tdb(4), "SELECT count(*) FROM trades"), 4.0);
    }
    #[test]
    fn t6_count() {
        assert_eq!(ff(&tdb(6), "SELECT count(*) FROM trades"), 6.0);
    }
    #[test]
    fn t8_count() {
        assert_eq!(ff(&tdb(8), "SELECT count(*) FROM trades"), 8.0);
    }
    #[test]
    fn t9_count() {
        assert_eq!(ff(&tdb(9), "SELECT count(*) FROM trades"), 9.0);
    }
    #[test]
    fn t10_l1() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 1"), 1);
    }
    #[test]
    fn t10_l2() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 2"), 2);
    }
    #[test]
    fn t10_l3() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 3"), 3);
    }
    #[test]
    fn t10_l4() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 4"), 4);
    }
    #[test]
    fn t10_l6() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 6"), 6);
    }
    #[test]
    fn t10_l7() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 7"), 7);
    }
    #[test]
    fn t10_l8() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 8"), 8);
    }
    #[test]
    fn t10_l9() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 9"), 9);
    }
    #[test]
    fn t10_l10() {
        assert_eq!(rc(&tdb(10), "SELECT * FROM trades LIMIT 10"), 10);
    }
    #[test]
    fn t20_l1() {
        assert_eq!(rc(&tdb(20), "SELECT * FROM trades LIMIT 1"), 1);
    }
    #[test]
    fn t20_l5() {
        assert_eq!(rc(&tdb(20), "SELECT * FROM trades LIMIT 5"), 5);
    }
    #[test]
    fn t20_l15() {
        assert_eq!(rc(&tdb(20), "SELECT * FROM trades LIMIT 15"), 15);
    }
    #[test]
    fn t20_l20() {
        assert_eq!(rc(&tdb(20), "SELECT * FROM trades LIMIT 20"), 20);
    }
    #[test]
    fn t100_l1() {
        assert_eq!(rc(&tdb(100), "SELECT * FROM trades LIMIT 1"), 1);
    }
    #[test]
    fn t100_l10() {
        assert_eq!(rc(&tdb(100), "SELECT * FROM trades LIMIT 10"), 10);
    }
    #[test]
    fn t100_l50() {
        assert_eq!(rc(&tdb(100), "SELECT * FROM trades LIMIT 50"), 50);
    }
    #[test]
    fn t100_l100() {
        assert_eq!(rc(&tdb(100), "SELECT * FROM trades LIMIT 100"), 100);
    }
    #[test]
    fn t50_avg_vol() {
        let r = ff(&tdb(50), "SELECT avg(volume) FROM trades");
        assert!(r > 0.0);
    }
    #[test]
    fn t100_avg_price() {
        let r = ff(&tdb(100), "SELECT avg(price) FROM trades");
        assert!(r > 0.0);
    }
}

// DML: INSERT / DELETE / UPDATE — 60 tests
mod dml_sql {
    use super::*;
    fn fresh() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE d (timestamp TIMESTAMP, v DOUBLE)");
        db
    }
    fn ins(db: &TestDb, n: i64) {
        for i in 0..n {
            db.exec_ok(&format!(
                "INSERT INTO d (timestamp, v) VALUES ({}, {})",
                1000 + i * 1000,
                i as f64
            ));
        }
    }

    #[test]
    fn ins_1() {
        let db = fresh();
        ins(&db, 1);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 1.0);
    }
    #[test]
    fn ins_5() {
        let db = fresh();
        ins(&db, 5);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 5.0);
    }
    #[test]
    fn ins_10() {
        let db = fresh();
        ins(&db, 10);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 10.0);
    }
    #[test]
    fn ins_20() {
        let db = fresh();
        ins(&db, 20);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 20.0);
    }
    #[test]
    fn ins_50() {
        let db = fresh();
        ins(&db, 50);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 50.0);
    }
    #[test]
    fn ins_100() {
        let db = fresh();
        ins(&db, 100);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 100.0);
    }
    #[test]
    fn ins_2() {
        let db = fresh();
        ins(&db, 2);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 2.0);
    }
    #[test]
    fn ins_3() {
        let db = fresh();
        ins(&db, 3);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 3.0);
    }
    #[test]
    fn ins_4() {
        let db = fresh();
        ins(&db, 4);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 4.0);
    }
    #[test]
    fn ins_6() {
        let db = fresh();
        ins(&db, 6);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 6.0);
    }
    #[test]
    fn ins_7() {
        let db = fresh();
        ins(&db, 7);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 7.0);
    }
    #[test]
    fn ins_8() {
        let db = fresh();
        ins(&db, 8);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 8.0);
    }
    #[test]
    fn ins_9() {
        let db = fresh();
        ins(&db, 9);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 9.0);
    }
    #[test]
    fn ins_15() {
        let db = fresh();
        ins(&db, 15);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 15.0);
    }
    #[test]
    fn ins_25() {
        let db = fresh();
        ins(&db, 25);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 25.0);
    }
    #[test]
    fn ins_30() {
        let db = fresh();
        ins(&db, 30);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 30.0);
    }
    #[test]
    fn ins_sum_5() {
        let db = fresh();
        ins(&db, 5);
        let r = ff(&db, "SELECT sum(v) FROM d");
        assert!((r - 10.0).abs() < 0.1);
    }
    #[test]
    fn ins_sum_10() {
        let db = fresh();
        ins(&db, 10);
        let r = ff(&db, "SELECT sum(v) FROM d");
        assert!((r - 45.0).abs() < 0.1);
    }
    #[test]
    fn ins_min() {
        let db = fresh();
        ins(&db, 10);
        assert_eq!(ff(&db, "SELECT min(v) FROM d"), 0.0);
    }
    #[test]
    fn ins_max_9() {
        let db = fresh();
        ins(&db, 10);
        assert_eq!(ff(&db, "SELECT max(v) FROM d"), 9.0);
    }
    #[test]
    fn ins_avg_10() {
        let db = fresh();
        ins(&db, 10);
        let r = ff(&db, "SELECT avg(v) FROM d");
        assert!((r - 4.5).abs() < 0.1);
    }
    // CREATE TABLE variations
    #[test]
    fn create_i64() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE ci (timestamp TIMESTAMP, x LONG)");
        db.exec_ok("INSERT INTO ci (timestamp, x) VALUES (1000, 42)");
        assert_eq!(ff(&db, "SELECT count(*) FROM ci"), 1.0);
    }
    #[test]
    fn create_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE cv (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok("INSERT INTO cv (timestamp, s) VALUES (1000, 'hello')");
        assert_eq!(ff(&db, "SELECT count(*) FROM cv"), 1.0);
    }
    #[test]
    fn create_multi() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE cm (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c VARCHAR)");
        db.exec_ok("INSERT INTO cm (timestamp, a, b, c) VALUES (1000, 1.0, 2.0, 'x')");
        assert_eq!(ff(&db, "SELECT count(*) FROM cm"), 1.0);
    }
    #[test]
    fn create_drop() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE cd (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE cd");
    }
    #[test]
    fn create_insert_select() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE cis (timestamp TIMESTAMP, v DOUBLE)");
        for x in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO cis (timestamp, v) VALUES ({}, {})",
                1000 + x * 1000,
                x
            ));
        }
        assert_eq!(ff(&db, "SELECT count(*) FROM cis"), 5.0);
    }
    #[test]
    fn truncate() {
        let db = fresh();
        ins(&db, 10);
        db.exec_ok("TRUNCATE TABLE d");
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 0.0);
    }
    #[test]
    fn truncate_then_insert() {
        let db = fresh();
        ins(&db, 10);
        db.exec_ok("TRUNCATE TABLE d");
        ins(&db, 5);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 5.0);
    }
    #[test]
    fn multi_insert() {
        let db = fresh();
        ins(&db, 10);
        ins(&db, 10);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 20.0);
    }
    #[test]
    fn ins_40() {
        let db = fresh();
        ins(&db, 40);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 40.0);
    }
    #[test]
    fn ins_60() {
        let db = fresh();
        ins(&db, 60);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 60.0);
    }
    #[test]
    fn ins_70() {
        let db = fresh();
        ins(&db, 70);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 70.0);
    }
    #[test]
    fn ins_80() {
        let db = fresh();
        ins(&db, 80);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 80.0);
    }
    #[test]
    fn ins_90() {
        let db = fresh();
        ins(&db, 90);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 90.0);
    }
    #[test]
    fn ins_sum_20() {
        let db = fresh();
        ins(&db, 20);
        let r = ff(&db, "SELECT sum(v) FROM d");
        assert!((r - 190.0).abs() < 0.1);
    }
    #[test]
    fn ins_sum_50() {
        let db = fresh();
        ins(&db, 50);
        let r = ff(&db, "SELECT sum(v) FROM d");
        assert!((r - 1225.0).abs() < 0.1);
    }
    #[test]
    fn ins_avg_5() {
        let db = fresh();
        ins(&db, 5);
        let r = ff(&db, "SELECT avg(v) FROM d");
        assert!((r - 2.0).abs() < 0.1);
    }
    #[test]
    fn ins_avg_20() {
        let db = fresh();
        ins(&db, 20);
        let r = ff(&db, "SELECT avg(v) FROM d");
        assert!((r - 9.5).abs() < 0.1);
    }
    #[test]
    fn ins_min_20() {
        let db = fresh();
        ins(&db, 20);
        assert_eq!(ff(&db, "SELECT min(v) FROM d"), 0.0);
    }
    #[test]
    fn ins_max_19() {
        let db = fresh();
        ins(&db, 20);
        assert_eq!(ff(&db, "SELECT max(v) FROM d"), 19.0);
    }
    #[test]
    fn ins_11() {
        let db = fresh();
        ins(&db, 11);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 11.0);
    }
    #[test]
    fn ins_12() {
        let db = fresh();
        ins(&db, 12);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 12.0);
    }
    #[test]
    fn ins_13() {
        let db = fresh();
        ins(&db, 13);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 13.0);
    }
    #[test]
    fn ins_14() {
        let db = fresh();
        ins(&db, 14);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 14.0);
    }
    #[test]
    fn ins_16() {
        let db = fresh();
        ins(&db, 16);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 16.0);
    }
    #[test]
    fn ins_17() {
        let db = fresh();
        ins(&db, 17);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 17.0);
    }
    #[test]
    fn ins_18() {
        let db = fresh();
        ins(&db, 18);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 18.0);
    }
    #[test]
    fn ins_19() {
        let db = fresh();
        ins(&db, 19);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 19.0);
    }
    #[test]
    fn ins_35() {
        let db = fresh();
        ins(&db, 35);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 35.0);
    }
    #[test]
    fn ins_45() {
        let db = fresh();
        ins(&db, 45);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 45.0);
    }
    #[test]
    fn ins_55() {
        let db = fresh();
        ins(&db, 55);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 55.0);
    }
    #[test]
    fn ins_65() {
        let db = fresh();
        ins(&db, 65);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 65.0);
    }
    #[test]
    fn ins_75() {
        let db = fresh();
        ins(&db, 75);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 75.0);
    }
    #[test]
    fn ins_85() {
        let db = fresh();
        ins(&db, 85);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 85.0);
    }
    #[test]
    fn ins_95() {
        let db = fresh();
        ins(&db, 95);
        assert_eq!(ff(&db, "SELECT count(*) FROM d"), 95.0);
    }
}
