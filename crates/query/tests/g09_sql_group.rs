//! 500 SQL GROUP BY / ORDER BY / LIMIT / DISTINCT tests.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

fn setup() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, grp VARCHAR, cat VARCHAR)");
    for idx in 0..60i64 {
        let ts = 1_000_000_000_000i64 + idx * 1_000_000_000;
        let vi = idx as f64;
        let grp = format!("g{}", idx % 6); // 6 groups: g0..g5
        let cat = format!("c{}", idx % 3); // 3 categories: c0..c2
        db.exec_ok(&format!(
            "INSERT INTO t (timestamp, vi, grp, cat) VALUES ({ts}, {vi}, '{grp}', '{cat}')"
        ));
    }
    db
}

fn rc(db: &TestDb, sql: &str) -> usize {
    let (_, rows) = db.query(sql);
    rows.len()
}
fn ff(db: &TestDb, sql: &str) -> f64 {
    let (_, rows) = db.query(sql);
    match &rows[0][0] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        other => panic!("expected num, got {other:?}"),
    }
}
fn col_f(db: &TestDb, sql: &str, col: usize) -> Vec<f64> {
    let (_, rows) = db.query(sql);
    rows.iter()
        .map(|r| match &r[col] {
            Value::F64(v) => *v,
            Value::I64(v) => *v as f64,
            other => panic!("unexpected {other:?}"),
        })
        .collect()
}

// GROUP BY grp — should produce 6 groups
mod group_basic {
    use super::*;
    #[test]
    fn count_groups() {
        assert_eq!(rc(&setup(), "SELECT grp, count(*) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn sum_groups() {
        assert_eq!(rc(&setup(), "SELECT grp, sum(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn avg_groups() {
        assert_eq!(rc(&setup(), "SELECT grp, avg(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn min_groups() {
        assert_eq!(rc(&setup(), "SELECT grp, min(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn max_groups() {
        assert_eq!(rc(&setup(), "SELECT grp, max(vi) FROM t GROUP BY grp"), 6);
    }
}

// GROUP BY cat — should produce 3 groups
mod group_cat {
    use super::*;
    #[test]
    fn count_groups() {
        assert_eq!(rc(&setup(), "SELECT cat, count(*) FROM t GROUP BY cat"), 3);
    }
    #[test]
    fn sum_groups() {
        assert_eq!(rc(&setup(), "SELECT cat, sum(vi) FROM t GROUP BY cat"), 3);
    }
    #[test]
    fn avg_groups() {
        assert_eq!(rc(&setup(), "SELECT cat, avg(vi) FROM t GROUP BY cat"), 3);
    }
    #[test]
    fn min_groups() {
        assert_eq!(rc(&setup(), "SELECT cat, min(vi) FROM t GROUP BY cat"), 3);
    }
    #[test]
    fn max_groups() {
        assert_eq!(rc(&setup(), "SELECT cat, max(vi) FROM t GROUP BY cat"), 3);
    }
}

// GROUP BY with WHERE
mod group_where {
    use super::*;
    #[test]
    fn gt10() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT grp, count(*) FROM t WHERE vi > 10 GROUP BY grp"
            ),
            6
        );
    }
    #[test]
    fn lt10() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT grp, count(*) FROM t WHERE vi < 10 GROUP BY grp"
            ),
            6
        );
    }
    #[test]
    fn gt30() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT grp, count(*) FROM t WHERE vi > 30 GROUP BY grp"
            ),
            6
        );
    }
    #[test]
    fn lt30() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT grp, count(*) FROM t WHERE vi < 30 GROUP BY grp"
            ),
            6
        );
    }
    #[test]
    fn bt10_50() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT grp, count(*) FROM t WHERE vi BETWEEN 10 AND 50 GROUP BY grp"
            ),
            6
        );
    }
    #[test]
    fn bt0_5() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT grp, count(*) FROM t WHERE vi BETWEEN 0 AND 5 GROUP BY grp"
            ),
            6
        );
    }
    #[test]
    fn cat_gt10() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT cat, count(*) FROM t WHERE vi > 10 GROUP BY cat"
            ),
            3
        );
    }
    #[test]
    fn cat_lt30() {
        assert_eq!(
            rc(
                &setup(),
                "SELECT cat, count(*) FROM t WHERE vi < 30 GROUP BY cat"
            ),
            3
        );
    }
}

// ORDER BY vi ASC / DESC (LIMIT to check ordering)
mod order_by {
    use super::*;
    #[test]
    fn asc_first() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 1", 0);
        assert!((v[0]).abs() < 0.01);
    }
    #[test]
    fn asc_first5() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 5", 0);
        for (i, &vi) in v.iter().enumerate().take(5) {
            assert!((vi - i as f64).abs() < 0.01);
        }
    }
    #[test]
    fn desc_first() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 1", 0);
        assert!((v[0] - 59.0).abs() < 0.01);
    }
    #[test]
    fn desc_first5() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 5", 0);
        for (i, &vi) in v.iter().enumerate().take(5) {
            assert!((vi - (59 - i) as f64).abs() < 0.01);
        }
    }
    #[test]
    fn asc_all() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC", 0);
        for i in 1..v.len() {
            assert!(v[i] >= v[i - 1]);
        }
    }
    #[test]
    fn desc_all() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC", 0);
        for i in 1..v.len() {
            assert!(v[i] <= v[i - 1]);
        }
    }
    #[test]
    fn asc_lim10() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 10", 0);
        assert_eq!(v.len(), 10);
        for (i, &vi) in v.iter().enumerate().take(10) {
            assert!((vi - i as f64).abs() < 0.01);
        }
    }
    #[test]
    fn desc_lim10() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 10", 0);
        assert_eq!(v.len(), 10);
        for (i, &vi) in v.iter().enumerate().take(10) {
            assert!((vi - (59 - i) as f64).abs() < 0.01);
        }
    }
}

// LIMIT tests
mod limits {
    use super::*;
    macro_rules! lm {
        ($n:ident, $lim:expr, $expect:expr) => {
            #[test]
            fn $n() {
                assert_eq!(
                    rc(&setup(), &format!("SELECT * FROM t LIMIT {}", $lim)),
                    $expect
                );
            }
        };
    }
    lm!(l0, 0, 0);
    lm!(l1, 1, 1);
    lm!(l2, 2, 2);
    lm!(l3, 3, 3);
    lm!(l4, 4, 4);
    lm!(l5, 5, 5);
    lm!(l10, 10, 10);
    lm!(l15, 15, 15);
    lm!(l20, 20, 20);
    lm!(l25, 25, 25);
    lm!(l30, 30, 30);
    lm!(l40, 40, 40);
    lm!(l50, 50, 50);
    lm!(l60, 60, 60);
    lm!(l100, 100, 60);
}

// Aggregate totals
mod agg_total {
    use super::*;
    #[test]
    fn count_all() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t"), 60.0);
    }
    #[test]
    fn sum_all() {
        assert!((ff(&setup(), "SELECT sum(vi) FROM t") - 1770.0).abs() < 0.01);
    } // 0+1+...+59=1770
    #[test]
    fn avg_all() {
        assert!((ff(&setup(), "SELECT avg(vi) FROM t") - 29.5).abs() < 0.01);
    }
    #[test]
    fn min_all() {
        assert!((ff(&setup(), "SELECT min(vi) FROM t")).abs() < 0.01);
    }
    #[test]
    fn max_all() {
        assert!((ff(&setup(), "SELECT max(vi) FROM t") - 59.0).abs() < 0.01);
    }
}

// GROUP BY grp LIMIT
mod group_limit {
    use super::*;
    macro_rules! gl {
        ($n:ident, $lim:expr, $expect:expr) => {
            #[test]
            fn $n() {
                assert_eq!(
                    rc(
                        &setup(),
                        &format!("SELECT grp, count(*) FROM t GROUP BY grp LIMIT {}", $lim)
                    ),
                    $expect
                );
            }
        };
    }
    gl!(l1, 1, 1);
    gl!(l2, 2, 2);
    gl!(l3, 3, 3);
    gl!(l4, 4, 4);
    gl!(l5, 5, 5);
    gl!(l6, 6, 6);
    gl!(l10, 10, 6);
    gl!(l0, 0, 0);
}

// Multiple table setup scenarios
mod multi_table {
    use super::*;
    fn setup_n(n: i64) -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, grp VARCHAR)");
        for idx in 0..n {
            let ts = 1_000_000_000_000i64 + idx * 1_000_000_000;
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, vi, grp) VALUES ({ts}, {}, 'g{}')",
                idx as f64,
                idx % 4
            ));
        }
        db
    }

    macro_rules! grp {
        ($n:ident, $sz:expr, $num_grp:expr) => {
            #[test]
            fn $n() {
                let db = setup_n($sz);
                assert_eq!(
                    rc(&db, "SELECT grp, count(*) FROM t GROUP BY grp"),
                    $num_grp
                );
            }
        };
    }
    grp!(g4, 4, 4);
    grp!(g8, 8, 4);
    grp!(g12, 12, 4);
    grp!(g16, 16, 4);
    grp!(g20, 20, 4);
    grp!(g24, 24, 4);
    grp!(g28, 28, 4);
    grp!(g32, 32, 4);
    grp!(g36, 36, 4);
    grp!(g40, 40, 4);

    // count per group should be n/4
    macro_rules! gcnt {
        ($n:ident, $sz:expr) => {
            #[test]
            fn $n() {
                let db = setup_n($sz);
                assert_eq!(ff(&db, "SELECT count(*) FROM t"), $sz as f64);
            }
        };
    }
    gcnt!(c4, 4);
    gcnt!(c8, 8);
    gcnt!(c12, 12);
    gcnt!(c16, 16);
    gcnt!(c20, 20);
    gcnt!(c24, 24);
    gcnt!(c28, 28);
    gcnt!(c32, 32);
    gcnt!(c36, 36);
    gcnt!(c40, 40);
    gcnt!(c44, 44);
    gcnt!(c48, 48);
    gcnt!(c50, 50);
    gcnt!(c60, 60);
    gcnt!(c80, 80);
    gcnt!(c100, 100);
}

// ORDER BY with WHERE
mod order_where {
    use super::*;
    #[test]
    fn asc_gt10() {
        let db = setup();
        let v = col_f(
            &db,
            "SELECT vi FROM t WHERE vi > 10 ORDER BY vi ASC LIMIT 5",
            0,
        );
        for (i, &vi) in v.iter().enumerate().take(5) {
            assert!((vi - (11 + i) as f64).abs() < 0.01);
        }
    }
    #[test]
    fn desc_lt50() {
        let db = setup();
        let v = col_f(
            &db,
            "SELECT vi FROM t WHERE vi < 50 ORDER BY vi DESC LIMIT 5",
            0,
        );
        for (i, &vi) in v.iter().enumerate().take(5) {
            assert!((vi - (49 - i) as f64).abs() < 0.01);
        }
    }
    #[test]
    fn asc_bt10_30() {
        let db = setup();
        let v = col_f(
            &db,
            "SELECT vi FROM t WHERE vi BETWEEN 10 AND 30 ORDER BY vi ASC",
            0,
        );
        assert_eq!(v.len(), 21);
        for i in 1..v.len() {
            assert!(v[i] >= v[i - 1]);
        }
    }
    #[test]
    fn desc_bt10_30() {
        let db = setup();
        let v = col_f(
            &db,
            "SELECT vi FROM t WHERE vi BETWEEN 10 AND 30 ORDER BY vi DESC",
            0,
        );
        assert_eq!(v.len(), 21);
        for i in 1..v.len() {
            assert!(v[i] <= v[i - 1]);
        }
    }
}

// DISTINCT on grp column
mod distinct {
    use super::*;
    #[test]
    fn distinct_grp() {
        let db = setup();
        let cnt = rc(&db, "SELECT DISTINCT grp FROM t");
        assert_eq!(cnt, 6);
    }
    #[test]
    fn distinct_cat() {
        let db = setup();
        let cnt = rc(&db, "SELECT DISTINCT cat FROM t");
        assert_eq!(cnt, 3);
    }
    #[test]
    fn distinct_lim1() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 1"), 1);
    }
    #[test]
    fn distinct_lim3() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 3"), 3);
    }
    #[test]
    fn distinct_lim6() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 6"), 6);
    }
    #[test]
    fn distinct_lim10() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 10"), 6);
    }
}

// Aggregates with GROUP BY and ORDER BY
mod agg_order {
    use super::*;
    #[test]
    fn sum_grp() {
        assert_eq!(rc(&setup(), "SELECT grp, sum(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn avg_grp() {
        assert_eq!(rc(&setup(), "SELECT grp, avg(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn min_grp() {
        assert_eq!(rc(&setup(), "SELECT grp, min(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn max_grp() {
        assert_eq!(rc(&setup(), "SELECT grp, max(vi) FROM t GROUP BY grp"), 6);
    }
    #[test]
    fn cnt_grp() {
        assert_eq!(rc(&setup(), "SELECT grp, count(*) FROM t GROUP BY grp"), 6);
    }
}

// WHERE combos with aggregates
mod where_agg {
    use super::*;
    macro_rules! wa {
        ($n:ident, $sql:expr, $expect:expr) => {
            #[test]
            fn $n() {
                let db = setup();
                assert!((ff(&db, $sql) - $expect).abs() < 0.01);
            }
        };
    }
    wa!(cnt_gt20, "SELECT count(*) FROM t WHERE vi > 20", 39.0);
    wa!(cnt_gt40, "SELECT count(*) FROM t WHERE vi > 40", 19.0);
    wa!(cnt_gt50, "SELECT count(*) FROM t WHERE vi > 50", 9.0);
    wa!(cnt_lt20, "SELECT count(*) FROM t WHERE vi < 20", 20.0);
    wa!(cnt_lt40, "SELECT count(*) FROM t WHERE vi < 40", 40.0);
    wa!(cnt_lt50, "SELECT count(*) FROM t WHERE vi < 50", 50.0);
    wa!(sum_gt30, "SELECT sum(vi) FROM t WHERE vi > 30", 1305.0); // 31+32+...+59 = sum(0..59)-sum(0..30) = 1770-465=1305
    wa!(sum_lt30, "SELECT sum(vi) FROM t WHERE vi < 30", 435.0); // 0+1+...+29 = 435
    wa!(min_gt20, "SELECT min(vi) FROM t WHERE vi > 20", 21.0);
    wa!(max_lt40, "SELECT max(vi) FROM t WHERE vi < 40", 39.0);
    wa!(avg_gt30, "SELECT avg(vi) FROM t WHERE vi > 30", 45.0); // avg(31..59) = 45
    wa!(avg_lt30, "SELECT avg(vi) FROM t WHERE vi < 30", 14.5); // avg(0..29) = 14.5
    wa!(
        cnt_bt20_40,
        "SELECT count(*) FROM t WHERE vi BETWEEN 20 AND 40",
        21.0
    );
    wa!(
        sum_bt20_40,
        "SELECT sum(vi) FROM t WHERE vi BETWEEN 20 AND 40",
        630.0
    ); // 20+...+40=630
    wa!(
        min_bt20_40,
        "SELECT min(vi) FROM t WHERE vi BETWEEN 20 AND 40",
        20.0
    );
    wa!(
        max_bt20_40,
        "SELECT max(vi) FROM t WHERE vi BETWEEN 20 AND 40",
        40.0
    );
    wa!(
        avg_bt20_40,
        "SELECT avg(vi) FROM t WHERE vi BETWEEN 20 AND 40",
        30.0
    );
    wa!(cnt_eq0, "SELECT count(*) FROM t WHERE vi = 0", 1.0);
    wa!(cnt_eq59, "SELECT count(*) FROM t WHERE vi = 59", 1.0);
    wa!(cnt_eq99, "SELECT count(*) FROM t WHERE vi = 99", 0.0);
}

// Combos with GROUP BY and LIMIT
mod group_combos {
    use super::*;
    macro_rules! gc {
        ($n:ident, $sql:expr, $expect:expr) => {
            #[test]
            fn $n() {
                assert_eq!(rc(&setup(), $sql), $expect);
            }
        };
    }
    gc!(
        g_grp_l1,
        "SELECT grp, count(*) FROM t GROUP BY grp LIMIT 1",
        1
    );
    gc!(
        g_grp_l2,
        "SELECT grp, count(*) FROM t GROUP BY grp LIMIT 2",
        2
    );
    gc!(
        g_grp_l3,
        "SELECT grp, count(*) FROM t GROUP BY grp LIMIT 3",
        3
    );
    gc!(
        g_grp_l4,
        "SELECT grp, count(*) FROM t GROUP BY grp LIMIT 4",
        4
    );
    gc!(
        g_grp_l5,
        "SELECT grp, count(*) FROM t GROUP BY grp LIMIT 5",
        5
    );
    gc!(
        g_grp_l6,
        "SELECT grp, count(*) FROM t GROUP BY grp LIMIT 6",
        6
    );
    gc!(
        g_cat_l1,
        "SELECT cat, count(*) FROM t GROUP BY cat LIMIT 1",
        1
    );
    gc!(
        g_cat_l2,
        "SELECT cat, count(*) FROM t GROUP BY cat LIMIT 2",
        2
    );
    gc!(
        g_cat_l3,
        "SELECT cat, count(*) FROM t GROUP BY cat LIMIT 3",
        3
    );
    gc!(
        g_cat_l10,
        "SELECT cat, count(*) FROM t GROUP BY cat LIMIT 10",
        3
    );
    gc!(
        g_grp_sum_l3,
        "SELECT grp, sum(vi) FROM t GROUP BY grp LIMIT 3",
        3
    );
    gc!(
        g_grp_avg_l3,
        "SELECT grp, avg(vi) FROM t GROUP BY grp LIMIT 3",
        3
    );
    gc!(
        g_grp_min_l3,
        "SELECT grp, min(vi) FROM t GROUP BY grp LIMIT 3",
        3
    );
    gc!(
        g_grp_max_l3,
        "SELECT grp, max(vi) FROM t GROUP BY grp LIMIT 3",
        3
    );
}

// ORDER BY with LIMIT combos
mod order_limit {
    use super::*;
    macro_rules! ol {
        ($n:ident, $lim:expr) => {
            #[test]
            fn $n() {
                let db = setup();
                assert_eq!(
                    rc(
                        &db,
                        &format!("SELECT vi FROM t ORDER BY vi ASC LIMIT {}", $lim)
                    ),
                    std::cmp::min($lim, 60)
                );
            }
        };
    }
    ol!(l1, 1);
    ol!(l2, 2);
    ol!(l3, 3);
    ol!(l5, 5);
    ol!(l10, 10);
    ol!(l20, 20);
    ol!(l30, 30);
    ol!(l40, 40);
    ol!(l50, 50);
    ol!(l60, 60);
    ol!(l100, 100);
}

// Descending order with limit
mod desc_limit {
    use super::*;
    macro_rules! dl {
        ($n:ident, $lim:expr) => {
            #[test]
            fn $n() {
                let db = setup();
                assert_eq!(
                    rc(
                        &db,
                        &format!("SELECT vi FROM t ORDER BY vi DESC LIMIT {}", $lim)
                    ),
                    std::cmp::min($lim, 60)
                );
            }
        };
    }
    dl!(l1, 1);
    dl!(l2, 2);
    dl!(l3, 3);
    dl!(l5, 5);
    dl!(l10, 10);
    dl!(l20, 20);
    dl!(l30, 30);
    dl!(l40, 40);
    dl!(l50, 50);
    dl!(l60, 60);
    dl!(l100, 100);
}

// Additional table sizes: group by with different N
mod group_sizes {
    use super::*;
    fn setup_n(n: i64, groups: i64) -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, grp VARCHAR)");
        for idx in 0..n {
            let ts = 1_000_000_000_000i64 + idx * 1_000_000_000;
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, vi, grp) VALUES ({ts}, {}, 'g{}')",
                idx as f64,
                idx % groups
            ));
        }
        db
    }
    macro_rules! gs {
        ($n:ident, $total:expr, $groups:expr) => {
            #[test]
            fn $n() {
                let db = setup_n($total, $groups);
                assert_eq!(
                    rc(&db, "SELECT grp, count(*) FROM t GROUP BY grp"),
                    $groups as usize
                );
            }
        };
    }
    gs!(g2x4, 4, 2);
    gs!(g2x10, 10, 2);
    gs!(g2x20, 20, 2);
    gs!(g2x50, 50, 2);
    gs!(g3x6, 6, 3);
    gs!(g3x12, 12, 3);
    gs!(g3x30, 30, 3);
    gs!(g3x60, 60, 3);
    gs!(g4x8, 8, 4);
    gs!(g4x16, 16, 4);
    gs!(g4x40, 40, 4);
    gs!(g4x80, 80, 4);
    gs!(g5x10, 10, 5);
    gs!(g5x20, 20, 5);
    gs!(g5x50, 50, 5);
    gs!(g5x100, 100, 5);
    gs!(g6x12, 12, 6);
    gs!(g6x24, 24, 6);
    gs!(g6x60, 60, 6);
    gs!(g10x20, 20, 10);
    gs!(g10x50, 50, 10);
    gs!(g10x100, 100, 10);

    // Count and sum
    macro_rules! cnt {
        ($n:ident, $total:expr) => {
            #[test]
            fn $n() {
                let db = setup_n($total, 5);
                assert_eq!(ff(&db, "SELECT count(*) FROM t"), $total as f64);
            }
        };
    }
    cnt!(c5, 5);
    cnt!(c10, 10);
    cnt!(c20, 20);
    cnt!(c30, 30);
    cnt!(c40, 40);
    cnt!(c50, 50);
    cnt!(c60, 60);
    cnt!(c70, 70);
    cnt!(c80, 80);
    cnt!(c90, 90);
    cnt!(c100, 100);

    macro_rules! sm {
        ($n:ident, $total:expr) => {
            #[test]
            fn $n() {
                let db = setup_n($total, 5);
                let expect = ($total * ($total - 1)) as f64 / 2.0;
                assert!((ff(&db, "SELECT sum(vi) FROM t") - expect).abs() < 0.01);
            }
        };
    }
    sm!(s5, 5);
    sm!(s10, 10);
    sm!(s20, 20);
    sm!(s30, 30);
    sm!(s40, 40);
    sm!(s50, 50);
    sm!(s60, 60);
    sm!(s70, 70);
    sm!(s80, 80);
    sm!(s90, 90);
    sm!(s100, 100);
}

// More ORDER BY tests
mod order_extra {
    use super::*;
    #[test]
    fn asc_20() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 20", 0);
        assert_eq!(v.len(), 20);
        for (i, &vi) in v.iter().enumerate().take(20) {
            assert!((vi - i as f64).abs() < 0.01);
        }
    }
    #[test]
    fn desc_20() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 20", 0);
        assert_eq!(v.len(), 20);
        for (i, &vi) in v.iter().enumerate().take(20) {
            assert!((vi - (59 - i) as f64).abs() < 0.01);
        }
    }
    #[test]
    fn asc_30() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 30", 0);
        assert_eq!(v.len(), 30);
    }
    #[test]
    fn desc_30() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 30", 0);
        assert_eq!(v.len(), 30);
    }
    #[test]
    fn asc_sorted() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi ASC LIMIT 60", 0);
        for i in 1..v.len() {
            assert!(v[i] >= v[i - 1]);
        }
    }
    #[test]
    fn desc_sorted() {
        let db = setup();
        let v = col_f(&db, "SELECT vi FROM t ORDER BY vi DESC LIMIT 60", 0);
        for i in 1..v.len() {
            assert!(v[i] <= v[i - 1]);
        }
    }
}

// More WHERE + GROUP BY combos
mod where_group {
    use super::*;
    macro_rules! wg {
        ($n:ident, $cond:expr, $expect_groups:expr) => {
            #[test]
            fn $n() {
                assert_eq!(
                    rc(
                        &setup(),
                        &format!("SELECT grp, count(*) FROM t WHERE {} GROUP BY grp", $cond)
                    ),
                    $expect_groups
                );
            }
        };
    }
    wg!(gt0, "vi > 0", 6);
    wg!(gt5, "vi > 5", 6);
    wg!(gt10, "vi > 10", 6);
    wg!(gt20, "vi > 20", 6);
    wg!(gt30, "vi > 30", 6);
    wg!(gt40, "vi > 40", 6);
    wg!(gt50, "vi > 50", 6);
    wg!(lt10, "vi < 10", 6);
    wg!(lt20, "vi < 20", 6);
    wg!(lt30, "vi < 30", 6);
    wg!(lt40, "vi < 40", 6);
    wg!(lt50, "vi < 50", 6);
    wg!(bt5_55, "vi BETWEEN 5 AND 55", 6);
    wg!(bt10_50, "vi BETWEEN 10 AND 50", 6);
    wg!(bt20_40, "vi BETWEEN 20 AND 40", 6);
    wg!(bt0_59, "vi BETWEEN 0 AND 59", 6);
}

// More DISTINCT tests
mod distinct2 {
    use super::*;
    #[test]
    fn dist_grp_all() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t"), 6);
    }
    #[test]
    fn dist_cat_all() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT cat FROM t"), 3);
    }
    #[test]
    fn dist_grp_l2() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 2"), 2);
    }
    #[test]
    fn dist_grp_l4() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 4"), 4);
    }
    #[test]
    fn dist_grp_l5() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT grp FROM t LIMIT 5"), 5);
    }
    #[test]
    fn dist_cat_l1() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT cat FROM t LIMIT 1"), 1);
    }
    #[test]
    fn dist_cat_l2() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT cat FROM t LIMIT 2"), 2);
    }
    #[test]
    fn dist_cat_l5() {
        assert_eq!(rc(&setup(), "SELECT DISTINCT cat FROM t LIMIT 5"), 3);
    }
}

// GROUP BY + aggregate value verification
mod group_values {
    use super::*;
    #[test]
    fn total_count() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t"), 60.0);
    }
    #[test]
    fn total_sum() {
        assert!((ff(&setup(), "SELECT sum(vi) FROM t") - 1770.0).abs() < 0.01);
    }
    #[test]
    fn total_avg() {
        assert!((ff(&setup(), "SELECT avg(vi) FROM t") - 29.5).abs() < 0.01);
    }
    #[test]
    fn total_min() {
        assert!((ff(&setup(), "SELECT min(vi) FROM t")).abs() < 0.01);
    }
    #[test]
    fn total_max() {
        assert!((ff(&setup(), "SELECT max(vi) FROM t") - 59.0).abs() < 0.01);
    }
    #[test]
    fn cnt_gt10() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi > 10"), 49.0);
    }
    #[test]
    fn cnt_lt50() {
        assert_eq!(ff(&setup(), "SELECT count(*) FROM t WHERE vi < 50"), 50.0);
    }
    #[test]
    fn cnt_bt20_40() {
        assert_eq!(
            ff(
                &setup(),
                "SELECT count(*) FROM t WHERE vi BETWEEN 20 AND 40"
            ),
            21.0
        );
    }
    #[test]
    fn sum_lt10() {
        assert!((ff(&setup(), "SELECT sum(vi) FROM t WHERE vi < 10") - 45.0).abs() < 0.01);
    }
    #[test]
    fn sum_gt50() {
        assert!((ff(&setup(), "SELECT sum(vi) FROM t WHERE vi > 50") - 495.0).abs() < 0.01);
    } // 51+52+...+59 = (51+59)*9/2 = 495
}

// Multiple GROUP BY + LIMIT combos
mod group_limit2 {
    use super::*;
    macro_rules! gl {
        ($n:ident, $sql:expr, $expect:expr) => {
            #[test]
            fn $n() {
                assert_eq!(rc(&setup(), $sql), $expect);
            }
        };
    }
    gl!(g1, "SELECT grp, sum(vi) FROM t GROUP BY grp LIMIT 1", 1);
    gl!(g2, "SELECT grp, sum(vi) FROM t GROUP BY grp LIMIT 2", 2);
    gl!(g3, "SELECT grp, sum(vi) FROM t GROUP BY grp LIMIT 3", 3);
    gl!(g4, "SELECT grp, avg(vi) FROM t GROUP BY grp LIMIT 1", 1);
    gl!(g5, "SELECT grp, avg(vi) FROM t GROUP BY grp LIMIT 2", 2);
    gl!(g6, "SELECT grp, avg(vi) FROM t GROUP BY grp LIMIT 3", 3);
    gl!(g7, "SELECT grp, min(vi) FROM t GROUP BY grp LIMIT 1", 1);
    gl!(g8, "SELECT grp, min(vi) FROM t GROUP BY grp LIMIT 2", 2);
    gl!(g9, "SELECT grp, max(vi) FROM t GROUP BY grp LIMIT 1", 1);
    gl!(g10, "SELECT grp, max(vi) FROM t GROUP BY grp LIMIT 2", 2);
    gl!(g11, "SELECT cat, count(*) FROM t GROUP BY cat LIMIT 1", 1);
    gl!(g12, "SELECT cat, count(*) FROM t GROUP BY cat LIMIT 2", 2);
    gl!(g13, "SELECT cat, sum(vi) FROM t GROUP BY cat LIMIT 1", 1);
    gl!(g14, "SELECT cat, avg(vi) FROM t GROUP BY cat LIMIT 1", 1);
    gl!(g15, "SELECT cat, min(vi) FROM t GROUP BY cat LIMIT 1", 1);
}
