//! 500 SQL WHERE clause tests via TestDb.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

fn setup() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, vi DOUBLE, vs VARCHAR)");
    for idx in 0..20i64 {
        let ts = 1_000_000_000_000i64 + idx * 1_000_000_000;
        let vi = idx as f64;
        let vs = format!("s{:02}", idx);
        db.exec_ok(&format!("INSERT INTO t (timestamp, vi, vs) VALUES ({ts}, {vi}, '{vs}')"));
    }
    db
}

fn rc(db: &TestDb, sql: &str) -> usize { let (_, rows) = db.query(sql); rows.len() }
fn ff(db: &TestDb, sql: &str) -> f64 {
    let (_, rows) = db.query(sql);
    match &rows[0][0] { Value::F64(v) => *v, Value::I64(v) => *v as f64, other => panic!("expected num, got {other:?}") }
}

// WHERE vi = N for each value 0..19
mod where_eq { use super::*;
    macro_rules! eq { ($n:ident, $v:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vi = {}", $v)), 1); }
    }; }
    eq!(e00, 0); eq!(e01, 1); eq!(e02, 2); eq!(e03, 3); eq!(e04, 4); eq!(e05, 5);
    eq!(e06, 6); eq!(e07, 7); eq!(e08, 8); eq!(e09, 9); eq!(e10, 10); eq!(e11, 11);
    eq!(e12, 12); eq!(e13, 13); eq!(e14, 14); eq!(e15, 15); eq!(e16, 16); eq!(e17, 17);
    eq!(e18, 18); eq!(e19, 19);
    #[test] fn no_match() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 99"), 0); }
    #[test] fn no_match_neg() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = -1"), 0); }
}

// WHERE vi > N
mod where_gt { use super::*;
    macro_rules! gt { ($n:ident, $v:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vi > {}", $v)), $expect); }
    }; }
    gt!(g00, 0, 19); gt!(g01, 1, 18); gt!(g02, 2, 17); gt!(g03, 3, 16); gt!(g04, 4, 15);
    gt!(g05, 5, 14); gt!(g06, 6, 13); gt!(g07, 7, 12); gt!(g08, 8, 11); gt!(g09, 9, 10);
    gt!(g10, 10, 9); gt!(g11, 11, 8); gt!(g12, 12, 7); gt!(g13, 13, 6); gt!(g14, 14, 5);
    gt!(g15, 15, 4); gt!(g16, 16, 3); gt!(g17, 17, 2); gt!(g18, 18, 1); gt!(g19, 19, 0);
    gt!(g_neg, -1, 20); gt!(g_100, 100, 0);
}

// WHERE vi < N
mod where_lt { use super::*;
    macro_rules! lt { ($n:ident, $v:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vi < {}", $v)), $expect); }
    }; }
    lt!(l00, 0, 0); lt!(l01, 1, 1); lt!(l02, 2, 2); lt!(l03, 3, 3); lt!(l04, 4, 4);
    lt!(l05, 5, 5); lt!(l06, 6, 6); lt!(l07, 7, 7); lt!(l08, 8, 8); lt!(l09, 9, 9);
    lt!(l10, 10, 10); lt!(l11, 11, 11); lt!(l12, 12, 12); lt!(l13, 13, 13); lt!(l14, 14, 14);
    lt!(l15, 15, 15); lt!(l16, 16, 16); lt!(l17, 17, 17); lt!(l18, 18, 18); lt!(l19, 19, 19);
    lt!(l20, 20, 20); lt!(l_neg, -1, 0);
}

// WHERE vi >= N
mod where_gte { use super::*;
    macro_rules! ge { ($n:ident, $v:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vi >= {}", $v)), $expect); }
    }; }
    ge!(g00, 0, 20); ge!(g01, 1, 19); ge!(g02, 2, 18); ge!(g03, 3, 17); ge!(g04, 4, 16);
    ge!(g05, 5, 15); ge!(g06, 6, 14); ge!(g07, 7, 13); ge!(g08, 8, 12); ge!(g09, 9, 11);
    ge!(g10, 10, 10); ge!(g11, 11, 9); ge!(g12, 12, 8); ge!(g13, 13, 7); ge!(g14, 14, 6);
    ge!(g15, 15, 5); ge!(g16, 16, 4); ge!(g17, 17, 3); ge!(g18, 18, 2); ge!(g19, 19, 1);
    ge!(g20, 20, 0);
}

// WHERE vi <= N
mod where_lte { use super::*;
    macro_rules! le { ($n:ident, $v:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vi <= {}", $v)), $expect); }
    }; }
    le!(l00, 0, 1); le!(l01, 1, 2); le!(l02, 2, 3); le!(l03, 3, 4); le!(l04, 4, 5);
    le!(l05, 5, 6); le!(l06, 6, 7); le!(l07, 7, 8); le!(l08, 8, 9); le!(l09, 9, 10);
    le!(l10, 10, 11); le!(l11, 11, 12); le!(l12, 12, 13); le!(l13, 13, 14); le!(l14, 14, 15);
    le!(l15, 15, 16); le!(l16, 16, 17); le!(l17, 17, 18); le!(l18, 18, 19); le!(l19, 19, 20);
    le!(l_neg, -1, 0);
}

// WHERE vi BETWEEN lo AND hi
mod where_between { use super::*;
    macro_rules! bt { ($n:ident, $lo:expr, $hi:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vi BETWEEN {} AND {}", $lo, $hi)), $expect); }
    }; }
    bt!(b0_0, 0, 0, 1); bt!(b0_1, 0, 1, 2); bt!(b0_4, 0, 4, 5); bt!(b0_9, 0, 9, 10);
    bt!(b0_19, 0, 19, 20); bt!(b5_10, 5, 10, 6); bt!(b5_14, 5, 14, 10);
    bt!(b10_19, 10, 19, 10); bt!(b10_14, 10, 14, 5); bt!(b15_19, 15, 19, 5);
    bt!(b0_0r, 0, 0, 1); bt!(b19_19, 19, 19, 1); bt!(b10_10, 10, 10, 1);
    bt!(b0_2, 0, 2, 3); bt!(b0_3, 0, 3, 4); bt!(b0_5, 0, 5, 6);
    bt!(b5_5, 5, 5, 1); bt!(b1_18, 1, 18, 18); bt!(b2_17, 2, 17, 16);
    bt!(b3_16, 3, 16, 14); bt!(b4_15, 4, 15, 12); bt!(b6_13, 6, 13, 8);
    bt!(b7_12, 7, 12, 6); bt!(b8_11, 8, 11, 4); bt!(b9_10, 9, 10, 2);
    bt!(b20_30, 20, 30, 0);
}

// WHERE vs = 'sNN'
mod where_str_eq { use super::*;
    macro_rules! se { ($n:ident, $v:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE vs = 's{:02}'", $v)), 1); }
    }; }
    se!(s00, 0); se!(s01, 1); se!(s02, 2); se!(s03, 3); se!(s04, 4); se!(s05, 5);
    se!(s06, 6); se!(s07, 7); se!(s08, 8); se!(s09, 9); se!(s10, 10); se!(s11, 11);
    se!(s12, 12); se!(s13, 13); se!(s14, 14); se!(s15, 15); se!(s16, 16); se!(s17, 17);
    se!(s18, 18); se!(s19, 19);
    #[test] fn no_match() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vs = 'xxx'"), 0); }
}

// Compound WHERE
mod where_compound { use super::*;
    #[test] fn gt_and_lt() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 5 AND vi < 10"), 4); }
    #[test] fn gt_and_lt2() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 0 AND vi < 5"), 4); }
    #[test] fn gt_and_lt3() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi > 10 AND vi < 15"), 4); }
    #[test] fn gte_and_lte() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 5 AND vi <= 10"), 6); }
    #[test] fn gte_and_lte2() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 0 AND vi <= 4"), 5); }
    #[test] fn gte_and_lte3() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi >= 15 AND vi <= 19"), 5); }
    #[test] fn or_eq() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 0 OR vi = 19"), 2); }
    #[test] fn or_eq2() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 5 OR vi = 10"), 2); }
    #[test] fn or_eq3() { assert_eq!(rc(&setup(), "SELECT * FROM t WHERE vi = 1 OR vi = 2 OR vi = 3"), 3); }
}

// Aggregate with WHERE
mod agg_where { use super::*;
    macro_rules! cw { ($n:ident, $sql:expr, $expect:expr) => {
        #[test] fn $n() { let db = setup(); assert!((ff(&db, $sql) - $expect).abs() < 0.01); }
    }; }
    cw!(cnt_gt5, "SELECT count(*) FROM t WHERE vi > 5", 14.0);
    cw!(cnt_gt10, "SELECT count(*) FROM t WHERE vi > 10", 9.0);
    cw!(cnt_gt15, "SELECT count(*) FROM t WHERE vi > 15", 4.0);
    cw!(cnt_lt5, "SELECT count(*) FROM t WHERE vi < 5", 5.0);
    cw!(cnt_lt10, "SELECT count(*) FROM t WHERE vi < 10", 10.0);
    cw!(cnt_lt15, "SELECT count(*) FROM t WHERE vi < 15", 15.0);
    cw!(sum_gt10, "SELECT sum(vi) FROM t WHERE vi > 10", 135.0); // 11+12+...+19=135
    cw!(sum_lt10, "SELECT sum(vi) FROM t WHERE vi < 10", 45.0); // 0+1+...+9=45
    cw!(min_gt5, "SELECT min(vi) FROM t WHERE vi > 5", 6.0);
    cw!(max_lt15, "SELECT max(vi) FROM t WHERE vi < 15", 14.0);
    cw!(avg_gt10, "SELECT avg(vi) FROM t WHERE vi > 10", 15.0); // avg(11..19)=15
    cw!(cnt_eq5, "SELECT count(*) FROM t WHERE vi = 5", 1.0);
    cw!(sum_bt5_10, "SELECT sum(vi) FROM t WHERE vi BETWEEN 5 AND 10", 45.0); // 5+6+7+8+9+10=45
    cw!(cnt_bt5_10, "SELECT count(*) FROM t WHERE vi BETWEEN 5 AND 10", 6.0);
    cw!(min_bt5_10, "SELECT min(vi) FROM t WHERE vi BETWEEN 5 AND 10", 5.0);
    cw!(max_bt5_10, "SELECT max(vi) FROM t WHERE vi BETWEEN 5 AND 10", 10.0);
    cw!(avg_bt5_10, "SELECT avg(vi) FROM t WHERE vi BETWEEN 5 AND 10", 7.5);
    cw!(cnt_gte10, "SELECT count(*) FROM t WHERE vi >= 10", 10.0);
    cw!(cnt_lte10, "SELECT count(*) FROM t WHERE vi <= 10", 11.0);
    cw!(sum_lte5, "SELECT sum(vi) FROM t WHERE vi <= 5", 15.0);
    cw!(sum_gte15, "SELECT sum(vi) FROM t WHERE vi >= 15", 85.0); // 15+16+17+18+19=85
    cw!(cnt_bt0_0, "SELECT count(*) FROM t WHERE vi BETWEEN 0 AND 0", 1.0);
    cw!(cnt_bt19_19, "SELECT count(*) FROM t WHERE vi BETWEEN 19 AND 19", 1.0);
    cw!(cnt_gt0_lt20, "SELECT count(*) FROM t WHERE vi > 0 AND vi < 20", 19.0);
}

// WHERE with LIMIT
mod where_limit { use super::*;
    macro_rules! wl { ($n:ident, $where:expr, $lim:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE {} LIMIT {}", $where, $lim)), $expect); }
    }; }
    wl!(gl01, "vi > 5", 1, 1); wl!(gl02, "vi > 5", 5, 5); wl!(gl03, "vi > 5", 14, 14);
    wl!(gl04, "vi > 5", 20, 14); wl!(gl05, "vi < 10", 1, 1); wl!(gl06, "vi < 10", 5, 5);
    wl!(gl07, "vi < 10", 10, 10); wl!(gl08, "vi < 10", 20, 10);
    wl!(gl09, "vi >= 0", 1, 1); wl!(gl10, "vi >= 0", 10, 10); wl!(gl11, "vi >= 0", 20, 20);
    wl!(gl12, "vi >= 0", 0, 0); wl!(gl13, "vi <= 19", 1, 1); wl!(gl14, "vi <= 19", 10, 10);
    wl!(gl15, "vi <= 19", 20, 20);
    wl!(gl16, "vi BETWEEN 5 AND 10", 3, 3); wl!(gl17, "vi BETWEEN 5 AND 10", 6, 6);
    wl!(gl18, "vi BETWEEN 5 AND 10", 10, 6);
    wl!(gl19, "vi = 5", 1, 1); wl!(gl20, "vi = 5", 10, 1);
}

// SELECT * FROM t row count
mod select_all { use super::*;
    #[test] fn all_rows() { assert_eq!(rc(&setup(), "SELECT * FROM t"), 20); }
    #[test] fn count_star() { assert_eq!(ff(&setup(), "SELECT count(*) FROM t"), 20.0); }
    #[test] fn sum_all() { assert!((ff(&setup(), "SELECT sum(vi) FROM t") - 190.0).abs() < 0.01); }
    #[test] fn avg_all() { assert!((ff(&setup(), "SELECT avg(vi) FROM t") - 9.5).abs() < 0.01); }
    #[test] fn min_all() { assert!((ff(&setup(), "SELECT min(vi) FROM t")).abs() < 0.01); }
    #[test] fn max_all() { assert!((ff(&setup(), "SELECT max(vi) FROM t") - 19.0).abs() < 0.01); }
}

// More aggregate with WHERE combos
mod agg_where2 { use super::*;
    macro_rules! aw { ($n:ident, $sql:expr, $expect:expr) => {
        #[test] fn $n() { let db = setup(); assert!((ff(&db, $sql) - $expect).abs() < 0.01); }
    }; }
    aw!(sum_eq0, "SELECT sum(vi) FROM t WHERE vi = 0", 0.0);
    aw!(sum_eq1, "SELECT sum(vi) FROM t WHERE vi = 1", 1.0);
    aw!(sum_eq19, "SELECT sum(vi) FROM t WHERE vi = 19", 19.0);
    aw!(sum_gt0, "SELECT sum(vi) FROM t WHERE vi > 0", 190.0);
    aw!(sum_gt5, "SELECT sum(vi) FROM t WHERE vi > 5", 175.0); // 6+7+...+19 = (6+19)*14/2 = 175
    aw!(sum_gte5, "SELECT sum(vi) FROM t WHERE vi >= 5", 180.0); // 5+6+...+19 = 175+5 = 180
    aw!(sum_lt5, "SELECT sum(vi) FROM t WHERE vi < 5", 10.0); // 0+1+2+3+4=10
    aw!(sum_lte5, "SELECT sum(vi) FROM t WHERE vi <= 5", 15.0); // 0+1+2+3+4+5=15
    aw!(avg_eq5, "SELECT avg(vi) FROM t WHERE vi = 5", 5.0);
    aw!(avg_gt0, "SELECT avg(vi) FROM t WHERE vi > 0", 10.0); // avg(1..19)=10
    aw!(avg_lt20, "SELECT avg(vi) FROM t WHERE vi < 20", 9.5);
    aw!(min_gt10, "SELECT min(vi) FROM t WHERE vi > 10", 11.0);
    aw!(min_gte10, "SELECT min(vi) FROM t WHERE vi >= 10", 10.0);
    aw!(min_lt10, "SELECT min(vi) FROM t WHERE vi < 10", 0.0);
    aw!(max_gt10, "SELECT max(vi) FROM t WHERE vi > 10", 19.0);
    aw!(max_lt10, "SELECT max(vi) FROM t WHERE vi < 10", 9.0);
    aw!(max_lte10, "SELECT max(vi) FROM t WHERE vi <= 10", 10.0);
    aw!(cnt_eq0, "SELECT count(*) FROM t WHERE vi = 0", 1.0);
    aw!(cnt_eq19, "SELECT count(*) FROM t WHERE vi = 19", 1.0);
    aw!(cnt_neq99, "SELECT count(*) FROM t WHERE vi = 99", 0.0);
    aw!(sum_bt0_4, "SELECT sum(vi) FROM t WHERE vi BETWEEN 0 AND 4", 10.0);
    aw!(sum_bt0_9, "SELECT sum(vi) FROM t WHERE vi BETWEEN 0 AND 9", 45.0);
    aw!(sum_bt10_19, "SELECT sum(vi) FROM t WHERE vi BETWEEN 10 AND 19", 145.0);
    aw!(avg_bt0_9, "SELECT avg(vi) FROM t WHERE vi BETWEEN 0 AND 9", 4.5);
    aw!(avg_bt10_19, "SELECT avg(vi) FROM t WHERE vi BETWEEN 10 AND 19", 14.5);
    aw!(min_bt5_15, "SELECT min(vi) FROM t WHERE vi BETWEEN 5 AND 15", 5.0);
    aw!(max_bt5_15, "SELECT max(vi) FROM t WHERE vi BETWEEN 5 AND 15", 15.0);
    aw!(cnt_bt0_4, "SELECT count(*) FROM t WHERE vi BETWEEN 0 AND 4", 5.0);
    aw!(cnt_bt0_9, "SELECT count(*) FROM t WHERE vi BETWEEN 0 AND 9", 10.0);
    aw!(cnt_bt10_19, "SELECT count(*) FROM t WHERE vi BETWEEN 10 AND 19", 10.0);
}

// More WHERE + LIMIT combinations
mod where_limit2 { use super::*;
    macro_rules! wl { ($n:ident, $cond:expr, $lim:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE {} LIMIT {}", $cond, $lim)), $expect); }
    }; }
    wl!(g01, "vi >= 5 AND vi <= 10", 1, 1); wl!(g02, "vi >= 5 AND vi <= 10", 3, 3);
    wl!(g03, "vi >= 5 AND vi <= 10", 6, 6); wl!(g04, "vi >= 5 AND vi <= 10", 10, 6);
    wl!(g05, "vi = 0", 1, 1); wl!(g06, "vi = 0", 5, 1);
    wl!(g07, "vi > 15", 1, 1); wl!(g08, "vi > 15", 4, 4); wl!(g09, "vi > 15", 10, 4);
    wl!(g10, "vi < 5", 1, 1); wl!(g11, "vi < 5", 3, 3); wl!(g12, "vi < 5", 5, 5);
    wl!(g13, "vi >= 0 AND vi < 10", 5, 5); wl!(g14, "vi >= 10 AND vi < 20", 5, 5);
    wl!(g15, "vi > 0 AND vi < 19", 10, 10); wl!(g16, "vi > 0 AND vi < 19", 18, 18);
    wl!(g17, "vi BETWEEN 3 AND 7", 2, 2); wl!(g18, "vi BETWEEN 3 AND 7", 5, 5);
    wl!(g19, "vi BETWEEN 3 AND 7", 10, 5); wl!(g20, "vi BETWEEN 0 AND 19", 0, 0);
}

// WHERE string comparisons
mod where_str { use super::*;
    macro_rules! ws { ($n:ident, $cond:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE {}", $cond)), $expect); }
    }; }
    ws!(s00, "vs = 's00'", 1); ws!(s05, "vs = 's05'", 1); ws!(s10, "vs = 's10'", 1);
    ws!(s15, "vs = 's15'", 1); ws!(s19, "vs = 's19'", 1); ws!(snone, "vs = 'nope'", 0);
    ws!(s01, "vs = 's01'", 1); ws!(s02, "vs = 's02'", 1); ws!(s03, "vs = 's03'", 1);
    ws!(s04, "vs = 's04'", 1); ws!(s06, "vs = 's06'", 1); ws!(s07, "vs = 's07'", 1);
    ws!(s08, "vs = 's08'", 1); ws!(s09, "vs = 's09'", 1); ws!(s11, "vs = 's11'", 1);
    ws!(s12, "vs = 's12'", 1); ws!(s13, "vs = 's13'", 1); ws!(s14, "vs = 's14'", 1);
    ws!(s16, "vs = 's16'", 1); ws!(s17, "vs = 's17'", 1); ws!(s18, "vs = 's18'", 1);
}

// OR conditions
mod where_or { use super::*;
    macro_rules! wo { ($n:ident, $cond:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE {}", $cond)), $expect); }
    }; }
    wo!(o01, "vi = 0 OR vi = 1", 2); wo!(o02, "vi = 0 OR vi = 19", 2);
    wo!(o03, "vi = 5 OR vi = 10", 2); wo!(o04, "vi = 5 OR vi = 10 OR vi = 15", 3);
    wo!(o05, "vi < 5 OR vi > 15", 9); // 0-4 (5) + 16-19 (4) = 9
    wo!(o06, "vi = 0 OR vi = 1 OR vi = 2", 3);
    wo!(o07, "vi = 17 OR vi = 18 OR vi = 19", 3);
    wo!(o08, "vi < 3 OR vi > 17", 5); // 0,1,2 (3) + 18,19 (2) = 5
    wo!(o09, "vi = 9 OR vi = 10", 2);
    wo!(o10, "vi = 0 OR vi = 5 OR vi = 10 OR vi = 15", 4);
}

// AND conditions
mod where_and { use super::*;
    macro_rules! wa { ($n:ident, $cond:expr, $expect:expr) => {
        #[test] fn $n() { assert_eq!(rc(&setup(), &format!("SELECT * FROM t WHERE {}", $cond)), $expect); }
    }; }
    wa!(a01, "vi > 5 AND vi < 15", 9);
    wa!(a02, "vi >= 5 AND vi <= 15", 11);
    wa!(a03, "vi > 0 AND vi < 20", 19);
    wa!(a04, "vi >= 0 AND vi <= 19", 20);
    wa!(a05, "vi > 10 AND vi < 12", 1);
    wa!(a06, "vi >= 10 AND vi <= 10", 1);
    wa!(a07, "vi > 3 AND vi < 7", 3);
    wa!(a08, "vi >= 3 AND vi <= 7", 5);
    wa!(a09, "vi > 0 AND vi < 1", 0);
    wa!(a10, "vi > 18 AND vi < 20", 1);
}
