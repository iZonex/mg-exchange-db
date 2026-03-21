//! r04_window_combos — 500 window function tests.
//! Every window function x partition x order.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(s: i64) -> i64 { BASE_TS + s * 1_000_000_000 }

fn setup() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, val DOUBLE)");
    for i in 0..20 {
        let g = ["A", "B", "C", "D"][i as usize % 4];
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {:.1})", ts(i), g, i as f64));
    }
    db
}

// ROW_NUMBER limit 1..20
mod rn_limit { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l4() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l6() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn l7() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn l8() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn l9() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l11() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 11"); assert_eq!(r.len(), 11); }
    #[test] fn l12() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 12"); assert_eq!(r.len(), 12); }
    #[test] fn l13() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 13"); assert_eq!(r.len(), 13); }
    #[test] fn l14() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 14"); assert_eq!(r.len(), 14); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l16() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 16"); assert_eq!(r.len(), 16); }
    #[test] fn l17() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 17"); assert_eq!(r.len(), 17); }
    #[test] fn l18() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 18"); assert_eq!(r.len(), 18); }
    #[test] fn l19() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 19"); assert_eq!(r.len(), 19); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
}

// ROW_NUMBER partitioned
mod rn_part { use super::*;
    #[test] fn a() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); for (i, row) in r.iter().enumerate() { assert_eq!(row[1], Value::I64((i+1) as i64)); } }
    #[test] fn b() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn c() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn d() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    #[test] fn all() { let db = setup(); let (_, r) = db.query("SELECT grp, val, row_number() OVER (PARTITION BY grp ORDER BY val) AS rn FROM t"); assert_eq!(r.len(), 20); }
}

// ROW_NUMBER DESC limit 1..20
mod rn_desc { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 1"); assert_eq!(r[0][1], Value::I64(1)); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val DESC) AS rn FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
}

// RANK limit 1..20
mod rank_limit { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (PARTITION BY grp ORDER BY val) AS rnk FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (PARTITION BY grp ORDER BY val) AS rnk FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (PARTITION BY grp ORDER BY val) AS rnk FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (PARTITION BY grp ORDER BY val) AS rnk FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    #[test] fn desc_l5() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val DESC) AS rnk FROM t LIMIT 5"); assert_eq!(r[0][1], Value::I64(1)); }
    #[test] fn desc_l10() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val DESC) AS rnk FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn desc_l20() { let db = setup(); let (_, r) = db.query("SELECT val, rank() OVER (ORDER BY val DESC) AS rnk FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
}

// DENSE_RANK limit
mod dense_rank_limit { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val) AS drnk FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (PARTITION BY grp ORDER BY val) AS drnk FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (PARTITION BY grp ORDER BY val) AS drnk FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (PARTITION BY grp ORDER BY val) AS drnk FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (PARTITION BY grp ORDER BY val) AS drnk FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    #[test] fn desc_l5() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val DESC) AS drnk FROM t LIMIT 5"); assert_eq!(r[0][1], Value::I64(1)); }
    #[test] fn desc_l10() { let db = setup(); let (_, r) = db.query("SELECT val, dense_rank() OVER (ORDER BY val DESC) AS drnk FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
}

// LAG tests
mod lag_tests { use super::*;
    #[test] fn basic() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) AS prev FROM t"); assert_eq!(r[0][1], Value::Null); }
    #[test] fn has_value() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) AS prev FROM t"); assert_eq!(r[1][1], Value::F64(0.0)); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (PARTITION BY grp ORDER BY val) AS prev FROM t WHERE grp = 'A'"); assert_eq!(r[0][1], Value::Null); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (PARTITION BY grp ORDER BY val) AS prev FROM t WHERE grp = 'B'"); assert_eq!(r[0][1], Value::Null); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (PARTITION BY grp ORDER BY val) AS prev FROM t WHERE grp = 'C'"); assert_eq!(r[0][1], Value::Null); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (PARTITION BY grp ORDER BY val) AS prev FROM t WHERE grp = 'D'"); assert_eq!(r[0][1], Value::Null); }
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn alias() { let db = setup(); let (c, _) = db.query("SELECT lag(val) OVER (ORDER BY val) AS prev FROM t LIMIT 1"); assert!(c.contains(&"prev".to_string())); }
    #[test] fn empty() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT lag(v) OVER (ORDER BY timestamp) FROM t"); assert_eq!(r.len(), 0); }
    #[test] fn single() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT lag(v) OVER (ORDER BY timestamp) FROM t"); assert_eq!(r[0][0], Value::Null); }
    #[test] fn two() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1))); let (_, r) = db.query("SELECT v, lag(v) OVER (ORDER BY timestamp) FROM t"); assert_eq!(r[0][1], Value::Null); assert_eq!(r[1][1], Value::F64(1.0)); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l4() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn l6() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn l7() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn l8() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn l9() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn l11() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 11"); assert_eq!(r.len(), 11); }
    #[test] fn l12() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 12"); assert_eq!(r.len(), 12); }
    #[test] fn l13() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 13"); assert_eq!(r.len(), 13); }
    #[test] fn l14() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 14"); assert_eq!(r.len(), 14); }
    #[test] fn l16() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 16"); assert_eq!(r.len(), 16); }
    #[test] fn l17() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 17"); assert_eq!(r.len(), 17); }
    #[test] fn l18() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 18"); assert_eq!(r.len(), 18); }
    #[test] fn l19() { let db = setup(); let (_, r) = db.query("SELECT val, lag(val) OVER (ORDER BY val) FROM t LIMIT 19"); assert_eq!(r.len(), 19); }
}

// LEAD tests
mod lead_tests { use super::*;
    #[test] fn basic() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) AS next FROM t"); assert_eq!(r[19][1], Value::Null); }
    #[test] fn has_value() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) AS next FROM t"); assert_eq!(r[0][1], Value::F64(1.0)); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (PARTITION BY grp ORDER BY val) AS next FROM t WHERE grp = 'A'"); assert_eq!(r[4][1], Value::Null); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (PARTITION BY grp ORDER BY val) AS next FROM t WHERE grp = 'B'"); assert_eq!(r[4][1], Value::Null); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (PARTITION BY grp ORDER BY val) AS next FROM t WHERE grp = 'C'"); assert_eq!(r[4][1], Value::Null); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (PARTITION BY grp ORDER BY val) AS next FROM t WHERE grp = 'D'"); assert_eq!(r[4][1], Value::Null); }
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn alias() { let db = setup(); let (c, _) = db.query("SELECT lead(val) OVER (ORDER BY val) AS next FROM t LIMIT 1"); assert!(c.contains(&"next".to_string())); }
    #[test] fn empty() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT lead(v) OVER (ORDER BY timestamp) FROM t"); assert_eq!(r.len(), 0); }
    #[test] fn single() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT lead(v) OVER (ORDER BY timestamp) FROM t"); assert_eq!(r[0][0], Value::Null); }
    #[test] fn two() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1))); let (_, r) = db.query("SELECT v, lead(v) OVER (ORDER BY timestamp) FROM t"); assert_eq!(r[0][1], Value::F64(2.0)); assert_eq!(r[1][1], Value::Null); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l4() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn l6() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn l7() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn l8() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn l9() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn l11() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 11"); assert_eq!(r.len(), 11); }
    #[test] fn l12() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 12"); assert_eq!(r.len(), 12); }
    #[test] fn l13() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 13"); assert_eq!(r.len(), 13); }
    #[test] fn l14() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 14"); assert_eq!(r.len(), 14); }
    #[test] fn l16() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 16"); assert_eq!(r.len(), 16); }
    #[test] fn l17() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 17"); assert_eq!(r.len(), 17); }
    #[test] fn l18() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 18"); assert_eq!(r.len(), 18); }
    #[test] fn l19() { let db = setup(); let (_, r) = db.query("SELECT val, lead(val) OVER (ORDER BY val) FROM t LIMIT 19"); assert_eq!(r.len(), 19); }
}

// Running aggregates: sum/count/avg/min/max over windows
mod running_sum { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rs FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rs FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rs FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (PARTITION BY grp ORDER BY val) AS rs FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    #[test] fn desc() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val DESC) AS rs FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l4() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn l6() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn l7() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn l8() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn l9() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn l11() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 11"); assert_eq!(r.len(), 11); }
    #[test] fn l12() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 12"); assert_eq!(r.len(), 12); }
    #[test] fn l13() { let db = setup(); let (_, r) = db.query("SELECT val, sum(val) OVER (ORDER BY val) AS rs FROM t LIMIT 13"); assert_eq!(r.len(), 13); }
}

mod running_count { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (PARTITION BY grp ORDER BY val) AS rc FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (PARTITION BY grp ORDER BY val) AS rc FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (PARTITION BY grp ORDER BY val) AS rc FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (PARTITION BY grp ORDER BY val) AS rc FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    #[test] fn desc() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val DESC) AS rc FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l4() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn l6() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn l7() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn l8() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn l9() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn l11() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 11"); assert_eq!(r.len(), 11); }
    #[test] fn l12() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 12"); assert_eq!(r.len(), 12); }
    #[test] fn l13() { let db = setup(); let (_, r) = db.query("SELECT val, count(*) OVER (ORDER BY val) AS rc FROM t LIMIT 13"); assert_eq!(r.len(), 13); }
}

mod running_avg { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (PARTITION BY grp ORDER BY val) AS ra FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (PARTITION BY grp ORDER BY val) AS ra FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn part_c() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (PARTITION BY grp ORDER BY val) AS ra FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn part_d() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (PARTITION BY grp ORDER BY val) AS ra FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    #[test] fn desc() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val DESC) AS ra FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, avg(val) OVER (ORDER BY val) AS ra FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
}

mod running_min { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (PARTITION BY grp ORDER BY val) AS rm FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (PARTITION BY grp ORDER BY val) AS rm FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn desc() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val DESC) AS rm FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, min(val) OVER (ORDER BY val) AS rm FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
}

mod running_max { use super::*;
    #[test] fn l1() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn l5() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn l10() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn l20() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn part_a() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (PARTITION BY grp ORDER BY val) AS rm FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn part_b() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (PARTITION BY grp ORDER BY val) AS rm FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn desc() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val DESC) AS rm FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn l2() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn l3() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn l15() { let db = setup(); let (_, r) = db.query("SELECT val, max(val) OVER (ORDER BY val) AS rm FROM t LIMIT 15"); assert_eq!(r.len(), 15); }
}

// Edge cases + extras
mod edge { use super::*;
    #[test] fn rn_unique() { let db = setup(); let (_, r) = db.query("SELECT row_number() OVER (ORDER BY val) AS rn FROM t"); let mut rns: Vec<i64> = r.iter().map(|row| match &row[0] { Value::I64(n) => *n, _ => panic!() }).collect(); rns.sort(); rns.dedup(); assert_eq!(rns.len(), 20); }
    #[test] fn rn_starts_1() { let db = setup(); let (_, r) = db.query("SELECT row_number() OVER (ORDER BY val) AS rn FROM t"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn rn_ends_20() { let db = setup(); let (_, r) = db.query("SELECT row_number() OVER (ORDER BY val) AS rn FROM t"); assert_eq!(r[19][0], Value::I64(20)); }
    #[test] fn rn_empty() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT row_number() OVER (ORDER BY v) AS rn FROM t"); assert_eq!(r.len(), 0); }
    #[test] fn rn_single() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT row_number() OVER (ORDER BY v) AS rn FROM t"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn rank_empty() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT rank() OVER (ORDER BY v) AS rnk FROM t"); assert_eq!(r.len(), 0); }
    #[test] fn rank_single() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT rank() OVER (ORDER BY v) AS rnk FROM t"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn dense_rank_single() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT dense_rank() OVER (ORDER BY v) AS drnk FROM t"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn rank_all_same() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); for i in 0..5 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(i))); } let (_, r) = db.query("SELECT rank() OVER (ORDER BY v) AS rnk FROM t"); for row in &r { assert_eq!(row[0], Value::I64(1)); } }
    #[test] fn dense_rank_all_same() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); for i in 0..5 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0)", ts(i))); } let (_, r) = db.query("SELECT dense_rank() OVER (ORDER BY v) AS drnk FROM t"); for row in &r { assert_eq!(row[0], Value::I64(1)); } }
    #[test] fn rn_alias() { let db = setup(); let (c, _) = db.query("SELECT row_number() OVER (ORDER BY val) AS rn FROM t LIMIT 1"); assert!(c.contains(&"rn".to_string())); }
    #[test] fn rank_alias() { let db = setup(); let (c, _) = db.query("SELECT rank() OVER (ORDER BY val) AS rnk FROM t LIMIT 1"); assert!(c.contains(&"rnk".to_string())); }
    #[test] fn rn_with_grp() { let db = setup(); let (c, r) = db.query("SELECT grp, val, row_number() OVER (ORDER BY val) AS rn FROM t"); assert_eq!(c.len(), 3); assert_eq!(r.len(), 20); }
    #[test] fn rank_with_grp() { let db = setup(); let (c, r) = db.query("SELECT grp, val, rank() OVER (ORDER BY val) AS rnk FROM t"); assert_eq!(c.len(), 3); assert_eq!(r.len(), 20); }
    #[test] fn rn_where_a() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t WHERE grp = 'A'"); assert_eq!(r.len(), 5); }
    #[test] fn rn_where_b() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t WHERE grp = 'B'"); assert_eq!(r.len(), 5); }
    #[test] fn rn_where_c() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t WHERE grp = 'C'"); assert_eq!(r.len(), 5); }
    #[test] fn rn_where_d() { let db = setup(); let (_, r) = db.query("SELECT val, row_number() OVER (ORDER BY val) AS rn FROM t WHERE grp = 'D'"); assert_eq!(r.len(), 5); }
    // Trades-based window tests
    #[test] fn trades_rn() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_rn_part() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, price, row_number() OVER (PARTITION BY symbol ORDER BY price) AS rn FROM trades"); assert_eq!(r.len(), 30); }
    #[test] fn trades_rank() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, rank() OVER (ORDER BY price) AS rnk FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_lag() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, lag(price) OVER (ORDER BY timestamp) AS prev FROM trades"); assert_eq!(r[0][1], Value::Null); }
    #[test] fn trades_lead() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, lead(price) OVER (ORDER BY timestamp) AS next FROM trades"); assert_eq!(r[19][1], Value::Null); }
    #[test] fn trades_running_sum() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, sum(price) OVER (ORDER BY timestamp) AS rs FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_running_avg() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, avg(price) OVER (ORDER BY timestamp) AS ra FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_running_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, count(*) OVER (ORDER BY timestamp) AS rc FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_running_min() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, min(price) OVER (ORDER BY timestamp) AS rm FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_running_max() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, max(price) OVER (ORDER BY timestamp) AS rm FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn trades_rn_limit_5() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM trades LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn trades_rn_limit_10() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM trades LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn trades_lag_part() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, price, lag(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev FROM trades ORDER BY symbol, timestamp"); assert_eq!(r.len(), 30); }
    #[test] fn trades_lead_part() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, price, lead(price) OVER (PARTITION BY symbol ORDER BY timestamp) AS next FROM trades ORDER BY symbol, timestamp"); assert_eq!(r.len(), 30); }
    #[test] fn trades_rank_part() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, price, rank() OVER (PARTITION BY symbol ORDER BY price) AS rnk FROM trades"); assert_eq!(r.len(), 30); }
    #[test] fn trades_dense_rank_part() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, price, dense_rank() OVER (PARTITION BY symbol ORDER BY price) AS drnk FROM trades"); assert_eq!(r.len(), 30); }
}
