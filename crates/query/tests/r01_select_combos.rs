//! r01_select_combos — 500 SELECT feature combination tests.
//!
//! Covers: count, WHERE =, WHERE >, GROUP BY, ORDER BY, LIMIT, DISTINCT,
//! SAMPLE BY, BETWEEN, IN, LIKE, CASE WHEN, arithmetic, CTEs, subqueries, UNION.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(s: i64) -> i64 { BASE_TS + s * 1_000_000_000 }

fn db_trades(n: u64) -> TestDb { TestDb::with_trades(n) }

fn db30() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, price DOUBLE, vol DOUBLE, cat VARCHAR)");
    for i in 0..30 {
        let sym = ["BTC", "ETH", "SOL"][(i as usize) % 3];
        let cat = ["A", "B"][(i as usize) % 2];
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, '{}', {:.1}, {:.1}, '{}')",
            ts(i), sym, 100.0 + i as f64, 10.0 + i as f64 * 0.5, cat
        ));
    }
    db
}

// ============================================================================
// 1. COUNT with varying trade sizes (50 tests)
// ============================================================================
mod count_trades {
    use super::*;
    #[test] fn count_1() { let db = db_trades(1); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn count_2() { let db = db_trades(2); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(2)); }
    #[test] fn count_3() { let db = db_trades(3); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(3)); }
    #[test] fn count_4() { let db = db_trades(4); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(4)); }
    #[test] fn count_5() { let db = db_trades(5); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(5)); }
    #[test] fn count_6() { let db = db_trades(6); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(6)); }
    #[test] fn count_7() { let db = db_trades(7); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(7)); }
    #[test] fn count_8() { let db = db_trades(8); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(8)); }
    #[test] fn count_9() { let db = db_trades(9); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(9)); }
    #[test] fn count_10() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn count_11() { let db = db_trades(11); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(11)); }
    #[test] fn count_12() { let db = db_trades(12); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(12)); }
    #[test] fn count_13() { let db = db_trades(13); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(13)); }
    #[test] fn count_14() { let db = db_trades(14); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(14)); }
    #[test] fn count_15() { let db = db_trades(15); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn count_16() { let db = db_trades(16); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(16)); }
    #[test] fn count_17() { let db = db_trades(17); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(17)); }
    #[test] fn count_18() { let db = db_trades(18); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(18)); }
    #[test] fn count_19() { let db = db_trades(19); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(19)); }
    #[test] fn count_20() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test] fn count_21() { let db = db_trades(21); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(21)); }
    #[test] fn count_22() { let db = db_trades(22); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(22)); }
    #[test] fn count_23() { let db = db_trades(23); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(23)); }
    #[test] fn count_24() { let db = db_trades(24); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(24)); }
    #[test] fn count_25() { let db = db_trades(25); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(25)); }
    #[test] fn count_26() { let db = db_trades(26); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(26)); }
    #[test] fn count_27() { let db = db_trades(27); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(27)); }
    #[test] fn count_28() { let db = db_trades(28); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(28)); }
    #[test] fn count_29() { let db = db_trades(29); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(29)); }
    #[test] fn count_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn count_31() { let db = db_trades(31); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(31)); }
    #[test] fn count_32() { let db = db_trades(32); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(32)); }
    #[test] fn count_33() { let db = db_trades(33); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(33)); }
    #[test] fn count_34() { let db = db_trades(34); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(34)); }
    #[test] fn count_35() { let db = db_trades(35); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(35)); }
    #[test] fn count_36() { let db = db_trades(36); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(36)); }
    #[test] fn count_37() { let db = db_trades(37); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(37)); }
    #[test] fn count_38() { let db = db_trades(38); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(38)); }
    #[test] fn count_39() { let db = db_trades(39); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(39)); }
    #[test] fn count_40() { let db = db_trades(40); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(40)); }
    #[test] fn count_41() { let db = db_trades(41); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(41)); }
    #[test] fn count_42() { let db = db_trades(42); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(42)); }
    #[test] fn count_43() { let db = db_trades(43); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(43)); }
    #[test] fn count_44() { let db = db_trades(44); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(44)); }
    #[test] fn count_45() { let db = db_trades(45); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(45)); }
    #[test] fn count_46() { let db = db_trades(46); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(46)); }
    #[test] fn count_47() { let db = db_trades(47); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(47)); }
    #[test] fn count_48() { let db = db_trades(48); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(48)); }
    #[test] fn count_49() { let db = db_trades(49); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(49)); }
    #[test] fn count_50() { let db = db_trades(50); let (_, r) = db.query("SELECT count(*) FROM trades"); assert_eq!(r[0][0], Value::I64(50)); }
}

// ============================================================================
// 2. WHERE = with symbols (30 tests)
// ============================================================================
mod where_eq {
    use super::*;
    // BTC/USD tests with 10 rows
    #[test] fn eq_btc_10() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn eq_eth_10() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn eq_sol_10() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn eq_btc_20() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 6); }
    #[test] fn eq_eth_20() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 6); }
    #[test] fn eq_sol_20() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 6); }
    #[test] fn eq_buy_10() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'buy'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert_eq!(c, 5); }
    #[test] fn eq_sell_10() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'sell'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert_eq!(c, 5); }
    #[test] fn eq_buy_20() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'buy'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert_eq!(c, 10); }
    #[test] fn eq_sell_20() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'sell'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert_eq!(c, 10); }
    #[test] fn eq_sym_btc_30() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn eq_sym_eth_30() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'ETH'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn eq_sym_sol_30() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'SOL'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn eq_cat_a() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE cat = 'A'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn eq_cat_b() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE cat = 'B'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn eq_sym_and_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' AND cat = 'A'"); assert_eq!(r[0][0], Value::I64(5)); }
    #[test] fn eq_sym_or_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' OR cat = 'A'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 15); }
    #[test] fn eq_btc_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn eq_eth_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn eq_sol_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn eq_btc_40() { let db = db_trades(40); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 13); }
    #[test] fn eq_eth_40() { let db = db_trades(40); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 13); }
    #[test] fn eq_btc_buy_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn eq_eth_sell_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD' AND side = 'sell'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn eq_nonexistent() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'DOGE/USD'"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn eq_btc_50() { let db = db_trades(50); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 16); }
    #[test] fn eq_buy_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'buy'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn eq_sell_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'sell'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn neq_btc_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol != 'BTC/USD'"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test] fn neq_sell_30() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side != 'sell'"); assert_eq!(r[0][0], Value::I64(15)); }
}

// ============================================================================
// 3. WHERE > thresholds (20 tests)
// ============================================================================
mod where_gt {
    use super::*;
    #[test] fn gt_100() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 100.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 29); }
    #[test] fn gt_105() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 105.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 24); }
    #[test] fn gt_110() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 110.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 19); }
    #[test] fn gt_115() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 115.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 14); }
    #[test] fn gt_120() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 120.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 9); }
    #[test] fn gt_125() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 125.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 4); }
    #[test] fn gt_0() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 0"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn gt_999() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 999.0"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn lt_105() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price < 105.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 4); }
    #[test] fn lt_110() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price < 110.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 9); }
    #[test] fn gte_100() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price >= 100.0"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn gte_129() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price >= 129.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn lte_100() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price <= 100.0"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn lte_129() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price <= 129.0"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn gt_vol_15() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol > 15.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 10); }
    #[test] fn gt_vol_20() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol > 20.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn lt_vol_12() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol < 12.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn gt_and_lt() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 110.0 AND price < 120.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }
    #[test] fn gt_or() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 128.0 OR price < 101.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 2); }
    #[test] fn gt_sym_and_price() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' AND price > 110.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
}

// ============================================================================
// 4. GROUP BY with aggregates (50 tests)
// ============================================================================
mod group_by_agg {
    use super::*;
    #[test] fn group_sym_count() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); for row in &r { assert_eq!(row[1], Value::I64(10)); } }
    #[test] fn group_cat_count() { let db = db30(); let (_, r) = db.query("SELECT cat, count(*) FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); for row in &r { assert_eq!(row[1], Value::I64(15)); } }
    #[test] fn group_sym_sum() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_avg() { let db = db30(); let (_, r) = db.query("SELECT sym, avg(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_min() { let db = db30(); let (_, r) = db.query("SELECT sym, min(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); assert_eq!(r[0][1], Value::F64(100.0)); }
    #[test] fn group_sym_max() { let db = db30(); let (_, r) = db.query("SELECT sym, max(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_cat_sum() { let db = db30(); let (_, r) = db.query("SELECT cat, sum(price) FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); }
    #[test] fn group_cat_avg() { let db = db30(); let (_, r) = db.query("SELECT cat, avg(price) FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); }
    #[test] fn group_cat_min() { let db = db30(); let (_, r) = db.query("SELECT cat, min(price) FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); }
    #[test] fn group_cat_max() { let db = db30(); let (_, r) = db.query("SELECT cat, max(price) FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); }
    #[test] fn group_sym_sum_vol() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_avg_vol() { let db = db30(); let (_, r) = db.query("SELECT sym, avg(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_min_vol() { let db = db30(); let (_, r) = db.query("SELECT sym, min(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_max_vol() { let db = db30(); let (_, r) = db.query("SELECT sym, max(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_count_sum() { let db = db30(); let (c, r) = db.query("SELECT sym, count(*), sum(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(c.len(), 3); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_count_avg() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*), avg(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_min_max() { let db = db30(); let (_, r) = db.query("SELECT sym, min(price), max(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_sym_all_aggs() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*), sum(price), avg(price), min(price), max(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_symbol_count() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_symbol_sum() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, sum(price) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_symbol_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_symbol_min() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, min(price) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_symbol_max() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, max(price) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_side_count() { let db = db_trades(30); let (_, r) = db.query("SELECT side, count(*) FROM trades GROUP BY side ORDER BY side"); assert_eq!(r.len(), 2); }
    #[test] fn trades_group_side_sum() { let db = db_trades(30); let (_, r) = db.query("SELECT side, sum(price) FROM trades GROUP BY side ORDER BY side"); assert_eq!(r.len(), 2); }
    #[test] fn group_having_count() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) AS c FROM t GROUP BY sym HAVING count(*) > 5 ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_having_sum() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) AS s FROM t GROUP BY sym HAVING sum(price) > 1000 ORDER BY sym"); assert!(r.len() >= 1); }
    #[test] fn group_having_avg() { let db = db30(); let (_, r) = db.query("SELECT sym, avg(price) FROM t GROUP BY sym HAVING avg(price) > 100 ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_limit() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t GROUP BY sym ORDER BY sym LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn group_limit_1() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) FROM t GROUP BY sym ORDER BY sum(price) DESC LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn group_having_limit() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t GROUP BY sym HAVING count(*) >= 10 ORDER BY sym LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn group_multi_key() { let db = db30(); let (_, r) = db.query("SELECT sym, cat, count(*) FROM t GROUP BY sym, cat ORDER BY sym, cat"); assert_eq!(r.len(), 6); }
    #[test] fn group_multi_key_sum() { let db = db30(); let (_, r) = db.query("SELECT sym, cat, sum(price) FROM t GROUP BY sym, cat ORDER BY sym, cat"); assert_eq!(r.len(), 6); }
    #[test] fn group_multi_key_avg() { let db = db30(); let (_, r) = db.query("SELECT sym, cat, avg(price) FROM t GROUP BY sym, cat ORDER BY sym, cat"); assert_eq!(r.len(), 6); }
    #[test] fn trades_group_symbol_side() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side ORDER BY symbol, side"); assert_eq!(r.len(), 6); }
    #[test] fn group_sum_order_desc() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) AS s FROM t GROUP BY sym ORDER BY s DESC"); assert_eq!(r.len(), 3); }
    #[test] fn group_avg_order_desc() { let db = db30(); let (_, r) = db.query("SELECT sym, avg(price) AS a FROM t GROUP BY sym ORDER BY a DESC"); assert_eq!(r.len(), 3); }
    #[test] fn group_count_order_desc() { let db = db30(); let (_, r) = db.query("SELECT cat, count(*) AS c FROM t GROUP BY cat ORDER BY c DESC"); assert_eq!(r.len(), 2); }
    #[test] fn group_min_order() { let db = db30(); let (_, r) = db.query("SELECT sym, min(price) AS m FROM t GROUP BY sym ORDER BY m"); assert_eq!(r.len(), 3); }
    #[test] fn group_max_order() { let db = db30(); let (_, r) = db.query("SELECT sym, max(price) AS m FROM t GROUP BY sym ORDER BY m DESC"); assert_eq!(r.len(), 3); }
    #[test] fn group_count_col() { let db = db30(); let (_, r) = db.query("SELECT sym, count(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_where_then_group() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t WHERE price > 110.0 GROUP BY sym ORDER BY sym"); assert!(r.len() >= 2); }
    #[test] fn group_where_having() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) FROM t WHERE cat = 'A' GROUP BY sym HAVING sum(price) > 100 ORDER BY sym"); assert!(r.len() >= 1); }
    #[test] fn trades_group_avg_having() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol HAVING avg(price) > 100 ORDER BY symbol"); assert!(r.len() >= 2); }
    #[test] fn group_count_star_vs_col() { let db = db30(); let v1 = db.query_scalar("SELECT count(*) FROM t"); let v2 = db.query_scalar("SELECT count(vol) FROM t"); assert_eq!(v1, v2); }
    #[test] fn group_three_aggs() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*), sum(price), avg(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_four_aggs() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*), sum(price), min(vol), max(vol) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_five_aggs() { let db = db30(); let (_, r) = db.query("SELECT cat, count(*), sum(price), avg(price), min(vol), max(vol) FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); }
    #[test] fn group_six_aggs() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*), sum(price), avg(price), min(price), max(price) FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); assert_eq!(r[0].len(), 6); }
    #[test] fn group_alias_in_having() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) AS cnt FROM t GROUP BY sym HAVING count(*) = 10 ORDER BY sym"); assert_eq!(r.len(), 3); }
}

// ============================================================================
// 5. ORDER BY ASC/DESC x LIMIT (40 tests)
// ============================================================================
mod order_limit {
    use super::*;
    #[test] fn asc_limit_1() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 1"); assert_eq!(r.len(), 1); assert_eq!(r[0][0], Value::F64(100.0)); }
    #[test] fn asc_limit_2() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn asc_limit_3() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn asc_limit_5() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn asc_limit_10() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn asc_limit_15() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn asc_limit_20() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn desc_limit_1() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 1"); assert_eq!(r.len(), 1); assert_eq!(r[0][0], Value::F64(129.0)); }
    #[test] fn desc_limit_2() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn desc_limit_3() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn desc_limit_5() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn desc_limit_10() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn desc_limit_15() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 15"); assert_eq!(r.len(), 15); }
    #[test] fn desc_limit_20() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 20"); assert_eq!(r.len(), 20); }
    #[test] fn vol_asc_limit_5() { let db = db30(); let (_, r) = db.query("SELECT vol FROM t ORDER BY vol LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn vol_desc_limit_5() { let db = db30(); let (_, r) = db.query("SELECT vol FROM t ORDER BY vol DESC LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn sym_asc_limit_5() { let db = db30(); let (_, r) = db.query("SELECT sym FROM t ORDER BY sym LIMIT 5"); assert_eq!(r.len(), 5); assert_eq!(r[0][0], Value::Str("BTC".into())); }
    #[test] fn sym_desc_limit_5() { let db = db30(); let (_, r) = db.query("SELECT sym FROM t ORDER BY sym DESC LIMIT 5"); assert_eq!(r.len(), 5); assert_eq!(r[0][0], Value::Str("SOL".into())); }
    #[test] fn limit_0() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 0"); assert_eq!(r.len(), 0); }
    #[test] fn limit_all() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 100"); assert_eq!(r.len(), 30); }
    #[test] fn asc_limit_4() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn asc_limit_6() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn asc_limit_7() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn asc_limit_8() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn asc_limit_9() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn desc_limit_4() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 4"); assert_eq!(r.len(), 4); }
    #[test] fn desc_limit_6() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 6"); assert_eq!(r.len(), 6); }
    #[test] fn desc_limit_7() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 7"); assert_eq!(r.len(), 7); }
    #[test] fn desc_limit_8() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 8"); assert_eq!(r.len(), 8); }
    #[test] fn desc_limit_9() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 9"); assert_eq!(r.len(), 9); }
    #[test] fn desc_limit_11() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 11"); assert_eq!(r.len(), 11); }
    #[test] fn desc_limit_12() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 12"); assert_eq!(r.len(), 12); }
    #[test] fn desc_limit_13() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 13"); assert_eq!(r.len(), 13); }
    #[test] fn desc_limit_14() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 14"); assert_eq!(r.len(), 14); }
    #[test] fn desc_limit_16() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 16"); assert_eq!(r.len(), 16); }
    #[test] fn desc_limit_17() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 17"); assert_eq!(r.len(), 17); }
    #[test] fn desc_limit_18() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 18"); assert_eq!(r.len(), 18); }
    #[test] fn desc_limit_19() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price DESC LIMIT 19"); assert_eq!(r.len(), 19); }
    #[test] fn asc_limit_25() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 25"); assert_eq!(r.len(), 25); }
    #[test] fn asc_limit_30() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY price LIMIT 30"); assert_eq!(r.len(), 30); }
}

// ============================================================================
// 6. DISTINCT x ORDER BY (20 tests)
// ============================================================================
mod distinct_order {
    use super::*;
    #[test] fn distinct_sym() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn distinct_cat() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT cat FROM t ORDER BY cat"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_sym_desc() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t ORDER BY sym DESC"); assert_eq!(r[0][0], Value::Str("SOL".into())); }
    #[test] fn distinct_cat_desc() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT cat FROM t ORDER BY cat DESC"); assert_eq!(r[0][0], Value::Str("B".into())); }
    #[test] fn distinct_sym_limit() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t ORDER BY sym LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_cat_limit() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT cat FROM t ORDER BY cat LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn distinct_trades_symbol() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn distinct_trades_side() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT side FROM trades ORDER BY side"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_trades_symbol_desc() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol DESC"); assert_eq!(r[0][0], Value::Str("SOL/USD".into())); }
    #[test] fn distinct_sym_where() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t WHERE price > 115.0 ORDER BY sym"); assert!(r.len() >= 2); }
    #[test] fn distinct_cat_where() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT cat FROM t WHERE price > 120.0 ORDER BY cat"); assert!(r.len() >= 1); }
    #[test] fn distinct_sym_limit_1() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t ORDER BY sym LIMIT 1"); assert_eq!(r.len(), 1); assert_eq!(r[0][0], Value::Str("BTC".into())); }
    #[test] fn distinct_no_dupes() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t"); assert_eq!(r.len(), 3); }
    #[test] fn distinct_all() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT price FROM t ORDER BY price"); assert_eq!(r.len(), 30); }
    #[test] fn distinct_vol_sorted() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT vol FROM t ORDER BY vol"); assert_eq!(r.len(), 30); }
    #[test] fn distinct_trades_3_syms() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT symbol FROM trades"); assert_eq!(r.len(), 3); }
    #[test] fn distinct_trades_2_sides() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT side FROM trades"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_trades_sym_limit_2() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_trades_side_limit_1() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT side FROM trades ORDER BY side LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn distinct_trades_symbol_where() { let db = db_trades(30); let (_, r) = db.query("SELECT DISTINCT symbol FROM trades WHERE side = 'buy' ORDER BY symbol"); assert_eq!(r.len(), 3); }
}

// ============================================================================
// 7. SAMPLE BY combos (30 tests)
// ============================================================================
mod sample_combos {
    use super::*;
    #[test] fn sample_1m_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_sum() { let db = db_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1d_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d"); assert!(!r.is_empty()); }
    #[test] fn sample_1d_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1d"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m"); assert!(r.len() >= 10); }
    #[test] fn sample_10m_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_sum() { let db = db_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_min() { let db = db_trades(30); let (_, r) = db.query("SELECT min(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_max() { let db = db_trades(30); let (_, r) = db.query("SELECT max(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_5m_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn sample_5m_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn sample_15m_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 15m"); assert!(!r.is_empty()); }
    #[test] fn sample_30m_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 30m"); assert!(!r.is_empty()); }
    #[test] fn sample_2h_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 2h"); assert!(!r.is_empty()); }
    #[test] fn sample_4h_count() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 4h"); assert!(!r.is_empty()); }
    #[test] fn sample_where_btc_1h() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_where_eth_1h() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_where_buy_10m() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'buy' SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_multi_agg_1h() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*), avg(price), sum(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_order_ts() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_order_desc() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h ORDER BY timestamp DESC"); assert!(!r.is_empty()); }
    #[test] fn sample_limit_3() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m LIMIT 3"); assert!(r.len() <= 3); }
    #[test] fn sample_limit_5() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m LIMIT 5"); assert!(r.len() <= 5); }
    #[test] fn sample_min_max_1h() { let db = db_trades(30); let (_, r) = db.query("SELECT min(price), max(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_where_order_limit() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h ORDER BY timestamp LIMIT 2"); assert!(r.len() <= 2); }
    #[test] fn sample_empty() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1h"); assert_eq!(r.len(), 0); }
    #[test] fn sample_single() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1h"); assert_eq!(r.len(), 1); }
}

// ============================================================================
// 8. BETWEEN (20 tests)
// ============================================================================
mod between {
    use super::*;
    #[test] fn between_100_110() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 100.0 AND 110.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 10); }
    #[test] fn between_110_120() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 110.0 AND 120.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 10); }
    #[test] fn between_120_130() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 120.0 AND 130.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 9); }
    #[test] fn between_100_129() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 100.0 AND 129.0"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn between_0_99() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 0 AND 99"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn between_200_300() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 200 AND 300"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn between_vol_10_15() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol BETWEEN 10.0 AND 15.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }
    #[test] fn between_vol_15_20() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol BETWEEN 15.0 AND 20.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }
    #[test] fn between_vol_20_25() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol BETWEEN 20.0 AND 25.0"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn between_exact() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 100.0 AND 100.0"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn between_and_sym() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 100 AND 115 AND sym = 'BTC'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn between_order() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE price BETWEEN 100 AND 105 ORDER BY price"); assert!(r.len() >= 5); }
    #[test] fn between_order_desc() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE price BETWEEN 100 AND 105 ORDER BY price DESC"); assert!(r.len() >= 5); }
    #[test] fn between_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE price BETWEEN 100 AND 120 ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn between_count_group() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t WHERE price BETWEEN 100 AND 115 GROUP BY sym ORDER BY sym"); assert!(r.len() >= 2); }
    #[test] fn trades_between_prices() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE price BETWEEN 0 AND 100000"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn trades_between_none() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE price BETWEEN 0 AND 1"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn between_vol_all() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE vol BETWEEN 0 AND 100"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn between_103_107() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 103 AND 107"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 4); }
    #[test] fn between_sym_and_order() { let db = db30(); let (_, r) = db.query("SELECT sym, price FROM t WHERE price BETWEEN 105 AND 115 ORDER BY price"); assert!(r.len() >= 5); }
}

// ============================================================================
// 9. IN (20 tests)
// ============================================================================
mod in_list {
    use super::*;
    #[test] fn in_sym_1() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC')"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn in_sym_2() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC', 'ETH')"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test] fn in_sym_3() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC', 'ETH', 'SOL')"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn in_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE cat IN ('A')"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn in_cat_both() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE cat IN ('A', 'B')"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn in_empty_match() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('DOGE')"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn in_trades_symbol_1() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol IN ('BTC/USD')"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn in_trades_symbol_2() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test] fn in_trades_side() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side IN ('buy')"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn in_trades_both_sides() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side IN ('buy', 'sell')"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn in_and_order() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym IN ('BTC') ORDER BY price"); assert_eq!(r.len(), 10); }
    #[test] fn in_and_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym IN ('BTC', 'ETH') ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test]fn in_and_group() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t WHERE sym IN ('BTC', 'SOL') GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 2); }
    #[test] fn in_and_between() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC', 'ETH') AND price BETWEEN 100 AND 110"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn in_trades_symbol_none() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol IN ('DOGE/USD')"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn in_order_desc() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym IN ('SOL') ORDER BY price DESC"); assert_eq!(r.len(), 10); }
    #[test]fn in_group_sum() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) FROM t WHERE sym IN ('BTC', 'ETH') GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 2); }
    #[test] fn in_distinct() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT sym FROM t WHERE sym IN ('BTC', 'ETH') ORDER BY sym"); assert_eq!(r.len(), 2); }
    #[test] fn in_where_and_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC') AND cat = 'A'"); assert_eq!(r[0][0], Value::I64(5)); }
    #[test] fn in_trades_3_syms() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD', 'SOL/USD')"); assert_eq!(r[0][0], Value::I64(30)); }
}

// ============================================================================
// 10. LIKE (20 tests)
// ============================================================================
mod like_tests {
    use super::*;
    #[test] fn like_btc_prefix() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol LIKE 'BTC%'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_eth_prefix() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol LIKE 'ETH%'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_sol_prefix() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol LIKE 'SOL%'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_usd_suffix() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol LIKE '%USD'"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn like_slash() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol LIKE '%/%'"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn like_buy() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side LIKE 'buy'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn like_b_prefix() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side LIKE 'b%'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn like_s_prefix() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side LIKE 's%'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn like_btc_exact() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'BTC'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_eth_exact() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'ETH'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_sol_exact() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'SOL'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_percent_only() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE '%'"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn like_no_match() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'XYZ%'"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn like_a_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE cat LIKE 'A'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn like_b_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE cat LIKE 'B'"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn like_and_order() { let db = db_trades(30); let (_, r) = db.query("SELECT price FROM trades WHERE symbol LIKE 'BTC%' ORDER BY price"); assert_eq!(r.len(), 10); }
    #[test] fn like_and_limit() { let db = db_trades(30); let (_, r) = db.query("SELECT price FROM trades WHERE symbol LIKE 'ETH%' ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn like_and_group() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, count(*) FROM trades WHERE symbol LIKE '%USD' GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn like_and_between() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'B%' AND price BETWEEN 100 AND 115"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn like_underscore() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'B__'"); assert_eq!(r[0][0], Value::I64(10)); }
}

// ============================================================================
// 11. CASE WHEN (20 tests)
// ============================================================================
mod case_when {
    use super::*;
    #[test] fn case_sym_label() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN sym = 'BTC' THEN 'bitcoin' ELSE 'other' END AS label FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::Str("bitcoin".into())); }
    #[test] fn case_price_high_low() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t ORDER BY timestamp"); assert_eq!(r.len(), 30); }
    #[test] fn case_count_high() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t) WHERE tier = 'high'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 14); }
    #[test] fn case_count_low() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t) WHERE tier = 'low'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 15); }
    #[test] fn case_nested() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price > 120 THEN 'high' WHEN price > 110 THEN 'mid' ELSE 'low' END AS tier FROM t ORDER BY timestamp"); assert_eq!(r.len(), 30); }
    #[test] fn case_with_order() { let db = db30(); let (_, r) = db.query("SELECT price, CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t ORDER BY price"); assert_eq!(r.len(), 30); }
    #[test] fn case_with_limit() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn case_vol_tier() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN vol > 20 THEN 'big' ELSE 'small' END AS size FROM t ORDER BY timestamp"); assert_eq!(r.len(), 30); }
    #[test] fn case_trades_sym() { let db = db_trades(20); let (_, r) = db.query("SELECT CASE WHEN symbol = 'BTC/USD' THEN 'btc' WHEN symbol = 'ETH/USD' THEN 'eth' ELSE 'other' END AS lbl FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn case_trades_side() { let db = db_trades(20); let (_, r) = db.query("SELECT CASE WHEN side = 'buy' THEN 1 ELSE 0 END AS is_buy FROM trades"); assert_eq!(r.len(), 20); }
    #[test] fn case_sym_a_b() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN sym = 'BTC' THEN 'A' WHEN sym = 'ETH' THEN 'B' ELSE 'C' END AS grp FROM t"); assert_eq!(r.len(), 30); }
    #[test] fn case_price_bucket_1() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price < 105 THEN '100-105' ELSE '105+' END AS bucket FROM t ORDER BY price LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn case_price_bucket_2() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price < 110 THEN 'low' WHEN price < 120 THEN 'mid' ELSE 'high' END AS bucket FROM t"); assert_eq!(r.len(), 30); }
    #[test] fn case_cat_label() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN cat = 'A' THEN 'alpha' ELSE 'bravo' END AS lbl FROM t"); assert_eq!(r.len(), 30); }
    #[test] fn case_where_and_case() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t WHERE sym = 'BTC'"); assert_eq!(r.len(), 10); }
    #[test] fn case_order_by_case() { let db = db30(); let (_, r) = db.query("SELECT price, CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t ORDER BY tier, price"); assert_eq!(r.len(), 30); }
    #[test] fn case_group_by() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier, count(*) FROM t GROUP BY tier ORDER BY tier"); assert_eq!(r.len(), 2); }
    #[test] fn case_limit_10() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn case_else_null() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN sym = 'BTC' THEN 'yes' END AS m FROM t ORDER BY timestamp"); assert_eq!(r.len(), 30); }
    #[test]fn case_in_where() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE CASE WHEN sym = 'BTC' THEN 1 ELSE 0 END = 1 ORDER BY price"); assert_eq!(r.len(), 10); }
}

// ============================================================================
// 12. Arithmetic expressions (20 tests)
// ============================================================================
mod arithmetic {
    use super::*;
    #[test] fn add_price() { let db = db30(); let (_, r) = db.query("SELECT price + 1.0 AS p FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(101.0)); }
    #[test] fn sub_price() { let db = db30(); let (_, r) = db.query("SELECT price - 100.0 AS p FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(0.0)); }
    #[test] fn mul_price() { let db = db30(); let (_, r) = db.query("SELECT price * 2.0 AS p FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(200.0)); }
    #[test] fn div_price() { let db = db30(); let (_, r) = db.query("SELECT price / 2.0 AS p FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(50.0)); }
    #[test] fn add_vol() { let db = db30(); let (_, r) = db.query("SELECT vol + 5.0 AS v FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(15.0)); }
    #[test] fn mul_vol() { let db = db30(); let (_, r) = db.query("SELECT vol * 3.0 AS v FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(30.0)); }
    #[test] fn price_times_vol() { let db = db30(); let (_, r) = db.query("SELECT price * vol AS notional FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(1000.0)); }
    #[test] fn price_minus_vol() { let db = db30(); let (_, r) = db.query("SELECT price - vol AS diff FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(90.0)); }
    #[test] fn price_div_vol() { let db = db30(); let (_, r) = db.query("SELECT price / vol AS ratio FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(10.0)); }
    #[test] fn arith_order_by() { let db = db30(); let (_, r) = db.query("SELECT price * 2.0 AS dp FROM t ORDER BY dp LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn arith_where() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE price + 0.0 > 115 ORDER BY price"); let c = r.len(); assert!(c >= 14); }
    #[test] fn arith_group_sum() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price * vol) AS total FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn arith_complex() { let db = db30(); let (_, r) = db.query("SELECT (price + vol) / 2.0 AS midpoint FROM t ORDER BY timestamp LIMIT 1"); match &r[0][0] { Value::F64(v) => assert!((*v - 55.0).abs() < 0.01), _ => panic!() } }
    #[test] fn arith_trades_notional() { let db = db_trades(20); let (_, r) = db.query("SELECT price * volume AS notional FROM trades ORDER BY timestamp LIMIT 5"); assert!(r.len() <= 5); }
    #[test] fn arith_neg() { let db = db30(); let (_, r) = db.query("SELECT -price AS neg FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(-100.0)); }
    #[test]fn arith_add_const() { let db = db30(); let (_, r) = db.query("SELECT 1 + 1 AS two FROM t LIMIT 1"); assert_eq!(r[0][0], Value::I64(2)); }
    #[test]fn arith_mul_const() { let db = db30(); let (_, r) = db.query("SELECT 3 * 4 AS twelve FROM t LIMIT 1"); assert_eq!(r[0][0], Value::I64(12)); }
    #[test]fn arith_float_const() { let db = db30(); let (_, r) = db.query("SELECT 1.5 + 2.5 AS four FROM t LIMIT 1"); assert_eq!(r[0][0], Value::F64(4.0)); }
    #[test] fn arith_sum_product() { let db = db30(); let v = db.query_scalar("SELECT sum(price * vol) FROM t"); match v { Value::F64(f) => assert!(f > 0.0), _ => panic!("expected F64") } }
    #[test] fn arith_avg_product() { let db = db30(); let v = db.query_scalar("SELECT avg(price * vol) FROM t"); match v { Value::F64(f) => assert!(f > 0.0), _ => panic!("expected F64") } }
}

// ============================================================================
// 13. CTE (WITH) (10 tests)
// ============================================================================
mod cte_tests {
    use super::*;
    #[test] fn cte_basic() { let db = db30(); let (_, r) = db.query("WITH btc AS (SELECT price FROM t WHERE sym = 'BTC') SELECT count(*) FROM btc"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn cte_agg() { let db = db30(); let (_, r) = db.query("WITH btc AS (SELECT price FROM t WHERE sym = 'BTC') SELECT avg(price) FROM btc"); match &r[0][0] { Value::F64(f) => assert!(*f > 100.0), _ => panic!() } }
    #[test] fn cte_sum() { let db = db30(); let (_, r) = db.query("WITH all_t AS (SELECT price FROM t) SELECT sum(price) FROM all_t"); match &r[0][0] { Value::F64(f) => assert!(*f > 0.0), _ => panic!() } }
    #[test] fn cte_where() { let db = db30(); let (_, r) = db.query("WITH high AS (SELECT price FROM t WHERE price > 120) SELECT count(*) FROM high"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 9); }
    #[test] fn cte_order() { let db = db30(); let (_, r) = db.query("WITH ordered AS (SELECT price FROM t ORDER BY price) SELECT price FROM ordered LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn cte_min() { let db = db30(); let v = db.query_scalar("WITH btc AS (SELECT price FROM t WHERE sym = 'BTC') SELECT min(price) FROM btc"); assert_eq!(v, Value::F64(100.0)); }
    #[test] fn cte_max() { let db = db30(); let v = db.query_scalar("WITH sol AS (SELECT price FROM t WHERE sym = 'SOL') SELECT max(price) FROM sol"); match v { Value::F64(f) => assert!(f > 100.0), _ => panic!() } }
    #[test] fn cte_with_limit() { let db = db30(); let (_, r) = db.query("WITH lim AS (SELECT price FROM t ORDER BY price LIMIT 10) SELECT count(*) FROM lim"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn cte_trades() { let db = db_trades(20); let (_, r) = db.query("WITH btc AS (SELECT price FROM trades WHERE symbol = 'BTC/USD') SELECT count(*) FROM btc"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 6); }
    #[test] fn cte_distinct() { let db = db30(); let (_, r) = db.query("WITH syms AS (SELECT DISTINCT sym FROM t) SELECT count(*) FROM syms"); assert_eq!(r[0][0], Value::I64(3)); }
}

// ============================================================================
// 14. Subqueries (10 tests)
// ============================================================================
mod subquery_tests {
    use super::*;
    #[test] fn subq_count() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT price FROM t WHERE sym = 'BTC')"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn subq_avg() { let db = db30(); let (_, r) = db.query("SELECT avg(price) FROM (SELECT price FROM t WHERE sym = 'ETH')"); match &r[0][0] { Value::F64(f) => assert!(*f > 100.0), _ => panic!() } }
    #[test] fn subq_sum() { let db = db30(); let (_, r) = db.query("SELECT sum(price) FROM (SELECT price FROM t WHERE price > 120)"); match &r[0][0] { Value::F64(f) => assert!(*f > 0.0), _ => panic!() } }
    #[test] fn subq_min() { let db = db30(); let v = db.query_scalar("SELECT min(price) FROM (SELECT price FROM t)"); assert_eq!(v, Value::F64(100.0)); }
    #[test] fn subq_max() { let db = db30(); let v = db.query_scalar("SELECT max(price) FROM (SELECT price FROM t)"); assert_eq!(v, Value::F64(129.0)); }
    #[test] fn subq_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM (SELECT price FROM t ORDER BY price LIMIT 5)"); assert_eq!(r.len(), 5); }
    #[test] fn subq_where() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT price FROM t WHERE cat = 'A')"); assert_eq!(r[0][0], Value::I64(15)); }
    #[test] fn subq_distinct() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT DISTINCT sym FROM t)"); assert_eq!(r[0][0], Value::I64(3)); }
    #[test] fn subq_trades() { let db = db_trades(20); let (_, r) = db.query("SELECT count(*) FROM (SELECT price FROM trades WHERE side = 'buy')"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn subq_nested() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT price FROM (SELECT price FROM t WHERE sym = 'SOL'))"); assert_eq!(r[0][0], Value::I64(10)); }
}

// ============================================================================
// 15. UNION (10 tests)
// ============================================================================
mod union_tests {
    use super::*;
    #[test] fn union_basic() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'ETH'"); assert_eq!(r.len(), 20); }
    #[test] fn union_three() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'ETH' UNION ALL SELECT price FROM t WHERE sym = 'SOL'"); assert_eq!(r.len(), 30); }
    #[test] fn union_count() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM (SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'ETH')"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test] fn union_same() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'BTC'"); assert_eq!(r.len(), 20); }
    #[test] fn union_order() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'ETH' ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn union_empty_right() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'XYZ'"); assert_eq!(r.len(), 10); }
    #[test] fn union_empty_left() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'XYZ' UNION ALL SELECT price FROM t WHERE sym = 'ETH'"); assert_eq!(r.len(), 10); }
    #[test] fn union_trades() { let db = db_trades(20); let (_, r) = db.query("SELECT price FROM trades WHERE symbol = 'BTC/USD' UNION ALL SELECT price FROM trades WHERE symbol = 'ETH/USD'"); let c = r.len(); assert!(c >= 12); }
    #[test] fn union_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'BTC' UNION ALL SELECT price FROM t WHERE sym = 'ETH' LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn union_both_empty() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym = 'X' UNION ALL SELECT price FROM t WHERE sym = 'Y'"); assert_eq!(r.len(), 0); }
}

// ============================================================================
// 16. Additional combos to reach 500 (130 tests)
// ============================================================================
mod extra_combos {
    use super::*;

    // SELECT * variations
    #[test] fn star_all() { let db = db30(); let (c, r) = db.query("SELECT * FROM t"); assert_eq!(c.len(), 5); assert_eq!(r.len(), 30); }
    #[test] fn star_limit_5() { let db = db30(); let (_, r) = db.query("SELECT * FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn star_limit_10() { let db = db30(); let (_, r) = db.query("SELECT * FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn star_where_sym() { let db = db30(); let (_, r) = db.query("SELECT * FROM t WHERE sym = 'BTC'"); assert_eq!(r.len(), 10); }
    #[test] fn star_order_price() { let db = db30(); let (_, r) = db.query("SELECT * FROM t ORDER BY price LIMIT 5"); assert_eq!(r.len(), 5); }

    // Mixed operations
    #[test] fn count_where_order() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) AS c FROM t WHERE price > 110 GROUP BY sym ORDER BY c DESC"); assert!(r.len() >= 2); }
    #[test] fn sum_where_group_limit() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) FROM t WHERE cat = 'A' GROUP BY sym ORDER BY sym LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn avg_between_group() { let db = db30(); let (_, r) = db.query("SELECT sym, avg(price) FROM t WHERE price BETWEEN 105 AND 120 GROUP BY sym ORDER BY sym"); assert!(r.len() >= 2); }
    #[test]fn min_in_group() { let db = db30(); let (_, r) = db.query("SELECT sym, min(price) FROM t WHERE sym IN ('BTC', 'ETH') GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 2); }
    #[test]fn max_like_group() { let db = db30(); let (_, r) = db.query("SELECT sym, max(price) FROM t WHERE sym LIKE 'B%' GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 1); }

    // Complex WHERE
    #[test] fn complex_where_1() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' AND price > 105 AND cat = 'A'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn complex_where_2() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE (sym = 'BTC' OR sym = 'ETH') AND price > 110"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }
    #[test] fn complex_where_3() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym != 'SOL' AND cat = 'B' AND price < 120"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn complex_where_4() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC', 'SOL') AND price BETWEEN 100 AND 115"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn complex_where_5() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol LIKE 'BTC%' AND side = 'buy'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }

    // Column projection
    #[test] fn select_one_col() { let db = db30(); let (c, _) = db.query("SELECT sym FROM t LIMIT 1"); assert_eq!(c.len(), 1); }
    #[test] fn select_two_cols() { let db = db30(); let (c, _) = db.query("SELECT sym, price FROM t LIMIT 1"); assert_eq!(c.len(), 2); }
    #[test] fn select_three_cols() { let db = db30(); let (c, _) = db.query("SELECT sym, price, vol FROM t LIMIT 1"); assert_eq!(c.len(), 3); }
    #[test] fn select_four_cols() { let db = db30(); let (c, _) = db.query("SELECT sym, price, vol, cat FROM t LIMIT 1"); assert_eq!(c.len(), 4); }
    #[test] fn select_all_cols() { let db = db30(); let (c, _) = db.query("SELECT timestamp, sym, price, vol, cat FROM t LIMIT 1"); assert_eq!(c.len(), 5); }

    // Aliases
    #[test] fn alias_col() { let db = db30(); let (c, _) = db.query("SELECT sym AS symbol FROM t LIMIT 1"); assert!(c.contains(&"symbol".to_string())); }
    #[test] fn alias_agg() { let db = db30(); let (c, _) = db.query("SELECT count(*) AS total FROM t"); assert!(c.contains(&"total".to_string())); }
    #[test] fn alias_sum() { let db = db30(); let (c, _) = db.query("SELECT sum(price) AS total_price FROM t"); assert!(c.contains(&"total_price".to_string())); }
    #[test] fn alias_avg() { let db = db30(); let (c, _) = db.query("SELECT avg(price) AS mean_price FROM t"); assert!(c.contains(&"mean_price".to_string())); }
    #[test] fn alias_arith() { let db = db30(); let (c, _) = db.query("SELECT price * vol AS notional FROM t LIMIT 1"); assert!(c.contains(&"notional".to_string())); }

    // Empty table edge cases
    #[test] fn empty_count() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT count(*) FROM t"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn empty_select() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT * FROM t"); assert_eq!(r.len(), 0); }
    #[test] fn empty_sum() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT sum(v) FROM t"); assert_eq!(r[0][0], Value::Null); }
    #[test] fn empty_avg() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT avg(v) FROM t"); assert_eq!(r[0][0], Value::Null); }
    #[test] fn empty_min() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT min(v) FROM t"); assert_eq!(r[0][0], Value::Null); }
    #[test] fn empty_max() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT max(v) FROM t"); assert_eq!(r[0][0], Value::Null); }

    // Single row edge cases
    #[test] fn single_count() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT count(*) FROM t"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn single_sum() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0))); let v = db.query_scalar("SELECT sum(v) FROM t"); assert_eq!(v, Value::F64(42.0)); }
    #[test] fn single_avg() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0))); let v = db.query_scalar("SELECT avg(v) FROM t"); assert_eq!(v, Value::F64(42.0)); }
    #[test] fn single_min() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0))); let v = db.query_scalar("SELECT min(v) FROM t"); assert_eq!(v, Value::F64(42.0)); }
    #[test] fn single_max() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0))); let v = db.query_scalar("SELECT max(v) FROM t"); assert_eq!(v, Value::F64(42.0)); }

    // Trades with various row counts
    #[test] fn trades_select_star_5() { let db = db_trades(5); let (_, r) = db.query("SELECT * FROM trades"); assert_eq!(r.len(), 5); }
    #[test] fn trades_select_star_15() { let db = db_trades(15); let (_, r) = db.query("SELECT * FROM trades"); assert_eq!(r.len(), 15); }
    #[test] fn trades_group_5() { let db = db_trades(6); let (_, r) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn trades_group_9() { let db = db_trades(9); let (_, r) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol"); for row in &r { assert_eq!(row[1], Value::I64(3)); } }
    #[test] fn trades_distinct_5() { let db = db_trades(5); let (_, r) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol"); assert!(r.len() >= 2); }

    // ORDER BY multiple columns
    #[test] fn order_multi_1() { let db = db30(); let (_, r) = db.query("SELECT sym, price FROM t ORDER BY sym, price LIMIT 5"); assert_eq!(r.len(), 5); assert_eq!(r[0][0], Value::Str("BTC".into())); }
    #[test] fn order_multi_2() { let db = db30(); let (_, r) = db.query("SELECT sym, price FROM t ORDER BY sym DESC, price ASC LIMIT 5"); assert_eq!(r.len(), 5); assert_eq!(r[0][0], Value::Str("SOL".into())); }
    #[test] fn order_multi_3() { let db = db30(); let (_, r) = db.query("SELECT cat, price FROM t ORDER BY cat, price LIMIT 5"); assert_eq!(r.len(), 5); }

    // NULL handling
    #[test] fn null_count_col() { let db = db_trades(10); let v = db.query_scalar("SELECT count(volume) FROM trades"); match v { Value::I64(n) => assert!(n <= 10), _ => panic!() } }
    #[test] fn null_sum_col() { let db = db_trades(10); let (_, r) = db.query("SELECT sum(volume) FROM trades"); assert!(!r.is_empty()); }
    #[test] fn null_avg_col() { let db = db_trades(10); let (_, r) = db.query("SELECT avg(volume) FROM trades"); assert!(!r.is_empty()); }

    // IS NULL / IS NOT NULL
    #[test] fn is_null_vol() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE volume IS NULL"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn is_not_null_vol() { let db = db_trades(10); let (_, r) = db.query("SELECT count(*) FROM trades WHERE volume IS NOT NULL"); assert_eq!(r[0][0], Value::I64(9)); }

    // More WHERE combos
    #[test] fn where_gt_lt() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price > 105 AND price < 125"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 15); }
    #[test] fn where_or_and() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE (sym = 'BTC' OR sym = 'ETH') AND cat = 'A'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }

    // GROUP BY + DISTINCT
    #[test] fn group_distinct_sym() { let db = db30(); let (_, r) = db.query("SELECT sym FROM t GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn group_distinct_cat() { let db = db30(); let (_, r) = db.query("SELECT cat FROM t GROUP BY cat ORDER BY cat"); assert_eq!(r.len(), 2); }

    // Additional BETWEEN combos
    #[test] fn between_106_114() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 106 AND 114"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 8); }
    #[test] fn between_115_125() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 115 AND 125"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 10); }
    #[test] fn between_and_in() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price BETWEEN 100 AND 115 AND sym IN ('BTC', 'ETH')"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }

    // Additional IN combos
    #[test] fn in_and_gt() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC', 'SOL') AND price > 115"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }
    #[test] fn in_order_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym IN ('ETH') ORDER BY price LIMIT 3"); assert_eq!(r.len(), 3); }

    // Additional LIKE combos
    #[test] fn like_percent_tc() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE '%TC'"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn like_and_cat() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym LIKE 'E%' AND cat = 'B'"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }

    // More CASE combos
    #[test] fn case_four_branches() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price < 105 THEN 'a' WHEN price < 115 THEN 'b' WHEN price < 125 THEN 'c' ELSE 'd' END AS tier FROM t"); assert_eq!(r.len(), 30); }
    #[test]fn case_with_arith() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN price * vol > 2000 THEN 'big' ELSE 'small' END AS size FROM t"); assert_eq!(r.len(), 30); }

    // More arithmetic combos
    #[test] fn arith_add_sub() { let db = db30(); let (_, r) = db.query("SELECT price + vol - 100 AS x FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(10.0)); }
    #[test] fn arith_mul_div() { let db = db30(); let (_, r) = db.query("SELECT price * vol / 100 AS x FROM t ORDER BY timestamp LIMIT 1"); assert_eq!(r[0][0], Value::F64(10.0)); }

    // More SAMPLE BY combos
    #[test] fn sample_30m_avg() { let db = db_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 30m"); assert!(!r.is_empty()); }
    #[test] fn sample_2h_sum() { let db = db_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 2h"); assert!(!r.is_empty()); }

    // More CTE combos
    #[test] fn cte_group() { let db = db30(); let (_, r) = db.query("WITH g AS (SELECT sym, price FROM t) SELECT sym, count(*) FROM g GROUP BY sym ORDER BY sym"); assert_eq!(r.len(), 3); }
    #[test] fn cte_where_group() { let db = db30(); let (_, r) = db.query("WITH h AS (SELECT sym, price FROM t WHERE price > 110) SELECT sym, avg(price) FROM h GROUP BY sym ORDER BY sym"); assert!(r.len() >= 2); }

    // More subquery combos
    #[test] fn subq_group() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM (SELECT sym, price FROM t WHERE price > 115) GROUP BY sym ORDER BY sym"); assert!(r.len() >= 2); }
    #[test] fn subq_order_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM (SELECT price FROM t ORDER BY price DESC) LIMIT 3"); assert_eq!(r.len(), 3); }

    // More UNION combos
    #[test] fn union_distinct_syms() { let db = db30(); let (_, r) = db.query("SELECT sym FROM t WHERE sym = 'BTC' UNION ALL SELECT sym FROM t WHERE sym = 'ETH'"); assert_eq!(r.len(), 20); }
    #[test] fn union_with_where() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE price < 105 UNION ALL SELECT price FROM t WHERE price > 125"); let c = r.len(); assert!(c >= 8); }

    // Mixed features
    #[test] fn like_between_order() { let db = db30(); let (_, r) = db.query("SELECT price FROM t WHERE sym LIKE 'B%' AND price BETWEEN 100 AND 115 ORDER BY price"); assert!(r.len() >= 1); }
    #[test] fn in_case_order() { let db = db30(); let (_, r) = db.query("SELECT CASE WHEN sym = 'BTC' THEN 'btc' ELSE 'other' END AS lbl FROM t WHERE sym IN ('BTC', 'ETH') ORDER BY timestamp"); assert_eq!(r.len(), 20); }
    #[test] fn group_having_order_limit() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price) AS s FROM t GROUP BY sym HAVING sum(price) > 1000 ORDER BY s DESC LIMIT 2"); assert!(r.len() >= 1); }
    #[test] fn between_group_having() { let db = db30(); let (_, r) = db.query("SELECT sym, count(*) FROM t WHERE price BETWEEN 100 AND 120 GROUP BY sym HAVING count(*) > 3 ORDER BY sym"); assert!(r.len() >= 2); }
    #[test] fn in_group_having() { let db = db30(); let (_, r) = db.query("SELECT sym, avg(price) FROM t WHERE sym IN ('BTC', 'SOL') GROUP BY sym HAVING avg(price) > 100 ORDER BY sym"); assert_eq!(r.len(), 2); }
    #[test] fn arith_group() { let db = db30(); let (_, r) = db.query("SELECT sym, sum(price * vol) AS total FROM t GROUP BY sym ORDER BY total DESC LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn case_distinct() { let db = db30(); let (_, r) = db.query("SELECT DISTINCT CASE WHEN price > 115 THEN 'high' ELSE 'low' END AS tier FROM t ORDER BY tier"); assert_eq!(r.len(), 2); }
    #[test] fn subq_in_where() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym IN ('BTC', 'ETH') AND price > 110"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 3); }
    #[test] fn cte_union() { let db = db30(); let (_, r) = db.query("WITH btc AS (SELECT price FROM t WHERE sym = 'BTC'), eth AS (SELECT price FROM t WHERE sym = 'ETH') SELECT price FROM btc UNION ALL SELECT price FROM eth"); assert_eq!(r.len(), 20); }
    #[test] fn sample_where_in() { let db = db_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD') SAMPLE BY 1h"); assert!(!r.is_empty()); }

    // Trades-specific combos
    #[test] fn trades_avg_by_symbol() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol ORDER BY avg(price) DESC LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn trades_min_max_by_side() { let db = db_trades(30); let (_, r) = db.query("SELECT side, min(price), max(price) FROM trades GROUP BY side ORDER BY side"); assert_eq!(r.len(), 2); }
    #[test] fn trades_count_by_sym_side() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side ORDER BY symbol, side"); assert_eq!(r.len(), 6); }
    #[test] fn trades_like_btc_order() { let db = db_trades(30); let (_, r) = db.query("SELECT price FROM trades WHERE symbol LIKE 'BTC%' ORDER BY price DESC LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn trades_between_order() { let db = db_trades(30); let (_, r) = db.query("SELECT price FROM trades WHERE price BETWEEN 60000 AND 70000 ORDER BY price"); assert!(!r.is_empty()); }
    #[test]fn trades_is_null_group() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, count(*) FROM trades WHERE volume IS NULL GROUP BY symbol ORDER BY symbol"); assert!(r.len() >= 1); }
    #[test] fn trades_is_not_null_group() { let db = db_trades(30); let (_, r) = db.query("SELECT symbol, count(*) FROM trades WHERE volume IS NOT NULL GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }

    // Additional ORDER + LIMIT combos
    #[test] fn order_by_ts_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY timestamp LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn order_by_ts_desc_limit() { let db = db30(); let (_, r) = db.query("SELECT price FROM t ORDER BY timestamp DESC LIMIT 5"); assert_eq!(r.len(), 5); }

    // Combinatorial WHERE
    #[test] fn where_3_and() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' AND cat = 'A' AND price > 105"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 1); }
    #[test] fn where_or_or() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' OR sym = 'ETH' OR sym = 'SOL'"); assert_eq!(r[0][0], Value::I64(30)); }
    #[test] fn where_not_in() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym NOT IN ('BTC')"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test] fn where_not_like() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym NOT LIKE 'B%'"); assert_eq!(r[0][0], Value::I64(20)); }
    #[test]fn where_not_between() { let db = db30(); let (_, r) = db.query("SELECT count(*) FROM t WHERE price NOT BETWEEN 105 AND 125"); let c = match &r[0][0] { Value::I64(n) => *n, _ => panic!() }; assert!(c >= 5); }
}
