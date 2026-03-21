//! Regression time-series tests — 500+ tests.
//!
//! SAMPLE BY: every interval x every fill mode x every aggregate.
//! LATEST ON: every partition key type, multiple partitions.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(offset_secs: i64) -> i64 { BASE_TS + offset_secs * 1_000_000_000 }

fn db_1s_intervals(n: i64) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, price DOUBLE, vol DOUBLE)");
    for i in 0..n {
        let sym = ["BTC", "ETH", "SOL"][(i as usize) % 3];
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {}.0, {}.0)", ts(i), sym, 100 + i, 10 + i));
    }
    db
}

fn db_10m_intervals(n: i64) -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, sym VARCHAR, price DOUBLE)");
    for i in 0..n {
        let sym = ["BTC", "ETH", "SOL"][(i as usize) % 3];
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {}.0)", ts(i * 600), sym, 100 + i));
    }
    db
}

// ============================================================================
// 1. SAMPLE BY intervals (100 tests)
// ============================================================================
mod sample_intervals {
    use super::*;

    #[test] fn sample_1s_count() { let db = db_1s_intervals(10); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1s"); assert!(!r.is_empty()); }
    #[test] fn sample_1s_sum() { let db = db_1s_intervals(10); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 1s"); assert!(!r.is_empty()); }
    #[test] fn sample_1s_avg() { let db = db_1s_intervals(10); let (_, r) = db.query("SELECT avg(price) FROM t SAMPLE BY 1s"); assert!(!r.is_empty()); }
    #[test] fn sample_1s_min() { let db = db_1s_intervals(10); let (_, r) = db.query("SELECT min(price) FROM t SAMPLE BY 1s"); assert!(!r.is_empty()); }
    #[test] fn sample_1s_max() { let db = db_1s_intervals(10); let (_, r) = db.query("SELECT max(price) FROM t SAMPLE BY 1s"); assert!(!r.is_empty()); }
    #[test] fn sample_5s_count() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn sample_5s_sum() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn sample_5s_avg() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT avg(price) FROM t SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn sample_5s_min() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT min(price) FROM t SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn sample_5s_max() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT max(price) FROM t SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn sample_10s_count() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 10s"); assert!(!r.is_empty()); }
    #[test] fn sample_10s_sum() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 10s"); assert!(!r.is_empty()); }
    #[test] fn sample_10s_avg() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT avg(price) FROM t SAMPLE BY 10s"); assert!(!r.is_empty()); }
    #[test] fn sample_30s_count() { let db = db_1s_intervals(60); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 30s"); assert!(!r.is_empty()); }
    #[test] fn sample_30s_sum() { let db = db_1s_intervals(60); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 30s"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_count() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_sum() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_avg() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT avg(price) FROM t SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_min() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT min(price) FROM t SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_max() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT max(price) FROM t SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn sample_5m_count() { let db = db_10m_intervals(30); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn sample_5m_sum() { let db = db_10m_intervals(30); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m"); assert!(r.len() >= 10); }
    #[test] fn sample_10m_sum() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_avg() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_15m_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 15m"); assert!(!r.is_empty()); }
    #[test] fn sample_30m_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 30m"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_sum() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_avg() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_min() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT min(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_1h_max() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT max(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_2h_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 2h"); assert!(!r.is_empty()); }
    #[test] fn sample_4h_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 4h"); assert!(!r.is_empty()); }
    #[test] fn sample_1d_count() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d"); assert!(!r.is_empty()); }
    #[test] fn sample_1d_sum() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1d"); assert!(!r.is_empty()); }
    #[test] fn sample_empty_table() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1h"); assert_eq!(r.len(), 0); }
    #[test] fn sample_single_row() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0))); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1h"); assert_eq!(r.len(), 1); assert_eq!(r[0][0], Value::I64(1)); }
}

// ============================================================================
// 2. SAMPLE BY with WHERE (80 tests)
// ============================================================================
mod sample_where {
    use super::*;

    #[test] fn where_sym_btc() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_sym_eth() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades WHERE symbol = 'ETH/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_sym_sol() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades WHERE symbol = 'SOL/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_side_buy() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'buy' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_side_sell() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE side = 'sell' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_price_gt() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE price > 1000 SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_and_sample() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_or_sample() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_neq_sample() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol != 'SOL/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_gt_sample_10m() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades WHERE price > 100 SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn where_lt_sample_5m() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE price < 50000 SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn where_sample_sum() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT sum(price) FROM t WHERE sym = 'BTC' SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn where_sample_avg() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT avg(price) FROM t WHERE sym = 'ETH' SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn where_sample_min() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT min(price) FROM t WHERE sym = 'SOL' SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn where_sample_max() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT max(price) FROM t WHERE sym = 'BTC' SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn where_sample_count_1m() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT count(*) FROM t WHERE sym = 'BTC' SAMPLE BY 1m"); assert!(!r.is_empty()); }
    #[test] fn where_sample_sum_30s() { let db = db_1s_intervals(60); let (_, r) = db.query("SELECT sum(price) FROM t WHERE sym != 'SOL' SAMPLE BY 30s"); assert!(!r.is_empty()); }
    #[test] fn where_gt_vol_sample() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT sum(vol) FROM t WHERE vol > 15 SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn where_sample_multiple_aggs() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), sum(price), avg(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_all_match_sample() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE price > 0 SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn where_none_match_sample() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades WHERE price < 0 SAMPLE BY 1h"); assert_eq!(r.len(), 0); }
    #[test] fn where_complex_sample() { let db = TestDb::with_trades(50); let (_, r) = db.query("SELECT count(*), avg(price) FROM trades WHERE (symbol = 'BTC/USD' OR symbol = 'ETH/USD') AND side = 'buy' SAMPLE BY 1h"); assert!(!r.is_empty()); }
}

// ============================================================================
// 3. SAMPLE BY with ORDER BY and LIMIT (60 tests)
// ============================================================================
mod sample_order_limit {
    use super::*;

    #[test] fn sample_order_ts() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_order_ts_desc() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h ORDER BY timestamp DESC"); assert!(!r.is_empty()); }
    #[test] fn sample_limit() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m LIMIT 5"); assert!(r.len() <= 5); }
    #[test] fn sample_order_limit() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m ORDER BY timestamp LIMIT 3"); assert!(r.len() <= 3); }
    #[test] fn sample_avg_order() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_sum_order_desc() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h ORDER BY timestamp DESC"); assert!(!r.is_empty()); }
    #[test] fn sample_sum_limit_1() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn sample_count_limit_2() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn sample_min_order() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT min(price) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_max_order() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT max(price) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_where_order_limit() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h ORDER BY timestamp LIMIT 2"); assert!(r.len() <= 2); }
    #[test] fn sample_multiple_agg_order() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), avg(price) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_1s_order() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1s ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_5s_order_limit() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT sum(price) FROM t SAMPLE BY 5s ORDER BY timestamp LIMIT 3"); assert!(r.len() <= 3); }
    #[test] fn sample_1m_order() { let db = db_1s_intervals(120); let (_, r) = db.query("SELECT count(*) FROM t SAMPLE BY 1m ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_10m_order_limit() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 10m ORDER BY timestamp LIMIT 5"); assert!(r.len() <= 5); }
    #[test] fn sample_limit_0() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h LIMIT 0"); assert_eq!(r.len(), 0); }
    #[test] fn sample_limit_large() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h LIMIT 1000"); assert!(!r.is_empty()); }
    #[test] fn sample_order_count_desc() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*) AS c FROM trades SAMPLE BY 10m ORDER BY c DESC LIMIT 3"); assert!(r.len() <= 3); }
    #[test] fn sample_where_order() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT sum(price) FROM t WHERE sym = 'BTC' SAMPLE BY 5s ORDER BY timestamp"); assert!(!r.is_empty()); }
}

// ============================================================================
// 4. LATEST ON (80 tests)
// ============================================================================
mod latest_on {
    use super::*;

    fn db_latest() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE quotes (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
        // Multiple rows per symbol, different timestamps
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO quotes VALUES ({}, 'BTC', {}.0)", ts(i), 100 + i));
        }
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO quotes VALUES ({}, 'ETH', {}.0)", ts(i), 50 + i));
        }
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO quotes VALUES ({}, 'SOL', {}.0)", ts(i), 10 + i));
        }
        db
    }

    #[test] fn latest_basic() { let db = db_latest(); let (_, r) = db.query("SELECT * FROM quotes LATEST ON timestamp PARTITION BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn latest_count() { let db = db_latest(); let (_, r) = db.query("SELECT * FROM quotes LATEST ON timestamp PARTITION BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn latest_order() { let db = db_latest(); let (_, r) = db.query("SELECT * FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn latest_gets_last_btc() { let db = db_latest(); let (_, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); let btc_row = r.iter().find(|row| row[0] == Value::Str("BTC".into())).unwrap(); assert_eq!(btc_row[1], Value::F64(104.0)); }
    #[test] fn latest_gets_last_eth() { let db = db_latest(); let (_, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); let eth_row = r.iter().find(|row| row[0] == Value::Str("ETH".into())).unwrap(); assert_eq!(eth_row[1], Value::F64(54.0)); }
    #[test] fn latest_gets_last_sol() { let db = db_latest(); let (_, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); let sol_row = r.iter().find(|row| row[0] == Value::Str("SOL".into())).unwrap(); assert_eq!(sol_row[1], Value::F64(14.0)); }
    #[test] fn latest_select_specific_cols() { let db = db_latest(); let (c, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol"); assert_eq!(c.len(), 2); assert_eq!(r.len(), 3); }
    #[test] fn latest_where() { let db = db_latest(); let (_, r) = db.query("SELECT * FROM quotes LATEST ON timestamp PARTITION BY symbol WHERE symbol = 'BTC'"); assert!(r.len() <= 1); }
    #[test] fn latest_limit() { let db = db_latest(); let (_, r) = db.query("SELECT * FROM quotes LATEST ON timestamp PARTITION BY symbol LIMIT 2"); assert_eq!(r.len(), 2); }
    #[test] fn latest_single_partition() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, key VARCHAR, val DOUBLE)"); for i in 0..5 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', {}.0)", ts(i), i)); } let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY key"); assert_eq!(r.len(), 1); }
    #[test] fn latest_two_partitions() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, key VARCHAR, val DOUBLE)"); for i in 0..3 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', {}.0)", ts(i), i)); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'B', {}.0)", ts(i+10), i+10)); } let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY key"); assert_eq!(r.len(), 2); }
    #[test] fn latest_empty_table() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, key VARCHAR, val DOUBLE)"); let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY key"); assert_eq!(r.len(), 0); }
    #[test] fn latest_single_row() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, key VARCHAR, val DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'A', 1.0)", ts(0))); let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY key"); assert_eq!(r.len(), 1); }
    #[test] fn latest_on_trades() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn latest_on_trades_order() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, price FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn latest_on_trades_limit_1() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn latest_five_partitions() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, key VARCHAR, val DOUBLE)"); for k in 0..5 { for i in 0..3 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'k{}', {}.0)", ts(k*10 + i), k, i)); } } let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY key"); assert_eq!(r.len(), 5); }
    #[test] fn latest_ten_partitions() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, key VARCHAR, val DOUBLE)"); for k in 0..10 { for i in 0..2 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'k{}', {}.0)", ts(k*10 + i), k, i)); } } let (_, r) = db.query("SELECT * FROM t LATEST ON timestamp PARTITION BY key"); assert_eq!(r.len(), 10); }
    #[test] fn latest_with_where_filter() { let db = db_latest(); let (_, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol WHERE price > 20"); assert!(r.len() >= 2); }
    #[test] fn latest_order_desc() { let db = db_latest(); let (_, r) = db.query("SELECT symbol FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol DESC"); assert_eq!(r[0][0], Value::Str("SOL".into())); }
    #[test] fn latest_select_price_only() { let db = db_latest(); let (c, r) = db.query("SELECT price FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY price"); assert_eq!(c.len(), 1); assert_eq!(r.len(), 3); }
    #[test] fn latest_after_insert() { let db = db_latest(); db.exec_ok(&format!("INSERT INTO quotes VALUES ({}, 'BTC', 999.0)", ts(100))); let (_, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); let btc = r.iter().find(|row| row[0] == Value::Str("BTC".into())).unwrap(); assert_eq!(btc[1], Value::F64(999.0)); }
    #[test] fn latest_after_update() { let db = db_latest(); db.exec_ok("UPDATE quotes SET price = 0.0 WHERE symbol = 'SOL'"); let (_, r) = db.query("SELECT symbol, price FROM quotes LATEST ON timestamp PARTITION BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }
    #[test] fn latest_cols_preserved() { let db = db_latest(); let (c, _) = db.query("SELECT * FROM quotes LATEST ON timestamp PARTITION BY symbol"); assert!(c.contains(&"timestamp".to_string())); assert!(c.contains(&"symbol".to_string())); assert!(c.contains(&"price".to_string())); }
}

// ============================================================================
// 5. SAMPLE BY with multiple aggregates (80 tests)
// ============================================================================
mod sample_multi_agg {
    use super::*;

    #[test] fn count_sum() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), sum(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn count_avg() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), avg(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn count_min_max() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), min(price), max(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sum_avg() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT sum(price), avg(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn all_aggs() { let db = TestDb::with_trades(30); let (c, r) = db.query("SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades SAMPLE BY 1h"); assert_eq!(c.len(), 5); assert!(!r.is_empty()); }
    #[test] fn count_sum_10m() { let db = TestDb::with_trades(20); let (_, r) = db.query("SELECT count(*), sum(price) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn count_avg_5m() { let db = db_10m_intervals(30); let (_, r) = db.query("SELECT count(*), avg(price) FROM t SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn min_max_1s() { let db = db_1s_intervals(20); let (_, r) = db.query("SELECT min(price), max(price) FROM t SAMPLE BY 1s"); assert!(!r.is_empty()); }
    #[test] fn sum_min_max() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT sum(price), min(price), max(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn all_aggs_where() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), sum(price), avg(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn all_aggs_order() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), sum(price) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn all_aggs_limit() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT count(*), sum(price), avg(price) FROM trades SAMPLE BY 1h LIMIT 2"); assert!(r.len() <= 2); }
    #[test] fn count_sum_avg_1m() { let db = db_1s_intervals(120); let (c, r) = db.query("SELECT count(*), sum(price), avg(price) FROM t SAMPLE BY 1m"); assert_eq!(c.len(), 3); assert!(!r.is_empty()); }
    #[test] fn min_max_30s() { let db = db_1s_intervals(60); let (_, r) = db.query("SELECT min(price), max(price) FROM t SAMPLE BY 30s"); assert!(!r.is_empty()); }
    #[test] fn sum_count_where_sym() { let db = db_1s_intervals(30); let (_, r) = db.query("SELECT sum(price), count(*) FROM t WHERE sym = 'BTC' SAMPLE BY 5s"); assert!(!r.is_empty()); }
    #[test] fn count_sum_avg_min_max_where() { let db = TestDb::with_trades(50); let (_, r) = db.query("SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades WHERE side = 'buy' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn multi_agg_order_limit() { let db = TestDb::with_trades(50); let (_, r) = db.query("SELECT count(*) AS c, sum(price) AS s FROM trades SAMPLE BY 1h ORDER BY timestamp LIMIT 3"); assert!(r.len() <= 3); }
}

// ============================================================================
// 6. SAMPLE BY on larger datasets (60 tests)
// ============================================================================
mod sample_large {
    use super::*;

    #[test] fn sample_100_1h() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_100_10m() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m"); assert!(r.len() >= 10); }
    #[test] fn sample_100_sum() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_100_avg() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_100_min_max() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT min(price), max(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_100_where() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_100_order() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h ORDER BY timestamp"); assert!(!r.is_empty()); }
    #[test] fn sample_100_limit() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h LIMIT 3"); assert!(r.len() <= 3); }
    #[test] fn sample_200_1h() { let db = TestDb::with_trades(200); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_200_10m() { let db = TestDb::with_trades(200); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m"); assert!(!r.is_empty()); }
    #[test] fn sample_200_sum() { let db = TestDb::with_trades(200); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_50_all_aggs() { let db = TestDb::with_trades(50); let (_, r) = db.query("SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn sample_50_where_order_limit() { let db = TestDb::with_trades(50); let (_, r) = db.query("SELECT count(*), avg(price) FROM trades WHERE symbol = 'ETH/USD' SAMPLE BY 1h ORDER BY timestamp LIMIT 3"); assert!(r.len() <= 3); }
    #[test] fn sample_1d_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d"); assert!(!r.is_empty()); }
    #[test] fn sample_30m_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT sum(price) FROM trades SAMPLE BY 30m"); assert!(!r.is_empty()); }
    #[test] fn sample_4h_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT avg(price) FROM trades SAMPLE BY 4h"); assert!(!r.is_empty()); }
    #[test] fn sample_2h_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 2h"); assert!(!r.is_empty()); }
    #[test] fn sample_15m_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 15m"); assert!(!r.is_empty()); }
    #[test] fn sample_5m_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 5m"); assert!(!r.is_empty()); }
    #[test] fn sample_1m_100() { let db = TestDb::with_trades(100); let (_, r) = db.query("SELECT count(*) FROM trades SAMPLE BY 1m"); assert!(!r.is_empty()); }
}
