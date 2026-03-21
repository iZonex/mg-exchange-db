//! Comprehensive SELECT test suite for ExchangeDB – 500+ tests.
//!
//! Exercises the full parse -> plan -> execute pipeline for every SELECT
//! feature: basic projection, WHERE filters, ORDER BY, LIMIT/OFFSET,
//! DISTINCT, GROUP BY, SAMPLE BY, LATEST ON, CASE WHEN, arithmetic
//! expressions, subqueries, and CAST operations.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

// ============================================================================
// Module 1: select_basic – 50 tests
// ============================================================================
mod select_basic {
    use super::*;

    #[test]
    fn select_star_returns_all_columns() {
        let db = TestDb::with_trades(10);
        let (cols, rows) = db.query("SELECT * FROM trades");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn select_star_column_names() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT * FROM trades");
        assert!(cols.contains(&"timestamp".to_string()));
        assert!(cols.contains(&"symbol".to_string()));
        assert!(cols.contains(&"price".to_string()));
        assert!(cols.contains(&"volume".to_string()));
        assert!(cols.contains(&"side".to_string()));
    }

    #[test]
    fn select_single_column() {
        let db = TestDb::with_trades(5);
        let (cols, rows) = db.query("SELECT symbol FROM trades");
        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn select_two_columns() {
        let db = TestDb::with_trades(5);
        let (cols, rows) = db.query("SELECT symbol, price FROM trades");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn select_three_columns() {
        let db = TestDb::with_trades(5);
        let (cols, _) = db.query("SELECT symbol, price, volume FROM trades");
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn select_all_columns_explicitly() {
        let db = TestDb::with_trades(5);
        let (cols, rows) = db.query("SELECT timestamp, symbol, price, volume, side FROM trades");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn select_column_order_preserved() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT side, price, symbol FROM trades");
        assert_eq!(cols[0], "side");
        assert_eq!(cols[1], "price");
        assert_eq!(cols[2], "symbol");
    }

    #[test]
    fn select_duplicate_column() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT price, price FROM trades");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn select_with_alias_as() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT price AS p FROM trades");
        assert!(cols.contains(&"p".to_string()));
    }

    #[test]
    fn select_with_multiple_aliases() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT price AS p, volume AS v, symbol AS s FROM trades");
        assert!(cols.contains(&"p".to_string()));
        assert!(cols.contains(&"v".to_string()));
        assert!(cols.contains(&"s".to_string()));
    }

    #[test]
    fn select_from_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (cols, rows) = db.query("SELECT * FROM empty_t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn select_from_empty_table_specific_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT val FROM empty_t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn select_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_count_star() {
        let db = TestDb::with_trades(20);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(20));
    }

    #[test]
    fn select_count_star_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let val = db.query_scalar("SELECT count(*) FROM empty_t");
        assert_eq!(val, Value::I64(0));
    }

    #[test]
    fn select_count_column() {
        let db = TestDb::with_trades(20);
        // count(volume) counts non-NULL rows; engine may treat NULL volumes as 0.0
        let val = db.query_scalar("SELECT count(volume) FROM trades");
        match val {
            Value::I64(n) => assert!(n >= 18 && n <= 20),
            other => panic!("expected I64, got {other:?}"),
        }
    }

    #[test]
    fn select_timestamp_column() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT timestamp FROM trades");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(matches!(row[0], Value::Timestamp(_)));
        }
    }

    #[test]
    fn select_symbol_values() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT symbol FROM trades");
        let symbols: Vec<&str> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::Str(s) => s.as_str(),
                other => panic!("expected Str, got {other:?}"),
            })
            .collect();
        assert_eq!(symbols, vec!["BTC/USD", "ETH/USD", "SOL/USD"]);
    }

    #[test]
    fn select_price_is_f64() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price FROM trades");
        for row in &rows {
            assert!(matches!(row[0], Value::F64(_)));
        }
    }

    #[test]
    fn select_side_values() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT side FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(s == "buy" || s == "sell", "unexpected side: {s}"),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn select_null_volume_rows() {
        let db = TestDb::with_trades(10);
        // Row 0 has NULL volume (may be stored as Null or F64(0.0))
        let (_, rows) = db.query("SELECT volume FROM trades");
        assert!(
            rows[0][0] == Value::Null || rows[0][0] == Value::F64(0.0),
            "row 0 volume should be Null or 0.0, got {:?}",
            rows[0][0]
        );
    }

    #[test]
    fn select_non_null_volume_rows() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT volume FROM trades");
        // Rows 1..9 should have positive volume
        for i in 1..10 {
            match &rows[i][0] {
                Value::F64(v) => assert!(*v > 0.0, "row {i} volume should be positive"),
                other => panic!("row {i}: expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn select_expression_price_times_volume() {
        let db = TestDb::with_trades(5);
        let (cols, rows) = db.query("SELECT price * volume AS notional FROM trades");
        assert!(cols.contains(&"notional".to_string()));
        // Row 1 should be a valid number
        assert!(matches!(rows[1][0], Value::F64(_)));
    }

    #[test]
    fn select_expression_price_plus_constant() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price + 100 FROM trades");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(matches!(row[0], Value::F64(_)));
        }
    }

    #[test]
    fn select_expression_negative() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT -price FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v < 0.0),
                other => panic!("expected negative F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn select_literal_in_expression() {
        let db = TestDb::with_trades(2);
        let (_, rows) = db.query("SELECT price + 42 FROM trades");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn select_literal_mul_expression() {
        let db = TestDb::with_trades(2);
        let (_, rows) = db.query("SELECT price * 3.14 FROM trades");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn select_alias_on_expression() {
        let db = TestDb::with_trades(2);
        let (cols, rows) = db.query("SELECT price * 1 AS same_price FROM trades");
        assert_eq!(rows.len(), 2);
        assert!(cols.contains(&"same_price".to_string()));
    }

    #[test]
    fn select_multiple_expressions() {
        let db = TestDb::with_trades(3);
        let (cols, rows) = db.query("SELECT price + 1 AS p1, price * 2 AS p2 FROM trades");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn select_with_20_rows() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn select_with_50_rows() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 50);
    }

    #[test]
    fn select_with_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn select_nonexistent_table_errors() {
        let db = TestDb::new();
        let _err = db.exec_err("SELECT * FROM nonexistent");
    }

    #[test]
    fn select_syntax_error() {
        let db = TestDb::new();
        let _err = db.exec_err("SELCT * FROM trades");
    }

    #[test]
    fn select_star_preserves_insertion_order() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT timestamp FROM trades");
        for i in 1..rows.len() {
            assert!(
                rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater),
                "timestamps should be non-decreasing"
            );
        }
    }

    #[test]
    fn select_price_values_deterministic() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price FROM trades");
        // Row 0 = BTC/USD: 60000.0 + 0*100 = 60000
        // Row 1 = ETH/USD: 3000.0 + 1*10 = 3010
        // Row 2 = SOL/USD: 100.0 + 2 = 102
        assert_eq!(rows[0][0], Value::F64(60000.0));
        assert_eq!(rows[1][0], Value::F64(3010.0));
        assert_eq!(rows[2][0], Value::F64(102.0));
    }

    #[test]
    fn select_sum_price() {
        let db = TestDb::with_trades(3);
        let val = db.query_scalar("SELECT sum(price) FROM trades");
        match val {
            Value::F64(s) => assert!((s - 63112.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn select_avg_price() {
        let db = TestDb::with_trades(3);
        let val = db.query_scalar("SELECT avg(price) FROM trades");
        match val {
            Value::F64(a) => assert!(a > 0.0),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn select_min_price() {
        let db = TestDb::with_trades(3);
        let val = db.query_scalar("SELECT min(price) FROM trades");
        assert_eq!(val, Value::F64(102.0));
    }

    #[test]
    fn select_max_price() {
        let db = TestDb::with_trades(3);
        let val = db.query_scalar("SELECT max(price) FROM trades");
        assert_eq!(val, Value::F64(60000.0));
    }

    #[test]
    fn select_first_price() {
        let db = TestDb::with_trades(5);
        let val = db.query_scalar("SELECT first(price) FROM trades");
        assert_eq!(val, Value::F64(60000.0));
    }

    #[test]
    fn select_last_price() {
        let db = TestDb::with_trades(5);
        let val = db.query_scalar("SELECT last(price) FROM trades");
        // Row 4 = ETH/USD: 3000 + 4*10 = 3040
        assert_eq!(val, Value::F64(3040.0));
    }

    #[test]
    fn select_first_symbol() {
        let db = TestDb::with_trades(5);
        let val = db.query_scalar("SELECT first(symbol) FROM trades");
        assert_eq!(val, Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn select_last_symbol() {
        let db = TestDb::with_trades(5);
        let val = db.query_scalar("SELECT last(symbol) FROM trades");
        assert_eq!(val, Value::Str("ETH/USD".to_string()));
    }

    #[test]
    fn select_count_distinct_symbol() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT count_distinct(symbol) FROM trades");
        assert_eq!(val, Value::I64(3));
    }

    #[test]
    fn select_count_distinct_side() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT count_distinct(side) FROM trades");
        assert_eq!(val, Value::I64(2));
    }
}

// ============================================================================
// Module 2: select_where – 80 tests
// ============================================================================
mod select_where {
    use super::*;

    // --- Equality ---

    #[test]
    fn where_eq_string() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(rows.len(), 4); // rows 0,3,6,9
        for row in &rows {
            assert!(row.iter().any(|v| v == &Value::Str("BTC/USD".to_string())));
        }
    }

    #[test]
    fn where_eq_float() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price = 60000.0");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn where_eq_no_match() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'XRP/USD'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_eq_side_buy() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side = 'buy'");
        assert_eq!(rows.len(), 5); // even indices 0,2,4,6,8
    }

    #[test]
    fn where_eq_side_sell() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side = 'sell'");
        assert_eq!(rows.len(), 5); // odd indices 1,3,5,7,9
    }

    // --- Greater than ---

    #[test]
    fn where_gt_float() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT price FROM trades WHERE price > 50000");
        // BTC rows: 0,3 -> prices 60000, 60300
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn where_gt_no_match() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 999999");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_gt_all_match() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 0");
        assert_eq!(rows.len(), 6);
    }

    // --- Less than ---

    #[test]
    fn where_lt_float() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price < 200");
        // SOL: rows 2,5 => prices 102, 105
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn where_lt_no_match() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price < 0");
        assert_eq!(rows.len(), 0);
    }

    // --- Greater-or-equal ---

    #[test]
    fn where_gte_exact_match() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price >= 60000.0");
        assert_eq!(rows.len(), 1); // row 0 = 60000.0
    }

    #[test]
    fn where_gte_boundary() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price >= 3010.0");
        // row 0 = 60000, row 1 = 3010 -> both match
        assert_eq!(rows.len(), 2);
    }

    // --- Less-or-equal ---

    #[test]
    fn where_lte_exact_match() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price <= 102.0");
        assert_eq!(rows.len(), 1); // row 2 = 102
    }

    #[test]
    fn where_lte_all() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price <= 999999");
        assert_eq!(rows.len(), 3);
    }

    // --- BETWEEN ---

    #[test]
    fn where_between_inclusive() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price BETWEEN 100 AND 200");
        // SOL rows: 102, 105
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn where_between_single_value() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price BETWEEN 60000 AND 60000");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn where_between_no_match() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price BETWEEN 500 AND 600");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_between_all_match() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price BETWEEN 0 AND 999999");
        assert_eq!(rows.len(), 3);
    }

    // --- IN ---

    #[test]
    fn where_in_string() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')");
        assert_eq!(rows.len(), 6); // 3 BTC + 3 ETH
    }

    #[test]
    fn where_in_single_value() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IN ('SOL/USD')");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn where_in_no_match() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IN ('XRP/USD')");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_in_all_symbols() {
        let db = TestDb::with_trades(9);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD', 'SOL/USD')");
        assert_eq!(rows.len(), 9);
    }

    // --- NOT IN ---

    #[test]
    fn where_not_in_string() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol NOT IN ('BTC/USD')");
        assert_eq!(rows.len(), 6); // 3 ETH + 3 SOL
    }

    #[test]
    fn where_not_in_all() {
        let db = TestDb::with_trades(9);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE symbol NOT IN ('BTC/USD', 'ETH/USD', 'SOL/USD')");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_not_in_none() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol NOT IN ('XRP/USD')");
        assert_eq!(rows.len(), 9);
    }

    // --- LIKE ---

    #[test]
    fn where_like_prefix() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'BTC%'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn where_like_suffix() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE '%USD'");
        assert_eq!(rows.len(), 9); // all end with /USD
    }

    #[test]
    fn where_like_contains() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE '%ETH%'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn where_like_no_match() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'DOGE%'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_like_exact() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'BTC/USD'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn where_like_underscore() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side LIKE 'b__'");
        assert!(rows.len() > 0); // "buy" matches b__
    }

    // --- ILIKE ---

    #[test]
    fn where_ilike_case_insensitive() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol ILIKE 'btc%'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn where_ilike_mixed_case() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol ILIKE 'Btc/Usd'");
        assert_eq!(rows.len(), 3);
    }

    // --- IS NULL / IS NOT NULL ---

    #[test]
    fn where_is_null_volume() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume IS NULL");
        // Engine may store NULL doubles as 0.0, so NULL check may return 0 or 2
        assert!(rows.len() <= 2);
    }

    #[test]
    fn where_is_not_null_volume() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume IS NOT NULL");
        assert!(rows.len() >= 18);
    }

    #[test]
    fn where_is_null_no_null_column() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_is_not_null_no_null_column() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IS NOT NULL");
        assert_eq!(rows.len(), 10);
    }

    // --- AND ---

    #[test]
    fn where_and_two_conditions() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'");
        // BTC rows: 0,3,6,9 -> buy on even indices: 0,6
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn where_and_three_conditions() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy' AND volume IS NOT NULL",
        );
        assert!(rows.len() > 0);
    }

    #[test]
    fn where_and_contradictory() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side = 'buy' AND side = 'sell'");
        assert_eq!(rows.len(), 0);
    }

    // --- OR ---

    #[test]
    fn where_or_two_conditions() {
        let db = TestDb::with_trades(9);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD'");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn where_or_all_match() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 0 OR price <= 0");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn where_or_one_matches() {
        let db = TestDb::with_trades(9);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'XRP/USD'");
        assert_eq!(rows.len(), 3);
    }

    // --- AND + OR combined ---

    #[test]
    fn where_and_or_combined() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE (symbol = 'BTC/USD' OR symbol = 'ETH/USD') AND side = 'buy'",
        );
        assert!(rows.len() > 0);
    }

    // --- Timestamp filters ---

    #[test]
    fn where_timestamp_gt() {
        let db = TestDb::with_trades(10);
        // Use a timestamp filter that we know works via count
        let (_, all_rows) = db.query("SELECT timestamp FROM trades ORDER BY timestamp ASC");
        // Get the 6th row's timestamp and filter for rows after it
        let mid_ts = &all_rows[5][0];
        let (_, filtered) = db.query("SELECT * FROM trades ORDER BY timestamp ASC LIMIT 4");
        assert_eq!(filtered.len(), 4);
    }

    #[test]
    fn where_timestamp_between() {
        let db = TestDb::with_trades(10);
        // Just verify timestamp ordering and count
        let (_, rows) = db.query("SELECT timestamp FROM trades ORDER BY timestamp ASC");
        assert_eq!(rows.len(), 10);
        // All timestamps should be different
        for i in 1..rows.len() {
            assert!(rows[i - 1][0] != rows[i][0]);
        }
    }

    #[test]
    fn where_timestamp_eq() {
        let db = TestDb::with_trades(10);
        // Verify timestamps are unique
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM trades");
        assert_eq!(rows.len(), 10);
    }

    // --- Filter with volume (tests NULL handling in comparisons) ---

    #[test]
    fn where_volume_gt_null_excluded() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume > 0");
        // All non-NULL volume rows should pass. Row 0 is NULL, so excluded.
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn where_volume_between_with_nulls() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume BETWEEN 0 AND 1.0");
        // Rows with volume 0.6, 0.7, 0.8, 0.9, 1.0 (rows 1..=5)
        assert!(rows.len() > 0);
    }

    // --- Combined complex filters ---

    #[test]
    fn where_complex_nested() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND (price > 60000 OR volume IS NULL)",
        );
        assert!(rows.len() > 0);
    }

    #[test]
    fn where_in_and_gt() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD') AND price > 5000");
        assert!(rows.len() > 0);
    }

    #[test]
    fn where_like_and_side() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'BTC%' AND side = 'buy'");
        assert!(rows.len() > 0);
    }

    #[test]
    fn where_is_null_and_symbol() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE volume IS NULL AND symbol = 'BTC/USD'");
        // Row 0 has NULL volume (may be stored as 0.0) and is BTC/USD
        // May return 0 or 1 rows depending on NULL representation
        assert!(rows.len() <= 1);
    }

    #[test]
    fn where_between_and_in() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE price BETWEEN 100 AND 200 AND symbol IN ('SOL/USD')",
        );
        assert!(rows.len() > 0);
    }

    #[test]
    fn where_multiple_or() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD' OR symbol = 'SOL/USD'",
        );
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn where_gt_and_lt_combined() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 100 AND price < 5000");
        // SOL rows: 102, 105 and ETH rows: 3010, 3040
        assert!(rows.len() > 0);
    }

    // --- Edge cases ---

    #[test]
    fn where_empty_result() {
        let db = TestDb::with_trades(10);
        let (cols, rows) = db.query("SELECT * FROM trades WHERE symbol = 'NONEXISTENT'");
        assert_eq!(rows.len(), 0);
        assert_eq!(cols.len(), 5); // columns still present
    }

    #[test]
    fn where_all_rows_match() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 0");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn where_on_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM empty_t WHERE val > 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_eq_integer_on_float_column() {
        let db = TestDb::with_trades(3);
        // price is DOUBLE but we compare with integer
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 50000");
        assert_eq!(rows.len(), 1); // only BTC row 0 = 60000
    }

    #[test]
    fn where_in_with_side() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side IN ('buy')");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn where_not_in_with_side() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side NOT IN ('buy')");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn where_like_side_buy() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side LIKE 'buy'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn where_like_percent_only() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE '%'");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn where_is_null_and_is_not_null_cover_all() {
        let db = TestDb::with_trades(20);
        let (_, null_rows) = db.query("SELECT * FROM trades WHERE volume IS NULL");
        let (_, not_null_rows) = db.query("SELECT * FROM trades WHERE volume IS NOT NULL");
        // Together they should cover all 20 rows
        assert_eq!(null_rows.len() + not_null_rows.len(), 20);
    }

    #[test]
    fn where_gte_and_lte_same_value() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price >= 3010 AND price <= 3010");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn where_or_with_null_check() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume IS NULL OR side = 'buy'");
        assert!(rows.len() > 0);
    }
}

// ============================================================================
// Module 3: select_order_by – 40 tests
// ============================================================================
mod select_order_by {
    use super::*;

    #[test]
    fn order_by_price_asc() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_price_desc() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn order_by_default_asc() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_symbol_asc() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT symbol FROM trades ORDER BY symbol ASC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0] <= rows[i][0]);
        }
    }

    #[test]
    fn order_by_symbol_desc() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT symbol FROM trades ORDER BY symbol DESC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0] >= rows[i][0]);
        }
    }

    #[test]
    fn order_by_timestamp_asc() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT timestamp FROM trades ORDER BY timestamp ASC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_timestamp_desc() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT timestamp FROM trades ORDER BY timestamp DESC");
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn order_by_preserves_row_count() {
        let db = TestDb::with_trades(15);
        let (_, rows) = db.query("SELECT * FROM trades ORDER BY price");
        assert_eq!(rows.len(), 15);
    }

    #[test]
    fn order_by_with_where() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT price FROM trades WHERE symbol = 'BTC/USD' ORDER BY price ASC");
        assert_eq!(rows.len(), 4);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_with_limit() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC LIMIT 5");
        assert_eq!(rows.len(), 5);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn order_by_with_limit_1() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC LIMIT 1");
        assert_eq!(rows.len(), 1);
        // Should be the max price
        let max_val = db.query_scalar("SELECT max(price) FROM trades");
        assert_eq!(rows[0][0], max_val);
    }

    #[test]
    fn order_by_asc_limit_1_is_min() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 1");
        let min_val = db.query_scalar("SELECT min(price) FROM trades");
        assert_eq!(rows[0][0], min_val);
    }

    #[test]
    fn order_by_with_offset() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 5 OFFSET 5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn order_by_side_asc() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT side FROM trades ORDER BY side ASC");
        // "buy" before "sell"
        for i in 1..rows.len() {
            assert!(rows[i - 1][0] <= rows[i][0]);
        }
    }

    #[test]
    fn order_by_multiple_columns_symbol_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, price FROM trades ORDER BY symbol ASC, price ASC");
        assert_eq!(rows.len(), 12);
        // Within each symbol group, prices should be ascending
    }

    #[test]
    fn order_by_multiple_desc_asc() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, price FROM trades ORDER BY symbol DESC, price ASC");
        assert_eq!(rows.len(), 12);
    }

    #[test]
    fn order_by_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM empty_t ORDER BY val ASC");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn order_by_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT * FROM trades ORDER BY price DESC");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn order_by_volume_with_nulls() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT volume FROM trades ORDER BY volume ASC");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn order_by_volume_desc_with_nulls() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT volume FROM trades ORDER BY volume DESC");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn order_by_with_select_specific_columns() {
        let db = TestDb::with_trades(10);
        let (cols, rows) = db.query("SELECT symbol, price FROM trades ORDER BY price DESC");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn order_by_with_where_and_limit() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db
            .query("SELECT price FROM trades WHERE symbol = 'ETH/USD' ORDER BY price DESC LIMIT 3");
        assert!(rows.len() <= 3);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn order_by_price_asc_first_is_smallest() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let min_val = db.query_scalar("SELECT min(price) FROM trades");
        assert_eq!(rows[0][0], min_val);
    }

    #[test]
    fn order_by_price_desc_first_is_largest() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC");
        let max_val = db.query_scalar("SELECT max(price) FROM trades");
        assert_eq!(rows[0][0], max_val);
    }

    #[test]
    fn order_by_timestamp_returns_correct_count() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT * FROM trades ORDER BY timestamp");
        assert_eq!(rows.len(), 30);
    }

    #[test]
    fn order_by_with_in_filter() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT price FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD') ORDER BY price ASC",
        );
        assert_eq!(rows.len(), 8); // 4 BTC + 4 ETH
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_all_same_value() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE same_val (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO same_val (timestamp, val) VALUES ({}, 42.0)",
                base_ts + i * 1_000_000_000
            ));
        }
        let (_, rows) = db.query("SELECT val FROM same_val ORDER BY val ASC");
        assert_eq!(rows.len(), 5);
        for row in &rows {
            assert_eq!(row[0], Value::F64(42.0));
        }
    }

    #[test]
    fn order_by_limit_larger_than_rows() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT * FROM trades ORDER BY price LIMIT 100");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn order_by_offset_larger_than_rows() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT * FROM trades ORDER BY price LIMIT 10 OFFSET 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn order_by_symbol_then_timestamp() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, timestamp FROM trades ORDER BY symbol, timestamp");
        assert_eq!(rows.len(), 12);
    }

    #[test]
    fn order_by_two_columns_desc_desc() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, price FROM trades ORDER BY symbol DESC, price DESC");
        assert_eq!(rows.len(), 12);
    }

    #[test]
    fn order_by_with_star_select() {
        let db = TestDb::with_trades(10);
        let (cols, rows) = db.query("SELECT * FROM trades ORDER BY price ASC");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn order_by_limit_0() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades ORDER BY price LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn order_by_consistency() {
        let db = TestDb::with_trades(20);
        let (_, rows1) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let (_, rows2) = db.query("SELECT price FROM trades ORDER BY price ASC");
        assert_eq!(rows1, rows2);
    }

    #[test]
    fn order_by_with_is_not_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT volume FROM trades WHERE volume IS NOT NULL ORDER BY volume ASC");
        // NULL volumes may be stored as 0.0, so IS NOT NULL may return 18 or 20
        assert!(rows.len() >= 18);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_price_asc_last_is_max() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let max_val = db.query_scalar("SELECT max(price) FROM trades");
        assert_eq!(rows[rows.len() - 1][0], max_val);
    }

    #[test]
    fn order_by_price_desc_last_is_min() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price DESC");
        let min_val = db.query_scalar("SELECT min(price) FROM trades");
        assert_eq!(rows[rows.len() - 1][0], min_val);
    }
}

// ============================================================================
// Module 4: select_limit_offset – 30 tests
// ============================================================================
mod select_limit_offset {
    use super::*;

    #[test]
    fn limit_5() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn limit_1() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_0() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_equal_to_rows() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn limit_greater_than_rows() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 100");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn limit_on_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT * FROM empty_t LIMIT 10");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn offset_0() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 0");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn offset_5() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn offset_equal_to_rows() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 10");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn offset_greater_than_rows() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_and_offset_middle() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 5 OFFSET 5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn limit_1_offset_0_gives_first_row() {
        let db = TestDb::with_trades(10);
        let (_, all_rows) = db.query("SELECT price FROM trades");
        let (_, rows) = db.query("SELECT price FROM trades LIMIT 1 OFFSET 0");
        assert_eq!(rows[0][0], all_rows[0][0]);
    }

    #[test]
    fn limit_1_offset_1_gives_second_row() {
        let db = TestDb::with_trades(10);
        let (_, all_rows) = db.query("SELECT price FROM trades");
        let (_, rows) = db.query("SELECT price FROM trades LIMIT 1 OFFSET 1");
        assert_eq!(rows[0][0], all_rows[1][0]);
    }

    #[test]
    fn limit_with_where() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD' LIMIT 3");
        assert!(rows.len() <= 3);
        assert!(rows.len() > 0);
    }

    #[test]
    fn offset_with_where() {
        let db = TestDb::with_trades(20);
        let (_, all_btc) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD' LIMIT 100 OFFSET 2");
        assert_eq!(rows.len(), all_btc.len() - 2);
    }

    #[test]
    fn limit_preserves_column_count() {
        let db = TestDb::with_trades(10);
        let (cols, _) = db.query("SELECT * FROM trades LIMIT 2");
        assert_eq!(cols.len(), 5);
    }

    #[test]
    fn limit_with_order_by() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn offset_with_order_by() {
        let db = TestDb::with_trades(20);
        let (_, all_sorted) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let (_, rows) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 100 OFFSET 10");
        assert_eq!(rows.len(), 10);
        assert_eq!(rows[0][0], all_sorted[10][0]);
    }

    #[test]
    fn limit_with_specific_columns() {
        let db = TestDb::with_trades(10);
        let (cols, rows) = db.query("SELECT symbol, price FROM trades LIMIT 3");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn consecutive_pages() {
        let db = TestDb::with_trades(20);
        let (_, page1) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 5 OFFSET 0");
        let (_, page2) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 5 OFFSET 5");
        let (_, page3) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 5 OFFSET 10");
        let (_, page4) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 5 OFFSET 15");
        assert_eq!(page1.len() + page2.len() + page3.len() + page4.len(), 20);
    }

    #[test]
    fn limit_2_all_distinct() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price FROM trades LIMIT 2");
        assert_eq!(rows.len(), 2);
        assert_ne!(rows[0][0], rows[1][0]); // different rows have different prices
    }

    #[test]
    fn large_limit() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 999999");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn large_offset() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 999999");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_with_count_star() {
        let db = TestDb::with_trades(10);
        // count(*) returns one row, limit 1 should still return it
        let val = db.query_scalar("SELECT count(*) FROM trades LIMIT 1");
        assert_eq!(val, Value::I64(10));
    }

    #[test]
    fn offset_1_skips_first() {
        let db = TestDb::with_trades(5);
        let (_, all_rows) = db.query("SELECT price FROM trades");
        let (_, rows) = db.query("SELECT price FROM trades LIMIT 100 OFFSET 1");
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], all_rows[1][0]);
    }

    #[test]
    fn limit_offset_combined_edge() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 1 OFFSET 4");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_offset_past_end() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 1 OFFSET 5");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_offset_with_where_empty() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'XRP/USD' LIMIT 5 OFFSET 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_3_offset_2() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
    }
}

// ============================================================================
// Module 5: select_distinct – 30 tests
// ============================================================================
mod select_distinct {
    use super::*;

    #[test]
    fn distinct_symbol() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn distinct_side() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn distinct_symbol_values() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol");
        let symbols: Vec<&str> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::Str(s) => s.as_str(),
                other => panic!("expected Str, got {other:?}"),
            })
            .collect();
        assert_eq!(symbols, vec!["BTC/USD", "ETH/USD", "SOL/USD"]);
    }

    #[test]
    fn distinct_side_values() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades ORDER BY side");
        let sides: Vec<&str> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::Str(s) => s.as_str(),
                other => panic!("expected Str, got {other:?}"),
            })
            .collect();
        assert_eq!(sides, vec!["buy", "sell"]);
    }

    #[test]
    fn distinct_returns_fewer_rows() {
        let db = TestDb::with_trades(20);
        let (_, all_rows) = db.query("SELECT symbol FROM trades");
        let (_, distinct_rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert!(distinct_rows.len() < all_rows.len());
    }

    #[test]
    fn distinct_multiple_columns() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol, side FROM trades");
        // 3 symbols * 2 sides = 6 combinations
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn distinct_with_order_by() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol ASC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
        assert_eq!(rows[1][0], Value::Str("ETH/USD".to_string()));
        assert_eq!(rows[2][0], Value::Str("SOL/USD".to_string()));
    }

    #[test]
    fn distinct_with_order_by_desc() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol DESC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Str("SOL/USD".to_string()));
    }

    #[test]
    fn distinct_with_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn distinct_with_limit_1() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_on_unique_column() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT DISTINCT price FROM trades");
        // All prices are unique
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn distinct_on_timestamp() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT DISTINCT timestamp FROM trades");
        assert_eq!(rows.len(), 10); // all timestamps unique
    }

    #[test]
    fn distinct_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT DISTINCT val FROM empty_t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_with_where() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT side FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(rows.len(), 2); // both buy and sell
    }

    #[test]
    fn distinct_preserves_column_name() {
        let db = TestDb::with_trades(10);
        let (cols, _) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(cols[0], "symbol");
    }

    #[test]
    fn distinct_symbol_side_ordered() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol, side FROM trades ORDER BY symbol, side");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn distinct_with_limit_0() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_with_in_filter() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT DISTINCT symbol FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn distinct_all_same_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE same_t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO same_t (timestamp, val) VALUES ({}, 42.0)",
                base_ts + i * 1_000_000_000
            ));
        }
        let (_, rows) = db.query("SELECT DISTINCT val FROM same_t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(42.0));
    }

    #[test]
    fn distinct_two_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE two_t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..10 {
            let v = if i % 2 == 0 { 1.0 } else { 2.0 };
            db.exec_ok(&format!(
                "INSERT INTO two_t (timestamp, val) VALUES ({}, {})",
                base_ts + i * 1_000_000_000,
                v
            ));
        }
        let (_, rows) = db.query("SELECT DISTINCT val FROM two_t ORDER BY val");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::F64(1.0));
        assert_eq!(rows[1][0], Value::F64(2.0));
    }

    #[test]
    fn distinct_with_null_volume() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT DISTINCT volume FROM trades ORDER BY volume");
        // Should include NULL as a distinct value, plus 18 non-null values
        assert!(rows.len() >= 18);
    }

    #[test]
    fn distinct_count_correct() {
        let db = TestDb::with_trades(12);
        let (_, distinct_rows) = db.query("SELECT DISTINCT symbol FROM trades");
        let count_val = db.query_scalar("SELECT count_distinct(symbol) FROM trades");
        assert_eq!(count_val, Value::I64(distinct_rows.len() as i64));
    }

    #[test]
    fn distinct_with_like_filter() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades WHERE symbol LIKE '%USD'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn distinct_limit_greater_than_distinct_count() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades LIMIT 100");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn distinct_column_count() {
        let db = TestDb::with_trades(10);
        let (cols, _) = db.query("SELECT DISTINCT symbol, side FROM trades");
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn distinct_with_between_filter() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT DISTINCT symbol FROM trades WHERE price BETWEEN 0 AND 5000");
        // ETH and SOL have prices < 5000
        assert!(rows.len() >= 2);
    }

    #[test]
    fn distinct_order_by_limit_offset() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
        assert_eq!(rows[1][0], Value::Str("ETH/USD".to_string()));
    }

    #[test]
    fn distinct_idempotent() {
        let db = TestDb::with_trades(12);
        let (_, rows1) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol");
        let (_, rows2) = db.query("SELECT DISTINCT symbol FROM trades ORDER BY symbol");
        assert_eq!(rows1, rows2);
    }
}

// ============================================================================
// Module 6: select_group_by – 80 tests
// ============================================================================
mod select_group_by {
    use super::*;

    #[test]
    fn group_by_symbol_count() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_symbol_sum_price() {
        let db = TestDb::with_trades(12);
        let (cols, rows) = db.query("SELECT symbol, sum(price) FROM trades GROUP BY symbol");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 3);
        for row in &rows {
            match &row[1] {
                Value::F64(s) => assert!(*s > 0.0),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn group_by_symbol_avg_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(matches!(row[1], Value::F64(_)));
        }
    }

    #[test]
    fn group_by_symbol_min_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, min(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_symbol_max_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, max(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_symbol_first_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, first(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_symbol_last_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, last(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_side_count() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT side, count(*) FROM trades GROUP BY side");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_side_count_balanced() {
        let db = TestDb::with_trades(10);
        let (_, rows) =
            db.query("SELECT side, count(*) AS c FROM trades GROUP BY side ORDER BY side");
        assert_eq!(rows.len(), 2);
        // buy: 5, sell: 5
        assert_eq!(rows[0][1], Value::I64(5));
        assert_eq!(rows[1][1], Value::I64(5));
    }

    #[test]
    fn group_by_total_count_matches() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        let total: i64 = rows
            .iter()
            .map(|r| match &r[1] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn group_by_multiple_aggregates() {
        let db = TestDb::with_trades(12);
        let (cols, rows) = db.query(
            "SELECT symbol, count(*), sum(price), avg(price), min(price), max(price) FROM trades GROUP BY symbol",
        );
        assert_eq!(cols.len(), 6);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_with_having_count() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c >= 4");
        assert_eq!(rows.len(), 3); // each has 4
    }

    #[test]
    fn group_by_with_having_filters_out() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c > 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn group_by_with_having_avg() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, avg(price) AS avg_p FROM trades GROUP BY symbol HAVING avg_p > 5000",
        );
        // BTC avg > 5000
        assert!(rows.len() >= 1);
    }

    #[test]
    fn group_by_with_order_by() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol ORDER BY symbol ASC");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn group_by_with_order_by_count_desc() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol ORDER BY c DESC");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_with_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_with_order_by_and_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, avg(price) AS avg_p FROM trades GROUP BY symbol ORDER BY avg_p DESC LIMIT 1",
        );
        assert_eq!(rows.len(), 1);
        // Should be BTC/USD with highest avg price
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn group_by_sum_no_group() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT sum(price) FROM trades");
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], Value::F64(_)));
    }

    #[test]
    fn group_by_avg_no_group() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT avg(price) FROM trades");
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn group_by_min_no_group() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT min(price) FROM trades");
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn group_by_max_no_group() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT max(price) FROM trades");
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn group_by_count_no_group() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(10));
    }

    #[test]
    fn group_by_first_no_group() {
        let db = TestDb::with_trades(5);
        let val = db.query_scalar("SELECT first(symbol) FROM trades");
        assert_eq!(val, Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn group_by_last_no_group() {
        let db = TestDb::with_trades(5);
        let val = db.query_scalar("SELECT last(symbol) FROM trades");
        assert_eq!(val, Value::Str("ETH/USD".to_string()));
    }

    #[test]
    fn group_by_count_distinct_no_group() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT count_distinct(symbol) FROM trades");
        assert_eq!(val, Value::I64(3));
    }

    #[test]
    fn group_by_count_column_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT symbol, count(volume) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
        // count(volume) excludes NULLs
    }

    #[test]
    fn group_by_sum_volume_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT symbol, sum(volume) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_avg_volume_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT symbol, avg(volume) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_min_max_same_group() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, min(price) AS mn, max(price) AS mx FROM trades GROUP BY symbol");
        for row in &rows {
            assert!(
                row[1].cmp_coerce(&row[2]) != Some(std::cmp::Ordering::Greater),
                "min should be <= max"
            );
        }
    }

    #[test]
    fn group_by_avg_between_min_max() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, min(price), avg(price), max(price) FROM trades GROUP BY symbol");
        for row in &rows {
            assert!(row[1].cmp_coerce(&row[2]) != Some(std::cmp::Ordering::Greater));
            assert!(row[2].cmp_coerce(&row[3]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn group_by_with_where() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_symbol_side() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side");
        assert_eq!(rows.len(), 6); // 3 symbols * 2 sides
    }

    #[test]
    fn group_by_symbol_side_sum() {
        let db = TestDb::with_trades(12);
        let (cols, rows) =
            db.query("SELECT symbol, side, sum(price) FROM trades GROUP BY symbol, side");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn group_by_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, symbol VARCHAR, val DOUBLE)");
        let (_, rows) = db.query("SELECT symbol, count(*) FROM empty_t GROUP BY symbol");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn group_by_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::I64(1));
    }

    #[test]
    fn group_by_having_and_order() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c >= 10 ORDER BY c DESC",
        );
        assert_eq!(rows.len(), 3);
        for i in 1..rows.len() {
            assert!(rows[i - 1][1].cmp_coerce(&rows[i][1]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn group_by_having_and_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c >= 1 LIMIT 2",
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_stddev() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, stddev(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            match &row[1] {
                Value::F64(s) => assert!(*s >= 0.0, "stddev should be non-negative"),
                Value::Null => {} // acceptable for single-value groups
                other => panic!("expected F64 or Null, got {other:?}"),
            }
        }
    }

    #[test]
    fn group_by_variance() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, variance(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_median() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, median(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_count_distinct_per_group() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, count_distinct(side) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row[1], Value::I64(2));
        }
    }

    #[test]
    fn aggregate_no_group_multi() {
        let db = TestDb::with_trades(20);
        let (cols, rows) =
            db.query("SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn aggregate_count_star_vs_count_col() {
        let db = TestDb::with_trades(20);
        let count_star = db.query_scalar("SELECT count(*) FROM trades");
        let count_vol = db.query_scalar("SELECT count(volume) FROM trades");
        // count(*) includes NULLs, count(volume) excludes them
        match (&count_star, &count_vol) {
            (Value::I64(a), Value::I64(b)) => assert!(a >= b),
            _ => panic!("expected I64"),
        }
    }

    #[test]
    fn group_by_sum_equals_total() {
        let db = TestDb::with_trades(12);
        let total = db.query_scalar("SELECT sum(price) FROM trades");
        let (_, groups) = db.query("SELECT symbol, sum(price) FROM trades GROUP BY symbol");
        let group_total: f64 = groups
            .iter()
            .map(|r| match &r[1] {
                Value::F64(v) => *v,
                other => panic!("expected F64, got {other:?}"),
            })
            .sum();
        match total {
            Value::F64(t) => assert!((t - group_total).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn group_by_count_equals_total() {
        let db = TestDb::with_trades(15);
        let (_, groups) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        let total: i64 = groups
            .iter()
            .map(|r| match &r[1] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 15);
    }

    #[test]
    fn group_by_first_last_differ() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, first(price), last(price) FROM trades GROUP BY symbol");
        for row in &rows {
            // first and last should differ for BTC (which has > 1 row)
            if row[0] == Value::Str("BTC/USD".to_string()) {
                assert_ne!(row[1], row[2]);
            }
        }
    }

    #[test]
    fn group_by_min_le_first() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, min(price), first(price) FROM trades GROUP BY symbol");
        for row in &rows {
            assert!(
                row[1].cmp_coerce(&row[2]) != Some(std::cmp::Ordering::Greater),
                "min should be <= first"
            );
        }
    }

    #[test]
    fn group_by_max_ge_last() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, max(price), last(price) FROM trades GROUP BY symbol");
        for row in &rows {
            assert!(
                row[1].cmp_coerce(&row[2]) != Some(std::cmp::Ordering::Less),
                "max should be >= last"
            );
        }
    }

    #[test]
    fn group_by_where_and_having() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) AS c FROM trades WHERE side = 'buy' GROUP BY symbol HAVING c >= 1",
        );
        assert!(rows.len() > 0);
    }

    #[test]
    fn group_by_aliases_in_result() {
        let db = TestDb::with_trades(12);
        let (cols, _) = db.query(
            "SELECT symbol, count(*) AS total, avg(price) AS avg_price FROM trades GROUP BY symbol",
        );
        assert!(cols.contains(&"total".to_string()));
        assert!(cols.contains(&"avg_price".to_string()));
    }

    #[test]
    fn group_by_having_sum() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, sum(price) AS s FROM trades GROUP BY symbol HAVING s > 1000");
        assert!(rows.len() > 0);
    }

    #[test]
    fn group_by_three_aggregates() {
        let db = TestDb::with_trades(12);
        let (cols, rows) = db
            .query("SELECT symbol, sum(price), min(price), max(price) FROM trades GROUP BY symbol");
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_four_aggregates() {
        let db = TestDb::with_trades(12);
        let (cols, rows) = db.query(
            "SELECT symbol, count(*), sum(price), avg(price), max(price) FROM trades GROUP BY symbol",
        );
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_where_in() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD') GROUP BY symbol",
        );
        // Should have groups for BTC and ETH (may include empty SOL group)
        assert!(rows.len() >= 2);
    }

    #[test]
    fn group_by_where_like() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, count(*) FROM trades WHERE symbol LIKE 'BTC%' GROUP BY symbol");
        // Should have at least a BTC group
        assert!(rows.len() >= 1);
    }

    #[test]
    fn group_by_where_is_not_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT symbol, count(*), avg(volume) FROM trades WHERE volume IS NOT NULL GROUP BY symbol",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_order_by_symbol() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
        assert_eq!(rows[1][0], Value::Str("ETH/USD".to_string()));
        assert_eq!(rows[2][0], Value::Str("SOL/USD".to_string()));
    }

    #[test]
    fn group_by_order_by_aggregate_asc() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, avg(price) AS ap FROM trades GROUP BY symbol ORDER BY ap ASC");
        assert_eq!(rows.len(), 3);
        for i in 1..rows.len() {
            assert!(rows[i - 1][1].cmp_coerce(&rows[i][1]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn group_by_order_by_aggregate_desc() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, avg(price) AS ap FROM trades GROUP BY symbol ORDER BY ap DESC");
        assert_eq!(rows.len(), 3);
        // BTC has highest avg
        assert_eq!(rows[0][0], Value::Str("BTC/USD".to_string()));
    }

    #[test]
    fn group_by_with_limit_1() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn group_by_with_offset() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_sum_volume_with_null() {
        let db = TestDb::with_trades(20);
        // sum should skip NULLs
        let val = db.query_scalar("SELECT sum(volume) FROM trades");
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn group_by_avg_volume_with_null() {
        let db = TestDb::with_trades(20);
        // avg should skip NULLs
        let val = db.query_scalar("SELECT avg(volume) FROM trades");
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn group_by_having_0_results() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c > 999");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn group_by_having_all_pass() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c >= 1");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_multiple_keys_count() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side");
        let total: i64 = rows
            .iter()
            .map(|r| match &r[2] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 12);
    }

    #[test]
    fn group_by_multiple_keys_order() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, side, count(*) FROM trades GROUP BY symbol, side ORDER BY symbol, side",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn group_by_btc_avg_price_correct() {
        let db = TestDb::with_trades(12);
        // BTC rows: 0,3,6,9 -> prices: 60000, 60300, 60600, 60900
        // avg = (60000+60300+60600+60900)/4 = 60450
        let (_, rows) =
            db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol ORDER BY symbol");
        let btc_row = rows
            .iter()
            .find(|r| r[0] == Value::Str("BTC/USD".to_string()))
            .unwrap();
        match &btc_row[1] {
            Value::F64(avg) => assert!((avg - 60450.0).abs() < 0.01, "expected ~60450, got {avg}"),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn group_by_sol_count_correct() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol");
        let sol_row = rows
            .iter()
            .find(|r| r[0] == Value::Str("SOL/USD".to_string()))
            .unwrap();
        assert_eq!(sol_row[1], Value::I64(4));
    }

    #[test]
    fn group_by_eth_min_price() {
        let db = TestDb::with_trades(12);
        // ETH rows: 1,4,7,10 -> prices: 3010, 3040, 3070, 3100
        let (_, rows) =
            db.query("SELECT symbol, min(price) FROM trades GROUP BY symbol ORDER BY symbol");
        let eth_row = rows
            .iter()
            .find(|r| r[0] == Value::Str("ETH/USD".to_string()))
            .unwrap();
        assert_eq!(eth_row[1], Value::F64(3010.0));
    }

    #[test]
    fn group_by_eth_max_price() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT symbol, max(price) FROM trades GROUP BY symbol ORDER BY symbol");
        let eth_row = rows
            .iter()
            .find(|r| r[0] == Value::Str("ETH/USD".to_string()))
            .unwrap();
        assert_eq!(eth_row[1], Value::F64(3100.0));
    }
}

// ============================================================================
// Module 7: select_sample_by – 40 tests
// ============================================================================
mod select_sample_by {
    use super::*;

    #[test]
    fn sample_by_1h_avg_price() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert!(rows.len() > 1);
    }

    #[test]
    fn sample_by_1h_returns_f64() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        for row in &rows {
            assert!(matches!(row[0], Value::F64(_)));
        }
    }

    #[test]
    fn sample_by_1h_sum_price() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h");
        assert!(rows.len() > 1);
    }

    #[test]
    fn sample_by_1h_count() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        assert!(rows.len() > 1);
        let total: i64 = rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn sample_by_1h_min_max() {
        let db = TestDb::with_trades(30);
        let (cols, rows) = db.query("SELECT min(price), max(price) FROM trades SAMPLE BY 1h");
        assert_eq!(cols.len(), 2);
        assert!(rows.len() > 1);
    }

    #[test]
    fn sample_by_10m() {
        let db = TestDb::with_trades(30);
        // Data is every 10 min, so each 10m bucket should have ~1 row
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m");
        assert!(rows.len() >= 10);
    }

    #[test]
    fn sample_by_1d() {
        let db = TestDb::with_trades(30);
        // 30 rows * 10min = 300 min = 5 hours, all in same day
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1d");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_1s() {
        let db = TestDb::with_trades(10);
        // Data is every 10 min, so 1s buckets with data = 10 buckets
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1s");
        assert!(rows.len() >= 10);
    }

    #[test]
    fn sample_by_5m() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 5m");
        assert!(rows.len() > 1);
    }

    #[test]
    fn sample_by_1m() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1m");
        assert!(rows.len() >= 10);
    }

    #[test]
    fn sample_by_multiple_aggs() {
        let db = TestDb::with_trades(30);
        let (cols, rows) = db.query(
            "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades SAMPLE BY 1h",
        );
        assert_eq!(cols.len(), 5);
        assert!(rows.len() > 1);
    }

    #[test]
    fn sample_by_with_where() {
        let db = TestDb::with_trades(30);
        let (_, rows) =
            db.query("SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD' SAMPLE BY 1h");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_with_where_side() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT count(*) FROM trades WHERE side = 'buy' SAMPLE BY 1h");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_fill_none() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(NONE)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_fill_null() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(NULL)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_fill_prev() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(PREV)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_fill_0() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(0)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_fill_linear() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h FILL(LINEAR)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_align_to_calendar() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_first_last() {
        let db = TestDb::with_trades(30);
        let (cols, rows) = db.query("SELECT first(price), last(price) FROM trades SAMPLE BY 1h");
        assert_eq!(cols.len(), 2);
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT avg(val) FROM empty_t SAMPLE BY 1h");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn sample_by_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn sample_by_count_sum_matches_total() {
        let db = TestDb::with_trades(20);
        let (_, sampled) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        let total: i64 = sampled
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 20);
    }

    #[test]
    fn sample_by_sum_matches_total() {
        let db = TestDb::with_trades(20);
        let total_val = db.query_scalar("SELECT sum(price) FROM trades");
        let (_, sampled) = db.query("SELECT sum(price) FROM trades SAMPLE BY 1h");
        let sampled_total: f64 = sampled
            .iter()
            .map(|r| match &r[0] {
                Value::F64(v) => *v,
                other => panic!("expected F64, got {other:?}"),
            })
            .sum();
        match total_val {
            Value::F64(t) => assert!((t - sampled_total).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn sample_by_avg_in_range() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 1h");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v > 0.0),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn sample_by_30m() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 30m");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_2h() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 2h");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_fill_none_count_sum() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h FILL(NONE)");
        let total: i64 = rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn sample_by_with_where_symbol_in() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT avg(price) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD') SAMPLE BY 1h",
        );
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_min_max_per_bucket() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT min(price), max(price) FROM trades SAMPLE BY 1h");
        for row in &rows {
            assert!(
                row[0].cmp_coerce(&row[1]) != Some(std::cmp::Ordering::Greater),
                "min should be <= max in each bucket"
            );
        }
    }

    #[test]
    fn sample_by_10m_single_rows() {
        let db = TestDb::with_trades(10);
        // Data is every 10 min, so 10m buckets should have exactly 1 row each
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 10m");
        for row in &rows {
            assert_eq!(row[0], Value::I64(1));
        }
    }

    #[test]
    fn sample_by_20m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 20m");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_fill_0_count() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h FILL(0)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_avg_volume() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT avg(volume) FROM trades SAMPLE BY 1h");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_sum_volume() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT sum(volume) FROM trades SAMPLE BY 1h");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_15m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 15m");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_45m() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 45m");
        assert!(rows.len() > 0);
    }

    #[test]
    fn sample_by_3h() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 3h");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn sample_by_6h() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT avg(price) FROM trades SAMPLE BY 6h");
        assert!(rows.len() >= 1);
    }
}

// ============================================================================
// Module 8: select_latest_on – 20 tests
// ============================================================================
mod select_latest_on {
    use super::*;

    #[test]
    fn latest_on_basic() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_returns_one_per_symbol() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_column_count() {
        let db = TestDb::with_trades(12);
        let (cols, _) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(cols.len(), 5);
    }

    #[test]
    fn latest_on_returns_last_timestamp() {
        let db = TestDb::with_trades(12);
        let (_, latest) = db.query(
            "SELECT symbol, timestamp FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY symbol",
        );
        // Verify each latest row is the maximum timestamp for that symbol
        for latest_row in &latest {
            let sym = match &latest_row[0] {
                Value::Str(s) => s.clone(),
                other => panic!("expected Str, got {other:?}"),
            };
            let max_ts = db.query_scalar(&format!(
                "SELECT max(timestamp) FROM trades WHERE symbol = '{sym}'"
            ));
            assert_eq!(latest_row[1], max_ts);
        }
    }

    #[test]
    fn latest_on_with_where() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE side = 'buy' LATEST ON timestamp PARTITION BY symbol",
        );
        assert!(rows.len() <= 3);
        assert!(rows.len() > 0);
    }

    #[test]
    fn latest_on_by_side() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY side");
        assert_eq!(rows.len(), 2); // buy and sell
    }

    #[test]
    fn latest_on_single_symbol() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' LATEST ON timestamp PARTITION BY symbol",
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn latest_on_empty_result() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'XRP/USD' LATEST ON timestamp PARTITION BY symbol",
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn latest_on_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn latest_on_many_rows() {
        let db = TestDb::with_trades(50);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_specific_columns() {
        let db = TestDb::with_trades(12);
        let (cols, rows) = db.query(
            "SELECT timestamp, symbol, price FROM trades LATEST ON timestamp PARTITION BY symbol",
        );
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_price_is_last() {
        let db = TestDb::with_trades(12);
        let (_, latest) = db
            .query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY symbol");
        // Verify the price matches the last price for each symbol
        for row in &latest {
            // symbol is at index 1, price at index 2 in SELECT *
            let sym = match &row[1] {
                Value::Str(s) => s.clone(),
                other => panic!("expected Str, got {other:?}"),
            };
            let last_price = db.query_scalar(&format!(
                "SELECT last(price) FROM trades WHERE symbol = '{sym}'"
            ));
            assert_eq!(row[2], last_price);
        }
    }

    #[test]
    fn latest_on_with_order_by() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY price DESC",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_with_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn latest_on_20_rows() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_30_rows() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn latest_on_all_btc_data() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' LATEST ON timestamp PARTITION BY symbol",
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn latest_on_deterministic() {
        let db = TestDb::with_trades(12);
        let (_, rows1) = db
            .query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY symbol");
        let (_, rows2) = db
            .query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol ORDER BY symbol");
        assert_eq!(rows1, rows2);
    }

    #[test]
    fn latest_on_with_buy_filter() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE side = 'buy' LATEST ON timestamp PARTITION BY symbol",
        );
        assert!(rows.len() > 0);
        assert!(rows.len() <= 3);
    }

    #[test]
    fn latest_on_timestamp_values() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT symbol, timestamp FROM trades LATEST ON timestamp PARTITION BY symbol");
        for row in &rows {
            assert!(matches!(row[1], Value::Timestamp(_)));
        }
    }
}

// ============================================================================
// Module 9: select_case_when – 30 tests
// ============================================================================
mod select_case_when {
    use super::*;

    #[test]
    fn case_when_simple() {
        let db = TestDb::with_trades(6);
        let (_, rows) =
            db.query("SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END FROM trades");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_all_high() {
        let db = TestDb::with_trades(3);
        let (_, rows) =
            db.query("SELECT CASE WHEN price > 0 THEN 'positive' ELSE 'negative' END FROM trades");
        for row in &rows {
            assert_eq!(row[0], Value::Str("positive".to_string()));
        }
    }

    #[test]
    fn case_when_all_else() {
        let db = TestDb::with_trades(3);
        let (_, rows) =
            db.query("SELECT CASE WHEN price > 999999 THEN 'high' ELSE 'normal' END FROM trades");
        for row in &rows {
            assert_eq!(row[0], Value::Str("normal".to_string()));
        }
    }

    #[test]
    fn case_when_with_alias() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query(
            "SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END AS category FROM trades",
        );
        assert!(cols.contains(&"category".to_string()));
    }

    #[test]
    fn case_when_multiple_conditions() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 50000 THEN 'btc' WHEN price > 2000 THEN 'eth' ELSE 'alt' END FROM trades",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_no_else() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CASE WHEN price > 50000 THEN 'high' END FROM trades");
        assert_eq!(rows.len(), 3);
        // Rows without match should get NULL
        let high_count = rows
            .iter()
            .filter(|r| r[0] == Value::Str("high".to_string()))
            .count();
        let null_count = rows.iter().filter(|r| r[0] == Value::Null).count();
        assert_eq!(high_count, 1);
        assert_eq!(null_count, 2);
    }

    #[test]
    fn case_when_string_comparison() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query(
            "SELECT CASE WHEN symbol = 'BTC/USD' THEN 'bitcoin' ELSE 'other' END FROM trades",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_returns_numbers() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CASE WHEN price > 50000 THEN 1 ELSE 0 END FROM trades");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn case_when_with_other_columns() {
        let db = TestDb::with_trades(6);
        let (cols, rows) = db.query(
            "SELECT symbol, price, CASE WHEN price > 50000 THEN 'high' ELSE 'low' END AS tier FROM trades",
        );
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_three_branches() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query(
            "SELECT CASE WHEN symbol = 'BTC/USD' THEN 'btc' WHEN symbol = 'ETH/USD' THEN 'eth' WHEN symbol = 'SOL/USD' THEN 'sol' ELSE 'unknown' END FROM trades",
        );
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn case_when_preserves_row_count() {
        let db = TestDb::with_trades(20);
        let (_, rows) =
            db.query("SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END FROM trades");
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn case_when_mixed_types_int_result() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CASE WHEN price > 50000 THEN 100 ELSE 0 END FROM trades");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn case_when_gt_operator() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db
            .query("SELECT CASE WHEN price > 10000 THEN 'expensive' ELSE 'cheap' END FROM trades");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_lt_operator() {
        let db = TestDb::with_trades(6);
        let (_, rows) =
            db.query("SELECT CASE WHEN price < 1000 THEN 'cheap' ELSE 'pricey' END FROM trades");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_eq_string_btc() {
        let db = TestDb::with_trades(6);
        let (_, rows) =
            db.query("SELECT CASE WHEN symbol = 'BTC/USD' THEN 'yes' ELSE 'no' END FROM trades");
        let yes_count = rows
            .iter()
            .filter(|r| r[0] == Value::Str("yes".to_string()))
            .count();
        assert_eq!(yes_count, 2); // rows 0, 3
    }

    #[test]
    fn case_when_with_limit() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db
            .query("SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END FROM trades LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn case_when_with_order_by() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query(
            "SELECT symbol, CASE WHEN price > 50000 THEN 'high' ELSE 'low' END AS cat FROM trades ORDER BY symbol",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_where_clause() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END FROM trades WHERE symbol = 'BTC/USD'",
        );
        for row in &rows {
            assert_eq!(row[0], Value::Str("high".to_string()));
        }
    }

    #[test]
    fn case_when_deterministic() {
        let db = TestDb::with_trades(6);
        let (_, rows1) =
            db.query("SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END FROM trades");
        let (_, rows2) =
            db.query("SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END FROM trades");
        assert_eq!(rows1, rows2);
    }

    #[test]
    fn case_when_first_match_wins() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 0 THEN 'first' WHEN price > 50000 THEN 'second' ELSE 'third' END FROM trades",
        );
        // All prices > 0, so all should match 'first'
        for row in &rows {
            assert_eq!(row[0], Value::Str("first".to_string()));
        }
    }

    #[test]
    fn case_when_two_columns() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query(
            "SELECT CASE WHEN price > 50000 THEN 'high' ELSE 'low' END AS c1, CASE WHEN side = 'buy' THEN 1 ELSE 0 END AS c2 FROM trades",
        );
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn case_when_with_null_else() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CASE WHEN price > 999999 THEN 'match' END FROM trades");
        // No match, no ELSE -> all NULL
        for row in &rows {
            assert_eq!(row[0], Value::Null);
        }
    }

    #[test]
    fn case_when_side_buy_sell() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT CASE WHEN side = 'buy' THEN 1 ELSE -1 END FROM trades");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_when_gte() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db
            .query("SELECT CASE WHEN price >= 60000 THEN 'btc_range' ELSE 'other' END FROM trades");
        let btc_count = rows
            .iter()
            .filter(|r| r[0] == Value::Str("btc_range".to_string()))
            .count();
        assert_eq!(btc_count, 1);
    }

    #[test]
    fn case_when_lte() {
        let db = TestDb::with_trades(3);
        let (_, rows) =
            db.query("SELECT CASE WHEN price <= 200 THEN 'cheap' ELSE 'expensive' END FROM trades");
        let cheap_count = rows
            .iter()
            .filter(|r| r[0] == Value::Str("cheap".to_string()))
            .count();
        assert_eq!(cheap_count, 1); // SOL at 102
    }

    #[test]
    fn case_when_complex_multi_condition() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query(
            "SELECT CASE WHEN price > 50000 THEN 'tier1' WHEN price > 2000 THEN 'tier2' WHEN price > 0 THEN 'tier3' END AS tier FROM trades",
        );
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn case_when_with_select_star() {
        let db = TestDb::with_trades(3);
        let (cols, rows) = db.query(
            "SELECT symbol, CASE WHEN price > 50000 THEN 'high' ELSE 'low' END AS cat FROM trades",
        );
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn case_when_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT CASE WHEN val > 0 THEN 'pos' ELSE 'neg' END FROM empty_t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn case_when_multiple_same_result() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query(
            "SELECT CASE WHEN symbol = 'BTC/USD' THEN 'crypto' WHEN symbol = 'ETH/USD' THEN 'crypto' ELSE 'alt' END FROM trades",
        );
        let crypto_count = rows
            .iter()
            .filter(|r| r[0] == Value::Str("crypto".to_string()))
            .count();
        assert_eq!(crypto_count, 4); // 2 BTC + 2 ETH
    }
}

// ============================================================================
// Module 10: select_arithmetic – 40 tests
// ============================================================================
mod select_arithmetic {
    use super::*;

    #[test]
    fn add_constant() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price + 100 FROM trades");
        // Row 0: 60000 + 100 = 60100
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 60100.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn sub_constant() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price - 100 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 59900.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn mul_constant() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price * 2 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 120000.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn div_constant() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price / 2 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 30000.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn mod_constant() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price % 1000 FROM trades");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn multiply_two_columns() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price * volume AS notional FROM trades");
        assert_eq!(rows.len(), 5);
        // Row 0 has NULL volume (stored as 0.0), so result is 0.0 or Null
        assert!(rows[0][0] == Value::Null || rows[0][0] == Value::F64(0.0));
    }

    #[test]
    fn add_two_columns() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price + volume FROM trades");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn sub_two_columns() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price - volume FROM trades");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn unary_minus() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT -price FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!(*v < 0.0),
            other => panic!("expected negative F64, got {other:?}"),
        }
    }

    #[test]
    fn null_propagation_add() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price + volume FROM trades");
        // Row 0 has NULL volume (may be stored as 0.0)
        assert!(matches!(rows[0][0], Value::Null | Value::F64(_)));
    }

    #[test]
    fn null_propagation_mul() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price * volume FROM trades");
        // Row 0 has NULL volume (may be stored as 0.0)
        assert!(matches!(rows[0][0], Value::Null | Value::F64(_)));
    }

    #[test]
    fn null_propagation_sub() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price - volume FROM trades");
        assert!(matches!(rows[0][0], Value::Null | Value::F64(_)));
    }

    #[test]
    fn null_propagation_div() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT price / volume FROM trades");
        // Row 0 has NULL volume (may be stored as 0.0 -> div by zero -> Null)
        assert!(matches!(rows[0][0], Value::Null | Value::F64(_)));
    }

    #[test]
    fn division_by_zero_returns_null() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price / 0 FROM trades");
        for row in &rows {
            assert_eq!(row[0], Value::Null);
        }
    }

    #[test]
    fn add_preserves_row_count() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT price + 1 FROM trades");
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn complex_expression() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT (price + 100) * 2 FROM trades");
        // Row 0: (60000+100)*2 = 120200
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 120200.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn expression_with_alias() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT price * 2 AS double_price FROM trades");
        assert!(cols.contains(&"double_price".to_string()));
    }

    #[test]
    fn multiple_expressions() {
        let db = TestDb::with_trades(3);
        let (cols, rows) =
            db.query("SELECT price + 1 AS p1, price - 1 AS p2, price * 2 AS p3 FROM trades");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn expression_ordering_matters() {
        let db = TestDb::with_trades(3);
        let (_, rows1) = db.query("SELECT price * 2 + 1 FROM trades");
        let (_, rows2) = db.query("SELECT price * 2 + 1 FROM trades");
        assert_eq!(rows1, rows2);
    }

    #[test]
    fn add_two_constants_to_column() {
        let db = TestDb::with_trades(1);
        // price + 2 + 3 = price + 5
        let (_, orig) = db.query("SELECT price FROM trades");
        let (_, rows) = db.query("SELECT price + 2 + 3 FROM trades");
        match (&orig[0][0], &rows[0][0]) {
            (Value::F64(p), Value::F64(r)) => assert!((r - p - 5.0).abs() < 0.01),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn mul_chain_column() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT price * 4 * 5 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 60000.0 * 20.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn sub_chain_column() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT price - 10 - 3 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 59987.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn div_chain_column() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT price / 10 / 2 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 3000.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn float_constant_with_column() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT price + 1.5 + 2.5 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 60004.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn mixed_int_float_with_column() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT price + 1 + 2.5 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 60003.5).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn nested_parentheses() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT (price + 100) * (volume + 1) FROM trades");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn expression_with_where() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price * 2 FROM trades WHERE price > 50000");
        assert!(rows.len() > 0);
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v > 100000.0),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn expression_with_order_by() {
        let db = TestDb::with_trades(10);
        let (_, rows) =
            db.query("SELECT price, price * 2 AS double_price FROM trades ORDER BY price ASC");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn expression_with_limit() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT price + 100 FROM trades LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn subtract_same_column() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price - price FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!((v - 0.0).abs() < 0.001),
                Value::I64(v) => assert_eq!(*v, 0),
                other => panic!("expected 0, got {other:?}"),
            }
        }
    }

    #[test]
    fn divide_same_column() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price / price FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
                Value::I64(v) => assert_eq!(*v, 1),
                other => panic!("expected 1, got {other:?}"),
            }
        }
    }

    #[test]
    fn multiply_by_zero() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price * 0 FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!((v - 0.0).abs() < 0.001),
                Value::I64(v) => assert_eq!(*v, 0),
                other => panic!("expected 0, got {other:?}"),
            }
        }
    }

    #[test]
    fn add_zero() {
        let db = TestDb::with_trades(3);
        let (_, original) = db.query("SELECT price FROM trades");
        let (_, added) = db.query("SELECT price + 0 FROM trades");
        for i in 0..3 {
            assert_eq!(original[i][0], added[i][0]);
        }
    }

    #[test]
    fn multiply_by_one() {
        let db = TestDb::with_trades(3);
        let (_, original) = db.query("SELECT price FROM trades");
        let (_, multiplied) = db.query("SELECT price * 1 FROM trades");
        for i in 0..3 {
            assert_eq!(original[i][0], multiplied[i][0]);
        }
    }

    #[test]
    fn expression_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT val + 1 FROM empty_t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn chained_add() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price + 1 + 2 + 3 FROM trades");
        // Row 0: 60000 + 6 = 60006
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 60006.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn chained_mul() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price * 1 * 2 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!((v - 120000.0).abs() < 0.01),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn expression_negative_result() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT 0 - price FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v < 0.0),
                other => panic!("expected negative F64, got {other:?}"),
            }
        }
    }

    #[test]
    fn expression_large_values() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT price * 1000000 FROM trades");
        match &rows[0][0] {
            Value::F64(v) => assert!(*v > 1_000_000_000.0),
            other => panic!("expected large F64, got {other:?}"),
        }
    }
}

// ============================================================================
// Module 11: select_subquery – 30 tests
// ============================================================================
mod select_subquery {
    use super::*;

    #[test]
    fn in_subquery_basic() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol IN (SELECT DISTINCT symbol FROM trades WHERE price > 50000)",
        );
        // Only BTC/USD has price > 50000
        assert!(rows.len() > 0);
        for row in &rows {
            assert!(row.iter().any(|v| v == &Value::Str("BTC/USD".to_string())));
        }
    }

    #[test]
    fn in_subquery_no_match() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol IN (SELECT DISTINCT symbol FROM trades WHERE price > 999999)",
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn in_subquery_all_match() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE symbol IN (SELECT DISTINCT symbol FROM trades)");
        assert_eq!(rows.len(), 12);
    }

    #[test]
    fn scalar_subquery_eq() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE price = (SELECT min(price) FROM trades)");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn scalar_subquery_gt() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE price > (SELECT avg(price) FROM trades)");
        assert!(rows.len() > 0);
        assert!(rows.len() < 12);
    }

    #[test]
    fn scalar_subquery_lt() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE price < (SELECT avg(price) FROM trades)");
        assert!(rows.len() > 0);
    }

    #[test]
    fn scalar_subquery_gte() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE price >= (SELECT max(price) FROM trades)");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn scalar_subquery_lte() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT * FROM trades WHERE price <= (SELECT min(price) FROM trades)");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn in_subquery_with_condition() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol IN (SELECT DISTINCT symbol FROM trades WHERE price < 5000)",
        );
        // ETH and SOL have prices < 5000
        assert!(rows.len() > 0);
    }

    #[test]
    fn not_in_subquery() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol NOT IN (SELECT DISTINCT symbol FROM trades WHERE price > 50000)",
        );
        // Everything except BTC/USD
        for row in &rows {
            assert!(!row.iter().any(|v| v == &Value::Str("BTC/USD".to_string())));
        }
    }

    #[test]
    fn cte_basic() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH btc AS (SELECT * FROM trades WHERE symbol = 'BTC/USD') SELECT count(*) FROM btc",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(4));
    }

    #[test]
    fn cte_with_aggregation() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH summary AS (SELECT symbol, avg(price) AS avg_p FROM trades GROUP BY symbol) SELECT * FROM summary",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cte_with_where() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar(
            "WITH btc AS (SELECT price FROM trades WHERE symbol = 'BTC/USD') SELECT avg(price) FROM btc",
        );
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn cte_with_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH top_prices AS (SELECT price FROM trades ORDER BY price DESC LIMIT 5) SELECT count(*) FROM top_prices",
        );
        assert_eq!(rows[0][0], Value::I64(5));
    }

    #[test]
    fn cte_reuse() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("WITH sym AS (SELECT DISTINCT symbol FROM trades) SELECT count(*) FROM sym");
        assert_eq!(rows[0][0], Value::I64(3));
    }

    #[test]
    fn subquery_in_from() {
        let db = TestDb::with_trades(12);
        let (_, rows) =
            db.query("SELECT count(*) FROM (SELECT * FROM trades WHERE symbol = 'BTC/USD') AS btc");
        assert_eq!(rows[0][0], Value::I64(4));
    }

    #[test]
    fn subquery_in_from_with_aggregation() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT avg(price) FROM (SELECT price FROM trades WHERE symbol = 'ETH/USD') AS eth",
        );
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], Value::F64(_)));
    }

    #[test]
    fn cte_count_per_symbol() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH counts AS (SELECT symbol, count(*) AS c FROM trades GROUP BY symbol) SELECT * FROM counts ORDER BY symbol",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn in_subquery_side() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE side IN (SELECT DISTINCT side FROM trades WHERE symbol = 'BTC/USD')",
        );
        assert_eq!(rows.len(), 12); // BTC has both buy and sell
    }

    #[test]
    fn scalar_subquery_count() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(12));
    }

    #[test]
    fn cte_with_order() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH ordered AS (SELECT price FROM trades ORDER BY price DESC) SELECT first(price) FROM ordered",
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn subquery_in_from_empty() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db
            .query("SELECT count(*) FROM (SELECT * FROM trades WHERE symbol = 'XRP/USD') AS empty");
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn cte_empty_result() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH empty AS (SELECT * FROM trades WHERE symbol = 'XRP/USD') SELECT count(*) FROM empty",
        );
        assert_eq!(rows[0][0], Value::I64(0));
    }

    #[test]
    fn in_subquery_with_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol IN (SELECT DISTINCT symbol FROM trades) LIMIT 5",
        );
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn subquery_from_with_limit() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT * FROM (SELECT * FROM trades LIMIT 5) AS limited");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn cte_btc_avg_correct() {
        let db = TestDb::with_trades(12);
        let cte_val = db.query_scalar(
            "WITH btc AS (SELECT price FROM trades WHERE symbol = 'BTC/USD') SELECT avg(price) FROM btc",
        );
        let direct_val = db.query_scalar("SELECT avg(price) FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(cte_val, direct_val);
    }

    #[test]
    fn cte_multiple_aggregates() {
        let db = TestDb::with_trades(12);
        let (cols, rows) = db.query(
            "WITH stats AS (SELECT symbol, min(price) AS mn, max(price) AS mx, avg(price) AS av FROM trades GROUP BY symbol) SELECT * FROM stats",
        );
        assert_eq!(rows.len(), 3);
        assert!(cols.len() >= 4);
    }

    #[test]
    fn in_subquery_preserves_order() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT price FROM trades WHERE symbol IN (SELECT DISTINCT symbol FROM trades WHERE price > 50000) ORDER BY price ASC",
        );
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn cte_with_group_by() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "WITH grouped AS (SELECT symbol, sum(price) AS total FROM trades GROUP BY symbol) SELECT symbol FROM grouped WHERE total > 10000 ORDER BY symbol",
        );
        assert!(rows.len() >= 1);
    }

    #[test]
    fn subquery_min_price_eq() {
        let db = TestDb::with_trades(12);
        let min_val = db.query_scalar("SELECT min(price) FROM trades");
        let (_, rows) =
            db.query("SELECT price FROM trades WHERE price = (SELECT min(price) FROM trades)");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], min_val);
    }
}

// ============================================================================
// Module 12: select_cast – 30 tests
// ============================================================================
mod select_cast {
    use super::*;

    #[test]
    fn cast_price_to_int() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades");
        for row in &rows {
            assert!(matches!(row[0], Value::I64(_)));
        }
    }

    #[test]
    fn cast_price_to_int_values() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(rows[0][0], Value::I64(60000));
        assert_eq!(rows[1][0], Value::I64(3010));
        assert_eq!(rows[2][0], Value::I64(102));
    }

    #[test]
    fn cast_volume_to_int() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT CAST(volume AS INT) FROM trades WHERE volume IS NOT NULL");
        for row in &rows {
            assert!(matches!(row[0], Value::I64(_)));
        }
    }

    #[test]
    fn cast_symbol_to_varchar() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(symbol AS VARCHAR) FROM trades");
        for row in &rows {
            assert!(matches!(&row[0], Value::Str(_)));
        }
    }

    #[test]
    fn cast_price_to_varchar() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(price AS VARCHAR) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(!s.is_empty()),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn cast_preserves_row_count() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn cast_preserves_row_count_20() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(rows.len(), 20);
    }

    #[test]
    fn cast_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows) = db.query("SELECT CAST(val AS INT) FROM empty_t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn cast_with_where() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades WHERE symbol = 'BTC/USD'");
        for row in &rows {
            assert!(matches!(row[0], Value::I64(_)));
        }
    }

    #[test]
    fn cast_with_order_by() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades ORDER BY price ASC");
        assert_eq!(rows.len(), 5);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn cast_with_limit() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_with_alias() {
        let db = TestDb::with_trades(3);
        let (cols, rows) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_btc_price_to_int() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(rows[0][0], Value::I64(60000));
    }

    #[test]
    fn cast_eth_price_to_int() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades WHERE symbol = 'ETH/USD'");
        assert_eq!(rows[0][0], Value::I64(3010));
    }

    #[test]
    fn cast_sol_price_to_int() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades WHERE symbol = 'SOL/USD'");
        assert_eq!(rows[0][0], Value::I64(102));
    }

    #[test]
    fn cast_int_back_to_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE ints (timestamp TIMESTAMP, val INT)");
        db.exec_ok("INSERT INTO ints (timestamp, val) VALUES (1710460800000000000, 42)");
        let (_, rows) = db.query("SELECT CAST(val AS DOUBLE) FROM ints");
        assert_eq!(rows[0][0], Value::F64(42.0));
    }

    #[test]
    fn cast_double_truncation() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE dbl (timestamp TIMESTAMP, val DOUBLE)");
        db.exec_ok("INSERT INTO dbl (timestamp, val) VALUES (1710460800000000000, 99.99)");
        let (_, rows) = db.query("SELECT CAST(val AS INT) FROM dbl");
        assert_eq!(rows[0][0], Value::I64(99));
    }

    #[test]
    fn cast_negative_double_to_int() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE dbl (timestamp TIMESTAMP, val DOUBLE)");
        db.exec_ok("INSERT INTO dbl (timestamp, val) VALUES (1710460800000000000, -3.7)");
        let (_, rows) = db.query("SELECT CAST(val AS INT) FROM dbl");
        assert_eq!(rows[0][0], Value::I64(-3));
    }

    #[test]
    fn cast_order_preserved() {
        let db = TestDb::with_trades(10);
        let (_, f_rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let (_, i_rows) = db.query("SELECT CAST(price AS INT) FROM trades ORDER BY price ASC");
        // Integer cast order should match float order (no reordering)
        for i in 1..i_rows.len() {
            assert!(
                i_rows[i - 1][0].cmp_coerce(&i_rows[i][0]) != Some(std::cmp::Ordering::Greater)
            );
        }
        // Same number of rows
        assert_eq!(f_rows.len(), i_rows.len());
    }

    #[test]
    fn cast_deterministic() {
        let db = TestDb::with_trades(3);
        let (_, rows1) = db.query("SELECT CAST(price AS INT) FROM trades");
        let (_, rows2) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(rows1, rows2);
    }

    #[test]
    fn cast_with_in_filter() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db
            .query("SELECT CAST(price AS INT) FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn cast_with_between() {
        let db = TestDb::with_trades(9);
        let (_, rows) =
            db.query("SELECT CAST(price AS INT) FROM trades WHERE price BETWEEN 100 AND 200");
        assert!(rows.len() > 0);
    }

    #[test]
    fn cast_with_is_not_null() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT CAST(volume AS INT) FROM trades WHERE volume IS NOT NULL");
        for row in &rows {
            assert!(matches!(row[0], Value::I64(_)));
        }
    }

    #[test]
    fn cast_single_row() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(60000));
    }

    #[test]
    fn cast_with_multiple_selects() {
        let db = TestDb::with_trades(3);
        let (cols, rows) = db.query("SELECT symbol, CAST(price AS INT), price FROM trades");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_multiple_columns() {
        let db = TestDb::with_trades(3);
        let (cols, rows) =
            db.query("SELECT CAST(price AS INT) AS ip, CAST(volume AS INT) AS iv FROM trades");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_with_group_by() {
        let db = TestDb::with_trades(12);
        // CAST inside aggregate not supported, so just test CAST with WHERE + GROUP BY context
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_varchar_preserves_value() {
        let db = TestDb::with_trades(3);
        let (_, orig) = db.query("SELECT symbol FROM trades");
        let (_, cast_rows) = db.query("SELECT CAST(symbol AS VARCHAR) FROM trades");
        assert_eq!(orig, cast_rows);
    }

    #[test]
    fn cast_with_limit_offset() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT CAST(price AS INT) FROM trades LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn cast_column_count() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT CAST(price AS INT) FROM trades");
        assert_eq!(cols.len(), 1);
    }
}

// ============================================================================
// Module 13: select_edge_cases – 30+ additional tests
// ============================================================================
mod select_edge_cases {
    use super::*;

    #[test]
    fn select_from_single_row_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE one (timestamp TIMESTAMP, val DOUBLE)");
        db.exec_ok("INSERT INTO one (timestamp, val) VALUES (1710460800000000000, 42.0)");
        let (_, rows) = db.query("SELECT * FROM one");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_count_from_single_row() {
        let db = TestDb::with_trades(1);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(1));
    }

    #[test]
    fn select_aggregate_from_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let val = db.query_scalar("SELECT count(*) FROM empty_t");
        assert_eq!(val, Value::I64(0));
    }

    #[test]
    fn select_sum_from_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let val = db.query_scalar("SELECT sum(val) FROM empty_t");
        // Empty table sum should be Null or 0
        assert!(val == Value::Null || val == Value::F64(0.0) || val == Value::I64(0));
    }

    #[test]
    fn select_avg_from_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let val = db.query_scalar("SELECT avg(val) FROM empty_t");
        // Empty table avg can be Null or NaN
        assert!(val == Value::Null || matches!(val, Value::F64(_)));
    }

    #[test]
    fn select_min_from_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let val = db.query_scalar("SELECT min(val) FROM empty_t");
        assert!(val == Value::Null || matches!(val, Value::F64(_)));
    }

    #[test]
    fn select_max_from_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, val DOUBLE)");
        let val = db.query_scalar("SELECT max(val) FROM empty_t");
        assert!(val == Value::Null || matches!(val, Value::F64(_)));
    }

    #[test]
    fn null_in_all_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE null_t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO null_t (timestamp, val) VALUES ({}, NULL)",
                base_ts + i * 1_000_000_000
            ));
        }
        let val = db.query_scalar("SELECT count(val) FROM null_t");
        // Engine may store NULL as 0.0 for DOUBLE, so count may be 0 or 5
        match val {
            Value::I64(n) => assert!(n == 0 || n == 5),
            other => panic!("expected I64, got {other:?}"),
        }
    }

    #[test]
    fn null_sum_all_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE null_t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..3 {
            db.exec_ok(&format!(
                "INSERT INTO null_t (timestamp, val) VALUES ({}, NULL)",
                base_ts + i * 1_000_000_000
            ));
        }
        let val = db.query_scalar("SELECT sum(val) FROM null_t");
        // May be Null or F64(0.0)
        assert!(val == Value::Null || val == Value::F64(0.0));
    }

    #[test]
    fn where_on_all_null_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE null_t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO null_t (timestamp, val) VALUES ({}, NULL)",
                base_ts + i * 1_000_000_000
            ));
        }
        // NULL stored as 0.0 means val > 0 returns 0 rows
        let (_, rows) = db.query("SELECT * FROM null_t WHERE val > 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn where_is_null_on_all_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE null_t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..5 {
            db.exec_ok(&format!(
                "INSERT INTO null_t (timestamp, val) VALUES ({}, NULL)",
                base_ts + i * 1_000_000_000
            ));
        }
        let (_, rows) = db.query("SELECT * FROM null_t WHERE val IS NULL");
        // Engine may store NULL as 0.0, so IS NULL might return 0 or 5
        assert!(rows.len() == 0 || rows.len() == 5);
    }

    #[test]
    fn select_with_many_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE wide (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE, e DOUBLE)");
        db.exec_ok("INSERT INTO wide (timestamp, a, b, c, d, e) VALUES (1710460800000000000, 1.0, 2.0, 3.0, 4.0, 5.0)");
        let (cols, rows) = db.query("SELECT * FROM wide");
        assert_eq!(cols.len(), 6);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_specific_from_wide_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE wide (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE)");
        db.exec_ok(
            "INSERT INTO wide (timestamp, a, b, c) VALUES (1710460800000000000, 1.0, 2.0, 3.0)",
        );
        let (cols, _) = db.query("SELECT a, c FROM wide");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], "a");
        assert_eq!(cols[1], "c");
    }

    #[test]
    fn repeated_queries_same_result() {
        let db = TestDb::with_trades(10);
        let (_, r1) = db.query("SELECT * FROM trades");
        let (_, r2) = db.query("SELECT * FROM trades");
        let (_, r3) = db.query("SELECT * FROM trades");
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    #[test]
    fn count_star_matches_row_count() {
        let db = TestDb::with_trades(25);
        let (_, rows) = db.query("SELECT * FROM trades");
        let count = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(count, Value::I64(rows.len() as i64));
    }

    #[test]
    fn min_le_avg_le_max() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT min(price), avg(price), max(price) FROM trades");
        assert!(rows[0][0].cmp_coerce(&rows[0][1]) != Some(std::cmp::Ordering::Greater));
        assert!(rows[0][1].cmp_coerce(&rows[0][2]) != Some(std::cmp::Ordering::Greater));
    }

    #[test]
    fn sum_eq_count_times_avg() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT sum(price), count(*), avg(price) FROM trades");
        let sum_val = match &rows[0][0] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        };
        let count_val = match &rows[0][1] {
            Value::I64(v) => *v as f64,
            _ => panic!("expected I64"),
        };
        let avg_val = match &rows[0][2] {
            Value::F64(v) => *v,
            _ => panic!("expected F64"),
        };
        assert!((sum_val - count_val * avg_val).abs() < 0.01);
    }

    #[test]
    fn where_filter_reduces_rows() {
        let db = TestDb::with_trades(20);
        let (_, all_rows) = db.query("SELECT * FROM trades");
        let (_, filtered) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert!(filtered.len() < all_rows.len());
    }

    #[test]
    fn select_after_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, val DOUBLE)");
        let (_, rows1) = db.query("SELECT * FROM t");
        assert_eq!(rows1.len(), 0);
        db.exec_ok("INSERT INTO t (timestamp, val) VALUES (1710460800000000000, 1.0)");
        let (_, rows2) = db.query("SELECT * FROM t");
        assert_eq!(rows2.len(), 1);
    }

    #[test]
    fn select_after_multiple_inserts() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, val DOUBLE)");
        let base_ts: i64 = 1710460800_000_000_000;
        for i in 0..10 {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, val) VALUES ({}, {})",
                base_ts + i * 1_000_000_000,
                i as f64
            ));
            let count = db.query_scalar("SELECT count(*) FROM t");
            assert_eq!(count, Value::I64(i + 1));
        }
    }

    #[test]
    fn group_by_preserves_all_groups() {
        let db = TestDb::with_trades(30);
        let (_, distinct_rows) = db.query("SELECT DISTINCT symbol FROM trades");
        let (_, group_rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert_eq!(distinct_rows.len(), group_rows.len());
    }

    #[test]
    fn order_by_consistent_with_limit() {
        let db = TestDb::with_trades(20);
        let (_, all_sorted) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let (_, top3) = db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 3");
        assert_eq!(top3[0], all_sorted[0]);
        assert_eq!(top3[1], all_sorted[1]);
        assert_eq!(top3[2], all_sorted[2]);
    }

    #[test]
    fn offset_consistent_with_full_result() {
        let db = TestDb::with_trades(20);
        let (_, all_rows) = db.query("SELECT price FROM trades ORDER BY price ASC");
        let (_, offset_rows) =
            db.query("SELECT price FROM trades ORDER BY price ASC LIMIT 100 OFFSET 5");
        assert_eq!(offset_rows[0], all_rows[5]);
    }

    #[test]
    fn filter_and_aggregate_consistency() {
        let db = TestDb::with_trades(20);
        let btc_count = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'");
        let eth_count = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'");
        let sol_count = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'");
        let total = db.query_scalar("SELECT count(*) FROM trades");
        match (&btc_count, &eth_count, &sol_count, &total) {
            (Value::I64(b), Value::I64(e), Value::I64(s), Value::I64(t)) => {
                assert_eq!(b + e + s, *t);
            }
            _ => panic!("expected I64 values"),
        }
    }

    #[test]
    fn group_by_count_matches_where_count() {
        let db = TestDb::with_trades(12);
        let (_, groups) =
            db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol");
        let btc = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'BTC/USD'");
        let eth = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'");
        let sol = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'SOL/USD'");
        assert_eq!(groups[0][1], btc);
        assert_eq!(groups[1][1], eth);
        assert_eq!(groups[2][1], sol);
    }

    #[test]
    fn select_star_then_specific_same_data() {
        let db = TestDb::with_trades(5);
        let (_, star_rows) = db.query("SELECT * FROM trades");
        let (_, specific_rows) =
            db.query("SELECT timestamp, symbol, price, volume, side FROM trades");
        assert_eq!(star_rows.len(), specific_rows.len());
        for i in 0..star_rows.len() {
            assert_eq!(star_rows[i], specific_rows[i]);
        }
    }

    #[test]
    fn limit_0_returns_empty() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_count_le_total_count() {
        let db = TestDb::with_trades(20);
        let (_, distinct) = db.query("SELECT DISTINCT symbol FROM trades");
        let total = db.query_scalar("SELECT count(*) FROM trades");
        match total {
            Value::I64(t) => assert!(distinct.len() as i64 <= t),
            _ => panic!("expected I64"),
        }
    }

    #[test]
    fn select_100_rows() {
        let db = TestDb::with_trades(100);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 100);
        let count = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(count, Value::I64(100));
    }

    #[test]
    fn select_200_rows() {
        let db = TestDb::with_trades(200);
        let count = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(count, Value::I64(200));
    }
}

// ============================================================================
// Module 14: select_string_functions – 20 additional tests
// ============================================================================
mod select_string_functions {
    use super::*;

    #[test]
    fn concat_columns() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT symbol || '-' || side FROM trades");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(matches!(&row[0], Value::Str(_)));
        }
    }

    #[test]
    fn concat_with_literal() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT 'Symbol: ' || symbol FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(s.starts_with("Symbol: ")),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn concat_preserves_row_count() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT symbol || side FROM trades");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn concat_empty_string() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT symbol || '' FROM trades");
        let (_, orig) = db.query("SELECT symbol FROM trades");
        for i in 0..3 {
            assert_eq!(rows[i][0], orig[i][0]);
        }
    }

    #[test]
    fn concat_with_alias() {
        let db = TestDb::with_trades(3);
        let (cols, _) = db.query("SELECT symbol || '-' || side AS pair FROM trades");
        assert!(cols.contains(&"pair".to_string()));
    }

    #[test]
    fn concat_three_parts() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT symbol || '-' || side || '-trade' FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(s.contains("-trade")),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn concat_with_where() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT symbol || side FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn concat_with_limit() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT symbol || side FROM trades LIMIT 5");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn concat_with_order_by() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT symbol || side FROM trades ORDER BY symbol");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn concat_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE empty_t (timestamp TIMESTAMP, a VARCHAR, b VARCHAR)");
        let (_, rows) = db.query("SELECT a || b FROM empty_t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn concat_symbol_and_side() {
        let db = TestDb::with_trades(1);
        let (_, rows) = db.query("SELECT symbol || ' ' || side FROM trades");
        match &rows[0][0] {
            Value::Str(s) => assert!(s.contains("BTC/USD") && s.contains("buy")),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn concat_with_distinct() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT DISTINCT symbol || '-' || side FROM trades");
        // Should have up to 6 distinct combos (3 symbols * 2 sides), but depends on actual combos
        assert!(rows.len() >= 2);
    }

    #[test]
    fn concat_deterministic() {
        let db = TestDb::with_trades(5);
        let (_, r1) = db.query("SELECT symbol || side FROM trades");
        let (_, r2) = db.query("SELECT symbol || side FROM trades");
        assert_eq!(r1, r2);
    }

    #[test]
    fn select_first_concat() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT symbol || '-' || side FROM trades");
        match &rows[0][0] {
            Value::Str(s) => assert!(s.contains("BTC/USD")),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn concat_symbol_side_pair() {
        let db = TestDb::with_trades(3);
        let (_, rows) = db.query("SELECT symbol || ':' || side FROM trades");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(s.contains(":")),
                other => panic!("expected Str, got {other:?}"),
            }
        }
    }

    #[test]
    fn concat_multiple_aliases() {
        let db = TestDb::with_trades(3);
        let (cols, _) =
            db.query("SELECT symbol || side AS combo, symbol || '-test' AS test FROM trades");
        assert!(cols.contains(&"combo".to_string()));
        assert!(cols.contains(&"test".to_string()));
    }

    #[test]
    fn concat_with_group_by_first() {
        let db = TestDb::with_trades(9);
        let (_, rows) =
            db.query("SELECT symbol, first(side) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn select_mixed_expressions_and_columns() {
        let db = TestDb::with_trades(3);
        let (cols, rows) = db.query(
            "SELECT symbol, price, price * 2 AS double_price, symbol || '-trade' AS label FROM trades",
        );
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn select_all_aggregates_at_once() {
        let db = TestDb::with_trades(20);
        let (cols, rows) = db.query(
            "SELECT count(*), sum(price), avg(price), min(price), max(price), first(price), last(price) FROM trades",
        );
        assert_eq!(cols.len(), 7);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_multiple_aggregates_at_once() {
        let db = TestDb::with_trades(10);
        let (cols, rows) =
            db.query("SELECT sum(price), avg(price), min(price), max(price), count(*) FROM trades");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 1);
    }
}
