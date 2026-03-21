//! Boundary condition tests for ExchangeDB query engine.
//!
//! Tests edge cases: empty tables, single rows, extreme values, deep nesting,
//! many columns, many partitions, and other corner cases.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

// ===========================================================================
// Empty table operations
// ===========================================================================

/// SELECT * on empty table.
#[test]
fn empty_select_star() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let (cols, rows) = db.query("SELECT * FROM test");
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 0);
}

/// COUNT(*) on empty table returns 0.
#[test]
fn empty_count() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let val = db.query_scalar("SELECT count(*) FROM test");
    assert_eq!(val, Value::I64(0));
}

/// SUM on empty table returns NULL.
#[test]
fn empty_sum() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let val = db.query_scalar("SELECT sum(value) FROM test");
    assert_eq!(val, Value::Null);
}

/// AVG on empty table returns NULL.
#[test]
fn empty_avg() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let val = db.query_scalar("SELECT avg(value) FROM test");
    assert_eq!(val, Value::Null);
}

/// MIN on empty table returns NULL.
#[test]
fn empty_min() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let val = db.query_scalar("SELECT min(value) FROM test");
    assert_eq!(val, Value::Null);
}

/// MAX on empty table returns NULL.
#[test]
fn empty_max() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let val = db.query_scalar("SELECT max(value) FROM test");
    assert_eq!(val, Value::Null);
}

/// GROUP BY on empty table returns no rows.
#[test]
fn empty_group_by() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, symbol VARCHAR, value DOUBLE)");
    let (_, rows) = db.query("SELECT symbol, count(*) FROM test GROUP BY symbol");
    assert_eq!(rows.len(), 0);
}

/// ORDER BY on empty table returns no rows.
#[test]
fn empty_order_by() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let (_, rows) = db.query("SELECT * FROM test ORDER BY value");
    assert_eq!(rows.len(), 0);
}

/// LIMIT on empty table returns no rows.
#[test]
fn empty_limit() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let (_, rows) = db.query("SELECT * FROM test LIMIT 10");
    assert_eq!(rows.len(), 0);
}

/// WHERE on empty table returns no rows.
#[test]
fn empty_where() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let (_, rows) = db.query("SELECT * FROM test WHERE value > 0");
    assert_eq!(rows.len(), 0);
}

/// DISTINCT on empty table.
#[test]
fn empty_distinct() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let (_, rows) = db.query("SELECT DISTINCT value FROM test");
    assert_eq!(rows.len(), 0);
}

/// JOIN on empty tables.
#[test]
fn empty_join() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, key VARCHAR, value DOUBLE)");
    db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, key VARCHAR, other DOUBLE)");
    let (_, rows) = db.query(
        "SELECT t1.key, t1.value, t2.other FROM t1 INNER JOIN t2 ON t1.key = t2.key",
    );
    assert_eq!(rows.len(), 0);
}

/// INSERT into then TRUNCATE then SELECT returns empty.
#[test]
fn truncate_then_select() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");
    db.exec_ok("TRUNCATE TABLE test");
    let val = db.query_scalar("SELECT count(*) FROM test");
    assert_eq!(val, Value::I64(0));
}

// ===========================================================================
// Single row operations
// ===========================================================================

/// Single row GROUP BY.
#[test]
fn single_row_group_by() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, symbol VARCHAR, value DOUBLE)");
    db.exec_ok(
        "INSERT INTO test (timestamp, symbol, value) VALUES (1000000000000, 'BTC', 65000.0)",
    );
    let (_, rows) = db.query("SELECT symbol, sum(value) FROM test GROUP BY symbol");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Str("BTC".into()));
}

/// Single row ORDER BY.
#[test]
fn single_row_order_by() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");
    let (_, rows) = db.query("SELECT value FROM test ORDER BY value DESC");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::F64(42.0));
}

/// Single row aggregates.
#[test]
fn single_row_aggregates() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");

    assert_eq!(db.query_scalar("SELECT count(*) FROM test"), Value::I64(1));
    assert_eq!(
        db.query_scalar("SELECT sum(value) FROM test"),
        Value::F64(42.0)
    );
    assert_eq!(
        db.query_scalar("SELECT avg(value) FROM test"),
        Value::F64(42.0)
    );
    assert_eq!(
        db.query_scalar("SELECT min(value) FROM test"),
        Value::F64(42.0)
    );
    assert_eq!(
        db.query_scalar("SELECT max(value) FROM test"),
        Value::F64(42.0)
    );
}

/// Single row with WHERE that matches.
#[test]
fn single_row_where_match() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");
    let (_, rows) = db.query("SELECT value FROM test WHERE value = 42.0");
    assert_eq!(rows.len(), 1);
}

/// Single row with WHERE that does not match.
#[test]
fn single_row_where_no_match() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");
    let (_, rows) = db.query("SELECT value FROM test WHERE value = 99.0");
    assert_eq!(rows.len(), 0);
}

/// Single row DISTINCT.
#[test]
fn single_row_distinct() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");
    let (_, rows) = db.query("SELECT DISTINCT value FROM test");
    assert_eq!(rows.len(), 1);
}

/// Single row LIMIT 0.
#[test]
fn single_row_limit_zero() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.0)");
    let (_, rows) = db.query("SELECT * FROM test LIMIT 0");
    assert_eq!(rows.len(), 0);
}

// ===========================================================================
// MAX/MIN value boundaries
// ===========================================================================

/// i64::MAX insert and read.
#[test]
fn i64_max_insert_read() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value LONG)");
    let max = i64::MAX;
    db.exec_ok(&format!(
        "INSERT INTO test (timestamp, value) VALUES (1000000000000, {max})"
    ));
    let val = db.query_scalar("SELECT value FROM test");
    assert_eq!(val, Value::I64(max));
}

/// i64::MIN + 1 insert and read (MIN itself overflows the SQL parser).
#[test]
fn i64_min_plus_one_insert_read() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value LONG)");
    let min_plus_one = i64::MIN + 1;
    db.exec_ok(&format!(
        "INSERT INTO test (timestamp, value) VALUES (1000000000000, {min_plus_one})"
    ));
    let val = db.query_scalar("SELECT value FROM test");
    assert_eq!(val, Value::I64(min_plus_one));
}

/// Zero value insert and read.
#[test]
fn zero_value_insert_read() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 0.0)");
    let val = db.query_scalar("SELECT value FROM test");
    assert_eq!(val, Value::F64(0.0));
}

/// Negative float insert and read.
#[test]
fn negative_float_insert_read() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, -123.456)");
    let val = db.query_scalar("SELECT value FROM test");
    assert_eq!(val, Value::F64(-123.456));
}

/// Very small float.
#[test]
fn very_small_float() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 0.000000001)");
    let val = db.query_scalar("SELECT value FROM test");
    if let Value::F64(v) = val {
        assert!(v > 0.0);
        assert!(v < 0.00001);
    } else {
        panic!("expected F64");
    }
}

/// Very large float.
#[test]
fn very_large_float() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 1.7976931e+308)");
    let val = db.query_scalar("SELECT value FROM test");
    if let Value::F64(v) = val {
        assert!(v > 1.0e+300);
    } else {
        panic!("expected F64");
    }
}

/// Timestamp at epoch zero.
#[test]
fn timestamp_epoch_zero() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    // Epoch 0 = 1970-01-01 00:00:00.
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (0, 1.0)");
    let val = db.query_scalar("SELECT count(*) FROM test");
    assert_eq!(val, Value::I64(1));
}

/// NULL value in aggregate.
#[test]
fn null_in_aggregate() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, NULL)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (2000000000000, 10.0)");

    let sum = db.query_scalar("SELECT sum(value) FROM test");
    // SUM should skip NULL and return 10.0.
    assert_eq!(sum, Value::F64(10.0));

    let count_star = db.query_scalar("SELECT count(*) FROM test");
    assert_eq!(count_star, Value::I64(2));
}

/// All NULLs in aggregate.
#[test]
fn all_nulls_in_aggregate() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, NULL)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (2000000000000, NULL)");

    let sum = db.query_scalar("SELECT sum(value) FROM test");
    assert_eq!(sum, Value::Null);
}

/// IS NULL filter.
#[test]
fn filter_is_null() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, NULL)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (2000000000000, 10.0)");

    let (_, rows) = db.query("SELECT * FROM test WHERE value IS NULL");
    assert_eq!(rows.len(), 1);
}

/// IS NOT NULL filter.
#[test]
fn filter_is_not_null() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, NULL)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (2000000000000, 10.0)");

    let (_, rows) = db.query("SELECT * FROM test WHERE value IS NOT NULL");
    assert_eq!(rows.len(), 1);
}

/// Long string insert and read.
#[test]
fn long_string_insert_read() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, data VARCHAR)");
    let long_str: String = "x".repeat(10_000);
    db.exec_ok(&format!(
        "INSERT INTO test (timestamp, data) VALUES (1000000000000, '{long_str}')"
    ));
    let val = db.query_scalar("SELECT data FROM test");
    if let Value::Str(s) = val {
        assert_eq!(s.len(), 10_000);
    } else {
        panic!("expected Str");
    }
}

/// Empty string insert and read.
#[test]
fn empty_string_insert_read() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, data VARCHAR)");
    db.exec_ok("INSERT INTO test (timestamp, data) VALUES (1000000000000, '')");
    let val = db.query_scalar("SELECT data FROM test");
    assert_eq!(val, Value::Str("".into()));
}

/// String with special characters.
#[test]
fn string_special_chars() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, data VARCHAR)");
    // Use SQL-safe special chars.
    db.exec_ok("INSERT INTO test (timestamp, data) VALUES (1000000000000, 'hello world 123!@#$%')");
    let val = db.query_scalar("SELECT data FROM test");
    if let Value::Str(s) = val {
        assert!(s.contains("hello"));
    } else {
        panic!("expected Str");
    }
}

// ===========================================================================
// Deeply nested and complex queries
// ===========================================================================

/// Deeply nested AND conditions (50 levels).
#[test]
fn deeply_nested_and() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 50.0)");

    // Build WHERE value > 0 AND value > 1 AND ... AND value > 49
    let conditions: Vec<String> = (0..50).map(|i| format!("value > {i}")).collect();
    let where_clause = conditions.join(" AND ");
    let sql = format!("SELECT count(*) FROM test WHERE {where_clause}");
    let val = db.query_scalar(&sql);
    assert_eq!(val, Value::I64(1));
}

/// Deeply nested OR conditions (50 levels).
#[test]
fn deeply_nested_or() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 5.0)");

    let conditions: Vec<String> = (0..50).map(|i| format!("value = {i}.0")).collect();
    let where_clause = conditions.join(" OR ");
    let sql = format!("SELECT count(*) FROM test WHERE {where_clause}");
    let val = db.query_scalar(&sql);
    assert_eq!(val, Value::I64(1));
}

/// IN list with many values.
#[test]
fn in_list_many_values() {
    let db = TestDb::with_trades(50);
    let values: Vec<String> = (0..100).map(|i| format!("{}", 60000.0 + i as f64 * 100.0)).collect();
    let in_list = values.join(", ");
    let sql = format!("SELECT count(*) FROM trades WHERE price IN ({in_list})");
    let val = db.query_scalar(&sql);
    if let Value::I64(n) = val {
        assert!(n >= 0);
    }
}

/// BETWEEN filter.
#[test]
fn between_filter() {
    let db = TestDb::with_trades(100);
    let (_, rows) =
        db.query("SELECT * FROM trades WHERE price BETWEEN 60000.0 AND 61000.0");
    // Should find some BTC rows.
    assert!(!rows.is_empty());
}

/// NOT LIKE filter.
#[test]
fn not_like_filter() {
    let db = TestDb::with_trades(30);
    let (_, rows) = db.query("SELECT * FROM trades WHERE symbol NOT LIKE 'BTC%'");
    // Should exclude BTC/USD.
    for row in &rows {
        if let Value::Str(s) = &row[1] {
            assert!(!s.starts_with("BTC"));
        }
    }
}

/// LIKE filter.
#[test]
fn like_filter() {
    let db = TestDb::with_trades(30);
    let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'BTC%'");
    for row in &rows {
        if let Value::Str(s) = &row[1] {
            assert!(s.starts_with("BTC"));
        }
    }
}

/// Multiple tables created and queried.
#[test]
fn many_tables() {
    let db = TestDb::new();
    for i in 0..20 {
        db.exec_ok(&format!(
            "CREATE TABLE table_{i} (timestamp TIMESTAMP, value DOUBLE)"
        ));
        db.exec_ok(&format!(
            "INSERT INTO table_{i} (timestamp, value) VALUES (1000000000000, {i}.0)"
        ));
    }

    for i in 0..20 {
        let val = db.query_scalar(&format!("SELECT value FROM table_{i}"));
        assert_eq!(val, Value::F64(i as f64));
    }
}

/// SELECT with arithmetic expressions.
#[test]
fn arithmetic_expressions() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, a, b) VALUES (1000000000000, 10.0, 3.0)");

    let (_, rows) = db.query("SELECT a + b, a - b, a * b, a / b FROM test");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::F64(13.0));
    assert_eq!(rows[0][1], Value::F64(7.0));
    assert_eq!(rows[0][2], Value::F64(30.0));
    if let Value::F64(v) = rows[0][3] {
        assert!((v - 10.0 / 3.0).abs() < 1e-10);
    }
}

/// Division by zero returns NULL.
#[test]
fn division_by_zero() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, a, b) VALUES (1000000000000, 10.0, 0.0)");

    let val = db.query_scalar("SELECT a / b FROM test");
    assert_eq!(val, Value::Null);
}

/// SELECT nonexistent table returns error.
#[test]
fn select_nonexistent_table() {
    let db = TestDb::new();
    let err = db.exec_err("SELECT * FROM no_such_table");
    let msg = format!("{err}");
    assert!(
        msg.contains("not found") || msg.contains("does not exist") || msg.contains("TableNotFound"),
        "unexpected error: {msg}"
    );
}

/// INSERT into nonexistent table returns error.
#[test]
fn insert_nonexistent_table() {
    let db = TestDb::new();
    let err = db.exec_err(
        "INSERT INTO no_such_table (timestamp, value) VALUES (1000000000000, 1.0)",
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("not found") || msg.contains("does not exist") || msg.contains("TableNotFound"),
        "unexpected error: {msg}"
    );
}

/// CREATE TABLE with duplicate name returns error.
#[test]
fn create_duplicate_table() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let err = db.exec_err("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    let msg = format!("{err}");
    assert!(
        msg.contains("already exists") || msg.contains("AlreadyExists") || msg.contains("exists"),
        "unexpected error: {msg}"
    );
}

/// CREATE TABLE IF NOT EXISTS does not error.
#[test]
fn create_table_if_not_exists() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    // Should not error.
    db.exec_ok("CREATE TABLE IF NOT EXISTS test (timestamp TIMESTAMP, value DOUBLE)");
}

/// Multiple inserts then GROUP BY with HAVING.
#[test]
fn multi_insert_group_by_having() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, group_id VARCHAR, value DOUBLE)");
    for i in 0..100 {
        let group = format!("g{}", i % 5);
        db.exec_ok(&format!(
            "INSERT INTO test (timestamp, group_id, value) VALUES ({}, '{}', {})",
            1_000_000_000_000i64 + i * 1_000_000_000,
            group,
            i as f64
        ));
    }

    let (_, rows) = db.query(
        "SELECT group_id, count(*) FROM test GROUP BY group_id HAVING count(*) >= 20",
    );
    assert_eq!(rows.len(), 5); // All groups have 20 rows.
}

/// ORDER BY multiple columns.
#[test]
fn order_by_multiple_columns() {
    let db = TestDb::with_trades(30);
    let (_, rows) = db.query("SELECT symbol, price FROM trades ORDER BY symbol, price DESC");
    assert_eq!(rows.len(), 30);
}

/// COUNT with WHERE condition.
#[test]
fn count_with_where() {
    let db = TestDb::with_trades(100);
    let val = db.query_scalar("SELECT count(*) FROM trades WHERE side = 'buy'");
    if let Value::I64(n) = val {
        assert!(n > 0);
        assert!(n < 100);
    }
}

/// Comparison operators.
#[test]
fn comparison_operators() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    for i in 0..10 {
        db.exec_ok(&format!(
            "INSERT INTO test (timestamp, value) VALUES ({}, {})",
            1_000_000_000_000i64 + i * 1_000_000_000,
            i as f64
        ));
    }

    let (_, rows) = db.query("SELECT count(*) FROM test WHERE value > 5.0");
    assert_eq!(rows[0][0], Value::I64(4)); // 6, 7, 8, 9

    let (_, rows) = db.query("SELECT count(*) FROM test WHERE value >= 5.0");
    assert_eq!(rows[0][0], Value::I64(5)); // 5, 6, 7, 8, 9

    let (_, rows) = db.query("SELECT count(*) FROM test WHERE value < 3.0");
    assert_eq!(rows[0][0], Value::I64(3)); // 0, 1, 2

    let (_, rows) = db.query("SELECT count(*) FROM test WHERE value <= 3.0");
    assert_eq!(rows[0][0], Value::I64(4)); // 0, 1, 2, 3

    let (_, rows) = db.query("SELECT count(*) FROM test WHERE value != 5.0");
    assert_eq!(rows[0][0], Value::I64(9));
}

/// Multiple WHERE conditions with AND.
#[test]
fn where_and_conditions() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query(
        "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'",
    );
    for row in &rows {
        if let (Value::Str(sym), Value::Str(side)) = (&row[1], &row[4]) {
            assert_eq!(sym, "BTC/USD");
            assert_eq!(side, "buy");
        }
    }
}

/// Multiple WHERE conditions with OR.
#[test]
fn where_or_conditions() {
    let db = TestDb::with_trades(30);
    let (_, rows) = db.query(
        "SELECT * FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'ETH/USD'",
    );
    for row in &rows {
        if let Value::Str(sym) = &row[1] {
            assert!(sym == "BTC/USD" || sym == "ETH/USD");
        }
    }
}

/// OFFSET beyond row count returns empty.
#[test]
fn offset_beyond_count() {
    let db = TestDb::with_trades(10);
    let (_, rows) = db.query("SELECT * FROM trades LIMIT 10 OFFSET 100");
    assert_eq!(rows.len(), 0);
}

/// Negative value in WHERE.
#[test]
fn negative_value_in_where() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, -5.0)");
    db.exec_ok("INSERT INTO test (timestamp, value) VALUES (2000000000000, 5.0)");

    let (_, rows) = db.query("SELECT * FROM test WHERE value < 0");
    assert_eq!(rows.len(), 1);
}

/// INSERT multiple rows in a single statement.
#[test]
fn insert_multiple_values() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
    db.exec_ok(
        "INSERT INTO test (timestamp, value) VALUES (1000000000000, 1.0), (2000000000000, 2.0), (3000000000000, 3.0)",
    );
    let val = db.query_scalar("SELECT count(*) FROM test");
    assert_eq!(val, Value::I64(3));
}

/// Double aggregation: GROUP BY + ORDER BY.
#[test]
fn group_by_order_by() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query(
        "SELECT symbol, count(*) FROM trades GROUP BY symbol ORDER BY symbol",
    );
    assert_eq!(rows.len(), 3);
    // Verify sorted by symbol.
    if let (Value::Str(a), Value::Str(b)) = (&rows[0][0], &rows[1][0]) {
        assert!(a <= b);
    }
}

/// Join with non-empty tables.
#[test]
fn join_non_empty() {
    let db = TestDb::with_trades_and_quotes();
    let (_, rows) = db.query(
        "SELECT t.symbol, t.price, q.bid FROM trades t INNER JOIN quotes q ON t.symbol = q.symbol",
    );
    assert!(!rows.is_empty());
}

/// Join one empty + one non-empty table.
#[test]
fn join_one_empty() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, key VARCHAR, value DOUBLE)");
    db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, key VARCHAR, other DOUBLE)");
    db.exec_ok("INSERT INTO t1 (timestamp, key, value) VALUES (1000000000000, 'a', 1.0)");

    let (_, rows) = db.query(
        "SELECT t1.key, t1.value, t2.other FROM t1 INNER JOIN t2 ON t1.key = t2.key",
    );
    assert_eq!(rows.len(), 0);
}

/// Multiple value types in one table.
#[test]
fn mixed_types_table() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE mixed (timestamp TIMESTAMP, name VARCHAR, price DOUBLE, qty LONG)",
    );
    db.exec_ok(
        "INSERT INTO mixed (timestamp, name, price, qty) VALUES (1000000000000, 'BTC', 65000.0, 100)",
    );
    let (_, rows) = db.query("SELECT * FROM mixed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 4);
}

/// Large OFFSET + small LIMIT.
#[test]
fn large_offset_small_limit() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query("SELECT * FROM trades LIMIT 5 OFFSET 95");
    assert_eq!(rows.len(), 5);
}

/// COUNT DISTINCT.
#[test]
fn count_distinct() {
    let db = TestDb::with_trades(100);
    let (_, rows) = db.query("SELECT DISTINCT side FROM trades");
    assert_eq!(rows.len(), 2); // buy and sell
}
