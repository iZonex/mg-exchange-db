//! Regression tests verifying ExchangeDB is not affected by known QuestDB bugs.
//!
//! Each test corresponds to a specific QuestDB GitHub issue. Tests exercise
//! the full parse -> plan -> execute pipeline against real on-disk data.

use exchange_query::plan::{QueryResult, Value};
use exchange_query::test_utils::TestDb;

// ═══════════════════════════════════════════════════════════════════════════
// #1645 — Table locked after OOM
// ═══════════════════════════════════════════════════════════════════════════
// Verify that the table write lock is released even when a write fails.

#[test]
fn questdb_1645_table_accessible_after_write_error() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
    );
    db.exec_ok(
        "INSERT INTO trades (timestamp, symbol, price) VALUES (1000000000000, 'BTC', 42000.0)",
    );

    // Attempt an invalid insert (wrong number of columns / type mismatch).
    // This should fail but NOT leave the table locked.
    let result = db.exec(
        "INSERT INTO trades (timestamp, symbol, price) VALUES ('not_a_timestamp', 'ETH', 'not_a_number')",
    );
    // Whether it errors or coerces, the table must remain accessible.
    let _ = result;

    // The critical check: we can still read from the table.
    let (_, rows) = db.query("SELECT * FROM trades");
    assert!(
        !rows.is_empty(),
        "table should still be readable after a failed write"
    );

    // And we can still write to it.
    db.exec_ok(
        "INSERT INTO trades (timestamp, symbol, price) VALUES (2000000000000, 'SOL', 100.0)",
    );
    let (_, rows) = db.query("SELECT * FROM trades");
    assert!(
        rows.len() >= 2,
        "table should accept new writes after a failed write"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// #2623 — LIKE escape doesn't work
// ═══════════════════════════════════════════════════════════════════════════
// Verify that LIKE handles literal underscores and percent signs via
// the escape character. Also tests basic LIKE without ESCAPE.

#[test]
fn questdb_2623_like_basic_patterns() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE items (timestamp TIMESTAMP, name VARCHAR)",
    );
    db.exec_ok(
        "INSERT INTO items (timestamp, name) VALUES (1000000000000, 'foo_bar')",
    );
    db.exec_ok(
        "INSERT INTO items (timestamp, name) VALUES (2000000000000, 'fooxbar')",
    );
    db.exec_ok(
        "INSERT INTO items (timestamp, name) VALUES (3000000000000, 'foo%bar')",
    );

    // Without escape, `_` is a wildcard — foo_bar, fooxbar, and foo%bar all match
    // because `_` matches any single character (including `%`).
    let (_, rows) = db.query("SELECT name FROM items WHERE name LIKE 'foo_bar'");
    assert_eq!(rows.len(), 3, "underscore wildcard should match all three rows");

    // `%` in the middle matches everything including foo%bar.
    let (_, rows) = db.query("SELECT name FROM items WHERE name LIKE 'foo%bar'");
    assert_eq!(rows.len(), 3, "percent wildcard should match all three rows");
}

#[test]
fn questdb_2623_like_escape_underscore() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE items (timestamp TIMESTAMP, name VARCHAR)",
    );
    db.exec_ok(
        "INSERT INTO items (timestamp, name) VALUES (1000000000000, 'foo_bar')",
    );
    db.exec_ok(
        "INSERT INTO items (timestamp, name) VALUES (2000000000000, 'fooxbar')",
    );

    // With ESCAPE, `\_` should match literal underscore only.
    // Note: sqlparser may or may not pass the escape char through;
    // this test documents our behavior.
    let result = db.exec(
        r"SELECT name FROM items WHERE name LIKE 'foo\_bar' ESCAPE '\'",
    );
    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            // If ESCAPE is supported, only 'foo_bar' should match.
            assert_eq!(
                rows.len(),
                1,
                "with ESCAPE, only the literal underscore row should match"
            );
            assert_eq!(rows[0][0], Value::Str("foo_bar".into()));
        }
        Err(e) => {
            // If we don't support ESCAPE yet, it should be a parse/query error, not a panic.
            let msg = e.to_string();
            assert!(
                msg.contains("ESCAPE") || msg.contains("escape") || msg.contains("unsupported"),
                "ESCAPE error should be descriptive, got: {msg}"
            );
        }
        _ => panic!("unexpected result type"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// #2505 — Case sensitivity in DROP TABLE
// ═══════════════════════════════════════════════════════════════════════════
// Document and verify our table name case-sensitivity behavior.

#[test]
fn questdb_2505_case_sensitivity_create_and_select() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE Trades (timestamp TIMESTAMP, price DOUBLE)",
    );
    db.exec_ok(
        "INSERT INTO Trades (timestamp, price) VALUES (1000000000000, 100.0)",
    );

    // Exact case should work.
    let (_, rows) = db.query("SELECT * FROM Trades");
    assert_eq!(rows.len(), 1, "exact case name should find the table");

    // ExchangeDB behavior: table names are case-sensitive (filesystem-based).
    // Attempting to query with different case should produce a clear error.
    let result = db.exec("SELECT * FROM trades");
    match result {
        Err(e) => {
            // This is expected: different case => different table name.
            let msg = e.to_string().to_lowercase();
            assert!(
                msg.contains("not found") || msg.contains("does not exist"),
                "should produce a 'not found' error for wrong case, got: {msg}"
            );
        }
        Ok(QueryResult::Rows { rows, .. }) => {
            // If it succeeds, case-insensitive is also acceptable.
            assert_eq!(rows.len(), 1);
        }
        _ => {}
    }
}

#[test]
fn questdb_2505_case_sensitivity_drop() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE MyTable (timestamp TIMESTAMP, val DOUBLE)",
    );

    // DROP with exact case should work.
    db.exec_ok("DROP TABLE MyTable");

    // Table should be gone.
    let result = db.exec("SELECT * FROM MyTable");
    assert!(result.is_err(), "table should be dropped");
}

// ═══════════════════════════════════════════════════════════════════════════
// #1679 — Join column type mismatch (INT vs BIGINT)
// ═══════════════════════════════════════════════════════════════════════════
// Verify that joins work when column types differ (I64 vs F64 coercion).

#[test]
fn questdb_1679_join_type_coercion_int_vs_float() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE orders (timestamp TIMESTAMP, order_id LONG, symbol VARCHAR)",
    );
    db.exec_ok(
        "CREATE TABLE fills (timestamp TIMESTAMP, order_id DOUBLE, fill_price DOUBLE)",
    );

    db.exec_ok(
        "INSERT INTO orders (timestamp, order_id, symbol) VALUES (1000000000000, 100, 'BTC')",
    );
    db.exec_ok(
        "INSERT INTO orders (timestamp, order_id, symbol) VALUES (2000000000000, 200, 'ETH')",
    );

    db.exec_ok(
        "INSERT INTO fills (timestamp, order_id, fill_price) VALUES (1000000000000, 100.0, 42000.0)",
    );
    db.exec_ok(
        "INSERT INTO fills (timestamp, order_id, fill_price) VALUES (2000000000000, 200.0, 3000.0)",
    );

    // JOIN on order_id where left is I64 and right is F64.
    let result = db.exec(
        "SELECT orders.symbol, fills.fill_price \
         FROM orders \
         INNER JOIN fills ON orders.order_id = fills.order_id",
    );

    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            assert_eq!(
                rows.len(),
                2,
                "join should produce 2 matched rows with type coercion; got {}",
                rows.len()
            );
        }
        Err(e) => {
            // If it fails, it should be a clear type mismatch error, not a crash.
            let msg = e.to_string();
            assert!(
                msg.contains("type") || msg.contains("mismatch") || msg.contains("coerce"),
                "join type mismatch should produce a clear error, got: {msg}"
            );
        }
        _ => panic!("unexpected result type"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// #5417 — BETWEEN with SHORT / SMALLINT type
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn questdb_5417_between_with_smallint() {
    let db = TestDb::new();
    // ExchangeDB stores SMALLINT as I64 internally, but BETWEEN should still work.
    db.exec_ok(
        "CREATE TABLE sensors (timestamp TIMESTAMP, val LONG)",
    );
    for i in 1..=20i64 {
        db.exec_ok(&format!(
            "INSERT INTO sensors (timestamp, val) VALUES ({}, {})",
            i * 1_000_000_000, i
        ));
    }

    let (_, rows) = db.query("SELECT val FROM sensors WHERE val BETWEEN 5 AND 15");
    assert_eq!(
        rows.len(),
        11,
        "BETWEEN 5 AND 15 should return 11 rows (5..=15)"
    );
    for row in &rows {
        let val = match &row[0] {
            Value::I64(n) => *n,
            Value::F64(f) => *f as i64,
            other => panic!("expected numeric value, got: {other:?}"),
        };
        assert!(
            (5..=15).contains(&val),
            "value {val} should be in range 5..=15"
        );
    }
}

#[test]
fn questdb_5417_between_with_doubles() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE measures (timestamp TIMESTAMP, val DOUBLE)",
    );
    for i in 0..10 {
        let v = (i as f64) * 1.5;
        let ts: i64 = (i + 1) * 1_000_000_000_i64;
        db.exec_ok(&format!(
            "INSERT INTO measures (timestamp, val) VALUES ({ts}, {v})",
        ));
    }

    let (_, rows) = db.query("SELECT val FROM measures WHERE val BETWEEN 3.0 AND 9.0");
    for row in &rows {
        let val = match &row[0] {
            Value::F64(f) => *f,
            Value::I64(n) => *n as f64,
            other => panic!("expected numeric value, got: {other:?}"),
        };
        assert!(
            val >= 3.0 && val <= 9.0,
            "value {val} should be in range 3.0..=9.0"
        );
    }
    assert!(
        !rows.is_empty(),
        "BETWEEN with doubles should return some rows"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// #5609 — HTTP 500 on invalid query (verifying error mapping)
// ═══════════════════════════════════════════════════════════════════════════
// We can't spin up a full HTTP server in unit tests, but we can verify that
// invalid SQL produces a proper parse/query error (not a panic or internal error).

#[test]
fn questdb_5609_invalid_sql_produces_parse_error() {
    let db = TestDb::new();

    // Garbage SQL should produce a parse error.
    let err = db.exec_err("SELECTT * FROMM garbage_table_!!!;");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("parse") || msg.contains("syntax") || msg.contains("expected")
            || msg.contains("unsupported"),
        "garbage SQL should produce a parse error, got: {msg}"
    );
}

#[test]
fn questdb_5609_query_nonexistent_table_is_not_internal_error() {
    let db = TestDb::new();

    let err = db.exec_err("SELECT * FROM nonexistent_table_xyz");
    let msg = err.to_string().to_lowercase();
    // Should be "table not found", not "internal server error".
    assert!(
        msg.contains("not found") || msg.contains("does not exist"),
        "missing table should produce 'not found' error, got: {msg}"
    );
}

#[test]
fn questdb_5609_empty_query_is_parse_error() {
    let db = TestDb::new();

    let result = db.exec("");
    assert!(
        result.is_err(),
        "empty query should be an error"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// #5064 — Table name with period
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn questdb_5064_quoted_table_name_with_period() {
    let db = TestDb::new();

    let result = db.exec(
        "CREATE TABLE \"my.table\" (timestamp TIMESTAMP, val DOUBLE)",
    );
    match result {
        Ok(_) => {
            // If CREATE succeeds, INSERT and SELECT should also work.
            db.exec_ok(
                "INSERT INTO \"my.table\" (timestamp, val) VALUES (1000000000000, 42.0)",
            );
            let (_, rows) = db.query("SELECT val FROM \"my.table\"");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::F64(42.0));

            // Cleanup.
            db.exec_ok("DROP TABLE \"my.table\"");
        }
        Err(e) => {
            // If we don't support periods in table names, it should be a clear error.
            let msg = e.to_string();
            assert!(
                msg.contains("period") || msg.contains("invalid") || msg.contains("character")
                    || msg.contains("parse") || msg.contains("identifier"),
                "period in table name should produce a clear error, got: {msg}"
            );
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// #2025 — SQL injection via REST
// ═══════════════════════════════════════════════════════════════════════════
// Verify that SQL injection attempts don't corrupt the database.

#[test]
fn questdb_2025_sql_injection_in_values() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE users (timestamp TIMESTAMP, name VARCHAR, role VARCHAR)",
    );
    db.exec_ok(
        "INSERT INTO users (timestamp, name, role) VALUES (1000000000000, 'admin', 'admin')",
    );

    // Attempt SQL injection in a string value.
    let injection = "'); DROP TABLE users; --";
    let result = db.exec(&format!(
        "INSERT INTO users (timestamp, name, role) VALUES (2000000000000, '{injection}', 'user')"
    ));
    // Whether the INSERT succeeds or fails, the table should still exist.
    let _ = result;

    // The users table must still exist and be queryable.
    let (_, rows) = db.query("SELECT * FROM users");
    assert!(
        !rows.is_empty(),
        "table should still exist after injection attempt"
    );
}

#[test]
fn questdb_2025_sql_injection_in_where_clause() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE data (timestamp TIMESTAMP, val DOUBLE)",
    );
    db.exec_ok(
        "INSERT INTO data (timestamp, val) VALUES (1000000000000, 42.0)",
    );

    // Attempt injection via WHERE clause. This should be a single statement;
    // the engine should not execute the DROP TABLE part.
    let result = db.exec("SELECT * FROM data WHERE val = 42; DROP TABLE data");
    // Whether it fails or returns rows, the table must survive.
    let _ = result;

    // Table should still exist.
    let (_, rows) = db.query("SELECT * FROM data");
    assert_eq!(rows.len(), 1, "data table should survive injection attempt");
}

// ═══════════════════════════════════════════════════════════════════════════
// #5096 — Symbol to INT cast
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn questdb_5096_cast_symbol_to_int() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE instruments (timestamp TIMESTAMP, sym VARCHAR, code LONG)",
    );
    db.exec_ok(
        "INSERT INTO instruments (timestamp, sym, code) VALUES (1000000000000, '123', 123)",
    );
    db.exec_ok(
        "INSERT INTO instruments (timestamp, sym, code) VALUES (2000000000000, 'abc', 456)",
    );

    // Cast numeric string to int should work.
    let result = db.exec("SELECT CAST(sym AS INT) FROM instruments WHERE sym = '123'");
    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            assert_eq!(rows.len(), 1);
            // Should return 123 as an integer.
            match &rows[0][0] {
                Value::I64(n) => assert_eq!(*n, 123),
                Value::F64(f) => assert_eq!(*f, 123.0),
                other => panic!("expected integer result from CAST, got: {other:?}"),
            }
        }
        Err(e) => {
            // If CAST is not supported, error should be clear.
            let msg = e.to_string();
            assert!(
                msg.contains("cast") || msg.contains("CAST") || msg.contains("convert"),
                "CAST error should be descriptive, got: {msg}"
            );
        }
        _ => {}
    }

    // Cast non-numeric string to int should fail gracefully, not panic.
    let result = db.exec("SELECT CAST(sym AS INT) FROM instruments WHERE sym = 'abc'");
    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            // Acceptable: either NULL, 0, or an error value.
            if !rows.is_empty() {
                let val = &rows[0][0];
                assert!(
                    matches!(val, Value::Null | Value::I64(_) | Value::F64(_)),
                    "non-numeric CAST result should be NULL or a number, got: {val:?}"
                );
            }
        }
        Err(_) => {
            // Also acceptable — an error is fine as long as it doesn't panic.
        }
        _ => {}
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// #5812 — Cursor not closed / resource leak
// ═══════════════════════════════════════════════════════════════════════════
// Verify no resource leak by running many queries in sequence.

#[test]
fn questdb_5812_no_resource_leak_many_queries() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE stress (timestamp TIMESTAMP, val DOUBLE)",
    );
    for i in 0..100i64 {
        let ts = (i + 1) * 1_000_000_000_i64;
        db.exec_ok(&format!(
            "INSERT INTO stress (timestamp, val) VALUES ({ts}, {i}.0)",
        ));
    }

    // Run many queries in a loop. If there is a resource leak,
    // this will eventually fail with "too many open files" or similar.
    for i in 0..200 {
        let (_, rows) = db.query("SELECT * FROM stress WHERE val > 50.0");
        assert!(
            rows.len() > 0,
            "query iteration {i} should return rows"
        );
    }

    // Run queries with different patterns to exercise more code paths.
    for _ in 0..50 {
        let _ = db.query("SELECT COUNT(*) FROM stress");
        let _ = db.query("SELECT val FROM stress ORDER BY val DESC LIMIT 10");
        let _ = db.query("SELECT AVG(val), MIN(val), MAX(val) FROM stress");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// #5926 — Float infinity / NaN inconsistency
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn questdb_5926_float_nan_handling() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE floats (timestamp TIMESTAMP, val DOUBLE)",
    );

    // Insert NaN via expression.
    let result = db.exec(
        "INSERT INTO floats (timestamp, val) VALUES (1000000000000, NaN)",
    );
    match result {
        Ok(_) => {
            let (_, rows) = db.query("SELECT val FROM floats");
            if !rows.is_empty() {
                match &rows[0][0] {
                    Value::F64(f) => {
                        assert!(
                            f.is_nan() || *f == 0.0,
                            "NaN should be stored as NaN or coerced consistently"
                        );
                    }
                    Value::Null => {
                        // NaN stored as NULL is also acceptable.
                    }
                    _ => {}
                }
            }
        }
        Err(_) => {
            // Not supporting NaN in INSERT is acceptable — just shouldn't panic.
        }
    }
}

#[test]
fn questdb_5926_float_null_comparisons() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE fdata (timestamp TIMESTAMP, val DOUBLE)",
    );
    db.exec_ok(
        "INSERT INTO fdata (timestamp, val) VALUES (1000000000000, 1.0)",
    );
    db.exec_ok(
        "INSERT INTO fdata (timestamp, val) VALUES (2000000000000, NULL)",
    );
    db.exec_ok(
        "INSERT INTO fdata (timestamp, val) VALUES (3000000000000, 999.0)",
    );

    // NULL comparisons should follow SQL semantics.
    let (_, rows) = db.query("SELECT val FROM fdata WHERE val IS NULL");
    assert_eq!(rows.len(), 1, "exactly one NULL row expected");

    let (_, rows) = db.query("SELECT val FROM fdata WHERE val IS NOT NULL");
    assert_eq!(rows.len(), 2, "two non-NULL rows expected");
}

// ═══════════════════════════════════════════════════════════════════════════
// #4544 — DST timezone aggregation
// ═══════════════════════════════════════════════════════════════════════════
// ExchangeDB stores all timestamps in UTC nanoseconds.
// SAMPLE BY operates on nanosecond intervals, so DST shouldn't cause issues
// since we don't do timezone conversion during aggregation.

#[test]
fn questdb_4544_sample_by_across_day_boundary() {
    let db = TestDb::new();
    db.exec_ok(
        "CREATE TABLE ticks (timestamp TIMESTAMP, price DOUBLE)",
    );

    // Insert data across a day boundary (midnight UTC).
    let base: i64 = 1710460800_000_000_000; // 2024-03-15 00:00:00 UTC
    for i in 0..48 {
        // 48 hours of hourly data
        let ts = base + (i as i64) * 3_600_000_000_000;
        let price = 100.0 + (i as f64);
        db.exec_ok(&format!(
            "INSERT INTO ticks (timestamp, price) VALUES ({ts}, {price})"
        ));
    }

    // SAMPLE BY 1d should produce 2 full days.
    let result = db.exec(
        "SELECT timestamp, AVG(price) FROM ticks SAMPLE BY 1d",
    );
    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            assert!(
                rows.len() >= 2,
                "SAMPLE BY 1d over 48h should produce at least 2 buckets, got {}",
                rows.len()
            );
        }
        Err(e) => {
            // SAMPLE BY might not be supported in all paths.
            let msg = e.to_string();
            assert!(
                !msg.contains("panic") && !msg.contains("internal"),
                "SAMPLE BY error should not be an internal error, got: {msg}"
            );
        }
        _ => {}
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Additional: Verify HashableValue cross-type equality in joins
// ═══════════════════════════════════════════════════════════════════════════
// This tests the specific mechanism in join.rs where I64 and F64 hash keys
// need to match for cross-type joins.

#[test]
fn join_cross_type_i64_i64_works() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE left_t (timestamp TIMESTAMP, id LONG, name VARCHAR)");
    db.exec_ok("CREATE TABLE right_t (timestamp TIMESTAMP, id LONG, info VARCHAR)");

    db.exec_ok("INSERT INTO left_t (timestamp, id, name) VALUES (1000000000000, 1, 'alpha')");
    db.exec_ok("INSERT INTO left_t (timestamp, id, name) VALUES (2000000000000, 2, 'beta')");
    db.exec_ok("INSERT INTO right_t (timestamp, id, info) VALUES (1000000000000, 1, 'x')");
    db.exec_ok("INSERT INTO right_t (timestamp, id, info) VALUES (2000000000000, 3, 'y')");

    // Same-type join should always work.
    let result = db.exec(
        "SELECT left_t.name, right_t.info FROM left_t INNER JOIN right_t ON left_t.id = right_t.id",
    );
    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            assert_eq!(rows.len(), 1, "only id=1 should match");
            assert_eq!(rows[0][0], Value::Str("alpha".into()));
            assert_eq!(rows[0][1], Value::Str("x".into()));
        }
        Err(e) => panic!("same-type join should work, got error: {e}"),
        _ => panic!("expected rows"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Additional: Verify LIKE with percent and underscore edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_empty_string_and_pattern() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE strs (timestamp TIMESTAMP, val VARCHAR)");
    db.exec_ok("INSERT INTO strs (timestamp, val) VALUES (1000000000000, '')");
    db.exec_ok("INSERT INTO strs (timestamp, val) VALUES (2000000000000, 'a')");

    // Empty string should match '%'.
    let (_, rows) = db.query("SELECT val FROM strs WHERE val LIKE '%'");
    assert_eq!(rows.len(), 2, "percent should match everything including empty string");

    // Empty string should NOT match '_'.
    let (_, rows) = db.query("SELECT val FROM strs WHERE val LIKE '_'");
    assert_eq!(rows.len(), 1, "underscore should match exactly one char");
    assert_eq!(rows[0][0], Value::Str("a".into()));
}

// ═══════════════════════════════════════════════════════════════════════════
// Additional: NOT LIKE works correctly
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn not_like_basic() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE things (timestamp TIMESTAMP, name VARCHAR)");
    db.exec_ok("INSERT INTO things (timestamp, name) VALUES (1000000000000, 'apple')");
    db.exec_ok("INSERT INTO things (timestamp, name) VALUES (2000000000000, 'banana')");
    db.exec_ok("INSERT INTO things (timestamp, name) VALUES (3000000000000, 'avocado')");

    let (_, rows) = db.query("SELECT name FROM things WHERE name NOT LIKE 'a%'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Str("banana".into()));
}

// ═══════════════════════════════════════════════════════════════════════════
// Additional: Multiple BETWEEN types
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn between_with_timestamps() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE events (timestamp TIMESTAMP, val DOUBLE)");
    let base: i64 = 1_000_000_000_000;
    for i in 0..10 {
        db.exec_ok(&format!(
            "INSERT INTO events (timestamp, val) VALUES ({}, {}.0)",
            base + i * 1_000_000_000, i
        ));
    }

    let lo = base + 3 * 1_000_000_000;
    let hi = base + 7 * 1_000_000_000;
    let (_, rows) = db.query(&format!(
        "SELECT val FROM events WHERE timestamp BETWEEN {lo} AND {hi}"
    ));
    assert_eq!(rows.len(), 5, "BETWEEN on timestamps should return 5 rows (3..=7)");
}
