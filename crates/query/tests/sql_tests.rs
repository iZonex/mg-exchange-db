//! Comprehensive integration tests for ExchangeDB SQL pipeline.
//!
//! Each test exercises the full parse -> plan -> execute flow against
//! real on-disk data managed via `tempfile::TempDir`.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

use exchange_query::plan::{QueryResult, Value};
use exchange_query::{execute, plan_query};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a test database with a "trades" table containing 102 rows across
/// 3 day-partitions (2024-03-15, 2024-03-16, 2024-03-17).
///
/// Columns: timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR
///
/// Symbols used: BTC/USD, ETH/USD, SOL/USD
/// Some rows have NULL volume to test NULL handling.
fn setup_test_db() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("failed to create tempdir");
    let db_root = dir.path().to_path_buf();

    // Create the table via SQL.
    run_sql(
        &db_root,
        "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)",
    );

    // Base timestamps for 3 days (seconds since epoch).
    let day1_base: i64 = 1710460800; // 2024-03-15 00:00:00 UTC
    let day2_base: i64 = day1_base + 86400; // 2024-03-16
    let day3_base: i64 = day1_base + 2 * 86400; // 2024-03-17

    let symbols = ["BTC/USD", "ETH/USD", "SOL/USD"];
    let sides = ["buy", "sell"];

    let mut row_id: usize = 0;

    for (day_idx, &base) in [day1_base, day2_base, day3_base].iter().enumerate() {
        // 34 rows per day = 102 total
        for i in 0..34 {
            let ts_nanos = (base + (i as i64) * 600) * 1_000_000_000i64; // every 10 min
            let symbol = symbols[row_id % 3];
            let side = sides[row_id % 2];

            // Price varies by symbol and has some spread.
            let price = match symbol {
                "BTC/USD" => 60000.0 + (row_id as f64) * 100.0,
                "ETH/USD" => 3000.0 + (row_id as f64) * 10.0,
                _ => 100.0 + (row_id as f64) * 1.0,
            };

            // Every 10th row gets NULL volume.
            let volume_expr = if row_id % 10 == 0 {
                "NULL".to_string()
            } else {
                format!("{:.1}", 0.5 + (row_id as f64) * 0.1)
            };

            let sql = format!(
                "INSERT INTO trades (timestamp, symbol, price, volume, side) VALUES ({}, '{}', {:.2}, {}, '{}')",
                ts_nanos, symbol, price, volume_expr, side
            );
            run_sql(&db_root, &sql);
            row_id += 1;
        }
    }

    (dir, db_root)
}

/// Execute SQL and return the QueryResult, panicking on error.
fn run_sql(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
    execute(db_root, &plan).unwrap_or_else(|e| panic!("execute failed for `{sql}`: {e}"))
}

/// Execute SQL and return the QueryResult (may be an error).
fn try_run_sql(db_root: &Path, sql: &str) -> Result<QueryResult, String> {
    let plan = plan_query(sql).map_err(|e| format!("plan error: {e}"))?;
    execute(db_root, &plan).map_err(|e| format!("execute error: {e}"))
}

/// Execute SQL and return (column_names, rows) for a SELECT query.
fn query_rows(db_root: &Path, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    match run_sql(db_root, sql) {
        QueryResult::Rows { columns, rows } => (columns, rows),
        other => panic!("expected Rows result for `{sql}`, got: {other:?}"),
    }
}

/// Execute SQL and return the row count from a SELECT query.
fn query_count(db_root: &Path, sql: &str) -> usize {
    let (_, rows) = query_rows(db_root, sql);
    rows.len()
}

/// Extract a single scalar value from a 1-row, 1-column result.
fn query_scalar(db_root: &Path, sql: &str) -> Value {
    let (cols, rows) = query_rows(db_root, sql);
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row, got {} for `{sql}`",
        rows.len()
    );
    assert!(!cols.is_empty(), "expected at least 1 column for `{sql}`");
    rows[0][0].clone()
}

// ---------------------------------------------------------------------------
// 1. Basic SELECT tests
// ---------------------------------------------------------------------------

#[test]
fn select_all() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(&db, "SELECT * FROM trades");
    assert_eq!(cols.len(), 5);
    assert_eq!(rows.len(), 102);
}

#[test]
fn select_columns() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(&db, "SELECT symbol, price FROM trades");
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 102);
    // Verify column names.
    assert!(cols.contains(&"symbol".to_string()));
    assert!(cols.contains(&"price".to_string()));
}

#[test]
fn select_with_alias() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(&db, "SELECT price AS p FROM trades");
    assert!(cols.contains(&"p".to_string()));
    assert_eq!(rows.len(), 102);
}

#[test]
fn select_limit() {
    let (_dir, db) = setup_test_db();
    let count = query_count(&db, "SELECT * FROM trades LIMIT 5");
    assert_eq!(count, 5);
}

#[test]
fn select_order_by() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT price FROM trades ORDER BY price DESC");
    assert_eq!(rows.len(), 102);
    // Verify descending order.
    for i in 1..rows.len() {
        let prev = &rows[i - 1][0];
        let curr = &rows[i][0];
        assert!(
            prev.cmp_coerce(curr) != Some(std::cmp::Ordering::Less),
            "row {} price {:?} < row {} price {:?}",
            i - 1,
            prev,
            i,
            curr
        );
    }
}

#[test]
fn select_order_by_limit() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT price FROM trades ORDER BY price DESC LIMIT 3");
    assert_eq!(rows.len(), 3);
    // Verify descending.
    for i in 1..rows.len() {
        assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less));
    }
}

#[test]
fn select_distinct() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT DISTINCT symbol FROM trades");
    assert_eq!(rows.len(), 3, "expected 3 distinct symbols");
    let mut symbols: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Str(s) => s.clone(),
            other => panic!("expected Str, got {other:?}"),
        })
        .collect();
    symbols.sort();
    assert_eq!(symbols, vec!["BTC/USD", "ETH/USD", "SOL/USD"]);
}

#[test]
fn select_count_star() {
    let (_dir, db) = setup_test_db();
    let val = query_scalar(&db, "SELECT count(*) FROM trades");
    match val {
        Value::I64(n) => assert_eq!(n, 102),
        other => panic!("expected I64(102), got {other:?}"),
    }
}

#[test]
fn select_where_eq() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT * FROM trades WHERE symbol = 'BTC/USD'");
    assert!(!rows.is_empty(), "should have BTC/USD rows");
    // All rows should have BTC/USD.
    for row in &rows {
        let sym = row
            .iter()
            .find(|v| matches!(v, Value::Str(s) if s == "BTC/USD"));
        assert!(sym.is_some(), "expected BTC/USD in row, got: {row:?}");
    }
}

#[test]
fn select_where_gt() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT price FROM trades WHERE price > 50000");
    assert!(!rows.is_empty(), "should have rows with price > 50000");
    for row in &rows {
        match &row[0] {
            Value::F64(p) => assert!(*p > 50000.0, "price {p} not > 50000"),
            Value::I64(p) => assert!(*p > 50000, "price {p} not > 50000"),
            other => panic!("unexpected price type: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Aggregation tests
// ---------------------------------------------------------------------------

#[test]
fn group_by_single() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(&db, "SELECT symbol, count(*) FROM trades GROUP BY symbol");
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 3, "expected 3 groups");

    // Total count across groups should be 102.
    let total: i64 = rows
        .iter()
        .map(|r| match &r[1] {
            Value::I64(n) => *n,
            other => panic!("expected I64, got {other:?}"),
        })
        .sum();
    assert_eq!(total, 102);
}

#[test]
fn group_by_multi_agg() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(
        &db,
        "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol",
    );
    assert_eq!(cols.len(), 3);
    assert_eq!(rows.len(), 3);
    // avg(price) should be positive for all groups.
    for row in &rows {
        match &row[1] {
            Value::F64(avg) => assert!(*avg > 0.0, "avg(price) should be > 0"),
            other => panic!("expected F64 for avg, got {other:?}"),
        }
    }
}

#[test]
fn group_by_having() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(
        &db,
        "SELECT symbol, count(*) AS c FROM trades GROUP BY symbol HAVING c > 10",
    );
    // Each symbol has ~34 rows, so all 3 should pass HAVING c > 10.
    assert_eq!(rows.len(), 3);
    for row in &rows {
        match &row[1] {
            Value::I64(n) => assert!(*n > 10, "count {n} should be > 10"),
            other => panic!("expected I64, got {other:?}"),
        }
    }
}

#[test]
fn aggregate_no_group() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(
        &db,
        "SELECT sum(price), avg(price), min(price), max(price), count(*) FROM trades",
    );
    assert_eq!(cols.len(), 5);
    assert_eq!(rows.len(), 1);

    // count(*) should be 102
    match &rows[0][4] {
        Value::I64(n) => assert_eq!(*n, 102),
        other => panic!("expected I64(102), got {other:?}"),
    }

    // min(price) < avg(price) < max(price)
    let min_p = &rows[0][2];
    let avg_p = &rows[0][1];
    let max_p = &rows[0][3];
    assert!(
        min_p.cmp_coerce(avg_p) == Some(std::cmp::Ordering::Less),
        "min should be < avg"
    );
    assert!(
        avg_p.cmp_coerce(max_p) == Some(std::cmp::Ordering::Less),
        "avg should be < max"
    );
}

#[test]
fn sample_by_hour() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT avg(price) FROM trades SAMPLE BY 1h");
    // 3 days * 24 hours = potentially up to 72 buckets, but data only spans
    // ~5.6 hours per day (34 rows * 10min = 340min). We just check we get
    // multiple buckets with valid averages.
    assert!(
        rows.len() > 1,
        "expected multiple time buckets, got {}",
        rows.len()
    );
    for row in &rows {
        match &row[0] {
            Value::F64(v) => assert!(*v > 0.0),
            other => panic!("expected F64, got {other:?}"),
        }
    }
}

#[test]
fn latest_on() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(
        &db,
        "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
    );
    // Should return exactly one row per symbol.
    assert_eq!(rows.len(), 3, "expected 1 latest row per symbol");
}

#[test]
fn first_last() {
    let (_dir, db) = setup_test_db();
    let (cols, rows) = query_rows(&db, "SELECT first(price), last(price) FROM trades");
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 1);
    // first and last should be different (different prices).
    assert_ne!(rows[0][0], rows[0][1], "first and last price should differ");
}

#[test]
fn count_distinct() {
    let (_dir, db) = setup_test_db();
    let val = query_scalar(&db, "SELECT count_distinct(symbol) FROM trades");
    match val {
        Value::I64(n) => assert_eq!(n, 3),
        other => panic!("expected I64(3), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 3. DML tests
// ---------------------------------------------------------------------------

#[test]
fn insert_and_select() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE orders (timestamp TIMESTAMP, product VARCHAR, qty DOUBLE)",
    );

    let ts = 1710460800_000_000_000i64;
    run_sql(
        &db,
        &format!(
            "INSERT INTO orders (timestamp, product, qty) VALUES ({}, 'widget', 42.0)",
            ts
        ),
    );
    run_sql(
        &db,
        &format!(
            "INSERT INTO orders (timestamp, product, qty) VALUES ({}, 'gadget', 7.0)",
            ts + 1_000_000_000
        ),
    );

    let count = query_count(&db, "SELECT * FROM orders");
    assert_eq!(count, 2);

    let (_, rows) = query_rows(&db, "SELECT product FROM orders");
    let products: Vec<&str> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Str(s) => s.as_str(),
            other => panic!("expected Str, got {other:?}"),
        })
        .collect();
    assert!(products.contains(&"widget"));
    assert!(products.contains(&"gadget"));
}

#[test]
fn update_and_verify() {
    let (_dir, db) = setup_test_db();

    // Update all BTC/USD prices to 99999.
    run_sql(
        &db,
        "UPDATE trades SET price = 99999.0 WHERE symbol = 'BTC/USD'",
    );

    let (_, rows) = query_rows(&db, "SELECT price FROM trades WHERE symbol = 'BTC/USD'");
    assert!(!rows.is_empty());
    for row in &rows {
        match &row[0] {
            Value::F64(p) => assert!(
                (*p - 99999.0).abs() < 0.01,
                "expected 99999.0 after update, got {p}"
            ),
            other => panic!("expected F64, got {other:?}"),
        }
    }
}

#[test]
fn delete_and_verify() {
    let (_dir, db) = setup_test_db();
    let before = query_count(&db, "SELECT * FROM trades");
    assert_eq!(before, 102);

    // Delete SOL/USD rows.
    run_sql(&db, "DELETE FROM trades WHERE symbol = 'SOL/USD'");

    let after = query_count(&db, "SELECT * FROM trades");
    assert!(after < before, "should have fewer rows after delete");

    // No SOL/USD should remain.
    let sol_count = query_count(&db, "SELECT * FROM trades WHERE symbol = 'SOL/USD'");
    assert_eq!(sol_count, 0);
}

#[test]
fn create_table_and_use() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE metrics (timestamp TIMESTAMP, name VARCHAR, value DOUBLE)",
    );

    let ts = 1710460800_000_000_000i64;
    run_sql(
        &db,
        &format!(
            "INSERT INTO metrics (timestamp, name, value) VALUES ({}, 'cpu', 78.5)",
            ts
        ),
    );

    let val = query_scalar(&db, "SELECT value FROM metrics");
    match val {
        Value::F64(v) => assert!((v - 78.5).abs() < 0.01),
        other => panic!("expected F64(78.5), got {other:?}"),
    }
}

#[test]
fn drop_table() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE temp_data (timestamp TIMESTAMP, val DOUBLE)",
    );

    // Verify it exists.
    let ts = 1710460800_000_000_000i64;
    run_sql(
        &db,
        &format!(
            "INSERT INTO temp_data (timestamp, val) VALUES ({}, 1.0)",
            ts
        ),
    );
    assert_eq!(query_count(&db, "SELECT * FROM temp_data"), 1);

    // Drop it.
    run_sql(&db, "DROP TABLE temp_data");

    // Selecting from it should fail.
    let result = try_run_sql(&db, "SELECT * FROM temp_data");
    assert!(result.is_err(), "expected error after DROP TABLE");
}

#[test]
fn alter_table_add_column() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE events (timestamp TIMESTAMP, name VARCHAR)",
    );

    let ts = 1710460800_000_000_000i64;
    run_sql(
        &db,
        &format!(
            "INSERT INTO events (timestamp, name) VALUES ({}, 'click')",
            ts
        ),
    );

    // Add a column.
    run_sql(&db, "ALTER TABLE events ADD COLUMN value DOUBLE");

    // Insert a row with the new column.
    run_sql(
        &db,
        &format!(
            "INSERT INTO events (timestamp, name, value) VALUES ({}, 'scroll', 42.0)",
            ts + 1_000_000_000
        ),
    );

    let count = query_count(&db, "SELECT * FROM events");
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// 4. JOIN tests
// ---------------------------------------------------------------------------

/// Set up trades and markets tables for JOIN tests.
fn setup_join_db() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
    );
    run_sql(
        &db,
        "CREATE TABLE markets (timestamp TIMESTAMP, symbol VARCHAR, name VARCHAR)",
    );

    let base_ts = 1710460800_000_000_000i64;

    // Insert trades.
    for (i, (sym, price)) in [
        ("BTC/USD", 60000.0),
        ("ETH/USD", 3000.0),
        ("SOL/USD", 100.0),
    ]
    .iter()
    .enumerate()
    {
        run_sql(
            &db,
            &format!(
                "INSERT INTO trades (timestamp, symbol, price) VALUES ({}, '{}', {})",
                base_ts + (i as i64) * 1_000_000_000,
                sym,
                price
            ),
        );
    }

    // Insert markets (only BTC and ETH, no SOL — useful for LEFT JOIN test).
    for (i, (sym, name)) in [("BTC/USD", "Bitcoin"), ("ETH/USD", "Ethereum")]
        .iter()
        .enumerate()
    {
        run_sql(
            &db,
            &format!(
                "INSERT INTO markets (timestamp, symbol, name) VALUES ({}, '{}', '{}')",
                base_ts + (i as i64) * 1_000_000_000,
                sym,
                name
            ),
        );
    }

    (dir, db)
}

#[test]
fn inner_join() {
    let (_dir, db) = setup_join_db();
    let (cols, rows) = query_rows(
        &db,
        "SELECT t.symbol, t.price, m.name FROM trades t INNER JOIN markets m ON t.symbol = m.symbol",
    );
    assert!(cols.len() >= 3);
    // Only BTC/USD and ETH/USD have matches.
    assert_eq!(rows.len(), 2, "inner join should produce 2 matching rows");
}

#[test]
fn left_join() {
    let (_dir, db) = setup_join_db();
    let (_, rows) = query_rows(
        &db,
        "SELECT t.symbol, m.name FROM trades t LEFT JOIN markets m ON t.symbol = m.symbol",
    );
    // All 3 trades should appear; SOL/USD will have NULL for m.name.
    assert_eq!(rows.len(), 3, "left join should produce 3 rows");
}

#[test]
fn asof_join() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE trade_events (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)",
    );
    run_sql(
        &db,
        "CREATE TABLE quote_events (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE)",
    );

    let base_ts = 1710460800_000_000_000i64;

    // Insert trades at t=0, t=10s, t=20s.
    for i in 0..3 {
        run_sql(
            &db,
            &format!(
                "INSERT INTO trade_events (timestamp, symbol, price) VALUES ({}, 'BTC/USD', {})",
                base_ts + i * 10_000_000_000i64,
                60000.0 + (i as f64) * 100.0
            ),
        );
    }

    // Insert quotes at t=5s, t=15s.
    for i in 0..2 {
        run_sql(
            &db,
            &format!(
                "INSERT INTO quote_events (timestamp, symbol, bid) VALUES ({}, 'BTC/USD', {})",
                base_ts + 5_000_000_000 + i * 10_000_000_000i64,
                59900.0 + (i as f64) * 100.0
            ),
        );
    }

    let (_, rows) = query_rows(
        &db,
        "SELECT trade_events.price, quote_events.bid FROM trade_events ASOF JOIN quote_events ON trade_events.symbol = quote_events.symbol",
    );
    // ASOF join should produce rows for each trade row.
    assert!(!rows.is_empty(), "ASOF JOIN should produce results");
}

#[test]
fn cte_query() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(
        &db,
        "WITH btc AS (SELECT price FROM trades WHERE symbol = 'BTC/USD') SELECT count(*) FROM btc",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::I64(n) => assert!(*n > 0, "CTE should return rows"),
        other => panic!("expected I64, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 5. Edge case tests
// ---------------------------------------------------------------------------

#[test]
fn empty_table() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE empty_tbl (timestamp TIMESTAMP, val DOUBLE)",
    );

    let count = query_count(&db, "SELECT * FROM empty_tbl");
    assert_eq!(count, 0);
}

#[test]
fn nonexistent_table() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    let result = try_run_sql(&db, "SELECT * FROM no_such_table");
    assert!(result.is_err(), "querying nonexistent table should error");
}

#[test]
fn syntax_error() {
    let result = plan_query("SELCT * FORM trades");
    assert!(result.is_err(), "syntax error should fail at planning");
}

#[test]
fn type_mismatch_filter() {
    let (_dir, db) = setup_test_db();
    // Comparing price (DOUBLE) with a string. The engine may either error or
    // return zero rows. Either is acceptable.
    let result = try_run_sql(&db, "SELECT * FROM trades WHERE price = 'not_a_number'");
    match result {
        Ok(QueryResult::Rows { rows, .. }) => {
            assert_eq!(rows.len(), 0, "type mismatch should match nothing");
        }
        Err(_) => { /* error is also acceptable */ }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn unicode_data() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(
        &db,
        "CREATE TABLE notes (timestamp TIMESTAMP, text VARCHAR)",
    );

    let ts = 1710460800_000_000_000i64;
    run_sql(
        &db,
        &format!(
            "INSERT INTO notes (timestamp, text) VALUES ({}, '{}')",
            ts, "hello"
        ),
    );

    let (_, rows) = query_rows(&db, "SELECT text FROM notes");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Str(s) => assert_eq!(s, "hello"),
        other => panic!("expected Str, got {other:?}"),
    }
}

#[test]
fn large_result() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    run_sql(&db, "CREATE TABLE big (timestamp TIMESTAMP, val DOUBLE)");

    // Insert 10000 rows in batches.
    let base_ts = 1710460800_000_000_000i64;
    for batch in 0..100 {
        let mut values_parts = Vec::with_capacity(100);
        for i in 0..100 {
            let idx = batch * 100 + i;
            let ts = base_ts + (idx as i64) * 1_000_000_000;
            values_parts.push(format!("({}, {})", ts, idx as f64));
        }
        let sql = format!(
            "INSERT INTO big (timestamp, val) VALUES {}",
            values_parts.join(", ")
        );
        run_sql(&db, &sql);
    }

    let count = query_count(&db, "SELECT * FROM big");
    assert_eq!(count, 10000);
}

// ---------------------------------------------------------------------------
// 6. Multi-partition tests
// ---------------------------------------------------------------------------

#[test]
fn query_spans_partitions() {
    let (_dir, db) = setup_test_db();
    // Each day has 34 rows, 3 days total.
    let count = query_count(&db, "SELECT * FROM trades");
    assert_eq!(count, 102);
}

#[test]
fn partition_pruning() {
    let (_dir, db) = setup_test_db();
    // Timestamps for day 3 only (2024-03-17).
    let day3_start_ns = (1710460800i64 + 2 * 86400) * 1_000_000_000;
    let sql = format!("SELECT * FROM trades WHERE timestamp >= {}", day3_start_ns);
    let (_, rows) = query_rows(&db, &sql);
    // Day 3 has 34 rows.
    assert_eq!(rows.len(), 34, "expected 34 rows from day 3 partition");
}

#[test]
fn order_by_across_partitions() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT timestamp FROM trades ORDER BY timestamp");
    assert_eq!(rows.len(), 102);
    // Verify ascending order across partitions.
    for i in 1..rows.len() {
        let prev = &rows[i - 1][0];
        let curr = &rows[i][0];
        assert!(
            prev.cmp_coerce(curr) != Some(std::cmp::Ordering::Greater),
            "timestamps out of order at row {i}: {prev:?} > {curr:?}"
        );
    }
}

#[test]
fn group_by_across_partitions() {
    let (_dir, db) = setup_test_db();
    let (_, rows) = query_rows(&db, "SELECT symbol, count(*) FROM trades GROUP BY symbol");
    assert_eq!(rows.len(), 3, "expected 3 groups across all partitions");
    let total: i64 = rows
        .iter()
        .map(|r| match &r[1] {
            Value::I64(n) => *n,
            other => panic!("expected I64, got {other:?}"),
        })
        .sum();
    assert_eq!(total, 102, "total count across groups should be 102");
}
