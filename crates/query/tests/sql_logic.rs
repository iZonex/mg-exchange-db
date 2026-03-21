//! Simplified SQL logic test runner.
//!
//! Runs a suite of SQL statements against a fresh database and verifies
//! that each produces the expected result. This tests the full
//! parse -> plan -> execute pipeline for a wide variety of SQL patterns.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

use exchange_query::plan::{QueryResult, Value};
use exchange_query::{execute, plan_query};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Execute SQL and return the QueryResult, panicking on error.
fn run_sql(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
    execute(db_root, &plan).unwrap_or_else(|e| panic!("execute failed for `{sql}`: {e}"))
}

/// Execute a SELECT and return the single scalar value as a string.
fn query_scalar_str(db_root: &Path, sql: &str) -> String {
    match run_sql(db_root, sql) {
        QueryResult::Rows { rows, .. } => {
            assert!(
                !rows.is_empty() && !rows[0].is_empty(),
                "expected 1 row with 1 column for `{sql}`"
            );
            value_to_string(&rows[0][0])
        }
        other => panic!("expected Rows for `{sql}`, got {other:?}"),
    }
}

/// Convert a Value to its display string for comparison.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::I64(n) => n.to_string(),
        Value::F64(f) => {
            // Normalize: remove trailing zeros but keep at least one decimal.
            if *f == (*f as i64) as f64 && f.abs() < 1e15 {
                format!("{}", *f as i64)
            } else {
                format!("{f}")
            }
        }
        Value::Str(s) => s.clone(),
        Value::Timestamp(ns) => ns.to_string(),
    }
}

/// Set up a database with a "data" table for logic tests.
fn setup_logic_db() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    // Create a table with various column types for testing.
    run_sql(
        &db,
        "CREATE TABLE data (timestamp TIMESTAMP, label VARCHAR, x DOUBLE, y DOUBLE)",
    );

    let base_ts = 1_710_460_800_000_000_000i64;

    // Insert test data.
    let rows = vec![
        (base_ts, "alpha", 10.0, 20.0),
        (base_ts + 1_000_000_000, "beta", 30.0, 40.0),
        (base_ts + 2_000_000_000, "gamma", 50.0, 60.0),
        (base_ts + 3_000_000_000, "delta", 70.0, 80.0),
        (base_ts + 4_000_000_000, "alpha", 15.0, 25.0),
    ];

    for (ts, label, x, y) in &rows {
        run_sql(
            &db,
            &format!(
                "INSERT INTO data (timestamp, label, x, y) VALUES ({ts}, '{label}', {x}, {y})"
            ),
        );
    }

    (dir, db)
}

// ---------------------------------------------------------------------------
// SQL logic test suite
// ---------------------------------------------------------------------------

/// A single test case: (sql, expected_value_as_string).
struct LogicCase {
    sql: &'static str,
    expected: &'static str,
    /// Description for error messages.
    desc: &'static str,
}

impl LogicCase {
    const fn new(sql: &'static str, expected: &'static str, desc: &'static str) -> Self {
        Self {
            sql,
            expected,
            desc,
        }
    }
}

#[test]
fn sql_logic_suite() {
    let (_dir, db) = setup_logic_db();

    let cases = vec![
        // --- Aggregate functions ---
        LogicCase::new("SELECT count(*) FROM data", "5", "count all rows"),
        LogicCase::new("SELECT sum(x) FROM data", "175", "sum of x column"),
        LogicCase::new("SELECT min(x) FROM data", "10", "minimum x"),
        LogicCase::new("SELECT max(x) FROM data", "70", "maximum x"),
        LogicCase::new("SELECT avg(x) FROM data", "35", "average x"),
        LogicCase::new("SELECT sum(y) FROM data", "225", "sum of y column"),
        LogicCase::new("SELECT min(y) FROM data", "20", "minimum y"),
        LogicCase::new("SELECT max(y) FROM data", "80", "maximum y"),
        // --- Filtered aggregates ---
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x > 20",
            "3",
            "count where x > 20",
        ),
        LogicCase::new(
            "SELECT sum(x) FROM data WHERE label = 'alpha'",
            "25",
            "sum x for alpha",
        ),
        LogicCase::new(
            "SELECT max(x) FROM data WHERE x < 50",
            "30",
            "max x where x < 50",
        ),
        LogicCase::new(
            "SELECT min(y) FROM data WHERE y > 30",
            "40",
            "min y where y > 30",
        ),
        LogicCase::new(
            "SELECT count(*) FROM data WHERE label = 'alpha'",
            "2",
            "count alpha rows",
        ),
        // --- GROUP BY ---
        LogicCase::new(
            "SELECT count(*) FROM data WHERE label = 'beta'",
            "1",
            "count beta rows",
        ),
        // --- LIMIT ---
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x >= 10",
            "5",
            "all rows have x >= 10",
        ),
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x > 70",
            "0",
            "no rows with x > 70",
        ),
        // --- DISTINCT via count_distinct ---
        LogicCase::new(
            "SELECT count_distinct(label) FROM data",
            "4",
            "4 distinct labels",
        ),
        // --- first/last ---
        LogicCase::new("SELECT first(x) FROM data", "10", "first x value"),
        LogicCase::new("SELECT last(x) FROM data", "15", "last x value"),
        LogicCase::new("SELECT first(label) FROM data", "alpha", "first label"),
        LogicCase::new("SELECT last(label) FROM data", "alpha", "last label"),
        // --- Arithmetic in filter ---
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x > 15",
            "3",
            "count where x > 15",
        ),
        LogicCase::new(
            "SELECT count(*) FROM data WHERE y >= 60",
            "2",
            "count where y >= 60",
        ),
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x >= 30",
            "3",
            "count where x >= 30",
        ),
        // --- Combined filters ---
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x > 10 AND y < 80",
            "3",
            "combined AND filter",
        ),
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x < 20 OR x > 60",
            "3",
            "combined OR filter",
        ),
        // --- ORDER BY + LIMIT ---
        LogicCase::new("SELECT min(x) FROM data", "10", "min x is 10"),
        LogicCase::new("SELECT max(y) FROM data", "80", "max y is 80"),
        // --- WHERE with equality ---
        LogicCase::new(
            "SELECT sum(x) FROM data WHERE label = 'gamma'",
            "50",
            "sum x for gamma",
        ),
        LogicCase::new(
            "SELECT sum(x) FROM data WHERE label = 'delta'",
            "70",
            "sum x for delta",
        ),
        // --- Edge: all filtered out ---
        LogicCase::new(
            "SELECT count(*) FROM data WHERE x > 1000",
            "0",
            "no matches returns 0",
        ),
    ];

    let mut failures = Vec::new();

    for (i, case) in cases.iter().enumerate() {
        let actual = query_scalar_str(&db, case.sql);
        if actual != case.expected {
            failures.push(format!(
                "  Case {}: {} — `{}`\n    expected: {}\n    actual:   {}",
                i + 1,
                case.desc,
                case.sql,
                case.expected,
                actual
            ));
        }
    }

    if !failures.is_empty() {
        panic!(
            "\n{} sql_logic test(s) FAILED:\n{}\n",
            failures.len(),
            failures.join("\n")
        );
    }
}

/// Test a variety of SELECT patterns with multiple result rows.
#[test]
fn sql_logic_multi_row() {
    let (_dir, db) = setup_logic_db();

    // DISTINCT labels.
    let result = run_sql(&db, "SELECT DISTINCT label FROM data");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 4, "expected 4 distinct labels");
    } else {
        panic!("expected Rows");
    }

    // ORDER BY x ASC.
    let result = run_sql(&db, "SELECT x FROM data ORDER BY x");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 5);
        let xs: Vec<f64> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::F64(f) => *f,
                Value::I64(i) => *i as f64,
                other => panic!("expected numeric, got {other:?}"),
            })
            .collect();
        for i in 1..xs.len() {
            assert!(xs[i - 1] <= xs[i], "not sorted ascending at position {i}");
        }
    }

    // ORDER BY x DESC LIMIT 2.
    let result = run_sql(&db, "SELECT x FROM data ORDER BY x DESC LIMIT 2");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2);
        let first_x = match &rows[0][0] {
            Value::F64(f) => *f,
            Value::I64(i) => *i as f64,
            other => panic!("expected numeric, got {other:?}"),
        };
        assert!((first_x - 70.0).abs() < 0.01, "top row should be 70.0");
    }

    // GROUP BY label with count.
    let result = run_sql(&db, "SELECT label, count(*) FROM data GROUP BY label");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 4, "4 distinct labels => 4 groups");
        // Find the "alpha" group — should have count 2.
        let alpha_row = rows
            .iter()
            .find(|r| r[0] == Value::Str("alpha".to_string()));
        assert!(alpha_row.is_some(), "should have alpha group");
        match &alpha_row.unwrap()[1] {
            Value::I64(n) => assert_eq!(*n, 2, "alpha should have count 2"),
            other => panic!("expected I64, got {other:?}"),
        }
    }
}

/// Test DML operations in sequence.
#[test]
fn sql_logic_dml_sequence() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    // Create table.
    run_sql(
        &db,
        "CREATE TABLE counters (timestamp TIMESTAMP, name VARCHAR, val DOUBLE)",
    );

    let base_ts = 1_710_460_800_000_000_000i64;

    // Insert 3 rows.
    for i in 0..3 {
        run_sql(
            &db,
            &format!(
                "INSERT INTO counters (timestamp, name, val) VALUES ({}, 'c{}', {})",
                base_ts + i * 1_000_000_000,
                i,
                i * 10
            ),
        );
    }

    // Verify count.
    let count = query_scalar_str(&db, "SELECT count(*) FROM counters");
    assert_eq!(count, "3", "should have 3 rows after insert");

    // Update one row.
    run_sql(&db, "UPDATE counters SET val = 999 WHERE name = 'c1'");
    let sum = query_scalar_str(&db, "SELECT sum(val) FROM counters");
    // c0=0, c1=999, c2=20 => 1019
    assert_eq!(sum, "1019", "sum after update should be 1019");

    // Delete one row.
    run_sql(&db, "DELETE FROM counters WHERE name = 'c0'");
    let count_after = query_scalar_str(&db, "SELECT count(*) FROM counters");
    assert_eq!(count_after, "2", "should have 2 rows after delete");
}

/// Test M1 SQL gap features.
#[test]
fn sql_logic_m1_gaps() {
    let (_dir, db) = setup_logic_db();

    // --- 1. Nested function calls: round(avg(price), 2) ---
    let result = run_sql(&db, "SELECT round(avg(x), 2) FROM data");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 1, "should have 1 aggregated row");
        let val = match &rows[0][0] {
            Value::F64(f) => *f,
            Value::I64(i) => *i as f64,
            other => panic!("expected numeric for round(avg(x)), got {other:?}"),
        };
        assert!(
            (val - 35.0).abs() < 0.01,
            "round(avg(x), 2) should be 35.0, got {val}"
        );
    } else {
        panic!("expected Rows");
    }

    // --- 2. Column aliases in GROUP BY/ORDER BY ---
    let result = run_sql(&db, "SELECT label AS lbl FROM data ORDER BY lbl");
    if let QueryResult::Rows { columns, rows, .. } = result {
        assert!(
            columns.contains(&"lbl".to_string()),
            "column should be aliased to 'lbl', got {:?}",
            columns
        );
        assert_eq!(rows.len(), 5);
    } else {
        panic!("expected Rows");
    }

    // --- 3. NULL literal in expressions (COALESCE) ---
    let result = run_sql(&db, "SELECT coalesce(label, 'unknown') FROM data");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 5);
        // All labels are non-null, so should return the actual labels.
        for row in &rows {
            assert!(
                matches!(&row[0], Value::Str(_)),
                "coalesce should return string, got {:?}",
                row[0]
            );
        }
    } else {
        panic!("expected Rows");
    }

    // --- 4. Negative numbers in WHERE ---
    let count = query_scalar_str(&db, "SELECT count(*) FROM data WHERE x > -100");
    assert_eq!(count, "5", "all rows should have x > -100");

    let count = query_scalar_str(&db, "SELECT count(*) FROM data WHERE x > -1");
    assert_eq!(count, "5", "all rows should have x > -1");

    // --- 5. Multiple column ORDER BY ---
    let result = run_sql(&db, "SELECT label, x FROM data ORDER BY label ASC, x DESC");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 5);
        // alpha rows should come first (alpha < beta < delta < gamma).
        // For alpha, x should be DESC: 15.0 before 10.0.
        let first_label = match &rows[0][0] {
            Value::Str(s) => s.clone(),
            _ => panic!("expected string label"),
        };
        assert_eq!(first_label, "alpha", "first row should be alpha");

        // Check that the two alpha rows have x in descending order.
        let alpha_rows: Vec<f64> = rows
            .iter()
            .filter(|r| r[0] == Value::Str("alpha".to_string()))
            .map(|r| match &r[1] {
                Value::F64(f) => *f,
                Value::I64(i) => *i as f64,
                other => panic!("expected numeric, got {other:?}"),
            })
            .collect();
        assert_eq!(alpha_rows.len(), 2);
        assert!(
            alpha_rows[0] >= alpha_rows[1],
            "alpha x values should be DESC: {:?}",
            alpha_rows
        );
    } else {
        panic!("expected Rows");
    }

    // --- 6. OFFSET ---
    let result = run_sql(&db, "SELECT x FROM data ORDER BY x LIMIT 2 OFFSET 1");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2, "LIMIT 2 OFFSET 1 should return 2 rows");
        // ORDER BY x gives [10, 15, 30, 50, 70]. OFFSET 1 skips 10, LIMIT 2 gives [15, 30].
        let first_x = match &rows[0][0] {
            Value::F64(f) => *f,
            Value::I64(i) => *i as f64,
            other => panic!("expected numeric, got {other:?}"),
        };
        assert!(
            (first_x - 15.0).abs() < 0.01,
            "first value after OFFSET 1 should be 15, got {first_x}"
        );
    } else {
        panic!("expected Rows");
    }

    // OFFSET beyond data size.
    let result = run_sql(&db, "SELECT x FROM data OFFSET 100");
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 0, "OFFSET 100 should return 0 rows");
    } else {
        panic!("expected Rows");
    }

    // --- 7. Expression aliases in HAVING ---
    let result = run_sql(
        &db,
        "SELECT label, count(*) FROM data GROUP BY label HAVING count(*) >= 2",
    );
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 1, "only alpha has count >= 2");
        assert_eq!(rows[0][0], Value::Str("alpha".to_string()));
    } else {
        panic!("expected Rows");
    }
}

/// Test error cases that should be caught at parse or plan time.
#[test]
fn sql_logic_errors() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().to_path_buf();

    // Syntax error.
    assert!(plan_query("SELCT * FROM foo").is_err(), "syntax error");
    assert!(plan_query("SELECT FROM").is_err(), "incomplete SELECT");
    assert!(plan_query("INSERT INTO").is_err(), "incomplete INSERT");

    // Table not found.
    let plan = plan_query("SELECT * FROM nonexistent").unwrap();
    assert!(execute(&db, &plan).is_err(), "table not found");

    // Drop non-existent table.
    let plan = plan_query("DROP TABLE ghost").unwrap();
    assert!(execute(&db, &plan).is_err(), "drop non-existent");
}
