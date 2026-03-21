//! SQL Logic Test runner in the sqllogictest format.
//!
//! Supports:
//!   - `statement ok` / `statement error`: execute a statement, expect success or error.
//!   - `query <type_string> <sort_mode>`: execute a query and compare results.
//!
//! Format example:
//! ```text
//! statement ok
//! CREATE TABLE t (timestamp TIMESTAMP, x DOUBLE)
//!
//! statement ok
//! INSERT INTO t (timestamp, x) VALUES (1000000000, 42.5)
//!
//! query R nosort
//! SELECT x FROM t
//! ----
//! 42.5
//!
//! query IR rowsort
//! SELECT count(*), sum(x) FROM t
//! ----
//! 1
//! 42.5
//! ```

use std::path::Path;

use tempfile::TempDir;

use exchange_query::plan::{QueryResult, Value};
use exchange_query::{execute, plan_query};

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

/// Result of running a sqllogictest suite.
struct SltResult {
    passed: usize,
    failed: usize,
    errors: Vec<String>,
}

/// Sort mode for query results.
#[derive(Debug, Clone, Copy, PartialEq)]
enum SortMode {
    NoSort,
    RowSort,
    ValueSort,
}

/// A single test directive parsed from the .slt file.
#[derive(Debug)]
enum Directive {
    StatementOk(String),
    StatementError(String),
    Query {
        sql: String,
        sort_mode: SortMode,
        expected_values: Vec<String>,
    },
}

/// Parse an .slt file into a list of directives.
fn parse_slt(input: &str) -> Vec<Directive> {
    let mut directives = Vec::new();
    let lines: Vec<&str> = input.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        // Skip blank lines and comments.
        if line.is_empty() || line.starts_with('#') {
            i += 1;
            continue;
        }

        if line == "statement ok" {
            i += 1;
            let sql = collect_sql_lines(&lines, &mut i);
            directives.push(Directive::StatementOk(sql));
        } else if line == "statement error" {
            i += 1;
            let sql = collect_sql_lines(&lines, &mut i);
            directives.push(Directive::StatementError(sql));
        } else if line.starts_with("query ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            let sort_mode = if parts.len() >= 3 {
                match parts[2] {
                    "rowsort" => SortMode::RowSort,
                    "valuesort" => SortMode::ValueSort,
                    _ => SortMode::NoSort,
                }
            } else {
                SortMode::NoSort
            };

            i += 1;
            let sql = collect_sql_until_separator(&lines, &mut i);

            // Expect "----" separator.
            let mut expected_values = Vec::new();
            if i < lines.len() && lines[i].trim() == "----" {
                i += 1;
                // Collect expected values until blank line or end.
                while i < lines.len() && !lines[i].trim().is_empty() {
                    let val = lines[i].trim();
                    if val == "(empty)" {
                        // Represents zero rows.
                        break;
                    }
                    expected_values.push(val.to_string());
                    i += 1;
                }
            }

            directives.push(Directive::Query {
                sql,
                sort_mode,
                expected_values,
            });
        } else {
            // Unknown directive, skip.
            i += 1;
        }
    }

    directives
}

/// Collect SQL lines until a blank line, returning the concatenated SQL.
fn collect_sql_lines(lines: &[&str], i: &mut usize) -> String {
    let mut sql_parts = Vec::new();
    while *i < lines.len() && !lines[*i].trim().is_empty() {
        sql_parts.push(lines[*i].trim());
        *i += 1;
    }
    sql_parts.join(" ")
}

/// Collect SQL lines until "----" separator or blank line.
fn collect_sql_until_separator(lines: &[&str], i: &mut usize) -> String {
    let mut sql_parts = Vec::new();
    while *i < lines.len() {
        let trimmed = lines[*i].trim();
        if trimmed == "----" || trimmed.is_empty() {
            break;
        }
        sql_parts.push(trimmed);
        *i += 1;
    }
    sql_parts.join(" ")
}

/// Run a sqllogictest suite against a fresh database.
fn run_sqllogictest(input: &str) -> SltResult {
    let dir = TempDir::new().expect("failed to create tempdir");
    let db_root = dir.path().to_path_buf();

    let directives = parse_slt(input);
    let mut passed = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for (idx, directive) in directives.iter().enumerate() {
        match directive {
            Directive::StatementOk(sql) => match exec_sql(&db_root, sql) {
                Ok(_) => passed += 1,
                Err(e) => {
                    failed += 1;
                    errors.push(format!(
                        "Directive {}: statement ok failed for `{sql}`: {e}",
                        idx + 1
                    ));
                }
            },
            Directive::StatementError(sql) => match exec_sql(&db_root, sql) {
                Ok(_) => {
                    failed += 1;
                    errors.push(format!(
                        "Directive {}: expected error for `{sql}`, but got Ok",
                        idx + 1
                    ));
                }
                Err(_) => passed += 1,
            },
            Directive::Query {
                sql,
                sort_mode,
                expected_values,
            } => {
                match exec_sql(&db_root, sql) {
                    Ok(result) => {
                        let mut actual_values = result_to_values(&result);

                        // Apply sort mode.
                        let mut expected_sorted = expected_values.clone();
                        match sort_mode {
                            SortMode::RowSort | SortMode::ValueSort => {
                                actual_values.sort();
                                expected_sorted.sort();
                            }
                            SortMode::NoSort => {}
                        }

                        if actual_values == expected_sorted {
                            passed += 1;
                        } else {
                            failed += 1;
                            errors.push(format!(
                                "Directive {}: query mismatch for `{sql}`\n  expected: {:?}\n  actual:   {:?}",
                                idx + 1, expected_sorted, actual_values
                            ));
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        errors.push(format!(
                            "Directive {}: query failed for `{sql}`: {e}",
                            idx + 1
                        ));
                    }
                }
            }
        }
    }

    SltResult {
        passed,
        failed,
        errors,
    }
}

/// Execute SQL and return the QueryResult.
fn exec_sql(db_root: &Path, sql: &str) -> Result<QueryResult, String> {
    let plan = plan_query(sql).map_err(|e| e.to_string())?;
    execute(db_root, &plan).map_err(|e| e.to_string())
}

/// Convert a QueryResult into a flat list of string values.
/// Each cell is one entry; multi-column rows are flattened left-to-right.
fn result_to_values(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Ok { affected_rows } => {
            vec![affected_rows.to_string()]
        }
        QueryResult::Rows { rows, .. } => {
            let mut values = Vec::new();
            for row in rows {
                for val in row {
                    values.push(value_to_string(val));
                }
            }
            values
        }
    }
}

/// Convert a Value to its display string.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::I64(n) => n.to_string(),
        Value::F64(f) => {
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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[test]
fn sqllogictest_basic_suite() {
    let test = include_str!("basic.slt");
    let result = run_sqllogictest(test);

    if result.failed > 0 {
        panic!(
            "\nsqllogictest: {} passed, {} FAILED\nErrors:\n{}\n",
            result.passed,
            result.failed,
            result.errors.join("\n")
        );
    }

    assert!(result.passed > 0, "expected at least one test case to pass");
    eprintln!(
        "sqllogictest: {} passed, {} failed",
        result.passed, result.failed
    );
}
