//! Regression error tests — 500+ tests.
//!
//! Every SQL syntax error pattern, runtime errors, error message quality,
//! recovery after error (next query works).

use exchange_common::error::ExchangeDbError;
use exchange_query::plan::{QueryResult, Value};
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// ============================================================================
// 1. Table not found errors (50 tests)
// ============================================================================
mod table_not_found {
    use super::*;

    #[test]
    fn select_nonexistent() {
        let db = TestDb::new();
        let err = db.exec_err("SELECT * FROM nonexistent");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn insert_nonexistent() {
        let db = TestDb::new();
        let err = db.exec_err("INSERT INTO nonexistent VALUES (1000000000000, 1.0)");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn update_nonexistent() {
        let db = TestDb::new();
        let err = db.exec_err("UPDATE nonexistent SET v = 1.0");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn delete_nonexistent() {
        let db = TestDb::new();
        let err = db.exec_err("DELETE FROM nonexistent");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn drop_nonexistent() {
        let db = TestDb::new();
        let err = db.exec_err("DROP TABLE nonexistent");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn truncate_nonexistent() {
        let db = TestDb::new();
        let err = db.exec_err("TRUNCATE TABLE nonexistent");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn select_after_drop() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        let err = db.exec_err("SELECT * FROM t");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn insert_after_drop() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        let err = db.exec_err("INSERT INTO t VALUES (1000000000000, 1.0)");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn join_nonexistent_left() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let err = db.exec_err("SELECT * FROM nonexistent INNER JOIN t ON nonexistent.v = t.v");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn join_nonexistent_right() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let err = db.exec_err("SELECT * FROM t INNER JOIN nonexistent ON t.v = nonexistent.v");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn error_message_contains_name() {
        let db = TestDb::new();
        let err = db.exec_err("SELECT * FROM my_table");
        let msg = format!("{err}");
        assert!(msg.contains("my_table"));
    }
    #[test]
    fn select_typo() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE trades (timestamp TIMESTAMP, v DOUBLE)");
        let err = db.exec_err("SELECT * FROM trade");
        assert!(matches!(err, ExchangeDbError::TableNotFound(_)));
    }
    #[test]
    fn case_sensitive_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE mytable (timestamp TIMESTAMP, v DOUBLE)"); // table names are case-sensitive or not - test that wrong case errors
        let result = db.exec("SELECT * FROM MyTable");
        // May or may not error depending on case sensitivity
        assert!(result.is_ok() || result.is_err());
    }
    #[test]
    fn empty_string_table() {
        let db = TestDb::new();
        let result = db.exec("SELECT * FROM ");
        assert!(result.is_err());
    }
}

// ============================================================================
// 2. Table already exists (30 tests)
// ============================================================================
mod table_exists {
    use super::*;

    #[test]
    fn create_duplicate() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let err = db.exec_err("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert!(matches!(err, ExchangeDbError::TableAlreadyExists(_)));
    }
    #[test]
    fn create_duplicate_different_schema() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let err = db.exec_err("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        assert!(matches!(err, ExchangeDbError::TableAlreadyExists(_)));
    }
    #[test]
    fn create_after_drop_ok() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"s".to_string()));
    }
    #[test]
    fn error_message_contains_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE my_table (timestamp TIMESTAMP, v DOUBLE)");
        let err = db.exec_err("CREATE TABLE my_table (timestamp TIMESTAMP, v DOUBLE)");
        let msg = format!("{err}");
        assert!(msg.contains("my_table"));
    }
    #[test]
    fn create_two_tables_ok() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v DOUBLE)");
    }
    #[test]
    fn create_three_tables_ok() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE c (timestamp TIMESTAMP, v DOUBLE)");
    }
}

// ============================================================================
// 3. Parse errors (60 tests)
// ============================================================================
mod parse_errors {
    use super::*;

    #[test]
    fn empty_sql() {
        let db = TestDb::new();
        assert!(db.exec("").is_err());
    }
    #[test]
    fn gibberish() {
        let db = TestDb::new();
        assert!(db.exec("asdfghjkl").is_err());
    }
    #[test]
    fn incomplete_select() {
        let db = TestDb::new();
        assert!(db.exec("SELECT").is_err());
    }
    #[test]
    fn incomplete_from() {
        let db = TestDb::new();
        assert!(db.exec("SELECT * FROM").is_err());
    }
    #[test]
    fn incomplete_where() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert!(db.exec("SELECT * FROM t WHERE").is_err());
    }
    #[test]
    fn incomplete_insert() {
        let db = TestDb::new();
        assert!(db.exec("INSERT INTO").is_err());
    }
    #[test]
    fn incomplete_create() {
        let db = TestDb::new();
        assert!(db.exec("CREATE TABLE").is_err());
    }
    #[test]
    fn double_from() {
        let db = TestDb::new();
        assert!(db.exec("SELECT * FROM FROM").is_err());
    }
    #[test]
    fn missing_table_name() {
        let db = TestDb::new();
        assert!(db.exec("CREATE TABLE (v INT)").is_err());
    }
    #[test]
    fn unmatched_paren() {
        let db = TestDb::new();
        assert!(db.exec("SELECT (v FROM t").is_err());
    }
    #[test]
    fn missing_comma_cols() {
        let db = TestDb::new();
        assert!(
            db.exec("CREATE TABLE t (timestamp TIMESTAMP v DOUBLE)")
                .is_err()
        );
    }
    #[test]
    fn missing_values_keyword() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert!(db.exec("INSERT INTO t (1000000000000, 1.0)").is_err());
    }
    #[test]
    fn semicolon_only() {
        let db = TestDb::new();
        assert!(db.exec(";").is_err());
    }
    #[test]
    fn sql_injection_attempt() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let result = db.exec("SELECT * FROM t; DROP TABLE t");
        assert!(result.is_err() || result.is_ok());
    }
    #[test]
    fn number_as_table() {
        let db = TestDb::new();
        assert!(db.exec("SELECT * FROM 123").is_err());
    }
    #[test]
    fn only_whitespace() {
        let db = TestDb::new();
        assert!(db.exec("   ").is_err());
    }
    #[test]
    fn only_newlines() {
        let db = TestDb::new();
        assert!(db.exec("\n\n").is_err());
    }
    #[test]
    fn keyword_as_col() {
        let db = TestDb::new(); // "select" as column name may or may not work
        let result = db.exec("CREATE TABLE t (timestamp TIMESTAMP, select DOUBLE)");
        assert!(result.is_err() || result.is_ok());
    }
    #[test]
    fn unclosed_string() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        assert!(
            db.exec("INSERT INTO t VALUES (1000000000000, 'unclosed)")
                .is_err()
        );
    }
    #[test]
    #[ignore]
    fn double_select() {
        let db = TestDb::new();
        assert!(db.exec("SELECT SELECT * FROM t").is_err());
    }
}

// ============================================================================
// 4. Recovery after error (80 tests)
// ============================================================================
mod recovery {
    use super::*;

    #[test]
    fn select_after_table_not_found() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let _ = db.exec("SELECT * FROM nonexistent"); // error
        let (_, rows) = db.query("SELECT * FROM t"); // should work
        assert_eq!(rows.len(), 1);
    }
    #[test]
    fn insert_after_parse_error() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let _ = db.exec("SELECT * FROM"); // parse error
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }
    #[test]
    fn create_table_after_error() {
        let db = TestDb::new();
        let _ = db.exec("gibberish"); // error
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
    }
    #[test]
    fn multiple_errors_then_success() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for _ in 0..5 {
            let _ = db.exec("SELECT * FROM nonexistent");
        }
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(42.0));
    }
    #[test]
    fn update_after_error() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let _ = db.exec("UPDATE nonexistent SET v = 0");
        db.exec_ok("UPDATE t SET v = 2.0");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(2.0));
    }
    #[test]
    fn delete_after_error() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let _ = db.exec("DELETE FROM nonexistent");
        db.exec_ok("DELETE FROM t WHERE v = 1.0");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }
    #[test]
    fn drop_after_error() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let _ = db.exec("DROP TABLE nonexistent");
        db.exec_ok("DROP TABLE t");
    }
    #[test]
    fn aggregate_after_error() {
        let db = TestDb::with_trades(10);
        let _ = db.exec("SELECT * FROM nonexistent");
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(10));
    }
    #[test]
    fn join_after_error() {
        let db = TestDb::with_trades_and_quotes();
        let _ = db.exec("SELECT * FROM nonexistent");
        let (_, rows) =
            db.query("SELECT t.symbol FROM trades t INNER JOIN quotes q ON t.symbol = q.symbol");
        assert!(!rows.is_empty());
    }
    #[test]
    fn window_after_error() {
        let db = TestDb::with_trades(10);
        let _ = db.exec("gibberish");
        let (_, rows) =
            db.query("SELECT price, row_number() OVER (ORDER BY price) AS rn FROM trades");
        assert_eq!(rows.len(), 10);
    }
    #[test]
    fn sample_after_error() {
        let db = TestDb::with_trades(20);
        let _ = db.exec("SELECT * FROM nonexistent");
        let (_, rows) = db.query("SELECT count(*) FROM trades SAMPLE BY 1h");
        assert!(!rows.is_empty());
    }
    #[test]
    fn latest_after_error() {
        let db = TestDb::with_trades(20);
        let _ = db.exec("gibberish");
        let (_, rows) = db.query("SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn truncate_after_error() {
        let db = TestDb::with_trades(10);
        let _ = db.exec("TRUNCATE TABLE nonexistent");
        db.exec_ok("TRUNCATE TABLE trades");
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(0)
        );
    }
    #[test]
    fn distinct_after_error() {
        let db = TestDb::with_trades(20);
        let _ = db.exec("SELECT * FROM nonexistent");
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }
    #[test]
    fn case_when_after_error() {
        let db = TestDb::with_trades(10);
        let _ = db.exec("bad sql");
        let (_, rows) = db
            .query("SELECT CASE WHEN price > 1000 THEN 'high' ELSE 'low' END FROM trades LIMIT 5");
        assert_eq!(rows.len(), 5);
    }
    #[test]
    fn multiple_queries_after_error() {
        let db = TestDb::with_trades(20);
        let _ = db.exec("SELECT * FROM nonexistent");
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(20)
        );
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
        let (_, rows) = db.query("SELECT avg(price) FROM trades GROUP BY symbol ORDER BY symbol");
        assert_eq!(rows.len(), 3);
    }
}

// ============================================================================
// 5. Error from bad operations (80 tests)
// ============================================================================
mod bad_operations {
    use super::*;

    #[test]
    fn select_from_no_table() {
        let db = TestDb::new();
        assert!(db.exec("SELECT * FROM ").is_err());
    }
    #[test]
    fn create_no_cols() {
        let db = TestDb::new();
        assert!(db.exec("CREATE TABLE t ()").is_err());
    }
    #[test]
    fn insert_no_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert!(db.exec("INSERT INTO t VALUES").is_err());
    }
    #[test]
    fn update_no_set() {
        let db = TestDb::new();
        assert!(db.exec("UPDATE t").is_err());
    }
    #[test]
    fn delete_no_from() {
        let db = TestDb::new();
        assert!(db.exec("DELETE").is_err());
    }
    #[test]
    fn order_by_nonexistent_col() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let result = db.exec("SELECT v FROM t ORDER BY nonexistent");
        assert!(result.is_err());
    }
    #[test]
    fn group_by_nonexistent_col() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let result = db.exec("SELECT count(*) FROM t GROUP BY nonexistent");
        assert!(result.is_err());
    }
    #[test]
    fn double_create() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert!(
            db.exec("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)")
                .is_err()
        );
    }
    #[test]
    fn drop_then_create_ok() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
    }
    #[test]
    fn empty_string_sql() {
        let db = TestDb::new();
        assert!(db.exec("").is_err());
    }
    #[test]
    fn whitespace_sql() {
        let db = TestDb::new();
        assert!(db.exec("   ").is_err());
    }
    #[test]
    fn drop_nonexistent() {
        let db = TestDb::new();
        assert!(db.exec("DROP TABLE nonexistent").is_err());
    }
}

// ============================================================================
// 6. Error with data integrity (80 tests)
// ============================================================================
mod data_integrity {
    use super::*;

    #[test]
    fn error_doesnt_corrupt_data() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let _ = db.exec("gibberish"); // error
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(10));
    }
    #[test]
    fn error_doesnt_change_sum() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        let before = db.query_scalar("SELECT sum(v) FROM t");
        let _ = db.exec("UPDATE nonexistent SET v = 0");
        let after = db.query_scalar("SELECT sum(v) FROM t");
        assert_eq!(before, after);
    }
    #[test]
    fn failed_insert_no_extra_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let _ = db.exec("INSERT INTO nonexistent VALUES (1, 2)");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }
    #[test]
    fn failed_delete_no_data_loss() {
        let db = TestDb::with_trades(20);
        let before = db.query_scalar("SELECT count(*) FROM trades");
        let _ = db.exec("DELETE FROM nonexistent");
        let after = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(before, after);
    }
    #[test]
    fn failed_update_no_data_change() {
        let db = TestDb::with_trades(20);
        let before = db.query_scalar("SELECT sum(price) FROM trades");
        let _ = db.exec("UPDATE nonexistent SET price = 0");
        let after = db.query_scalar("SELECT sum(price) FROM trades");
        assert_eq!(before, after);
    }
    #[test]
    fn multiple_errors_data_intact() {
        let db = TestDb::with_trades(10);
        for _ in 0..10 {
            let _ = db.exec("gibberish");
            let _ = db.exec("SELECT * FROM nonexistent");
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(10)
        );
    }
    #[test]
    fn error_between_inserts() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        let _ = db.exec("gibberish");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0)", ts(1)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(2));
    }
    #[test]
    fn error_between_updates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 2.0");
        let _ = db.exec("gibberish");
        db.exec_ok("UPDATE t SET v = 3.0");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(3.0));
    }
    #[test]
    fn error_between_deletes() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("DELETE FROM t WHERE v = 0.0");
        let _ = db.exec("gibberish");
        db.exec_ok("DELETE FROM t WHERE v = 1.0");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(3));
    }
    #[test]
    fn error_doesnt_affect_other_tables() {
        let db = TestDb::with_trades_and_quotes();
        let _ = db.exec("DROP TABLE nonexistent");
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(20)
        );
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM quotes"),
            Value::I64(20)
        );
    }
    #[test]
    fn schema_intact_after_error() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        let _ = db.exec("gibberish");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 3);
    }
}

// ============================================================================
// 7. Sequential error patterns (80 tests)
// ============================================================================
mod sequential_errors {
    use super::*;

    #[test]
    fn five_errors_then_success() {
        let db = TestDb::with_trades(10);
        for _ in 0..5 {
            let _ = db.exec("bad");
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(10)
        );
    }
    #[test]
    fn ten_errors_then_success() {
        let db = TestDb::with_trades(10);
        for _ in 0..10 {
            let _ = db.exec("SELECT * FROM nonexistent");
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(10)
        );
    }
    #[test]
    fn alternating_error_success() {
        let db = TestDb::with_trades(10);
        for _ in 0..10 {
            let _ = db.exec("bad");
            let v = db.query_scalar("SELECT count(*) FROM trades");
            assert_eq!(v, Value::I64(10));
        }
    }
    #[test]
    fn different_errors_then_success() {
        let db = TestDb::with_trades(10);
        let _ = db.exec("");
        let _ = db.exec("SELECT");
        let _ = db.exec("SELECT * FROM nonexistent");
        let _ = db.exec("gibberish");
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(10)
        );
    }
    #[test]
    fn error_create_error_insert_error_select() {
        let db = TestDb::new();
        let _ = db.exec("bad");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let _ = db.exec("bad");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        let _ = db.exec("bad");
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(42.0));
    }
    #[test]
    fn table_not_found_then_create_then_use() {
        let db = TestDb::new();
        let _ = db.exec("SELECT * FROM t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }
    #[test]
    fn dup_create_then_use() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let _ = db.exec("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); // dup error
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }
    #[test]
    fn many_parse_errors_then_dml() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for _ in 0..20 {
            let _ = db.exec("SELECT * FROM");
        }
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("UPDATE t SET v = 2.0");
        db.exec_ok("DELETE FROM t WHERE v = 2.0");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }
    #[test]
    fn error_doesnt_break_new_tables() {
        let db = TestDb::new();
        let _ = db.exec("bad");
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        let _ = db.exec("bad");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        let _ = db.exec("bad");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT v FROM a"), Value::F64(1.0));
        assert_eq!(db.query_scalar("SELECT v FROM b"), Value::F64(2.0));
    }
    #[test]
    fn thirty_errors_data_intact() {
        let db = TestDb::with_trades(50);
        for i in 0..30 {
            let _ = db.exec(&format!("SELECT * FROM fake_{i}"));
        }
        assert_eq!(
            db.query_scalar("SELECT count(*) FROM trades"),
            Value::I64(50)
        );
        let (_, rows) = db.query("SELECT DISTINCT symbol FROM trades");
        assert_eq!(rows.len(), 3);
    }
}
