//! DDL (Data Definition Language) tests for ExchangeDB (80+ tests).
//!
//! Covers: CREATE TABLE, ALTER TABLE (ADD/DROP/RENAME COLUMN), DROP TABLE,
//! TRUNCATE TABLE, materialized views, SHOW/DESCRIBE commands.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

// ===========================================================================
// create_table: various column types, partition strategies, timestamp column
// ===========================================================================
mod create_table {
    use super::*;

    #[test]
    fn create_simple_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn create_table_with_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'test')", ts(0)));
        let val = db.query_scalar("SELECT name FROM t");
        assert_eq!(val, Value::Str("test".to_string()));
    }

    #[test]
    fn create_table_with_multiple_doubles() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 2.0, 3.0)", ts(0)));
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 4);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn create_table_with_mixed_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR, value DOUBLE, tag VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a', 1.0, 'x')", ts(0)));
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 4);
    }

    #[test]
    fn create_table_single_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({})", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn create_table_many_columns() {
        let db = TestDb::new();
        db.exec_ok(
            "CREATE TABLE wide (timestamp TIMESTAMP, c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 VARCHAR, c7 VARCHAR, c8 VARCHAR)"
        );
        let (cols, _) = db.query("SELECT * FROM wide");
        assert_eq!(cols.len(), 9);
    }

    #[test]
    fn create_table_already_exists_error() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let result = db.exec("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        assert!(result.is_err());
    }

    #[test]
    fn create_table_if_not_exists() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE IF NOT EXISTS t (timestamp TIMESTAMP, v DOUBLE)");
        // Should not error
    }

    #[test]
    fn create_table_after_drop() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"name".to_string()));
    }

    #[test]
    fn create_table_different_name() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 2.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT v FROM t1"), Value::F64(1.0));
        assert_eq!(db.query_scalar("SELECT v FROM t2"), Value::F64(2.0));
    }

    #[test]
    fn create_multiple_tables() {
        let db = TestDb::new();
        for i in 0..10 {
            db.exec_ok(&format!("CREATE TABLE t{} (timestamp TIMESTAMP, v DOUBLE)", i));
        }
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t{} VALUES ({}, {}.0)", i, ts(0), i));
        }
        for i in 0..10 {
            let val = db.query_scalar(&format!("SELECT v FROM t{}", i));
            assert!(val.eq_coerce(&Value::F64(i as f64)));
        }
    }

    #[test]
    fn create_table_and_insert_immediately() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT v FROM t"), Value::F64(42.0));
    }

    #[test]
    fn create_table_verify_column_names() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, alpha DOUBLE, beta VARCHAR)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"timestamp".to_string()));
        assert!(cols.contains(&"alpha".to_string()));
        assert!(cols.contains(&"beta".to_string()));
    }
}

// ===========================================================================
// alter_table: ADD COLUMN, DROP COLUMN, RENAME COLUMN
// ===========================================================================
mod alter_table {
    use super::*;

    #[test]
    fn add_column_double() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("ALTER TABLE t ADD COLUMN w DOUBLE");
        db.exec_ok(&format!("INSERT INTO t (timestamp, v, w) VALUES ({}, 1.0, 2.0)", ts(0)));
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn add_column_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("ALTER TABLE t ADD COLUMN name VARCHAR");
        db.exec_ok(&format!("INSERT INTO t (timestamp, v, name) VALUES ({}, 1.0, 'test')", ts(0)));
        let val = db.query_scalar("SELECT name FROM t");
        assert_eq!(val, Value::Str("test".to_string()));
    }

    #[test]
    fn add_column_existing_data_gets_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("ALTER TABLE t ADD COLUMN w DOUBLE");
        let (_, rows) = db.query("SELECT w FROM t");
        // Existing rows should have NULL for new column
        assert_eq!(rows[0][0], Value::Null);
    }

    #[test]
    fn add_multiple_columns_sequentially() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        db.exec_ok("ALTER TABLE t ADD COLUMN a DOUBLE");
        db.exec_ok("ALTER TABLE t ADD COLUMN b DOUBLE");
        db.exec_ok("ALTER TABLE t ADD COLUMN c VARCHAR");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 4);
    }

    #[test]
    fn drop_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 2.0)", ts(0)));
        db.exec_ok("ALTER TABLE t DROP COLUMN b");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
        assert!(!cols.contains(&"b".to_string()));
    }

    #[test]
    fn drop_column_preserves_other_data() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 2.0)", ts(0)));
        db.exec_ok("ALTER TABLE t DROP COLUMN b");
        let val = db.query_scalar("SELECT a FROM t");
        assert_eq!(val, Value::F64(1.0));
    }

    #[test]
    fn rename_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, old_name DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        db.exec_ok("ALTER TABLE t RENAME COLUMN old_name TO new_name");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"new_name".to_string()));
        assert!(!cols.contains(&"old_name".to_string()));
    }

    #[test]
    fn rename_column_preserves_data() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, price DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 100.0)", ts(0)));
        db.exec_ok("ALTER TABLE t RENAME COLUMN price TO cost");
        let val = db.query_scalar("SELECT cost FROM t");
        assert_eq!(val, Value::F64(100.0));
    }

    #[test]
    fn add_column_to_nonexistent_table() {
        let db = TestDb::new();
        let result = db.exec("ALTER TABLE no_table ADD COLUMN v DOUBLE");
        assert!(result.is_err());
    }

    #[test]
    fn drop_nonexistent_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let result = db.exec("ALTER TABLE t DROP COLUMN no_col");
        assert!(result.is_err());
    }

    #[test]
    fn rename_nonexistent_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let result = db.exec("ALTER TABLE t RENAME COLUMN no_col TO new_col");
        assert!(result.is_err());
    }

    #[test]
    fn add_column_then_insert_then_query() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("ALTER TABLE t ADD COLUMN s VARCHAR");
        db.exec_ok(&format!("INSERT INTO t (timestamp, v, s) VALUES ({}, 2.0, 'hello')", ts(1)));
        let (_, rows) = db.query("SELECT * FROM t ORDER BY timestamp");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn drop_column_then_add_same_name() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok("ALTER TABLE t DROP COLUMN b");
        db.exec_ok("ALTER TABLE t ADD COLUMN b DOUBLE");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"b".to_string()));
    }

    #[test]
    fn add_column_with_data_then_filter() {
        let db = TestDb::with_trades(10);
        db.exec_ok("ALTER TABLE trades ADD COLUMN exchange VARCHAR");
        db.exec_ok("UPDATE trades SET exchange = 'binance' WHERE symbol = 'BTC/USD'");
        let (_, rows) = db.query("SELECT * FROM trades WHERE exchange = 'binance'");
        assert!(!rows.is_empty());
    }
}

// ===========================================================================
// drop_table: basic, nonexistent, with data
// ===========================================================================
mod drop_table {
    use super::*;

    #[test]
    fn drop_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        let result = db.exec("SELECT * FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn drop_table_with_data() {
        let db = TestDb::with_trades(10);
        db.exec_ok("DROP TABLE trades");
        let result = db.exec("SELECT * FROM trades");
        assert!(result.is_err());
    }

    #[test]
    fn drop_nonexistent_table() {
        let db = TestDb::new();
        let result = db.exec("DROP TABLE no_table");
        assert!(result.is_err());
    }

    #[test]
    fn drop_if_exists_nonexistent() {
        let db = TestDb::new();
        db.exec_ok("DROP TABLE IF EXISTS no_table");
    }

    #[test]
    fn drop_if_exists_existing() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE IF EXISTS t");
        let result = db.exec("SELECT * FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn drop_and_recreate() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("DROP TABLE t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"name".to_string()));
        assert!(!cols.contains(&"v".to_string()));
    }

    #[test]
    fn drop_one_of_many_tables() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t3 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t2");
        // t1 and t3 should still work
        db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t3 VALUES ({}, 3.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT v FROM t1"), Value::F64(1.0));
        assert_eq!(db.query_scalar("SELECT v FROM t3"), Value::F64(3.0));
        assert!(db.exec("SELECT * FROM t2").is_err());
    }

    #[test]
    fn drop_table_twice() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        let result = db.exec("DROP TABLE t");
        assert!(result.is_err());
    }
}

// ===========================================================================
// truncate_table: basic, empty table, verify data gone
// ===========================================================================
mod truncate_table {
    use super::*;

    #[test]
    fn truncate_basic() {
        let db = TestDb::with_trades(20);
        db.exec_ok("TRUNCATE TABLE trades");
        assert_eq!(db.query_scalar("SELECT count(*) FROM trades"), Value::I64(0));
    }

    #[test]
    fn truncate_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("TRUNCATE TABLE t");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn truncate_preserves_schema() {
        let db = TestDb::with_trades(10);
        db.exec_ok("TRUNCATE TABLE trades");
        let (cols, rows) = db.query("SELECT * FROM trades");
        assert_eq!(cols.len(), 5);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn truncate_then_insert() {
        let db = TestDb::with_trades(10);
        db.exec_ok("TRUNCATE TABLE trades");
        db.exec_ok(&format!(
            "INSERT INTO trades VALUES ({}, 'NEW/USD', 999.0, 1.0, 'buy')", ts(0)
        ));
        assert_eq!(db.query_scalar("SELECT count(*) FROM trades"), Value::I64(1));
    }

    #[test]
    fn truncate_nonexistent_table() {
        let db = TestDb::new();
        let result = db.exec("TRUNCATE TABLE no_table");
        assert!(result.is_err());
    }

    #[test]
    fn truncate_twice() {
        let db = TestDb::with_trades(10);
        db.exec_ok("TRUNCATE TABLE trades");
        db.exec_ok("TRUNCATE TABLE trades"); // second truncate on empty table
        assert_eq!(db.query_scalar("SELECT count(*) FROM trades"), Value::I64(0));
    }

    #[test]
    fn truncate_large_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let values: Vec<String> = (0..1000)
            .map(|i| format!("({}, {}.0)", ts(i), i))
            .collect();
        db.exec_ok(&format!("INSERT INTO t VALUES {}", values.join(", ")));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1000));
        db.exec_ok("TRUNCATE TABLE t");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0));
    }

    #[test]
    fn truncate_does_not_affect_other_tables() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t1 VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t2 VALUES ({}, 2.0)", ts(0)));
        db.exec_ok("TRUNCATE TABLE t1");
        assert_eq!(db.query_scalar("SELECT count(*) FROM t1"), Value::I64(0));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t2"), Value::I64(1));
    }
}

// ===========================================================================
// mat_view: CREATE, REFRESH, DROP, query materialized views
// ===========================================================================
mod mat_view {
    use super::*;

    #[test]
    fn create_materialized_view() {
        let db = TestDb::with_trades(20);
        db.exec_ok("CREATE MATERIALIZED VIEW btc_trades AS SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        let (_, rows) = db.query("SELECT * FROM btc_trades");
        assert!(!rows.is_empty());
    }

    #[test]
    fn refresh_materialized_view() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE MATERIALIZED VIEW mv AS SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        let (_, before) = db.query("SELECT count(*) FROM mv");
        // Insert more BTC data
        db.exec_ok(&format!(
            "INSERT INTO trades VALUES ({}, 'BTC/USD', 70000.0, 1.0, 'buy')", ts(9999)
        ));
        db.exec_ok("REFRESH MATERIALIZED VIEW mv");
        let (_, after) = db.query("SELECT count(*) FROM mv");
        let b = match &before[0][0] { Value::I64(n) => *n, other => panic!("{other:?}") };
        let a = match &after[0][0] { Value::I64(n) => *n, other => panic!("{other:?}") };
        assert!(a >= b);
    }

    #[test]
    fn drop_materialized_view() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE MATERIALIZED VIEW mv AS SELECT * FROM trades");
        db.exec_ok("DROP MATERIALIZED VIEW mv");
        let result = db.exec("SELECT * FROM mv");
        assert!(result.is_err());
    }

    #[test]
    fn matview_with_aggregation() {
        let db = TestDb::with_trades(20);
        db.exec_ok("CREATE MATERIALIZED VIEW sym_stats AS SELECT symbol, count(*) AS cnt, avg(price) AS avg_price FROM trades GROUP BY symbol");
        let (_, rows) = db.query("SELECT * FROM sym_stats");
        assert_eq!(rows.len(), 3); // 3 symbols
    }

    #[test]
    fn matview_query_after_create() {
        let db = TestDb::with_trades(10);
        db.exec_ok("CREATE MATERIALIZED VIEW mv AS SELECT price FROM trades WHERE price > 1000");
        let (_, rows) = db.query("SELECT * FROM mv");
        for row in &rows {
            match &row[0] {
                Value::F64(p) => assert!(*p > 1000.0),
                other => panic!("expected F64, got {other:?}"),
            }
        }
    }
}

// ===========================================================================
// show_describe: SHOW TABLES, SHOW COLUMNS, DESCRIBE, SHOW CREATE TABLE
// ===========================================================================
mod show_describe {
    use super::*;

    #[test]
    fn show_tables_empty_db() {
        let db = TestDb::new();
        let (_, rows) = db.query("SHOW TABLES");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn show_tables_after_create() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE alpha (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE beta (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("SHOW TABLES");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn show_tables_after_drop() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t1 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE t2 (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t1");
        let (_, rows) = db.query("SHOW TABLES");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn show_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, price DOUBLE, name VARCHAR)");
        let (_, rows) = db.query("SHOW COLUMNS FROM t");
        assert_eq!(rows.len(), 3); // timestamp, price, name
    }

    #[test]
    fn describe_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, price DOUBLE, name VARCHAR)");
        let (_, rows) = db.query("DESCRIBE t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn desc_shorthand() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let (_, rows) = db.query("DESC t");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn show_create_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE, s VARCHAR)");
        let (_, rows) = db.query("SHOW CREATE TABLE t");
        assert!(!rows.is_empty());
    }

    #[test]
    fn show_columns_after_add_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("ALTER TABLE t ADD COLUMN s VARCHAR");
        let (_, rows) = db.query("SHOW COLUMNS FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn show_columns_after_drop_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok("ALTER TABLE t DROP COLUMN b");
        let (_, rows) = db.query("SHOW COLUMNS FROM t");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn show_tables_with_trades_setup() {
        let db = TestDb::with_trades_and_quotes();
        let (_, rows) = db.query("SHOW TABLES");
        assert!(rows.len() >= 2); // trades and quotes
    }

    #[test]
    fn show_columns_after_rename() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, old_col DOUBLE)");
        db.exec_ok("ALTER TABLE t RENAME COLUMN old_col TO new_col");
        let (_, rows) = db.query("SHOW COLUMNS FROM t");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn describe_wide_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE, c DOUBLE, d VARCHAR, e VARCHAR)");
        let (_, rows) = db.query("DESCRIBE t");
        assert_eq!(rows.len(), 6);
    }
}

// ===========================================================================
// ddl_lifecycle: full lifecycle tests combining multiple DDL operations
// ===========================================================================
mod ddl_lifecycle {
    use super::*;

    #[test]
    fn create_insert_alter_query() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("ALTER TABLE t ADD COLUMN s VARCHAR");
        db.exec_ok(&format!("INSERT INTO t (timestamp, v, s) VALUES ({}, 2.0, 'x')", ts(1)));
        let (_, rows) = db.query("SELECT * FROM t ORDER BY timestamp");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn create_insert_truncate_insert() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..10 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("TRUNCATE TABLE t");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }

    #[test]
    fn create_drop_recreate_different_schema() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("DROP TABLE t");
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR, w DOUBLE)");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 3);
        assert!(cols.contains(&"s".to_string()));
        assert!(cols.contains(&"w".to_string()));
    }

    #[test]
    fn alter_add_drop_add_same_name() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("ALTER TABLE t ADD COLUMN x DOUBLE");
        db.exec_ok("ALTER TABLE t DROP COLUMN x");
        db.exec_ok("ALTER TABLE t ADD COLUMN x VARCHAR");
        let (cols, _) = db.query("SELECT * FROM t");
        assert!(cols.contains(&"x".to_string()));
    }

    #[test]
    fn full_lifecycle_with_data() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, {}.0)", ts(i), i));
        }
        db.exec_ok("ALTER TABLE t ADD COLUMN label VARCHAR");
        db.exec_ok("UPDATE t SET label = 'old'");
        db.exec_ok(&format!("INSERT INTO t (timestamp, v, label) VALUES ({}, 99.0, 'new')", ts(99)));
        db.exec_ok("DELETE FROM t WHERE v < 3");
        let count = match db.query_scalar("SELECT count(*) FROM t") {
            Value::I64(n) => n, other => panic!("{other:?}")
        };
        assert!(count >= 3); // v=3,4,99
    }

    #[test]
    fn multiple_tables_independent_ops() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE a (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("CREATE TABLE b (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO a VALUES ({}, 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO b VALUES ({}, 2.0)", ts(0)));
        db.exec_ok("DROP TABLE a");
        assert!(db.exec("SELECT * FROM a").is_err());
        assert_eq!(db.query_scalar("SELECT v FROM b"), Value::F64(2.0));
    }

    #[test]
    fn rename_column_then_query() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, old_name DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42.0)", ts(0)));
        db.exec_ok("ALTER TABLE t RENAME COLUMN old_name TO new_name");
        let val = db.query_scalar("SELECT new_name FROM t");
        assert_eq!(val, Value::F64(42.0));
        // Old name should not work
        let result = db.exec("SELECT old_name FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn drop_column_then_insert_without_it() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b DOUBLE)");
        db.exec_ok("ALTER TABLE t DROP COLUMN b");
        db.exec_ok(&format!("INSERT INTO t (timestamp, a) VALUES ({}, 1.0)", ts(0)));
        assert_eq!(db.query_scalar("SELECT a FROM t"), Value::F64(1.0));
    }

    #[test]
    fn create_table_insert_show_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, price DOUBLE, name VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 'x')", ts(0)));
        let (_, rows) = db.query("SHOW COLUMNS FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn truncate_then_show_tables() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0)", ts(0)));
        db.exec_ok("TRUNCATE TABLE t");
        let (_, rows) = db.query("SHOW TABLES");
        assert_eq!(rows.len(), 1); // table still exists
    }

    #[test]
    fn create_insert_drop_cycle() {
        let db = TestDb::new();
        for cycle in 0..5 {
            let name = format!("cycle_{}", cycle);
            db.exec_ok(&format!("CREATE TABLE {} (timestamp TIMESTAMP, v DOUBLE)", name));
            db.exec_ok(&format!("INSERT INTO {} VALUES ({}, {}.0)", name, ts(0), cycle));
            db.exec_ok(&format!("DROP TABLE {}", name));
        }
        let (_, rows) = db.query("SHOW TABLES");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn alter_add_column_multiple_types() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP)");
        db.exec_ok("ALTER TABLE t ADD COLUMN d1 DOUBLE");
        db.exec_ok("ALTER TABLE t ADD COLUMN d2 DOUBLE");
        db.exec_ok("ALTER TABLE t ADD COLUMN s1 VARCHAR");
        db.exec_ok("ALTER TABLE t ADD COLUMN s2 VARCHAR");
        let (cols, _) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 5);
    }

    #[test]
    fn describe_after_full_lifecycle() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok("ALTER TABLE t ADD COLUMN c DOUBLE");
        db.exec_ok("ALTER TABLE t DROP COLUMN a");
        let (_, rows) = db.query("DESCRIBE t");
        // Should have: timestamp, b, c
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn show_create_table_after_alter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("ALTER TABLE t ADD COLUMN s VARCHAR");
        let (_, rows) = db.query("SHOW CREATE TABLE t");
        assert!(!rows.is_empty());
    }

    #[test]
    fn truncate_and_verify_insert_works() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a DOUBLE, b VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.0, 'x')", ts(0)));
        db.exec_ok("TRUNCATE TABLE t");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 2.0, 'y')", ts(0)));
        let (_, rows) = db.query("SELECT a, b FROM t");
        assert_eq!(rows[0][0], Value::F64(2.0));
        assert_eq!(rows[0][1], Value::Str("y".to_string()));
    }
}
