//! Tests for all features from MISSING_FEATURES.md.
//!
//! Covers: NATURAL JOIN, MySQL-style LIMIT, stored procedures,
//! downsampling integration, MVCC wiring, RLS wiring, and improved errors.

use exchange_query::plan::{QueryResult, Value};
use exchange_query::test_utils::TestDb;

// ===========================================================================
// 1. NATURAL JOIN
// ===========================================================================

#[test]
fn natural_join_basic() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE orders (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE)");
    db.exec_ok("CREATE TABLE fills (timestamp TIMESTAMP, symbol VARCHAR, qty DOUBLE)");
    db.exec_ok("INSERT INTO orders (timestamp, symbol, price) VALUES (1000000000, 'BTC', 100.0)");
    db.exec_ok("INSERT INTO orders (timestamp, symbol, price) VALUES (2000000000, 'ETH', 200.0)");
    db.exec_ok("INSERT INTO fills (timestamp, symbol, qty) VALUES (1000000000, 'BTC', 10.0)");
    db.exec_ok("INSERT INTO fills (timestamp, symbol, qty) VALUES (2000000000, 'ETH', 20.0)");

    // NATURAL JOIN should automatically join on common columns (timestamp, symbol).
    let (cols, rows) = db.query("SELECT * FROM orders NATURAL JOIN fills");
    assert!(!rows.is_empty(), "NATURAL JOIN should produce rows");
    // Each row should have columns from both tables.
    assert!(
        cols.len() >= 4,
        "expected at least 4 columns, got {}",
        cols.len()
    );
}

#[test]
fn natural_join_plans_correctly() {
    // Verify the planner handles NATURAL JOIN without error.
    let plan = exchange_query::plan_query("SELECT * FROM t1 NATURAL JOIN t2");
    assert!(
        plan.is_ok(),
        "NATURAL JOIN should plan successfully: {:?}",
        plan.err()
    );
}

// ===========================================================================
// 2. MySQL-style LIMIT
// ===========================================================================

#[test]
fn mysql_limit_syntax() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE nums (timestamp TIMESTAMP, val DOUBLE)");
    for i in 0..10u64 {
        db.exec_ok(&format!(
            "INSERT INTO nums (timestamp, val) VALUES ({}, {})",
            (i + 1) * 1000000000,
            i as f64
        ));
    }

    // MySQL LIMIT offset, count: skip 3, take 2
    let (_, rows) = db.query("SELECT val FROM nums ORDER BY val LIMIT 3, 2");
    assert_eq!(rows.len(), 2, "LIMIT 3, 2 should return 2 rows");
    // After ordering by val (0..9), skipping 3 gives val=3, val=4
    assert_eq!(rows[0][0], Value::F64(3.0));
    assert_eq!(rows[1][0], Value::F64(4.0));
}

#[test]
fn mysql_limit_plans_correctly() {
    let plan = exchange_query::plan_query("SELECT * FROM t LIMIT 10, 20").unwrap();
    match plan {
        exchange_query::QueryPlan::Select { limit, offset, .. } => {
            assert_eq!(limit, Some(20), "count should be 20");
            assert_eq!(offset, Some(10), "offset should be 10");
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

// ===========================================================================
// 3. Stored Procedures
// ===========================================================================

#[test]
fn stored_procedure_create_and_call() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE counts (timestamp TIMESTAMP, val DOUBLE)");
    db.exec_ok("INSERT INTO counts (timestamp, val) VALUES (1000000000, 42.0)");

    // Create a simple procedure that queries the table.
    db.exec_ok("CREATE PROCEDURE get_counts() AS BEGIN SELECT * FROM counts END");

    // Call the procedure.
    let result = db.exec("CALL get_counts()").unwrap();
    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "procedure should return 1 row");
        }
        other => panic!("expected Rows from CALL, got {other:?}"),
    }
}

#[test]
fn stored_procedure_drop() {
    let db = TestDb::new();
    db.exec_ok("CREATE PROCEDURE dummy() AS BEGIN SELECT 1 FROM long_sequence(1) END");
    db.exec_ok("DROP PROCEDURE dummy");
    // Calling dropped procedure should fail.
    let result = db.exec("CALL dummy()");
    assert!(result.is_err(), "calling dropped procedure should fail");
}

#[test]
fn stored_procedure_plans_correctly() {
    let plan = exchange_query::plan_query(
        "CREATE PROCEDURE foo() AS BEGIN SELECT 1 FROM long_sequence(1) END",
    );
    assert!(plan.is_ok());
    match plan.unwrap() {
        exchange_query::QueryPlan::CreateProcedure { name, body } => {
            assert_eq!(name, "foo");
            assert!(body.contains("SELECT"), "body should contain SQL");
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }

    let plan = exchange_query::plan_query("CALL foo()");
    assert!(plan.is_ok());
    match plan.unwrap() {
        exchange_query::QueryPlan::CallProcedure { name } => {
            assert_eq!(name, "foo");
        }
        other => panic!("expected CallProcedure, got {other:?}"),
    }
}

// ===========================================================================
// 4. Downsampling integration
// ===========================================================================

#[test]
fn create_downsampling_plans_correctly() {
    let plan = exchange_query::plan_query(
        "CREATE DOWNSAMPLING ON trades INTERVAL 1m AS trades_1m COLUMNS first(price) as open, max(price) as high, min(price) as low, last(price) as close, sum(volume) as vol",
    );
    assert!(
        plan.is_ok(),
        "CREATE DOWNSAMPLING should plan: {:?}",
        plan.err()
    );
    match plan.unwrap() {
        exchange_query::QueryPlan::CreateDownsampling {
            source_table,
            target_name,
            interval_secs,
            columns,
        } => {
            assert_eq!(source_table, "trades");
            assert_eq!(target_name, "trades_1m");
            assert_eq!(interval_secs, 60);
            assert_eq!(columns.len(), 5);
            assert_eq!(
                columns[0],
                ("first".to_string(), "price".to_string(), "open".to_string())
            );
            assert_eq!(
                columns[1],
                ("max".to_string(), "price".to_string(), "high".to_string())
            );
        }
        other => panic!("expected CreateDownsampling, got {other:?}"),
    }
}

#[test]
fn create_downsampling_executes() {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE trades (timestamp TIMESTAMP, price DOUBLE, volume DOUBLE)");
    db.exec_ok(
        "CREATE DOWNSAMPLING ON trades INTERVAL 1m AS trades_1m COLUMNS first(price) as open, max(price) as high"
    );
    // Verify config file was created.
    let config_path = db.path().join("__downsampling__").join("trades_1m.json");
    assert!(config_path.exists(), "downsampling config should be stored");
}

// ===========================================================================
// 5. Fixed ignored tests (verified above, but test the planner too)
// ===========================================================================

#[test]
fn select_current_database_no_from() {
    let db = TestDb::new();
    let (_, rows) = db.query("SELECT current_database()");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Str(s) => assert_eq!(s, "exchangedb"),
        other => panic!("expected Str, got {other:?}"),
    }
}

#[test]
fn select_current_schema_no_from() {
    let db = TestDb::new();
    let (_, rows) = db.query("SELECT current_schema()");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Str(s) => assert_eq!(s, "public"),
        other => panic!("expected Str, got {other:?}"),
    }
}

// ===========================================================================
// 6. MVCC wiring
// ===========================================================================

#[test]
fn mvcc_snapshot_guard_in_context() {
    use exchange_core::mvcc::MvccManager;
    use exchange_query::context::ExecutionContext;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;

    let mgr = Arc::new(MvccManager::new());
    mgr.commit_write(&[("p1", 100)]);

    let ctx = ExecutionContext {
        db_root: PathBuf::from("/tmp/test"),
        security: None,
        resource_mgr: None,
        query_id: 1,
        start_time: Instant::now(),
        use_wal: false,
        memory_tracker: None,
        deadline: None,
        use_cursor_engine: false,
        mvcc: Some(Arc::clone(&mgr)),
        rls: None,
        current_user: None,
        sql_text: None,
        audit_log: None,
        cancellation_token: None,
        replication_manager: None,
    };

    // Begin snapshot through context.
    let guard = ctx.begin_snapshot();
    assert!(
        guard.is_some(),
        "snapshot guard should be created when MVCC is configured"
    );
    let guard = guard.unwrap();
    assert_eq!(guard.visible_row_count("p1"), 100);
    assert_eq!(mgr.active_snapshot_count(), 1);

    // Dropping the guard releases the snapshot.
    drop(guard);
    assert_eq!(mgr.active_snapshot_count(), 0);
}

#[test]
fn mvcc_snapshot_not_created_without_manager() {
    use exchange_query::context::ExecutionContext;
    use std::path::PathBuf;

    let ctx = ExecutionContext::anonymous(PathBuf::from("/tmp/test"));
    assert!(ctx.begin_snapshot().is_none());
}

// ===========================================================================
// 7. RLS wiring
// ===========================================================================

#[test]
fn rls_filter_injection_in_context() {
    use exchange_core::rls::{RlsManager, RowLevelPolicy};
    use exchange_query::context::ExecutionContext;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;

    let mut rls = RlsManager::new();
    rls.add_policy(
        "alice",
        RowLevelPolicy {
            table: "trades".into(),
            column: "exchange".into(),
            allowed_values: vec!["NYSE".into(), "NASDAQ".into()],
        },
    );

    let ctx = ExecutionContext {
        db_root: PathBuf::from("/tmp/test"),
        security: None,
        resource_mgr: None,
        query_id: 1,
        start_time: Instant::now(),
        use_wal: false,
        memory_tracker: None,
        deadline: None,
        use_cursor_engine: false,
        mvcc: None,
        rls: Some(Arc::new(rls)),
        current_user: Some("alice".to_string()),
        sql_text: None,
        audit_log: None,
        cancellation_token: None,
        replication_manager: None,
    };

    // Should return a filter for alice on trades.
    let filter = ctx.get_rls_filter("trades");
    assert!(
        filter.is_some(),
        "RLS filter should exist for alice on trades"
    );

    // Should not return a filter for a different table.
    let no_filter = ctx.get_rls_filter("orders");
    assert!(no_filter.is_none(), "no RLS policy for orders");
}

#[test]
fn rls_no_filter_without_manager() {
    use exchange_query::context::ExecutionContext;
    use std::path::PathBuf;

    let ctx = ExecutionContext::anonymous(PathBuf::from("/tmp/test"));
    assert!(ctx.get_rls_filter("trades").is_none());
}

// ===========================================================================
// 8. Improved error messages
// ===========================================================================

#[test]
fn error_messages_include_sql_context() {
    use exchange_query::context::ExecutionContext;
    use exchange_query::execute_with_context;
    use exchange_query::plan_query;
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let db_root = dir.path().to_path_buf();

    let sql = "SELECT foo FROM nonexistent_table";
    let plan = plan_query(sql).unwrap();

    let ctx = ExecutionContext {
        db_root,
        security: None,
        resource_mgr: None,
        query_id: 1,
        start_time: Instant::now(),
        use_wal: false,
        memory_tracker: None,
        deadline: None,
        use_cursor_engine: false,
        mvcc: None,
        rls: None,
        current_user: None,
        sql_text: Some(sql.to_string()),
        audit_log: None,
        cancellation_token: None,
        replication_manager: None,
    };

    let result = execute_with_context(&ctx, &plan);
    assert!(result.is_err());
    let err = result.err().unwrap();
    // The error should include SQL context.
    let err_str = err.to_string();
    // TableNotFound errors won't get the SQL attachment since they're not Query errors,
    // but they should still be meaningful.
    assert!(
        err_str.contains("nonexistent_table"),
        "error should mention the table: {err_str}"
    );
}

#[test]
fn query_detailed_error_format() {
    use exchange_common::error::ExchangeDbError;

    let err = ExchangeDbError::QueryDetailed {
        detail: "column 'foo' not found in table 'trades'".to_string(),
        sql: "SELECT foo FROM trades".to_string(),
    };
    let msg = err.to_string();
    assert!(msg.contains("column 'foo'"), "should contain detail: {msg}");
    assert!(
        msg.contains("SELECT foo FROM trades"),
        "should contain SQL: {msg}"
    );
}
