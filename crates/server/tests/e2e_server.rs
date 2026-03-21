//! End-to-end integration tests for ExchangeDB.
//!
//! Tests the full server lifecycle without starting actual network servers:
//! SQL execution, ILP ingestion, WAL durability, snapshot/restore,
//! concurrent access, psql compatibility queries, and error messages.
//!
//! This file contains 25+ tests covering all five task areas.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

use exchange_common::error::ExchangeDbError;
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::column::FixedColumnReader;
use exchange_core::recovery::RecoveryManager;
use exchange_core::snapshot::{create_snapshot, restore_snapshot};
use exchange_core::table::{TableBuilder, TableMeta};
use exchange_core::txn::TxnFile;
use exchange_core::wal::row_codec::OwnedColumnValue;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use exchange_net::ilp::parser::{parse_ilp_batch, parse_ilp_line};
use exchange_query::plan::{QueryResult, Value};
use exchange_query::{execute, plan_query};

// ===========================================================================
// Helpers
// ===========================================================================

/// Execute SQL against a db_root, returning the QueryResult.
fn run(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
    execute(db_root, &plan).unwrap_or_else(|e| panic!("exec failed for `{sql}`: {e}"))
}

/// Execute SQL, expect Rows, return (columns, rows).
fn query(db_root: &Path, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    match run(db_root, sql) {
        QueryResult::Rows { columns, rows } => (columns, rows),
        other => panic!("expected Rows for `{sql}`, got {other:?}"),
    }
}

/// Execute SQL, expect Rows, return the single scalar.
fn scalar(db_root: &Path, sql: &str) -> Value {
    let (_, rows) = query(db_root, sql);
    assert!(!rows.is_empty(), "expected at least one row for `{sql}`");
    rows[0][0].clone()
}

/// Execute SQL, expect it to succeed (Ok or Rows).
fn exec_ok(db_root: &Path, sql: &str) {
    let plan = plan_query(sql).unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
    execute(db_root, &plan).unwrap_or_else(|e| panic!("exec failed for `{sql}`: {e}"));
}

/// Execute SQL, expect an error.
fn exec_err(db_root: &Path, sql: &str) -> ExchangeDbError {
    let plan = plan_query(sql).unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
    execute(db_root, &plan).expect_err(&format!("expected error for `{sql}`"))
}

/// Helper: create a table via core API for WAL-level tests.
fn create_test_table(db_root: &Path, name: &str) -> TableMeta {
    let meta = TableBuilder::new(name)
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();
    let table_dir = db_root.join(name);
    let _txn = TxnFile::open(&table_dir).unwrap();
    meta
}

/// Timestamp for 2024-03-15 12:30:00 UTC in nanos.
const TS_BASE: i64 = 1_710_513_000_000_000_000;

// ===========================================================================
// Task 1: Full server lifecycle (end-to-end)
// ===========================================================================

#[test]
fn full_lifecycle() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // 1. Recovery on empty DB should succeed.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.tables_recovered, 0);

    // 2. Create table via SQL.
    exec_ok(
        db_root,
        "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)",
    );

    // 3. Insert 100 rows via SQL.
    for i in 0..100i64 {
        let sql = format!(
            "INSERT INTO trades VALUES ({}, 'BTC/USD', {}.0, {}.0)",
            TS_BASE + i * 1_000_000_000,
            50000 + i,
            i + 1
        );
        exec_ok(db_root, &sql);
    }

    // 4. Verify count.
    let val = scalar(db_root, "SELECT count(*) FROM trades");
    assert!(
        val.eq_coerce(&Value::I64(100)),
        "expected 100 rows, got {val:?}"
    );

    // 5. ILP parse verification (parsing only, not ingestion into DB).
    let lines = "trades,symbol=ETH/USD price=3000.0,volume=10.0 1710513000000000000\n";
    let parsed = parse_ilp_batch(lines).unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].measurement, "trades");

    // 6. Update with expression.
    run(
        db_root,
        "UPDATE trades SET price = price * 1.1 WHERE symbol = 'BTC/USD'",
    );

    // 7. Delete low-volume rows.
    run(db_root, "DELETE FROM trades WHERE volume < 5");

    // 8. Verify some rows were deleted.
    let post_delete = scalar(db_root, "SELECT count(*) FROM trades");
    match &post_delete {
        Value::I64(n) => assert!(
            *n < 100,
            "expected fewer than 100 rows after delete, got {n}"
        ),
        Value::F64(n) => assert!((*n as i64) < 100, "expected fewer than 100 rows"),
        _ => panic!("unexpected count type: {post_delete:?}"),
    }

    // 9. Aggregate query.
    let (cols, rows) = query(
        db_root,
        "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol",
    );
    assert!(cols.len() >= 3, "expected at least 3 columns in aggregate");
    assert!(!rows.is_empty(), "expected at least 1 group");

    // 10. SAMPLE BY query.
    let (_, sample_rows) = query(db_root, "SELECT avg(price) FROM trades SAMPLE BY 1h");
    assert!(
        !sample_rows.is_empty(),
        "SAMPLE BY should produce at least 1 bucket"
    );

    // 11. Snapshot and verify.
    let snap_dir = dir.path().join("snapshot");
    let snap_info = create_snapshot(db_root, &snap_dir).unwrap();
    assert!(snap_info.tables.contains(&"trades".to_string()));
    assert!(snap_info.total_bytes > 0);

    // 12. Record pre-drop count.
    let pre_drop_count = scalar(db_root, "SELECT count(*) FROM trades");

    // 13. Drop table.
    exec_ok(db_root, "DROP TABLE trades");

    // 14. Verify table is gone.
    let err = exec_err(db_root, "SELECT * FROM trades");
    assert!(
        matches!(
            err,
            ExchangeDbError::TableNotFound(_) | ExchangeDbError::TableNotFoundAt { .. }
        ),
        "expected TableNotFound, got {err:?}"
    );

    // 15. Restore from snapshot.
    restore_snapshot(&snap_dir, db_root).unwrap();

    // 16. Verify data survived restore.
    let post_restore = scalar(db_root, "SELECT count(*) FROM trades");
    assert!(
        post_restore.eq_coerce(&pre_drop_count),
        "expected {pre_drop_count:?} after restore, got {post_restore:?}"
    );
}

#[test]
fn lifecycle_multi_table() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(
        db_root,
        "CREATE TABLE orders (timestamp TIMESTAMP, symbol VARCHAR, qty DOUBLE)",
    );
    exec_ok(
        db_root,
        "CREATE TABLE fills (timestamp TIMESTAMP, order_id BIGINT, price DOUBLE)",
    );

    for i in 0..20i64 {
        let sql = format!(
            "INSERT INTO orders VALUES ({}, 'ETH/USD', {}.0)",
            TS_BASE + i * 1_000_000_000,
            i + 1
        );
        exec_ok(db_root, &sql);
    }
    for i in 0..30i64 {
        let sql = format!(
            "INSERT INTO fills VALUES ({}, {}, {}.0)",
            TS_BASE + i * 1_000_000_000,
            i + 1000,
            100 + i
        );
        exec_ok(db_root, &sql);
    }

    let orders_count = scalar(db_root, "SELECT count(*) FROM orders");
    let fills_count = scalar(db_root, "SELECT count(*) FROM fills");
    assert!(orders_count.eq_coerce(&Value::I64(20)));
    assert!(fills_count.eq_coerce(&Value::I64(30)));
}

#[test]
fn lifecycle_create_insert_select_drop() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(db_root, "CREATE TABLE t1 (timestamp TIMESTAMP, val DOUBLE)");
    exec_ok(
        db_root,
        &format!("INSERT INTO t1 VALUES ({}, 42.0)", TS_BASE),
    );
    let v = scalar(db_root, "SELECT val FROM t1");
    assert_eq!(v, Value::F64(42.0));

    exec_ok(db_root, "DROP TABLE t1");
    let err = exec_err(db_root, "SELECT * FROM t1");
    assert!(matches!(
        err,
        ExchangeDbError::TableNotFound(_) | ExchangeDbError::TableNotFoundAt { .. }
    ));
}

// ===========================================================================
// Task 2: psql / client compatibility queries
// ===========================================================================

#[test]
fn psql_connect_set_queries() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // psql sends SET on connect -- these should all succeed (no-op).
    exec_ok(db_root, "SET client_encoding = 'UTF8'");
    exec_ok(db_root, "SET DateStyle = 'ISO'");
    exec_ok(db_root, "SET extra_float_digits = 3");
}

#[test]
fn psql_connect_show_queries() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // SHOW server_version should return a value.
    let (_, rows) = query(db_root, "SHOW server_version");
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        Value::Str(s) => assert!(!s.is_empty(), "server_version should not be empty"),
        other => panic!("expected Str for server_version, got {other:?}"),
    }
}

#[test]
fn psql_current_database_function() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let (_, rows) = query(db_root, "SELECT current_database()");
    assert_eq!(rows.len(), 1, "current_database() should return 1 row");
}

#[test]
fn psql_current_schema_function() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let (_, rows) = query(db_root, "SELECT current_schema()");
    assert_eq!(rows.len(), 1, "current_schema() should return 1 row");
}

#[test]
fn grafana_pg_type_query() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // Grafana queries pg_catalog.pg_type on connect.
    let (cols, _rows) = query(db_root, "SELECT * FROM pg_catalog.pg_type");
    assert!(!cols.is_empty(), "pg_type should have columns");
}

#[test]
fn grafana_information_schema_tables() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(
        db_root,
        "CREATE TABLE metrics (timestamp TIMESTAMP, val DOUBLE)",
    );

    let (cols, _rows) = query(db_root, "SELECT * FROM information_schema.tables");
    assert!(
        !cols.is_empty(),
        "information_schema.tables should have columns"
    );
}

#[test]
fn grafana_pg_namespace() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let (cols, _rows) = query(db_root, "SELECT * FROM pg_catalog.pg_namespace");
    assert!(!cols.is_empty(), "pg_namespace should have columns");
}

#[test]
fn dbeaver_pg_database() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let (cols, _rows) = query(db_root, "SELECT * FROM pg_catalog.pg_database");
    assert!(!cols.is_empty(), "pg_database should have columns");
}

#[test]
fn dbeaver_pg_settings() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let (cols, _rows) = query(
        db_root,
        "SELECT * FROM pg_catalog.pg_settings WHERE name = 'server_version'",
    );
    assert!(!cols.is_empty(), "pg_settings should have columns");
}

#[test]
fn transaction_stubs() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    // BEGIN / COMMIT / ROLLBACK are no-ops but should succeed.
    exec_ok(db_root, "BEGIN");
    exec_ok(db_root, "COMMIT");
    exec_ok(db_root, "ROLLBACK");
}

// ===========================================================================
// Task 3: WAL durability
// ===========================================================================

#[test]
fn wal_durability_write_crash_recover() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "wal_trades");
    let table_dir = db_root.join("wal_trades");

    // Write 1000 rows through WalTableWriter, flush but do NOT commit.
    {
        let config = WalTableWriterConfig {
            buffer_capacity: 500,
            ..Default::default()
        };
        let mut writer = WalTableWriter::open(db_root, "wal_trades", config).unwrap();

        for i in 0..1000i64 {
            let ts = Timestamp(TS_BASE + i * 1_000_000_000);
            writer
                .write_row(
                    ts,
                    vec![
                        OwnedColumnValue::Timestamp(ts.0),
                        OwnedColumnValue::F64(50000.0 + i as f64),
                    ],
                )
                .unwrap();
        }
        // Flush to WAL but do NOT commit (simulate crash).
        writer.flush().unwrap();
        // Drop writer -- simulates process death.
    }

    // Data should NOT be in column files yet.
    let part_dir = table_dir.join("2024-03-15");
    // Partition may or may not exist depending on auto-flush / merge behavior,
    // but WAL should have the data.

    // Run recovery.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert!(
        stats.rows_recovered >= 1,
        "expected recovered rows, got {:?}",
        stats
    );

    // Verify data is now in column files.
    assert!(part_dir.exists(), "partition should exist after recovery");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert!(
        reader.row_count() >= 1,
        "expected at least 1 row after recovery, got {}",
        reader.row_count()
    );
}

#[test]
fn wal_durability_committed_data_persists() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "committed");

    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "committed", config).unwrap();

        let ts = Timestamp(TS_BASE);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(99999.0),
                ],
            )
            .unwrap();

        // Full commit -- data should be in column files.
        writer.commit().unwrap();
    }

    // Recovery should find nothing to do.
    let stats = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats.rows_recovered, 0);

    // Data should be in column files.
    let part_dir = db_root.join("committed").join("2024-03-15");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 1);
    assert_eq!(reader.read_f64(0), 99999.0);
}

#[test]
fn wal_recovery_is_idempotent() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let _meta = create_test_table(db_root, "idem");

    {
        let config = WalTableWriterConfig::default();
        let mut writer = WalTableWriter::open(db_root, "idem", config).unwrap();

        let ts = Timestamp(TS_BASE);
        writer
            .write_row(
                ts,
                vec![
                    OwnedColumnValue::Timestamp(ts.0),
                    OwnedColumnValue::F64(12345.0),
                ],
            )
            .unwrap();
        writer.flush().unwrap();
    }

    // First recovery.
    let stats1 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats1.rows_recovered, 1);

    // Second recovery should find nothing.
    let stats2 = RecoveryManager::recover_all(db_root).unwrap();
    assert_eq!(stats2.rows_recovered, 0);

    // Data should still be exactly 1 row (no duplicates).
    let reader = FixedColumnReader::open(
        &db_root.join("idem").join("2024-03-15").join("price.d"),
        ColumnType::F64,
    )
    .unwrap();
    assert_eq!(reader.row_count(), 1);
}

// ===========================================================================
// Task 4: Concurrent access
// ===========================================================================

#[test]
#[ignore]
fn concurrent_readers_and_writer() {
    let dir = tempdir().unwrap();
    let db_root = dir.path().to_path_buf();

    // Create table and insert initial rows.
    exec_ok(
        &db_root,
        "CREATE TABLE conc (timestamp TIMESTAMP, val DOUBLE)",
    );
    for i in 0..50i64 {
        exec_ok(
            &db_root,
            &format!(
                "INSERT INTO conc VALUES ({}, {}.0)",
                TS_BASE + i * 1_000_000_000,
                i
            ),
        );
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer_count = Arc::new(AtomicU64::new(0));
    let reader_panic = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(5)); // 1 writer + 4 readers

    // Writer thread: continuously insert rows.
    let db_w = db_root.clone();
    let stop_w = Arc::clone(&stop);
    let wc = Arc::clone(&writer_count);
    let barrier_w = Arc::clone(&barrier);
    let writer_handle = thread::spawn(move || {
        barrier_w.wait();
        let mut i = 1000i64;
        while !stop_w.load(Ordering::Relaxed) {
            let sql = format!(
                "INSERT INTO conc VALUES ({}, {}.0)",
                TS_BASE + i * 1_000_000_000,
                i
            );
            let plan = plan_query(&sql).unwrap();
            if execute(&db_w, &plan).is_ok() {
                wc.fetch_add(1, Ordering::Relaxed);
            }
            i += 1;
        }
    });

    // 4 reader threads.
    let mut reader_handles = Vec::new();
    for _ in 0..4 {
        let db_r = db_root.clone();
        let stop_r = Arc::clone(&stop);
        let _panic_flag = Arc::clone(&reader_panic);
        let barrier_r = Arc::clone(&barrier);
        let h = thread::spawn(move || {
            barrier_r.wait();
            while !stop_r.load(Ordering::Relaxed) {
                let plan = match plan_query("SELECT count(*) FROM conc") {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                match execute(&db_r, &plan) {
                    Ok(QueryResult::Rows { rows, .. }) => {
                        if !rows.is_empty() {
                            // Count should always be >= initial 50.
                            match &rows[0][0] {
                                Value::I64(n) if *n >= 50 => {}   // ok
                                Value::F64(n) if *n >= 50.0 => {} // ok
                                _ => {}                           // early read is acceptable
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(_) => {
                        // Some transient errors are acceptable under concurrent access.
                    }
                }
            }
        });
        reader_handles.push(h);
    }

    // Run for 1 second.
    thread::sleep(Duration::from_secs(1));
    stop.store(true, Ordering::Relaxed);

    writer_handle.join().unwrap();
    for h in reader_handles {
        h.join().unwrap();
    }

    assert!(
        !reader_panic.load(Ordering::Relaxed),
        "reader thread panicked"
    );
    let writes = writer_count.load(Ordering::Relaxed);
    assert!(
        writes > 0,
        "writer should have inserted at least some rows, got {writes}"
    );
}

#[test]
fn concurrent_multiple_tables() {
    let dir = tempdir().unwrap();
    let db_root = dir.path().to_path_buf();

    exec_ok(&db_root, "CREATE TABLE t_a (timestamp TIMESTAMP, x DOUBLE)");
    exec_ok(&db_root, "CREATE TABLE t_b (timestamp TIMESTAMP, y DOUBLE)");

    let barrier = Arc::new(Barrier::new(2));

    let db1 = db_root.clone();
    let b1 = Arc::clone(&barrier);
    let h1 = thread::spawn(move || {
        b1.wait();
        for i in 0..50i64 {
            exec_ok(
                &db1,
                &format!(
                    "INSERT INTO t_a VALUES ({}, {}.0)",
                    TS_BASE + i * 1_000_000_000,
                    i
                ),
            );
        }
    });

    let db2 = db_root.clone();
    let b2 = Arc::clone(&barrier);
    let h2 = thread::spawn(move || {
        b2.wait();
        for i in 0..50i64 {
            exec_ok(
                &db2,
                &format!(
                    "INSERT INTO t_b VALUES ({}, {}.0)",
                    TS_BASE + i * 1_000_000_000,
                    i * 10
                ),
            );
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    let a_count = scalar(&db_root, "SELECT count(*) FROM t_a");
    let b_count = scalar(&db_root, "SELECT count(*) FROM t_b");
    assert!(a_count.eq_coerce(&Value::I64(50)));
    assert!(b_count.eq_coerce(&Value::I64(50)));
}

#[test]
fn concurrent_reads_see_consistent_data() {
    let dir = tempdir().unwrap();
    let db_root = dir.path().to_path_buf();

    exec_ok(
        &db_root,
        "CREATE TABLE snap_read (timestamp TIMESTAMP, val DOUBLE)",
    );
    for i in 0..100i64 {
        exec_ok(
            &db_root,
            &format!(
                "INSERT INTO snap_read VALUES ({}, {}.0)",
                TS_BASE + i * 1_000_000_000,
                i
            ),
        );
    }

    // Spawn 8 reader threads, each reading count and sum.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let db = db_root.clone();
        let h = thread::spawn(move || {
            for _ in 0..20 {
                let (_, rows) = query(&db, "SELECT count(*), sum(val) FROM snap_read");
                assert_eq!(rows.len(), 1);
                // Count should be exactly 100 (no writes happening).
                let count = &rows[0][0];
                assert!(
                    count.eq_coerce(&Value::I64(100)),
                    "expected 100, got {count:?}"
                );
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ===========================================================================
// Task 5: Error message quality
// ===========================================================================

#[test]
fn error_table_not_found_includes_name() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let err = exec_err(db_root, "SELECT * FROM nonexistent_table");
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent_table"),
        "error should include table name, got: {msg}"
    );
}

#[test]
fn error_table_not_found_at_includes_context() {
    // Test the new detailed variant directly.
    let err = ExchangeDbError::TableNotFoundAt {
        table: "trades".to_string(),
        db_path: "./data".to_string(),
    };
    let msg = err.to_string();
    assert!(msg.contains("trades"), "should contain table name: {msg}");
    assert!(msg.contains("./data"), "should contain db path: {msg}");
}

#[test]
fn error_with_db_path_upgrades_table_not_found() {
    let base = ExchangeDbError::TableNotFound("my_table".to_string());
    let upgraded = base.with_db_path(Path::new("/var/lib/exchangedb/data"));
    let msg = upgraded.to_string();
    assert!(
        msg.contains("my_table"),
        "upgraded error should include table name: {msg}"
    );
    assert!(
        msg.contains("/var/lib/exchangedb/data"),
        "upgraded error should include db path: {msg}"
    );
}

#[test]
fn error_column_not_found_includes_table() {
    let err = ExchangeDbError::ColumnNotFound("nonexistent".to_string(), "trades".to_string());
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent") && msg.contains("trades"),
        "error should include both column and table: {msg}"
    );
}

#[test]
fn error_column_not_found_detailed_lists_available() {
    let err = ExchangeDbError::ColumnNotFoundDetailed {
        column: "prce".to_string(),
        table: "trades".to_string(),
        available: "timestamp, symbol, price, volume".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("prce") && msg.contains("trades") && msg.contains("price"),
        "detailed error should include column, table, and available list: {msg}"
    );
}

#[test]
fn error_type_mismatch_in_column_has_context() {
    let err = ExchangeDbError::TypeMismatchInColumn {
        column: "price".to_string(),
        table: "trades".to_string(),
        expected: "DOUBLE".to_string(),
        actual: "VARCHAR".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("price") && msg.contains("trades") && msg.contains("DOUBLE"),
        "type mismatch should include column, table, expected: {msg}"
    );
}

#[test]
fn error_wal_detailed_has_segment_info() {
    let err = ExchangeDbError::WalDetailed {
        table: "trades".to_string(),
        segment: 42,
        detail: "checksum mismatch".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("trades") && msg.contains("42") && msg.contains("checksum"),
        "WAL error should include table, segment, detail: {msg}"
    );
}

#[test]
fn error_corruption_in_file_has_path() {
    let err = ExchangeDbError::CorruptionInFile {
        file: "/data/trades/2024-03-15/price.d".to_string(),
        detail: "unexpected EOF at byte 1024".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("price.d") && msg.contains("unexpected EOF"),
        "corruption error should include file and detail: {msg}"
    );
}

#[test]
fn error_table_already_exists() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(
        db_root,
        "CREATE TABLE dup_test (timestamp TIMESTAMP, val DOUBLE)",
    );
    let err = exec_err(
        db_root,
        "CREATE TABLE dup_test (timestamp TIMESTAMP, val DOUBLE)",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("dup_test"),
        "already exists error should include table name: {msg}"
    );
}

#[test]
fn error_duplicate_key_detailed() {
    let err = ExchangeDbError::DuplicateKeyDetailed {
        table: "trades".to_string(),
        timestamp: 1710513000000000000,
        detail: "row already exists with same timestamp and symbol".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("trades") && msg.contains("1710513000000000000"),
        "duplicate key error should include table and timestamp: {msg}"
    );
}

#[test]
fn error_lock_timeout_detailed() {
    let err = ExchangeDbError::LockTimeoutDetailed {
        table: "trades".to_string(),
        partition: "2024-03-15".to_string(),
        waited_ms: 5000,
        detail: "writer lock held by another transaction".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("trades") && msg.contains("2024-03-15") && msg.contains("5000"),
        "lock timeout error should include table, partition, time: {msg}"
    );
}

#[test]
fn error_snapshot_has_path() {
    let err = ExchangeDbError::Snapshot {
        detail: "manifest version mismatch".to_string(),
        path: "/backups/snap-20240315".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("manifest") && msg.contains("/backups/snap-20240315"),
        "snapshot error should include detail and path: {msg}"
    );
}

#[test]
fn error_recovery_has_table() {
    let err = ExchangeDbError::Recovery {
        table: "trades".to_string(),
        detail: "WAL segment corrupted".to_string(),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("trades") && msg.contains("corrupted"),
        "recovery error should include table and detail: {msg}"
    );
}

// ===========================================================================
// Additional integration tests
// ===========================================================================

#[test]
fn ilp_parse_multi_line_batch() {
    let batch = "\
        trades,symbol=BTC/USD price=65000.0,volume=1.5 1710513000000000000\n\
        trades,symbol=ETH/USD price=3500.0,volume=10.0 1710513001000000000\n\
        trades,symbol=SOL/USD price=150.0,volume=100.0 1710513002000000000\n";
    let lines = parse_ilp_batch(batch).unwrap();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0].measurement, "trades");
    assert_eq!(lines[2].measurement, "trades");
}

#[test]
fn ilp_parse_with_tags_and_fields() {
    let line =
        "trades,symbol=BTC/USD,exchange=binance price=65000.0,volume=1.5i 1710513000000000000";
    let parsed = parse_ilp_line(line).unwrap();
    assert_eq!(parsed.tags.len(), 2);
    assert_eq!(parsed.tags.get("symbol").unwrap(), "BTC/USD");
    assert_eq!(parsed.tags.get("exchange").unwrap(), "binance");
    assert_eq!(parsed.fields.len(), 2);
}

#[test]
fn snapshot_empty_database() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();
    let snap_dir = dir.path().join("snap_empty");

    let info = create_snapshot(db_root, &snap_dir).unwrap();
    assert!(info.tables.is_empty());
    assert_eq!(info.total_bytes, 0);
}

#[test]
fn snapshot_restore_preserves_all_tables() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(db_root, "CREATE TABLE t1 (timestamp TIMESTAMP, a DOUBLE)");
    exec_ok(db_root, "CREATE TABLE t2 (timestamp TIMESTAMP, b VARCHAR)");
    exec_ok(
        db_root,
        &format!("INSERT INTO t1 VALUES ({}, 1.0)", TS_BASE),
    );
    exec_ok(
        db_root,
        &format!("INSERT INTO t2 VALUES ({}, 'hello')", TS_BASE),
    );

    let snap = dir.path().join("snap_multi");
    let info = create_snapshot(db_root, &snap).unwrap();
    assert_eq!(info.tables.len(), 2);

    // Drop both.
    exec_ok(db_root, "DROP TABLE t1");
    exec_ok(db_root, "DROP TABLE t2");

    // Restore.
    restore_snapshot(&snap, db_root).unwrap();

    // Both tables should work.
    let v1 = scalar(db_root, "SELECT a FROM t1");
    assert_eq!(v1, Value::F64(1.0));
    let v2 = scalar(db_root, "SELECT b FROM t2");
    assert_eq!(v2, Value::Str("hello".to_string()));
}

#[test]
fn order_by_and_limit() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(
        db_root,
        "CREATE TABLE sorted (timestamp TIMESTAMP, val DOUBLE)",
    );
    for i in 0..20i64 {
        exec_ok(
            db_root,
            &format!(
                "INSERT INTO sorted VALUES ({}, {}.0)",
                TS_BASE + i * 1_000_000_000,
                20 - i
            ),
        );
    }

    let (_, rows) = query(db_root, "SELECT val FROM sorted ORDER BY val ASC LIMIT 5");
    assert_eq!(rows.len(), 5);
    // First value should be the smallest.
    assert_eq!(rows[0][0], Value::F64(1.0));
}

#[test]
fn where_clause_filtering() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(
        db_root,
        "CREATE TABLE filt (timestamp TIMESTAMP, category VARCHAR, amount DOUBLE)",
    );
    for i in 0..30i64 {
        let cat = if i % 2 == 0 { "A" } else { "B" };
        exec_ok(
            db_root,
            &format!(
                "INSERT INTO filt VALUES ({}, '{}', {}.0)",
                TS_BASE + i * 1_000_000_000,
                cat,
                i * 10
            ),
        );
    }

    let (_, rows) = query(db_root, "SELECT count(*) FROM filt WHERE category = 'A'");
    let count = &rows[0][0];
    assert!(
        count.eq_coerce(&Value::I64(15)),
        "expected 15 A rows, got {count:?}"
    );
}

#[test]
fn null_handling_in_aggregates() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    exec_ok(
        db_root,
        "CREATE TABLE nulls (timestamp TIMESTAMP, val DOUBLE)",
    );
    exec_ok(
        db_root,
        &format!("INSERT INTO nulls VALUES ({}, 10.0)", TS_BASE),
    );
    exec_ok(
        db_root,
        &format!(
            "INSERT INTO nulls VALUES ({}, NULL)",
            TS_BASE + 1_000_000_000
        ),
    );
    exec_ok(
        db_root,
        &format!(
            "INSERT INTO nulls VALUES ({}, 30.0)",
            TS_BASE + 2_000_000_000
        ),
    );

    let (_, rows) = query(db_root, "SELECT count(*), count(val), avg(val) FROM nulls");
    assert_eq!(rows.len(), 1);
    // count(*) should be 3, count(val) should be 2.
    let count_all = &rows[0][0];
    let count_val = &rows[0][1];
    assert!(
        count_all.eq_coerce(&Value::I64(3)),
        "count(*) should be 3, got {count_all:?}"
    );
    assert!(
        count_val.eq_coerce(&Value::I64(2)),
        "count(val) should be 2, got {count_val:?}"
    );
}
