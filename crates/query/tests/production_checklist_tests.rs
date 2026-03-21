//! Tests for PRODUCTION_CHECKLIST query-layer items:
//! 1. Atomicity (multi-row INSERT all-or-nothing)
//! 7. Query cancel (CancellationToken and QueryRegistry)

use exchange_common::types::{ColumnType, PartitionBy};
use exchange_core::column::FixedColumnReader;
use exchange_core::table::TableBuilder;
use exchange_query::context::{CancellationToken, QueryRegistry};
use exchange_query::{execute, plan_query};
use std::sync::Arc;
use tempfile::tempdir;

// ── Item 1: Atomicity ──────────────────────────────────────────────────

#[test]
fn multi_row_insert_writes_all_rows() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("price", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    let plan = plan_query(
        "INSERT INTO trades (timestamp, price) VALUES \
         (1710513000000000000, 100.0), \
         (1710513001000000000, 200.0), \
         (1710513002000000000, 300.0)",
    )
    .unwrap();

    let result = execute(db_root, &plan).unwrap();
    match result {
        exchange_query::QueryResult::Ok { affected_rows } => {
            assert_eq!(affected_rows, 3);
        }
        _ => panic!("expected Ok result"),
    }

    // Verify all 3 rows are present.
    let part_dir = db_root.join("trades").join("2024-03-15");
    let reader = FixedColumnReader::open(&part_dir.join("price.d"), ColumnType::F64).unwrap();
    assert_eq!(reader.row_count(), 3);
    assert_eq!(reader.read_f64(0), 100.0);
    assert_eq!(reader.read_f64(1), 200.0);
    assert_eq!(reader.read_f64(2), 300.0);
}

#[test]
fn single_row_insert_still_works() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    TableBuilder::new("orders")
        .column("timestamp", ColumnType::Timestamp)
        .column("amount", ColumnType::I64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(db_root)
        .unwrap();

    let plan =
        plan_query("INSERT INTO orders (timestamp, amount) VALUES (1710513000000000000, 42)")
            .unwrap();

    let result = execute(db_root, &plan).unwrap();
    match result {
        exchange_query::QueryResult::Ok { affected_rows } => {
            assert_eq!(affected_rows, 1);
        }
        _ => panic!("expected Ok result"),
    }
}

#[test]
fn insert_into_nonexistent_table_fails() {
    let dir = tempdir().unwrap();
    let db_root = dir.path();

    let plan =
        plan_query("INSERT INTO phantom (timestamp, x) VALUES (1710513000000000000, 1)").unwrap();

    let result = execute(db_root, &plan);
    assert!(result.is_err(), "insert into nonexistent table should fail");
}

// ── Item 7: Query cancel ──────────────────────────────────────────────

#[test]
fn cancellation_token_starts_uncancelled() {
    let token = CancellationToken::new();
    assert!(!token.is_cancelled());
    assert!(token.check().is_ok());
}

#[test]
fn cancellation_token_cancel_sets_flag() {
    let token = CancellationToken::new();
    token.cancel();
    assert!(token.is_cancelled());
    assert!(token.check().is_err());
}

#[test]
fn cancellation_token_clone_shares_state() {
    let token = CancellationToken::new();
    let clone = token.clone();
    token.cancel();
    assert!(clone.is_cancelled());
    assert!(clone.check().is_err());
}

#[test]
fn cancellation_token_check_returns_query_error() {
    let token = CancellationToken::new();
    token.cancel();
    let err = token.check().unwrap_err();
    assert!(err.to_string().contains("cancelled"), "err: {err}");
}

#[test]
fn query_registry_register_and_deregister() {
    let registry = QueryRegistry::new();
    let (id1, _token1) = registry.register();
    let (id2, _token2) = registry.register();

    assert_ne!(id1, id2);
    assert_eq!(registry.active_count(), 2);

    registry.deregister(id1);
    assert_eq!(registry.active_count(), 1);

    registry.deregister(id2);
    assert_eq!(registry.active_count(), 0);
}

#[test]
fn query_registry_cancel_existing() {
    let registry = QueryRegistry::new();
    let (id, token) = registry.register();

    assert!(!token.is_cancelled());
    let cancelled = registry.cancel(id);
    assert!(cancelled);
    assert!(token.is_cancelled());
    assert!(token.check().is_err());
}

#[test]
fn query_registry_cancel_nonexistent() {
    let registry = QueryRegistry::new();
    let cancelled = registry.cancel(999);
    assert!(!cancelled);
}

#[test]
fn query_registry_cancel_after_deregister() {
    let registry = QueryRegistry::new();
    let (id, _token) = registry.register();
    registry.deregister(id);
    let cancelled = registry.cancel(id);
    assert!(!cancelled, "cannot cancel a deregistered query");
}

#[test]
fn query_registry_active_query_ids() {
    let registry = QueryRegistry::new();
    let (id1, _) = registry.register();
    let (id2, _) = registry.register();

    let ids = registry.active_query_ids();
    assert!(ids.contains(&id1));
    assert!(ids.contains(&id2));
    assert_eq!(ids.len(), 2);
}

#[test]
fn query_registry_concurrent_access() {
    let registry = Arc::new(QueryRegistry::new());
    let mut handles = vec![];

    for _ in 0..10 {
        let reg = Arc::clone(&registry);
        handles.push(std::thread::spawn(move || {
            let (id, token) = reg.register();
            assert!(!token.is_cancelled());
            std::thread::sleep(std::time::Duration::from_millis(5));
            reg.deregister(id);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(registry.active_count(), 0);
}

#[test]
fn query_registry_cancel_from_another_thread() {
    let registry = Arc::new(QueryRegistry::new());
    let (id, token) = registry.register();

    let reg = Arc::clone(&registry);
    let handle = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        reg.cancel(id);
    });

    // Wait for cancellation.
    handle.join().unwrap();
    assert!(token.is_cancelled());
    assert!(token.check().is_err());
}
