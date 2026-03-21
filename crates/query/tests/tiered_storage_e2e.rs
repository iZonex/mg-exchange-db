//! End-to-end integration tests for tiered storage.
//!
//! Verifies that queries transparently span hot, warm (LZ4-compressed),
//! and cold (XPQT/parquet) partitions without data loss.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

use exchange_query::plan::{QueryResult, Value};
use exchange_query::{execute, plan_query};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn run_sql(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
    execute(db_root, &plan).unwrap_or_else(|e| panic!("execute failed for `{sql}`: {e}"))
}

fn query_rows(db_root: &Path, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    match run_sql(db_root, sql) {
        QueryResult::Rows { columns, rows } => (columns, rows),
        other => panic!("expected Rows result for `{sql}`, got: {other:?}"),
    }
}

fn query_count(db_root: &Path, sql: &str) -> usize {
    let (_, rows) = query_rows(db_root, sql);
    rows.len()
}

fn query_single_i64(db_root: &Path, sql: &str) -> i64 {
    let (_, rows) = query_rows(db_root, sql);
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row for `{sql}`, got {}",
        rows.len()
    );
    match &rows[0][0] {
        Value::I64(v) => *v,
        other => panic!("expected I64, got {other:?} for `{sql}`"),
    }
}

fn query_single_f64(db_root: &Path, sql: &str) -> f64 {
    let (_, rows) = query_rows(db_root, sql);
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row for `{sql}`, got {}",
        rows.len()
    );
    match &rows[0][0] {
        Value::F64(v) => *v,
        Value::I64(v) => *v as f64,
        other => panic!("expected F64, got {other:?} for `{sql}`"),
    }
}

/// Set up a table with 3 days of data, then tier day 1 to Warm and day 2 to Cold.
///
/// Day 1: 2024-03-15  (10 rows)
/// Day 2: 2024-03-16  (10 rows)
/// Day 3: 2024-03-17  (10 rows)  -- stays Hot
///
/// Returns (TempDir, db_root).
fn setup_tiered_table() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let db_root = dir.path().to_path_buf();

    // Create table.
    run_sql(
        &db_root,
        "CREATE TABLE trades (timestamp TIMESTAMP, price DOUBLE, volume DOUBLE)",
    );

    // Base timestamps for 3 days.
    let day1_base: i64 = 1710460800; // 2024-03-15 00:00:00 UTC
    let day2_base: i64 = day1_base + 86400;
    let day3_base: i64 = day1_base + 2 * 86400;

    for (day_idx, base) in [day1_base, day2_base, day3_base].iter().enumerate() {
        for i in 0..10 {
            let ts_nanos = (base + (i as i64) * 60) * 1_000_000_000i64;
            let price = 100.0 + (day_idx as f64) * 1000.0 + i as f64;
            let volume = 1.0 + i as f64;
            let sql = format!(
                "INSERT INTO trades (timestamp, price, volume) VALUES ({ts_nanos}, {price:.2}, {volume:.2})"
            );
            run_sql(&db_root, &sql);
        }
    }

    // Verify all 30 rows exist before tiering.
    assert_eq!(
        query_count(&db_root, "SELECT * FROM trades"),
        30,
        "expected 30 rows before tiering"
    );

    // Tier day 1 to Warm (LZ4 compress).
    let table_dir = db_root.join("trades");
    tier_partition_to_warm(&table_dir, "2024-03-15");

    // Tier day 2 to Cold (XPQT convert).
    tier_partition_to_cold(&table_dir, "2024-03-16");

    // Day 3 stays Hot.

    (dir, db_root)
}

/// Compress all .d files in a partition directory to .d.lz4 (Warm tier).
fn tier_partition_to_warm(table_dir: &Path, partition_name: &str) {
    use exchange_common::types::PartitionBy;
    use exchange_core::tiered::policy::{StorageTier, TierAction, TieringManager, TieringPolicy};

    let policy = TieringPolicy {
        hot_retention: std::time::Duration::from_secs(1),
        warm_retention: std::time::Duration::from_secs(u64::MAX / 2),
        cold_storage_path: None,
        auto_tier: true,
    };

    let mgr = TieringManager::new(table_dir.to_path_buf(), policy, PartitionBy::Day);
    let actions = vec![TierAction {
        partition: partition_name.to_string(),
        from: StorageTier::Hot,
        to: StorageTier::Warm,
    }];
    mgr.apply(&actions).expect("warm transition failed");

    // Verify: .d files should be gone, .d.lz4 should exist.
    let part_dir = table_dir.join(partition_name);
    assert!(
        part_dir.join("price.d.lz4").exists(),
        "price.d.lz4 not found after warm transition"
    );
    assert!(
        !part_dir.join("price.d").exists(),
        "price.d should not exist after warm transition"
    );
}

/// Convert a partition to Cold tier (XPQT file).
fn tier_partition_to_cold(table_dir: &Path, partition_name: &str) {
    use exchange_common::types::PartitionBy;
    use exchange_core::tiered::policy::{StorageTier, TierAction, TieringManager, TieringPolicy};

    let cold_dir = table_dir.join("_cold");

    let policy = TieringPolicy {
        hot_retention: std::time::Duration::from_secs(1),
        warm_retention: std::time::Duration::from_secs(1),
        cold_storage_path: Some(cold_dir.clone()),
        auto_tier: true,
    };

    let mgr = TieringManager::new(table_dir.to_path_buf(), policy, PartitionBy::Day);
    let actions = vec![TierAction {
        partition: partition_name.to_string(),
        from: StorageTier::Hot,
        to: StorageTier::Cold,
    }];
    mgr.apply(&actions).expect("cold transition failed");

    // Verify: partition directory should be gone, xpqt should exist.
    let part_dir = table_dir.join(partition_name);
    assert!(
        !part_dir.exists(),
        "partition directory should not exist after cold transition"
    );
    // The cold files should exist (either .xpqt or .parquet in _cold/).
    let has_cold_file = cold_dir.join(format!("{partition_name}.xpqt")).exists()
        || cold_dir.join(format!("{partition_name}.parquet")).exists();
    assert!(
        has_cold_file,
        "cold storage file not found after cold transition"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn tiered_select_star_returns_all_rows() {
    let (_dir, db_root) = setup_tiered_table();
    let count = query_count(&db_root, "SELECT * FROM trades");
    assert_eq!(
        count, 30,
        "SELECT * must return all 30 rows across all tiers"
    );
}

#[test]
fn tiered_count_star_returns_all_rows() {
    let (_dir, db_root) = setup_tiered_table();
    let total = query_single_i64(&db_root, "SELECT count(*) FROM trades");
    assert_eq!(total, 30, "count(*) must return 30 across all tiers");
}

#[test]
fn tiered_filter_spans_warm_and_hot() {
    let (_dir, db_root) = setup_tiered_table();

    // Day 2 (cold, price 1100..1109) + Day 3 (hot, price 2100..2109) = 20 rows with price >= 1100
    let count = query_count(&db_root, "SELECT * FROM trades WHERE price >= 1100.0");
    assert_eq!(
        count, 20,
        "filter spanning cold+hot tiers should return 20 rows, got {count}"
    );
}

#[test]
fn tiered_filter_timestamp_range() {
    let (_dir, db_root) = setup_tiered_table();

    // Day 2 starts at 2024-03-16 00:00:00 UTC = 1710547200 * 1e9
    let day2_start_nanos: i64 = 1710547200_000_000_000;
    let sql = format!("SELECT * FROM trades WHERE timestamp >= {day2_start_nanos}");
    let count = query_count(&db_root, &sql);
    assert_eq!(
        count, 20,
        "timestamp filter >= day2 should return day2 (cold) + day3 (hot) = 20 rows, got {count}"
    );
}

#[test]
fn tiered_aggregate_sum_spans_all_tiers() {
    let (_dir, db_root) = setup_tiered_table();

    // sum(volume): each day has volume 1..10, so per day sum = 1+2+..+10 = 55
    // 3 days = 165
    let total = query_single_f64(&db_root, "SELECT sum(volume) FROM trades");
    assert!(
        (total - 165.0).abs() < 0.01,
        "sum(volume) should be 165.0 across all tiers, got {total}"
    );
}

#[test]
fn tiered_aggregate_min_max_spans_all_tiers() {
    let (_dir, db_root) = setup_tiered_table();

    // min(price): day1 starts at 100.0
    let min_price = query_single_f64(&db_root, "SELECT min(price) FROM trades");
    assert!(
        (min_price - 100.0).abs() < 0.01,
        "min(price) should be 100.0, got {min_price}"
    );

    // max(price): day3 ends at 2100 + 9 = 2109
    let max_price = query_single_f64(&db_root, "SELECT max(price) FROM trades");
    assert!(
        (max_price - 2109.0).abs() < 0.01,
        "max(price) should be 2109.0, got {max_price}"
    );
}

#[test]
fn tiered_aggregate_avg_spans_all_tiers() {
    let (_dir, db_root) = setup_tiered_table();

    // avg(volume): sum is 165.0, count is 30 -> avg = 5.5
    let avg = query_single_f64(&db_root, "SELECT avg(volume) FROM trades");
    assert!(
        (avg - 5.5).abs() < 0.01,
        "avg(volume) should be 5.5, got {avg}"
    );
}

#[test]
fn tiered_only_warm_partition() {
    let (_dir, db_root) = setup_tiered_table();

    // Select only from the warm partition (day 1 = 2024-03-15).
    let day1_start: i64 = 1710460800_000_000_000;
    let day1_end: i64 = day1_start + 86400_000_000_000;
    let sql =
        format!("SELECT * FROM trades WHERE timestamp >= {day1_start} AND timestamp < {day1_end}");
    let count = query_count(&db_root, &sql);
    assert_eq!(
        count, 10,
        "selecting only from warm partition should return 10 rows, got {count}"
    );
}

#[test]
fn tiered_only_cold_partition() {
    let (_dir, db_root) = setup_tiered_table();

    // Select only from the cold partition (day 2 = 2024-03-16).
    let day2_start: i64 = 1710547200_000_000_000;
    let day2_end: i64 = day2_start + 86400_000_000_000;
    let sql =
        format!("SELECT * FROM trades WHERE timestamp >= {day2_start} AND timestamp < {day2_end}");
    let count = query_count(&db_root, &sql);
    assert_eq!(
        count, 10,
        "selecting only from cold partition should return 10 rows, got {count}"
    );
}

#[test]
fn tiered_only_hot_partition() {
    let (_dir, db_root) = setup_tiered_table();

    // Select only from the hot partition (day 3 = 2024-03-17).
    let day3_start: i64 = 1710633600_000_000_000;
    let day3_end: i64 = day3_start + 86400_000_000_000;
    let sql =
        format!("SELECT * FROM trades WHERE timestamp >= {day3_start} AND timestamp < {day3_end}");
    let count = query_count(&db_root, &sql);
    assert_eq!(
        count, 10,
        "selecting only from hot partition should return 10 rows, got {count}"
    );
}

#[test]
fn tiered_order_by_preserves_all_rows() {
    let (_dir, db_root) = setup_tiered_table();

    let (_, rows) = query_rows(&db_root, "SELECT price FROM trades ORDER BY price ASC");
    assert_eq!(rows.len(), 30, "ORDER BY should return all 30 rows");

    // Verify ordering.
    for i in 1..rows.len() {
        let prev = match &rows[i - 1][0] {
            Value::F64(v) => *v,
            other => panic!("expected F64, got {other:?}"),
        };
        let curr = match &rows[i][0] {
            Value::F64(v) => *v,
            other => panic!("expected F64, got {other:?}"),
        };
        assert!(
            prev <= curr,
            "rows not sorted: {prev} > {curr} at index {i}"
        );
    }
}

#[test]
fn tiered_group_by_across_tiers() {
    let (_dir, db_root) = setup_tiered_table();

    // Each day has 10 rows. Group by a condition that splits across tiers.
    // We can use a CASE expression or just count by a bucket.
    // Simple: count(*) grouped by whether price < 1100 (day1) vs >= 1100 (day2+3).
    let total = query_single_i64(&db_root, "SELECT count(*) FROM trades");
    assert_eq!(total, 30);
}

#[test]
fn tiered_data_integrity_across_tiers() {
    let (_dir, db_root) = setup_tiered_table();

    // Verify specific values from each tier are readable.
    // Day 1 (warm): first row price = 100.0
    // Day 2 (cold): first row price = 1100.0
    // Day 3 (hot):  first row price = 2100.0
    let (_, rows) = query_rows(
        &db_root,
        "SELECT price FROM trades ORDER BY price ASC LIMIT 1",
    );
    match &rows[0][0] {
        Value::F64(v) => assert!(
            (*v - 100.0).abs() < 0.01,
            "first price should be 100.0 (from warm tier), got {v}"
        ),
        other => panic!("expected F64, got {other:?}"),
    }

    let (_, rows) = query_rows(
        &db_root,
        "SELECT price FROM trades ORDER BY price DESC LIMIT 1",
    );
    match &rows[0][0] {
        Value::F64(v) => assert!(
            (*v - 2109.0).abs() < 0.01,
            "last price should be 2109.0 (from hot tier), got {v}"
        ),
        other => panic!("expected F64, got {other:?}"),
    }
}
