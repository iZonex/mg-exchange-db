//! End-to-end benchmarks: full SQL pipeline (parse -> plan -> execute).

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
use exchange_query::{QueryResult, execute, plan_query};
use std::path::Path;
use tempfile::TempDir;

const HUNDRED_K: u64 = 100_000;

/// Populate a trades table with `num_rows` rows for benchmarking.
fn populate_trades(dir: &Path, num_rows: u64) {
    let _meta = TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("symbol", ColumnType::I32)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::F64)
        .column("side", ColumnType::I32)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(dir)
        .expect("create table failed");

    let mut writer = TableWriter::open(dir, "trades").expect("open writer failed");
    let base_ts: i64 = 1_704_067_200 * 1_000_000_000; // 2024-01-01 UTC
    let mut price = 50_000.0_f64;

    for i in 0..num_rows {
        let ts = Timestamp(base_ts + i as i64 * 1_000_000);
        let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
        price += delta;
        if price < 1.0 {
            price = 1.0;
        }

        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32((i % 100) as i32),
                    ColumnValue::F64(price),
                    ColumnValue::F64(0.01 + (i % 10_000) as f64 * 0.01),
                    ColumnValue::I32((i % 2) as i32),
                ],
            )
            .unwrap();
    }
    writer.flush().unwrap();
}

/// Run a SQL statement through the full pipeline: parse -> plan -> execute.
fn full_pipeline(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).expect("plan_query failed");
    execute(db_root, &plan).expect("execute failed")
}

// ---------------------------------------------------------------------------
// 1. Full SQL pipeline: parse -> plan -> execute (SELECT on pre-populated data)
// ---------------------------------------------------------------------------

fn sql_parse_plan_execute(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    populate_trades(dir.path(), HUNDRED_K);

    let mut group = c.benchmark_group("sql_parse_plan_execute");
    group.throughput(Throughput::Elements(HUNDRED_K));
    group.sample_size(10);

    group.bench_function("select_star_100K", |b| {
        b.iter(|| {
            let result = full_pipeline(dir.path(), "SELECT * FROM trades");
            black_box(result);
        });
    });

    group.bench_function("select_filter_100K", |b| {
        b.iter(|| {
            let result = full_pipeline(dir.path(), "SELECT * FROM trades WHERE price > 50000");
            black_box(result);
        });
    });

    group.bench_function("select_aggregate_100K", |b| {
        b.iter(|| {
            let result = full_pipeline(
                dir.path(),
                "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. CREATE TABLE pipeline throughput
// ---------------------------------------------------------------------------

fn create_table_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_table_pipeline");
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_table_sql", |b| {
        b.iter_with_setup(
            || TempDir::new().unwrap(),
            |dir| {
                let result = full_pipeline(
                    dir.path(),
                    "CREATE TABLE bench_tbl (\
                         timestamp TIMESTAMP,\
                         symbol INT,\
                         price DOUBLE,\
                         volume DOUBLE,\
                         side INT\
                     )",
                );
                black_box(result);
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. INSERT via SQL pipeline throughput
// ---------------------------------------------------------------------------

fn insert_via_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_via_sql");
    // We insert batches of 100 rows per SQL statement (multi-row INSERT).
    let batch_size: u64 = 100;
    let num_batches: u64 = 1000;
    let total_rows = batch_size * num_batches;
    group.throughput(Throughput::Elements(total_rows));
    group.sample_size(10);

    // Build a multi-row INSERT statement with `batch_size` rows.
    let base_ts: i64 = 1_704_067_200_000_000_000;
    let batches: Vec<String> = (0..num_batches)
        .map(|batch_idx| {
            let rows: Vec<String> = (0..batch_size)
                .map(|row_idx| {
                    let i = batch_idx * batch_size + row_idx;
                    let ts = base_ts + i as i64 * 1_000_000;
                    let symbol = (i % 100) as i32;
                    let price = 50_000.0 + (i % 1000) as f64 * 0.01;
                    let volume = 0.01 + (i % 10_000) as f64 * 0.01;
                    let side = (i % 2) as i32;
                    format!("({ts}, {symbol}, {price}, {volume}, {side})")
                })
                .collect();
            format!(
                "INSERT INTO trades (timestamp, symbol, price, volume, side) VALUES {}",
                rows.join(", ")
            )
        })
        .collect();

    group.bench_function("100K_rows_via_sql_batches", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                // Create the table first.
                full_pipeline(
                    dir.path(),
                    "CREATE TABLE trades (\
                         timestamp TIMESTAMP,\
                         symbol INT,\
                         price DOUBLE,\
                         volume DOUBLE,\
                         side INT\
                     )",
                );
                dir
            },
            |dir| {
                for sql in &batches {
                    let result = full_pipeline(dir.path(), sql);
                    black_box(&result);
                }
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    sql_parse_plan_execute,
    create_table_pipeline,
    insert_via_sql,
);
criterion_main!(benches);
