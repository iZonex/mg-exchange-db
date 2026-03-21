use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter};
use exchange_query::{QueryResult, execute, plan_query};
use std::path::Path;
use tempfile::TempDir;

const HUNDRED_K: u64 = 100_000;
const MILLION: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Setup helper
// ---------------------------------------------------------------------------

/// Create a `trades` table and populate it with synthetic data.
///
/// Columns: timestamp (TIMESTAMP), symbol (SYMBOL/I32), price (F64),
///          volume (F64), side (I32).
///
/// - Prices follow a deterministic random walk around 50 000.
/// - Volumes are between 0.01 and 100.0.
/// - Symbols: SYM_000 .. SYM_{num_symbols-1} (stored as i32 ids).
/// - Timestamps are spread evenly across `partition_days` days starting
///   from 2024-01-01 00:00:00 UTC.
fn setup_trades_table(
    dir: &Path,
    num_rows: u64,
    num_symbols: u64,
    partition_days: u64,
) -> TableMeta {
    let partition_by = if partition_days <= 1 {
        PartitionBy::None
    } else {
        PartitionBy::Day
    };

    let meta = TableBuilder::new("trades")
        .column("timestamp", ColumnType::Timestamp)
        .column("symbol", ColumnType::I32)
        .column("price", ColumnType::F64)
        .column("volume", ColumnType::F64)
        .column("side", ColumnType::I32)
        .timestamp("timestamp")
        .partition_by(partition_by)
        .build(dir)
        .expect("failed to create trades table");

    let mut writer = TableWriter::open(dir, "trades").expect("failed to open writer");

    // Base timestamp: 2024-01-01T00:00:00 UTC
    let base_ts_secs: i64 = 1_704_067_200;
    let total_span_secs = partition_days * 86_400;
    let step_ns = if num_rows > 1 {
        (total_span_secs as i64 * 1_000_000_000) / (num_rows as i64)
    } else {
        1_000_000_000
    };

    let mut price = 50_000.0_f64;

    for i in 0..num_rows {
        let ts_nanos = base_ts_secs * 1_000_000_000 + i as i64 * step_ns;
        let ts = Timestamp(ts_nanos);

        // Deterministic pseudo-random walk for price.
        let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
        price += delta;
        if price < 1.0 {
            price = 1.0;
        }

        let symbol_id = (i % num_symbols) as i32;
        let volume = 0.01 + (i % 10_000) as f64 * 0.01;
        let side = (i % 2) as i32; // 0 = buy, 1 = sell

        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32(symbol_id),
                    ColumnValue::F64(price),
                    ColumnValue::F64(volume),
                    ColumnValue::I32(side),
                ],
            )
            .expect("write_row failed");
    }

    writer.flush().expect("flush failed");

    meta
}

/// Helper: execute a SQL string against a db root and return the result.
fn run_sql(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).expect("parse/plan failed");
    execute(db_root, &plan).expect("execute failed")
}

// ---------------------------------------------------------------------------
// 1. Insert throughput – 100K rows
// ---------------------------------------------------------------------------

fn insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_throughput");
    group.throughput(Throughput::Elements(HUNDRED_K));
    group.sample_size(10);

    group.bench_function("100K_rows_5cols", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                // Create table schema only (no data).
                let _meta = TableBuilder::new("trades")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("symbol", ColumnType::I32)
                    .column("price", ColumnType::F64)
                    .column("volume", ColumnType::F64)
                    .column("side", ColumnType::I32)
                    .timestamp("timestamp")
                    .partition_by(PartitionBy::Day)
                    .build(dir.path())
                    .unwrap();
                dir
            },
            |dir| {
                let mut writer = TableWriter::open(dir.path(), "trades").unwrap();
                let base_ts: i64 = 1_704_067_200 * 1_000_000_000;
                let mut price = 50_000.0_f64;

                for i in 0..HUNDRED_K {
                    let ts = Timestamp(base_ts + i as i64 * 1_000_000);
                    let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
                    price += delta;

                    writer
                        .write_row(
                            ts,
                            &[
                                ColumnValue::I32((i % 100) as i32),
                                ColumnValue::F64(black_box(price)),
                                ColumnValue::F64(black_box(0.01 + (i % 10_000) as f64 * 0.01)),
                                ColumnValue::I32((i % 2) as i32),
                            ],
                        )
                        .unwrap();
                }
                writer.flush().unwrap();
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. SELECT * (full scan) – 1M rows
// ---------------------------------------------------------------------------

fn select_full_scan(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("select_full_scan");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT * FROM trades");
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. SELECT with filter – 1M rows
// ---------------------------------------------------------------------------

fn select_with_filter(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("select_with_filter");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_price_gt_50000", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT * FROM trades WHERE price > 50000");
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. SELECT with GROUP BY – 1M rows, 100 symbols
// ---------------------------------------------------------------------------

fn select_with_group_by(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("select_with_group_by");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_100_symbols", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. SAMPLE BY – 1M rows spanning 24 hours, bucket = 1h
// ---------------------------------------------------------------------------

fn select_sample_by(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    // 1 day = 24 hours worth of data, no day-partitioning to keep it simple.
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("select_sample_by");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_1h_bucket", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT avg(price), sum(volume) FROM trades SAMPLE BY 1h",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. LATEST ON – 1M rows, 100 symbols
// ---------------------------------------------------------------------------

fn select_latest_on(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("select_latest_on");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_100_symbols", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. ORDER BY + LIMIT – 1M rows
// ---------------------------------------------------------------------------

fn select_order_by_limit(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("select_order_by_limit");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_top100_by_price", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM trades ORDER BY price DESC LIMIT 100",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 8. Multi-partition scan – 30 day-partitions (~33K rows each)
// ---------------------------------------------------------------------------

fn multi_partition_scan(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 30);

    let mut group = c.benchmark_group("multi_partition_scan");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("30_partitions_1M_rows", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT * FROM trades");
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 9. Aggregate functions – 1M rows
// ---------------------------------------------------------------------------

fn aggregate_functions(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("aggregate_functions");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("count_sum_avg_min_max", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 10. Parallel vs sequential scan – 30 partitions
// ---------------------------------------------------------------------------

fn parallel_vs_sequential(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades_table(dir.path(), MILLION, 100, 30);

    let mut group = c.benchmark_group("parallel_vs_sequential");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    // The default execute path uses parallel scanning when partition count
    // exceeds the threshold. We benchmark the same full-scan query and
    // compare with a single-threaded variant by using LIMIT to force
    // sequential path, then the full parallel path.

    group.bench_function("parallel_30_partitions", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT * FROM trades");
            black_box(result);
        });
    });

    // For the sequential comparison we use the parallel module directly
    // with num_threads = 1.
    group.bench_function("sequential_30_partitions", |b| {
        b.iter(|| {
            let table_dir = dir.path().join("trades");
            let meta = exchange_core::table::TableMeta::load(&table_dir.join("_meta")).unwrap();
            let selected_cols: Vec<(usize, String)> = meta
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (i, c.name.clone()))
                .collect();

            let rows = exchange_query::parallel::parallel_scan_partitions_with_threads(
                &table_dir,
                &meta,
                &selected_cols,
                None,
                Some(1),
            )
            .unwrap();
            black_box(rows);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    insert_throughput,
    select_full_scan,
    select_with_filter,
    select_with_group_by,
    select_sample_by,
    select_latest_on,
    select_order_by_limit,
    multi_partition_scan,
    aggregate_functions,
    parallel_vs_sequential,
);
criterion_main!(benches);
