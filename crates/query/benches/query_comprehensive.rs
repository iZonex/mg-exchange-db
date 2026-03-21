//! Comprehensive query engine benchmarks for ExchangeDB.
//!
//! Covers: SQL parse+plan time, filter pushdown, GROUP BY strategies,
//! TopK vs Sort+Limit, compiled vs interpreted filter, SAMPLE BY intervals,
//! ASOF JOIN, and SIMD aggregation through the query engine.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter};
use exchange_query::plan::{Filter, Value};
use exchange_query::{QueryResult, execute, plan_query};
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

const HUNDRED_K: u64 = 100_000;
const MILLION: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create and populate a `trades` table.
fn setup_trades(dir: &Path, num_rows: u64, num_symbols: u64, partition_days: u64) -> TableMeta {
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
        .expect("create table");

    let mut writer = TableWriter::open(dir, "trades").expect("open writer");

    let base_ts: i64 = 1_704_067_200_000_000_000;
    let total_span_ns = partition_days as i64 * 86_400_000_000_000;
    let step_ns = if num_rows > 1 {
        total_span_ns / num_rows as i64
    } else {
        1_000_000_000
    };

    let mut price = 50_000.0_f64;

    for i in 0..num_rows {
        let ts = Timestamp(base_ts + i as i64 * step_ns);
        let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
        price += delta;
        if price < 1.0 {
            price = 1.0;
        }

        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32((i % num_symbols) as i32),
                    ColumnValue::F64(price),
                    ColumnValue::F64(0.01 + (i % 10_000) as f64 * 0.01),
                    ColumnValue::I32((i % 2) as i32),
                ],
            )
            .expect("write_row");
    }
    writer.flush().expect("flush");
    meta
}

/// Create and populate a `quotes` table for ASOF JOIN benchmarks.
fn setup_quotes(dir: &Path, num_rows: u64, num_symbols: u64) -> TableMeta {
    let meta = TableBuilder::new("quotes")
        .column("timestamp", ColumnType::Timestamp)
        .column("symbol", ColumnType::I32)
        .column("bid", ColumnType::F64)
        .column("ask", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::None)
        .build(dir)
        .expect("create quotes table");

    let mut writer = TableWriter::open(dir, "quotes").expect("open writer");

    let base_ts: i64 = 1_704_067_200_000_000_000;
    // Quotes arrive roughly 5x more frequently than trades.
    let step_ns: i64 = 86_400_000_000_000 / num_rows as i64;

    for i in 0..num_rows {
        let ts = Timestamp(base_ts + i as i64 * step_ns);
        let mid = 50_000.0 + (i % 500) as f64 * 0.1;

        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32((i % num_symbols) as i32),
                    ColumnValue::F64(mid - 0.5),
                    ColumnValue::F64(mid + 0.5),
                ],
            )
            .expect("write_row");
    }
    writer.flush().expect("flush");
    meta
}

fn run_sql(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).expect("plan_query failed");
    execute(db_root, &plan).expect("execute failed")
}

// ---------------------------------------------------------------------------
// 1. SQL parse + plan + optimize time
// ---------------------------------------------------------------------------

fn bench_parse_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_plan");
    group.throughput(Throughput::Elements(1));

    // Simple SELECT.
    group.bench_function("simple_select", |b| {
        b.iter(|| {
            let plan = plan_query(black_box("SELECT * FROM trades")).unwrap();
            black_box(plan);
        });
    });

    // Medium complexity: filter + GROUP BY + aggregates.
    group.bench_function("medium_groupby", |b| {
        b.iter(|| {
            let plan = plan_query(black_box(
                "SELECT symbol, avg(price), sum(volume) FROM trades WHERE price > 50000 GROUP BY symbol",
            ))
            .unwrap();
            black_box(plan);
        });
    });

    // Complex: SAMPLE BY + ORDER BY + LIMIT.
    group.bench_function("complex_sample_by", |b| {
        b.iter(|| {
            let plan = plan_query(black_box(
                "SELECT avg(price), min(price), max(price), sum(volume) \
                 FROM trades \
                 WHERE symbol = 1 \
                 SAMPLE BY 1h \
                 ORDER BY avg(price) DESC \
                 LIMIT 100",
            ))
            .unwrap();
            black_box(plan);
        });
    });

    // LATEST ON.
    group.bench_function("latest_on", |b| {
        b.iter(|| {
            let plan = plan_query(black_box(
                "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
            ))
            .unwrap();
            black_box(plan);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Filter pushdown benefit
// ---------------------------------------------------------------------------

fn bench_filter_pushdown(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("filter_pushdown");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    // Query with filter (pushdown used internally).
    group.bench_function("with_filter", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT * FROM trades WHERE price > 51000");
            black_box(result);
        });
    });

    // Baseline: full scan without any filter.
    group.bench_function("full_scan_no_filter", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT * FROM trades");
            black_box(result);
        });
    });

    // Highly selective filter (should benefit most from pushdown).
    group.bench_function("highly_selective", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM trades WHERE price > 55000 AND side = 0",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. GROUP BY: varying group counts
// ---------------------------------------------------------------------------

fn bench_groupby_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("groupby_strategies");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    for num_groups in [10u64, 100, 1000] {
        let dir = TempDir::new().unwrap();
        setup_trades(dir.path(), MILLION, num_groups, 1);

        group.bench_with_input(
            BenchmarkId::new("groups", num_groups),
            &num_groups,
            |b, _| {
                b.iter(|| {
                    let result = run_sql(
                        dir.path(),
                        "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol",
                    );
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. TopK vs Sort + Limit
// ---------------------------------------------------------------------------

fn bench_topk(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("topk_vs_sort_limit");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    // Small LIMIT (TopK optimization should kick in).
    group.bench_function("top_10", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM trades ORDER BY price DESC LIMIT 10",
            );
            black_box(result);
        });
    });

    // Medium LIMIT.
    group.bench_function("top_100", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM trades ORDER BY price DESC LIMIT 100",
            );
            black_box(result);
        });
    });

    // Large LIMIT (full sort essentially).
    group.bench_function("top_10000", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM trades ORDER BY price DESC LIMIT 10000",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Compiled filter vs interpreted filter
// ---------------------------------------------------------------------------

fn bench_compiled_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("compiled_filter");
    group.throughput(Throughput::Elements(MILLION));

    // Build column index map.
    let column_indices: HashMap<String, usize> = [
        ("timestamp", 0),
        ("symbol", 1),
        ("price", 2),
        ("volume", 3),
        ("side", 4),
    ]
    .iter()
    .map(|&(k, v)| (k.to_string(), v))
    .collect();

    // Generate test rows.
    let rows: Vec<Vec<Value>> = (0..MILLION)
        .map(|i| {
            vec![
                Value::Timestamp(1_704_067_200_000_000_000 + i as i64 * 1_000_000),
                Value::I64((i % 100) as i64),
                Value::F64(50_000.0 + (i % 5000) as f64 * 0.1),
                Value::F64(0.01 + (i % 10_000) as f64 * 0.01),
                Value::I64((i % 2) as i64),
            ]
        })
        .collect();

    // Simple filter: price > 50100.
    let simple_filter = Filter::Gt("price".to_string(), Value::F64(50_100.0));
    let compiled_simple =
        exchange_query::compiled_filter::compile_filter(&simple_filter, &column_indices);

    group.bench_function("compiled_simple_filter", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &rows {
                if compiled_simple(black_box(row)) {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    // Interpreted simple filter (manually apply Filter tree).
    group.bench_function("interpreted_simple_filter", |b| {
        b.iter(|| {
            let mut count = 0u64;
            let threshold = 50_100.0_f64;
            for row in &rows {
                if let Value::F64(p) = &row[2] {
                    if *p > threshold {
                        count += 1;
                    }
                }
            }
            black_box(count);
        });
    });

    // Complex filter: price > 50100 AND side = 0.
    let complex_filter = Filter::And(vec![
        Filter::Gt("price".to_string(), Value::F64(50_100.0)),
        Filter::Eq("side".to_string(), Value::I64(0)),
    ]);
    let compiled_complex =
        exchange_query::compiled_filter::compile_filter(&complex_filter, &column_indices);

    group.bench_function("compiled_complex_filter", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &rows {
                if compiled_complex(black_box(row)) {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    group.bench_function("interpreted_complex_filter", |b| {
        b.iter(|| {
            let mut count = 0u64;
            let threshold = 50_100.0_f64;
            for row in &rows {
                let price_ok = if let Value::F64(p) = &row[2] {
                    *p > threshold
                } else {
                    false
                };
                let side_ok = if let Value::I64(s) = &row[4] {
                    *s == 0
                } else {
                    false
                };
                if price_ok && side_ok {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    // LinearFilter benchmarks (stack-machine bytecode with type-specialized ops).
    let linear_simple =
        exchange_query::compiled_filter::build_linear_filter(&simple_filter, &column_indices);

    group.bench_function("linear_simple_filter", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &rows {
                if linear_simple.evaluate(black_box(row)) {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    let linear_complex =
        exchange_query::compiled_filter::build_linear_filter(&complex_filter, &column_indices);

    group.bench_function("linear_complex_filter", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for row in &rows {
                if linear_complex.evaluate(black_box(row)) {
                    count += 1;
                }
            }
            black_box(count);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. SAMPLE BY with different intervals
// ---------------------------------------------------------------------------

fn bench_sample_by(c: &mut Criterion) {
    // 1M rows spanning 30 days.
    let dir = TempDir::new().unwrap();
    setup_trades(dir.path(), MILLION, 100, 30);

    let mut group = c.benchmark_group("sample_by_intervals");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    for interval in ["1m", "1h", "1d"] {
        let sql = format!("SELECT avg(price), sum(volume) FROM trades SAMPLE BY {interval}");

        group.bench_with_input(BenchmarkId::new("interval", interval), &interval, |b, _| {
            b.iter(|| {
                let result = run_sql(dir.path(), &sql);
                black_box(result);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. ASOF JOIN
// ---------------------------------------------------------------------------

fn bench_asof_join(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    // 100K trades, 500K quotes, 100 symbols.
    setup_trades(dir.path(), HUNDRED_K, 100, 1);
    setup_quotes(dir.path(), 500_000, 100);

    let mut group = c.benchmark_group("asof_join");
    group.throughput(Throughput::Elements(HUNDRED_K));
    group.sample_size(10);

    group.bench_function("100K_trades_500K_quotes", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT t.symbol, t.price, q.bid, q.ask \
                 FROM trades t \
                 ASOF JOIN quotes q ON t.symbol = q.symbol",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 8. SIMD aggregation through the query engine
// ---------------------------------------------------------------------------

fn bench_simd_aggregation(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades(dir.path(), MILLION, 100, 1);

    let mut group = c.benchmark_group("query_aggregation");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    // Single aggregate: sum.
    group.bench_function("sum_1M", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT sum(price) FROM trades");
            black_box(result);
        });
    });

    // Multiple aggregates in one query.
    group.bench_function("multi_agg_1M", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT count(*), sum(price), avg(price), min(price), max(price) FROM trades",
            );
            black_box(result);
        });
    });

    // count(*) only.
    group.bench_function("count_1M", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT count(*) FROM trades");
            black_box(result);
        });
    });

    // min/max.
    group.bench_function("min_max_1M", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), "SELECT min(price), max(price) FROM trades");
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 9. Multi-partition GROUP BY
// ---------------------------------------------------------------------------

fn bench_multi_partition_groupby(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    setup_trades(dir.path(), MILLION, 100, 30);

    let mut group = c.benchmark_group("multi_partition_groupby");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("30_partitions_100_groups", |b| {
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
// 10. LATEST ON at scale
// ---------------------------------------------------------------------------

fn bench_latest_on_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("latest_on_scale");
    group.sample_size(10);

    for (num_rows, num_symbols) in [(HUNDRED_K, 100u64), (MILLION, 1000)] {
        let dir = TempDir::new().unwrap();
        setup_trades(dir.path(), num_rows, num_symbols, 1);

        group.throughput(Throughput::Elements(num_rows));

        group.bench_with_input(
            BenchmarkId::new("rows_symbols", format!("{num_rows}_{num_symbols}")),
            &(num_rows, num_symbols),
            |b, _| {
                b.iter(|| {
                    let result = run_sql(
                        dir.path(),
                        "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
                    );
                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_parse_plan,
    bench_filter_pushdown,
    bench_groupby_strategies,
    bench_topk,
    bench_compiled_filter,
    bench_sample_by,
    bench_asof_join,
    bench_simd_aggregation,
    bench_multi_partition_groupby,
    bench_latest_on_scale,
);
criterion_main!(benches);
