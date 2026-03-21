//! TSBS (Time Series Benchmark Suite) style benchmarks for ExchangeDB.
//!
//! Simulates the TSBS devops scenario: CPU metrics for N hosts, then runs
//! the standard TSBS query types (last-point, max-cpu-12h, double-groupby,
//! high-cpu, groupby-orderby-limit).

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::table::{ColumnValue, TableBuilder, TableMeta, TableWriter};
use exchange_query::{QueryResult, execute, plan_query};
use std::path::Path;
use tempfile::TempDir;

const HUNDRED_K: u64 = 100_000;
const MILLION: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create the TSBS `cpu` table schema (modelling the devops CPU scenario).
///
/// Columns:
/// - `timestamp` (TIMESTAMP) -- designated timestamp
/// - `hostname`  (I32)       -- host ID (0..num_hosts-1)
/// - `region`    (I32)       -- region ID (0..4)
/// - `usage_user`   (F64)
/// - `usage_system`  (F64)
/// - `usage_idle`    (F64)
/// - `usage_iowait`  (F64)
/// - `usage_irq`     (F64)
/// - `usage_softirq` (F64)
/// - `usage_steal`   (F64)
fn create_cpu_table(dir: &Path) -> TableMeta {
    TableBuilder::new("cpu")
        .column("timestamp", ColumnType::Timestamp)
        .column("hostname", ColumnType::I32)
        .column("region", ColumnType::I32)
        .column("usage_user", ColumnType::F64)
        .column("usage_system", ColumnType::F64)
        .column("usage_idle", ColumnType::F64)
        .column("usage_iowait", ColumnType::F64)
        .column("usage_irq", ColumnType::F64)
        .column("usage_softirq", ColumnType::F64)
        .column("usage_steal", ColumnType::F64)
        .timestamp("timestamp")
        .partition_by(PartitionBy::Day)
        .build(dir)
        .expect("failed to create cpu table")
}

/// Insert `num_rows` TSBS-style CPU metric rows for `num_hosts` hosts.
///
/// Metrics are deterministic pseudo-random values derived from the row index
/// so benchmarks are reproducible.
fn insert_cpu_metrics(dir: &Path, num_hosts: u64, num_rows: u64) {
    let mut writer = TableWriter::open(dir, "cpu").expect("open writer");

    // Base: 2024-01-01 00:00:00 UTC in nanos
    let base_ts: i64 = 1_704_067_200_000_000_000;
    // 10-second reporting interval
    let step_ns: i64 = 10_000_000_000;

    for i in 0..num_rows {
        let ts = Timestamp(base_ts + (i as i64) * step_ns);
        let host_id = (i % num_hosts) as i32;
        let region_id = (host_id % 5) as i32;

        // Deterministic pseudo-random metrics in [0, 100].
        let seed = i.wrapping_mul(2654435761);
        let usage_user = (seed % 10000) as f64 / 100.0;
        let usage_system = ((seed >> 4) % 5000) as f64 / 100.0;
        let usage_idle = 100.0 - usage_user - usage_system;
        let usage_iowait = ((seed >> 8) % 1000) as f64 / 100.0;
        let usage_irq = ((seed >> 12) % 500) as f64 / 100.0;
        let usage_softirq = ((seed >> 16) % 300) as f64 / 100.0;
        let usage_steal = ((seed >> 20) % 200) as f64 / 100.0;

        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32(host_id),
                    ColumnValue::I32(region_id),
                    ColumnValue::F64(usage_user),
                    ColumnValue::F64(usage_system),
                    ColumnValue::F64(usage_idle),
                    ColumnValue::F64(usage_iowait),
                    ColumnValue::F64(usage_irq),
                    ColumnValue::F64(usage_softirq),
                    ColumnValue::F64(usage_steal),
                ],
            )
            .expect("write_row failed");
    }
    writer.flush().expect("flush failed");
}

/// Setup a TSBS CPU table with the given number of hosts, returning
/// the temp directory (whose lifetime keeps the data alive).
fn setup_tsbs_table(num_hosts: u64, num_rows: u64) -> TempDir {
    let dir = TempDir::new().unwrap();
    let _meta = create_cpu_table(dir.path());
    insert_cpu_metrics(dir.path(), num_hosts, num_rows);
    dir
}

/// Run SQL through the full pipeline.
fn run_sql(db_root: &Path, sql: &str) -> QueryResult {
    let plan = plan_query(sql).expect("plan_query failed");
    execute(db_root, &plan).expect("execute failed")
}

// ---------------------------------------------------------------------------
// 1. TSBS Insert: CPU metrics for N hosts
// ---------------------------------------------------------------------------

fn tsbs_insert_cpu(c: &mut Criterion) {
    let mut group = c.benchmark_group("tsbs_insert");
    group.sample_size(10);

    for num_hosts in [10u64, 100, 1000] {
        group.throughput(Throughput::Elements(HUNDRED_K));
        group.bench_with_input(
            BenchmarkId::new("cpu", num_hosts),
            &num_hosts,
            |b, &hosts| {
                b.iter_with_setup(
                    || {
                        let dir = TempDir::new().unwrap();
                        let _meta = create_cpu_table(dir.path());
                        dir
                    },
                    |dir| {
                        insert_cpu_metrics(dir.path(), hosts, HUNDRED_K);
                    },
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. TSBS Query: Last point per host
//    Equivalent: SELECT * FROM cpu LATEST ON timestamp PARTITION BY hostname
// ---------------------------------------------------------------------------

fn tsbs_query_lastpoint(c: &mut Criterion) {
    let dir = setup_tsbs_table(100, MILLION);

    let mut group = c.benchmark_group("tsbs_query_lastpoint");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_100_hosts", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM cpu LATEST ON timestamp PARTITION BY hostname",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. TSBS Query: Max CPU over 12 hours
//    SELECT hostname, max(usage_user) FROM cpu
//      WHERE timestamp BETWEEN 1704067200000000000 AND 1704110400000000000
//      GROUP BY hostname
// ---------------------------------------------------------------------------

fn tsbs_query_max_cpu_12h(c: &mut Criterion) {
    let dir = setup_tsbs_table(100, MILLION);

    let mut group = c.benchmark_group("tsbs_query_max_cpu_12h");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    // 12 hours = 43200 seconds from the base timestamp.
    let ts_start: i64 = 1_704_067_200_000_000_000;
    let ts_end: i64 = ts_start + 43_200_000_000_000;

    let sql = format!(
        "SELECT hostname, max(usage_user) FROM cpu \
         WHERE timestamp BETWEEN {ts_start} AND {ts_end} \
         GROUP BY hostname"
    );

    group.bench_function("1M_rows_100_hosts", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), &sql);
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. TSBS Query: Double GroupBy
//    SELECT hostname, avg(usage_user) FROM cpu
//      WHERE timestamp > X GROUP BY hostname SAMPLE BY 1h
// ---------------------------------------------------------------------------

fn tsbs_query_double_groupby(c: &mut Criterion) {
    let dir = setup_tsbs_table(100, MILLION);

    let mut group = c.benchmark_group("tsbs_query_double_groupby");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    let ts_start: i64 = 1_704_067_200_000_000_000;
    let sql = format!(
        "SELECT avg(usage_user) FROM cpu \
         WHERE timestamp > {ts_start} \
         SAMPLE BY 1h"
    );

    group.bench_function("1M_rows_100_hosts", |b| {
        b.iter(|| {
            let result = run_sql(dir.path(), &sql);
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. TSBS Query: High CPU
//    SELECT * FROM cpu WHERE usage_user > 90 ORDER BY timestamp DESC LIMIT 10
// ---------------------------------------------------------------------------

fn tsbs_query_high_cpu(c: &mut Criterion) {
    let dir = setup_tsbs_table(100, MILLION);

    let mut group = c.benchmark_group("tsbs_query_high_cpu");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_top10", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT * FROM cpu WHERE usage_user > 90 ORDER BY timestamp DESC LIMIT 10",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 6. TSBS Query: GroupBy OrderBy Limit
//    SELECT hostname, max(usage_user) FROM cpu
//      GROUP BY hostname ORDER BY max(usage_user) DESC LIMIT 5
// ---------------------------------------------------------------------------

fn tsbs_query_groupby_orderby_limit(c: &mut Criterion) {
    let dir = setup_tsbs_table(100, MILLION);

    let mut group = c.benchmark_group("tsbs_query_groupby_orderby_limit");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows_top5_hosts", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT hostname, max(usage_user) FROM cpu \
                 GROUP BY hostname \
                 ORDER BY max(usage_user) DESC LIMIT 5",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 7. TSBS Query: Aggregate across all hosts
//    SELECT count(*), avg(usage_user), max(usage_user), min(usage_idle) FROM cpu
// ---------------------------------------------------------------------------

fn tsbs_query_aggregate_all(c: &mut Criterion) {
    let dir = setup_tsbs_table(100, MILLION);

    let mut group = c.benchmark_group("tsbs_query_aggregate_all");
    group.throughput(Throughput::Elements(MILLION));
    group.sample_size(10);

    group.bench_function("1M_rows", |b| {
        b.iter(|| {
            let result = run_sql(
                dir.path(),
                "SELECT count(*), avg(usage_user), max(usage_user), min(usage_idle) FROM cpu",
            );
            black_box(result);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    tsbs_insert_cpu,
    tsbs_query_lastpoint,
    tsbs_query_max_cpu_12h,
    tsbs_query_double_groupby,
    tsbs_query_high_cpu,
    tsbs_query_groupby_orderby_limit,
    tsbs_query_aggregate_all,
);
criterion_main!(benches);
