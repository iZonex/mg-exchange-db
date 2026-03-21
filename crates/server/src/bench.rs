//! TSBS-style benchmark harness for ExchangeDB.
//!
//! Measures insert throughput and query latency against realistic exchange
//! data (trades table). Runs as a standalone binary:
//!
//! ```sh
//! cargo run --release --bin exchangedb-bench
//! ```

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::engine::Engine;
use exchange_core::table::{ColumnValue, TableBuilder};
use exchange_query::{execute, plan_query, QueryResult};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const SYMBOLS: &[&str] = &[
    "BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD", "XRP/USD",
    "ADA/USD", "AVAX/USD", "DOT/USD", "MATIC/USD", "LINK/USD",
];

const HUNDRED_K: u64 = 100_000;
const MILLION: u64 = 1_000_000;

const CONCURRENT_READERS: usize = 4;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

struct BenchResult {
    operation: String,
    rows: u64,
    duration: Duration,
}

impl BenchResult {
    fn throughput(&self) -> f64 {
        if self.duration.as_secs_f64() > 0.0 {
            self.rows as f64 / self.duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

// ---------------------------------------------------------------------------
// Table setup & data generation
// ---------------------------------------------------------------------------

fn create_trades_table(engine: &Engine) {
    let _ = engine.create_table(
        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("symbol", ColumnType::I32)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .column("side", ColumnType::I32)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day),
    );
}

/// Insert `num_rows` rows into the trades table via the Engine writer.
/// Returns the duration of the insert operation.
fn insert_rows(engine: &Engine, num_rows: u64, base_ts: i64) -> Duration {
    let start = Instant::now();

    let mut handle = engine.get_writer("trades").expect("get writer");
    let writer = handle.writer();

    let mut price = 50_000.0_f64;
    let num_symbols = SYMBOLS.len() as u64;

    for i in 0..num_rows {
        let ts = Timestamp(base_ts + i as i64 * 1_000_000); // 1ms apart
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
                    ColumnValue::I32((i % 2) as i32), // 0 = buy, 1 = sell
                ],
            )
            .expect("write_row");
    }
    writer.flush().expect("flush");
    drop(handle);

    start.elapsed()
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

fn run_query(db_root: &Path, sql: &str) -> (Duration, u64) {
    let plan = plan_query(sql).expect("plan_query");
    let start = Instant::now();
    let result = execute(db_root, &plan).expect("execute");
    let elapsed = start.elapsed();

    let row_count = match &result {
        QueryResult::Rows { rows, .. } => rows.len() as u64,
        QueryResult::Ok { affected_rows } => *affected_rows as u64,
    };

    (elapsed, row_count)
}

/// Run a query multiple times and return the median duration.
fn run_query_n(db_root: &Path, sql: &str, iterations: u32) -> (Duration, u64) {
    let mut durations = Vec::with_capacity(iterations as usize);
    let mut last_rows = 0u64;

    for _ in 0..iterations {
        let (d, rows) = run_query(db_root, sql);
        durations.push(d);
        last_rows = rows;
    }

    durations.sort();
    let median = durations[durations.len() / 2];
    (median, last_rows)
}

// ---------------------------------------------------------------------------
// Benchmark suites
// ---------------------------------------------------------------------------

fn bench_inserts(results: &mut Vec<BenchResult>) {
    println!("\n=== INSERT BENCHMARKS ===\n");

    for &num_rows in &[HUNDRED_K, MILLION] {
        // Create a fresh temp dir for each size to avoid partition accumulation.
        let dir = tempfile::tempdir().expect("tempdir");
        let eng = Engine::open(dir.path()).expect("engine open");
        create_trades_table(&eng);

        let base_ts: i64 = 1_704_067_200_000_000_000; // 2024-01-01T00:00:00Z
        let duration = insert_rows(&eng, num_rows, base_ts);

        let label = format!("INSERT {} rows", format_count(num_rows));
        results.push(BenchResult {
            operation: label,
            rows: num_rows,
            duration,
        });
    }
}

fn bench_queries(db_root: &Path, num_rows: u64, results: &mut Vec<BenchResult>) {
    println!("\n=== QUERY BENCHMARKS ({} rows) ===\n", format_count(num_rows));

    let iterations = 5;
    let base_ts: i64 = 1_704_067_200_000_000_000;

    // 1. Point query: specific symbol + timestamp
    {
        // Pick a timestamp in the middle of the dataset.
        let target_ts = base_ts + (num_rows / 2) as i64 * 1_000_000;
        let sql = format!(
            "SELECT * FROM trades WHERE symbol = 0 AND timestamp = {target_ts}"
        );
        let (duration, rows) = run_query_n(db_root, &sql, iterations);
        results.push(BenchResult {
            operation: "Point query (symbol + timestamp)".to_string(),
            rows,
            duration,
        });
    }

    // 2. Range scan: 1 hour window
    {
        let range_start = base_ts + (num_rows / 4) as i64 * 1_000_000;
        let range_end = range_start + 3_600_000_000_000; // +1 hour in ns
        let sql = format!(
            "SELECT * FROM trades WHERE timestamp >= {range_start} AND timestamp < {range_end}"
        );
        let (duration, rows) = run_query_n(db_root, &sql, iterations);
        results.push(BenchResult {
            operation: "Range scan (1h window)".to_string(),
            rows,
            duration,
        });
    }

    // 3. Aggregation: avg(price), sum(volume) GROUP BY symbol
    {
        let sql = "SELECT symbol, avg(price), sum(volume) FROM trades GROUP BY symbol";
        let (duration, rows) = run_query_n(db_root, sql, iterations);
        results.push(BenchResult {
            operation: "Aggregation (avg/sum GROUP BY)".to_string(),
            rows,
            duration,
        });
    }

    // 4. VWAP: vwap(price, volume) GROUP BY symbol
    //    vwap is registered as vwap(sum_pv, sum_v), so we compute it manually
    //    or use the scalar: SELECT symbol, sum(price * volume) / sum(volume) ...
    {
        let sql = "SELECT symbol, sum(price * volume) / sum(volume) as vwap FROM trades GROUP BY symbol";
        let (duration, rows) = run_query_n(db_root, sql, iterations);
        results.push(BenchResult {
            operation: "VWAP (price*volume/volume)".to_string(),
            rows,
            duration,
        });
    }

    // 5. Time bucket: SAMPLE BY 1h
    {
        let sql = "SELECT symbol, avg(price) FROM trades SAMPLE BY 1h";
        let (duration, rows) = run_query_n(db_root, sql, iterations);
        results.push(BenchResult {
            operation: "Time bucket (SAMPLE BY 1h)".to_string(),
            rows,
            duration,
        });
    }

    // 6. LATEST ON: most recent row per symbol
    {
        let sql = "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol";
        let (duration, rows) = run_query_n(db_root, sql, iterations);
        results.push(BenchResult {
            operation: "LATEST ON (per symbol)".to_string(),
            rows,
            duration,
        });
    }
}

fn bench_concurrent(db_root_path: PathBuf, num_rows: u64, results: &mut Vec<BenchResult>) {
    println!(
        "\n=== CONCURRENT BENCHMARK ({} readers + 1 writer, {} rows) ===\n",
        CONCURRENT_READERS,
        format_count(num_rows),
    );

    let engine = Arc::new(Engine::open(&db_root_path).expect("engine open"));
    let barrier = Arc::new(Barrier::new(CONCURRENT_READERS + 1 + 1)); // readers + writer + main

    let start = Instant::now();

    // Spawn reader threads
    let reader_handles: Vec<_> = (0..CONCURRENT_READERS)
        .map(|i| {
            let db_root = db_root_path.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let queries = [
                    "SELECT symbol, avg(price) FROM trades GROUP BY symbol",
                    "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol",
                    "SELECT symbol, sum(volume) FROM trades GROUP BY symbol",
                ];
                let sql = queries[i % queries.len()];
                let mut total_rows = 0u64;
                for _ in 0..3 {
                    let (_, rows) = run_query(&db_root, sql);
                    total_rows += rows;
                }
                total_rows
            })
        })
        .collect();

    // Spawn writer thread
    let writer_engine = Arc::clone(&engine);
    let writer_barrier = Arc::clone(&barrier);
    let write_rows = num_rows / 10; // write 10% more rows
    let writer_handle = std::thread::spawn(move || {
        writer_barrier.wait();
        let base_ts: i64 = 1_704_067_200_000_000_000 + num_rows as i64 * 1_000_000;
        insert_rows(&writer_engine, write_rows, base_ts)
    });

    // Release all threads
    barrier.wait();

    // Wait for all threads
    let mut total_read_rows = 0u64;
    for h in reader_handles {
        total_read_rows += h.join().expect("reader thread");
    }
    let write_duration = writer_handle.join().expect("writer thread");
    let total_duration = start.elapsed();

    results.push(BenchResult {
        operation: format!(
            "Concurrent: {} readers query while 1 writer inserts {}",
            CONCURRENT_READERS,
            format_count(write_rows),
        ),
        rows: total_read_rows + write_rows,
        duration: total_duration,
    });

    results.push(BenchResult {
        operation: format!(
            "  Writer throughput (under contention, {} rows)",
            format_count(write_rows),
        ),
        rows: write_rows,
        duration: write_duration,
    });
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

fn format_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        format!("{n}")
    }
}

fn format_duration(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1_000.0;
    if ms < 1.0 {
        format!("{:.3} ms", ms)
    } else if ms < 1_000.0 {
        format!("{:.1} ms", ms)
    } else {
        format!("{:.2} s", ms / 1_000.0)
    }
}

fn format_throughput(rows_per_sec: f64) -> String {
    if rows_per_sec >= 1_000_000.0 {
        format!("{:.2}M rows/s", rows_per_sec / 1_000_000.0)
    } else if rows_per_sec >= 1_000.0 {
        format!("{:.1}K rows/s", rows_per_sec / 1_000.0)
    } else {
        format!("{:.0} rows/s", rows_per_sec)
    }
}

fn print_results(results: &[BenchResult]) {
    println!();
    println!(
        "{:<58} {:>10} {:>14} {:>18}",
        "Operation", "Rows", "Duration", "Throughput"
    );
    println!("{}", "-".repeat(104));

    for r in results {
        println!(
            "{:<58} {:>10} {:>14} {:>18}",
            r.operation,
            format_count(r.rows),
            format_duration(r.duration),
            format_throughput(r.throughput()),
        );
    }

    println!("{}", "-".repeat(104));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!("ExchangeDB TSBS Benchmark Harness");
    println!("==================================\n");

    let mut results = Vec::new();

    // --- INSERT benchmarks (isolated temp dirs) ---
    bench_inserts(&mut results);

    // --- QUERY benchmarks with 100K rows ---
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Engine::open(dir.path()).expect("engine open");
        create_trades_table(&engine);

        let base_ts: i64 = 1_704_067_200_000_000_000;
        let _ = insert_rows(&engine, HUNDRED_K, base_ts);

        bench_queries(dir.path(), HUNDRED_K, &mut results);
    }

    // --- QUERY benchmarks with 1M rows ---
    {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Engine::open(dir.path()).expect("engine open");
        create_trades_table(&engine);

        let base_ts: i64 = 1_704_067_200_000_000_000;
        let _ = insert_rows(&engine, MILLION, base_ts);

        bench_queries(dir.path(), MILLION, &mut results);

        // --- CONCURRENT benchmark (reuse the 1M table) ---
        bench_concurrent(dir.path().to_path_buf(), MILLION, &mut results);
    }

    // --- Print final report ---
    print_results(&results);
}
