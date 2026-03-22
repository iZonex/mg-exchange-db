//! Load testing binary for ExchangeDB.
//!
//! Measures write throughput and read latency under concurrent load.
//!
//! ```sh
//! cargo run --release --bin exchangedb-loadtest
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::engine::Engine;
use exchange_core::table::{ColumnValue, TableBuilder};
use exchange_query::{execute, plan_query};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const WRITER_THREADS: usize = 4;
const READER_THREADS: usize = 4;
const DEFAULT_DURATION_SECS: u64 = 10;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_ohlcv_table(engine: &Engine) {
    let _ = engine.create_table(
        TableBuilder::new("ohlcv")
            .column("timestamp", ColumnType::Timestamp)
            .column("symbol", ColumnType::I32)
            .column("open", ColumnType::F64)
            .column("high", ColumnType::F64)
            .column("low", ColumnType::F64)
            .column("close", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day),
    );
}

// ---------------------------------------------------------------------------
// Statistics collection
// ---------------------------------------------------------------------------

struct LatencyCollector {
    samples: Vec<Duration>,
}

impl LatencyCollector {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(10_000),
        }
    }

    fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    fn percentile(&mut self, p: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        self.samples.sort();
        let idx = ((p / 100.0) * (self.samples.len() as f64 - 1.0)).round() as usize;
        self.samples[idx.min(self.samples.len() - 1)]
    }

    fn count(&self) -> usize {
        self.samples.len()
    }
}

// ---------------------------------------------------------------------------
// Writer thread
// ---------------------------------------------------------------------------

fn writer_thread(
    engine: Arc<Engine>,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    thread_id: usize,
    rows_written: Arc<AtomicU64>,
    write_errors: Arc<AtomicU64>,
) {
    barrier.wait();

    let base_ts: i64 = 1_704_067_200_000_000_000 // 2024-01-01T00:00:00Z
        + (thread_id as i64) * 100_000_000_000_000_000; // offset per thread to avoid partition contention
    let mut i: u64 = 0;
    let mut price = 50_000.0_f64;
    let batch_size = 100;

    while !stop.load(Ordering::Relaxed) {
        match engine.get_writer("ohlcv") {
            Ok(mut handle) => {
                let writer = handle.writer();
                let mut batch_ok = true;
                for _ in 0..batch_size {
                    let ts = Timestamp(base_ts + i as i64 * 1_000_000); // 1ms apart
                    let delta = ((i.wrapping_mul(7).wrapping_add(3)) % 11) as f64 * 0.5 - 2.5;
                    price += delta;
                    if price < 1.0 {
                        price = 1.0;
                    }
                    let high = price + 10.0;
                    let low = price - 10.0;

                    let result = writer.write_row(
                        ts,
                        &[
                            ColumnValue::I32((i % 10) as i32),
                            ColumnValue::F64(price),
                            ColumnValue::F64(high),
                            ColumnValue::F64(low),
                            ColumnValue::F64(price + 0.5),
                            ColumnValue::F64(100.0 + (i % 1000) as f64),
                        ],
                    );
                    match result {
                        Ok(_) => {
                            rows_written.fetch_add(1, Ordering::Relaxed);
                            i += 1;
                        }
                        Err(_) => {
                            write_errors.fetch_add(1, Ordering::Relaxed);
                            batch_ok = false;
                            break;
                        }
                    }
                }
                if batch_ok && writer.flush().is_err() {
                    write_errors.fetch_add(1, Ordering::Relaxed);
                }
                drop(handle);
            }
            Err(_) => {
                write_errors.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reader thread
// ---------------------------------------------------------------------------

fn reader_thread(
    db_root: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    thread_id: usize,
) -> (LatencyCollector, u64) {
    barrier.wait();

    // Give writers a moment to seed data before querying.
    std::thread::sleep(Duration::from_millis(200));

    let queries = [
        "SELECT symbol, avg(open), avg(close) FROM ohlcv GROUP BY symbol",
        "SELECT symbol, sum(volume) FROM ohlcv GROUP BY symbol",
        "SELECT * FROM ohlcv LATEST ON timestamp PARTITION BY symbol",
    ];
    let sql = queries[thread_id % queries.len()];

    let mut latencies = LatencyCollector::new();
    let mut errors = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let plan = match plan_query(sql) {
            Ok(p) => p,
            Err(_) => {
                errors += 1;
                continue;
            }
        };
        let start = Instant::now();
        match execute(&db_root, &plan) {
            Ok(_) => {
                latencies.record(start.elapsed());
            }
            Err(_) => {
                errors += 1;
            }
        }
    }

    (latencies, errors)
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let duration_secs = std::env::var("LOADTEST_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);

    println!("ExchangeDB Load Test");
    println!("=====================");
    println!(
        "Configuration: {} writer threads, {} reader threads, {} second duration",
        WRITER_THREADS, READER_THREADS, duration_secs
    );
    println!();

    // Create a temp data directory.
    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let db_root = dir.path();

    // Create engine and test table.
    let engine = Arc::new(Engine::open(db_root).expect("failed to open engine"));
    create_ohlcv_table(&engine);

    // Seed some initial data so readers have something to query.
    {
        let mut handle = engine.get_writer("ohlcv").expect("get writer for seed");
        let writer = handle.writer();
        let base_ts: i64 = 1_704_067_200_000_000_000;
        for i in 0..1000_u64 {
            let ts = Timestamp(base_ts + i as i64 * 1_000_000);
            writer
                .write_row(
                    ts,
                    &[
                        ColumnValue::I32((i % 10) as i32),
                        ColumnValue::F64(50000.0 + i as f64),
                        ColumnValue::F64(50010.0 + i as f64),
                        ColumnValue::F64(49990.0 + i as f64),
                        ColumnValue::F64(50000.5 + i as f64),
                        ColumnValue::F64(100.0 + i as f64),
                    ],
                )
                .expect("seed write_row");
        }
        writer.flush().expect("seed flush");
        drop(handle);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let total_threads = WRITER_THREADS + READER_THREADS;
    let barrier = Arc::new(Barrier::new(total_threads + 1)); // +1 for main
    let rows_written = Arc::new(AtomicU64::new(0));
    let write_errors = Arc::new(AtomicU64::new(0));

    // Spawn writer threads.
    let mut writer_handles = Vec::with_capacity(WRITER_THREADS);
    for tid in 0..WRITER_THREADS {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let rows_written = Arc::clone(&rows_written);
        let write_errors = Arc::clone(&write_errors);
        writer_handles.push(std::thread::spawn(move || {
            writer_thread(engine, stop, barrier, tid, rows_written, write_errors);
        }));
    }

    // Spawn reader threads.
    let mut reader_handles = Vec::with_capacity(READER_THREADS);
    for tid in 0..READER_THREADS {
        let db_root_path = db_root.to_path_buf();
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        reader_handles.push(std::thread::spawn(move || {
            reader_thread(db_root_path, stop, barrier, tid)
        }));
    }

    // Release all threads.
    let start = Instant::now();
    barrier.wait();

    println!("Running load test for {} seconds...", duration_secs);
    std::thread::sleep(Duration::from_secs(duration_secs));

    // Signal stop.
    stop.store(true, Ordering::Relaxed);
    let elapsed = start.elapsed();

    // Join all threads.
    for h in writer_handles {
        h.join().expect("writer thread panicked");
    }

    let mut all_read_latencies = LatencyCollector::new();
    let mut total_read_errors = 0u64;
    for h in reader_handles {
        let (mut latencies, errors) = h.join().expect("reader thread panicked");
        all_read_latencies.samples.append(&mut latencies.samples);
        total_read_errors += errors;
    }

    // Collect results.
    let total_writes = rows_written.load(Ordering::Relaxed);
    let total_write_errors = write_errors.load(Ordering::Relaxed);
    let total_reads = all_read_latencies.count();
    let write_throughput = total_writes as f64 / elapsed.as_secs_f64();
    let read_p50 = all_read_latencies.percentile(50.0);
    let read_p99 = all_read_latencies.percentile(99.0);

    let total_ops = total_writes + total_reads as u64;
    let write_error_rate = if total_writes + total_write_errors > 0 {
        total_write_errors as f64 / (total_writes + total_write_errors) as f64 * 100.0
    } else {
        0.0
    };
    let read_error_rate = if total_reads as u64 + total_read_errors > 0 {
        total_read_errors as f64 / (total_reads as u64 + total_read_errors) as f64 * 100.0
    } else {
        0.0
    };

    // Print results.
    println!();
    println!("=== LOAD TEST RESULTS ===");
    println!();
    println!("Duration:           {}", format_duration(elapsed));
    println!();
    println!("--- Writes ---");
    println!("Total rows written: {}", total_writes);
    println!(
        "Write throughput:   {}",
        format_throughput(write_throughput)
    );
    println!(
        "Write errors:       {} ({:.2}%)",
        total_write_errors, write_error_rate
    );
    println!();
    println!("--- Reads ---");
    println!("Total queries:      {}", total_reads);
    println!(
        "Read throughput:    {:.1} queries/s",
        total_reads as f64 / elapsed.as_secs_f64()
    );
    println!("Read latency p50:   {}", format_duration(read_p50));
    println!("Read latency p99:   {}", format_duration(read_p99));
    println!(
        "Read errors:        {} ({:.2}%)",
        total_read_errors, read_error_rate
    );
    println!();
    println!("--- Summary ---");
    println!(
        "Total operations:   {} ({} writes + {} reads)",
        total_ops, total_writes, total_reads
    );
    println!(
        "Overall throughput: {:.1} ops/s",
        total_ops as f64 / elapsed.as_secs_f64()
    );
    println!();

    // Final sanity: no panics, data written.
    if total_writes == 0 {
        eprintln!("WARNING: No rows were written during the load test.");
        std::process::exit(1);
    }

    println!("Load test completed successfully.");
}
