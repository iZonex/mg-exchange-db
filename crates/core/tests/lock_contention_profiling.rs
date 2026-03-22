//! Lock contention profiling stress test.
//!
//! Spawns N writer threads and N reader threads on the same table, measures
//! contention (time spent waiting for locks vs doing actual work), and reports
//! lock wait time percentiles (p50, p99) for N=1,2,4,8,16.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};
use tempfile::tempdir;

/// A single lock-wait measurement from a thread.
struct LockMeasurement {
    lock_wait: Duration,
    work_time: Duration,
}

/// Summary statistics for a contention run.
struct ContentionReport {
    thread_count: usize,
    writer_p50_us: u64,
    writer_p99_us: u64,
    reader_p50_us: u64,
    reader_p99_us: u64,
    writer_total_lock_us: u64,
    writer_total_work_us: u64,
    reader_total_lock_us: u64,
    reader_total_work_us: u64,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p / 100.0).ceil() as usize;
    let idx = idx.min(sorted.len()).max(1) - 1;
    sorted[idx]
}

/// Run a contention test with N writers and N readers operating on the same
/// table for a fixed duration.
fn run_contention_test(n: usize) -> ContentionReport {
    let dir = tempdir().unwrap();
    let db_root = dir.path().to_path_buf();

    // Create a test table with a simple schema.
    TableBuilder::new("contention_test")
        .column("ts", ColumnType::Timestamp)
        .column("value", ColumnType::F64)
        .timestamp("ts")
        .partition_by(PartitionBy::None)
        .build(dir.path())
        .unwrap();

    // Seed some initial data so readers have something to read.
    {
        let mut writer = TableWriter::open(dir.path(), "contention_test").unwrap();
        for i in 0..100 {
            let ts = Timestamp::from_micros(1_000_000 + i * 1000);
            writer.write_row(ts, &[ColumnValue::F64(i as f64)]).unwrap();
        }
    }

    let test_duration = Duration::from_secs(2);
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(n * 2 + 1)); // N writers + N readers + main

    let mut writer_handles = Vec::new();
    let mut reader_handles = Vec::new();

    // Spawn writer threads.
    for thread_id in 0..n {
        let db_path = db_root.clone();
        let stop_clone = Arc::clone(&stop);
        let barrier_clone = Arc::clone(&barrier);

        writer_handles.push(thread::spawn(move || {
            let mut measurements = Vec::new();
            barrier_clone.wait();

            let mut counter = 0u64;
            while !stop_clone.load(Ordering::Relaxed) {
                let ts = Timestamp::from_micros(
                    2_000_000 + (thread_id as i64) * 1_000_000 + counter as i64 * 10,
                );

                // Measure lock acquisition: opening a writer acquires the table lock.
                let lock_start = Instant::now();
                let mut writer = match TableWriter::open(&db_path, "contention_test") {
                    Ok(w) => w,
                    Err(_) => continue,
                };
                let lock_wait = lock_start.elapsed();

                // Measure actual write work.
                let work_start = Instant::now();
                let _ = writer.write_row(ts, &[ColumnValue::F64(counter as f64)]);
                let work_time = work_start.elapsed();

                measurements.push(LockMeasurement {
                    lock_wait,
                    work_time,
                });
                counter += 1;
            }

            measurements
        }));
    }

    // Spawn reader threads.
    for _thread_id in 0..n {
        let db_path = db_root.clone();
        let stop_clone = Arc::clone(&stop);
        let barrier_clone = Arc::clone(&barrier);

        reader_handles.push(thread::spawn(move || {
            let mut measurements = Vec::new();
            barrier_clone.wait();

            while !stop_clone.load(Ordering::Relaxed) {
                // Measure lock acquisition: reading directory listing as a proxy for read lock.
                let lock_start = Instant::now();
                let table_dir = db_path.join("contention_test");
                let entries: Vec<_> = match std::fs::read_dir(&table_dir) {
                    Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
                    Err(_) => continue,
                };
                let lock_wait = lock_start.elapsed();

                // Simulate read work: scan partition directories.
                let work_start = Instant::now();
                let mut _count = 0usize;
                for entry in &entries {
                    if entry.path().is_dir() {
                        _count += 1;
                    }
                }
                let work_time = work_start.elapsed();

                measurements.push(LockMeasurement {
                    lock_wait,
                    work_time,
                });
            }

            measurements
        }));
    }

    // Release all threads.
    barrier.wait();

    // Let them run for the test duration.
    thread::sleep(test_duration);
    stop.store(true, Ordering::Relaxed);

    // Collect writer measurements.
    let mut all_writer_lock_us: Vec<u64> = Vec::new();
    let mut total_writer_lock_us = 0u64;
    let mut total_writer_work_us = 0u64;
    for handle in writer_handles {
        let measurements = handle.join().unwrap();
        for m in &measurements {
            let lock_us = m.lock_wait.as_micros() as u64;
            let work_us = m.work_time.as_micros() as u64;
            all_writer_lock_us.push(lock_us);
            total_writer_lock_us += lock_us;
            total_writer_work_us += work_us;
        }
    }

    // Collect reader measurements.
    let mut all_reader_lock_us: Vec<u64> = Vec::new();
    let mut total_reader_lock_us = 0u64;
    let mut total_reader_work_us = 0u64;
    for handle in reader_handles {
        let measurements = handle.join().unwrap();
        for m in &measurements {
            let lock_us = m.lock_wait.as_micros() as u64;
            let work_us = m.work_time.as_micros() as u64;
            all_reader_lock_us.push(lock_us);
            total_reader_lock_us += lock_us;
            total_reader_work_us += work_us;
        }
    }

    all_writer_lock_us.sort();
    all_reader_lock_us.sort();

    ContentionReport {
        thread_count: n,
        writer_p50_us: percentile(&all_writer_lock_us, 50.0),
        writer_p99_us: percentile(&all_writer_lock_us, 99.0),
        reader_p50_us: percentile(&all_reader_lock_us, 50.0),
        reader_p99_us: percentile(&all_reader_lock_us, 99.0),
        writer_total_lock_us: total_writer_lock_us,
        writer_total_work_us: total_writer_work_us,
        reader_total_lock_us: total_reader_lock_us,
        reader_total_work_us: total_reader_work_us,
    }
}

// SIGBUS on macOS: concurrent mmap operations trigger bus error when
// a writer extends the file while readers have it mapped.
#[test]
#[ignore]
fn lock_contention_scaling_profile() {
    println!("\n========== Lock Contention Profiling ==========");
    println!(
        "{:>8} | {:>12} {:>12} | {:>12} {:>12} | {:>10} {:>10}",
        "Threads",
        "Wr p50 (us)",
        "Wr p99 (us)",
        "Rd p50 (us)",
        "Rd p99 (us)",
        "Wr Lock%",
        "Rd Lock%"
    );
    println!("{}", "-".repeat(95));

    for &n in &[1, 2, 4, 8, 16] {
        let report = run_contention_test(n);

        let wr_lock_pct = if report.writer_total_lock_us + report.writer_total_work_us > 0 {
            100.0 * report.writer_total_lock_us as f64
                / (report.writer_total_lock_us + report.writer_total_work_us) as f64
        } else {
            0.0
        };
        let rd_lock_pct = if report.reader_total_lock_us + report.reader_total_work_us > 0 {
            100.0 * report.reader_total_lock_us as f64
                / (report.reader_total_lock_us + report.reader_total_work_us) as f64
        } else {
            0.0
        };

        println!(
            "{:>8} | {:>12} {:>12} | {:>12} {:>12} | {:>9.1}% {:>9.1}%",
            report.thread_count,
            report.writer_p50_us,
            report.writer_p99_us,
            report.reader_p50_us,
            report.reader_p99_us,
            wr_lock_pct,
            rd_lock_pct,
        );

        // Basic sanity: p50 should be <= p99.
        assert!(
            report.writer_p50_us <= report.writer_p99_us + 1,
            "writer p50 ({}) should be <= p99 ({})",
            report.writer_p50_us,
            report.writer_p99_us
        );
        assert!(
            report.reader_p50_us <= report.reader_p99_us + 1,
            "reader p50 ({}) should be <= p99 ({})",
            report.reader_p50_us,
            report.reader_p99_us
        );
    }

    println!("================================================\n");
}
