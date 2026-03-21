//! Benchmark report generator.
//!
//! Produces a Markdown-formatted performance report from benchmark results.
//! Intended to be used as a post-processing step after `cargo bench` completes.

use std::fmt;
use std::time::Duration;

/// A single benchmark measurement.
#[derive(Debug, Clone)]
pub struct BenchmarkMeasurement {
    /// Human-readable name of the benchmark.
    pub name: String,
    /// Number of rows or elements processed.
    pub num_rows: u64,
    /// Mean latency of the operation.
    pub latency: Duration,
    /// Throughput in rows per second (computed from latency and num_rows).
    pub rows_per_sec: f64,
    /// P99 latency, if available.
    pub p99_latency: Option<Duration>,
}

impl BenchmarkMeasurement {
    /// Create a new measurement from latency and row count.
    pub fn new(name: &str, num_rows: u64, latency: Duration) -> Self {
        let secs = latency.as_secs_f64();
        let rows_per_sec = if secs > 0.0 {
            num_rows as f64 / secs
        } else {
            0.0
        };
        Self {
            name: name.to_string(),
            num_rows,
            latency,
            rows_per_sec,
            p99_latency: None,
        }
    }

    /// Set the p99 latency.
    pub fn with_p99(mut self, p99: Duration) -> Self {
        self.p99_latency = Some(p99);
        self
    }
}

/// Collection of benchmark results for report generation.
#[derive(Debug, Clone, Default)]
pub struct BenchmarkResults {
    /// Insert throughput benchmarks.
    pub inserts: Vec<BenchmarkMeasurement>,
    /// Query latency benchmarks.
    pub queries: Vec<BenchmarkMeasurement>,
    /// Storage benchmarks.
    pub storage: Vec<StorageMeasurement>,
}

/// A storage-specific measurement (compression, column I/O, etc.).
#[derive(Debug, Clone)]
pub struct StorageMeasurement {
    /// Human-readable metric name.
    pub name: String,
    /// The measured value as a string (e.g., "3.2x", "1.2 GB/s").
    pub value: String,
}

impl StorageMeasurement {
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            value: value.to_string(),
        }
    }
}

/// Format a duration for display in the report table.
fn format_duration(d: &Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.1} us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.3} s", d.as_secs_f64())
    }
}

/// Format rows/sec in human-readable form.
fn format_throughput(rows_per_sec: f64) -> String {
    if rows_per_sec >= 1_000_000.0 {
        format!("{:.2}M rows/s", rows_per_sec / 1_000_000.0)
    } else if rows_per_sec >= 1_000.0 {
        format!("{:.1}K rows/s", rows_per_sec / 1_000.0)
    } else {
        format!("{:.0} rows/s", rows_per_sec)
    }
}

/// Generate a Markdown benchmark report from collected results.
pub fn generate_report(results: &BenchmarkResults) -> String {
    let mut out = String::with_capacity(4096);

    out.push_str("# ExchangeDB Performance Report\n\n");

    // -- Insert throughput table --
    out.push_str("## Insert Throughput\n\n");
    out.push_str("| Scenario | Rows/sec | Latency (mean) | Latency (p99) |\n");
    out.push_str("|----------|----------|----------------|---------------|\n");
    for m in &results.inserts {
        let p99 = m
            .p99_latency
            .as_ref()
            .map(format_duration)
            .unwrap_or_else(|| "N/A".to_string());
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            m.name,
            format_throughput(m.rows_per_sec),
            format_duration(&m.latency),
            p99,
        ));
    }
    out.push('\n');

    // -- Query latency table --
    out.push_str("## Query Latency\n\n");
    out.push_str("| Query Type | Rows | Latency | Throughput |\n");
    out.push_str("|------------|------|---------|------------|\n");
    for m in &results.queries {
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            m.name,
            format_row_count(m.num_rows),
            format_duration(&m.latency),
            format_throughput(m.rows_per_sec),
        ));
    }
    out.push('\n');

    // -- Storage metrics table --
    out.push_str("## Storage\n\n");
    out.push_str("| Metric | Value |\n");
    out.push_str("|--------|-------|\n");
    for s in &results.storage {
        out.push_str(&format!("| {} | {} |\n", s.name, s.value));
    }
    out.push('\n');

    out
}

/// Format a row count for display.
fn format_row_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{}M", n / 1_000_000)
    } else if n >= 1_000 {
        format!("{}K", n / 1_000)
    } else {
        format!("{}", n)
    }
}

impl fmt::Display for BenchmarkResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", generate_report(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_report_empty() {
        let results = BenchmarkResults::default();
        let report = generate_report(&results);
        assert!(report.contains("# ExchangeDB Performance Report"));
        assert!(report.contains("## Insert Throughput"));
        assert!(report.contains("## Query Latency"));
        assert!(report.contains("## Storage"));
    }

    #[test]
    fn generate_report_with_data() {
        let results = BenchmarkResults {
            inserts: vec![
                BenchmarkMeasurement::new(
                    "Single partition",
                    1_000_000,
                    Duration::from_millis(250),
                )
                .with_p99(Duration::from_millis(280)),
                BenchmarkMeasurement::new(
                    "Multi partition",
                    1_000_000,
                    Duration::from_millis(400),
                ),
                BenchmarkMeasurement::new(
                    "WAL enabled",
                    1_000_000,
                    Duration::from_millis(600),
                ),
            ],
            queries: vec![
                BenchmarkMeasurement::new("Full scan", 1_000_000, Duration::from_millis(120)),
                BenchmarkMeasurement::new("Filtered", 1_000_000, Duration::from_millis(45)),
                BenchmarkMeasurement::new(
                    "GROUP BY",
                    1_000_000,
                    Duration::from_millis(80),
                ),
                BenchmarkMeasurement::new(
                    "SAMPLE BY",
                    1_000_000,
                    Duration::from_millis(95),
                ),
                BenchmarkMeasurement::new(
                    "ASOF JOIN",
                    600_000,
                    Duration::from_millis(150),
                ),
                BenchmarkMeasurement::new("Top-10", 1_000_000, Duration::from_millis(55)),
            ],
            storage: vec![
                StorageMeasurement::new("Compression ratio (LZ4, f64)", "0.82x"),
                StorageMeasurement::new("Compression ratio (LZ4, timestamps)", "0.15x"),
                StorageMeasurement::new("Column write (f64)", "12.5M /sec"),
                StorageMeasurement::new("Column read (f64)", "250.0M /sec"),
            ],
        };

        let report = generate_report(&results);

        assert!(report.contains("Single partition"));
        assert!(report.contains("4.00M rows/s"));
        assert!(report.contains("Full scan"));
        assert!(report.contains("Compression ratio"));
        assert!(report.contains("280"));
    }

    #[test]
    fn format_duration_ranges() {
        assert_eq!(format_duration(&Duration::from_nanos(500)), "500 ns");
        assert_eq!(format_duration(&Duration::from_micros(150)), "150.0 us");
        assert_eq!(format_duration(&Duration::from_millis(42)), "42.00 ms");
        assert_eq!(format_duration(&Duration::from_secs(2)), "2.000 s");
    }

    #[test]
    fn format_throughput_ranges() {
        assert_eq!(format_throughput(500.0), "500 rows/s");
        assert_eq!(format_throughput(50_000.0), "50.0K rows/s");
        assert_eq!(format_throughput(4_000_000.0), "4.00M rows/s");
    }

    #[test]
    fn display_impl() {
        let results = BenchmarkResults::default();
        let display = format!("{}", results);
        assert!(display.contains("# ExchangeDB Performance Report"));
    }
}
