//! Query profiler for EXPLAIN ANALYZE instrumentation.
//!
//! Records timing and row-count information for each step of query execution,
//! then formats a human-readable report.

use std::time::{Duration, Instant};

/// A single profiling step with timing and row metrics.
#[derive(Debug, Clone)]
pub struct ProfilingStep {
    pub name: String,
    pub duration: Duration,
    pub rows_in: u64,
    pub rows_out: u64,
    pub bytes_scanned: u64,
    pub details: String,
}

/// A timer handle returned by `QueryProfiler::step()`.
///
/// Records the elapsed time into the profiler when `finish()` is called
/// or when dropped.
pub struct StepTimer<'a> {
    profiler: &'a mut QueryProfiler,
    name: String,
    start: Instant,
    rows_in: u64,
    rows_out: u64,
    bytes_scanned: u64,
    details: String,
    finished: bool,
}

impl<'a> StepTimer<'a> {
    /// Set the number of input rows for this step.
    pub fn set_rows_in(&mut self, n: u64) {
        self.rows_in = n;
    }

    /// Set the number of output rows for this step.
    pub fn set_rows_out(&mut self, n: u64) {
        self.rows_out = n;
    }

    /// Set the bytes scanned during this step.
    pub fn set_bytes_scanned(&mut self, n: u64) {
        self.bytes_scanned = n;
    }

    /// Set a details string for this step.
    pub fn set_details(&mut self, details: impl Into<String>) {
        self.details = details.into();
    }

    /// Finish the step and record its duration.
    pub fn finish(mut self) {
        self.record();
        self.finished = true;
    }

    fn record(&mut self) {
        let duration = self.start.elapsed();
        self.profiler.steps.push(ProfilingStep {
            name: self.name.clone(),
            duration,
            rows_in: self.rows_in,
            rows_out: self.rows_out,
            bytes_scanned: self.bytes_scanned,
            details: self.details.clone(),
        });
    }
}

impl<'a> Drop for StepTimer<'a> {
    fn drop(&mut self) {
        if !self.finished {
            self.record();
        }
    }
}

/// Query profiler that records execution steps with timing.
pub struct QueryProfiler {
    start: Instant,
    steps: Vec<ProfilingStep>,
}

impl QueryProfiler {
    /// Create a new profiler, starting the clock immediately.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            steps: Vec::new(),
        }
    }

    /// Begin timing a named step. Returns a `StepTimer` that records
    /// the elapsed duration when finished or dropped.
    pub fn step(&mut self, name: &str) -> StepTimer<'_> {
        StepTimer {
            profiler: self,
            name: name.to_string(),
            start: Instant::now(),
            rows_in: 0,
            rows_out: 0,
            bytes_scanned: 0,
            details: String::new(),
            finished: false,
        }
    }

    /// Manually add a completed profiling step.
    pub fn add_step(&mut self, step: ProfilingStep) {
        self.steps.push(step);
    }

    /// Consume the profiler and return the recorded steps.
    pub fn finish(self) -> Vec<ProfilingStep> {
        self.steps
    }

    /// Total elapsed time since the profiler was created.
    pub fn total_elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Format a human-readable profiling report.
    pub fn format_report(&self) -> String {
        let mut out = String::new();
        out.push_str("Query Plan:\n");

        for step in &self.steps {
            out.push_str(&format!("  {}:\n", step.name));
            if step.rows_in > 0 {
                out.push_str(&format!("    Rows in: {}\n", step.rows_in));
            }
            if step.rows_out > 0 {
                out.push_str(&format!("    Rows out: {}\n", step.rows_out));
            }
            if step.bytes_scanned > 0 {
                out.push_str(&format!("    Bytes scanned: {}\n", format_bytes(step.bytes_scanned)));
            }
            out.push_str(&format!("    Time: {}\n", format_duration(step.duration)));
            if !step.details.is_empty() {
                out.push_str(&format!("    {}\n", step.details));
            }
        }

        let total = self.total_elapsed();
        out.push_str(&format!("  Total execution time: {}\n", format_duration(total)));
        out
    }
}

impl Default for QueryProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Format a duration in human-readable form (e.g., "12.3ms", "1.5s").
fn format_duration(d: Duration) -> String {
    let micros = d.as_micros();
    if micros < 1_000 {
        format!("{}us", micros)
    } else if micros < 1_000_000 {
        format!("{:.1}ms", micros as f64 / 1_000.0)
    } else {
        format!("{:.3}s", d.as_secs_f64())
    }
}

/// Format byte count in human-readable form.
fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiler_records_steps() {
        let mut profiler = QueryProfiler::new();

        {
            let mut step = profiler.step("Scan");
            step.set_rows_in(10000);
            step.set_rows_out(500);
            step.set_bytes_scanned(1024 * 1024);
            step.set_details("Filter: price > 50000");
            step.finish();
        }

        {
            let mut step = profiler.step("Sort");
            step.set_rows_in(500);
            step.set_rows_out(500);
            step.set_details("Sort method: in-memory");
            step.finish();
        }

        let steps = profiler.finish();
        assert_eq!(steps.len(), 2);

        assert_eq!(steps[0].name, "Scan");
        assert_eq!(steps[0].rows_in, 10000);
        assert_eq!(steps[0].rows_out, 500);
        assert_eq!(steps[0].bytes_scanned, 1024 * 1024);
        assert!(steps[0].details.contains("price > 50000"));

        assert_eq!(steps[1].name, "Sort");
        assert_eq!(steps[1].rows_in, 500);
    }

    #[test]
    fn profiler_format_report_contains_timing() {
        let mut profiler = QueryProfiler::new();

        {
            let mut step = profiler.step("Scan");
            step.set_rows_out(100);
            step.finish();
        }

        let report = profiler.format_report();
        assert!(report.contains("Query Plan:"));
        assert!(report.contains("Scan:"));
        assert!(report.contains("Rows out: 100"));
        assert!(report.contains("Time:"));
        assert!(report.contains("Total execution time:"));
    }

    #[test]
    fn step_timer_auto_records_on_drop() {
        let mut profiler = QueryProfiler::new();

        {
            let mut step = profiler.step("AutoDrop");
            step.set_rows_out(42);
            // No explicit finish() — should record on drop.
        }

        let steps = profiler.finish();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].name, "AutoDrop");
        assert_eq!(steps[0].rows_out, 42);
    }

    #[test]
    fn format_duration_ranges() {
        assert_eq!(format_duration(Duration::from_micros(500)), "500us");
        assert_eq!(format_duration(Duration::from_micros(12300)), "12.3ms");
        assert_eq!(format_duration(Duration::from_secs_f64(1.5)), "1.500s");
    }

    #[test]
    fn format_bytes_ranges() {
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(2048), "2.0KB");
        assert_eq!(format_bytes(4 * 1024 * 1024), "4.0MB");
    }
}
