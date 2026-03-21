//! Slow query log.
//!
//! Logs queries that exceed a configurable duration threshold. Can write
//! to a file and/or to the `tracing` subsystem.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;

/// Maximum number of recent slow queries kept in memory.
const MAX_RECENT_ENTRIES: usize = 100;

/// A recorded slow query entry.
#[derive(Debug, Clone, Serialize)]
pub struct SlowQueryEntry {
    /// ISO-8601 timestamp when the query was logged.
    pub timestamp: String,
    /// The SQL query (truncated to 500 chars).
    pub sql: String,
    /// Execution duration in seconds.
    pub duration_secs: f64,
    /// Number of result rows.
    pub rows: u64,
}

/// Slow query logger that records queries exceeding a time threshold.
pub struct SlowQueryLog {
    threshold: Duration,
    log_file: Option<Mutex<BufWriter<File>>>,
    /// In-memory ring buffer of recent slow queries.
    recent: Mutex<VecDeque<SlowQueryEntry>>,
}

impl SlowQueryLog {
    /// Create a new slow query log.
    ///
    /// - `threshold`: queries taking longer than this are logged (default: 1 second).
    /// - `log_path`: optional file path to write slow query entries.
    pub fn new(threshold: Duration, log_path: Option<&Path>) -> Self {
        let log_file = log_path.and_then(|path| {
            // Ensure parent directory exists.
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()
                .map(|f| Mutex::new(BufWriter::new(f)))
        });

        Self {
            threshold,
            log_file,
            recent: Mutex::new(VecDeque::with_capacity(MAX_RECENT_ENTRIES)),
        }
    }

    /// Create a slow query log with default threshold (1 second) and no file output.
    pub fn default_config() -> Self {
        Self::new(Duration::from_secs(1), None)
    }

    /// Return the most recent slow queries (newest first).
    pub fn recent_queries(&self) -> Vec<SlowQueryEntry> {
        let buf = self.recent.lock().unwrap_or_else(|e| e.into_inner());
        buf.iter().rev().cloned().collect()
    }

    /// Log a query if it exceeded the threshold.
    ///
    /// Format: `[<ISO-8601 timestamp>] <duration>s | <rows> rows | <sql>`
    pub fn maybe_log(&self, sql: &str, duration: Duration, rows: u64) {
        if duration < self.threshold {
            return;
        }

        let duration_secs = duration.as_secs_f64();
        let timestamp = format_timestamp_now();

        // Truncate very long SQL for logging.
        let sql_display = if sql.len() > 500 {
            format!("{}...", &sql[..500])
        } else {
            sql.to_string()
        };

        let line = format!(
            "[{}] {:.3}s | {} rows | {}\n",
            timestamp, duration_secs, rows, sql_display,
        );

        // Store in ring buffer.
        {
            let entry = SlowQueryEntry {
                timestamp: timestamp.clone(),
                sql: sql_display.clone(),
                duration_secs,
                rows,
            };
            let mut buf = self.recent.lock().unwrap_or_else(|e| e.into_inner());
            if buf.len() >= MAX_RECENT_ENTRIES {
                buf.pop_front();
            }
            buf.push_back(entry);
        }

        // Log via tracing.
        tracing::warn!(
            duration_ms = duration.as_millis() as u64,
            rows = rows,
            "slow query: {}",
            sql_display,
        );

        // Write to file if configured.
        if let Some(ref file) = self.log_file
            && let Ok(mut writer) = file.lock()
        {
            let _ = writer.write_all(line.as_bytes());
            let _ = writer.flush();
        }
    }

    /// Returns the configured threshold.
    pub fn threshold(&self) -> Duration {
        self.threshold
    }
}

/// Format the current wall-clock time as ISO-8601.
fn format_timestamp_now() -> String {
    // Use a simple UTC timestamp without pulling in chrono.
    // We format from SystemTime.
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = since_epoch.as_secs();

    // Decompose seconds into date/time components.
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    // Convert days since epoch to Y-M-D using Howard Hinnant's algorithm.
    let z = days as i64 + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hours, minutes, seconds
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn logs_slow_query_to_file() {
        let tmp_dir = std::env::temp_dir().join("exchange_slow_log_test");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let log_path = tmp_dir.join("slow.log");

        // Clean up from previous runs.
        let _ = std::fs::remove_file(&log_path);

        let log = SlowQueryLog::new(Duration::from_millis(100), Some(&log_path));

        // This query is "slow" (200ms > 100ms threshold).
        log.maybe_log(
            "SELECT * FROM trades WHERE price > 50000",
            Duration::from_millis(200),
            1000,
        );

        // Read the log file.
        let mut contents = String::new();
        File::open(&log_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        assert!(contents.contains("0.200s"));
        assert!(contents.contains("1000 rows"));
        assert!(contents.contains("SELECT * FROM trades"));

        // Clean up.
        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_dir(&tmp_dir);
    }

    #[test]
    fn does_not_log_fast_query() {
        let tmp_dir = std::env::temp_dir().join("exchange_slow_log_test2");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let log_path = tmp_dir.join("slow2.log");
        let _ = std::fs::remove_file(&log_path);

        let log = SlowQueryLog::new(Duration::from_secs(1), Some(&log_path));

        // This query is fast (10ms < 1s threshold).
        log.maybe_log("SELECT 1", Duration::from_millis(10), 1);

        // File should not exist or be empty.
        let exists = log_path.exists();
        if exists {
            let contents = std::fs::read_to_string(&log_path).unwrap_or_default();
            assert!(contents.is_empty(), "fast query should not be logged");
        }

        let _ = std::fs::remove_file(&log_path);
        let _ = std::fs::remove_dir(&tmp_dir);
    }

    #[test]
    fn no_file_still_works() {
        // Logging without a file should not panic.
        let log = SlowQueryLog::new(Duration::from_millis(1), None);
        log.maybe_log("SELECT 1", Duration::from_millis(100), 42);
    }
}
