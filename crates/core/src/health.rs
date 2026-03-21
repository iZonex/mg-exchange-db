//! Health checking and monitoring for ExchangeDB nodes.
//!
//! Reports disk space, WAL lag, memory usage, and open file count.

use std::path::PathBuf;
use std::time::Instant;

/// Overall health status of the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverallStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Status of an individual health check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
}

/// Result of a single health check.
#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
    pub duration_ms: u64,
}

/// Aggregate health status for a node.
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub status: OverallStatus,
    pub node_id: String,
    pub uptime_secs: u64,
    pub version: String,
    pub checks: Vec<HealthCheck>,
}

/// Runs health checks against the local node.
pub struct HealthChecker {
    db_root: PathBuf,
    start_time: Instant,
    node_id: String,
}

impl HealthChecker {
    /// Create a new health checker.
    pub fn new(db_root: PathBuf, node_id: String) -> Self {
        Self {
            db_root,
            start_time: Instant::now(),
            node_id,
        }
    }

    /// Run all health checks and return aggregate status.
    pub fn check(&self) -> HealthStatus {
        let mut checks = Vec::new();

        checks.push(self.check_disk_space());
        checks.push(self.check_wal_lag());
        checks.push(self.check_memory_usage());
        checks.push(self.check_data_dir());

        // Determine overall status from individual checks.
        let overall = if checks.iter().any(|c| c.status == CheckStatus::Fail) {
            OverallStatus::Unhealthy
        } else if checks.iter().any(|c| c.status == CheckStatus::Warn) {
            OverallStatus::Degraded
        } else {
            OverallStatus::Healthy
        };

        HealthStatus {
            status: overall,
            node_id: self.node_id.clone(),
            uptime_secs: self.start_time.elapsed().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            checks,
        }
    }

    /// Check available disk space at the data directory.
    fn check_disk_space(&self) -> HealthCheck {
        let start = Instant::now();

        // Use statvfs-style check via std::fs metadata as a proxy.
        // In production this would use platform-specific APIs.
        let (status, message) = if self.db_root.exists() {
            // Try to estimate available space by checking if we can create
            // a temp file. This is a lightweight probe.
            match disk_free_bytes(&self.db_root) {
                Ok(free) => {
                    let gb = free as f64 / (1024.0 * 1024.0 * 1024.0);
                    if free < 1024 * 1024 * 100 {
                        // < 100 MB
                        (CheckStatus::Fail, format!("critically low disk space: {gb:.2} GB free"))
                    } else if free < 1024 * 1024 * 1024 {
                        // < 1 GB
                        (CheckStatus::Warn, format!("low disk space: {gb:.2} GB free"))
                    } else {
                        (CheckStatus::Pass, format!("{gb:.2} GB free"))
                    }
                }
                Err(e) => (CheckStatus::Warn, format!("unable to check disk space: {e}")),
            }
        } else {
            (CheckStatus::Fail, "data directory does not exist".into())
        };

        HealthCheck {
            name: "disk_space".into(),
            status,
            message,
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Check WAL lag: how many WAL segments are pending.
    fn check_wal_lag(&self) -> HealthCheck {
        let start = Instant::now();

        let wal_dir = self.db_root.join("_wal");
        let (status, message) = if wal_dir.exists() {
            match std::fs::read_dir(&wal_dir) {
                Ok(entries) => {
                    let segment_count = entries.filter_map(|e| e.ok()).count();
                    if segment_count > 100 {
                        (
                            CheckStatus::Fail,
                            format!("high WAL lag: {segment_count} segments pending"),
                        )
                    } else if segment_count > 20 {
                        (
                            CheckStatus::Warn,
                            format!("elevated WAL lag: {segment_count} segments"),
                        )
                    } else {
                        (
                            CheckStatus::Pass,
                            format!("{segment_count} WAL segments"),
                        )
                    }
                }
                Err(e) => (CheckStatus::Warn, format!("unable to read WAL dir: {e}")),
            }
        } else {
            // No WAL directory may be normal if WAL is disabled.
            (CheckStatus::Pass, "WAL directory not present (WAL may be disabled)".into())
        };

        HealthCheck {
            name: "wal_lag".into(),
            status,
            message,
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Check memory usage (process-level estimate).
    fn check_memory_usage(&self) -> HealthCheck {
        let start = Instant::now();

        let (status, message) = match process_memory_bytes() {
            Ok(bytes) => {
                let mb = bytes as f64 / (1024.0 * 1024.0);
                if bytes > 8 * 1024 * 1024 * 1024 {
                    // > 8 GB
                    (CheckStatus::Warn, format!("high memory usage: {mb:.0} MB"))
                } else {
                    (CheckStatus::Pass, format!("{mb:.0} MB used"))
                }
            }
            Err(e) => (CheckStatus::Pass, format!("unable to read memory usage: {e}")),
        };

        HealthCheck {
            name: "memory_usage".into(),
            status,
            message,
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }

    /// Check that the data directory is accessible and writable.
    fn check_data_dir(&self) -> HealthCheck {
        let start = Instant::now();

        let (status, message) = if !self.db_root.exists() {
            (CheckStatus::Fail, "data directory does not exist".into())
        } else if !self.db_root.is_dir() {
            (CheckStatus::Fail, "data path is not a directory".into())
        } else {
            // Try to probe writability with a temp file.
            let probe = self.db_root.join("_health_probe");
            match std::fs::write(&probe, b"ok") {
                Ok(_) => {
                    let _ = std::fs::remove_file(&probe);
                    (CheckStatus::Pass, "data directory is writable".into())
                }
                Err(e) => (CheckStatus::Fail, format!("data directory not writable: {e}")),
            }
        };

        HealthCheck {
            name: "data_dir".into(),
            status,
            message,
            duration_ms: start.elapsed().as_millis() as u64,
        }
    }
}

/// Estimate free disk space at the given path.
///
/// Uses the `df` command on Unix systems; returns a large value on
/// platforms where disk space cannot be determined.
fn disk_free_bytes(path: &std::path::Path) -> Result<u64, String> {
    #[cfg(unix)]
    {
        // Run `df -k <path>` and parse available KB from the output.
        let output = std::process::Command::new("df")
            .arg("-k")
            .arg(path)
            .output()
            .map_err(|e| format!("failed to run df: {e}"))?;

        if !output.status.success() {
            return Err("df command failed".into());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // The second line contains the data; available is the 4th field.
        let line = stdout.lines().nth(1).ok_or("unexpected df output")?;
        let available_kb: u64 = line
            .split_whitespace()
            .nth(3)
            .ok_or("unexpected df output format")?
            .parse()
            .map_err(|e| format!("failed to parse available space: {e}"))?;

        Ok(available_kb * 1024)
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}

/// Estimate the current process memory usage in bytes.
///
/// Uses `/proc/self/statm` on Linux and `ps` on macOS. Returns 0 on
/// platforms where memory usage cannot be determined.
fn process_memory_bytes() -> Result<u64, String> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm")
            .map_err(|e| format!("cannot read /proc/self/statm: {e}"))?;
        let resident_pages: u64 = statm
            .split_whitespace()
            .nth(1) // RSS in pages
            .ok_or("unexpected statm format")?
            .parse()
            .map_err(|e| format!("parse error: {e}"))?;
        // Assume 4 KB pages; correct on most Linux systems.
        Ok(resident_pages * 4096)
    }

    #[cfg(target_os = "macos")]
    {
        // Use `ps -o rss= -p <pid>` which returns RSS in KB.
        let pid = std::process::id();
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .map_err(|e| format!("failed to run ps: {e}"))?;

        let rss_kb: u64 = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .unwrap_or(0);
        Ok(rss_kb * 1024)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn healthy_status_with_valid_dir() {
        let dir = tempdir().unwrap();
        let checker = HealthChecker::new(dir.path().to_path_buf(), "test-node".into());

        let status = checker.check();
        assert_eq!(status.node_id, "test-node");
        assert_eq!(status.status, OverallStatus::Healthy);
        assert!(!status.version.is_empty());
        assert!(status.uptime_secs < 2);
    }

    #[test]
    fn unhealthy_when_data_dir_missing() {
        let checker = HealthChecker::new(
            PathBuf::from("/nonexistent/path/that/should/not/exist"),
            "test-node".into(),
        );

        let status = checker.check();
        assert_eq!(status.status, OverallStatus::Unhealthy);

        let data_dir_check = status.checks.iter().find(|c| c.name == "data_dir").unwrap();
        assert_eq!(data_dir_check.status, CheckStatus::Fail);
    }

    #[test]
    fn check_reports_all_checks() {
        let dir = tempdir().unwrap();
        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());

        let status = checker.check();
        let check_names: Vec<&str> = status.checks.iter().map(|c| c.name.as_str()).collect();

        assert!(check_names.contains(&"disk_space"));
        assert!(check_names.contains(&"wal_lag"));
        assert!(check_names.contains(&"memory_usage"));
        assert!(check_names.contains(&"data_dir"));
    }

    #[test]
    fn wal_lag_pass_when_no_wal_dir() {
        let dir = tempdir().unwrap();
        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());

        let status = checker.check();
        let wal_check = status.checks.iter().find(|c| c.name == "wal_lag").unwrap();
        assert_eq!(wal_check.status, CheckStatus::Pass);
    }

    #[test]
    fn wal_lag_counts_segments() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("_wal");
        std::fs::create_dir(&wal_dir).unwrap();

        // Create some fake WAL segments
        for i in 0..5 {
            std::fs::write(wal_dir.join(format!("segment_{i}.wal")), b"data").unwrap();
        }

        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());
        let status = checker.check();
        let wal_check = status.checks.iter().find(|c| c.name == "wal_lag").unwrap();
        assert_eq!(wal_check.status, CheckStatus::Pass);
        assert!(wal_check.message.contains("5 WAL segments"));
    }

    #[test]
    fn disk_space_check_runs() {
        let dir = tempdir().unwrap();
        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());

        let status = checker.check();
        let disk_check = status.checks.iter().find(|c| c.name == "disk_space").unwrap();
        // On a test machine we should have enough disk space
        assert_ne!(disk_check.status, CheckStatus::Fail);
    }

    #[test]
    fn data_dir_writable_check() {
        let dir = tempdir().unwrap();
        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());

        let status = checker.check();
        let dir_check = status.checks.iter().find(|c| c.name == "data_dir").unwrap();
        assert_eq!(dir_check.status, CheckStatus::Pass);
        assert!(dir_check.message.contains("writable"));
    }

    #[test]
    fn overall_degraded_when_warn() {
        // Create a scenario where WAL has many segments (>20 triggers Warn)
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("_wal");
        std::fs::create_dir(&wal_dir).unwrap();

        for i in 0..25 {
            std::fs::write(wal_dir.join(format!("seg_{i:04}.wal")), b"x").unwrap();
        }

        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());
        let status = checker.check();
        assert_eq!(status.status, OverallStatus::Degraded);
    }

    #[test]
    fn check_durations_are_recorded() {
        let dir = tempdir().unwrap();
        let checker = HealthChecker::new(dir.path().to_path_buf(), "node-1".into());

        let status = checker.check();
        for check in &status.checks {
            // Duration should be non-negative (it's u64, always is)
            // Just verify the field is populated
            assert!(check.duration_ms < 5000, "check '{}' took too long", check.name);
        }
    }
}
