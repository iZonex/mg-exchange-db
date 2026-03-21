use exchange_common::error::Result;
use exchange_common::types::PartitionBy;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Policy controlling which partitions are retained and which are dropped.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Drop partitions whose timestamp is older than `now - max_age`.
    pub max_age: Option<Duration>,
    /// Keep only the N most recent partitions (by directory name sort order).
    pub max_partitions: Option<usize>,
    /// Drop oldest partitions when total on-disk size exceeds this limit.
    pub max_disk_size: Option<u64>,
}

impl RetentionPolicy {
    /// A policy that retains everything (no limits).
    pub fn unlimited() -> Self {
        Self {
            max_age: None,
            max_partitions: None,
            max_disk_size: None,
        }
    }
}

/// Result of enforcing a retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionStats {
    pub partitions_dropped: u32,
    pub bytes_freed: u64,
}

/// Manages retention for a single table's partitions.
pub struct RetentionManager {
    table_dir: PathBuf,
    partition_by: PartitionBy,
    policy: RetentionPolicy,
}

impl RetentionManager {
    pub fn new(table_dir: PathBuf, partition_by: PartitionBy, policy: RetentionPolicy) -> Self {
        Self {
            table_dir,
            partition_by,
            policy,
        }
    }

    /// List partition directories sorted by name (ascending / oldest first).
    fn list_partitions_sorted(&self) -> Result<Vec<PathBuf>> {
        let mut partitions = Vec::new();
        if !self.table_dir.exists() {
            return Ok(partitions);
        }
        for entry in std::fs::read_dir(&self.table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                // Skip internal directories (e.g. _meta, _wal)
                if !name_str.starts_with('_') {
                    partitions.push(path);
                }
            }
        }
        partitions.sort();
        Ok(partitions)
    }

    /// Evaluate the retention policy and return the list of partition
    /// directories that should be dropped, **without** actually removing them.
    pub fn evaluate(&self) -> Result<Vec<PathBuf>> {
        let partitions = self.list_partitions_sorted()?;
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        let mut to_drop = std::collections::HashSet::new();

        // --- max_age ---
        if let Some(max_age) = self.policy.max_age {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let cutoff_secs = now_secs.saturating_sub(max_age.as_secs());

            for p in &partitions {
                let name = p.file_name().unwrap().to_string_lossy();
                if let Some(partition_secs) = parse_partition_timestamp(&name, self.partition_by) {
                    if partition_secs < cutoff_secs {
                        to_drop.insert(p.clone());
                    }
                }
            }
        }

        // --- max_partitions ---
        if let Some(max) = self.policy.max_partitions {
            if partitions.len() > max {
                let excess = partitions.len() - max;
                for p in partitions.iter().take(excess) {
                    to_drop.insert(p.clone());
                }
            }
        }

        // --- max_disk_size ---
        if let Some(max_bytes) = self.policy.max_disk_size {
            // Calculate sizes for each partition (oldest first)
            let sizes: Vec<(PathBuf, u64)> = partitions
                .iter()
                .map(|p| {
                    let size = dir_size(p).unwrap_or(0);
                    (p.clone(), size)
                })
                .collect();

            let total_size: u64 = sizes.iter().map(|(_, s)| *s).sum();
            if total_size > max_bytes {
                let mut freed: u64 = 0;
                let need_to_free = total_size - max_bytes;
                // Drop oldest partitions until we are under the limit
                for (p, size) in &sizes {
                    if freed >= need_to_free {
                        break;
                    }
                    to_drop.insert(p.clone());
                    freed += size;
                }
            }
        }

        // Return sorted for deterministic results
        let mut result: Vec<PathBuf> = to_drop.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Evaluate and enforce the retention policy by removing expired partitions.
    pub fn enforce(&self) -> Result<RetentionStats> {
        let to_drop = self.evaluate()?;
        let mut bytes_freed: u64 = 0;
        let mut partitions_dropped: u32 = 0;

        for p in &to_drop {
            let size = dir_size(p).unwrap_or(0);
            std::fs::remove_dir_all(p)?;
            bytes_freed += size;
            partitions_dropped += 1;
        }

        Ok(RetentionStats {
            partitions_dropped,
            bytes_freed,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a partition directory name into a Unix timestamp (seconds).
///
/// Supported formats (matching `PartitionBy`):
/// - `YYYY`            (Year)
/// - `YYYY-MM`         (Month)
/// - `YYYY-Www`        (Week — approximated)
/// - `YYYY-MM-DD`      (Day)
/// - `YYYY-MM-DDThh`   (Hour)
fn parse_partition_timestamp(name: &str, partition_by: PartitionBy) -> Option<u64> {
    match partition_by {
        PartitionBy::None => None, // "default" partition has no inherent time
        PartitionBy::Year => {
            let year: i32 = name.parse().ok()?;
            civil_to_epoch(year, 1, 1, 0)
        }
        PartitionBy::Month => {
            let parts: Vec<&str> = name.splitn(2, '-').collect();
            if parts.len() != 2 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            civil_to_epoch(year, month, 1, 0)
        }
        PartitionBy::Week => {
            // Format: YYYY-Www
            let parts: Vec<&str> = name.splitn(2, "-W").collect();
            if parts.len() != 2 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let week: u32 = parts[1].parse().ok()?;
            // Approximate: week 1 starts ~Jan 1
            let day = (week.saturating_sub(1)) * 7 + 1;
            civil_to_epoch(year, 1, day.min(28), 0)
        }
        PartitionBy::Day => {
            let parts: Vec<&str> = name.splitn(3, '-').collect();
            if parts.len() != 3 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            let day: u32 = parts[2].parse().ok()?;
            civil_to_epoch(year, month, day, 0)
        }
        PartitionBy::Hour => {
            // Format: YYYY-MM-DDThh
            let (date_part, hour_part) = name.split_once('T')?;
            let parts: Vec<&str> = date_part.splitn(3, '-').collect();
            if parts.len() != 3 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            let day: u32 = parts[2].parse().ok()?;
            let hour: u32 = hour_part.parse().ok()?;
            civil_to_epoch(year, month, day, hour)
        }
    }
}

/// Convert a civil date+hour to Unix epoch seconds using the same algorithm
/// as the partition module (Howard Hinnant's algorithm, inverted).
fn civil_to_epoch(year: i32, month: u32, day: u32, hour: u32) -> Option<u64> {
    if month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }

    // Adjust so March is month 1 (same trick as Hinnant)
    let (y, m) = if month <= 2 {
        (year as i64 - 1, month + 9)
    } else {
        (year as i64, month - 3)
    };

    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;

    Some((days * 86400 + hour as i64 * 3600) as u64)
}

/// Recursively compute the on-disk size of a directory.
fn dir_size(path: &Path) -> Result<u64> {
    let mut total: u64 = 0;
    if path.is_file() {
        return Ok(std::fs::metadata(path)?.len());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_file() {
            total += entry.metadata()?.len();
        } else if ft.is_dir() {
            total += dir_size(&entry.path())?;
        }
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Helper: create a fake partition directory with a file of the given size.
    fn create_partition(root: &Path, name: &str, data_bytes: usize) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("col.d");
        fs::write(&file, vec![0u8; data_bytes]).unwrap();
        dir
    }

    // -- parse_partition_timestamp tests ------------------------------------

    #[test]
    fn parse_day_partition() {
        let secs = parse_partition_timestamp("2024-03-15", PartitionBy::Day).unwrap();
        // 2024-03-15 00:00:00 UTC
        assert_eq!(secs, 1710460800);
    }

    #[test]
    fn parse_hour_partition() {
        let secs = parse_partition_timestamp("2024-03-15T14", PartitionBy::Hour).unwrap();
        assert_eq!(secs, 1710460800 + 14 * 3600);
    }

    #[test]
    fn parse_month_partition() {
        let secs = parse_partition_timestamp("2024-03", PartitionBy::Month).unwrap();
        // 2024-03-01 00:00:00 UTC
        assert_eq!(secs, 1709251200);
    }

    #[test]
    fn parse_year_partition() {
        let secs = parse_partition_timestamp("2024", PartitionBy::Year).unwrap();
        // 2024-01-01 00:00:00 UTC
        assert_eq!(secs, 1704067200);
    }

    #[test]
    fn parse_none_partition() {
        assert!(parse_partition_timestamp("default", PartitionBy::None).is_none());
    }

    // -- RetentionManager tests ---------------------------------------------

    #[test]
    fn evaluate_max_partitions() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition(root, "2024-01-01", 100);
        create_partition(root, "2024-01-02", 100);
        create_partition(root, "2024-01-03", 100);
        create_partition(root, "2024-01-04", 100);
        create_partition(root, "2024-01-05", 100);

        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: Some(3),
            max_disk_size: None,
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let to_drop = mgr.evaluate().unwrap();

        assert_eq!(to_drop.len(), 2);
        assert!(to_drop[0].ends_with("2024-01-01"));
        assert!(to_drop[1].ends_with("2024-01-02"));
    }

    #[test]
    fn evaluate_max_disk_size() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        // Each partition has ~1000 bytes of data
        create_partition(root, "2024-01-01", 1000);
        create_partition(root, "2024-01-02", 1000);
        create_partition(root, "2024-01-03", 1000);
        create_partition(root, "2024-01-04", 1000);

        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: None,
            max_disk_size: Some(2500), // Only ~2.5KB allowed
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let to_drop = mgr.evaluate().unwrap();

        // Need to free ~1500 bytes, so 2 oldest partitions should be dropped
        assert_eq!(to_drop.len(), 2);
        assert!(to_drop[0].ends_with("2024-01-01"));
        assert!(to_drop[1].ends_with("2024-01-02"));
    }

    #[test]
    fn evaluate_max_age() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        // Create partitions: one very old, one recent
        create_partition(root, "2020-01-01", 100);
        create_partition(root, "2020-06-15", 100);
        // A partition that should be within any reasonable max_age from "now"
        // We use a date far in the future relative to 2020 but close to now.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Create a "today" partition
        let today_name = {
            let days = now_secs / 86400;
            let z = days as i64 + 719468;
            let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
            let doe = (z - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i64 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };
            format!("{:04}-{:02}-{:02}", y, m, d)
        };
        create_partition(root, &today_name, 100);

        let policy = RetentionPolicy {
            max_age: Some(Duration::from_secs(365 * 86400)), // 1 year
            max_partitions: None,
            max_disk_size: None,
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let to_drop = mgr.evaluate().unwrap();

        // 2020 partitions should be dropped (older than 1 year from now)
        assert!(to_drop.len() >= 2);
        assert!(to_drop.iter().any(|p| p.ends_with("2020-01-01")));
        assert!(to_drop.iter().any(|p| p.ends_with("2020-06-15")));
        // Today's partition should NOT be dropped
        assert!(!to_drop.iter().any(|p| p.ends_with(&today_name)));
    }

    #[test]
    fn enforce_removes_partitions() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition(root, "2024-01-01", 500);
        create_partition(root, "2024-01-02", 500);
        create_partition(root, "2024-01-03", 500);

        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: Some(1),
            max_disk_size: None,
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let stats = mgr.enforce().unwrap();

        assert_eq!(stats.partitions_dropped, 2);
        assert!(stats.bytes_freed >= 1000); // at least 2 * 500 bytes

        // Only the newest partition should remain
        let remaining: Vec<_> = fs::read_dir(root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        assert_eq!(remaining.len(), 1);
        assert!(remaining[0].path().ends_with("2024-01-03"));
    }

    #[test]
    fn enforce_no_op_when_within_limits() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition(root, "2024-01-01", 100);
        create_partition(root, "2024-01-02", 100);

        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: Some(5),
            max_disk_size: Some(1_000_000),
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let stats = mgr.enforce().unwrap();

        assert_eq!(stats.partitions_dropped, 0);
        assert_eq!(stats.bytes_freed, 0);
    }

    #[test]
    fn enforce_empty_table() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: Some(1),
            max_disk_size: None,
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let stats = mgr.enforce().unwrap();

        assert_eq!(stats.partitions_dropped, 0);
        assert_eq!(stats.bytes_freed, 0);
    }

    #[test]
    fn evaluate_skips_internal_dirs() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition(root, "_meta", 100);
        create_partition(root, "_wal", 100);
        create_partition(root, "2024-01-01", 100);

        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: Some(0), // drop all partitions
            max_disk_size: None,
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let to_drop = mgr.evaluate().unwrap();

        // Should only include the real partition, not _meta or _wal
        assert_eq!(to_drop.len(), 1);
        assert!(to_drop[0].ends_with("2024-01-01"));
    }

    #[test]
    fn combined_policies() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition(root, "2024-01-01", 1000);
        create_partition(root, "2024-01-02", 1000);
        create_partition(root, "2024-01-03", 1000);
        create_partition(root, "2024-01-04", 1000);
        create_partition(root, "2024-01-05", 1000);

        // max_partitions = 4 would drop 1, but max_disk_size = 2500 wants to drop 3
        // The union of both should yield 3 dropped.
        let policy = RetentionPolicy {
            max_age: None,
            max_partitions: Some(4),
            max_disk_size: Some(2500),
        };

        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let to_drop = mgr.evaluate().unwrap();

        // Disk size needs 3 dropped (5000 - 2500 = 2500 to free, each ~1000)
        assert!(to_drop.len() >= 3);
    }

    #[test]
    fn unlimited_policy_drops_nothing() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition(root, "2024-01-01", 1000);
        create_partition(root, "2024-01-02", 1000);

        let policy = RetentionPolicy::unlimited();
        let mgr = RetentionManager::new(root.to_path_buf(), PartitionBy::Day, policy);
        let to_drop = mgr.evaluate().unwrap();

        assert!(to_drop.is_empty());
    }

    // -- civil_to_epoch tests -----------------------------------------------

    #[test]
    fn civil_to_epoch_known_dates() {
        // 2024-01-01 00:00:00 UTC
        assert_eq!(civil_to_epoch(2024, 1, 1, 0), Some(1704067200));
        // 1970-01-01 00:00:00 UTC
        assert_eq!(civil_to_epoch(1970, 1, 1, 0), Some(0));
    }

    #[test]
    fn civil_to_epoch_invalid() {
        assert_eq!(civil_to_epoch(2024, 0, 1, 0), None);
        assert_eq!(civil_to_epoch(2024, 13, 1, 0), None);
        assert_eq!(civil_to_epoch(2024, 1, 0, 0), None);
    }
}
