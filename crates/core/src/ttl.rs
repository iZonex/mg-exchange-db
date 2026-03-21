//! TTL (Time-To-Live) management for ExchangeDB.
//!
//! Provides automatic expiration and cleanup of old data based on configurable
//! per-table TTL policies.

use std::path::{Path, PathBuf};
use std::time::Duration;

use exchange_common::error::Result;

/// Storage tier for tiered storage TTL actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageTier {
    /// Hot storage (default, fast SSD).
    Hot,
    /// Warm storage (slower, cheaper).
    Warm,
    /// Cold storage (archival, cheapest).
    Cold,
}

/// Action to take when data expires.
#[derive(Debug, Clone)]
pub enum TtlAction {
    /// Delete expired data permanently.
    Delete,
    /// Move expired data to an archive location.
    Archive { path: PathBuf },
    /// Move expired data to a different storage tier.
    Tier { target: StorageTier },
}

/// Configuration for TTL on a single table.
#[derive(Debug, Clone)]
pub struct TtlConfig {
    /// Name of the table.
    pub table: String,
    /// Maximum age of data before the TTL action is applied.
    pub max_age: Duration,
    /// What to do with expired data.
    pub action: TtlAction,
    /// How often to check for expired data.
    pub check_interval: Duration,
}

/// Statistics from a TTL enforcement run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlStats {
    /// Number of partitions that were acted upon.
    pub partitions_expired: u32,
    /// Total bytes freed (for Delete actions) or moved.
    pub bytes_affected: u64,
    /// Number of tables processed.
    pub tables_processed: u32,
}

/// Result of evaluating TTL policies (before enforcement).
#[derive(Debug, Clone)]
pub struct TtlEvaluation {
    /// Table name.
    pub table: String,
    /// Partition directories that are expired.
    pub expired_partitions: Vec<PathBuf>,
    /// The action to take.
    pub action: TtlAction,
}

/// Manages TTL policies for multiple tables.
pub struct TtlManager {
    configs: Vec<TtlConfig>,
    db_root: PathBuf,
}

impl TtlManager {
    /// Create a new TTL manager.
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            configs: Vec::new(),
            db_root,
        }
    }

    /// Register a TTL configuration for a table.
    pub fn register(&mut self, config: TtlConfig) {
        self.configs.push(config);
    }

    /// Evaluate all TTL policies and return the list of actions to take,
    /// without actually performing them.
    pub fn evaluate(&self) -> Result<Vec<TtlEvaluation>> {
        let mut evaluations = Vec::new();

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for config in &self.configs {
            let table_dir = self.db_root.join(&config.table);
            if !table_dir.exists() {
                continue;
            }

            let cutoff_secs = now_secs.saturating_sub(config.max_age.as_secs());

            let mut expired_partitions = Vec::new();

            // Scan for partition directories.
            for entry in std::fs::read_dir(&table_dir)? {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }

                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                // Skip internal directories.
                if name_str.starts_with('_') {
                    continue;
                }

                // Try to parse the partition timestamp.
                // Try Day format first, then other common formats.
                if let Some(partition_secs) = parse_partition_timestamp(&name_str) {
                    if partition_secs < cutoff_secs {
                        expired_partitions.push(path);
                    }
                }
            }

            expired_partitions.sort();

            if !expired_partitions.is_empty() {
                evaluations.push(TtlEvaluation {
                    table: config.table.clone(),
                    expired_partitions,
                    action: config.action.clone(),
                });
            }
        }

        Ok(evaluations)
    }

    /// Enforce all TTL policies: evaluate and then execute the actions.
    pub fn enforce(&self) -> Result<TtlStats> {
        let evaluations = self.evaluate()?;

        let mut stats = TtlStats {
            partitions_expired: 0,
            bytes_affected: 0,
            tables_processed: 0,
        };

        for eval in &evaluations {
            stats.tables_processed += 1;

            for partition_path in &eval.expired_partitions {
                let size = dir_size(partition_path).unwrap_or(0);

                match &eval.action {
                    TtlAction::Delete => {
                        std::fs::remove_dir_all(partition_path)?;
                    }
                    TtlAction::Archive { path: archive_path } => {
                        let partition_name = partition_path
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string();
                        let dest = archive_path.join(&eval.table).join(&partition_name);
                        std::fs::create_dir_all(&dest)?;
                        copy_dir_recursive(partition_path, &dest)?;
                        std::fs::remove_dir_all(partition_path)?;
                    }
                    TtlAction::Tier { target } => {
                        // In a real system, this would move data to a different
                        // storage backend. For now, we simulate by moving to a
                        // tier-specific subdirectory.
                        let tier_name = match target {
                            StorageTier::Hot => "hot",
                            StorageTier::Warm => "warm",
                            StorageTier::Cold => "cold",
                        };
                        let tier_dir = self.db_root.join(format!("_tier_{}", tier_name));
                        let partition_name = partition_path
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string();
                        let dest = tier_dir.join(&eval.table).join(&partition_name);
                        std::fs::create_dir_all(&dest)?;
                        copy_dir_recursive(partition_path, &dest)?;
                        std::fs::remove_dir_all(partition_path)?;
                    }
                }

                stats.partitions_expired += 1;
                stats.bytes_affected += size;
            }
        }

        Ok(stats)
    }
}

/// Try to parse a partition directory name into a Unix timestamp (seconds).
///
/// Supports common formats:
/// - `YYYY-MM-DD` (Day)
/// - `YYYY-MM-DDThh` (Hour)
/// - `YYYY-MM` (Month)
/// - `YYYY` (Year)
fn parse_partition_timestamp(name: &str) -> Option<u64> {
    // Try Day: YYYY-MM-DD
    if name.len() == 10 && name.chars().filter(|c| *c == '-').count() == 2 {
        let parts: Vec<&str> = name.splitn(3, '-').collect();
        if parts.len() == 3 {
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            let day: u32 = parts[2].parse().ok()?;
            return civil_to_epoch(year, month, day, 0);
        }
    }

    // Try Hour: YYYY-MM-DDThh
    if name.contains('T') {
        let (date_part, hour_part) = name.split_once('T')?;
        let parts: Vec<&str> = date_part.splitn(3, '-').collect();
        if parts.len() == 3 {
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            let day: u32 = parts[2].parse().ok()?;
            let hour: u32 = hour_part.parse().ok()?;
            return civil_to_epoch(year, month, day, hour);
        }
    }

    // Try Month: YYYY-MM
    if name.len() == 7 && name.chars().filter(|c| *c == '-').count() == 1 {
        let parts: Vec<&str> = name.splitn(2, '-').collect();
        if parts.len() == 2 {
            let year: i32 = parts[0].parse().ok()?;
            let month: u32 = parts[1].parse().ok()?;
            return civil_to_epoch(year, month, 1, 0);
        }
    }

    // Try Year: YYYY
    if name.len() == 4 {
        let year: i32 = name.parse().ok()?;
        return civil_to_epoch(year, 1, 1, 0);
    }

    None
}

/// Convert civil date to Unix epoch seconds (same algorithm as retention module).
fn civil_to_epoch(year: i32, month: u32, day: u32, hour: u32) -> Option<u64> {
    if month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }

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

/// Recursively compute the size of a directory.
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

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dest_path = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            std::fs::copy(&entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Helper: create a fake partition directory with a data file.
    fn create_partition(table_dir: &Path, name: &str, data_bytes: usize) -> PathBuf {
        let dir = table_dir.join(name);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("col.d"), vec![0u8; data_bytes]).unwrap();
        dir
    }

    /// Helper: create a table directory with _meta.
    fn create_table_dir(db_root: &Path, name: &str) -> PathBuf {
        let table_dir = db_root.join(name);
        fs::create_dir_all(&table_dir).unwrap();
        // Write a minimal _meta file.
        fs::write(table_dir.join("_meta"), "{}").unwrap();
        table_dir
    }

    #[test]
    fn test_ttl_delete_old_partitions() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let table_dir = create_table_dir(db_root, "trades");

        // Create old partitions (2020) and a recent one.
        create_partition(&table_dir, "2020-01-01", 500);
        create_partition(&table_dir, "2020-06-15", 500);

        // Create a "today" partition that should not be expired.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
        create_partition(&table_dir, &today_name, 500);

        let mut mgr = TtlManager::new(db_root.to_path_buf());
        mgr.register(TtlConfig {
            table: "trades".into(),
            max_age: Duration::from_secs(365 * 86400), // 1 year
            action: TtlAction::Delete,
            check_interval: Duration::from_secs(3600),
        });

        // Evaluate first.
        let evals = mgr.evaluate().unwrap();
        assert_eq!(evals.len(), 1);
        assert!(evals[0].expired_partitions.len() >= 2);

        // Enforce.
        let stats = mgr.enforce().unwrap();
        assert!(stats.partitions_expired >= 2);
        assert!(stats.bytes_affected > 0);
        assert_eq!(stats.tables_processed, 1);

        // Old partitions should be gone.
        assert!(!table_dir.join("2020-01-01").exists());
        assert!(!table_dir.join("2020-06-15").exists());
        // Today's partition should still exist.
        assert!(table_dir.join(&today_name).exists());
    }

    #[test]
    fn test_ttl_archive_action() {
        let dir = tempdir().unwrap();
        let archive_dir = tempdir().unwrap();
        let db_root = dir.path();

        let table_dir = create_table_dir(db_root, "trades");
        create_partition(&table_dir, "2020-01-01", 100);

        let mut mgr = TtlManager::new(db_root.to_path_buf());
        mgr.register(TtlConfig {
            table: "trades".into(),
            max_age: Duration::from_secs(365 * 86400),
            action: TtlAction::Archive {
                path: archive_dir.path().to_path_buf(),
            },
            check_interval: Duration::from_secs(3600),
        });

        let stats = mgr.enforce().unwrap();
        assert_eq!(stats.partitions_expired, 1);

        // Original should be gone.
        assert!(!table_dir.join("2020-01-01").exists());

        // Archive should have the data.
        assert!(archive_dir
            .path()
            .join("trades")
            .join("2020-01-01")
            .join("col.d")
            .exists());
    }

    #[test]
    fn test_ttl_tier_action() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let table_dir = create_table_dir(db_root, "trades");
        create_partition(&table_dir, "2020-01-01", 100);

        let mut mgr = TtlManager::new(db_root.to_path_buf());
        mgr.register(TtlConfig {
            table: "trades".into(),
            max_age: Duration::from_secs(365 * 86400),
            action: TtlAction::Tier {
                target: StorageTier::Cold,
            },
            check_interval: Duration::from_secs(3600),
        });

        let stats = mgr.enforce().unwrap();
        assert_eq!(stats.partitions_expired, 1);

        // Original should be gone.
        assert!(!table_dir.join("2020-01-01").exists());

        // Cold tier should have the data.
        assert!(db_root
            .join("_tier_cold")
            .join("trades")
            .join("2020-01-01")
            .join("col.d")
            .exists());
    }

    #[test]
    fn test_ttl_no_expired_data() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let table_dir = create_table_dir(db_root, "trades");

        // Create only a recent partition.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
        create_partition(&table_dir, &today_name, 100);

        let mut mgr = TtlManager::new(db_root.to_path_buf());
        mgr.register(TtlConfig {
            table: "trades".into(),
            max_age: Duration::from_secs(365 * 86400),
            action: TtlAction::Delete,
            check_interval: Duration::from_secs(3600),
        });

        let stats = mgr.enforce().unwrap();
        assert_eq!(stats.partitions_expired, 0);
        assert_eq!(stats.bytes_affected, 0);
    }

    #[test]
    fn test_ttl_multiple_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let table1_dir = create_table_dir(db_root, "trades");
        let table2_dir = create_table_dir(db_root, "quotes");

        create_partition(&table1_dir, "2020-01-01", 100);
        create_partition(&table2_dir, "2020-06-01", 200);

        let mut mgr = TtlManager::new(db_root.to_path_buf());
        mgr.register(TtlConfig {
            table: "trades".into(),
            max_age: Duration::from_secs(365 * 86400),
            action: TtlAction::Delete,
            check_interval: Duration::from_secs(3600),
        });
        mgr.register(TtlConfig {
            table: "quotes".into(),
            max_age: Duration::from_secs(365 * 86400),
            action: TtlAction::Delete,
            check_interval: Duration::from_secs(3600),
        });

        let stats = mgr.enforce().unwrap();
        assert_eq!(stats.tables_processed, 2);
        assert_eq!(stats.partitions_expired, 2);

        assert!(!table1_dir.join("2020-01-01").exists());
        assert!(!table2_dir.join("2020-06-01").exists());
    }

    #[test]
    fn test_ttl_nonexistent_table() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let mut mgr = TtlManager::new(db_root.to_path_buf());
        mgr.register(TtlConfig {
            table: "nonexistent".into(),
            max_age: Duration::from_secs(3600),
            action: TtlAction::Delete,
            check_interval: Duration::from_secs(3600),
        });

        let stats = mgr.enforce().unwrap();
        assert_eq!(stats.partitions_expired, 0);
        assert_eq!(stats.tables_processed, 0);
    }

    #[test]
    fn test_parse_partition_timestamp_formats() {
        // Day
        let secs = parse_partition_timestamp("2024-03-15").unwrap();
        assert_eq!(secs, 1710460800);

        // Hour
        let secs = parse_partition_timestamp("2024-03-15T14").unwrap();
        assert_eq!(secs, 1710460800 + 14 * 3600);

        // Month
        let secs = parse_partition_timestamp("2024-03").unwrap();
        assert_eq!(secs, 1709251200);

        // Year
        let secs = parse_partition_timestamp("2024").unwrap();
        assert_eq!(secs, 1704067200);

        // Invalid
        assert!(parse_partition_timestamp("default").is_none());
        assert!(parse_partition_timestamp("_meta").is_none());
    }
}
