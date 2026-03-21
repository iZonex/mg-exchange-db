use exchange_common::error::Result;
use exchange_common::types::{PartitionBy, Timestamp};
use std::path::{Path, PathBuf};

/// Calculates the partition directory name for a given timestamp.
pub fn partition_dir(ts: Timestamp, partition_by: PartitionBy) -> String {
    use std::time::{Duration, UNIX_EPOCH};

    if partition_by == PartitionBy::None {
        return "default".to_string();
    }

    let nanos = ts.as_nanos();
    let secs = (nanos / 1_000_000_000) as u64;
    let system_time = UNIX_EPOCH + Duration::from_secs(secs);
    let datetime: chrono_lite::DateTime = system_time.into();

    match partition_by {
        PartitionBy::None => "default".to_string(),
        PartitionBy::Year => format!("{:04}", datetime.year),
        PartitionBy::Month => format!("{:04}-{:02}", datetime.year, datetime.month),
        PartitionBy::Day => format!(
            "{:04}-{:02}-{:02}",
            datetime.year, datetime.month, datetime.day
        ),
        PartitionBy::Hour => format!(
            "{:04}-{:02}-{:02}T{:02}",
            datetime.year, datetime.month, datetime.day, datetime.hour
        ),
        PartitionBy::Week => {
            // ISO week
            let week = datetime.day / 7 + 1;
            format!("{:04}-W{:02}", datetime.year, week)
        }
    }
}

/// Minimal datetime extraction without pulling in chrono.
mod chrono_lite {
    use std::time::SystemTime;

    pub struct DateTime {
        pub year: i32,
        pub month: u32,
        pub day: u32,
        pub hour: u32,
    }

    impl From<SystemTime> for DateTime {
        fn from(st: SystemTime) -> Self {
            let secs = st
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Days since epoch
            let days = (secs / 86400) as i64;
            let time_of_day = secs % 86400;
            let hour = (time_of_day / 3600) as u32;

            // Civil date from days since epoch (algorithm from Howard Hinnant)
            let z = days + 719468;
            let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
            let doe = (z - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i64 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };

            Self {
                year: y as i32,
                month: m,
                day: d,
                hour,
            }
        }
    }
}

/// Manages the set of partitions for a table.
pub struct PartitionManager {
    root: PathBuf,
    partition_by: PartitionBy,
    /// Cached last partition directory name to avoid repeated date formatting
    /// and filesystem checks when writing rows in timestamp order.
    cached_partition_name: Option<String>,
    cached_partition_path: Option<PathBuf>,
    /// Cached timestamp range (lo inclusive, hi exclusive) for the current
    /// partition.  When a timestamp falls within this range we can skip the
    /// date-formatting step entirely (pure integer comparison).
    cached_ts_lo: i64,
    cached_ts_hi: i64,
}

/// Compute the nanosecond boundaries [lo, hi) for the partition that
/// contains `ts_nanos`.
fn partition_bounds(ts_nanos: i64, partition_by: PartitionBy) -> (i64, i64) {
    const NANOS_PER_SEC: i64 = 1_000_000_000;
    const SECS_PER_HOUR: i64 = 3600;
    const SECS_PER_DAY: i64 = 86400;

    if partition_by == PartitionBy::None {
        return (i64::MIN, i64::MAX);
    }

    let secs = ts_nanos / NANOS_PER_SEC;
    let days_since_epoch = secs.div_euclid(SECS_PER_DAY);

    match partition_by {
        PartitionBy::None => (i64::MIN, i64::MAX),
        PartitionBy::Hour => {
            let hour_start_secs = secs.div_euclid(SECS_PER_HOUR) * SECS_PER_HOUR;
            (hour_start_secs * NANOS_PER_SEC, (hour_start_secs + SECS_PER_HOUR) * NANOS_PER_SEC)
        }
        PartitionBy::Day => {
            let day_start_secs = days_since_epoch * SECS_PER_DAY;
            (day_start_secs * NANOS_PER_SEC, (day_start_secs + SECS_PER_DAY) * NANOS_PER_SEC)
        }
        PartitionBy::Week => {
            // ISO weeks: epoch (1970-01-01) was a Thursday (day 4).
            // We want Monday-based weeks.
            let dow = (days_since_epoch + 3).rem_euclid(7); // 0 = Monday
            let week_start_days = days_since_epoch - dow;
            let week_start_secs = week_start_days * SECS_PER_DAY;
            (week_start_secs * NANOS_PER_SEC, (week_start_secs + 7 * SECS_PER_DAY) * NANOS_PER_SEC)
        }
        PartitionBy::Month => {
            // Use the chrono_lite algorithm to get year/month, then compute start of month and next month.
            let z = days_since_epoch + 719468;
            let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
            let doe = (z - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i64 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };

            let month_start = civil_to_days(y, m, 1);
            let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
            let next_month_start = civil_to_days(ny, nm, 1);

            (month_start * SECS_PER_DAY * NANOS_PER_SEC, next_month_start * SECS_PER_DAY * NANOS_PER_SEC)
        }
        PartitionBy::Year => {
            let z = days_since_epoch + 719468;
            let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
            let doe = (z - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i64 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };

            let year_start = civil_to_days(y, 1, 1);
            let next_year_start = civil_to_days(y + 1, 1, 1);
            (year_start * SECS_PER_DAY * NANOS_PER_SEC, next_year_start * SECS_PER_DAY * NANOS_PER_SEC)
        }
    }
}

/// Convert a civil date to days since Unix epoch (inverse of Hinnant's algorithm).
fn civil_to_days(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u32;
    let m_adj = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * m_adj + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

impl PartitionManager {
    pub fn new(root: PathBuf, partition_by: PartitionBy) -> Self {
        Self {
            root,
            partition_by,
            cached_partition_name: None,
            cached_partition_path: None,
            cached_ts_lo: 0,
            cached_ts_hi: 0,
        }
    }

    /// Get the directory path for a given timestamp's partition.
    pub fn partition_path(&self, ts: Timestamp) -> PathBuf {
        let dir_name = partition_dir(ts, self.partition_by);
        self.root.join(dir_name)
    }

    /// Ensure partition directory exists, creating it if needed.
    ///
    /// Uses an internal cache with timestamp-range comparison to skip the
    /// date formatting and filesystem existence check when consecutive
    /// timestamps map to the same partition (the common case for
    /// time-ordered inserts).  The fast path is a single integer comparison
    /// instead of formatting a date string.
    #[inline]
    pub fn ensure_partition(&mut self, ts: Timestamp) -> Result<PathBuf> {
        let ts_nanos = ts.as_nanos();

        // Fast path: timestamp is within the cached partition's time range.
        // This avoids date formatting entirely — just two integer compares.
        if ts_nanos >= self.cached_ts_lo && ts_nanos < self.cached_ts_hi {
            // SAFETY: cached_partition_path is always Some when bounds are valid.
            return Ok(self.cached_partition_path.clone().unwrap());
        }

        // Slow path: format partition name, create directory if needed.
        let dir_name = partition_dir(ts, self.partition_by);
        let path = self.root.join(&dir_name);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        // Cache the timestamp bounds for this partition.
        let (lo, hi) = partition_bounds(ts_nanos, self.partition_by);
        self.cached_ts_lo = lo;
        self.cached_ts_hi = hi;
        self.cached_partition_name = Some(dir_name);
        self.cached_partition_path = Some(path.clone());
        Ok(path)
    }

    /// List existing partition directories sorted by name.
    pub fn list_partitions(&self) -> Result<Vec<PathBuf>> {
        let mut partitions = Vec::new();
        if self.root.exists() {
            for entry in std::fs::read_dir(&self.root)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    // Skip internal directories
                    if !name_str.starts_with('_') {
                        partitions.push(path);
                    }
                }
            }
            partitions.sort();
        }
        Ok(partitions)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn partition_by(&self) -> PartitionBy {
        self.partition_by
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_dir_names() {
        // 2024-03-15 14:30:00 UTC in nanos
        let ts = Timestamp::from_secs(1710513000);

        assert_eq!(partition_dir(ts, PartitionBy::None), "default");
        assert_eq!(partition_dir(ts, PartitionBy::Year), "2024");
        assert_eq!(partition_dir(ts, PartitionBy::Month), "2024-03");
        assert_eq!(partition_dir(ts, PartitionBy::Day), "2024-03-15");
        assert_eq!(partition_dir(ts, PartitionBy::Hour), "2024-03-15T14");
    }
}
