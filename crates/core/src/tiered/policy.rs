use crate::compression::{compress_column_file, decompress_column_file};
use crate::parquet::reader::ParquetReader;
use crate::parquet::writer::{ParquetColumn, ParquetType, ParquetWriter};
use crate::tiered::parquet::{parquet_to_partition, partition_to_parquet};
use crate::tiered::partition_meta::{PartitionTierInfo, load_tier_info, save_tier_info};
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::{ColumnType, PartitionBy};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Which storage tier a partition lives on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageTier {
    /// Native column files on local SSD -- fastest read/write.
    Hot,
    /// LZ4-compressed column files on local disk.
    Warm,
    /// Parquet-like files on local disk or object storage path.
    Cold,
}

impl std::fmt::Display for StorageTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageTier::Hot => write!(f, "Hot"),
            StorageTier::Warm => write!(f, "Warm"),
            StorageTier::Cold => write!(f, "Cold"),
        }
    }
}

/// Configuration for automatic tier transitions.
#[derive(Debug, Clone)]
pub struct TieringPolicy {
    /// Keep partitions hot for this long (e.g., 7 days).
    pub hot_retention: Duration,
    /// Keep warm for this long (e.g., 30 days).
    pub warm_retention: Duration,
    /// Path for cold storage (local or mount point).
    pub cold_storage_path: Option<PathBuf>,
    /// Whether to automatically tier partitions.
    pub auto_tier: bool,
}

impl TieringPolicy {
    /// A policy that keeps everything hot (no tiering).
    pub fn disabled() -> Self {
        Self {
            hot_retention: Duration::from_secs(u64::MAX / 2),
            warm_retention: Duration::from_secs(u64::MAX / 2),
            cold_storage_path: None,
            auto_tier: false,
        }
    }
}

/// Describes a planned transition of a partition between tiers.
#[derive(Debug, Clone)]
pub struct TierAction {
    pub partition: String,
    pub from: StorageTier,
    pub to: StorageTier,
}

/// Statistics from executing tier transitions.
#[derive(Debug, Clone, Copy, Default)]
pub struct TieringStats {
    pub partitions_moved: u32,
    pub bytes_saved: u64,
    pub bytes_moved: u64,
}

/// Manages tier transitions for a single table's partitions.
pub struct TieringManager {
    table_dir: PathBuf,
    policy: TieringPolicy,
    partition_by: PartitionBy,
}

impl TieringManager {
    pub fn new(table_dir: PathBuf, policy: TieringPolicy, partition_by: PartitionBy) -> Self {
        Self {
            table_dir,
            policy,
            partition_by,
        }
    }

    /// Evaluate which partitions should be moved between tiers based on the
    /// configured policy.
    pub fn evaluate(&self) -> Result<Vec<TierAction>> {
        if !self.policy.auto_tier {
            return Ok(Vec::new());
        }

        let tiers = self.partition_tiers()?;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let hot_cutoff = now_secs.saturating_sub(self.policy.hot_retention.as_secs());
        let warm_cutoff = now_secs.saturating_sub(self.policy.warm_retention.as_secs());

        let mut actions = Vec::new();

        for (partition_name, current_tier) in &tiers {
            let partition_secs = match parse_partition_timestamp(partition_name, self.partition_by)
            {
                Some(s) => s,
                None => continue,
            };

            let desired_tier = if partition_secs >= hot_cutoff {
                StorageTier::Hot
            } else if partition_secs >= warm_cutoff {
                StorageTier::Warm
            } else {
                StorageTier::Cold
            };

            if *current_tier != desired_tier {
                // Only allow forward transitions (Hot -> Warm -> Cold) automatically.
                let should_transition = match (current_tier, desired_tier) {
                    (StorageTier::Hot, StorageTier::Warm) => true,
                    (StorageTier::Hot, StorageTier::Cold) => true,
                    (StorageTier::Warm, StorageTier::Cold) => true,
                    _ => false, // Reverse transitions (recall) are manual only
                };

                if should_transition {
                    actions.push(TierAction {
                        partition: partition_name.clone(),
                        from: *current_tier,
                        to: desired_tier,
                    });
                }
            }
        }

        actions.sort_by(|a, b| a.partition.cmp(&b.partition));
        Ok(actions)
    }

    /// Execute tier transitions.
    pub fn apply(&self, actions: &[TierAction]) -> Result<TieringStats> {
        let mut stats = TieringStats::default();
        let mut tier_infos = load_tier_info(&self.table_dir)?;

        // Load table metadata for parquet operations.
        let meta_path = self.table_dir.join("_meta");
        let table_meta = if meta_path.exists() {
            Some(crate::table::TableMeta::load(&meta_path)?)
        } else {
            None
        };

        for action in actions {
            let partition_path = self.table_dir.join(&action.partition);

            let result = match (&action.from, &action.to) {
                (StorageTier::Hot, StorageTier::Warm) => {
                    self.transition_hot_to_warm(&partition_path)
                }
                (StorageTier::Hot, StorageTier::Cold) => {
                    // Two-step: Hot -> Warm -> Cold
                    self.transition_hot_to_warm(&partition_path)?;
                    let meta = table_meta.as_ref().ok_or_else(|| {
                        ExchangeDbError::Corruption("table _meta not found".to_string())
                    })?;
                    self.transition_warm_to_cold(&partition_path, &action.partition, meta)
                }
                (StorageTier::Warm, StorageTier::Cold) => {
                    let meta = table_meta.as_ref().ok_or_else(|| {
                        ExchangeDbError::Corruption("table _meta not found".to_string())
                    })?;
                    self.transition_warm_to_cold(&partition_path, &action.partition, meta)
                }
                (StorageTier::Cold, StorageTier::Hot) => {
                    let meta = table_meta.as_ref().ok_or_else(|| {
                        ExchangeDbError::Corruption("table _meta not found".to_string())
                    })?;
                    self.transition_cold_to_hot(&partition_path, &action.partition, meta)
                }
                (StorageTier::Cold, StorageTier::Warm) => {
                    // Cold -> Hot first, then compress to Warm
                    let meta = table_meta.as_ref().ok_or_else(|| {
                        ExchangeDbError::Corruption("table _meta not found".to_string())
                    })?;
                    self.transition_cold_to_hot(&partition_path, &action.partition, meta)?;
                    self.transition_hot_to_warm(&partition_path)
                }
                (StorageTier::Warm, StorageTier::Hot) => {
                    self.transition_warm_to_hot(&partition_path)
                }
                _ => Ok((0u64, 0u64)), // no-op for same tier
            };

            match result {
                Ok((bytes_before, bytes_after)) => {
                    stats.partitions_moved += 1;
                    stats.bytes_moved += bytes_before;
                    if bytes_before > bytes_after {
                        stats.bytes_saved += bytes_before - bytes_after;
                    }

                    // Determine the parquet path for cold tier info
                    let parquet_path = if action.to == StorageTier::Cold {
                        let cold_base = self
                            .policy
                            .cold_storage_path
                            .as_deref()
                            .unwrap_or(&self.table_dir);
                        Some(
                            cold_base
                                .join(format!("{}.parquet", action.partition))
                                .to_string_lossy()
                                .to_string(),
                        )
                    } else {
                        None
                    };

                    // Update tier info
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64;

                    let info = PartitionTierInfo {
                        partition_name: action.partition.clone(),
                        tier: action.to,
                        tier_changed_at: now,
                        compressed_size: bytes_after,
                        original_size: bytes_before,
                        parquet_path,
                    };

                    // Update or insert
                    if let Some(existing) = tier_infos
                        .iter_mut()
                        .find(|t| t.partition_name == action.partition)
                    {
                        *existing = info;
                    } else {
                        tier_infos.push(info);
                    }
                }
                Err(e) => return Err(e),
            }
        }

        save_tier_info(&self.table_dir, &tier_infos)?;
        Ok(stats)
    }

    /// Get current tier for each partition.
    pub fn partition_tiers(&self) -> Result<Vec<(String, StorageTier)>> {
        let tier_infos = load_tier_info(&self.table_dir)?;
        let mut result = Vec::new();

        // List all partition directories
        if self.table_dir.exists() {
            for entry in std::fs::read_dir(&self.table_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy().to_string();
                    if !name_str.starts_with('_') {
                        let tier = tier_infos
                            .iter()
                            .find(|t| t.partition_name == name_str)
                            .map(|t| t.tier)
                            .unwrap_or(StorageTier::Hot);
                        result.push((name_str, tier));
                    }
                }
            }
        }

        // Also include cold partitions that may not have directories on disk
        for info in &tier_infos {
            if info.tier == StorageTier::Cold && !result.iter().any(|r| r.0 == info.partition_name)
            {
                result.push((info.partition_name.clone(), StorageTier::Cold));
            }
        }

        // Scan _cold/ directory for XPQT files not already known
        let cold_dir = self.table_dir.join("_cold");
        if cold_dir.exists()
            && let Ok(entries) = std::fs::read_dir(&cold_dir)
        {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let pname = if name.ends_with(".parquet") {
                    Some(name.trim_end_matches(".parquet").to_string())
                } else if name.ends_with(".xpqt") {
                    Some(name.trim_end_matches(".xpqt").to_string())
                } else {
                    None
                };
                if let Some(partition_name) = pname
                    && !result.iter().any(|r| r.0 == partition_name)
                {
                    result.push((partition_name, StorageTier::Cold));
                }
            }
        }

        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    /// Convenience: get a reference to this manager's table directory.
    pub fn table_dir(&self) -> &Path {
        &self.table_dir
    }

    // -----------------------------------------------------------------------
    // Tier transition implementations
    // -----------------------------------------------------------------------

    /// Hot -> Warm: compress all .d column files with LZ4.
    /// Returns (original_size, compressed_size).
    fn transition_hot_to_warm(&self, partition_path: &Path) -> Result<(u64, u64)> {
        let mut original_total = 0u64;
        let mut compressed_total = 0u64;

        if !partition_path.exists() {
            return Ok((0, 0));
        }

        let entries: Vec<_> = std::fs::read_dir(partition_path)?
            .filter_map(|e| e.ok())
            .collect();

        for entry in entries {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Compress .d files (both fixed and variable data files) and .i index files
            if (name.ends_with(".d") || name.ends_with(".i")) && path.is_file() {
                let original_size = std::fs::metadata(&path)?.len();
                original_total += original_size;
                let compressed_size = compress_column_file(&path)?;
                compressed_total += compressed_size;
            }
        }

        Ok((original_total, compressed_total))
    }

    /// Warm -> Hot: decompress all .lz4 files back to native column files.
    /// Returns (compressed_size, decompressed_size).
    fn transition_warm_to_hot(&self, partition_path: &Path) -> Result<(u64, u64)> {
        let mut compressed_total = 0u64;
        let mut decompressed_total = 0u64;

        if !partition_path.exists() {
            return Ok((0, 0));
        }

        let entries: Vec<_> = std::fs::read_dir(partition_path)?
            .filter_map(|e| e.ok())
            .collect();

        for entry in entries {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            if name.ends_with(".lz4") && path.is_file() {
                let compressed_size = std::fs::metadata(&path)?.len();
                compressed_total += compressed_size;
                let decompressed_size = decompress_column_file(&path)?;
                decompressed_total += decompressed_size;
            }
        }

        Ok((compressed_total, decompressed_total))
    }

    /// Warm -> Cold: convert compressed column files to a single Parquet-like file.
    /// First decompresses, then writes Parquet, then removes partition dir.
    /// Returns (warm_size, cold_size).
    fn transition_warm_to_cold(
        &self,
        partition_path: &Path,
        partition_name: &str,
        meta: &crate::table::TableMeta,
    ) -> Result<(u64, u64)> {
        // First decompress warm files back to native (temporary)
        self.transition_warm_to_hot(partition_path)?;

        let warm_size = dir_size(partition_path)?;

        // Determine output path
        let cold_base = self
            .policy
            .cold_storage_path
            .as_deref()
            .unwrap_or(&self.table_dir);

        if !cold_base.exists() {
            std::fs::create_dir_all(cold_base)?;
        }

        // Use the new PAR1XCHG format for cold storage.
        let parquet_path = cold_base.join(format!("{partition_name}.parquet"));

        // Build schema from table metadata.
        let schema: Vec<ParquetColumn> = meta
            .columns
            .iter()
            .map(|c| {
                let ct: ColumnType = c.col_type.into();
                ParquetColumn {
                    name: c.name.clone(),
                    parquet_type: ParquetType::from_column_type(ct),
                    col_type: ct,
                }
            })
            .collect();

        let writer = ParquetWriter::new(&parquet_path, schema);
        let pstats = writer.write_partition(partition_path, meta)?;

        // Also store a copy in <table_dir>/_cold/ for partition discovery.
        let cold_subdir = self.table_dir.join("_cold");
        if !cold_subdir.exists() {
            std::fs::create_dir_all(&cold_subdir)?;
        }
        let cold_subdir_path = cold_subdir.join(format!("{partition_name}.parquet"));
        if cold_subdir_path != parquet_path {
            std::fs::copy(&parquet_path, &cold_subdir_path)?;
        }

        // Also write legacy XPQT for backward compatibility.
        let xpqt_path = cold_base.join(format!("{partition_name}.xpqt"));
        let _ = partition_to_parquet(partition_path, meta, &xpqt_path);
        let xpqt_subdir_path = cold_subdir.join(format!("{partition_name}.xpqt"));
        if xpqt_subdir_path != xpqt_path && xpqt_path.exists() {
            let _ = std::fs::copy(&xpqt_path, &xpqt_subdir_path);
        }

        // Remove partition directory
        std::fs::remove_dir_all(partition_path)?;

        Ok((warm_size, pstats.bytes_written))
    }

    /// Cold -> Hot: read Parquet file back into native column files.
    /// Tries the new PAR1XCHG format first, then falls back to legacy XPQT.
    /// Returns (cold_size, hot_size).
    fn transition_cold_to_hot(
        &self,
        partition_path: &Path,
        partition_name: &str,
        meta: &crate::table::TableMeta,
    ) -> Result<(u64, u64)> {
        let cold_base = self
            .policy
            .cold_storage_path
            .as_deref()
            .unwrap_or(&self.table_dir);

        // Try the new PAR1XCHG format first.
        let new_parquet_path = cold_base.join(format!("{partition_name}.parquet"));
        let legacy_parquet_path = cold_base.join(format!("{partition_name}.xpqt"));

        let (parquet_path, use_new_format) = if new_parquet_path.exists() {
            (new_parquet_path, true)
        } else if legacy_parquet_path.exists() {
            (legacy_parquet_path, false)
        } else {
            return Err(ExchangeDbError::Corruption(format!(
                "cold parquet file not found for partition '{partition_name}'"
            )));
        };

        let cold_size = std::fs::metadata(&parquet_path)?.len();

        // Recreate partition directory
        if !partition_path.exists() {
            std::fs::create_dir_all(partition_path)?;
        }

        if use_new_format {
            let reader = ParquetReader::open(&parquet_path)?;
            reader.to_partition(partition_path, meta)?;
        } else {
            parquet_to_partition(&parquet_path, meta, partition_path)?;
        }

        // Remove cold files (both formats if they exist).
        let _ = std::fs::remove_file(&parquet_path);
        let other = if use_new_format {
            cold_base.join(format!("{partition_name}.xpqt"))
        } else {
            cold_base.join(format!("{partition_name}.parquet"))
        };
        let _ = std::fs::remove_file(&other);

        let hot_size = dir_size(partition_path)?;

        Ok((cold_size, hot_size))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse a partition directory name into a Unix timestamp (seconds).
/// Re-uses the same logic as the retention module.
fn parse_partition_timestamp(name: &str, partition_by: PartitionBy) -> Option<u64> {
    match partition_by {
        PartitionBy::None => None,
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
            let parts: Vec<&str> = name.splitn(2, "-W").collect();
            if parts.len() != 2 {
                return None;
            }
            let year: i32 = parts[0].parse().ok()?;
            let week: u32 = parts[1].parse().ok()?;
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

/// Discover all partitions across all storage tiers for a given table
/// directory.
///
/// Returns a sorted list of `(path, StorageTier)` pairs:
/// - Hot/Warm partitions: path is the partition directory.
/// - Cold partitions: path is the `.xpqt` file (in `_cold/` or from tier metadata).
pub fn list_all_partitions(table_dir: &Path) -> Result<Vec<(PathBuf, StorageTier)>> {
    let tier_infos = load_tier_info(table_dir)?;
    let mut result = Vec::new();
    let mut seen_names = std::collections::HashSet::new();

    // 1. Scan regular partition directories (hot/warm).
    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.starts_with('_') {
                    let tier = tier_infos
                        .iter()
                        .find(|t| t.partition_name == name)
                        .map(|t| t.tier)
                        .unwrap_or_else(|| {
                            // Detect from contents
                            if let Ok(entries) = std::fs::read_dir(&path) {
                                for e in entries.flatten() {
                                    let n = e.file_name().to_string_lossy().to_string();
                                    if n.ends_with(".d.lz4") {
                                        return StorageTier::Warm;
                                    }
                                }
                            }
                            StorageTier::Hot
                        });
                    result.push((path, tier));
                    seen_names.insert(name);
                }
            }
        }
    }

    // 2. Scan _cold/ directory for XPQT and PAR1XCHG (.parquet) files.
    let cold_dir = table_dir.join("_cold");
    if cold_dir.exists()
        && let Ok(entries) = std::fs::read_dir(&cold_dir)
    {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let partition_name = if name.ends_with(".parquet") {
                Some(name.trim_end_matches(".parquet").to_string())
            } else if name.ends_with(".xpqt") {
                Some(name.trim_end_matches(".xpqt").to_string())
            } else {
                None
            };
            if let Some(pname) = partition_name
                && !seen_names.contains(&pname)
            {
                result.push((entry.path(), StorageTier::Cold));
                seen_names.insert(pname);
            }
        }
    }

    // 3. Include cold partitions from tier metadata that we haven't found yet.
    for info in &tier_infos {
        if info.tier == StorageTier::Cold
            && !seen_names.contains(&info.partition_name)
            && let Some(ref ppath) = info.parquet_path
        {
            let p = PathBuf::from(ppath);
            if p.exists() {
                result.push((p, StorageTier::Cold));
                seen_names.insert(info.partition_name.clone());
            }
        }
    }

    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
    use std::fs;
    use tempfile::tempdir;

    fn test_table_meta() -> TableMeta {
        TableMeta {
            name: "trades".to_string(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".to_string(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "price".to_string(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
                ColumnDef {
                    name: "volume".to_string(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        }
    }

    /// Helper: create a fake partition with column data files.
    fn create_partition_with_data(root: &Path, name: &str, num_rows: usize) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();

        // Write timestamp column (i64 = 8 bytes each)
        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1_710_460_800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        fs::write(dir.join("timestamp.d"), &ts_data).unwrap();

        // Write price column (f64 = 8 bytes each)
        let price_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (100.0 + i as f64 * 0.5).to_le_bytes())
            .collect();
        fs::write(dir.join("price.d"), &price_data).unwrap();

        // Write volume column (f64 = 8 bytes each)
        let volume_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (1.0 + i as f64 * 0.1).to_le_bytes())
            .collect();
        fs::write(dir.join("volume.d"), &volume_data).unwrap();

        dir
    }

    #[test]
    fn tier_evaluation_assigns_correct_tiers() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        // Create partitions with different ages
        // "Today" -- should stay hot
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
        create_partition_with_data(root, &today_name, 10);

        // "Old" partition -- should move to warm (> 7 days ago)
        create_partition_with_data(root, "2020-01-01", 10);

        // "Very old" partition -- should move to cold (> 30 days ago)
        create_partition_with_data(root, "2019-01-01", 10);

        let policy = TieringPolicy {
            hot_retention: Duration::from_secs(7 * 86400), // 7 days
            warm_retention: Duration::from_secs(30 * 86400), // 30 days
            cold_storage_path: None,
            auto_tier: true,
        };

        let mgr = TieringManager::new(root.to_path_buf(), policy, PartitionBy::Day);
        let actions = mgr.evaluate().unwrap();

        // Both old partitions should have actions (Hot -> Warm or Hot -> Cold)
        assert!(
            actions.len() >= 2,
            "expected at least 2 actions, got {}",
            actions.len()
        );

        // The 2019 partition should go to Cold
        let action_2019 = actions
            .iter()
            .find(|a| a.partition == "2019-01-01")
            .unwrap();
        assert_eq!(action_2019.from, StorageTier::Hot);
        assert_eq!(action_2019.to, StorageTier::Cold);

        // The 2020 partition should go to Cold too (both are > 30 days old)
        let action_2020 = actions
            .iter()
            .find(|a| a.partition == "2020-01-01")
            .unwrap();
        assert_eq!(action_2020.from, StorageTier::Hot);
        assert_eq!(action_2020.to, StorageTier::Cold);

        // Today's partition should NOT have any action
        assert!(actions.iter().all(|a| a.partition != today_name));
    }

    #[test]
    fn hot_to_warm_transition() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        // Create a partition with data
        let partition_path = create_partition_with_data(root, "2024-01-15", 100);

        // Record original sizes
        let original_ts_size = fs::metadata(partition_path.join("timestamp.d"))
            .unwrap()
            .len();
        assert!(original_ts_size > 0);

        let policy = TieringPolicy {
            hot_retention: Duration::from_secs(1),
            warm_retention: Duration::from_secs(86400 * 365 * 10),
            cold_storage_path: None,
            auto_tier: true,
        };

        let mgr = TieringManager::new(root.to_path_buf(), policy, PartitionBy::Day);

        let actions = vec![TierAction {
            partition: "2024-01-15".to_string(),
            from: StorageTier::Hot,
            to: StorageTier::Warm,
        }];

        let stats = mgr.apply(&actions).unwrap();
        assert_eq!(stats.partitions_moved, 1);

        // Verify .d files are gone and .lz4 files exist
        assert!(!partition_path.join("timestamp.d").exists());
        assert!(partition_path.join("timestamp.d.lz4").exists());
        assert!(!partition_path.join("price.d").exists());
        assert!(partition_path.join("price.d.lz4").exists());
    }

    #[test]
    fn warm_to_cold_transition() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        // Create and compress a partition (simulate warm state)
        let partition_path = create_partition_with_data(root, "2024-01-15", 100);

        let cold_dir = dir.path().join("cold_storage");
        fs::create_dir_all(&cold_dir).unwrap();

        let policy = TieringPolicy {
            hot_retention: Duration::from_secs(1),
            warm_retention: Duration::from_secs(1),
            cold_storage_path: Some(cold_dir.clone()),
            auto_tier: true,
        };

        let mgr = TieringManager::new(root.to_path_buf(), policy, PartitionBy::Day);

        // First go Hot -> Warm
        let actions = vec![TierAction {
            partition: "2024-01-15".to_string(),
            from: StorageTier::Hot,
            to: StorageTier::Warm,
        }];
        mgr.apply(&actions).unwrap();

        // Then Warm -> Cold
        let actions = vec![TierAction {
            partition: "2024-01-15".to_string(),
            from: StorageTier::Warm,
            to: StorageTier::Cold,
        }];
        let stats = mgr.apply(&actions).unwrap();
        assert_eq!(stats.partitions_moved, 1);

        // Partition directory should be gone
        assert!(!partition_path.exists());

        // Parquet file should exist in cold storage
        assert!(cold_dir.join("2024-01-15.xpqt").exists());
    }

    #[test]
    fn cold_to_hot_recall() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        let num_rows: usize = 50;
        let partition_path = create_partition_with_data(root, "2024-01-15", num_rows);

        // Read original data for verification
        let original_ts = fs::read(partition_path.join("timestamp.d")).unwrap();
        let original_price = fs::read(partition_path.join("price.d")).unwrap();
        let original_volume = fs::read(partition_path.join("volume.d")).unwrap();

        let cold_dir = dir.path().join("cold_storage");
        fs::create_dir_all(&cold_dir).unwrap();

        let policy = TieringPolicy {
            hot_retention: Duration::from_secs(1),
            warm_retention: Duration::from_secs(1),
            cold_storage_path: Some(cold_dir.clone()),
            auto_tier: true,
        };

        let mgr = TieringManager::new(root.to_path_buf(), policy, PartitionBy::Day);

        // Move Hot -> Cold (two-step)
        let actions = vec![TierAction {
            partition: "2024-01-15".to_string(),
            from: StorageTier::Hot,
            to: StorageTier::Cold,
        }];
        mgr.apply(&actions).unwrap();
        assert!(!partition_path.exists());
        assert!(cold_dir.join("2024-01-15.xpqt").exists());

        // Recall Cold -> Hot
        let actions = vec![TierAction {
            partition: "2024-01-15".to_string(),
            from: StorageTier::Cold,
            to: StorageTier::Hot,
        }];
        let stats = mgr.apply(&actions).unwrap();
        assert_eq!(stats.partitions_moved, 1);

        // Partition should be restored
        assert!(partition_path.exists());
        assert!(!cold_dir.join("2024-01-15.xpqt").exists());

        // Verify data matches original
        let restored_ts = fs::read(partition_path.join("timestamp.d")).unwrap();
        let restored_price = fs::read(partition_path.join("price.d")).unwrap();
        let restored_volume = fs::read(partition_path.join("volume.d")).unwrap();

        assert_eq!(
            original_ts, restored_ts,
            "timestamp data mismatch after round-trip"
        );
        assert_eq!(
            original_price, restored_price,
            "price data mismatch after round-trip"
        );
        assert_eq!(
            original_volume, restored_volume,
            "volume data mismatch after round-trip"
        );
    }

    #[test]
    fn disabled_policy_produces_no_actions() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        create_partition_with_data(root, "2019-01-01", 10);

        let policy = TieringPolicy::disabled();
        let mgr = TieringManager::new(root.to_path_buf(), policy, PartitionBy::Day);
        let actions = mgr.evaluate().unwrap();
        assert!(actions.is_empty());
    }

    #[test]
    fn partition_tiers_default_to_hot() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        create_partition_with_data(root, "2024-01-01", 5);
        create_partition_with_data(root, "2024-01-02", 5);

        let policy = TieringPolicy::disabled();
        let mgr = TieringManager::new(root.to_path_buf(), policy, PartitionBy::Day);
        let tiers = mgr.partition_tiers().unwrap();

        assert_eq!(tiers.len(), 2);
        assert!(tiers.iter().all(|(_, t)| *t == StorageTier::Hot));
    }
}
