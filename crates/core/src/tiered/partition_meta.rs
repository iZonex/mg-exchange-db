//! Per-table tier metadata, persisted as a JSON file (`_tier_info`).

use crate::tiered::policy::StorageTier;
use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Tier information for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionTierInfo {
    pub partition_name: String,
    pub tier: StorageTier,
    /// Unix timestamp (seconds) when the tier was last changed.
    pub tier_changed_at: i64,
    /// Size on disk after tier transition.
    pub compressed_size: u64,
    /// Original (uncompressed) size.
    pub original_size: u64,
    /// Path to the parquet file if cold.
    pub parquet_path: Option<String>,
}

const TIER_INFO_FILE: &str = "_tier_info";

/// Load tier info for all partitions of a table.
/// Returns an empty vec if the file does not exist.
pub fn load_tier_info(table_dir: &Path) -> Result<Vec<PartitionTierInfo>> {
    let path = table_dir.join(TIER_INFO_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let json = std::fs::read_to_string(&path)?;
    let infos: Vec<PartitionTierInfo> =
        serde_json::from_str(&json).map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    Ok(infos)
}

/// Save tier info for all partitions of a table.
pub fn save_tier_info(table_dir: &Path, infos: &[PartitionTierInfo]) -> Result<()> {
    let path = table_dir.join(TIER_INFO_FILE);
    let json = serde_json::to_string_pretty(infos)
        .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    std::fs::write(&path, json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn save_and_load_tier_info() {
        let dir = tempdir().unwrap();

        let infos = vec![
            PartitionTierInfo {
                partition_name: "2024-01-01".to_string(),
                tier: StorageTier::Hot,
                tier_changed_at: 1710460800,
                compressed_size: 0,
                original_size: 1000,
                parquet_path: None,
            },
            PartitionTierInfo {
                partition_name: "2024-01-02".to_string(),
                tier: StorageTier::Warm,
                tier_changed_at: 1710460800,
                compressed_size: 500,
                original_size: 1000,
                parquet_path: None,
            },
            PartitionTierInfo {
                partition_name: "2024-01-03".to_string(),
                tier: StorageTier::Cold,
                tier_changed_at: 1710460800,
                compressed_size: 300,
                original_size: 1000,
                parquet_path: Some("/cold/2024-01-03.xpqt".to_string()),
            },
        ];

        save_tier_info(dir.path(), &infos).unwrap();
        let loaded = load_tier_info(dir.path()).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].partition_name, "2024-01-01");
        assert_eq!(loaded[0].tier, StorageTier::Hot);
        assert_eq!(loaded[1].tier, StorageTier::Warm);
        assert_eq!(loaded[2].tier, StorageTier::Cold);
        assert_eq!(
            loaded[2].parquet_path,
            Some("/cold/2024-01-03.xpqt".to_string())
        );
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let dir = tempdir().unwrap();
        let infos = load_tier_info(dir.path()).unwrap();
        assert!(infos.is_empty());
    }

    #[test]
    fn overwrite_tier_info() {
        let dir = tempdir().unwrap();

        let initial = vec![PartitionTierInfo {
            partition_name: "2024-01-01".to_string(),
            tier: StorageTier::Hot,
            tier_changed_at: 1710460800,
            compressed_size: 0,
            original_size: 1000,
            parquet_path: None,
        }];

        save_tier_info(dir.path(), &initial).unwrap();

        let updated = vec![
            PartitionTierInfo {
                partition_name: "2024-01-01".to_string(),
                tier: StorageTier::Warm,
                tier_changed_at: 1710547200,
                compressed_size: 500,
                original_size: 1000,
                parquet_path: None,
            },
            PartitionTierInfo {
                partition_name: "2024-01-02".to_string(),
                tier: StorageTier::Hot,
                tier_changed_at: 1710547200,
                compressed_size: 0,
                original_size: 800,
                parquet_path: None,
            },
        ];

        save_tier_info(dir.path(), &updated).unwrap();
        let loaded = load_tier_info(dir.path()).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].tier, StorageTier::Warm);
    }
}
