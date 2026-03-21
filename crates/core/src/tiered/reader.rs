//! Transparent partition reader that handles hot, warm, and cold tiers.
//!
//! `TieredPartitionReader` detects the storage tier of a partition and
//! provides a path from which native `.d` column files can be read,
//! regardless of the original tier.
//!
//! - **Hot**: column files are `.d` -- read directly, no conversion.
//! - **Warm**: column files are `.d.lz4` -- decompressed to a temp dir.
//! - **Cold**: partition is a single `.xpqt` file -- converted to native
//!   column files in a temp dir.
//!
//! The temporary directory (if any) is cleaned up when the reader is dropped.

use crate::compression::decompress_column_file;
use crate::table::TableMeta;
use crate::tiered::parquet::parquet_to_partition;
use crate::tiered::partition_meta::load_tier_info;
use crate::tiered::policy::StorageTier;
use exchange_common::error::{ExchangeDbError, Result};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Reads data from a partition regardless of its storage tier.
///
/// Automatically decompresses warm (LZ4) and converts cold (XPQT) to
/// temporary native format. The temp directory is cleaned up on drop.
pub struct TieredPartitionReader {
    tier: StorageTier,
    #[allow(dead_code)]
    partition_path: PathBuf,
    /// For warm and cold partitions: a temp directory holding native column files.
    /// Held so that its `Drop` impl cleans up the temp directory.
    #[allow(dead_code)]
    temp_dir: Option<TempDir>,
    /// The path to read native `.d` column files from.
    native_path: PathBuf,
}

impl TieredPartitionReader {
    /// Detect the tier of a partition and prepare it for reading.
    ///
    /// `partition_path` is the path to the partition directory (for hot/warm)
    /// or the `.xpqt` file (for cold).
    ///
    /// `table_dir` is the parent table directory, used to look up tier
    /// metadata and table meta.
    pub fn open(partition_path: &Path, table_dir: &Path) -> Result<Self> {
        let tier = detect_tier(partition_path, table_dir)?;

        match tier {
            StorageTier::Hot => Ok(Self {
                tier,
                native_path: partition_path.to_path_buf(),
                partition_path: partition_path.to_path_buf(),
                temp_dir: None,
            }),
            StorageTier::Warm => {
                let temp_dir = TempDir::new().map_err(|e| {
                    ExchangeDbError::Io(std::io::Error::other(e))
                })?;
                let native_path = temp_dir.path().to_path_buf();

                // Copy .lz4 files to temp and decompress them there.
                decompress_warm_to_dir(partition_path, &native_path)?;

                Ok(Self {
                    tier,
                    partition_path: partition_path.to_path_buf(),
                    temp_dir: Some(temp_dir),
                    native_path,
                })
            }
            StorageTier::Cold => {
                let temp_dir = TempDir::new().map_err(|e| {
                    ExchangeDbError::Io(std::io::Error::other(e))
                })?;
                let native_path = temp_dir.path().to_path_buf();

                // The partition_path for cold is the .xpqt file itself.
                let xpqt_path = if partition_path.extension().and_then(|e| e.to_str())
                    == Some("xpqt")
                {
                    partition_path.to_path_buf()
                } else {
                    // Try to find the XPQT file via _cold/ or tier metadata.
                    find_cold_xpqt(partition_path, table_dir)?
                };

                let meta_path = table_dir.join("_meta");
                let table_meta = TableMeta::load(&meta_path)?;
                recall_cold_partition(&xpqt_path, &table_meta, &native_path)?;

                Ok(Self {
                    tier,
                    partition_path: partition_path.to_path_buf(),
                    temp_dir: Some(temp_dir),
                    native_path,
                })
            }
        }
    }

    /// Get the path to read native column files from.
    ///
    /// - For hot: returns the partition path directly.
    /// - For warm: returns a temp directory with decompressed files.
    /// - For cold: returns a temp directory with converted native files.
    pub fn native_path(&self) -> &Path {
        &self.native_path
    }

    /// The detected storage tier of this partition.
    pub fn tier(&self) -> StorageTier {
        self.tier
    }
}

// The TempDir inside is dropped automatically, cleaning up temp files.

// ---------------------------------------------------------------------------
// Detection logic
// ---------------------------------------------------------------------------

/// Detect the storage tier of a partition.
///
/// Detection order:
/// 1. If the path is a `.xpqt` file -> Cold.
/// 2. If the partition dir contains `.d.lz4` files -> Warm.
/// 3. If the partition dir contains `.xpqt` files -> Cold.
/// 4. If the partition dir contains `.d` files -> Hot.
/// 5. Fall back to tier metadata (`_tier_info`).
fn detect_tier(partition_path: &Path, table_dir: &Path) -> Result<StorageTier> {
    // If the path itself is an XPQT file, it's cold.
    if partition_path.extension().and_then(|e| e.to_str()) == Some("xpqt") {
        return Ok(StorageTier::Cold);
    }

    if !partition_path.is_dir() {
        // Not a directory and not an xpqt file -- check tier metadata.
        return tier_from_metadata(partition_path, table_dir);
    }

    let mut has_lz4 = false;
    let mut has_d = false;
    let mut has_xpqt = false;

    if let Ok(entries) = std::fs::read_dir(partition_path) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".d.lz4") {
                has_lz4 = true;
            } else if name.ends_with(".d") {
                has_d = true;
            } else if name.ends_with(".xpqt") {
                has_xpqt = true;
            }
        }
    }

    if has_lz4 {
        return Ok(StorageTier::Warm);
    }
    if has_xpqt {
        return Ok(StorageTier::Cold);
    }
    if has_d {
        return Ok(StorageTier::Hot);
    }

    // Fall back to metadata.
    tier_from_metadata(partition_path, table_dir)
}

/// Look up the tier from the `_tier_info` metadata file.
fn tier_from_metadata(partition_path: &Path, table_dir: &Path) -> Result<StorageTier> {
    let partition_name = partition_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let tier_infos = load_tier_info(table_dir)?;
    for info in &tier_infos {
        if info.partition_name == partition_name {
            return Ok(info.tier);
        }
    }

    // Default to Hot if no metadata found.
    Ok(StorageTier::Hot)
}

// ---------------------------------------------------------------------------
// Warm decompression
// ---------------------------------------------------------------------------

/// Copy all files from a warm partition directory into `output_dir`,
/// decompressing `.lz4` files and copying non-lz4 files as-is.
fn decompress_warm_to_dir(partition_path: &Path, output_dir: &Path) -> Result<()> {
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)?;
    }

    for entry in std::fs::read_dir(partition_path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        let src = entry.path();

        if name.ends_with(".lz4") && src.is_file() {
            // Copy the .lz4 file to temp, then decompress it there.
            let dest_lz4 = output_dir.join(&name);
            std::fs::copy(&src, &dest_lz4)?;
            decompress_column_file(&dest_lz4)?;
        } else if src.is_file() {
            // Copy other files (metadata, etc.) as-is.
            let dest = output_dir.join(&name);
            std::fs::copy(&src, &dest)?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Cold recall
// ---------------------------------------------------------------------------

/// Find the XPQT file for a cold partition.
///
/// Checks:
/// 1. `<table_dir>/_cold/<partition_name>.xpqt`
/// 2. `<table_dir>/<partition_name>.xpqt`
/// 3. Tier metadata `parquet_path` field.
fn find_cold_xpqt(partition_path: &Path, table_dir: &Path) -> Result<PathBuf> {
    let partition_name = partition_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    // Check _cold/ subdirectory
    let cold_path = table_dir
        .join("_cold")
        .join(format!("{partition_name}.xpqt"));
    if cold_path.exists() {
        return Ok(cold_path);
    }

    // Check table_dir directly
    let direct_path = table_dir.join(format!("{partition_name}.xpqt"));
    if direct_path.exists() {
        return Ok(direct_path);
    }

    // Check tier metadata
    let tier_infos = load_tier_info(table_dir)?;
    for info in &tier_infos {
        if info.partition_name == partition_name
            && let Some(ref ppath) = info.parquet_path {
                let p = PathBuf::from(ppath);
                if p.exists() {
                    return Ok(p);
                }
            }
    }

    Err(ExchangeDbError::Corruption(format!(
        "cold XPQT file not found for partition '{partition_name}'"
    )))
}

/// Temporarily convert a cold partition (XPQT) to native column files
/// in the given output directory for querying.
///
/// The caller is responsible for cleaning up the output directory when done
/// (typically via a `TempDir`).
pub fn recall_cold_partition(
    xpqt_path: &Path,
    meta: &TableMeta,
    output_dir: &Path,
) -> Result<u64> {
    if !xpqt_path.exists() {
        return Err(ExchangeDbError::Corruption(format!(
            "cold XPQT file not found: {}",
            xpqt_path.display()
        )));
    }

    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)?;
    }

    parquet_to_partition(xpqt_path, meta, output_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::compress_column_file;
    use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
    use crate::tiered::parquet::partition_to_parquet;
    use crate::tiered::partition_meta::{save_tier_info, PartitionTierInfo};
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

    fn create_partition_with_data(root: &Path, name: &str, num_rows: usize) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir_all(&dir).unwrap();

        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1710460800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        fs::write(dir.join("timestamp.d"), &ts_data).unwrap();

        let price_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (100.0 + i as f64 * 0.5).to_le_bytes())
            .collect();
        fs::write(dir.join("price.d"), &price_data).unwrap();

        let volume_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (1.0 + i as f64 * 0.1).to_le_bytes())
            .collect();
        fs::write(dir.join("volume.d"), &volume_data).unwrap();

        dir
    }

    fn compress_partition(partition_path: &Path) {
        let entries: Vec<_> = fs::read_dir(partition_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".d") && path.is_file() {
                compress_column_file(&path).unwrap();
            }
        }
    }

    #[test]
    fn reader_hot_partition() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        let partition_path = create_partition_with_data(root, "2024-01-15", 50);

        let reader = TieredPartitionReader::open(&partition_path, root).unwrap();
        assert_eq!(reader.tier(), StorageTier::Hot);
        assert_eq!(reader.native_path(), partition_path);

        // Verify .d files exist at native_path
        assert!(reader.native_path().join("timestamp.d").exists());
        assert!(reader.native_path().join("price.d").exists());
        assert!(reader.native_path().join("volume.d").exists());
    }

    #[test]
    fn reader_warm_partition() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        let partition_path = create_partition_with_data(root, "2024-01-15", 50);

        // Read original data for comparison
        let original_ts = fs::read(partition_path.join("timestamp.d")).unwrap();
        let original_price = fs::read(partition_path.join("price.d")).unwrap();

        // Compress to simulate warm tier
        compress_partition(&partition_path);
        assert!(partition_path.join("timestamp.d.lz4").exists());
        assert!(!partition_path.join("timestamp.d").exists());

        let reader = TieredPartitionReader::open(&partition_path, root).unwrap();
        assert_eq!(reader.tier(), StorageTier::Warm);
        // native_path should be a temp directory, not the original
        assert_ne!(reader.native_path(), partition_path);

        // Verify decompressed .d files exist at native_path
        assert!(reader.native_path().join("timestamp.d").exists());
        assert!(reader.native_path().join("price.d").exists());
        assert!(reader.native_path().join("volume.d").exists());

        // Verify data matches original
        let restored_ts = fs::read(reader.native_path().join("timestamp.d")).unwrap();
        let restored_price = fs::read(reader.native_path().join("price.d")).unwrap();
        assert_eq!(original_ts, restored_ts);
        assert_eq!(original_price, restored_price);
    }

    #[test]
    fn reader_cold_partition_xpqt_file() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        let partition_path = create_partition_with_data(root, "2024-01-15", 50);

        // Read original data for comparison
        let original_ts = fs::read(partition_path.join("timestamp.d")).unwrap();
        let original_price = fs::read(partition_path.join("price.d")).unwrap();

        // Convert to XPQT (cold)
        let xpqt_path = root.join("2024-01-15.xpqt");
        partition_to_parquet(&partition_path, &meta, &xpqt_path).unwrap();

        // Remove partition directory (simulating cold state)
        fs::remove_dir_all(&partition_path).unwrap();

        // Open reader with the XPQT path directly
        let reader = TieredPartitionReader::open(&xpqt_path, root).unwrap();
        assert_eq!(reader.tier(), StorageTier::Cold);

        // Verify native files were created in temp dir
        assert!(reader.native_path().join("timestamp.d").exists());
        assert!(reader.native_path().join("price.d").exists());
        assert!(reader.native_path().join("volume.d").exists());

        // Verify data matches original
        let restored_ts = fs::read(reader.native_path().join("timestamp.d")).unwrap();
        let restored_price = fs::read(reader.native_path().join("price.d")).unwrap();
        assert_eq!(original_ts, restored_ts);
        assert_eq!(original_price, restored_price);
    }

    #[test]
    fn reader_cold_partition_via_cold_dir() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        let partition_path = create_partition_with_data(root, "2024-01-15", 30);

        // Convert to XPQT in _cold/ directory
        let cold_dir = root.join("_cold");
        fs::create_dir_all(&cold_dir).unwrap();
        let xpqt_path = cold_dir.join("2024-01-15.xpqt");
        partition_to_parquet(&partition_path, &meta, &xpqt_path).unwrap();

        // Remove original partition
        fs::remove_dir_all(&partition_path).unwrap();

        // Save tier info so the reader can find the cold partition
        save_tier_info(
            root,
            &[PartitionTierInfo {
                partition_name: "2024-01-15".to_string(),
                tier: StorageTier::Cold,
                tier_changed_at: 0,
                compressed_size: 0,
                original_size: 0,
                parquet_path: Some(xpqt_path.to_string_lossy().to_string()),
            }],
        )
        .unwrap();

        // Open with the partition name path (not the xpqt path)
        let reader =
            TieredPartitionReader::open(&root.join("2024-01-15"), root).unwrap();
        assert_eq!(reader.tier(), StorageTier::Cold);

        assert!(reader.native_path().join("timestamp.d").exists());
        assert!(reader.native_path().join("price.d").exists());
    }

    #[test]
    fn cold_partition_recall_creates_temp_files() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        let partition_path = create_partition_with_data(root, "2024-01-15", 20);

        let xpqt_path = root.join("cold_test.xpqt");
        partition_to_parquet(&partition_path, &meta, &xpqt_path).unwrap();

        let temp_out = tempdir().unwrap();
        let rows = recall_cold_partition(&xpqt_path, &meta, temp_out.path()).unwrap();
        assert_eq!(rows, 20);
        assert!(temp_out.path().join("timestamp.d").exists());
        assert!(temp_out.path().join("price.d").exists());
        assert!(temp_out.path().join("volume.d").exists());
    }

    #[test]
    fn reader_cleanup_removes_temp_files_on_drop() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        let partition_path = create_partition_with_data(root, "2024-01-15", 10);
        compress_partition(&partition_path);

        let temp_native_path;
        {
            let reader = TieredPartitionReader::open(&partition_path, root).unwrap();
            assert_eq!(reader.tier(), StorageTier::Warm);
            temp_native_path = reader.native_path().to_path_buf();
            assert!(temp_native_path.exists());
        }
        // After dropping the reader, the temp dir should be cleaned up.
        assert!(!temp_native_path.exists());
    }

    #[test]
    fn query_spanning_hot_and_warm_partitions() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        // Create a hot partition
        let hot_path = create_partition_with_data(root, "2024-01-15", 10);

        // Create a warm partition
        let warm_path = create_partition_with_data(root, "2024-01-16", 10);
        let original_warm_ts = fs::read(warm_path.join("timestamp.d")).unwrap();
        compress_partition(&warm_path);

        // Read hot partition directly
        let hot_reader = TieredPartitionReader::open(&hot_path, root).unwrap();
        assert_eq!(hot_reader.tier(), StorageTier::Hot);

        // Read warm partition (decompressed to temp)
        let warm_reader = TieredPartitionReader::open(&warm_path, root).unwrap();
        assert_eq!(warm_reader.tier(), StorageTier::Warm);

        // Both should have readable .d files
        let hot_ts = fs::read(hot_reader.native_path().join("timestamp.d")).unwrap();
        let warm_ts = fs::read(warm_reader.native_path().join("timestamp.d")).unwrap();

        assert_eq!(hot_ts.len(), 10 * 8); // 10 i64 values
        assert_eq!(warm_ts.len(), 10 * 8);
        assert_eq!(warm_ts, original_warm_ts);
    }

    #[test]
    fn query_spanning_hot_and_cold_partitions() {
        let dir = tempdir().unwrap();
        let root = dir.path();

        let meta = test_table_meta();
        meta.save(&root.join("_meta")).unwrap();

        // Create a hot partition
        let _hot_path = create_partition_with_data(root, "2024-01-15", 10);

        // Create a cold partition
        let cold_src = create_partition_with_data(root, "2024-01-10", 15);
        let original_ts = fs::read(cold_src.join("timestamp.d")).unwrap();

        let cold_dir = root.join("_cold");
        fs::create_dir_all(&cold_dir).unwrap();
        let xpqt_path = cold_dir.join("2024-01-10.xpqt");
        partition_to_parquet(&cold_src, &meta, &xpqt_path).unwrap();
        fs::remove_dir_all(&cold_src).unwrap();

        // Read cold partition
        let cold_reader = TieredPartitionReader::open(&xpqt_path, root).unwrap();
        assert_eq!(cold_reader.tier(), StorageTier::Cold);

        let cold_ts = fs::read(cold_reader.native_path().join("timestamp.d")).unwrap();
        assert_eq!(cold_ts, original_ts);
    }
}
