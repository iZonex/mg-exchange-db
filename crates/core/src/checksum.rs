//! Partition checksums for data integrity verification.
//!
//! Uses a simple 64-bit hash (FNV-1a variant) computed over all column file
//! contents in a partition directory.

use exchange_common::error::{ExchangeDbError, Result};
use std::path::Path;

const CHECKSUM_FILE: &str = ".checksum";

/// Result of verifying all partitions in a table.
#[derive(Debug)]
pub struct VerifyResult {
    pub partitions_checked: u32,
    pub partitions_ok: u32,
    pub partitions_corrupt: Vec<String>,
}

/// Calculate a checksum over all column files (`.d`, `.i`, `.k`, `.v`) in a
/// partition directory.
pub fn partition_checksum(partition_path: &Path) -> Result<u64> {
    if !partition_path.exists() || !partition_path.is_dir() {
        return Err(ExchangeDbError::InvalidPartition(format!(
            "partition path '{}' does not exist or is not a directory",
            partition_path.display()
        )));
    }

    let mut entries: Vec<_> = std::fs::read_dir(partition_path)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            // Include column data/index files, exclude the checksum file itself.
            (name.ends_with(".d")
                || name.ends_with(".i")
                || name.ends_with(".k")
                || name.ends_with(".v"))
                && e.path().is_file()
        })
        .collect();

    // Sort by name for deterministic ordering.
    entries.sort_by_key(|e| e.file_name());

    let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a offset basis

    for entry in &entries {
        let path = entry.path();
        let data = std::fs::read(&path)?;

        // Mix in the file name for added safety.
        let name_bytes = entry.file_name();
        let name_bytes = name_bytes.to_string_lossy();
        for byte in name_bytes.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3); // FNV-1a prime
        }

        // Mix in file contents.
        for byte in &data {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }

    Ok(hash)
}

/// Verify partition integrity against an expected checksum.
pub fn verify_partition(partition_path: &Path, expected: u64) -> Result<bool> {
    let actual = partition_checksum(partition_path)?;
    Ok(actual == expected)
}

/// Write a checksum to the `.checksum` file inside the partition directory.
/// Returns the computed checksum.
pub fn write_checksum(partition_path: &Path) -> Result<u64> {
    let checksum = partition_checksum(partition_path)?;
    let checksum_path = partition_path.join(CHECKSUM_FILE);
    std::fs::write(&checksum_path, checksum.to_le_bytes())?;
    Ok(checksum)
}

/// Read a previously written checksum from the `.checksum` file.
fn read_checksum(partition_path: &Path) -> Result<Option<u64>> {
    let checksum_path = partition_path.join(CHECKSUM_FILE);
    if !checksum_path.exists() {
        return Ok(None);
    }
    let data = std::fs::read(&checksum_path)?;
    if data.len() < 8 {
        return Err(ExchangeDbError::Corruption(
            "checksum file too small".into(),
        ));
    }
    let checksum = u64::from_le_bytes(data[..8].try_into().unwrap());
    Ok(Some(checksum))
}

/// Verify all partitions in a table directory.
///
/// Only partitions that have a `.checksum` file are verified. Partitions
/// without a checksum file are skipped (not counted as corrupt).
pub fn verify_table(table_dir: &Path) -> Result<VerifyResult> {
    let partitions = crate::table::list_partitions(table_dir)?;
    let mut result = VerifyResult {
        partitions_checked: 0,
        partitions_ok: 0,
        partitions_corrupt: Vec::new(),
    };

    for partition_path in &partitions {
        if !partition_path.is_dir() {
            continue;
        }

        if let Some(expected) = read_checksum(partition_path)? {
            result.partitions_checked += 1;
            let actual = partition_checksum(partition_path)?;
            if actual == expected {
                result.partitions_ok += 1;
            } else {
                let name = partition_path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                result.partitions_corrupt.push(name);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnValue, TableBuilder, TableWriter};
    use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
    use tempfile::tempdir;

    #[test]
    fn checksum_deterministic() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        writer
            .write_row(Timestamp::from_secs(1710513000), &[ColumnValue::F64(100.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let part_path = db_root.join("trades").join("2024-03-15");
        let c1 = partition_checksum(&part_path).unwrap();
        let c2 = partition_checksum(&part_path).unwrap();
        assert_eq!(c1, c2);
    }

    #[test]
    fn checksum_detects_corruption() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        writer
            .write_row(Timestamp::from_secs(1710513000), &[ColumnValue::F64(100.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let part_path = db_root.join("trades").join("2024-03-15");
        let original_checksum = write_checksum(&part_path).unwrap();

        // Verify passes.
        assert!(verify_partition(&part_path, original_checksum).unwrap());

        // Corrupt a file.
        let price_file = part_path.join("price.d");
        let mut data = std::fs::read(&price_file).unwrap();
        if !data.is_empty() {
            data[0] ^= 0xFF;
        }
        std::fs::write(&price_file, &data).unwrap();

        // Verify fails.
        assert!(!verify_partition(&part_path, original_checksum).unwrap());
    }

    #[test]
    fn verify_table_works() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        writer
            .write_row(Timestamp::from_secs(1710513000), &[ColumnValue::F64(100.0)])
            .unwrap();
        writer
            .write_row(Timestamp::from_secs(1710599400), &[ColumnValue::F64(200.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let table_dir = db_root.join("trades");

        // Write checksums for all partitions.
        for part in crate::table::list_partitions(&table_dir).unwrap() {
            if part.is_dir() {
                write_checksum(&part).unwrap();
            }
        }

        // Verify all ok.
        let result = verify_table(&table_dir).unwrap();
        assert!(result.partitions_checked > 0);
        assert_eq!(result.partitions_ok, result.partitions_checked);
        assert!(result.partitions_corrupt.is_empty());
    }

    #[test]
    fn write_and_read_checksum() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        writer
            .write_row(Timestamp::from_secs(1710513000), &[ColumnValue::F64(42.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let part_path = db_root.join("trades").join("2024-03-15");
        let written = write_checksum(&part_path).unwrap();
        let read_back = read_checksum(&part_path).unwrap();
        assert_eq!(read_back, Some(written));
    }
}
