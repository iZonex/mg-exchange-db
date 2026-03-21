//! VACUUM: reclaims disk space by cleaning up stale data in a table directory.
//!
//! Removes:
//! - Applied WAL segments (.applied files)
//! - Empty partition directories
//! - Orphaned column files (columns dropped from schema but files remain)
//! - Stale entries in the column version file

use crate::column_version::ColumnVersionFile;
use crate::table::TableMeta;
use exchange_common::error::Result;
use std::collections::HashSet;
use std::path::PathBuf;

/// Statistics returned after a VACUUM run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumStats {
    pub wal_segments_removed: u32,
    pub empty_partitions_removed: u32,
    pub orphan_files_removed: u32,
    pub bytes_freed: u64,
}

/// A VACUUM job for a single table.
pub struct VacuumJob {
    table_dir: PathBuf,
    meta: TableMeta,
}

impl VacuumJob {
    /// Create a new vacuum job.
    pub fn new(table_dir: PathBuf, meta: TableMeta) -> Self {
        Self { table_dir, meta }
    }

    /// Execute the vacuum, returning statistics about what was cleaned up.
    pub fn run(&self) -> Result<VacuumStats> {
        let mut stats = VacuumStats {
            wal_segments_removed: 0,
            empty_partitions_removed: 0,
            orphan_files_removed: 0,
            bytes_freed: 0,
        };

        // 1. Remove applied WAL segments.
        self.clean_wal(&mut stats)?;

        // 2. Remove orphaned column files and empty partitions.
        self.clean_partitions(&mut stats)?;

        // 3. Compact column version file.
        self.compact_cv(&mut stats)?;

        Ok(stats)
    }

    /// Delete WAL segments that have been applied (*.applied files).
    fn clean_wal(&self, stats: &mut VacuumStats) -> Result<()> {
        let wal_dir = self.table_dir.join("wal");
        if !wal_dir.exists() {
            return Ok(());
        }

        let entries = std::fs::read_dir(&wal_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.ends_with(".applied") {
                    let file_size = std::fs::metadata(&path)?.len();
                    std::fs::remove_file(&path)?;
                    stats.wal_segments_removed += 1;
                    stats.bytes_freed += file_size;
                }
            }
        }

        Ok(())
    }

    /// Remove orphaned column files (columns not in current schema) and empty
    /// partition directories.
    fn clean_partitions(&self, stats: &mut VacuumStats) -> Result<()> {
        // Build a set of current column names from the metadata.
        let current_columns: HashSet<&str> = self
            .meta
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        // List partition directories.
        let mut partitions = Vec::new();
        if self.table_dir.exists() {
            for entry in std::fs::read_dir(&self.table_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    // Skip internal directories (starting with _) and wal.
                    if !name_str.starts_with('_') && name_str != "wal" {
                        partitions.push(path);
                    }
                }
            }
        }

        for partition_path in &partitions {
            let mut has_files = false;

            let entries: Vec<_> = std::fs::read_dir(partition_path)?
                .filter_map(|e| e.ok())
                .collect();

            for entry in &entries {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                let file_name = entry.file_name();
                let name_str = file_name.to_string_lossy();

                // Column files have extensions .d or .i
                let col_name = if let Some(base) = name_str.strip_suffix(".d") {
                    Some(base)
                } else { name_str.strip_suffix(".i") };

                if let Some(col) = col_name {
                    if current_columns.contains(col) {
                        has_files = true;
                    } else {
                        // Orphaned column file -- remove it.
                        let file_size = std::fs::metadata(&path)?.len();
                        std::fs::remove_file(&path)?;
                        stats.orphan_files_removed += 1;
                        stats.bytes_freed += file_size;
                    }
                } else {
                    // Non-column file in partition dir; keep it.
                    has_files = true;
                }
            }

            // If no files remain, remove the partition directory.
            if !has_files {
                // Re-check in case we removed files above.
                let remaining: Vec<_> = std::fs::read_dir(partition_path)?
                    .filter_map(|e| e.ok())
                    .collect();
                if remaining.is_empty() {
                    std::fs::remove_dir(partition_path)?;
                    stats.empty_partitions_removed += 1;
                }
            }
        }

        Ok(())
    }

    /// Compact the column version file by removing entries for columns that
    /// no longer exist in the current schema.
    fn compact_cv(&self, _stats: &mut VacuumStats) -> Result<()> {
        let cv_path = self.table_dir.join("_cv");
        if !cv_path.exists() {
            return Ok(());
        }

        let cv = ColumnVersionFile::load(&cv_path)?;

        // Build a set of current column names.
        let current_columns: HashSet<&str> = self
            .meta
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        // Keep only entries that reference columns still in the schema,
        // plus Dropped entries (for audit trail).
        let filtered: Vec<_> = cv
            .entries
            .into_iter()
            .filter(|entry| {
                use crate::column_version::ColumnAction;
                match &entry.action {
                    ColumnAction::Dropped => true, // always keep drop records
                    _ => current_columns.contains(entry.column_name.as_str()),
                }
            })
            .collect();

        let compacted = ColumnVersionFile { entries: filtered };
        compacted.save(&cv_path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
    use tempfile::tempdir;

    fn test_meta() -> TableMeta {
        TableMeta {
            name: "test_table".to_string(),
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
            ],
            partition_by: PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        }
    }

    #[test]
    fn vacuum_removes_applied_wal_segments() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let meta = test_meta();
        meta.save(&table_dir.join("_meta")).unwrap();

        // Create WAL directory with applied segments.
        let wal_dir = table_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::write(wal_dir.join("wal-000000.applied"), b"data1").unwrap();
        std::fs::write(wal_dir.join("wal-000001.applied"), b"data2").unwrap();
        // An active segment should NOT be removed.
        std::fs::write(wal_dir.join("wal-000002.wal"), b"data3").unwrap();

        let job = VacuumJob::new(table_dir.clone(), meta);
        let stats = job.run().unwrap();

        assert_eq!(stats.wal_segments_removed, 2);
        assert!(!wal_dir.join("wal-000000.applied").exists());
        assert!(!wal_dir.join("wal-000001.applied").exists());
        assert!(wal_dir.join("wal-000002.wal").exists());
    }

    #[test]
    fn vacuum_removes_orphan_files() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        let partition_dir = table_dir.join("2024-03-15");
        std::fs::create_dir_all(&partition_dir).unwrap();

        let meta = test_meta();
        meta.save(&table_dir.join("_meta")).unwrap();

        // Create valid column files.
        std::fs::write(partition_dir.join("timestamp.d"), b"ts_data").unwrap();
        std::fs::write(partition_dir.join("price.d"), b"price_data").unwrap();

        // Create orphan column files (column "volume" was dropped).
        std::fs::write(partition_dir.join("volume.d"), b"orphan_data").unwrap();
        std::fs::write(partition_dir.join("volume.i"), b"orphan_idx").unwrap();

        let job = VacuumJob::new(table_dir.clone(), meta);
        let stats = job.run().unwrap();

        assert_eq!(stats.orphan_files_removed, 2);
        assert!(!partition_dir.join("volume.d").exists());
        assert!(!partition_dir.join("volume.i").exists());
        assert!(partition_dir.join("timestamp.d").exists());
        assert!(partition_dir.join("price.d").exists());
    }

    #[test]
    fn vacuum_removes_empty_partitions() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let meta = test_meta();
        meta.save(&table_dir.join("_meta")).unwrap();

        // Create an empty partition directory.
        let empty_partition = table_dir.join("2024-01-01");
        std::fs::create_dir_all(&empty_partition).unwrap();

        // Create a non-empty partition.
        let non_empty = table_dir.join("2024-03-15");
        std::fs::create_dir_all(&non_empty).unwrap();
        std::fs::write(non_empty.join("timestamp.d"), b"data").unwrap();

        let job = VacuumJob::new(table_dir.clone(), meta);
        let stats = job.run().unwrap();

        assert_eq!(stats.empty_partitions_removed, 1);
        assert!(!empty_partition.exists());
        assert!(non_empty.exists());
    }
}
