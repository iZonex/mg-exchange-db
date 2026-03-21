//! Snapshot/backup functionality for ExchangeDB.
//!
//! Provides simple snapshot creation and restore by copying table directories.

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Information about a created snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Unix timestamp (seconds) when the snapshot was created.
    pub timestamp: u64,
    /// List of table names included in the snapshot.
    pub tables: Vec<String>,
    /// Total size of the snapshot in bytes.
    pub total_bytes: u64,
}

/// Manifest file stored in the snapshot directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotManifest {
    pub version: u32,
    pub timestamp: u64,
    pub tables: Vec<String>,
    pub total_size: u64,
}

const MANIFEST_FILENAME: &str = "manifest.json";
const MANIFEST_VERSION: u32 = 1;

/// Create a snapshot of the database by copying all table directories to
/// the given snapshot directory.
///
/// The snapshot directory will be created if it does not exist.
/// Returns a `SnapshotInfo` describing the snapshot.
pub fn create_snapshot(db_root: &Path, snapshot_dir: &Path) -> Result<SnapshotInfo> {
    // Ensure db_root exists
    if !db_root.exists() {
        return Err(ExchangeDbError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("database root directory not found: {}", db_root.display()),
        )));
    }

    // Create snapshot directory
    std::fs::create_dir_all(snapshot_dir)?;

    let mut tables = Vec::new();
    let mut total_bytes = 0u64;

    // Scan for table directories (directories containing a _meta file)
    let entries = std::fs::read_dir(db_root)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        let meta_path = path.join("_meta");
        if !meta_path.exists() {
            continue;
        }

        let table_name = entry
            .file_name()
            .to_string_lossy()
            .to_string();

        let dest_table_dir = snapshot_dir.join(&table_name);
        let bytes = copy_dir_recursive(&path, &dest_table_dir)?;
        total_bytes += bytes;
        tables.push(table_name);
    }

    tables.sort();

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let manifest = SnapshotManifest {
        version: MANIFEST_VERSION,
        timestamp,
        tables: tables.clone(),
        total_size: total_bytes,
    };

    // Write manifest
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    std::fs::write(snapshot_dir.join(MANIFEST_FILENAME), manifest_json)?;

    Ok(SnapshotInfo {
        timestamp,
        tables,
        total_bytes,
    })
}

/// Restore a snapshot from the given snapshot directory into the database root.
///
/// Validates the manifest and copies all table directories back.
pub fn restore_snapshot(snapshot_dir: &Path, db_root: &Path) -> Result<()> {
    // Validate manifest exists
    let manifest_path = snapshot_dir.join(MANIFEST_FILENAME);
    if !manifest_path.exists() {
        return Err(ExchangeDbError::Corruption(format!(
            "snapshot manifest not found at: {}",
            manifest_path.display()
        )));
    }

    // Load and validate manifest
    let manifest_json = std::fs::read_to_string(&manifest_path)?;
    let manifest: SnapshotManifest = serde_json::from_str(&manifest_json)
        .map_err(|e| ExchangeDbError::Corruption(format!("invalid snapshot manifest: {e}")))?;

    if manifest.version != MANIFEST_VERSION {
        return Err(ExchangeDbError::Corruption(format!(
            "unsupported snapshot version: {} (expected {})",
            manifest.version, MANIFEST_VERSION
        )));
    }

    // Ensure db_root exists
    std::fs::create_dir_all(db_root)?;

    // Restore each table
    for table_name in &manifest.tables {
        let src_table_dir = snapshot_dir.join(table_name);
        if !src_table_dir.exists() {
            return Err(ExchangeDbError::Corruption(format!(
                "table directory '{}' missing from snapshot",
                table_name
            )));
        }

        let dest_table_dir = db_root.join(table_name);

        // Remove existing table directory if it exists
        if dest_table_dir.exists() {
            std::fs::remove_dir_all(&dest_table_dir)?;
        }

        copy_dir_recursive(&src_table_dir, &dest_table_dir)?;
    }

    Ok(())
}

/// Recursively copy a directory and all its contents. Returns total bytes copied.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<u64> {
    std::fs::create_dir_all(dst)?;

    let mut total_bytes = 0u64;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let entry_path = entry.path();
        let dest_path = dst.join(entry.file_name());

        if entry_path.is_dir() {
            total_bytes += copy_dir_recursive(&entry_path, &dest_path)?;
        } else {
            let bytes = std::fs::copy(&entry_path, &dest_path)?;
            total_bytes += bytes;
        }
    }

    Ok(total_bytes)
}

/// Verify that a snapshot at the given path is valid and could be restored.
///
/// Checks:
/// 1. `manifest.json` exists and is valid JSON with the expected schema.
/// 2. All table directories listed in the manifest exist.
/// 3. Each table directory contains a valid `_meta` file.
///
/// Returns `Ok(SnapshotInfo)` on success, or an error describing what is wrong.
pub fn verify_snapshot(snapshot_dir: &Path) -> Result<SnapshotInfo> {
    // Check 1: manifest exists and parses correctly.
    let manifest_path = snapshot_dir.join(MANIFEST_FILENAME);
    if !manifest_path.exists() {
        return Err(ExchangeDbError::Snapshot {
            detail: format!("manifest.json not found"),
            path: snapshot_dir.display().to_string(),
        });
    }

    let manifest_json = std::fs::read_to_string(&manifest_path)?;
    let manifest: SnapshotManifest = serde_json::from_str(&manifest_json)
        .map_err(|e| ExchangeDbError::Snapshot {
            detail: format!("invalid manifest.json: {e}"),
            path: manifest_path.display().to_string(),
        })?;

    if manifest.version != MANIFEST_VERSION {
        return Err(ExchangeDbError::Snapshot {
            detail: format!(
                "unsupported manifest version: {} (expected {})",
                manifest.version, MANIFEST_VERSION
            ),
            path: manifest_path.display().to_string(),
        });
    }

    // Check 2: all table directories exist.
    for table_name in &manifest.tables {
        let table_dir = snapshot_dir.join(table_name);
        if !table_dir.exists() {
            return Err(ExchangeDbError::Snapshot {
                detail: format!("table directory '{}' missing from snapshot", table_name),
                path: snapshot_dir.display().to_string(),
            });
        }

        // Check 3: _meta file exists and is valid.
        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            return Err(ExchangeDbError::Snapshot {
                detail: format!("table '{}' has no _meta file", table_name),
                path: table_dir.display().to_string(),
            });
        }

        // Validate _meta can be loaded (structurally valid).
        crate::table::TableMeta::load(&meta_path).map_err(|e| {
            ExchangeDbError::Snapshot {
                detail: format!("table '{}' has invalid _meta: {}", table_name, e),
                path: meta_path.display().to_string(),
            }
        })?;
    }

    Ok(SnapshotInfo {
        timestamp: manifest.timestamp,
        tables: manifest.tables,
        total_bytes: manifest.total_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
    use crate::table::{ColumnValue, TableBuilder, TableWriter};

    #[test]
    fn test_create_and_restore_snapshot() {
        let db_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let restore_dir = tempfile::tempdir().unwrap();

        let db_root = db_dir.path();
        let snap_path = snapshot_dir.path().join("snap1");
        let restore_root = restore_dir.path();

        // Create a table with data
        let _meta = TableBuilder::new("test_table")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("name", ColumnType::Varchar)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "test_table").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        writer
            .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::Str("alpha")])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Create snapshot
        let info = create_snapshot(db_root, &snap_path).unwrap();
        assert_eq!(info.tables, vec!["test_table"]);
        assert!(info.total_bytes > 0);
        assert!(info.timestamp > 0);

        // Verify manifest exists
        assert!(snap_path.join("manifest.json").exists());

        // Restore to new location
        restore_snapshot(&snap_path, restore_root).unwrap();

        // Verify restored table
        assert!(restore_root.join("test_table").join("_meta").exists());

        // Verify metadata is valid
        let meta = crate::table::TableMeta::load(&restore_root.join("test_table").join("_meta"))
            .unwrap();
        assert_eq!(meta.name, "test_table");
        assert_eq!(meta.columns.len(), 3);
    }

    #[test]
    fn test_create_snapshot_empty_db() {
        let db_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();

        let info = create_snapshot(db_dir.path(), &snapshot_dir.path().join("snap")).unwrap();
        assert!(info.tables.is_empty());
        assert_eq!(info.total_bytes, 0);
    }

    #[test]
    fn test_restore_missing_manifest() {
        let snapshot_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();

        let result = restore_snapshot(snapshot_dir.path(), db_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_restore_invalid_manifest() {
        let snapshot_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();

        std::fs::write(
            snapshot_dir.path().join("manifest.json"),
            "not valid json",
        )
        .unwrap();

        let result = restore_snapshot(snapshot_dir.path(), db_dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_snapshot_multiple_tables() {
        let db_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let db_root = db_dir.path();

        // Create two tables
        TableBuilder::new("table_a")
            .column("timestamp", ColumnType::Timestamp)
            .column("val", ColumnType::I64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        TableBuilder::new("table_b")
            .column("timestamp", ColumnType::Timestamp)
            .column("name", ColumnType::Varchar)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        let info = create_snapshot(db_root, &snapshot_dir.path().join("s")).unwrap();
        assert_eq!(info.tables.len(), 2);
        assert!(info.tables.contains(&"table_a".to_string()));
        assert!(info.tables.contains(&"table_b".to_string()));
    }

    #[test]
    fn test_restore_overwrites_existing() {
        let db_dir = tempfile::tempdir().unwrap();
        let snapshot_dir = tempfile::tempdir().unwrap();
        let restore_dir = tempfile::tempdir().unwrap();
        let db_root = db_dir.path();
        let restore_root = restore_dir.path();

        // Create table and snapshot
        TableBuilder::new("mytable")
            .column("timestamp", ColumnType::Timestamp)
            .column("x", ColumnType::I64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        let snap_path = snapshot_dir.path().join("s");
        create_snapshot(db_root, &snap_path).unwrap();

        // Create a pre-existing table in restore target
        TableBuilder::new("mytable")
            .column("timestamp", ColumnType::Timestamp)
            .column("different", ColumnType::F64)
            .timestamp("timestamp")
            .build(restore_root)
            .unwrap();

        // Restore should overwrite
        restore_snapshot(&snap_path, restore_root).unwrap();

        let meta = crate::table::TableMeta::load(&restore_root.join("mytable").join("_meta"))
            .unwrap();
        // Should have "x" column from snapshot, not "different"
        assert!(meta.columns.iter().any(|c| c.name == "x"));
        assert!(!meta.columns.iter().any(|c| c.name == "different"));
    }

    #[test]
    fn test_snapshot_nonexistent_db_root() {
        let result = create_snapshot(
            Path::new("/nonexistent/path/db"),
            Path::new("/tmp/snap"),
        );
        assert!(result.is_err());
    }
}
