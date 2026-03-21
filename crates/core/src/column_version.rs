//! Column versioning: tracks schema changes over time.
//!
//! Each ALTER TABLE operation appends an entry to the `_cv` file in the table
//! directory. This provides a complete audit trail of schema evolution.

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// A single column schema change event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnVersionEntry {
    /// Schema version at which this change occurred.
    pub version: u64,
    /// The column name this action applies to.
    pub column_name: String,
    /// The type of change.
    pub action: ColumnAction,
    /// Unix timestamp (seconds) when this change was recorded.
    pub timestamp: i64,
}

/// The type of schema change.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnAction {
    /// A new column was added.
    Added,
    /// A column was dropped.
    Dropped,
    /// A column was renamed (stores the previous name).
    Renamed { old_name: String },
    /// A column's type was changed (stores the previous type name).
    TypeChanged { old_type: String },
}

/// Manages the column version file (`_cv`) for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnVersionFile {
    pub entries: Vec<ColumnVersionEntry>,
}

impl ColumnVersionFile {
    /// Create a new, empty column version file.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Load from disk. If the file does not exist, return an empty instance.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| ExchangeDbError::Corruption(e.to_string()))
    }

    /// Save to disk.
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Append a new entry and save.
    pub fn append_and_save(&mut self, entry: ColumnVersionEntry, path: &Path) -> Result<()> {
        self.entries.push(entry);
        self.save(path)
    }

    /// Get the current Unix timestamp in seconds.
    pub fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Convenience: record an ADD COLUMN event.
    pub fn record_add(
        &mut self,
        version: u64,
        column_name: &str,
        path: &Path,
    ) -> Result<()> {
        self.append_and_save(
            ColumnVersionEntry {
                version,
                column_name: column_name.to_string(),
                action: ColumnAction::Added,
                timestamp: Self::now_secs(),
            },
            path,
        )
    }

    /// Convenience: record a DROP COLUMN event.
    pub fn record_drop(
        &mut self,
        version: u64,
        column_name: &str,
        path: &Path,
    ) -> Result<()> {
        self.append_and_save(
            ColumnVersionEntry {
                version,
                column_name: column_name.to_string(),
                action: ColumnAction::Dropped,
                timestamp: Self::now_secs(),
            },
            path,
        )
    }

    /// Convenience: record a RENAME COLUMN event.
    pub fn record_rename(
        &mut self,
        version: u64,
        new_name: &str,
        old_name: &str,
        path: &Path,
    ) -> Result<()> {
        self.append_and_save(
            ColumnVersionEntry {
                version,
                column_name: new_name.to_string(),
                action: ColumnAction::Renamed {
                    old_name: old_name.to_string(),
                },
                timestamp: Self::now_secs(),
            },
            path,
        )
    }

    /// Convenience: record a TYPE CHANGE event.
    pub fn record_type_change(
        &mut self,
        version: u64,
        column_name: &str,
        old_type: &str,
        path: &Path,
    ) -> Result<()> {
        self.append_and_save(
            ColumnVersionEntry {
                version,
                column_name: column_name.to_string(),
                action: ColumnAction::TypeChanged {
                    old_type: old_type.to_string(),
                },
                timestamp: Self::now_secs(),
            },
            path,
        )
    }
}

impl Default for ColumnVersionFile {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn column_version_roundtrip() {
        let dir = tempdir().unwrap();
        let cv_path = dir.path().join("_cv");

        let mut cv = ColumnVersionFile::new();
        cv.record_add(2, "exchange", &cv_path).unwrap();
        cv.record_rename(3, "trade_price", "price", &cv_path)
            .unwrap();
        cv.record_type_change(4, "volume", "F32", &cv_path)
            .unwrap();
        cv.record_drop(5, "old_col", &cv_path).unwrap();

        assert_eq!(cv.entries.len(), 4);

        // Reload and verify.
        let loaded = ColumnVersionFile::load(&cv_path).unwrap();
        assert_eq!(loaded.entries.len(), 4);
        assert_eq!(loaded.entries[0].column_name, "exchange");
        assert_eq!(loaded.entries[0].action, ColumnAction::Added);
        assert_eq!(loaded.entries[0].version, 2);
        assert_eq!(
            loaded.entries[1].action,
            ColumnAction::Renamed {
                old_name: "price".to_string()
            }
        );
        assert_eq!(
            loaded.entries[2].action,
            ColumnAction::TypeChanged {
                old_type: "F32".to_string()
            }
        );
        assert_eq!(loaded.entries[3].action, ColumnAction::Dropped);
    }

    #[test]
    fn load_nonexistent_returns_empty() {
        let dir = tempdir().unwrap();
        let cv_path = dir.path().join("nonexistent_cv");
        let cv = ColumnVersionFile::load(&cv_path).unwrap();
        assert!(cv.entries.is_empty());
    }
}
