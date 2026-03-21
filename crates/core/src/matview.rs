//! Materialized view metadata and persistence.

use crate::table::ColumnDef;
use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Metadata for a materialized view, stored alongside its backing table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatViewMeta {
    /// Name of the materialized view (also the name of the backing table).
    pub name: String,
    /// Name of the source table that the defining query reads from.
    pub source_table: String,
    /// The original SQL query that defines this view.
    pub query_sql: String,
    /// Result column definitions.
    pub columns: Vec<ColumnDef>,
    /// Timestamp (nanoseconds since epoch) of the last refresh, if any.
    pub last_refresh: Option<i64>,
    /// Whether the view should auto-refresh on query.
    pub auto_refresh: bool,
    /// Metadata version counter.
    pub version: u64,
}

impl MatViewMeta {
    /// Persist metadata to a `_matview` JSON file at the given path.
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load metadata from a `_matview` JSON file.
    pub fn load(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| ExchangeDbError::Corruption(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, ColumnTypeSerializable};

    #[test]
    fn save_and_load_matview_meta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("_matview");

        let meta = MatViewMeta {
            name: "ohlcv_1m".to_string(),
            source_table: "trades".to_string(),
            query_sql: "SELECT symbol, first(price) as open FROM trades SAMPLE BY 1m".to_string(),
            columns: vec![
                ColumnDef {
                    name: "symbol".to_string(),
                    col_type: ColumnTypeSerializable::Varchar,
                    indexed: false,
                },
                ColumnDef {
                    name: "open".to_string(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            last_refresh: Some(1_000_000_000),
            auto_refresh: false,
            version: 1,
        };

        meta.save(&path).unwrap();
        let loaded = MatViewMeta::load(&path).unwrap();

        assert_eq!(loaded.name, "ohlcv_1m");
        assert_eq!(loaded.source_table, "trades");
        assert_eq!(loaded.columns.len(), 2);
        assert_eq!(loaded.last_refresh, Some(1_000_000_000));
        assert_eq!(loaded.version, 1);
    }
}
