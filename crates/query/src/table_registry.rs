//! Hot table registry — keeps tables open in memory for zero-overhead scanning.
//!
//! Instead of opening mmap handles, loading metadata, and listing partitions
//! on every query, tables are opened once and kept resident. This eliminates
//! ~48µs per partition of per-query setup overhead.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use exchange_core::column::{FixedColumnReader, VarColumnReader};
use exchange_core::table::TableMeta;

/// A column reader that is pre-opened and kept in memory.
pub enum OpenColumn {
    Fixed(FixedColumnReader, ColumnType),
    Var(VarColumnReader, ColumnType),
}

/// A partition with all its column readers pre-opened.
pub struct OpenPartition {
    pub path: PathBuf,
    pub columns: Vec<(String, OpenColumn)>,
    pub row_count: u64,
}

/// A table with metadata and all partitions pre-opened.
pub struct OpenTable {
    pub meta: TableMeta,
    pub table_dir: PathBuf,
    pub partitions: Vec<OpenPartition>,
}

impl OpenTable {
    /// Open a table: load metadata, discover partitions, open all column readers.
    pub fn open(db_root: &Path, table_name: &str) -> Result<Self> {
        let table_dir = db_root.join(table_name);
        let meta = TableMeta::load(&table_dir.join("_meta"))?;
        let mut partition_paths = exchange_core::table::list_partitions(&table_dir)?;
        partition_paths.sort();

        let mut partitions = Vec::with_capacity(partition_paths.len());

        for part_path in &partition_paths {
            let mut columns = Vec::with_capacity(meta.columns.len());
            let mut row_count = 0u64;

            for col_def in &meta.columns {
                let ct: ColumnType = col_def.col_type.into();

                if ct.is_variable_length() {
                    let data_path = part_path.join(format!("{}.d", col_def.name));
                    let idx_path = part_path.join(format!("{}.i", col_def.name));
                    if data_path.exists() && idx_path.exists() {
                        let reader = VarColumnReader::open(&data_path, &idx_path)?;
                        if row_count == 0 {
                            row_count = reader.row_count();
                        }
                        columns.push((col_def.name.clone(), OpenColumn::Var(reader, ct)));
                    }
                } else {
                    let data_path = part_path.join(format!("{}.d", col_def.name));
                    if data_path.exists() {
                        let reader = FixedColumnReader::open(&data_path, ct)?;
                        if row_count == 0 {
                            row_count = reader.row_count();
                        }
                        columns.push((col_def.name.clone(), OpenColumn::Fixed(reader, ct)));
                    }
                }
            }

            partitions.push(OpenPartition {
                path: part_path.clone(),
                columns,
                row_count,
            });
        }

        Ok(Self {
            meta,
            table_dir,
            partitions,
        })
    }

    /// Total row count across all partitions.
    pub fn total_rows(&self) -> u64 {
        self.partitions.iter().map(|p| p.row_count).sum()
    }

    /// Reload a table (after DDL, compaction, or new data).
    pub fn reload(db_root: &Path, table_name: &str) -> Result<Self> {
        Self::open(db_root, table_name)
    }
}

/// Global table registry — keeps all tables open in memory.
pub struct TableRegistry {
    tables: DashMap<String, Arc<OpenTable>>,
    db_root: PathBuf,
}

impl TableRegistry {
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            tables: DashMap::new(),
            db_root,
        }
    }

    /// Get or open a table. First call opens it, subsequent calls return cached.
    pub fn get(&self, table_name: &str) -> Result<Arc<OpenTable>> {
        if let Some(t) = self.tables.get(table_name) {
            let arc: Arc<OpenTable> = t.value().clone();
            return Ok(arc);
        }

        let table = Arc::new(OpenTable::open(&self.db_root, table_name)?);
        self.tables.insert(table_name.to_string(), table.clone());
        Ok(table)
    }

    /// Invalidate a table (after DDL, insert, compaction).
    pub fn invalidate(&self, table_name: &str) {
        self.tables.remove(table_name);
    }

    /// Invalidate all tables.
    pub fn invalidate_all(&self) {
        self.tables.clear();
    }

    /// Load all existing tables at startup.
    pub fn load_all(&self) -> Result<usize> {
        let mut count = 0;
        if let Ok(entries) = std::fs::read_dir(&self.db_root) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let meta_path = entry.path().join("_meta");
                    if meta_path.exists() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if let Ok(table) = OpenTable::open(&self.db_root, &name) {
                            self.tables.insert(name, Arc::new(table));
                            count += 1;
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

/// Global singleton registry.
static GLOBAL_REGISTRY: OnceLock<TableRegistry> = OnceLock::new();

/// Initialize the global registry (call once at server startup).
pub fn init_global(db_root: PathBuf) -> &'static TableRegistry {
    GLOBAL_REGISTRY.get_or_init(|| {
        let reg = TableRegistry::new(db_root);
        let count = reg.load_all().unwrap_or(0);
        tracing::info!(tables = count, "table registry initialized");
        reg
    })
}

/// Get the global registry.
pub fn global() -> Option<&'static TableRegistry> {
    GLOBAL_REGISTRY.get()
}
