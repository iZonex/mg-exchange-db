use crate::column::{FixedColumnReader, FixedColumnWriter, VarColumnReader, VarColumnWriter};
use crate::partition::PartitionManager;
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::{ColumnType, PartitionBy, Timestamp};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Column definition in table metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: ColumnTypeSerializable,
    pub indexed: bool,
}

/// Serializable wrapper for ColumnType.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnTypeSerializable {
    Boolean,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Timestamp,
    Symbol,
    Varchar,
    Binary,
    Uuid,
    Date,
    Char,
    IPv4,
    Long128,
    Long256,
    GeoHash,
    String,
    TimestampMicro,
    TimestampMilli,
    Interval,
    Decimal8,
    Decimal16,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    GeoByte,
    GeoShort,
    GeoInt,
    Array,
    Cursor,
    Record,
    RegClass,
    RegProcedure,
    ArrayString,
    Null,
    VarArg,
    Parameter,
    VarcharSlice,
    IPv6,
}

impl From<ColumnType> for ColumnTypeSerializable {
    fn from(ct: ColumnType) -> Self {
        match ct {
            ColumnType::Boolean => Self::Boolean,
            ColumnType::I8 => Self::I8,
            ColumnType::I16 => Self::I16,
            ColumnType::I32 => Self::I32,
            ColumnType::I64 => Self::I64,
            ColumnType::F32 => Self::F32,
            ColumnType::F64 => Self::F64,
            ColumnType::Timestamp => Self::Timestamp,
            ColumnType::Symbol => Self::Symbol,
            ColumnType::Varchar => Self::Varchar,
            ColumnType::Binary => Self::Binary,
            ColumnType::Uuid => Self::Uuid,
            ColumnType::Date => Self::Date,
            ColumnType::Char => Self::Char,
            ColumnType::IPv4 => Self::IPv4,
            ColumnType::Long128 => Self::Long128,
            ColumnType::Long256 => Self::Long256,
            ColumnType::GeoHash => Self::GeoHash,
            ColumnType::String => Self::String,
            ColumnType::TimestampMicro => Self::TimestampMicro,
            ColumnType::TimestampMilli => Self::TimestampMilli,
            ColumnType::Interval => Self::Interval,
            ColumnType::Decimal8 => Self::Decimal8,
            ColumnType::Decimal16 => Self::Decimal16,
            ColumnType::Decimal32 => Self::Decimal32,
            ColumnType::Decimal64 => Self::Decimal64,
            ColumnType::Decimal128 => Self::Decimal128,
            ColumnType::Decimal256 => Self::Decimal256,
            ColumnType::GeoByte => Self::GeoByte,
            ColumnType::GeoShort => Self::GeoShort,
            ColumnType::GeoInt => Self::GeoInt,
            ColumnType::Array => Self::Array,
            ColumnType::Cursor => Self::Cursor,
            ColumnType::Record => Self::Record,
            ColumnType::RegClass => Self::RegClass,
            ColumnType::RegProcedure => Self::RegProcedure,
            ColumnType::ArrayString => Self::ArrayString,
            ColumnType::Null => Self::Null,
            ColumnType::VarArg => Self::VarArg,
            ColumnType::Parameter => Self::Parameter,
            ColumnType::VarcharSlice => Self::VarcharSlice,
            ColumnType::IPv6 => Self::IPv6,
        }
    }
}

impl From<ColumnTypeSerializable> for ColumnType {
    fn from(ct: ColumnTypeSerializable) -> Self {
        match ct {
            ColumnTypeSerializable::Boolean => Self::Boolean,
            ColumnTypeSerializable::I8 => Self::I8,
            ColumnTypeSerializable::I16 => Self::I16,
            ColumnTypeSerializable::I32 => Self::I32,
            ColumnTypeSerializable::I64 => Self::I64,
            ColumnTypeSerializable::F32 => Self::F32,
            ColumnTypeSerializable::F64 => Self::F64,
            ColumnTypeSerializable::Timestamp => Self::Timestamp,
            ColumnTypeSerializable::Symbol => Self::Symbol,
            ColumnTypeSerializable::Varchar => Self::Varchar,
            ColumnTypeSerializable::Binary => Self::Binary,
            ColumnTypeSerializable::Uuid => Self::Uuid,
            ColumnTypeSerializable::Date => Self::Date,
            ColumnTypeSerializable::Char => Self::Char,
            ColumnTypeSerializable::IPv4 => Self::IPv4,
            ColumnTypeSerializable::Long128 => Self::Long128,
            ColumnTypeSerializable::Long256 => Self::Long256,
            ColumnTypeSerializable::GeoHash => Self::GeoHash,
            ColumnTypeSerializable::String => Self::String,
            ColumnTypeSerializable::TimestampMicro => Self::TimestampMicro,
            ColumnTypeSerializable::TimestampMilli => Self::TimestampMilli,
            ColumnTypeSerializable::Interval => Self::Interval,
            ColumnTypeSerializable::Decimal8 => Self::Decimal8,
            ColumnTypeSerializable::Decimal16 => Self::Decimal16,
            ColumnTypeSerializable::Decimal32 => Self::Decimal32,
            ColumnTypeSerializable::Decimal64 => Self::Decimal64,
            ColumnTypeSerializable::Decimal128 => Self::Decimal128,
            ColumnTypeSerializable::Decimal256 => Self::Decimal256,
            ColumnTypeSerializable::GeoByte => Self::GeoByte,
            ColumnTypeSerializable::GeoShort => Self::GeoShort,
            ColumnTypeSerializable::GeoInt => Self::GeoInt,
            ColumnTypeSerializable::Array => Self::Array,
            ColumnTypeSerializable::Cursor => Self::Cursor,
            ColumnTypeSerializable::Record => Self::Record,
            ColumnTypeSerializable::RegClass => Self::RegClass,
            ColumnTypeSerializable::RegProcedure => Self::RegProcedure,
            ColumnTypeSerializable::ArrayString => Self::ArrayString,
            ColumnTypeSerializable::Null => Self::Null,
            ColumnTypeSerializable::VarArg => Self::VarArg,
            ColumnTypeSerializable::Parameter => Self::Parameter,
            ColumnTypeSerializable::VarcharSlice => Self::VarcharSlice,
            ColumnTypeSerializable::IPv6 => Self::IPv6,
        }
    }
}

/// Table metadata stored in _meta file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub partition_by: PartitionBySerializable,
    /// Index of the designated timestamp column.
    pub timestamp_column: usize,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum PartitionBySerializable {
    None,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl From<PartitionBy> for PartitionBySerializable {
    fn from(pb: PartitionBy) -> Self {
        match pb {
            PartitionBy::None => Self::None,
            PartitionBy::Hour => Self::Hour,
            PartitionBy::Day => Self::Day,
            PartitionBy::Week => Self::Week,
            PartitionBy::Month => Self::Month,
            PartitionBy::Year => Self::Year,
        }
    }
}

impl From<PartitionBySerializable> for PartitionBy {
    fn from(pb: PartitionBySerializable) -> Self {
        match pb {
            PartitionBySerializable::None => Self::None,
            PartitionBySerializable::Hour => Self::Hour,
            PartitionBySerializable::Day => Self::Day,
            PartitionBySerializable::Week => Self::Week,
            PartitionBySerializable::Month => Self::Month,
            PartitionBySerializable::Year => Self::Year,
        }
    }
}

impl TableMeta {
    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json).map_err(|e| ExchangeDbError::Corruption(e.to_string()))
    }

    /// Add a new column to the table metadata. Bumps version.
    /// Does NOT create column files in existing partitions — the caller is responsible for that.
    pub fn add_column(&mut self, name: &str, col_type: ColumnType) -> Result<()> {
        if self.columns.iter().any(|c| c.name == name) {
            return Err(ExchangeDbError::ColumnAlreadyExists(
                name.to_string(),
                self.name.clone(),
            ));
        }
        self.columns.push(ColumnDef {
            name: name.to_string(),
            col_type: col_type.into(),
            indexed: false,
        });
        self.version += 1;
        Ok(())
    }

    /// Remove a column from the table metadata. Bumps version.
    /// Does NOT delete column files in existing partitions — the caller is responsible for that.
    pub fn drop_column(&mut self, name: &str) -> Result<()> {
        let idx = self
            .columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(name.to_string(), self.name.clone())
            })?;

        // Disallow dropping the designated timestamp column.
        if idx == self.timestamp_column {
            return Err(ExchangeDbError::CannotDropTimestampColumn(
                name.to_string(),
                self.name.clone(),
            ));
        }

        self.columns.remove(idx);

        // Adjust timestamp_column index if needed.
        if idx < self.timestamp_column {
            self.timestamp_column -= 1;
        }

        self.version += 1;
        Ok(())
    }

    /// Rename a column in the table metadata. Bumps version.
    /// Does NOT rename column files in existing partitions — the caller is responsible for that.
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if self.columns.iter().any(|c| c.name == new_name) {
            return Err(ExchangeDbError::ColumnAlreadyExists(
                new_name.to_string(),
                self.name.clone(),
            ));
        }
        let col = self
            .columns
            .iter_mut()
            .find(|c| c.name == old_name)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(old_name.to_string(), self.name.clone())
            })?;
        col.name = new_name.to_string();
        self.version += 1;
        Ok(())
    }

    /// Change the type of a column in the table metadata. Bumps version.
    pub fn set_column_type(&mut self, name: &str, new_type: ColumnType) -> Result<()> {
        let col = self
            .columns
            .iter_mut()
            .find(|c| c.name == name)
            .ok_or_else(|| {
                ExchangeDbError::ColumnNotFound(name.to_string(), self.name.clone())
            })?;
        col.col_type = new_type.into();
        self.version += 1;
        Ok(())
    }
}

/// Builder for creating new tables.
pub struct TableBuilder {
    name: String,
    columns: Vec<ColumnDef>,
    partition_by: PartitionBy,
    timestamp_column: Option<usize>,
}

impl TableBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
            partition_by: PartitionBy::Day,
            timestamp_column: None,
        }
    }

    pub fn column(mut self, name: &str, col_type: ColumnType) -> Self {
        self.columns.push(ColumnDef {
            name: name.to_string(),
            col_type: col_type.into(),
            indexed: false,
        });
        self
    }

    pub fn indexed_column(mut self, name: &str, col_type: ColumnType) -> Self {
        self.columns.push(ColumnDef {
            name: name.to_string(),
            col_type: col_type.into(),
            indexed: true,
        });
        self
    }

    /// Set the designated timestamp column by name.
    pub fn timestamp(mut self, column_name: &str) -> Self {
        let idx = self
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .expect("timestamp column must be added before calling .timestamp()");
        self.timestamp_column = Some(idx);
        self
    }

    pub fn partition_by(mut self, pb: PartitionBy) -> Self {
        self.partition_by = pb;
        self
    }

    /// Create the table on disk at the given root directory.
    pub fn build(self, db_root: &Path) -> Result<TableMeta> {
        // Validate table name to prevent path traversal attacks.
        exchange_common::validation::validate_table_name(&self.name)?;

        let table_dir = db_root.join(&self.name);
        if table_dir.exists() {
            return Err(ExchangeDbError::TableAlreadyExists(self.name));
        }

        std::fs::create_dir_all(&table_dir)?;

        let meta = TableMeta {
            name: self.name,
            columns: self.columns,
            partition_by: self.partition_by.into(),
            timestamp_column: self.timestamp_column.unwrap_or(0),
            version: 1,
        };

        meta.save(&table_dir.join("_meta"))?;
        Ok(meta)
    }
}

/// Pre-computed column metadata for fast access on the write hot path.
#[derive(Debug, Clone)]
struct ColumnInfo {
    #[allow(dead_code)]
    name: String,
    col_type: ColumnType,
    #[allow(dead_code)]
    element_size: Option<usize>,
    is_variable: bool,
}

/// Appends rows to a table, managing partitions and column files.
#[allow(dead_code)]
pub struct TableWriter {
    meta: TableMeta,
    table_dir: PathBuf,
    partition_mgr: PartitionManager,
    /// Column writers for the current partition.
    current_partition: Option<PartitionWriter>,
    current_partition_path: Option<PathBuf>,
    /// Pre-computed column info (types, sizes, variability) to avoid per-row lookups.
    column_info: Vec<ColumnInfo>,
    /// Pre-computed mapping from column name to index for write_batch.
    column_name_to_idx: std::collections::HashMap<String, usize>,
    /// Optional deduplication configuration.
    dedup_config: Option<crate::dedup::DedupConfig>,
}

/// Column writers stored as indexed arrays for O(1) access instead of HashMap.
struct PartitionWriter {
    /// One slot per column. Fixed-width columns have `Some(writer)`.
    fixed_columns: Vec<Option<FixedColumnWriter>>,
    /// One slot per column. Variable-length columns have `Some(writer)`.
    var_columns: Vec<Option<VarColumnWriter>>,
}

impl TableWriter {
    pub fn open(db_root: &Path, table_name: &str) -> Result<Self> {
        exchange_common::validation::validate_table_name(table_name)?;
        let table_dir = db_root.join(table_name);
        let meta = TableMeta::load(&table_dir.join("_meta"))?;
        let partition_by: PartitionBy = meta.partition_by.into();

        let partition_mgr = PartitionManager::new(table_dir.clone(), partition_by);

        // Pre-compute column info for the hot path
        let column_info: Vec<ColumnInfo> = meta
            .columns
            .iter()
            .map(|c| {
                let col_type: ColumnType = c.col_type.into();
                ColumnInfo {
                    name: c.name.clone(),
                    col_type,
                    element_size: col_type.fixed_size(),
                    is_variable: col_type.is_variable_length(),
                }
            })
            .collect();

        // Pre-compute column name -> index map for write_batch
        let column_name_to_idx: std::collections::HashMap<String, usize> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();

        Ok(Self {
            meta,
            table_dir,
            partition_mgr,
            current_partition: None,
            current_partition_path: None,
            column_info,
            column_name_to_idx,
            dedup_config: None,
        })
    }

    /// Write a row. Values must match column order in metadata.
    /// The timestamp value is used to determine the partition.
    #[inline]
    pub fn write_row(&mut self, ts: Timestamp, values: &[ColumnValue]) -> Result<()> {
        let partition_path = self.partition_mgr.ensure_partition(ts)?;

        // Compare by PathBuf identity — avoids the per-row String allocation
        // that `to_string_lossy().to_string()` would incur.
        let need_switch = match &self.current_partition_path {
            Some(cached) => *cached != partition_path,
            None => true,
        };

        if need_switch {
            self.flush()?;
            self.open_partition(&partition_path)?;
            self.current_partition_path = Some(partition_path);
        }

        let pw = self.current_partition.as_mut().unwrap();
        let ts_col = self.meta.timestamp_column;
        let num_cols = self.column_info.len();

        for i in 0..num_cols {
            // Use pre-computed column info (avoids From conversion and
            // is_variable_length() call per row).
            let ci = unsafe { self.column_info.get_unchecked(i) };
            let value = if i == ts_col {
                &ColumnValue::Timestamp(ts)
            } else {
                let val_idx = if i > ts_col { i - 1 } else { i };
                if val_idx < values.len() {
                    &values[val_idx]
                } else {
                    continue;
                }
            };

            if ci.is_variable {
                // Direct Vec index: O(1)
                if let Some(ref mut writer) = pw.var_columns[i] {
                    match value {
                        ColumnValue::Str(s) => writer.append_str(s)?,
                        ColumnValue::Bytes(b) => writer.append(b)?,
                        ColumnValue::Null => writer.append(b"\0")?,
                        _ => writer.append(b"")?,
                    }
                }
            } else if let Some(ref mut writer) = pw.fixed_columns[i] {
                match ci.col_type {
                    ColumnType::F64 => {
                        let fval = match value {
                            ColumnValue::F64(v) => *v,
                            ColumnValue::I64(v) => *v as f64,
                            ColumnValue::I32(v) => *v as f64,
                            ColumnValue::Timestamp(t) => t.as_nanos() as f64,
                            ColumnValue::Null => f64::NAN,
                            _ => 0.0,
                        };
                        writer.append_f64(fval)?;
                    }
                    ColumnType::F32 => {
                        let fval = match value {
                            ColumnValue::F64(v) => *v as f32,
                            ColumnValue::I64(v) => *v as f32,
                            ColumnValue::I32(v) => *v as f32,
                            _ => 0.0,
                        };
                        writer.append(&fval.to_le_bytes())?;
                    }
                    ColumnType::I64 | ColumnType::Timestamp => {
                        let ival = match value {
                            ColumnValue::I64(v) => *v,
                            ColumnValue::F64(v) => *v as i64,
                            ColumnValue::I32(v) => *v as i64,
                            ColumnValue::Timestamp(t) => t.as_nanos(),
                            _ => 0,
                        };
                        writer.append_i64(ival)?;
                    }
                    ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
                        let ival = match value {
                            ColumnValue::I32(v) => *v,
                            ColumnValue::I64(v) => *v as i32,
                            ColumnValue::F64(v) => *v as i32,
                            _ => 0,
                        };
                        writer.append_i32(ival)?;
                    }
                    _ => {
                        match value {
                            ColumnValue::I64(v) => writer.append_i64(*v)?,
                            ColumnValue::F64(v) => writer.append_f64(*v)?,
                            ColumnValue::I32(v) => writer.append_i32(*v)?,
                            ColumnValue::Timestamp(t) => writer.append_i64(t.as_nanos())?,
                            _ => writer.append_i64(0)?,
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Write N rows at once using columnar bulk writes.
    ///
    /// All timestamps must map to the same partition. For multi-partition
    /// data, the caller should sort by timestamp and call this once per
    /// partition group.
    ///
    /// `columns` is a slice of `(name, raw_data)` where `raw_data` is a
    /// contiguous byte buffer of N values in native little-endian format.
    ///
    /// This avoids all per-row overhead: no HashMap lookups, no partition
    /// name formatting, no per-value type coercion -- just memcpy.
    pub fn write_batch(
        &mut self,
        timestamps: &[i64],
        columns: &[(&str, &[u8])],
    ) -> Result<u64> {
        if timestamps.is_empty() {
            return Ok(0);
        }

        let n = timestamps.len();

        // Use first timestamp to determine partition
        let ts = Timestamp(timestamps[0]);
        let partition_path = self.partition_mgr.ensure_partition(ts)?;

        let need_switch = match &self.current_partition_path {
            Some(cached) => *cached != partition_path,
            None => true,
        };

        if need_switch {
            self.flush()?;
            self.open_partition(&partition_path)?;
            self.current_partition_path = Some(partition_path);
        }

        let pw = self.current_partition.as_mut().unwrap();

        // Write timestamp column using bulk memcpy
        let ts_col = self.meta.timestamp_column;
        if let Some(ref mut writer) = pw.fixed_columns[ts_col] {
            let ts_bytes = unsafe {
                std::slice::from_raw_parts(
                    timestamps.as_ptr() as *const u8,
                    timestamps.len() * 8,
                )
            };
            writer.append_bulk(ts_bytes)?;
        }

        // Write data columns via bulk memcpy — use pre-computed name->index map
        // instead of O(N) linear scan per column.
        for (col_name, raw_data) in columns {
            if let Some(&col_idx) = self.column_name_to_idx.get(*col_name) {
                if col_idx == ts_col {
                    continue;
                }
                if self.column_info[col_idx].is_variable {
                    continue;
                }
                if let Some(ref mut writer) = pw.fixed_columns[col_idx] {
                    writer.append_bulk(raw_data)?;
                }
            }
        }

        Ok(n as u64)
    }

    /// Write N rows at once using columnar bulk writes, indexed by column position.
    ///
    /// This is the fastest write path: no name lookups, no type coercion.
    /// All timestamps must map to the same partition.
    ///
    /// `col_data` is a slice of `(column_index, raw_bytes)` pairs where
    /// `column_index` is the 0-based position in the table schema (excluding
    /// the timestamp column, which is written from `timestamps`).
    ///
    /// Each `raw_bytes` buffer must contain exactly `timestamps.len()` elements
    /// in native little-endian format, packed contiguously.
    pub fn write_batch_raw(
        &mut self,
        timestamps: &[i64],
        col_data: &[(usize, &[u8])],
    ) -> Result<u64> {
        if timestamps.is_empty() {
            return Ok(0);
        }

        let n = timestamps.len();

        let ts = Timestamp(timestamps[0]);
        let partition_path = self.partition_mgr.ensure_partition(ts)?;

        let need_switch = match &self.current_partition_path {
            Some(cached) => *cached != partition_path,
            None => true,
        };

        if need_switch {
            self.flush()?;
            self.open_partition(&partition_path)?;
            self.current_partition_path = Some(partition_path);
        }

        let pw = self.current_partition.as_mut().unwrap();

        // Write timestamp column
        let ts_col = self.meta.timestamp_column;
        if let Some(ref mut writer) = pw.fixed_columns[ts_col] {
            let ts_bytes = unsafe {
                std::slice::from_raw_parts(
                    timestamps.as_ptr() as *const u8,
                    n * 8,
                )
            };
            writer.append_bulk(ts_bytes)?;
        }

        // Write data columns by index — zero overhead name resolution
        for &(col_idx, raw_data) in col_data {
            if col_idx == ts_col || col_idx >= self.column_info.len() {
                continue;
            }
            if self.column_info[col_idx].is_variable {
                continue;
            }
            if let Some(ref mut writer) = pw.fixed_columns[col_idx] {
                writer.append_bulk(raw_data)?;
            }
        }

        Ok(n as u64)
    }

    pub fn flush(&self) -> Result<()> {
        if let Some(pw) = &self.current_partition {
            for slot in &pw.fixed_columns {
                if let Some(writer) = slot {
                    writer.flush()?;
                }
            }
            for slot in &pw.var_columns {
                if let Some(writer) = slot {
                    writer.flush()?;
                }
            }
        }
        Ok(())
    }

    fn open_partition(&mut self, partition_path: &Path) -> Result<()> {
        let num_cols = self.meta.columns.len();
        let mut fixed_columns: Vec<Option<FixedColumnWriter>> = (0..num_cols).map(|_| None).collect();
        let mut var_columns: Vec<Option<VarColumnWriter>> = (0..num_cols).map(|_| None).collect();

        for (i, col_def) in self.meta.columns.iter().enumerate() {
            let col_type: ColumnType = col_def.col_type.into();

            if col_type.is_variable_length() {
                let data_path = partition_path.join(format!("{}.d", col_def.name));
                let index_path = partition_path.join(format!("{}.i", col_def.name));
                let writer = VarColumnWriter::open(&data_path, &index_path)?;
                var_columns[i] = Some(writer);
            } else {
                let data_path = partition_path.join(format!("{}.d", col_def.name));
                let writer = FixedColumnWriter::open(&data_path, col_type)?;
                fixed_columns[i] = Some(writer);
            }
        }

        self.current_partition = Some(PartitionWriter {
            fixed_columns,
            var_columns,
        });

        Ok(())
    }

    pub fn meta(&self) -> &TableMeta {
        &self.meta
    }

    /// Set deduplication configuration for this writer.
    pub fn set_dedup_config(&mut self, config: crate::dedup::DedupConfig) {
        self.dedup_config = Some(config);
    }

    /// Write multiple rows with optional deduplication.
    /// Each inner slice must match column order in metadata (excluding timestamp
    /// which is auto-filled from `timestamps`).
    pub fn write_rows_batch(
        &mut self,
        timestamps: &[Timestamp],
        rows: &[Vec<ColumnValue<'_>>],
    ) -> Result<u64> {
        assert_eq!(timestamps.len(), rows.len());

        // If dedup is configured, build a row-index set of unique keys.
        let indices_to_write: Vec<usize> = if let Some(cfg) = &self.dedup_config {
            if cfg.enabled && !cfg.key_columns.is_empty() {
                // Resolve key column indices.
                let key_indices: Vec<usize> = cfg
                    .key_columns
                    .iter()
                    .filter_map(|name| self.meta.columns.iter().position(|c| c.name == *name))
                    .collect();
                crate::dedup::unique_row_indices(rows, &key_indices)
            } else {
                (0..rows.len()).collect()
            }
        } else {
            (0..rows.len()).collect()
        };

        let mut count = 0u64;
        for &i in &indices_to_write {
            self.write_row(timestamps[i], &rows[i])?;
            count += 1;
        }
        Ok(count)
    }
}

/// A value to be written to a column.
#[derive(Debug, Clone)]
pub enum ColumnValue<'a> {
    I32(i32),
    I64(i64),
    F64(f64),
    Timestamp(Timestamp),
    Str(&'a str),
    Bytes(&'a [u8]),
    Null,
}

/// Controls whether writes go through the WAL (durable) or directly to
/// column files (fastest, but not crash-safe).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteMode {
    /// Write directly to column files (current behavior, fastest).
    Direct,
    /// Write through WAL (durable, default for production).
    Wal,
}

impl Default for WriteMode {
    fn default() -> Self {
        Self::Wal
    }
}

impl<'a> ColumnValue<'a> {
    /// Convert a borrowed `ColumnValue` into an `OwnedColumnValue` for use
    /// with the WAL writer.
    pub fn to_owned_value(&self) -> crate::wal::row_codec::OwnedColumnValue {
        use crate::wal::row_codec::OwnedColumnValue as OV;
        match self {
            ColumnValue::I32(v) => OV::I32(*v),
            ColumnValue::I64(v) => OV::I64(*v),
            ColumnValue::F64(v) => OV::F64(*v),
            ColumnValue::Timestamp(t) => OV::Timestamp(t.as_nanos()),
            ColumnValue::Str(s) => OV::Varchar(s.to_string()),
            ColumnValue::Bytes(b) => OV::Binary(b.to_vec()),
            ColumnValue::Null => OV::Null,
        }
    }

    /// Borrow a `ColumnValue<'static>` as a `ColumnValue<'a>` with a
    /// shorter lifetime. Useful when converting owned data back into
    /// references for partition rewriting.
    pub fn borrow_column_value(&self) -> ColumnValue<'_> {
        match self {
            ColumnValue::I32(v) => ColumnValue::I32(*v),
            ColumnValue::I64(v) => ColumnValue::I64(*v),
            ColumnValue::F64(v) => ColumnValue::F64(*v),
            ColumnValue::Timestamp(t) => ColumnValue::Timestamp(*t),
            ColumnValue::Str(s) => ColumnValue::Str(s),
            ColumnValue::Bytes(b) => ColumnValue::Bytes(b),
            ColumnValue::Null => ColumnValue::Null,
        }
    }
}

/// Fill NULL values for a newly added column across all existing partitions.
/// For fixed-width columns, writes zero bytes; for variable-width, writes empty strings.
pub fn add_column_to_partitions(
    table_dir: &Path,
    col_name: &str,
    col_type: ColumnType,
) -> Result<()> {
    let partition_mgr_entries = list_partition_dirs(table_dir)?;

    for partition_path in &partition_mgr_entries {
        // Determine the row count from any existing column file.
        let row_count = detect_partition_row_count(partition_path)?;
        if row_count == 0 {
            continue;
        }

        if col_type.is_variable_length() {
            let data_path = partition_path.join(format!("{col_name}.d"));
            let index_path = partition_path.join(format!("{col_name}.i"));
            let mut writer = VarColumnWriter::open(&data_path, &index_path)?;
            for _ in 0..row_count {
                writer.append(b"")?;
            }
            writer.flush()?;
        } else {
            let data_path = partition_path.join(format!("{col_name}.d"));
            let mut writer = FixedColumnWriter::open(&data_path, col_type)?;
            // Use NaN for floating-point types to represent NULL, zero for others.
            let null_bytes = match col_type {
                ColumnType::F64 => f64::NAN.to_le_bytes().to_vec(),
                ColumnType::F32 => f32::NAN.to_le_bytes().to_vec(),
                _ => vec![0u8; col_type.fixed_size().unwrap()],
            };
            for _ in 0..row_count {
                writer.append(&null_bytes)?;
            }
            writer.flush()?;
        }
    }
    Ok(())
}

/// Delete column files from all existing partitions.
pub fn drop_column_from_partitions(table_dir: &Path, col_name: &str) -> Result<()> {
    let partition_dirs = list_partition_dirs(table_dir)?;
    for partition_path in &partition_dirs {
        let data_path = partition_path.join(format!("{col_name}.d"));
        if data_path.exists() {
            std::fs::remove_file(&data_path)?;
        }
        let index_path = partition_path.join(format!("{col_name}.i"));
        if index_path.exists() {
            std::fs::remove_file(&index_path)?;
        }
    }
    Ok(())
}

/// Rename column files in all existing partitions.
pub fn rename_column_in_partitions(
    table_dir: &Path,
    old_name: &str,
    new_name: &str,
) -> Result<()> {
    let partition_dirs = list_partition_dirs(table_dir)?;
    for partition_path in &partition_dirs {
        let old_data = partition_path.join(format!("{old_name}.d"));
        let new_data = partition_path.join(format!("{new_name}.d"));
        if old_data.exists() {
            std::fs::rename(&old_data, &new_data)?;
        }
        let old_index = partition_path.join(format!("{old_name}.i"));
        let new_index = partition_path.join(format!("{new_name}.i"));
        if old_index.exists() {
            std::fs::rename(&old_index, &new_index)?;
        }
    }
    Ok(())
}

/// List partition directories under a table directory (anything that is a
/// directory and does not start with '_').
///
/// Also includes cold partitions stored as `.xpqt` files in the `_cold/`
/// subdirectory -- these are returned as paths to the `.xpqt` files so that
/// `TieredPartitionReader` can open them transparently.
fn list_partition_dirs(table_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    let mut seen_names = std::collections::HashSet::new();

    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy().to_string();
                if !name_str.starts_with('_') {
                    seen_names.insert(name_str);
                    dirs.push(path);
                }
            }
        }
    }

    // Also discover cold partitions in _cold/ subdirectory.
    let cold_dir = table_dir.join("_cold");
    if cold_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&cold_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let pname = if name.ends_with(".parquet") {
                    Some(name.trim_end_matches(".parquet").to_string())
                } else if name.ends_with(".xpqt") {
                    Some(name.trim_end_matches(".xpqt").to_string())
                } else {
                    None
                };
                if let Some(partition_name) = pname {
                    if !seen_names.contains(&partition_name) {
                        dirs.push(entry.path());
                        seen_names.insert(partition_name);
                    }
                }
            }
        }
    }

    dirs.sort();
    Ok(dirs)
}

/// Detect how many rows exist in a partition by inspecting the first column file found.
fn detect_partition_row_count(partition_path: &Path) -> Result<u64> {
    for entry in std::fs::read_dir(partition_path)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".d") && path.is_file() {
            // Check if there is a matching .i file (variable-length).
            let base = &name[..name.len() - 2];
            let index_path = partition_path.join(format!("{base}.i"));
            if index_path.exists() {
                // Variable-length column: row count = index file size / 8.
                let index_len = std::fs::metadata(&index_path)?.len();
                return Ok(index_len / 8);
            } else {
                // Fixed-width column: we need to know the element size.
                // Use file size / 8 as a rough estimate (most common is 8-byte columns).
                // For a more precise approach, we'd need the column type, but this works
                // for the typical case (timestamps, i64, f64 are all 8 bytes).
                let file_len = std::fs::metadata(&path)?.len();
                // Try common sizes: 8, 4, 1, 16, 2
                // The simplest approach: assume 8 bytes (timestamps are always present).
                return Ok(file_len / 8);
            }
        }
    }
    Ok(0)
}

/// Rewrite an entire partition from in-memory row data.
///
/// Deletes all existing column files in the partition directory and writes
/// new ones from the supplied rows. Each inner `Vec<ColumnValue>` must have
/// values in the same order as `meta.columns`.
///
/// Returns the number of rows written. If `rows` is empty the partition
/// directory is removed entirely.
pub fn rewrite_partition(
    partition_path: &Path,
    meta: &TableMeta,
    rows: &[Vec<ColumnValue<'_>>],
) -> Result<u64> {
    if rows.is_empty() {
        // Remove the entire partition directory.
        if partition_path.exists() {
            std::fs::remove_dir_all(partition_path)?;
        }
        return Ok(0);
    }

    // Delete all existing column files.
    for col_def in &meta.columns {
        let data_path = partition_path.join(format!("{}.d", col_def.name));
        if data_path.exists() {
            std::fs::remove_file(&data_path)?;
        }
        let index_path = partition_path.join(format!("{}.i", col_def.name));
        if index_path.exists() {
            std::fs::remove_file(&index_path)?;
        }
    }

    // Write new column files.
    for (col_idx, col_def) in meta.columns.iter().enumerate() {
        let col_type: ColumnType = col_def.col_type.into();

        if col_type.is_variable_length() {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            let index_path = partition_path.join(format!("{}.i", col_def.name));
            let mut writer = VarColumnWriter::open(&data_path, &index_path)?;
            for row in rows {
                if col_idx < row.len() {
                    match &row[col_idx] {
                        ColumnValue::Str(s) => writer.append_str(s)?,
                        ColumnValue::Bytes(b) => writer.append(b)?,
                        _ => writer.append(b"")?,
                    }
                } else {
                    writer.append(b"")?;
                }
            }
            writer.flush()?;
        } else {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            let mut writer = FixedColumnWriter::open(&data_path, col_type)?;
            for row in rows {
                if col_idx < row.len() {
                    // Coerce value to match column type (same logic as write_row).
                    match col_type {
                        ColumnType::F64 => {
                            let fval = match &row[col_idx] {
                                ColumnValue::F64(v) => *v,
                                ColumnValue::I64(v) => *v as f64,
                                ColumnValue::I32(v) => *v as f64,
                                ColumnValue::Timestamp(t) => t.as_nanos() as f64,
                                ColumnValue::Null => f64::NAN,
                                _ => 0.0,
                            };
                            writer.append_f64(fval)?;
                        }
                        ColumnType::I64 | ColumnType::Timestamp => {
                            let ival = match &row[col_idx] {
                                ColumnValue::I64(v) => *v,
                                ColumnValue::F64(v) => *v as i64,
                                ColumnValue::I32(v) => *v as i64,
                                ColumnValue::Timestamp(t) => t.as_nanos(),
                                _ => 0,
                            };
                            writer.append_i64(ival)?;
                        }
                        ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
                            let ival = match &row[col_idx] {
                                ColumnValue::I32(v) => *v,
                                ColumnValue::I64(v) => *v as i32,
                                ColumnValue::F64(v) => *v as i32,
                                _ => 0,
                            };
                            writer.append_i32(ival)?;
                        }
                        _ => {
                            match &row[col_idx] {
                                ColumnValue::I64(v) => writer.append_i64(*v)?,
                                ColumnValue::F64(v) => writer.append_f64(*v)?,
                                ColumnValue::I32(v) => writer.append_i32(*v)?,
                                ColumnValue::Timestamp(t) => writer.append_i64(t.as_nanos())?,
                                _ => writer.append_i64(0)?,
                            }
                        }
                    }
                } else {
                    writer.append_i64(0)?;
                }
            }
            writer.flush()?;
        }
    }

    Ok(rows.len() as u64)
}

/// List all partition directories under a table directory (public for use by
/// the query executor). Anything that is a directory and does not start with
/// `_`.
pub fn list_partitions(table_dir: &Path) -> Result<Vec<PathBuf>> {
    list_partition_dirs(table_dir)
}

/// Read all rows from a partition, returning one `Vec<ColumnValue<'static>>`
/// per row with values ordered by `meta.columns`.
pub fn read_partition_rows(
    partition_path: &Path,
    meta: &TableMeta,
) -> Result<Vec<Vec<ColumnValue<'static>>>> {
    read_partition_rows_inner(partition_path, meta, None)
}

/// Like [`read_partition_rows`] but with tiered storage support.
///
/// When `table_dir` is provided, warm (LZ4-compressed) and cold (XPQT)
/// partitions are transparently decompressed/converted before reading.
pub fn read_partition_rows_tiered(
    partition_path: &Path,
    meta: &TableMeta,
    table_dir: &Path,
) -> Result<Vec<Vec<ColumnValue<'static>>>> {
    read_partition_rows_inner(partition_path, meta, Some(table_dir))
}

fn read_partition_rows_inner(
    partition_path: &Path,
    meta: &TableMeta,
    table_dir: Option<&Path>,
) -> Result<Vec<Vec<ColumnValue<'static>>>> {
    use crate::tiered::TieredPartitionReader;

    // Use TieredPartitionReader to get the native path for reading.
    let tiered_reader = if let Some(td) = table_dir {
        TieredPartitionReader::open(partition_path, td).ok()
    } else {
        None
    };
    let native_path = tiered_reader
        .as_ref()
        .map(|r| r.native_path())
        .unwrap_or(partition_path);

    // Open readers for all columns.
    let mut fixed_readers: Vec<(usize, FixedColumnReader, ColumnType)> = Vec::new();
    let mut var_readers: Vec<(usize, VarColumnReader)> = Vec::new();

    for (i, col_def) in meta.columns.iter().enumerate() {
        let col_type: ColumnType = col_def.col_type.into();
        if col_type.is_variable_length() {
            let data_path = native_path.join(format!("{}.d", col_def.name));
            let index_path = native_path.join(format!("{}.i", col_def.name));
            if data_path.exists() && index_path.exists() {
                let reader = VarColumnReader::open(&data_path, &index_path)?;
                var_readers.push((i, reader));
            }
        } else {
            let data_path = native_path.join(format!("{}.d", col_def.name));
            if data_path.exists() {
                let reader = FixedColumnReader::open(&data_path, col_type)?;
                fixed_readers.push((i, reader, col_type));
            }
        }
    }

    // Determine row count from the first available reader.
    let row_count = if let Some((_, reader, _)) = fixed_readers.first() {
        reader.row_count()
    } else if let Some((_, reader)) = var_readers.first() {
        reader.row_count()
    } else {
        return Ok(Vec::new());
    };

    if row_count == 0 {
        return Ok(Vec::new());
    }

    let mut rows = Vec::with_capacity(row_count as usize);
    for row_idx in 0..row_count {
        let mut row: Vec<ColumnValue<'static>> = vec![ColumnValue::Null; meta.columns.len()];

        for (col_idx, reader, col_type) in &fixed_readers {
            let val = match col_type {
                ColumnType::Timestamp => ColumnValue::Timestamp(Timestamp(reader.read_i64(row_idx))),
                ColumnType::I64 => ColumnValue::I64(reader.read_i64(row_idx)),
                ColumnType::F64 => {
                    let v = reader.read_f64(row_idx);
                    if v.is_nan() { ColumnValue::Null } else { ColumnValue::F64(v) }
                }
                ColumnType::I32 | ColumnType::Symbol => ColumnValue::I32(reader.read_i32(row_idx)),
                ColumnType::F32 => ColumnValue::F64(f32::from_le_bytes(reader.read_raw(row_idx).try_into().unwrap()) as f64),
                _ => ColumnValue::I64(reader.read_i64(row_idx)),
            };
            row[*col_idx] = val;
        }

        for (col_idx, reader) in &var_readers {
            let s = reader.read_str(row_idx).to_string();
            if s == "\0" {
                row[*col_idx] = ColumnValue::Null;
            } else {
                // We use a leaked string to get a 'static lifetime.
                // This is acceptable since these are temporary values used during
                // partition rewriting.
                row[*col_idx] = ColumnValue::Str(Box::leak(s.into_boxed_str()));
            }
        }

        rows.push(row);
    }

    Ok(rows)
}

/// Drop an entire table directory.
pub fn drop_table(db_root: &Path, table_name: &str) -> Result<()> {
    let table_dir = db_root.join(table_name);
    if !table_dir.exists() {
        return Err(ExchangeDbError::TableNotFound(table_name.to_string()));
    }
    std::fs::remove_dir_all(&table_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_and_write_table() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create table
        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .indexed_column("symbol", ColumnType::Symbol)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Write data
        let mut writer = TableWriter::open(db_root, "trades").unwrap();

        let ts = Timestamp::from_secs(1710513000); // 2024-03-15
        writer
            .write_row(
                ts,
                &[
                    ColumnValue::I32(0), // symbol ID
                    ColumnValue::F64(65000.50),
                    ColumnValue::F64(1.5),
                ],
            )
            .unwrap();

        writer.flush().unwrap();

        // Verify partition created
        let partition_dir = db_root.join("trades").join("2024-03-15");
        assert!(partition_dir.exists());
        assert!(partition_dir.join("timestamp.d").exists());
        assert!(partition_dir.join("price.d").exists());
    }

    #[test]
    fn alter_table_add_column() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Write a row
        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        writer
            .write_row(ts, &[ColumnValue::F64(100.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        // Add column
        let table_dir = db_root.join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.version, 1);
        meta.add_column("exchange", ColumnType::Varchar).unwrap();
        meta.save(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.version, 2);
        assert_eq!(meta.columns.len(), 3);
        assert_eq!(meta.columns[2].name, "exchange");

        // Fill NULL in existing partitions
        add_column_to_partitions(&table_dir, "exchange", ColumnType::Varchar).unwrap();

        // Verify the new column file exists in the partition
        let partition_dir = db_root.join("trades").join("2024-03-15");
        assert!(partition_dir.join("exchange.d").exists());
        assert!(partition_dir.join("exchange.i").exists());
    }

    #[test]
    fn alter_table_drop_column() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Write a row
        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        writer
            .write_row(ts, &[ColumnValue::F64(100.0), ColumnValue::F64(1.5)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let table_dir = db_root.join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.drop_column("volume").unwrap();
        meta.save(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.version, 2);

        drop_column_from_partitions(&table_dir, "volume").unwrap();

        let partition_dir = db_root.join("trades").join("2024-03-15");
        assert!(!partition_dir.join("volume.d").exists());
        assert!(partition_dir.join("price.d").exists());
    }

    #[test]
    fn alter_table_rename_column() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Write a row
        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let ts = Timestamp::from_secs(1710513000);
        writer
            .write_row(ts, &[ColumnValue::F64(100.0)])
            .unwrap();
        writer.flush().unwrap();
        drop(writer);

        let table_dir = db_root.join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.rename_column("price", "trade_price").unwrap();
        meta.save(&table_dir.join("_meta")).unwrap();
        assert_eq!(meta.columns[1].name, "trade_price");
        assert_eq!(meta.version, 2);

        rename_column_in_partitions(&table_dir, "price", "trade_price").unwrap();

        let partition_dir = db_root.join("trades").join("2024-03-15");
        assert!(!partition_dir.join("price.d").exists());
        assert!(partition_dir.join("trade_price.d").exists());
    }

    #[test]
    fn alter_table_cannot_drop_timestamp() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        let table_dir = db_root.join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        let result = meta.drop_column("timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn alter_table_add_duplicate_column_fails() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        let table_dir = db_root.join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        let result = meta.add_column("price", ColumnType::F64);
        assert!(result.is_err());
    }

    #[test]
    fn alter_table_set_column_type() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F32)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        let table_dir = db_root.join("trades");
        let mut meta = TableMeta::load(&table_dir.join("_meta")).unwrap();
        meta.set_column_type("price", ColumnType::F64).unwrap();
        meta.save(&table_dir.join("_meta")).unwrap();

        let reloaded = TableMeta::load(&table_dir.join("_meta")).unwrap();
        let price_type: ColumnType = reloaded.columns[1].col_type.into();
        assert_eq!(price_type, ColumnType::F64);
    }

    #[test]
    fn drop_table_works() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();

        assert!(db_root.join("trades").exists());
        drop_table(db_root, "trades").unwrap();
        assert!(!db_root.join("trades").exists());
    }

    #[test]
    fn drop_table_not_found() {
        let dir = tempdir().unwrap();
        let result = drop_table(dir.path(), "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn write_batch_correctness() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("symbol", ColumnType::I32)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::None)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();

        let n = 1000;
        let base_ts: i64 = 1_704_067_200_000_000_000;
        let timestamps: Vec<i64> = (0..n).map(|i| base_ts + i as i64 * 1_000_000).collect();
        let symbols: Vec<i32> = (0..n).map(|i| (i % 100) as i32).collect();
        let prices: Vec<f64> = (0..n).map(|i| 50_000.0 + i as f64 * 0.5).collect();
        let volumes: Vec<f64> = (0..n).map(|i| 1.0 + i as f64 * 0.01).collect();

        let sym_bytes = unsafe {
            std::slice::from_raw_parts(symbols.as_ptr() as *const u8, symbols.len() * 4)
        };
        let price_bytes = unsafe {
            std::slice::from_raw_parts(prices.as_ptr() as *const u8, prices.len() * 8)
        };
        let vol_bytes = unsafe {
            std::slice::from_raw_parts(volumes.as_ptr() as *const u8, volumes.len() * 8)
        };

        let written = writer
            .write_batch(
                &timestamps,
                &[
                    ("symbol", sym_bytes),
                    ("price", price_bytes),
                    ("volume", vol_bytes),
                ],
            )
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(written, n as u64);

        // Drop writer to truncate mmap files to logical length
        drop(writer);

        // Verify data by reading back
        let partition_dir = db_root.join("trades").join("default");
        let ts_reader =
            FixedColumnReader::open(&partition_dir.join("timestamp.d"), ColumnType::Timestamp)
                .unwrap();
        let price_reader =
            FixedColumnReader::open(&partition_dir.join("price.d"), ColumnType::F64).unwrap();
        let sym_reader =
            FixedColumnReader::open(&partition_dir.join("symbol.d"), ColumnType::I32).unwrap();
        let vol_reader =
            FixedColumnReader::open(&partition_dir.join("volume.d"), ColumnType::F64).unwrap();

        assert_eq!(ts_reader.row_count(), n as u64);
        assert_eq!(price_reader.row_count(), n as u64);
        assert_eq!(sym_reader.row_count(), n as u64);
        assert_eq!(vol_reader.row_count(), n as u64);

        // Spot-check values
        assert_eq!(ts_reader.read_i64(0), base_ts);
        assert_eq!(ts_reader.read_i64(999), base_ts + 999 * 1_000_000);
        assert_eq!(sym_reader.read_i32(42), 42);
        assert_eq!(price_reader.read_f64(0), 50_000.0);
        assert_eq!(price_reader.read_f64(500), 50_000.0 + 500.0 * 0.5);
        assert!((vol_reader.read_f64(100) - (1.0 + 100.0 * 0.01)).abs() < 1e-10);
    }

    #[test]
    fn write_batch_empty() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let _meta = TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("price", ColumnType::F64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::None)
            .build(db_root)
            .unwrap();

        let mut writer = TableWriter::open(db_root, "trades").unwrap();
        let written = writer.write_batch(&[], &[]).unwrap();
        assert_eq!(written, 0);
    }
}
