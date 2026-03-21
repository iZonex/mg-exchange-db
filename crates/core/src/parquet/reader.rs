//! Reader for the PAR1XCHG columnar format.

use crate::parquet::writer::MAGIC;
use crate::table::TableMeta;
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::ColumnType;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

/// A value read from a PAR1XCHG file.
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
    Null,
}

/// Metadata for a single column in the file.
#[derive(Debug, Clone)]
pub struct ParquetColumnMeta {
    pub name: String,
    pub col_type: ColumnType,
    pub data_offset: u64,
    pub data_length: u64,
    pub compressed_length: u64,
    pub encoding: u8,
}

/// File-level metadata.
#[derive(Debug, Clone)]
pub struct ParquetMetadata {
    pub num_rows: u64,
    pub columns: Vec<ParquetColumnMeta>,
}

/// Reader that can open and decode PAR1XCHG files.
pub struct ParquetReader {
    #[allow(dead_code)]
    path: PathBuf,
    data: Vec<u8>,
    metadata: ParquetMetadata,
}

impl ParquetReader {
    /// Open a PAR1XCHG file and parse its metadata.
    pub fn open(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;

        // Minimum file size: 8 (header magic) + 2 (version) + 2 (num_cols) + 8 (num_rows)
        //                    + 8 (checksum) + 8 (footer magic) = 36
        if data.len() < 36 {
            return Err(ExchangeDbError::Corruption(
                "PAR1XCHG file too short".to_string(),
            ));
        }

        // Validate footer magic.
        let footer_magic = &data[data.len() - 8..];
        if footer_magic != MAGIC {
            return Err(ExchangeDbError::Corruption(
                "invalid PAR1XCHG footer magic".to_string(),
            ));
        }

        // Validate header magic.
        if &data[0..8] != MAGIC {
            return Err(ExchangeDbError::Corruption(
                "invalid PAR1XCHG header magic".to_string(),
            ));
        }

        // Verify checksum: everything before the last 16 bytes (checksum + magic).
        let payload_end = data.len() - 16;
        let stored_checksum =
            u64::from_le_bytes(data[payload_end..payload_end + 8].try_into().unwrap());
        let computed_checksum = xxhash_rust::xxh3::xxh3_64(&data[..payload_end]);
        if stored_checksum != computed_checksum {
            return Err(ExchangeDbError::Corruption(format!(
                "PAR1XCHG checksum mismatch: stored {stored_checksum:#x}, computed {computed_checksum:#x}"
            )));
        }

        // Parse header.
        let _version = u16::from_le_bytes(data[8..10].try_into().unwrap());
        let num_columns = u16::from_le_bytes(data[10..12].try_into().unwrap());
        let num_rows = u64::from_le_bytes(data[12..20].try_into().unwrap());

        // Parse column metadata.
        let mut cursor = Cursor::new(&data[20..]);
        let mut columns = Vec::with_capacity(num_columns as usize);

        for _ in 0..num_columns {
            let mut name_len_buf = [0u8; 2];
            cursor.read_exact(&mut name_len_buf)?;
            let name_len = u16::from_le_bytes(name_len_buf) as usize;

            let mut name_buf = vec![0u8; name_len];
            cursor.read_exact(&mut name_buf)?;
            let name = String::from_utf8(name_buf)
                .map_err(|e| ExchangeDbError::Corruption(format!("invalid column name: {e}")))?;

            let mut type_buf = [0u8; 1];
            cursor.read_exact(&mut type_buf)?;
            let col_type = column_type_from_tag(type_buf[0]);

            let mut offset_buf = [0u8; 8];
            cursor.read_exact(&mut offset_buf)?;
            let data_offset = u64::from_le_bytes(offset_buf);

            let mut len_buf = [0u8; 8];
            cursor.read_exact(&mut len_buf)?;
            let data_length = u64::from_le_bytes(len_buf);

            let mut comp_buf = [0u8; 8];
            cursor.read_exact(&mut comp_buf)?;
            let compressed_length = u64::from_le_bytes(comp_buf);

            let mut enc_buf = [0u8; 1];
            cursor.read_exact(&mut enc_buf)?;
            let encoding = enc_buf[0];

            columns.push(ParquetColumnMeta {
                name,
                col_type,
                data_offset,
                data_length,
                compressed_length,
                encoding,
            });
        }

        let metadata = ParquetMetadata { num_rows, columns };

        Ok(Self {
            path: path.to_path_buf(),
            data,
            metadata,
        })
    }

    /// Access parsed metadata without reading data.
    pub fn metadata(&self) -> &ParquetMetadata {
        &self.metadata
    }

    /// Read all rows from the file.
    pub fn read_all(&self) -> Result<Vec<Vec<RowValue>>> {
        if self.metadata.num_rows == 0 {
            return Ok(Vec::new());
        }

        // Decompress all columns.
        let decoded_columns: Vec<Vec<u8>> = self
            .metadata
            .columns
            .iter()
            .map(|cm| self.decompress_column(cm))
            .collect::<Result<Vec<_>>>()?;

        // Decode row-by-row.
        let num_rows = self.metadata.num_rows as usize;
        let mut rows = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(self.metadata.columns.len());
            for (col_idx, cm) in self.metadata.columns.iter().enumerate() {
                let val = decode_value_from_column(
                    &decoded_columns[col_idx],
                    row_idx,
                    cm.col_type,
                    self.metadata.num_rows,
                );
                row.push(val);
            }
            rows.push(row);
        }

        Ok(rows)
    }

    /// Read only specific columns (projection pushdown).
    pub fn read_columns(&self, columns: &[String]) -> Result<Vec<Vec<RowValue>>> {
        if self.metadata.num_rows == 0 {
            return Ok(Vec::new());
        }

        // Find column indices.
        let selected: Vec<&ParquetColumnMeta> = columns
            .iter()
            .filter_map(|name| self.metadata.columns.iter().find(|cm| cm.name == *name))
            .collect();

        // Decompress only selected columns.
        let decoded_columns: Vec<Vec<u8>> = selected
            .iter()
            .map(|cm| self.decompress_column(cm))
            .collect::<Result<Vec<_>>>()?;

        let num_rows = self.metadata.num_rows as usize;
        let mut rows = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let mut row = Vec::with_capacity(selected.len());
            for (col_idx, cm) in selected.iter().enumerate() {
                let val = decode_value_from_column(
                    &decoded_columns[col_idx],
                    row_idx,
                    cm.col_type,
                    self.metadata.num_rows,
                );
                row.push(val);
            }
            rows.push(row);
        }

        Ok(rows)
    }

    /// Convert back to native partition format (column files on disk).
    pub fn to_partition(&self, output_dir: &Path, meta: &TableMeta) -> Result<u64> {
        if self.metadata.num_rows == 0 {
            if !output_dir.exists() {
                std::fs::create_dir_all(output_dir)?;
            }
            // Write empty column files.
            for col_def in &meta.columns {
                let col_type: ColumnType = col_def.col_type.into();
                std::fs::write(output_dir.join(format!("{}.d", col_def.name)), &[])?;
                if col_type.is_variable_length() {
                    std::fs::write(output_dir.join(format!("{}.i", col_def.name)), &[])?;
                }
            }
            return Ok(0);
        }

        if !output_dir.exists() {
            std::fs::create_dir_all(output_dir)?;
        }

        // Decompress each column and write to disk.
        for cm in &self.metadata.columns {
            let decompressed = self.decompress_column(cm)?;

            // Look up in table meta to determine if variable-length.
            let col_def = meta.columns.iter().find(|c| c.name == cm.name);
            let col_type: Option<ColumnType> = col_def.map(|c| c.col_type.into());

            if col_type.map_or(false, |ct| ct.is_variable_length()) {
                // Variable-length: stored as [4 bytes data_len][data][4 bytes index_len][index]
                let mut cursor = Cursor::new(&decompressed);

                let mut len_buf = [0u8; 4];
                cursor.read_exact(&mut len_buf)?;
                let data_len = u32::from_le_bytes(len_buf) as usize;

                let mut d_data = vec![0u8; data_len];
                cursor.read_exact(&mut d_data)?;

                cursor.read_exact(&mut len_buf)?;
                let index_len = u32::from_le_bytes(len_buf) as usize;

                let mut i_data = vec![0u8; index_len];
                cursor.read_exact(&mut i_data)?;

                std::fs::write(output_dir.join(format!("{}.d", cm.name)), &d_data)?;
                std::fs::write(output_dir.join(format!("{}.i", cm.name)), &i_data)?;
            } else {
                std::fs::write(output_dir.join(format!("{}.d", cm.name)), &decompressed)?;
            }
        }

        Ok(self.metadata.num_rows)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn decompress_column(&self, cm: &ParquetColumnMeta) -> Result<Vec<u8>> {
        let start = cm.data_offset as usize;
        let end = start + cm.compressed_length as usize;
        if end > self.data.len() {
            return Err(ExchangeDbError::Corruption(format!(
                "column '{}' data extends past end of file",
                cm.name
            )));
        }
        let compressed = &self.data[start..end];
        let decompressed = lz4_flex::decompress_size_prepended(compressed).map_err(|e| {
            ExchangeDbError::Corruption(format!(
                "LZ4 decompression failed for column '{}': {e}",
                cm.name
            ))
        })?;
        Ok(decompressed)
    }
}

// ---------------------------------------------------------------------------
// Decode helpers
// ---------------------------------------------------------------------------

/// Decode a single value at `row_idx` from a decompressed column buffer.
fn decode_value_from_column(
    buf: &[u8],
    row_idx: usize,
    col_type: ColumnType,
    _num_rows: u64,
) -> RowValue {
    if col_type.is_variable_length() {
        // The whole column is stored as [4 bytes data_len][data][4 bytes index_len][index]
        // We need to parse the index to find the value for this row.
        decode_variable_value(buf, row_idx)
    } else {
        let elem_size = col_type.fixed_size().unwrap();
        let offset = row_idx * elem_size;
        if offset + elem_size > buf.len() {
            return RowValue::Null;
        }
        let bytes = &buf[offset..offset + elem_size];
        match col_type {
            ColumnType::Timestamp => RowValue::Timestamp(i64::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::I64 | ColumnType::GeoHash => {
                RowValue::I64(i64::from_le_bytes(bytes.try_into().unwrap()))
            }
            ColumnType::F64 => RowValue::F64(f64::from_le_bytes(bytes.try_into().unwrap())),
            ColumnType::F32 => {
                RowValue::F64(f32::from_le_bytes(bytes.try_into().unwrap()) as f64)
            }
            ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
                RowValue::I64(i32::from_le_bytes(bytes.try_into().unwrap()) as i64)
            }
            ColumnType::I16 | ColumnType::Char => {
                RowValue::I64(i16::from_le_bytes(bytes.try_into().unwrap()) as i64)
            }
            ColumnType::Boolean | ColumnType::I8 => RowValue::I64(bytes[0] as i64),
            _ => {
                // For Uuid, Long128, Long256 etc. return as Bytes.
                RowValue::Bytes(bytes.to_vec())
            }
        }
    }
}

/// Decode a variable-length value from the combined [data_len][data][index_len][index] format.
fn decode_variable_value(buf: &[u8], row_idx: usize) -> RowValue {
    if buf.len() < 8 {
        return RowValue::Str(String::new());
    }
    // Parse: [4 bytes data_len][data][4 bytes index_len][index]
    let data_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let data_section = &buf[4..4 + data_len];

    let index_start = 4 + data_len;
    if index_start + 4 > buf.len() {
        return RowValue::Str(String::new());
    }
    let index_len = u32::from_le_bytes(buf[index_start..index_start + 4].try_into().unwrap()) as usize;
    let index_section = &buf[index_start + 4..index_start + 4 + index_len];

    // Each index entry is 8 bytes (u64 offset into data section).
    let idx_offset = row_idx * 8;
    if idx_offset + 8 > index_section.len() {
        return RowValue::Str(String::new());
    }
    let data_offset =
        u64::from_le_bytes(index_section[idx_offset..idx_offset + 8].try_into().unwrap()) as usize;

    // Read length prefix from data section.
    if data_offset + 4 > data_section.len() {
        return RowValue::Str(String::new());
    }
    let val_len =
        u32::from_le_bytes(data_section[data_offset..data_offset + 4].try_into().unwrap()) as usize;
    if data_offset + 4 + val_len > data_section.len() {
        return RowValue::Str(String::new());
    }
    let val_bytes = &data_section[data_offset + 4..data_offset + 4 + val_len];

    match std::str::from_utf8(val_bytes) {
        Ok(s) => RowValue::Str(s.to_string()),
        Err(_) => RowValue::Bytes(val_bytes.to_vec()),
    }
}

/// Map a u8 tag back to a `ColumnType`.
fn column_type_from_tag(tag: u8) -> ColumnType {
    match tag {
        0 => ColumnType::Boolean,
        1 => ColumnType::I8,
        2 => ColumnType::I16,
        3 => ColumnType::I32,
        4 => ColumnType::I64,
        5 => ColumnType::F32,
        6 => ColumnType::F64,
        7 => ColumnType::Timestamp,
        8 => ColumnType::Symbol,
        9 => ColumnType::Varchar,
        10 => ColumnType::Binary,
        11 => ColumnType::Uuid,
        12 => ColumnType::Date,
        13 => ColumnType::Char,
        14 => ColumnType::IPv4,
        15 => ColumnType::Long128,
        16 => ColumnType::Long256,
        17 => ColumnType::GeoHash,
        _ => ColumnType::I64, // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrupt_magic_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.parquet");
        std::fs::write(&path, b"BADMAGICXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX").unwrap();
        let result = ParquetReader::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn file_too_short_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tiny.parquet");
        std::fs::write(&path, b"PAR1XCHG").unwrap();
        let result = ParquetReader::open(&path);
        assert!(result.is_err());
    }
}
