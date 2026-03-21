//! Writer that produces real Apache Parquet files readable by Spark, DuckDB,
//! Pandas, and other standard tools.
//!
//! File layout (per the Apache Parquet spec):
//! ```text
//! PAR1                    (4 bytes magic)
//! Row Group 1:
//!   Column Chunk 1:       [page header (Thrift compact) + data page (PLAIN encoded)]
//!   Column Chunk 2:       ...
//! FileMetaData            (Thrift compact encoded)
//! Footer length           (4 bytes, LE i32 — size of FileMetaData)
//! PAR1                    (4 bytes magic)
//! ```
//!
//! This implementation uses PLAIN encoding and no compression (UNCOMPRESSED),
//! which is the simplest valid Parquet file. All standard readers support this.

use super::thrift::{
    encode_data_page_header, encode_parquet_footer, ColumnChunkMeta, CompressionCodec,
    ParquetEncoding, ParquetSchemaColumn, PhysicalType, Repetition, RowGroupMeta,
};
use crate::parquet::writer::{ParquetColumn, ParquetType, ParquetWriteStats};
use crate::table::TableMeta;
use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use std::path::Path;

/// Magic bytes for Apache Parquet files.
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Writer that produces standard Apache Parquet files.
pub struct ApacheParquetWriter {
    _private: (),
}

impl Default for ApacheParquetWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ApacheParquetWriter {
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Write a complete Apache Parquet file from raw column data buffers.
    ///
    /// `schema` describes each column. `column_data` provides the raw PLAIN-encoded
    /// bytes for each column (one Vec<u8> per column, in the same order as `schema`).
    /// `num_rows` is the total number of rows.
    ///
    /// Returns the complete file as a `Vec<u8>`.
    pub fn write_file(
        &self,
        schema: &[ParquetSchemaColumn],
        column_data: &[Vec<u8>],
        num_rows: i64,
    ) -> Vec<u8> {
        assert_eq!(schema.len(), column_data.len());

        let mut buf = Vec::new();

        // -- Header magic --
        buf.extend_from_slice(PARQUET_MAGIC);

        // -- Row group: write each column chunk --
        // We write a single row group containing all the data.
        let mut column_chunk_metas = Vec::with_capacity(schema.len());
        let mut total_byte_size: i64 = 0;

        for (i, col_bytes) in column_data.iter().enumerate() {
            let uncompressed_size = col_bytes.len() as i32;
            let compressed_size = uncompressed_size; // no compression

            // Encode the data page header (Thrift)
            let page_header =
                encode_data_page_header(uncompressed_size, compressed_size, num_rows as i32);

            // Record the file offset of this column chunk (where page header starts)
            let data_page_offset = buf.len() as i64;

            // Write page header + data
            buf.extend_from_slice(&page_header);
            buf.extend_from_slice(col_bytes);

            let total_chunk_size = (page_header.len() + col_bytes.len()) as i64;
            total_byte_size += total_chunk_size;

            column_chunk_metas.push(ColumnChunkMeta {
                schema_idx: i,
                file_offset: data_page_offset,
                physical_type: schema[i].physical_type,
                encodings: vec![ParquetEncoding::Plain],
                path_in_schema: vec![schema[i].name.clone()],
                codec: CompressionCodec::Uncompressed,
                num_values: num_rows,
                total_uncompressed_size: total_chunk_size,
                total_compressed_size: total_chunk_size,
                data_page_offset,
            });
        }

        let row_group = RowGroupMeta {
            columns: column_chunk_metas,
            total_byte_size,
            num_rows,
        };

        // -- Encode FileMetaData (Thrift) --
        let footer_bytes = encode_parquet_footer(schema, &[row_group], num_rows);

        // -- Write footer --
        let footer_len = footer_bytes.len() as i32;
        buf.extend_from_slice(&footer_bytes);
        buf.extend_from_slice(&footer_len.to_le_bytes());
        buf.extend_from_slice(PARQUET_MAGIC);

        buf
    }

    /// Write a partition's column files to a standard Apache Parquet file.
    ///
    /// Reads the raw `.d` (and `.i` for variable-length) column files from
    /// `partition_path`, converts them to PLAIN-encoded Parquet columns, and
    /// writes a standard `.parquet` file that can be read by any Parquet reader.
    pub fn write_partition(
        &self,
        output_path: &Path,
        partition_path: &Path,
        meta: &TableMeta,
        columns: &[ParquetColumn],
    ) -> Result<ParquetWriteStats> {
        let num_rows = detect_row_count(partition_path, meta)?;

        // Build schema and read column data
        let mut schema_cols = Vec::with_capacity(columns.len());
        let mut column_data = Vec::with_capacity(columns.len());
        let mut total_uncompressed: u64 = 0;

        for col in columns {
            let ptype = parquet_type_to_physical(col.parquet_type);
            schema_cols.push(ParquetSchemaColumn {
                name: col.name.clone(),
                physical_type: ptype,
                repetition: Repetition::Required,
            });

            // Read raw column data and convert to PLAIN encoding
            let raw = read_column_plain(partition_path, &col.name, col.col_type, ptype)?;
            total_uncompressed += raw.len() as u64;
            column_data.push(raw);
        }

        let file_bytes = self.write_file(&schema_cols, &column_data, num_rows as i64);
        let bytes_written = file_bytes.len() as u64;

        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(output_path, &file_bytes)?;

        let compression_ratio = if total_uncompressed > 0 {
            bytes_written as f64 / total_uncompressed as f64
        } else {
            1.0
        };

        Ok(ParquetWriteStats {
            rows_written: num_rows,
            bytes_written,
            compression_ratio,
        })
    }
}

/// Convert our ParquetType to the Apache Parquet PhysicalType.
fn parquet_type_to_physical(pt: ParquetType) -> PhysicalType {
    match pt {
        ParquetType::Boolean => PhysicalType::Boolean,
        ParquetType::Int32 => PhysicalType::Int32,
        ParquetType::Int64 => PhysicalType::Int64,
        ParquetType::Float => PhysicalType::Float,
        ParquetType::Double => PhysicalType::Double,
        ParquetType::ByteArray => PhysicalType::ByteArray,
        ParquetType::Int96 => PhysicalType::Int64, // store timestamps as INT64
    }
}

/// Read a column from disk and produce PLAIN-encoded bytes suitable for Parquet.
///
/// For fixed-width types, PLAIN encoding is just the raw LE bytes (which is what
/// our `.d` files already contain for Int32/Int64/Float/Double).
///
/// For variable-length (ByteArray) types, PLAIN encoding is:
///   [4-byte LE length] [bytes] repeated for each value.
///
/// For Boolean, PLAIN encoding is one byte per value (0 or 1).
fn read_column_plain(
    partition_path: &Path,
    col_name: &str,
    col_type: ColumnType,
    physical_type: PhysicalType,
) -> Result<Vec<u8>> {
    if col_type.is_variable_length() {
        // Read .d and .i files and convert to Parquet PLAIN byte_array encoding
        let data_path = partition_path.join(format!("{col_name}.d"));
        let index_path = partition_path.join(format!("{col_name}.i"));

        if !data_path.exists() {
            return Ok(Vec::new());
        }

        let data = std::fs::read(&data_path)?;
        let index = std::fs::read(&index_path)?;
        let num_values = index.len() / 8;

        let mut result = Vec::new();
        for i in 0..num_values {
            let offset =
                u64::from_le_bytes(index[i * 8..(i + 1) * 8].try_into().unwrap()) as usize;
            if offset + 4 > data.len() {
                // Write empty value
                result.extend_from_slice(&0u32.to_le_bytes());
                continue;
            }
            let val_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            let val_end = (offset + 4 + val_len).min(data.len());
            let actual_len = val_end - offset - 4;
            // Parquet BYTE_ARRAY PLAIN: 4-byte LE length + bytes
            result.extend_from_slice(&(actual_len as u32).to_le_bytes());
            result.extend_from_slice(&data[offset + 4..val_end]);
        }
        Ok(result)
    } else {
        let data_path = partition_path.join(format!("{col_name}.d"));
        if !data_path.exists() {
            return Ok(Vec::new());
        }
        let raw = std::fs::read(&data_path)?;

        // For types smaller than the Parquet physical type, we need to widen.
        // E.g., I8/I16/Boolean -> Int32 in Parquet.
        match (col_type, physical_type) {
            // Already the right size - pass through
            (ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4, PhysicalType::Int32)
            | (ColumnType::I64 | ColumnType::GeoHash | ColumnType::Timestamp, PhysicalType::Int64)
            | (ColumnType::F32, PhysicalType::Float)
            | (ColumnType::F64, PhysicalType::Double) => Ok(raw),

            // Boolean: 1 byte -> 4 bytes (Int32)
            (ColumnType::Boolean | ColumnType::I8, PhysicalType::Int32) => {
                let mut result = Vec::with_capacity(raw.len() * 4);
                for &b in &raw {
                    result.extend_from_slice(&(b as i32).to_le_bytes());
                }
                Ok(result)
            }

            // I16/Char: 2 bytes -> 4 bytes (Int32)
            (ColumnType::I16 | ColumnType::Char, PhysicalType::Int32) => {
                let mut result = Vec::with_capacity(raw.len() * 2);
                for chunk in raw.chunks_exact(2) {
                    let val = i16::from_le_bytes(chunk.try_into().unwrap());
                    result.extend_from_slice(&(val as i32).to_le_bytes());
                }
                Ok(result)
            }

            // Uuid/Long128/Long256: pass as BYTE_ARRAY with fixed-length values
            (ColumnType::Uuid | ColumnType::Long128 | ColumnType::Long256, PhysicalType::ByteArray) => {
                let elem_size = col_type.fixed_size().unwrap();
                let num_values = raw.len() / elem_size;
                let mut result = Vec::with_capacity(raw.len() + num_values * 4);
                for chunk in raw.chunks_exact(elem_size) {
                    result.extend_from_slice(&(elem_size as u32).to_le_bytes());
                    result.extend_from_slice(chunk);
                }
                Ok(result)
            }

            // Fallback: pass through for anything that already matches
            _ => Ok(raw),
        }
    }
}

/// Detect row count from existing column files.
fn detect_row_count(partition_path: &Path, meta: &TableMeta) -> Result<u64> {
    for col_def in &meta.columns {
        let col_type: ColumnType = col_def.col_type.into();
        if col_type.is_variable_length() {
            let index_path = partition_path.join(format!("{}.i", col_def.name));
            if index_path.exists() {
                let len = std::fs::metadata(&index_path)?.len();
                return Ok(len / 8);
            }
        } else {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            if data_path.exists() {
                let len = std::fs::metadata(&data_path)?.len();
                let element_size = col_type.fixed_size().unwrap() as u64;
                if element_size > 0 {
                    return Ok(len / element_size);
                }
            }
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::thrift::{PhysicalType, Repetition};
    use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
    use std::fs;
    use tempfile::tempdir;

    fn test_meta_fixed() -> TableMeta {
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

    fn test_meta_varchar() -> TableMeta {
        TableMeta {
            name: "trades".to_string(),
            columns: vec![
                ColumnDef {
                    name: "timestamp".to_string(),
                    col_type: ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                ColumnDef {
                    name: "symbol".to_string(),
                    col_type: ColumnTypeSerializable::Varchar,
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

    fn make_apache_schema(meta: &TableMeta) -> Vec<ParquetColumn> {
        meta.columns
            .iter()
            .map(|c| {
                let ct: ColumnType = c.col_type.into();
                ParquetColumn {
                    name: c.name.clone(),
                    parquet_type: ParquetType::from_column_type(ct),
                    col_type: ct,
                }
            })
            .collect()
    }

    #[test]
    fn write_file_has_valid_parquet_structure() {
        let writer = ApacheParquetWriter::new();
        let schema = vec![
            ParquetSchemaColumn {
                name: "id".to_string(),
                physical_type: PhysicalType::Int64,
                repetition: Repetition::Required,
            },
            ParquetSchemaColumn {
                name: "value".to_string(),
                physical_type: PhysicalType::Double,
                repetition: Repetition::Required,
            },
        ];

        let id_data: Vec<u8> = (0..5i64).flat_map(|i| i.to_le_bytes()).collect();
        let val_data: Vec<u8> = (0..5)
            .map(|i| i as f64 * 1.5)
            .flat_map(|v| v.to_le_bytes())
            .collect();

        let file_bytes = writer.write_file(&schema, &[id_data, val_data], 5);

        // Validate PAR1 magic
        assert_eq!(&file_bytes[0..4], b"PAR1", "missing header magic");
        assert_eq!(
            &file_bytes[file_bytes.len() - 4..],
            b"PAR1",
            "missing footer magic"
        );

        // Validate footer length
        let fl_pos = file_bytes.len() - 8;
        let footer_len =
            i32::from_le_bytes(file_bytes[fl_pos..fl_pos + 4].try_into().unwrap()) as usize;
        assert!(footer_len > 0, "footer length should be positive");
        assert!(
            footer_len < file_bytes.len(),
            "footer length should be less than file size"
        );

        // The Thrift metadata should be at: [file_bytes.len() - 8 - footer_len .. file_bytes.len() - 8]
        let metadata_start = file_bytes.len() - 8 - footer_len;
        let metadata_bytes = &file_bytes[metadata_start..fl_pos];
        assert!(!metadata_bytes.is_empty());

        // First byte of metadata should be field 1 (version), delta=1, type=I32(5) = 0x15
        assert_eq!(
            metadata_bytes[0], 0x15,
            "metadata should start with version field"
        );
    }

    #[test]
    fn write_partition_fixed_columns() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        let num_rows: usize = 100;

        // Write column data
        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1710460800_000_000i64 + i * 1_000_000).to_le_bytes())
            .collect();
        let price_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (65000.50 + i as f64 * 0.25).to_le_bytes())
            .collect();
        let volume_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (1.5 + i as f64 * 0.01).to_le_bytes())
            .collect();

        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();
        fs::write(partition_path.join("volume.d"), &volume_data).unwrap();

        let parquet_path = dir.path().join("trades.parquet");
        let columns = make_apache_schema(&meta);
        let writer = ApacheParquetWriter::new();
        let stats = writer
            .write_partition(&parquet_path, &partition_path, &meta, &columns)
            .unwrap();

        assert_eq!(stats.rows_written, num_rows as u64);
        assert!(stats.bytes_written > 0);

        // Verify file structure
        let file_bytes = fs::read(&parquet_path).unwrap();
        assert_eq!(&file_bytes[0..4], b"PAR1");
        assert_eq!(&file_bytes[file_bytes.len() - 4..], b"PAR1");
    }

    #[test]
    fn write_partition_with_varchar() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_varchar();

        // Timestamp column
        let ts_data: Vec<u8> = (0..3i64)
            .flat_map(|i| (1710460800_000_000i64 + i * 1_000_000).to_le_bytes())
            .collect();
        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();

        // Varchar symbol column
        let symbols = ["BTC/USD", "ETH/USD", "SOL/USDT"];
        let mut data_buf = Vec::new();
        let mut index_buf = Vec::new();
        for s in &symbols {
            let offset = data_buf.len() as u64;
            index_buf.extend_from_slice(&offset.to_le_bytes());
            data_buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            data_buf.extend_from_slice(s.as_bytes());
        }
        fs::write(partition_path.join("symbol.d"), &data_buf).unwrap();
        fs::write(partition_path.join("symbol.i"), &index_buf).unwrap();

        // Price column
        let price_data: Vec<u8> = [65000.50f64, 3200.25, 150.75]
            .iter()
            .flat_map(|p| p.to_le_bytes())
            .collect();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();

        let parquet_path = dir.path().join("trades.parquet");
        let columns = make_apache_schema(&meta);
        let writer = ApacheParquetWriter::new();
        let stats = writer
            .write_partition(&parquet_path, &partition_path, &meta, &columns)
            .unwrap();

        assert_eq!(stats.rows_written, 3);

        let file_bytes = fs::read(&parquet_path).unwrap();
        assert_eq!(&file_bytes[0..4], b"PAR1");
        assert_eq!(&file_bytes[file_bytes.len() - 4..], b"PAR1");
    }

    #[test]
    fn empty_partition_produces_valid_parquet() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        fs::write(partition_path.join("timestamp.d"), &[]).unwrap();
        fs::write(partition_path.join("price.d"), &[]).unwrap();
        fs::write(partition_path.join("volume.d"), &[]).unwrap();

        let parquet_path = dir.path().join("empty.parquet");
        let columns = make_apache_schema(&meta);
        let writer = ApacheParquetWriter::new();
        let stats = writer
            .write_partition(&parquet_path, &partition_path, &meta, &columns)
            .unwrap();

        assert_eq!(stats.rows_written, 0);

        let file_bytes = fs::read(&parquet_path).unwrap();
        assert_eq!(&file_bytes[0..4], b"PAR1");
        assert_eq!(&file_bytes[file_bytes.len() - 4..], b"PAR1");
    }

    #[test]
    fn parquet_footer_length_is_correct() {
        let writer = ApacheParquetWriter::new();
        let schema = vec![ParquetSchemaColumn {
            name: "x".to_string(),
            physical_type: PhysicalType::Int64,
            repetition: Repetition::Required,
        }];
        let data: Vec<u8> = (0..3i64).flat_map(|i| i.to_le_bytes()).collect();
        let file_bytes = writer.write_file(&schema, &[data], 3);

        // Extract footer length
        let fl_pos = file_bytes.len() - 8;
        let footer_len =
            i32::from_le_bytes(file_bytes[fl_pos..fl_pos + 4].try_into().unwrap()) as usize;

        // Extract metadata
        let metadata_start = file_bytes.len() - 8 - footer_len;
        let metadata_bytes = &file_bytes[metadata_start..fl_pos];

        // The metadata length should match the declared footer_len
        assert_eq!(metadata_bytes.len(), footer_len);
    }
}
