//! Writer for the PAR1XCHG columnar format.

use crate::table::TableMeta;
use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Magic bytes that identify a PAR1XCHG file.
pub(crate) const MAGIC: &[u8; 8] = b"PAR1XCHG";
/// Current format version.
pub(crate) const VERSION: u16 = 1;

/// Parquet-style column type tag (conceptual, maps to ColumnType).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetType {
    Int32,
    Int64,
    Float,
    Double,
    ByteArray, // strings / binary
    Boolean,
    Int96, // legacy timestamps
}

impl ParquetType {
    /// Map from our ColumnType to a ParquetType.
    pub fn from_column_type(ct: ColumnType) -> Self {
        match ct {
            ColumnType::Boolean => Self::Boolean,
            ColumnType::I8
            | ColumnType::I16
            | ColumnType::I32
            | ColumnType::Symbol
            | ColumnType::Date
            | ColumnType::IPv4
            | ColumnType::Char => Self::Int32,
            ColumnType::I64 | ColumnType::GeoHash => Self::Int64,
            ColumnType::Timestamp => Self::Int96,
            ColumnType::F32 => Self::Float,
            ColumnType::F64 => Self::Double,
            ColumnType::Varchar | ColumnType::Binary => Self::ByteArray,
            ColumnType::Uuid | ColumnType::Long128 | ColumnType::Long256 => Self::ByteArray,
            // New types
            ColumnType::TimestampMicro
            | ColumnType::TimestampMilli
            | ColumnType::Decimal64
            | ColumnType::Cursor
            | ColumnType::Record => Self::Int64,
            ColumnType::Decimal8 | ColumnType::GeoByte => Self::Int32, // promoted to 4-byte
            ColumnType::Decimal16 | ColumnType::GeoShort => Self::Int32,
            ColumnType::Decimal32
            | ColumnType::GeoInt
            | ColumnType::RegClass
            | ColumnType::RegProcedure => Self::Int32,
            ColumnType::Interval | ColumnType::Decimal128 | ColumnType::Decimal256 => {
                Self::ByteArray
            }
            ColumnType::String | ColumnType::Array | ColumnType::ArrayString => Self::ByteArray,
            ColumnType::VarcharSlice => Self::ByteArray, // transient varchar slice
            ColumnType::IPv6 => Self::ByteArray,         // 16-byte IPv6 address
            ColumnType::Null | ColumnType::VarArg | ColumnType::Parameter => Self::Int32, // zero-size sentinel
        }
    }
}

/// Description of a column within a Parquet file.
#[derive(Debug, Clone)]
pub struct ParquetColumn {
    pub name: String,
    pub parquet_type: ParquetType,
    pub col_type: ColumnType,
}

/// Statistics produced by a write operation.
#[derive(Debug, Clone)]
pub struct ParquetWriteStats {
    pub rows_written: u64,
    pub bytes_written: u64,
    pub compression_ratio: f64,
}

/// Encoding tag stored in the file.
#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum Encoding {
    Plain = 0,
    _Dictionary = 1,
    _Rle = 2,
}

/// Writer that produces PAR1XCHG files from column data.
pub struct ParquetWriter {
    path: PathBuf,
    schema: Vec<ParquetColumn>,
}

impl ParquetWriter {
    /// Create a new writer targeting the given output path.
    pub fn new(path: &Path, schema: Vec<ParquetColumn>) -> Self {
        Self {
            path: path.to_path_buf(),
            schema,
        }
    }

    /// Write a partition's column files to a PAR1XCHG file.
    ///
    /// Reads the raw `.d` (and `.i` for variable-length) column files from
    /// `partition_path`, compresses each column with LZ4, and writes a single
    /// self-describing binary file.
    pub fn write_partition(
        &self,
        partition_path: &Path,
        meta: &TableMeta,
    ) -> Result<ParquetWriteStats> {
        let num_rows = detect_row_count(partition_path, meta)?;
        let num_columns = self.schema.len() as u16;

        let mut buf = Vec::new();

        // -- File header --
        buf.write_all(MAGIC)?;
        buf.write_all(&VERSION.to_le_bytes())?;
        buf.write_all(&num_columns.to_le_bytes())?;
        buf.write_all(&num_rows.to_le_bytes())?;

        // -- Read and compress column data --
        struct ColumnBlock {
            raw_len: u64,
            compressed: Vec<u8>,
        }
        let mut blocks: Vec<ColumnBlock> = Vec::with_capacity(self.schema.len());

        for col in &self.schema {
            let raw_data = read_column_raw(partition_path, &col.name, col.col_type)?;
            let raw_len = raw_data.len() as u64;
            let compressed = lz4_flex::compress_prepend_size(&raw_data);
            blocks.push(ColumnBlock {
                raw_len,
                compressed,
            });
        }

        // -- Write column metadata (with placeholder offsets) --
        // Record positions of offset fields so we can fix them up later.
        let mut offset_fixup_positions: Vec<usize> = Vec::with_capacity(self.schema.len());

        for (i, col) in self.schema.iter().enumerate() {
            let name_bytes = col.name.as_bytes();
            buf.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            buf.write_all(name_bytes)?;
            buf.write_all(&[col.col_type as u8])?;
            offset_fixup_positions.push(buf.len());
            buf.write_all(&0u64.to_le_bytes())?; // data_offset placeholder
            buf.write_all(&blocks[i].raw_len.to_le_bytes())?; // data_length (uncompressed)
            buf.write_all(&(blocks[i].compressed.len() as u64).to_le_bytes())?; // compressed_length
            buf.write_all(&[Encoding::Plain as u8])?;
        }

        // -- Write column data blocks and record offsets --
        let mut total_compressed: u64 = 0;
        let mut total_uncompressed: u64 = 0;
        for (i, block) in blocks.iter().enumerate() {
            let data_offset = buf.len() as u64;
            // Fix up the offset field in the metadata.
            let fixup_pos = offset_fixup_positions[i];
            buf[fixup_pos..fixup_pos + 8].copy_from_slice(&data_offset.to_le_bytes());

            buf.write_all(&block.compressed)?;
            total_compressed += block.compressed.len() as u64;
            total_uncompressed += block.raw_len;
        }

        // -- Footer --
        let checksum = xxhash_rust::xxh3::xxh3_64(&buf);
        buf.write_all(&checksum.to_le_bytes())?;
        buf.write_all(MAGIC)?;

        let bytes_written = buf.len() as u64;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, &buf)?;

        let compression_ratio = if total_uncompressed > 0 {
            total_compressed as f64 / total_uncompressed as f64
        } else {
            1.0
        };

        Ok(ParquetWriteStats {
            rows_written: num_rows,
            bytes_written,
            compression_ratio,
        })
    }

    /// Write rows directly (for SQL COPY TO export).
    ///
    /// Each inner `Vec<Value>` must have values in the same order as
    /// `self.schema`.
    pub fn write_rows(
        &self,
        columns: &[String],
        rows: &[Vec<crate::parquet::reader::RowValue>],
    ) -> Result<ParquetWriteStats> {
        let num_rows = rows.len() as u64;
        let num_columns = columns.len() as u16;

        // Build per-column raw bytes.
        let mut col_buffers: Vec<Vec<u8>> = vec![Vec::new(); columns.len()];

        for row in rows {
            for (col_idx, val) in row.iter().enumerate() {
                if col_idx >= self.schema.len() {
                    break;
                }
                let ct = self.schema[col_idx].col_type;
                encode_value_to_buffer(&mut col_buffers[col_idx], val, ct);
            }
        }

        // Now write using the standard file layout.
        let mut buf = Vec::new();
        buf.write_all(MAGIC)?;
        buf.write_all(&VERSION.to_le_bytes())?;
        buf.write_all(&num_columns.to_le_bytes())?;
        buf.write_all(&num_rows.to_le_bytes())?;

        // Compress columns.
        let compressed_bufs: Vec<Vec<u8>> = col_buffers
            .iter()
            .map(|raw| lz4_flex::compress_prepend_size(raw))
            .collect();

        // Write column metadata.
        let mut offset_fixup_positions: Vec<usize> = Vec::with_capacity(columns.len());
        for (i, col) in self.schema.iter().enumerate() {
            let name_bytes = col.name.as_bytes();
            buf.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            buf.write_all(name_bytes)?;
            buf.write_all(&[col.col_type as u8])?;
            offset_fixup_positions.push(buf.len());
            buf.write_all(&0u64.to_le_bytes())?; // data_offset placeholder
            buf.write_all(&(col_buffers[i].len() as u64).to_le_bytes())?; // uncompressed
            buf.write_all(&(compressed_bufs[i].len() as u64).to_le_bytes())?; // compressed
            buf.write_all(&[Encoding::Plain as u8])?;
        }

        // Write data blocks.
        let mut total_compressed: u64 = 0;
        let mut total_uncompressed: u64 = 0;
        for (i, compressed) in compressed_bufs.iter().enumerate() {
            let data_offset = buf.len() as u64;
            let fixup_pos = offset_fixup_positions[i];
            buf[fixup_pos..fixup_pos + 8].copy_from_slice(&data_offset.to_le_bytes());
            buf.write_all(compressed)?;
            total_compressed += compressed.len() as u64;
            total_uncompressed += col_buffers[i].len() as u64;
        }

        // Footer.
        let checksum = xxhash_rust::xxh3::xxh3_64(&buf);
        buf.write_all(&checksum.to_le_bytes())?;
        buf.write_all(MAGIC)?;

        let bytes_written = buf.len() as u64;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, &buf)?;

        let compression_ratio = if total_uncompressed > 0 {
            total_compressed as f64 / total_uncompressed as f64
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read raw column bytes from a partition directory, combining .d and .i files
/// for variable-length columns.
fn read_column_raw(partition_path: &Path, col_name: &str, col_type: ColumnType) -> Result<Vec<u8>> {
    if col_type.is_variable_length() {
        let data_path = partition_path.join(format!("{col_name}.d"));
        let index_path = partition_path.join(format!("{col_name}.i"));
        let mut combined = Vec::new();
        if data_path.exists() {
            let d = std::fs::read(&data_path)?;
            let i = std::fs::read(&index_path)?;
            combined.extend_from_slice(&(d.len() as u32).to_le_bytes());
            combined.extend_from_slice(&d);
            combined.extend_from_slice(&(i.len() as u32).to_le_bytes());
            combined.extend_from_slice(&i);
        }
        Ok(combined)
    } else {
        let data_path = partition_path.join(format!("{col_name}.d"));
        if data_path.exists() {
            Ok(std::fs::read(&data_path)?)
        } else {
            Ok(Vec::new())
        }
    }
}

/// Detect row count from existing column files in a partition.
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

use crate::parquet::reader::RowValue;

/// Encode a single value into a column buffer using the PLAIN encoding.
fn encode_value_to_buffer(buf: &mut Vec<u8>, val: &RowValue, col_type: ColumnType) {
    if col_type.is_variable_length() {
        // Variable-length: write as length-prefixed bytes.
        let bytes = match val {
            RowValue::Str(s) => s.as_bytes(),
            RowValue::Bytes(b) => b.as_slice(),
            _ => b"",
        };
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    } else {
        match col_type {
            ColumnType::Boolean | ColumnType::I8 => {
                let v = match val {
                    RowValue::I64(n) => *n as u8,
                    _ => 0,
                };
                buf.push(v);
            }
            ColumnType::I16 | ColumnType::Char => {
                let v = match val {
                    RowValue::I64(n) => *n as i16,
                    _ => 0,
                };
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ColumnType::I32 | ColumnType::Symbol | ColumnType::Date | ColumnType::IPv4 => {
                let v = match val {
                    RowValue::I64(n) => *n as i32,
                    RowValue::F64(n) => *n as i32,
                    _ => 0,
                };
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ColumnType::F32 => {
                let v = match val {
                    RowValue::F64(n) => *n as f32,
                    RowValue::I64(n) => *n as f32,
                    _ => 0.0,
                };
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ColumnType::F64 => {
                let v = match val {
                    RowValue::F64(n) => *n,
                    RowValue::I64(n) => *n as f64,
                    _ => 0.0,
                };
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ColumnType::I64 | ColumnType::Timestamp | ColumnType::GeoHash => {
                let v = match val {
                    RowValue::I64(n) => *n,
                    RowValue::Timestamp(n) => *n,
                    RowValue::F64(n) => *n as i64,
                    _ => 0,
                };
                buf.extend_from_slice(&v.to_le_bytes());
            }
            ColumnType::Uuid | ColumnType::Long128 => {
                // 16 bytes
                let bytes = match val {
                    RowValue::Bytes(b) => b.as_slice(),
                    _ => &[0u8; 16],
                };
                let padded: [u8; 16] = {
                    let mut arr = [0u8; 16];
                    let n = bytes.len().min(16);
                    arr[..n].copy_from_slice(&bytes[..n]);
                    arr
                };
                buf.extend_from_slice(&padded);
            }
            ColumnType::Long256 => {
                let bytes = match val {
                    RowValue::Bytes(b) => b.as_slice(),
                    _ => &[0u8; 32],
                };
                let padded: [u8; 32] = {
                    let mut arr = [0u8; 32];
                    let n = bytes.len().min(32);
                    arr[..n].copy_from_slice(&bytes[..n]);
                    arr
                };
                buf.extend_from_slice(&padded);
            }
            _ => {
                // Fallback: 8 bytes.
                let v: i64 = match val {
                    RowValue::I64(n) => *n,
                    RowValue::F64(n) => *n as i64,
                    RowValue::Timestamp(n) => *n,
                    _ => 0,
                };
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::reader::ParquetReader;
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

    fn make_schema(meta: &TableMeta) -> Vec<ParquetColumn> {
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
    fn roundtrip_fixed_columns() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        let num_rows: usize = 200;

        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1710460800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
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

        let parquet_path = dir.path().join("test.parquet");
        let schema = make_schema(&meta);
        let writer = ParquetWriter::new(&parquet_path, schema);
        let stats = writer.write_partition(&partition_path, &meta).unwrap();

        assert_eq!(stats.rows_written, num_rows as u64);
        assert!(stats.bytes_written > 0);

        // Read back.
        let reader = ParquetReader::open(&parquet_path).unwrap();
        assert_eq!(reader.metadata().num_rows, num_rows as u64);
        assert_eq!(reader.metadata().columns.len(), 3);

        let rows = reader.read_all().unwrap();
        assert_eq!(rows.len(), num_rows);

        // Verify first row.
        match &rows[0][0] {
            RowValue::Timestamp(ts) => assert_eq!(*ts, 1710460800_000_000_000i64),
            other => panic!("expected Timestamp, got {:?}", other),
        }
        match &rows[0][1] {
            RowValue::F64(p) => assert_eq!(*p, 65000.50),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn roundtrip_partition_to_partition() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        let num_rows: usize = 50;

        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1710460800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        let price_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (100.0 + i as f64 * 0.5).to_le_bytes())
            .collect();
        let volume_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (1.0 + i as f64 * 0.1).to_le_bytes())
            .collect();

        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();
        fs::write(partition_path.join("volume.d"), &volume_data).unwrap();

        let parquet_path = dir.path().join("test.parquet");
        let schema = make_schema(&meta);
        let writer = ParquetWriter::new(&parquet_path, schema);
        writer.write_partition(&partition_path, &meta).unwrap();

        // Convert back to partition.
        let restored_path = dir.path().join("restored");
        let reader = ParquetReader::open(&parquet_path).unwrap();
        let rows_restored = reader.to_partition(&restored_path, &meta).unwrap();
        assert_eq!(rows_restored, num_rows as u64);

        // Verify data matches byte-for-byte.
        assert_eq!(
            ts_data,
            fs::read(restored_path.join("timestamp.d")).unwrap()
        );
        assert_eq!(price_data, fs::read(restored_path.join("price.d")).unwrap());
        assert_eq!(
            volume_data,
            fs::read(restored_path.join("volume.d")).unwrap()
        );
    }

    #[test]
    fn roundtrip_with_varchar() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_varchar();
        let num_rows: usize = 3;

        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1710460800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();

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

        let price_data: Vec<u8> = [65000.50f64, 3200.25, 150.75]
            .iter()
            .flat_map(|p| p.to_le_bytes())
            .collect();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();

        let parquet_path = dir.path().join("test.parquet");
        let schema = make_schema(&meta);
        let writer = ParquetWriter::new(&parquet_path, schema);
        let stats = writer.write_partition(&partition_path, &meta).unwrap();
        assert_eq!(stats.rows_written, num_rows as u64);

        // Round-trip back to partition.
        let restored_path = dir.path().join("restored");
        let reader = ParquetReader::open(&parquet_path).unwrap();
        reader.to_partition(&restored_path, &meta).unwrap();

        assert_eq!(
            ts_data,
            fs::read(restored_path.join("timestamp.d")).unwrap()
        );
        assert_eq!(data_buf, fs::read(restored_path.join("symbol.d")).unwrap());
        assert_eq!(index_buf, fs::read(restored_path.join("symbol.i")).unwrap());
        assert_eq!(price_data, fs::read(restored_path.join("price.d")).unwrap());
    }

    #[test]
    fn empty_partition_roundtrip() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        fs::write(partition_path.join("timestamp.d"), &[]).unwrap();
        fs::write(partition_path.join("price.d"), &[]).unwrap();
        fs::write(partition_path.join("volume.d"), &[]).unwrap();

        let parquet_path = dir.path().join("empty.parquet");
        let schema = make_schema(&meta);
        let writer = ParquetWriter::new(&parquet_path, schema);
        let stats = writer.write_partition(&partition_path, &meta).unwrap();
        assert_eq!(stats.rows_written, 0);

        let reader = ParquetReader::open(&parquet_path).unwrap();
        assert_eq!(reader.metadata().num_rows, 0);
        let rows = reader.read_all().unwrap();
        assert!(rows.is_empty());

        let restored_path = dir.path().join("restored");
        let rows_restored = reader.to_partition(&restored_path, &meta).unwrap();
        assert_eq!(rows_restored, 0);
    }

    #[test]
    fn projection_pushdown() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        let num_rows: usize = 10;

        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1710460800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        let price_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (100.0 + i as f64).to_le_bytes())
            .collect();
        let volume_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (1.0 + i as f64 * 0.1).to_le_bytes())
            .collect();

        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();
        fs::write(partition_path.join("volume.d"), &volume_data).unwrap();

        let parquet_path = dir.path().join("test.parquet");
        let schema = make_schema(&meta);
        let writer = ParquetWriter::new(&parquet_path, schema);
        writer.write_partition(&partition_path, &meta).unwrap();

        // Read only the "price" column.
        let reader = ParquetReader::open(&parquet_path).unwrap();
        let rows = reader.read_columns(&["price".to_string()]).unwrap();
        assert_eq!(rows.len(), num_rows);
        // Each row should have exactly 1 column.
        assert_eq!(rows[0].len(), 1);
        match &rows[0][0] {
            RowValue::F64(p) => assert_eq!(*p, 100.0),
            other => panic!("expected F64, got {:?}", other),
        }
    }

    #[test]
    fn metadata_correct() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed();
        let num_rows: usize = 42;

        let ts_data: Vec<u8> = (0..num_rows as i64).flat_map(|i| i.to_le_bytes()).collect();
        let price_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (i as f64).to_le_bytes())
            .collect();
        let volume_data: Vec<u8> = (0..num_rows)
            .flat_map(|i| (i as f64).to_le_bytes())
            .collect();

        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();
        fs::write(partition_path.join("volume.d"), &volume_data).unwrap();

        let parquet_path = dir.path().join("test.parquet");
        let schema = make_schema(&meta);
        let writer = ParquetWriter::new(&parquet_path, schema);
        writer.write_partition(&partition_path, &meta).unwrap();

        let reader = ParquetReader::open(&parquet_path).unwrap();
        let pq_meta = reader.metadata();
        assert_eq!(pq_meta.num_rows, 42);
        assert_eq!(pq_meta.columns.len(), 3);
        assert_eq!(pq_meta.columns[0].name, "timestamp");
        assert_eq!(pq_meta.columns[1].name, "price");
        assert_eq!(pq_meta.columns[2].name, "volume");
        assert_eq!(pq_meta.columns[0].col_type, ColumnType::Timestamp);
        assert_eq!(pq_meta.columns[1].col_type, ColumnType::F64);
    }

    #[test]
    fn write_rows_direct() {
        let dir = tempdir().unwrap();
        let parquet_path = dir.path().join("direct.parquet");

        let schema = vec![
            ParquetColumn {
                name: "ts".to_string(),
                parquet_type: ParquetType::Int96,
                col_type: ColumnType::Timestamp,
            },
            ParquetColumn {
                name: "price".to_string(),
                parquet_type: ParquetType::Double,
                col_type: ColumnType::F64,
            },
        ];

        let rows = vec![
            vec![RowValue::Timestamp(1000), RowValue::F64(99.5)],
            vec![RowValue::Timestamp(2000), RowValue::F64(100.0)],
        ];

        let writer = ParquetWriter::new(&parquet_path, schema);
        let stats = writer
            .write_rows(&["ts".to_string(), "price".to_string()], &rows)
            .unwrap();
        assert_eq!(stats.rows_written, 2);

        let reader = ParquetReader::open(&parquet_path).unwrap();
        let read_rows = reader.read_all().unwrap();
        assert_eq!(read_rows.len(), 2);
        match &read_rows[0][0] {
            RowValue::Timestamp(ts) => assert_eq!(*ts, 1000),
            other => panic!("expected Timestamp, got {:?}", other),
        }
        match &read_rows[1][1] {
            RowValue::F64(p) => assert_eq!(*p, 100.0),
            other => panic!("expected F64, got {:?}", other),
        }
    }
}
