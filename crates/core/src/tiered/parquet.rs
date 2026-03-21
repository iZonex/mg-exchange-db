//! Minimal Parquet-compatible columnar format ("XPQT") for cold storage.
//!
//! File layout:
//! ```text
//! [4 bytes]  magic "XPQT"
//! [4 bytes]  version (u32 LE) -- currently 1
//! [4 bytes]  num_columns (u32 LE)
//! [8 bytes]  num_rows (u64 LE)
//! -- per column metadata (repeated num_columns times) --
//!   [4 bytes]  column name length (u32 LE)
//!   [N bytes]  column name (UTF-8)
//!   [1 byte]   column type tag (ColumnType repr)
//!   [8 bytes]  data offset in file (u64 LE)
//!   [8 bytes]  compressed_size (u64 LE)
//!   [8 bytes]  uncompressed_size (u64 LE)
//! -- column data blocks (LZ4-compressed column bytes) --
//! -- footer --
//!   [8 bytes]  metadata_offset (u64 LE) -- offset to start of column metadata
//!   [8 bytes]  checksum (xxh3 of everything before footer)
//!   [4 bytes]  magic "XPQT"
//! ```

use crate::table::TableMeta;
use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::ColumnType;
use std::io::{Cursor, Read, Write};
use std::path::Path;

const XPQT_MAGIC: &[u8; 4] = b"XPQT";
const XPQT_VERSION: u32 = 1;

/// Statistics about a parquet write operation.
#[derive(Debug, Clone)]
pub struct ParquetStats {
    pub num_rows: u64,
    pub num_columns: u32,
    pub file_size: u64,
    pub compressed_data_size: u64,
    pub uncompressed_data_size: u64,
}

/// Column metadata within the XPQT file.
#[derive(Debug, Clone)]
struct ColumnMeta {
    name: String,
    col_type: u8,
    data_offset: u64,
    compressed_size: u64,
    uncompressed_size: u64,
}

/// Convert a partition's column files to a single XPQT file.
pub fn partition_to_parquet(
    partition_path: &Path,
    meta: &TableMeta,
    output_path: &Path,
) -> Result<ParquetStats> {
    let mut buf = Vec::new();

    // Determine row count from the first column
    let num_rows = detect_row_count(partition_path, meta)?;
    let num_columns = meta.columns.len() as u32;

    // Write file header
    buf.write_all(XPQT_MAGIC)?;
    buf.write_all(&XPQT_VERSION.to_le_bytes())?;
    buf.write_all(&num_columns.to_le_bytes())?;
    buf.write_all(&num_rows.to_le_bytes())?;

    // Record where metadata starts
    let metadata_offset = buf.len() as u64;

    // First pass: read and compress all column data, build metadata
    let mut column_metas: Vec<ColumnMeta> = Vec::new();
    let mut column_data_blocks: Vec<Vec<u8>> = Vec::new();

    for col_def in &meta.columns {
        let col_type: ColumnType = col_def.col_type.into();

        let raw_data = if col_type.is_variable_length() {
            // Read both .d and .i files, combine them with a length prefix
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            let index_path = partition_path.join(format!("{}.i", col_def.name));

            let mut combined = Vec::new();
            if data_path.exists() {
                let d = std::fs::read(&data_path)?;
                let i = std::fs::read(&index_path)?;
                // Store: [4 bytes data_len][data][4 bytes index_len][index]
                combined.extend_from_slice(&(d.len() as u32).to_le_bytes());
                combined.extend_from_slice(&d);
                combined.extend_from_slice(&(i.len() as u32).to_le_bytes());
                combined.extend_from_slice(&i);
            }
            combined
        } else {
            let data_path = partition_path.join(format!("{}.d", col_def.name));
            if data_path.exists() {
                std::fs::read(&data_path)?
            } else {
                Vec::new()
            }
        };

        let uncompressed_size = raw_data.len() as u64;
        let compressed = lz4_flex::compress_prepend_size(&raw_data);
        let compressed_size = compressed.len() as u64;

        column_metas.push(ColumnMeta {
            name: col_def.name.clone(),
            col_type: col_type as u8,
            data_offset: 0, // will be filled in
            compressed_size,
            uncompressed_size,
        });
        column_data_blocks.push(compressed);
    }

    // Write column metadata (placeholder offsets)
    let meta_start = buf.len();
    for cm in &column_metas {
        let name_bytes = cm.name.as_bytes();
        buf.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        buf.write_all(name_bytes)?;
        buf.write_all(&[cm.col_type])?;
        buf.write_all(&cm.data_offset.to_le_bytes())?; // placeholder
        buf.write_all(&cm.compressed_size.to_le_bytes())?;
        buf.write_all(&cm.uncompressed_size.to_le_bytes())?;
    }

    // Write column data blocks and record their offsets
    let mut data_offsets = Vec::new();
    for block in &column_data_blocks {
        data_offsets.push(buf.len() as u64);
        buf.write_all(block)?;
    }

    // Go back and fix up the data_offset fields in the metadata section
    let mut offset_in_meta = meta_start;
    for (i, cm) in column_metas.iter().enumerate() {
        let name_len = cm.name.len();
        // Skip: name_len(4) + name(N) + col_type(1) = 5 + name_len
        let offset_field_pos = offset_in_meta + 4 + name_len + 1;
        let offset_bytes = data_offsets[i].to_le_bytes();
        buf[offset_field_pos..offset_field_pos + 8].copy_from_slice(&offset_bytes);
        // Advance past this column's metadata:
        // name_len(4) + name(N) + col_type(1) + data_offset(8) + compressed_size(8) + uncompressed_size(8)
        offset_in_meta += 4 + name_len + 1 + 8 + 8 + 8;
    }

    // Write footer
    let checksum = xxhash_rust::xxh3::xxh3_64(&buf);
    buf.write_all(&metadata_offset.to_le_bytes())?;
    buf.write_all(&checksum.to_le_bytes())?;
    buf.write_all(XPQT_MAGIC)?;

    let file_size = buf.len() as u64;
    std::fs::write(output_path, &buf)?;

    let compressed_data_size: u64 = column_metas.iter().map(|c| c.compressed_size).sum();
    let uncompressed_data_size: u64 = column_metas.iter().map(|c| c.uncompressed_size).sum();

    Ok(ParquetStats {
        num_rows,
        num_columns,
        file_size,
        compressed_data_size,
        uncompressed_data_size,
    })
}

/// Read an XPQT file back into column files in the output directory.
/// Returns the number of rows restored.
pub fn parquet_to_partition(
    parquet_path: &Path,
    meta: &TableMeta,
    output_dir: &Path,
) -> Result<u64> {
    let data = std::fs::read(parquet_path)?;

    if data.len() < 20 {
        return Err(ExchangeDbError::Corruption(
            "XPQT file too short".to_string(),
        ));
    }

    // Validate footer magic
    let footer_magic = &data[data.len() - 4..];
    if footer_magic != XPQT_MAGIC {
        return Err(ExchangeDbError::Corruption(
            "invalid XPQT footer magic".to_string(),
        ));
    }

    // Read footer
    let footer_start = data.len() - 20; // metadata_offset(8) + checksum(8) + magic(4)
    let metadata_offset =
        u64::from_le_bytes(data[footer_start..footer_start + 8].try_into().unwrap());
    let stored_checksum = u64::from_le_bytes(
        data[footer_start + 8..footer_start + 16]
            .try_into()
            .unwrap(),
    );

    // Verify checksum (everything before footer)
    let computed_checksum = xxhash_rust::xxh3::xxh3_64(&data[..footer_start]);
    if computed_checksum != stored_checksum {
        return Err(ExchangeDbError::Corruption(format!(
            "XPQT checksum mismatch: expected {stored_checksum:#x}, got {computed_checksum:#x}"
        )));
    }

    // Read header
    let header_magic = &data[0..4];
    if header_magic != XPQT_MAGIC {
        return Err(ExchangeDbError::Corruption(
            "invalid XPQT header magic".to_string(),
        ));
    }

    let _version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let num_columns = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let num_rows = u64::from_le_bytes(data[12..20].try_into().unwrap());

    // Read column metadata
    let mut cursor = Cursor::new(&data[metadata_offset as usize..]);
    let mut column_metas = Vec::new();

    for _ in 0..num_columns {
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let name_len = u32::from_le_bytes(len_buf) as usize;

        let mut name_buf = vec![0u8; name_len];
        cursor.read_exact(&mut name_buf)?;
        let name = String::from_utf8(name_buf)
            .map_err(|e| ExchangeDbError::Corruption(format!("invalid column name: {e}")))?;

        let mut type_buf = [0u8; 1];
        cursor.read_exact(&mut type_buf)?;
        let col_type = type_buf[0];

        let mut offset_buf = [0u8; 8];
        cursor.read_exact(&mut offset_buf)?;
        let data_offset = u64::from_le_bytes(offset_buf);

        let mut comp_buf = [0u8; 8];
        cursor.read_exact(&mut comp_buf)?;
        let compressed_size = u64::from_le_bytes(comp_buf);

        let mut uncomp_buf = [0u8; 8];
        cursor.read_exact(&mut uncomp_buf)?;
        let uncompressed_size = u64::from_le_bytes(uncomp_buf);

        column_metas.push(ColumnMeta {
            name,
            col_type,
            data_offset,
            compressed_size,
            uncompressed_size,
        });
    }

    // Ensure output directory exists
    if !output_dir.exists() {
        std::fs::create_dir_all(output_dir)?;
    }

    // Decompress and write each column
    for cm in &column_metas {
        let compressed_data =
            &data[cm.data_offset as usize..(cm.data_offset + cm.compressed_size) as usize];

        let decompressed = lz4_flex::decompress_size_prepended(compressed_data).map_err(|e| {
            ExchangeDbError::Corruption(format!(
                "LZ4 decompression failed for column '{}': {e}",
                cm.name
            ))
        })?;

        if decompressed.len() as u64 != cm.uncompressed_size {
            return Err(ExchangeDbError::Corruption(format!(
                "size mismatch for column '{}': expected {}, got {}",
                cm.name,
                cm.uncompressed_size,
                decompressed.len()
            )));
        }

        // Look up the column definition to determine if it's variable-length
        let col_def = meta.columns.iter().find(|c| c.name == cm.name);
        let col_type: Option<ColumnType> = col_def.map(|c| c.col_type.into());

        if col_type.is_some_and(|ct| ct.is_variable_length()) {
            // Variable-length: data is stored as [4 bytes data_len][data][4 bytes index_len][index]
            let mut reader = Cursor::new(&decompressed);

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let data_len = u32::from_le_bytes(len_buf) as usize;

            let mut d_data = vec![0u8; data_len];
            reader.read_exact(&mut d_data)?;

            reader.read_exact(&mut len_buf)?;
            let index_len = u32::from_le_bytes(len_buf) as usize;

            let mut i_data = vec![0u8; index_len];
            reader.read_exact(&mut i_data)?;

            std::fs::write(output_dir.join(format!("{}.d", cm.name)), &d_data)?;
            std::fs::write(output_dir.join(format!("{}.i", cm.name)), &i_data)?;
        } else {
            // Fixed-width: write directly
            std::fs::write(output_dir.join(format!("{}.d", cm.name)), &decompressed)?;
        }
    }

    Ok(num_rows)
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
                return Ok(len / element_size);
            }
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
    use std::fs;
    use tempfile::tempdir;

    fn test_meta_fixed_only() -> TableMeta {
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

    fn test_meta_with_varchar() -> TableMeta {
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

    #[test]
    fn roundtrip_fixed_columns() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed_only();
        let num_rows: usize = 200;

        // Write test data
        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1_710_460_800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
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

        // Convert to parquet
        let parquet_path = dir.path().join("test.xpqt");
        let stats = partition_to_parquet(&partition_path, &meta, &parquet_path).unwrap();

        assert_eq!(stats.num_rows, num_rows as u64);
        assert_eq!(stats.num_columns, 3);
        assert!(stats.file_size > 0);
        assert!(stats.compressed_data_size < stats.uncompressed_data_size);

        // Convert back
        let restored_path = dir.path().join("restored");
        fs::create_dir_all(&restored_path).unwrap();
        let rows = parquet_to_partition(&parquet_path, &meta, &restored_path).unwrap();
        assert_eq!(rows, num_rows as u64);

        // Verify data matches
        let restored_ts = fs::read(restored_path.join("timestamp.d")).unwrap();
        let restored_price = fs::read(restored_path.join("price.d")).unwrap();
        let restored_volume = fs::read(restored_path.join("volume.d")).unwrap();

        assert_eq!(ts_data, restored_ts);
        assert_eq!(price_data, restored_price);
        assert_eq!(volume_data, restored_volume);
    }

    #[test]
    fn roundtrip_with_varchar() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_with_varchar();
        let num_rows: usize = 3;

        // Write timestamp column
        let ts_data: Vec<u8> = (0..num_rows as i64)
            .flat_map(|i| (1_710_460_800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        fs::write(partition_path.join("timestamp.d"), &ts_data).unwrap();

        // Write varchar symbol column (data + index files)
        // Data file format: [4 bytes len][data] repeated
        // Index file format: [8 bytes offset] repeated
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

        // Write price column
        let price_data: Vec<u8> = [65000.50f64, 3200.25, 150.75]
            .iter()
            .flat_map(|p| p.to_le_bytes())
            .collect();
        fs::write(partition_path.join("price.d"), &price_data).unwrap();

        // Convert to parquet
        let parquet_path = dir.path().join("test.xpqt");
        let stats = partition_to_parquet(&partition_path, &meta, &parquet_path).unwrap();
        assert_eq!(stats.num_rows, num_rows as u64);

        // Convert back
        let restored_path = dir.path().join("restored");
        fs::create_dir_all(&restored_path).unwrap();
        let rows = parquet_to_partition(&parquet_path, &meta, &restored_path).unwrap();
        assert_eq!(rows, num_rows as u64);

        // Verify all files match
        assert_eq!(
            ts_data,
            fs::read(restored_path.join("timestamp.d")).unwrap()
        );
        assert_eq!(data_buf, fs::read(restored_path.join("symbol.d")).unwrap());
        assert_eq!(index_buf, fs::read(restored_path.join("symbol.i")).unwrap());
        assert_eq!(price_data, fs::read(restored_path.join("price.d")).unwrap());
    }

    #[test]
    fn corrupt_magic_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.xpqt");
        fs::write(&path, b"BADDxxxxxxxxxxxxxxxxxxxx").unwrap();

        let meta = test_meta_fixed_only();
        let result = parquet_to_partition(&path, &meta, dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn file_too_short_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tiny.xpqt");
        fs::write(&path, b"XPQT").unwrap();

        let meta = test_meta_fixed_only();
        let result = parquet_to_partition(&path, &meta, dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn empty_partition_roundtrip() {
        let dir = tempdir().unwrap();
        let partition_path = dir.path().join("2024-01-15");
        fs::create_dir_all(&partition_path).unwrap();

        let meta = test_meta_fixed_only();

        // Write empty column files
        fs::write(partition_path.join("timestamp.d"), []).unwrap();
        fs::write(partition_path.join("price.d"), []).unwrap();
        fs::write(partition_path.join("volume.d"), []).unwrap();

        let parquet_path = dir.path().join("empty.xpqt");
        let stats = partition_to_parquet(&partition_path, &meta, &parquet_path).unwrap();
        assert_eq!(stats.num_rows, 0);

        let restored_path = dir.path().join("restored");
        let rows = parquet_to_partition(&parquet_path, &meta, &restored_path).unwrap();
        assert_eq!(rows, 0);
    }
}
