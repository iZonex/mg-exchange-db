//! Binary metadata format for the `_meta` file.
//!
//! Layout:
//!   Magic: "XMTA" (4 bytes)
//!   Version: u16
//!   Table name length: u16, then UTF-8 bytes
//!   Partition by: u8
//!   Timestamp column index: u16
//!   Meta version: u64
//!   Column count: u16
//!   For each column:
//!     Name length: u16, then UTF-8 bytes
//!     Type: u8
//!     Flags: u8 (bit 0 = indexed)

use crate::table::{ColumnDef, ColumnTypeSerializable, PartitionBySerializable, TableMeta};
use exchange_common::error::{ExchangeDbError, Result};
use std::io::{Cursor, Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"XMTA";
const FORMAT_VERSION: u16 = 1;

/// Write table metadata in compact binary format.
pub fn write_binary_meta(path: &Path, meta: &TableMeta) -> Result<()> {
    let mut buf = Vec::new();

    // Magic
    buf.write_all(MAGIC)?;
    // Format version
    buf.write_all(&FORMAT_VERSION.to_le_bytes())?;
    // Table name
    let name_bytes = meta.name.as_bytes();
    buf.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
    buf.write_all(name_bytes)?;
    // Partition by
    let pb_byte = partition_by_to_u8(meta.partition_by);
    buf.write_all(&[pb_byte])?;
    // Timestamp column index
    buf.write_all(&(meta.timestamp_column as u16).to_le_bytes())?;
    // Meta version
    buf.write_all(&meta.version.to_le_bytes())?;
    // Column count
    buf.write_all(&(meta.columns.len() as u16).to_le_bytes())?;
    // Columns
    for col in &meta.columns {
        let col_name = col.name.as_bytes();
        buf.write_all(&(col_name.len() as u16).to_le_bytes())?;
        buf.write_all(col_name)?;
        buf.write_all(&[col_type_to_u8(col.col_type)])?;
        let flags: u8 = if col.indexed { 1 } else { 0 };
        buf.write_all(&[flags])?;
    }

    std::fs::write(path, &buf)?;
    Ok(())
}

/// Read table metadata from binary format.
pub fn read_binary_meta(path: &Path) -> Result<TableMeta> {
    let data = std::fs::read(path)?;
    let mut cur = Cursor::new(&data);

    // Magic
    let mut magic = [0u8; 4];
    cur.read_exact(&mut magic)
        .map_err(|_| ExchangeDbError::Corruption("binary meta too short for magic".into()))?;
    if &magic != MAGIC {
        return Err(ExchangeDbError::Corruption(format!(
            "invalid binary meta magic: {:?}",
            magic
        )));
    }

    // Format version
    let version = read_u16(&mut cur)?;
    if version != FORMAT_VERSION {
        return Err(ExchangeDbError::Corruption(format!(
            "unsupported binary meta version: {}",
            version
        )));
    }

    // Table name
    let name_len = read_u16(&mut cur)? as usize;
    let name = read_string(&mut cur, name_len)?;

    // Partition by
    let mut pb_byte = [0u8; 1];
    cur.read_exact(&mut pb_byte)
        .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    let partition_by = u8_to_partition_by(pb_byte[0])?;

    // Timestamp column index
    let timestamp_column = read_u16(&mut cur)? as usize;

    // Meta version
    let meta_version = read_u64(&mut cur)?;

    // Column count
    let col_count = read_u16(&mut cur)? as usize;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let col_name_len = read_u16(&mut cur)? as usize;
        let col_name = read_string(&mut cur, col_name_len)?;
        let mut type_byte = [0u8; 1];
        cur.read_exact(&mut type_byte)
            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        let col_type = u8_to_col_type(type_byte[0])?;
        let mut flags_byte = [0u8; 1];
        cur.read_exact(&mut flags_byte)
            .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        let indexed = flags_byte[0] & 1 != 0;

        columns.push(ColumnDef {
            name: col_name,
            col_type,
            indexed,
        });
    }

    Ok(TableMeta {
        name,
        columns,
        partition_by,
        timestamp_column,
        version: meta_version,
    })
}

/// Auto-detect format (JSON or binary) and read.
pub fn read_meta_auto(path: &Path) -> Result<TableMeta> {
    let data = std::fs::read(path)?;
    if data.len() >= 4 && &data[0..4] == MAGIC {
        read_binary_meta(path)
    } else {
        // Try JSON.
        let json =
            std::str::from_utf8(&data).map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
        serde_json::from_str(json).map_err(|e| ExchangeDbError::Corruption(e.to_string()))
    }
}

// ---- helper functions ----

fn read_u16(cur: &mut Cursor<&Vec<u8>>) -> Result<u16> {
    let mut buf = [0u8; 2];
    cur.read_exact(&mut buf)
        .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u64(cur: &mut Cursor<&Vec<u8>>) -> Result<u64> {
    let mut buf = [0u8; 8];
    cur.read_exact(&mut buf)
        .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    Ok(u64::from_le_bytes(buf))
}

fn read_string(cur: &mut Cursor<&Vec<u8>>, len: usize) -> Result<String> {
    let mut buf = vec![0u8; len];
    cur.read_exact(&mut buf)
        .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
    String::from_utf8(buf).map_err(|e| ExchangeDbError::Corruption(e.to_string()))
}

fn partition_by_to_u8(pb: PartitionBySerializable) -> u8 {
    match pb {
        PartitionBySerializable::None => 0,
        PartitionBySerializable::Hour => 1,
        PartitionBySerializable::Day => 2,
        PartitionBySerializable::Week => 3,
        PartitionBySerializable::Month => 4,
        PartitionBySerializable::Year => 5,
    }
}

fn u8_to_partition_by(v: u8) -> Result<PartitionBySerializable> {
    match v {
        0 => Ok(PartitionBySerializable::None),
        1 => Ok(PartitionBySerializable::Hour),
        2 => Ok(PartitionBySerializable::Day),
        3 => Ok(PartitionBySerializable::Week),
        4 => Ok(PartitionBySerializable::Month),
        5 => Ok(PartitionBySerializable::Year),
        _ => Err(ExchangeDbError::Corruption(format!(
            "unknown partition_by value: {}",
            v
        ))),
    }
}

fn col_type_to_u8(ct: ColumnTypeSerializable) -> u8 {
    match ct {
        ColumnTypeSerializable::Boolean => 0,
        ColumnTypeSerializable::I8 => 1,
        ColumnTypeSerializable::I16 => 2,
        ColumnTypeSerializable::I32 => 3,
        ColumnTypeSerializable::I64 => 4,
        ColumnTypeSerializable::F32 => 5,
        ColumnTypeSerializable::F64 => 6,
        ColumnTypeSerializable::Timestamp => 7,
        ColumnTypeSerializable::Symbol => 8,
        ColumnTypeSerializable::Varchar => 9,
        ColumnTypeSerializable::Binary => 10,
        ColumnTypeSerializable::Uuid => 11,
        ColumnTypeSerializable::Date => 12,
        ColumnTypeSerializable::Char => 13,
        ColumnTypeSerializable::IPv4 => 14,
        ColumnTypeSerializable::Long128 => 15,
        ColumnTypeSerializable::Long256 => 16,
        ColumnTypeSerializable::GeoHash => 17,
        ColumnTypeSerializable::String => 18,
        ColumnTypeSerializable::TimestampMicro => 19,
        ColumnTypeSerializable::TimestampMilli => 20,
        ColumnTypeSerializable::Interval => 21,
        ColumnTypeSerializable::Decimal8 => 22,
        ColumnTypeSerializable::Decimal16 => 23,
        ColumnTypeSerializable::Decimal32 => 24,
        ColumnTypeSerializable::Decimal64 => 25,
        ColumnTypeSerializable::Decimal128 => 26,
        ColumnTypeSerializable::Decimal256 => 27,
        ColumnTypeSerializable::GeoByte => 28,
        ColumnTypeSerializable::GeoShort => 29,
        ColumnTypeSerializable::GeoInt => 30,
        ColumnTypeSerializable::Array => 31,
        ColumnTypeSerializable::Cursor => 32,
        ColumnTypeSerializable::Record => 33,
        ColumnTypeSerializable::RegClass => 34,
        ColumnTypeSerializable::RegProcedure => 35,
        ColumnTypeSerializable::ArrayString => 36,
        ColumnTypeSerializable::Null => 37,
        ColumnTypeSerializable::VarArg => 38,
        ColumnTypeSerializable::Parameter => 39,
        ColumnTypeSerializable::VarcharSlice => 40,
        ColumnTypeSerializable::IPv6 => 41,
    }
}

fn u8_to_col_type(v: u8) -> Result<ColumnTypeSerializable> {
    match v {
        0 => Ok(ColumnTypeSerializable::Boolean),
        1 => Ok(ColumnTypeSerializable::I8),
        2 => Ok(ColumnTypeSerializable::I16),
        3 => Ok(ColumnTypeSerializable::I32),
        4 => Ok(ColumnTypeSerializable::I64),
        5 => Ok(ColumnTypeSerializable::F32),
        6 => Ok(ColumnTypeSerializable::F64),
        7 => Ok(ColumnTypeSerializable::Timestamp),
        8 => Ok(ColumnTypeSerializable::Symbol),
        9 => Ok(ColumnTypeSerializable::Varchar),
        10 => Ok(ColumnTypeSerializable::Binary),
        11 => Ok(ColumnTypeSerializable::Uuid),
        12 => Ok(ColumnTypeSerializable::Date),
        13 => Ok(ColumnTypeSerializable::Char),
        14 => Ok(ColumnTypeSerializable::IPv4),
        15 => Ok(ColumnTypeSerializable::Long128),
        16 => Ok(ColumnTypeSerializable::Long256),
        17 => Ok(ColumnTypeSerializable::GeoHash),
        18 => Ok(ColumnTypeSerializable::String),
        19 => Ok(ColumnTypeSerializable::TimestampMicro),
        20 => Ok(ColumnTypeSerializable::TimestampMilli),
        21 => Ok(ColumnTypeSerializable::Interval),
        22 => Ok(ColumnTypeSerializable::Decimal8),
        23 => Ok(ColumnTypeSerializable::Decimal16),
        24 => Ok(ColumnTypeSerializable::Decimal32),
        25 => Ok(ColumnTypeSerializable::Decimal64),
        26 => Ok(ColumnTypeSerializable::Decimal128),
        27 => Ok(ColumnTypeSerializable::Decimal256),
        28 => Ok(ColumnTypeSerializable::GeoByte),
        29 => Ok(ColumnTypeSerializable::GeoShort),
        30 => Ok(ColumnTypeSerializable::GeoInt),
        31 => Ok(ColumnTypeSerializable::Array),
        32 => Ok(ColumnTypeSerializable::Cursor),
        33 => Ok(ColumnTypeSerializable::Record),
        34 => Ok(ColumnTypeSerializable::RegClass),
        35 => Ok(ColumnTypeSerializable::RegProcedure),
        36 => Ok(ColumnTypeSerializable::ArrayString),
        37 => Ok(ColumnTypeSerializable::Null),
        38 => Ok(ColumnTypeSerializable::VarArg),
        39 => Ok(ColumnTypeSerializable::Parameter),
        40 => Ok(ColumnTypeSerializable::VarcharSlice),
        41 => Ok(ColumnTypeSerializable::IPv6),
        _ => Err(ExchangeDbError::Corruption(format!(
            "unknown column type value: {}",
            v
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_common::types::{ColumnType, PartitionBy};
    use tempfile::tempdir;

    fn sample_meta() -> TableMeta {
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
                    col_type: ColumnTypeSerializable::Symbol,
                    indexed: true,
                },
                ColumnDef {
                    name: "price".to_string(),
                    col_type: ColumnTypeSerializable::F64,
                    indexed: false,
                },
                ColumnDef {
                    name: "exchange".to_string(),
                    col_type: ColumnTypeSerializable::Varchar,
                    indexed: false,
                },
            ],
            partition_by: PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 42,
        }
    }

    #[test]
    fn binary_meta_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("_meta");
        let meta = sample_meta();

        write_binary_meta(&path, &meta).unwrap();
        let loaded = read_binary_meta(&path).unwrap();

        assert_eq!(loaded.name, meta.name);
        assert_eq!(loaded.columns.len(), meta.columns.len());
        assert_eq!(loaded.timestamp_column, meta.timestamp_column);
        assert_eq!(loaded.version, meta.version);

        for (a, b) in loaded.columns.iter().zip(meta.columns.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(col_type_to_u8(a.col_type), col_type_to_u8(b.col_type));
            assert_eq!(a.indexed, b.indexed);
        }
    }

    #[test]
    fn binary_meta_matches_json_meta() {
        let dir = tempdir().unwrap();
        let meta = sample_meta();

        // Write as JSON.
        let json_path = dir.path().join("_meta_json");
        meta.save(&json_path).unwrap();
        let json_loaded = TableMeta::load(&json_path).unwrap();

        // Write as binary.
        let bin_path = dir.path().join("_meta_bin");
        write_binary_meta(&bin_path, &meta).unwrap();
        let bin_loaded = read_binary_meta(&bin_path).unwrap();

        // Compare.
        assert_eq!(json_loaded.name, bin_loaded.name);
        assert_eq!(json_loaded.columns.len(), bin_loaded.columns.len());
        assert_eq!(json_loaded.timestamp_column, bin_loaded.timestamp_column);
        assert_eq!(json_loaded.version, bin_loaded.version);

        for (a, b) in json_loaded.columns.iter().zip(bin_loaded.columns.iter()) {
            assert_eq!(a.name, b.name);
            assert_eq!(a.indexed, b.indexed);
        }
    }

    #[test]
    fn auto_detect_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("_meta");
        let meta = sample_meta();
        meta.save(&path).unwrap();

        let loaded = read_meta_auto(&path).unwrap();
        assert_eq!(loaded.name, "trades");
    }

    #[test]
    fn auto_detect_binary() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("_meta");
        let meta = sample_meta();
        write_binary_meta(&path, &meta).unwrap();

        let loaded = read_meta_auto(&path).unwrap();
        assert_eq!(loaded.name, "trades");
        assert_eq!(loaded.version, 42);
    }

    #[test]
    fn corrupt_magic_fails() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("_meta");
        std::fs::write(&path, b"BAAD_DATA").unwrap();

        let result = read_binary_meta(&path);
        assert!(result.is_err());
    }
}
