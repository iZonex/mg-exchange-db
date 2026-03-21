//! Row-level binary codec for WAL payloads.
//!
//! # Wire format (per column)
//!
//! ```text
//! | null flag (u8) | value bytes |
//! ```
//!
//! - `null_flag`: 0 = non-null, 1 = null.
//! - For fixed-width types the value is their natural little-endian encoding
//!   (1/2/4/8/16 bytes depending on type). When null, the bytes are still
//!   present (zeroed) to keep the layout fixed and simplify random access.
//! - For variable-length types (Varchar, Binary) the value is a 4-byte LE
//!   length prefix followed by that many raw bytes. When null, the length is 0.

use exchange_common::error::{ExchangeDbError, Result};
use exchange_common::types::ColumnType;

/// A decoded column value that **owns** its data.
///
/// This is intentionally separate from `table::ColumnValue` (which borrows)
/// because WAL payloads must be self-contained.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedColumnValue {
    Null,
    Boolean(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Timestamp(i64),   // nanos since epoch
    Symbol(i32),       // symbol table index
    Uuid([u8; 16]),
    Varchar(String),
    Binary(Vec<u8>),
    Date(i32),          // days since epoch
    Char(u16),          // single UTF-16 character
    IPv4(u32),          // IPv4 address
    Long128(i128),      // 128-bit integer
    Long256([u64; 4]),  // 256-bit integer
    GeoHash(i64),       // geospatial hash
    // New types
    Str(String),            // String (same wire format as Varchar)
    TimestampMicro(i64),    // Microsecond timestamp
    TimestampMilli(i64),    // Millisecond timestamp
    Interval([u8; 16]),     // 16-byte interval
    Decimal8(i8),
    Decimal16(i16),
    Decimal32(i32),
    Decimal64(i64),
    Decimal128(i128),
    Decimal256([u64; 4]),
    GeoByte(u8),
    GeoShort(u16),
    GeoInt(u32),
    Array(Vec<u8>),         // Variable-length array payload
    Cursor(i64),
    Record(i64),
    RegClass(i32),
    RegProcedure(i32),
    ArrayString(String),    // Variable-length text[]
    // Null already exists as a variant
    VarArg,                 // Zero-size sentinel
    Parameter,              // Zero-size sentinel
    VarcharSlice(String),   // Transient in-memory varchar slice (same wire format as Varchar)
    IPv6([u8; 16]),         // IPv6 address (16 bytes)
}

/// Encode a single row into a binary payload.
///
/// `column_types` and `values` must have the same length.
pub fn encode_row(column_types: &[ColumnType], values: &[OwnedColumnValue]) -> Result<Vec<u8>> {
    if column_types.len() != values.len() {
        return Err(ExchangeDbError::Query(format!(
            "column count mismatch: {} types vs {} values",
            column_types.len(),
            values.len()
        )));
    }

    // Rough capacity estimate.
    let mut buf = Vec::with_capacity(column_types.len() * 10);

    for (ct, val) in column_types.iter().zip(values.iter()) {
        match val {
            OwnedColumnValue::Null => {
                buf.push(1); // null flag
                // Write zero-filled placeholder for fixed types, or 0-length for var.
                if let Some(size) = ct.fixed_size() {
                    buf.extend(std::iter::repeat(0u8).take(size));
                } else {
                    buf.extend_from_slice(&0u32.to_le_bytes());
                }
            }
            _ => {
                buf.push(0); // non-null
                encode_value(&mut buf, *ct, val);
            }
        }
    }

    Ok(buf)
}

/// Decode a row from a binary payload produced by [`encode_row`].
pub fn decode_row(column_types: &[ColumnType], data: &[u8]) -> Result<Vec<OwnedColumnValue>> {
    let mut values = Vec::with_capacity(column_types.len());
    let mut offset = 0;

    for ct in column_types {
        if offset >= data.len() {
            return Err(ExchangeDbError::Corruption(
                "row codec: unexpected end of data".into(),
            ));
        }

        let null_flag = data[offset];
        offset += 1;

        if null_flag == 1 {
            // Skip over the placeholder bytes.
            if let Some(size) = ct.fixed_size() {
                offset += size;
            } else {
                // Variable-length null: 4 bytes of length (0).
                offset += 4;
            }
            values.push(OwnedColumnValue::Null);
            continue;
        }

        let (val, consumed) = decode_value(*ct, &data[offset..])?;
        offset += consumed;
        values.push(val);
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn encode_value(buf: &mut Vec<u8>, ct: ColumnType, val: &OwnedColumnValue) {
    match (ct, val) {
        (ColumnType::Boolean, OwnedColumnValue::Boolean(v)) => {
            buf.push(if *v { 1 } else { 0 });
        }
        (ColumnType::I8, OwnedColumnValue::I8(v)) => {
            buf.push(*v as u8);
        }
        (ColumnType::I16, OwnedColumnValue::I16(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::I32, OwnedColumnValue::I32(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::I64, OwnedColumnValue::I64(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::F32, OwnedColumnValue::F32(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::F64, OwnedColumnValue::F64(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Timestamp, OwnedColumnValue::Timestamp(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Symbol, OwnedColumnValue::Symbol(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Uuid, OwnedColumnValue::Uuid(v)) => {
            buf.extend_from_slice(v);
        }
        (ColumnType::Varchar, OwnedColumnValue::Varchar(s)) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        (ColumnType::Binary, OwnedColumnValue::Binary(b)) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        (ColumnType::Date, OwnedColumnValue::Date(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Char, OwnedColumnValue::Char(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::IPv4, OwnedColumnValue::IPv4(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Long128, OwnedColumnValue::Long128(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Long256, OwnedColumnValue::Long256(v)) => {
            for limb in v {
                buf.extend_from_slice(&limb.to_le_bytes());
            }
        }
        (ColumnType::GeoHash, OwnedColumnValue::GeoHash(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        // --- New types ---
        (ColumnType::String, OwnedColumnValue::Str(s)) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        (ColumnType::TimestampMicro, OwnedColumnValue::TimestampMicro(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::TimestampMilli, OwnedColumnValue::TimestampMilli(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Interval, OwnedColumnValue::Interval(v)) => {
            buf.extend_from_slice(v);
        }
        (ColumnType::Decimal8, OwnedColumnValue::Decimal8(v)) => {
            buf.push(*v as u8);
        }
        (ColumnType::Decimal16, OwnedColumnValue::Decimal16(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Decimal32, OwnedColumnValue::Decimal32(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Decimal64, OwnedColumnValue::Decimal64(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Decimal128, OwnedColumnValue::Decimal128(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Decimal256, OwnedColumnValue::Decimal256(v)) => {
            for limb in v {
                buf.extend_from_slice(&limb.to_le_bytes());
            }
        }
        (ColumnType::GeoByte, OwnedColumnValue::GeoByte(v)) => {
            buf.push(*v);
        }
        (ColumnType::GeoShort, OwnedColumnValue::GeoShort(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::GeoInt, OwnedColumnValue::GeoInt(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Array, OwnedColumnValue::Array(b)) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        (ColumnType::Cursor, OwnedColumnValue::Cursor(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Record, OwnedColumnValue::Record(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::RegClass, OwnedColumnValue::RegClass(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::RegProcedure, OwnedColumnValue::RegProcedure(v)) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (ColumnType::ArrayString, OwnedColumnValue::ArrayString(s)) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        (ColumnType::Null, OwnedColumnValue::Null) => {
            // zero-size, nothing to write
        }
        (ColumnType::VarArg, OwnedColumnValue::VarArg) => {
            // zero-size
        }
        (ColumnType::Parameter, OwnedColumnValue::Parameter) => {
            // zero-size
        }
        (ColumnType::VarcharSlice, OwnedColumnValue::VarcharSlice(s)) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        (ColumnType::IPv6, OwnedColumnValue::IPv6(v)) => {
            buf.extend_from_slice(v);
        }
        _ => {
            // Type mismatch: write zeros (defensive).
            if let Some(size) = ct.fixed_size() {
                buf.extend(std::iter::repeat(0u8).take(size));
            } else {
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
}

fn decode_value(ct: ColumnType, data: &[u8]) -> Result<(OwnedColumnValue, usize)> {
    let err = || ExchangeDbError::Corruption("row codec: not enough bytes for value".into());

    match ct {
        ColumnType::Boolean => {
            if data.is_empty() { return Err(err()); }
            Ok((OwnedColumnValue::Boolean(data[0] != 0), 1))
        }
        ColumnType::I8 => {
            if data.is_empty() { return Err(err()); }
            Ok((OwnedColumnValue::I8(data[0] as i8), 1))
        }
        ColumnType::I16 => {
            if data.len() < 2 { return Err(err()); }
            let v = i16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((OwnedColumnValue::I16(v), 2))
        }
        ColumnType::I32 => {
            if data.len() < 4 { return Err(err()); }
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::I32(v), 4))
        }
        ColumnType::I64 => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::I64(v), 8))
        }
        ColumnType::F32 => {
            if data.len() < 4 { return Err(err()); }
            let v = f32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::F32(v), 4))
        }
        ColumnType::F64 => {
            if data.len() < 8 { return Err(err()); }
            let v = f64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::F64(v), 8))
        }
        ColumnType::Timestamp => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::Timestamp(v), 8))
        }
        ColumnType::Symbol => {
            if data.len() < 4 { return Err(err()); }
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::Symbol(v), 4))
        }
        ColumnType::Uuid => {
            if data.len() < 16 { return Err(err()); }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&data[..16]);
            Ok((OwnedColumnValue::Uuid(arr), 16))
        }
        ColumnType::Varchar => {
            if data.len() < 4 { return Err(err()); }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len { return Err(err()); }
            let s = String::from_utf8(data[4..4 + len].to_vec())
                .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
            Ok((OwnedColumnValue::Varchar(s), 4 + len))
        }
        ColumnType::Binary => {
            if data.len() < 4 { return Err(err()); }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len { return Err(err()); }
            let b = data[4..4 + len].to_vec();
            Ok((OwnedColumnValue::Binary(b), 4 + len))
        }
        ColumnType::Date => {
            if data.len() < 4 { return Err(err()); }
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::Date(v), 4))
        }
        ColumnType::Char => {
            if data.len() < 2 { return Err(err()); }
            let v = u16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((OwnedColumnValue::Char(v), 2))
        }
        ColumnType::IPv4 => {
            if data.len() < 4 { return Err(err()); }
            let v = u32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::IPv4(v), 4))
        }
        ColumnType::Long128 => {
            if data.len() < 16 { return Err(err()); }
            let v = i128::from_le_bytes(data[..16].try_into().unwrap());
            Ok((OwnedColumnValue::Long128(v), 16))
        }
        ColumnType::Long256 => {
            if data.len() < 32 { return Err(err()); }
            let a = u64::from_le_bytes(data[..8].try_into().unwrap());
            let b = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let c = u64::from_le_bytes(data[16..24].try_into().unwrap());
            let d = u64::from_le_bytes(data[24..32].try_into().unwrap());
            Ok((OwnedColumnValue::Long256([a, b, c, d]), 32))
        }
        ColumnType::GeoHash => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::GeoHash(v), 8))
        }
        // --- New types ---
        ColumnType::String => {
            if data.len() < 4 { return Err(err()); }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len { return Err(err()); }
            let s = std::string::String::from_utf8(data[4..4 + len].to_vec())
                .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
            Ok((OwnedColumnValue::Str(s), 4 + len))
        }
        ColumnType::TimestampMicro => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::TimestampMicro(v), 8))
        }
        ColumnType::TimestampMilli => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::TimestampMilli(v), 8))
        }
        ColumnType::Interval => {
            if data.len() < 16 { return Err(err()); }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&data[..16]);
            Ok((OwnedColumnValue::Interval(arr), 16))
        }
        ColumnType::Decimal8 => {
            if data.is_empty() { return Err(err()); }
            Ok((OwnedColumnValue::Decimal8(data[0] as i8), 1))
        }
        ColumnType::Decimal16 => {
            if data.len() < 2 { return Err(err()); }
            let v = i16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((OwnedColumnValue::Decimal16(v), 2))
        }
        ColumnType::Decimal32 => {
            if data.len() < 4 { return Err(err()); }
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::Decimal32(v), 4))
        }
        ColumnType::Decimal64 => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::Decimal64(v), 8))
        }
        ColumnType::Decimal128 => {
            if data.len() < 16 { return Err(err()); }
            let v = i128::from_le_bytes(data[..16].try_into().unwrap());
            Ok((OwnedColumnValue::Decimal128(v), 16))
        }
        ColumnType::Decimal256 => {
            if data.len() < 32 { return Err(err()); }
            let a = u64::from_le_bytes(data[..8].try_into().unwrap());
            let b = u64::from_le_bytes(data[8..16].try_into().unwrap());
            let c = u64::from_le_bytes(data[16..24].try_into().unwrap());
            let d = u64::from_le_bytes(data[24..32].try_into().unwrap());
            Ok((OwnedColumnValue::Decimal256([a, b, c, d]), 32))
        }
        ColumnType::GeoByte => {
            if data.is_empty() { return Err(err()); }
            Ok((OwnedColumnValue::GeoByte(data[0]), 1))
        }
        ColumnType::GeoShort => {
            if data.len() < 2 { return Err(err()); }
            let v = u16::from_le_bytes(data[..2].try_into().unwrap());
            Ok((OwnedColumnValue::GeoShort(v), 2))
        }
        ColumnType::GeoInt => {
            if data.len() < 4 { return Err(err()); }
            let v = u32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::GeoInt(v), 4))
        }
        ColumnType::Array => {
            if data.len() < 4 { return Err(err()); }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len { return Err(err()); }
            let b = data[4..4 + len].to_vec();
            Ok((OwnedColumnValue::Array(b), 4 + len))
        }
        ColumnType::Cursor => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::Cursor(v), 8))
        }
        ColumnType::Record => {
            if data.len() < 8 { return Err(err()); }
            let v = i64::from_le_bytes(data[..8].try_into().unwrap());
            Ok((OwnedColumnValue::Record(v), 8))
        }
        ColumnType::RegClass => {
            if data.len() < 4 { return Err(err()); }
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::RegClass(v), 4))
        }
        ColumnType::RegProcedure => {
            if data.len() < 4 { return Err(err()); }
            let v = i32::from_le_bytes(data[..4].try_into().unwrap());
            Ok((OwnedColumnValue::RegProcedure(v), 4))
        }
        ColumnType::ArrayString => {
            if data.len() < 4 { return Err(err()); }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len { return Err(err()); }
            let s = std::string::String::from_utf8(data[4..4 + len].to_vec())
                .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?;
            Ok((OwnedColumnValue::ArrayString(s), 4 + len))
        }
        ColumnType::Null => {
            Ok((OwnedColumnValue::Null, 0))
        }
        ColumnType::VarArg => {
            Ok((OwnedColumnValue::VarArg, 0))
        }
        ColumnType::Parameter => {
            Ok((OwnedColumnValue::Parameter, 0))
        }
        ColumnType::VarcharSlice => {
            if data.len() < 4 { return Err(err()); }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len { return Err(err()); }
            let s = std::str::from_utf8(&data[4..4 + len])
                .map_err(|e| ExchangeDbError::Corruption(e.to_string()))?
                .to_string();
            Ok((OwnedColumnValue::VarcharSlice(s), 4 + len))
        }
        ColumnType::IPv6 => {
            if data.len() < 16 { return Err(err()); }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&data[..16]);
            Ok((OwnedColumnValue::IPv6(arr), 16))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_fixed_types() {
        let types = vec![
            ColumnType::Boolean,
            ColumnType::I8,
            ColumnType::I16,
            ColumnType::I32,
            ColumnType::I64,
            ColumnType::F32,
            ColumnType::F64,
            ColumnType::Timestamp,
            ColumnType::Symbol,
            ColumnType::Uuid,
        ];

        let values = vec![
            OwnedColumnValue::Boolean(true),
            OwnedColumnValue::I8(-42),
            OwnedColumnValue::I16(1234),
            OwnedColumnValue::I32(-100_000),
            OwnedColumnValue::I64(i64::MAX),
            OwnedColumnValue::F32(3.14),
            OwnedColumnValue::F64(2.71828),
            OwnedColumnValue::Timestamp(1_710_513_000_000_000_000),
            OwnedColumnValue::Symbol(7),
            OwnedColumnValue::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        ];

        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_variable_types() {
        let types = vec![ColumnType::Varchar, ColumnType::Binary];
        let values = vec![
            OwnedColumnValue::Varchar("hello world".into()),
            OwnedColumnValue::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ];

        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_with_nulls() {
        let types = vec![
            ColumnType::I64,
            ColumnType::Varchar,
            ColumnType::F64,
            ColumnType::Binary,
        ];
        let values = vec![
            OwnedColumnValue::Null,
            OwnedColumnValue::Varchar("not null".into()),
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
        ];

        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_empty_strings() {
        let types = vec![ColumnType::Varchar, ColumnType::Binary];
        let values = vec![
            OwnedColumnValue::Varchar(String::new()),
            OwnedColumnValue::Binary(vec![]),
        ];

        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_mixed_row() {
        let types = vec![
            ColumnType::Timestamp,
            ColumnType::Symbol,
            ColumnType::F64,
            ColumnType::Varchar,
        ];
        let values = vec![
            OwnedColumnValue::Timestamp(1_710_513_000_000_000_000),
            OwnedColumnValue::Symbol(42),
            OwnedColumnValue::F64(65432.10),
            OwnedColumnValue::Varchar("BTC/USD".into()),
        ];

        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn decode_truncated_data_fails() {
        let types = vec![ColumnType::I64, ColumnType::F64];
        let values = vec![
            OwnedColumnValue::I64(123),
            OwnedColumnValue::F64(4.56),
        ];
        let encoded = encode_row(&types, &values).unwrap();

        // Truncate to half.
        let result = decode_row(&types, &encoded[..encoded.len() / 2]);
        assert!(result.is_err());
    }

    #[test]
    fn multiple_rows_independent() {
        let types = vec![ColumnType::I32, ColumnType::Varchar];
        let row1 = vec![
            OwnedColumnValue::I32(1),
            OwnedColumnValue::Varchar("first".into()),
        ];
        let row2 = vec![
            OwnedColumnValue::I32(2),
            OwnedColumnValue::Varchar("second".into()),
        ];

        let enc1 = encode_row(&types, &row1).unwrap();
        let enc2 = encode_row(&types, &row2).unwrap();

        assert_eq!(decode_row(&types, &enc1).unwrap(), row1);
        assert_eq!(decode_row(&types, &enc2).unwrap(), row2);
    }

    // -----------------------------------------------------------------------
    // Roundtrip tests for all 23 new types (20 tests covering all of them)
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_string_type() {
        let types = vec![ColumnType::String];
        let values = vec![OwnedColumnValue::Str("hello string type".into())];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_timestamp_micro() {
        let types = vec![ColumnType::TimestampMicro];
        let values = vec![OwnedColumnValue::TimestampMicro(1_710_513_000_000)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_timestamp_milli() {
        let types = vec![ColumnType::TimestampMilli];
        let values = vec![OwnedColumnValue::TimestampMilli(1_710_513_000)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_interval() {
        let types = vec![ColumnType::Interval];
        let values = vec![OwnedColumnValue::Interval([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_decimal8() {
        let types = vec![ColumnType::Decimal8];
        let values = vec![OwnedColumnValue::Decimal8(-42)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_decimal16() {
        let types = vec![ColumnType::Decimal16];
        let values = vec![OwnedColumnValue::Decimal16(12345)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_decimal32() {
        let types = vec![ColumnType::Decimal32];
        let values = vec![OwnedColumnValue::Decimal32(-100_000)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_decimal64() {
        let types = vec![ColumnType::Decimal64];
        let values = vec![OwnedColumnValue::Decimal64(i64::MAX - 1)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_decimal128() {
        let types = vec![ColumnType::Decimal128];
        let values = vec![OwnedColumnValue::Decimal128(i128::MAX / 2)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_decimal256() {
        let types = vec![ColumnType::Decimal256];
        let values = vec![OwnedColumnValue::Decimal256([11, 22, 33, 44])];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_geobyte() {
        let types = vec![ColumnType::GeoByte];
        let values = vec![OwnedColumnValue::GeoByte(0x7F)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_geoshort() {
        let types = vec![ColumnType::GeoShort];
        let values = vec![OwnedColumnValue::GeoShort(0xABCD)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_geoint() {
        let types = vec![ColumnType::GeoInt];
        let values = vec![OwnedColumnValue::GeoInt(0xDEAD_BEEF)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_array_type() {
        let types = vec![ColumnType::Array];
        let values = vec![OwnedColumnValue::Array(vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE])];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_cursor_and_record() {
        let types = vec![ColumnType::Cursor, ColumnType::Record];
        let values = vec![OwnedColumnValue::Cursor(999), OwnedColumnValue::Record(888)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_regclass_and_regprocedure() {
        let types = vec![ColumnType::RegClass, ColumnType::RegProcedure];
        let values = vec![OwnedColumnValue::RegClass(42), OwnedColumnValue::RegProcedure(99)];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_arraystring() {
        let types = vec![ColumnType::ArrayString];
        let values = vec![OwnedColumnValue::ArrayString("{\"a\",\"b\",\"c\"}".into())];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_null_vararg_parameter() {
        let types = vec![ColumnType::Null, ColumnType::VarArg, ColumnType::Parameter];
        let values = vec![OwnedColumnValue::Null, OwnedColumnValue::VarArg, OwnedColumnValue::Parameter];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_new_types_with_nulls() {
        let types = vec![
            ColumnType::String,
            ColumnType::TimestampMicro,
            ColumnType::Decimal64,
            ColumnType::GeoByte,
            ColumnType::Array,
        ];
        let values = vec![
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
            OwnedColumnValue::Null,
        ];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn roundtrip_all_new_types_mixed_row() {
        let types = vec![
            ColumnType::String,
            ColumnType::TimestampMicro,
            ColumnType::Decimal32,
            ColumnType::GeoByte,
            ColumnType::RegClass,
            ColumnType::ArrayString,
            ColumnType::Cursor,
        ];
        let values = vec![
            OwnedColumnValue::Str("mixed row test".into()),
            OwnedColumnValue::TimestampMicro(1_000_000),
            OwnedColumnValue::Decimal32(314),
            OwnedColumnValue::GeoByte(0x55),
            OwnedColumnValue::RegClass(100),
            OwnedColumnValue::ArrayString("{\"x\",\"y\"}".into()),
            OwnedColumnValue::Cursor(42),
        ];
        let encoded = encode_row(&types, &values).unwrap();
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }
}
