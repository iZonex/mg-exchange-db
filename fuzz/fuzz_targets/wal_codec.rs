#![no_main]

use libfuzzer_sys::fuzz_target;

use exchange_common::types::ColumnType;
use exchange_core::wal::row_codec::{decode_row, encode_row, OwnedColumnValue};

/// All known column types, used to pick a schema from fuzzer bytes.
const ALL_TYPES: &[ColumnType] = &[
    ColumnType::Boolean,
    ColumnType::I8,
    ColumnType::I16,
    ColumnType::I32,
    ColumnType::I64,
    ColumnType::F32,
    ColumnType::F64,
    ColumnType::Timestamp,
    ColumnType::Symbol,
    ColumnType::Varchar,
    ColumnType::Binary,
    ColumnType::Uuid,
    ColumnType::Date,
    ColumnType::Char,
    ColumnType::IPv4,
    ColumnType::Long128,
    ColumnType::Long256,
    ColumnType::GeoHash,
];

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // --- Test 1: feed raw bytes to decode_row with various schemas ---
    // Use the first byte to pick a small schema of 1–4 column types.
    let schema_seed = data[0] as usize;
    let num_cols = (schema_seed % 4) + 1;
    let schema: Vec<ColumnType> = (0..num_cols)
        .map(|i| {
            let idx = if i + 1 < data.len() {
                data[i + 1] as usize % ALL_TYPES.len()
            } else {
                0
            };
            ALL_TYPES[idx]
        })
        .collect();

    // decode_row must not panic — errors are fine.
    let payload = if data.len() > num_cols + 1 {
        &data[num_cols + 1..]
    } else {
        &[]
    };
    let _ = decode_row(&schema, payload);

    // --- Test 2: encode/decode round-trip with a single fixed-width column ---
    // Use a Boolean column for simplicity: encode a known value, then decode it,
    // and verify the round-trip is lossless.
    let types = &[ColumnType::Boolean];
    let values = &[OwnedColumnValue::Boolean(true)];
    if let Ok(encoded) = encode_row(types, values) {
        if let Ok(decoded) = decode_row(types, &encoded) {
            assert_eq!(decoded.len(), 1);
            assert_eq!(decoded[0], OwnedColumnValue::Boolean(true));
        }
    }
});
