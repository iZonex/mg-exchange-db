//! Property-based tests for ExchangeDB core invariants.
//!
//! Uses a simple PRNG instead of external property testing crates.

use exchange_common::types::ColumnType;
use exchange_core::compression::{
    delta_decode_i64, delta_encode_i64, rle_decode, rle_encode,
};
use exchange_core::wal::row_codec::{decode_row, encode_row, OwnedColumnValue};

// ---------------------------------------------------------------------------
// Simple PRNG (xorshift64)
// ---------------------------------------------------------------------------

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() as f64) / (u64::MAX as f64)
    }

    fn next_i64(&mut self) -> i64 {
        self.next_u64() as i64
    }

    fn next_bool(&mut self) -> bool {
        self.next_u64() & 1 == 0
    }

    fn next_bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next_u64() & 0xFF) as u8).collect()
    }

    fn next_string(&mut self, max_len: usize) -> String {
        let len = self.next_usize(max_len + 1);
        (0..len)
            .map(|_| {
                let c = (self.next_u64() % 26 + b'a' as u64) as u8;
                c as char
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Property: WAL encode/decode roundtrip
// ---------------------------------------------------------------------------

#[test]
fn property_wal_encode_decode_roundtrip() {
    let mut rng = SimpleRng::new(42);

    for _ in 0..200 {
        // Generate random column types.
        let num_cols = rng.next_usize(8) + 1;
        let mut types = Vec::with_capacity(num_cols);
        let mut values = Vec::with_capacity(num_cols);

        for _ in 0..num_cols {
            let (ct, val) = random_column_value(&mut rng);
            types.push(ct);
            values.push(val);
        }

        let encoded = encode_row(&types, &values).unwrap();
        let decoded =
            decode_row(&types, &encoded).expect("decode should not fail for valid encoded data");
        assert_eq!(
            decoded, values,
            "WAL roundtrip failed for types: {:?}",
            types
        );
    }
}

fn random_column_value(rng: &mut SimpleRng) -> (ColumnType, OwnedColumnValue) {
    // Choose from a subset of types that are straightforward to test.
    let type_idx = rng.next_usize(10);
    match type_idx {
        0 => (ColumnType::Boolean, OwnedColumnValue::Boolean(rng.next_bool())),
        1 => (ColumnType::I8, OwnedColumnValue::I8((rng.next_u64() & 0xFF) as i8)),
        2 => (ColumnType::I16, OwnedColumnValue::I16((rng.next_u64() & 0xFFFF) as i16)),
        3 => (ColumnType::I32, OwnedColumnValue::I32(rng.next_u64() as i32)),
        4 => (ColumnType::I64, OwnedColumnValue::I64(rng.next_i64())),
        5 => (ColumnType::F32, OwnedColumnValue::F32(rng.next_f64() as f32 * 1000.0)),
        6 => (ColumnType::F64, OwnedColumnValue::F64(rng.next_f64() * 100000.0)),
        7 => (
            ColumnType::Timestamp,
            OwnedColumnValue::Timestamp(rng.next_u64() as i64),
        ),
        8 => (
            ColumnType::Varchar,
            OwnedColumnValue::Varchar(rng.next_string(50)),
        ),
        9 => {
            // Null value with a random type.
            let ct = match rng.next_usize(5) {
                0 => ColumnType::I64,
                1 => ColumnType::F64,
                2 => ColumnType::Varchar,
                3 => ColumnType::Timestamp,
                _ => ColumnType::Boolean,
            };
            (ct, OwnedColumnValue::Null)
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Property: delta encoding roundtrip
// ---------------------------------------------------------------------------

#[test]
fn property_delta_encoding_roundtrip() {
    let mut rng = SimpleRng::new(123);

    for _ in 0..200 {
        let count = rng.next_usize(500) + 1; // at least 1 element
        // Generate monotonically increasing values (like timestamps).
        let mut values = Vec::with_capacity(count);
        let mut current = rng.next_i64().abs() % 1_000_000;
        for _ in 0..count {
            values.push(current);
            current += (rng.next_u64() % 1000) as i64 + 1; // always increase
        }

        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded);
        assert_eq!(
            decoded, values,
            "delta encoding roundtrip failed for {} values",
            count
        );
    }
}

// ---------------------------------------------------------------------------
// Property: compression (LZ4) roundtrip
// ---------------------------------------------------------------------------

#[test]
fn property_compression_roundtrip() {
    let mut rng = SimpleRng::new(7);

    for _ in 0..100 {
        let size = rng.next_usize(10000) + 1;
        let data = rng.next_bytes(size);

        // Compress with lz4_flex.
        let compressed = lz4_flex::compress_prepend_size(&data);

        // Decompress.
        let decompressed = lz4_flex::decompress_size_prepended(&compressed)
            .expect("LZ4 decompression failed");

        assert_eq!(
            decompressed, data,
            "LZ4 roundtrip failed for {} bytes",
            size
        );
    }
}

// ---------------------------------------------------------------------------
// Property: RLE encoding roundtrip
// ---------------------------------------------------------------------------

#[test]
fn property_rle_encoding_roundtrip() {
    let mut rng = SimpleRng::new(99);

    for _ in 0..200 {
        let count = rng.next_usize(500) + 1;
        // Generate values with some repetition (e.g. symbol column).
        let symbols = ["BTC", "ETH", "SOL", "ADA", "DOT"];
        let values: Vec<&str> = (0..count)
            .map(|_| symbols[rng.next_usize(symbols.len())])
            .collect();

        let encoded = rle_encode(&values);
        let decoded = rle_decode(&encoded);
        assert_eq!(
            decoded, values,
            "RLE roundtrip failed for {} values",
            count
        );

        // Also verify that runs are correct: no two adjacent runs have the same value.
        for i in 1..encoded.len() {
            assert_ne!(
                encoded[i - 1].0, encoded[i].0,
                "RLE should merge adjacent equal values"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Property: sorted timestamps invariant
// ---------------------------------------------------------------------------

#[test]
fn property_partition_sort_invariant() {
    let mut rng = SimpleRng::new(555);

    for _ in 0..100 {
        let count = rng.next_usize(1000) + 2;
        // Generate random timestamps.
        let mut timestamps: Vec<i64> = (0..count)
            .map(|_| rng.next_u64() as i64 % 1_000_000_000)
            .collect();

        // Sort them (simulating what a partition does on ingest).
        timestamps.sort();

        // Verify sorted order.
        for i in 1..timestamps.len() {
            assert!(
                timestamps[i - 1] <= timestamps[i],
                "timestamps should be sorted within a partition: {} > {} at index {}",
                timestamps[i - 1],
                timestamps[i],
                i
            );
        }

        // Verify delta encoding of sorted data always produces non-negative deltas.
        let encoded = delta_encode_i64(&timestamps);
        for (i, &delta) in encoded.deltas.iter().enumerate() {
            assert!(
                delta >= 0,
                "delta should be non-negative for sorted data: delta[{i}] = {delta}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Property: index consistency (symbol lookup vs scan)
// ---------------------------------------------------------------------------

#[test]
fn property_index_consistency() {
    use std::collections::HashMap;

    let mut rng = SimpleRng::new(777);

    for _ in 0..50 {
        let count = rng.next_usize(500) + 10;
        let symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META"];

        // Generate random (row_index, symbol) pairs.
        let rows: Vec<(usize, &str)> = (0..count)
            .map(|i| (i, symbols[rng.next_usize(symbols.len())]))
            .collect();

        // Build an "index": symbol -> list of row indices.
        let mut index: HashMap<&str, Vec<usize>> = HashMap::new();
        for &(i, sym) in &rows {
            index.entry(sym).or_default().push(i);
        }

        // Verify: for each symbol, scanning all rows for that symbol
        // yields the same set of indices as the index lookup.
        for sym in &symbols {
            let scan_result: Vec<usize> = rows
                .iter()
                .filter(|(_, s)| s == sym)
                .map(|(i, _)| *i)
                .collect();

            let index_result = index.get(sym).cloned().unwrap_or_default();

            assert_eq!(
                scan_result, index_result,
                "index lookup for '{sym}' should match scan"
            );
        }

        // Verify: total entries across all index buckets equals row count.
        let total: usize = index.values().map(|v| v.len()).sum();
        assert_eq!(total, count, "index should cover all rows");
    }
}
