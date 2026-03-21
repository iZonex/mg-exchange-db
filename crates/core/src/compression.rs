use exchange_common::error::{ExchangeDbError, Result};
use std::fs;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Delta encoding for monotonically increasing i64 values (e.g. timestamps)
// ---------------------------------------------------------------------------

/// Delta-encoded representation of a sequence of i64 values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaEncoded {
    /// The first value in the original sequence.
    pub base: i64,
    /// Differences between consecutive values. Length is `original_len - 1`
    /// for non-empty input, or 0 for empty input.
    pub deltas: Vec<i64>,
}

/// Delta-encode a slice of i64 values.
///
/// For an input of N values the result stores the first value as `base` and
/// N-1 deltas.  An empty input produces `base = 0` and an empty `deltas` vec.
pub fn delta_encode_i64(values: &[i64]) -> DeltaEncoded {
    if values.is_empty() {
        return DeltaEncoded {
            base: 0,
            deltas: Vec::new(),
        };
    }

    let base = values[0];
    let deltas = values.windows(2).map(|w| w[1] - w[0]).collect();

    DeltaEncoded { base, deltas }
}

/// Reconstruct the original i64 slice from its delta-encoded form.
pub fn delta_decode_i64(encoded: &DeltaEncoded) -> Vec<i64> {
    if encoded.deltas.is_empty() && encoded.base == 0 {
        // Could be truly empty, or a single element with value 0.
        // Convention: if deltas is empty and base is 0, that was an empty input.
        // For a single element, deltas would also be empty but base != 0 in
        // the general case.  We handle the single-element case below.
        return Vec::new();
    }

    let mut result = Vec::with_capacity(encoded.deltas.len() + 1);
    result.push(encoded.base);
    let mut current = encoded.base;
    for &d in &encoded.deltas {
        current += d;
        result.push(current);
    }
    result
}

/// Variant of `delta_decode_i64` that always returns at least one element when
/// the encoding represents a single value (base with empty deltas).  Use this
/// when you *know* the original sequence was non-empty.
pub fn delta_decode_i64_nonempty(encoded: &DeltaEncoded) -> Vec<i64> {
    let mut result = Vec::with_capacity(encoded.deltas.len() + 1);
    result.push(encoded.base);
    let mut current = encoded.base;
    for &d in &encoded.deltas {
        current += d;
        result.push(current);
    }
    result
}

// ---------------------------------------------------------------------------
// Run-Length Encoding for repetitive values (e.g. symbol columns)
// ---------------------------------------------------------------------------

/// Run-length-encode a slice of values.
///
/// Consecutive equal elements are collapsed into `(value, count)` pairs.
pub fn rle_encode<T: Eq + Clone>(values: &[T]) -> Vec<(T, u32)> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut result: Vec<(T, u32)> = Vec::new();
    let mut current = values[0].clone();
    let mut count: u32 = 1;

    for v in &values[1..] {
        if *v == current {
            count += 1;
        } else {
            result.push((current, count));
            current = v.clone();
            count = 1;
        }
    }
    result.push((current, count));
    result
}

/// Decode a run-length-encoded sequence back to the original values.
pub fn rle_decode<T: Clone>(encoded: &[(T, u32)]) -> Vec<T> {
    let total: u32 = encoded.iter().map(|(_, c)| c).sum();
    let mut result = Vec::with_capacity(total as usize);
    for (value, count) in encoded {
        for _ in 0..*count {
            result.push(value.clone());
        }
    }
    result
}

// ---------------------------------------------------------------------------
// LZ4 compression for cold partition column files
// ---------------------------------------------------------------------------

/// Statistics about a compression or decompression operation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompressionStats {
    pub original_bytes: u64,
    pub compressed_bytes: u64,
    pub ratio: f64,
}

const LZ4_MAGIC: &[u8; 4] = b"LZ4E";
const LZ4_HEADER_SIZE: usize = 4 + 8; // magic + original-size (u64 LE)

/// Compress a column `.d` file in-place using LZ4.
///
/// The original file at `path` is replaced with a compressed version at
/// `path.with_extension("d.lz4")`. Returns the compressed size.
///
/// The compressed file layout:
/// ```text
/// [4 bytes] magic "LZ4E"
/// [8 bytes] original uncompressed size (u64 LE)
/// [rest]    LZ4 compressed data
/// ```
pub fn compress_column_file(path: &Path) -> Result<u64> {
    let data = fs::read(path).map_err(ExchangeDbError::Io)?;
    let original_size = data.len() as u64;

    let compressed = lz4_flex::compress_prepend_size(&data);

    // Build output: header + compressed payload
    let mut output = Vec::with_capacity(LZ4_HEADER_SIZE + compressed.len());
    output.extend_from_slice(LZ4_MAGIC);
    output.extend_from_slice(&original_size.to_le_bytes());
    output.extend_from_slice(&compressed);

    let compressed_size = output.len() as u64;

    // Write compressed file with .lz4 extension
    let lz4_path = lz4_path_for(path);
    fs::write(&lz4_path, &output)?;

    // Remove original
    fs::remove_file(path)?;

    Ok(compressed_size)
}

/// Decompress a `.d.lz4` column file back to the original `.d` file.
///
/// Returns the decompressed (original) size.
pub fn decompress_column_file(path: &Path) -> Result<u64> {
    let lz4_path = if path.extension().and_then(|e| e.to_str()) == Some("lz4") {
        path.to_path_buf()
    } else {
        lz4_path_for(path)
    };

    let data = fs::read(&lz4_path)?;

    if data.len() < LZ4_HEADER_SIZE {
        return Err(ExchangeDbError::Corruption(
            "LZ4 file too short".to_string(),
        ));
    }

    if &data[..4] != LZ4_MAGIC {
        return Err(ExchangeDbError::Corruption(
            "invalid LZ4 magic bytes".to_string(),
        ));
    }

    let original_size = u64::from_le_bytes(data[4..12].try_into().unwrap());
    let compressed_payload = &data[LZ4_HEADER_SIZE..];

    let decompressed = lz4_flex::decompress_size_prepended(compressed_payload).map_err(|e| {
        ExchangeDbError::Corruption(format!("LZ4 decompression failed: {e}"))
    })?;

    if decompressed.len() as u64 != original_size {
        return Err(ExchangeDbError::Corruption(format!(
            "LZ4 size mismatch: expected {original_size}, got {}",
            decompressed.len()
        )));
    }

    // Write decompressed data to the .d path
    let d_path = d_path_from_lz4(&lz4_path);
    fs::write(&d_path, &decompressed)?;

    // Remove compressed file
    fs::remove_file(&lz4_path)?;

    Ok(original_size)
}

/// Compute compression stats for a before/after pair.
pub fn compression_stats(original_bytes: u64, compressed_bytes: u64) -> CompressionStats {
    let ratio = if original_bytes == 0 {
        1.0
    } else {
        compressed_bytes as f64 / original_bytes as f64
    };
    CompressionStats {
        original_bytes,
        compressed_bytes,
        ratio,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Given `foo/bar/price.d` return `foo/bar/price.d.lz4`.
fn lz4_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".lz4");
    PathBuf::from(s)
}

/// Given `foo/bar/price.d.lz4` return `foo/bar/price.d`.
fn d_path_from_lz4(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(stripped) = s.strip_suffix(".lz4") {
        PathBuf::from(stripped)
    } else {
        path.to_path_buf()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // -- Delta encoding tests -----------------------------------------------

    #[test]
    fn delta_encode_decode_monotonic() {
        let values = vec![100, 110, 125, 150, 200];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 100);
        assert_eq!(encoded.deltas, vec![10, 15, 25, 50]);
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn delta_encode_decode_empty() {
        let encoded = delta_encode_i64(&[]);
        assert_eq!(encoded.base, 0);
        assert!(encoded.deltas.is_empty());
        let decoded = delta_decode_i64(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn delta_encode_single() {
        let values = vec![42];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 42);
        assert!(encoded.deltas.is_empty());
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn delta_encode_constant_values() {
        let values = vec![7, 7, 7, 7];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 7);
        assert_eq!(encoded.deltas, vec![0, 0, 0]);
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn delta_encode_decreasing() {
        let values = vec![100, 90, 85, 80];
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, 100);
        assert_eq!(encoded.deltas, vec![-10, -5, -5]);
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn delta_encode_timestamps_realistic() {
        // Simulate nanosecond timestamps 1 second apart
        let base_ns: i64 = 1_710_513_000_000_000_000;
        let values: Vec<i64> = (0..100).map(|i| base_ns + i * 1_000_000_000).collect();
        let encoded = delta_encode_i64(&values);
        assert_eq!(encoded.base, base_ns);
        // All deltas should be 1 second in nanos
        assert!(encoded.deltas.iter().all(|&d| d == 1_000_000_000));
        let decoded = delta_decode_i64_nonempty(&encoded);
        assert_eq!(decoded, values);
    }

    // -- RLE tests ----------------------------------------------------------

    #[test]
    fn rle_encode_decode_basic() {
        let values = vec!["BTC", "BTC", "BTC", "ETH", "ETH", "SOL"];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![("BTC", 3), ("ETH", 2), ("SOL", 1)]);
        let decoded = rle_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn rle_encode_decode_empty() {
        let values: Vec<i32> = vec![];
        let encoded = rle_encode(&values);
        assert!(encoded.is_empty());
        let decoded = rle_decode(&encoded);
        assert!(decoded.is_empty());
    }

    #[test]
    fn rle_encode_single() {
        let values = vec![42];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(42, 1)]);
        let decoded = rle_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn rle_encode_no_runs() {
        let values = vec![1, 2, 3, 4, 5];
        let encoded = rle_encode(&values);
        assert_eq!(
            encoded,
            vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)]
        );
        let decoded = rle_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn rle_encode_all_same() {
        let values = vec![7; 1000];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(7, 1000)]);
        let decoded = rle_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn rle_encode_decode_i32_symbols() {
        // Symbol column stores i32 IDs; same ticker repeats in bursts
        let values = vec![0, 0, 0, 1, 1, 2, 0, 0];
        let encoded = rle_encode(&values);
        assert_eq!(encoded, vec![(0, 3), (1, 2), (2, 1), (0, 2)]);
        let decoded = rle_decode(&encoded);
        assert_eq!(decoded, values);
    }

    // -- LZ4 compression tests ---------------------------------------------

    #[test]
    fn lz4_compress_decompress_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("price.d");

        // Write some test data
        let original_data: Vec<u8> = (0..1000u64)
            .flat_map(|i| i.to_le_bytes())
            .collect();
        fs::write(&path, &original_data).unwrap();

        let original_size = original_data.len() as u64;

        // Compress
        let compressed_size = compress_column_file(&path).unwrap();
        assert!(compressed_size > 0);
        assert!(!path.exists(), "original .d file should be removed");
        let lz4_path = dir.path().join("price.d.lz4");
        assert!(lz4_path.exists(), ".d.lz4 file should exist");

        // Verify compressed is smaller for this pattern
        assert!(compressed_size < original_size);

        // Decompress (pass the .d path — it will find .d.lz4)
        let decompressed_size = decompress_column_file(&path).unwrap();
        assert_eq!(decompressed_size, original_size);
        assert!(path.exists(), "original .d file should be restored");
        assert!(!lz4_path.exists(), ".d.lz4 file should be removed");

        // Verify contents
        let restored = fs::read(&path).unwrap();
        assert_eq!(restored, original_data);
    }

    #[test]
    fn lz4_compress_decompress_via_lz4_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts.d");
        let data = vec![0xABu8; 512];
        fs::write(&path, &data).unwrap();

        compress_column_file(&path).unwrap();

        // Decompress using the .lz4 path directly
        let lz4_path = dir.path().join("ts.d.lz4");
        let size = decompress_column_file(&lz4_path).unwrap();
        assert_eq!(size, 512);

        let restored = fs::read(&path).unwrap();
        assert_eq!(restored, data);
    }

    #[test]
    fn lz4_corrupted_magic() {
        let dir = tempdir().unwrap();
        let lz4_path = dir.path().join("bad.d.lz4");
        fs::write(&lz4_path, b"BADDxxxxxxxx").unwrap();

        let result = decompress_column_file(&lz4_path);
        assert!(result.is_err());
    }

    #[test]
    fn lz4_file_too_short() {
        let dir = tempdir().unwrap();
        let lz4_path = dir.path().join("short.d.lz4");
        fs::write(&lz4_path, b"LZ4").unwrap(); // too short

        let result = decompress_column_file(&lz4_path);
        assert!(result.is_err());
    }

    // -- CompressionStats ---------------------------------------------------

    #[test]
    fn compression_stats_basic() {
        let stats = compression_stats(1000, 400);
        assert_eq!(stats.original_bytes, 1000);
        assert_eq!(stats.compressed_bytes, 400);
        assert!((stats.ratio - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn compression_stats_zero_original() {
        let stats = compression_stats(0, 0);
        assert!((stats.ratio - 1.0).abs() < f64::EPSILON);
    }

    // -- Path helper tests --------------------------------------------------

    #[test]
    fn lz4_path_helpers() {
        let p = Path::new("/data/trades/2024-03-15/price.d");
        let lz4 = lz4_path_for(p);
        assert_eq!(lz4, Path::new("/data/trades/2024-03-15/price.d.lz4"));
        let back = d_path_from_lz4(&lz4);
        assert_eq!(back, p);
    }
}
