//! Deduplication support for table writes.
//!
//! Provides configurable row-level deduplication based on key columns.
//! When enabled, duplicate rows (based on key column values) are removed,
//! keeping the last occurrence.

use crate::table::ColumnValue;
use std::collections::HashMap;

/// Deduplication configuration for a table.
#[derive(Debug, Clone)]
pub struct DedupConfig {
    /// Whether deduplication is enabled.
    pub enabled: bool,
    /// Column names that form the deduplication key.
    pub key_columns: Vec<String>,
}

impl DedupConfig {
    /// Create a new dedup config with the given key columns.
    pub fn new(key_columns: Vec<String>) -> Self {
        Self {
            enabled: true,
            key_columns,
        }
    }

    /// Create a disabled dedup config.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            key_columns: Vec::new(),
        }
    }
}

/// Extract a hashable key from a row at the given column indices.
/// Uses a string-based key for simplicity and generality.
fn extract_key(row: &[ColumnValue<'_>], key_col_indices: &[usize]) -> String {
    let mut key = String::new();
    for (i, &idx) in key_col_indices.iter().enumerate() {
        if i > 0 {
            key.push('\x00');
        }
        if idx < row.len() {
            match &row[idx] {
                ColumnValue::I32(v) => key.push_str(&v.to_string()),
                ColumnValue::I64(v) => key.push_str(&v.to_string()),
                ColumnValue::F64(v) => key.push_str(&format!("{v:?}")),
                ColumnValue::Timestamp(t) => key.push_str(&t.as_nanos().to_string()),
                ColumnValue::Str(s) => key.push_str(s),
                ColumnValue::Bytes(b) => {
                    for byte in *b {
                        key.push_str(&format!("{byte:02x}"));
                    }
                }
                ColumnValue::Null => key.push_str("NULL"),
            }
        } else {
            key.push_str("NULL");
        }
    }
    key
}

/// Return indices of unique rows, keeping the last occurrence for each key.
pub fn unique_row_indices(
    rows: &[Vec<ColumnValue<'_>>],
    key_col_indices: &[usize],
) -> Vec<usize> {
    // Map from key -> last index.
    let mut last_seen: HashMap<String, usize> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let key = extract_key(row, key_col_indices);
        last_seen.insert(key, i);
    }

    // Collect unique indices in original order.
    let mut unique_indices: Vec<usize> = last_seen.into_values().collect();
    unique_indices.sort();
    unique_indices
}

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_common::types::Timestamp;

    #[test]
    fn dedup_keeps_last_occurrence() {
        let rows: Vec<Vec<ColumnValue<'_>>> = vec![
            vec![ColumnValue::I64(1), ColumnValue::Str("BTC"), ColumnValue::F64(100.0)],
            vec![ColumnValue::I64(2), ColumnValue::Str("ETH"), ColumnValue::F64(200.0)],
            vec![ColumnValue::I64(3), ColumnValue::Str("BTC"), ColumnValue::F64(150.0)],
            vec![ColumnValue::I64(4), ColumnValue::Str("ETH"), ColumnValue::F64(250.0)],
        ];

        // Dedup by column 1 (symbol).
        let indices = unique_row_indices(&rows, &[1]);
        assert_eq!(indices, vec![2, 3]); // last BTC at index 2, last ETH at index 3
    }

    #[test]
    fn dedup_no_duplicates() {
        let rows: Vec<Vec<ColumnValue<'_>>> = vec![
            vec![ColumnValue::I64(1), ColumnValue::Str("BTC")],
            vec![ColumnValue::I64(2), ColumnValue::Str("ETH")],
            vec![ColumnValue::I64(3), ColumnValue::Str("SOL")],
        ];

        let indices = unique_row_indices(&rows, &[1]);
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn dedup_empty_rows() {
        let rows: Vec<Vec<ColumnValue<'_>>> = vec![];
        let indices = unique_row_indices(&rows, &[0]);
        assert!(indices.is_empty());
    }

    #[test]
    fn dedup_composite_key() {
        let rows: Vec<Vec<ColumnValue<'_>>> = vec![
            vec![ColumnValue::Str("BTC"), ColumnValue::Str("binance"), ColumnValue::F64(100.0)],
            vec![ColumnValue::Str("BTC"), ColumnValue::Str("kraken"), ColumnValue::F64(101.0)],
            vec![ColumnValue::Str("BTC"), ColumnValue::Str("binance"), ColumnValue::F64(102.0)],
        ];

        // Dedup by columns 0 and 1 (symbol + exchange).
        let indices = unique_row_indices(&rows, &[0, 1]);
        assert_eq!(indices, vec![1, 2]); // (BTC,kraken) at 1, (BTC,binance) last at 2
    }

    #[test]
    fn dedup_with_timestamp() {
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        let rows: Vec<Vec<ColumnValue<'_>>> = vec![
            vec![ColumnValue::Timestamp(ts1), ColumnValue::Str("BTC")],
            vec![ColumnValue::Timestamp(ts2), ColumnValue::Str("BTC")],
        ];

        let indices = unique_row_indices(&rows, &[1]);
        assert_eq!(indices, vec![1]); // keeps last BTC
    }

    #[test]
    fn dedup_with_null_values() {
        let rows: Vec<Vec<ColumnValue<'_>>> = vec![
            vec![ColumnValue::Null, ColumnValue::Str("BTC")],
            vec![ColumnValue::Null, ColumnValue::Str("ETH")],
            vec![ColumnValue::Null, ColumnValue::Str("BTC")],
        ];

        let indices = unique_row_indices(&rows, &[0, 1]);
        assert_eq!(indices, vec![1, 2]);
    }

    #[test]
    fn dedup_config_creation() {
        let cfg = DedupConfig::new(vec!["symbol".to_string(), "exchange".to_string()]);
        assert!(cfg.enabled);
        assert_eq!(cfg.key_columns.len(), 2);

        let disabled = DedupConfig::disabled();
        assert!(!disabled.enabled);
    }
}
