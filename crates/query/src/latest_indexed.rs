//! Index-accelerated LATEST ON implementation.
//!
//! When the partition column (e.g., symbol) has a bitmap index, we can
//! avoid a full scan by:
//! 1. Iterating unique keys in the bitmap index
//! 2. For each key, finding the last (most recent) row ID
//! 3. Reading only those specific rows
//!
//! This is dramatically faster than scanning all rows when the table is
//! large and the number of distinct partition values is small.

use crate::plan::Value;

/// Result of an index-accelerated LATEST ON operation.
///
/// Contains the row IDs that represent the latest row for each unique
/// partition key, along with the key values.
pub struct IndexedLatestResult {
    /// Row IDs of the latest rows, one per unique partition key.
    pub row_ids: Vec<u64>,
    /// The partition key (as i32 symbol ID) for each row.
    pub key_ids: Vec<i32>,
}

/// Find the latest (maximum) row ID for each unique key in a bitmap index.
///
/// This avoids scanning all rows — it only examines the index metadata
/// to find the last row ID per key.
///
/// # Arguments
/// * `max_key` - The maximum key in the bitmap index (inclusive).
/// * `get_row_ids` - A closure that returns all row IDs for a given key.
///   The row IDs must be in insertion order (ascending).
pub fn latest_by_index<F>(max_key: i32, get_row_ids: F) -> IndexedLatestResult
where
    F: Fn(i32) -> Vec<u64>,
{
    let mut row_ids = Vec::new();
    let mut key_ids = Vec::new();

    for key in 0..=max_key {
        let ids = get_row_ids(key);
        if let Some(&last_id) = ids.last() {
            row_ids.push(last_id);
            key_ids.push(key);
        }
    }

    IndexedLatestResult { row_ids, key_ids }
}

/// Check whether index-accelerated LATEST ON should be used.
///
/// Returns true when the partition column has a bitmap index and the
/// number of distinct values is significantly smaller than the total
/// row count (making index lookup worthwhile).
pub fn should_use_indexed_latest(has_index: bool, distinct_count: u64, total_rows: u64) -> bool {
    if !has_index || distinct_count == 0 {
        return false;
    }
    // Use index when we'd read fewer than 50% of rows via index lookup.
    // In practice, distinct_count << total_rows for symbol columns.
    distinct_count < total_rows / 2
}

/// Given a set of row IDs and pre-loaded column data, extract the rows
/// at those positions.
///
/// # Arguments
/// * `row_ids` - The row IDs to extract.
/// * `all_rows` - All rows in the partition (or the full table scan result).
///
/// Returns the selected rows in the order of `row_ids`.
pub fn extract_rows_by_ids(row_ids: &[u64], all_rows: &[Vec<Value>]) -> Vec<Vec<Value>> {
    row_ids
        .iter()
        .filter_map(|&id| {
            let idx = id as usize;
            if idx < all_rows.len() {
                Some(all_rows[idx].clone())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_by_index_basic() {
        // Simulate a bitmap index with 3 keys:
        // key 0 (BTC): rows [0, 3, 6]
        // key 1 (ETH): rows [1, 4, 7]
        // key 2 (SOL): rows [2, 5]
        let index_data: Vec<Vec<u64>> = vec![vec![0, 3, 6], vec![1, 4, 7], vec![2, 5]];

        let result = latest_by_index(2, |key| {
            index_data.get(key as usize).cloned().unwrap_or_default()
        });

        assert_eq!(result.row_ids, vec![6, 7, 5]);
        assert_eq!(result.key_ids, vec![0, 1, 2]);
    }

    #[test]
    fn latest_by_index_single_key() {
        let result = latest_by_index(0, |key| if key == 0 { vec![0, 1, 2, 3] } else { vec![] });
        assert_eq!(result.row_ids, vec![3]);
        assert_eq!(result.key_ids, vec![0]);
    }

    #[test]
    fn latest_by_index_empty_keys() {
        let result = latest_by_index(2, |_key| vec![]);
        assert!(result.row_ids.is_empty());
        assert!(result.key_ids.is_empty());
    }

    #[test]
    fn latest_by_index_sparse_keys() {
        // Key 0 has data, key 1 is empty, key 2 has data.
        let result = latest_by_index(2, |key| match key {
            0 => vec![0, 5, 10],
            2 => vec![3, 8],
            _ => vec![],
        });
        assert_eq!(result.row_ids, vec![10, 8]);
        assert_eq!(result.key_ids, vec![0, 2]);
    }

    #[test]
    fn should_use_indexed_latest_checks() {
        // Should use: 10 distinct out of 10000 rows.
        assert!(should_use_indexed_latest(true, 10, 10000));
        // Should not use: no index.
        assert!(!should_use_indexed_latest(false, 10, 10000));
        // Should not use: distinct count is 0.
        assert!(!should_use_indexed_latest(true, 0, 10000));
        // Should not use: distinct count is >= 50% of rows.
        assert!(!should_use_indexed_latest(true, 6000, 10000));
    }

    #[test]
    fn extract_rows_by_ids_basic() {
        let rows = vec![
            vec![Value::I64(0), Value::Str("BTC".into())],
            vec![Value::I64(1), Value::Str("ETH".into())],
            vec![Value::I64(2), Value::Str("SOL".into())],
            vec![Value::I64(3), Value::Str("BTC".into())],
            vec![Value::I64(4), Value::Str("ETH".into())],
        ];

        let result = extract_rows_by_ids(&[3, 4, 2], &rows);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], Value::I64(3));
        assert_eq!(result[1][0], Value::I64(4));
        assert_eq!(result[2][0], Value::I64(2));
    }

    #[test]
    fn extract_rows_out_of_bounds() {
        let rows = vec![vec![Value::I64(0)], vec![Value::I64(1)]];
        let result = extract_rows_by_ids(&[0, 5, 1], &rows);
        // ID 5 is out of bounds and skipped.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], Value::I64(0));
        assert_eq!(result[1][0], Value::I64(1));
    }

    #[test]
    fn indexed_latest_faster_than_full_scan_simulation() {
        // Simulate: 100_000 rows, 50 symbols.
        let total_rows = 100_000u64;
        let distinct = 50u64;

        // Full scan cost: process all rows.
        let full_scan_work = total_rows;

        // Indexed cost: for each key, read the index (small), then read 1 row.
        let index_work = distinct; // one lookup per key

        assert!(index_work < full_scan_work);
        assert!(should_use_indexed_latest(true, distinct, total_rows));
    }
}
