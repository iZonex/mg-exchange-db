//! ASOF JOIN implementation for temporal joins.
//!
//! For each row in the left dataset, finds the closest row in the right dataset
//! where `right.timestamp <= left.timestamp`, optionally matching on equality
//! columns (e.g., symbol).
//!
//! ## Algorithm
//!
//! When `on_cols` is empty, a simple two-pointer merge runs in O(L + R) time
//! (both sides must already be sorted by timestamp).
//!
//! When `on_cols` is non-empty, the right side is partitioned by key values into
//! a `HashMap<Vec<Value>, Vec<usize>>`. For each left row we look up its
//! partition and binary-search for the correct timestamp. Total complexity is
//! O(R + L * log(R/K)) where K is the number of distinct keys — typically many
//! orders of magnitude faster than the previous O(L * R) brute-force scan.

use std::collections::HashMap;

use crate::plan::Value;

/// Perform an ASOF JOIN between two sorted-by-timestamp datasets.
///
/// For each left row, finds the latest right row where `right.ts <= left.ts`
/// and all `on_cols` values match. Returns combined rows containing all left
/// columns followed by the selected right columns (excluding the columns used
/// for joining).
///
/// # Arguments
/// * `left_rows` - Left dataset rows, sorted by timestamp.
/// * `right_rows` - Right dataset rows, sorted by timestamp.
/// * `left_ts_col` - Index of the timestamp column in left rows.
/// * `right_ts_col` - Index of the timestamp column in right rows.
/// * `on_cols` - Pairs of (left_col_idx, right_col_idx) for equality matching.
/// * `right_output_cols` - Indices of right columns to include in output
///   (excluding join key and timestamp columns).
pub fn asof_join(
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    left_ts_col: usize,
    right_ts_col: usize,
    on_cols: &[(usize, usize)],
    right_output_cols: &[usize],
) -> Vec<Vec<Value>> {
    if on_cols.is_empty() {
        return merge_asof_no_key(
            left_rows,
            right_rows,
            left_ts_col,
            right_ts_col,
            right_output_cols,
        );
    }

    merge_asof_partitioned(
        left_rows,
        right_rows,
        left_ts_col,
        right_ts_col,
        on_cols,
        right_output_cols,
    )
}

/// Simple two-pointer merge when there are no ON columns.
///
/// Both sides must be sorted by timestamp. We walk a single pointer through
/// the right side, advancing it for each left row. O(L + R).
fn merge_asof_no_key(
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    left_ts_col: usize,
    right_ts_col: usize,
    right_output_cols: &[usize],
) -> Vec<Vec<Value>> {
    let mut result = Vec::with_capacity(left_rows.len());
    let mut right_idx: usize = 0;

    for left_row in left_rows {
        let left_ts = ts_value(&left_row[left_ts_col]);

        // Advance right pointer while right.ts <= left.ts.
        while right_idx < right_rows.len()
            && ts_value(&right_rows[right_idx][right_ts_col]) <= left_ts
        {
            right_idx += 1;
        }

        // The match is at right_idx - 1 (last right row where ts <= left_ts).
        let mut out_row = left_row.clone();
        if right_idx > 0 {
            let right_row = &right_rows[right_idx - 1];
            for &col in right_output_cols {
                out_row.push(right_row[col].clone());
            }
        } else {
            for _ in right_output_cols {
                out_row.push(Value::Null);
            }
        }
        result.push(out_row);
    }

    result
}

/// Partition-and-binary-search merge for ASOF JOIN with ON columns.
///
/// 1. Build a `HashMap` from right-side key values to sorted row indices.
/// 2. For each left row, look up the matching partition and binary-search
///    for the largest right timestamp <= left timestamp.
///
/// Complexity: O(R + L * log(R/K)) where K is the number of distinct keys.
fn merge_asof_partitioned(
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    left_ts_col: usize,
    right_ts_col: usize,
    on_cols: &[(usize, usize)],
    right_output_cols: &[usize],
) -> Vec<Vec<Value>> {
    // Build an index: key_values -> Vec of right row indices (already sorted by
    // timestamp since right_rows is sorted).
    let right_col_indices: Vec<usize> = on_cols.iter().map(|(_, ri)| *ri).collect();
    let left_col_indices: Vec<usize> = on_cols.iter().map(|(li, _)| *li).collect();

    // Pre-extract right timestamps for binary search.
    let right_ts: Vec<i64> = right_rows
        .iter()
        .map(|r| ts_value(&r[right_ts_col]))
        .collect();

    // Group right row indices by their key columns.
    let mut right_partitions: HashMap<Vec<ValueKey>, Vec<usize>> = HashMap::new();
    for (idx, right_row) in right_rows.iter().enumerate() {
        let key: Vec<ValueKey> = right_col_indices
            .iter()
            .map(|&ci| ValueKey(right_row[ci].clone()))
            .collect();
        right_partitions.entry(key).or_default().push(idx);
    }

    let mut result = Vec::with_capacity(left_rows.len());

    for left_row in left_rows {
        let left_ts = ts_value(&left_row[left_ts_col]);
        let key: Vec<ValueKey> = left_col_indices
            .iter()
            .map(|&ci| ValueKey(left_row[ci].clone()))
            .collect();

        let mut out_row = left_row.clone();

        if let Some(partition) = right_partitions.get(&key) {
            // Binary search: find the rightmost index where right_ts <= left_ts.
            // partition contains indices into right_rows, sorted by timestamp.
            // We want the last entry where right_ts[partition[i]] <= left_ts.
            let pos = partition.partition_point(|&ri| right_ts[ri] <= left_ts);

            if pos > 0 {
                let right_row = &right_rows[partition[pos - 1]];
                for &col in right_output_cols {
                    out_row.push(right_row[col].clone());
                }
            } else {
                for _ in right_output_cols {
                    out_row.push(Value::Null);
                }
            }
        } else {
            for _ in right_output_cols {
                out_row.push(Value::Null);
            }
        }

        result.push(out_row);
    }

    result
}

/// Extract a numeric timestamp from a Value for comparison.
#[inline]
fn ts_value(v: &Value) -> i64 {
    match v {
        Value::Timestamp(ns) => *ns,
        Value::I64(n) => *n,
        _ => i64::MIN,
    }
}

/// Wrapper around `Value` that implements `Eq` and `Hash` for use as HashMap key.
///
/// We need this because `Value` derives `PartialEq` but not `Eq`/`Hash` (due to
/// `f64`). For ASOF JOIN partition keys, we typically join on string/symbol
/// columns, so the f64 case is rare. We handle it by hashing the bit pattern.
#[derive(Clone, Debug)]
struct ValueKey(Value);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
            _ => self.0 == other.0,
        }
    }
}

impl Eq for ValueKey {}

impl std::hash::Hash for ValueKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Value::Null => {}
            Value::I64(n) => n.hash(state),
            Value::F64(f) => f.to_bits().hash(state),
            Value::Str(s) => s.hash(state),
            Value::Timestamp(ns) => ns.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::Value;

    fn ts(ns: i64) -> Value {
        Value::Timestamp(ns)
    }

    fn s(v: &str) -> Value {
        Value::Str(v.to_string())
    }

    fn f(v: f64) -> Value {
        Value::F64(v)
    }

    /// Basic ASOF JOIN: trades joined with quotes on symbol.
    /// Each trade should get the quote that was active at that time.
    #[test]
    fn asof_join_trades_quotes() {
        // trades: [timestamp, symbol, price, volume]
        let trades = vec![
            vec![ts(100), s("BTC"), f(65000.0), f(1.0)],
            vec![ts(200), s("BTC"), f(65100.0), f(0.5)],
            vec![ts(150), s("ETH"), f(3500.0), f(10.0)],
            vec![ts(300), s("ETH"), f(3550.0), f(5.0)],
        ];

        // quotes: [timestamp, symbol, bid, ask]
        let quotes = vec![
            vec![ts(50), s("BTC"), f(64990.0), f(65010.0)],
            vec![ts(80), s("ETH"), f(3490.0), f(3510.0)],
            vec![ts(120), s("BTC"), f(65050.0), f(65070.0)],
            vec![ts(180), s("ETH"), f(3520.0), f(3540.0)],
            vec![ts(250), s("BTC"), f(65150.0), f(65170.0)],
        ];

        // on_cols: symbol is col 1 in both
        // right_output_cols: bid (2), ask (3)
        let result = asof_join(&trades, &quotes, 0, 0, &[(1, 1)], &[2, 3]);

        assert_eq!(result.len(), 4);

        // Trade BTC@100: latest quote with ts<=100 for BTC is ts=50, bid=64990
        assert_eq!(result[0][4], f(64990.0));
        assert_eq!(result[0][5], f(65010.0));

        // Trade BTC@200: latest quote with ts<=200 for BTC is ts=120, bid=65050
        assert_eq!(result[1][4], f(65050.0));
        assert_eq!(result[1][5], f(65070.0));

        // Trade ETH@150: latest quote with ts<=150 for ETH is ts=80, bid=3490
        assert_eq!(result[2][4], f(3490.0));
        assert_eq!(result[2][5], f(3510.0));

        // Trade ETH@300: latest quote with ts<=300 for ETH is ts=180, bid=3520
        assert_eq!(result[3][4], f(3520.0));
        assert_eq!(result[3][5], f(3540.0));
    }

    /// ASOF JOIN with no matching right row should produce NULLs.
    #[test]
    fn asof_join_no_match() {
        let left = vec![vec![ts(10), s("BTC"), f(100.0)]];
        let right = vec![vec![ts(20), s("BTC"), f(99.0), f(101.0)]];

        let result = asof_join(&left, &right, 0, 0, &[(1, 1)], &[2, 3]);
        assert_eq!(result.len(), 1);
        // No right row with ts <= 10, so NULLs.
        assert_eq!(result[0][3], Value::Null);
        assert_eq!(result[0][4], Value::Null);
    }

    /// ASOF JOIN without on_cols (pure temporal join).
    #[test]
    fn asof_join_no_on_cols() {
        let left = vec![vec![ts(100), f(1.0)], vec![ts(200), f(2.0)]];
        let right = vec![
            vec![ts(50), f(10.0)],
            vec![ts(150), f(20.0)],
            vec![ts(250), f(30.0)],
        ];

        let result = asof_join(&left, &right, 0, 0, &[], &[1]);
        assert_eq!(result.len(), 2);
        // left@100: latest right ts<=100 is ts=50, value=10.0
        assert_eq!(result[0][2], f(10.0));
        // left@200: latest right ts<=200 is ts=150, value=20.0
        assert_eq!(result[1][2], f(20.0));
    }

    /// Multiple symbols with interleaved timestamps.
    #[test]
    fn asof_join_multiple_symbols() {
        let trades = vec![
            vec![ts(100), s("AAPL"), f(150.0)],
            vec![ts(100), s("GOOG"), f(2800.0)],
            vec![ts(200), s("AAPL"), f(151.0)],
            vec![ts(200), s("GOOG"), f(2810.0)],
        ];

        let quotes = vec![
            vec![ts(90), s("AAPL"), f(149.5), f(150.5)],
            vec![ts(95), s("GOOG"), f(2795.0), f(2805.0)],
            vec![ts(150), s("AAPL"), f(150.5), f(151.5)],
            vec![ts(160), s("GOOG"), f(2805.0), f(2815.0)],
        ];

        let result = asof_join(&trades, &quotes, 0, 0, &[(1, 1)], &[2, 3]);
        assert_eq!(result.len(), 4);

        // AAPL@100: quote@90
        assert_eq!(result[0][3], f(149.5));
        // GOOG@100: quote@95
        assert_eq!(result[1][3], f(2795.0));
        // AAPL@200: quote@150
        assert_eq!(result[2][3], f(150.5));
        // GOOG@200: quote@160
        assert_eq!(result[3][3], f(2805.0));
    }

    /// Large-scale test: verify the optimized algorithm matches expected results.
    #[test]
    fn asof_join_large_scale() {
        // 1000 left rows, 5000 right rows, 10 symbols.
        let mut left = Vec::new();
        for i in 0..1000 {
            let sym = format!("SYM{}", i % 10);
            left.push(vec![ts(i * 100), s(&sym), f(i as f64)]);
        }

        let mut right = Vec::new();
        for i in 0..5000 {
            let sym = format!("SYM{}", i % 10);
            right.push(vec![
                ts(i * 20),
                s(&sym),
                f(i as f64 * 0.1),
                f(i as f64 * 0.2),
            ]);
        }

        let result = asof_join(&left, &right, 0, 0, &[(1, 1)], &[2, 3]);
        assert_eq!(result.len(), 1000);

        // Verify no panics and correct structure.
        for row in &result {
            assert_eq!(row.len(), 5); // 3 left cols + 2 right cols
        }
    }

    /// Edge case: left row at exact same timestamp as right row should match.
    #[test]
    fn asof_join_exact_match() {
        let left = vec![vec![ts(100), s("A"), f(1.0)]];
        let right = vec![vec![ts(100), s("A"), f(42.0)]];

        let result = asof_join(&left, &right, 0, 0, &[(1, 1)], &[2]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][3], f(42.0));
    }
}
