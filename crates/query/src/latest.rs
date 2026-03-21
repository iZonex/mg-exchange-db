//! LATEST ON implementation for time-series queries.
//!
//! Returns the most recent row for each unique value of a partition column,
//! using a designated timestamp column to determine recency.

use crate::plan::Value;
use std::collections::HashMap;

/// Return the most recent row for each unique partition value.
///
/// Groups `rows` by the value in column `partition_col_idx`, then keeps
/// only the row with the maximum value in column `timestamp_col_idx` for
/// each group.
///
/// # Arguments
/// * `rows` - Input rows.
/// * `timestamp_col_idx` - Index of the timestamp column used for ordering.
/// * `partition_col_idx` - Index of the column to partition by (e.g., symbol).
pub fn latest_on(
    rows: &[Vec<Value>],
    timestamp_col_idx: usize,
    partition_col_idx: usize,
) -> Vec<Vec<Value>> {
    // Use a map from partition key -> (best_ts, best_row_index).
    let mut best: HashMap<ValueKey, (i64, usize)> = HashMap::new();

    for (i, row) in rows.iter().enumerate() {
        let key = ValueKey(row[partition_col_idx].clone());
        let ts = ts_value(&row[timestamp_col_idx]);

        match best.get(&key) {
            Some(&(existing_ts, _)) if ts < existing_ts => {}
            _ => {
                best.insert(key, (ts, i));
            }
        }
    }

    // Collect the winning rows, preserving insertion order by sorting by
    // the original row index.
    let mut indices: Vec<usize> = best.values().map(|(_, idx)| *idx).collect();
    indices.sort();

    indices.iter().map(|&i| rows[i].clone()).collect()
}

/// Extract a numeric timestamp from a Value for comparison.
fn ts_value(v: &Value) -> i64 {
    match v {
        Value::Timestamp(ns) => *ns,
        Value::I64(n) => *n,
        _ => i64::MIN,
    }
}

/// Wrapper for Value that implements Eq + Hash so it can be used as a HashMap key.
#[derive(Debug, Clone)]
struct ValueKey(Value);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => true,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
            (Value::Str(a), Value::Str(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ValueKey {}

impl std::hash::Hash for ValueKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Value::Null => {}
            Value::I64(v) => v.hash(state),
            Value::F64(v) => v.to_bits().hash(state),
            Value::Str(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
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

    /// Basic LATEST ON: multiple symbols with multiple timestamps.
    #[test]
    fn latest_on_basic() {
        // [timestamp, symbol, price]
        let rows = vec![
            vec![ts(100), s("BTC"), f(65000.0)],
            vec![ts(200), s("BTC"), f(65100.0)],
            vec![ts(300), s("BTC"), f(65200.0)],
            vec![ts(100), s("ETH"), f(3500.0)],
            vec![ts(200), s("ETH"), f(3550.0)],
        ];

        // timestamp_col=0, partition_col=1(symbol)
        let result = latest_on(&rows, 0, 1);
        assert_eq!(result.len(), 2);

        // BTC latest is at ts=300
        let btc = result.iter().find(|r| r[1] == s("BTC")).unwrap();
        assert_eq!(btc[0], ts(300));
        assert_eq!(btc[2], f(65200.0));

        // ETH latest is at ts=200
        let eth = result.iter().find(|r| r[1] == s("ETH")).unwrap();
        assert_eq!(eth[0], ts(200));
        assert_eq!(eth[2], f(3550.0));
    }

    /// LATEST ON with single symbol returns one row.
    #[test]
    fn latest_on_single_symbol() {
        let rows = vec![
            vec![ts(100), s("BTC"), f(100.0)],
            vec![ts(200), s("BTC"), f(200.0)],
            vec![ts(150), s("BTC"), f(150.0)],
        ];

        let result = latest_on(&rows, 0, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], ts(200));
        assert_eq!(result[0][2], f(200.0));
    }

    /// LATEST ON with empty input returns empty output.
    #[test]
    fn latest_on_empty() {
        let rows: Vec<Vec<Value>> = vec![];
        let result = latest_on(&rows, 0, 1);
        assert!(result.is_empty());
    }

    /// LATEST ON with many symbols.
    #[test]
    fn latest_on_many_symbols() {
        let rows = vec![
            vec![ts(100), s("AAPL"), f(150.0)],
            vec![ts(200), s("AAPL"), f(151.0)],
            vec![ts(100), s("GOOG"), f(2800.0)],
            vec![ts(300), s("GOOG"), f(2850.0)],
            vec![ts(100), s("MSFT"), f(300.0)],
        ];

        let result = latest_on(&rows, 0, 1);
        assert_eq!(result.len(), 3);

        let aapl = result.iter().find(|r| r[1] == s("AAPL")).unwrap();
        assert_eq!(aapl[0], ts(200));

        let goog = result.iter().find(|r| r[1] == s("GOOG")).unwrap();
        assert_eq!(goog[0], ts(300));

        let msft = result.iter().find(|r| r[1] == s("MSFT")).unwrap();
        assert_eq!(msft[0], ts(100));
    }

    /// LATEST ON with equal timestamps keeps the last occurrence.
    #[test]
    fn latest_on_equal_timestamps() {
        let rows = vec![
            vec![ts(100), s("BTC"), f(100.0)],
            vec![ts(100), s("BTC"), f(200.0)],
        ];

        let result = latest_on(&rows, 0, 1);
        assert_eq!(result.len(), 1);
        // Second row has same ts but appears later, so it wins.
        assert_eq!(result[0][2], f(200.0));
    }
}
