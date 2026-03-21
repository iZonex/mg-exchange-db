//! LatestBy cursor — efficient LATEST ON implementation.
//!
//! Tracks the last row per partition key and outputs one row per key
//! after consuming the entire source.

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits the last row for each distinct partition key value.
///
/// This is the cursor equivalent of `SELECT ... LATEST ON timestamp PARTITION BY symbol`.
/// It materializes the source, keeping only the latest row per key.
pub struct LatestByCursor {
    source: Option<Box<dyn RecordCursor>>,
    /// Column indices that form the partition key.
    partition_cols: Vec<usize>,
    /// Column index of the timestamp used for ordering (latest = largest).
    ts_col: usize,
    result: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl LatestByCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        partition_cols: Vec<usize>,
        ts_col: usize,
    ) -> Self {
        let schema = source.schema().to_vec();
        Self {
            source: Some(source),
            partition_cols,
            ts_col,
            result: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");

        // Map from serialized partition key -> (timestamp, full row).
        let mut latest: HashMap<Vec<u8>, (i64, Vec<Value>)> = HashMap::new();
        let mut key_order: Vec<Vec<u8>> = Vec::new();

        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols)
                            .map(|c| batch.get_value(r, c))
                            .collect();

                        let ts = match &row[self.ts_col] {
                            Value::Timestamp(n) | Value::I64(n) => *n,
                            _ => 0,
                        };

                        let key = self.partition_key(&row);

                        match latest.get(&key) {
                            None => {
                                key_order.push(key.clone());
                                latest.insert(key, (ts, row));
                            }
                            Some((existing_ts, _)) if ts >= *existing_ts => {
                                latest.insert(key, (ts, row));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        let mut result = RecordBatch::new(self.schema.clone());
        for key in &key_order {
            if let Some((_, row)) = latest.get(key) {
                result.append_row(row);
            }
        }
        self.result = Some(result);
        Ok(())
    }

    fn partition_key(&self, row: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();
        for &col in &self.partition_cols {
            serialize_value(&row[col], &mut buf);
        }
        buf
    }
}

fn serialize_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(0),
        Value::I64(n) => {
            buf.push(1);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::F64(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_bits().to_le_bytes());
        }
        Value::Str(s) => {
            buf.push(3);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Timestamp(n) => {
            buf.push(4);
            buf.extend_from_slice(&n.to_le_bytes());
        }
    }
}

impl RecordCursor for LatestByCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() {
            self.materialize()?;
        }

        let mat = self.result.as_ref().unwrap();
        if self.current_row >= mat.row_count() {
            return Ok(None);
        }

        let remaining = mat.row_count() - self.current_row;
        let n = remaining.min(max_rows);
        let batch = mat.slice(self.current_row, n);
        self.current_row += n;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn latest_by_partition() {
        let schema = vec![
            ("symbol".to_string(), ColumnType::Varchar),
            ("ts".to_string(), ColumnType::Timestamp),
            ("price".to_string(), ColumnType::F64),
        ];
        let rows = vec![
            vec![Value::Str("BTC".into()), Value::Timestamp(100), Value::F64(50000.0)],
            vec![Value::Str("ETH".into()), Value::Timestamp(100), Value::F64(3000.0)],
            vec![Value::Str("BTC".into()), Value::Timestamp(200), Value::F64(51000.0)],
            vec![Value::Str("ETH".into()), Value::Timestamp(200), Value::F64(3100.0)],
            vec![Value::Str("BTC".into()), Value::Timestamp(300), Value::F64(52000.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        // partition by symbol (col 0), order by ts (col 1)
        let mut cursor = LatestByCursor::new(Box::new(source), vec![0], 1);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all.push(row);
            }
        }
        // BTC latest at ts=300 price=52000, ETH latest at ts=200 price=3100
        assert_eq!(all.len(), 2);
        assert_eq!(all[0][2], Value::F64(52000.0));
        assert_eq!(all[1][2], Value::F64(3100.0));
    }
}
