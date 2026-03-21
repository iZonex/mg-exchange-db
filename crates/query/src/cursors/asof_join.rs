//! ASOF join cursor — temporal join for time-series data.
//!
//! For each left row, finds the most recent right row whose timestamp is
//! less than or equal to the left row's timestamp (within an optional
//! partition key).

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// ASOF join cursor: for each left row, matches the latest right row
/// where `right.ts <= left.ts` (optionally within a partition key).
///
/// Both sides must be sorted by timestamp ascending.
pub struct AsofJoinCursor {
    left: Box<dyn RecordCursor>,
    /// Right rows grouped by partition key, sorted by timestamp.
    right_partitions: HashMap<Vec<u8>, Vec<(i64, Vec<Value>)>>,
    left_ts_col: usize,
    right_ts_col: usize,
    /// Optional partition key columns on the left.
    left_key_cols: Vec<usize>,
    /// Optional partition key columns on the right.
    right_key_cols: Vec<usize>,
    schema: Vec<(String, ColumnType)>,
    right_col_count: usize,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
}

impl AsofJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_ts_col: usize,
        right_ts_col: usize,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
    ) -> Self {
        let left_schema = left.schema().to_vec();
        let right_schema = right.schema().to_vec();
        let right_col_count = right_schema.len();
        let mut schema = left_schema;
        schema.extend(right_schema);

        Self {
            left,
            right_partitions: HashMap::new(),
            left_ts_col,
            right_ts_col,
            left_key_cols,
            right_key_cols,
            schema,
            right_col_count,
            built: false,
            right_source: Some(right),
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self.right_source.take().expect("right source already consumed");
        loop {
            match right.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols)
                            .map(|c| batch.get_value(r, c))
                            .collect();

                        let ts = match &row[self.right_ts_col] {
                            Value::Timestamp(n) | Value::I64(n) => *n,
                            _ => 0,
                        };

                        let key = serialize_key(&row, &self.right_key_cols);
                        self.right_partitions
                            .entry(key)
                            .or_default()
                            .push((ts, row));
                    }
                }
            }
        }

        // Sort each partition by timestamp.
        for rows in self.right_partitions.values_mut() {
            rows.sort_by_key(|(ts, _)| *ts);
        }

        self.built = true;
        Ok(())
    }

    /// Binary search for the latest right row with ts <= target.
    fn find_asof(rows: &[(i64, Vec<Value>)], target_ts: i64) -> Option<&Vec<Value>> {
        if rows.is_empty() {
            return None;
        }
        // Find the rightmost entry where ts <= target_ts.
        let mut lo = 0usize;
        let mut hi = rows.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if rows[mid].0 <= target_ts {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            None
        } else {
            Some(&rows[lo - 1].1)
        }
    }
}

fn serialize_key(row: &[Value], key_cols: &[usize]) -> Vec<u8> {
    let mut buf = Vec::new();
    for &col in key_cols {
        if col < row.len() {
            match &row[col] {
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
    }
    buf
}

impl RecordCursor for AsofJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }

        let mut result = RecordBatch::new(self.schema.clone());

        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let left_row: Vec<Value> = (0..ncols)
                            .map(|c| batch.get_value(r, c))
                            .collect();

                        let left_ts = match &left_row[self.left_ts_col] {
                            Value::Timestamp(n) | Value::I64(n) => *n,
                            _ => 0,
                        };

                        let key = serialize_key(&left_row, &self.left_key_cols);
                        let right_match = self
                            .right_partitions
                            .get(&key)
                            .and_then(|rows| Self::find_asof(rows, left_ts));

                        let mut combined = left_row;
                        if let Some(right_row) = right_match {
                            combined.extend(right_row.iter().cloned());
                        } else {
                            for _ in 0..self.right_col_count {
                                combined.push(Value::Null);
                            }
                        }
                        result.append_row(&combined);

                        if result.row_count() >= max_rows {
                            break;
                        }
                    }
                }
            }
        }

        if result.row_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn asof_join_basic() {
        let left_schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("symbol".to_string(), ColumnType::Varchar),
        ];
        let right_schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("symbol".to_string(), ColumnType::Varchar),
            ("bid".to_string(), ColumnType::F64),
        ];

        let left = MemoryCursor::from_rows(
            left_schema,
            &[
                vec![Value::Timestamp(100), Value::Str("BTC".into())],
                vec![Value::Timestamp(250), Value::Str("BTC".into())],
                vec![Value::Timestamp(400), Value::Str("BTC".into())],
            ],
        );
        let right = MemoryCursor::from_rows(
            right_schema,
            &[
                vec![Value::Timestamp(50), Value::Str("BTC".into()), Value::F64(100.0)],
                vec![Value::Timestamp(200), Value::Str("BTC".into()), Value::F64(200.0)],
                vec![Value::Timestamp(300), Value::Str("BTC".into()), Value::F64(300.0)],
            ],
        );

        let mut cursor = AsofJoinCursor::new(
            Box::new(left),
            Box::new(right),
            0, // left ts col
            0, // right ts col
            vec![1], // left partition key: symbol
            vec![1], // right partition key: symbol
        );

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all.push(row);
            }
        }

        assert_eq!(all.len(), 3);
        // ts=100 -> right ts=50 (bid=100)
        assert_eq!(all[0][4], Value::F64(100.0));
        // ts=250 -> right ts=200 (bid=200)
        assert_eq!(all[1][4], Value::F64(200.0));
        // ts=400 -> right ts=300 (bid=300)
        assert_eq!(all[2][4], Value::F64(300.0));
    }
}
