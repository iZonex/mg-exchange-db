//! Timestamp range scan — only reads rows within [lo, hi] on a timestamp column.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits only rows where `ts_col` value is in `[lo, hi]`.
pub struct TimestampRangeScanCursor {
    source: Box<dyn RecordCursor>,
    ts_col: usize,
    lo: i64,
    hi: i64,
}

impl TimestampRangeScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, ts_col: usize, lo: i64, hi: i64) -> Self {
        Self { source, ts_col, lo, hi }
    }
}

impl RecordCursor for TimestampRangeScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let ts = match b.get_value(r, self.ts_col) {
                            Value::Timestamp(t) => t,
                            Value::I64(t) => t,
                            _ => continue,
                        };
                        if ts >= self.lo && ts <= self.hi {
                            let row: Vec<Value> =
                                (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                            result.append_row(&row);
                            if result.row_count() >= max_rows { break; }
                        }
                    }
                }
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn filters_timestamp_range() {
        let schema = vec![("ts".to_string(), ColumnType::Timestamp), ("v".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::Timestamp(100), Value::I64(1)],
            vec![Value::Timestamp(200), Value::I64(2)],
            vec![Value::Timestamp(300), Value::I64(3)],
            vec![Value::Timestamp(400), Value::I64(4)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = TimestampRangeScanCursor::new(Box::new(source), 0, 150, 350);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
    }
}
