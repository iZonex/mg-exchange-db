//! Incremental aggregate — maintains running state, emits cumulative results per batch.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits running sum and count after each row (cumulative aggregate).
pub struct IncrementalAggregateCursor {
    source: Box<dyn RecordCursor>,
    agg_col: usize,
    schema: Vec<(String, ColumnType)>,
    running_sum: f64,
    running_count: i64,
}

impl IncrementalAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, agg_col: usize) -> Self {
        let mut schema = source.schema().to_vec();
        schema.push(("running_sum".to_string(), ColumnType::F64));
        schema.push(("running_count".to_string(), ColumnType::I64));
        Self { source, agg_col, schema, running_sum: 0.0, running_count: 0 }
    }
}

impl RecordCursor for IncrementalAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                let ncols = b.columns.len();
                for r in 0..b.row_count() {
                    let v = match b.get_value(r, self.agg_col) {
                        Value::I64(n) => n as f64,
                        Value::F64(n) => n,
                        _ => 0.0,
                    };
                    self.running_sum += v;
                    self.running_count += 1;
                    let mut row: Vec<Value> = (0..ncols).map(|c| b.get_value(r, c)).collect();
                    row.push(Value::F64(self.running_sum));
                    row.push(Value::I64(self.running_count));
                    result.append_row(&row);
                }
                Ok(Some(result))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn incremental_running_sum() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(10)], vec![Value::I64(20)], vec![Value::I64(30)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = IncrementalAggregateCursor::new(Box::new(source), 0);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.get_value(2, 1), Value::F64(60.0)); // running sum
        assert_eq!(batch.get_value(2, 2), Value::I64(3));    // running count
    }
}
