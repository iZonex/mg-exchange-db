//! Parallel aggregate — fan-out to N partition aggregates, then merge partial results.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Pulls from multiple child aggregate cursors and merges their SUM/COUNT results.
pub struct ParallelAggregateCursor {
    children: Vec<Box<dyn RecordCursor>>,
    schema: Vec<(String, ColumnType)>,
    emitted: bool,
}

impl ParallelAggregateCursor {
    /// Each child is expected to produce a single row with (sum: F64, count: I64).
    pub fn new(children: Vec<Box<dyn RecordCursor>>) -> Self {
        let schema = vec![
            ("sum".to_string(), ColumnType::F64),
            ("count".to_string(), ColumnType::I64),
        ];
        Self {
            children,
            schema,
            emitted: false,
        }
    }
}

impl RecordCursor for ParallelAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        let mut total_sum = 0.0f64;
        let mut total_count = 0i64;

        for child in &mut self.children {
            while let Some(b) = child.next_batch(1024)? {
                for r in 0..b.row_count() {
                    if let Value::F64(s) = b.get_value(r, 0) {
                        total_sum += s;
                    }
                    if let Value::I64(c) = b.get_value(r, 1) {
                        total_count += c;
                    }
                }
            }
        }

        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::F64(total_sum), Value::I64(total_count)]);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn merges_partial_aggregates() {
        let schema = vec![
            ("sum".to_string(), ColumnType::F64),
            ("count".to_string(), ColumnType::I64),
        ];
        let c1 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::F64(100.0), Value::I64(5)]]);
        let c2 = MemoryCursor::from_rows(schema, &[vec![Value::F64(200.0), Value::I64(10)]]);
        let mut cursor = ParallelAggregateCursor::new(vec![Box::new(c1), Box::new(c2)]);
        let batch = cursor.next_batch(1).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 0), Value::F64(300.0));
        assert_eq!(batch.get_value(0, 1), Value::I64(15));
    }
}
