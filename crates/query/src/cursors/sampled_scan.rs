//! Sampled scan — reads every Nth row for approximate queries.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits every Nth row from the source, useful for approximate aggregations.
pub struct SampledScanCursor {
    source: Box<dyn RecordCursor>,
    step: usize,
    counter: usize,
}

impl SampledScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, step: usize) -> Self {
        Self { source, step: step.max(1), counter: 0 }
    }
}

impl RecordCursor for SampledScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows * self.step)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        if self.counter % self.step == 0 {
                            let row: Vec<Value> =
                                (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                            result.append_row(&row);
                            if result.row_count() >= max_rows { break; }
                        }
                        self.counter += 1;
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
    fn samples_every_nth() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = SampledScanCursor::new(Box::new(source), 3);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        // rows 0, 3, 6, 9
        assert_eq!(batch.row_count(), 4);
        assert_eq!(batch.get_value(0, 0), Value::I64(0));
        assert_eq!(batch.get_value(1, 0), Value::I64(3));
    }
}
