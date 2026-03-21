//! Buffer cursor — buffers N rows before emitting (for batch processing).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Accumulates rows until `buffer_size` is reached, then emits them as one batch.
pub struct BufferCursor {
    source: Box<dyn RecordCursor>,
    buffer_size: usize,
    schema: Vec<(String, ColumnType)>,
}

impl BufferCursor {
    pub fn new(source: Box<dyn RecordCursor>, buffer_size: usize) -> Self {
        let schema = source.schema().to_vec();
        Self { source, buffer_size: buffer_size.max(1), schema }
    }
}

impl RecordCursor for BufferCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut result = RecordBatch::new(self.schema.clone());

        while result.row_count() < self.buffer_size {
            match self.source.next_batch(self.buffer_size - result.row_count())? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let row: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        result.append_row(&row);
                        if result.row_count() >= self.buffer_size { break; }
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
    fn buffers_rows() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..7).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = BufferCursor::new(Box::new(source), 5);
        let batch1 = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch1.row_count(), 5); // buffered 5
        let batch2 = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch2.row_count(), 2); // remaining 2
        assert!(cursor.next_batch(100).unwrap().is_none());
    }
}
