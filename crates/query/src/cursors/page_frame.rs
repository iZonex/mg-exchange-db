//! Page frame cursor — reads data in fixed-size page frames.
//!
//! Buffers source rows and emits them in aligned page-sized chunks,
//! ensuring downstream consumers receive uniformly sized batches.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits rows from a source cursor in fixed-size page frames.
///
/// This is useful for vectorized processing where downstream operators
/// expect uniform batch sizes. The page size determines how many rows
/// are buffered before emitting.
pub struct PageFrameCursor {
    source: Box<dyn RecordCursor>,
    page_size: usize,
    buffer: Vec<Vec<Value>>,
    source_exhausted: bool,
    schema: Vec<(String, ColumnType)>,
}

impl PageFrameCursor {
    pub fn new(source: Box<dyn RecordCursor>, page_size: usize) -> Self {
        let schema = source.schema().to_vec();
        Self {
            source,
            page_size: page_size.max(1),
            buffer: Vec::new(),
            source_exhausted: false,
            schema,
        }
    }
}

impl RecordCursor for PageFrameCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        // Fill buffer to page_size.
        while self.buffer.len() < self.page_size && !self.source_exhausted {
            match self.source.next_batch(self.page_size)? {
                None => {
                    self.source_exhausted = true;
                    break;
                }
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols).map(|c| batch.get_value(r, c)).collect();
                        self.buffer.push(row);
                    }
                }
            }
        }

        if self.buffer.is_empty() {
            return Ok(None);
        }

        let emit_count = self.buffer.len().min(self.page_size);
        let mut result = RecordBatch::new(self.schema.clone());
        for row in self.buffer.drain(..emit_count) {
            result.append_row(&row);
        }

        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn page_frame_fixed_size() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..7).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = PageFrameCursor::new(Box::new(source), 3);

        let b1 = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(b1.row_count(), 3);

        let b2 = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(b2.row_count(), 3);

        // Last page has only 1 row.
        let b3 = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(b3.row_count(), 1);

        assert!(cursor.next_batch(100).unwrap().is_none());
    }
}
