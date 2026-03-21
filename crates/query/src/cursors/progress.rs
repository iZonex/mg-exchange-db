//! Progress cursor — tracks rows processed and provides progress info.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Wraps a source cursor and tracks how many rows have been processed.
pub struct ProgressCursor {
    source: Box<dyn RecordCursor>,
    rows_processed: u64,
    batches_processed: u64,
}

impl ProgressCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, rows_processed: 0, batches_processed: 0 }
    }

    /// How many rows have been processed so far.
    pub fn rows_processed(&self) -> u64 { self.rows_processed }

    /// How many batches have been produced.
    pub fn batches_processed(&self) -> u64 { self.batches_processed }
}

impl RecordCursor for ProgressCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                self.rows_processed += b.row_count() as u64;
                self.batches_processed += 1;
                Ok(Some(b))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::Value;

    #[test]
    fn tracks_progress() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..5).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = ProgressCursor::new(Box::new(source));
        cursor.next_batch(3).unwrap();
        assert_eq!(cursor.rows_processed(), 3);
        assert_eq!(cursor.batches_processed(), 1);
        cursor.next_batch(3).unwrap();
        assert_eq!(cursor.rows_processed(), 5);
    }
}
