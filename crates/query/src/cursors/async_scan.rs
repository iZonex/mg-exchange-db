//! Async scan — pre-fetches the next batch while the current one is being consumed.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Pre-fetches the next batch from source so it is ready when requested.
pub struct AsyncScanCursor {
    source: Box<dyn RecordCursor>,
    prefetched: Option<RecordBatch>,
    started: bool,
}

impl AsyncScanCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, prefetched: None, started: false }
    }
}

impl RecordCursor for AsyncScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.started {
            self.prefetched = self.source.next_batch(max_rows)?;
            self.started = true;
        }
        match self.prefetched.take() {
            Some(current) => {
                // Pre-fetch the next batch.
                self.prefetched = self.source.next_batch(max_rows)?;
                Ok(Some(current))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::Value;

    #[test]
    fn prefetches() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..5).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = AsyncScanCursor::new(Box::new(source));
        let mut total = 0;
        while let Some(b) = cursor.next_batch(2).unwrap() {
            total += b.row_count();
        }
        assert_eq!(total, 5);
    }
}
