//! Rate limit cursor — limits output to at most N rows per call.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Limits the number of rows emitted per `next_batch` call, regardless of `max_rows`.
pub struct RateLimitCursor {
    source: Box<dyn RecordCursor>,
    max_per_batch: usize,
    leftover: Option<RecordBatch>,
    leftover_offset: usize,
}

impl RateLimitCursor {
    pub fn new(source: Box<dyn RecordCursor>, max_per_batch: usize) -> Self {
        Self { source, max_per_batch: max_per_batch.max(1), leftover: None, leftover_offset: 0 }
    }
}

impl RecordCursor for RateLimitCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        // Check leftover first.
        if let Some(lo) = &self.leftover {
            if self.leftover_offset < lo.row_count() {
                let n = self.max_per_batch.min(lo.row_count() - self.leftover_offset);
                let batch = lo.slice(self.leftover_offset, n);
                self.leftover_offset += n;
                if self.leftover_offset >= lo.row_count() {
                    self.leftover = None;
                }
                return Ok(Some(batch));
            }
            self.leftover = None;
        }

        match self.source.next_batch(self.max_per_batch * 4)? {
            None => Ok(None),
            Some(b) => {
                if b.row_count() <= self.max_per_batch {
                    Ok(Some(b))
                } else {
                    let result = b.slice(0, self.max_per_batch);
                    self.leftover = Some(b);
                    self.leftover_offset = self.max_per_batch;
                    Ok(Some(result))
                }
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
    fn limits_batch_size() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = RateLimitCursor::new(Box::new(source), 3);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert!(batch.row_count() <= 3);
        let mut total = batch.row_count();
        while let Some(b) = cursor.next_batch(100).unwrap() {
            assert!(b.row_count() <= 3);
            total += b.row_count();
        }
        assert_eq!(total, 10);
    }
}
