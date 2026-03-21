//! Timeout cursor — cancels query after a deadline.

use std::time::{Duration, Instant};

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Wraps a source cursor and returns None (stops) after the deadline has passed.
pub struct TimeoutCursor {
    source: Box<dyn RecordCursor>,
    deadline: Instant,
}

impl TimeoutCursor {
    pub fn new(source: Box<dyn RecordCursor>, timeout: Duration) -> Self {
        Self {
            source,
            deadline: Instant::now() + timeout,
        }
    }
}

impl RecordCursor for TimeoutCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if Instant::now() >= self.deadline {
            return Ok(None);
        }
        self.source.next_batch(max_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::Value;

    #[test]
    fn respects_timeout() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        // Long timeout — should work normally.
        let mut cursor = TimeoutCursor::new(Box::new(source), Duration::from_secs(60));
        assert!(cursor.next_batch(10).unwrap().is_some());
        assert!(cursor.next_batch(10).unwrap().is_none());
    }

    #[test]
    fn expires_immediately() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        // Zero timeout — should expire immediately.
        let mut cursor = TimeoutCursor::new(Box::new(source), Duration::from_nanos(0));
        // Small race window, but practically always expires.
        std::thread::sleep(Duration::from_millis(1));
        assert!(cursor.next_batch(10).unwrap().is_none());
    }
}
