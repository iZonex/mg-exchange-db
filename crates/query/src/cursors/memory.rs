//! In-memory cursor from pre-computed data.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Cursor that streams rows from an in-memory `RecordBatch`.
pub struct MemoryCursor {
    batch: RecordBatch,
    offset: usize,
}

impl MemoryCursor {
    /// Create a cursor from a `RecordBatch`.
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch, offset: 0 }
    }

    /// Create a cursor from row-major data.
    pub fn from_rows(schema: Vec<(String, ColumnType)>, rows: &[Vec<Value>]) -> Self {
        let mut batch = RecordBatch::new(schema);
        for row in rows {
            batch.append_row(row);
        }
        Self { batch, offset: 0 }
    }
}

impl RecordCursor for MemoryCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.batch.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.offset >= self.batch.row_count() {
            return Ok(None);
        }

        let remaining = self.batch.row_count() - self.offset;
        let n = remaining.min(max_rows);
        let result = self.batch.slice(self.offset, n);
        self.offset += n;
        Ok(Some(result))
    }

    fn estimated_rows(&self) -> Option<u64> {
        Some(self.batch.row_count() as u64)
    }
}
