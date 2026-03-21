//! Empty cursor — always returns no rows.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// A cursor that yields zero rows.
pub struct EmptyCursor {
    schema: Vec<(String, ColumnType)>,
}

impl EmptyCursor {
    pub fn new(schema: Vec<(String, ColumnType)>) -> Self {
        Self { schema }
    }
}

impl RecordCursor for EmptyCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        Ok(None)
    }

    fn estimated_rows(&self) -> Option<u64> {
        Some(0)
    }
}
