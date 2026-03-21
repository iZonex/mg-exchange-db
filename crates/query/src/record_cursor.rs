//! Pull-based cursor trait for streaming query results.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;

/// Pull-based cursor for streaming query results.
///
/// Implementations produce `RecordBatch` values on demand, avoiding the
/// need to materialize an entire result set in memory.
pub trait RecordCursor: Send {
    /// Get the schema (column names and types).
    fn schema(&self) -> &[(String, ColumnType)];

    /// Get the next batch of rows. Returns `None` when exhausted.
    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>>;

    /// Get total row count if known (useful for EXPLAIN).
    fn estimated_rows(&self) -> Option<u64> {
        None
    }
}
