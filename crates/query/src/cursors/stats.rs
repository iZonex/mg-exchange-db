//! Stats cursor — collects execution statistics (rows, bytes, time).

use std::time::Instant;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Wraps a source cursor and collects execution statistics.
#[allow(dead_code)]
pub struct StatsCursor {
    source: Box<dyn RecordCursor>,
    total_rows: u64,
    total_batches: u64,
    start: Instant,
    elapsed_nanos: u128,
}

impl StatsCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, total_rows: 0, total_batches: 0, start: Instant::now(), elapsed_nanos: 0 }
    }

    pub fn total_rows(&self) -> u64 { self.total_rows }
    pub fn total_batches(&self) -> u64 { self.total_batches }
    pub fn elapsed_nanos(&self) -> u128 { self.elapsed_nanos }
}

impl RecordCursor for StatsCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let t0 = Instant::now();
        let result = self.source.next_batch(max_rows)?;
        self.elapsed_nanos += t0.elapsed().as_nanos();
        if let Some(ref b) = result {
            self.total_rows += b.row_count() as u64;
            self.total_batches += 1;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::Value;

    #[test]
    fn collects_stats() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = StatsCursor::new(Box::new(source));
        while cursor.next_batch(2).unwrap().is_some() {}
        assert_eq!(cursor.total_rows(), 3);
        assert!(cursor.total_batches() >= 1);
        assert!(cursor.elapsed_nanos() > 0);
    }
}
