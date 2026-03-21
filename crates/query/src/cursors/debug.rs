//! Debug cursor — logs every batch for debugging purposes.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Passes through all batches unchanged but records them for debug inspection.
pub struct DebugCursor {
    source: Box<dyn RecordCursor>,
    label: String,
    batch_count: usize,
    row_count: usize,
    /// Stores a summary of each batch for later inspection.
    log: Vec<String>,
}

impl DebugCursor {
    pub fn new(source: Box<dyn RecordCursor>, label: &str) -> Self {
        Self { source, label: label.to_string(), batch_count: 0, row_count: 0, log: Vec::new() }
    }

    /// Returns all logged messages.
    pub fn log(&self) -> &[String] { &self.log }
}

impl RecordCursor for DebugCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => {
                self.log.push(format!("[{}] exhausted after {} batches, {} rows", self.label, self.batch_count, self.row_count));
                Ok(None)
            }
            Some(b) => {
                self.batch_count += 1;
                self.row_count += b.row_count();
                self.log.push(format!("[{}] batch #{}: {} rows", self.label, self.batch_count, b.row_count()));
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
    fn logs_batches() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = DebugCursor::new(Box::new(source), "test");
        cursor.next_batch(10).unwrap();
        cursor.next_batch(10).unwrap(); // exhausted
        assert_eq!(cursor.log().len(), 2);
        assert!(cursor.log()[0].contains("batch #1"));
        assert!(cursor.log()[1].contains("exhausted"));
    }
}
