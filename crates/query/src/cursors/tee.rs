//! Tee cursor — duplicates output so it can be replayed (for multi-output plans).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Captures all batches from source, allowing multiple reads via `clone_reader`.
#[allow(dead_code)]
pub struct TeeCursor {
    source: Box<dyn RecordCursor>,
    captured: Vec<RecordBatch>,
    done: bool,
    replay_offset: usize,
}

impl TeeCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self { source, captured: Vec::new(), done: false, replay_offset: 0 }
    }

    /// Returns the captured batches so far (for the second consumer).
    pub fn captured_batches(&self) -> &[RecordBatch] {
        &self.captured
    }

    /// Create a MemoryCursor that replays all captured data.
    pub fn into_replay(self) -> crate::cursors::memory::MemoryCursor {
        let schema = self.source.schema().to_vec();
        let mut all_rows = Vec::new();
        for batch in &self.captured {
            for r in 0..batch.row_count() {
                all_rows.push((0..batch.columns.len()).map(|c| batch.get_value(r, c)).collect());
            }
        }
        crate::cursors::memory::MemoryCursor::from_rows(schema, &all_rows)
    }
}

impl RecordCursor for TeeCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.source.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        match self.source.next_batch(max_rows)? {
            None => { self.done = true; Ok(None) }
            Some(b) => {
                // Clone for capture: rebuild from rows.
                let schema = b.schema.clone();
                let mut copy = RecordBatch::new(schema);
                for r in 0..b.row_count() {
                    let row: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                    copy.append_row(&row);
                }
                self.captured.push(copy);
                Ok(Some(b))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn tee_captures_and_passes_through() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = TeeCursor::new(Box::new(source));
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(cursor.captured_batches().len(), 1);
        assert_eq!(cursor.captured_batches()[0].row_count(), 2);
    }
}
