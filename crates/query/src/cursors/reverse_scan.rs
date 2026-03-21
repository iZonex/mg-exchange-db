//! Reverse scan cursor — emits rows in reverse order (newest first).
//!
//! Useful for `ORDER BY timestamp DESC LIMIT N` queries: materializes the
//! source and then streams rows from the end.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Streams rows from a source cursor in reverse order.
///
/// Materializes the entire source, then yields rows from last to first.
pub struct ReverseScanCursor {
    source: Option<Box<dyn RecordCursor>>,
    rows: Vec<Vec<Value>>,
    /// Position counting backward (next row to emit = rows.len() - 1 - pos).
    pos: usize,
    schema: Vec<(String, ColumnType)>,
}

impl ReverseScanCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = source.schema().to_vec();
        Self {
            source: Some(source),
            rows: Vec::new(),
            pos: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");
        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols).map(|c| batch.get_value(r, c)).collect();
                        self.rows.push(row);
                    }
                }
            }
        }
        Ok(())
    }
}

impl RecordCursor for ReverseScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.source.is_some() {
            self.materialize()?;
        }

        if self.pos >= self.rows.len() {
            return Ok(None);
        }

        let mut result = RecordBatch::new(self.schema.clone());
        let remaining = self.rows.len() - self.pos;
        let n = remaining.min(max_rows);

        for i in 0..n {
            let idx = self.rows.len() - 1 - self.pos - i;
            result.append_row(&self.rows[idx]);
        }
        self.pos += n;

        if result.row_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn reverse_order() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(3)],
            vec![Value::I64(4)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = ReverseScanCursor::new(Box::new(source));

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(
            all,
            vec![Value::I64(4), Value::I64(3), Value::I64(2), Value::I64(1)]
        );
    }
}
