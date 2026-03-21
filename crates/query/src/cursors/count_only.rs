//! CountOnly cursor — optimized `SELECT count(*) FROM t`.
//!
//! Counts rows from the source without materializing column data,
//! then emits a single row containing the count.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Counts rows from the source cursor and emits a single `count(*)` result.
///
/// This avoids allocating per-row data; it only tracks the running total.
pub struct CountOnlyCursor {
    source: Option<Box<dyn RecordCursor>>,
    done: bool,
    schema: Vec<(String, ColumnType)>,
}

impl CountOnlyCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self {
            source: Some(source),
            done: false,
            schema: vec![("count".to_string(), ColumnType::I64)],
        }
    }
}

impl RecordCursor for CountOnlyCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut source = self.source.take().expect("source already consumed");
        let mut total: i64 = 0;

        loop {
            match source.next_batch(4096)? {
                None => break,
                Some(batch) => {
                    total += batch.row_count() as i64;
                }
            }
        }

        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(total)]);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn count_rows() {
        let schema = vec![("x".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(3)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = CountOnlyCursor::new(Box::new(source));

        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 1);
        assert_eq!(batch.get_value(0, 0), Value::I64(3));

        // Second call returns None.
        assert!(cursor.next_batch(100).unwrap().is_none());
    }

    #[test]
    fn count_empty() {
        let schema = vec![("x".to_string(), ColumnType::I64)];
        let source = MemoryCursor::from_rows(schema, &[]);
        let mut cursor = CountOnlyCursor::new(Box::new(source));

        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 0), Value::I64(0));
    }
}
