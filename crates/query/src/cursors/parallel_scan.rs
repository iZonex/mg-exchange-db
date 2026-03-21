//! Parallel scan — wraps multiple scan cursors, round-robins through them.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Merges output from multiple child cursors by pulling from each in turn.
pub struct ParallelScanCursor {
    children: Vec<Box<dyn RecordCursor>>,
    current: usize,
    schema: Vec<(String, ColumnType)>,
    exhausted: Vec<bool>,
}

impl ParallelScanCursor {
    pub fn new(children: Vec<Box<dyn RecordCursor>>) -> Self {
        assert!(!children.is_empty(), "ParallelScanCursor needs at least one child");
        let schema = children[0].schema().to_vec();
        let n = children.len();
        Self { children, current: 0, schema, exhausted: vec![false; n] }
    }
}

impl RecordCursor for ParallelScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let n = self.children.len();
        for _ in 0..n {
            if self.exhausted[self.current] {
                self.current = (self.current + 1) % n;
                continue;
            }
            match self.children[self.current].next_batch(max_rows)? {
                Some(batch) => {
                    self.current = (self.current + 1) % n;
                    return Ok(Some(batch));
                }
                None => {
                    self.exhausted[self.current] = true;
                    self.current = (self.current + 1) % n;
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::Value;

    #[test]
    fn merges_children() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let c1 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let c2 = MemoryCursor::from_rows(schema, &[vec![Value::I64(3)]]);
        let mut cursor = ParallelScanCursor::new(vec![Box::new(c1), Box::new(c2)]);
        let mut total = 0;
        while let Some(b) = cursor.next_batch(1).unwrap() {
            total += b.row_count();
        }
        assert_eq!(total, 3);
    }
}
