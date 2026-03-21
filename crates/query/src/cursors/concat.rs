//! Concat cursor — concatenates N cursors sequentially.
//!
//! More general than `UnionCursor`: supports heterogeneous schemas by
//! projecting each source to a common output schema (if needed).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Concatenates an arbitrary number of cursors, streaming each to completion
/// before moving to the next. All cursors must share a compatible schema.
pub struct ConcatCursor {
    sources: Vec<Box<dyn RecordCursor>>,
    current: usize,
    schema: Vec<(String, ColumnType)>,
}

impl ConcatCursor {
    pub fn new(sources: Vec<Box<dyn RecordCursor>>) -> Self {
        let schema = if sources.is_empty() {
            Vec::new()
        } else {
            sources[0].schema().to_vec()
        };
        Self {
            sources,
            current: 0,
            schema,
        }
    }

    /// Create from exactly two cursors.
    pub fn pair(a: Box<dyn RecordCursor>, b: Box<dyn RecordCursor>) -> Self {
        Self::new(vec![a, b])
    }
}

impl RecordCursor for ConcatCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        while self.current < self.sources.len() {
            match self.sources[self.current].next_batch(max_rows)? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    self.current += 1;
                }
            }
        }
        Ok(None)
    }

    fn estimated_rows(&self) -> Option<u64> {
        let mut total = 0u64;
        for s in &self.sources {
            total += s.estimated_rows()?;
        }
        Some(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::Value;

    #[test]
    fn concat_three_sources() {
        let schema = vec![("v".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(1)]]);
        let s2 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(2)]]);
        let s3 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(3)]]);

        let mut cursor = ConcatCursor::new(vec![
            Box::new(s1),
            Box::new(s2),
            Box::new(s3),
        ]);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(all, vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    }

    #[test]
    fn concat_empty_sources() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let s1 = MemoryCursor::from_rows(schema.clone(), &[]);
        let s2 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(42)]]);

        let mut cursor = ConcatCursor::pair(Box::new(s1), Box::new(s2));

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(all, vec![Value::I64(42)]);
    }
}
