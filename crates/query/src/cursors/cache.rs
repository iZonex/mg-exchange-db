//! Cached cursor — materializes source once, allows multiple iteration passes.
//!
//! Useful for window functions and self-joins that need to read the
//! same data multiple times.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Materializes a source cursor on first iteration, then replays
/// the cached data on subsequent passes.
pub struct CachedCursor {
    source: Option<Box<dyn RecordCursor>>,
    cache: Vec<Vec<Value>>,
    offset: usize,
    materialized: bool,
    schema: Vec<(String, ColumnType)>,
}

impl CachedCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        let schema = source.schema().to_vec();
        Self {
            source: Some(source),
            cache: Vec::new(),
            offset: 0,
            materialized: false,
            schema,
        }
    }

    /// Reset the cursor position to the beginning for a new pass.
    pub fn rewind(&mut self) {
        self.offset = 0;
    }

    /// Return the number of cached rows.
    pub fn cached_row_count(&self) -> usize {
        self.cache.len()
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");
        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols)
                            .map(|c| batch.get_value(r, c))
                            .collect();
                        self.cache.push(row);
                    }
                }
            }
        }
        self.materialized = true;
        Ok(())
    }
}

impl RecordCursor for CachedCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.materialized {
            self.materialize()?;
        }

        if self.offset >= self.cache.len() {
            return Ok(None);
        }

        let remaining = self.cache.len() - self.offset;
        let n = remaining.min(max_rows);
        let mut result = RecordBatch::new(self.schema.clone());
        for row in &self.cache[self.offset..self.offset + n] {
            result.append_row(row);
        }
        self.offset += n;
        Ok(Some(result))
    }

    fn estimated_rows(&self) -> Option<u64> {
        if self.materialized {
            Some(self.cache.len() as u64)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn cache_and_rewind() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)], vec![Value::I64(3)]];
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = CachedCursor::new(Box::new(source));

        // First pass.
        let mut pass1 = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                pass1.push(batch.get_value(r, 0));
            }
        }

        // Rewind and second pass.
        cursor.rewind();
        let mut pass2 = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                pass2.push(batch.get_value(r, 0));
            }
        }

        assert_eq!(pass1, pass2);
        assert_eq!(pass1, vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
        assert_eq!(cursor.cached_row_count(), 3);
    }
}
