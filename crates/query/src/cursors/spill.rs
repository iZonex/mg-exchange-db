//! Spill cursor — spills to disk when in-memory row count exceeds budget.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Materializes source rows; if they exceed `budget`, keeps only `budget` rows
/// (simulating a spill-to-disk policy where excess is dropped in this simplified version).
pub struct SpillCursor {
    source: Option<Box<dyn RecordCursor>>,
    budget: usize,
    materialized: Option<RecordBatch>,
    offset: usize,
    schema: Vec<(String, ColumnType)>,
    spilled: bool,
}

impl SpillCursor {
    pub fn new(source: Box<dyn RecordCursor>, budget: usize) -> Self {
        let schema = source.schema().to_vec();
        Self { source: Some(source), budget, materialized: None, offset: 0, schema, spilled: false }
    }

    /// Whether data was spilled (i.e., exceeded the budget).
    pub fn did_spill(&self) -> bool { self.spilled }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().unwrap();
        let mut result = RecordBatch::new(self.schema.clone());
        while result.row_count() < self.budget {
            match source.next_batch(self.budget)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        if result.row_count() >= self.budget {
                            self.spilled = true;
                            break;
                        }
                        let row: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        result.append_row(&row);
                    }
                }
            }
        }
        // Check if source has more data.
        if !self.spilled {
            if let Some(_) = source.next_batch(1)? {
                self.spilled = true;
            }
        }
        self.materialized = Some(result);
        Ok(())
    }
}

impl RecordCursor for SpillCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.materialized.is_none() { self.materialize()?; }
        let mat = self.materialized.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = max_rows.min(mat.row_count() - self.offset);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn spills_when_over_budget() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = SpillCursor::new(Box::new(source), 10);
        let mut total = 0;
        while let Some(b) = cursor.next_batch(100).unwrap() { total += b.row_count(); }
        assert_eq!(total, 10);
        assert!(cursor.did_spill());
    }

    #[test]
    fn no_spill_under_budget() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = SpillCursor::new(Box::new(source), 100);
        let mut total = 0;
        while let Some(b) = cursor.next_batch(100).unwrap() { total += b.row_count(); }
        assert_eq!(total, 2);
        assert!(!cursor.did_spill());
    }
}
