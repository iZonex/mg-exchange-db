//! Column-only scan — reads a single column from source (for SELECT DISTINCT col, COUNT(*)).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::record_cursor::RecordCursor;

/// Reads only a single column from the source, avoiding the cost of materializing all columns.
pub struct ColumnOnlyScanCursor {
    source: Box<dyn RecordCursor>,
    col_idx: usize,
    schema: Vec<(String, ColumnType)>,
}

impl ColumnOnlyScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let src_schema = source.schema();
        let col_idx = src_schema.iter().position(|(n, _)| n == col_name).unwrap_or(0);
        let schema = vec![src_schema[col_idx].clone()];
        Self { source, col_idx, schema }
    }
}

impl RecordCursor for ColumnOnlyScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    result.append_row(&[b.get_value(r, self.col_idx)]);
                }
                Ok(Some(result))
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
    fn reads_single_column() {
        let schema = vec![("a".to_string(), ColumnType::I64), ("b".to_string(), ColumnType::Varchar)];
        let rows = vec![
            vec![Value::I64(1), Value::Str("x".into())],
            vec![Value::I64(2), Value::Str("y".into())],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = ColumnOnlyScanCursor::new(Box::new(source), "b");
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.schema.len(), 1);
        assert_eq!(batch.schema[0].0, "b");
    }
}
