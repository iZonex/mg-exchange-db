//! Rename cursor — renames columns (for AS aliases).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Renames columns in the output schema without modifying data.
pub struct RenameCursor {
    source: Box<dyn RecordCursor>,
    schema: Vec<(String, ColumnType)>,
}

impl RenameCursor {
    /// `renames` maps old column name to new column name.
    pub fn new(source: Box<dyn RecordCursor>, renames: &[(String, String)]) -> Self {
        let schema: Vec<(String, ColumnType)> = source
            .schema()
            .iter()
            .map(|(name, ct)| {
                let new_name = renames
                    .iter()
                    .find(|(old, _)| old == name)
                    .map(|(_, new)| new.clone())
                    .unwrap_or_else(|| name.clone());
                (new_name, *ct)
            })
            .collect();
        Self { source, schema }
    }
}

impl RecordCursor for RenameCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let row: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                    result.append_row(&row);
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

    #[test]
    fn renames_columns() {
        let schema = vec![("old_name".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(42)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = RenameCursor::new(Box::new(source), &[("old_name".into(), "new_name".into())]);
        assert_eq!(cursor.schema()[0].0, "new_name");
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 0), Value::I64(42));
    }
}
