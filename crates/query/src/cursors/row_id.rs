//! Row ID cursor — adds a sequential row ID column.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Prepends a monotonically increasing row ID column to each row.
pub struct RowIdCursor {
    source: Box<dyn RecordCursor>,
    schema: Vec<(String, ColumnType)>,
    next_id: i64,
}

impl RowIdCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let mut schema = vec![(col_name.to_string(), ColumnType::I64)];
        schema.extend(source.schema().to_vec());
        Self { source, schema, next_id: 1 }
    }
}

impl RecordCursor for RowIdCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                let ncols = b.columns.len();
                for r in 0..b.row_count() {
                    let mut row = vec![Value::I64(self.next_id)];
                    for c in 0..ncols {
                        row.push(b.get_value(r, c));
                    }
                    result.append_row(&row);
                    self.next_id += 1;
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
    fn adds_row_ids() {
        let schema = vec![("name".to_string(), ColumnType::Varchar)];
        let rows = vec![
            vec![Value::Str("a".into())],
            vec![Value::Str("b".into())],
            vec![Value::Str("c".into())],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = RowIdCursor::new(Box::new(source), "rowid");
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 0), Value::I64(1));
        assert_eq!(batch.get_value(2, 0), Value::I64(3));
        assert_eq!(batch.schema[0].0, "rowid");
    }
}
