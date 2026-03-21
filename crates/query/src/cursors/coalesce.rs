//! Coalesce cursor — applies COALESCE across columns, returning first non-null.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Appends a column that is the COALESCE of the specified source columns.
#[allow(dead_code)]
pub struct CoalesceCursor {
    source: Box<dyn RecordCursor>,
    col_indices: Vec<usize>,
    output_name: String,
    schema: Vec<(String, ColumnType)>,
}

impl CoalesceCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_indices: Vec<usize>, output_name: &str) -> Self {
        let mut schema = source.schema().to_vec();
        let ct = col_indices.first()
            .and_then(|&i| source.schema().get(i))
            .map(|(_, ct)| *ct)
            .unwrap_or(ColumnType::Varchar);
        schema.push((output_name.to_string(), ct));
        Self { source, col_indices, output_name: output_name.to_string(), schema }
    }
}

impl RecordCursor for CoalesceCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                let ncols = b.columns.len();
                for r in 0..b.row_count() {
                    let mut row: Vec<Value> = (0..ncols).map(|c| b.get_value(r, c)).collect();
                    let coalesced = self.col_indices.iter()
                        .map(|&i| b.get_value(r, i))
                        .find(|v| *v != Value::Null)
                        .unwrap_or(Value::Null);
                    row.push(coalesced);
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
    fn coalesce_first_non_null() {
        // Note: Null pushed to I64 column becomes 0, so we use F64 to test NaN->Null.
        let schema_f = vec![
            ("a".to_string(), ColumnType::F64),
            ("b".to_string(), ColumnType::F64),
        ];
        let rows = vec![
            vec![Value::Null, Value::F64(10.0)],
            vec![Value::F64(5.0), Value::F64(20.0)],
        ];
        let source = MemoryCursor::from_rows(schema_f, &rows);
        let mut cursor = CoalesceCursor::new(Box::new(source), vec![0, 1], "result");
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 2), Value::F64(10.0));
        assert_eq!(batch.get_value(1, 2), Value::F64(5.0));
    }
}
