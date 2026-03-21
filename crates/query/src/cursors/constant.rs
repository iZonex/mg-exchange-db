//! Constant cursor — adds constant value columns.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Appends one or more constant-value columns to each row from the source.
pub struct ConstantCursor {
    source: Box<dyn RecordCursor>,
    constants: Vec<(String, Value)>,
    schema: Vec<(String, ColumnType)>,
}

impl ConstantCursor {
    pub fn new(source: Box<dyn RecordCursor>, constants: Vec<(String, Value)>) -> Self {
        let mut schema = source.schema().to_vec();
        for (name, val) in &constants {
            let ct = match val {
                Value::I64(_) => ColumnType::I64,
                Value::F64(_) => ColumnType::F64,
                Value::Str(_) => ColumnType::Varchar,
                Value::Timestamp(_) => ColumnType::Timestamp,
                Value::Null => ColumnType::I64,
            };
            schema.push((name.clone(), ct));
        }
        Self { source, constants, schema }
    }
}

impl RecordCursor for ConstantCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                let ncols = b.columns.len();
                for r in 0..b.row_count() {
                    let mut row: Vec<Value> = (0..ncols).map(|c| b.get_value(r, c)).collect();
                    for (_, val) in &self.constants {
                        row.push(val.clone());
                    }
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
    fn adds_constant_columns() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(1)], vec![Value::I64(2)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = ConstantCursor::new(Box::new(source), vec![
            ("source".into(), Value::Str("test".into())),
            ("version".into(), Value::I64(42)),
        ]);
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.get_value(0, 1), Value::Str("test".into()));
        assert_eq!(batch.get_value(1, 2), Value::I64(42));
    }
}
