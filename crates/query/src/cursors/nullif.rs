//! NullIf cursor — applies NULLIF transformation (returns NULL if value equals a sentinel).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Replaces values in a column with NULL if they equal the sentinel.
pub struct NullIfCursor {
    source: Box<dyn RecordCursor>,
    col_idx: usize,
    sentinel: Value,
}

impl NullIfCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_idx: usize, sentinel: Value) -> Self {
        Self {
            source,
            col_idx,
            sentinel,
        }
    }
}

impl RecordCursor for NullIfCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
                let mut result = RecordBatch::new(schema);
                for r in 0..b.row_count() {
                    let row: Vec<Value> = (0..b.columns.len())
                        .map(|c| {
                            let v = b.get_value(r, c);
                            if c == self.col_idx && v.eq_coerce(&self.sentinel) {
                                Value::Null
                            } else {
                                v
                            }
                        })
                        .collect();
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
    fn nullif_replaces_sentinel() {
        let schema = vec![("v".to_string(), ColumnType::F64)];
        let rows = vec![
            vec![Value::F64(0.0)],
            vec![Value::F64(42.0)],
            vec![Value::F64(0.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = NullIfCursor::new(Box::new(source), 0, Value::F64(0.0));
        let batch = cursor.next_batch(10).unwrap().unwrap();
        // 0.0 -> Null (stored as NaN in F64 column, which reads back as Null)
        assert_eq!(batch.get_value(0, 0), Value::Null);
        assert_eq!(batch.get_value(1, 0), Value::F64(42.0));
    }
}
