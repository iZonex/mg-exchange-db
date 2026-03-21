//! Type cast cursor — casts column types.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Casts one column from its source type to a target type.
pub struct TypeCastCursor {
    source: Box<dyn RecordCursor>,
    col_idx: usize,
    target_type: ColumnType,
    schema: Vec<(String, ColumnType)>,
}

impl TypeCastCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_idx: usize, target_type: ColumnType) -> Self {
        let mut schema = source.schema().to_vec();
        schema[col_idx].1 = target_type;
        Self { source, col_idx, target_type, schema }
    }

    fn cast_value(&self, v: Value) -> Value {
        match self.target_type {
            ColumnType::I64 | ColumnType::I32 | ColumnType::I16 | ColumnType::I8 => match v {
                Value::I64(n) => Value::I64(n),
                Value::F64(n) => Value::I64(n as i64),
                Value::Str(s) => Value::I64(s.parse().unwrap_or(0)),
                Value::Timestamp(n) => Value::I64(n),
                Value::Null => Value::Null,
            },
            ColumnType::F64 | ColumnType::F32 => match v {
                Value::I64(n) => Value::F64(n as f64),
                Value::F64(n) => Value::F64(n),
                Value::Str(s) => Value::F64(s.parse().unwrap_or(0.0)),
                Value::Timestamp(n) => Value::F64(n as f64),
                Value::Null => Value::Null,
            },
            ColumnType::Varchar => Value::Str(format!("{v}")),
            _ => v,
        }
    }
}

impl RecordCursor for TypeCastCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let row: Vec<Value> = (0..b.columns.len())
                        .map(|c| {
                            let v = b.get_value(r, c);
                            if c == self.col_idx { self.cast_value(v) } else { v }
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
    fn casts_i64_to_f64() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(42)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = TypeCastCursor::new(Box::new(source), 0, ColumnType::F64);
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 0), Value::F64(42.0));
    }
}
