//! Values cursor — returns constant rows from a VALUES clause.
//!
//! Produces rows directly from in-memory data without any disk I/O,
//! supporting `INSERT ... VALUES (...)` and `SELECT * FROM (VALUES ...)`.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Cursor that yields constant rows from a VALUES clause.
///
/// All rows are provided at construction time and streamed out in batches.
pub struct ValuesCursor {
    rows: Vec<Vec<Value>>,
    offset: usize,
    schema: Vec<(String, ColumnType)>,
}

impl ValuesCursor {
    /// Create a values cursor with explicit schema and rows.
    pub fn new(schema: Vec<(String, ColumnType)>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            rows,
            offset: 0,
            schema,
        }
    }

    /// Create a values cursor with auto-generated column names.
    ///
    /// Column types are inferred from the first row.
    pub fn from_rows(rows: Vec<Vec<Value>>) -> Self {
        let ncols = rows.first().map(|r| r.len()).unwrap_or(0);
        let mut types = vec![ColumnType::I64; ncols];
        if let Some(row) = rows.first() {
            for (i, val) in row.iter().enumerate() {
                types[i] = match val {
                    Value::I64(_) => ColumnType::I64,
                    Value::F64(_) => ColumnType::F64,
                    Value::Str(_) => ColumnType::Varchar,
                    Value::Timestamp(_) => ColumnType::Timestamp,
                    Value::Null => ColumnType::I64,
                };
            }
        }

        let schema: Vec<(String, ColumnType)> = (0..ncols)
            .map(|i| (format!("column{}", i + 1), types[i]))
            .collect();

        Self {
            rows,
            offset: 0,
            schema,
        }
    }
}

impl RecordCursor for ValuesCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.offset >= self.rows.len() {
            return Ok(None);
        }

        let remaining = self.rows.len() - self.offset;
        let n = remaining.min(max_rows);
        let mut result = RecordBatch::new(self.schema.clone());

        for row in &self.rows[self.offset..self.offset + n] {
            result.append_row(row);
        }
        self.offset += n;

        Ok(Some(result))
    }

    fn estimated_rows(&self) -> Option<u64> {
        Some(self.rows.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_explicit_schema() {
        let schema = vec![
            ("id".to_string(), ColumnType::I64),
            ("name".to_string(), ColumnType::Varchar),
        ];
        let rows = vec![
            vec![Value::I64(1), Value::Str("Alice".into())],
            vec![Value::I64(2), Value::Str("Bob".into())],
        ];
        let mut cursor = ValuesCursor::new(schema, rows);

        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.get_value(0, 0), Value::I64(1));
        assert_eq!(batch.get_value(1, 1), Value::Str("Bob".into()));

        assert!(cursor.next_batch(100).unwrap().is_none());
    }

    #[test]
    fn values_auto_schema() {
        let rows = vec![
            vec![Value::I64(10), Value::F64(1.5)],
            vec![Value::I64(20), Value::F64(2.5)],
        ];
        let mut cursor = ValuesCursor::from_rows(rows);

        assert_eq!(cursor.schema()[0].0, "column1");
        assert_eq!(cursor.schema()[1].0, "column2");
        assert_eq!(cursor.schema()[0].1, ColumnType::I64);
        assert_eq!(cursor.schema()[1].1, ColumnType::F64);

        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
    }
}
