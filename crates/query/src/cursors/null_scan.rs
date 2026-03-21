//! Null scan — returns N rows of NULLs (for OUTER JOIN padding).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Produces `total` rows where every column is NULL.
pub struct NullScanCursor {
    schema: Vec<(String, ColumnType)>,
    total: usize,
    emitted: usize,
}

impl NullScanCursor {
    pub fn new(schema: Vec<(String, ColumnType)>, total: usize) -> Self {
        Self { schema, total, emitted: 0 }
    }
}

impl RecordCursor for NullScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.emitted >= self.total {
            return Ok(None);
        }
        let n = max_rows.min(self.total - self.emitted);
        let mut result = RecordBatch::new(self.schema.clone());
        let null_row: Vec<Value> = vec![Value::Null; self.schema.len()];
        for _ in 0..n {
            result.append_row(&null_row);
        }
        self.emitted += n;
        Ok(Some(result))
    }

    fn estimated_rows(&self) -> Option<u64> {
        Some(self.total as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produces_null_rows() {
        let schema = vec![("a".to_string(), ColumnType::I64), ("b".to_string(), ColumnType::Varchar)];
        let mut cursor = NullScanCursor::new(schema, 5);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 5);
        // All values pushed as defaults (0 for I64, "" for Str due to Null coercion)
        assert!(cursor.next_batch(100).unwrap().is_none());
    }
}
