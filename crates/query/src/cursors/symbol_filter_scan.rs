//! Symbol-filter scan cursor — skips rows whose symbol ID is not in the allowed set.

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Scans source and only emits rows whose symbol column value is in the allowed set.
pub struct SymbolFilterScanCursor {
    source: Box<dyn RecordCursor>,
    symbol_col: usize,
    allowed: HashSet<i64>,
}

impl SymbolFilterScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, symbol_col: usize, allowed_ids: Vec<i64>) -> Self {
        Self {
            source,
            symbol_col,
            allowed: allowed_ids.into_iter().collect(),
        }
    }
}

impl RecordCursor for SymbolFilterScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        if let Value::I64(id) = b.get_value(r, self.symbol_col)
                            && self.allowed.contains(&id)
                        {
                            let row: Vec<Value> =
                                (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                            result.append_row(&row);
                            if result.row_count() >= max_rows {
                                break;
                            }
                        }
                    }
                }
            }
        }
        if result.row_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn filters_by_symbol_id() {
        let schema = vec![
            ("sym".to_string(), ColumnType::I64),
            ("val".to_string(), ColumnType::I64),
        ];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(2), Value::I64(20)],
            vec![Value::I64(1), Value::I64(30)],
            vec![Value::I64(3), Value::I64(40)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = SymbolFilterScanCursor::new(Box::new(source), 0, vec![1, 3]);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 3);
    }
}
