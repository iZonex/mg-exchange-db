//! Filtered scan — combined scan + filter in one step using a closure predicate.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// A scan cursor with an integrated filter predicate to avoid separate filter cursor overhead.
pub struct FilteredScanCursor {
    source: Box<dyn RecordCursor>,
    /// Column index and value to match.
    col_idx: usize,
    expected: Value,
}

impl FilteredScanCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_idx: usize, expected: Value) -> Self {
        Self {
            source,
            col_idx,
            expected,
        }
    }
}

impl RecordCursor for FilteredScanCursor {
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
                        let v = b.get_value(r, self.col_idx);
                        if v.eq_coerce(&self.expected) {
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
    fn filters_during_scan() {
        let schema = vec![
            ("k".to_string(), ColumnType::I64),
            ("v".to_string(), ColumnType::I64),
        ];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(2), Value::I64(20)],
            vec![Value::I64(1), Value::I64(30)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = FilteredScanCursor::new(Box::new(source), 0, Value::I64(1));
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
    }
}
