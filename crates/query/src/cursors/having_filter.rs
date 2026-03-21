//! Having filter — applies a predicate to aggregated results.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Filters aggregated rows by comparing a column against a threshold.
pub struct HavingFilterCursor {
    source: Box<dyn RecordCursor>,
    col_idx: usize,
    min_value: Value,
}

impl HavingFilterCursor {
    /// Only emits rows where `col_idx >= min_value`.
    pub fn new(source: Box<dyn RecordCursor>, col_idx: usize, min_value: Value) -> Self {
        Self {
            source,
            col_idx,
            min_value,
        }
    }
}

impl RecordCursor for HavingFilterCursor {
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
                        if matches!(
                            v.cmp_coerce(&self.min_value),
                            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                        ) {
                            let row: Vec<Value> =
                                (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                            result.append_row(&row);
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
    fn having_filters_aggregates() {
        let schema = vec![
            ("group".to_string(), ColumnType::Varchar),
            ("cnt".to_string(), ColumnType::I64),
        ];
        let rows = vec![
            vec![Value::Str("a".into()), Value::I64(5)],
            vec![Value::Str("b".into()), Value::I64(15)],
            vec![Value::Str("c".into()), Value::I64(3)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = HavingFilterCursor::new(Box::new(source), 1, Value::I64(10));
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 1);
    }
}
