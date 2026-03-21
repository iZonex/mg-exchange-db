//! Distinct cursor — deduplicates rows using a hash set.

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Deduplicates rows from a source cursor using `HashSet<serialized_row>`.
pub struct DistinctCursor {
    source: Box<dyn RecordCursor>,
    seen: HashSet<Vec<u8>>,
}

impl DistinctCursor {
    pub fn new(source: Box<dyn RecordCursor>) -> Self {
        Self {
            source,
            seen: HashSet::new(),
        }
    }

    fn row_key(row: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();
        for val in row {
            match val {
                Value::Null => buf.push(0),
                Value::I64(n) => {
                    buf.push(1);
                    buf.extend_from_slice(&n.to_le_bytes());
                }
                Value::F64(n) => {
                    buf.push(2);
                    buf.extend_from_slice(&n.to_bits().to_le_bytes());
                }
                Value::Str(s) => {
                    buf.push(3);
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                Value::Timestamp(n) => {
                    buf.push(4);
                    buf.extend_from_slice(&n.to_le_bytes());
                }
            }
        }
        buf
    }
}

impl RecordCursor for DistinctCursor {
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
                        let row: Vec<Value> =
                            (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        let key = Self::row_key(&row);
                        if self.seen.insert(key) {
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
    fn deduplicates_rows() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(1)],
            vec![Value::I64(3)],
            vec![Value::I64(2)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = DistinctCursor::new(Box::new(source));

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(all, vec![Value::I64(1), Value::I64(2), Value::I64(3)]);
    }
}
