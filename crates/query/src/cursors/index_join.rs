//! Index join — uses a pre-built index (HashMap) on the right side for O(1) probing.

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Uses a pre-built index on the right side's key column for efficient probing.
pub struct IndexJoinCursor {
    left: Box<dyn RecordCursor>,
    index: HashMap<i64, Vec<Vec<Value>>>,
    left_key_col: usize,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
}

impl IndexJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>,
        left_key_col: usize, right_key_col: usize,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend(right.schema().to_vec());
        Self {
            left, index: HashMap::new(), left_key_col, schema, built: false,
            right_source: Some(right), right_key_col,
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self.right_source.take().unwrap();
        while let Some(b) = right.next_batch(1024)? {
            for r in 0..b.row_count() {
                let row: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                let key = match &row[self.right_key_col] {
                    Value::I64(n) => *n,
                    Value::Timestamp(n) => *n,
                    _ => continue,
                };
                self.index.entry(key).or_default().push(row);
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for IndexJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        let mut result = RecordBatch::new(self.schema.clone());
        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let lrow: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        let key = match &lrow[self.left_key_col] {
                            Value::I64(n) => *n,
                            Value::Timestamp(n) => *n,
                            _ => continue,
                        };
                        if let Some(right_rows) = self.index.get(&key) {
                            for rrow in right_rows {
                                let mut combined = lrow.clone();
                                combined.extend(rrow.iter().cloned());
                                result.append_row(&combined);
                            }
                        }
                    }
                }
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn index_join_lookup() {
        let ls = vec![("id".to_string(), ColumnType::I64)];
        let rs = vec![("uid".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let right = MemoryCursor::from_rows(rs, &[
            vec![Value::I64(1), Value::I64(10)], vec![Value::I64(1), Value::I64(20)],
        ]);
        let mut cursor = IndexJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2); // id=1 matches twice, id=2 matches nothing
    }
}
