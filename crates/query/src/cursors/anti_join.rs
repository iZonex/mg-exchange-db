//! Anti join — returns left rows with NO match in right (for NOT EXISTS).

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Returns left rows whose join key does NOT exist in the right side.
pub struct AntiJoinCursor {
    left: Box<dyn RecordCursor>,
    right_keys: HashSet<Vec<u8>>,
    left_key_col: usize,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
}

impl AntiJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_key_col: usize,
        right_key_col: usize,
    ) -> Self {
        Self {
            left,
            right_keys: HashSet::new(),
            left_key_col,
            built: false,
            right_source: Some(right),
            right_key_col,
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self.right_source.take().unwrap();
        while let Some(b) = right.next_batch(1024)? {
            for r in 0..b.row_count() {
                let v = b.get_value(r, self.right_key_col);
                self.right_keys
                    .insert(crate::cursors::semi_join::serialize_value(&v));
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for AntiJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.left.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }
        let schema: Vec<(String, ColumnType)> = self.left.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let k = crate::cursors::semi_join::serialize_value(
                            &b.get_value(r, self.left_key_col),
                        );
                        if !self.right_keys.contains(&k) {
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
    fn anti_join_not_exists() {
        let ls = vec![("id".to_string(), ColumnType::I64)];
        let rs = vec![("uid".to_string(), ColumnType::I64)];
        let left = MemoryCursor::from_rows(
            ls,
            &[
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)],
            ],
        );
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1)], vec![Value::I64(3)]]);
        let mut cursor = AntiJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 1);
        assert_eq!(batch.get_value(0, 0), Value::I64(2));
    }
}
