//! Mark join — adds a boolean column indicating whether a match was found in the right side.

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits all left rows with an appended boolean column: true if key exists in right.
pub struct MarkJoinCursor {
    left: Box<dyn RecordCursor>,
    right_keys: HashSet<Vec<u8>>,
    left_key_col: usize,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
    schema: Vec<(String, ColumnType)>,
}

impl MarkJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_key_col: usize,
        right_key_col: usize,
        mark_col_name: &str,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.push((mark_col_name.to_string(), ColumnType::Boolean));
        Self {
            left,
            right_keys: HashSet::new(),
            left_key_col,
            built: false,
            right_source: Some(right),
            right_key_col,
            schema,
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self.right_source.take().unwrap();
        while let Some(b) = right.next_batch(1024)? {
            for r in 0..b.row_count() {
                self.right_keys
                    .insert(crate::cursors::semi_join::serialize_value(
                        &b.get_value(r, self.right_key_col),
                    ));
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for MarkJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }
        match self.left.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let k = crate::cursors::semi_join::serialize_value(
                        &b.get_value(r, self.left_key_col),
                    );
                    let matched = self.right_keys.contains(&k);
                    let mut row: Vec<Value> =
                        (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                    row.push(Value::I64(if matched { 1 } else { 0 }));
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
    fn marks_matches() {
        let ls = vec![("id".to_string(), ColumnType::I64)];
        let rs = vec![("uid".to_string(), ColumnType::I64)];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1)]]);
        let mut cursor = MarkJoinCursor::new(Box::new(left), Box::new(right), 0, 0, "has_match");
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
        assert_eq!(batch.get_value(0, 1), Value::I64(1)); // true
        assert_eq!(batch.get_value(1, 1), Value::I64(0)); // false
    }
}
