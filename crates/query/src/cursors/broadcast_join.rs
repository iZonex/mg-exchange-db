//! Broadcast join — materializes the small right table and joins against every left batch.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Materializes the (small) right table and for each left row, emits matching right rows.
pub struct BroadcastJoinCursor {
    left: Box<dyn RecordCursor>,
    right_rows: Vec<Vec<Value>>,
    left_key_col: usize,
    right_key_col: usize,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
}

impl BroadcastJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_key_col: usize,
        right_key_col: usize,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend(right.schema().to_vec());
        Self {
            left,
            right_rows: Vec::new(),
            left_key_col,
            right_key_col,
            schema,
            built: false,
            right_source: Some(right),
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self.right_source.take().unwrap();
        while let Some(b) = right.next_batch(1024)? {
            for r in 0..b.row_count() {
                self.right_rows
                    .push((0..b.columns.len()).map(|c| b.get_value(r, c)).collect());
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for BroadcastJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }
        let mut result = RecordBatch::new(self.schema.clone());
        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let lrow: Vec<Value> =
                            (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        for rrow in &self.right_rows {
                            if lrow[self.left_key_col].eq_coerce(&rrow[self.right_key_col]) {
                                let mut combined = lrow.clone();
                                combined.extend(rrow.iter().cloned());
                                result.append_row(&combined);
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
    fn broadcast_small_right() {
        let ls = vec![("id".to_string(), ColumnType::I64)];
        let rs = vec![
            ("id".to_string(), ColumnType::I64),
            ("v".to_string(), ColumnType::I64),
        ];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1), Value::I64(99)]]);
        let mut cursor = BroadcastJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 1);
    }
}
