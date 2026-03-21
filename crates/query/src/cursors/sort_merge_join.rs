//! Sort-merge join — joins two pre-sorted cursors on a key column without a hash table.

use std::collections::VecDeque;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Joins two pre-sorted-by-key cursors by walking them in lockstep.
pub struct SortMergeJoinCursor {
    left: Box<dyn RecordCursor>,
    right: Box<dyn RecordCursor>,
    left_key: usize,
    right_key: usize,
    schema: Vec<(String, ColumnType)>,
    left_rows: VecDeque<Vec<Value>>,
    right_rows: VecDeque<Vec<Value>>,
    buffer: VecDeque<Vec<Value>>,
    left_done: bool,
    right_done: bool,
}

impl SortMergeJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_key: usize,
        right_key: usize,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend(right.schema().to_vec());
        Self {
            left,
            right,
            left_key,
            right_key,
            schema,
            left_rows: VecDeque::new(),
            right_rows: VecDeque::new(),
            buffer: VecDeque::new(),
            left_done: false,
            right_done: false,
        }
    }

    fn fill_left(&mut self) -> Result<()> {
        if self.left_rows.is_empty() && !self.left_done {
            match self.left.next_batch(256)? {
                Some(b) => {
                    for r in 0..b.row_count() {
                        self.left_rows
                            .push_back((0..b.columns.len()).map(|c| b.get_value(r, c)).collect());
                    }
                }
                None => self.left_done = true,
            }
        }
        Ok(())
    }

    fn fill_right(&mut self) -> Result<()> {
        if self.right_rows.is_empty() && !self.right_done {
            match self.right.next_batch(256)? {
                Some(b) => {
                    for r in 0..b.row_count() {
                        self.right_rows
                            .push_back((0..b.columns.len()).map(|c| b.get_value(r, c)).collect());
                    }
                }
                None => self.right_done = true,
            }
        }
        Ok(())
    }
}

impl RecordCursor for SortMergeJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut result = RecordBatch::new(self.schema.clone());

        while result.row_count() < max_rows {
            // Drain buffer first.
            while let Some(row) = self.buffer.pop_front() {
                result.append_row(&row);
                if result.row_count() >= max_rows {
                    return Ok(Some(result));
                }
            }

            self.fill_left()?;
            self.fill_right()?;

            if self.left_rows.is_empty() || self.right_rows.is_empty() {
                break;
            }

            let lk = &self.left_rows[0][self.left_key];
            let rk = &self.right_rows[0][self.right_key];

            match lk.cmp_coerce(rk) {
                Some(std::cmp::Ordering::Less) => {
                    self.left_rows.pop_front();
                }
                Some(std::cmp::Ordering::Greater) => {
                    self.right_rows.pop_front();
                }
                _ => {
                    // Match — combine.
                    let left_row = self.left_rows.pop_front().unwrap();
                    let mut combined = left_row.clone();
                    combined.extend(self.right_rows[0].iter().cloned());
                    self.buffer.push_back(combined);
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
    fn merge_join_sorted() {
        let ls = vec![("id".to_string(), ColumnType::I64)];
        let rs = vec![
            ("id".to_string(), ColumnType::I64),
            ("v".to_string(), ColumnType::I64),
        ];
        let left = MemoryCursor::from_rows(
            ls,
            &[
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)],
            ],
        );
        let right = MemoryCursor::from_rows(
            rs,
            &[
                vec![Value::I64(1), Value::I64(10)],
                vec![Value::I64(3), Value::I64(30)],
            ],
        );
        let mut cursor = SortMergeJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let mut total = 0;
        while let Some(b) = cursor.next_batch(100).unwrap() {
            total += b.row_count();
        }
        assert_eq!(total, 2);
    }
}
