//! Nested-loop join cursor — fallback for non-equi joins.
//!
//! Handles arbitrary join conditions (e.g., `ON a.x > b.y`) by iterating
//! over every pair of left/right rows. O(n*m) but fully general.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Predicate function type for the join condition.
pub type JoinPredicate = Box<dyn Fn(&[Value], &[Value]) -> bool + Send>;

/// Nested-loop join: materializes the right side, then for each left row
/// iterates over all right rows and emits pairs that satisfy the predicate.
pub struct NestedLoopJoinCursor {
    left: Box<dyn RecordCursor>,
    right_rows: Vec<Vec<Value>>,
    predicate: JoinPredicate,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    /// Buffered output from current left row.
    buffer: Vec<Vec<Value>>,
    buffer_pos: usize,
}

impl NestedLoopJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        predicate: JoinPredicate,
    ) -> Self {
        let left_schema = left.schema().to_vec();
        let right_schema = right.schema().to_vec();
        let mut schema = left_schema;
        schema.extend(right_schema);

        Self {
            left,
            right_rows: Vec::new(),
            predicate,
            schema,
            built: false,
            right_source: Some(right),
            buffer: Vec::new(),
            buffer_pos: 0,
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self
            .right_source
            .take()
            .expect("right source already consumed");
        loop {
            match right.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols).map(|c| batch.get_value(r, c)).collect();
                        self.right_rows.push(row);
                    }
                }
            }
        }
        self.built = true;
        Ok(())
    }
}

impl RecordCursor for NestedLoopJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }

        let mut result = RecordBatch::new(self.schema.clone());

        // Drain buffer first.
        while self.buffer_pos < self.buffer.len() && result.row_count() < max_rows {
            result.append_row(&self.buffer[self.buffer_pos]);
            self.buffer_pos += 1;
        }
        if self.buffer_pos >= self.buffer.len() {
            self.buffer.clear();
            self.buffer_pos = 0;
        }

        if result.row_count() >= max_rows {
            return Ok(Some(result));
        }

        // Pull left rows and join.
        loop {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let left_row: Vec<Value> =
                            (0..ncols).map(|c| batch.get_value(r, c)).collect();

                        for right_row in &self.right_rows {
                            if (self.predicate)(&left_row, right_row) {
                                let mut combined = left_row.clone();
                                combined.extend(right_row.iter().cloned());

                                if result.row_count() < max_rows {
                                    result.append_row(&combined);
                                } else {
                                    self.buffer.push(combined);
                                }
                            }
                        }

                        if result.row_count() >= max_rows && !self.buffer.is_empty() {
                            return Ok(Some(result));
                        }
                    }
                }
            }

            if result.row_count() >= max_rows {
                break;
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
    fn nested_loop_non_equi_join() {
        let left_schema = vec![("a".to_string(), ColumnType::I64)];
        let right_schema = vec![("b".to_string(), ColumnType::I64)];

        let left = MemoryCursor::from_rows(
            left_schema,
            &[
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)],
            ],
        );
        let right =
            MemoryCursor::from_rows(right_schema, &[vec![Value::I64(2)], vec![Value::I64(3)]]);

        // Join on a < b
        let predicate: JoinPredicate =
            Box::new(|left_row, right_row| match (&left_row[0], &right_row[0]) {
                (Value::I64(a), Value::I64(b)) => a < b,
                _ => false,
            });

        let mut cursor = NestedLoopJoinCursor::new(Box::new(left), Box::new(right), predicate);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all.push(row);
            }
        }
        // (1,2), (1,3), (2,3) = 3 rows
        assert_eq!(all.len(), 3);
        assert_eq!(all[0], vec![Value::I64(1), Value::I64(2)]);
        assert_eq!(all[1], vec![Value::I64(1), Value::I64(3)]);
        assert_eq!(all[2], vec![Value::I64(2), Value::I64(3)]);
    }
}
