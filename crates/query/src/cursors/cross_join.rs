//! Cross join cursor — Cartesian product of two cursors.
//!
//! Produces every combination of (left_row, right_row). Materializes
//! the right side and streams the left.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Produces the Cartesian product of two cursors.
///
/// Materializes the right side, then for each left row emits one output
/// row per right row with all columns concatenated.
pub struct CrossJoinCursor {
    left: Box<dyn RecordCursor>,
    right_rows: Vec<Vec<Value>>,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    /// Pending left row being cross-joined with right rows.
    current_left_row: Option<Vec<Value>>,
    /// Index into right_rows for the current left row.
    right_idx: usize,
}

impl CrossJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>) -> Self {
        let left_schema = left.schema().to_vec();
        let right_schema = right.schema().to_vec();
        let mut schema = left_schema;
        schema.extend(right_schema);

        Self {
            left,
            right_rows: Vec::new(),
            schema,
            built: false,
            right_source: Some(right),
            current_left_row: None,
            right_idx: 0,
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

impl RecordCursor for CrossJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }

        if self.right_rows.is_empty() {
            return Ok(None);
        }

        let mut result = RecordBatch::new(self.schema.clone());

        loop {
            // Continue emitting cross product of current left row.
            if let Some(left_row) = &self.current_left_row {
                while self.right_idx < self.right_rows.len() && result.row_count() < max_rows {
                    let mut combined = left_row.clone();
                    combined.extend(self.right_rows[self.right_idx].iter().cloned());
                    result.append_row(&combined);
                    self.right_idx += 1;
                }

                if self.right_idx >= self.right_rows.len() {
                    self.current_left_row = None;
                    self.right_idx = 0;
                }

                if result.row_count() >= max_rows {
                    return Ok(Some(result));
                }
            }

            // Get next left row.
            match self.left.next_batch(1)? {
                None => break,
                Some(batch) => {
                    if batch.row_count() > 0 {
                        let ncols = batch.columns.len();
                        let row: Vec<Value> = (0..ncols).map(|c| batch.get_value(0, c)).collect();
                        self.current_left_row = Some(row);
                        self.right_idx = 0;
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
    fn cross_join_basic() {
        let left_schema = vec![("a".to_string(), ColumnType::I64)];
        let right_schema = vec![("b".to_string(), ColumnType::I64)];

        let left =
            MemoryCursor::from_rows(left_schema, &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let right = MemoryCursor::from_rows(
            right_schema,
            &[
                vec![Value::I64(10)],
                vec![Value::I64(20)],
                vec![Value::I64(30)],
            ],
        );

        let mut cursor = CrossJoinCursor::new(Box::new(left), Box::new(right));

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all.push(row);
            }
        }

        // 2 * 3 = 6 rows
        assert_eq!(all.len(), 6);
        assert_eq!(all[0], vec![Value::I64(1), Value::I64(10)]);
        assert_eq!(all[1], vec![Value::I64(1), Value::I64(20)]);
        assert_eq!(all[2], vec![Value::I64(1), Value::I64(30)]);
        assert_eq!(all[3], vec![Value::I64(2), Value::I64(10)]);
        assert_eq!(all[4], vec![Value::I64(2), Value::I64(20)]);
        assert_eq!(all[5], vec![Value::I64(2), Value::I64(30)]);
    }

    #[test]
    fn cross_join_empty_right() {
        let left_schema = vec![("a".to_string(), ColumnType::I64)];
        let right_schema = vec![("b".to_string(), ColumnType::I64)];

        let left = MemoryCursor::from_rows(left_schema, &[vec![Value::I64(1)]]);
        let right = MemoryCursor::from_rows(right_schema, &[]);

        let mut cursor = CrossJoinCursor::new(Box::new(left), Box::new(right));
        assert!(cursor.next_batch(100).unwrap().is_none());
    }
}
