//! Generate series cursor — produces sequences without disk I/O.
//!
//! Generates integer or timestamp ranges as virtual rows, useful for
//! `generate_series(1, 1000)` or creating time grids.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Generates a sequence of values from `start` to `stop` with a given `step`.
pub struct GenerateSeriesCursor {
    current: i64,
    stop: i64,
    step: i64,
    is_timestamp: bool,
    schema: Vec<(String, ColumnType)>,
}

impl GenerateSeriesCursor {
    /// Create an integer series from `start` to `stop` (inclusive) with step.
    pub fn new_i64(start: i64, stop: i64, step: i64) -> Self {
        let effective_step = if step == 0 { 1 } else { step };
        Self {
            current: start,
            stop,
            step: effective_step,
            is_timestamp: false,
            schema: vec![("generate_series".to_string(), ColumnType::I64)],
        }
    }

    /// Create a timestamp series from `start` to `stop` (inclusive) with step in nanoseconds.
    pub fn new_timestamp(start: i64, stop: i64, step: i64) -> Self {
        let effective_step = if step == 0 { 1 } else { step };
        Self {
            current: start,
            stop,
            step: effective_step,
            is_timestamp: true,
            schema: vec![("generate_series".to_string(), ColumnType::Timestamp)],
        }
    }

    fn is_done(&self) -> bool {
        if self.step > 0 {
            self.current > self.stop
        } else {
            self.current < self.stop
        }
    }
}

impl RecordCursor for GenerateSeriesCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.is_done() {
            return Ok(None);
        }

        let mut result = RecordBatch::new(self.schema.clone());
        let mut count = 0;

        while count < max_rows && !self.is_done() {
            let val = if self.is_timestamp {
                Value::Timestamp(self.current)
            } else {
                Value::I64(self.current)
            };
            result.append_row(&[val]);
            self.current += self.step;
            count += 1;
        }

        if result.row_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }

    fn estimated_rows(&self) -> Option<u64> {
        if self.step == 0 {
            return Some(0);
        }
        let range = if self.step > 0 {
            self.stop.saturating_sub(self.current)
        } else {
            self.current.saturating_sub(self.stop)
        };
        Some((range / self.step.abs() + 1).max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_integer_series() {
        let mut cursor = GenerateSeriesCursor::new_i64(1, 5, 1);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(
            all,
            vec![Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4), Value::I64(5)]
        );
    }

    #[test]
    fn generate_with_step() {
        let mut cursor = GenerateSeriesCursor::new_i64(0, 10, 3);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(
            all,
            vec![Value::I64(0), Value::I64(3), Value::I64(6), Value::I64(9)]
        );
    }

    #[test]
    fn generate_timestamp_series() {
        let mut cursor = GenerateSeriesCursor::new_timestamp(1000, 3000, 1000);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(
            all,
            vec![Value::Timestamp(1000), Value::Timestamp(2000), Value::Timestamp(3000)]
        );
    }

    #[test]
    fn generate_descending() {
        let mut cursor = GenerateSeriesCursor::new_i64(5, 1, -1);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(
            all,
            vec![Value::I64(5), Value::I64(4), Value::I64(3), Value::I64(2), Value::I64(1)]
        );
    }
}
