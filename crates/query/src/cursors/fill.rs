//! Fill cursor — fills gaps in time-series data.
//!
//! Supports FILL NULL, FILL PREV, and FILL LINEAR strategies for missing
//! time buckets in SAMPLE BY results.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Strategy for filling missing values.
#[derive(Debug, Clone)]
pub enum FillStrategy {
    /// Fill with NULL.
    Null,
    /// Fill with the previous non-null value.
    Prev,
    /// Fill with linearly interpolated values between neighbors.
    Linear,
    /// Fill with a constant value.
    Constant(Value),
}

/// Fills gaps in time-series data from a source cursor.
///
/// The source is expected to have a timestamp column at `ts_col` and numeric
/// value columns. Gaps are detected when the timestamp difference exceeds
/// the expected `interval`, and synthetic rows are inserted according to
/// the chosen `FillStrategy`.
pub struct FillCursor {
    source: Option<Box<dyn RecordCursor>>,
    ts_col: usize,
    interval: i64,
    strategy: FillStrategy,
    result: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl FillCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        ts_col: usize,
        interval: i64,
        strategy: FillStrategy,
    ) -> Self {
        let schema = source.schema().to_vec();
        Self {
            source: Some(source),
            ts_col,
            interval,
            strategy,
            result: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");

        // Collect all rows.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols).map(|c| batch.get_value(r, c)).collect();
                        rows.push(row);
                    }
                }
            }
        }

        let mut result = RecordBatch::new(self.schema.clone());

        if rows.is_empty() {
            self.result = Some(result);
            return Ok(());
        }

        let ncols = self.schema.len();

        for i in 0..rows.len() {
            result.append_row(&rows[i]);

            // Check if there's a gap before the next row.
            if i + 1 < rows.len() {
                let curr_ts = self.get_ts(&rows[i]);
                let next_ts = self.get_ts(&rows[i + 1]);

                let mut ts = curr_ts + self.interval;
                while ts < next_ts {
                    let mut fill_row = Vec::with_capacity(ncols);
                    for col in 0..ncols {
                        if col == self.ts_col {
                            fill_row.push(Value::Timestamp(ts));
                        } else {
                            fill_row.push(self.fill_value(
                                col,
                                &rows[i],
                                &rows[i + 1],
                                curr_ts,
                                next_ts,
                                ts,
                            ));
                        }
                    }
                    result.append_row(&fill_row);
                    ts += self.interval;
                }
            }
        }

        self.result = Some(result);
        Ok(())
    }

    fn get_ts(&self, row: &[Value]) -> i64 {
        match &row[self.ts_col] {
            Value::Timestamp(n) | Value::I64(n) => *n,
            _ => 0,
        }
    }

    fn fill_value(
        &self,
        _col: usize,
        prev_row: &[Value],
        next_row: &[Value],
        prev_ts: i64,
        next_ts: i64,
        current_ts: i64,
    ) -> Value {
        match &self.strategy {
            FillStrategy::Null => Value::Null,
            FillStrategy::Prev => prev_row.get(_col).cloned().unwrap_or(Value::Null),
            FillStrategy::Linear => {
                // Interpolate numeric values.
                let prev_val = Self::to_f64(prev_row.get(_col));
                let next_val = Self::to_f64(next_row.get(_col));
                match (prev_val, next_val) {
                    (Some(p), Some(n)) => {
                        let frac = (current_ts - prev_ts) as f64 / (next_ts - prev_ts) as f64;
                        Value::F64(p + (n - p) * frac)
                    }
                    _ => Value::Null,
                }
            }
            FillStrategy::Constant(v) => v.clone(),
        }
    }

    fn to_f64(val: Option<&Value>) -> Option<f64> {
        match val? {
            Value::F64(n) => Some(*n),
            Value::I64(n) => Some(*n as f64),
            _ => None,
        }
    }
}

impl RecordCursor for FillCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() {
            self.materialize()?;
        }

        let mat = self.result.as_ref().unwrap();
        if self.current_row >= mat.row_count() {
            return Ok(None);
        }

        let remaining = mat.row_count() - self.current_row;
        let n = remaining.min(max_rows);
        let batch = mat.slice(self.current_row, n);
        self.current_row += n;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn fill_null_inserts_gaps() {
        let schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("val".to_string(), ColumnType::F64),
        ];
        // Gap between ts=100 and ts=400 with interval=100.
        let rows = vec![
            vec![Value::Timestamp(100), Value::F64(1.0)],
            vec![Value::Timestamp(400), Value::F64(4.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = FillCursor::new(Box::new(source), 0, 100, FillStrategy::Null);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all.push(row);
            }
        }

        // Original 2 rows + 2 fill rows (ts=200, ts=300).
        assert_eq!(all.len(), 4);
        assert_eq!(all[1][0], Value::Timestamp(200));
        assert_eq!(all[1][1], Value::Null);
        assert_eq!(all[2][0], Value::Timestamp(300));
    }

    #[test]
    fn fill_prev_uses_last_value() {
        let schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("val".to_string(), ColumnType::F64),
        ];
        let rows = vec![
            vec![Value::Timestamp(100), Value::F64(10.0)],
            vec![Value::Timestamp(300), Value::F64(30.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = FillCursor::new(Box::new(source), 0, 100, FillStrategy::Prev);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 1));
            }
        }

        // ts=100 -> 10.0, ts=200 -> 10.0 (prev), ts=300 -> 30.0
        assert_eq!(
            all,
            vec![Value::F64(10.0), Value::F64(10.0), Value::F64(30.0)]
        );
    }
}
