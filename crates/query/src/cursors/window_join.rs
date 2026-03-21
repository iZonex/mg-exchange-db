//! Window join — temporal join within a time window (left.ts - window <= right.ts <= left.ts).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// For each left row, matches right rows where right.ts is within [left.ts - window, left.ts].
pub struct WindowJoinCursor {
    left: Box<dyn RecordCursor>,
    right_rows: Vec<Vec<Value>>,
    left_ts_col: usize,
    right_ts_col: usize,
    window_nanos: i64,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
}

impl WindowJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_ts_col: usize,
        right_ts_col: usize,
        window_nanos: i64,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend(right.schema().to_vec());
        Self {
            left,
            right_rows: Vec::new(),
            left_ts_col,
            right_ts_col,
            window_nanos,
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

    fn ts_val(v: &Value) -> i64 {
        match v {
            Value::Timestamp(n) | Value::I64(n) => *n,
            _ => 0,
        }
    }
}

impl RecordCursor for WindowJoinCursor {
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
                        let lts = Self::ts_val(&lrow[self.left_ts_col]);
                        for rrow in &self.right_rows {
                            let rts = Self::ts_val(&rrow[self.right_ts_col]);
                            if rts >= lts - self.window_nanos && rts <= lts {
                                let mut combined = lrow.clone();
                                combined.extend(rrow.iter().cloned());
                                result.append_row(&combined);
                                if result.row_count() >= max_rows {
                                    break;
                                }
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
    fn window_join_temporal() {
        let ls = vec![("ts".to_string(), ColumnType::Timestamp)];
        let rs = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("v".to_string(), ColumnType::I64),
        ];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::Timestamp(1000)]]);
        let right = MemoryCursor::from_rows(
            rs,
            &[
                vec![Value::Timestamp(900), Value::I64(1)],
                vec![Value::Timestamp(999), Value::I64(2)],
                vec![Value::Timestamp(500), Value::I64(3)],
            ],
        );
        let mut cursor = WindowJoinCursor::new(Box::new(left), Box::new(right), 0, 0, 200);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2); // 900 and 999 are within [800, 1000]
    }
}
