//! Band join — joins where right.val BETWEEN left.val - N AND left.val + N.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Band join: for each left row, matches right rows where the key is within +/- band.
#[allow(dead_code)]
pub struct BandJoinCursor {
    left: Box<dyn RecordCursor>,
    right_rows: Vec<Vec<Value>>,
    left_key_col: usize,
    right_key_col: usize,
    band: i64,
    schema: Vec<(String, ColumnType)>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    left_buffer: Vec<Vec<Value>>,
    out_buffer: Vec<Vec<Value>>,
}

impl BandJoinCursor {
    pub fn new(
        left: Box<dyn RecordCursor>,
        right: Box<dyn RecordCursor>,
        left_key_col: usize,
        right_key_col: usize,
        band: i64,
    ) -> Self {
        let mut schema = left.schema().to_vec();
        schema.extend(right.schema().to_vec());
        Self {
            left,
            right_rows: Vec::new(),
            left_key_col,
            right_key_col,
            band,
            schema,
            built: false,
            right_source: Some(right),
            left_buffer: Vec::new(),
            out_buffer: Vec::new(),
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

    fn key_i64(v: &Value) -> i64 {
        match v {
            Value::I64(n) => *n,
            Value::Timestamp(n) => *n,
            Value::F64(n) => *n as i64,
            _ => 0,
        }
    }
}

impl RecordCursor for BandJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }
        let mut result = RecordBatch::new(self.schema.clone());

        // Drain output buffer.
        while let Some(row) = self.out_buffer.pop() {
            result.append_row(&row);
            if result.row_count() >= max_rows {
                return Ok(Some(result));
            }
        }

        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let lrow: Vec<Value> =
                            (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        let lk = Self::key_i64(&lrow[self.left_key_col]);
                        for rrow in &self.right_rows {
                            let rk = Self::key_i64(&rrow[self.right_key_col]);
                            if rk >= lk - self.band && rk <= lk + self.band {
                                let mut combined = lrow.clone();
                                combined.extend(rrow.iter().cloned());
                                if result.row_count() < max_rows {
                                    result.append_row(&combined);
                                } else {
                                    self.out_buffer.push(combined);
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
    fn band_join_within_range() {
        let ls = vec![("ts".to_string(), ColumnType::I64)];
        let rs = vec![
            ("ts".to_string(), ColumnType::I64),
            ("v".to_string(), ColumnType::I64),
        ];
        let left = MemoryCursor::from_rows(ls, &[vec![Value::I64(100)], vec![Value::I64(200)]]);
        let right = MemoryCursor::from_rows(
            rs,
            &[
                vec![Value::I64(95), Value::I64(1)],
                vec![Value::I64(110), Value::I64(2)],
                vec![Value::I64(250), Value::I64(3)],
            ],
        );
        let mut cursor = BandJoinCursor::new(Box::new(left), Box::new(right), 0, 0, 15);
        let mut total = 0;
        while let Some(b) = cursor.next_batch(100).unwrap() {
            total += b.row_count();
        }
        // 100 matches 95 (within 15), 100 matches 110 (within 15), 200 doesn't match any within 15
        assert_eq!(total, 2);
    }
}
