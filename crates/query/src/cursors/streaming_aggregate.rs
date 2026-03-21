//! Streaming aggregate — one-pass aggregate for pre-sorted GROUP BY keys.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// One-pass aggregate that assumes the source is pre-sorted by group key.
/// Emits groups as soon as the key changes.
pub struct StreamingAggregateCursor {
    source: Box<dyn RecordCursor>,
    key_col: usize,
    agg_col: usize,
    schema: Vec<(String, ColumnType)>,
    current_key: Option<Value>,
    sum: f64,
    count: u64,
    done: bool,
}

impl StreamingAggregateCursor {
    /// Groups by `key_col`, computes SUM and COUNT of `agg_col`.
    pub fn new(source: Box<dyn RecordCursor>, key_col: usize, agg_col: usize) -> Self {
        let schema = vec![
            (source.schema()[key_col].0.clone(), source.schema()[key_col].1),
            ("sum".to_string(), ColumnType::F64),
            ("count".to_string(), ColumnType::I64),
        ];
        Self { source, key_col, agg_col, schema, current_key: None, sum: 0.0, count: 0, done: false }
    }
}

impl RecordCursor for StreamingAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.done { return Ok(None); }
        let mut result = RecordBatch::new(self.schema.clone());

        while result.row_count() < max_rows {
            match self.source.next_batch(256)? {
                None => {
                    self.done = true;
                    if let Some(key) = self.current_key.take() {
                        result.append_row(&[key, Value::F64(self.sum), Value::I64(self.count as i64)]);
                    }
                    break;
                }
                Some(b) => {
                    for r in 0..b.row_count() {
                        let k = b.get_value(r, self.key_col);
                        let v = match b.get_value(r, self.agg_col) {
                            Value::I64(n) => n as f64,
                            Value::F64(n) => n,
                            _ => 0.0,
                        };
                        match &self.current_key {
                            None => {
                                self.current_key = Some(k);
                                self.sum = v;
                                self.count = 1;
                            }
                            Some(ck) if ck.eq_coerce(&k) => {
                                self.sum += v;
                                self.count += 1;
                            }
                            Some(_) => {
                                let old_key = self.current_key.take().unwrap();
                                result.append_row(&[old_key, Value::F64(self.sum), Value::I64(self.count as i64)]);
                                self.current_key = Some(k);
                                self.sum = v;
                                self.count = 1;
                                if result.row_count() >= max_rows { break; }
                            }
                        }
                    }
                }
            }
        }
        if result.row_count() == 0 { Ok(None) } else { Ok(Some(result)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn streaming_agg_sorted() {
        let schema = vec![("g".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(1), Value::I64(20)],
            vec![Value::I64(2), Value::I64(5)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = StreamingAggregateCursor::new(Box::new(source), 0, 1);
        let mut all = Vec::new();
        while let Some(b) = cursor.next_batch(100).unwrap() {
            for r in 0..b.row_count() { all.push((b.get_value(r, 1), b.get_value(r, 2))); }
        }
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], (Value::F64(30.0), Value::I64(2)));
        assert_eq!(all[1], (Value::F64(5.0), Value::I64(1)));
    }
}
