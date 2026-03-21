//! Semi join — returns left rows that have ANY match in right (for EXISTS).

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Returns left rows where the join key exists in the right side. Builds a set from right.
pub struct SemiJoinCursor {
    left: Box<dyn RecordCursor>,
    right_keys: HashSet<Vec<u8>>,
    left_key_col: usize,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
    right_key_col: usize,
}

impl SemiJoinCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>, left_key_col: usize, right_key_col: usize) -> Self {
        Self {
            left, right_keys: HashSet::new(), left_key_col, built: false,
            right_source: Some(right), right_key_col,
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut right = self.right_source.take().unwrap();
        while let Some(b) = right.next_batch(1024)? {
            for r in 0..b.row_count() {
                let v = b.get_value(r, self.right_key_col);
                self.right_keys.insert(serialize_value(&v));
            }
        }
        self.built = true;
        Ok(())
    }
}

pub fn serialize_value(val: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    match val {
        Value::Null => buf.push(0),
        Value::I64(n) => { buf.push(1); buf.extend_from_slice(&n.to_le_bytes()); }
        Value::F64(n) => { buf.push(2); buf.extend_from_slice(&n.to_bits().to_le_bytes()); }
        Value::Str(s) => { buf.push(3); buf.extend_from_slice(s.as_bytes()); }
        Value::Timestamp(n) => { buf.push(4); buf.extend_from_slice(&n.to_le_bytes()); }
    }
    buf
}

impl RecordCursor for SemiJoinCursor {
    fn schema(&self) -> &[(String, ColumnType)] { self.left.schema() }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built { self.build()?; }
        let schema: Vec<(String, ColumnType)> = self.left.schema().to_vec();
        let mut result = RecordBatch::new(schema);
        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let k = serialize_value(&b.get_value(r, self.left_key_col));
                        if self.right_keys.contains(&k) {
                            let row: Vec<Value> = (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                            result.append_row(&row);
                            if result.row_count() >= max_rows { break; }
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
    fn semi_join_exists() {
        let ls = vec![("id".to_string(), ColumnType::I64), ("name".to_string(), ColumnType::Varchar)];
        let rs = vec![("uid".to_string(), ColumnType::I64)];
        let left = MemoryCursor::from_rows(ls, &[
            vec![Value::I64(1), Value::Str("a".into())],
            vec![Value::I64(2), Value::Str("b".into())],
            vec![Value::I64(3), Value::Str("c".into())],
        ]);
        let right = MemoryCursor::from_rows(rs, &[vec![Value::I64(1)], vec![Value::I64(3)]]);
        let mut cursor = SemiJoinCursor::new(Box::new(left), Box::new(right), 0, 0);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2);
    }
}
