//! Intersect cursor — returns rows present in both sources (set intersection).

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Returns only rows that appear in both the left and right source cursors.
///
/// Materializes the right side into a hash set, then streams the left side
/// and emits rows whose serialized key exists in the set.
pub struct IntersectCursor {
    left: Box<dyn RecordCursor>,
    right_set: HashSet<Vec<u8>>,
    /// Tracks rows already emitted to avoid duplicates.
    emitted: HashSet<Vec<u8>>,
    built: bool,
    right_source: Option<Box<dyn RecordCursor>>,
}

impl IntersectCursor {
    pub fn new(left: Box<dyn RecordCursor>, right: Box<dyn RecordCursor>) -> Self {
        Self {
            left,
            right_set: HashSet::new(),
            emitted: HashSet::new(),
            built: false,
            right_source: Some(right),
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
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..batch.columns.len())
                            .map(|c| batch.get_value(r, c))
                            .collect();
                        self.right_set.insert(Self::row_key(&row));
                    }
                }
            }
        }
        self.built = true;
        Ok(())
    }

    fn row_key(row: &[Value]) -> Vec<u8> {
        let mut buf = Vec::new();
        for val in row {
            match val {
                Value::Null => buf.push(0),
                Value::I64(n) => {
                    buf.push(1);
                    buf.extend_from_slice(&n.to_le_bytes());
                }
                Value::F64(n) => {
                    buf.push(2);
                    buf.extend_from_slice(&n.to_bits().to_le_bytes());
                }
                Value::Str(s) => {
                    buf.push(3);
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                Value::Timestamp(n) => {
                    buf.push(4);
                    buf.extend_from_slice(&n.to_le_bytes());
                }
            }
        }
        buf
    }
}

impl RecordCursor for IntersectCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.left.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }

        let schema: Vec<(String, ColumnType)> = self.left.schema().to_vec();
        let mut result = RecordBatch::new(schema);

        while result.row_count() < max_rows {
            match self.left.next_batch(max_rows)? {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let row: Vec<Value> =
                            (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        let key = Self::row_key(&row);
                        if self.right_set.contains(&key) && self.emitted.insert(key) {
                            result.append_row(&row);
                            if result.row_count() >= max_rows {
                                break;
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
    fn intersect_basic() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let left = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)],
            ],
        );
        let right = MemoryCursor::from_rows(
            schema,
            &[
                vec![Value::I64(2)],
                vec![Value::I64(3)],
                vec![Value::I64(4)],
            ],
        );

        let mut cursor = IntersectCursor::new(Box::new(left), Box::new(right));
        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(all, vec![Value::I64(2), Value::I64(3)]);
    }
}
