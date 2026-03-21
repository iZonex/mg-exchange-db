//! Union cursors — UNION ALL and UNION DISTINCT.

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Concatenates results from multiple cursors (UNION ALL).
///
/// Sources are consumed in order: all rows from source 0, then all from
/// source 1, and so on.
pub struct UnionCursor {
    sources: Vec<Box<dyn RecordCursor>>,
    current: usize,
    schema: Vec<(String, ColumnType)>,
}

impl UnionCursor {
    /// Create a new union cursor from multiple source cursors.
    ///
    /// All sources should have compatible schemas. The schema of the first
    /// source is used as the output schema.
    pub fn new(sources: Vec<Box<dyn RecordCursor>>) -> Self {
        let schema = if sources.is_empty() {
            Vec::new()
        } else {
            sources[0].schema().to_vec()
        };
        Self {
            sources,
            current: 0,
            schema,
        }
    }
}

impl RecordCursor for UnionCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        while self.current < self.sources.len() {
            match self.sources[self.current].next_batch(max_rows)? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    self.current += 1;
                }
            }
        }
        Ok(None)
    }
}

/// Dedup union cursor (UNION without ALL).
///
/// Wraps a `UnionCursor` and removes duplicate rows by tracking seen
/// row keys in a hash set.
pub struct UnionDistinctCursor {
    source: UnionCursor,
    /// Serialized row keys we've already emitted.
    seen: HashSet<Vec<u8>>,
}

impl UnionDistinctCursor {
    /// Create a new distinct union cursor.
    pub fn new(sources: Vec<Box<dyn RecordCursor>>) -> Self {
        Self {
            source: UnionCursor::new(sources),
            seen: HashSet::new(),
        }
    }

    /// Serialize a row into a byte key for deduplication.
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

impl RecordCursor for UnionDistinctCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema.clone());

        while result.row_count() < max_rows {
            let batch = self.source.next_batch(max_rows)?;
            match batch {
                None => break,
                Some(b) => {
                    for r in 0..b.row_count() {
                        let row: Vec<Value> =
                            (0..b.columns.len()).map(|c| b.get_value(r, c)).collect();
                        let key = Self::row_key(&row);
                        if self.seen.insert(key) {
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
    fn union_all_concatenates() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 =
            MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let s2 =
            MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(3)], vec![Value::I64(4)]]);

        let mut cursor = UnionCursor::new(vec![Box::new(s1), Box::new(s2)]);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn union_all_empty_source() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(schema.clone(), &[]);
        let s2 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(1)]]);
        let s3 = MemoryCursor::from_rows(schema.clone(), &[]);

        let mut cursor = UnionCursor::new(vec![Box::new(s1), Box::new(s2), Box::new(s3)]);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![1]);
    }

    #[test]
    fn union_distinct_deduplicates() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(1)],
                vec![Value::I64(2)],
                vec![Value::I64(3)],
            ],
        );
        let s2 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(2)],
                vec![Value::I64(3)],
                vec![Value::I64(4)],
            ],
        );

        let mut cursor = UnionDistinctCursor::new(vec![Box::new(s1), Box::new(s2)]);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![1, 2, 3, 4]);
    }
}
