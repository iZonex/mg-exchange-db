//! Deferred filter cursor — lazily evaluates a subquery for filtering.
//!
//! Runs a subquery cursor once to build a set of allowed values, then
//! filters the main source against that set.

use std::collections::HashSet;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Lazily evaluates a subquery cursor and uses its result set to filter
/// rows from the main source.
///
/// Equivalent to `WHERE col IN (SELECT ...)` but executed as a cursor
/// pipeline rather than a nested query.
pub struct DeferredFilterCursor {
    source: Box<dyn RecordCursor>,
    /// Column index in the source to check against the subquery result.
    filter_col: usize,
    /// Set of allowed values from the subquery.
    allowed: HashSet<Vec<u8>>,
    built: bool,
    subquery: Option<Box<dyn RecordCursor>>,
    /// Column index in the subquery result to use for matching.
    subquery_col: usize,
}

impl DeferredFilterCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        filter_col: usize,
        subquery: Box<dyn RecordCursor>,
        subquery_col: usize,
    ) -> Self {
        Self {
            source,
            filter_col,
            allowed: HashSet::new(),
            built: false,
            subquery: Some(subquery),
            subquery_col,
        }
    }

    fn build(&mut self) -> Result<()> {
        let mut sub = self.subquery.take().expect("subquery already consumed");
        loop {
            match sub.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    for r in 0..batch.row_count() {
                        let val = batch.get_value(r, self.subquery_col);
                        self.allowed.insert(Self::serialize(&val));
                    }
                }
            }
        }
        self.built = true;
        Ok(())
    }

    fn serialize(val: &Value) -> Vec<u8> {
        let mut buf = Vec::new();
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
        buf
    }
}

impl RecordCursor for DeferredFilterCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        self.source.schema()
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.built {
            self.build()?;
        }

        let schema: Vec<(String, ColumnType)> = self.source.schema().to_vec();
        let mut result = RecordBatch::new(schema);

        while result.row_count() < max_rows {
            match self.source.next_batch(max_rows)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let val = batch.get_value(r, self.filter_col);
                        if self.allowed.contains(&Self::serialize(&val)) {
                            let row: Vec<Value> = (0..ncols)
                                .map(|c| batch.get_value(r, c))
                                .collect();
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
    fn deferred_filter_basic() {
        let main_schema = vec![
            ("id".to_string(), ColumnType::I64),
            ("name".to_string(), ColumnType::Varchar),
        ];
        let sub_schema = vec![("id".to_string(), ColumnType::I64)];

        let source = MemoryCursor::from_rows(
            main_schema,
            &[
                vec![Value::I64(1), Value::Str("Alice".into())],
                vec![Value::I64(2), Value::Str("Bob".into())],
                vec![Value::I64(3), Value::Str("Carol".into())],
            ],
        );
        let subquery = MemoryCursor::from_rows(
            sub_schema,
            &[vec![Value::I64(1)], vec![Value::I64(3)]],
        );

        let mut cursor = DeferredFilterCursor::new(
            Box::new(source),
            0, // filter on id
            Box::new(subquery),
            0, // match subquery col 0
        );

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 1));
            }
        }
        assert_eq!(
            all,
            vec![Value::Str("Alice".into()), Value::Str("Carol".into())]
        );
    }
}
