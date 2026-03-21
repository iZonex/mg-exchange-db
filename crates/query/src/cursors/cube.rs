//! Cube cursor — CUBE output (all dimension combinations).

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Produces CUBE aggregation: all possible subsets of grouping columns.
pub struct CubeCursor {
    source: Option<Box<dyn RecordCursor>>,
    key_cols: Vec<usize>,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
    offset: usize,
}

impl CubeCursor {
    pub fn new(source: Box<dyn RecordCursor>, key_cols: Vec<usize>) -> Self {
        let src = source.schema();
        let mut schema: Vec<(String, ColumnType)> = key_cols.iter().map(|&i| src[i].clone()).collect();
        schema.push(("count".to_string(), ColumnType::I64));
        Self { source: Some(source), key_cols, schema, result: None, offset: 0 }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().unwrap();
        let mut all_rows = Vec::new();
        while let Some(b) = source.next_batch(1024)? {
            for r in 0..b.row_count() {
                let key: Vec<Value> = self.key_cols.iter().map(|&c| b.get_value(r, c)).collect();
                all_rows.push(key);
            }
        }

        let n = self.key_cols.len();
        let mut result = RecordBatch::new(self.schema.clone());

        // For each subset of dimensions (2^n subsets).
        for mask in 0..(1u32 << n) {
            let mut groups: HashMap<String, (Vec<Value>, i64)> = HashMap::new();
            for row in &all_rows {
                let key: Vec<Value> = (0..n)
                    .map(|i| if mask & (1 << i) != 0 { row[i].clone() } else { Value::Null })
                    .collect();
                let k = format!("{key:?}");
                groups.entry(k).or_insert_with(|| (key, 0)).1 += 1;
            }
            for (_, (key, cnt)) in &groups {
                let mut r = key.clone();
                r.push(Value::I64(*cnt));
                result.append_row(&r);
            }
        }

        self.result = Some(result);
        Ok(())
    }
}

impl RecordCursor for CubeCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() { self.materialize()?; }
        let mat = self.result.as_ref().unwrap();
        if self.offset >= mat.row_count() { return Ok(None); }
        let n = max_rows.min(mat.row_count() - self.offset);
        let batch = mat.slice(self.offset, n);
        self.offset += n;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn cube_all_combos() {
        let schema = vec![("a".to_string(), ColumnType::I64), ("b".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(1), Value::I64(20)],
            vec![Value::I64(2), Value::I64(10)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = CubeCursor::new(Box::new(source), vec![0, 1]);
        let mut total = 0;
        while let Some(b) = cursor.next_batch(100).unwrap() { total += b.row_count(); }
        // 2^2 = 4 subsets, each with varying group counts.
        assert!(total >= 4);
    }
}
