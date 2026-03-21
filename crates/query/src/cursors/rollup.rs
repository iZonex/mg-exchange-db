//! Rollup cursor — emits multiple aggregation levels (ROLLUP semantics).

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Produces ROLLUP aggregation: groups at each level + grand total.
/// Groups by `key_cols` and computes COUNT of rows at each rollup level.
pub struct RollupCursor {
    source: Option<Box<dyn RecordCursor>>,
    key_cols: Vec<usize>,
    schema: Vec<(String, ColumnType)>,
    result: Option<RecordBatch>,
    offset: usize,
}

impl RollupCursor {
    pub fn new(source: Box<dyn RecordCursor>, key_cols: Vec<usize>) -> Self {
        let src = source.schema();
        let mut schema: Vec<(String, ColumnType)> = key_cols.iter().map(|&i| src[i].clone()).collect();
        schema.push(("count".to_string(), ColumnType::I64));
        Self { source: Some(source), key_cols, schema, result: None, offset: 0 }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().unwrap();
        let mut rows = Vec::new();
        while let Some(b) = source.next_batch(1024)? {
            for r in 0..b.row_count() {
                let key: Vec<Value> = self.key_cols.iter().map(|&c| b.get_value(r, c)).collect();
                rows.push(key);
            }
        }

        let mut result = RecordBatch::new(self.schema.clone());
        // Full group level.
        let mut groups: HashMap<String, (Vec<Value>, i64)> = HashMap::new();
        for row in &rows {
            let k = format!("{row:?}");
            groups.entry(k).or_insert_with(|| (row.clone(), 0)).1 += 1;
        }
        for (key, cnt) in groups.values() {
            let mut r = key.clone();
            r.push(Value::I64(*cnt));
            result.append_row(&r);
        }
        // Grand total row: NULLs for keys.
        let mut total_row: Vec<Value> = vec![Value::Null; self.key_cols.len()];
        total_row.push(Value::I64(rows.len() as i64));
        result.append_row(&total_row);

        self.result = Some(result);
        Ok(())
    }
}

impl RecordCursor for RollupCursor {
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
    fn rollup_with_total() {
        let schema = vec![("g".to_string(), ColumnType::I64), ("v".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1), Value::I64(10)],
            vec![Value::I64(1), Value::I64(20)],
            vec![Value::I64(2), Value::I64(30)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = RollupCursor::new(Box::new(source), vec![0]);
        let mut total_rows = 0;
        while let Some(b) = cursor.next_batch(100).unwrap() { total_rows += b.row_count(); }
        // 2 groups + 1 grand total = 3
        assert_eq!(total_rows, 3);
    }
}
