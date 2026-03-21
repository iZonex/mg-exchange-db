//! Pivoted aggregate — pivots aggregation results into columns.

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Pivots distinct values of a pivot column into separate output columns, aggregating with COUNT.
pub struct PivotedAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    group_col: usize,
    pivot_col: usize,
    result: Option<RecordBatch>,
    schema: Vec<(String, ColumnType)>,
    emitted: bool,
}

impl PivotedAggregateCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        group_col: usize,
        pivot_col: usize,
        pivot_values: Vec<String>,
    ) -> Self {
        let src = source.schema();
        let mut schema = vec![src[group_col].clone()];
        for pv in &pivot_values {
            schema.push((pv.clone(), ColumnType::I64));
        }
        Self {
            source: Some(source),
            group_col,
            pivot_col,
            result: None,
            schema,
            emitted: false,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().unwrap();
        let pivot_names: Vec<String> = self.schema[1..].iter().map(|(n, _)| n.clone()).collect();

        // group_key -> { pivot_value -> count }
        let mut data: HashMap<String, (Value, HashMap<String, i64>)> = HashMap::new();
        let mut order = Vec::new();

        while let Some(b) = source.next_batch(1024)? {
            for r in 0..b.row_count() {
                let gk = b.get_value(r, self.group_col);
                let pv = format!("{}", b.get_value(r, self.pivot_col));
                let key = format!("{gk:?}");
                let entry = data.entry(key.clone()).or_insert_with(|| {
                    order.push(key);
                    (gk, HashMap::new())
                });
                *entry.1.entry(pv).or_insert(0) += 1;
            }
        }

        let mut result = RecordBatch::new(self.schema.clone());
        for key in &order {
            if let Some((gv, counts)) = data.get(key) {
                let mut row = vec![gv.clone()];
                for pn in &pivot_names {
                    row.push(Value::I64(*counts.get(pn).unwrap_or(&0)));
                }
                result.append_row(&row);
            }
        }

        self.result = Some(result);
        Ok(())
    }
}

impl RecordCursor for PivotedAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        if self.result.is_none() {
            self.materialize()?;
        }
        self.emitted = true;
        Ok(self.result.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn pivot_aggregation() {
        let schema = vec![
            ("region".to_string(), ColumnType::Varchar),
            ("product".to_string(), ColumnType::Varchar),
        ];
        let rows = vec![
            vec![Value::Str("US".into()), Value::Str("A".into())],
            vec![Value::Str("US".into()), Value::Str("B".into())],
            vec![Value::Str("US".into()), Value::Str("A".into())],
            vec![Value::Str("EU".into()), Value::Str("A".into())],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor =
            PivotedAggregateCursor::new(Box::new(source), 0, 1, vec!["A".into(), "B".into()]);
        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 2); // US, EU
    }
}
