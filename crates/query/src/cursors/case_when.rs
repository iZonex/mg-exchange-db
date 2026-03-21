//! Case-when cursor — evaluates CASE WHEN expressions per row.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// A single WHEN branch: (column_idx, equals_value, then_value).
pub struct WhenBranch {
    pub col_idx: usize,
    pub equals: Value,
    pub then_val: Value,
}

/// Appends a column computed from CASE WHEN logic.
#[allow(dead_code)]
pub struct CaseWhenCursor {
    source: Box<dyn RecordCursor>,
    branches: Vec<WhenBranch>,
    else_val: Value,
    output_name: String,
    schema: Vec<(String, ColumnType)>,
}

impl CaseWhenCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        branches: Vec<WhenBranch>,
        else_val: Value,
        output_name: &str,
    ) -> Self {
        let mut schema = source.schema().to_vec();
        schema.push((output_name.to_string(), ColumnType::Varchar));
        Self {
            source,
            branches,
            else_val,
            output_name: output_name.to_string(),
            schema,
        }
    }
}

impl RecordCursor for CaseWhenCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        match self.source.next_batch(max_rows)? {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                let ncols = b.columns.len();
                for r in 0..b.row_count() {
                    let mut row: Vec<Value> = (0..ncols).map(|c| b.get_value(r, c)).collect();
                    let case_result = self
                        .branches
                        .iter()
                        .find(|br| b.get_value(r, br.col_idx).eq_coerce(&br.equals))
                        .map(|br| br.then_val.clone())
                        .unwrap_or_else(|| self.else_val.clone());
                    row.push(case_result);
                    result.append_row(&row);
                }
                Ok(Some(result))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn case_when_evaluates() {
        let schema = vec![("status".to_string(), ColumnType::I64)];
        let rows = vec![
            vec![Value::I64(1)],
            vec![Value::I64(2)],
            vec![Value::I64(3)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = CaseWhenCursor::new(
            Box::new(source),
            vec![
                WhenBranch {
                    col_idx: 0,
                    equals: Value::I64(1),
                    then_val: Value::Str("active".into()),
                },
                WhenBranch {
                    col_idx: 0,
                    equals: Value::I64(2),
                    then_val: Value::Str("inactive".into()),
                },
            ],
            Value::Str("unknown".into()),
            "label",
        );
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 1), Value::Str("active".into()));
        assert_eq!(batch.get_value(2, 1), Value::Str("unknown".into()));
    }
}
