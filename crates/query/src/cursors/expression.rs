//! Expression cursor — evaluates computed expressions (e.g., price * volume).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Supported expression operations for the cursor.
#[derive(Clone)]
pub enum ExprOp {
    /// Multiply two columns: (col_a_idx, col_b_idx, output_name)
    Mul(usize, usize, String),
    /// Add two columns.
    Add(usize, usize, String),
    /// Subtract col_b from col_a.
    Sub(usize, usize, String),
}

/// Evaluates computed expressions and appends result columns.
pub struct ExpressionCursor {
    source: Box<dyn RecordCursor>,
    ops: Vec<ExprOp>,
    schema: Vec<(String, ColumnType)>,
}

impl ExpressionCursor {
    pub fn new(source: Box<dyn RecordCursor>, ops: Vec<ExprOp>) -> Self {
        let mut schema = source.schema().to_vec();
        for op in &ops {
            let name = match op {
                ExprOp::Mul(_, _, n) | ExprOp::Add(_, _, n) | ExprOp::Sub(_, _, n) => n.clone(),
            };
            schema.push((name, ColumnType::F64));
        }
        Self {
            source,
            ops,
            schema,
        }
    }

    fn eval_op(op: &ExprOp, row: &[Value]) -> Value {
        let (a_idx, b_idx) = match op {
            ExprOp::Mul(a, b, _) | ExprOp::Add(a, b, _) | ExprOp::Sub(a, b, _) => (*a, *b),
        };
        let a = match &row[a_idx] {
            Value::I64(n) => *n as f64,
            Value::F64(n) => *n,
            _ => return Value::Null,
        };
        let b = match &row[b_idx] {
            Value::I64(n) => *n as f64,
            Value::F64(n) => *n,
            _ => return Value::Null,
        };
        Value::F64(match op {
            ExprOp::Mul(..) => a * b,
            ExprOp::Add(..) => a + b,
            ExprOp::Sub(..) => a - b,
        })
    }
}

impl RecordCursor for ExpressionCursor {
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
                    let base: Vec<Value> = (0..ncols).map(|c| b.get_value(r, c)).collect();
                    let mut row = base.clone();
                    for op in &self.ops {
                        row.push(Self::eval_op(op, &base));
                    }
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
    fn evaluates_mul_expression() {
        let schema = vec![
            ("price".to_string(), ColumnType::F64),
            ("volume".to_string(), ColumnType::I64),
        ];
        let rows = vec![vec![Value::F64(100.0), Value::I64(5)]];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor =
            ExpressionCursor::new(Box::new(source), vec![ExprOp::Mul(0, 1, "notional".into())]);
        let batch = cursor.next_batch(10).unwrap().unwrap();
        assert_eq!(batch.get_value(0, 2), Value::F64(500.0));
    }
}
