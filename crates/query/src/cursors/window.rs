//! Window function cursor — adds computed window columns.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;
use crate::window::{apply_window_functions, WindowFunction};

/// Window function cursor: materializes the source, applies window functions,
/// and streams out the result with window columns appended.
///
/// Window functions require seeing all rows (or at least all rows within a
/// partition) before they can produce results, so full materialization is
/// necessary.
pub struct WindowCursor {
    source: Option<Box<dyn RecordCursor>>,
    window_fns: Vec<WindowFunction>,
    /// Materialized result with window columns appended.
    materialized: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl WindowCursor {
    /// Create a new window cursor.
    ///
    /// `window_fns` specifies the window functions to compute. Each one
    /// adds a new column to the output schema.
    pub fn new(source: Box<dyn RecordCursor>, window_fns: Vec<WindowFunction>) -> Self {
        let mut schema = source.schema().to_vec();

        // Add output columns for each window function.
        for wf in &window_fns {
            let col_name = wf
                .alias
                .clone()
                .unwrap_or_else(|| format!("{}", wf));
            // Window functions typically produce I64 (row_number, rank, count)
            // or F64 (sum, avg) or same type as input (lag, lead, first_value, last_value).
            // Use I64 as default — the RecordBatch will coerce.
            let col_type = infer_window_type(&wf.name);
            schema.push((col_name, col_type));
        }

        Self {
            source: Some(source),
            window_fns,
            materialized: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");
        let base_schema: Vec<(String, ColumnType)> = source.schema().to_vec();
        let base_columns: Vec<String> = base_schema.iter().map(|(n, _)| n.clone()).collect();

        // Drain all rows from the source.
        let mut rows: Vec<Vec<Value>> = Vec::new();
        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..batch.columns.len())
                            .map(|c| batch.get_value(r, c))
                            .collect();
                        rows.push(row);
                    }
                }
            }
        }

        // Apply window functions (mutates rows in place by appending columns).
        apply_window_functions(&mut rows, &base_columns, &self.window_fns);

        // Build the result batch with the extended schema.
        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &rows {
            batch.append_row(row);
        }

        self.materialized = Some(batch);
        Ok(())
    }
}

/// Infer the output column type for a window function.
fn infer_window_type(name: &str) -> ColumnType {
    match name.to_ascii_lowercase().as_str() {
        "row_number" | "rank" | "dense_rank" | "count" => ColumnType::I64,
        "sum" | "avg" => ColumnType::F64,
        // lag, lead, first_value, last_value return the same type as input,
        // but we don't know the input type here. Use F64 as a safe default.
        _ => ColumnType::F64,
    }
}

impl RecordCursor for WindowCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.materialized.is_none() {
            self.materialize()?;
        }

        let mat = self.materialized.as_ref().unwrap();
        if self.current_row >= mat.row_count() {
            return Ok(None);
        }

        let remaining = mat.row_count() - self.current_row;
        let n = remaining.min(max_rows);
        let batch = mat.slice(self.current_row, n);
        self.current_row += n;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;
    use crate::plan::OrderBy;
    use crate::window::{WindowFuncArg, WindowSpec};

    #[test]
    fn window_row_number() {
        let schema = vec![
            ("name".to_string(), ColumnType::Varchar),
            ("score".to_string(), ColumnType::I64),
        ];
        let rows = vec![
            vec![Value::Str("A".into()), Value::I64(30)],
            vec![Value::Str("B".into()), Value::I64(10)],
            vec![Value::Str("C".into()), Value::I64(20)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);

        let wf = WindowFunction {
            name: "row_number".into(),
            args: vec![],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![OrderBy {
                    column: "score".to_string(),
                    descending: false,
                }],
                frame: None,
            },
            alias: Some("rn".into()),
        };

        let mut cursor = WindowCursor::new(Box::new(source), vec![wf]);

        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 3);

        // Schema should have 3 columns: name, score, rn.
        assert_eq!(cursor.schema().len(), 3);
        assert_eq!(cursor.schema()[2].0, "rn");

        // Rows are in original order, but row_number is based on sorted order.
        // Original order: A(30), B(10), C(20)
        // Sorted by score ASC: B(10)->rn1, C(20)->rn2, A(30)->rn3
        // So A gets rn=3, B gets rn=1, C gets rn=2.
        assert_eq!(batch.get_value(0, 2), Value::I64(3)); // A -> rn 3
        assert_eq!(batch.get_value(1, 2), Value::I64(1)); // B -> rn 1
        assert_eq!(batch.get_value(2, 2), Value::I64(2)); // C -> rn 2
    }

    #[test]
    fn window_cumulative_sum() {
        let schema = vec![("val".to_string(), ColumnType::F64)];
        let rows = vec![
            vec![Value::F64(10.0)],
            vec![Value::F64(20.0)],
            vec![Value::F64(30.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);

        let wf = WindowFunction {
            name: "sum".into(),
            args: vec![WindowFuncArg::Column("val".into())],
            over: WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: Some(crate::window::WindowFrame::Rows {
                    start: crate::window::FrameBound::UnboundedPreceding,
                    end: crate::window::FrameBound::CurrentRow,
                }),
            },
            alias: Some("cumsum".into()),
        };

        let mut cursor = WindowCursor::new(Box::new(source), vec![wf]);

        let batch = cursor.next_batch(100).unwrap().unwrap();
        assert_eq!(batch.row_count(), 3);

        assert_eq!(batch.get_value(0, 1), Value::F64(10.0));
        assert_eq!(batch.get_value(1, 1), Value::F64(30.0));
        assert_eq!(batch.get_value(2, 1), Value::F64(60.0));
    }
}
