//! Column projection cursor.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Cursor that selects a subset of columns from the source.
pub struct ProjectCursor {
    source: Box<dyn RecordCursor>,
    /// Indices into the source schema to project.
    columns: Vec<usize>,
    schema: Vec<(String, ColumnType)>,
}

impl ProjectCursor {
    pub fn new(source: Box<dyn RecordCursor>, columns: Vec<usize>) -> Self {
        let source_schema = source.schema();
        let schema: Vec<(String, ColumnType)> = columns
            .iter()
            .filter_map(|&i| source_schema.get(i).cloned())
            .collect();
        Self {
            source,
            columns,
            schema,
        }
    }

    /// Create a projection cursor by column names.
    pub fn by_names(source: Box<dyn RecordCursor>, names: &[String]) -> Self {
        let source_schema = source.schema();
        let columns: Vec<usize> = names
            .iter()
            .filter_map(|name| source_schema.iter().position(|(n, _)| n == name))
            .collect();
        Self::new(source, columns)
    }
}

impl RecordCursor for ProjectCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let batch = self.source.next_batch(max_rows)?;
        match batch {
            None => Ok(None),
            Some(b) => {
                let mut result = RecordBatch::new(self.schema.clone());
                for r in 0..b.row_count() {
                    let row: Vec<Value> = self.columns.iter().map(|&c| b.get_value(r, c)).collect();
                    result.append_row(&row);
                }
                Ok(Some(result))
            }
        }
    }
}
