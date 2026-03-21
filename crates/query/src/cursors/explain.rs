//! Explain cursor — returns query plan description as rows.
//!
//! Instead of executing a query, this cursor emits the execution plan
//! as human-readable text rows (one row per plan node).

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Emits query plan description lines as rows.
///
/// Each row contains a single `QUERY PLAN` column with a text description
/// of a plan node, optionally with estimated row counts.
pub struct ExplainCursor {
    lines: Vec<String>,
    offset: usize,
    schema: Vec<(String, ColumnType)>,
}

impl ExplainCursor {
    /// Create an explain cursor from a list of plan description lines.
    pub fn new(lines: Vec<String>) -> Self {
        Self {
            lines,
            offset: 0,
            schema: vec![("QUERY PLAN".to_string(), ColumnType::Varchar)],
        }
    }

    /// Build an explain cursor by describing a cursor tree.
    pub fn from_cursor_tree(cursor: &dyn RecordCursor, label: &str) -> Self {
        let mut lines = Vec::new();
        Self::describe(cursor, label, 0, &mut lines);
        Self::new(lines)
    }

    fn describe(
        cursor: &dyn RecordCursor,
        label: &str,
        depth: usize,
        lines: &mut Vec<String>,
    ) {
        let indent = "  ".repeat(depth);
        let est = cursor
            .estimated_rows()
            .map(|n| format!(" [est. {n} rows]"))
            .unwrap_or_default();
        let ncols = cursor.schema().len();
        lines.push(format!("{indent}{label} ({ncols} cols{est})"));
    }
}

impl RecordCursor for ExplainCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.offset >= self.lines.len() {
            return Ok(None);
        }

        let remaining = self.lines.len() - self.offset;
        let n = remaining.min(max_rows);
        let mut result = RecordBatch::new(self.schema.clone());

        for line in &self.lines[self.offset..self.offset + n] {
            result.append_row(&[Value::Str(line.clone())]);
        }
        self.offset += n;
        Ok(Some(result))
    }

    fn estimated_rows(&self) -> Option<u64> {
        Some(self.lines.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_emits_plan_lines() {
        let lines = vec![
            "Scan trades (5 cols)".to_string(),
            "  Filter (price > 100)".to_string(),
            "  Sort (timestamp ASC)".to_string(),
            "  Limit (10)".to_string(),
        ];
        let mut cursor = ExplainCursor::new(lines.clone());

        assert_eq!(cursor.schema()[0].0, "QUERY PLAN");

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }

        assert_eq!(all.len(), 4);
        assert_eq!(all[0], Value::Str("Scan trades (5 cols)".into()));
    }
}
