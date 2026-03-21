//! K-way merge sort cursor — merges N pre-sorted cursors.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::{OrderBy, Value};
use crate::record_cursor::RecordCursor;

/// K-way merge sort cursor: merges N pre-sorted cursors while maintaining
/// the sort order. Used for parallel partition scans that need to produce
/// globally sorted output.
pub struct MergeSortCursor {
    sources: Vec<Box<dyn RecordCursor>>,
    /// Current row and position within the buffered batch for each source.
    heads: Vec<Option<HeadEntry>>,
    order_by: Vec<OrderBy>,
    schema: Vec<(String, ColumnType)>,
    /// Column indices for ORDER BY columns (resolved once from schema).
    order_col_indices: Vec<Option<usize>>,
    /// Whether heads have been initialized.
    initialized: bool,
}

/// A buffered batch with a read position for one source.
struct HeadEntry {
    batch: RecordBatch,
    position: usize,
}

impl HeadEntry {
    fn current_row(&self) -> Vec<Value> {
        (0..self.batch.columns.len())
            .map(|c| self.batch.get_value(self.position, c))
            .collect()
    }

    fn exhausted(&self) -> bool {
        self.position >= self.batch.row_count()
    }
}

impl MergeSortCursor {
    /// Create a merge sort cursor from multiple pre-sorted source cursors.
    pub fn new(sources: Vec<Box<dyn RecordCursor>>, order_by: Vec<OrderBy>) -> Self {
        let schema = if sources.is_empty() {
            Vec::new()
        } else {
            sources[0].schema().to_vec()
        };

        let order_col_indices = order_by
            .iter()
            .map(|ob| schema.iter().position(|(name, _)| name == &ob.column))
            .collect();

        let head_count = sources.len();
        Self {
            sources,
            heads: (0..head_count).map(|_| None).collect(),
            order_by,
            schema,
            order_col_indices,
            initialized: false,
        }
    }

    /// Initialize heads by reading the first batch from each source.
    fn initialize(&mut self) -> Result<()> {
        for i in 0..self.sources.len() {
            self.refill(i)?;
        }
        self.initialized = true;
        Ok(())
    }

    /// Refill the head for source `i` by reading its next batch.
    fn refill(&mut self, i: usize) -> Result<()> {
        match self.sources[i].next_batch(1024)? {
            Some(batch) => {
                self.heads[i] = Some(HeadEntry { batch, position: 0 });
            }
            None => {
                self.heads[i] = None;
            }
        }
        Ok(())
    }

    /// Find the index of the source whose current row is smallest.
    fn pick_min(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        let mut best_row: Option<Vec<Value>> = None;

        for (i, head) in self.heads.iter().enumerate() {
            if let Some(entry) = head {
                if entry.exhausted() {
                    continue;
                }
                let row = entry.current_row();
                let is_better = match &best_row {
                    None => true,
                    Some(br) => self.compare_rows(&row, br) == std::cmp::Ordering::Less,
                };
                if is_better {
                    best = Some(i);
                    best_row = Some(row);
                }
            }
        }

        best
    }

    /// Compare two rows according to the ORDER BY specification.
    fn compare_rows(&self, a: &[Value], b: &[Value]) -> std::cmp::Ordering {
        for (idx, ob) in self.order_by.iter().enumerate() {
            if let Some(col_idx) = self.order_col_indices[idx] {
                let va = &a[col_idx];
                let vb = &b[col_idx];
                let cmp = va.cmp_coerce(vb).unwrap_or(std::cmp::Ordering::Equal);
                let cmp = if ob.descending { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl RecordCursor for MergeSortCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if !self.initialized {
            self.initialize()?;
        }

        let mut result = RecordBatch::new(self.schema.clone());

        while result.row_count() < max_rows {
            let min_idx = match self.pick_min() {
                Some(idx) => idx,
                None => break,
            };

            // Get the current row from the winning source.
            let row = self.heads[min_idx].as_ref().unwrap().current_row();
            result.append_row(&row);

            // Advance the position in that source.
            if let Some(entry) = self.heads[min_idx].as_mut() {
                entry.position += 1;
                if entry.exhausted() {
                    // Try to refill from the source.
                    self.refill(min_idx)?;
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
    fn merge_three_sorted_streams() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(1)],
                vec![Value::I64(4)],
                vec![Value::I64(7)],
            ],
        );
        let s2 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(2)],
                vec![Value::I64(5)],
                vec![Value::I64(8)],
            ],
        );
        let s3 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(3)],
                vec![Value::I64(6)],
                vec![Value::I64(9)],
            ],
        );

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: false,
        }];

        let mut cursor =
            MergeSortCursor::new(vec![Box::new(s1), Box::new(s2), Box::new(s3)], order_by);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn merge_descending() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(9)],
                vec![Value::I64(6)],
                vec![Value::I64(3)],
            ],
        );
        let s2 = MemoryCursor::from_rows(
            schema.clone(),
            &[
                vec![Value::I64(8)],
                vec![Value::I64(5)],
                vec![Value::I64(2)],
            ],
        );

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: true,
        }];

        let mut cursor = MergeSortCursor::new(vec![Box::new(s1), Box::new(s2)], order_by);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![9, 8, 6, 5, 3, 2]);
    }

    #[test]
    fn merge_empty_sources() {
        let schema = vec![("val".to_string(), ColumnType::I64)];

        let s1 = MemoryCursor::from_rows(schema.clone(), &[]);
        let s2 = MemoryCursor::from_rows(schema.clone(), &[vec![Value::I64(1)]]);
        let s3 = MemoryCursor::from_rows(schema.clone(), &[]);

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: false,
        }];

        let mut cursor =
            MergeSortCursor::new(vec![Box::new(s1), Box::new(s2), Box::new(s3)], order_by);

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
}
