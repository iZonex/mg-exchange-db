//! Index scan cursor — uses precomputed row IDs for targeted reads.
//!
//! Instead of scanning all rows and filtering, this cursor reads only
//! the rows at specific offsets, as determined by an index lookup.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Reads specific row IDs from a source cursor, skipping non-matching rows.
///
/// In a full implementation this would integrate with bitmap indices on disk.
/// Here it wraps a source cursor and only emits rows at the given offsets.
pub struct IndexScanCursor {
    source: Box<dyn RecordCursor>,
    /// Sorted row IDs to emit (0-based global row offsets).
    row_ids: Vec<usize>,
    /// Next position in `row_ids` to look for.
    next_id_pos: usize,
    /// Global row counter across all source batches.
    global_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl IndexScanCursor {
    /// Create an index scan cursor.
    ///
    /// `row_ids` must be sorted in ascending order.
    pub fn new(source: Box<dyn RecordCursor>, mut row_ids: Vec<usize>) -> Self {
        let schema = source.schema().to_vec();
        row_ids.sort_unstable();
        row_ids.dedup();
        Self {
            source,
            row_ids,
            next_id_pos: 0,
            global_row: 0,
            schema,
        }
    }
}

impl RecordCursor for IndexScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.next_id_pos >= self.row_ids.len() {
            return Ok(None);
        }

        let schema: Vec<(String, ColumnType)> = self.schema.clone();
        let mut result = RecordBatch::new(schema);

        while result.row_count() < max_rows && self.next_id_pos < self.row_ids.len() {
            match self.source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let batch_start = self.global_row;
                    let batch_end = batch_start + batch.row_count();

                    while self.next_id_pos < self.row_ids.len() {
                        let target = self.row_ids[self.next_id_pos];
                        if target >= batch_end {
                            break;
                        }
                        if target >= batch_start {
                            let local_row = target - batch_start;
                            let row: Vec<Value> = (0..batch.columns.len())
                                .map(|c| batch.get_value(local_row, c))
                                .collect();
                            result.append_row(&row);
                            if result.row_count() >= max_rows {
                                self.next_id_pos += 1;
                                self.global_row = batch_end;
                                return Ok(Some(result));
                            }
                        }
                        self.next_id_pos += 1;
                    }

                    self.global_row = batch_end;
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
    fn index_scan_picks_specific_rows() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = IndexScanCursor::new(Box::new(source), vec![1, 3, 7]);

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                all.push(batch.get_value(r, 0));
            }
        }
        assert_eq!(all, vec![Value::I64(1), Value::I64(3), Value::I64(7)]);
    }
}
