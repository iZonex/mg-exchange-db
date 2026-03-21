//! Sort cursor — materializes the source then emits sorted batches.
//!
//! For large datasets (>= 1024 rows), uses parallel merge sort via rayon
//! to exploit multiple cores. For small datasets, falls back to sequential
//! sort to avoid thread-pool overhead.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::{OrderBy, Value};
use crate::record_cursor::RecordCursor;

/// Cursor that sorts all input rows before emitting.
///
/// This cursor must materialize the entire source since sorting requires
/// seeing all rows. Results are then streamed out in sorted order.
pub struct SortCursor {
    source: Option<Box<dyn RecordCursor>>,
    order_by: Vec<OrderBy>,
    materialized: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl SortCursor {
    pub fn new(source: Box<dyn RecordCursor>, order_by: Vec<OrderBy>) -> Self {
        let schema = source.schema().to_vec();
        Self {
            source: Some(source),
            order_by,
            materialized: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let source = self.source.take().expect("source already consumed");
        let mut all_rows = self.drain_source(source)?;

        // Build column name -> index mapping.
        let col_indices: Vec<Option<usize>> = self
            .order_by
            .iter()
            .map(|ob| {
                self.schema
                    .iter()
                    .position(|(name, _)| name == &ob.column)
            })
            .collect();

        // For large datasets, use parallel sort via the parallel_sort module.
        // Convert ORDER BY column names to positional indices so the parallel
        // sort comparator can resolve them.
        if all_rows.len() >= 1024 {
            let par_order_by: Vec<OrderBy> = self
                .order_by
                .iter()
                .zip(col_indices.iter())
                .filter_map(|(ob, idx)| {
                    idx.map(|i| OrderBy {
                        column: i.to_string(),
                        descending: ob.descending,
                    })
                })
                .collect();
            let parallelism = rayon::current_num_threads().max(2);
            crate::parallel_sort::parallel_sort(&mut all_rows, &par_order_by, parallelism);
        } else {
            let order_by = &self.order_by;
            all_rows.sort_by(|a, b| {
                for (i, ob) in order_by.iter().enumerate() {
                    if let Some(col_idx) = col_indices[i] {
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
            });
        }

        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &all_rows {
            batch.append_row(row);
        }
        self.materialized = Some(batch);
        Ok(())
    }

    fn drain_source(&self, mut source: Box<dyn RecordCursor>) -> Result<Vec<Vec<Value>>> {
        let mut rows = Vec::new();
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
        Ok(rows)
    }
}

impl RecordCursor for SortCursor {
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
