//! Top-K cursor — efficient bounded sort using a binary heap.
//!
//! Shares the ORDER BY metadata via `Arc` so that each `HeapRow` only
//! stores the row values and a cheap pointer, avoiding per-row clones
//! of the column-index and descending-flag vectors.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::{OrderBy, Value};
use crate::record_cursor::RecordCursor;

/// Shared ORDER BY metadata — allocated once and referenced by every `HeapRow`.
struct OrderSpec {
    /// Column indices for ORDER BY columns.
    col_indices: Vec<usize>,
    /// Whether each ORDER BY column is descending.
    descending: Vec<bool>,
}

/// A row wrapper that implements `Ord` for use in a `BinaryHeap`.
///
/// The ordering is *reversed* relative to the desired output order so that
/// the heap naturally evicts the "worst" row when it exceeds capacity K.
struct HeapRow {
    values: Vec<Value>,
    /// Shared reference to ORDER BY metadata (avoids per-row cloning).
    spec: Arc<OrderSpec>,
}

impl HeapRow {
    fn cmp_key(&self, other: &Self) -> Ordering {
        for (i, &col_idx) in self.spec.col_indices.iter().enumerate() {
            let va = &self.values[col_idx];
            let vb = &other.values[col_idx];
            let cmp = va.cmp_coerce(vb).unwrap_or(Ordering::Equal);
            let cmp = if self.spec.descending[i] { cmp.reverse() } else { cmp };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.cmp_key(other) == Ordering::Equal
    }
}

impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cmp_key(other)
    }
}

/// Efficient Top-K cursor: maintains a bounded heap instead of full sort.
///
/// Much faster than Sort+Limit for small K on large data, since it only
/// keeps K rows in memory at any time and avoids sorting the entire input.
/// ORDER BY metadata is shared via `Arc` to eliminate per-row allocation.
pub struct TopKCursor {
    source: Option<Box<dyn RecordCursor>>,
    k: usize,
    order_by: Vec<OrderBy>,
    /// Shared ORDER BY spec (allocated once, referenced by every HeapRow).
    spec: Arc<OrderSpec>,
    /// Materialized result after heap processing.
    result: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl TopKCursor {
    /// Create a new Top-K cursor that returns the top `k` rows.
    pub fn new(source: Box<dyn RecordCursor>, k: usize, order_by: Vec<OrderBy>) -> Self {
        let schema = source.schema().to_vec();

        let order_col_indices: Vec<usize> = order_by
            .iter()
            .filter_map(|ob| schema.iter().position(|(name, _)| name == &ob.column))
            .collect();

        let descending: Vec<bool> = order_by.iter().map(|ob| ob.descending).collect();

        let spec = Arc::new(OrderSpec {
            col_indices: order_col_indices,
            descending,
        });

        Self {
            source: Some(source),
            k,
            order_by,
            spec,
            result: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");

        let mut heap: BinaryHeap<HeapRow> = BinaryHeap::with_capacity(self.k + 1);

        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    for r in 0..batch.row_count() {
                        let values: Vec<Value> = (0..batch.columns.len())
                            .map(|c| batch.get_value(r, c))
                            .collect();

                        let row = HeapRow {
                            values,
                            spec: Arc::clone(&self.spec),
                        };

                        heap.push(row);

                        // Evict the worst row if we exceed K.
                        if heap.len() > self.k {
                            heap.pop();
                        }
                    }
                }
            }
        }

        // Extract rows from the heap and sort them in the correct order.
        let mut rows: Vec<Vec<Value>> = heap.into_iter().map(|hr| hr.values).collect();

        // Sort in the desired output order.
        let spec = &self.spec;
        let order_by = &self.order_by;
        rows.sort_by(|a, b| {
            for (i, &col_idx) in spec.col_indices.iter().enumerate() {
                let va = &a[col_idx];
                let vb = &b[col_idx];
                let cmp = va.cmp_coerce(vb).unwrap_or(Ordering::Equal);
                let cmp = if order_by[i].descending {
                    cmp.reverse()
                } else {
                    cmp
                };
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &rows {
            batch.append_row(row);
        }
        self.result = Some(batch);
        Ok(())
    }
}

impl RecordCursor for TopKCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.result.is_none() {
            self.materialize()?;
        }

        let mat = self.result.as_ref().unwrap();
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

    #[test]
    fn top_10_from_10000() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..10000).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: true,
        }];

        let mut cursor = TopKCursor::new(Box::new(source), 10, order_by);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values.len(), 10);
        // Top 10 descending: 9999, 9998, ..., 9990
        assert_eq!(all_values, vec![9999, 9998, 9997, 9996, 9995, 9994, 9993, 9992, 9991, 9990]);
    }

    #[test]
    fn top_k_ascending() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::I64(i)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: false,
        }];

        let mut cursor = TopKCursor::new(Box::new(source), 5, order_by);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn top_k_larger_than_input() {
        let schema = vec![("val".to_string(), ColumnType::I64)];
        let rows = vec![vec![Value::I64(3)], vec![Value::I64(1)], vec![Value::I64(2)]];
        let source = MemoryCursor::from_rows(schema, &rows);

        let order_by = vec![OrderBy {
            column: "val".to_string(),
            descending: false,
        }];

        let mut cursor = TopKCursor::new(Box::new(source), 100, order_by);

        let mut all_values = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                if let Value::I64(v) = batch.get_value(r, 0) {
                    all_values.push(v);
                }
            }
        }

        assert_eq!(all_values, vec![1, 2, 3]);
    }
}
