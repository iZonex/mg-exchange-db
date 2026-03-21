//! Hash-based GROUP BY cursor with streaming output.
//!
//! Uses a hash map to group rows by key columns and compute aggregates,
//! then streams out the result. Unlike `AggregateCursor`, this cursor
//! takes column indices rather than column names, making it usable as
//! a lower-level building block.

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Supported aggregate operations for HashGroupByCursor.
#[derive(Debug, Clone, Copy)]
pub enum HashAggOp {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

struct HashAccumulator {
    op: HashAggOp,
    sum: f64,
    count: u64,
    min: Option<f64>,
    max: Option<f64>,
}

impl HashAccumulator {
    fn new(op: HashAggOp) -> Self {
        Self {
            op,
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
        }
    }

    fn accumulate(&mut self, val: &Value) {
        let n = match val {
            Value::I64(v) => *v as f64,
            Value::F64(v) => *v,
            Value::Timestamp(v) => *v as f64,
            _ => return,
        };
        self.count += 1;
        self.sum += n;
        self.min = Some(self.min.map_or(n, |m: f64| m.min(n)));
        self.max = Some(self.max.map_or(n, |m: f64| m.max(n)));
    }

    fn finalize(&self) -> Value {
        match self.op {
            HashAggOp::Count => Value::I64(self.count as i64),
            HashAggOp::Sum => Value::F64(self.sum),
            HashAggOp::Min => self.min.map(Value::F64).unwrap_or(Value::Null),
            HashAggOp::Max => self.max.map(Value::F64).unwrap_or(Value::Null),
            HashAggOp::Avg => {
                if self.count > 0 {
                    Value::F64(self.sum / self.count as f64)
                } else {
                    Value::Null
                }
            }
        }
    }
}

/// Hash-based GROUP BY cursor.
///
/// Groups rows by `group_cols` and applies `agg_specs` (op, column_index) aggregates.
pub struct HashGroupByCursor {
    source: Option<Box<dyn RecordCursor>>,
    group_cols: Vec<usize>,
    agg_specs: Vec<(HashAggOp, usize)>,
    result: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl HashGroupByCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        group_cols: Vec<usize>,
        agg_specs: Vec<(HashAggOp, usize)>,
    ) -> Self {
        let src_schema = source.schema();
        let mut schema: Vec<(String, ColumnType)> =
            group_cols.iter().map(|&i| src_schema[i].clone()).collect();
        for (op, col) in &agg_specs {
            let name = format!("{:?}({})", op, src_schema[*col].0).to_lowercase();
            let ct = match op {
                HashAggOp::Count => ColumnType::I64,
                _ => ColumnType::F64,
            };
            schema.push((name, ct));
        }

        Self {
            source: Some(source),
            group_cols,
            agg_specs,
            result: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");

        // Use a u64 hash as the primary key for O(1) lookup, falling back
        // to full key comparison on collision via the stored `Vec<Value>`.
        // This avoids allocating a `Vec<u8>` serialized key per row.
        #[allow(clippy::type_complexity)]
        let mut groups: HashMap<u64, Vec<(Vec<Value>, Vec<HashAccumulator>)>> = HashMap::new();
        let mut key_order: Vec<u64> = Vec::new();
        let mut unique_keys: Vec<Vec<Value>> = Vec::new();

        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    let ncols = batch.columns.len();
                    for r in 0..batch.row_count() {
                        let row: Vec<Value> = (0..ncols).map(|c| batch.get_value(r, c)).collect();

                        let hash = hash_group_key(&row, &self.group_cols);

                        let bucket = groups.entry(hash).or_insert_with(|| {
                            key_order.push(hash);
                            Vec::new()
                        });

                        // Find existing entry with matching key values (handle collisions).
                        let found = bucket.iter_mut().find(|(kv, _)| {
                            kv.len() == self.group_cols.len()
                                && self
                                    .group_cols
                                    .iter()
                                    .enumerate()
                                    .all(|(ki, &ci)| kv[ki] == row[ci])
                        });

                        match found {
                            Some((_, accs)) => {
                                for (i, (_, col)) in self.agg_specs.iter().enumerate() {
                                    accs[i].accumulate(&row[*col]);
                                }
                            }
                            None => {
                                let key_vals: Vec<Value> =
                                    self.group_cols.iter().map(|&i| row[i].clone()).collect();
                                let mut accs: Vec<HashAccumulator> = self
                                    .agg_specs
                                    .iter()
                                    .map(|(op, _)| HashAccumulator::new(*op))
                                    .collect();
                                for (i, (_, col)) in self.agg_specs.iter().enumerate() {
                                    accs[i].accumulate(&row[*col]);
                                }
                                unique_keys.push(key_vals.clone());
                                bucket.push((key_vals, accs));
                            }
                        }
                    }
                }
            }
        }

        // Build the result in insertion order.
        let mut result = RecordBatch::new(self.schema.clone());
        for key_vals in &unique_keys {
            let hash = hash_group_key_from_values(key_vals);
            if let Some(bucket) = groups.get(&hash) {
                for (kv, accs) in bucket {
                    if kv == key_vals {
                        let mut row = kv.clone();
                        for acc in accs {
                            row.push(acc.finalize());
                        }
                        result.append_row(&row);
                        break;
                    }
                }
            }
        }

        self.result = Some(result);
        Ok(())
    }
}

/// Hash group key directly from column values without intermediate allocation.
///
/// Uses a simple FNV-like mixing function to produce a 64-bit hash from the
/// key column values. This avoids the `Vec<u8>` serialization overhead that
/// dominated GROUP BY memory usage.
#[inline]
fn hash_group_key(row: &[Value], key_indices: &[usize]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325; // FNV offset basis
    for &idx in key_indices {
        match &row[idx] {
            Value::Null => {
                h ^= 0xFF;
                h = h.wrapping_mul(0x100000001b3);
            }
            Value::I64(v) => {
                for b in v.to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
            Value::F64(v) => {
                for b in v.to_bits().to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
            Value::Str(s) => {
                for b in (s.len() as u32).to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                for b in s.as_bytes() {
                    h ^= *b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
            Value::Timestamp(v) => {
                for b in v.to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
        }
    }
    h
}

/// Hash a pre-extracted group key (Vec<Value>) for lookup.
#[inline]
fn hash_group_key_from_values(key_vals: &[Value]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for val in key_vals {
        match val {
            Value::Null => {
                h ^= 0xFF;
                h = h.wrapping_mul(0x100000001b3);
            }
            Value::I64(v) => {
                for b in v.to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
            Value::F64(v) => {
                for b in v.to_bits().to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
            Value::Str(s) => {
                for b in (s.len() as u32).to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                for b in s.as_bytes() {
                    h ^= *b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
            Value::Timestamp(v) => {
                for b in v.to_le_bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
            }
        }
    }
    h
}

impl RecordCursor for HashGroupByCursor {
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
    fn hash_group_by_sum() {
        let schema = vec![
            ("category".to_string(), ColumnType::Varchar),
            ("amount".to_string(), ColumnType::F64),
        ];
        let rows = vec![
            vec![Value::Str("A".into()), Value::F64(10.0)],
            vec![Value::Str("B".into()), Value::F64(20.0)],
            vec![Value::Str("A".into()), Value::F64(30.0)],
            vec![Value::Str("B".into()), Value::F64(40.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = HashGroupByCursor::new(
            Box::new(source),
            vec![0],                   // group by category
            vec![(HashAggOp::Sum, 1)], // sum(amount)
        );

        let mut all = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all.push(row);
            }
        }

        assert_eq!(all.len(), 2);
        // A: sum = 40
        assert_eq!(all[0][0], Value::Str("A".into()));
        assert_eq!(all[0][1], Value::F64(40.0));
        // B: sum = 60
        assert_eq!(all[1][0], Value::Str("B".into()));
        assert_eq!(all[1][1], Value::F64(60.0));
    }
}
