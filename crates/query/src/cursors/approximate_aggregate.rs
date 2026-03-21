//! Approximate aggregate — HyperLogLog-based approximate count distinct.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Approximate COUNT(DISTINCT col) using HyperLogLog.
pub struct ApproxAggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    col_idx: usize,
    schema: Vec<(String, ColumnType)>,
    registers: Vec<u8>,
    emitted: bool,
}

const NUM_REGISTERS: usize = 256;

impl ApproxAggregateCursor {
    pub fn new(source: Box<dyn RecordCursor>, col_name: &str) -> Self {
        let col_idx = source.schema().iter().position(|(n, _)| n == col_name).unwrap_or(0);
        let schema = vec![(format!("approx_distinct({col_name})"), ColumnType::I64)];
        Self { source: Some(source), col_idx, schema, registers: vec![0u8; NUM_REGISTERS], emitted: false }
    }

    fn hash_value(v: &Value) -> u64 {
        let mut h = DefaultHasher::new();
        match v {
            Value::I64(n) => n.hash(&mut h),
            Value::Str(s) => s.hash(&mut h),
            Value::Timestamp(n) => n.hash(&mut h),
            Value::F64(n) => n.to_bits().hash(&mut h),
            Value::Null => 0u8.hash(&mut h),
        }
        h.finish()
    }

    fn estimate(&self) -> u64 {
        let m = NUM_REGISTERS as f64;
        let sum: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        // Small range correction: use linear counting when many registers are 0.
        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        if raw <= 2.5 * m && zeros > 0 {
            // Linear counting
            (m * (m / zeros as f64).ln()) as u64
        } else {
            raw as u64
        }
    }
}

impl RecordCursor for ApproxAggregateCursor {
    fn schema(&self) -> &[(String, ColumnType)] { &self.schema }

    fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RecordBatch>> {
        if self.emitted { return Ok(None); }
        if let Some(mut source) = self.source.take() {
            while let Some(b) = source.next_batch(1024)? {
                for r in 0..b.row_count() {
                    let hash = Self::hash_value(&b.get_value(r, self.col_idx));
                    let idx = (hash & (NUM_REGISTERS as u64 - 1)) as usize;
                    let w = (hash >> 8) | 1;
                    let rho = (w.trailing_zeros() + 1) as u8;
                    if rho > self.registers[idx] {
                        self.registers[idx] = rho;
                    }
                }
            }
        }
        self.emitted = true;
        let mut result = RecordBatch::new(self.schema.clone());
        result.append_row(&[Value::I64(self.estimate() as i64)]);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::memory::MemoryCursor;

    #[test]
    fn approx_count_distinct() {
        let schema = vec![("v".to_string(), ColumnType::I64)];
        let rows: Vec<Vec<Value>> = (0..100).map(|i| vec![Value::I64(i % 10)]).collect();
        let source = MemoryCursor::from_rows(schema, &rows);
        let mut cursor = ApproxAggregateCursor::new(Box::new(source), "v");
        let batch = cursor.next_batch(1).unwrap().unwrap();
        let approx = match batch.get_value(0, 0) { Value::I64(n) => n, _ => 0 };
        // HLL is approximate; should be roughly 10.
        assert!(approx >= 5 && approx <= 20, "approx was {approx}");
    }
}
