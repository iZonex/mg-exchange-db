//! SAMPLE BY cursor — time-bucketed aggregation.

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::{AggregateKind, FillMode, Value};
use crate::record_cursor::RecordCursor;

/// Time-bucketed aggregation cursor.
///
/// Groups rows into fixed-interval time buckets based on a timestamp column,
/// computes aggregates within each bucket, and optionally fills empty buckets
/// according to the specified fill mode.
pub struct SampleByCursor {
    source: Option<Box<dyn RecordCursor>>,
    /// Bucket interval in nanoseconds.
    interval_nanos: i64,
    /// Column index of the timestamp column in the source schema.
    ts_col: usize,
    /// Aggregate functions to compute: (kind, column_index_in_source).
    aggregates: Vec<(AggregateKind, usize)>,
    /// How to fill empty buckets.
    fill_mode: FillMode,
    /// Materialized result.
    result: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
    /// Names of aggregate source columns (for schema building).
    #[allow(dead_code)]
    agg_col_names: Vec<String>,
}

/// Accumulator for a single aggregate within a time bucket.
struct BucketAcc {
    kind: AggregateKind,
    sum: f64,
    count: u64,
    min: Option<Value>,
    max: Option<Value>,
    first: Option<Value>,
    last: Option<Value>,
}

impl BucketAcc {
    fn new(kind: AggregateKind) -> Self {
        Self {
            kind,
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
            first: None,
            last: None,
        }
    }

    fn accumulate(&mut self, val: &Value) {
        self.count += 1;
        self.last = Some(val.clone());
        if self.first.is_none() {
            self.first = Some(val.clone());
        }

        let numeric = match val {
            Value::I64(n) => Some(*n as f64),
            Value::F64(n) => Some(*n),
            Value::Timestamp(n) => Some(*n as f64),
            _ => None,
        };

        if let Some(n) = numeric {
            self.sum += n;
        }

        match &self.min {
            None => self.min = Some(val.clone()),
            Some(cur) => {
                if val.cmp_coerce(cur) == Some(std::cmp::Ordering::Less) {
                    self.min = Some(val.clone());
                }
            }
        }

        match &self.max {
            None => self.max = Some(val.clone()),
            Some(cur) => {
                if val.cmp_coerce(cur) == Some(std::cmp::Ordering::Greater) {
                    self.max = Some(val.clone());
                }
            }
        }
    }

    fn finalize(&self) -> Value {
        match self.kind {
            AggregateKind::Count | AggregateKind::CountDistinct => Value::I64(self.count as i64),
            AggregateKind::Sum => Value::F64(self.sum),
            AggregateKind::Avg => {
                if self.count > 0 {
                    Value::F64(self.sum / self.count as f64)
                } else {
                    Value::Null
                }
            }
            AggregateKind::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateKind::Max => self.max.clone().unwrap_or(Value::Null),
            AggregateKind::First => self.first.clone().unwrap_or(Value::Null),
            AggregateKind::Last => self.last.clone().unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }
}

impl SampleByCursor {
    /// Create a new SAMPLE BY cursor.
    ///
    /// `ts_col` is the index of the timestamp column in the source schema.
    /// `aggregates` is a list of `(AggregateKind, source_column_index)` pairs.
    /// `interval_nanos` is the bucket width in nanoseconds.
    pub fn new(
        source: Box<dyn RecordCursor>,
        interval_nanos: i64,
        ts_col: usize,
        aggregates: Vec<(AggregateKind, usize)>,
        fill_mode: FillMode,
    ) -> Self {
        let source_schema = source.schema().to_vec();

        // Build output schema: bucket_start (timestamp) + one column per aggregate.
        let mut schema = vec![("bucket".to_string(), ColumnType::Timestamp)];
        let mut agg_col_names = Vec::new();

        for (kind, col_idx) in &aggregates {
            let col_name = if *col_idx < source_schema.len() {
                &source_schema[*col_idx].0
            } else {
                "?"
            };
            agg_col_names.push(col_name.to_string());
            let name = format!("{kind:?}({col_name})").to_lowercase();
            let ct = match kind {
                AggregateKind::Count | AggregateKind::CountDistinct => ColumnType::I64,
                _ => ColumnType::F64,
            };
            schema.push((name, ct));
        }

        Self {
            source: Some(source),
            interval_nanos,
            ts_col,
            aggregates,
            fill_mode,
            result: None,
            current_row: 0,
            schema,
            agg_col_names,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");

        let fill = !matches!(self.fill_mode, FillMode::None);

        // ── Streaming single-pass path (no fill, common case) ────────
        //
        // When there is no fill mode, we can bucket rows in a single pass
        // without collecting them all first. The source is typically
        // already sorted by timestamp for time-series tables, so we get
        // correct results without a sort. Even for unsorted input, the
        // result is correct (just unordered buckets). We detect monotonic
        // timestamps as we go and only fall back to sorting if needed.
        if !fill {
            let mut buckets: Vec<(i64, Vec<BucketAcc>)> = Vec::new();
            let mut is_sorted = true;
            let mut last_ts = i64::MIN;

            loop {
                match source.next_batch(1024)? {
                    None => break,
                    Some(batch) => {
                        let ncols = batch.columns.len();
                        for r in 0..batch.row_count() {
                            let ts = Self::extract_ts(&batch.get_value(r, self.ts_col));
                            if ts < last_ts {
                                is_sorted = false;
                            }
                            last_ts = ts;

                            let row_bucket = (ts / self.interval_nanos) * self.interval_nanos;

                            // Fast path: current bucket matches last bucket.
                            if !buckets.is_empty() && buckets.last().unwrap().0 == row_bucket {
                                let bucket_pos = buckets.len() - 1;
                                for (i, (_, col_idx)) in self.aggregates.iter().enumerate() {
                                    let val = if *col_idx < ncols {
                                        batch.get_value(r, *col_idx)
                                    } else {
                                        Value::Null
                                    };
                                    buckets[bucket_pos].1[i].accumulate(&val);
                                }
                            } else {
                                // Check if we already have this bucket (unsorted case).
                                let found = if is_sorted {
                                    None
                                } else {
                                    buckets.iter().position(|(b, _)| *b == row_bucket)
                                };

                                if let Some(pos) = found {
                                    for (i, (_, col_idx)) in self.aggregates.iter().enumerate() {
                                        let val = if *col_idx < ncols {
                                            batch.get_value(r, *col_idx)
                                        } else {
                                            Value::Null
                                        };
                                        buckets[pos].1[i].accumulate(&val);
                                    }
                                } else {
                                    let mut accs: Vec<BucketAcc> = self
                                        .aggregates
                                        .iter()
                                        .map(|(kind, _)| BucketAcc::new(*kind))
                                        .collect();
                                    for (i, (_, col_idx)) in self.aggregates.iter().enumerate() {
                                        let val = if *col_idx < ncols {
                                            batch.get_value(r, *col_idx)
                                        } else {
                                            Value::Null
                                        };
                                        accs[i].accumulate(&val);
                                    }
                                    buckets.push((row_bucket, accs));
                                }
                            }
                        }
                    }
                }
            }

            // Sort buckets by timestamp for deterministic output.
            if !is_sorted {
                buckets.sort_by_key(|(ts, _)| *ts);
            }

            let mut result = RecordBatch::new(self.schema.clone());
            for (bucket_ts, accs) in &buckets {
                let mut row = vec![Value::Timestamp(*bucket_ts)];
                for acc in accs {
                    row.push(acc.finalize());
                }
                result.append_row(&row);
            }
            self.result = Some(result);
            return Ok(());
        }

        // ── Fill mode path (needs all rows to determine range) ───────
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

        if rows.is_empty() {
            self.result = Some(RecordBatch::new(self.schema.clone()));
            return Ok(());
        }

        // Sort by timestamp column.
        let ts_col = self.ts_col;
        rows.sort_by(|a, b| {
            let ta = Self::extract_ts(&a[ts_col]);
            let tb = Self::extract_ts(&b[ts_col]);
            ta.cmp(&tb)
        });

        // Determine bucket boundaries.
        let first_ts = Self::extract_ts(&rows[0][ts_col]);
        let last_ts = Self::extract_ts(&rows[rows.len() - 1][ts_col]);
        let bucket_start = (first_ts / self.interval_nanos) * self.interval_nanos;

        // Pre-create all buckets from first to last.
        let mut buckets: Vec<(i64, Vec<BucketAcc>)> = Vec::new();
        let mut b = bucket_start;
        while b <= last_ts {
            let accs: Vec<BucketAcc> = self
                .aggregates
                .iter()
                .map(|(kind, _)| BucketAcc::new(*kind))
                .collect();
            buckets.push((b, accs));
            b += self.interval_nanos;
        }

        // Assign rows to buckets.
        for row in &rows {
            let ts = Self::extract_ts(&row[ts_col]);
            let bucket_idx = ((ts - bucket_start) / self.interval_nanos) as usize;
            if bucket_idx < buckets.len() {
                for (i, (_, col_idx)) in self.aggregates.iter().enumerate() {
                    let val = if *col_idx < row.len() {
                        &row[*col_idx]
                    } else {
                        &Value::Null
                    };
                    buckets[bucket_idx].1[i].accumulate(val);
                }
            }
        }

        // Apply fill mode and build result.
        let mut result = RecordBatch::new(self.schema.clone());
        let mut prev_values: Option<Vec<Value>> = None;

        for (bucket_ts, accs) in &buckets {
            let has_data = accs.iter().any(|a| a.count > 0);

            if has_data || !matches!(self.fill_mode, FillMode::None) {
                let mut row = vec![Value::Timestamp(*bucket_ts)];

                if has_data {
                    let agg_values: Vec<Value> = accs.iter().map(|a| a.finalize()).collect();
                    row.extend(agg_values.clone());
                    prev_values = Some(agg_values);
                } else {
                    // Fill empty bucket.
                    match &self.fill_mode {
                        FillMode::None => continue,
                        FillMode::Null => {
                            for _ in &self.aggregates {
                                row.push(Value::Null);
                            }
                        }
                        FillMode::Prev => {
                            if let Some(prev) = &prev_values {
                                row.extend(prev.iter().cloned());
                            } else {
                                for _ in &self.aggregates {
                                    row.push(Value::Null);
                                }
                            }
                        }
                        FillMode::Value(v) => {
                            for _ in &self.aggregates {
                                row.push(v.clone());
                            }
                        }
                        FillMode::Linear => {
                            // Linear interpolation is complex; fall back to NULL.
                            for _ in &self.aggregates {
                                row.push(Value::Null);
                            }
                        }
                    }
                }

                result.append_row(&row);
            }
        }

        self.result = Some(result);
        Ok(())
    }

    fn extract_ts(val: &Value) -> i64 {
        match val {
            Value::Timestamp(n) => *n,
            Value::I64(n) => *n,
            _ => 0,
        }
    }
}

impl RecordCursor for SampleByCursor {
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
    fn basic_bucketing() {
        let schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("price".to_string(), ColumnType::F64),
        ];
        // 3 buckets of interval 100: [0..100), [100..200), [200..300)
        let rows = vec![
            vec![Value::Timestamp(10), Value::F64(1.0)],
            vec![Value::Timestamp(20), Value::F64(2.0)],
            vec![Value::Timestamp(110), Value::F64(3.0)],
            vec![Value::Timestamp(150), Value::F64(4.0)],
            vec![Value::Timestamp(250), Value::F64(5.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = SampleByCursor::new(
            Box::new(source),
            100,                                          // interval
            0,                                            // ts_col
            vec![(AggregateKind::Avg, 1), (AggregateKind::Count, 1)], // aggregates
            FillMode::None,
        );

        let mut all_rows = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all_rows.push(row);
            }
        }

        // 3 buckets.
        assert_eq!(all_rows.len(), 3);

        // Bucket [0..100): ts=0, avg=1.5, count=2
        assert_eq!(all_rows[0][0], Value::Timestamp(0));
        assert_eq!(all_rows[0][1], Value::F64(1.5));
        assert_eq!(all_rows[0][2], Value::I64(2));

        // Bucket [100..200): ts=100, avg=3.5, count=2
        assert_eq!(all_rows[1][0], Value::Timestamp(100));
        assert_eq!(all_rows[1][1], Value::F64(3.5));
        assert_eq!(all_rows[1][2], Value::I64(2));

        // Bucket [200..300): ts=200, avg=5.0, count=1
        assert_eq!(all_rows[2][0], Value::Timestamp(200));
        assert_eq!(all_rows[2][1], Value::F64(5.0));
        assert_eq!(all_rows[2][2], Value::I64(1));
    }

    #[test]
    fn fill_null() {
        let schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("val".to_string(), ColumnType::F64),
        ];
        // Gap at bucket [100..200).
        let rows = vec![
            vec![Value::Timestamp(10), Value::F64(1.0)],
            vec![Value::Timestamp(210), Value::F64(2.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = SampleByCursor::new(
            Box::new(source),
            100,
            0,
            vec![(AggregateKind::Sum, 1)],
            FillMode::Null,
        );

        let mut all_rows = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all_rows.push(row);
            }
        }

        // 3 buckets: [0..100), [100..200), [200..300)
        assert_eq!(all_rows.len(), 3);

        // Middle bucket should have 0.0 for sum (empty bucket, NULL fill).
        // Actually with FillMode::Null, the empty bucket gets Value::Null,
        // but ColumnData::F64 stores 0.0 for Null. Let's check the raw value.
        assert_eq!(all_rows[0][0], Value::Timestamp(0));
        assert_eq!(all_rows[0][1], Value::F64(1.0));

        assert_eq!(all_rows[1][0], Value::Timestamp(100));
        // This is the filled bucket - NULL fill for empty F64 bucket.
        assert_eq!(all_rows[1][1], Value::Null);

        assert_eq!(all_rows[2][0], Value::Timestamp(200));
        assert_eq!(all_rows[2][1], Value::F64(2.0));
    }

    #[test]
    fn fill_prev() {
        let schema = vec![
            ("ts".to_string(), ColumnType::Timestamp),
            ("val".to_string(), ColumnType::F64),
        ];
        let rows = vec![
            vec![Value::Timestamp(10), Value::F64(42.0)],
            vec![Value::Timestamp(210), Value::F64(99.0)],
        ];
        let source = MemoryCursor::from_rows(schema, &rows);

        let mut cursor = SampleByCursor::new(
            Box::new(source),
            100,
            0,
            vec![(AggregateKind::Last, 1)],
            FillMode::Prev,
        );

        let mut all_rows = Vec::new();
        while let Some(batch) = cursor.next_batch(100).unwrap() {
            for r in 0..batch.row_count() {
                let row: Vec<Value> = (0..batch.columns.len())
                    .map(|c| batch.get_value(r, c))
                    .collect();
                all_rows.push(row);
            }
        }

        assert_eq!(all_rows.len(), 3);

        // Bucket [0,100): last=42.0
        assert_eq!(all_rows[0][1], Value::F64(42.0));
        // Bucket [100,200): filled with prev -> 42.0
        assert_eq!(all_rows[1][1], Value::F64(42.0));
        // Bucket [200,300): last=99.0
        assert_eq!(all_rows[2][1], Value::F64(99.0));
    }
}
