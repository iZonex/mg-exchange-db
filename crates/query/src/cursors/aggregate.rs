//! Aggregate cursor (GROUP BY).

use std::collections::HashMap;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;

use crate::batch::RecordBatch;
use crate::plan::{AggregateKind, Value};
use crate::record_cursor::RecordCursor;

/// Accumulator for a single aggregate function.
struct Accumulator {
    kind: AggregateKind,
    sum: f64,
    count: u64,
    min: Option<Value>,
    max: Option<Value>,
    first: Option<Value>,
    last: Option<Value>,
}

impl Accumulator {
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
            _ => None,
        };

        if let Some(n) = numeric {
            self.sum += n;
        }

        // Update min.
        match &self.min {
            None => self.min = Some(val.clone()),
            Some(cur) => {
                if val.cmp_coerce(cur) == Some(std::cmp::Ordering::Less) {
                    self.min = Some(val.clone());
                }
            }
        }

        // Update max.
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

/// Cursor that performs GROUP BY aggregation.
///
/// Materializes the source, groups rows, computes aggregates, then
/// streams out the result.
pub struct AggregateCursor {
    source: Option<Box<dyn RecordCursor>>,
    group_by: Vec<String>,
    aggregates: Vec<(AggregateKind, String)>,
    result: Option<RecordBatch>,
    current_row: usize,
    schema: Vec<(String, ColumnType)>,
}

impl AggregateCursor {
    pub fn new(
        source: Box<dyn RecordCursor>,
        group_by: Vec<String>,
        aggregates: Vec<(AggregateKind, String)>,
    ) -> Self {
        // Build output schema: group columns + aggregate results.
        let source_schema = source.schema();
        let mut schema = Vec::new();
        for gb in &group_by {
            let ct = source_schema
                .iter()
                .find(|(n, _)| n == gb)
                .map(|(_, ct)| *ct)
                .unwrap_or(ColumnType::Varchar);
            schema.push((gb.clone(), ct));
        }
        for (kind, col) in &aggregates {
            let name = format!("{kind:?}({col})").to_lowercase();
            let ct = match kind {
                AggregateKind::Count | AggregateKind::CountDistinct => ColumnType::I64,
                _ => ColumnType::F64,
            };
            schema.push((name, ct));
        }

        Self {
            source: Some(source),
            group_by,
            aggregates,
            result: None,
            current_row: 0,
            schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let mut source = self.source.take().expect("source already consumed");
        let source_schema: Vec<(String, ColumnType)> = source.schema().to_vec();

        // Group-by column indices.
        let gb_indices: Vec<usize> = self
            .group_by
            .iter()
            .filter_map(|name| source_schema.iter().position(|(n, _)| n == name))
            .collect();

        // Aggregate column indices.
        let agg_col_indices: Vec<usize> = self
            .aggregates
            .iter()
            .filter_map(|(_, col)| source_schema.iter().position(|(n, _)| n == col))
            .collect();

        // Collect all rows.
        let mut groups: HashMap<Vec<String>, Vec<Accumulator>> = HashMap::new();
        // Preserve insertion order.
        let mut group_order: Vec<Vec<Value>> = Vec::new();

        loop {
            match source.next_batch(1024)? {
                None => break,
                Some(batch) => {
                    for r in 0..batch.row_count() {
                        // Build group key.
                        let key: Vec<String> = gb_indices
                            .iter()
                            .map(|&idx| format!("{}", batch.get_value(r, idx)))
                            .collect();

                        let key_values: Vec<Value> = gb_indices
                            .iter()
                            .map(|&idx| batch.get_value(r, idx))
                            .collect();

                        let accs = groups.entry(key.clone()).or_insert_with(|| {
                            group_order.push(key_values);
                            self.aggregates
                                .iter()
                                .map(|(kind, _)| Accumulator::new(*kind))
                                .collect()
                        });

                        for (i, &col_idx) in agg_col_indices.iter().enumerate() {
                            let val = batch.get_value(r, col_idx);
                            accs[i].accumulate(&val);
                        }
                    }
                }
            }
        }

        // If no group-by columns, produce a single aggregate row.
        let mut result = RecordBatch::new(self.schema.clone());

        if self.group_by.is_empty() {
            // Single group covering all rows.
            if let Some(accs) = groups.values().next() {
                let mut row: Vec<Value> = Vec::new();
                for acc in accs {
                    row.push(acc.finalize());
                }
                result.append_row(&row);
            } else {
                // No rows at all — produce zeros/nulls.
                let row: Vec<Value> = self
                    .aggregates
                    .iter()
                    .map(|(kind, _)| match kind {
                        AggregateKind::Count | AggregateKind::CountDistinct => Value::I64(0),
                        _ => Value::Null,
                    })
                    .collect();
                result.append_row(&row);
            }
        } else {
            for key_values in &group_order {
                let key: Vec<String> = key_values.iter().map(|v| format!("{v}")).collect();
                if let Some(accs) = groups.get(&key) {
                    let mut row = key_values.clone();
                    for acc in accs {
                        row.push(acc.finalize());
                    }
                    result.append_row(&row);
                }
            }
        }

        self.result = Some(result);
        Ok(())
    }
}

impl RecordCursor for AggregateCursor {
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
