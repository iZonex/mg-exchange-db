//! Columnar record batch for efficient batch-oriented query processing.
//!
//! `RecordBatch` stores rows in a column-major layout, which is more
//! cache-friendly for analytical scans than `Vec<Vec<Value>>`.

use exchange_common::types::ColumnType;

use crate::plan::Value;

/// A batch of rows organized by column.
pub struct RecordBatch {
    pub columns: Vec<ColumnData>,
    pub schema: Vec<(String, ColumnType)>,
    pub row_count: usize,
}

/// Column-oriented storage for a single column within a `RecordBatch`.
#[derive(Debug, Clone)]
pub enum ColumnData {
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(Vec<String>),
    Timestamp(Vec<i64>),
    /// N null values (column has no data).
    Null(usize),
    Bool(Vec<bool>),
}

impl ColumnData {
    /// Create an empty `ColumnData` matching the given `ColumnType`.
    pub fn empty_for(ct: ColumnType) -> Self {
        match ct {
            ColumnType::I64 | ColumnType::I32 | ColumnType::I16 | ColumnType::I8
            | ColumnType::Symbol => ColumnData::I64(Vec::new()),
            ColumnType::F64 | ColumnType::F32 => ColumnData::F64(Vec::new()),
            ColumnType::Varchar | ColumnType::Binary | ColumnType::Char
            | ColumnType::Uuid | ColumnType::IPv4 | ColumnType::GeoHash
            | ColumnType::VarcharSlice => ColumnData::Str(Vec::new()),
            ColumnType::Timestamp | ColumnType::Date => ColumnData::Timestamp(Vec::new()),
            ColumnType::Boolean => ColumnData::Bool(Vec::new()),
            _ => ColumnData::I64(Vec::new()),
        }
    }

    /// Number of values stored.
    pub fn len(&self) -> usize {
        match self {
            ColumnData::I64(v) => v.len(),
            ColumnData::F64(v) => v.len(),
            ColumnData::Str(v) => v.len(),
            ColumnData::Timestamp(v) => v.len(),
            ColumnData::Null(n) => *n,
            ColumnData::Bool(v) => v.len(),
        }
    }

    /// Push a `Value` into this column, coercing to the column's native type.
    pub fn push(&mut self, val: &Value) {
        match self {
            ColumnData::I64(v) => match val {
                Value::I64(n) => v.push(*n),
                Value::F64(n) => v.push(*n as i64),
                Value::Timestamp(n) => v.push(*n),
                _ => v.push(0),
            },
            ColumnData::F64(v) => match val {
                Value::F64(n) => v.push(*n),
                Value::I64(n) => v.push(*n as f64),
                Value::Null => v.push(f64::NAN),
                _ => v.push(0.0),
            },
            ColumnData::Str(v) => match val {
                Value::Str(s) => v.push(s.clone()),
                Value::Null => v.push(String::new()),
                other => v.push(format!("{other}")),
            },
            ColumnData::Timestamp(v) => match val {
                Value::Timestamp(n) => v.push(*n),
                Value::I64(n) => v.push(*n),
                _ => v.push(0),
            },
            ColumnData::Null(n) => *n += 1,
            ColumnData::Bool(v) => match val {
                Value::I64(n) => v.push(*n != 0),
                _ => v.push(false),
            },
        }
    }

    /// Get a `Value` at the given row index.
    pub fn get(&self, row: usize) -> Value {
        match self {
            ColumnData::I64(v) => Value::I64(v[row]),
            ColumnData::F64(v) => if v[row].is_nan() { Value::Null } else { Value::F64(v[row]) },
            ColumnData::Str(v) => Value::Str(v[row].clone()),
            ColumnData::Timestamp(v) => Value::Timestamp(v[row]),
            ColumnData::Null(_) => Value::Null,
            ColumnData::Bool(v) => Value::I64(if v[row] { 1 } else { 0 }),
        }
    }
}

impl RecordBatch {
    /// Create an empty batch with the given schema.
    pub fn new(schema: Vec<(String, ColumnType)>) -> Self {
        let columns: Vec<ColumnData> = schema.iter().map(|(_, ct)| ColumnData::empty_for(*ct)).collect();
        Self {
            columns,
            schema,
            row_count: 0,
        }
    }

    /// Append a row of `Value`s to this batch.
    pub fn append_row(&mut self, values: &[Value]) {
        for (i, col) in self.columns.iter_mut().enumerate() {
            if i < values.len() {
                col.push(&values[i]);
            } else {
                col.push(&Value::Null);
            }
        }
        self.row_count += 1;
    }

    /// Get a single value by (row, col) index.
    pub fn get_value(&self, row: usize, col: usize) -> Value {
        self.columns[col].get(row)
    }

    /// Number of rows in this batch.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Convert the entire batch back to row-major format.
    pub fn to_rows(&self) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(self.row_count);
        for r in 0..self.row_count {
            let mut row = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                row.push(col.get(r));
            }
            rows.push(row);
        }
        rows
    }

    /// Build a `RecordBatch` from row-major data.
    ///
    /// Column types are inferred from the first non-null value in each column.
    pub fn from_rows(col_names: &[String], rows: &[Vec<Value>]) -> Self {
        let num_cols = col_names.len();
        // Infer types from first non-null value in each column.
        let mut types: Vec<ColumnType> = vec![ColumnType::I64; num_cols];
        for row in rows {
            for (i, val) in row.iter().enumerate() {
                if i < num_cols {
                    match val {
                        Value::I64(_) => types[i] = ColumnType::I64,
                        Value::F64(_) => types[i] = ColumnType::F64,
                        Value::Str(_) => types[i] = ColumnType::Varchar,
                        Value::Timestamp(_) => types[i] = ColumnType::Timestamp,
                        Value::Null => {}
                    }
                }
            }
        }

        let schema: Vec<(String, ColumnType)> = col_names
            .iter()
            .zip(types.iter())
            .map(|(n, t)| (n.clone(), *t))
            .collect();

        let mut batch = Self::new(schema);
        for row in rows {
            batch.append_row(row);
        }
        batch
    }

    /// Return a sub-batch from `offset` for `len` rows.
    pub fn slice(&self, offset: usize, len: usize) -> RecordBatch {
        let end = (offset + len).min(self.row_count);
        let actual_len = if offset >= self.row_count {
            0
        } else {
            end - offset
        };

        let columns: Vec<ColumnData> = self
            .columns
            .iter()
            .map(|col| match col {
                ColumnData::I64(v) => ColumnData::I64(v[offset..offset + actual_len].to_vec()),
                ColumnData::F64(v) => ColumnData::F64(v[offset..offset + actual_len].to_vec()),
                ColumnData::Str(v) => ColumnData::Str(v[offset..offset + actual_len].to_vec()),
                ColumnData::Timestamp(v) => {
                    ColumnData::Timestamp(v[offset..offset + actual_len].to_vec())
                }
                ColumnData::Null(_) => ColumnData::Null(actual_len),
                ColumnData::Bool(v) => ColumnData::Bool(v[offset..offset + actual_len].to_vec()),
            })
            .collect();

        RecordBatch {
            columns,
            schema: self.schema.clone(),
            row_count: actual_len,
        }
    }

    /// Concatenate multiple batches (must share the same schema).
    pub fn concat(batches: &[&RecordBatch]) -> RecordBatch {
        if batches.is_empty() {
            return RecordBatch {
                columns: Vec::new(),
                schema: Vec::new(),
                row_count: 0,
            };
        }

        let schema = batches[0].schema.clone();
        let total_rows: usize = batches.iter().map(|b| b.row_count).sum();
        let num_cols = schema.len();

        let mut columns: Vec<ColumnData> = schema.iter().map(|(_, ct)| ColumnData::empty_for(*ct)).collect();

        for batch in batches {
            for row in 0..batch.row_count {
                for col_idx in 0..num_cols {
                    columns[col_idx].push(&batch.columns[col_idx].get(row));
                }
            }
        }

        RecordBatch {
            columns,
            schema,
            row_count: total_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_rows() {
        let cols = vec!["id".to_string(), "name".to_string(), "price".to_string()];
        let rows = vec![
            vec![Value::I64(1), Value::Str("BTC".into()), Value::F64(50000.0)],
            vec![Value::I64(2), Value::Str("ETH".into()), Value::F64(3000.0)],
            vec![Value::I64(3), Value::Str("SOL".into()), Value::F64(100.0)],
        ];

        let batch = RecordBatch::from_rows(&cols, &rows);
        assert_eq!(batch.row_count(), 3);

        let back = batch.to_rows();
        assert_eq!(back, rows);
    }

    #[test]
    fn slice_batch() {
        let cols = vec!["x".to_string()];
        let rows = vec![
            vec![Value::I64(10)],
            vec![Value::I64(20)],
            vec![Value::I64(30)],
            vec![Value::I64(40)],
        ];
        let batch = RecordBatch::from_rows(&cols, &rows);
        let sliced = batch.slice(1, 2);
        assert_eq!(sliced.row_count(), 2);
        assert_eq!(sliced.get_value(0, 0), Value::I64(20));
        assert_eq!(sliced.get_value(1, 0), Value::I64(30));
    }

    #[test]
    fn concat_batches() {
        let cols = vec!["v".to_string()];
        let b1 = RecordBatch::from_rows(&cols, &[vec![Value::I64(1)], vec![Value::I64(2)]]);
        let b2 = RecordBatch::from_rows(&cols, &[vec![Value::I64(3)]]);
        let merged = RecordBatch::concat(&[&b1, &b2]);
        assert_eq!(merged.row_count(), 3);
        assert_eq!(merged.get_value(0, 0), Value::I64(1));
        assert_eq!(merged.get_value(2, 0), Value::I64(3));
    }

    #[test]
    fn empty_batch() {
        let schema = vec![("a".to_string(), ColumnType::I64)];
        let batch = RecordBatch::new(schema);
        assert_eq!(batch.row_count(), 0);
        assert!(batch.to_rows().is_empty());
    }
}
