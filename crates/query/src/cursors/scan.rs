//! Full table scan cursor — reads partitions lazily.

use std::path::PathBuf;

use exchange_common::error::Result;
use exchange_common::types::ColumnType;
use exchange_core::column::{FixedColumnReader, VarColumnReader};

use crate::batch::RecordBatch;
use crate::plan::Value;
use crate::record_cursor::RecordCursor;

/// Column reader for a single open partition.
#[allow(dead_code)]
enum ColReader {
    Fixed(FixedColumnReader, ColumnType),
    Var(VarColumnReader, ColumnType),
}

impl ColReader {
    fn row_count(&self) -> u64 {
        match self {
            ColReader::Fixed(r, _) => r.row_count(),
            ColReader::Var(r, _) => r.row_count(),
        }
    }

    fn read_value(&self, row: u64) -> Value {
        match self {
            ColReader::Fixed(r, ct) => match ct {
                ColumnType::I64 => Value::I64(r.read_i64(row)),
                ColumnType::F64 => Value::F64(r.read_f64(row)),
                ColumnType::I32 | ColumnType::Symbol => Value::I64(r.read_i32(row) as i64),
                ColumnType::Timestamp => Value::Timestamp(r.read_i64(row)),
                ColumnType::F32 => {
                    Value::F64(f32::from_le_bytes(r.read_raw(row).try_into().unwrap()) as f64)
                }
                ColumnType::I16 => {
                    Value::I64(i16::from_le_bytes(r.read_raw(row).try_into().unwrap()) as i64)
                }
                ColumnType::I8 => Value::I64(r.read_raw(row)[0] as i8 as i64),
                ColumnType::Boolean => Value::I64(if r.read_raw(row)[0] != 0 { 1 } else { 0 }),
                _ => Value::Null,
            },
            ColReader::Var(r, _) => Value::Str(r.read_str(row).to_string()),
        }
    }
}

/// Full table scan cursor that reads partitions on demand.
pub struct ScanCursor {
    partitions: Vec<PathBuf>,
    current_partition: usize,
    /// (column_index_in_table, name, type)
    columns_to_read: Vec<(usize, String, ColumnType)>,
    schema: Vec<(String, ColumnType)>,
    current_row: u64,
    partition_rows: u64,
    /// Lazily opened column readers for the current partition.
    readers: Vec<(usize, ColReader)>,
    opened: bool,
}

impl ScanCursor {
    /// Create a new scan cursor.
    ///
    /// `columns_to_read` contains tuples of `(column_index, name, type)`.
    pub fn new(
        partitions: Vec<PathBuf>,
        columns_to_read: Vec<(usize, String, ColumnType)>,
    ) -> Self {
        let schema: Vec<(String, ColumnType)> = columns_to_read
            .iter()
            .map(|(_, name, ct)| (name.clone(), *ct))
            .collect();
        Self {
            partitions,
            current_partition: 0,
            columns_to_read,
            schema,
            current_row: 0,
            partition_rows: 0,
            readers: Vec::new(),
            opened: false,
        }
    }

    fn open_partition(&mut self) -> Result<bool> {
        if self.current_partition >= self.partitions.len() {
            return Ok(false);
        }

        let path = &self.partitions[self.current_partition];
        self.readers.clear();

        for (col_idx, name, col_type) in &self.columns_to_read {
            if col_type.is_variable_length() {
                let data_path = path.join(format!("{name}.d"));
                let index_path = path.join(format!("{name}.i"));
                if data_path.exists() && index_path.exists() {
                    let reader = VarColumnReader::open(&data_path, &index_path)?;
                    self.readers
                        .push((*col_idx, ColReader::Var(reader, *col_type)));
                }
            } else {
                let data_path = path.join(format!("{name}.d"));
                if data_path.exists() {
                    let reader = FixedColumnReader::open(&data_path, *col_type)?;
                    self.readers
                        .push((*col_idx, ColReader::Fixed(reader, *col_type)));
                }
            }
        }

        self.partition_rows = self
            .readers
            .first()
            .map(|(_, r)| r.row_count())
            .unwrap_or(0);
        self.current_row = 0;
        self.opened = true;
        Ok(true)
    }

    /// Try to advance to the next non-empty partition.
    fn advance_partition(&mut self) -> Result<bool> {
        loop {
            self.current_partition += 1;
            if !self.open_partition()? {
                return Ok(false);
            }
            if self.partition_rows > 0 {
                return Ok(true);
            }
        }
    }
}

impl RecordCursor for ScanCursor {
    fn schema(&self) -> &[(String, ColumnType)] {
        &self.schema
    }

    fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        // Open the first partition if we haven't yet.
        if !self.opened {
            if !self.open_partition()? {
                return Ok(None);
            }
            // Skip empty partitions.
            if self.partition_rows == 0 && !self.advance_partition()? {
                return Ok(None);
            }
        }

        let mut batch = RecordBatch::new(self.schema.clone());
        let mut rows_emitted = 0usize;

        while rows_emitted < max_rows {
            if self.current_row >= self.partition_rows && !self.advance_partition()? {
                break;
            }

            // Read rows from current partition.
            let rows_remaining_in_partition = (self.partition_rows - self.current_row) as usize;
            let rows_to_read = rows_remaining_in_partition.min(max_rows - rows_emitted);

            for _ in 0..rows_to_read {
                let mut row = Vec::with_capacity(self.columns_to_read.len());
                for (target_col_idx, _) in self.columns_to_read.iter().enumerate() {
                    // Find the reader for this column.
                    let val = if let Some((_, reader)) = self.readers.get(target_col_idx) {
                        reader.read_value(self.current_row)
                    } else {
                        Value::Null
                    };
                    row.push(val);
                }
                batch.append_row(&row);
                self.current_row += 1;
                rows_emitted += 1;
            }
        }

        if rows_emitted == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}
